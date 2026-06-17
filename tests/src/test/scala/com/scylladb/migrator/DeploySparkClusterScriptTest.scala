package com.scylladb.migrator

import java.nio.file.{ Files, Path, Paths }

import scala.sys.process.{ Process, ProcessLogger }

class DeploySparkClusterScriptTest extends munit.FunSuite {
  import DeploySparkClusterScriptTest.CommandResult

  private val repoRoot = findRepoRoot()
  private val script = repoRoot.resolve("deploy_spark_cluster.py")
  private val python = sys.env.getOrElse("PYTHON", "python3")
  private val missingPrivateKey =
    repoRoot.resolve("target/deploy-script-test/nonexistent-key")

  test("deploy_spark_cluster.py compiles with Python") {
    val result = runPython("-m", "py_compile", script.toString)

    assertEquals(result.exitCode, 0, result.output)
  }

  test("empty config metadata path resolves as absent") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |print(module.resolve_path("") is None)
         |print(module.config_file_from_args_or_metadata(None, {"config_file": ""}) is None)
         |print(module.config_file_from_args_or_metadata(None, {"config_file": "/tmp/does-not-exist-scylla-migrator.yml"}) is None)
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertEquals(result.output.linesIterator.toList, List("True", "True", "True"))
  }

  test("migration type is persisted without a new config file") {
    val result = runPython(
      "-c",
      s"""import importlib.util, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |with tempfile.TemporaryDirectory() as state_dir:
         |    metadata = {"config_file": "/tmp/config.yaml", "migration_type": "cql"}
         |    module.remember_config_file(
         |        Path(state_dir),
         |        metadata,
         |        config_file=None,
         |        migration_type="alternator",
         |    )
         |    saved = module.read_json(Path(state_dir) / "metadata.json")
         |    print(saved["migration_type"])
         |    print(saved["config_file"])
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertEquals(result.output.linesIterator.toList, List("alternator", "/tmp/config.yaml"))
  }

  test("SSH private key resolver reports empty values clearly") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |for value in (None, ""):
         |    try:
         |        module.resolve_ssh_private_key(value)
         |    except SystemExit as exc:
         |        print(exc)
         |try:
         |    module.resolve_ssh_private_key("~/does-not-exist-scylla-migrator-key")
         |except SystemExit as exc:
         |    print(exc)
         |print(Path("~/does-not-exist-scylla-migrator-key").expanduser().resolve())
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    val lines = result.output.linesIterator.toList
    assertEquals(lines(0), "SSH private key is required. Pass --ssh-private-key.")
    assertEquals(lines(1), "SSH private key is required. Pass --ssh-private-key.")
    assertEquals(lines(2), s"SSH private key does not exist: ${lines(3)}")
  }

  test("invalid JSON state files report a clear CLI error") {
    val badJson = repoRoot.resolve("target/deploy-script-test/invalid.json")
    Files.createDirectories(badJson.getParent)
    Files.writeString(badJson, "{not valid json")

    val result = runPython(
      "-c",
      s"""import importlib.util
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |try:
         |    module.read_json(Path("${badJson}"))
         |except SystemExit as exc:
         |    print(exc)
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, s"Invalid JSON in ${badJson}")
  }

  test("state directory deletion refuses unsafe paths") {
    val result = runPython(
      "-c",
      s"""import importlib.util, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    state_dir = Path(temp_dir) / ".deploy_spark_cluster"
         |    state_dir.mkdir()
         |    module.write_state_dir_marker(state_dir)
         |    (state_dir / "terraform.tfstate").write_text("{}")
         |    (state_dir / "main.tf").write_text("# generated")
         |    module.write_json(state_dir / "metadata.json", {"state_dir": str(state_dir.resolve())})
         |    module.validate_state_dir_safe_to_delete(state_dir)
         |    print("safe")
         |    (state_dir / "user-file.txt").write_text("do not delete")
         |    try:
         |        module.validate_state_dir_safe_to_delete(state_dir)
         |    except SystemExit as exc:
         |        print(exc)
         |try:
         |    module.validate_state_dir_safe_to_delete(Path("${repoRoot}"))
         |except SystemExit as exc:
         |    print(exc)
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "safe")
    assertOutputContains(result.output, "unexpected files are present: user-file.txt")
    assertOutputContains(result.output, "Refusing to delete unsafe state directory")
  }

  test("Terraform output IP addresses are validated") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |module.validate_terraform_output_ips({
         |    "master": {"public_ip": "203.0.113.10", "private_ip": "10.42.1.10"},
         |    "workers": [{"public_ip": "203.0.113.11", "private_ip": "10.42.1.11"}],
         |})
         |print("valid")
         |try:
         |    module.validate_terraform_output_ips({
         |        "master": {"public_ip": "203.0.113.10", "private_ip": "10.42.1.10"},
         |        "workers": [{"public_ip": "not an ip", "private_ip": "10.42.1.11"}],
         |    })
         |except SystemExit as exc:
         |    print(exc)
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "valid")
    assertOutputContains(
      result.output,
      "Terraform output workers[1].public_ip is not a valid IP address"
    )
  }

  test("Terraform output JSON parse and schema errors are CLI errors") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |try:
         |    module.parse_terraform_output_json("{not valid json")
         |except SystemExit as exc:
         |    print(exc)
         |try:
         |    module.parse_terraform_output_json('{"master": {"type": "object"}}')
         |except SystemExit as exc:
         |    print(exc)
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "Invalid JSON from terraform output -json")
    assertOutputContains(result.output, "output 'master' must be an object with a value field")
  }

  test("top-level help lists supported subcommands") {
    val result = runScript("--help")

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "deploy")
    assertOutputContains(result.output, "show")
    assertOutputContains(result.output, "run")
    assertOutputContains(result.output, "redeploy")
    assertOutputContains(result.output, "destroy")
    assertOutputContains(result.output, "Deploy and operate a Spark cluster")
  }

  test("deploy help documents required safe access CIDRs") {
    val result = runScript("deploy", "--help")

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--allowed-ssh-cidr")
    assertOutputContains(result.output, "--allowed-web-cidr")
    assertOutputContains(result.output, "--allow-public-access")
    assertOutputContains(result.output, "--vpc-id")
    assertOutputContains(result.output, "--subnet-id")
    assertOutputContains(result.output, "--owner-tag")
    assertOutputContains(result.output, "--insecure-ssh")
  }

  test("deploy requires explicit SSH and web access CIDRs") {
    val result = runScript("deploy", "--skip-ansible")

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--allowed-ssh-cidr")
    assertOutputContains(result.output, "--allowed-web-cidr")
  }

  test("deploy rejects public SSH CIDR unless explicitly allowed") {
    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "0.0.0.0/0",
      "--allowed-web-cidr",
      "203.0.113.10/32",
      "--ssh-private-key",
      missingPrivateKey.toString
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--allowed-ssh-cidr opens access to the public internet")
  }

  test("deploy rejects public web CIDR unless explicitly allowed") {
    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "203.0.113.10/32",
      "--allowed-web-cidr",
      "0.0.0.0/0",
      "--ssh-private-key",
      missingPrivateKey.toString
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--allowed-web-cidr opens access to the public internet")
  }

  test("deploy rejects non-IPv4 CIDRs") {
    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "2001:db8::/32",
      "--allowed-web-cidr",
      "203.0.113.10/32"
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "must be an IPv4 CIDR")
  }

  test("deploy requires existing VPC and subnet IDs together") {
    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "203.0.113.10/32",
      "--allowed-web-cidr",
      "203.0.113.10/32",
      "--vpc-id",
      "vpc-0123456789abcdef0",
      "--ssh-private-key",
      missingPrivateKey.toString
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--vpc-id and --subnet-id must be provided together")
  }

  test("deploy requires generated subnet CIDR to be inside generated VPC CIDR") {
    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "203.0.113.10/32",
      "--allowed-web-cidr",
      "203.0.113.10/32",
      "--vpc-cidr",
      "10.42.0.0/16",
      "--public-subnet-cidr",
      "10.43.1.0/24",
      "--ssh-private-key",
      missingPrivateKey.toString
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--public-subnet-cidr (10.43.1.0/24)")
    assertOutputContains(result.output, "must be contained in --vpc-cidr (10.42.0.0/16)")
  }

  test("deploy validates local config file before Terraform work") {
    val missingConfig = repoRoot.resolve("target/deploy-script-test/missing.yaml")
    Files.deleteIfExists(missingConfig)

    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "203.0.113.10/32",
      "--allowed-web-cidr",
      "203.0.113.10/32",
      "--config-file",
      missingConfig.toString,
      "--ssh-private-key",
      missingPrivateKey.toString
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, s"Config file does not exist: ${missingConfig}")
    assert(!result.output.contains("SSH private key does not exist"), result.output)
  }

  test("AWS architecture inference keeps x86 GPU families distinct from Graviton") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |print(module.infer_aws_architecture("i8g.4xlarge"))
         |print(module.infer_aws_architecture("g5.4xlarge"))
         |print(module.infer_aws_architecture("g6.4xlarge"))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertEquals(result.output.linesIterator.toList, List("arm64", "x86_64", "x86_64"))
  }

  test("allow-public-access gates the public CIDR guard") {
    Files.deleteIfExists(missingPrivateKey)

    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allow-public-access",
      "--allowed-ssh-cidr",
      "0.0.0.0/0",
      "--allowed-web-cidr",
      "0.0.0.0/0",
      "--ssh-private-key",
      missingPrivateKey.toString
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "SSH private key does not exist")
    assert(!result.output.contains("opens access to the public internet"), result.output)
  }

  test("run help documents migration and SSH safety options") {
    val result = runScript("run", "--help")

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--migration-type")
    assertOutputContains(result.output, "--validator")
    assertOutputContains(result.output, "--insecure-ssh")
  }

  test("insecure SSH mode is command-scoped and not persisted in metadata") {
    val deployScript = Files.readString(script)

    assert(!deployScript.contains("\"insecure_ssh\""), deployScript)
    assert(!deployScript.contains("metadata.get(\"insecure_ssh\""), deployScript)
  }

  test("run performs a remote config presence preflight before submit") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "ensure_remote_config_exists")
    assertOutputContains(deployScript, "Required Migrator config was not found")
    assertOutputContains(deployScript, "Pass --config-file to upload it before running")
  }

  test("run counts only live Spark workers as registered") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "worker.get('state') == 'ALIVE'")
    assertOutputContains(deployScript, ") -> int | None:")
    assertOutputContains(deployScript, "return None")
    assertOutputContains(deployScript, "registered = \"unknown\" if count is None else str(count)")
    assertOutputContains(deployScript, "if current_workers is None:")
    assertOutputContains(deployScript, "elif current_workers == 0 and expected_workers > 0:")
    assertOutputContains(deployScript, "zero live workers are registered")
    assert(!deployScript.contains("print(len(data.get('workers', [])))"), deployScript)
  }

  test("run launches remote Spark submit through nohup") {
    val deployScript = Files.readString(script)
    val readme = Files.readString(repoRoot.resolve("README.md"))

    assertOutputContains(deployScript, "def remote_submit_command")
    assertOutputContains(deployScript, "nohup {script}")
    assertOutputContains(deployScript, "2>&1 < /dev/null")
    assertOutputContains(deployScript, "echo $! >")
    assertOutputContains(deployScript, "remote_submit_command(submit_script)")
    assertOutputContains(readme, "The submit script is launched with `nohup`")
  }

  test("deploy metadata and Terraform vars are written with restricted permissions") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "path.chmod(0o600)")
    assertOutputContains(deployScript, "state_dir.chmod(0o700)")
    assertOutputContains(deployScript, "path.parent.chmod(0o700)")
  }

  test("fresh deploy clears state-local known hosts") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "known_hosts = known_hosts_path(state_dir)")
    assertOutputContains(deployScript, "\"ssh_known_hosts\": str(known_hosts_path(state_dir))")
    assert(!deployScript.contains("metadata.get(\"ssh_known_hosts\")"), deployScript)
    assertOutputContains(deployScript, "if not (state_dir / \"terraform.tfstate\").exists():")
    assertOutputContains(deployScript, "known_hosts.write_text(\"\")")
    assertOutputContains(deployScript, "known_hosts.chmod(0o600)")
  }

  test("Ansible SSH host key checking matches direct SSH behavior") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "StrictHostKeyChecking=accept-new")
    assert(!deployScript.contains("StrictHostKeyChecking=yes"), deployScript)
  }

  test("Ansible SSH common args quote known hosts path") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |print(module.ssh_options(Path("/tmp/key"), Path("/tmp/known hosts;rm -rf/known_hosts"), False))
         |print(module.ansible_ssh_common_args(Path("/tmp/known hosts;rm -rf/known_hosts"), False))
         |print(module.ansible_ssh_common_args(Path("/tmp/ignored"), True))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(
      result.output,
      "'UserKnownHostsFile=\"/tmp/known hosts;rm -rf/known_hosts\"'"
    )
    assertOutputContains(
      result.output,
      "UserKnownHostsFile=\"/tmp/known hosts;rm -rf/known_hosts\""
    )
    assertOutputContains(result.output, "StrictHostKeyChecking=accept-new")
    assertOutputContains(result.output, "UserKnownHostsFile=/dev/null")
  }

  test("SSH and SCP commands use a shared connection timeout") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "def ssh_options")
    assertOutputContains(deployScript, "ConnectTimeout=10")
    assertOutputContains(deployScript, "*ssh_options(private_key, known_hosts, insecure)")
  }

  test("repository Ansible config enables host key checking by default") {
    val ansibleConfig = Files.readString(repoRoot.resolve("ansible/ansible.cfg"))

    assertOutputContains(ansibleConfig, "host_key_checking = True")
    assert(!ansibleConfig.contains("host_key_checking = False"), ansibleConfig)
  }

  test("Ansible getting-started docs use explicit inventory and private key arguments") {
    val ansibleDocs =
      Files.readString(repoRoot.resolve("docs/source/getting-started/ansible.rst"))

    assertOutputContains(ansibleDocs, "ansible-playbook -i /path/to/inventory.ini")
    assertOutputContains(ansibleDocs, "--private-key /path/to/private-key")
    assertOutputContains(ansibleDocs, "scp -i /path/to/private-key config.yaml")
    assertOutputContains(ansibleDocs, "config.dynamodb.yml")
    assertOutputContains(ansibleDocs, "http://<spark-master-hostname>:18080")
    assertOutputContains(ansibleDocs, "cd /home/ubuntu/scylla-migrator")
    assertOutputContains(ansibleDocs, "Use ``nohup`` or a terminal multiplexer such as ``tmux``")
    assertOutputContains(ansibleDocs, "nohup ./submit-cql-job.sh > submit-cql-job.log 2>&1 &")
    assertOutputContains(ansibleDocs, "./submit-cql-job-validator.sh")
    assert(!ansibleDocs.contains("ansible/inventory/hosts"), ansibleDocs)
    assert(!ansibleDocs.contains("Update ``ansible/ansible.cfg``"), ansibleDocs)
    assert(!ansibleDocs.contains("private_key_file"), ansibleDocs)
  }

  test("Deploy script pins repository Ansible config") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "\"ANSIBLE_CONFIG\": str(ansible_dir / \"ansible.cfg\")")
  }

  test("destroy reports Terraform stdout and stderr on failure") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "Terraform destroy failed.")
    assertOutputContains(deployScript, "Terraform stdout:")
    assertOutputContains(deployScript, "Terraform stderr:")
    assertOutputContains(deployScript, "raise SystemExit(exc.returncode)")
    assertOutputContains(deployScript, "validate_state_dir_safe_to_delete(state_dir)")
    assertOutputContains(deployScript, "shutil.rmtree(state_dir)")
  }

  test("main reports captured subprocess output on command failures") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "print_command_error(exc)")
    assertOutputContains(deployScript, "Command stdout:")
    assertOutputContains(deployScript, "Command stderr:")
    assertOutputContains(deployScript, "shlex.join(exc.cmd)")
  }

  test("run, show, redeploy, and destroy check state before running Terraform") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "require_terraform_state(state_dir)")
    assertOutputContains(deployScript, "State directory does not exist")
    assertOutputContains(deployScript, "Terraform state file does not exist")
    assertOutputContains(deployScript, "terraform.tfstate")
  }

  test("redeploy help documents inventory rerun and SSH safety options") {
    val result = runScript("redeploy", "--help")

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "Rerun Ansible")
    assertOutputContains(result.output, "--ssh-private-key")
    assertOutputContains(result.output, "--migration-type")
    assertOutputContains(result.output, "--config-file")
    assertOutputContains(result.output, "--skip-start")
    assertOutputContains(result.output, "--insecure-ssh")
  }

  test("redeploy refreshes Terraform outputs before SSH and Ansible work") {
    val deployScript = Files.readString(script)

    assertOutputContains(
      deployScript,
      "require_commands([\"terraform\", \"ansible-playbook\", \"ssh\", \"scp\"])"
    )
    assertOutputContains(deployScript, "outputs = terraform_output(state_dir)")
    assertOutputContains(deployScript, "metadata[\"terraform_outputs\"] = outputs")
    assertOutputContains(deployScript, "inventory_path = write_ansible_inventory(")
    assertOutputContains(
      deployScript,
      "wait_for_ssh(all_public_ips, private_key, known_hosts, insecure)"
    )
    assertOutputContains(deployScript, "master_public_ip=outputs[\"master\"][\"public_ip\"]")
    assert(!deployScript.contains("Inventory not found"), deployScript)
    assert(!deployScript.contains("inventory_path.exists()"), deployScript)
    assert(!deployScript.contains("master_host_from_inventory"), deployScript)
    assert(!deployScript.contains("metadata.get(\"terraform_outputs\")"), deployScript)
  }

  test("generated Ansible inventory does not embed private key path") {
    val result = runPython(
      "-c",
      s"""import importlib.util, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |outputs = {
         |    "master": {"public_ip": "203.0.113.10"},
         |    "workers": [{"public_ip": "203.0.113.11"}],
         |}
         |with tempfile.TemporaryDirectory() as state_dir:
         |    inventory = module.write_ansible_inventory(
         |        outputs,
         |        state_dir=Path(state_dir),
         |    )
         |    print(inventory.read_text())
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(
      Files.readString(script),
      "def write_ansible_inventory(\n    outputs: dict[str, Any],\n    *,\n    state_dir: Path,"
    )
    assertOutputContains(
      result.output,
      "spark_master ansible_host=203.0.113.10 ansible_user=ubuntu"
    )
    assertOutputContains(
      result.output,
      "spark_worker1 ansible_host=203.0.113.11 ansible_user=ubuntu"
    )
    assert(!result.output.contains("ansible_ssh_private_key_file"), result.output)
    assert(!result.output.contains("/tmp/key with spaces"), result.output)
  }

  test("Ansible installs the Alternator validator submit script") {
    val playbook = Files.readString(repoRoot.resolve("ansible/scylla-migrator.yml"))

    assertOutputContains(playbook, "submit-alternator-validator.sh")
  }

  test("recommended local Migrator config filenames are ignored") {
    val gitignore = Files.readString(repoRoot.resolve(".gitignore"))

    assertOutputContains(gitignore, "config.yaml")
    assertOutputContains(gitignore, "config.dynamodb.yaml")
    assertOutputContains(gitignore, "config.dynamodb.yml")
  }

  test("deploy script Python dependencies are pinned") {
    val requirements = Files.readString(repoRoot.resolve("requirements.txt"))
    val readme = Files.readString(repoRoot.resolve("README.md"))

    assertOutputContains(requirements, "ansible-core==")
    assert(!requirements.contains("ansible-core\n"), requirements)
    assertOutputContains(readme, "pip install -r requirements.txt")
    assertOutputContains(readme, "`ansible-core` is pinned")
  }

  test("test workflow runs for deploy helper and Ansible changes") {
    val workflow = Files.readString(repoRoot.resolve(".github/workflows/tests.yml"))

    assertOutputContains(workflow, "'deploy_spark_cluster.py'")
    assertOutputContains(workflow, "'requirements.txt'")
    assertOutputContains(workflow, "'ansible/**'")
  }

  test("Ansible derives Spark resource settings from host facts") {
    val playbook = Files.readString(repoRoot.resolve("ansible/scylla-migrator.yml"))

    assertOutputContains(playbook, "Derive Spark hardware settings")
    assertOutputContains(playbook, "spark_worker_cores")
    assertOutputContains(playbook, "spark_worker_memory")
    assertOutputContains(playbook, "spark_executor_cores")
    assertOutputContains(playbook, "spark_executor_memory")
    assertOutputContains(
      playbook,
      "spark_executor_core_candidates: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]"
    )
    assertOutputContains(playbook, "for candidate in spark_executor_core_candidates")
    assert(!playbook.contains("10 if (spark_worker_cores | int)"), playbook)
    assert(!playbook.contains("spark_executor_instances_per_worker"), playbook)
  }

  test("Ansible does not maintain legacy Spark slaves file") {
    val playbook = Files.readString(repoRoot.resolve("ansible/scylla-migrator.yml"))

    assert(!playbook.contains("conf/slaves"), playbook)
    assert(!playbook.contains("Add spark nodes to slaves file"), playbook)
  }

  test("Ansible skips AWS CLI install when binary already exists") {
    val playbook = Files.readString(repoRoot.resolve("ansible/scylla-migrator.yml"))

    assertOutputContains(playbook, "Check whether AWS CLI is installed")
    assertOutputContains(playbook, "path: /usr/local/bin/aws")
    assertOutputContains(playbook, "aws_cli_installed")
    assertOutputContains(playbook, "dest: \"{{ home_dir }}/awscliv2.zip\"")
    assertOutputContains(playbook, "Delete aws zip file")
    assertOutputContains(playbook, "Delete aws install directory")
    assertOutputContains(playbook, "when: not aws_cli_installed.stat.exists")
    assert(playbook.split("when: not aws_cli_installed.stat.exists", -1).length - 1 >= 5, playbook)
    assert(!playbook.contains("path: awscliv2.zip"), playbook)
    assert(!playbook.contains("stat_result"), playbook)
    assert(!playbook.contains("aws_cli_download_bundle"), playbook)
    assert(!playbook.contains("aws_cli_unarchive_installer"), playbook)
  }

  test("Ansible validates Spark archive before skipping download") {
    val playbook = Files.readString(repoRoot.resolve("ansible/scylla-migrator.yml"))

    assertOutputContains(playbook, "Check whether Spark archive exists")
    assertOutputContains(playbook, "spark_archive")
    assertOutputContains(playbook, "Check whether Spark archive is complete")
    assertOutputContains(playbook, "spark_archive_valid")
    assertOutputContains(playbook, "Check whether Spark is already installed")
    assertOutputContains(playbook, "spark_installed")
    assertOutputContains(playbook, "Download spark archive with resume and progress")
    assertOutputContains(playbook, "--continue-at -")
    assertOutputContains(
      playbook,
      "when: not spark_installed.stat.exists and (not spark_archive.stat.exists or (spark_archive_valid.rc | default(1)) != 0)"
    )
  }

  test("Ansible become tasks do not embed sudo commands") {
    val playbook = Files.readString(repoRoot.resolve("ansible/scylla-migrator.yml"))

    assert(!playbook.contains("sudo "), playbook)
  }

  test("Ansible avoids regional EC2 Ubuntu mirrors and retries apt cache updates") {
    val playbook = Files.readString(repoRoot.resolve("ansible/scylla-migrator.yml"))

    assertOutputContains(playbook, "Use canonical Ubuntu ports mirror")
    assertOutputContains(playbook, "https://ports.ubuntu.com/ubuntu-ports")
    assertOutputContains(playbook, "Use canonical Ubuntu archive mirror")
    assertOutputContains(playbook, "https://archive.ubuntu.com/ubuntu")
    assert(!playbook.contains("replace: 'http://ports.ubuntu.com/ubuntu-ports'"), playbook)
    assert(!playbook.contains("replace: 'http://archive.ubuntu.com/ubuntu'"), playbook)
    assertOutputContains(playbook, "Install add-apt-repository dependency")
    assertOutputContains(playbook, "name: software-properties-common")
    assert(
      playbook.indexOf("name: software-properties-common") <
        playbook.indexOf("command: add-apt-repository -y -n universe"),
      playbook
    )
    assertOutputContains(playbook, "update_cache_retries: 12")
    assertOutputContains(playbook, "update_cache_retry_max_delay: 30")
  }

  test("Spark env templates apply derived worker and executor settings") {
    val masterTemplate =
      Files.readString(repoRoot.resolve("ansible/templates/spark-env-master"))
    val workerTemplate =
      Files.readString(repoRoot.resolve("ansible/templates/spark-env-worker"))
    val workerUnit = Files.readString(repoRoot.resolve("ansible/templates/spark-worker.service"))

    assertOutputContains(masterTemplate, "EXECUTOR_CORES={{ master_executor_cores")
    assertOutputContains(masterTemplate, "EXECUTOR_MEMORY={{ master_executor_memory")
    assertOutputContains(masterTemplate, "SPARK_LOCAL_DIRS={{ master_spark_local_dirs")
    assertOutputContains(workerTemplate, "SPARK_WORKER_CORES={{ spark_worker_cores }}")
    assertOutputContains(workerTemplate, "SPARK_WORKER_MEMORY={{ spark_worker_memory }}")
    assertOutputContains(workerTemplate, "SPARK_WORKER_DIR={{ spark_worker_dir }}")
    assertOutputContains(workerUnit, "--cores \"$SPARK_WORKER_CORES\"")
    assertOutputContains(workerUnit, "--memory \"$SPARK_WORKER_MEMORY\"")
  }

  test("Ansible installs Spark systemd unit files") {
    val playbook = Files.readString(repoRoot.resolve("ansible/scylla-migrator.yml"))
    val masterUnit = Files.readString(repoRoot.resolve("ansible/templates/spark-master.service"))
    val historyUnit =
      Files.readString(repoRoot.resolve("ansible/templates/spark-history-server.service"))
    val workerUnit = Files.readString(repoRoot.resolve("ansible/templates/spark-worker.service"))

    assertOutputContains(playbook, "spark-master.service")
    assertOutputContains(playbook, "spark-history-server.service")
    assertOutputContains(playbook, "spark-worker.service")
    assertOutputContains(playbook, "src: spark-env-master")
    assertOutputContains(playbook, "src: spark-env-worker")
    assert(!playbook.contains("spark-env-master-sample"), playbook)
    assert(!playbook.contains("spark-env-worker-sample"), playbook)
    assertOutputContains(masterUnit, "org.apache.spark.deploy.master.Master")
    assertOutputContains(historyUnit, "org.apache.spark.deploy.history.HistoryServer")
    assertOutputContains(workerUnit, "org.apache.spark.deploy.worker.Worker")
    assert(!playbook.contains("start-spark.sh"), playbook)
    assert(!playbook.contains("stop-spark.sh"), playbook)
    assert(!playbook.contains("start-slave.sh"), playbook)
    assert(!playbook.contains("stop-slave.sh"), playbook)
  }

  test("Deploy script controls Spark through systemd without script fallback") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "sudo systemctl restart spark-master spark-history-server")
    assertOutputContains(deployScript, "sudo systemctl restart spark-worker")
    assertOutputContains(deployScript, "sudo systemctl stop spark-worker")
    assertOutputContains(deployScript, "sudo systemctl stop spark-history-server spark-master")
    assertOutputContains(deployScript, "stop_spark(outputs, private_key, known_hosts, insecure)")
    assertOutputContains(
      deployScript,
      "run_ansible(inventory_path, private_key, known_hosts, insecure)"
    )
    assert(!deployScript.contains("./start-spark.sh"), deployScript)
    assert(!deployScript.contains("./start-slave.sh"), deployScript)
  }

  test("Terraform supports deploying into an existing VPC and subnet") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "existing_vpc_id")
    assertOutputContains(deployScript, "existing_subnet_id")
    assertOutputContains(deployScript, "use_existing_network")
    assertOutputContains(deployScript, "local.vpc_id")
    assertOutputContains(deployScript, "local.subnet_id")
    assertOutputContains(deployScript, "resource \"aws_security_group\" \"spark_master_ui\"")
    assertOutputContains(
      deployScript,
      "vpc_security_group_ids      = [aws_security_group.spark.id, aws_security_group.spark_master_ui.id]"
    )
    assertOutputContains(
      deployScript,
      "vpc_security_group_ids      = [aws_security_group.spark.id]"
    )
    assertOutputContains(deployScript, "output \"cluster_security_group_id\"")
    assertOutputContains(deployScript, "output \"master_ui_security_group_id\"")
    assertOutputContains(deployScript, "Master UI security group")
    assert(!deployScript.contains("output \"security_group_id\""), deployScript)
  }

  private def runScript(args: String*): CommandResult =
    runPython((script.toString +: args): _*)

  private def runPython(args: String*): CommandResult = {
    val output = new StringBuilder
    val exitCode = Process(python +: args, repoRoot.toFile).!(
      ProcessLogger(output append _ append "\n")
    )
    CommandResult(exitCode, output.toString)
  }

  private def assertOutputContains(haystack: String, needle: String): Unit =
    assert(
      haystack.contains(needle),
      s"Expected output to contain '$needle'. Output:\n$haystack"
    )

  private def findRepoRoot(): Path = {
    val start = Paths.get("").toAbsolutePath
    Iterator
      .iterate(start)(_.getParent)
      .takeWhile(_ != null)
      .find(path => Files.isRegularFile(path.resolve("deploy_spark_cluster.py")))
      .getOrElse(fail(s"Could not find deploy_spark_cluster.py from $start"))
  }
}

object DeploySparkClusterScriptTest {
  private final case class CommandResult(exitCode: Int, output: String)
}
