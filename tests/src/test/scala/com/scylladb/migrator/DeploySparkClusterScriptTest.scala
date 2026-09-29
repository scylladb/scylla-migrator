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

  test("deploy helper reports unsupported Python versions clearly") {
    val deployScript = Files.readString(script)

    assertOutputContains(deployScript, "if sys.version_info < (3, 10):")
    assertOutputContains(
      deployScript,
      "deploy_spark_cluster.py requires Python 3.10 or later."
    )
    assert(
      deployScript.indexOf("if sys.version_info < (3, 10):") <
        deployScript.indexOf("import argparse"),
      deployScript
    )
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

  test("migration type from metadata is validated before use") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |try:
         |    module.resolve_migration_type(None, {"migration_type": "bad-type"})
         |except SystemExit as exc:
         |    print(exc)
         |print(module.resolve_migration_type("cql", {"migration_type": "bad-type"}))
         |print(module.resolve_migration_type(None, {"migration_type": ""}))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    val lines = result.output.linesIterator.toList
    assertEquals(
      lines(0),
      "Unsupported migration type from metadata.json: 'bad-type'. Expected one of: cql, alternator."
    )
    assertEquals(lines(1), "cql")
    assertEquals(lines(2), "cql")
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

  test("Terraform file generation rejects SSH public key directories") {
    val result = runPython(
      "-c",
      s"""import importlib.util, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    temp_path = Path(temp_dir)
         |    public_key_dir = temp_path / "id_rsa.pub"
         |    public_key_dir.mkdir()
         |    args = module.build_parser().parse_args([
         |        "deploy",
         |        "--state-dir",
         |        str(temp_path / "state"),
         |        "--skip-ansible",
         |        "--allowed-ssh-cidr",
         |        "203.0.113.10/32",
         |        "--allowed-web-cidr",
         |        "203.0.113.10/32",
         |        "--ssh-public-key",
         |        str(public_key_dir),
         |    ])
         |    try:
         |        module.write_terraform_files(args, temp_path / "state")
         |    except SystemExit as exc:
         |        print(exc)
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "SSH public key path is not a file:")
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
    assertOutputContains(result.output, "--cloud-provider {aws,gcp}")
    assertOutputContains(result.output, "--gcp-project")
    assertOutputContains(result.output, "--zone")
    assertOutputContains(result.output, "--gcp-service-account-file")
    assertOutputContains(result.output, "--gcp-instance-service-account")
    assertOutputContains(result.output, "--network")
    assertOutputContains(result.output, "--subnetwork")
    assertOutputContains(result.output, "n2-custom-8-262144-ext")
    assertOutputContains(result.output, "c4a-highmem-16")
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

  test("deploy refuses provider switches before rewriting the state directory") {
    val result = runPython(
      "-c",
      s"""import importlib.util, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    temp_path = Path(temp_dir)
         |    public_key = temp_path / "id_rsa.pub"
         |    public_key.write_text("ssh-rsa AAAAB3NzaTest user@example\\n")
         |    for existing_provider, requested_provider in (("aws", "gcp"), ("gcp", "aws")):
         |        state_dir = temp_path / (existing_provider + "-state")
         |        state_dir.mkdir()
         |        original_main = "# existing " + existing_provider + " configuration\\n"
         |        (state_dir / "main.tf").write_text(original_main)
         |        module.write_json(
         |            state_dir / "terraform.tfstate",
         |            {"resources": [{"mode": "managed", "type": "example", "name": "node"}]},
         |        )
         |        module.write_json(
         |            state_dir / "metadata.json",
         |            {"cloud_provider": existing_provider},
         |        )
         |        command = [
         |            "deploy",
         |            "--cloud-provider", requested_provider,
         |            "--state-dir", str(state_dir),
         |            "--skip-ansible",
         |            "--ssh-public-key", str(public_key),
         |            "--allowed-ssh-cidr", "203.0.113.10/32",
         |            "--allowed-web-cidr", "203.0.113.10/32",
         |        ]
         |        if requested_provider == "gcp":
         |            command.extend(["--gcp-project", "example-project"])
         |        args = module.build_parser().parse_args(command)
         |        try:
         |            module.handle_deploy(args)
         |        except SystemExit as exc:
         |            print(exc)
         |        print("main_unchanged=" + str((state_dir / "main.tf").read_text() == original_main))
         |        print("known_hosts_created=" + str((state_dir / "known_hosts").exists()))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "contains an existing AWS deployment")
    assertOutputContains(result.output, "contains an existing GCP deployment")
    assertEquals(result.output.split("main_unchanged=True", -1).length - 1, 2)
    assertEquals(result.output.split("known_hosts_created=False", -1).length - 1, 2)
  }

  test("skip-ansible deploy accepts a public key without resolving a private key") {
    val result = runPython(
      "-c",
      s"""import importlib.util, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |outputs = {
         |    "master": {
         |        "instance_id": "i-master",
         |        "public_ip": "203.0.113.10",
         |        "private_ip": "10.42.1.10",
         |    },
         |    "workers": [],
         |    "spark_master_url": "spark://10.42.1.10:7077",
         |    "spark_master_ui": "http://203.0.113.10:8080",
         |    "spark_application_ui": "http://203.0.113.10:4040",
         |    "spark_history_ui": "http://203.0.113.10:18080",
         |    "vpc_id": "vpc-0123456789abcdef0",
         |    "public_subnet_id": "subnet-0123456789abcdef0",
         |    "cluster_security_group_id": "sg-0123456789abcdef0",
         |    "master_ui_security_group_id": "sg-0123456789abcdef1",
         |    "key_name": "scylla-migrator-spark-key",
         |}
         |def fail_private_key_resolution(*_args, **_kwargs):
         |    raise AssertionError("private key should not be resolved")
         |module.resolve_ssh_private_key = fail_private_key_resolution
         |module.require_commands = lambda commands: print("required=" + ",".join(commands))
         |module.run_command = lambda *args, **kwargs: print("run=" + " ".join(args[0]))
         |module.terraform_output = lambda state_dir, env=None: outputs
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    state_dir = Path(temp_dir) / "state"
         |    public_key = Path(temp_dir) / "id_rsa.pub"
         |    public_key.write_text("ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCtest user@example\\n")
         |    args = module.build_parser().parse_args([
         |        "deploy",
         |        "--state-dir",
         |        str(state_dir),
         |        "--skip-ansible",
         |        "--allowed-ssh-cidr",
         |        "203.0.113.10/32",
         |        "--allowed-web-cidr",
         |        "203.0.113.10/32",
         |        "--ssh-public-key",
         |        str(public_key),
         |    ])
         |    module.handle_deploy(args)
         |    metadata = module.read_json(state_dir / "metadata.json")
         |    print("metadata_private_key=" + repr(metadata["ssh_private_key"]))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "required=terraform")
    assertOutputContains(result.output, "Skipping Ansible configuration.")
    assertOutputContains(result.output, "metadata_private_key=''")
    assert(!result.output.contains("private key should not be resolved"), result.output)
    assert(!result.output.contains("ansible-playbook"), result.output)
  }

  test("GCP deploy passes explicit credentials without invoking cloud APIs in tests") {
    val result = runPython(
      "-c",
      s"""import importlib.util, json, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |outputs = {
         |    "master": {
         |        "instance_id": "gcp-master",
         |        "public_ip": "203.0.113.10",
         |        "private_ip": "10.42.1.10",
         |    },
         |    "workers": [],
         |    "spark_master_url": "spark://10.42.1.10:7077",
         |    "spark_master_ui": "http://203.0.113.10:8080",
         |    "spark_application_ui": "http://203.0.113.10:4040",
         |    "spark_history_ui": "http://203.0.113.10:18080",
         |    "vpc_id": "projects/example/global/networks/test",
         |    "public_subnet_id": "projects/example/regions/us-central1/subnetworks/test",
         |    "cluster_security_group_id": "internal-firewall",
         |    "master_ui_security_group_id": "ui-firewall",
         |    "key_name": "ubuntu (instance metadata)",
         |}
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    temp_path = Path(temp_dir)
         |    state_dir = temp_path / "state"
         |    public_key = temp_path / "id_rsa.pub"
         |    public_key.write_text("ssh-rsa AAAAB3NzaTest user@example\\n")
         |    credentials = temp_path / "service-account.json"
         |    credentials.write_text(json.dumps({
         |        "type": "service_account",
         |        "project_id": "example-project",
         |        "client_email": "deployer@example-project.iam.gserviceaccount.com",
         |        "private_key": "not-a-real-private-key",
         |    }))
         |    expected_credentials = str(credentials.resolve())
         |    module.require_commands = lambda commands: print("required=" + ",".join(commands))
         |    def fake_run(command, **kwargs):
         |        env = kwargs.get("env") or {}
         |        print("run=" + " ".join(command))
         |        print("run_credentials=" + str(env.get("GOOGLE_APPLICATION_CREDENTIALS") == expected_credentials))
         |        if command[1] == "apply":
         |            preliminary = module.read_json(state_dir / "metadata.json")
         |            print("preapply_provider=" + preliminary["cloud_provider"])
         |            print("preapply_credentials=" + str(preliminary["gcp_service_account_file"] == expected_credentials))
         |    module.run_command = fake_run
         |    def fake_output(state_dir, *, env=None):
         |        print("output_credentials=" + str(env["GOOGLE_APPLICATION_CREDENTIALS"] == expected_credentials))
         |        return outputs
         |    module.terraform_output = fake_output
         |    args = module.build_parser().parse_args([
         |        "deploy",
         |        "--cloud-provider", "gcp",
         |        "--gcp-project", "example-project",
         |        "--gcp-service-account-file", str(credentials),
         |        "--state-dir", str(state_dir),
         |        "--skip-ansible",
         |        "--ssh-public-key", str(public_key),
         |        "--allowed-ssh-cidr", "203.0.113.10/32",
         |        "--allowed-web-cidr", "203.0.113.10/32",
         |    ])
         |    module.handle_deploy(args)
         |    metadata = module.read_json(state_dir / "metadata.json")
         |    print("provider=" + metadata["cloud_provider"])
         |    print("credentials_saved=" + str(metadata["gcp_service_account_file"] == expected_credentials))
         |    print("master_type=" + metadata["master_instance_type"])
         |    print("worker_type=" + metadata["worker_instance_type"])
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "required=terraform")
    assertEquals(result.output.split("run_credentials=True", -1).length - 1, 2)
    assertOutputContains(result.output, "output_credentials=True")
    assertOutputContains(result.output, "preapply_provider=gcp")
    assertOutputContains(result.output, "preapply_credentials=True")
    assertOutputContains(result.output, "provider=gcp")
    assertOutputContains(result.output, "credentials_saved=True")
    assertOutputContains(result.output, "master_type=n2-custom-8-262144-ext")
    assertOutputContains(result.output, "worker_type=c4a-highmem-16")
    assert(!result.output.contains("ansible-playbook"), result.output)
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

  test("cloud-specific defaults preserve AWS and size GCP comparably") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |parser = module.build_parser()
         |common = [
         |    "deploy",
         |    "--skip-ansible",
         |    "--allowed-ssh-cidr", "203.0.113.10/32",
         |    "--allowed-web-cidr", "203.0.113.10/32",
         |]
         |aws = parser.parse_args(common)
         |module.apply_cloud_defaults(aws)
         |print(aws.region, aws.master_instance_type, aws.worker_instance_type)
         |gcp = parser.parse_args(common + [
         |    "--cloud-provider", "gcp",
         |    "--gcp-project", "example-project",
         |])
         |module.apply_cloud_defaults(gcp)
         |print(gcp.region, gcp.zone, gcp.master_instance_type, gcp.worker_instance_type)
         |print(module.infer_gcp_architecture(gcp.master_instance_type))
         |print(module.infer_gcp_architecture(gcp.worker_instance_type))
         |print(module.gcp_boot_disk_type(gcp.master_instance_type))
         |print(module.gcp_boot_disk_type(gcp.worker_instance_type))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertEquals(
      result.output.linesIterator.toList,
      List(
        "us-east-1 x2iedn.2xlarge i8g.4xlarge",
        "us-central1 us-central1-a n2-custom-8-262144-ext c4a-highmem-16",
        "x86_64",
        "arm64",
        "pd-balanced",
        "hyperdisk-balanced"
      )
    )
  }

  test("GCP Terraform generation uses Compute Engine and normalized outputs") {
    val result = runPython(
      "-c",
      s"""import importlib.util, json, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    temp_path = Path(temp_dir)
         |    public_key = temp_path / "id_rsa.pub"
         |    public_key.write_text("ssh-rsa AAAAB3NzaTest user@example\\n")
         |    args = module.build_parser().parse_args([
         |        "deploy",
         |        "--cloud-provider", "gcp",
         |        "--gcp-project", "example-project",
         |        "--skip-ansible",
         |        "--ssh-public-key", str(public_key),
         |        "--network", "existing-network",
         |        "--subnetwork", "existing-subnetwork",
         |        "--gcp-instance-service-account",
         |        "spark@example-project.iam.gserviceaccount.com",
         |        "--allowed-ssh-cidr", "203.0.113.10/32",
         |        "--allowed-web-cidr", "203.0.113.10/32",
         |    ])
         |    module.validate_cloud_args(args)
         |    state_dir = temp_path / "state"
         |    module.write_terraform_files(args, state_dir)
         |    main = (state_dir / "main.tf").read_text()
         |    tfvars = json.loads((state_dir / "terraform.tfvars.json").read_text())
         |    print(tfvars["project_id"], tfvars["region"], tfvars["zone"])
         |    print(tfvars["master_instance_type"], tfvars["worker_instance_type"])
         |    print(tfvars["master_image_family"], tfvars["worker_image_family"])
         |    print(tfvars["master_boot_disk_type"], tfvars["worker_boot_disk_type"])
         |    print(tfvars["existing_network"], tfvars["existing_subnetwork"])
         |    print(tfvars["instance_service_account"])
         |    print("credentials" in tfvars)
         |    for expected in (
         |        'provider "google"',
         |        'resource "google_compute_network" "spark"',
         |        'resource "google_compute_firewall" "spark_ssh"',
         |        'resource "google_compute_instance" "spark_master"',
         |        'output "spark_master_url"',
         |        'output "ssh_firewall_id"',
         |        'block-project-ssh-keys = "true"',
         |        'source_tags = [local.cluster_tag]',
         |        'condition = local.use_existing_network ? (',
         |    ):
         |        print(expected in main)
         |    print('source_ranges = [local.subnetwork_cidr]' not in main)
         |    print('!local.use_existing_network ||' not in main)
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertEquals(
      result.output.linesIterator.toList,
      List(
        "example-project us-central1 us-central1-a",
        "n2-custom-8-262144-ext c4a-highmem-16",
        "ubuntu-2404-lts-amd64 ubuntu-2404-lts-arm64",
        "pd-balanced hyperdisk-balanced",
        "existing-network existing-subnetwork",
        "spark@example-project.iam.gserviceaccount.com",
        "False",
        "True",
        "True",
        "True",
        "True",
        "True",
        "True",
        "True",
        "True",
        "True",
        "True",
        "True"
      )
    )
  }

  test("GCP service account files are validated and exported to Terraform") {
    val result = runPython(
      "-c",
      s"""import importlib.util, json, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    temp_path = Path(temp_dir)
         |    valid = temp_path / "service-account.json"
         |    valid.write_text(json.dumps({
         |        "type": "service_account",
         |        "project_id": "example-project",
         |        "client_email": "deployer@example-project.iam.gserviceaccount.com",
         |        "private_key": "not-a-real-private-key",
         |    }))
         |    env = module.terraform_auth_env("gcp", str(valid))
         |    print(env["GOOGLE_APPLICATION_CREDENTIALS"] == str(valid.resolve()))
         |    print(module.terraform_auth_env("gcp", None) is None)
         |    saved_env = module.terraform_auth_env(
         |        "gcp", None, {"gcp_service_account_file": str(valid)}
         |    )
         |    print(saved_env["GOOGLE_APPLICATION_CREDENTIALS"] == str(valid.resolve()))
         |    invalid_json = temp_path / "invalid.json"
         |    invalid_json.write_text("{bad json")
         |    wrong_type = temp_path / "wrong-type.json"
         |    wrong_type.write_text(json.dumps({"type": "external_account"}))
         |    missing_fields = temp_path / "missing-fields.json"
         |    missing_fields.write_text(json.dumps({"type": "service_account"}))
         |    missing_file = temp_path / "missing.json"
         |    credential_directory = temp_path / "credential-directory"
         |    credential_directory.mkdir()
         |    for path in (
         |        missing_file,
         |        credential_directory,
         |        invalid_json,
         |        wrong_type,
         |        missing_fields,
         |    ):
         |        try:
         |            module.terraform_auth_env("gcp", str(path))
         |        except SystemExit as exc:
         |            print(exc)
         |    try:
         |        module.terraform_auth_env("aws", str(valid))
         |    except SystemExit as exc:
         |        print(exc)
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    val lines = result.output.linesIterator.toList
    assertEquals(lines.take(3), List("True", "True", "True"))
    assertOutputContains(lines(3), "GCP service account file does not exist")
    assertOutputContains(lines(4), "GCP service account path is not a file")
    assertOutputContains(lines(5), "Invalid JSON in GCP service account file")
    assertOutputContains(lines(6), "is not a service account key")
    assertOutputContains(
      lines(7),
      "missing required field(s): project_id, client_email, private_key"
    )
    assertEquals(lines(8), "--gcp-service-account-file can only be used with GCP.")
  }

  test("GCP argument validation reports provider-specific mistakes") {
    val base = Seq(
      "deploy",
      "--cloud-provider",
      "gcp",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "203.0.113.10/32",
      "--allowed-web-cidr",
      "203.0.113.10/32"
    )

    val missingProject = runScript(base: _*)
    assertNotEquals(missingProject.exitCode, 0, missingProject.output)
    assertOutputContains(
      missingProject.output,
      "--gcp-project is required when --cloud-provider=gcp"
    )

    val mismatchedZone = runScript(
      (base ++ Seq(
        "--gcp-project",
        "example-project",
        "--region",
        "us-central1",
        "--zone",
        "us-east1-b"
      )): _*
    )
    assertNotEquals(mismatchedZone.exitCode, 0, mismatchedZone.output)
    assertOutputContains(mismatchedZone.output, "is not in the selected region")

    val unpairedNetwork = runScript(
      (base ++ Seq(
        "--gcp-project",
        "example-project",
        "--network",
        "existing-network"
      )): _*
    )
    assertNotEquals(unpairedNetwork.exitCode, 0, unpairedNetwork.output)
    assertOutputContains(
      unpairedNetwork.output,
      "--network and --subnetwork must be provided together"
    )

    val gcpVpcCidr = runScript(
      (base ++ Seq(
        "--gcp-project",
        "example-project",
        "--vpc-cidr",
        "10.42.0.0/16"
      )): _*
    )
    assertNotEquals(gcpVpcCidr.exitCode, 0, gcpVpcCidr.output)
    assertOutputContains(
      gcpVpcCidr.output,
      "--vpc-cidr is AWS-only because GCP VPC networks do not have a network-wide CIDR"
    )

    val awsWithGcpProject = runScript(
      "deploy",
      "--skip-ansible",
      "--gcp-project",
      "example-project",
      "--allowed-ssh-cidr",
      "203.0.113.10/32",
      "--allowed-web-cidr",
      "203.0.113.10/32"
    )
    assertNotEquals(awsWithGcpProject.exitCode, 0, awsWithGcpProject.output)
    assertOutputContains(
      awsWithGcpProject.output,
      "--gcp-project can only be used with GCP"
    )
  }

  test("GCP default zones exist in regions without an -a zone") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |parser = module.build_parser()
         |common = [
         |    "deploy",
         |    "--cloud-provider", "gcp",
         |    "--gcp-project", "example-project",
         |    "--skip-ansible",
         |    "--allowed-ssh-cidr", "203.0.113.10/32",
         |    "--allowed-web-cidr", "203.0.113.10/32",
         |]
         |for extra in (
         |    ["--region", "us-east1"],
         |    ["--region", "europe-west1"],
         |    ["--region", "europe-west4"],
         |    [],
         |    ["--zone", "us-east1-c"],
         |):
         |    args = parser.parse_args(common + extra)
         |    module.validate_cloud_args(args)
         |    print(args.region, args.zone)
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertEquals(
      result.output.linesIterator.toList,
      List(
        "us-east1 us-east1-b",
        "europe-west1 europe-west1-b",
        "europe-west4 europe-west4-a",
        "us-central1 us-central1-a",
        "us-east1 us-east1-c"
      )
    )
  }

  test("explicit GCP service account file overrides competing credential variables") {
    val result = runPython(
      "-c",
      s"""import importlib.util, json, os, subprocess, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |competing = (
         |    "GOOGLE_OAUTH_ACCESS_TOKEN",
         |    "GOOGLE_CLOUD_KEYFILE_JSON",
         |    "GCLOUD_KEYFILE_JSON",
         |    "GOOGLE_IMPERSONATE_SERVICE_ACCOUNT",
         |)
         |os.environ.update({name: "other-" + name.lower() for name in competing})
         |os.environ["GOOGLE_CREDENTIALS"] = "/tmp/other-credentials.json"
         |os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/other-adc.json"
         |os.environ["UNRELATED_SETTING"] = "kept"
         |captured = {}
         |def fake_subprocess_run(args, **kwargs):
         |    captured.clear()
         |    captured.update(kwargs["env"])
         |    return subprocess.CompletedProcess(args, 0, "", "")
         |module.subprocess.run = fake_subprocess_run
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    credentials = Path(temp_dir) / "service-account.json"
         |    credentials.write_text(json.dumps({
         |        "type": "service_account",
         |        "project_id": "example-project",
         |        "client_email": "deployer@example-project.iam.gserviceaccount.com",
         |        "private_key": "not-a-real-private-key",
         |    }))
         |    expected = str(credentials.resolve())
         |    module.run_command(
         |        ["terraform", "plan"],
         |        env=module.terraform_auth_env("gcp", str(credentials)),
         |    )
         |    print("google_credentials=" + str(captured["GOOGLE_CREDENTIALS"] == expected))
         |    print("adc=" + str(captured["GOOGLE_APPLICATION_CREDENTIALS"] == expected))
         |    print("competing_removed=" + str(not any(name in captured for name in competing)))
         |    print("unrelated=" + captured["UNRELATED_SETTING"])
         |    module.run_command(["terraform", "plan"], env=module.terraform_auth_env("gcp", None))
         |    print("ambient_kept=" + str(all(name in captured for name in competing)))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "google_credentials=True")
    assertOutputContains(result.output, "adc=True")
    assertOutputContains(result.output, "competing_removed=True")
    assertOutputContains(result.output, "unrelated=kept")
    assertOutputContains(result.output, "ambient_kept=True")
  }

  test("destroyed state directories can be reused and reset known hosts") {
    val result = runPython(
      "-c",
      s"""import importlib.util, json, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |outputs = {
         |    "master": {
         |        "instance_id": "gcp-master",
         |        "public_ip": "203.0.113.10",
         |        "private_ip": "10.42.1.10",
         |    },
         |    "workers": [],
         |    "spark_master_url": "spark://10.42.1.10:7077",
         |    "spark_master_ui": "http://203.0.113.10:8080",
         |    "spark_application_ui": "http://203.0.113.10:4040",
         |    "spark_history_ui": "http://203.0.113.10:18080",
         |    "vpc_id": "projects/example/global/networks/test",
         |    "public_subnet_id": "projects/example/regions/us-central1/subnetworks/test",
         |    "cluster_security_group_id": "internal-firewall",
         |    "master_ui_security_group_id": "ui-firewall",
         |    "key_name": "ubuntu (instance metadata)",
         |}
         |managed_state = {"resources": [{"mode": "managed", "type": "google_compute_instance", "name": "spark_master"}]}
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    temp_path = Path(temp_dir)
         |    state_dir = temp_path / "state"
         |    state_dir.mkdir()
         |    tfstate = state_dir / "terraform.tfstate"
         |    for label, content in (
         |        ("missing", None),
         |        ("empty_object", {}),
         |        ("no_resources", {"resources": []}),
         |        ("data_only", {"resources": [{"mode": "data", "type": "google_compute_image", "name": "ubuntu"}]}),
         |        ("managed", managed_state),
         |    ):
         |        if content is None:
         |            tfstate.unlink(missing_ok=True)
         |        else:
         |            tfstate.write_text(json.dumps(content))
         |        print(label + "=" + str(module.terraform_state_has_resources(state_dir)))
         |    tfstate.write_text("{not json")
         |    print("invalid=" + str(module.terraform_state_has_resources(state_dir)))
         |
         |    module.require_commands = lambda commands: None
         |    def fake_run(command, **kwargs):
         |        if command[1] == "apply":
         |            preliminary = module.read_json(state_dir / "metadata.json")
         |            print("preapply_outputs=" + json.dumps(preliminary["terraform_outputs"]))
         |    module.run_command = fake_run
         |    module.terraform_output = lambda state_dir, env=None: outputs
         |    public_key = temp_path / "id_rsa.pub"
         |    public_key.write_text("ssh-rsa AAAAB3NzaTest user@example\\n")
         |    args = module.build_parser().parse_args([
         |        "deploy",
         |        "--cloud-provider", "gcp",
         |        "--gcp-project", "example-project",
         |        "--state-dir", str(state_dir),
         |        "--skip-ansible",
         |        "--ssh-public-key", str(public_key),
         |        "--allowed-ssh-cidr", "203.0.113.10/32",
         |        "--allowed-web-cidr", "203.0.113.10/32",
         |    ])
         |
         |    # An AWS cluster destroyed without --delete-state-dir leaves empty state behind.
         |    module.write_json(tfstate, {"resources": []})
         |    module.write_json(
         |        state_dir / "metadata.json",
         |        {"cloud_provider": "aws", "terraform_outputs": {"stale": True}},
         |    )
         |    (state_dir / "known_hosts").write_text("203.0.113.10 ssh-ed25519 AAAAstale\\n")
         |    module.handle_deploy(args)
         |    print("provider=" + module.read_json(state_dir / "metadata.json")["cloud_provider"])
         |    print("reset_known_hosts=" + str((state_dir / "known_hosts").read_text() == ""))
         |
         |    # A live deployment keeps its host keys on re-deploy.
         |    module.write_json(tfstate, managed_state)
         |    (state_dir / "known_hosts").write_text("203.0.113.10 ssh-ed25519 AAAAlive\\n")
         |    module.handle_deploy(args)
         |    print("kept_known_hosts=" + str("AAAAlive" in (state_dir / "known_hosts").read_text()))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    val lines = result.output.linesIterator.toList
    assertEquals(
      lines.take(6),
      List(
        "missing=False",
        "empty_object=False",
        "no_resources=False",
        "data_only=False",
        "managed=True",
        "invalid=True"
      )
    )
    assertOutputContains(result.output, "preapply_outputs={}")
    assertOutputContains(result.output, "provider=gcp")
    assertOutputContains(result.output, "reset_known_hosts=True")
    assertOutputContains(result.output, "kept_known_hosts=True")
    assert(!result.output.contains("refusing to replace"), result.output)
  }

  test("Terraform receives validated SSH public key contents instead of a path") {
    val result = runPython(
      "-c",
      s"""import importlib.util, json, tempfile
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |def parse(provider, key_path):
         |    command = [
         |        "deploy",
         |        "--cloud-provider", provider,
         |        "--skip-ansible",
         |        "--ssh-public-key", str(key_path),
         |        "--allowed-ssh-cidr", "203.0.113.10/32",
         |        "--allowed-web-cidr", "203.0.113.10/32",
         |    ]
         |    if provider == "gcp":
         |        command.extend(["--gcp-project", "example-project"])
         |    return module.build_parser().parse_args(command)
         |with tempfile.TemporaryDirectory() as temp_dir:
         |    temp_path = Path(temp_dir)
         |    public_key = temp_path / "id_ed25519.pub"
         |    public_key.write_text("\\nssh-ed25519 AAAAC3NzaTest user@example\\n\\n")
         |    for provider in ("aws", "gcp"):
         |        state_dir = temp_path / (provider + "-state")
         |        module.write_terraform_files(parse(provider, public_key), state_dir)
         |        tfvars = json.loads((state_dir / "terraform.tfvars.json").read_text())
         |        main = (state_dir / "main.tf").read_text()
         |        print(provider + "_key=" + tfvars["ssh_public_key"])
         |        print(provider + "_uses_path=" + str("ssh_public_key_path" in tfvars or "file(" in main))
         |    for label, content in (
         |        ("private", "-----BEGIN OPENSSH PRIVATE KEY-----\\nnot-a-real-key\\n-----END OPENSSH PRIVATE KEY-----\\n"),
         |        ("multiple", "ssh-rsa AAAAB3NzaOne one@example\\nssh-rsa AAAAB3NzaTwo two@example\\n"),
         |        ("empty", "\\n"),
         |        ("no_key_data", "ssh-ed25519\\n"),
         |    ):
         |        key_path = temp_path / (label + ".pub")
         |        key_path.write_text(content)
         |        state_dir = temp_path / (label + "-state")
         |        try:
         |            module.write_terraform_files(parse("gcp", key_path), state_dir)
         |        except SystemExit as exc:
         |            print(label + "_rejected=" + str("exactly one OpenSSH public key" in str(exc)))
         |        print(label + "_state_created=" + str(state_dir.exists()))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    val lines = result.output.linesIterator.toList
    assertEquals(
      lines,
      List(
        "aws_key=ssh-ed25519 AAAAC3NzaTest user@example",
        "aws_uses_path=False",
        "gcp_key=ssh-ed25519 AAAAC3NzaTest user@example",
        "gcp_uses_path=False",
        "private_rejected=True",
        "private_state_created=False",
        "multiple_rejected=True",
        "multiple_state_created=False",
        "empty_rejected=True",
        "empty_state_created=False",
        "no_key_data_rejected=True",
        "no_key_data_state_created=False"
      )
    )
  }

  test("Terraform does not replace existing nodes when newer Ubuntu images are published") {
    val result = runPython(
      "-c",
      s"""import importlib.util
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |print(module.AWS_TERRAFORM_MAIN.count("ignore_changes = [ami]"))
         |print(module.GCP_TERRAFORM_MAIN.count(
         |    "ignore_changes = [boot_disk[0].initialize_params[0].image]"
         |))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertEquals(result.output.linesIterator.toList, List("2", "2"))
  }

  test("generated subnet CIDRs are normalized and sized for GCP") {
    val tooSmall = runScript(
      "deploy",
      "--cloud-provider",
      "gcp",
      "--gcp-project",
      "example-project",
      "--skip-ansible",
      "--public-subnet-cidr",
      "10.42.1.0/30",
      "--allowed-ssh-cidr",
      "203.0.113.10/32",
      "--allowed-web-cidr",
      "203.0.113.10/32"
    )
    assertNotEquals(tooSmall.exitCode, 0, tooSmall.output)
    assertOutputContains(
      tooSmall.output,
      "--public-subnet-cidr (10.42.1.0/30) is too small for a GCP subnetwork; use a /29 or larger range."
    )

    val normalized = runPython(
      "-c",
      s"""import importlib.util
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |args = module.build_parser().parse_args([
         |    "deploy",
         |    "--skip-ansible",
         |    "--vpc-cidr", "10.42.7.1/16",
         |    "--public-subnet-cidr", "10.42.1.5/24",
         |    "--allowed-ssh-cidr", "203.0.113.10/32",
         |    "--allowed-web-cidr", "203.0.113.10/32",
         |])
         |print(args.vpc_cidr, args.public_subnet_cidr)
         |""".stripMargin
    )
    assertEquals(normalized.exitCode, 0, normalized.output)
    assertEquals(normalized.output.trim, "10.42.0.0/16 10.42.1.0/24")
  }

  test("SSH wait timeout reports the last SSH error") {
    val result = runPython(
      "-c",
      s"""import importlib.util, subprocess
         |from pathlib import Path
         |spec = importlib.util.spec_from_file_location("deploy_spark_cluster", "${script}")
         |module = importlib.util.module_from_spec(spec)
         |spec.loader.exec_module(module)
         |attempts = []
         |def fake_subprocess_run(args, **kwargs):
         |    attempts.append(args)
         |    return subprocess.CompletedProcess(args, 255, "", "Permission denied (publickey).\\n")
         |module.subprocess.run = fake_subprocess_run
         |module.time.sleep = lambda _seconds: None
         |try:
         |    module.wait_for_ssh(["203.0.113.10"], Path("/tmp/key"), Path("/tmp/known_hosts"), False)
         |except SystemExit as exc:
         |    print(exc)
         |print("attempts=" + str(len(attempts)))
         |""".stripMargin
    )

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(
      result.output,
      "Timed out waiting for SSH on 203.0.113.10. Last SSH error:\nPermission denied (publickey)."
    )
    assertOutputContains(result.output, "attempts=30")
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
    assertOutputContains(result.output, "SSH public key does not exist")
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
    assertOutputContains(deployScript, "if not terraform_state_has_resources(state_dir):")
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
    assertOutputContains(deployScript, "outputs = terraform_output(state_dir, env=terraform_env)")
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

  test("deploy documentation covers GCP sizing and authentication choices") {
    val readme = Files.readString(repoRoot.resolve("README.md"))

    assertOutputContains(readme, "Application Default Credentials (ADC)")
    assertOutputContains(readme, "gcloud auth application-default login")
    assertOutputContains(readme, "`GOOGLE_APPLICATION_CREDENTIALS`")
    assertOutputContains(readme, "`--gcp-service-account-file`")
    assertOutputContains(readme, "`--gcp-instance-service-account`")
    assertOutputContains(readme, "`n2-custom-8-262144-ext`")
    assertOutputContains(readme, "`c4a-highmem-16`")
    assertOutputContains(readme, "8 x86_64 vCPUs and 256 GiB")
    assertOutputContains(readme, "16 Google Axion Arm vCPUs and 128 GiB")
    assertOutputContains(readme, "key contents are never copied")
    assertOutputContains(readme, "It takes precedence over any Google credential environment variables")
    assertOutputContains(readme, "where it defaults to `<region>-b`")
    assertOutputContains(readme, "only newly created instances use the latest image")
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
