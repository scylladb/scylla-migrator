# ScyllaDB Hydra

<img src="monster-hydra.png" width="120" height="120" alt="ScyllaDB Migrator Logo">

The ScyllaDB Hydra (Previously ScyllaDB Migrator) is a Spark application that migrates data to ScyllaDB from CQL-compatible or DynamoDB-compatible databases.

## Table of Contents

- [Documentation](#documentation)
- [Building](#building)
- [Deploying a Spark Cluster](#deploying-a-spark-cluster)
  - [Prerequisites](#prerequisites)
  - [Cloud Authentication](#cloud-authentication)
  - [Runbook](#runbook)
  - [Default Instance Sizing](#default-instance-sizing)
  - [Derived Spark Settings](#derived-spark-settings)
  - [Systemd Units](#systemd-units)
  - [Security Notes](#security-notes)
  - [Command Reference](#command-reference)
- [Contributing](#contributing)

## Documentation

See https://migrator.docs.scylladb.com.

## Building

To test a custom version of the migrator that has not been [released](https://github.com/scylladb/scylla-migrator/releases), you can build it yourself by cloning this Git repository and following the steps below:

Build locally:
1. Make sure the Java 8+ JDK and `sbt` are installed on your machine.
2. Export the `JAVA_HOME` environment variable with the path to the JDK installation.
3. Run `make build`

Build locally in Docker (no JDK/sbt required):
1. Run `make docker-build-jar`

Both options will produce the .jar file to use in `spark-submit` command at path `migrator/target/scala-2.13/scylla-migrator-assembly.jar`.

## Deploying a Spark Cluster

The `deploy_spark_cluster.py` helper can create an AWS- or GCP-backed Spark cluster, configure it with the existing Ansible playbook, run a Migrator job, show cluster details, and tear the cluster down.

The script uses Terraform for cloud infrastructure and Ansible for Spark/Migrator setup. It does not use Docker. Generated Terraform files, Terraform state, Ansible inventory, deployment metadata, and SSH `known_hosts` data are stored in `.deploy_spark_cluster/` by default. Use a different `--state-dir` for each cluster; commands such as `show`, `run`, `redeploy`, and `destroy` operate on the cluster recorded in that directory. Also use a different `--name-prefix` for clusters that share an AWS region or GCP project, because names such as the AWS key pair and GCP networks and firewall rules must be unique there. A deploy using a different cloud provider is rejected while the state directory's Terraform state still tracks resources, preventing Terraform from replacing the existing provider's configuration. After `destroy`, the same state directory can be reused for either provider, and its `known_hosts` file is reset because new instances may reuse public IP addresses.

Re-running `deploy` against an existing state directory applies Terraform changes in place, for example to change `--workers`. Existing instances are not replaced when a newer Ubuntu image is published; only newly created instances use the latest image.

### Prerequisites

Install and configure the following on the machine where you run the script:

- Python 3.10 or later.
- Python dependencies installed with `pip install -r requirements.txt` (`ansible-core` is pinned there for the deploy helper).
- Terraform.
- `ssh` and `scp`.
- An SSH private key and matching public key. By default the script uses `~/.ssh/id_rsa` and `~/.ssh/id_rsa.pub`.

For AWS, the credentials used by Terraform need permission to manage EC2 instances, VPC networking, security groups, key pairs, and related tags in the target region.

For GCP:

- Enable the Compute Engine API in the target project.
- Grant the provisioning identity permission to manage Compute Engine instances, networks, subnetworks, firewall rules, disks, and labels. If `--gcp-instance-service-account` is used, the provisioning identity also needs permission to attach that service account.
- Pass the target project with `--gcp-project`.
- Ensure the chosen zone offers the selected machine types and that the project has enough CPU and memory quota.

AWS creates an EC2 key pair from the local public key. GCP puts the public key in instance metadata, disables OS Login on these instances, and blocks project-wide SSH keys so the helper can consistently connect as `ubuntu`. An organization policy that requires OS Login is not compatible with this direct-key SSH workflow. Both providers use Ubuntu 24.04 and the SSH user `ubuntu`.

### Cloud Authentication

AWS authentication follows the standard Terraform AWS provider credential chain, including environment variables, shared AWS credentials/config files, IAM Identity Center, web identity, and an attached instance role when Terraform runs on AWS. The deploy helper does not store AWS credentials.

Without `--gcp-service-account-file`, GCP authentication follows the Terraform Google provider's normal lookup: provider-specific environment variables such as `GOOGLE_CREDENTIALS` or `GOOGLE_OAUTH_ACCESS_TOKEN` when they are set, otherwise Application Default Credentials (ADC). Common ADC choices include:

- Developer credentials created by `gcloud auth application-default login`. Running only `gcloud auth login` is not sufficient to create local ADC.
- The credential configuration referenced by an existing `GOOGLE_APPLICATION_CREDENTIALS` environment variable. This can include supported service account, workload identity federation, or workforce identity federation configurations.
- The service account attached to the machine where Terraform is running.

To explicitly use a service account key file:

```bash
./deploy_spark_cluster.py deploy \
  --cloud-provider gcp \
  --gcp-project my-project \
  --gcp-service-account-file ~/.config/gcloud/migrator-deployer.json \
  --allowed-ssh-cidr "$MY_CIDR" \
  --allowed-web-cidr "$MY_CIDR"
```

The explicit file must be a valid service account JSON key containing `project_id`, `client_email`, and a PEM-encoded RSA `private_key`; the script checks the key structure locally before running Terraform. It takes precedence over any Google credential environment variables: the script passes it to Terraform as `GOOGLE_APPLICATION_CREDENTIALS` and removes `GOOGLE_CREDENTIALS`, `GOOGLE_OAUTH_ACCESS_TOKEN`, `GOOGLE_CLOUD_KEYFILE_JSON`, `GCLOUD_KEYFILE_JSON`, and `GOOGLE_IMPERSONATE_SERVICE_ACCOUNT` from Terraform's environment. The key contents are never copied into the state directory or onto cluster VMs. The provider and resolved credential path are saved in restricted deployment metadata before the first Terraform apply, so a partially created deployment can still be destroyed with the selected credentials. Successful applies refresh that metadata with Terraform outputs. When `deploy` is re-run against a live deployment, it reuses the saved credential path and keeps the last successful metadata until the new apply succeeds. Pass `--gcp-service-account-file` again to override the saved path if the file moves, or pass an empty value to use ADC instead. Treat both the key and the state directory as sensitive.

Provisioning credentials and VM runtime credentials are separate. `--gcp-service-account-file` authenticates local Terraform only. To give the cluster VMs access to Google Cloud APIs, use `--gcp-instance-service-account SERVICE_ACCOUNT_EMAIL`; the script attaches it with the `cloud-platform` OAuth scope, while its effective access remains limited by IAM roles granted outside this script.

### Runbook

1. Choose the provider, location, worker count, and network CIDRs allowed to reach the cluster. Prefer your current public IP as a `/32` CIDR:

   ```bash
   export MY_IP="$(curl -s https://checkip.amazonaws.com)"
   export MY_CIDR="${MY_IP}/32"
   ```

2. Prepare a Migrator config file. Use `config.yaml` for CQL migrations or an Alternator/DynamoDB config such as `config.dynamodb.yml` for Alternator migrations.

3. Deploy an AWS cluster for a CQL migration:

   ```bash
   ./deploy_spark_cluster.py deploy \
     --cloud-provider aws \
     --region us-east-1 \
     --workers 3 \
     --migration-type cql \
     --config-file config.yaml \
     --ssh-private-key ~/.ssh/id_rsa \
     --allowed-ssh-cidr "$MY_CIDR" \
     --allowed-web-cidr "$MY_CIDR"
   ```

   Deploy a GCP cluster for a CQL migration:

   ```bash
   ./deploy_spark_cluster.py deploy \
     --cloud-provider gcp \
     --gcp-project my-project \
     --region us-central1 \
     --zone us-central1-a \
     --workers 3 \
     --migration-type cql \
     --config-file config.yaml \
     --ssh-private-key ~/.ssh/id_rsa \
     --allowed-ssh-cidr "$MY_CIDR" \
     --allowed-web-cidr "$MY_CIDR"
   ```

   For an Alternator migration on either provider, use `--migration-type alternator --config-file config.dynamodb.yml`.

   To deploy into an existing AWS VPC, provide both the VPC ID and a subnet ID:

   ```bash
   ./deploy_spark_cluster.py deploy \
     --region us-east-1 \
     --vpc-id vpc-0123456789abcdef0 \
     --subnet-id subnet-0123456789abcdef0 \
     --workers 3 \
     --migration-type alternator \
     --config-file config.dynamodb.yml \
     --ssh-private-key ~/.ssh/id_rsa \
     --allowed-ssh-cidr "$MY_CIDR" \
     --allowed-web-cidr "$MY_CIDR"
   ```

   To deploy into an existing GCP VPC network, provide both the network and subnetwork names:

   ```bash
   ./deploy_spark_cluster.py deploy \
     --cloud-provider gcp \
     --gcp-project my-project \
     --region us-central1 \
     --zone us-central1-a \
     --network existing-network \
     --subnetwork existing-subnetwork \
     --workers 3 \
     --migration-type cql \
     --config-file config.yaml \
     --ssh-private-key ~/.ssh/id_rsa \
     --allowed-ssh-cidr "$MY_CIDR" \
     --allowed-web-cidr "$MY_CIDR"
   ```

   An existing subnet must have outbound internet access so Ansible can download packages, Spark, AWS CLI, and the Migrator assembly. The script still creates provider-specific SSH, cluster-internal, and master UI access rules in the supplied network. On GCP, the all-protocol internal rule accepts traffic only from instances carrying the cluster tag, rather than from every VM in the subnetwork.

4. Inspect the created infrastructure and Spark endpoints:

   ```bash
   ./deploy_spark_cluster.py show
   ```

   The output includes provider and location metadata, network and firewall/security group details, instance IDs, the Spark master URL, Spark UI, application UI, and history server UI.

5. Rerun the Ansible configuration when you need to apply local playbook or script changes to the current nodes:

   ```bash
   ./deploy_spark_cluster.py redeploy
   ```

   This refreshes Terraform outputs from the state directory, rewrites `.deploy_spark_cluster/inventory.ini`, and then reruns Ansible. It does not apply Terraform changes. If you deployed with `--config-file`, `redeploy` uploads that config again after Ansible finishes so the master keeps the intended migration settings. By default, `redeploy` stops the Spark systemd services before Ansible runs, then restarts them after Ansible finishes so unit, environment, and binary changes take effect cleanly.

   To upload or switch to a config explicitly during redeploy, pass it again:

   ```bash
   ./deploy_spark_cluster.py redeploy \
     --migration-type alternator \
     --config-file config.dynamodb.yml
   ```

6. Run the migration job:

   ```bash
   ./deploy_spark_cluster.py run
   ```

   The `run` command checks that the Spark master is reachable on port `7077` and that the expected workers are registered. If needed, the script restarts the Spark systemd services before submitting the job. The submit script is launched with `nohup`, and the command prints the remote PID and log file path before returning.

   To upload a revised config before running, pass `--config-file`:

   ```bash
   ./deploy_spark_cluster.py run --config-file config.yaml
   ```

   To run the validator entrypoint instead of the migrator entrypoint:

   ```bash
   ./deploy_spark_cluster.py run --validator
   ```

7. Monitor progress in the Spark UI printed by `show` or by the `deploy` command. You can also SSH to the master and tail the log file printed by `run`.

8. Destroy the cluster when the migration is complete. The state directory selects the provider automatically:

   ```bash
   ./deploy_spark_cluster.py destroy --yes
   ```

   To also remove the local generated state directory after Terraform destroys the infrastructure:

   ```bash
   ./deploy_spark_cluster.py destroy --yes --delete-state-dir
   ```

### Default Instance Sizing

Defaults are selected after `--cloud-provider` is parsed:

- AWS master: `x2iedn.2xlarge` (8 x86_64 vCPUs and 256 GiB memory).
- GCP master: `n2-custom-8-262144-ext` (8 x86_64 vCPUs and 256 GiB extended memory).
- AWS worker: `i8g.4xlarge` (16 Arm vCPUs and 128 GiB memory).
- GCP worker: `c4a-highmem-16` (16 Google Axion Arm vCPUs and 128 GiB memory).

The mappings intentionally preserve CPU count, memory capacity, and CPU architecture. The AWS `i8g.4xlarge` also includes local NVMe instance storage, while the default GCP worker does not; the deploy helper does not format or mount the AWS instance store, so both default deployments use only the configured 100 GB root volume. Choose a provider-specific machine and storage configuration if the workload needs local scratch capacity.

The default GCP master uses `pd-balanced`; the C4A worker requires `hyperdisk-balanced`. The script infers an architecture-compatible Ubuntu image and a compatible default boot disk type when a machine type is overridden. The disk type can be overridden explicitly with `--gcp-master-boot-disk-type` or `--gcp-worker-boot-disk-type`.

During Ansible configuration, the role derives Spark worker cores, worker memory, executor cores, executor memory, and local directories from the instance hardware. Spark master, history server, and worker processes are managed by systemd.

### Derived Spark Settings

During Ansible configuration, the role derives Spark settings from each node's gathered hardware facts and writes them into `spark-env`.

On each worker node:

- `SPARK_WORKER_CORES` is set to the detected vCPU count.
- `SPARK_WORKER_MEMORY` is set to 85% of detected system memory, with a minimum of `1G`.
- `SPARK_WORKER_DIR` is set under `/mnt/spark-work` when `/mnt` is mounted, otherwise `/tmp/spark-work`.
- `SPARK_LOCAL_DIRS` is set under `/mnt/spark-local` when `/mnt` is mounted, otherwise `/tmp/spark-local`.

Executor settings are derived from the worker sizing:

- `EXECUTOR_CORES` is chosen as a divisor of worker cores, preferring values from `10` down to `5`, then falling back through `4`, `3`, `2`, and `1`.
- `EXECUTOR_MEMORY` is based on the number of executors that fit per worker. The role divides worker memory by executor slots and uses 90% of that value, with a minimum of `1G`.

The master submit environment uses the first worker as the reference for `EXECUTOR_CORES` and `EXECUTOR_MEMORY`, so mixed worker instance types are not recommended.

### Systemd Units

Ansible installs and enables Spark systemd units on the provisioned nodes. The deploy helper uses these units for the managed Spark lifecycle.

On the Spark master node:

- `spark-master.service`: Runs the Spark standalone master on port `7077` with the web UI on port `8080`.
- `spark-history-server.service`: Runs the Spark history server using `/tmp/spark-events`.

On each Spark worker node:

- `spark-worker.service`: Runs a Spark standalone worker registered to `spark://<master-private-ip>:7077`, using the derived worker cores, worker memory, local directories, and work directory.

Useful commands on a node:

```bash
sudo systemctl status spark-master
sudo systemctl status spark-history-server
sudo systemctl status spark-worker
sudo journalctl -u spark-master -f
sudo journalctl -u spark-history-server -f
sudo journalctl -u spark-worker -f
```

The Spark environment consumed by these services is rendered to:

- Master: `/home/ubuntu/scylla-migrator/spark-env`
- Workers: `/home/ubuntu/spark-env`

### Security Notes

The script requires `--allowed-ssh-cidr` and `--allowed-web-cidr` so SSH and Spark web UIs are not exposed to the public internet by default. Passing `0.0.0.0/0` is rejected unless you also pass `--allow-public-access`.

SSH host key verification is enabled by default. The script uses `StrictHostKeyChecking=accept-new` and stores host keys in `.deploy_spark_cluster/known_hosts`. Use `--insecure-ssh` only in trusted test environments where disabling host key verification is intentional.

Cluster nodes receive public IP addresses for SSH and UI access. The CIDR rules restrict inbound access, but production deployments should also use provider-native controls, least-privilege identities, and a protected Terraform state backend or state directory as appropriate for the environment.

### Command Reference

Run `./deploy_spark_cluster.py --help` or `./deploy_spark_cluster.py <subcommand> --help` for the latest CLI help.

All subcommands accept:

- `--state-dir`: Directory for generated Terraform files, Terraform state, generated inventory, metadata, and SSH `known_hosts`. Defaults to `.deploy_spark_cluster`.
- `--gcp-service-account-file`: Explicit service account JSON key used instead of the normal GCP ADC lookup. On `show`, `run`, `redeploy`, `destroy`, and a repeated `deploy` of a live deployment, defaults to the path saved in deployment metadata.

#### `deploy`

Creates AWS or GCP infrastructure, runs the Ansible playbook, optionally uploads a Migrator config file, and starts Spark unless told not to.

Required arguments:

- `--allowed-ssh-cidr`: IPv4 CIDR allowed to SSH to cluster nodes, for example `203.0.113.10/32`.
- `--allowed-web-cidr`: IPv4 CIDR allowed to reach Spark web UIs, for example `203.0.113.10/32`.

Common arguments:

- `--cloud-provider`: Cloud provider to use: `aws` or `gcp`. Defaults to `aws`.
- `--region`: Cloud region. Defaults to `us-east-1` for AWS and `us-central1` for GCP.
- `--zone`: GCP Compute Engine zone. Defaults to `<region>-a`, except in regions without an `-a` zone (`us-east1` and `europe-west1`), where it defaults to `<region>-b`. Choose a zone that offers the selected machine types.
- `--gcp-project`: GCP project ID. Required for GCP.
- `--name-prefix`: Prefix for generated resource names. Defaults to `scylla-migrator-spark`. GCP prefixes must satisfy Compute Engine naming rules.
- `--key-name`: AWS key pair name to create. Defaults to `<name-prefix>-key`.
- `--ssh-private-key`: SSH private key for connecting to instances. Defaults to `~/.ssh/id_rsa`.
- `--ssh-public-key`: SSH public key to register in the AWS key pair or GCP instance metadata. Defaults to `<ssh-private-key>.pub`. The file must contain exactly one well-formed OpenSSH public key. AWS accepts RSA and ED25519 keys; GCP also accepts ECDSA and FIDO security keys (`sk-*`). RSA keys must be at least 1024 bits. Its contents are written to `terraform.tfvars.json`, so `destroy` still works if the local key file later moves.
- `--master-instance-type`: Spark master machine type. Defaults to `x2iedn.2xlarge` on AWS and `n2-custom-8-262144-ext` on GCP.
- `--worker-instance-type`: Spark worker machine type. Defaults to `i8g.4xlarge` on AWS and `c4a-highmem-16` on GCP.
- `--workers`: Number of Spark worker instances. Defaults to `1`.
- `--owner-tag`: Optional owner value applied as an AWS `Owner` tag or GCP `owner` label. GCP label syntax is validated.
- `--migration-type`: Migrator config and submit script family to use. Allowed values are `cql` and `alternator`. Defaults to `cql`.
- `--config-file`: Optional local Migrator config file to upload to the Spark master.

Networking and infrastructure arguments:

- `--vpc-cidr`: AWS VPC CIDR. Defaults to `10.42.0.0/16` on AWS. GCP VPC networks do not have a network-wide CIDR.
- `--public-subnet-cidr`: CIDR for a generated AWS or GCP subnet. Defaults to `10.42.1.0/24`. The generated subnet must have room for the master and every worker after the addresses the provider reserves (5 on AWS, 4 on GCP). AWS subnets must be `/28` or larger, and GCP subnetworks must be `/29` or larger.
- `--vpc-id`: Existing AWS VPC ID to use instead of creating a new VPC. Must be provided with `--subnet-id`.
- `--subnet-id`: Existing subnet ID for EC2 instances when `--vpc-id` is set. The subnet must have outbound internet access.
- `--network`: Existing GCP VPC network name. Must be provided with `--subnetwork`.
- `--subnetwork`: Existing GCP subnetwork name in the selected project and region. Must be provided with `--network`.
- `--allow-public-access`: Allow `0.0.0.0/0` for SSH or Spark UI access when explicitly requested.
- `--root-volume-size-gb`: Root EBS, Persistent Disk, or Hyperdisk volume size for each instance. Defaults to `100`.
- `--iam-instance-profile`: Optional existing IAM instance profile to attach to the EC2 instances.
- `--gcp-instance-service-account`: Optional service account email to attach to GCP instances with the `cloud-platform` scope.
- `--gcp-master-boot-disk-type`: Optional GCP master boot disk type override.
- `--gcp-worker-boot-disk-type`: Optional GCP worker boot disk type override.

Operational arguments:

- `--skip-ansible`: Create infrastructure but skip Ansible configuration. With `--ssh-public-key`, this does not require the matching private key to exist locally. To configure the nodes later, run `redeploy`; it refreshes Terraform outputs and regenerates the inventory before running Ansible.
- `--skip-start`: Configure the nodes but do not restart Spark systemd services.
- `--insecure-ssh`: Disable SSH host key verification. Use only in trusted test environments.

#### `show`

Displays infrastructure and Spark endpoint details from Terraform output.

Arguments:

- `--json`: Print metadata and Terraform outputs as JSON.

#### `run`

Starts the configured Spark job on the Spark master node using the submit scripts installed by Ansible. The job is launched with `nohup` so it can keep running if the local SSH session disconnects. The command prints the remote PID and log file path before returning.

Arguments:

- `--ssh-private-key`: SSH private key to use. Defaults to the key saved in deployment metadata.
- `--migration-type`: Override the saved migration type. Allowed values are `cql` and `alternator`.
- `--config-file`: Upload a local config file to the Spark master before running.
- `--validator`: Run the validator entrypoint instead of the migrator entrypoint.
- `--insecure-ssh`: Disable SSH host key verification. Use only in trusted test environments.

#### `redeploy`

Refreshes Terraform outputs from the state directory, rewrites the generated inventory, and reruns the Ansible playbook against the current Terraform-managed nodes. This is useful after changing files under `ansible/`. It does not apply Terraform changes. By default, it stops Spark systemd services before Ansible runs, uploads the saved or explicitly supplied Migrator config file after Ansible completes, and then restarts Spark services.

Arguments:

- `--ssh-private-key`: SSH private key to use. Defaults to the key saved in deployment metadata.
- `--migration-type`: Override the saved migration type. Allowed values are `cql` and `alternator`.
- `--config-file`: Upload a local config file after rerunning Ansible. Defaults to the config file saved in deployment metadata.
- `--skip-start`: Do not stop or restart Spark systemd services around the Ansible run.
- `--insecure-ssh`: Disable SSH host key verification. Use only in trusted test environments.

#### `destroy`

Destroys the Terraform-managed AWS or GCP infrastructure.

Arguments:

- `--yes`: Skip the interactive confirmation prompt.
- `--delete-state-dir`: Delete the local state directory after Terraform destroy succeeds.

## Contributing

Please refer to the file [CONTRIBUTING.md](/CONTRIBUTING.md).
