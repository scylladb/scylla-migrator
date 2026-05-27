# ScyllaDB Migrator

The ScyllaDB Migrator is a Spark application that migrates data to ScyllaDB from CQL-compatible or DynamoDB-compatible databases.

## Table of Contents

- [Documentation](#documentation)
- [Building](#building)
- [Deploying a Spark Cluster](#deploying-a-spark-cluster)
  - [Prerequisites](#prerequisites)
  - [Runbook](#runbook)
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

The `deploy_spark_cluster.py` helper can create an AWS-backed Spark cluster, configure it with the existing Ansible playbook, run a Migrator job, show cluster details, and tear the cluster down.

The script uses Terraform for AWS infrastructure and Ansible for Spark/Migrator setup. It does not use Docker. Generated Terraform files, Terraform state, Ansible inventory, metadata, and SSH `known_hosts` data are stored in `.deploy_spark_cluster/` by default.

### Prerequisites

Install and configure the following on the machine where you run the script:

- Python 3.
- Terraform.
- Ansible.
- `ssh` and `scp`.
- AWS credentials that can create EC2 instances, VPC networking, security groups, and key pairs in the target region.
- An SSH private key and matching public key. By default the script uses `~/.ssh/id_rsa` and `~/.ssh/id_rsa.pub`.

The deployment creates an AWS key pair from the local public key. The EC2 instances use Ubuntu and the default SSH user is `ubuntu`.

### Runbook

1. Choose the AWS region, worker count, and network CIDRs allowed to reach the cluster. Prefer your current public IP as a `/32` CIDR:

   ```bash
   export MY_IP="$(curl -s https://checkip.amazonaws.com)"
   export MY_CIDR="${MY_IP}/32"
   ```

2. Prepare a Migrator config file. Use `config.yaml` for CQL migrations or an Alternator/DynamoDB config such as `config.dynamodb.yml` for Alternator migrations.

3. Deploy the cluster for a CQL migration:

   ```bash
   ./deploy_spark_cluster.py deploy \
     --region us-east-1 \
     --workers 3 \
     --migration-type cql \
     --config-file config.yaml \
     --ssh-private-key ~/.ssh/id_rsa \
     --allowed-ssh-cidr "$MY_CIDR" \
     --allowed-web-cidr "$MY_CIDR"
   ```

   For an Alternator migration, use:

   ```bash
   ./deploy_spark_cluster.py deploy \
     --region us-east-1 \
     --workers 3 \
     --migration-type alternator \
     --config-file config.dynamodb.yml \
     --ssh-private-key ~/.ssh/id_rsa \
     --allowed-ssh-cidr "$MY_CIDR" \
     --allowed-web-cidr "$MY_CIDR"
   ```

   The default master instance type is `x2iedn.2xlarge`. The default worker instance type is `i8g.4xlarge`.

4. Inspect the created infrastructure and Spark endpoints:

   ```bash
   ./deploy_spark_cluster.py show
   ```

   The output includes the VPC, subnet, security group, EC2 instance IDs, Spark master URL, Spark UI, application UI, and history server UI.

5. Rerun the Ansible configuration when you need to apply local playbook or script changes to the current nodes:

   ```bash
   ./deploy_spark_cluster.py redeploy
   ```

   This uses the generated inventory in `.deploy_spark_cluster/inventory.ini` and does not run Terraform.

6. Run the migration job:

   ```bash
   ./deploy_spark_cluster.py run
   ```

   The `run` command checks that the Spark master is reachable on port `7077`. If it is not reachable, the script starts Spark and waits for the master before submitting the job.

   To upload a revised config before running, pass `--config-file`:

   ```bash
   ./deploy_spark_cluster.py run --config-file config.yaml
   ```

   To run the validator entrypoint instead of the migrator entrypoint:

   ```bash
   ./deploy_spark_cluster.py run --validator
   ```

7. Monitor progress in the Spark UI printed by `show` or by the `deploy` command.

8. Destroy the cluster when the migration is complete:

   ```bash
   ./deploy_spark_cluster.py destroy --yes
   ```

   To also remove the local generated state directory after Terraform destroys the infrastructure:

   ```bash
   ./deploy_spark_cluster.py destroy --yes --delete-state-dir
   ```

### Security Notes

The script requires `--allowed-ssh-cidr` and `--allowed-web-cidr` so SSH and Spark web UIs are not exposed to the public internet by default. Passing `0.0.0.0/0` is rejected unless you also pass `--allow-public-access`.

SSH host key verification is enabled by default. The script uses `StrictHostKeyChecking=accept-new` and stores host keys in `.deploy_spark_cluster/known_hosts`. Use `--insecure-ssh` only in trusted test environments where disabling host key verification is intentional.

### Command Reference

Run `./deploy_spark_cluster.py --help` or `./deploy_spark_cluster.py <subcommand> --help` for the latest CLI help.

All subcommands accept:

- `--state-dir`: Directory for generated Terraform files, Terraform state, generated inventory, metadata, and SSH `known_hosts`. Defaults to `.deploy_spark_cluster`.

#### `deploy`

Creates AWS infrastructure, runs the Ansible playbook, optionally uploads a Migrator config file, and starts Spark unless told not to.

Required arguments:

- `--allowed-ssh-cidr`: IPv4 CIDR allowed to SSH to cluster nodes, for example `203.0.113.10/32`.
- `--allowed-web-cidr`: IPv4 CIDR allowed to reach Spark web UIs, for example `203.0.113.10/32`.

Common arguments:

- `--cloud-provider`: Cloud provider to use. Currently only `aws` is supported.
- `--region`: AWS region. Defaults to `us-east-1`.
- `--name-prefix`: Prefix for generated AWS resource names. Defaults to `scylla-migrator-spark`.
- `--key-name`: AWS key pair name to create. Defaults to `<name-prefix>-key`.
- `--ssh-private-key`: SSH private key for connecting to EC2 instances. Defaults to `~/.ssh/id_rsa`.
- `--ssh-public-key`: SSH public key to register as the AWS key pair. Defaults to `<ssh-private-key>.pub`.
- `--master-instance-type`: Spark master EC2 instance type. Defaults to `x2iedn.2xlarge`.
- `--worker-instance-type`: Spark worker EC2 instance type. Defaults to `i8g.4xlarge`.
- `--workers`: Number of Spark worker instances. Defaults to `1`.
- `--owner-tag`: Optional `Owner` tag value to apply to the Spark master and worker EC2 instances.
- `--migration-type`: Migrator config and submit script family to use. Allowed values are `cql` and `alternator`. Defaults to `cql`.
- `--config-file`: Optional local Migrator config file to upload to the Spark master.

Networking and infrastructure arguments:

- `--vpc-cidr`: VPC CIDR. Defaults to `10.42.0.0/16`.
- `--public-subnet-cidr`: Public subnet CIDR. Defaults to `10.42.1.0/24`.
- `--allow-public-access`: Allow `0.0.0.0/0` for SSH or Spark UI access when explicitly requested.
- `--root-volume-size-gb`: Root EBS volume size for each EC2 instance. Defaults to `100`.
- `--iam-instance-profile`: Optional existing IAM instance profile to attach to the EC2 instances.

Operational arguments:

- `--skip-ansible`: Create infrastructure but skip Ansible configuration.
- `--skip-start`: Configure the nodes but do not start Spark.
- `--insecure-ssh`: Disable SSH host key verification. Use only in trusted test environments.

#### `show`

Displays infrastructure and Spark endpoint details from Terraform output.

Arguments:

- `--json`: Print metadata and Terraform outputs as JSON.

#### `run`

Runs the configured Spark job on the Spark master node using the submit scripts installed by Ansible.

Arguments:

- `--ssh-private-key`: SSH private key to use. Defaults to the key saved in deployment metadata.
- `--migration-type`: Override the saved migration type. Allowed values are `cql` and `alternator`.
- `--config-file`: Upload a local config file to the Spark master before running.
- `--validator`: Run the validator entrypoint instead of the migrator entrypoint.
- `--insecure-ssh`: Disable SSH host key verification. Use only in trusted test environments.

#### `redeploy`

Reruns the Ansible playbook against the current nodes in the generated inventory. This is useful after changing files under `ansible/` and does not run Terraform.

Arguments:

- `--ssh-private-key`: SSH private key to use. Defaults to the key saved in deployment metadata.
- `--insecure-ssh`: Disable SSH host key verification. Use only in trusted test environments.

#### `destroy`

Destroys the Terraform-managed AWS infrastructure.

Arguments:

- `--yes`: Skip the interactive confirmation prompt.
- `--delete-state-dir`: Delete the local state directory after Terraform destroy succeeds.

## Contributing

Please refer to the file [CONTRIBUTING.md](/CONTRIBUTING.md).
