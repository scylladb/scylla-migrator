#!/usr/bin/env python3
"""Provision and operate a standalone Spark cluster for ScyllaDB Migrator.

This script owns the cloud infrastructure lifecycle with Terraform and uses
the existing Ansible playbook in ./ansible to install Spark and Migrator on the
created EC2 instances.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STATE_DIR = ".deploy_spark_cluster"
DEFAULT_USER = "ubuntu"
REMOTE_MIGRATOR_DIR = "/home/ubuntu/scylla-migrator"


TERRAFORM_MAIN = """terraform {
  required_version = ">= 1.3.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "key_name" {
  type = string
}

variable "ssh_public_key_path" {
  type = string
}

variable "master_instance_type" {
  type = string
}

variable "worker_instance_type" {
  type = string
}

variable "worker_count" {
  type = number
}

variable "master_ami_architecture" {
  type = string
}

variable "worker_ami_architecture" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "public_subnet_cidr" {
  type = string
}

variable "existing_vpc_id" {
  type    = string
  default = ""
}

variable "existing_subnet_id" {
  type    = string
  default = ""
}

variable "allowed_ssh_cidr" {
  type = string
}

variable "allowed_web_cidr" {
  type = string
}

variable "root_volume_size_gb" {
  type = number
}

variable "iam_instance_profile" {
  type    = string
  default = ""
}

variable "owner_tag" {
  type    = string
  default = ""
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  use_existing_network = var.existing_vpc_id != ""
  vpc_id               = local.use_existing_network ? var.existing_vpc_id : aws_vpc.spark[0].id
  subnet_id            = local.use_existing_network ? var.existing_subnet_id : aws_subnet.public[0].id
}

data "aws_ami" "ubuntu_master" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-*-server-*"]
  }

  filter {
    name   = "architecture"
    values = [var.master_ami_architecture]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_ami" "ubuntu_worker" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-*-server-*"]
  }

  filter {
    name   = "architecture"
    values = [var.worker_ami_architecture]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_key_pair" "spark" {
  key_name   = var.key_name
  public_key = file(var.ssh_public_key_path)

  tags = {
    Name = "${var.name_prefix}-key"
  }
}

resource "aws_vpc" "spark" {
  count                = local.use_existing_network ? 0 : 1
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.name_prefix}-vpc"
  }
}

resource "aws_internet_gateway" "spark" {
  count  = local.use_existing_network ? 0 : 1
  vpc_id = aws_vpc.spark[0].id

  tags = {
    Name = "${var.name_prefix}-igw"
  }
}

resource "aws_subnet" "public" {
  count                   = local.use_existing_network ? 0 : 1
  vpc_id                  = aws_vpc.spark[0].id
  cidr_block              = var.public_subnet_cidr
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.name_prefix}-public-subnet"
  }
}

resource "aws_route_table" "public" {
  count  = local.use_existing_network ? 0 : 1
  vpc_id = aws_vpc.spark[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.spark[0].id
  }

  tags = {
    Name = "${var.name_prefix}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  count          = local.use_existing_network ? 0 : 1
  subnet_id      = aws_subnet.public[0].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_security_group" "spark" {
  name        = "${var.name_prefix}-sg"
  description = "Spark cluster access for ScyllaDB Migrator"
  vpc_id      = local.vpc_id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }

  ingress {
    description = "Spark master UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_web_cidr]
  }

  ingress {
    description = "Spark application UI"
    from_port   = 4040
    to_port     = 4040
    protocol    = "tcp"
    cidr_blocks = [var.allowed_web_cidr]
  }

  ingress {
    description = "Spark history server UI"
    from_port   = 18080
    to_port     = 18080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_web_cidr]
  }

  ingress {
    description = "Cluster-internal traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name_prefix}-sg"
  }
}

resource "aws_instance" "spark_master" {
  ami                         = data.aws_ami.ubuntu_master.id
  instance_type               = var.master_instance_type
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.spark.id]
  key_name                    = aws_key_pair.spark.key_name
  associate_public_ip_address = true
  iam_instance_profile        = var.iam_instance_profile == "" ? null : var.iam_instance_profile

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
  }

  tags = merge(
    {
      Name = "${var.name_prefix}-master"
      Role = "spark-master"
    },
    var.owner_tag == "" ? {} : { Owner = var.owner_tag }
  )
}

resource "aws_instance" "spark_worker" {
  count                       = var.worker_count
  ami                         = data.aws_ami.ubuntu_worker.id
  instance_type               = var.worker_instance_type
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.spark.id]
  key_name                    = aws_key_pair.spark.key_name
  associate_public_ip_address = true
  iam_instance_profile        = var.iam_instance_profile == "" ? null : var.iam_instance_profile

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
  }

  tags = merge(
    {
      Name = "${var.name_prefix}-worker-${count.index + 1}"
      Role = "spark-worker"
    },
    var.owner_tag == "" ? {} : { Owner = var.owner_tag }
  )
}

output "region" {
  value = var.region
}

output "vpc_id" {
  value = local.vpc_id
}

output "public_subnet_id" {
  value = local.subnet_id
}

output "security_group_id" {
  value = aws_security_group.spark.id
}

output "key_name" {
  value = aws_key_pair.spark.key_name
}

output "master" {
  value = {
    name        = "spark_master"
    instance_id = aws_instance.spark_master.id
    public_ip   = aws_instance.spark_master.public_ip
    private_ip  = aws_instance.spark_master.private_ip
  }
}

output "workers" {
  value = [
    for idx, worker in aws_instance.spark_worker : {
      name        = "spark_worker${idx + 1}"
      instance_id = worker.id
      public_ip   = worker.public_ip
      private_ip  = worker.private_ip
    }
  ]
}

output "spark_master_url" {
  value = "spark://${aws_instance.spark_master.private_ip}:7077"
}

output "spark_master_ui" {
  value = "http://${aws_instance.spark_master.public_ip}:8080"
}

output "spark_application_ui" {
  value = "http://${aws_instance.spark_master.public_ip}:4040"
}

output "spark_history_ui" {
  value = "http://${aws_instance.spark_master.public_ip}:18080"
}
"""


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def ipv4_cidr(value: str) -> str:
    try:
        network = ipaddress.ip_network(value, strict=False)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid CIDR: {value}") from exc
    if network.version != 4:
        raise argparse.ArgumentTypeError("must be an IPv4 CIDR")
    return str(network)


def repo_root() -> Path:
    return Path(__file__).resolve().parent


def resolve_path(value: str | None) -> Path | None:
    if value is None or value == "":
        return None
    return Path(value).expanduser().resolve()


def resolve_state_dir(value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = repo_root() / path
    return path.resolve()


def require_commands(commands: list[str]) -> None:
    missing = [command for command in commands if shutil.which(command) is None]
    if missing:
        joined = ", ".join(missing)
        raise SystemExit(f"Missing required command(s): {joined}")


def run_command(
    args: list[str],
    *,
    cwd: Path | None = None,
    capture_output: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    print(f"+ {shlex.join(args)}", file=sys.stderr)
    process_env = os.environ.copy()
    if env:
        process_env.update(env)
    return subprocess.run(
        args,
        cwd=cwd,
        check=True,
        text=True,
        capture_output=capture_output,
        env=process_env,
    )


def print_command_error(exc: subprocess.CalledProcessError) -> None:
    print(f"Command failed with exit code {exc.returncode}: {shlex.join(exc.cmd)}", file=sys.stderr)
    if exc.stdout:
        print("Command stdout:", file=sys.stderr)
        print(exc.stdout, file=sys.stderr)
    if exc.stderr:
        print("Command stderr:", file=sys.stderr)
        print(exc.stderr, file=sys.stderr)


def terraform_output(state_dir: Path) -> dict[str, Any]:
    completed = run_command(
        ["terraform", "output", "-json"],
        cwd=state_dir,
        capture_output=True,
    )
    raw_outputs = json.loads(completed.stdout)
    return {key: value["value"] for key, value in raw_outputs.items()}


def write_json(path: Path, content: dict[str, Any]) -> None:
    path.write_text(json.dumps(content, indent=2, sort_keys=True) + "\n")
    path.chmod(0o600)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc


def infer_aws_architecture(instance_type: str) -> str:
    family = instance_type.split(".", 1)[0].lower()
    arm64_families = {
        "a1",
        "c6g",
        "c6gd",
        "c6gn",
        "c7g",
        "c7gd",
        "c7gn",
        "c8g",
        "g5g",
        "hpc7g",
        "i4g",
        "i8g",
        "im4gn",
        "is4gen",
        "m6g",
        "m6gd",
        "m7g",
        "m7gd",
        "m8g",
        "r6g",
        "r6gd",
        "r7g",
        "r7gd",
        "r8g",
        "t4g",
        "x2gd",
    }
    if family in arm64_families:
        return "arm64"
    return "x86_64"


def known_hosts_path(state_dir: Path) -> Path:
    path = state_dir / "known_hosts"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    path.touch(exist_ok=True)
    path.chmod(0o600)
    return path


def ssh_options(private_key: Path, known_hosts: Path, insecure: bool) -> list[str]:
    options = [
        "-i",
        str(private_key),
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=10",
    ]

    if insecure:
        return [
            *options,
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
        ]

    return [
        *options,
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
    ]


def ssh_command(
    host: str,
    remote_command: str,
    *,
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
    user: str = DEFAULT_USER,
) -> None:
    run_command(
        [
            "ssh",
            *ssh_options(private_key, known_hosts, insecure),
            f"{user}@{host}",
            remote_command,
        ]
    )


def ssh_command_succeeds(
    host: str,
    remote_command: str,
    *,
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
    user: str = DEFAULT_USER,
) -> bool:
    completed = subprocess.run(
        [
            "ssh",
            *ssh_options(private_key, known_hosts, insecure),
            f"{user}@{host}",
            remote_command,
        ],
        text=True,
        capture_output=True,
    )
    return completed.returncode == 0


def scp_to_host(
    source: Path,
    host: str,
    destination: str,
    *,
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
    user: str = DEFAULT_USER,
) -> None:
    run_command(
        [
            "scp",
            *ssh_options(private_key, known_hosts, insecure),
            str(source),
            f"{user}@{host}:{destination}",
        ]
    )


def wait_for_ssh(
    hosts: list[str],
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
    user: str = DEFAULT_USER,
) -> None:
    for host in hosts:
        print(f"Waiting for SSH on {host} ...")
        for attempt in range(1, 31):
            completed = subprocess.run(
                [
                    "ssh",
                    *ssh_options(private_key, known_hosts, insecure),
                    f"{user}@{host}",
                    "true",
                ],
                text=True,
                capture_output=True,
            )
            if completed.returncode == 0:
                break
            if attempt == 30:
                raise SystemExit(f"Timed out waiting for SSH on {host}")
            time.sleep(10)


def migration_config_name(migration_type: str) -> str:
    if migration_type == "cql":
        return "config.yaml"
    if migration_type == "alternator":
        return "config.dynamodb.yml"
    raise ValueError(f"Unsupported migration type: {migration_type}")


def submit_script_name(migration_type: str, validator: bool) -> str:
    if migration_type == "cql":
        return "submit-cql-job-validator.sh" if validator else "submit-cql-job.sh"
    if migration_type == "alternator":
        return "submit-alternator-validator.sh" if validator else "submit-alternator-job.sh"
    raise ValueError(f"Unsupported migration type: {migration_type}")


def remote_submit_command(submit_script: str) -> str:
    log_stem = Path(submit_script).stem
    remote_dir = shlex.quote(REMOTE_MIGRATOR_DIR)
    remote_log_stem = shlex.quote(f"{REMOTE_MIGRATOR_DIR}/{log_stem}")
    script = shlex.quote(f"./{submit_script}")
    return (
        f"cd {remote_dir} && "
        "timestamp=$(date -u +%Y%m%dT%H%M%SZ) && "
        f"log_file={remote_log_stem}-$timestamp.log && "
        f"pid_file={remote_log_stem}-$timestamp.pid && "
        f"{{ nohup {script} > \"$log_file\" 2>&1 < /dev/null & echo $! > \"$pid_file\"; }} && "
        'echo "Started Spark submit job with PID $(cat "$pid_file")." && '
        'echo "Log file: $log_file"'
    )


def upload_config_if_requested(
    config_file: Path | None,
    *,
    migration_type: str,
    master_public_ip: str,
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    if config_file is None:
        return
    validate_local_config_file(config_file)

    remote_name = migration_config_name(migration_type)
    destination = f"{REMOTE_MIGRATOR_DIR}/{remote_name}"
    scp_to_host(
        config_file,
        master_public_ip,
        destination,
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=insecure,
    )


def validate_local_config_file(config_file: Path | None) -> None:
    if config_file is None:
        return
    if not config_file.exists():
        raise SystemExit(f"Config file does not exist: {config_file}")
    if not config_file.is_file():
        raise SystemExit(f"Config path is not a file: {config_file}")


def ensure_remote_config_exists(
    *,
    migration_type: str,
    master_public_ip: str,
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    remote_name = migration_config_name(migration_type)
    remote_path = f"{REMOTE_MIGRATOR_DIR}/{remote_name}"
    if ssh_command_succeeds(
        master_public_ip,
        f"test -f {shlex.quote(remote_path)}",
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=insecure,
    ):
        return

    raise SystemExit(
        f"Required Migrator config was not found on the Spark master: {remote_path}. "
        "Pass --config-file to upload it before running."
    )


def config_file_from_args_or_metadata(
    config_file_arg: str | None,
    metadata: dict[str, Any],
) -> Path | None:
    if config_file_arg is not None:
        return resolve_path(config_file_arg)

    saved_config_file = metadata.get("config_file")
    if not saved_config_file:
        return None
    return resolve_path(saved_config_file)


def remember_config_file(
    state_dir: Path,
    metadata: dict[str, Any],
    *,
    config_file: Path | None,
    migration_type: str,
) -> None:
    metadata["migration_type"] = migration_type
    if config_file is not None:
        metadata["config_file"] = str(config_file)
    write_json(state_dir / "metadata.json", metadata)


def write_terraform_files(args: argparse.Namespace, state_dir: Path) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    state_dir.chmod(0o700)
    (state_dir / "main.tf").write_text(TERRAFORM_MAIN)

    public_key = resolve_path(args.ssh_public_key)
    private_key = resolve_path(args.ssh_private_key)
    if public_key is None:
        if private_key is None:
            raise SystemExit("Either --ssh-public-key or --ssh-private-key is required")
        public_key = Path(f"{private_key}.pub")

    if not public_key.exists():
        raise SystemExit(f"SSH public key does not exist: {public_key}")

    tfvars = {
        "region": args.region,
        "name_prefix": args.name_prefix,
        "key_name": args.key_name or f"{args.name_prefix}-key",
        "ssh_public_key_path": str(public_key),
        "master_instance_type": args.master_instance_type,
        "worker_instance_type": args.worker_instance_type,
        "worker_count": args.workers,
        "master_ami_architecture": infer_aws_architecture(args.master_instance_type),
        "worker_ami_architecture": infer_aws_architecture(args.worker_instance_type),
        "vpc_cidr": args.vpc_cidr,
        "public_subnet_cidr": args.public_subnet_cidr,
        "existing_vpc_id": args.vpc_id or "",
        "existing_subnet_id": args.subnet_id or "",
        "allowed_ssh_cidr": args.allowed_ssh_cidr,
        "allowed_web_cidr": args.allowed_web_cidr,
        "root_volume_size_gb": args.root_volume_size_gb,
        "iam_instance_profile": args.iam_instance_profile or "",
        "owner_tag": args.owner_tag or "",
    }
    write_json(state_dir / "terraform.tfvars.json", tfvars)


def write_ansible_inventory(
    outputs: dict[str, Any],
    *,
    private_key: Path,
    state_dir: Path,
) -> Path:
    inventory_path = state_dir / "inventory.ini"
    master = outputs["master"]
    workers = outputs["workers"]

    lines = [
        "[spark]",
        (
            "spark_master "
            f"ansible_host={master['public_ip']} "
            f"ansible_user={DEFAULT_USER} "
            f"ansible_ssh_private_key_file={private_key}"
        ),
    ]

    for index, worker in enumerate(workers, start=1):
        lines.append(
            f"spark_worker{index} "
            f"ansible_host={worker['public_ip']} "
            f"ansible_user={DEFAULT_USER} "
            f"ansible_ssh_private_key_file={private_key}"
        )

    lines.extend(["", "[master]", "spark_master", "", "[worker]"])
    for index, _worker in enumerate(workers, start=1):
        lines.append(f"spark_worker{index}")

    inventory_path.write_text("\n".join(lines) + "\n")
    return inventory_path


def run_ansible(
    inventory_path: Path,
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    ansible_dir = repo_root() / "ansible"
    if insecure:
        host_key_checking = "False"
        ssh_common_args = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    else:
        host_key_checking = "True"
        ssh_common_args = (
            f"-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile={known_hosts}"
        )

    run_command(
        [
            "ansible-playbook",
            "-i",
            str(inventory_path),
            "--private-key",
            str(private_key),
            "-u",
            DEFAULT_USER,
            "--ssh-common-args",
            ssh_common_args,
            "scylla-migrator.yml",
        ],
        cwd=ansible_dir,
        env={
            "ANSIBLE_CONFIG": str(ansible_dir / "ansible.cfg"),
            "ANSIBLE_HOST_KEY_CHECKING": host_key_checking,
        },
    )


def start_spark(
    outputs: dict[str, Any],
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    master_public_ip = outputs["master"]["public_ip"]
    ssh_command(
        master_public_ip,
        "sudo systemctl restart spark-master spark-history-server",
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=insecure,
    )

    start_workers(outputs, private_key, known_hosts, insecure)


def stop_spark(
    outputs: dict[str, Any],
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    for worker in outputs["workers"]:
        ssh_command(
            worker["public_ip"],
            "sudo systemctl stop spark-worker",
            private_key=private_key,
            known_hosts=known_hosts,
            insecure=insecure,
        )

    ssh_command(
        outputs["master"]["public_ip"],
        "sudo systemctl stop spark-history-server spark-master",
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=insecure,
    )


def spark_master_is_reachable(
    outputs: dict[str, Any],
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> bool:
    master_public_ip = outputs["master"]["public_ip"]
    private_ip = outputs["master"]["private_ip"]
    return ssh_command_succeeds(
        master_public_ip,
        (
            "python3 -c "
            + shlex.quote(
                "import socket, sys; "
                f"s=socket.create_connection(('{private_ip}', 7077), 2); s.close()"
            )
        ),
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=insecure,
    )


def wait_for_spark_master(
    outputs: dict[str, Any],
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    for _attempt in range(1, 25):
        if spark_master_is_reachable(outputs, private_key, known_hosts, insecure):
            return
        time.sleep(5)
    master_url = outputs.get("spark_master_url", "spark://<master>:7077")
    raise SystemExit(f"Timed out waiting for Spark master at {master_url}")


def registered_worker_count(
    outputs: dict[str, Any],
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> int:
    master_public_ip = outputs["master"]["public_ip"]
    private_ip = outputs["master"]["private_ip"]
    command = (
        "python3 -c "
        + shlex.quote(
            "import json, urllib.request; "
            f"data=json.load(urllib.request.urlopen('http://{private_ip}:8080/json', timeout=5)); "
            "print(sum(1 for worker in data.get('workers', []) "
            "if worker.get('state') == 'ALIVE'))"
        )
    )
    completed = subprocess.run(
        [
            "ssh",
            *ssh_options(private_key, known_hosts, insecure),
            f"{DEFAULT_USER}@{master_public_ip}",
            command,
        ],
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        return 0
    try:
        return int(completed.stdout.strip())
    except ValueError:
        return 0


def start_workers(
    outputs: dict[str, Any],
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    for worker in outputs["workers"]:
        ssh_command(
            worker["public_ip"],
            "sudo systemctl restart spark-worker",
            private_key=private_key,
            known_hosts=known_hosts,
            insecure=insecure,
        )


def wait_for_spark_workers(
    outputs: dict[str, Any],
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    expected_workers = len(outputs["workers"])
    if expected_workers == 0:
        return

    for _attempt in range(1, 25):
        count = registered_worker_count(outputs, private_key, known_hosts, insecure)
        if count >= expected_workers:
            return
        time.sleep(5)

    count = registered_worker_count(outputs, private_key, known_hosts, insecure)
    raise SystemExit(
        "Timed out waiting for Spark workers to register "
        f"({count}/{expected_workers} registered)"
    )


def ensure_spark_running(
    outputs: dict[str, Any],
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    if not spark_master_is_reachable(outputs, private_key, known_hosts, insecure):
        print("Spark master is not reachable on port 7077; starting Spark...")
        start_spark(outputs, private_key, known_hosts, insecure)
        wait_for_spark_master(outputs, private_key, known_hosts, insecure)
    else:
        expected_workers = len(outputs["workers"])
        current_workers = registered_worker_count(outputs, private_key, known_hosts, insecure)
        if current_workers < expected_workers:
            print(
                "Spark master is reachable, but only "
                f"{current_workers}/{expected_workers} workers are registered; starting workers..."
            )
            start_workers(outputs, private_key, known_hosts, insecure)

    wait_for_spark_workers(outputs, private_key, known_hosts, insecure)


def save_metadata(
    args: argparse.Namespace,
    *,
    state_dir: Path,
    private_key: Path,
    outputs: dict[str, Any],
) -> None:
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "cloud_provider": args.cloud_provider,
        "region": args.region,
        "name_prefix": args.name_prefix,
        "master_instance_type": args.master_instance_type,
        "worker_instance_type": args.worker_instance_type,
        "workers": args.workers,
        "owner_tag": args.owner_tag or "",
        "vpc_id": args.vpc_id or "",
        "subnet_id": args.subnet_id or "",
        "migration_type": args.migration_type,
        "config_file": str(resolve_path(args.config_file)) if args.config_file else "",
        "ssh_private_key": str(private_key),
        "ssh_known_hosts": str(known_hosts_path(state_dir)),
        "state_dir": str(state_dir),
        "terraform_outputs": outputs,
    }
    write_json(state_dir / "metadata.json", metadata)


def load_metadata(state_dir: Path) -> dict[str, Any]:
    return read_json(state_dir / "metadata.json")


def require_terraform_state(state_dir: Path) -> None:
    if not state_dir.is_dir():
        raise SystemExit(f"State directory does not exist: {state_dir}")
    terraform_state = state_dir / "terraform.tfstate"
    if not terraform_state.is_file():
        raise SystemExit(f"Terraform state file does not exist: {terraform_state}")


def validate_access_cidrs(args: argparse.Namespace) -> None:
    public_cidrs = {
        "--allowed-ssh-cidr": args.allowed_ssh_cidr,
        "--allowed-web-cidr": args.allowed_web_cidr,
    }
    exposed = [flag for flag, cidr in public_cidrs.items() if cidr == "0.0.0.0/0"]
    if exposed and not args.allow_public_access:
        flags = ", ".join(exposed)
        raise SystemExit(
            f"{flags} opens access to the public internet. Re-run with "
            "--allow-public-access if this is intentional."
        )


def validate_network_args(args: argparse.Namespace) -> None:
    if bool(args.vpc_id) != bool(args.subnet_id):
        raise SystemExit("--vpc-id and --subnet-id must be provided together.")
    if args.vpc_id and args.subnet_id:
        return

    try:
        vpc_network = ipaddress.ip_network(args.vpc_cidr, strict=False)
        subnet_network = ipaddress.ip_network(args.public_subnet_cidr, strict=False)
    except ValueError as exc:
        raise SystemExit(f"Invalid VPC or subnet CIDR: {exc}") from exc

    if vpc_network.version != 4 or subnet_network.version != 4:
        raise SystemExit("--vpc-cidr and --public-subnet-cidr must be IPv4 CIDRs.")
    if not subnet_network.subnet_of(vpc_network):
        raise SystemExit(
            f"--public-subnet-cidr ({subnet_network}) must be contained in "
            f"--vpc-cidr ({vpc_network})."
        )


def handle_deploy(args: argparse.Namespace) -> None:
    if args.cloud_provider != "aws":
        raise SystemExit("Only AWS is currently supported")
    validate_access_cidrs(args)
    validate_network_args(args)

    state_dir = resolve_state_dir(args.state_dir)
    deploy_config_file = resolve_path(args.config_file)
    validate_local_config_file(deploy_config_file)

    private_key = resolve_path(args.ssh_private_key)
    if private_key is None or not private_key.exists():
        raise SystemExit(f"SSH private key does not exist: {private_key}")
    known_hosts = known_hosts_path(state_dir)

    required = ["terraform"]
    if not args.skip_ansible:
        required.extend(["ansible-playbook", "ssh", "scp"])
    require_commands(required)

    write_terraform_files(args, state_dir)
    run_command(["terraform", "init", "-input=false"], cwd=state_dir)
    run_command(["terraform", "apply", "-auto-approve"], cwd=state_dir)
    outputs = terraform_output(state_dir)
    save_metadata(args, state_dir=state_dir, private_key=private_key, outputs=outputs)

    if args.skip_ansible:
        print("Skipping Ansible configuration.")
        print_cluster_details(outputs, metadata=load_metadata(state_dir))
        return

    all_public_ips = [outputs["master"]["public_ip"]]
    all_public_ips.extend(worker["public_ip"] for worker in outputs["workers"])
    wait_for_ssh(all_public_ips, private_key, known_hosts, args.insecure_ssh)

    inventory_path = write_ansible_inventory(
        outputs,
        private_key=private_key,
        state_dir=state_dir,
    )
    run_ansible(inventory_path, private_key, known_hosts, args.insecure_ssh)

    deploy_metadata = load_metadata(state_dir)
    upload_config_if_requested(
        deploy_config_file,
        migration_type=args.migration_type,
        master_public_ip=outputs["master"]["public_ip"],
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=args.insecure_ssh,
    )
    remember_config_file(
        state_dir,
        deploy_metadata,
        config_file=deploy_config_file,
        migration_type=args.migration_type,
    )

    if not args.skip_start:
        start_spark(outputs, private_key, known_hosts, args.insecure_ssh)
        wait_for_spark_master(outputs, private_key, known_hosts, args.insecure_ssh)
        wait_for_spark_workers(outputs, private_key, known_hosts, args.insecure_ssh)

    print_cluster_details(outputs, metadata=load_metadata(state_dir))


def print_cluster_details(outputs: dict[str, Any], *, metadata: dict[str, Any]) -> None:
    master = outputs["master"]
    workers = outputs["workers"]

    print("")
    print("Spark cluster")
    if metadata:
        print(f"  Provider: {metadata.get('cloud_provider', 'aws')}")
        print(f"  Region: {metadata.get('region', outputs.get('region', 'unknown'))}")
        print(f"  Migration type: {metadata.get('migration_type', 'unknown')}")
    print(f"  Spark master: {outputs['spark_master_url']}")
    print(f"  Spark UI: {outputs['spark_master_ui']}")
    print(f"  Spark app UI: {outputs['spark_application_ui']}")
    print(f"  Spark history UI: {outputs['spark_history_ui']}")
    print("")
    print("Infrastructure")
    print(f"  VPC: {outputs['vpc_id']}")
    print(f"  Public subnet: {outputs['public_subnet_id']}")
    print(f"  Security group: {outputs['security_group_id']}")
    print(f"  Key pair: {outputs['key_name']}")
    print("")
    print("Instances")
    print(
        "  Master: "
        f"{master['instance_id']} public={master['public_ip']} private={master['private_ip']}"
    )
    for worker in workers:
        print(
            "  Worker: "
            f"{worker['instance_id']} public={worker['public_ip']} private={worker['private_ip']}"
        )


def handle_show(args: argparse.Namespace) -> None:
    state_dir = resolve_state_dir(args.state_dir)
    require_terraform_state(state_dir)
    require_commands(["terraform"])
    metadata = load_metadata(state_dir)
    outputs = terraform_output(state_dir)

    if args.json:
        print(json.dumps({"metadata": metadata, "terraform_outputs": outputs}, indent=2))
        return

    print_cluster_details(outputs, metadata=metadata)


def handle_run(args: argparse.Namespace) -> None:
    state_dir = resolve_state_dir(args.state_dir)
    require_terraform_state(state_dir)
    require_commands(["terraform", "ssh", "scp"])
    metadata = load_metadata(state_dir)
    outputs = terraform_output(state_dir)

    private_key = resolve_path(args.ssh_private_key or metadata.get("ssh_private_key"))
    if private_key is None or not private_key.exists():
        raise SystemExit("Could not find SSH private key. Pass --ssh-private-key.")
    known_hosts = resolve_path(metadata.get("ssh_known_hosts")) or known_hosts_path(state_dir)
    insecure = args.insecure_ssh

    ensure_spark_running(outputs, private_key, known_hosts, insecure)

    migration_type = args.migration_type or metadata.get("migration_type") or "cql"
    run_config_file = config_file_from_args_or_metadata(args.config_file, metadata)
    upload_config_if_requested(
        run_config_file,
        migration_type=migration_type,
        master_public_ip=outputs["master"]["public_ip"],
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=insecure,
    )
    remember_config_file(
        state_dir,
        metadata,
        config_file=run_config_file,
        migration_type=migration_type,
    )
    ensure_remote_config_exists(
        migration_type=migration_type,
        master_public_ip=outputs["master"]["public_ip"],
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=insecure,
    )

    submit_script = submit_script_name(migration_type, args.validator)
    ssh_command(
        outputs["master"]["public_ip"],
        remote_submit_command(submit_script),
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=insecure,
    )


def handle_redeploy(args: argparse.Namespace) -> None:
    state_dir = resolve_state_dir(args.state_dir)
    require_terraform_state(state_dir)
    require_commands(["terraform", "ansible-playbook", "ssh", "scp"])
    metadata = load_metadata(state_dir)

    private_key = resolve_path(args.ssh_private_key or metadata.get("ssh_private_key"))
    if private_key is None or not private_key.exists():
        raise SystemExit("Could not find SSH private key. Pass --ssh-private-key.")

    known_hosts = resolve_path(metadata.get("ssh_known_hosts")) or known_hosts_path(state_dir)
    insecure = args.insecure_ssh
    migration_type = args.migration_type or metadata.get("migration_type") or "cql"
    redeploy_config_file = config_file_from_args_or_metadata(args.config_file, metadata)
    outputs = terraform_output(state_dir)
    metadata["terraform_outputs"] = outputs
    write_json(state_dir / "metadata.json", metadata)

    inventory_path = write_ansible_inventory(
        outputs,
        private_key=private_key,
        state_dir=state_dir,
    )

    all_public_ips = [outputs["master"]["public_ip"]]
    all_public_ips.extend(worker["public_ip"] for worker in outputs["workers"])
    wait_for_ssh(all_public_ips, private_key, known_hosts, insecure)

    if not args.skip_start:
        stop_spark(outputs, private_key, known_hosts, insecure)

    run_ansible(inventory_path, private_key, known_hosts, insecure)
    upload_config_if_requested(
        redeploy_config_file,
        migration_type=migration_type,
        master_public_ip=outputs["master"]["public_ip"],
        private_key=private_key,
        known_hosts=known_hosts,
        insecure=insecure,
    )
    remember_config_file(
        state_dir,
        metadata,
        config_file=redeploy_config_file,
        migration_type=migration_type,
    )

    if args.skip_start:
        return

    start_spark(outputs, private_key, known_hosts, insecure)
    wait_for_spark_master(outputs, private_key, known_hosts, insecure)
    wait_for_spark_workers(outputs, private_key, known_hosts, insecure)


def handle_destroy(args: argparse.Namespace) -> None:
    state_dir = resolve_state_dir(args.state_dir)
    require_terraform_state(state_dir)
    require_commands(["terraform"])

    if not args.yes:
        answer = input(f"Destroy Spark cluster managed in {state_dir}? Type 'yes': ")
        if answer != "yes":
            raise SystemExit("Destroy cancelled.")

    try:
        run_command(
            ["terraform", "destroy", "-auto-approve"],
            cwd=state_dir,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        print("Terraform destroy failed.", file=sys.stderr)
        if exc.stdout:
            print("Terraform stdout:", file=sys.stderr)
            print(exc.stdout, file=sys.stderr)
        if exc.stderr:
            print("Terraform stderr:", file=sys.stderr)
            print(exc.stderr, file=sys.stderr)
        raise SystemExit(exc.returncode) from exc

    if args.delete_state_dir:
        shutil.rmtree(state_dir)
        print(f"Deleted {state_dir}")


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--state-dir",
        default=DEFAULT_STATE_DIR,
        help="Directory for generated Terraform files, state, inventory, and metadata.",
    )

    parser = argparse.ArgumentParser(
        description="Deploy and operate a Spark cluster for ScyllaDB Migrator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    deploy = subparsers.add_parser(
        "deploy",
        parents=[common],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Create AWS infrastructure and configure Spark with Ansible.",
    )
    deploy.add_argument("--cloud-provider", choices=["aws"], default="aws")
    deploy.add_argument("--region", default="us-east-1")
    deploy.add_argument("--name-prefix", default="scylla-migrator-spark")
    deploy.add_argument("--key-name", default=None, help="AWS key pair name to create.")
    deploy.add_argument("--ssh-private-key", default="~/.ssh/id_rsa")
    deploy.add_argument("--ssh-public-key", default=None)
    deploy.add_argument("--master-instance-type", default="x2iedn.2xlarge")
    deploy.add_argument("--worker-instance-type", default="i8g.4xlarge")
    deploy.add_argument("--workers", type=positive_int, default=1)
    deploy.add_argument("--vpc-cidr", default="10.42.0.0/16")
    deploy.add_argument("--public-subnet-cidr", default="10.42.1.0/24")
    deploy.add_argument(
        "--vpc-id",
        default="",
        help=(
            "Existing AWS VPC ID to use instead of creating a new VPC. "
            "Must be provided together with --subnet-id."
        ),
    )
    deploy.add_argument(
        "--subnet-id",
        default="",
        help=(
            "Existing subnet ID for EC2 instances when --vpc-id is set. "
            "The subnet must have outbound internet access for package downloads."
        ),
    )
    deploy.add_argument(
        "--allowed-ssh-cidr",
        type=ipv4_cidr,
        required=True,
        help="IPv4 CIDR allowed to SSH to cluster nodes, for example YOUR_IP/32.",
    )
    deploy.add_argument(
        "--allowed-web-cidr",
        type=ipv4_cidr,
        required=True,
        help="IPv4 CIDR allowed to reach Spark web UIs, for example YOUR_IP/32.",
    )
    deploy.add_argument(
        "--allow-public-access",
        action="store_true",
        help="Allow 0.0.0.0/0 for SSH or Spark UI access when explicitly requested.",
    )
    deploy.add_argument("--root-volume-size-gb", type=positive_int, default=100)
    deploy.add_argument(
        "--iam-instance-profile",
        default="",
        help="Optional existing IAM instance profile for the EC2 instances.",
    )
    deploy.add_argument(
        "--owner-tag",
        default="",
        help="Optional Owner tag value to apply to the Spark master and worker EC2 instances.",
    )
    deploy.add_argument("--migration-type", choices=["cql", "alternator"], default="cql")
    deploy.add_argument(
        "--config-file",
        default=None,
        help="Optional Migrator config to upload to the Spark master.",
    )
    deploy.add_argument("--skip-ansible", action="store_true")
    deploy.add_argument("--skip-start", action="store_true")
    deploy.add_argument(
        "--insecure-ssh",
        action="store_true",
        help=(
            "Disable SSH host key verification. This restores the previous "
            "behavior and should only be used in trusted test environments."
        ),
    )
    deploy.set_defaults(func=handle_deploy)

    show = subparsers.add_parser(
        "show",
        parents=[common],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Show Terraform-managed infrastructure details.",
    )
    show.add_argument("--json", action="store_true")
    show.set_defaults(func=handle_show)

    run = subparsers.add_parser(
        "run",
        parents=[common],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Run the configured Migrator Spark job on the master node.",
    )
    run.add_argument("--ssh-private-key", default=None)
    run.add_argument("--migration-type", choices=["cql", "alternator"], default=None)
    run.add_argument("--config-file", default=None)
    run.add_argument("--validator", action="store_true")
    run.add_argument(
        "--insecure-ssh",
        action="store_true",
        help=(
            "Disable SSH host key verification. This should only be used in "
            "trusted test environments."
        ),
    )
    run.set_defaults(func=handle_run)

    redeploy = subparsers.add_parser(
        "redeploy",
        parents=[common],
        description="Rerun Ansible on the current Terraform-managed nodes.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Rerun Ansible on the current Terraform-managed nodes.",
    )
    redeploy.add_argument("--ssh-private-key", default=None)
    redeploy.add_argument("--migration-type", choices=["cql", "alternator"], default=None)
    redeploy.add_argument(
        "--config-file",
        default=None,
        help=(
            "Optional Migrator config to upload after rerunning Ansible. "
            "Defaults to the config file saved in deployment metadata."
        ),
    )
    redeploy.add_argument(
        "--skip-start",
        action="store_true",
        help="Do not stop or restart Spark systemd services around the Ansible run.",
    )
    redeploy.add_argument(
        "--insecure-ssh",
        action="store_true",
        help=(
            "Disable SSH host key verification. This should only be used in "
            "trusted test environments."
        ),
    )
    redeploy.set_defaults(func=handle_redeploy)

    destroy = subparsers.add_parser(
        "destroy",
        parents=[common],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Destroy Terraform-managed infrastructure.",
    )
    destroy.add_argument("--yes", action="store_true", help="Skip confirmation prompt.")
    destroy.add_argument(
        "--delete-state-dir",
        action="store_true",
        help="Delete the local state directory after Terraform destroy succeeds.",
    )
    destroy.set_defaults(func=handle_destroy)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except subprocess.CalledProcessError as exc:
        print_command_error(exc)
        return exc.returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
