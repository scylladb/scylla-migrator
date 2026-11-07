#!/usr/bin/env python3
"""Provision and operate a standalone Spark cluster for ScyllaDB Migrator.

This script owns the cloud infrastructure lifecycle with Terraform and uses
the existing Ansible playbook in ./ansible to install Spark and Migrator on the
created AWS EC2 or Google Compute Engine instances.
"""

from __future__ import annotations

import sys

if sys.version_info < (3, 10):
    raise SystemExit(
        "deploy_spark_cluster.py requires Python 3.10 or later. "
        f"Current version: {sys.version.split()[0]}"
    )

import argparse
import base64
import binascii
import ipaddress
import json
import os
import re
import shlex
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STATE_DIR = ".deploy_spark_cluster"
DEFAULT_USER = "ubuntu"
STATE_DIR_MARKER = ".scylla-migrator-state"
REMOTE_MIGRATOR_DIR = "/home/ubuntu/scylla-migrator"
MIGRATION_TYPES = ("cql", "alternator")
CLOUD_PROVIDERS = ("aws", "gcp")
AWS_DEFAULT_REGION = "us-east-1"
AWS_DEFAULT_MASTER_INSTANCE_TYPE = "x2iedn.2xlarge"
AWS_DEFAULT_WORKER_INSTANCE_TYPE = "i8g.4xlarge"
GCP_DEFAULT_REGION = "us-central1"
GCP_DEFAULT_ZONE = "us-central1-a"
GCP_DEFAULT_MASTER_INSTANCE_TYPE = "n2-custom-8-262144-ext"
GCP_DEFAULT_WORKER_INSTANCE_TYPE = "c4a-highmem-16"
# Regions that do not have an "-a" zone.
GCP_REGION_DEFAULT_ZONES = {
    "europe-west1": "europe-west1-b",
    "us-east1": "us-east1-b",
}
GCP_MAX_SUBNET_PREFIX_LENGTH = 29
# Compute Engine reserves the network, gateway, second-to-last, and broadcast
# addresses in each subnet range.
GCP_SUBNET_RESERVED_ADDRESSES = 4
# The Google provider prefers these variables over GOOGLE_APPLICATION_CREDENTIALS,
# so they are removed when an explicit service account file is selected.
GCP_COMPETING_CREDENTIAL_ENV_VARS = (
    "GOOGLE_CREDENTIALS",
    "GOOGLE_OAUTH_ACCESS_TOKEN",
    "GOOGLE_CLOUD_KEYFILE_JSON",
    "GCLOUD_KEYFILE_JSON",
    "GOOGLE_IMPERSONATE_SERVICE_ACCOUNT",
)
AWS_DEFAULT_VPC_CIDR = "10.42.0.0/16"
AWS_MAX_SUBNET_PREFIX_LENGTH = 28
# AWS reserves the first four addresses and the last address in each subnet.
AWS_SUBNET_RESERVED_ADDRESSES = 5
DEFAULT_SUBNET_CIDR = "10.42.1.0/24"
SSH_PUBLIC_KEY_TYPES = (
    "ssh-ed25519",
    "ssh-rsa",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
    "sk-ssh-ed25519@openssh.com",
    "sk-ecdsa-sha2-nistp256@openssh.com",
)
# EC2 key pairs accept only RSA and ED25519 public keys.
AWS_SSH_PUBLIC_KEY_TYPES = ("ssh-ed25519", "ssh-rsa")
SSH_ECDSA_COORDINATE_LENGTHS = {"nistp256": 32, "nistp384": 48, "nistp521": 66}
# OpenSSH rejects smaller RSA keys by default (RequiredRSASize).
SSH_MIN_RSA_BITS = 1024
DER_INTEGER = 0x02
DER_OCTET_STRING = 0x04
DER_OBJECT_IDENTIFIER = 0x06
DER_SEQUENCE = 0x30
RSA_ENCRYPTION_OID = bytes.fromhex("2a864886f70d010101")


AWS_TERRAFORM_MAIN = """terraform {
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

variable "ssh_public_key" {
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
  public_key = var.ssh_public_key

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
  description = "Shared Spark cluster access for ScyllaDB Migrator"
  vpc_id      = local.vpc_id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
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

resource "aws_security_group" "spark_master_ui" {
  name        = "${var.name_prefix}-master-ui-sg"
  description = "Spark master web UI access for ScyllaDB Migrator"
  vpc_id      = local.vpc_id

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

  tags = {
    Name = "${var.name_prefix}-master-ui-sg"
  }
}

resource "aws_instance" "spark_master" {
  ami                         = data.aws_ami.ubuntu_master.id
  instance_type               = var.master_instance_type
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.spark.id, aws_security_group.spark_master_ui.id]
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

  lifecycle {
    # New AMIs are published regularly; do not replace running nodes on re-deploy.
    ignore_changes = [ami]
  }
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

  lifecycle {
    # New AMIs are published regularly; do not replace running nodes on re-deploy.
    ignore_changes = [ami]
  }
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

output "cluster_security_group_id" {
  value = aws_security_group.spark.id
}

output "master_ui_security_group_id" {
  value = aws_security_group.spark_master_ui.id
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


GCP_TERRAFORM_MAIN = """terraform {
  required_version = ">= 1.3.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "zone" {
  type = string
}

variable "name_prefix" {
  type = string
}

variable "ssh_public_key" {
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

variable "master_image_family" {
  type = string
}

variable "worker_image_family" {
  type = string
}

variable "master_boot_disk_type" {
  type = string
}

variable "worker_boot_disk_type" {
  type = string
}

variable "public_subnet_cidr" {
  type = string
}

variable "existing_network" {
  type    = string
  default = ""
}

variable "existing_subnetwork" {
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

variable "instance_service_account" {
  type    = string
  default = ""
}

variable "owner_tag" {
  type    = string
  default = ""
}

data "google_compute_network" "existing" {
  count   = var.existing_network == "" ? 0 : 1
  name    = var.existing_network
  project = var.project_id
}

data "google_compute_subnetwork" "existing" {
  count   = var.existing_subnetwork == "" ? 0 : 1
  name    = var.existing_subnetwork
  project = var.project_id
  region  = var.region
}

data "google_compute_image" "ubuntu_master" {
  family  = var.master_image_family
  project = "ubuntu-os-cloud"
}

data "google_compute_image" "ubuntu_worker" {
  family  = var.worker_image_family
  project = "ubuntu-os-cloud"
}

locals {
  use_existing_network = var.existing_network != ""
  network_id           = local.use_existing_network ? data.google_compute_network.existing[0].self_link : google_compute_network.spark[0].self_link
  subnetwork_id        = local.use_existing_network ? data.google_compute_subnetwork.existing[0].self_link : google_compute_subnetwork.public[0].self_link
  cluster_tag          = "${var.name_prefix}-cluster"
  master_tag           = "${var.name_prefix}-master"
  owner_labels         = var.owner_tag == "" ? {} : { owner = var.owner_tag }
  ssh_metadata = {
    block-project-ssh-keys = "true"
    enable-oslogin         = "FALSE"
    ssh-keys               = "ubuntu:${var.ssh_public_key}"
  }
}

resource "google_compute_network" "spark" {
  count                   = local.use_existing_network ? 0 : 1
  name                    = "${var.name_prefix}-vpc"
  project                 = var.project_id
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

resource "google_compute_subnetwork" "public" {
  count                    = local.use_existing_network ? 0 : 1
  name                     = "${var.name_prefix}-public-subnet"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.spark[0].id
  ip_cidr_range            = var.public_subnet_cidr
  private_ip_google_access = true
}

resource "google_compute_firewall" "spark_internal" {
  name        = "${var.name_prefix}-internal"
  project     = var.project_id
  network     = local.network_id
  direction   = "INGRESS"
  source_tags = [local.cluster_tag]
  target_tags = [local.cluster_tag]

  allow {
    protocol = "all"
  }

  lifecycle {
    precondition {
      condition = local.use_existing_network ? (
        data.google_compute_subnetwork.existing[0].network == data.google_compute_network.existing[0].self_link
      ) : true
      error_message = "The selected GCP subnetwork does not belong to the selected network."
    }
  }
}

resource "google_compute_firewall" "spark_ssh" {
  name          = "${var.name_prefix}-ssh"
  project       = var.project_id
  network       = local.network_id
  direction     = "INGRESS"
  source_ranges = [var.allowed_ssh_cidr]
  target_tags   = [local.cluster_tag]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "spark_master_ui" {
  name          = "${var.name_prefix}-master-ui"
  project       = var.project_id
  network       = local.network_id
  direction     = "INGRESS"
  source_ranges = [var.allowed_web_cidr]
  target_tags   = [local.master_tag]

  allow {
    protocol = "tcp"
    ports    = ["4040", "8080", "18080"]
  }
}

resource "google_compute_instance" "spark_master" {
  name                      = "${var.name_prefix}-master"
  project                   = var.project_id
  zone                      = var.zone
  machine_type              = var.master_instance_type
  allow_stopping_for_update = true
  tags                      = [local.cluster_tag, local.master_tag]
  labels                    = merge({ role = "spark-master" }, local.owner_labels)
  metadata                  = local.ssh_metadata

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu_master.self_link
      size  = var.root_volume_size_gb
      type  = var.master_boot_disk_type
    }
  }

  network_interface {
    subnetwork = local.subnetwork_id

    access_config {
      network_tier = "PREMIUM"
    }
  }

  dynamic "service_account" {
    for_each = var.instance_service_account == "" ? [] : [var.instance_service_account]

    content {
      email  = service_account.value
      scopes = ["cloud-platform"]
    }
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = true
    enable_vtpm                 = true
  }

  lifecycle {
    # New images are published regularly; do not replace running nodes on re-deploy.
    ignore_changes = [boot_disk[0].initialize_params[0].image]
  }
}

resource "google_compute_instance" "spark_worker" {
  count                     = var.worker_count
  name                      = "${var.name_prefix}-worker-${count.index + 1}"
  project                   = var.project_id
  zone                      = var.zone
  machine_type              = var.worker_instance_type
  allow_stopping_for_update = true
  tags                      = [local.cluster_tag]
  labels                    = merge({ role = "spark-worker" }, local.owner_labels)
  metadata                  = local.ssh_metadata

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu_worker.self_link
      size  = var.root_volume_size_gb
      type  = var.worker_boot_disk_type
    }
  }

  network_interface {
    subnetwork = local.subnetwork_id

    access_config {
      network_tier = "PREMIUM"
    }
  }

  dynamic "service_account" {
    for_each = var.instance_service_account == "" ? [] : [var.instance_service_account]

    content {
      email  = service_account.value
      scopes = ["cloud-platform"]
    }
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = true
    enable_vtpm                 = true
  }

  lifecycle {
    # New images are published regularly; do not replace running nodes on re-deploy.
    ignore_changes = [boot_disk[0].initialize_params[0].image]
  }
}

output "region" {
  value = var.region
}

output "vpc_id" {
  value = local.network_id
}

output "public_subnet_id" {
  value = local.subnetwork_id
}

output "cluster_security_group_id" {
  value = google_compute_firewall.spark_internal.id
}

output "ssh_firewall_id" {
  value = google_compute_firewall.spark_ssh.id
}

output "master_ui_security_group_id" {
  value = google_compute_firewall.spark_master_ui.id
}

output "key_name" {
  value = "ubuntu (instance metadata)"
}

output "master" {
  value = {
    name        = "spark_master"
    instance_id = google_compute_instance.spark_master.instance_id
    public_ip   = google_compute_instance.spark_master.network_interface[0].access_config[0].nat_ip
    private_ip  = google_compute_instance.spark_master.network_interface[0].network_ip
  }
}

output "workers" {
  value = [
    for idx, worker in google_compute_instance.spark_worker : {
      name        = "spark_worker${idx + 1}"
      instance_id = worker.instance_id
      public_ip   = worker.network_interface[0].access_config[0].nat_ip
      private_ip  = worker.network_interface[0].network_ip
    }
  ]
}

output "spark_master_url" {
  value = "spark://${google_compute_instance.spark_master.network_interface[0].network_ip}:7077"
}

output "spark_master_ui" {
  value = "http://${google_compute_instance.spark_master.network_interface[0].access_config[0].nat_ip}:8080"
}

output "spark_application_ui" {
  value = "http://${google_compute_instance.spark_master.network_interface[0].access_config[0].nat_ip}:4040"
}

output "spark_history_ui" {
  value = "http://${google_compute_instance.spark_master.network_interface[0].access_config[0].nat_ip}:18080"
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
    env: dict[str, str | None] | None = None,
) -> subprocess.CompletedProcess[str]:
    print(f"+ {shlex.join(args)}", file=sys.stderr)
    process_env = os.environ.copy()
    for key, value in (env or {}).items():
        if value is None:
            process_env.pop(key, None)
        else:
            process_env[key] = value
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


def validate_ip_output(value: Any, label: str) -> None:
    if not isinstance(value, str) or value == "":
        raise SystemExit(f"Terraform output {label} must be a non-empty IP address.")
    try:
        ipaddress.ip_address(value)
    except ValueError as exc:
        raise SystemExit(f"Terraform output {label} is not a valid IP address: {value}") from exc


def validate_terraform_output_ips(outputs: dict[str, Any]) -> None:
    master = outputs.get("master")
    if not isinstance(master, dict):
        raise SystemExit("Terraform output master must be an object.")

    for field in ("public_ip", "private_ip"):
        validate_ip_output(master.get(field), f"master.{field}")

    workers = outputs.get("workers")
    if not isinstance(workers, list):
        raise SystemExit("Terraform output workers must be a list.")

    for index, worker in enumerate(workers, start=1):
        if not isinstance(worker, dict):
            raise SystemExit(f"Terraform output workers[{index}] must be an object.")
        for field in ("public_ip", "private_ip"):
            validate_ip_output(worker.get(field), f"workers[{index}].{field}")


def parse_terraform_output_json(stdout: str) -> dict[str, Any]:
    try:
        raw_outputs = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON from terraform output -json: {exc}") from exc

    if not isinstance(raw_outputs, dict):
        raise SystemExit("Unexpected Terraform output JSON schema: expected an object.")

    outputs: dict[str, Any] = {}
    for key, output in raw_outputs.items():
        if not isinstance(output, dict) or "value" not in output:
            raise SystemExit(
                "Unexpected Terraform output JSON schema: "
                f"output {key!r} must be an object with a value field."
            )
        outputs[key] = output["value"]
    return outputs


def terraform_output(
    state_dir: Path,
    *,
    env: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    completed = run_command(
        ["terraform", "output", "-json"],
        cwd=state_dir,
        capture_output=True,
        env=env,
    )
    outputs = parse_terraform_output_json(completed.stdout)
    validate_terraform_output_ips(outputs)
    return outputs


def write_json(path: Path, content: dict[str, Any]) -> None:
    path.write_text(json.dumps(content, indent=2, sort_keys=True) + "\n")
    path.chmod(0o600)


def write_state_dir_marker(state_dir: Path) -> None:
    marker = state_dir / STATE_DIR_MARKER
    marker.write_text("generated by deploy_spark_cluster.py\n")
    marker.chmod(0o600)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc


def terraform_state_has_resources(state_dir: Path) -> bool:
    """Return whether local Terraform state may still track managed resources."""
    terraform_state = state_dir / "terraform.tfstate"
    if not terraform_state.is_file():
        return False
    try:
        state = json.loads(terraform_state.read_text())
    except (OSError, UnicodeError, json.JSONDecodeError):
        # Be conservative: unreadable state may still describe live infrastructure.
        return True
    if not isinstance(state, dict):
        return True
    resources = state.get("resources") or []
    if not isinstance(resources, list):
        return True
    return any(
        not isinstance(resource, dict) or resource.get("mode", "managed") == "managed"
        for resource in resources
    )


def gcp_default_zone(region: str) -> str:
    return GCP_REGION_DEFAULT_ZONES.get(region, f"{region}-a")


def apply_cloud_defaults(args: argparse.Namespace) -> None:
    if args.cloud_provider == "aws":
        args.region = args.region or AWS_DEFAULT_REGION
        args.vpc_cidr = args.vpc_cidr or AWS_DEFAULT_VPC_CIDR
        args.master_instance_type = (
            args.master_instance_type or AWS_DEFAULT_MASTER_INSTANCE_TYPE
        )
        args.worker_instance_type = (
            args.worker_instance_type or AWS_DEFAULT_WORKER_INSTANCE_TYPE
        )
        return

    if args.zone and not args.region:
        args.region = args.zone.rsplit("-", 1)[0]
    args.region = args.region or GCP_DEFAULT_REGION
    args.zone = args.zone or gcp_default_zone(args.region)
    args.master_instance_type = (
        args.master_instance_type or GCP_DEFAULT_MASTER_INSTANCE_TYPE
    )
    args.worker_instance_type = (
        args.worker_instance_type or GCP_DEFAULT_WORKER_INSTANCE_TYPE
    )


def read_der_element(data: bytes, offset: int = 0) -> tuple[int, bytes, int]:
    """Read one DER tag-length-value element, returning (tag, value, end offset)."""
    if offset + 2 > len(data):
        raise ValueError("truncated DER element")
    tag = data[offset]
    length = data[offset + 1]
    offset += 2
    if length & 0x80:
        length_size = length & 0x7F
        if length_size == 0 or length_size > 4 or offset + length_size > len(data):
            raise ValueError("invalid DER length")
        length = int.from_bytes(data[offset:offset + length_size], "big")
        offset += length_size
    if offset + length > len(data):
        raise ValueError("truncated DER element")
    return tag, data[offset:offset + length], offset + length


def read_der_sequence(data: bytes) -> list[tuple[int, bytes]]:
    elements = []
    offset = 0
    while offset < len(data):
        tag, value, offset = read_der_element(data, offset)
        elements.append((tag, value))
    return elements


def read_single_der_element(data: bytes, expected_tag: int) -> bytes:
    tag, value, end = read_der_element(data)
    if tag != expected_tag or end != len(data):
        raise ValueError("unexpected DER structure")
    return value


def validate_rsa_private_key_pem(pem: str) -> None:
    """Check that a PEM value holds a structurally valid RSA private key.

    This mirrors what Google's Go auth library accepts for service account keys:
    a PKCS#8 or PKCS#1 RSA private key.
    """
    match = re.fullmatch(
        r"\s*-----BEGIN (?P<label>RSA PRIVATE KEY|PRIVATE KEY)-----"
        r"(?P<body>[A-Za-z0-9+/=\s]+)"
        r"-----END (?P=label)-----\s*",
        pem,
    )
    if match is None:
        raise ValueError("expected a PEM-encoded RSA private key")
    key = read_single_der_element(
        base64.b64decode("".join(match["body"].split()), validate=True),
        DER_SEQUENCE,
    )

    if match["label"] == "PRIVATE KEY":
        # PKCS#8 PrivateKeyInfo: version, algorithm, and the wrapped key.
        elements = read_der_sequence(key)
        if (
            len(elements) < 3
            or elements[0] != (DER_INTEGER, b"\x00")
            or elements[1][0] != DER_SEQUENCE
            or elements[2][0] != DER_OCTET_STRING
        ):
            raise ValueError("unexpected PKCS#8 private key structure")
        algorithm = read_der_sequence(elements[1][1])
        if not algorithm or algorithm[0] != (DER_OBJECT_IDENTIFIER, RSA_ENCRYPTION_OID):
            raise ValueError("private key is not an RSA key")
        key = read_single_der_element(elements[2][1], DER_SEQUENCE)

    # PKCS#1 RSAPrivateKey: version followed by eight integers.
    elements = read_der_sequence(key)
    if (
        len(elements) != 9
        or elements[0] != (DER_INTEGER, b"\x00")
        or any(tag != DER_INTEGER or not value for tag, value in elements)
    ):
        raise ValueError("unexpected RSA private key structure")


def validate_gcp_service_account_file(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"GCP service account file does not exist: {path}")
    if not path.is_file():
        raise SystemExit(f"GCP service account path is not a file: {path}")

    try:
        credentials = json.loads(path.read_text())
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Invalid JSON in GCP service account file {path}: {exc}") from exc

    if not isinstance(credentials, dict) or credentials.get("type") != "service_account":
        raise SystemExit(
            f"GCP credential file is not a service account key: {path}. "
            "Expected a JSON object with type='service_account'."
        )

    required_fields = ("project_id", "client_email", "private_key")
    missing = [
        field
        for field in required_fields
        if not isinstance(credentials.get(field), str) or not credentials[field]
    ]
    if missing:
        raise SystemExit(
            f"GCP service account file is missing required field(s): {', '.join(missing)}"
        )

    try:
        validate_rsa_private_key_pem(credentials["private_key"])
    except ValueError as exc:
        raise SystemExit(
            f"GCP service account file {path} has an invalid private_key: {exc}"
        ) from exc


def resolve_gcp_service_account_file(
    explicit_value: str | None,
    metadata: dict[str, Any] | None = None,
) -> Path | None:
    saved_value = metadata.get("gcp_service_account_file") if metadata else None
    selected_value = explicit_value if explicit_value is not None else saved_value
    credentials_file = resolve_path(selected_value)
    if credentials_file is None:
        return None
    validate_gcp_service_account_file(credentials_file)
    return credentials_file


def terraform_auth_env(
    cloud_provider: str,
    explicit_service_account_file: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, str | None] | None:
    if cloud_provider != "gcp":
        if explicit_service_account_file:
            raise SystemExit("--gcp-service-account-file can only be used with GCP.")
        return None

    credentials_file = resolve_gcp_service_account_file(
        explicit_service_account_file,
        metadata,
    )
    if credentials_file is None:
        return None
    # Point ADC at the explicit file and remove every variable that the Google
    # provider would otherwise use instead of ADC.
    env: dict[str, str | None] = {"GOOGLE_APPLICATION_CREDENTIALS": str(credentials_file)}
    env.update({name: None for name in GCP_COMPETING_CREDENTIAL_ENV_VARS})
    return env


def saved_deployment_terraform_env(
    args: argparse.Namespace,
    metadata: dict[str, Any],
) -> dict[str, str | None] | None:
    return terraform_auth_env(
        metadata.get("cloud_provider", "aws"),
        args.gcp_service_account_file,
        metadata,
    )


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


def infer_gcp_architecture(instance_type: str) -> str:
    family = instance_type.split("-", 1)[0].lower()
    if family in {"a4x", "c4a", "n4a", "t2a"}:
        return "arm64"
    return "x86_64"


def gcp_image_family(instance_type: str) -> str:
    architecture = infer_gcp_architecture(instance_type)
    if architecture == "arm64":
        return "ubuntu-2404-lts-arm64"
    return "ubuntu-2404-lts-amd64"


def gcp_boot_disk_type(instance_type: str) -> str:
    family = instance_type.split("-", 1)[0].lower()
    hyperdisk_only_families = {
        "a4x",
        "c3",
        "c3d",
        "c4",
        "c4a",
        "c4d",
        "c4n",
        "h3",
        "h4d",
        "m3",
        "m4",
        "m4n",
        "n4",
        "n4a",
        "n4d",
        "z3",
    }
    if family in hyperdisk_only_families:
        return "hyperdisk-balanced"
    return "pd-balanced"


def known_hosts_path(state_dir: Path) -> Path:
    path = state_dir / "known_hosts"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    path.touch(exist_ok=True)
    path.chmod(0o600)
    return path


def quote_ssh_option_value(value: Path) -> str:
    escaped_value = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped_value}"'


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
        f"UserKnownHostsFile={quote_ssh_option_value(known_hosts)}",
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
                last_error = (completed.stderr or completed.stdout or "").strip()
                message = f"Timed out waiting for SSH on {host}"
                if last_error:
                    message += f". Last SSH error:\n{last_error}"
                raise SystemExit(message)
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


def resolve_migration_type(
    explicit_value: str | None,
    metadata: dict[str, Any],
) -> str:
    saved_value = metadata.get("migration_type")
    migration_type = explicit_value if explicit_value is not None else saved_value or "cql"
    if migration_type not in MIGRATION_TYPES:
        source = "--migration-type" if explicit_value is not None else "metadata.json"
        allowed = ", ".join(MIGRATION_TYPES)
        raise SystemExit(
            f"Unsupported migration type from {source}: {migration_type!r}. "
            f"Expected one of: {allowed}."
        )
    return migration_type


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
    resolved_config_file = resolve_path(saved_config_file)
    if resolved_config_file is None or not resolved_config_file.exists():
        return None
    return resolved_config_file


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


def resolve_ssh_private_key(
    explicit_value: str | None,
    metadata: dict[str, Any] | None = None,
) -> Path:
    saved_value = metadata.get("ssh_private_key") if metadata else None
    selected_value = explicit_value if explicit_value is not None else saved_value
    if selected_value is None or selected_value == "":
        raise SystemExit("SSH private key is required. Pass --ssh-private-key.")

    private_key = resolve_path(selected_value)
    if private_key is None or not private_key.exists():
        raise SystemExit(f"SSH private key does not exist: {private_key}")
    if not private_key.is_file():
        raise SystemExit(f"SSH private key path is not a file: {private_key}")
    return private_key


def resolve_ssh_public_key(args: argparse.Namespace) -> Path:
    public_key = resolve_path(args.ssh_public_key)
    private_key = resolve_path(args.ssh_private_key)
    if public_key is None:
        if private_key is None:
            raise SystemExit("Either --ssh-public-key or --ssh-private-key is required")
        public_key = Path(f"{private_key}.pub")

    if not public_key.exists():
        raise SystemExit(f"SSH public key does not exist: {public_key}")
    if not public_key.is_file():
        raise SystemExit(f"SSH public key path is not a file: {public_key}")
    return public_key


def read_ssh_wire_fields(blob: bytes) -> list[bytes]:
    """Split SSH wire-format data into its length-prefixed fields."""
    fields = []
    offset = 0
    while offset < len(blob):
        if offset + 4 > len(blob):
            raise ValueError("truncated field length")
        length = int.from_bytes(blob[offset:offset + 4], "big")
        offset += 4
        if offset + length > len(blob):
            raise ValueError("truncated field")
        fields.append(blob[offset:offset + length])
        offset += length
    return fields


def ssh_public_key_values_are_valid(key_type: str, values: list[bytes]) -> bool:
    """Check the fields that follow the key type in a supported public key blob."""
    if key_type == "ssh-ed25519":
        return len(values) == 1 and len(values[0]) == 32
    if key_type == "sk-ssh-ed25519@openssh.com":
        return len(values) == 2 and len(values[0]) == 32 and values[1].startswith(b"ssh:")
    if key_type == "ssh-rsa":
        return len(values) == 2 and all(values)

    # ECDSA keys hold the curve name and an uncompressed curve point.
    is_security_key = key_type.startswith("sk-")
    curve = key_type.removeprefix("sk-").removeprefix("ecdsa-sha2-").removesuffix("@openssh.com")
    coordinate_length = SSH_ECDSA_COORDINATE_LENGTHS[curve]
    return (
        len(values) == (3 if is_security_key else 2)
        and values[0] == curve.encode()
        and len(values[1]) == 1 + 2 * coordinate_length
        and values[1][0] == 0x04
        and (not is_security_key or values[2].startswith(b"ssh:"))
    )


def ssh_public_key_problem(key: str, allowed_types: tuple[str, ...]) -> str | None:
    """Return why an OpenSSH public key line is unusable, or None if it is valid."""
    fields = key.split()
    if len(fields) < 2:
        return "expected '<type> <base64 key data> [comment]'"
    key_type, encoded_key = fields[0], fields[1]
    if key_type not in allowed_types:
        return f"unsupported key type {key_type!r}; supported types: {', '.join(allowed_types)}"

    try:
        blob_fields = read_ssh_wire_fields(base64.b64decode(encoded_key, validate=True))
    except (binascii.Error, ValueError):
        return "key data is not valid base64-encoded SSH public key data"
    if not blob_fields or blob_fields[0] != key_type.encode():
        return f"key data does not match key type {key_type!r}"
    if not ssh_public_key_values_are_valid(key_type, blob_fields[1:]):
        return f"malformed {key_type} key data"
    if key_type == "ssh-rsa":
        modulus_bits = int.from_bytes(blob_fields[2], "big").bit_length()
        if modulus_bits < SSH_MIN_RSA_BITS:
            return f"RSA key is {modulus_bits} bits; at least {SSH_MIN_RSA_BITS} bits are required"
    return None


def read_ssh_public_key(public_key: Path, cloud_provider: str) -> str:
    """Read and validate a single OpenSSH public key.

    The contents, rather than the path, are passed to Terraform so that later
    destroys do not depend on the local key file still existing.
    """
    try:
        lines = [line.strip() for line in public_key.read_text().splitlines()]
    except (OSError, UnicodeError) as exc:
        raise SystemExit(f"Unable to read SSH public key {public_key}: {exc}") from exc

    keys = [line for line in lines if line]
    if len(keys) != 1:
        raise SystemExit(
            f"SSH public key file must contain exactly one OpenSSH public key: {public_key}. "
            "Check that --ssh-public-key does not point to a private key."
        )

    allowed_types = AWS_SSH_PUBLIC_KEY_TYPES if cloud_provider == "aws" else SSH_PUBLIC_KEY_TYPES
    problem = ssh_public_key_problem(keys[0], allowed_types)
    if problem:
        raise SystemExit(f"Invalid SSH public key in {public_key}: {problem}.")
    return keys[0]


def aws_terraform_vars(args: argparse.Namespace, public_key: str) -> dict[str, Any]:
    return {
        "region": args.region,
        "name_prefix": args.name_prefix,
        "key_name": args.key_name or f"{args.name_prefix}-key",
        "ssh_public_key": public_key,
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


def gcp_terraform_vars(args: argparse.Namespace, public_key: str) -> dict[str, Any]:
    return {
        "project_id": args.gcp_project,
        "region": args.region,
        "zone": args.zone,
        "name_prefix": args.name_prefix,
        "ssh_public_key": public_key,
        "master_instance_type": args.master_instance_type,
        "worker_instance_type": args.worker_instance_type,
        "worker_count": args.workers,
        "master_image_family": gcp_image_family(args.master_instance_type),
        "worker_image_family": gcp_image_family(args.worker_instance_type),
        "master_boot_disk_type": (
            args.gcp_master_boot_disk_type
            or gcp_boot_disk_type(args.master_instance_type)
        ),
        "worker_boot_disk_type": (
            args.gcp_worker_boot_disk_type
            or gcp_boot_disk_type(args.worker_instance_type)
        ),
        "public_subnet_cidr": args.public_subnet_cidr,
        "existing_network": args.network or "",
        "existing_subnetwork": args.subnetwork or "",
        "allowed_ssh_cidr": args.allowed_ssh_cidr,
        "allowed_web_cidr": args.allowed_web_cidr,
        "root_volume_size_gb": args.root_volume_size_gb,
        "instance_service_account": args.gcp_instance_service_account or "",
        "owner_tag": args.owner_tag or "",
    }


def write_terraform_files(args: argparse.Namespace, state_dir: Path) -> None:
    apply_cloud_defaults(args)
    public_key = read_ssh_public_key(resolve_ssh_public_key(args), args.cloud_provider)
    state_dir.mkdir(parents=True, exist_ok=True)
    state_dir.chmod(0o700)
    write_state_dir_marker(state_dir)

    if args.cloud_provider == "aws":
        terraform_main = AWS_TERRAFORM_MAIN
        tfvars = aws_terraform_vars(args, public_key)
    else:
        terraform_main = GCP_TERRAFORM_MAIN
        tfvars = gcp_terraform_vars(args, public_key)

    (state_dir / "main.tf").write_text(terraform_main)
    write_json(state_dir / "terraform.tfvars.json", tfvars)


def write_ansible_inventory(
    outputs: dict[str, Any],
    *,
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
            f"ansible_user={DEFAULT_USER}"
        ),
    ]

    for index, worker in enumerate(workers, start=1):
        lines.append(
            f"spark_worker{index} "
            f"ansible_host={worker['public_ip']} "
            f"ansible_user={DEFAULT_USER}"
        )

    lines.extend(["", "[master]", "spark_master", "", "[worker]"])
    for index, _worker in enumerate(workers, start=1):
        lines.append(f"spark_worker{index}")

    inventory_path.write_text("\n".join(lines) + "\n")
    return inventory_path


def ansible_ssh_common_args(known_hosts: Path, insecure: bool) -> str:
    if insecure:
        return shlex.join(
            [
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
            ]
        )

    return shlex.join(
        [
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            f"UserKnownHostsFile={quote_ssh_option_value(known_hosts)}",
        ]
    )


def run_ansible(
    inventory_path: Path,
    private_key: Path,
    known_hosts: Path,
    insecure: bool,
) -> None:
    ansible_dir = repo_root() / "ansible"
    if insecure:
        host_key_checking = "False"
    else:
        host_key_checking = "True"
    ssh_common_args = ansible_ssh_common_args(known_hosts, insecure)

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
) -> int | None:
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
        return None
    try:
        return int(completed.stdout.strip())
    except ValueError:
        return None


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
        if count is not None and count >= expected_workers:
            return
        time.sleep(5)

    count = registered_worker_count(outputs, private_key, known_hosts, insecure)
    registered = "unknown" if count is None else str(count)
    raise SystemExit(
        "Timed out waiting for Spark workers to register "
        f"({registered}/{expected_workers} registered)"
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
        if current_workers is None:
            print(
                "Spark master is reachable, but the worker registration count "
                "could not be read; starting workers..."
            )
            start_workers(outputs, private_key, known_hosts, insecure)
        elif current_workers == 0 and expected_workers > 0:
            print(
                "Spark master is reachable, but zero live workers are registered; "
                "starting workers..."
            )
            start_workers(outputs, private_key, known_hosts, insecure)
        elif current_workers < expected_workers:
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
    private_key: Path | None,
    gcp_service_account_file: Path | None,
    outputs: dict[str, Any],
) -> None:
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "cloud_provider": args.cloud_provider,
        "region": args.region,
        "zone": args.zone or "",
        "name_prefix": args.name_prefix,
        "master_instance_type": args.master_instance_type,
        "worker_instance_type": args.worker_instance_type,
        "workers": args.workers,
        "owner_tag": args.owner_tag or "",
        "vpc_id": args.vpc_id or "",
        "subnet_id": args.subnet_id or "",
        "gcp_project": args.gcp_project or "",
        "network": args.network or "",
        "subnetwork": args.subnetwork or "",
        "gcp_service_account_file": (
            str(gcp_service_account_file) if gcp_service_account_file else ""
        ),
        "gcp_instance_service_account": args.gcp_instance_service_account or "",
        "migration_type": args.migration_type,
        "config_file": str(resolve_path(args.config_file)) if args.config_file else "",
        "ssh_private_key": str(private_key) if private_key is not None else "",
        "ssh_known_hosts": str(known_hosts_path(state_dir)),
        "state_dir": str(state_dir),
        "terraform_outputs": outputs,
    }
    write_json(state_dir / "metadata.json", metadata)


def load_metadata(state_dir: Path) -> dict[str, Any]:
    return read_json(state_dir / "metadata.json")


def validate_existing_deployment_provider(
    state_dir: Path,
    requested_provider: str,
) -> dict[str, Any]:
    metadata_path = state_dir / "metadata.json"
    if not terraform_state_has_resources(state_dir):
        # Nothing is deployed, or the previous deployment was destroyed without
        # --delete-state-dir, so saved metadata does not describe live resources.
        return {}

    metadata = read_json(metadata_path)
    existing_provider = metadata.get("cloud_provider", "aws")
    if existing_provider not in CLOUD_PROVIDERS:
        raise SystemExit(
            f"Unsupported cloud provider in {metadata_path}: {existing_provider!r}"
        )
    if existing_provider != requested_provider:
        raise SystemExit(
            f"State directory {state_dir} contains an existing "
            f"{existing_provider.upper()} deployment; refusing to replace it with "
            f"{requested_provider.upper()} configuration. Use a different --state-dir "
            "for the new cluster."
        )
    return metadata


def require_terraform_state(state_dir: Path) -> None:
    if not state_dir.is_dir():
        raise SystemExit(f"State directory does not exist: {state_dir}")
    terraform_state = state_dir / "terraform.tfstate"
    if not terraform_state.is_file():
        raise SystemExit(f"Terraform state file does not exist: {terraform_state}")


def validate_state_dir_safe_to_delete(state_dir: Path) -> None:
    state_dir = state_dir.resolve()
    repository = repo_root()
    filesystem_root = Path(state_dir.anchor).resolve()
    home = Path.home().resolve()

    if state_dir in {filesystem_root, home, repository} or repository.is_relative_to(state_dir):
        raise SystemExit(f"Refusing to delete unsafe state directory: {state_dir}")

    for required_file in (STATE_DIR_MARKER, "terraform.tfstate", "main.tf", "metadata.json"):
        path = state_dir / required_file
        if not path.is_file():
            raise SystemExit(
                f"Refusing to delete {state_dir}: required state file is missing: {path}"
            )

    allowed_entries = {
        STATE_DIR_MARKER,
        ".terraform",
        ".terraform.lock.hcl",
        "inventory.ini",
        "known_hosts",
        "main.tf",
        "metadata.json",
        "terraform.tfstate",
        "terraform.tfstate.backup",
        "terraform.tfvars.json",
    }
    unexpected_entries = sorted(
        entry.name for entry in state_dir.iterdir() if entry.name not in allowed_entries
    )
    if unexpected_entries:
        raise SystemExit(
            f"Refusing to delete {state_dir}: unexpected files are present: "
            + ", ".join(unexpected_entries)
        )

    metadata = read_json(state_dir / "metadata.json")
    recorded_state_dir = resolve_path(metadata.get("state_dir"))
    if recorded_state_dir != state_dir:
        raise SystemExit(
            "Refusing to delete "
            f"{state_dir}: metadata state_dir does not match this directory."
        )


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


def validate_generated_network_cidrs(args: argparse.Namespace) -> None:
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


def validate_generated_subnet_capacity(
    args: argparse.Namespace,
    *,
    provider_name: str,
    max_prefix_length: int,
    reserved_addresses: int,
) -> None:
    """Reject generated subnets that cannot hold the master and every worker."""
    subnet_network = ipaddress.ip_network(args.public_subnet_cidr, strict=False)
    if subnet_network.prefixlen > max_prefix_length:
        raise SystemExit(
            f"--public-subnet-cidr ({subnet_network}) is too small for the generated "
            f"{provider_name} subnet; use a /{max_prefix_length} or larger range."
        )

    required_addresses = args.workers + 1
    usable_addresses = subnet_network.num_addresses - reserved_addresses
    if usable_addresses < required_addresses:
        raise SystemExit(
            f"--public-subnet-cidr ({subnet_network}) has {usable_addresses} usable "
            f"addresses after the {reserved_addresses} that {provider_name} reserves, but "
            f"{required_addresses} are needed for the master and {args.workers} worker(s)."
        )


def validate_aws_network_args(args: argparse.Namespace) -> None:
    if bool(args.vpc_id) != bool(args.subnet_id):
        raise SystemExit("--vpc-id and --subnet-id must be provided together.")
    if args.vpc_id and args.subnet_id:
        return
    validate_generated_network_cidrs(args)
    validate_generated_subnet_capacity(
        args,
        provider_name="AWS",
        max_prefix_length=AWS_MAX_SUBNET_PREFIX_LENGTH,
        reserved_addresses=AWS_SUBNET_RESERVED_ADDRESSES,
    )


def validate_gcp_network_args(args: argparse.Namespace) -> None:
    if bool(args.network) != bool(args.subnetwork):
        raise SystemExit("--network and --subnetwork must be provided together.")
    if args.network and args.subnetwork:
        return
    try:
        subnet_network = ipaddress.ip_network(args.public_subnet_cidr, strict=False)
    except ValueError as exc:
        raise SystemExit(f"Invalid subnet CIDR: {exc}") from exc
    if subnet_network.version != 4:
        raise SystemExit("--public-subnet-cidr must be an IPv4 CIDR.")
    validate_generated_subnet_capacity(
        args,
        provider_name="GCP",
        max_prefix_length=GCP_MAX_SUBNET_PREFIX_LENGTH,
        reserved_addresses=GCP_SUBNET_RESERVED_ADDRESSES,
    )


def validate_gcp_args(args: argparse.Namespace) -> None:
    if not args.gcp_project:
        raise SystemExit("--gcp-project is required when --cloud-provider=gcp.")
    if args.vpc_id or args.subnet_id:
        raise SystemExit(
            "--vpc-id and --subnet-id are AWS-only; use --network and "
            "--subnetwork for an existing GCP network."
        )
    if args.vpc_cidr:
        raise SystemExit(
            "--vpc-cidr is AWS-only because GCP VPC networks do not have a "
            "network-wide CIDR; use --public-subnet-cidr for the GCP subnetwork."
        )
    if args.key_name:
        raise SystemExit("--key-name can only be used with AWS.")
    if args.iam_instance_profile:
        raise SystemExit("--iam-instance-profile can only be used with AWS.")

    if not re.fullmatch(r"[a-z]+(?:-[a-z0-9]+)+-[a-z]", args.zone):
        raise SystemExit(f"Invalid GCP zone: {args.zone!r}. Expected a zone such as us-central1-a.")
    if args.zone.rsplit("-", 1)[0] != args.region:
        raise SystemExit(
            f"GCP zone {args.zone!r} is not in the selected region {args.region!r}."
        )
    if len(args.name_prefix) > 49 or not re.fullmatch(
        r"[a-z](?:[-a-z0-9]*[a-z0-9])?",
        args.name_prefix,
    ):
        raise SystemExit(
            "For GCP, --name-prefix must be at most 49 characters and contain "
            "only lowercase letters, digits, and hyphens; it must start with a "
            "letter and end with a letter or digit."
        )
    if args.owner_tag and not re.fullmatch(
        r"[a-z0-9](?:[-_a-z0-9]*[a-z0-9])?",
        args.owner_tag,
    ):
        raise SystemExit(
            "For GCP, --owner-tag must contain only lowercase letters, digits, "
            "underscores, and hyphens, and must start and end with a letter or digit."
        )
    if len(args.owner_tag) > 63:
        raise SystemExit("For GCP, --owner-tag must be at most 63 characters.")
    validate_gcp_network_args(args)


def validate_cloud_args(args: argparse.Namespace) -> None:
    apply_cloud_defaults(args)
    if args.cloud_provider == "gcp":
        validate_gcp_args(args)
        return

    gcp_only_args = {
        "--gcp-project": args.gcp_project,
        "--zone": args.zone,
        "--network": args.network,
        "--subnetwork": args.subnetwork,
        "--gcp-instance-service-account": args.gcp_instance_service_account,
        "--gcp-master-boot-disk-type": args.gcp_master_boot_disk_type,
        "--gcp-worker-boot-disk-type": args.gcp_worker_boot_disk_type,
    }
    supplied = [flag for flag, value in gcp_only_args.items() if value]
    if supplied:
        raise SystemExit(f"{', '.join(supplied)} can only be used with GCP.")
    validate_aws_network_args(args)


def handle_deploy(args: argparse.Namespace) -> None:
    validate_access_cidrs(args)
    validate_cloud_args(args)

    state_dir = resolve_state_dir(args.state_dir)
    deploy_config_file = resolve_path(args.config_file)
    validate_local_config_file(deploy_config_file)
    has_live_deployment = terraform_state_has_resources(state_dir)
    existing_metadata = validate_existing_deployment_provider(
        state_dir,
        args.cloud_provider,
    )
    # Updating a live deployment reuses its saved GCP credentials unless
    # --gcp-service-account-file overrides them.
    terraform_env = terraform_auth_env(
        args.cloud_provider,
        args.gcp_service_account_file,
        existing_metadata,
    )
    gcp_service_account_file = (
        Path(terraform_env["GOOGLE_APPLICATION_CREDENTIALS"]) if terraform_env else None
    )

    private_key = None if args.skip_ansible else resolve_ssh_private_key(args.ssh_private_key)
    known_hosts = known_hosts_path(state_dir)
    if not has_live_deployment:
        # New instances may reuse public IPs from a destroyed cluster.
        known_hosts.write_text("")
        known_hosts.chmod(0o600)

    required = ["terraform"]
    if not args.skip_ansible:
        required.extend(["ansible-playbook", "ssh", "scp"])

    write_terraform_files(args, state_dir)
    require_commands(required)
    run_command(
        ["terraform", "init", "-input=false"],
        cwd=state_dir,
        env=terraform_env,
    )
    if not has_live_deployment:
        # Record the provider and credentials before the first apply so that a
        # partially created deployment can still be destroyed. A live deployment
        # keeps its last successful metadata until this apply succeeds.
        save_metadata(
            args,
            state_dir=state_dir,
            private_key=private_key,
            gcp_service_account_file=gcp_service_account_file,
            outputs={},
        )
    run_command(
        ["terraform", "apply", "-auto-approve"],
        cwd=state_dir,
        env=terraform_env,
    )
    outputs = terraform_output(state_dir, env=terraform_env)
    save_metadata(
        args,
        state_dir=state_dir,
        private_key=private_key,
        gcp_service_account_file=gcp_service_account_file,
        outputs=outputs,
    )

    if args.skip_ansible:
        print("Skipping Ansible configuration.")
        print_cluster_details(outputs, metadata=load_metadata(state_dir))
        return

    all_public_ips = [outputs["master"]["public_ip"]]
    all_public_ips.extend(worker["public_ip"] for worker in outputs["workers"])
    wait_for_ssh(all_public_ips, private_key, known_hosts, args.insecure_ssh)

    inventory_path = write_ansible_inventory(
        outputs,
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
    cloud_provider = metadata.get("cloud_provider", "aws") if metadata else "aws"

    print("")
    print("Spark cluster")
    if metadata:
        print(f"  Provider: {cloud_provider}")
        if cloud_provider == "gcp":
            print(f"  Project: {metadata.get('gcp_project', 'unknown')}")
        print(f"  Region: {metadata.get('region', outputs.get('region', 'unknown'))}")
        if cloud_provider == "gcp":
            print(f"  Zone: {metadata.get('zone', 'unknown')}")
        print(f"  Migration type: {metadata.get('migration_type', 'unknown')}")
    print(f"  Spark master: {outputs['spark_master_url']}")
    print(f"  Spark UI: {outputs['spark_master_ui']}")
    print(f"  Spark app UI: {outputs['spark_application_ui']}")
    print(f"  Spark history UI: {outputs['spark_history_ui']}")
    print("")
    print("Infrastructure")
    if cloud_provider == "gcp":
        print(f"  Network: {outputs['vpc_id']}")
        print(f"  Subnetwork: {outputs['public_subnet_id']}")
        print(f"  Cluster internal firewall: {outputs['cluster_security_group_id']}")
        if outputs.get("ssh_firewall_id"):
            print(f"  SSH firewall: {outputs['ssh_firewall_id']}")
        print(f"  Master UI firewall: {outputs['master_ui_security_group_id']}")
        print(f"  SSH user/key source: {outputs['key_name']}")
    else:
        print(f"  VPC: {outputs['vpc_id']}")
        print(f"  Public subnet: {outputs['public_subnet_id']}")
        print(f"  Cluster security group: {outputs['cluster_security_group_id']}")
        print(f"  Master UI security group: {outputs['master_ui_security_group_id']}")
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
    terraform_env = saved_deployment_terraform_env(args, metadata)
    outputs = terraform_output(state_dir, env=terraform_env)

    if args.json:
        print(json.dumps({"metadata": metadata, "terraform_outputs": outputs}, indent=2))
        return

    print_cluster_details(outputs, metadata=metadata)


def handle_run(args: argparse.Namespace) -> None:
    state_dir = resolve_state_dir(args.state_dir)
    require_terraform_state(state_dir)
    require_commands(["terraform", "ssh", "scp"])
    metadata = load_metadata(state_dir)
    migration_type = resolve_migration_type(args.migration_type, metadata)
    terraform_env = saved_deployment_terraform_env(args, metadata)
    outputs = terraform_output(state_dir, env=terraform_env)

    private_key = resolve_ssh_private_key(args.ssh_private_key, metadata)
    known_hosts = known_hosts_path(state_dir)
    insecure = args.insecure_ssh

    ensure_spark_running(outputs, private_key, known_hosts, insecure)

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
    migration_type = resolve_migration_type(args.migration_type, metadata)
    terraform_env = saved_deployment_terraform_env(args, metadata)

    private_key = resolve_ssh_private_key(args.ssh_private_key, metadata)

    known_hosts = known_hosts_path(state_dir)
    insecure = args.insecure_ssh
    redeploy_config_file = config_file_from_args_or_metadata(args.config_file, metadata)
    outputs = terraform_output(state_dir, env=terraform_env)
    metadata["terraform_outputs"] = outputs
    write_json(state_dir / "metadata.json", metadata)

    inventory_path = write_ansible_inventory(
        outputs,
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
    metadata = load_metadata(state_dir)
    terraform_env = saved_deployment_terraform_env(args, metadata)
    if args.delete_state_dir:
        validate_state_dir_safe_to_delete(state_dir)

    if not args.yes:
        answer = input(f"Destroy Spark cluster managed in {state_dir}? Type 'yes': ")
        if answer != "yes":
            raise SystemExit("Destroy cancelled.")

    try:
        run_command(
            ["terraform", "destroy", "-auto-approve"],
            cwd=state_dir,
            capture_output=True,
            env=terraform_env,
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


class DefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Show defaults only for options whose default is known when parsing.

    Options defaulting to None or "" get provider- or metadata-dependent values
    after parsing, so their help text describes the effective default instead.
    """

    def _get_help_string(self, action: argparse.Action) -> str | None:
        if action.default is None or action.default == "":
            return action.help
        return super()._get_help_string(action)


def configure_deploy_parser(deploy: argparse.ArgumentParser) -> None:
    deploy.add_argument("--cloud-provider", choices=CLOUD_PROVIDERS, default="aws")
    deploy.add_argument(
        "--region",
        default=None,
        help=(
            f"Cloud region. AWS default: {AWS_DEFAULT_REGION}; "
            f"GCP default: {GCP_DEFAULT_REGION}."
        ),
    )
    deploy.add_argument(
        "--zone",
        default=None,
        help=(
            f"GCP Compute Engine zone. Defaults to REGION-a "
            f"({GCP_DEFAULT_ZONE} with the default region), or REGION-b for "
            f"regions without an -a zone ({', '.join(sorted(GCP_REGION_DEFAULT_ZONES))})."
        ),
    )
    deploy.add_argument(
        "--gcp-project",
        default="",
        help="GCP project ID. Required when --cloud-provider=gcp.",
    )
    deploy.add_argument("--name-prefix", default="scylla-migrator-spark")
    deploy.add_argument(
        "--key-name",
        default=None,
        help="AWS key pair name to create. Defaults to NAME_PREFIX-key.",
    )
    deploy.add_argument("--ssh-private-key", default="~/.ssh/id_rsa")
    deploy.add_argument(
        "--ssh-public-key",
        default=None,
        help=(
            "SSH public key to register in the AWS key pair or GCP instance "
            "metadata. Defaults to SSH_PRIVATE_KEY.pub."
        ),
    )
    deploy.add_argument(
        "--master-instance-type",
        default=None,
        help=(
            f"Master machine type. AWS default: {AWS_DEFAULT_MASTER_INSTANCE_TYPE}; "
            f"GCP default: {GCP_DEFAULT_MASTER_INSTANCE_TYPE}."
        ),
    )
    deploy.add_argument(
        "--worker-instance-type",
        default=None,
        help=(
            f"Worker machine type. AWS default: {AWS_DEFAULT_WORKER_INSTANCE_TYPE}; "
            f"GCP default: {GCP_DEFAULT_WORKER_INSTANCE_TYPE}."
        ),
    )
    deploy.add_argument("--workers", type=positive_int, default=1)
    deploy.add_argument(
        "--vpc-cidr",
        type=ipv4_cidr,
        default=None,
        help=f"AWS VPC CIDR. AWS default: {AWS_DEFAULT_VPC_CIDR}.",
    )
    deploy.add_argument(
        "--public-subnet-cidr",
        type=ipv4_cidr,
        default=DEFAULT_SUBNET_CIDR,
        help="IPv4 CIDR for the generated AWS or GCP subnet.",
    )
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
        "--network",
        default="",
        help=(
            "Existing GCP VPC network name to use instead of creating a network. "
            "Must be provided together with --subnetwork."
        ),
    )
    deploy.add_argument(
        "--subnetwork",
        default="",
        help=(
            "Existing GCP subnetwork name for Compute Engine instances. "
            "Must be provided together with --network and be in --region."
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
        "--gcp-instance-service-account",
        default="",
        help=(
            "Optional service account email to attach to GCP instances with the "
            "cloud-platform OAuth scope. IAM roles must be granted separately."
        ),
    )
    deploy.add_argument(
        "--gcp-master-boot-disk-type",
        default="",
        help=(
            "Optional GCP master boot disk type override. By default, the script "
            "chooses pd-balanced or hyperdisk-balanced for the machine series."
        ),
    )
    deploy.add_argument(
        "--gcp-worker-boot-disk-type",
        default="",
        help=(
            "Optional GCP worker boot disk type override. By default, the script "
            "chooses pd-balanced or hyperdisk-balanced for the machine series."
        ),
    )
    deploy.add_argument(
        "--owner-tag",
        default="",
        help="Optional owner tag/label for the Spark master and worker instances.",
    )
    deploy.add_argument("--migration-type", choices=MIGRATION_TYPES, default="cql")
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


def add_saved_ssh_and_migration_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--ssh-private-key",
        default=None,
        help="SSH private key. Defaults to the key saved in deployment metadata.",
    )
    parser.add_argument(
        "--migration-type",
        choices=MIGRATION_TYPES,
        default=None,
        help=(
            "Migration type. Defaults to the type saved in deployment metadata, "
            "or cql if none was saved."
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--state-dir",
        default=DEFAULT_STATE_DIR,
        help="Directory for generated Terraform files, state, inventory, and metadata.",
    )
    common.add_argument(
        "--gcp-service-account-file",
        default=None,
        help=(
            "Explicit GCP service account JSON key file. If omitted, GCP uses "
            "Application Default Credentials. Later commands reuse the path "
            "saved by deploy unless this option overrides it."
        ),
    )

    parser = argparse.ArgumentParser(
        description="Deploy and operate a Spark cluster for ScyllaDB Migrator.",
        formatter_class=DefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    deploy = subparsers.add_parser(
        "deploy",
        parents=[common],
        formatter_class=DefaultsHelpFormatter,
        help="Create AWS or GCP infrastructure and configure Spark with Ansible.",
    )
    configure_deploy_parser(deploy)

    show = subparsers.add_parser(
        "show",
        parents=[common],
        formatter_class=DefaultsHelpFormatter,
        help="Show Terraform-managed infrastructure details.",
    )
    show.add_argument("--json", action="store_true")
    show.set_defaults(func=handle_show)

    run = subparsers.add_parser(
        "run",
        parents=[common],
        formatter_class=DefaultsHelpFormatter,
        help="Run the configured Migrator Spark job on the master node.",
    )
    add_saved_ssh_and_migration_arguments(run)
    run.add_argument(
        "--config-file",
        default=None,
        help=(
            "Optional Migrator config to upload before running. "
            "Defaults to the config file saved in deployment metadata."
        ),
    )
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
        formatter_class=DefaultsHelpFormatter,
        help="Rerun Ansible on the current Terraform-managed nodes.",
    )
    add_saved_ssh_and_migration_arguments(redeploy)
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
        formatter_class=DefaultsHelpFormatter,
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
