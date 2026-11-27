# Feature Specification: Spark Cluster Setup and Job Submission

**Feature Branch**: `010-spark-cluster-and-job-submission`

**Created**: 2026-11-27

**Status**: Baseline (as-built, derived from docs)

**Input**: Existing documentation: `docs/source/index.rst` (feature list, "Compatibility Matrix"); `docs/source/getting-started/index.rst` (whole page); `docs/source/getting-started/docker.rst`; `docs/source/getting-started/spark-standalone.rst`; `docs/source/getting-started/ansible.rst`; `docs/source/run-the-migration.rst` (whole page); `docs/source/tutorials/dynamodb-to-scylladb-alternator/index.rst` (Spark/Docker setup, `spark-submit` invocation, executor sizing, monitoring); `docs/source/configuration.rst` (introduction: YAML format and `DecodingFailure`/`ParsingFailure` errors); `docs/source/validate.rst` (validator entry point per deployment)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Submit a migration job to a running Spark cluster (Priority: P1)

An operator has a Spark cluster, the `scylla-migrator-assembly.jar` and a `config.yaml` on the
Spark master node. They run `spark-submit` with `--class com.scylladb.migrator.Migrator`,
`--master spark://<spark-master-hostname>:7077`, `--conf spark.scylla.config=<path to config.yaml>`,
explicit `--executor-cores` and `--executor-memory`, and the jar path, then monitor the job.

**Why this priority**: Every migration, whatever its source and target, is started this way.

**Independent Test**: On any Spark cluster with a compatible version, submit the migrator with a
small valid configuration and confirm that the job starts, logs the migrator version, and completes.

**Acceptance Scenarios**:

1. **Given** a compatible Spark cluster and a valid configuration on the master node, **When** the
   operator runs `spark-submit --class com.scylladb.migrator.Migrator --master spark://<host>:7077
   --conf spark.scylla.config=<path> --executor-cores 2 --executor-memory 4G <jar>`, **Then** the
   migration job starts on the cluster and logs a line like `INFO migrator: ScyllaDB Migrator <version>`.
2. **Given** the same command with `--class com.scylladb.migrator.Validator`, **When** it is
   submitted, **Then** the validator runs instead of the migrator (see `009-migration-validation`).
3. **Given** a running job, **When** the operator opens `http://<spark-master>:4040`, **Then** the
   Spark application UI shows the job's stages and tasks; when no job is running that page is
   unavailable.

---

### User Story 2 - Get a clear error for a malformed configuration (Priority: P1)

The operator makes a YAML indentation mistake or omits a required property. The migrator fails at
startup with a message that names the problem.

**Why this priority**: Fail-fast with clear errors (Constitution principle IV) is the operator's
first feedback loop.

**Independent Test**: Submit with a configuration missing `target`, and with a configuration that is
not valid YAML; check the startup error.

**Acceptance Scenarios**:

1. **Given** a configuration without the top-level `target` property, **When** the job starts,
   **Then** it fails with `Exception in thread "main" DecodingFailure at .target: Missing required field`.
2. **Given** a configuration that is not well-formed YAML, **When** the job starts, **Then** it fails
   with a message starting with `ParsingFailure` describing the problem.
3. **Given** a property that is not documented as "Optional" is missing, **When** the job starts,
   **Then** it fails with a `DecodingFailure` naming that property's path.

---

### User Story 3 - Size executors correctly (Priority: P1)

The operator chooses executor CPU and memory according to the worker nodes: 5 to 10 cores per
executor and about 2 GB of memory per core (e.g. `--executor-cores 8 --executor-memory 16G` on a
16-CPU worker, giving two executors per worker).

**Why this priority**: Without explicit sizing, Spark creates a single executor with all cores but
only 1 GB of memory, which leads to `OutOfMemoryError`.

**Independent Test**: Submit once without `--executor-memory` and once with the recommended sizing
on the same worker; observe executor layout in the Spark UI and the absence of out-of-memory errors.

**Acceptance Scenarios**:

1. **Given** a 16-CPU worker and `--executor-cores 8 --executor-memory 16G`, **When** the job
   starts, **Then** two executors with 8 cores and 16 GB each are allocated on that worker.
2. **Given** no `--executor-cores`/`--executor-memory`, **When** the job starts, **Then** Spark
   defaults to a single executor using all cores and only 1 GB of memory, which may cause
   `OutOfMemoryError` at run time.
3. **Given** a worker with 20 cores and `--executor-cores 10`, **When** the job starts, **Then**
   two executors are allocated to fully use the worker.

---

### User Story 4 - Throttle or scale migration throughput (Priority: P2)

The operator needs to reduce load on the source/target databases, or wants more throughput while
the databases are not saturated.

**Why this priority**: Protects production databases (Constitution principle V).

**Independent Test**: Run the same migration with and without `--conf spark.cores.max=2` and compare
the number of concurrently running tasks.

**Acceptance Scenarios**:

1. **Given** `--conf spark.cores.max=2`, **When** the job runs, **Then** at most 2 cores are used by
   the application in total, lowering throughput.
2. **Given** the operator lowers `--executor-cores` to throttle, **When** the job runs, **Then**
   throughput is **not** reduced, because Spark allocates more executors to use the available cores;
   the documented throttles are a smaller cluster, `spark.cores.max`, and for DynamoDB
   `throughputReadPercent`/`throughputWritePercent` (see `003-dynamodb-family-migration`).
3. **Given** the databases are not saturated, **When** the operator adds worker nodes, **Then** the
   work is spread across them and throughput increases.

---

### User Story 5 - Try the migrator locally with Docker (Priority: P2)

The operator wants to try the migrator on a single machine. They clone the repository, download
`scylla-migrator-assembly.jar` into `migrator/target/scala-2.13/`, run `docker compose up -d`, copy
`config.yaml.example` to `config.yaml`, and submit with
`docker compose exec spark-master /spark/bin/spark-submit ... --conf spark.driver.host=spark-master
--conf spark.scylla.config=/app/config.yaml ... /jars/scylla-migrator-assembly.jar`.

**Why this priority**: Fast evaluation path; not recommended for production performance.

**Independent Test**: Follow the Docker steps on a machine with Docker and Git and reach the Spark
UI at `http://localhost:8080`, then run a small migration.

**Acceptance Scenarios**:

1. **Given** Docker and Git, **When** the operator runs `docker compose up -d` in the repository,
   **Then** a Spark master and worker start and the Spark UI is reachable at `http://localhost:8080`.
2. **Given** the cluster is up, **When** the job is submitted via `docker compose exec spark-master`,
   **Then** the jar is found under `/jars` (mounted from `./migrator/target/scala-2.13`) and the
   configuration under `/app` (the repository root), and progress is visible at `http://localhost:4040`.
3. **Given** the DynamoDB-to-Alternator tutorial compose file, **When** the services start, **Then**
   DynamoDB local listens on port 8000, ScyllaDB Alternator on port 8001, the Spark worker has 2
   cores and 4 GB, Spark 4.0.2 is installed, and the master exposes ports 8080 and 4040.

---

### User Story 6 - Deploy a Spark cluster with Ansible (Priority: P2)

The operator provisions Ubuntu hosts (e.g. EC2) and uses the provided Ansible playbook to install
Spark and the migrator, then submits jobs with the installed submit scripts.

**Why this priority**: The documented path for production clusters on the operator's
infrastructure.

**Independent Test**: Run the playbook against an inventory of one master and two workers, start
services, open the three web UIs, and run `submit-cql-job.sh` under `nohup`.

**Acceptance Scenarios**:

1. **Given** an inventory with groups `spark`, `master` and `worker`, hosts reachable over SSH as
   user `ubuntu`, and Ubuntu 22.04/24.04 (or compatible), **When** the operator runs
   `ansible-playbook -i <inventory> --private-key <key> -u ubuntu scylla-migrator.yml`, **Then**
   prerequisites and Spark are installed on all hosts and the migrator on the master node.
2. **Given** a provisioned cluster, **When** the operator runs
   `sudo systemctl restart spark-master spark-history-server` on the master and
   `sudo systemctl restart spark-worker` on each worker, **Then** the Spark master UI is available on
   TCP 8080, the active application UI on 4040 (while a job runs), and the history server on 18080,
   provided the network allows those ports.
3. **Given** `config.yaml` (CQL) or `config.dynamodb.yml` (Alternator) copied to
   `/home/ubuntu/scylla-migrator/`, **When** the operator runs
   `nohup ./submit-cql-job.sh > submit-cql-job.log 2>&1 &` (or `./submit-alternator-job.sh`),
   **Then** the migration runs and survives an SSH disconnection.
4. **Given** a differently named config file, **When** the operator edits the corresponding submit
   script, **Then** the script uses that file.
5. **Given** a completed migration, **When** the operator runs `./submit-cql-job-validator.sh` or
   `./submit-alternator-validator.sh`, **Then** the validator runs.
6. **Given** the resource environment templates `ansible/templates/spark-env-master` and
   `ansible/templates/spark-env-worker`, **When** the operator edits them before deploying, **Then**
   driver, executor and worker resource allocation follow those values.

---

### User Story 7 - Set up a Spark standalone cluster manually (Priority: P3)

The operator installs Spark by following the official standalone documentation, downloads the
latest (or a specific) release of the assembly jar and `config.yaml.example` to the master node,
configures it and runs `spark-submit` from the master.

**Why this priority**: Alternative to Ansible for custom infrastructure; same submission contract.

**Independent Test**: On a manually installed Spark standalone cluster, download the jar and example
config with `wget`, configure, submit, and monitor from the Spark web UI.

**Acceptance Scenarios**:

1. **Given** a running standalone Spark cluster of a compatible version, **When** the operator
   submits the downloaded jar with `--class com.scylladb.migrator.Migrator` and
   `--conf spark.scylla.config=<path>`, **Then** the migration runs.

---

### User Story 8 - Run on managed Spark (AWS EMR) (Priority: P3)

The operator runs the migrator or validator as an EMR cluster step using
`spark-submit --deploy-mode cluster --class <entry point> --conf spark.scylla.config=/mnt1/config.yaml
/mnt1/scylla-migrator-assembly.jar`.

**Why this priority**: Supported deployment, documented only by its submission arguments.

**Independent Test**: Add a custom-JAR step on EMR with those arguments and check it completes.

**Acceptance Scenarios**:

1. **Given** the jar and configuration at `/mnt1/`, **When** the EMR step runs with
   `--deploy-mode cluster`, **Then** the chosen entry point runs on the EMR cluster.

### Edge Cases

- Migrator, Spark and Scala versions must be compatible: Migrator 0.9.x and 1.x require Spark
  3.5.x; Migrator 2.x requires Spark 4.0.x; all use Scala 2.13.x. An incompatible combination is
  not supported.
- Running a submit script in a plain SSH session without `nohup` or `tmux` can abort the job when
  the SSH connection drops.
- The Spark application UI on port 4040 is unavailable when no job is running.
- Port 4040, 8080 and 18080 must be opened in the network configuration to reach the UIs remotely.
- Decreasing `--executor-cores` does not throttle a migration; it only changes executor layout.
- Throughput is bounded by the source's read throughput and the target's write throughput; adding
  workers beyond that point does not help.
- The Docker setup is intended for trials; production should use a real cluster.
- The configuration path and the jar path passed to `spark-submit` must be paths on the Spark master
  node (in client submission); for Docker they are container paths (`/app`, `/jars`).
- YAML is whitespace-sensitive; mis-indented properties lead to `DecodingFailure`/`ParsingFailure`
  at startup.
- The Ansible submit scripts expect `config.yaml` (CQL) and `config.dynamodb.yml` (Alternator) under
  `/home/ubuntu/scylla-migrator/` unless edited.
- The Ansible procedure requires the target table to exist before submission ("Ensure the table has
  been created in the target environment"), while the migration pages say a missing target table is
  created on the fly. [NEEDS CLARIFICATION: Is pre-creating the target table actually required for
  Ansible-submitted jobs (e.g. because of the submit scripts), or is step 10 only a recommendation
  that predates automatic table creation?]
- `index.rst` lists only Cassandra, Parquet, DynamoDB and DynamoDB S3 export as readable sources,
  while other pages and the constitution also cover ScyllaDB Alternator, DataStax Astra, Aerospike
  and MySQL sources; the feature list on the landing page is incomplete (source specifics are owned
  by specs 001–006).

## Requirements *(mandatory)*

### Functional Requirements

**Packaging and compatibility**

- **FR-001**: System MUST be distributed as a single `scylla-migrator-assembly.jar` bundling the
  migrator and its dependencies, downloadable from the latest or a specific GitHub release.
- **FR-002**: System MUST document and honour the compatibility matrix: Migrator 0.9.x → Spark 3.5.x,
  1.x → Spark 3.5.x, 2.x → Spark 4.0.x, all with Scala 2.13.x.
- **FR-003**: System MUST provide a documented example configuration (`config.yaml.example`) that
  operators copy to `config.yaml` and adapt.

**Submission**

- **FR-004**: System MUST expose the migration entry point `com.scylladb.migrator.Migrator` and the
  validation entry point `com.scylladb.migrator.Validator`, selected with `spark-submit --class`.
- **FR-005**: System MUST read its configuration file from the location given by
  `--conf spark.scylla.config=<path>`.
- **FR-006**: System MUST run on a Spark standalone master given by `--master spark://<host>:7077`,
  and in `--deploy-mode cluster` on managed Spark such as AWS EMR.
- **FR-007**: System MUST log its version at startup (`ScyllaDB Migrator <version>`).

**Configuration errors**

- **FR-008**: System MUST fail at startup, before reading or writing data, when the configuration
  file is not valid YAML, reporting a `ParsingFailure ...` message.
- **FR-009**: System MUST fail at startup, before reading or writing data, when a required property
  is missing or has the wrong shape, reporting `DecodingFailure at <path>: <reason>` (e.g.
  `DecodingFailure at .target: Missing required field`).
- **FR-010**: System MUST require the top-level `source` and `target` properties and treat
  `renames`, `skipTokenRanges` and `skipSegments` as optional; `savepoints` is required for
  resumable migrations and `validation` only in validation mode.

**Resources and throughput**

- **FR-011**: System MUST run with executor CPU/memory set through `--executor-cores` and
  `--executor-memory`; the documented recommendation is 5–10 cores per executor and 2 GB of memory
  per core.
- **FR-012**: System MUST distribute work across all executors of the cluster so that adding worker
  nodes increases throughput while the databases are not saturated.
- **FR-013**: System MUST respect `--conf spark.cores.max=<n>` as a cap on total cores used by the
  application.

**Deployment options**

- **FR-014**: System MUST provide a Docker Compose setup in the repository that starts a Spark master
  and worker, exposes the Spark UI at `localhost:8080` and the application UI at `localhost:4040`,
  mounts `./migrator/target/scala-2.13` at `/jars` and the repository root at `/app`, and accepts
  submission through `docker compose exec spark-master /spark/bin/spark-submit` with
  `--conf spark.driver.host=spark-master`.
- **FR-015**: System MUST provide an Ansible playbook (`ansible/scylla-migrator.yml`) that installs
  prerequisites and Spark on hosts of inventory groups `spark`, `master` and `worker`, installs the
  migrator on the master, connects as user `ubuntu`, and targets Ubuntu 22.04/24.04 LTS or
  Ubuntu-compatible distributions.
- **FR-016**: The Ansible deployment MUST install systemd services `spark-master`,
  `spark-history-server` and `spark-worker`, with the master UI on TCP 8080, the application UI on
  4040 and the history server on 18080.
- **FR-017**: The Ansible deployment MUST install submit scripts `submit-cql-job.sh`,
  `submit-alternator-job.sh`, `submit-cql-job-validator.sh` and `submit-alternator-validator.sh` in
  `/home/ubuntu/scylla-migrator/`, defaulting to `config.yaml` (CQL) and `config.dynamodb.yml`
  (Alternator), and editable to use other file names.
- **FR-018**: The Ansible deployment MUST expose Spark driver, executor and worker resource settings
  through the `ansible/templates/spark-env-master` and `ansible/templates/spark-env-worker`
  templates.
- **FR-019**: System MUST provide a self-contained DynamoDB-to-Alternator tutorial (Docker Compose
  with DynamoDB local on port 8000, a Spark 4.0.2 master/worker with 2 cores and 4 GB, ScyllaDB
  Alternator on port 8001, data-generation scripts and a `run-migrator.sh` submit script).

### Key Entities

- **Spark cluster**: master node, worker nodes, executors (cores and memory), driver; the unit that
  runs the migrator.
- **Assembly jar** (`scylla-migrator-assembly.jar`): the migrator binary bundling all dependencies.
- **Configuration file** (`config.yaml`): YAML with `source`, `target`, optional `renames`,
  `savepoints`, `validation`; passed via `spark.scylla.config`.
- **Entry point**: `com.scylladb.migrator.Migrator` or `com.scylladb.migrator.Validator`.
- **Ansible inventory**: hosts grouped into `spark`, `master`, `worker`.
- **Submit script**: per-mode wrapper around `spark-submit` installed by Ansible.
- **Compatibility matrix**: mapping from migrator release line to required Spark and Scala versions.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An operator following the Docker guide on a machine with Docker and Git reaches the
  Spark UI and starts a migration job without editing anything other than `config.yaml`.
- **SC-002**: 100% of configurations with a YAML syntax error or a missing required property fail
  before any data is read or written, with a `ParsingFailure` or `DecodingFailure` message that
  names the location of the problem.
- **SC-003**: With the recommended sizing (2 GB per core, 5–10 cores per executor), migrations do
  not fail with `OutOfMemoryError` due to default executor memory.
- **SC-004**: Setting `spark.cores.max=<n>` limits the application to at most n concurrently used
  cores, as visible in the Spark UI.
- **SC-005**: A job submitted with `nohup` or inside `tmux` on an Ansible master continues to
  completion after the operator's SSH session is closed.
- **SC-006**: The Ansible playbook brings up a cluster from an inventory of one master and N workers
  in a single playbook run, after which the master UI lists all N workers.

## Assumptions

- The operator provides and pays for the hosts, networking (ports 7077, 8080, 4040, 18080), SSH
  keys and security groups; the migrator does not provision infrastructure for Ansible.
- The operator ensures source and target databases are reachable from all Spark nodes.
- Files referenced by the configuration (TLS stores, Astra bundles) are distributed to executors by
  the operator, e.g. with `spark-submit --files` (see `001-cql-to-cql-migration`).
- The Spark version installed matches the compatibility matrix for the migrator release used.
- Source/target-specific configuration is out of scope and covered by specs 001–006; renames by
  `007-column-renames`; savepoints by `008-savepoints-and-resume`; validation by
  `009-migration-validation`.
- Kubernetes and other orchestrators are mentioned only as restart environments for automatic
  resume; their deployment is not documented and is out of scope.
