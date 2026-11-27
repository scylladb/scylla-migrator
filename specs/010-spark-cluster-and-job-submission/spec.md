# Feature Specification: Spark Cluster Setup and Job Submission

**Feature Branch**: `010-spark-cluster-and-job-submission`

**Created**: 2026-11-27

**Status**: Baseline (as-built; derived from docs, reviewed against code 2026-11-27)

**Input**: Existing documentation: `docs/source/index.rst` (feature list, "Compatibility Matrix"); `docs/source/getting-started/index.rst` (whole page); `docs/source/getting-started/docker.rst`; `docs/source/getting-started/spark-standalone.rst`; `docs/source/getting-started/ansible.rst`; `docs/source/run-the-migration.rst` (whole page); `docs/source/tutorials/dynamodb-to-scylladb-alternator/index.rst` (Spark/Docker setup, `spark-submit` invocation, executor sizing, monitoring); `docs/source/configuration.rst` (introduction: YAML format and `DecodingFailure`/`ParsingFailure` errors); `docs/source/validate.rst` (validator entry point per deployment); source code at commit 06835d0 (`Migrator.scala`, `Validator.scala`, `config/MigratorConfig.scala`, `config/SparkTaskRetries.scala`, `config/SparkSecretRedaction.scala`, `config/SensitiveKeys.scala`, `PathIO.scala`, `SavepointsManager.scala`, `writers/Scylla.scala`, `docker-compose.yaml`, `dockerfiles/spark/Dockerfile`, `ansible/` (playbook, templates, files), `Makefile`, `build.sbt`, `deploy_spark_cluster.py`, `README.md`)

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
4. **Given** `spark.scylla.config` is an `s3a://`, `gs://` or other Hadoop filesystem URI, **When**
   the job starts, **Then** the configuration is read from that location (a scheme without a Hadoop
   connector in the runtime fails with an error naming the missing connector).
5. **Given** the operator did not pass `spark.task.maxFailures`, **When** the job runs, **Then** a
   task may be retried up to 1024 times before the job fails; a value passed to `spark-submit` is
   kept. A stage may be re-attempted up to 60 consecutive times.
6. **Given** a running migration of a resumable source, **When** the driver receives SIGINT, SIGTERM
   or SIGUSR2, **Then** it writes a savepoint before exiting (a second signal forces exit; see
   `008-savepoints-and-resume`).

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
4. **Given** a source/target pair the entry point does not support (e.g. a Parquet source with the
   Validator), **When** the job starts, **Then** it fails before reading data with "Unsupported
   combination of source and target ...".

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
3. **Given** the repository `docker-compose.yaml`, **When** it starts, **Then** the worker has 2 cores
   and 4 GB, its UI is on port 8081, both containers mount `./parquet-data` at `/parquet-data`, and
   the image runs Spark 4.0.2 on OpenJDK 17.
4. **Given** the DynamoDB-to-Alternator tutorial compose file, **When** the services start, **Then**
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

1. **Given** an inventory with groups `spark`, `master` and `worker`, the master host named
   `spark_master`, hosts reachable over SSH as user `ubuntu`, and Ubuntu 22.04/24.04 (or
   compatible), **When** the operator runs
   `ansible-playbook -i <inventory> --private-key <key> -u ubuntu scylla-migrator.yml`, **Then**
   OpenJDK 17, cqlsh, AWS CLI v2 and Spark 4.0.2 are installed on all hosts and the latest released
   migrator jar on the master node; services are enabled but not started.
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
6. **Given** no edits to `ansible/templates/spark-env-master` and `ansible/templates/spark-env-worker`,
   **When** the playbook runs, **Then** worker cores (all vCPUs), worker memory (85% of RAM, at
   least 1 GB), executor cores (the largest of 10..1 that divides the worker cores), executor
   memory (90% of worker memory per executor, at least 1 GB) and local directories (`/mnt` if
   mounted, else `/tmp`) are derived from each host's hardware; edits to the templates override
   that.
7. **Given** the operator runs a submit script from a directory other than
   `/home/ubuntu/scylla-migrator`, **When** it starts, **Then** it fails to find `spark-env` (and, for
   the CQL scripts, `config.yaml`), because both are referenced relative to the current directory.

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

---

### User Story 9 - Provision an AWS cluster with the deploy helper (Priority: P3)

The operator uses `./deploy_spark_cluster.py` (README only) to create EC2 infrastructure with
Terraform, configure it with the Ansible playbook, upload a configuration, run the job and tear the
cluster down.

**Why this priority**: Convenience path on AWS; the manual Ansible path remains available.

**Independent Test**: Run `deploy`, `show`, `run`, `destroy --yes` against a test AWS account.

**Acceptance Scenarios**:

1. **Given** Python 3.10+, Terraform, `ssh`/`scp`, AWS credentials and an SSH key pair, **When** the
   operator runs `./deploy_spark_cluster.py deploy --region ... --workers N --migration-type cql|alternator
   --config-file ...`, **Then** the cluster is created, configured with the playbook, and the
   config uploaded; state is kept in `.deploy_spark_cluster/`.
2. **Given** a deployed cluster, **When** the operator runs `run` (optionally `--validator`),
   **Then** the helper checks the master and workers, launches the submit script with `nohup` and
   prints the remote PID and log path.
3. **Given** a finished migration, **When** the operator runs `destroy --yes`, **Then** the AWS
   resources are removed (`--delete-state-dir` also removes local state).

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
- Pre-creating the target table is required for CQL targets regardless of how the job is submitted:
  the migrator never creates CQL tables (see `001-cql-to-cql-migration` FR-028), and neither the
  playbook nor the submit scripts create them. DynamoDB/Alternator target tables are created by the
  migrator when missing (`003-dynamodb-family-migration`). Ansible step 10 is therefore correct for
  CQL and optional for Alternator.
- The Ansible templates reference the master by inventory name `spark_master`; an inventory that
  names the master differently fails when templates are rendered.
- The CQL submit scripts pass `spark.cassandra.input/output.consistency.level=LOCAL_QUORUM` and
  `spark.cassandra.connection.localConnectionsPerExecutor=4`; the configuration's
  `consistencyLevel` and `connections` (when set) take precedence. All submit scripts enable the
  Spark event log (read by the history server from `/tmp/spark-events`) and create
  `/tmp/savepoints`; the Alternator scripts use `--driver-memory 4G`.
- The playbook does not copy `ansible/files/config.dynamodb.yml` to the master; the operator copies
  the edited file.
- If `spark.redaction.regex` is set only on the SparkSession at run time (not on the SparkConf at
  submission), the job fails with an error asking to set it before session creation.
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
- **FR-001a**: The repository MUST build the jar at
  `migrator/target/scala-2.13/scylla-migrator-assembly.jar` with `make build` (sbt, JDK 17) or
  `make docker-build-jar` (Docker only); the build targets Spark 4.0.2 and Scala 2.13.
- **FR-005**: System MUST read its configuration file from the location given by
  `--conf spark.scylla.config=<path>`, which may be a local path or any Hadoop filesystem URI
  (`s3a://`, `gs://`, ...) whose connector is on the classpath; a missing connector fails with an
  error that names the scheme and the connector to add.
- **FR-006**: System MUST run on a Spark standalone master given by `--master spark://<host>:7077`,
  and in `--deploy-mode cluster` on managed Spark such as AWS EMR.
- **FR-007**: System MUST log its version at startup (`ScyllaDB Migrator <version>`), run as Spark
  application `scylla-migrator` (or `scylla-validator`), log the loaded configuration with secrets
  redacted, and set log levels to WARN globally and INFO for the migrator's own messages.

**Configuration errors**

- **FR-008**: System MUST fail at startup, before reading or writing data, when the configuration
  file is not valid YAML, reporting a `ParsingFailure ...` message.
- **FR-009**: System MUST fail at startup, before reading or writing data, when a required property
  is missing or has the wrong shape, reporting `DecodingFailure at <path>: <reason>` (e.g.
  `DecodingFailure at .target: Missing required field`).
- **FR-010**: System MUST require the top-level `source` and `target` properties and treat
  `renames`, `skipTokenRanges` and `skipSegments` as optional; `savepoints` is required for
  resumable migrations and `validation` only in validation mode.
- **FR-010a**: System MUST fail before reading data with "Unsupported combination of source and
  target ..." when the Migrator or Validator is given a source/target pair it does not support.

**Spark runtime defaults**

- **FR-010b**: System MUST set `spark.task.maxFailures` to 1024 when the operator has not set it,
  and MUST keep any value supplied to `spark-submit`.
- **FR-010c**: System MUST run with `spark.stage.maxConsecutiveAttempts=60` (both entry points).
- **FR-010d**: System MUST install a default `spark.redaction.regex`
  (`(?i)password|secret|token|credential|access[._-]?key|api[._-]?key|private[._-]?key|key[._-]?file`)
  when the operator has not set one, so that secrets are redacted in Spark UIs, event logs and
  savepoints; an operator-supplied value is kept. This applies to every source type.
- **FR-010e**: System MUST, for resumable sources, write a savepoint and exit on the first SIGINT,
  SIGTERM or SIGUSR2 and force exit on a second signal (behaviour owned by
  `008-savepoints-and-resume`).
- **FR-010f**: System MUST stop gracefully on shutdown when streaming changes
  (`spark.streaming.stopGracefullyOnShutdown=true`).

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
- **FR-014a**: The Docker Compose worker MUST have 2 cores and 4 GB with its UI on port 8081, and both
  containers MUST mount `./parquet-data` at `/parquet-data`; the image MUST run Spark 4.0.2 on
  OpenJDK 17.
- **FR-015**: System MUST provide an Ansible playbook (`ansible/scylla-migrator.yml`) that installs
  prerequisites (OpenJDK 17, cqlsh, AWS CLI v2) and Spark 4.0.2 on hosts of inventory groups
  `spark`, `master` and `worker`, downloads the latest released migrator jar to the master,
  connects as user `ubuntu`, requires the master host to be named `spark_master`, and targets
  Ubuntu 22.04/24.04 LTS or Ubuntu-compatible distributions.
- **FR-016**: The Ansible deployment MUST install systemd services `spark-master`,
  `spark-history-server` and `spark-worker`, with the master UI on TCP 8080, the application UI on
  4040 and the history server on 18080; the services are enabled (restart on failure) but not
  started by the playbook.
- **FR-017**: The Ansible deployment MUST install submit scripts `submit-cql-job.sh`,
  `submit-alternator-job.sh`, `submit-cql-job-validator.sh` and `submit-alternator-validator.sh` in
  `/home/ubuntu/scylla-migrator/`, defaulting to `config.yaml` (CQL) and `config.dynamodb.yml`
  (Alternator), and editable to use other file names. The scripts MUST be run from
  `/home/ubuntu/scylla-migrator` (they source `spark-env` and, for CQL, read `config.yaml` relative
  to the current directory).
- **FR-018**: The Ansible deployment MUST derive worker cores/memory, executor cores/memory and local
  directories from each host's hardware and write them through the
  `ansible/templates/spark-env-master` and `ansible/templates/spark-env-worker` templates, which
  operators may still edit.
- **FR-019**: System MUST provide a self-contained DynamoDB-to-Alternator tutorial (Docker Compose
  with DynamoDB local on port 8000, a Spark 4.0.2 master/worker with 2 cores and 4 GB, ScyllaDB
  Alternator on port 8001, data-generation scripts and a `run-migrator.sh` submit script).
- **FR-020**: The repository MUST provide `deploy_spark_cluster.py` (Python 3.10+, Terraform, AWS)
  with `deploy`, `show`, `redeploy`, `run` (optionally `--validator`) and `destroy` commands that
  provision EC2 hosts, configure them with the Ansible playbook, upload the configuration and
  submit the job with `nohup`.

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
- **Deploy helper state** (`.deploy_spark_cluster/`): Terraform files and state, generated Ansible
  inventory, metadata and SSH known hosts for an AWS cluster created by `deploy_spark_cluster.py`.

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

## Docs vs Code Review (2026-11-27)

| # | Topic | Docs say | Code does | Spec follows | Suggested docs fix |
|---|-------|----------|-----------|--------------|--------------------|
| 1 | Target table before submission (Ansible step 10) | Ansible: "Ensure the table has been created"; migration pages: CQL table "created on the fly". | No CQL table is ever created (`writers/Scylla.scala:346`); DynamoDB tables are created (`DynamoUtils.scala:249-254`); Ansible creates none (`ansible/scylla-migrator.yml`). | Code (Edge Cases) | Keep step 10 for CQL; fix the CQL target docs. |
| 2 | `spark.task.maxFailures` | Not documented. | Defaults to 1024 unless supplied (`config/SparkTaskRetries.scala:7-8`, `Migrator.scala:16`, `Validator.scala:42`). | Code (FR-010b, US1-5) | Document the default and how to override it. |
| 3 | `spark.stage.maxConsecutiveAttempts` | Not documented. | Hard-coded to 60 (`Migrator.scala:23`, `Validator.scala:49`). | Code (FR-010c) | Document it. |
| 4 | `spark.redaction.regex` | Not documented. | Default regex installed when unset, for every source (`config/SparkSecretRedaction.scala:18-20`, `config/SensitiveKeys.scala:6-7`, `Migrator.scala:17`, `Validator.scala:43`); runtime-only session value rejected (`config/SparkSecretRedaction.scala:31-49`). | Code (FR-010d) | Document the default and that overrides must be passed at submission. |
| 5 | Signals | Not in the deployment pages. | SIGINT/SIGTERM/SIGUSR2 write a savepoint then exit; second signal forces exit (`SavepointsManager.scala:42-46,153-175`). | Code (FR-010e; details in 008) | Cross-reference from "Run the Migration". |
| 6 | Config file location | "It must be a path on the Spark master node" (`run-the-migration.rst:27`). | Local path or Hadoop URI (`config/MigratorConfig.scala:192-212`, `PathIO.scala:34-48,180-203`). | Code (FR-005, US1-4) | Mention remote URIs. |
| 7 | Ansible inventory naming | Example names the master `spark_master` but does not say it is required. | Templates use `hostvars["spark_master"]` (`ansible/templates/spark-env-master`, `spark-env-worker`, submit scripts). | Code (FR-015) | State the name is mandatory. |
| 8 | Ansible resource settings | Templates "contain environment variables" to edit (`getting-started/ansible.rst` step 3). | Values are derived from hardware facts (`ansible/scylla-migrator.yml:88-131`); Spark 4.0.2, OpenJDK 17, cqlsh, AWS CLI installed; services only enabled. | Code (FR-015, FR-016, FR-018, US6-6) | Describe the derivation and that editing is optional. |
| 9 | Submit scripts | Run from `/home/ubuntu/scylla-migrator` (shown with `cd`). | Relative `spark-env`/`config.yaml`; CQL scripts force LOCAL_QUORUM and 4 local connections (overridden by config); event log on; `/tmp/savepoints` created (`ansible/templates/submit-*.sh`). | Code (FR-017, Edge Cases) | Note the working-directory requirement and the defaults. |
| 10 | Docker Compose details | Master/worker, ports 8080/4040, `/jars`, `/app`. | Also worker UI 8081, 2 cores/4 GB, `/parquet-data` mount, Spark 4.0.2/OpenJDK 17 image (`docker-compose.yaml`, `dockerfiles/spark/Dockerfile`). | Code (FR-014a) | Optional mention. |
| 11 | `deploy_spark_cluster.py` | Not in `docs/source/` (README only). | Terraform + Ansible AWS lifecycle helper (`deploy_spark_cluster.py`, `README.md:38-170`). | Code (US9, FR-020) | Add a docs page or link. |
| 12 | Unsupported combinations | Not described as an error. | Migrator/Validator fail with "Unsupported combination of source and target" after config load (`Migrator.scala:110-114`, `Validator.scala:33-37`). | Code (FR-010a) | Mention the error. |

Verified unchanged: entry points, `spark.scylla.config`, standalone master URL, EMR cluster mode,
version log line, `DecodingFailure`/`ParsingFailure` startup errors, executor sizing guidance,
`spark.cores.max`, Docker `/jars` and `/app` mounts and ports 8080/4040, Ansible groups, user,
systemd services and ports, submit script names and default config file names, and the
compatibility matrix (build targets Spark 4.0.2 / Scala 2.13) match the documentation.
