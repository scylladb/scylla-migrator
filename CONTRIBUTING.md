# Contributing to scylla-migrator

## Testing procedure

Tests are implemented in the `tests` sbt submodule. They simulate the submission of a Spark job as described in the README. Therefore, before running the tests you need to build the migrator fat-jar and to set up a stack with a Spark cluster and databases to read data from and to write to.

### Prerequisites

Build the migrator fat-jar (re-build every time you change the implementation):

~~~ sh
make build
~~~

### Quick start

Run all local tests (unit + integration) with a single command:

~~~ sh
make test
~~~

This starts Docker services, waits for readiness, runs the tests, and cleans up.

### Test categories

Tests are organized into categories using JUnit `@Category` annotations. The Makefile exposes targets for each:

| Target | Category | Services required | Description |
|--------|----------|-------------------|-------------|
| `make test-unit` | _(none)_ | No | Parsers, encoders, config validation |
| `make test-integration` | `Integration` | Yes | End-to-end migrations with small datasets |
| `make test-integration-aws` | `AWS` | Yes + AWS credentials | Tests that access real AWS services |
| `make test-benchmark-e2e-sanity` | `E2E` | Yes | All migration paths with minimal rows (~2 min) |
| `make test-benchmark-e2e` | `E2E` | Yes | Full throughput benchmarks (5M CQL / 500k DDB rows) |
| `make test-benchmark-jmh` | _(JMH)_ | No | JMH microbenchmarks for hot-path code |

### Running tests manually

1. Start the testing stack:

   ~~~ sh
   make start-services
   make wait-for-services
   ~~~

2. Run the desired tests:

   ~~~ sh
   # Unit tests (no services needed)
   make test-unit

   # Integration tests
   make test-integration

   # A single test class
   sbt "testOnly com.scylladb.migrator.BasicMigrationTest"

   # AWS tests (requires `aws configure` first)
   AWS_REGION=us-east-1 make test-integration-aws
   ~~~

3. Stop the Docker containers:

   ~~~ sh
   make stop-services
   ~~~

### E2E benchmark tests

E2E benchmarks exercise every supported migration path end-to-end: seed data into a source database, run the migrator via Spark, then verify row counts and spot-check data in the target.

**Migration paths covered:**

| Target | Source | Destination |
|--------|--------|-------------|
| `test-benchmark-e2e-cassandra-scylla` | Cassandra | ScyllaDB |
| `test-benchmark-e2e-scylla-scylla` | ScyllaDB | ScyllaDB |
| `test-benchmark-e2e-dynamodb-alternator` | DynamoDB | Alternator |
| `test-benchmark-e2e-scylla-parquet` | ScyllaDB | Parquet files |
| `test-benchmark-e2e-parquet-scylla` | Parquet files | ScyllaDB |
| `test-benchmark-e2e-cassandra-parquet` | Cassandra | Parquet files |
| `test-benchmark-e2e-dynamodb-s3export` | DynamoDB | S3 Export format |
| `test-benchmark-e2e-s3export-alternator` | S3 Export | Alternator |

**Dependencies:** `parquet-scylla` requires `scylla-parquet` to run first (produces the Parquet files), and `s3export-alternator` requires `dynamodb-s3export`. The Makefile encodes these as target dependencies.

**Row counts** are configurable via environment variables:

~~~ sh
# Sanity check (~2 min) — used in CI
make test-benchmark-e2e-sanity

# Custom row counts
make test-benchmark-e2e E2E_CQL_ROWS=100000 E2E_DDB_ROWS=10000

# Full benchmarks (default: 5M CQL, 500k DDB)
make test-benchmark-e2e
~~~

### JMH microbenchmarks

JMH benchmarks measure throughput of critical code paths (serialization, row comparison, value conversion) without requiring external services.

~~~ sh
# Quick smoke run (1 iteration, no warmup)
make test-benchmark-jmh-quick

# Full benchmark run with JSON output
make test-benchmark-jmh
~~~

Results are saved to `benchmarks/results/`.

### CI pipeline

The GitHub Actions workflow (`.github/workflows/tests.yml`) runs on every push to `master` and on pull requests:

1. **Lint** — `make lint` (scalafmt check)
2. **Build** — `make build` (assembly JAR, uploaded as artifact)
3. **Unit tests** — `make test-unit`
4. **Integration tests** — `make test-integration` + `make test-benchmark-e2e-sanity`

The E2E sanity suite runs as part of the integration test job, reusing the same Docker services.

## Debugging

The tests involve the execution of code on several locations:
- locally (ie, on the machine where you invoke `sbt test`): tests initialization and assertions
- on the Spark master node: the `Migrator` entry point
- on the Spark worker node: RDD operations

In all those cases, it is possible to debug them by using the Java Debug Wire Protocol.

### Local Debugging

Follow the procedure documented [here](https://stackoverflow.com/a/15505308/561721).

### Debugging on the Spark Master Node

1. In the file `MigratorSuite.scala`, uncomment the line that sets the
   `spark.driver.extraJavaOptions`.
2. Set up the remote debugger of your IDE to listen to the port 5005.
3. Run a test
4. When the test starts a Spark job, it waits for the remote debugger
5. Start the remote debugger from your IDE.
6. The test execution resumes, and you can interact with it from your debugger.

### Debugging on the Spark Worker Node

1. In the file `MigratorSuite.scala`, uncomment the line that sets the
   `spark.executor.extraJavaOptions`.
2. Set up the remote debugger of your IDE to listen to the port 5006.
3. Run a test
4. When the test starts an RDD operation, it waits for the remote debugger.
   Note that the Spark master node will not display the output of the worker node,
   but you can see it in the worker web UI: http://localhost:8081/.
5. Start the remote debugger from your IDE.
6. The test execution resumes, and you can interact with it from your debugger.

## Release Procedure

Publishing a new release includes building and uploading the release artifacts, and making sure the documentation lists the latest release.

### Building and Uploading the Release Artifacts

Create a new [GitHub release](https://github.com/scylladb/scylla-migrator/releases) off the branch `master`, give it a tag name (please see below), a title, and a description, and then click Publish. A workflow will be automatically triggered and will build the application fat-jar and upload it as a release asset.

Rules for the release tag name:
- Make sure to use tag names like `v1.2.3`, starting with `v` and followed by a [semantic version number](https://semver.org/).
- Bump the major version number if the new release breaks the backward compatibility (e.g., an existing configuration or setup will not work anymore with the new release). In such a case, make sure to also update the compatibility matrix in the documentation.
- Bump the minor version number if the new release introduces new features in a backward compatible manner.
- Bump the patch version number if the new release only introduces bugfixes in a backward compatible manner.

### Updating the Documentation

All the releases (major, minor, and patch releases) are cut off the branch `master`. Since the documentation is versioned by minor release series, we also have a branch corresponding to each minor release series (e.g. `branch-1.0.x`, `branch-1.1.x`, etc.). They are not really branches in the usual sense because they don’t diverge from the branch `master`. You can think of them as labels through the history that point to the latest patch release of each minor series:

~~~ text
* master
|
|                <- unpublished commits
|
* branch-2.0.x   <- latest release in the 2.0.x series
|
|
* branch-1.1.x   <- latest release in the 1.1.x series
|
|
* branch-1.0.x   <- latest release in the 1.0.x series
|
~~~

#### Updating the Documentation After a Patch Release

_Fast-forward-merge_ the branch `master` into the current stable feature-branch. For instance, let’s assume you released version `1.2.5`. The latest feature release was `1.2.0`, and its corresponding branch is `branch-1.2.x`:

~~~ sh
git checkout master
git pull --ff-only origin master
git checkout branch-1.2.x
git pull origin branch-1.2.x
git merge --ff-only master
git push origin branch-1.2.x
~~~

#### Updating the Documentation After a Minor or Major Release

Create a new branch for the minor version and include it in the documentation configuration. For instance, let’s assume you released version `1.2.0`:

1. Create the minor release series branch `branch-1.2.x`:

   ~~~ sh
   git checkout master
   git pull --ff-only origin master
   git branch branch-1.2.x
   git push --set-upstream origin branch-1.2.x
   ~~~

2. Add it to [conf.py](docs/source/conf.py):

   ~~~ diff
     BRANCHES = [
       "master",
       "branch-1.0.x",
       "branch-1.1.x",
   +   "branch-1.2.x",
     ]
   - LATEST_VERSION = "branch-1.1.x"
   + LATEST_VERSION = "branch-1.2.x"
   ~~~

   Commit and push the change (make sure to be on branch `master`):

   ~~~ sh
   git add docs/source/conf.py
   git commit --message="Publish documentation for release 1.2.0"
   git push origin master
   ~~~
