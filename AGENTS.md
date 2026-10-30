# Repository instructions

ScyllaDB Hydra migrates data with Apache Spark. Use Java 17, Scala 2.13, and sbt.
Read `CONTRIBUTING.md` for test procedures.
Read `README.md` for deployment instructions.

## Layout

- `migrator/src/main/scala/com/scylladb/migrator/` contains the application code.
  The entry points are `Migrator` and `Validator`.
  Within that package, `config/` defines the configuration.
  The `readers/` package reads from sources.
  The `writers/` package writes to destinations.
- `spark-kinesis-dynamodb/` contains the DynamoDB Streams adapter for Spark.
- `tests/src/test/scala/` contains the unit and integration tests.
- `tests/src/test/configurations/` contains the migration test fixtures.
- `benchmarks/` contains the JMH benchmarks.
- `docs/source/` contains the user documentation.
- `deploy_spark_cluster.py` and `ansible/` provide cluster deployment.

## Changes

- Follow the nearby Scala code and `.scalafmt.conf`.
  Use two spaces for indentation.
  Limit each line to 100 columns.
- Run `make lint-fix` to format Scala changes. Run `make lint` to check formatting.
- Unless the task requires breaking changes, preserve the existing configuration behavior and compatibility when you resume from savepoints.
- When configuration or user behavior changes, update `docs/source/` and the affected YAML examples.
- When you change dependencies in `build.sbt`, keep the Spark, Hadoop, and AWS SDK versions compatible.

## Validation

- Run commands from the repository root.
  Service commands require Docker Compose, `sudo` for directory permissions, and `curl` to check whether services are ready.
- For unit tests, use Python 3.10 or later with one of these options:
  - Use the `python3` command.
  - Set `PYTHON` to the Python executable.
- After each implementation change, run `make build` before you run tests, as specified in `CONTRIBUTING.md`.
  Migration tests use Spark in local mode.
- Run `make test-unit` for unit tests.
  These tests do not require Docker services.
- Run `make test` for local unit and integration tests.
  This command excludes AWS and E2E tests.
  It starts the services, waits until they are ready, and stops them on exit.
- Before manual integration, E2E, or AWS tests, run `make start-services`.
  Then run `make wait-for-services`.
  After these tests, run `make stop-services`, including after failures.
- For migration changes, while the services are running, also run `make test-benchmark-e2e-sanity`.
- Run one test class with `sbt "testOnly <fully.qualified.TestClass>"`.
  Integration test classes require the services.
- While the services are running, run AWS tests with `AWS_REGION=<region> make test-integration-aws`.
  Provide AWS credentials through one of these options:
  - Use `~/.aws/credentials`.
  - Set `AWS_ACCESS_KEY_ID`.
    Set `AWS_SECRET_ACCESS_KEY`.
    For temporary credentials, also set `AWS_SESSION_TOKEN`.
- Report the checks that you ran and any failures.
  Report the checks that you skipped and the reasons.

## Repository rules

If the repository rule startup hook reports a refresh error, run `$dk-repo-rules refresh`.
After this refresh attempt, read the repository rule index before you change code.
If the refresh fails again, read the repository guidance files directly.
If the refresh fails again, disclose that the private index may be stale.
