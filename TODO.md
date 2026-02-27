# Test Performance Improvements

## High Impact

- [ ] Split scylla and alternator integration tests into parallel CI jobs
  - Scylla tests need Cassandra + Scylla; alternator tests need DynamoDB Local + Alternator
  - Running as two separate jobs would cut integration test wall time from ~11m to ~6-7m
  - Each job starts only the Docker services it needs

- [ ] Share Spark session across tests in a suite
  - Each test submits a separate `spark-submit` via `docker compose exec` (`SparkUtils.scala`)
  - Spark JVM startup + init is repeated for every test case
  - Sharing a `SparkSession` within a suite would eliminate this overhead

## Medium Impact

- [ ] Start Docker services in parallel with build in CI
  - Docker services (Cassandra, Scylla, DynamoDB) take ~1m18s to start + ~1m10s to become ready
  - Starting `docker compose up` before or alongside `sbt assembly` would overlap service warmup with build
  - Note: naive approach (starting before artifact download) causes network contention — needs a separate job or background step

- [ ] Cache the Spark Docker image in CI
  - `make spark-image` pulls/builds the Spark image on every run
  - Use GitHub Actions Docker layer cache to avoid re-pulling

## Low Impact

- [ ] Use lighter Spark config for integration tests
  - Tests currently use 2 cores + 4GB executor memory (`SparkUtils.scala`)
  - Smaller allocations may reduce per-job Spark init time
