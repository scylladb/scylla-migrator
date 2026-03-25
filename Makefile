SHELL := bash
.ONESHELL:
.SHELLFLAGS := -ec

.PHONY: help build docker-build-jar docker-image lint lint-fix \
        spark-image start-services stop-services wait-for-services \
        start-services-scylla wait-for-services-scylla \
        start-services-cassandra wait-for-services-cassandra test-integration-cassandra \
        start-services-alternator wait-for-services-alternator \
        test test-unit test-integration test-integration-scylla \
        test-integration-alternator \
        test-integration-aws \
        test-benchmark test-benchmark-jmh test-benchmark-jmh-quick \
        test-benchmark-e2e test-benchmark-e2e-sanity test-benchmark-e2e-sanity-scylla \
        test-benchmark-e2e-cassandra-scylla test-benchmark-e2e-scylla-scylla test-benchmark-e2e-dynamodb-alternator \
        test-benchmark-e2e-scylla-parquet test-benchmark-e2e-parquet-scylla \
        test-benchmark-e2e-cassandra-parquet \
        test-benchmark-e2e-dynamodb-s3export test-benchmark-e2e-s3export-alternator \
        dump-logs

COMPOSE_FILE := docker-compose-tests.yml
CACHE_REPO ?= scylladb/migrator-cache
DOCKER_IMAGE ?= scylladb/scylla-migrator
DOCKER_TAG ?= latest
MAX_ATTEMPTS ?= 480
COVERAGE ?= false
VERBOSE ?= false
E2E_CQL_ROWS ?= 5000000
E2E_DDB_ROWS ?= 500000

ifeq ($(COVERAGE),true)
SBT_COVERAGE_PREFIX := coverage
SBT_COVERAGE_SUFFIX := coverageReport
else
SBT_COVERAGE_PREFIX :=
SBT_COVERAGE_SUFFIX :=
endif

DOCKER_SPARK_BIND_DIRS := ./tests/docker/parquet ./tests/docker/spark-master ./tests/docker/aws-profile
DOCKER_SCYLLA_BIND_DIRS := ./tests/docker/scylla ./tests/docker/scylla-source ./tests/docker/cassandra5 ./tests/docker/cassandra3 ./tests/docker/cassandra2

# Parameterized Cassandra version for per-version testing (2, 3, 4, or 5)
CASSANDRA_VERSION ?= 4

# Suppress command echo unless VERBOSE=true
ifeq ($(VERBOSE),true)
Q :=
else
Q := @
endif

help: ## Show this help
	$(Q)grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

# Retry a command up to MAX_ATTEMPTS times with 1s delay
define attempt
	attempts=0
	while ! $(1) ; do
		[[ $$attempts -ge $(MAX_ATTEMPTS) ]] && echo "Failed!" && exit 1
		attempts=$$((attempts+1))
		sleep 1
		echo "waiting... ($${attempts}/$(MAX_ATTEMPTS))"
	done
endef

define wait-for-port
	echo "Waiting for successful HTTP status code on port $(1)"
	$(call attempt,curl -s "http://127.0.0.1:$(1)" > /dev/null)
endef

define wait-for-cql
	echo "Waiting for CQL to be ready in service $(1)"
	$(call attempt,docker compose -f $(COMPOSE_FILE) exec $(1) bash -c "cqlsh -e 'describe cluster'" > /dev/null)
endef

lint: ## Check code formatting
	$(Q)sbt scalafmtCheckAll

lint-fix: ## Auto-fix code formatting
	$(Q)sbt scalafmtAll

build: ## Build assembly JAR
	$(Q)export TERM=xterm-color
	sbt -mem 8192 migrator/assembly

# Local-dev convenience: build the assembly JAR inside Docker without requiring
# a local JDK/sbt installation. Not used by CI workflows.
docker-build-jar: ## Build assembly JAR using Docker (local-dev convenience)
	$(Q)docker buildx build --file Dockerfile --output type=local,dest=./migrator/target/scala-2.13 .

docker-image: ## Build the migrator Docker image
	$(Q)docker buildx build --file Dockerfile.release -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

spark-image: ## Pull or build the Spark Docker image
	$(Q)HASH=$$(find dockerfiles/spark -type f | sort | xargs sha256sum | sha256sum | cut -d' ' -f1 | head -c 16)
	IMAGE="$(CACHE_REPO):spark-$${HASH}"
	echo "Spark cache image: $${IMAGE}"
	if docker image inspect "$${IMAGE}" > /dev/null 2>&1; then
		echo "Image already loaded locally"
	elif docker pull "$${IMAGE}" 2>/dev/null; then
		echo "Cache hit — pulled $${IMAGE}"
	else
		echo "Cache miss — building from dockerfiles/spark"
		docker build -t "$${IMAGE}" dockerfiles/spark
		if [ -n "$${DOCKERHUB_USERNAME:-}" ] && [ -n "$${DOCKERHUB_TOKEN:-}" ]; then
			echo "$${DOCKERHUB_TOKEN}" | docker login -u "$${DOCKERHUB_USERNAME}" --password-stdin
			docker push "$${IMAGE}"
			echo "Pushed $${IMAGE} to cache"
		else
			echo "No Docker Hub credentials — skipping push"
		fi
	fi
	docker tag "$${IMAGE}" spark-migrator
	echo "SPARK_IMAGE=$${IMAGE}" >> "$${GITHUB_ENV:-/dev/null}"

start-services: spark-image ## Start all Docker Compose test services
	$(Q)mkdir -p $(DOCKER_SPARK_BIND_DIRS) $(DOCKER_SCYLLA_BIND_DIRS)
	$(Q)sudo chmod -R 777 $(DOCKER_SPARK_BIND_DIRS) $(DOCKER_SCYLLA_BIND_DIRS)
	docker compose -f $(COMPOSE_FILE) up -d

start-services-scylla: spark-image ## Start services needed for Scylla integration tests
	$(Q)mkdir -p $(DOCKER_SPARK_BIND_DIRS) $(DOCKER_SCYLLA_BIND_DIRS)
	$(Q)sudo chmod -R 777 $(DOCKER_SPARK_BIND_DIRS) $(DOCKER_SCYLLA_BIND_DIRS)
	docker compose -f $(COMPOSE_FILE) up -d cassandra cassandra5 cassandra3 cassandra2 scylla-source scylla spark-master spark-worker

start-services-alternator: spark-image ## Start services needed for Alternator integration tests
	$(Q)mkdir -p $(DOCKER_SPARK_BIND_DIRS) ./tests/docker/scylla
	$(Q)sudo chmod -R 777 $(DOCKER_SPARK_BIND_DIRS) ./tests/docker/scylla
	docker compose -f $(COMPOSE_FILE) up -d dynamodb scylla s3 spark-master spark-worker

start-services-aws: spark-image ## Start only services needed for AWS tests
	$(Q)mkdir -p $(DOCKER_SPARK_BIND_DIRS) ./tests/docker/scylla
	$(Q)sudo chmod -R 777 $(DOCKER_SPARK_BIND_DIRS) ./tests/docker/scylla
	docker compose -f $(COMPOSE_FILE) up -d scylla spark-master spark-worker

wait-for-services: ## Wait for all test services to become ready
	$(Q)$(call wait-for-port,8000)
	$(call wait-for-port,8001)
	$(call wait-for-port,4566)
	$(call wait-for-cql,scylla)
	$(call wait-for-cql,cassandra)
	$(call wait-for-cql,scylla-source)
	$(call wait-for-port,8080)
	$(call wait-for-port,8081)

wait-for-services-scylla: ## Wait for Scylla test services to become ready
	$(Q)$(call wait-for-cql,cassandra)
	$(call wait-for-cql,cassandra5)
	$(call wait-for-cql,cassandra3)
	$(call wait-for-cql,cassandra2)
	$(call wait-for-cql,scylla-source)
	$(call wait-for-cql,scylla)
	$(call wait-for-port,8080)
	$(call wait-for-port,8081)

wait-for-services-alternator: ## Wait for Alternator test services to become ready
	$(Q)docker compose -f $(COMPOSE_FILE) logs dynamodb scylla s3 spark-master spark-worker
	$(call wait-for-port,8000)
	$(call wait-for-port,8001)
	$(call wait-for-port,4566)
	$(call wait-for-cql,scylla)
	$(call wait-for-port,8080)
	$(call wait-for-port,8081)

wait-for-services-aws: ## Wait for AWS test services to become ready
	$(Q)$(call wait-for-port,8000)
	$(call wait-for-cql,scylla)
	$(call wait-for-port,8080)
	$(call wait-for-port,8081)

stop-services: ## Stop all Docker Compose test services
	$(Q)docker compose -f $(COMPOSE_FILE) down

dump-logs: ## Dump Docker Compose container logs
	$(Q)docker compose -f $(COMPOSE_FILE) logs

# --- Per-version Cassandra source testing (CASSANDRA_VERSION=2|3|4|5) ---
# Docker service name: "cassandra" for v4, "cassandraN" for others
ifeq ($(CASSANDRA_VERSION),4)
_CASSANDRA_SERVICE := cassandra
else
_CASSANDRA_SERVICE := cassandra$(CASSANDRA_VERSION)
endif

start-services-cassandra: spark-image ## Start services for a single Cassandra version (CASSANDRA_VERSION=...)
	$(Q)mkdir -p $(DOCKER_SPARK_BIND_DIRS) ./tests/docker/scylla ./tests/docker/$(_CASSANDRA_SERVICE)
	$(Q)sudo chmod -R 777 $(DOCKER_SPARK_BIND_DIRS) ./tests/docker/scylla ./tests/docker/$(_CASSANDRA_SERVICE)
	docker compose -f $(COMPOSE_FILE) up -d $(_CASSANDRA_SERVICE) scylla spark-master spark-worker

wait-for-services-cassandra: ## Wait for a single Cassandra version to become ready (CASSANDRA_VERSION=...)
	$(Q)$(call wait-for-cql,$(_CASSANDRA_SERVICE))
	$(call wait-for-cql,scylla)
	$(call wait-for-port,8080)
	$(call wait-for-port,8081)

test-integration-cassandra: ## Run integration tests for a single Cassandra version (CASSANDRA_VERSION=...)
	$(Q)sbt $(SBT_COVERAGE_PREFIX) "testOnly com.scylladb.migrator.scylla.Cassandra$(CASSANDRA_VERSION)* -- --include-categories=com.scylladb.migrator.Integration --exclude-categories=com.scylladb.migrator.E2E" $(SBT_COVERAGE_SUFFIX)

test-unit: ## Run unit tests (no services required)
	$(Q)sbt $(SBT_COVERAGE_PREFIX) "testOnly -- --exclude-categories=com.scylladb.migrator.Integration" $(SBT_COVERAGE_SUFFIX)

test-integration: ## Run integration tests (requires services, excludes AWS, benchmarks, and E2E)
	$(Q)sbt $(SBT_COVERAGE_PREFIX) "testOnly -- --include-categories=com.scylladb.migrator.Integration --exclude-categories=com.scylladb.migrator.AWS,com.scylladb.migrator.E2E" $(SBT_COVERAGE_SUFFIX)

test-integration-scylla: ## Run all Scylla integration tests (requires all CQL sources running)
	$(Q)sbt $(SBT_COVERAGE_PREFIX) "testOnly com.scylladb.migrator.scylla.* -- --include-categories=com.scylladb.migrator.Integration --exclude-categories=com.scylladb.migrator.E2E" $(SBT_COVERAGE_SUFFIX)

test-integration-alternator: ## Run Alternator integration tests only (excludes AWS and E2E benchmarks)
	$(Q)sbt $(SBT_COVERAGE_PREFIX) "testOnly com.scylladb.migrator.alternator.* com.scylladb.migrator.writers.* -- --include-categories=com.scylladb.migrator.Integration --exclude-categories=com.scylladb.migrator.AWS,com.scylladb.migrator.E2E" $(SBT_COVERAGE_SUFFIX)

test-integration-aws: ## Run AWS integration tests (requires services + AWS credentials)
	$(Q)sbt $(SBT_COVERAGE_PREFIX) "testOnly -- --include-categories=com.scylladb.migrator.AWS" $(SBT_COVERAGE_SUFFIX)

test: start-services ## Run all local tests (unit + integration, excludes AWS)
	$(Q)trap '$(MAKE) dump-logs || true; $(MAKE) stop-services' EXIT
	$(MAKE) wait-for-services
	$(MAKE) test-unit
	$(MAKE) test-integration

test-benchmark-jmh: ## Run all JMH microbenchmarks with JSON output
	$(Q)mkdir -p benchmarks/results
	$(Q)sbt "benchmarks/Jmh/run -rf json -rff benchmarks/results/jmh-results.json"

test-benchmark-jmh-quick: ## Smoke run JMH benchmarks (1 iteration, no warmup)
	$(Q)mkdir -p benchmarks/results
	$(Q)sbt "benchmarks/Jmh/run -i 1 -wi 0 -f 1 -rf json -rff benchmarks/results/jmh-results-quick.json"

test-benchmark: start-services ## Start services and run all benchmarks (JMH + E2E)
	$(Q)trap '$(MAKE) dump-logs || true; $(MAKE) stop-services' EXIT
	$(MAKE) wait-for-services
	$(MAKE) test-benchmark-jmh
	$(MAKE) test-benchmark-e2e

test-benchmark-e2e-sanity: ## Run E2E sanity suite (small row counts, ~2min, for CI)
	$(Q)$(MAKE) test-benchmark-e2e E2E_CQL_ROWS=1000 E2E_DDB_ROWS=100

test-benchmark-e2e-sanity-scylla: ## Run Scylla-only E2E sanity (no DynamoDB services needed)
	$(Q)$(MAKE) test-benchmark-e2e-cassandra-scylla E2E_CQL_ROWS=1000
	$(Q)$(MAKE) test-benchmark-e2e-scylla-scylla E2E_CQL_ROWS=1000
	$(Q)$(MAKE) test-benchmark-e2e-scylla-parquet E2E_CQL_ROWS=1000
	$(Q)$(MAKE) test-benchmark-e2e-parquet-scylla E2E_CQL_ROWS=1000
	$(Q)$(MAKE) test-benchmark-e2e-cassandra-parquet E2E_CQL_ROWS=1000

test-benchmark-e2e: ## Run all E2E throughput benchmarks (requires services)
	# Sequential execution is required: parquet-scylla depends on scylla-parquet output,
	# and s3export-alternator depends on dynamodb-s3export output.
	$(Q)$(MAKE) test-benchmark-e2e-cassandra-scylla
	$(Q)$(MAKE) test-benchmark-e2e-scylla-scylla
	$(Q)$(MAKE) test-benchmark-e2e-dynamodb-alternator
	$(Q)$(MAKE) test-benchmark-e2e-scylla-parquet
	$(Q)$(MAKE) test-benchmark-e2e-parquet-scylla
	$(Q)$(MAKE) test-benchmark-e2e-cassandra-parquet
	$(Q)$(MAKE) test-benchmark-e2e-dynamodb-s3export
	$(Q)$(MAKE) test-benchmark-e2e-s3export-alternator

test-benchmark-e2e-cassandra-scylla: ## Run Cassandra->Scylla E2E benchmarks
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) "testOnly com.scylladb.migrator.scylla.CassandraToScyllaE2EBenchmark"

test-benchmark-e2e-scylla-scylla: ## Run Scylla->Scylla E2E benchmarks
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) "testOnly com.scylladb.migrator.scylla.ScyllaToScyllaE2EBenchmark"

test-benchmark-e2e-dynamodb-alternator: ## Run DynamoDB->Alternator E2E benchmarks
	$(Q)sbt -De2e.ddb.rows=$(E2E_DDB_ROWS) "testOnly com.scylladb.migrator.alternator.DynamoDBToAlternatorE2EBenchmark"

test-benchmark-e2e-scylla-parquet: ## Run Scylla->Parquet E2E benchmark
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) "testOnly com.scylladb.migrator.scylla.ScyllaToParquetE2EBenchmark"

test-benchmark-e2e-parquet-scylla: ## Run Parquet->Scylla E2E benchmark (requires scylla-parquet to have run first)
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) "testOnly com.scylladb.migrator.scylla.ParquetToScyllaE2EBenchmark"

test-benchmark-e2e-cassandra-parquet: ## Run Cassandra->Parquet E2E benchmark
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) "testOnly com.scylladb.migrator.scylla.CassandraToParquetE2EBenchmark"

test-benchmark-e2e-dynamodb-s3export: ## Run DynamoDB->S3Export E2E benchmark
	$(Q)sbt -De2e.ddb.rows=$(E2E_DDB_ROWS) "testOnly com.scylladb.migrator.alternator.DynamoDBToS3ExportE2EBenchmark"

test-benchmark-e2e-s3export-alternator: ## Run S3Export->Alternator E2E benchmark (requires dynamodb-s3export to have run first)
	$(Q)sbt -De2e.ddb.rows=$(E2E_DDB_ROWS) "testOnly com.scylladb.migrator.alternator.S3ExportToAlternatorE2EBenchmark"
