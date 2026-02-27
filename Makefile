SHELL := bash
.ONESHELL:
.SHELLFLAGS := -ec

.PHONY: help build docker-build-jar lint lint-fix \
        spark-image start-services stop-services wait-for-services \
        test test-unit test-integration test-integration-aws \
        test-benchmark test-benchmark-jmh test-benchmark-jmh-quick test-benchmark-integration \
        test-benchmark-e2e \
        test-benchmark-e2e-cassandra-scylla test-benchmark-e2e-scylla-scylla test-benchmark-e2e-dynamodb-alternator \
        test-benchmark-e2e-scylla-parquet test-benchmark-e2e-parquet-scylla \
        dump-logs

COMPOSE_FILE := docker-compose-tests.yml
CACHE_REPO ?= scylladb/migrator-cache
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

spark-image: ## Pull or build the Spark Docker image
	$(Q)HASH=$$(find dockerfiles/spark -type f | sort | xargs sha256sum | sha256sum | cut -d' ' -f1 | head -c 16)
	IMAGE="$(CACHE_REPO):spark-$${HASH}"
	echo "Spark cache image: $${IMAGE}"
	if docker pull "$${IMAGE}" 2>/dev/null; then
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
	export SPARK_IMAGE="$${IMAGE}"
	echo "SPARK_IMAGE=$${IMAGE}" >> "$${GITHUB_ENV:-/dev/null}"

start-services: spark-image ## Start all Docker Compose test services
	$(Q)sudo chmod -R 777 ./tests/docker/scylla ./tests/docker/scylla-source
	docker compose -f $(COMPOSE_FILE) up -d

start-services-aws: spark-image ## Start only services needed for AWS tests
	$(Q)sudo chmod -R 777 ./tests/docker/scylla
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

wait-for-services-aws: ## Wait for AWS test services to become ready
	$(Q)$(call wait-for-port,8000)
	$(call wait-for-cql,scylla)
	$(call wait-for-port,8080)
	$(call wait-for-port,8081)

stop-services: ## Stop all Docker Compose test services
	$(Q)docker compose -f $(COMPOSE_FILE) down

dump-logs: ## Dump Docker Compose container logs
	$(Q)docker compose -f $(COMPOSE_FILE) logs

test-unit: ## Run unit tests (no services required)
	$(Q)sbt $(SBT_COVERAGE_PREFIX) "testOnly -- --exclude-categories=com.scylladb.migrator.Integration" $(SBT_COVERAGE_SUFFIX)

test-integration: ## Run integration tests (requires services, excludes AWS, benchmarks, and E2E)
	$(Q)sbt $(SBT_COVERAGE_PREFIX) "testOnly -- --include-categories=com.scylladb.migrator.Integration --exclude-categories=com.scylladb.migrator.AWS,com.scylladb.migrator.Benchmark,com.scylladb.migrator.E2E" $(SBT_COVERAGE_SUFFIX)

test-integration-aws: ## Run AWS integration tests (requires services + AWS credentials)
	$(Q)sbt $(SBT_COVERAGE_PREFIX) "testOnly -- --include-categories=com.scylladb.migrator.AWS" $(SBT_COVERAGE_SUFFIX)

test: start-services ## Run all local tests (unit + integration, excludes AWS)
	$(Q)trap '$(MAKE) dump-logs || true; $(MAKE) stop-services' EXIT
	$(MAKE) wait-for-services
	$(MAKE) test-unit
	$(MAKE) test-integration

test-benchmark-jmh: ## Run all JMH microbenchmarks with JSON output
	$(Q)mkdir -p benchmarks/results
	sbt "benchmarks/Jmh/run -rf json -rff benchmarks/results/jmh-results.json"

test-benchmark-jmh-quick: ## Smoke run JMH benchmarks (1 iteration, no warmup)
	$(Q)mkdir -p benchmarks/results
	sbt "benchmarks/Jmh/run -i 1 -wi 0 -f 1 -rf json -rff benchmarks/results/jmh-results-quick.json"

test-benchmark-integration: ## Run integration throughput benchmarks (requires services)
	$(Q)sbt "testOnly -- --include-categories=com.scylladb.migrator.Benchmark"

test-benchmark: start-services ## Start services and run all benchmarks (JMH + integration)
	$(Q)trap '$(MAKE) dump-logs || true; $(MAKE) stop-services' EXIT
	$(MAKE) wait-for-services
	$(MAKE) test-benchmark-jmh
	$(MAKE) test-benchmark-integration

test-benchmark-e2e: ## Run all E2E throughput benchmarks (requires services)
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) -De2e.ddb.rows=$(E2E_DDB_ROWS) "testOnly -- --include-categories=com.scylladb.migrator.E2E"

test-benchmark-e2e-cassandra-scylla: ## Run Cassandra->Scylla E2E benchmarks
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) "testOnly com.scylladb.migrator.scylla.CassandraToScyllaE2EBenchmark"

test-benchmark-e2e-scylla-scylla: ## Run Scylla->Scylla E2E benchmarks
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) "testOnly com.scylladb.migrator.scylla.ScyllaToScyllaE2EBenchmark"

test-benchmark-e2e-dynamodb-alternator: ## Run DynamoDB->Alternator E2E benchmarks
	$(Q)sbt -De2e.ddb.rows=$(E2E_DDB_ROWS) "testOnly com.scylladb.migrator.alternator.DynamoDBToAlternatorE2EBenchmark"

test-benchmark-e2e-scylla-parquet: ## Run Scylla->Parquet E2E benchmark
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) "testOnly com.scylladb.migrator.scylla.ScyllaToParquetE2EBenchmark"

test-benchmark-e2e-parquet-scylla: ## Run Parquet->Scylla E2E benchmark
	$(Q)sbt -De2e.cql.rows=$(E2E_CQL_ROWS) "testOnly com.scylladb.migrator.scylla.ParquetToScyllaE2EBenchmark"
