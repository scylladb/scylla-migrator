# CI/CD Speed Improvements TODO

## P0 — High Impact (estimated 3-8 min saved per run)

- [x] **Split format check into its own parallel job**
  - File: `.github/workflows/tests.yml`
  - Currently `scalafmtCheckAll` runs before `build.sh` in the same job. Move it to a separate job so formatting failures are caught in ~30s instead of blocking behind the full build+test pipeline.
  ```yaml
  jobs:
    check-format:
      runs-on: ubuntu-latest
      steps:
        - uses: actions/checkout@v4
        - uses: actions/setup-java@v4
          with: { distribution: temurin, java-version: 8, cache: sbt }
        - uses: sbt/setup-sbt@v1
        - run: sbt scalafmtCheckAll

    test:
      runs-on: ubuntu-latest
      steps:
        # ... existing build + test steps (without scalafmtCheckAll)
  ```

- [x] **Overlap Docker service startup with sbt build**
  - File: `.github/workflows/tests.yml`
  - `docker compose up -d` and `./build.sh` are independent. Start services before the build so containers pull and initialize during compilation. Move the wait scripts to after the build.
  ```yaml
  - name: Start services (background)
    run: |
      sudo chmod -R 777 ./tests/docker/scylla ./tests/docker/scylla-source
      docker compose -f docker-compose-tests.yml up -d
  - name: Build migrator
    run: ./build.sh
  - name: Wait for services
    run: |
      .github/wait-for-port.sh 8000
      # ... rest of wait scripts
  ```

- [x] **Pin Docker image tags to stop cache invalidation**
  - File: `docker-compose-tests.yml`
  - `:latest` tags mean any upstream push invalidates the `ScribeMD/docker-cache`. Pin to specific versions:
  ```yaml
  # Before
  image: cassandra:latest
  image: scylladb/scylla:latest
  image: amazon/dynamodb-local:latest
  image: localstack/localstack:s3-latest

  # After (use current stable versions)
  image: cassandra:4.1
  image: scylladb/scylla:6.2
  image: amazon/dynamodb-local:2.5.3
  image: localstack/localstack:s3-4.3
  ```

- [x] **Deduplicate the sbt assembly build across workflows**
  - Files: `tests.yml`, `tests-aws.yml`, `tutorial-dynamodb.yaml`, new `build.yml`
  - All three workflows independently run `./build.sh` (sbt assembly, ~3-5 min each). Create a reusable workflow that builds once and shares the artifact:
  ```yaml
  # .github/workflows/build.yml
  name: Build
  on:
    workflow_call:
      # no inputs needed
  jobs:
    build:
      runs-on: ubuntu-latest
      steps:
        - uses: actions/checkout@v4
        - uses: actions/setup-java@v4
          with: { distribution: temurin, java-version: 8, cache: sbt }
        - uses: sbt/setup-sbt@v1
        - run: ./build.sh
        - uses: actions/upload-artifact@v4
          with:
            name: migrator-jar
            path: migrator/target/scala-2.13/scylla-migrator-assembly.jar
  ```
  Then in each consumer workflow:
  ```yaml
  jobs:
    build:
      uses: ./.github/workflows/build.yml
    test:
      needs: build
      steps:
        - uses: actions/download-artifact@v4
          with: { name: migrator-jar, path: migrator/target/scala-2.13/ }
        # ... run tests
  ```

## P1 — Medium Impact (estimated 1-3 min saved per run)

- [x] **Skip coverage instrumentation on PRs**
  - File: `.github/workflows/tests.yml`
  - Coverage instrumentation slows compilation and test execution by ~20-40%. Run coverage only on `master` push:
  ```yaml
  - name: Run tests
    run: |
      if [ "${{ github.event_name }}" = "push" ]; then
        sbt coverage "testOnly -- --exclude-categories=com.scylladb.migrator.AWS"
      else
        sbt "testOnly -- --exclude-categories=com.scylladb.migrator.AWS"
      fi
  - name: Generate coverage report
    if: github.event_name == 'push'
    run: sbt coverageReport
  - name: Upload coverage report
    if: github.event_name == 'push'
    uses: actions/upload-artifact@v4
    with:
      name: coverage-report
      path: |
        tests/target/scala-2.13/scoverage-report/
        migrator/target/scala-2.13/scoverage-report/
  ```

- [x] **Pre-build the Spark Docker image and push to GHCR**
  - Files: `docker-compose-tests.yml` (lines 62, 78), new workflow
  - `spark-master` and `spark-worker` use `build: dockerfiles/spark`, triggering a Docker build every CI run. Pre-build and push to a registry:
  ```yaml
  # docker-compose-tests.yml
  spark-master:
    image: ghcr.io/scylladb/scylla-migrator-spark:latest  # pre-built
    # remove: build: dockerfiles/spark
  ```
  Add a workflow that rebuilds and pushes the image only when `dockerfiles/spark/**` changes.

- [x] **Replace `ScribeMD/docker-cache` with more reliable caching**
  - Files: `tests.yml`, `tests-aws.yml`, `tutorial-dynamodb.yaml`
  - `docker-cache` uses `docker save/load` which is slow for large images. With pinned tags (see P0), the built-in GitHub Actions cache with a tag-based key is more efficient:
  ```yaml
  - name: Pull and cache Docker images
    uses: actions/cache@v4
    with:
      path: /tmp/docker-images
      key: docker-${{ runner.os }}-${{ hashFiles('docker-compose-tests.yml') }}
  - name: Load cached images
    run: |
      if [ -d /tmp/docker-images ]; then
        for img in /tmp/docker-images/*.tar; do docker load -i "$img"; done
      fi
  ```
  Or simply rely on pinned images + registry pulls (which are fast on GitHub runners).

## P2 — Lower Impact / Incremental Gains

- [ ] **Use larger GitHub Actions runners**
  - Files: all workflow files
  - If budget allows, switch from `ubuntu-latest` (2 cores) to `ubuntu-latest-4-cores` or `ubuntu-latest-8-cores`. Particularly benefits sbt compilation and Docker operations.
  ```yaml
  runs-on: ubuntu-latest-8-cores  # 4x faster compilation
  ```

- [ ] **Consolidate tutorial test into the main test workflow**
  - Files: `tests.yml`, `tutorial-dynamodb.yaml`
  - Both workflows have identical trigger paths and both build the jar. Make the tutorial a job in `tests.yml` that reuses the build artifact (see P0 dedup item), eliminating a redundant build.

- [ ] **Upgrade `actions/checkout` to v4 in `tests.yml`**
  - File: `.github/workflows/tests.yml` (line 30)
  - Still on `@v3`. v4 is marginally faster. Quick fix:
  ```yaml
  - uses: actions/checkout@v4
  ```

- [ ] **Add `sbt -mem` to reduce GC overhead**
  - `build.sh` already uses `-mem 8192`, but CI runners have limited RAM. Consider tuning to `-mem 4096` to avoid swap thrashing on 7GB runners, or use a larger runner.
