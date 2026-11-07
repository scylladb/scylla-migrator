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
- Before every push, run `make lint-fix` to apply scalafmt, then run `make lint`
  to check formatting. Commit any resulting formatting changes before pushing.
- Push branches to your personal fork and open pull requests from that fork
  against the upstream repository. Do not push PR branches to the upstream repository.
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

<!-- pr-review-bots:start -->
## PR review automation

Every push to a non-draft PR from this repo runs two bots:

1. **Review** (`.github/workflows/pr-code-review.yml`): waits 2 minutes (a newer push cancels the run), then
   runs the `pr-code-reviewer` skill. It posts one review with four passes (reviewer, org reviewer,
   over-engineering, security), each finding tagged 🔴 Blocking, 🟠 Important, 🟡 Minor, 🔵 Nit/Simplify,
   ❓ Question or 💡 Praise.
2. **Resolver** (`.github/workflows/pr-comment-resolver.yml`): runs the `prr` skill to fix open review
   threads and merge conflicts, committing and pushing up to `MAX_ROUNDS` (3) rounds. Each push is reviewed
   again, so the loop is review → fix → review until nothing is left to fix. A loop guard stops both bots after
   `MAX_BOT_RUNS` (5) runs since the last human commit, or after N runs when a `review_max_runs:N` (or, on
   the first push, `review_max_initial_runs:N`) label sets a larger N (99 at most).

Drafts, fork PRs and closed PRs are skipped; Dependabot PRs get only the resolver. Marking a PR as draft
also stops a running resolver after its current round. Helper logic and its tests:
`.github/scripts/pr-resolver-tools.sh`, `.github/scripts/test-pr-resolver-tools.sh`. The bots need the
`ANTHROPIC_API_KEY` or `CLAUDE_CODE_OAUTH_TOKEN` repository secret, and `PR_RESOLVER_TOKEN` when branch rules
keep `github-actions[bot]` from pushing to PR branches.

**Labels** limit both bots, or extend the loop for a large PR. Each gate reads them when it runs; adding or
removing one starts nothing. A label must exist before it can be added (`gh label create <name>`).

| Label | Effect |
|-------|--------|
| `review_stop` | No review and no fixes while set; a running resolver stops after its current round |
| `review_skip_on_open` | No review and no fixes for the PR's first push |
| `review_only` | Review only: every push is still reviewed, but the resolver fixes and resolves nothing, whatever other labels say. It overrides `review_max_*runs`, its loop guard included (the resolver's log reports it as a `review_max_*runs` limit). It applies from the next resolver run; to stop one already running, use `review_stop`. Remove it to have fixes from the next push on |
| `review_max_runs:N` | At most N fix rounds after each push; `0` or the bare label leaves the comments to people. N above 5 raises the loop guard to N |
| `review_max_initial_runs:N` | Same, for the first push only |
| `review_min_severity:<level>` | Resolver fixes only `blocking`, `important`, `minor` or `nit` (default) and up; ❓ and 💡 are always left to people |

A push is a person's: the resolver's own pushes, and pushes of commits authored by Claude Code
(`noreply@anthropic.com`), are fix rounds of the push before them. A change of the PR's base branch is not a
push and does not reset the rounds used. Findings the resolver leaves stay open; a reply on one from someone
with write access has it act on that thread.

**Advisory: pick labels to fit the PR.** No label drops a review pass; the labels limit how many rounds of
fixes the resolver commits on top of your work.

| PR | Suggested labels |
|----|------------------|
| Small or trivial (typo, docs, one-line fix) | `review_max_runs:1` |
| Normal feature or bug fix | No labels, or `review_min_severity:important` to skip nits |
| You want to address the findings yourself | `review_only` |
| Large | `review_max_runs:8` (up to 99) to go past the default 5 runs. Pushing a commit of your own also starts another round of runs |
| Work in progress | Keep it as draft, or set `review_stop` after the first review |

Agents working on a PR: the resolver's commits land on the PR branch, so `git pull` before pushing. Don't
push fixes for the same threads the resolver is working on; set `review_stop` first.
<!-- pr-review-bots:end -->
