#!/usr/bin/env bash
# Narrow GitHub operations for the PR review and comment-resolver workflows.
# Claude may call only this script for GitHub writes, so it can post reviews (never
# APPROVE), reply to and resolve review threads on this PR, and nothing else.
#
#   open-work            print "<unanswered threads> <conflict: yes|no|protected>"; protected (a
#                        conflict in a protected path, left to a person) is not work, nor is
#                        anything on a draft or with the review_stop label; with
#                        --output, print pending=, conflict= and work=true|false lines for
#                        $GITHUB_OUTPUT
#   threads              write unresolved review threads, one JSON object per line, to a
#                        file under /tmp and print its path; a comment by a user without
#                        write access is marked "trusted": false and its body hidden; outside
#                        the review job, without findings left to people (see THREAD_DEFS)
#   reply <id> <file>    reply to the review comment with database id <id>; text from <file>
#   resolve <thread-id>  resolve the review thread with GraphQL node id <thread-id>
#   permission <login>   print the user's permission on the repo (admin|write|read|none)
#   review <json-file>   post a review (event COMMENT or REQUEST_CHANGES only)
#   missing-passes       print the review passes not yet posted on the PR head commit for
#                        its base branch (each pass is a section of the combined review)
#   bot-runs             print how many commits made since the last human commit have a
#                        completed run of the calling workflow (the review loop guard)
#   limits               print skip=, guard=, left= and severity= from the PR's review_ labels for
#                        this push
#                        (see limits below)
#   scan-push            fail if unpushed commits not on the base branch carry a credential,
#                        add a submodule, change a protected path (see protected= below) or
#                        merge any branch but the base
#   merge-base           fetch the base branch and merge it without committing, so the base
#                        ref never reaches a shell command line (git merge --abort undoes it)
#   push                 scan-push, then push HEAD to the PR branch with PUSH_TOKEN, which only
#                        the workflow's push steps set
# reply and review read their file only from under /tmp, and start what they post with the
# poster's role in bold: "ScyllaDB Reviewer" in the review job, otherwise "ScyllaDB Automated
# PR Resolver" (see role below).
# Tests: .github/scripts/test-pr-resolver-tools.sh
set -euo pipefail
# Run system binaries only: an agent may have written a git or gh earlier on PATH, and the push
# runs with PUSH_TOKEN. The tests point PR_TOOLS_PATH at their stubs and reach a local origin
# through git config in the environment; push ignores the first, and in Actions, where the
# tests unset GITHUB_ACTIONS, nothing reads it and push ignores the second as well.
if [ "${1:-}" = push ] || [ -n "${GITHUB_ACTIONS:-}" ]; then unset PR_TOOLS_PATH; fi
if [ "${1:-}" = push ] && [ -n "${GITHUB_ACTIONS:-}" ]; then
  unset GIT_CONFIG_PARAMETERS GIT_CONFIG_COUNT
fi
export PATH="${PR_TOOLS_PATH:-/usr/bin:/bin}"
# Every git call here may carry a token: read no global or system git config (insteadOf,
# hooks, diff drivers), and ignore replace refs (git log follows them, push does not).
export GIT_NO_REPLACE_OBJECTS=1 GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

REPO="${GITHUB_REPOSITORY:?}"
PR_NUMBER=$(jq -r .pull_request.number "${GITHUB_EVENT_PATH:?}")
# The resolver job sets BASE_REF and HEAD_REF from the PR's live state, which can differ from
# the event if the PR was retargeted since; the review job reads the event.
BASE_REF=${BASE_REF:-$(jq -r .pull_request.base.ref "$GITHUB_EVENT_PATH")}
HEAD_REF=${HEAD_REF:-$(jq -r .pull_request.head.ref "$GITHUB_EVENT_PATH")}
HEAD_SHA=$(jq -r .pull_request.head.sha "$GITHUB_EVENT_PATH")
# Credential prefixes refused in pushed lines and paths. A prefix must not follow a letter
# or digit (B), so words such as highs_ do not match; a diff marker counts as a boundary.
B='(^|[^A-Za-z0-9])'
PFX='(gh[psour]_|github_pat_|sk-ant-)'
# Pushed lines are stricter than reviews on purpose: one that only names x-access-token, even
# with a placeholder, is refused and left to a person.
CRED_RE="(${B}${PFX}|x-access-token)"
# Full credential values (30+ characters, matched even right after a letter), refused
# everywhere, including review text where prose may name a prefix. Only the gh*_ form with
# underscores, which snake_case can mimic, needs B; placeholders after x-access-token: are
# not values ($VAR, "quoted", {braced}, <angled>, `code`, $(command)).
CRED_VALUE_RE="(gh[psour]_[A-Za-z0-9]{30,}|${B}gh[psour]_[A-Za-z0-9_]{30,}|github_pat_[A-Za-z0-9_]{30,}|sk-ant-[A-Za-z0-9_-]{30,}|x-access-token:[^\$%<*[{(\"'\`@[:space:]][^@[:space:]]{7,})"

# The role named at the start of every review and reply, from the Actions job id (the agents
# cannot change it): the review job is the reviewer, the resolver's job is the resolver.
role() { [ "${GITHUB_JOB:-}" = review ] && echo "ScyllaDB Reviewer" || echo "ScyllaDB Automated PR Resolver"; }
# Prefix $1 with the bold role line unless it already starts with it (a retried post).
with_role() { local h="**$(role)**"; [[ "$1" == "$h"* ]] && printf '%s' "$1" || printf '%s\n\n%s' "$h" "$1"; }

# The contents of $1, which must be a regular file under /tmp, where agents write. Elsewhere
# it could be a file the agents may not read, such as /proc/self/environ. The file is opened
# once and the open file is the one checked and read, so it cannot be swapped in between.
tmp_read() {
  local f
  if [ -f "$1" ] && exec 3<"$1" && f=$(realpath -e /proc/self/fd/3) && [[ "$f" == /tmp/* ]] &&
    [ -f /proc/self/fd/3 ]; then
    cat <&3
    exec 3<&-
    return
  fi
  echo "refused: $1 is not a file under /tmp" >&2
  exit 2
}

# Run git against GitHub with token $1 passed to this command only. The checkouts do not
# persist credentials; claude-code-action puts the job token, which agents already hold as
# GH_TOKEN, in the origin URL, but PUSH_TOKEN never reaches .git/config.
git_auth() {
  local auth
  auth=$(printf 'x-access-token:%s' "$1" | base64 -w0)
  shift
  git -c core.hooksPath=/dev/null -c core.fsmonitor=false -c http.https://github.com/.extraheader= \
    -c "http.https://github.com/.extraheader=AUTHORIZATION: basic $auth" "$@"
}
# Fetch branches $@ from GitHub into their origin/ tracking refs, force-updating them. The URL
# is named here, not taken from the checkout's origin remote.
fetch() {
  local b refspecs=()
  for b; do refspecs+=("+refs/heads/$b:refs/remotes/origin/$b"); done
  git_auth "${GH_TOKEN:?}" fetch -q "https://github.com/$REPO.git" "${refspecs[@]}"
}

# The workflows run from .github/; git reads .gitmodules and, at any depth, .gitattributes
# (merge drivers) and .lfsconfig (where LFS objects go); Claude reads .mcp.json and, at any depth, .claude/, CLAUDE.md,
# CLAUDE.local.md, AGENTS.md and .agents/; .husky/, .vscode/ and, at any depth,
# .devcontainer/, .devcontainer.json and .cursor/ hold hooks and tasks that run on developers' machines; and .env*,
# .npmrc and bunfig.toml configure tools there. In any letter case, as on macOS. The pushed
# head must leave them as the PR branch has them, or as a clean merge of the base has them.
# Keep in sync with the conflict regex below, the Edit denies in agent-settings.json and the
# do-not-edit lists in the pr-comment-resolver.yml prompts.
protected=(.github .gitmodules .mcp.json .husky .vscode ':(glob)**/.claude' ':(glob)**/.claude/**'
  ':(glob)**/.devcontainer' ':(glob)**/.devcontainer/**'
  ':(glob)**/.cursor' ':(glob)**/.cursor/**' ':(glob)**/.agents' ':(glob)**/.agents/**'
  ':(glob)**/CLAUDE.md' ':(glob)**/CLAUDE.local.md' ':(glob)**/AGENTS.md'
  ':(glob)**/.env*' ':(glob)**/.npmrc' ':(glob)**/bunfig.toml' ':(glob)**/.bunfig.toml'
  ':(glob)**/.gitattributes' ':(glob)**/.devcontainer.json' ':(glob)**/.lfsconfig')
# The same names as file paths, for the files merge-tree lists as conflicted; a file/directory
# conflict lists the directory's files or renames the file to <name>~<branch>.
protected_re='^"?((\.github|\.husky|\.vscode)(/|"?$)|(\.gitmodules|\.mcp\.json)"?$)|^"?(.*/)?((\.claude|\.cursor|\.agents|\.devcontainer)(/|"?$)|((CLAUDE|CLAUDE\.local|AGENTS)\.md|\.env[^/]*|\.npmrc|\.?bunfig\.toml|\.gitattributes|\.devcontainer\.json|\.lfsconfig)(/|~|"?$))'

# All review threads, every page; one JSON thread per line.
all_threads() {
  gh api graphql --paginate -F owner="${REPO%/*}" -F name="${REPO#*/}" -F pr="$PR_NUMBER" -f query='
    query($owner:String!,$name:String!,$pr:Int!,$endCursor:String){
      repository(owner:$owner,name:$name){pullRequest(number:$pr){
        reviewThreads(first:100,after:$endCursor){
          pageInfo{hasNextPage endCursor}
          nodes{id isResolved path line
            root: comments(first:1){nodes{databaseId body author{login __typename}}}
            comments(last:100){totalCount nodes{databaseId body author{login __typename}}}}}}}}' \
    --jq '.data.repository.pullRequest.reviewThreads.nodes[]'
}

# The resolver acts only on comments by bots (author __typename "Bot") and by users with write,
# maintain or admin permission. Mark each comment of the threads on stdin with "trusted" and
# replace an untrusted comment's body, so the agents never read text they must not act on.
mark_trust() {
  local threads login p perms='{}'
  threads=$(cat)
  while IFS= read -r login; do
    [[ "$login" =~ ^[A-Za-z0-9-]+$ ]] || continue
    p=$(gh api "repos/$REPO/collaborators/$login/permission" --jq .permission 2>/dev/null) || p=none
    perms=$(jq -c --arg l "$login" --arg p "$p" '.[$l] = $p' <<<"$perms")
  done < <(jq -r '.root.nodes[]?, .comments.nodes[] | .author | select(. != null and .__typename != "Bot")
    | .login' <<<"$threads" | sort -u)
  jq -c --argjson perms "$perms" '
    def mark: .trusted = (.author.__typename == "Bot"
        or ($perms[.author.login // ""] // "none" | IN("admin", "maintain", "write")))
      | if .trusted then . else .body = "[hidden: comment by a user without write access]" end;
    (.root.nodes[]?, .comments.nodes[]) |= mark' <<<"$threads"
}

# The reviewer's severity tags the resolver leaves to people: ❓ Question and 💡 Praise, which ask
# for no change, and below the level of a review_min_severity label (see limits), which the
# resolve job passes in MIN_SEVERITY. 🔵 is both Nit and Simplify.
case "${MIN_SEVERITY:-}" in
  blocking) LOW='["🟠", "🟡", "🔵", "❓", "💡"]' ;;
  important) LOW='["🟡", "🔵", "❓", "💡"]' ;;
  minor) LOW='["🔵", "❓", "💡"]' ;;
  *) LOW='["❓", "💡"]' ;;
esac
# jq for threads marked by mark_trust, given $low (LOW). gha: a comment by the Actions bot
# (GraphQL reports bot logins without the "[bot]" suffix; accept both). low: the tag that starts
# the reviewer's finding (after its role line and pass names) is in $low; the resolver leaves
# those to people unless one with write access asks on the thread. A finding without a leading
# tag is not low, whatever its text mentions. unanswered: after the Actions bot's last
# reply (for a low thread, the finding itself) there is a trusted comment by someone else, and
# on a low thread by a person, not another bot; with no reply, any trusted comment. Comments
# holds the last 100, so its first is the root only when there are no more.
THREAD_DEFS='def gha: (.author.login // "" | sub("\\[bot\\]$"; "")) == "github-actions";
  def low: .root.nodes[0] // {} | gha and
    ((.body // "" | capture("^(\\*\\*[^*\n]+\\*\\*\\s+)?(_[^_\n]+_ · )*(?<t>[🔴🟠🟡🔵❓💡])").t // "") | IN($low[]));
  def unanswered: low as $is_low | .comments as $c
    | ([$c.nodes | to_entries[] | select((.key > 0 or $c.totalCount > 100 or $is_low) and (.value | gha)) | .key]
       | last // (if $is_low then -1 else null end)) as $i
    | if $i == null then any($c.nodes[], .root.nodes[]?; .trusted)
      else any($c.nodes[$i + 1:][]; .trusted and (gha | not) and (($is_low | not) or .author.__typename != "Bot")) end;'

# The PR's commits from the last human commit on (all of them when none is), oldest first, as
# "<sha> human|bot": claude[bot] (the resolver), github-actions[bot] and Claude Code
# (noreply@anthropic.com) are not people; Dependabot's commits count as the PR author's.
# GitHub lists at most 250 commits of a PR.
chain() {
  gh api --paginate "repos/$REPO/pulls/$PR_NUMBER/commits" \
    --jq '.[] | [.sha, .commit.author.name, .commit.author.email] | @tsv' |
    awk -F'\t' '{ bot = $2 ~ /^(claude|github-actions)\[bot\]$/ || $3 == "noreply@anthropic.com"; if (!bot) n = 0
      s[n++] = $1 (bot ? " bot" : " human") } END { for (i = 0; i < n; i++) print s[i] }'
}
# The calling workflow's runs on the PR branch, one JSON object per line.
branch_runs() {
  local wf branch
  wf=$(gh api "repos/$REPO/actions/runs/${GITHUB_RUN_ID:?}" --jq .workflow_id)
  branch=$(jq -rn --arg b "$HEAD_REF" '$b | @uri')
  gh api --paginate "repos/$REPO/actions/workflows/$wf/runs?event=pull_request&branch=$branch&per_page=100" \
    --jq '.workflow_runs[]'
}
# A finished run that did work, cancelled ones too: the resolver's own push cancels the run that
# made it. Only a run cancelled within 150 seconds (by a newer push during the gate's 2-minute
# wait) did no work. run_started_at is when the run starts, not when its gate gets a runner, so
# a gate kept waiting for one can make a push that was replaced count as one that did work.
WORKED='def worked: .conclusion != null and .conclusion != "skipped" and
  (.conclusion != "cancelled" or (.updated_at | fromdate) - (.run_started_at | fromdate) > 150);'

case "${1:-}" in
  open-work)
    # A draft or the stop label leaves no work: the resolver stops after the round in progress,
    # and so does Claude's own loop, which runs open-work between fixes.
    pr=$(gh api "repos/$REPO/pulls/$PR_NUMBER")
    if [ "$(jq -r '.draft or any(.labels[]; .name == "review_stop")' <<<"$pr")" = true ]; then
      echo "the PR is a draft or has the review_stop label: no work" >&2
      if [ "${2:-}" = --output ]; then printf 'pending=0\nconflict=no\nwork=false\n'; else echo "0 no"; fi
      exit 0
    fi
    # Unresolved threads that are unanswered (see THREAD_DEFS).
    pending=$(all_threads | jq -c 'select(.isResolved | not)' | mark_trust |
      jq -s --argjson low "$LOW" "$THREAD_DEFS"' [.[] | select(unanswered)] | length')
    # Check for conflicts locally instead of trusting GitHub's lazily computed mergeable flag.
    fetch "$BASE_REF" "$HEAD_REF"
    rc=0
    out=$(git -c core.quotePath=false merge-tree --write-tree --no-messages --name-only \
      "refs/remotes/origin/$HEAD_REF" "refs/remotes/origin/$BASE_REF") || rc=$?
    case $rc in
      0) conflict=no ;;
      # The resolver must leave a conflict in a protected path to a person, so another round
      # could only abort the merge again. The first line is the tree id, which never matches.
      1) if grep -qiE "$protected_re" <<<"$out"; then conflict=protected; else conflict=yes; fi ;;
      *) echo "git merge-tree failed ($rc)" >&2; exit "$rc" ;;
    esac
    if [ "${2:-}" = --output ]; then
      work=$([ "$pending" -gt 0 ] || [ "$conflict" = yes ] && echo true || echo false)
      echo "unanswered threads=$pending conflict=$conflict" >&2
      printf 'pending=%s\nconflict=%s\nwork=%s\n' "$pending" "$conflict" "$work"
    else
      echo "$pending $conflict"
    fi
    ;;
  threads)
    # To a file: output over about 30 KB is saved where the agents may not read it, and a PR
    # with many long bot comments passes that. One compact thread per line, so the agents can
    # Read it in parts; root repeats the first comment unless the thread has over 100. The
    # resolver does not get low threads nobody asked on (see THREAD_DEFS); the review job,
    # which reads them to avoid duplicate findings, gets every thread.
    out=$(mktemp /tmp/pr-threads.XXXXXX)
    all_threads | jq -c 'select(.isResolved | not)' | mark_trust |
      jq -c --arg job "${GITHUB_JOB:-}" --argjson low "$LOW" "$THREAD_DEFS"' select($job == "review" or (low | not) or unanswered)
        | if .comments.totalCount <= 100 then del(.root) else . end' >"$out"
    echo "$out"
    ;;
  reply)
    [[ "${2:-}" =~ ^[0-9]+$ ]] || { echo "usage: reply <comment-database-id> <file>" >&2; exit 2; }
    body=$(tmp_read "${3:?usage: reply <comment-database-id> <file>}")
    if grep -qE "$CRED_VALUE_RE" <<<"$body"; then
      echo "reply refused: credential-like value in the body; delete it and post again" >&2; exit 2
    fi
    gh api "repos/$REPO/pulls/$PR_NUMBER/comments/$2/replies" -f body="$(with_role "$body")" --jq .html_url
    ;;
  resolve)
    [[ "${2:-}" =~ ^PRRT_[A-Za-z0-9_-]+$ ]] || { echo "usage: resolve <review-thread-node-id>" >&2; exit 2; }
    all_threads | jq -e --arg id "$2" -s 'any(.[]; .id == $id)' >/dev/null \
      || { echo "thread $2 is not on PR #$PR_NUMBER" >&2; exit 2; }
    gh api graphql -F id="$2" -f query='
      mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{isResolved}}}' \
      --jq .data.resolveReviewThread.thread.isResolved
    ;;
  review)
    # Read the file once, so the review posted is the one checked.
    json=$(tmp_read "${2:?usage: review <json-file>}")
    jq -e '.event == "COMMENT" or .event == "REQUEST_CHANGES"' <<<"$json" >/dev/null \
      || { echo "review refused: event must be COMMENT or REQUEST_CHANGES (or the file is not valid JSON)" >&2; exit 2; }
    # missing-passes counts only reviews on the head; one posted elsewhere would be run again.
    jq -e --arg sha "$HEAD_SHA" '.commit_id == $sha' <<<"$json" >/dev/null \
      || { echo "review refused: commit_id must be $HEAD_SHA" >&2; exit 2; }
    # Check every decoded string and object key: JSON escapes inside a token would hide it in
    # the raw file. The refusal never echoes what matched.
    if grep -qE "$CRED_VALUE_RE" <<<"$(jq -r '.. | (strings, (objects | keys[]))' <<<"$json")"; then
      echo "review refused: credential-like value in the review; delete it and post again" >&2
      exit 2
    fi
    json=$(jq --arg b "$(with_role "$(jq -r '.body // ""' <<<"$json")")" '.body = $b' <<<"$json")
    gh api "repos/$REPO/pulls/$PR_NUMBER/reviews" --method POST --input - --jq .html_url <<<"$json"
    ;;
  permission)
    [[ "${2:-}" =~ ^[A-Za-z0-9-]+(\[bot\])?$ ]] || { echo "usage: permission <login>" >&2; exit 2; }
    gh api "repos/$REPO/collaborators/$2/permission" --jq .permission 2>/dev/null || echo none
    ;;
  scan-push|push)
    [ "$1" = scan-push ] || : "${PUSH_TOKEN:?push runs only in the workflow push steps}"
    # Keep the push token out of the environment of every git call that reads the checkout.
    # unset hides it from child processes only: /proc/<pid>/environ of this shell and of the
    # step's shell still holds it. The agents cannot read it there: Read(//proc/**) is denied,
    # and so are gh's --jq and --template filters, which can read the environment.
    push_token=${PUSH_TOKEN:-}
    unset PUSH_TOKEN
    # Scans every unpushed commit not already on the base branch (a merged side branch
    # included; a merge shows only its conflict resolution, --cc): added lines, new and
    # renamed paths, and full values in commit messages and author/committer identity.
    # One grep per text and never grep -q in a pipe: with pipefail, its early exit fails it.
    # Force-refresh both tracking refs from GitHub and name them in full, so no moved
    # tracking ref, or tag or branch shadowing origin/<ref>, can empty the range.
    fetch "$BASE_REF" "$HEAD_REF"
    # If the base was force-pushed since the resolver merged it, the old base commits fall
    # back into scope, which only errs on the safe side.
    # Resolved once, so the commit pushed is the one scanned.
    head=$(git rev-parse --verify 'HEAD^{commit}')
    scope=("refs/remotes/origin/$HEAD_REF..$head" "^refs/remotes/origin/$BASE_REF")
    # Added lines only: not deleted lines, nor the +++ file headers between "diff" and the
    # first "@@" (paths are checked separately), so removing a credential or editing an
    # existing prefixed path can be pushed. A merge's conflict hunks can also show base-side
    # lines as added, which only errs on the safe side. --text, --no-textconv and
    # --no-ext-diff show every added line, whatever .gitattributes or a NUL byte says.
    added=$(git log -p --no-color --text --no-textconv --no-ext-diff -U0 --cc --format= "${scope[@]}" | awk '/^diff /{h=1} /^@@/{h=0} !h && /^[ +]?\+/')
    # Raw names (-z): git's C-quoting would put a letter or digit (the t of \t, an octal
    # byte) just before a prefix. tr reads all its input, so this pipe is safe under pipefail.
    paths=$(git log -z --cc --name-only -M --diff-filter=AR --format= "${scope[@]}" | tr '\0' '\n')
    messages=$(git log --format='%an <%ae>%n%cn <%ce>%n%B' "${scope[@]}")
    if grep -qE "$CRED_RE|$CRED_VALUE_RE" <<<"$added"$'\n'"$paths" || grep -qE "$CRED_VALUE_RE" <<<"$messages"; then
      echo "::error::Commits contain a credential-like string; not pushing." >&2
      exit 1
    fi
    # A gitlink (mode 160000) makes git run commands inside a nested repository whose config
    # the agent could have written; refuse adding, changing or removing one.
    raw=$(git log -c --raw --format= "${scope[@]}")
    # Only the mode fields before the tab; a path may itself contain " 160000 ". No pipe into
    # grep -q: under pipefail, cut killed by SIGPIPE after an early match would read as none.
    if grep -qE '(^|[: ])160000( |$)' <<<"$(cut -f1 <<<"$raw")"; then
      echo "::error::Commits add or change a submodule (gitlink); not pushing." >&2
      exit 1
    fi
    # Succeeds when merging $1 and $2 leaves no conflict in a protected path (a modify/delete
    # conflict leaves no markers) and matches $3 there. merge-tree exits 1 on any conflict but
    # still writes the tree, then lists the conflicted files; a conflict elsewhere is fine.
    clean_merge_matches() {
      local rc=0 out tree conflicted
      out=$(git -c core.quotePath=false merge-tree --write-tree --no-messages --name-only "$1" "$2") || rc=$?
      tree=${out%%$'\n'*} conflicted=${out#"$tree"}
      [ "$rc" -le 1 ] && ! grep -qiE "$protected_re" <<<"${conflicted#$'\n'}" &&
        git --icase-pathspecs diff --quiet "$tree" "$3" -- "${protected[@]}"
    }
    # No single commit may touch them either, even one a later commit reverts: anyone checking
    # it out would get its hooks. Merges are left to the tree checks below, so base merges pass;
    # --full-history keeps a merged side branch whose merge undid the change. Each git call
    # here is a plain assignment, so set -e stops the script if it fails.
    touched=$(git --icase-pathspecs log --full-history --no-merges --format=%H "${scope[@]}" -- "${protected[@]}")
    if [ -n "$touched" ]; then
      echo "::error::A commit changes a protected path, even if a later one reverts it; not pushing." >&2
      exit 1
    fi
    # Merges: the resolver merges only the base, so a merge's other parents must be on it (any
    # other branch would bring commits nobody reviewed for this PR). On protected paths each
    # merge must match a clean merge of its two parents, not just its first parent: that would
    # let a merge drop the base's changes to them. Only two-parent merges: the added-line scan
    # reads the first two combined-diff columns.
    merges=$(git rev-list --merges "${scope[@]}")
    for m in $merges; do
      if git rev-parse -q --verify "$m^3" >/dev/null; then
        echo "::error::A merge has more than two parents; not pushing." >&2
        exit 1
      fi
      git merge-base --is-ancestor "$m^2" "refs/remotes/origin/$BASE_REF" || {
        echo "::error::A merge brings in commits that are not on $BASE_REF; not pushing." >&2
        exit 1
      }
      clean_merge_matches "$m^1" "$m^2" "$m" || {
        echo "::error::A merge changes a protected path beyond merging its parents; not pushing." >&2
        exit 1
      }
    done
    # Without merges (merge-base may fast-forward), the head must match the PR branch on those
    # paths, or a clean merge of the base into it. With merges, the checks above already pin
    # them, and comparing with the live base would refuse a valid push whenever the base
    # changed a protected path again since the resolver merged it.
    if [ -z "$merges" ] && ! git --icase-pathspecs diff --quiet "refs/remotes/origin/$HEAD_REF" "$head" -- "${protected[@]}"; then
      clean_merge_matches "refs/remotes/origin/$HEAD_REF" "refs/remotes/origin/$BASE_REF" "$head" || {
        echo "::error::Commits change a protected path (see protected= in pr-tools); not pushing" \
          "(or the base branch changed one since the resolver merged it)." >&2
        exit 1
      }
    fi
    if [ "$1" = push ]; then
      # Push from a new bare repository that borrows this checkout's objects, so no setting in
      # the checkout's .git/config (proxy, credential helper, hooks) applies while the push token
      # is set.
      bare=$(mktemp -d)
      trap 'rm -rf "$bare"' EXIT
      git init -q --bare "$bare"
      objects=$(git rev-parse --path-format=absolute --git-common-dir)/objects
      echo "$objects" >"$bare/objects/info/alternates"
      GIT_DIR="$bare" git update-ref "refs/heads/$HEAD_REF" "$head"
      echo "::add-mask::$(printf 'x-access-token:%s' "$push_token" | base64 -w0)"
      GIT_DIR="$bare" git_auth "$push_token" push --no-verify "https://github.com/$REPO.git" \
        "refs/heads/$HEAD_REF:refs/heads/$HEAD_REF"
    fi
    ;;
  merge-base)
    fetch "$BASE_REF"
    # Exits 1 on conflicts, which the agent then resolves; a fast-forward needs no commit.
    git -c core.hooksPath=/dev/null merge --no-commit --no-edit "refs/remotes/origin/$BASE_REF"
    ;;
  bot-runs)
    # Bot commits since the last human commit (see chain) with a run that did work (see WORKED).
    # Re-runs on one commit count once.
    since=$(chain | awk '$2 == "bot" { print $1 }')
    ran=$(branch_runs | jq -r "$WORKED select(worked) | .head_sha")
    if [ -z "$since" ]; then
      echo 0
    else
      grep -Fxc -f <(printf '%s\n' "$since") <<<"$(sort -u <<<"$ran")" || true
    fi
    ;;
  limits)
    # The PR's review_ labels (see pr-code-review.yml), read from the PR now, so one added while
    # a run waits or reviews applies to its next gate. A push is a person's: the commits from
    # the last human commit on (see chain), the resolver's pushes after it being its fix rounds.
    # With PR_RESOLVER_TOKEN each round starts a run on its head: the rounds used are the bot
    # heads with a run created after the push's own run (its first that is running or did work,
    # whose head is the push's even when a bot's, as for a Claude Code commit or a PR without
    # human commits). Without the token the rounds stay in one run, bounded by MAX_ROUNDS, and a
    # later run on the same push (after a draft toggle, reopen or base change) counts only the
    # head it starts on. It is the first push unless a run that did work since the PR was opened
    # was on an older commit (an earlier push's, or one a force-push replaced). A run whose gate
    # stopped early still counts, or skip_on_open would skip every push; so does one for a PR
    # put back into draft during the wait, whose next push is then later. Label names below are
    # without the review_ prefix. Prints:
    #   skip=<the label that skips this push>: review_stop, or review_skip_on_open on the first
    #   guard=<runs the loop guard allows since the last human commit>: MAX_BOT_RUNS from the
    #     environment, or N when above it, from max_runs:N, or max_initial_runs:N on the first
    #     push (a bare label or a bad N is 0, an N above 99 is 99; with several, the first). N
    #     stays two digits so that a label cannot switch the guard off, or break the gate's
    #     integer tests
    #   left=<fix rounds the resolver may still push>: N minus the rounds used
    #   severity=blocking|important|minor|nit from min_severity:<level> (a bare label or a bad
    #     level is blocking; with several, the first): the lowest tag the resolver fixes (see LOW)
    # Except guard, each is empty without its labels; without skip_on_open and max_* only the PR
    # is read.
    # Without any human commit the PR stays on its first push, as Claude Code pushes are rounds.
    pr=$(gh api "repos/$REPO/pulls/$PR_NUMBER")
    labels=$(jq -r '.labels[].name | select(startswith("review_")) | ltrimstr("review_")' <<<"$pr")
    if grep -qx only <<<"$labels"; then labels=$(printf 'max_initial_runs:0\nmax_runs:0\n%s' "$labels"); fi # review_only (added by install-pr-review-bots.sh)
    severity=$(awk -F: '$1 == "min_severity" { v = ($2 ~ /^(important|minor|nit)$/) ? $2 : "blocking"; print v; exit }' <<<"$labels")
    push='' skip='' runs='' left=''
    if grep -qE '^(skip_on_open|max_(initial_)?runs)' <<<"$labels"; then
      commits=$(chain)
      state=$(branch_runs | jq -rs --arg opened "$(jq -r .created_at <<<"$pr")" \
        --argjson commits "$(jq -nRc '[inputs | split(" ") | select(length == 2)]' <<<"$commits")" "$WORKED"'
        ($commits | map(.[0])) as $chain | ($commits | map(select(.[1] == "bot") | .[0])) as $bots
        | (map(select((.head_sha | IN($chain[])) and (.conclusion == null or worked))) | min_by(.created_at)) as $own
        | (if any(.[]; .created_at >= $opened and worked and (.head_sha | IN($chain[]) | not))
           then "subsequent" else "first" end)
          + " " + ([.[] | select(.created_at > ($own.created_at // "") and .head_sha != $own.head_sha)
            | .head_sha | select(IN($bots[]))] | unique | length | tostring)')
      read -r push used <<<"$state"
      if [ "$push" = first ] && grep -qx skip_on_open <<<"$labels"; then skip=review_skip_on_open; fi
      read -r runs left <<<"$(awk -F: -v first="$([ "$push" = first ] && echo 1 || echo 0)" -v used="$used" '
        function n(v) { return v !~ /^[0-9]+$/ ? 0 : (v + 0 > 99 ? 99 : v + 0) }
        $1 == "max_runs" && !hr { hr = 1; r = n($2) }
        $1 == "max_initial_runs" && first && !hi { hi = 1; i = n($2) }
        END { if (hi) r = i; if (hi || hr) print r, (r > used ? r - used : 0) }' <<<"$labels")"
    fi
    if grep -qx stop <<<"$labels"; then skip=review_stop; fi
    guard=${MAX_BOT_RUNS:-}
    [ -z "$runs" ] || [ "$runs" -le "${guard:-0}" ] || guard=$runs
    printf 'skip=%s\nguard=%s\nleft=%s\nseverity=%s\n' "$skip" "$guard" "$left" "$severity"
    ;;
  missing-passes)
    # All passes go into one combined review from the Actions bot, one section each, ending with
    # a hidden marker for its pass, the head commit and the base branch. A pass counts as posted
    # when a bot review on the head holds its marker on a line of its own (not quoted inside
    # other text), so a retry can post just the missing sections, and a PR moved to another
    # base is reviewed again. The pr-code-reviewer marker is also the one the resolver waits
    # for; it is listed last.
    gh api --paginate "repos/$REPO/pulls/$PR_NUMBER/reviews" \
      --jq '.[] | select(.user.login == "github-actions[bot]") | {commit_id, body}' |
    jq -rs --arg sha "$HEAD_SHA" --arg base "$BASE_REF" '
      [.[] | select(.commit_id == $sha) | .body // "" | split("\n")[] | gsub("^\\s+|\\s+$"; "")] as $lines |
      ["pr-code-reviewer-org", "ponytail-review", "pr-security-reviewer", "pr-code-reviewer"]
      | map(select(. as $n | any($lines[]; . == "<!-- " + $n + ":sha=" + $sha + " base=" + $base + " -->") | not))
      | join(" ")'
    ;;
  *)
    sed -n '2,/^set /{/^#/p}' "$0" >&2
    exit 2
    ;;
esac
