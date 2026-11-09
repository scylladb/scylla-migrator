#!/usr/bin/env bash
# Tests for pr-resolver-tools.sh: scan-push against a scratch repo with a local "origin",
# and the review/reply credential checks with gh stubbed out. Runs only local git; no
# network, no GitHub calls. Usage: .github/scripts/test-pr-resolver-tools.sh
set -uo pipefail

TOOL="$(cd "$(dirname "$0")" && pwd)/pr-resolver-tools.sh"
T=$(mktemp -d /tmp/pr-tools-test.XXXXXX)
trap 'rm -rf "$T"' EXIT
fail=0
# Credential-shaped values are built at run time so this file never contains one.
TOK="ghp_$(printf 'A%.0s' {1..36})"
XAT="x-access-token:$(printf 'b%.0s' {1..12})"

printf '{"pull_request":{"number":1,"base":{"ref":"main"},"head":{"ref":"pr","sha":"abc"}}}' >"$T/ev.json"
export GITHUB_EVENT_PATH="$T/ev.json" GITHUB_REPOSITORY=o/r GH_TOKEN=dummy
# Stub gh: record that it was called, so a refused review or reply can be told apart.
mkdir "$T/bin"
printf '#!/bin/sh\necho called >>"%s/gh.log"\n' "$T" >"$T/bin/gh"
chmod +x "$T/bin/gh"
export PR_TOOLS_PATH="$T/bin:/usr/bin:/bin"
# pr-tools ignores PR_TOOLS_PATH in Actions, and the test workflow runs there.
unset GITHUB_ACTIONS
# Like scan-push, ignore the user's own git config (hooks, signing, merge settings).
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
# pr-tools fetches from and pushes to https://github.com/o/r.git; point that at the local origin.
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="url.$T/o.git.insteadOf" \
  GIT_CONFIG_VALUE_0=https://github.com/o/r.git

check() { # check <name> <want: allowed|refused> <command...>
  local name=$1 want=$2 got out
  shift 2
  if out=$("$@" 2>&1); then got=allowed; else got=refused; fi
  if [ "$got" = "$want" ]; then
    printf 'ok    %-48s %s\n' "$name" "$got"
  else
    printf 'FAIL  %-48s %s (want %s) %s\n' "$name" "$got" "$want" "$out"
    fail=1
  fi
}

# --- scan-push ---------------------------------------------------------------------------
git init -q --bare "$T/o.git"
git clone -q "$T/o.git" "$T/w" 2>/dev/null
cd "$T/w" || exit 1
git config user.email a@b.c
git config user.name t
echo "key=$TOK" >f; echo x >g; mkdir d; echo a >d/ghs_util.sh
git add .; git commit -qm init; git branch -M main; git push -q origin main 2>/dev/null
git checkout -q -b pr; echo pr >p; git add p; git commit -qm pr; git push -q origin pr 2>/dev/null
git checkout -q main; echo "base $TOK" >basefile; git add basefile; git commit -qm "base $TOK"
git push -q origin main 2>/dev/null; git checkout -q pr

scan() { # scan <name> <want>: run scan-push, then reset the branch to origin/pr
  check "scan-push: $1" "$2" bash "$TOOL" scan-push
  git merge --abort 2>/dev/null
  git checkout -q pr; git reset -q --hard origin/pr; git branch -D -q s 2>/dev/null
  rm -f .gitattributes
  for r in $(git for-each-ref --format='%(refname)' refs/replace); do git update-ref -d "$r"; done
}
side() { git checkout -q -b s "${1:-HEAD}"; }
# An allowed case whose setup merge failed would scan nothing and pass.
is_merge() { git rev-parse -q --verify HEAD^2 >/dev/null || { echo "FAIL  setup: $1: HEAD is not a merge"; fail=1; }; }

scan "no commits" allowed
git rm -q f; git commit -qm rm; scan "delete a token" allowed
sed -i "s/key=.*/key=x/" f; git commit -qam e; scan "replace a token line" allowed
echo "k=$TOK" >>g; git commit -qam a; scan "add a token" refused
echo "abc$TOK" >>g; git commit -qam a; scan "token right after a letter" refused
echo "ghp_short" >>g; git commit -qam a; scan "bare prefix at line start" refused
echo "highs_ ok" >>g; git commit -qam a; scan "prefix inside a word" allowed
echo b >>d/ghs_util.sh; git commit -qam a; scan "edit an existing prefixed path" allowed
git rm -q d/ghs_util.sh; git commit -qm a; scan "delete an existing prefixed path" allowed
echo n >d/ghs_new.sh; git add .; git commit -qm a; scan "new prefixed path" refused
touch "x$TOK"; git add .; git commit -qm a; scan "empty file named with a token" refused
git mv g "y$TOK"; git commit -qm a; scan "pure rename to a token name" refused
git commit -q --allow-empty -m "msg $TOK"; scan "token in a commit message" refused
git -c user.email="$TOK@x" commit -q --allow-empty -m m; scan "token in the author email" refused
GIT_COMMITTER_EMAIL="$TOK@x" git commit -q --allow-empty -m m; scan "token in the committer email only" refused
git merge -q --no-edit origin/main; is_merge "clean merge"; scan "clean merge of a base with a token" allowed
side; echo "y=$TOK" >h; git add h; git commit -qm side; git checkout -q pr
git merge -q --no-ff s -m m; scan "merged side branch adding a token" refused
side; echo z >d/ghs_x.sh; git add .; git commit -qm side; git checkout -q pr
git merge -q --no-ff s -m m; scan "merged side branch adding a prefixed path" refused
side origin/main; echo c >g; git commit -qam s; git checkout -q pr; echo dd >g; git commit -qam p
git merge -q s >/dev/null 2>&1; echo "res $TOK" >g; git commit -qam m
scan "conflict resolution adding a token" refused
echo "++ b/foo $TOK" >>g; git commit -qam a; scan "content line '++ b/' with a token" refused
echo "+++ b/foo ghp_x" >>g; git commit -qam a; scan "content line '+++ b/' with a prefix" refused
printf '* -diff\n' >.gitattributes; echo "k=$TOK" >bin1; git add bin1; git commit -qm a
scan "worktree .gitattributes -diff hiding a token" refused
printf '* -diff\n' >.gitattributes; echo "k=$TOK" >bin2; git add .; git commit -qm a
scan "committed .gitattributes -diff hiding a token" refused
printf 'a\0b\nk=%s\n' "$TOK" >bin3; git add bin3; git commit -qm a; scan "NUL-byte file with a token" refused
echo "k=$TOK" >rp; git add rp; git commit -qm bad; bad=$(git rev-parse HEAD); git reset -q --hard HEAD~1
echo clean >rp; git add rp; git commit -qm bad; good=$(git rev-parse HEAD); git reset -q --hard "$bad"
git replace "$bad" "$good"
git rev-parse -q --verify "refs/replace/$bad" >/dev/null || { echo "FAIL  setup: git replace"; fail=1; }
scan "replace ref hiding a token commit" refused
mkdir -p .github/scripts; echo x >.github/scripts/t.sh; git add .; git commit -qm a
scan "commit changing .github/" refused
mkdir -p .claude; echo '{}' >.claude/settings.json; git add .; git commit -qm a
scan "commit changing .claude/" refused
echo m >CLAUDE.md; git add .; git commit -qm a; scan "commit changing CLAUDE.md" refused
mkdir -p sub/.claude; echo m >sub/CLAUDE.md; git add .; git commit -qm a; scan "nested CLAUDE.md" refused
mkdir -p sub; echo m >sub/CLAUDE.local.md; git add .; git commit -qm a; scan "nested CLAUDE.local.md" refused
echo m >AGENTS.md; git add .; git commit -qm a; scan "AGENTS.md" refused
echo m >Agents.md; git add .; git commit -qm a; scan "mixed-case Agents.md" refused
mkdir -p .agents/skills; echo m >.agents/skills/s; git add .; git commit -qm a; scan ".agents directory" refused
mkdir -p sub/.Claude; echo m >sub/.Claude/s; git add .; git commit -qm a; scan "mixed-case nested .Claude directory" refused
echo m >'a 160000 b'; git add .; git commit -qm a; scan "file name containing 160000" allowed
echo m >"$(printf 'a\tghs_x')"; git add .; git commit -qm a; scan "prefixed file name after a tab" refused
echo 'git push https://x-access-token:${TOKEN}@github.com/o/r' >push.sh; git add .; git commit -qm a
scan "x-access-token placeholder in a pushed line" refused
mkdir -p .husky; echo m >.husky/pre-commit; git add .; git commit -qm a; scan "husky hook" refused
mkdir -p .husky; echo m >.husky/pre-commit; git add .; git commit -qm a; git rm -rq .husky; git commit -qm b
scan "husky hook added and then reverted" refused
echo x >.envrc; git add .; git commit -qm a; scan ".envrc" refused
echo '.github/** merge=union' >.gitattributes; git add .; git commit -qm a; scan ".gitattributes" refused
mkdir -p sub; echo x >sub/.npmrc; git add .; git commit -qm a; scan "nested .npmrc" refused
side; echo q >q; git add q; git commit -qm s; git checkout -q pr; git merge -q --no-ff s -m m
git branch -qD s; scan "merge of a branch that is not the base" refused
mkdir -p .devcontainer; echo '{}' >.devcontainer/devcontainer.json; git add .; git commit -qm a
scan "devcontainer config" refused
echo '{}' >.devcontainer.json; git add .; git commit -qm a; scan "root .devcontainer.json" refused
printf '[lfs]\n\turl = https://example.invalid/lfs\n' >.lfsconfig; git add .; git commit -qm a
scan ".lfsconfig" refused
side origin/pr; echo c1 >c1; git add c1; git commit -qm c1; git push -q origin s:c1 2>/dev/null
git checkout -q -b s8 origin/pr; echo c2 >c2; git add c2; git commit -qm c2; git push -q origin s8:c2 2>/dev/null
git merge -q --no-edit s; git push -q origin s8:b8 2>/dev/null; git checkout -q pr; git branch -qD s s8
echo p8 >p8; git add p8; git commit -qm p8; git merge -q -m o origin/c1 origin/c2
BASE_REF=b8 scan "octopus merge of base commits" refused
side origin/main; echo b5 >b5; git add b5; git commit -qm b5; git push -q origin s:b5 2>/dev/null
git checkout -q pr; git branch -qD s; git merge -q --no-commit origin/main >/dev/null 2>&1
mkdir -p .husky; echo m >.husky/pre-commit; git add .husky; git commit -q --no-edit
git merge -q --no-commit origin/b5 >/dev/null 2>&1; git rm -rq .husky; git commit -q --no-edit
BASE_REF=b5 scan "base merge adding a hook that a later base merge drops" refused
git checkout -q -b side2; mkdir -p .husky; echo m >.husky/pre-commit; git add .; git commit -qm a
git rm -rq .husky; git commit -qm b; git checkout -q pr; echo z >z; git add z; git commit -qm c
git merge -q --no-edit side2; git branch -qD side2
scan "husky hook added and reverted on a merged side branch" refused
git update-index --add --cacheinfo "160000,$(git rev-parse HEAD),sub2"; git commit -qm a; scan "adding a gitlink" refused
# The gitlink sorts first and more raw output than a pipe buffer holds follows it.
mkdir -p many; for i in $(seq 4000); do echo "$i" >"many/f$i"; done
git update-index --add --cacheinfo "160000,$(git rev-parse HEAD),a0"; git add many; git commit -qm a
scan "gitlink followed by a large diff" refused
echo m >.gitmodules; git add .; git commit -qm a; scan ".gitmodules" refused
mkdir -p sub/.cursor; echo m >sub/.cursor/hooks.json; git add .; git commit -qm a; scan "nested .cursor directory" refused
mkdir -p sub/.claude; echo m >sub/.claude/s; git add .; git commit -qm a; scan "nested .claude directory" refused
mkdir -p sub; echo m >sub/MY_CLAUDE.md; git add .; git commit -qm a; scan "file merely named like CLAUDE.md" allowed
mkdir -p docs/.github; echo x >docs/.github/a; git add .; git commit -qm a
scan "nested .github directory" allowed
git checkout -q main; mkdir -p .github; printf '%s\n' 1 2 3 4 5 6 7 8 >.github/w; git add .github
git commit -qm w; git push -q origin main 2>/dev/null
git checkout -q pr; git merge -q --no-edit origin/main >/dev/null; git push -q origin pr 2>/dev/null
git checkout -q main; sed -i 1s/1/one/ .github/w; git commit -qam w1; git push -q origin main 2>/dev/null
git checkout -q pr; sed -i 8s/8/eight/ .github/w; git commit -qam w8; git push -q origin pr 2>/dev/null
git merge -q --no-edit origin/main >/dev/null; is_merge "clean protected merge"
scan "clean base merge into a protected file" allowed
git checkout -q main; sed -i 2s/2/two/ .github/w; echo base >conf; git add -A; git commit -qm c
git push -q origin main 2>/dev/null; git checkout -q pr; echo pr >conf; git add conf; git commit -qm c
git push -q origin pr 2>/dev/null; git merge -q origin/main >/dev/null 2>&1; echo both >conf; git add conf
git commit -q --no-edit; is_merge "resolved conflict"; scan "base merge with a protected file and a resolved conflict" allowed
git checkout -q main; sed -i 3s/3/three/ .github/w; git commit -qam c3; git push -q origin main 2>/dev/null
git checkout -q pr; sed -i 3s/3/drei/ .github/w; git commit -qam c3; git push -q origin pr 2>/dev/null
git merge -q origin/main >/dev/null 2>&1
git show "$(git merge-tree --write-tree refs/remotes/origin/pr refs/remotes/origin/main | head -1):.github/w" >.github/w
echo both >conf; git add .github/w conf; git commit -q --no-edit
scan "protected file committed with conflict markers" refused
git mv .github/w w2; git commit -qm mv; scan "moving a file out of .github/" refused
echo x >".github/caf$(printf '\303\251').yml"; git add .; git commit -qm a
scan "non-ASCII path under .github/" refused
# A base b3 that changes a protected file: merging it is allowed, unless merge-tree fails.
side; sed -i 5s/5/five/ .github/w; git commit -qam w5; git push -q origin s:b3 2>/dev/null
git checkout -q pr; git merge -q --no-ff --no-edit origin/b3; is_merge "b3 merge"
BASE_REF=b3 scan "base merge changing a protected file" allowed
git merge -q -s ours --no-ff --no-edit origin/b3; BASE_REF=b3 scan "base merge dropping the base's protected change" refused
printf '#!/bin/sh\ncase " $* " in *" merge-tree "*) exit 2 ;; esac\nexec /usr/bin/git "$@"\n' >"$T/bin/git"
chmod +x "$T/bin/git"; git merge -q origin/b3
BASE_REF=b3 scan "the same merge with merge-tree failing" refused; rm "$T/bin/git"
side; sed -i 6s/6/six/ .github/w; git commit -qam w6; git push -q origin s:b9 2>/dev/null
git checkout -q pr; git merge -q --no-ff -m m origin/b9; git checkout -q s
sed -i 7s/7/seven/ .github/w; git commit -qam w7; git push -q origin s:b9 2>/dev/null; git checkout -q pr
is_merge "b9 merge"; BASE_REF=b9 scan "base merge, then the base changes the same file again" allowed
# A base b2 with only this conflict: a .claude file against the PR's .claude/ directory.
side; echo f >.claude; git add .claude; git commit -qm f; git push -q origin s:b2 2>/dev/null
git checkout -q pr; mkdir .claude; echo a >.claude/x; git add .claude; git commit -qm x
git push -q origin pr 2>/dev/null; git merge -q origin/b2 >/dev/null 2>&1
git rm -rq --cached .claude; rm -rf .claude; echo f >.claude; git add -A; git commit -q --no-edit
BASE_REF=b2 scan "protected file replacing a protected directory in a merge" refused
# A base b10 that edits .github/w, which the PR deletes: keeping the base's file leaves no
# conflict markers, but a conflict in a protected path is still left to a person.
side; sed -i 2s/.*/b10/ .github/w; git commit -qam b10; git push -q origin s:b10 2>/dev/null
git checkout -q pr; git rm -q .github/w; git commit -qm rmw; git push -q origin pr 2>/dev/null
git merge -q origin/b10 >/dev/null 2>&1; git add .github/w; git commit -q --no-edit
BASE_REF=b10 scan "protected delete/modify conflict kept from the base" refused
# A base b11 that adds a .lfsconfig file where the PR has a .lfsconfig/ directory.
side origin/main; echo f >.lfsconfig; git add .lfsconfig; git commit -qm f; git push -q origin s:b11 2>/dev/null
git checkout -q pr; mkdir .lfsconfig; echo a >.lfsconfig/x; git add .lfsconfig; git commit -qm x
git push -q origin pr 2>/dev/null; git merge -q origin/b11 >/dev/null 2>&1; git add -A; git commit -q --no-edit
BASE_REF=b11 scan "protected file/directory conflict committed as is" refused
check "push: refused without PUSH_TOKEN" refused bash "$TOOL" push
echo n >new; git add new; git commit -qm new
check "push: pushes HEAD to the PR branch" allowed env PUSH_TOKEN=dummy bash "$TOOL" push
[ "$(git -C "$T/o.git" rev-parse pr)" = "$(git rev-parse HEAD)" ] \
  || { echo "FAIL  push: origin/pr is not HEAD"; fail=1; }
cd "$T" || exit 1

# A base b6 that merges cleanly but not by fast-forward: the merge is staged, not committed.
(cd "$T/w" && git checkout -q -b s6 origin/pr~1 && echo q6 >q6 && git add q6 && git commit -qm q6 &&
  git push -q origin s6:b6 2>/dev/null && git checkout -q pr && git branch -qD s6)
check "merge-base stops before committing" allowed env BASE_REF=b6 bash -c 'cd "$2" &&
  git reset -q --hard origin/pr && bash "$1" merge-base >/dev/null 2>&1 &&
  git rev-parse -q --verify MERGE_HEAD >/dev/null && [ "$(git rev-parse HEAD)" = "$(git rev-parse origin/pr)" ] &&
  git merge --abort' _ "$TOOL" "$T/w"
check "open-work --output" allowed bash -c 'cd "$2" && out=$(bash "$1" open-work --output) &&
  [[ $out =~ ^pending=0$'"'"'\n'"'"'conflict=(yes|no|protected)$'"'"'\n'"'"'work=(true|false)$ ]]' _ "$TOOL" "$T/w"
# Bases that conflict with the PR only in .github/w, and only in conf.
(cd "$T/w" && git checkout -q -b s12 origin/main && sed -i 4s/.*/b12/ .github/w && git commit -qam b12 &&
  git push -q origin s12:b12 2>/dev/null && git checkout -q -b s13 origin/main && echo b13 >conf &&
  git commit -qam b13 && git push -q origin s13:b13 2>/dev/null && git checkout -q -b s14 origin/main &&
  sed -i 4s/.*/h12/ .github/w && echo h13 >conf && git commit -qam h && git push -q origin s14:h14 2>/dev/null &&
  git checkout -q pr && git branch -qD s12 s13 s14)
check "open-work: protected conflict is not work" allowed env BASE_REF=b12 HEAD_REF=h14 bash -c 'cd "$2" &&
  [ "$(bash "$1" open-work --output 2>/dev/null | tr "\n" " ")" = "pending=0 conflict=protected work=false " ]' _ "$TOOL" "$T/w"
check "open-work: other conflict is work" allowed env BASE_REF=b13 HEAD_REF=h14 bash -c 'cd "$2" &&
  [ "$(bash "$1" open-work --output 2>/dev/null | tr "\n" " ")" = "pending=0 conflict=yes work=true " ]' _ "$TOOL" "$T/w"

# bot-runs: h1 is a person's; b1..b3 are bot and Claude Code commits. Runs: h1, b1 twice, b3
# cancelled after doing work (counts), b2 cancelled in the wait and b2 still running (neither
# counts), so two bot runs. Then a person's commit h2 last: none.
mkdir "$T/bin2"
printf 'h1\tAl\ta@x\nb1\tclaude[bot]\tc@x\nb2\tClaude\tnoreply@anthropic.com\nb3\tgithub-actions[bot]\tg@x\n' >"$T/commits"
r() { # r <head> <conclusion> <minutes until updated> [created_at]
  printf '{"head_sha":"%s","conclusion":%s,"created_at":"%s","run_started_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:%s:00Z"}\n' \
    "$1" "$2" "${4:-2026-01-01T00:00:00Z}" "$3"
}
{ r h1 '"success"' 05; r b1 '"success"' 05 2026-01-01T00:10:00Z; r b1 '"failure"' 05 2026-01-01T00:11:00Z
  r b3 '"cancelled"' 09 2026-01-01T00:30:00Z; r b2 '"cancelled"' 01 2026-01-01T00:20:00Z
  r b2 null 01 2026-01-01T00:21:00Z; } >"$T/runs"
printf '#!/bin/sh\ncase "$*" in\n  */commits*) cat "%s/commits" ;;\n  */actions/runs/7*) echo 99 ;;\n  */runs*) cat "%s/runs" ;;\n  */pulls/1) cat "%s/pr" ;;\nesac\n' "$T" "$T" "$T" >"$T/bin2/gh"
chmod +x "$T/bin2/gh"
check "bot-runs: runs since the last human commit" allowed bash -c '[ "$(PR_TOOLS_PATH="$2/bin2:/usr/bin:/bin" GITHUB_RUN_ID=7 bash "$1" bot-runs)" = 2 ]' _ "$TOOL" "$T"
printf 'h2\tAl\ta@x\n' >>"$T/commits"
check "bot-runs: none after a human commit" allowed bash -c '[ "$(PR_TOOLS_PATH="$2/bin2:/usr/bin:/bin" GITHUB_RUN_ID=7 bash "$1" bot-runs)" = 0 ]' _ "$TOOL" "$T"

# limits: labels from the PR ($T/pr), opened when the runs above started. h1 is a person's and
# b1..b3 the resolver's pushes after it, each with a run: the first push, three rounds used.
# Runs on older commits that did no work (x8 skipped, x9 cancelled in the wait) or came before
# the PR was opened (x7) leave it the first push; a person's h2 after them is a later push.
lim() { # lim <name> <labels without review_, comma-separated; "-x" is x as is> <want: output lines joined by spaces>
  jq -cn --arg l "$2" '{created_at: "2026-01-01T00:00:00Z", labels: [$l | split(",")[]
    | {name: (if startswith("-") then .[1:] else "review_" + . end)}]}' >"$T/pr"
  check "limits: $1" allowed bash -c '[ "$(PR_TOOLS_PATH="$2/bin2:/usr/bin:/bin" GITHUB_RUN_ID=7 MAX_BOT_RUNS=5 bash "$1" limits | tr "\n" " ")" = "$3" ]' _ "$TOOL" "$T" "$3"
}
printf 'h1\tAl\ta@x\nb1\tclaude[bot]\tc@x\nb2\tClaude\tnoreply@anthropic.com\nb3\tgithub-actions[bot]\tg@x\n' >"$T/commits"
lim "no bot labels" "" "skip= guard=5 left= severity= "
lim "skip_on_open on the first push" skip_on_open "skip=review_skip_on_open guard=5 left= severity= "
lim "rounds left" max_runs:5 "skip= guard=5 left=2 severity= "
lim "rounds used up" max_runs:2 "skip= guard=5 left=0 severity= "
lim "bare label is 0" max_runs "skip= guard=5 left=0 severity= "
lim "bad number is 0" max_runs:x "skip= guard=5 left=0 severity= "
lim "N above MAX_BOT_RUNS raises the guard" max_runs:8 "skip= guard=8 left=5 severity= "
lim "N of two digits" max_runs:99 "skip= guard=99 left=96 severity= "
lim "N of three digits is capped at 99" max_runs:100 "skip= guard=99 left=96 severity= "
lim "N too long for an integer test is capped at 99" max_runs:99999999999999999999 "skip= guard=99 left=96 severity= "
lim "max_initial_runs on the first push" "max_runs:9,max_initial_runs:4" "skip= guard=5 left=1 severity= "
lim "max_initial_runs raises the guard on the first push" max_initial_runs:7 "skip= guard=7 left=4 severity= "
lim "min_severity level" min_severity:minor "skip= guard=5 left= severity=minor "
lim "bare min_severity is blocking" "min_severity,min_severity:nit" "skip= guard=5 left= severity=blocking "
lim "bad min_severity level is blocking" min_severity:all "skip= guard=5 left= severity=blocking "
lim "stop" stop "skip=review_stop guard=5 left= severity= "
lim "stop wins over skip_on_open" "skip_on_open,stop" "skip=review_stop guard=5 left= severity= "
lim "labels without the prefix" "-stop,-skip_on_open,-max_runs:0,-min_severity:minor,-ci:client-ssl" "skip= guard=5 left= severity= "
{ r x7 '"success"' 05 2025-12-31T00:00:00Z; r x8 '"skipped"' 05; r x9 '"cancelled"' 01; } >>"$T/runs"
lim "older runs that did no work" skip_on_open "skip=review_skip_on_open guard=5 left= severity= "
printf 'h2\tAl\ta@x\n' >>"$T/commits"
lim "skip_on_open on a later push" skip_on_open "skip= guard=5 left= severity= "
lim "max_runs on a later push" "max_initial_runs:0,max_runs:2" "skip= guard=5 left=2 severity= "
lim "max_initial_runs alone on a later push" max_initial_runs:0 "skip= guard=5 left= severity= "
lim "max_initial_runs does not raise the guard on a later push" max_initial_runs:20 "skip= guard=5 left= severity= "
# A PR with no commit by a person stays on its first push. Its own run (on c2; c1's was skipped
# as a draft) is not one of its rounds, nor is c1's head.
printf 'c1\tClaude\tnoreply@anthropic.com\nc2\tclaude[bot]\tc@x\n' >"$T/commits"
{ r c1 '"skipped"' 00; r c2 null 00 2026-01-01T00:05:00Z; } >"$T/runs"
lim "no commit by a person" "skip_on_open,max_initial_runs:1" "skip=review_skip_on_open guard=5 left=1 severity= "

# missing-passes: a pass counts when a review on the head commit (abc) holds its marker for
# the base branch (main). The stub ignores --jq, so the filter on the bot user is not tested here.
mkdir "$T/bin3"
printf '#!/bin/sh\ncat "%s/reviews"\n' "$T" >"$T/bin3/gh"
chmod +x "$T/bin3/gh"
missing() { # missing <name> <want output>; reviews in $T/reviews
  check "missing-passes: $1" allowed bash -c 'out=$(PR_TOOLS_PATH="$2/bin3:/usr/bin:/bin" bash "$1" missing-passes) && [ "$out" = "$3" ]' _ "$TOOL" "$T" "$2"
}
rv() { jq -cn --arg c "$1" --arg b "$2" '{commit_id: $c, body: $b}'; }
# sec <var> <pass> <title> [sha] [base]
sec() { printf -v "$1" '### %s\nNo findings.\n<!-- %s:sha=%s base=%s -->\n\n' "$3" "$2" "${4:-abc}" "${5:-main}"; }
sec S_OWN pr-code-reviewer 'Reviewer pass (pr-code-reviewer)'
sec S_ORG pr-code-reviewer-org 'Org reviewer pass (anthropic-skills:pr-code-reviewer)'
sec S_PONY ponytail-review 'Over-engineering pass (ponytail-review)'
sec S_SEC pr-security-reviewer 'Security pass (pr-security-reviewer)'
: >"$T/reviews"
missing "no reviews" "pr-code-reviewer-org ponytail-review pr-security-reviewer pr-code-reviewer"
rv abc "Summary"$'\n\n'"$S_OWN$S_ORG$S_PONY$S_SEC" >"$T/reviews"
missing "all sections in one review" ""
rv abc "Summary"$'\n\n'"$S_OWN$S_ORG" >"$T/reviews"
missing "two sections missing" "ponytail-review pr-security-reviewer"
rv abc "Retry"$'\n\n'"$S_PONY$S_SEC" >>"$T/reviews"
missing "missing sections in a retry review" ""
rv abc "$S_ORG$S_PONY$S_SEC"$'\n### Reviewer pass (pr-code-reviewer)\nok' >"$T/reviews"
missing "heading without its marker" "pr-code-reviewer"
rv def "$S_OWN$S_ORG$S_PONY$S_SEC" >"$T/reviews"
sec S_DEF pr-security-reviewer 'Security pass (pr-security-reviewer)' def
rv abc "$S_OWN$S_ORG$S_PONY$S_DEF" >>"$T/reviews"
missing "markers for another commit" "pr-security-reviewer"
sec S_DEV pr-security-reviewer 'Security pass (pr-security-reviewer)' abc dev
rv abc "$S_OWN$S_ORG$S_PONY$S_DEV" >"$T/reviews"
missing "markers for another base branch" "pr-security-reviewer"
rv abc $'**Security pass (pr-security-reviewer)**\n<!-- pr-code-reviewer:sha=abc -->' >"$T/reviews"
missing "reviews from before markers named the base" "pr-code-reviewer-org ponytail-review pr-security-reviewer pr-code-reviewer"
rv abc "$S_OWN$S_ORG$S_PONY"$'\nsee <!-- pr-security-reviewer:sha=abc base=main --> above\n> <!-- pr-security-reviewer:sha=abc base=main -->x' >"$T/reviews"
missing "marker quoted inside other text" "pr-security-reviewer"

printf '#!/bin/sh\necho called >>"%s/jq.log"\nexec /usr/bin/jq "$@"\n' "$T" >"$T/bin/jq"
chmod +x "$T/bin/jq"
check "PR_TOOLS_PATH outside Actions" allowed bash -c 'rm -f "$2/jq.log"; bash "$1" nothing >/dev/null 2>&1
  [ -s "$2/jq.log" ]' _ "$TOOL" "$T"
check "PR_TOOLS_PATH in Actions" refused bash -c 'rm -f "$2/jq.log"; GITHUB_ACTIONS=true bash "$1" nothing >/dev/null 2>&1
  [ -s "$2/jq.log" ]' _ "$TOOL" "$T"
rm "$T/bin/jq"

# --- review and reply --------------------------------------------------------------------
review() { # review <name> <want> <json>
  printf '%s' "$3" >"$T/review.json"
  : >"$T/gh.log"
  check "review: $1" "$2" bash "$TOOL" review "$T/review.json"
  called "review: $1" "$2"
}
reply() { # reply <name> <want> <body>
  printf '%s' "$3" >"$T/reply.md"
  : >"$T/gh.log"
  check "reply: $1" "$2" bash "$TOOL" reply 1 "$T/reply.md"
  called "reply: $1" "$2"
}
called() { # the stub gh must have run for allowed calls and must not have run for refused ones
  local got=refused
  [ -s "$T/gh.log" ] && got=allowed
  [ "$got" = "$2" ] || { echo "FAIL  $1: gh call $got"; fail=1; }
}
review "clean body" allowed '{"commit_id":"abc","event":"COMMENT","body":"looks fine"}'
review "prose naming a prefix" allowed '{"commit_id":"abc","event":"COMMENT","body":"refuses ghp_ and sk-ant- prefixes"}'
review "placeholder in backticks" allowed '{"commit_id":"abc","event":"COMMENT","body":"x-access-token:`TOKEN-NAME`@github.com"}'
review "placeholder in \$()" allowed '{"commit_id":"abc","event":"COMMENT","body":"x-access-token:$(cat token-file)@github.com"}'
review "quoted variable placeholder" allowed '{"commit_id":"abc","event":"COMMENT","body":"x-access-token:\"$TOKEN\"@github.com"}'
review "no commit_id" refused '{"event":"COMMENT","body":"b"}'
review "another commit" refused '{"commit_id":"def","event":"COMMENT","body":"b"}'

# The role line: the stub gh saves what it would post.
mkdir "$T/bin4"
printf '#!/bin/sh\nprintf "%%s\\n" "$@" >"%s/posted"; cat >>"%s/posted"\n' "$T" "$T" >"$T/bin4/gh"
chmod +x "$T/bin4/gh"
posted() { # posted <name> <job> <want first line> <command...>
  local name=$1 job=$2 want=$3; shift 3
  check "role: $name" allowed bash -c 'rm -f "$1/posted"; GITHUB_JOB=$2 PR_TOOLS_PATH="$1/bin4:/usr/bin:/bin" "${@:4}" </dev/null >/dev/null &&
    grep -qxF -- "$3" "$1/posted"' _ "$T" "$job" "$want" "$@"
}
printf '%s' '{"commit_id":"abc","event":"COMMENT","body":"Summary"}' >"$T/review.json"
posted "review job review" review '  "body": "**ScyllaDB Reviewer**\n\nSummary"' \
  bash "$TOOL" review "$T/review.json"
printf '%s' 'Fixed.' >"$T/reply.md"
posted "resolver reply" resolve 'body=**ScyllaDB Automated PR Resolver**' bash "$TOOL" reply 1 "$T/reply.md"
posted "review job reply" review 'body=**ScyllaDB Reviewer**' bash "$TOOL" reply 1 "$T/reply.md"
printf '%s' '**ScyllaDB Automated PR Resolver**

Fixed.' >"$T/reply.md"
check "role: not added twice" allowed bash -c 'GITHUB_JOB=resolve PR_TOOLS_PATH="$1/bin4:/usr/bin:/bin" bash "$2" reply 1 "$1/reply.md" </dev/null >/dev/null &&
  [ "$(grep -c "ScyllaDB Automated PR Resolver" "$1/posted")" = 1 ]' _ "$T" "$TOOL"
review "APPROVE event" refused '{"commit_id":"abc","event":"APPROVE","body":"ok"}'
review "invalid JSON" refused '{"commit_id":"abc","event":"COMMENT",'
review "token in the body" refused "{\"commit_id\":\"abc\",\"event\":\"COMMENT\",\"body\":\"see $TOK\"}"
review "token in an inline comment" refused "{\"commit_id\":\"abc\",\"event\":\"COMMENT\",\"body\":\"b\",\"comments\":[{\"path\":\"a\",\"line\":1,\"body\":\"$TOK\"}]}"
review "token behind a \\u escape" refused "{\"commit_id\":\"abc\",\"event\":\"COMMENT\",\"body\":\"\\u0067${TOK:1}\"}"
review "token as an object key" refused "{\"commit_id\":\"abc\",\"event\":\"COMMENT\",\"body\":\"b\",\"$TOK\":1}"
review "literal x-access-token value" refused "{\"commit_id\":\"abc\",\"event\":\"COMMENT\",\"body\":\"$XAT@github.com\"}"
reply "clean body" allowed "fixed in abc"
reply "token in the body" refused "here: $TOK"
: >"$T/gh.log"
check "reply: file outside /tmp" refused bash "$TOOL" reply 1 /etc/hostname
check "review: file outside /tmp" refused bash "$TOOL" review /etc/hostname
ln -s "$TOOL" "$T/link.md"
check "reply: /tmp symlink to a file outside" refused bash "$TOOL" reply 1 "$T/link.md"
mkfifo "$T/fifo"
check "reply: /tmp FIFO" refused bash "$TOOL" reply 1 "$T/fifo"
called "files outside /tmp" refused
check "threads: prints a file under /tmp" allowed \
  bash -c '[[ $(bash "$1" threads) == /tmp/pr-threads.* ]]' _ "$TOOL"

# Comment authors: bots and users with write access are trusted; others' bodies are hidden and
# their comments are not work. The stub gh serves $T/threads and the permissions in $T/perms.
mkdir "$T/bin5"
cat >"$T/bin5/gh" <<STUB
#!/bin/sh
case "\$2" in
  graphql) cat "$T/threads" ;;
  */pulls/1) cat "$T/pr5" ;;
  */permission) l=\${2#*collaborators/}; p=\$(grep "^\${l%/permission} " "$T/perms") || exit 1; echo "\${p#* }" ;;
esac
STUB
chmod +x "$T/bin5/gh"
printf '{"draft":false,"labels":[]}' >"$T/pr5"
printf '%s\n' 'maint write' 'boss admin' 'rando read' >"$T/perms"
c() { # c <login> [__typename] [body]
  jq -cn --arg l "$1" --arg t "${2:-User}" --arg b "${3:-do X}" '{databaseId: 1, body: $b, author: {login: $l, __typename: $t}}'
}
th() { # th <id> <comment>...: an unresolved thread with these comments, root first
  local id=$1; shift
  jq -cn --arg id "$id" --argjson n "$(printf '%s\n' "$@" | jq -s .)" \
    '{id: $id, isResolved: false, path: "f", line: 1, root: {nodes: $n[:1]}, comments: {totalCount: ($n | length), nodes: $n}}'
}
GA=$(c github-actions Bot); QO=$(c qodo-merge-pro Bot); MA=$(c maint); BO=$(c boss); RA=$(c rando)
GHOST='{"databaseId":1,"body":"do X","author":null}'
pend() { # pend <name> <want> <thread>...
  local name=$1 want=$2; shift 2
  printf '%s\n' "$@" >"$T/threads"
  check "open-work pending: $name" allowed bash -c 'cd "$2" && out=$(PR_TOOLS_PATH="$3/bin5:/usr/bin:/bin" bash "$1" open-work) &&
    [ "${out%% *}" = "$4" ]' _ "$TOOL" "$T/w" "$T" "$want"
}
pend "bot finding" 1 "$(th a "$GA")"
pend "bot finding answered" 0 "$(th a "$GA" "$GA")"
pend "maintainer and admin comments" 2 "$(th a "$MA")" "$(th b "$QO" "$GA" "$BO")"
pend "read-only and deleted users" 0 "$(th a "$RA")" "$(th b "$GHOST")" "$(th c "$MA" "$GA" "$RA")"
pend "read-only reply after a maintainer" 1 "$(th a "$MA" "$RA")"
pend "read-only reply to a bot finding" 1 "$(th a "$GA" "$RA")"
# Severity: the reviewer's findings tagged ❓ or 💡, or below MIN_SEVERITY (from a min_severity
# label), are left to people unless a maintainer asks on the thread; the first tag counts, and
# other bots' tags do not.
GA_NIT=$(c github-actions Bot $'**ScyllaDB Reviewer**\n\n_Org_ · 🔵 **Nit** — rename x')
GA_MIN=$(c github-actions Bot '_Org_ · 🟡 **Minor** — x')
GA_IMP=$(c github-actions Bot $'**ScyllaDB Reviewer**\n\n_Reviewer_ · 🟠 **Important** — x, unlike the 🔵 one')
GA_BLK=$(c github-actions Bot '_Security_ · 🔴 **High** — x')
GA_Q=$(c github-actions Bot '❓ **Question** — x')
QO_NIT=$(c qodo-merge-pro Bot '🔵 nit: rename x')
pend "nit and minor findings by default" 2 "$(th a "$GA_NIT")" "$(th b "$GA_MIN")" "$(th c "$GA_Q")" \
  "$(th d "$(c github-actions Bot '💡 **Praise** — x')")"
pend "maintainer asks on a question" 1 "$(th a "$GA_Q" "$MA")"
pend "only the leading tag counts" 1 "$(th a "$(c github-actions Bot $'**ScyllaDB Reviewer**\n\n**Medium** — unlike the 💡 above')")"
pend "a comment naming two passes" 1 "$(th a "$(c github-actions Bot '_Reviewer_ · _Org_ · 🟡 **Minor** — x')")"
export MIN_SEVERITY=minor
pend "min_severity minor" 1 "$(th a "$GA_NIT")" "$(th b "$GA_MIN")"
export MIN_SEVERITY=blocking
pend "min_severity blocking" 1 "$(th a "$GA_IMP")" "$(th b "$GA_BLK")"
pend "min_severity blocking, two passes named" 0 "$(th a "$(c github-actions Bot '_Reviewer_ · _Org_ · 🟡 **Minor** — x')")"
export MIN_SEVERITY=important
pend "min_severity important" 1 "$(th a "$GA_NIT")" "$(th b "$GA_MIN")" "$(th c "$GA_IMP")"
pend "maintainer asks on a low finding" 1 "$(th a "$GA_NIT" "$MA")"
pend "read-only reply to a low finding" 0 "$(th a "$GA_NIT" "$RA")"
pend "another bot's reply to a low finding" 0 "$(th a "$GA_NIT" "$QO")"
pend "another bot's reply to an important finding" 1 "$(th a "$GA_IMP" "$GA" "$QO")"
pend "low finding answered after a maintainer asked" 0 "$(th a "$GA_NIT" "$MA" "$GA")"
pend "another bot's low tag" 1 "$(th a "$QO_NIT")"
pend "low finding past the last 100 comments" 0 \
  "$(th a "$RA" | jq -c --argjson r "$GA_NIT" '.root.nodes = [$r] | .comments.totalCount = 101')"
printf '%s\n' "$(th a "$GA_NIT")" "$(th b "$GA_IMP")" "$(th c "$GA_NIT" "$MA")" >"$T/threads"
check "threads: the resolver does not get low findings" allowed bash -c 'f=$(PR_TOOLS_PATH="$2/bin5:/usr/bin:/bin" bash "$1" threads) &&
  [ "$(jq -r .id "$f" | tr "\n" " ")" = "b c " ]' _ "$TOOL" "$T"
check "threads: the review job gets every thread" allowed bash -c 'f=$(PR_TOOLS_PATH="$2/bin5:/usr/bin:/bin" GITHUB_JOB=review bash "$1" threads) &&
  [ "$(jq -r .id "$f" | tr "\n" " ")" = "a b c " ]' _ "$TOOL" "$T"
unset MIN_SEVERITY
# A draft, or the stop label, leaves no work even with an unanswered thread, without reading
# the threads; the label needs its prefix.
printf '%s\n' "$(th a "$MA")" >"$T/threads"
stopped() { # stopped <name> <PR labels JSON> [draft]
  printf '{"draft":%s,"labels":%s}' "${3:-false}" "$2" >"$T/pr5"
  check "open-work: $1" allowed bash -c 'cd "$2" && export PR_TOOLS_PATH="$3/bin5:/usr/bin:/bin" &&
    [ "$(bash "$1" open-work 2>/dev/null)" = "0 no" ] &&
    [ "$(bash "$1" open-work --output 2>/dev/null | tr "\n" " ")" = "pending=0 conflict=no work=false " ]' _ "$TOOL" "$T/w" "$T"
}
stopped "a draft has no work" '[]' true
stopped "the stop label leaves no work" '[{"name":"review_stop"}]'
printf '{"draft":false,"labels":[{"name":"stop"}]}' >"$T/pr5"
pend "the stop label needs its prefix" 1 "$(th a "$MA")"
printf '{"draft":false,"labels":[]}' >"$T/pr5"
printf '%s\n' "$(th a "$MA" "$RA" "$QO")" "$(th b "$GHOST")" >"$T/threads"
H='[hidden: comment by a user without write access]'
check "threads: untrusted bodies hidden" allowed bash -c 'f=$(PR_TOOLS_PATH="$2/bin5:/usr/bin:/bin" bash "$1" threads) &&
  [ "$(jq -sc "[.[].comments.nodes[] | [.trusted, .body]]" "$f")" = "$(jq -cn --arg h "$3" "[[true,\"do X\"],[false,\$h],[true,\"do X\"],[false,\$h]]")" ] &&
  ! grep -q root "$f"' _ "$TOOL" "$T" "$H"

# --- agent settings ----------------------------------------------------------------------
SETTINGS="$(dirname "$TOOL")/../agent-settings.json"
check "agent-settings.json: valid, hooks off, deny list" allowed \
  jq -e '.disableAllHooks == true and (.permissions.deny | length) > 0' "$SETTINGS"
# Agents could write HEAD, objects/, refs/ and a config with core.fsmonitor into any
# directory; with the settings' env, git does not take such a directory for a repository.
git init -q --bare "$T/planted"
mapfile -t agent_env < <(jq -r '.env | to_entries[] | "\(.key)=\(.value)"' "$SETTINGS")
check "implicit bare repository without the settings env" allowed git -C "$T/planted" rev-parse --git-dir
check "implicit bare repository with the settings env" refused \
  env "${agent_env[@]}" git -C "$T/planted" rev-parse --git-dir
# Nor do a planted repository's own fsmonitor command or hooks run.
git init -q "$T/planted2"; git -C "$T/planted2" config core.fsmonitor "touch $T/fsmonitor-ran"
echo x >"$T/planted2/f"; git -C "$T/planted2" add f
git -C "$T/planted2" -c user.name=t -c user.email=t@t commit -qm a
git -C "$T/planted2" status >/dev/null 2>&1
check "planted fsmonitor without the settings env" allowed test -e "$T/fsmonitor-ran"
rm -f "$T/fsmonitor-ran"
env "${agent_env[@]}" git -C "$T/planted2" status >/dev/null 2>&1
check "planted fsmonitor with the settings env" refused test -e "$T/fsmonitor-ran"
# No deny rule may contain whitespace other than a space: Claude Code normalizes it, and a rule
# like Bash(*<tab>*) then denied every command, so no review pass could run.
check "deny rules: no tab, newline or other control whitespace" allowed \
  jq -e 'all(.permissions.deny[]; test("[\t\n\r\f\u000b]") | not)' "$SETTINGS"
# Bash deny rules against commands the prompts rely on (allowed) and known bypasses (refused),
# matched the way Claude Code matches them: * is any text, \* and \\ are literal. This matches
# the raw command: whether Claude Code also turns a tab between arguments into a space (so a
# space-anchored rule such as Bash(git * /*) catches "git diff<tab>/etc/passwd") has not been
# checked on the pinned action version.
denied() {
  python3 -c '
import json, re, sys
def rx(p):
    out, i = "", 0
    while i < len(p):
        if p[i] == "\\" and p[i + 1:i + 2] in ("*", "\\"):
            out += re.escape(p[i + 1]); i += 2
        else:
            out += ".*" if p[i] == "*" else re.escape(p[i]); i += 1
    return out
rules = [r[5:-1] for r in json.load(open(sys.argv[1]))["permissions"]["deny"] if r.startswith("Bash(")]
sys.exit(0 if any(re.fullmatch(rx(r), sys.argv[2], re.S) for r in rules) else 1)' "$SETTINGS" "$1"
}
allowed_by_rules() { ! denied "$1"; }
rule() { check "deny list: $2" "$1" allowed_by_rules "$2"; } # rule <want: allowed|refused> <command>
rule allowed 'git diff abc123...def456'
rule allowed 'git diff abc123...def456 -- ad-hoc-scripts/eBPF/README.md'
rule allowed 'git log --oneline -5'
rule allowed 'git status'
rule refused 'git diff \/etc/passwd x'
rule refused 'git diff {~/.gitconfig,x}'
rule refused 'git diff x{,~/.gitconfig}'
rule refused "git diff {~/.''./.''./etc/hostname,x}"
rule refused 'git diff a..b -- x=~/.gitconfig'
rule refused 'git diff "".""/x y'
rule allowed 'git diff HEAD~1 -- x'
rule allowed 'gh pr view 1 --json title,body,baseRefName,headRefName,author'
rule allowed 'gh pr diff 1 --name-only'
rule allowed 'git add src/a.c'
# Agents get only git merge --abort today; the git merge rules guard a later git merge:* allow.
rule allowed 'git merge origin/main'
rule allowed 'git merge --abort'
rule allowed 'git commit --no-edit'
rule allowed 'git commit -m "Fix the parser (review comment)."'
rule allowed 'gh pr view 12 --json title'
# The gh -c/-q/-t/-w rules match anywhere, not only as options, so a branch name is refused too:
# deliberate, since they do not depend on how Claude Code splits whitespace; the prompts use
# PR numbers. The Edit denies are case-sensitive and repeat some ./**/ rules at the root in
# case **/ needs a directory; pr-tools push is the case-insensitive backstop.
rule refused 'gh pr view fix-ci'
rule refused 'gh pr view 12 --comments'
rule allowed '/home/runner/work/_temp/pr-tools reply 1 /tmp/reply.md'
rule refused 'git commit -aF/proc/self/environ'
rule refused 'git commit -F /tmp/x'
rule refused 'git commit --fil=.git/config'
rule refused 'git commit -m x \-F \/tmp/x'
rule refused 'git merge -nF/tmp/x origin/main'
rule refused 'git diff .. .'
rule refused 'git diff ./.. .'
rule refused 'git diff /etc/passwd x'
rule refused 'git diff -- ~/x'
rule refused 'git log --output=x'
rule refused 'git log -- .git/config'
rule refused 'git show :x >.git/config'
rule refused 'gh pr view 1 > x'
rule refused 'gh pr view 1 --json body --jq .body >>/tmp/x'
rule refused 'git grep -Obash -e . -- x.sh'
rule refused 'gh api repos/o/r'
# gh's jq and templates can read the job's environment (env, $ENV), so no gh filter at all.
rule refused "gh pr view 1 --json files --jq '.files[] | {path}'"
rule refused 'gh pr view 1 --json title --jq env'
rule refused 'gh pr view 1 --json title --jq=env'
rule refused 'gh pr view 1 --json title -q env'
rule refused 'gh pr view 1 --json title -wq env'
rule refused 'gh pr view 1 --json title -cwq env'
rule refused 'gh pr view 1 --json title -wct x'
rule refused 'gh pr view 1 --json title -ccq env'
rule refused 'gh pr view 1 --json title -wwct x'
rule refused 'gh pr view 1 --json title -cwcq env'
rule refused "gh pr view 1 --json title --template '{{env \"GH_TOKEN\"}}'"
rule refused "gh pr view 1 --json title -t '{{.title}}'"
rule refused "gh pr view 1 --json title --'j'q env"
rule refused 'gh pr view 1 --json title --j"q" env'
rule refused 'gh pr view 1 --json title --j? env'
rule refused 'gh pr view 1 --json title --j[q] env'
rule refused 'gh pr view 1 --json title --{jq,x} env'
rule refused 'gh pr view 1 --json title --j* env'
rule refused 'git diff $HOME/x'

[ "$fail" = 0 ] && echo "all passed"
exit "$fail"
