// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {test} = require('node:test');
const yaml = require('js-yaml');
const guard = require('./release-notes-guard.cjs');

const repo = {owner: 'scylladb', repo: 'scylla-migrator'};
const label = 'omit-from-release-notes';
const file = (filename, extra = {}) => ({filename, status: 'modified', ...extra});
const workflowText = readFileSync(join(__dirname, '../workflows/label-release-notes.yml'), 'utf8');
const workflow = yaml.load(workflowText);
const steps = workflow.jobs.classify.steps;
const scripts = steps.filter(step => step.with?.script).map(step => step.with.script);
const collectScript = steps.find(step => step.id === 'guard').with.script;
const verifyScript = steps.at(-1).with.script;
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

function fixture(options = {}) {
  const files = options.files ?? [file('README.md')];
  const labels = new Set(options.labels ?? ['unrelated']);
  const calls = [];
  let gets = 0;
  let listings = 0;
  const initial = {head: {sha: 'head'}, base: {sha: 'base'}, changed_files: files.length};
  const github = {rest: {pulls: {
    get: async params => {
      assert.deepEqual(params, {...repo, pull_number: 20});
      calls.push('get');
      gets++;
      if (options.failGetAt === gets) throw new Error('PR lookup unavailable');
      return {data: {...initial, ...options.revisions?.[gets - 1]}};
    },
    listFiles: () => {}
  }, issues: {
    listLabelsOnIssue: () => {},
    removeLabel: async params => {
      assert.deepEqual(params, {...repo, issue_number: 20, name: label});
      calls.push('remove');
      if (options.cleanupFailure) throw new Error('Removal unavailable');
      // GitHub looks up the logical label without case sensitivity while
      // preserving its stored spelling in returned metadata.
      const actual = [...labels].find(name => name.toLowerCase() === label);
      if (actual === undefined) throw {status: 404};
      labels.delete(actual);
    }
  }}, paginate: async (method, params) => {
    assert.equal(params.per_page, 100);
    if (method === github.rest.pulls.listFiles) {
      assert.deepEqual(params, {...repo, pull_number: 20, per_page: 100});
      calls.push('files');
      if (options.fileFailure) throw new Error('File listing unavailable');
      return options.listings?.[listings++] ?? files;
    }
    assert.equal(method, github.rest.issues.listLabelsOnIssue);
    assert.deepEqual(params, {...repo, issue_number: 20, per_page: 100});
    calls.push('labels');
    if (options.labelFailure) throw new Error('Labels unavailable');
    return options.labelMetadata ?? [...labels].map(name => ({name}));
  }};
  return {github, labels, calls};
}

async function adapter(f, hash, overrides = {}, load = () => guard) {
  const env = {PR_NUMBER: '20', CONFIRMED_PR: 'true', TARGET_OUTCOME: 'success', CHECKOUT_OUTCOME: 'success', INSTALL_OUTCOME: 'success',
    GUARD_OUTCOME: 'success', LABELER_OUTCOME: 'success', UNSAFE_RENAME: 'false',
    SNAPSHOT_HASH: hash, ...overrides};
  const logs = [];
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', verifyScript);
  let error;
  try {
    await run(f.github, {repo}, {info: text => logs.push(text), error: text => logs.push(text)},
      load, {env});
  } catch (caught) { error = caught; }
  return {error, logs};
}

test('canonical policy matches mixed support files and keeps runtime or unknown paths visible', async () => {
  const files = ['README.md', 'CONTRIBUTING.md', 'docs/guide.md', 'tests/src/test/configurations/input.yaml',
    'tests/docker/init.cql', 'benchmarks/src/main/scala/Benchmark.scala', '.github/dependabot.yml',
    '.github/workflows/tests.yml', '.github/workflows/docs-pr.yaml', '.github/scripts/test_release_notes.py',
    '.github/scripts/workflows.test.cjs', '.gitignore', '.scalafmt.conf', 'docker-compose-tests.yml'].map(name => file(name));
  assert.equal((await guard.snapshot(fixture({files}).github, repo, 20)).omit, true);
  for (const name of ['migrator/src/main/scala/Reader.scala', 'build.sbt', 'project/plugins.sbt',
    'Dockerfile.release', 'Makefile', 'ansible/scylla-migrator.yml', 'docker-compose.yaml',
    '.github/workflows/release.yaml', '.github/workflows/docker-publish.yml', '.github/workflows/build.yml',
    '.github/scripts/release_notes.py', '.github/scripts/release-notes-guard.cjs',
    '.github/labeler.yml', '.github/release.yml', '.github/package.json', '.github/package-lock.json',
    '.github/unknown.yml', 'LICENSE', 'unknown.txt']) {
    const current = await guard.snapshot(fixture({files: [...files, file(name)]}).github, repo, 20);
    assert.equal(current.omit, false, name);
  }
});

test('renames use the actual canonical policy for both prior and current paths', async () => {
  for (const [previous, omit] of [['README.md', true], ['migrator/src/main/scala/Reader.scala', false], ['unknown', false]]) {
    const current = await guard.snapshot(fixture({files: [file('CONTRIBUTING.md', {
      status: 'renamed', previous_filename: previous})]}).github, repo, 20);
    assert.equal(current.omit, omit);
    assert.equal(current.unsafeRename, !omit);
  }
});

test('support-to-runtime renames remain visible without skipping the stock action', async () => {
  const current = await guard.snapshot(fixture({files: [file('migrator/src/main/scala/Reader.scala', {
    status: 'renamed', previous_filename: 'tests/src/test/ReaderTest.scala'})]}).github, repo, 20);
  assert.equal(current.omit, false);
  assert.equal(current.unsafeRename, false);
});

test('unsupported config shapes fail instead of approximating a second policy', () => {
  for (const source of ['{}', 'omit-from-release-notes: []',
    'omit-from-release-notes: [{head-branch: [main]}]',
    'omit-from-release-notes: [{changed-files: [{any-glob-to-any-file: ["**"]}]}]',
    'omit-from-release-notes: [{changed-files: [{all-globs-to-all-files: []}]}]',
    'other-label: [{changed-files: [{all-globs-to-all-files: ["**"]}]}]']) {
    assert.throws(() => guard.policy(source), /Unsupported labeler configuration/);
  }
});

test('PR input rejects malformed, ambiguous and unsafe numbers', () => {
  assert.equal(guard.parseNumber('20'), 20);
  for (const value of ['', '0', '-1', '01', '1.0', '1e2', ' 20', '20\n',
    '20,21', '${{ arbitrary }}', '9007199254740992', undefined, 20]) {
    assert.throws(() => guard.parseNumber(value), /canonical positive safe PR number/);
  }
});

test('pagination requests full metadata and accepts a complete listing beyond one page', async () => {
  const f = fixture({files: Array.from({length: 105}, (_, i) => file(`tests/src/test/fixtures/${i}.json`))});
  const snapshot = await guard.snapshot(f.github, repo, 20);
  assert.equal(snapshot.count, 105);
  assert.equal(snapshot.omit, true);
  assert.match(snapshot.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(f.calls, ['get', 'files', 'get']);
});

test('zero, incomplete, truncated, duplicate and invalid file evidence fail visibly', async () => {
  const cases = [
    {files: []},
    {revisions: [{changed_files: 2}, {changed_files: 2}]},
    {files: [file('README.md'), file('README.md')]},
    {files: [file('README.md', {status: 'renamed'})]},
    {files: [file('README.md', {status: undefined})]},
    {files: [file('README.md', {previous_filename: ''})]},
    {files: [file('README.md')], listings: [[]]}
  ];
  for (const options of cases) await assert.rejects(guard.snapshot(fixture(options).github, repo, 20));
});

test('head, base and count changes during collection are rejected', async () => {
  for (const changed of [{head: {sha: 'new'}}, {base: {sha: 'new'}}, {changed_files: 2}]) {
    const f = fixture({revisions: [{}, changed]});
    await assert.rejects(guard.snapshot(f.github, repo, 20), /revisions changed/);
  }
});

test('verification detects before/after revisions and filename, status or prior-path changes', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [
    {revisions: [{head: {sha: 'new'}}, {head: {sha: 'new'}}]},
    {revisions: [{base: {sha: 'new'}}, {base: {sha: 'new'}}]},
    {files: [file('docs/new.md')]},
    {files: [file('README.md', {status: 'added'})]},
    {files: [file('README.md', {status: 'renamed', previous_filename: 'docs/old.md'})]}
  ]) {
    await assert.rejects(guard.verify(fixture(options).github, repo, 20, original.hash), /snapshot changed/);
  }
  const f = fixture({labels: [label], revisions: [{}, {}, {head: {sha: 'new'}}]});
  await assert.rejects(guard.verify(f.github, repo, 20, original.hash), /revisions changed during label/);
});

test('lookup, listing and actual-label lookup failures propagate', async () => {
  await assert.rejects(guard.snapshot(fixture({failGetAt: 1}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({failGetAt: 2}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({fileFailure: true}).github, repo, 20), /listing unavailable/);
  const original = await guard.snapshot(fixture().github, repo, 20);
  await assert.rejects(guard.verify(fixture({labelFailure: true}).github, repo, 20, original.hash), /Labels unavailable/);
});

test('actual-label verification catches stale state after internal action skip', async () => {
  for (const [files, labels] of [[[file('README.md')], []], [[file('migrator/src/main/scala/Reader.scala')], [label]]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Actual omission label/);
    assert.equal(f.labels.has(label), false);
    assert.equal(f.labels.has('unrelated'), true);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('failed final snapshot, lookup or label retrieval attempts cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [{files: [file('docs/changed.md')]}, {failGetAt: 1},
    {fileFailure: true}, {labelFailure: true}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const {error} = await adapter(f, original.hash);
    assert.ok(error);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('successful verification checks actual labels and preserves unrelated labels', async () => {
  for (const [files, labels] of [[[file('README.md')], [label]], [[file('migrator/src/main/scala/Reader.scala')], []]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error, logs} = await adapter(f, original.hash);
    assert.ifError(error);
    assert.match(logs[0], /Verified head=head base=base count=1 snapshot=.*actual label checked/);
    assert.equal(f.labels.has('unrelated'), true);
    assert.ok(!f.calls.includes('remove'));
  }
});

test('actual logical marker identity preserves support omissions and rejects runtime omissions regardless of casing', async () => {
  const marker = 'Omit-from-release-notes';
  const other = 'OMIT-FROM-RELEASE-NOTES-extra';
  for (const [name, omit] of [['README.md', true], ['migrator/src/main/scala/Reader.scala', false]]) {
    const f = fixture({files: [file(name)], labels: [marker, other, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error} = await adapter(f, original.hash);
    if (omit) {
      assert.ifError(error);
      assert.equal(f.labels.has(marker), true);
      assert.ok(!f.calls.includes('remove'));
    } else {
      assert.match(error.message, /Actual omission label/);
      assert.equal(f.labels.has(marker), false);
      assert.equal(f.calls.filter(call => call === 'remove').length, 1);
    }
    assert.equal(f.labels.has(other), true);
    assert.equal(f.labels.has('unrelated'), true);
  }
});

test('invalid actual-label metadata fails verification and attempts canonical cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const item of [null, {}, {name: 42}, {name: ''}]) {
    const f = fixture({labels: [label, 'unrelated'], labelMetadata: [item]});
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Invalid label metadata/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('intentional unsafe-rename skip removes omission label and verifies visible state', async () => {
  const f = fixture({files: [file('README.md', {status: 'renamed', previous_filename: 'migrator/src/main/scala/Reader.scala'})],
    labels: [label, 'unrelated']});
  const original = await guard.snapshot(f.github, repo, 20);
  const {error, logs} = await adapter(f, original.hash, {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.ifError(error);
  assert.match(logs[0], /omit=false; actual label checked/);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.equal(f.calls.filter(call => call === 'remove').length, 1);
});

test('failed or skipped setup, guard and labeler clean up without requiring the helper', async () => {
  for (const key of ['TARGET_OUTCOME', 'CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    for (const outcome of ['failure', 'skipped', 'cancelled', '']) {
      const f = fixture({labels: [label, 'unrelated']});
      const {error} = await adapter(f, '', {[key]: outcome}, () => { throw new Error('Must not require helper'); });
      assert.match(error.message, /predecessor failed or unexpectedly skipped/);
      assert.deepEqual([...f.labels], ['unrelated']);
      assert.deepEqual(f.calls, ['remove']);
    }
  }
  const f = fixture({labels: [label]});
  const {error} = await adapter(f, '', {}, () => { throw new Error('Helper missing'); });
  assert.match(error.message, /Helper missing/);
  assert.deepEqual(f.calls, ['remove']);
});

test('cleanup API failures remain visible without losing the original failure or retrying', async () => {
  const f = fixture({cleanupFailure: true});
  const result = await adapter(f, '', {INSTALL_OUTCOME: 'failure'});
  assert.match(result.error.message, /predecessor failed/);
  assert.match(result.logs[0], /cleanup failed: Removal unavailable/);
  assert.deepEqual(f.calls, ['remove']);
  const unsafe = fixture({cleanupFailure: true});
  const result2 = await adapter(unsafe, '', {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.match(result2.error.message, /Removal unavailable/);
  assert.deepEqual(unsafe.calls, ['remove']);
});

test('workflow adapters use environment input and emit bounded verified snapshot outputs', async () => {
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', collectScript);
  const outputs = {};
  const f = fixture();
  await run(f.github, {repo}, {setOutput: (key, value) => { outputs[key] = value; }, info: () => {}},
    () => guard, {env: {PR_NUMBER: '20'}});
  assert.deepEqual(Object.keys(outputs), ['hash', 'unsafe-rename']);
  assert.equal(outputs['unsafe-rename'], 'false');
  const invalid = await adapter(f, '', {PR_NUMBER: '01'});
  assert.match(invalid.error.message, /canonical positive safe PR number/);
  assert.equal(steps.at(-1).if, 'always()');
  assert.equal(steps.find(step => step.id === 'checkout').with.ref, '${{ github.event.repository.default_branch }}');
  assert.equal(steps.find(step => step.id === 'checkout').with['persist-credentials'], false);
  assert.equal(steps.find(step => step.id === 'install').run, 'npm ci --ignore-scripts --prefix .github');
  const action = steps.find(step => step.id === 'labeler');
  assert.equal(action.if, "success() && steps.guard.outputs.unsafe-rename == 'false'");
  assert.equal(action.with['sync-labels'], true);
  assert.equal(action.with['pr-number'], '${{ env.PR_NUMBER }}');
  for (const script of scripts) assert.ok(!script.includes('${{'));
});

test('merged-PR backfills use the explicit default branch rather than old PR revisions', () => {
  const expression = steps.find(step => step.id === 'checkout').with.ref.replace(/^\$\{\{\s*|\s*\}\}$/g, '');
  const checkoutRef = new Function('github', `return ${expression}`);
  const event = {repository: {default_branch: 'master'},
    pull_request: {head: {sha: 'untrusted'}, base: {sha: 'old-base'}}};
  assert.equal(checkoutRef({sha: 'selected-dispatch-commit', event}), 'master');
});

test('dispatch precondition rejects non-default refs before target confirmation or checkout', async () => {
  assert.equal(steps[0].name, 'Require default-branch manual dispatch');
  const run = new AsyncFunction('context', steps[0].with.script);
  const payload = {repository: {default_branch: 'master'}};
  await run({eventName: 'workflow_dispatch', ref: 'refs/heads/master', payload});
  await run({eventName: 'pull_request_target', ref: 'refs/heads/release-branch', payload});
  for (const ref of ['refs/heads/feature', 'refs/tags/v1.2.3']) {
    await assert.rejects(run({eventName: 'workflow_dispatch', ref, payload}), /repository default branch/);
  }
  const f = fixture({labels: [label, 'unrelated']});
  const {error} = await adapter(f, '', {CONFIRMED_PR: undefined, TARGET_OUTCOME: 'skipped',
    CHECKOUT_OUTCOME: 'skipped', INSTALL_OUTCOME: 'skipped', GUARD_OUTCOME: 'skipped',
    LABELER_OUTCOME: 'skipped'}, () => {throw new Error('Must not load helper');});
  assert.match(error.message, /not confirmed/);
  assert.deepEqual([...f.labels], [label, 'unrelated']);
  assert.deepEqual(f.calls, []);
});


async function confirmTarget(options = {}) {
  const outputs = {};
  const calls = [];
  const context = {repo, eventName: options.eventName ?? 'workflow_dispatch',
    payload: {pull_request: options.payload}};
  const github = {rest: {pulls: {get: async params => {
    calls.push(params);
    if (options.lookupError) throw options.lookupError;
    return {data: {number: options.number ?? 20}};
  }}}};
  let error;
  try {
    await new AsyncFunction('github', 'context', 'core', 'process',
      steps.find(step => step.id === 'target').with.script)(github, context,
      {setOutput: (key, value) => {outputs[key] = value;}}, {env: {PR_NUMBER: options.input ?? '20'}});
  } catch (caught) { error = caught; }
  return {outputs, calls, error};
}

test('identity is confirmed before setup from matching trusted event or manual PR lookup', async () => {
  assert.ok(steps.findIndex(step => step.id === 'target') < steps.findIndex(step => step.id === 'checkout'));
  assert.ok(steps.findIndex(step => step.id === 'target') < steps.findIndex(step => step.id === 'install'));
  const event = await confirmTarget({eventName: 'pull_request_target', payload: {number: 20}});
  assert.ifError(event.error);
  assert.deepEqual(event.calls, []);
  assert.equal(event.outputs['confirmed-pr'], 'true');
  const manual = await confirmTarget();
  assert.ifError(manual.error);
  assert.deepEqual(manual.calls, [{...repo, pull_number: 20}]);
  assert.equal(manual.outputs['confirmed-pr'], 'true');
  assert.equal(steps.at(-1).env.CONFIRMED_PR, '${{ steps.target.outputs.confirmed-pr }}');
  assert.equal(steps.at(-1).env.TARGET_OUTCOME, '${{ steps.target.outcome }}');
});

test('ordinary issue, unavailable lookup, mismatched identity and malformed input make zero label writes', async () => {
  for (const options of [{lookupError: {status: 404}}, {lookupError: new Error('Unavailable')},
    {number: 21}, {eventName: 'pull_request_target', payload: {number: 21}},
    {eventName: 'pull_request_target'}, {input: '01'}]) {
    const target = await confirmTarget(options);
    assert.ok(target.error);
    assert.equal(target.outputs['confirmed-pr'], undefined);
    const f = fixture({labels: [label, 'unrelated']});
    const result = await adapter(f, '', {CONFIRMED_PR: target.outputs['confirmed-pr'],
      TARGET_OUTCOME: 'failure', CHECKOUT_OUTCOME: 'skipped'},
      () => {throw new Error('Must not load helper');});
    assert.match(result.error.message, /not confirmed/);
    assert.deepEqual(f.calls, []);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
  }
});

test('confirmed identity survives later setup, helper and API failures without a fresh identity lookup', async () => {
  for (const options of [{}, {eventName: 'pull_request_target', payload: {number: 20}}]) {
    const target = await confirmTarget(options);
    assert.ifError(target.error);
    for (const overrides of [{CHECKOUT_OUTCOME: 'failure'}, {INSTALL_OUTCOME: 'failure'},
      {GUARD_OUTCOME: 'failure'}, {}]) {
      const f = fixture({labels: [label, 'unrelated'], failGetAt: 1});
      const result = await adapter(f, '', {CONFIRMED_PR: target.outputs['confirmed-pr'], ...overrides},
        () => {throw new Error('Helper unavailable');});
      assert.ok(result.error);
      assert.deepEqual(f.calls, ['remove']);
      assert.deepEqual([...f.labels], ['unrelated']);
    }
    const f = fixture({labels: [label, 'unrelated'], failGetAt: 1});
    const result = await adapter(f, '', {CONFIRMED_PR: target.outputs['confirmed-pr']});
    assert.match(result.error.message, /lookup unavailable/);
    assert.deepEqual(f.calls, ['get', 'remove']);
  }
});
