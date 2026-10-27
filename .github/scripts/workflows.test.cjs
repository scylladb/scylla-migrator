// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const yaml = require('js-yaml');

const workflow = yaml.load(readFileSync(join(__dirname, '../workflows/call_jira_sync.yml'), 'utf8'));
// These inspected conditions compare strings with lowercase literals. Actions
// ignores case in string comparisons; casefold these known fixture operands.
// This helper does not implement the general Actions expression language.
const routes = (action, label) => Object.entries(workflow.jobs).filter(([, job]) =>
  new Function('github', `return ${job.if}`)({event: {
    action: action.toLowerCase(), label: {name: label?.toLowerCase()}
  }})).map(([name]) => name);

test('Jira callers filter only exact omission-label add/remove events', () => {
  for (const [action, job] of [['labeled', 'jira-sync-add-label'], ['unlabeled', 'jira-sync-remove-label']]) {
    for (const label of ['omit-from-release-notes', 'Omit-from-release-notes', 'OMIT-FROM-RELEASE-NOTES']) {
      assert.deepEqual(routes(action, label), []);
    }
    for (const label of ['bug', 'omit-from-release-notes-extra', 'OMIT-FROM-RELEASE-NOTES-extra']) {
      assert.deepEqual(routes(action, label), [job]);
    }
  }
});

test('other Jira events retain their routes and reusable workflow authentication', () => {
  for (const [action, job] of [['opened', 'jira-sync-pr-opened'], ['edited', 'jira-sync-pr-opened'],
    ['ready_for_review', 'jira-sync-in-review'], ['review_requested', 'jira-sync-in-review'],
    ['closed', 'jira-sync-pr-closed']]) {
    assert.deepEqual(routes(action), [job]);
  }
  for (const job of Object.values(workflow.jobs)) {
    assert.match(job.uses, /^scylladb\/github-automation\/\.github\/workflows\/main_jira_sync_.*\.yml@main$/);
    assert.equal(job.secrets.caller_jira_auth, '${{ secrets.USER_AND_KEY_FOR_JIRA_AUTOMATION }}');
  }
});


test('release keeps exact tag build, trusted publisher, artifact and Docker dependencies', () => {
  const release = yaml.load(readFileSync(join(__dirname, '../workflows/release.yaml'), 'utf8'));
  assert.deepEqual(release.on.release.types, ['published']);
  assert.deepEqual(release.on.push.tags, ['v[0-9]*.[0-9]*.[0-9]*']);
  const {build, publish, docker} = release.jobs;
  assert.equal(build.steps[0].with.ref, 'refs/tags/${{ env.RELEASE_TAG }}');
  assert.match(build.steps.find(step => step.id === 'validate').run, /git rev-parse HEAD/);
  assert.ok(build.steps.some(step => step.run === 'make build'));
  const upload = build.steps.find(step => step.uses?.startsWith('actions/upload-artifact@'));
  assert.equal(upload.with.path, 'migrator/target/scala-2.13/scylla-migrator-assembly.jar');
  assert.equal(upload.with['if-no-files-found'], 'error');
  assert.equal(publish.needs, 'build');
  assert.equal(publish.steps[0].with.ref, '${{ github.event.repository.default_branch }}');
  const create = publish.steps.find(step => step.run === 'python3 .github/scripts/release_notes.py release');
  assert.equal(create.env.BUILT_COMMIT, '${{ needs.build.outputs.commit }}');
  assert.equal(create['continue-on-error'], undefined);
  const download = publish.steps.find(step => step.uses?.startsWith('actions/download-artifact@'));
  assert.equal(download.with.name, upload.with.name);
  assert.equal(download.with.path, 'release-artifact');
  assert.match(publish.steps.at(-1).run, /gh release upload.*release-artifact\/scylla-migrator-assembly.jar --clobber/);
  assert.equal(docker.needs, 'publish');
  assert.equal(publish.outputs.created, '${{ steps.release.outputs.created }}');
  assert.equal(create.id, 'release');
  const shouldPublish = new Function('github', 'needs', 'fromJSON', `return ${docker.if}`);
  for (const [event, attempt, created, expected] of [
    ['push', '1', 'true', true], ['push', '1', 'false', false],
    ['push', '2', 'true', true], ['push', '2', 'false', true],
    ['release', '1', 'true', false], ['release', '1', 'false', false],
    ['release', '2', 'false', false]]) {
    assert.equal(shouldPublish({event_name: event, run_attempt: attempt},
      {publish: {outputs: {created}}}, JSON.parse), expected, `${event}/${attempt}/${created}`);
  }
  // needs:publish and the absence of a status override retain Actions' success prerequisite.
  assert.ok(!docker.if.includes('always(') && !docker.if.includes('failure('));
  assert.equal(docker.uses, './.github/workflows/docker-publish.yml');
  const publisher = yaml.load(readFileSync(join(__dirname, '../workflows/docker-publish.yml'), 'utf8'));
  assert.equal(publisher.on.workflow_call.inputs.tag.required, true);
  assert.deepEqual(publisher.on.release.types, ['published']);
});


test('Docker shell receives release and manual tag text as literal environment data', () => {
  const {spawnSync} = require('node:child_process');
  const {mkdtempSync, rmSync, writeFileSync} = require('node:fs');
  const {tmpdir} = require('node:os');
  const workflow = yaml.load(readFileSync(join(__dirname, '../workflows/docker-publish.yml'), 'utf8'));
  const steps = workflow.jobs.publish.steps;
  const resolve = steps.find(step => step.id === 'resolve');
  const validate = steps.find(step => step.name === 'Validate tag format');
  const tags = steps.find(step => step.id === 'tags');
  assert.deepEqual(resolve.env, {EVENT_NAME: '${{ github.event_name }}',
    RELEASE_TAG: '${{ github.event.release.tag_name }}', INPUT_TAG: '${{ inputs.tag }}'});
  for (const step of [validate, tags]) assert.equal(step.env.GIT_TAG, '${{ steps.resolve.outputs.git_tag }}');
  for (const step of [resolve, validate, tags]) assert.ok(!step.run.includes('${{'));
  const dir = mkdtempSync(join(tmpdir(), 'migrator-release-shell-'));
  try {
    const output = join(dir, 'output');
    for (const event of ['release', 'workflow_dispatch', 'push']) {
      for (const tag of ['v2.1.7', 'v2.1.7$(printf injected)', 'v2.1.7"; printf injected; #', 'v2.1.7\ngit_tag=v3.0.0']) {
        writeFileSync(output, '');
        const env = {...process.env, GITHUB_OUTPUT: output, EVENT_NAME: event,
          RELEASE_TAG: event === 'release' ? tag : 'wrong', INPUT_TAG: event === 'release' ? 'wrong' : tag};
        const resolved = spawnSync('bash', ['-e', '-c', resolve.run], {env, encoding: 'utf8'});
        if (tag.includes('\n')) {
          assert.notEqual(resolved.status, 0);
          assert.equal(readFileSync(output, 'utf8'), '');
          continue;
        }
        assert.equal(resolved.status, 0, resolved.stderr);
        assert.equal(readFileSync(output, 'utf8'), `git_tag=${tag}\n`);
        assert.equal(resolved.stdout, '');
        const checked = spawnSync('bash', ['-e', '-c', validate.run],
          {env: {...env, GIT_TAG: tag}, encoding: 'utf8'});
        assert.equal(checked.status === 0, tag === 'v2.1.7');
        assert.ok(!checked.stdout.includes('v2.1.7injected'));
      }
    }
  } finally { rmSync(dir, {recursive: true, force: true}); }
});
