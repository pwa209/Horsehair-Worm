"""Build one source-bound archive test registration locally; never submit it."""
from pathlib import Path
import ast
import base64
import hashlib
import json
import zlib

WORK = Path('work')
ROOT = '/scratch/pwa209/horsehair-worm'
AREA = ROOT + '/maintenance/study-af-archive-consumer-qualification-stage-v3-20261023'
CASE = ROOT + '/maintenance/study-af-archive-consumer-qualification-support-v3-20261023'
BASE = ROOT + '/maintenance/study-af-registry-repair-support-v6-20261022/'
DRIVER = 'qualify-f2-archive-consumer-v3-20261023.py'
TEST = 'test_qualify_f2_archive_consumer_v3_20261023.py'
TARGET = WORK / 'hhw-recovery-stage-20261021-406.sh'
TEMPLATE = WORK / 'hhw-recovery-stage-20261021-394.sh'
assert hashlib.sha256(TEMPLATE.read_bytes()).hexdigest() == '8254e96c402b4d8aa43c98d43880a607f2d222408d0f1eb2c351a60277f29e9e'
freeze = json.loads((WORK / 'archive-consumer-qualification-freeze-20261023.json').read_text(encoding='utf8'))
assert freeze['kind'] == 'local_archive_consumer_qualification_candidate_v1'
for name, item in freeze['files'].items():
    raw = (WORK / name).read_bytes()
    assert len(raw) == item['bytes'] and hashlib.sha256(raw).hexdigest() == item['sha256'], name
assert DRIVER in freeze['files'] and TEST in freeze['files']
tree = ast.parse((WORK / DRIVER).read_bytes())
assignments = {target.id: node.value for node in tree.body if isinstance(node, ast.Assign)
               for target in node.targets if isinstance(target, ast.Name)}
# Explicit closure avoids importing an executable qualifier while building it.
names = [
    'run-af-agent-controller-archive-v3-20261023.py',
    'test_af_agent_controller_archive_v3_20261023.py',
    'verify-f2-condition-archives-v2-20261022.py',
    'test_verify_f2_condition_archives_v2_20261022.py',
    'run-af-agent-controller-registry-v6-20261022.py',
    TEST,
]
assert all(name in freeze['files'] for name in names)
sources = {}
for name in names:
    raw = (WORK / name).read_bytes()
    ast.parse(raw)
    sources[name] = {'data': base64.b64encode(zlib.compress(raw, 9)).decode(),
                     'sha256': freeze['files'][name]['sha256']}
admin = (WORK / 'registered-snapshot-read-guard-v4-20261022.py').read_bytes()
assert hashlib.sha256(admin).hexdigest() == '5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
sources['administration-registry-read-guard-v4-20261022.py'] = {
    'data': base64.b64encode(zlib.compress(admin, 9)).decode(),
    'sha256': hashlib.sha256(admin).hexdigest(),
}
text = TEMPLATE.read_text(encoding='utf8')
payload_line = next(line for line in text.splitlines() if line.startswith('payload='))
blob = next(node.value for node in ast.walk(ast.parse(payload_line))
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and len(node.value) > 100)
payload = json.loads(zlib.decompress(base64.b64decode(blob)))
assert payload['id'] == '394' and not payload['after'] and not payload['after_specs']
raw = (WORK / DRIVER).read_bytes()
payload.update(id='406', helper=DRIVER, helper_body=base64.b64encode(zlib.compress(raw, 9)).decode(),
               helper_sha256=freeze['files'][DRIVER]['sha256'], area=AREA)
plan = ROOT + '/plans/recovery-support-20261021-406/plan.json'
ledger = ROOT + '/state/recovery-support-20261021-406-submission.json'
script = ROOT + '/plans/recovery-support-20261021-406/recovery-406.sbatch'
payload['args'] = [
    '--overlay', AREA + '/' + names[0], '--overlay-sha256', freeze['files'][names[0]]['sha256'],
    '--verifier', AREA + '/' + names[2], '--verifier-sha256', freeze['files'][names[2]]['sha256'],
    '--base-controller', BASE + names[4], '--base-controller-sha256', freeze['files'][names[4]]['sha256'],
    '--overlay-tests', AREA + '/' + names[1], '--verifier-tests', AREA + '/' + names[3],
    '--qualification-case', CASE, '--native-plan', plan, '--native-ledger', ledger,
    '--native-script', script, '--native-task', 'recovery-406',
]
assert len(payload['args']) == 26 and payload['cpus'] == 4 and payload['hours'] == 1 and payload['memory'] == '16G'
encoded = base64.b64encode(zlib.compress(json.dumps(payload).encode(), 9)).decode()
text = text.replace(payload_line, "payload=json.loads(zlib.decompress(base64.b64decode('" + encoded + "')))", 1)
source_line = next(line for line in text.splitlines() if line.startswith('for name,item in {'))
text = text.replace(source_line, 'for name,item in ' + repr(sources) + '.items():', 1)
begin = text.index('    # Both previous registrations ended before plan/sbatch publication.')
end = text.index('    admission=combined_admission', begin)
text = text[:begin] + '''    # This is a new logical qualification, never a replay of a failed request.
    fresh=[root/'plans/recovery-support-20261021-406',root/'state/recovery-support-20261021-406-submission.json']
    assert all(not p.exists() and not p.is_symlink() for p in fresh),'Registration already published; reconciliation required'
''' + text[end:]
needle = "    task['external_dependencies']=[census_job]\n"
assert text.count(needle) == 1
replacement = '''    q_directory=root/'plans/recovery-support-20261021-394'
    q_plan=read_json(q_directory/'plan.json');q_ledger=read_json(root/'state/recovery-support-20261021-394-submission.json')
    assert q_plan['release_identity']['sha256']==q_ledger['source_sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
    assert canonical_hash(q_plan)==q_ledger['plan_sha256'] and q_plan['config_sha256']==q_ledger['config_sha256']==canonical_hash(config)
    q_tasks=[t for t in q_plan['tasks'] if t['name']=='recovery-394'];assert len(q_tasks)==1
    q_task=q_tasks[0];q_record=q_ledger['tasks']['recovery-394']
    assert q_task['resources']=={'cpus':4,'memory':'16G','hours':1,'gpu':False}
    assert canonical_hash(Path(q_task['script']).read_text())==q_task['script_sha256']==q_record['script_sha256']
    q_intent=read_json(root/'maintenance/study-af-registry-repair-support-v6-20261022/submission-intent.json')
    assert q_intent['helper_sha256']=='875ec697466351b21718eded61a6e81ac23d74ce0902ce481ebc970012261020'
    assert q_record['state']=='submitted' and q_record['job_id']=='6822432'
    assert census_job=='6811498'
    task['external_dependencies']=[census_job,q_record['job_id']]
'''
text = text.replace(needle, replacement, 1)
body = text.split("python -u - <<'PY'\n", 1)[1].rsplit('\nPY', 1)[0]
ast.parse(body)
raw = text.encode('utf8')
assert b'\r' not in raw and not TARGET.exists()
TARGET.write_bytes(raw)
print(json.dumps({'request': str(TARGET), 'sha256': hashlib.sha256(raw).hexdigest(),
                  'helper_sha256': payload['helper_sha256'], 'private_closure_files': len(sources),
                  'native_dependencies': ['6811498', '6822432'], 'scheduler_submitted': False}))
