"""Offline tests of only the generated source-bound qualification materializer."""
from pathlib import Path
import ast,base64,copy,hashlib,json,shlex,zlib
import pytest

SHELL=Path(__file__).with_name('finite-handoff-registration-20261103.sh')
REPO=Path(__file__).resolve().parents[4]
body=SHELL.read_text().split("python -B -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
tree=ast.parse(body)
def assignment(name):return next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id==name for x in n.targets))
payload=ast.literal_eval(assignment('payload').value)
requirements=ast.literal_eval(assignment('requirements').value)
start=tree.body.index(assignment('requirements'))
end=next(i for i,n in enumerate(tree.body) if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Attribute) and n.value.func.attr=='mkdir')
materializer=compile(ast.Module(body=tree.body[start:end],type_ignores=[]),'<materializer-only>','exec')

def test_remote_requirement_paths_are_absolute_posix_paths_on_every_build_host():
    for item in requirements.values():
        for value in (item['record'],item['manifest']['path']):
            assert value.startswith('/scratch/pwa209/horsehair-worm/maintenance/')
            assert '\\' not in value

def environment(tmp_path,change=None):
    mapping={};receipts={}
    local={'primary':REPO/Path('operations/20261024/census-resilience-consumers-v2/source-manifest.json'),'agent':REPO/Path('operations/20261025/agent-coordination-repair-v1/source-manifest.json')}
    for label,item in requirements.items():
        path=tmp_path/(label+'-manifest.json');path.write_bytes(local[label].read_bytes());mapping[item['manifest']['path']]=path
        value={'job_id':'900001' if label=='primary' else '900002'}
        if label=='primary':value.update(status='native_consumer_qualification_registered_and_released',source_manifest=item['manifest'])
        else:value.update(status='registered_and_released_qualification',source_manifest_sha256=item['manifest']['sha256'])
        receipts[item['record']]=value
    if change:change(mapping,receipts)
    def fake_path(path):return mapping[path]
    def read_json(path):
        if path not in receipts:raise FileNotFoundError(path)
        return receipts[path]
    return {'Path':fake_path,'hashlib':hashlib,'base64':base64,'zlib':zlib,'read_json':read_json,'payload':copy.deepcopy(payload)}

def test_actual_auto_receipts_materialize_unique_final_qids_and_all_source_pins(tmp_path):
    ns=environment(tmp_path);exec(materializer,ns)
    item=ns['payload']['handoff.py'];raw=zlib.decompress(base64.b64decode(item['data']))
    assert b'__HHW_' not in raw and b'8386042' not in raw and b'8390228' not in raw
    assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
    runtime=ast.parse(raw);dependencies=ast.literal_eval(next(n.value for n in runtime.body if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='DEPENDENCIES' for x in n.targets)))
    assert dependencies=={'900001':4,'900002':4,'6983037':8}
    # Evaluate just the final task literal. No file creation or Slurm call runs.
    canon=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    ns.update(name='fixture',script=tmp_path/'script',text='fixture source',argv=['python','handoff.py'],canonical_hash=canon,shlex=shlex)
    task=eval(compile(ast.Expression(assignment('task').value),'<task-only>','eval'),ns)
    assert task['external_dependencies']==list(dependencies)
    assert task['finite_resume_handoff']['source_sha256']==item['sha256']
    assert task['finite_resume_handoff']['children']=={name:value['sha256'] for name,value in ns['payload'].items() if name!='handoff.py'}

@pytest.mark.parametrize('failure',['missing','wrong_source','wrong_status','duplicate','invalid_id'])
def test_invalid_auto_prerequisite_fails_before_any_materialized_write(tmp_path,failure):
    def change(mapping,receipts):
        primary=requirements['primary']['record'];agent=requirements['agent']['record']
        if failure=='missing':receipts.pop(primary)
        elif failure=='wrong_source':receipts[agent]['source_manifest_sha256']='a'*64
        elif failure=='wrong_status':receipts[primary]['status']='submitting'
        elif failure=='duplicate':receipts[agent]['job_id']=receipts[primary]['job_id']
        else:receipts[agent]['job_id']='not-a-job'
    ns=environment(tmp_path,change);before=copy.deepcopy(ns['payload'])
    with pytest.raises((AssertionError,FileNotFoundError)):exec(materializer,ns)
    assert ns['payload']==before
    assert all(isinstance(n,ast.Assign) or not isinstance(n,ast.Expr) or not isinstance(n.value,ast.Call) or not isinstance(n.value.func,ast.Attribute) or n.value.func.attr!='mkdir' for n in tree.body[:end])
