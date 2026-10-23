"""Local factory contracts on explicit synthetic primary source fixtures only."""
from pathlib import Path
import ast
import base64
import importlib.util
import json
import shutil
import zlib

import pytest

W=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('primary469_factory',W/'build-primary-external-census-qualification-469.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)


@pytest.fixture
def frozen(tmp_path,monkeypatch):
    src=tmp_path/'source';src.mkdir();baseline=tmp_path/'baseline';(baseline/'tests').mkdir(parents=True)
    (src/b.HELPER).write_text('# Explicit synthetic source fixture; never deployed.\ndef main():\n return 0\n')
    (src/b.TEST).write_text('# Explicit synthetic test source; no qualification claim.\ndef test_fixture():\n pass\n')
    for name in ('test_pipeline.py','test_budget.py'):(baseline/'tests'/name).write_text('def test_fixture():\n pass\n')
    files={name:b.sha((src/name).read_bytes()) for name in (b.HELPER,b.TEST)}
    suites={scope:{'source':relative,'source_sha256':b.sha(((baseline if scope.startswith('baseline_') else src)/relative).read_bytes()),
        'tests':1,'roster_sha256':'a'*64} for scope,relative in {'baseline_pipeline':'tests/test_pipeline.py','baseline_budget':'tests/test_budget.py','primary_policy':b.TEST}.items()}
    manifest={'kind':'primary-external-census-source-manifest-v1','files':files,'suites':suites}
    path=src/'source-manifest.json';path.write_bytes(b.json_bytes(manifest));census=tmp_path/'census';census.mkdir()
    actual=W/'rolling-native-v5';cm=json.loads((actual/'source-manifest.json').read_text())
    for name in [*cm['files'],'source-manifest.json','run-af-agent-controller-registry-v6-20261022.py']:shutil.copyfile(actual/name,census/name)
    monkeypatch.setattr(b,'TEMPLATE',W/'hhw-recovery-stage-20261021-394.sh')
    return {'source':src,'baseline':baseline,'census':census,'manifest':manifest,'path':path}


def render(f):return b.render(f['source'],f['path'],b.sha(f['path'].read_bytes()),baseline_directory=f['baseline'],census_directory=f['census'])


def unpack(raw):
    text=raw.decode();line=next(l for l in text.splitlines() if l.startswith('payload='))
    blob=next(n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100)
    return text,json.loads(zlib.decompress(base64.b64decode(blob)))


def test_frozen_inputs_render_exact_own_qualification_source_arguments_and_native_contract(frozen):
    raw,mraw,praw,eraw=render(frozen);text,p=unpack(raw);policy=json.loads(praw);evidence=json.loads(eraw)
    assert mraw==frozen['path'].read_bytes() and policy['qualification'] is None
    assert policy['source_manifest']=={'path':b.AREA+'/source-manifest.json','sha256':b.sha(mraw)}
    assert p['id']=='469' and (p['cpus'],p['memory'],p['hours'])==(4,'64G',1)
    assert p['helper']==b.HELPER and zlib.decompress(base64.b64decode(p['helper_body']))==(frozen['source']/b.HELPER).read_bytes()
    args=dict(zip(p['args'][::2],p['args'][1::2]))
    assert len(p['args'])==16 and args['--mode']=='qualify' and args['--segment']=='0'
    assert args['--policy-descriptor']==b.AREA+'/qualification-policy.json' and args['--policy-descriptor-sha256']==b.sha(praw)
    assert args['--native-plan']==b.R+'/plans/recovery-support-20261021-469/plan.json'
    assert "task['resources']['array']=None" in text and "task['primary_external_census']=" in text
    assert "'mode':'qualify'" in text and "task['external_dependencies']=['6863966']" in text
    assert evidence['resources']=={'cpus':4,'memory':'64G','hours':1,'gpu':False,'array':None}
    assert evidence['qualification_claimed'] is False and evidence['scheduler_submitted'] is False and evidence['provider_dependency_required'] is False
    assert b'\r' not in raw


def test_exact_actual_q456_and_released429_graph_are_separate_proof_bindings(frozen):
    raw,_,praw,_=render(frozen);text,_=unpack(raw);census=json.loads(praw)['census'];schedule=census['provider_schedule']
    assert census['qualification']=={'path':b.QPATH,'sha256':b.QSHA,'job_id':'6863966'}
    assert schedule['qualification']==census['qualification'] and schedule['plan']['sha256']==b.PLAN_SHA and schedule['ledger']['sha256']==b.LEDGER_SHA
    assert schedule['case_root'].endswith('rolling-storage-census-v4-20261023')
    assert "completed_native_dependency(root,'6863966') is not None" in text
    assert 'outer.qualification(' in text and b.PLAN_SHA in text and b.LEDGER_SHA in text
    assert 'task[\'external_dependencies\']=[\'6863966\']' in text and "task['dependency_kind']=payload['dependency_kind']" in text


def test_guards_admission_qualification_and_submit_have_exact_order_and_no_provider_dependency(frozen):
    raw,_,_,_=render(frozen);text,p=unpack(raw)
    body=text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];tree=ast.parse(body,feature_version=(3,11));compile(tree,'<469-fixture>','exec')
    assert text.index("['install'](area/'submission-native-accounting-reads')")<text.index('from horsehair.campaign import combined_admission')
    assert text.index('with api[\'registration_lock\']')<text.index('admission=combined_admission')<text.index('outer.qualification(')<text.index('submitted=submit(')
    assert text.count("root/'maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py'")==2
    assert not p['after'] and not p['after_specs'] and p['dependency_kind']=='afterok'
    assert 'recovery-support-20261021-375' not in body and 'failed-384-385-publication-absence' not in body


@pytest.mark.parametrize('change',['kind','extra_source','missing_source','source_bytes','test_bytes','syntax','extra_suite','missing_suite','count_bool','count_zero','roster','private_sha','baseline_bytes','census_bytes','base_fixture','template'])
def test_source_closure_or_provenance_changes_fail_before_any_output(frozen,monkeypatch,change):
    f=frozen;m=f['manifest']
    if change=='kind':m['kind']='old-coverage'
    elif change=='extra_source':m['files']['unqualified.py']='a'*64
    elif change=='missing_source':m['files'].pop(b.TEST)
    elif change=='source_bytes':(f['source']/b.HELPER).write_text('tampered')
    elif change=='test_bytes':(f['source']/b.TEST).write_text('tampered')
    elif change=='syntax':
        (f['source']/b.HELPER).write_text('def :');m['files'][b.HELPER]=b.sha((f['source']/b.HELPER).read_bytes())
    elif change=='extra_suite':m['suites']['old_qualification']={}
    elif change=='missing_suite':m['suites'].pop('baseline_budget')
    elif change=='count_bool':m['suites']['primary_policy']['tests']=True
    elif change=='count_zero':m['suites']['primary_policy']['tests']=0
    elif change=='roster':m['suites']['primary_policy']['roster_sha256']='unfrozen'
    elif change=='private_sha':m['suites']['primary_policy']['source_sha256']='0'*64
    elif change=='baseline_bytes':(f['baseline']/'tests/test_budget.py').write_text('tampered')
    elif change=='census_bytes':(f['census']/b.OUTER).write_text('tampered')
    elif change=='base_fixture':(f['census']/'run-af-agent-controller-registry-v6-20261022.py').write_text('missing native fixture')
    elif change=='template':monkeypatch.setattr(b,'TEMPLATE_SHA','0'*64)
    f['path'].write_bytes(b.json_bytes(m));before=set(f['source'].iterdir())
    with pytest.raises((ValueError,SyntaxError)):render(f)
    assert set(f['source'].iterdir())==before


@pytest.mark.parametrize('expected',[None,'UNFROZEN','0'*64])
def test_manifest_requires_the_authors_explicit_exact_frozen_digest(frozen,expected):
    with pytest.raises(ValueError):
        b.render(frozen['source'],frozen['path'],expected,baseline_directory=frozen['baseline'],census_directory=frozen['census'])


def test_render_is_pure_and_output_creation_refuses_replay(frozen,tmp_path):
    f=frozen;out=tmp_path/'output';argv=['--source-directory',str(f['source']),'--source-manifest',str(f['path']),
        '--source-manifest-sha256',b.sha(f['path'].read_bytes()),'--baseline-directory',str(f['baseline']),'--census-directory',str(f['census']),'--output-directory',str(out)]
    b.main(argv);before={p.name:p.read_bytes() for p in out.iterdir()}
    assert set(before)==set(b.OUTPUTS)
    with pytest.raises(ValueError,match='Preserve existing'):b.main(argv)
    assert before=={p.name:p.read_bytes() for p in out.iterdir()}
