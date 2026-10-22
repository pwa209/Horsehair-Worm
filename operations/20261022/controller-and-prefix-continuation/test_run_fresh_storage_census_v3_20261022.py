"""Native census wrapper contracts using tiny synthetic inventories only."""
from contextlib import contextmanager
from datetime import datetime,timezone
import ast
import importlib.util
import json
from pathlib import Path
import subprocess

import pytest

SOURCE=Path(__file__).with_name('run-fresh-storage-census-v3-20261022.py')
spec=importlib.util.spec_from_file_location('fresh_census_private',SOURCE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    root=tmp_path/'personal';(root/'state').mkdir(parents=True);case=root/'maintenance/census';case.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',case)
    mono=iter([2.,22.]);wall=iter([1000.,1020.,1021.])
    return {'root':root,'case':case,'mono':lambda:next(mono),'wall':lambda:next(wall),'now':1000.}


def synthetic_snapshot(f,mutate=None):
    def snapshot(root,**kwargs):
        assert root==f['root'] and kwargs=={'exact_sizes':True}
        quota=root/'state/quota-synthetic.txt';quota.write_bytes(b'Only synthetic quota evidence')
        entries={'schema_version':1,'status':'measured','root':str(root),'method':'owned_regular_length',
            'count':2,'bytes':15,'items':[{'relative_path':'synthetic-a','status':'measured','method':'owned_regular_length','eof_observed':True,'bytes':7},
            {'relative_path':'synthetic-b','status':'measured','method':'owned_regular_length','eof_observed':True,'bytes':8}]}
        detail=root/'state/storage-entry-sizes.json';detail.write_text(json.dumps(entries))
        record={'status':'measured','root':str(root),'measured_unix':1001.,
            'captured_utc':datetime.fromtimestamp(1001.,timezone.utc).isoformat(),'study_files':2,'study_bytes':15,
            'exact_entry_measurements':{'method':'owned_regular_length','count':2,'bytes':15,'receipt_path':str(detail),'receipt_sha256':m.digest(detail)},
            'native_source_paths':[str(quota)],'native_source_hashes':{str(quota):m.digest(quota)}}
        budget=root/'state/storage-budget.json';budget.write_text(json.dumps(record))
        if mutate:mutate(root,record,entries)
        return record
    return snapshot


def test_exact_unchanged_original_function_called_outside_lock_and_conservative_clock_preserved(fixture):
    f=fixture
    result=m.run_snapshot(synthetic_snapshot(f),f['root'],f['case'],2000.,wall=f['wall'],mono=f['mono'])
    assert result['started_unix']==1000. and result['finished_unix']==1020.
    assert result['elapsed_monotonic_seconds']==20.
    assert result['unchanged_snapshot_measured_unix']==1001.
    assert result['original_freshness_clock_semantics']=='quota_capture_before_directory_sweep'
    assert result['snapshot_is_atomic_point_measurement'] is False
    assert all(result[k] is False for k in m.FALSE_FLAGS)
    start=json.loads((f['case']/'sweep-start.json').read_text())
    assert start['registration_mutex_held'] is False and start['original_function_modified'] is False
    assert (f['case']/'original-storage-budget.json').read_bytes()==(f['root']/'state/storage-budget.json').read_bytes()
    assert (f['case']/'original-storage-entry-sizes.json').read_bytes()==(f['root']/'state/storage-entry-sizes.json').read_bytes()
    original=json.loads((f['root']/'state/storage-budget.json').read_text())
    assert original['measured_unix']==1001. and result['study_files']==2 and result['study_bytes']==15


@pytest.mark.parametrize('value', [999.,1021.,True,float('nan'),float('inf')])
def test_invalid_or_outside_original_measured_clock_cannot_be_relabelled(value):
    record={'measured_unix':value,'captured_utc':datetime.fromtimestamp(1001.,timezone.utc).isoformat()}
    with pytest.raises(ValueError):m.snapshot_clocks(record,1000.,1020.)


@pytest.mark.parametrize('captured', ['2026-10-21T00:00:00','1970-01-01T00:16:39+00:00',
    '1970-01-01T00:17:01+00:00','1970-01-01T00:16:49+00:00'])
def test_timezone_missing_outside_or_inconsistent_captured_clock_refused(captured):
    with pytest.raises(ValueError):m.snapshot_clocks({'measured_unix':1001.,'captured_utc':captured},1000.,1020.)


@pytest.mark.parametrize('kind',['returned_record','detail_hash','counts','bytes','not_exact','missing_eof','quota_changed'])
def test_partial_or_mixed_census_never_produces_completed_proof(fixture,kind):
    f=fixture
    def mutation(root,record,entries):
        detail=root/'state/storage-entry-sizes.json'
        if kind=='returned_record':record['study_bytes']=16;return
        if kind=='quota_changed':Path(record['native_source_paths'][0]).write_bytes(b'changed');return
        if kind=='detail_hash':detail.write_bytes(detail.read_bytes()+b' ');return
        if kind=='counts':entries['count']=1
        if kind=='bytes':entries['bytes']=16
        if kind=='not_exact':entries['items'][0]['method']='stat'
        if kind=='missing_eof':entries['items'][0]['eof_observed']=False
        detail.write_text(json.dumps(entries));record['exact_entry_measurements']['receipt_sha256']=m.digest(detail)
        (root/'state/storage-budget.json').write_text(json.dumps(record))
    with pytest.raises(ValueError):m.run_snapshot(synthetic_snapshot(f,mutation),f['root'],f['case'],2000.,wall=f['wall'],mono=f['mono'])
    assert (f['case']/'sweep-failure.json').exists() and not (f['case']/'sweep-result.json').exists()


def test_underlying_failure_preserved_without_retry_or_timestamp_change(fixture):
    f=fixture;calls=[]
    def broken(*args,**kwargs):calls.append((args,kwargs));raise OSError('synthetic scan failure')
    with pytest.raises(OSError):m.run_snapshot(broken,f['root'],f['case'],2000.,wall=f['wall'],mono=f['mono'])
    assert len(calls)==1
    failure=json.loads((f['case']/'sweep-failure.json').read_text())
    assert failure['status']=='failed_no_timestamp_rewrite_or_automatic_replay'
    assert all(failure[k] is False for k in m.FALSE_FLAGS)


def test_no_snapshot_starts_without_finalization_margin(fixture):
    f=fixture;calls=[]
    with pytest.raises(ValueError):m.run_snapshot(lambda *a,**k:calls.append(1),f['root'],f['case'],1200.,wall=f['wall'],mono=f['mono'])
    assert calls==[] and not (f['case']/'sweep-start.json').exists()


def test_late_finish_retains_failure_and_original_record_without_extension(fixture):
    f=fixture
    ticks=iter([1000.,1500.,1501.])
    with pytest.raises(ValueError,match='finalization'):
        m.run_snapshot(synthetic_snapshot(f),f['root'],f['case'],1600.,wall=lambda:next(ticks),mono=f['mono'])
    assert (f['root']/'state/storage-budget.json').exists()
    assert not (f['case']/'sweep-result.json').exists()


def test_brief_registration_lock_exits_before_snapshot(fixture):
    held=[];events=[]
    @contextmanager
    def lock(evidence,deadline,*,role):
        assert evidence==fixture['case']/'lock' and deadline==2000. and role=='controller-start'
        held.append(True);events.append('enter')
        try:yield
        finally:held.clear();events.append('exit')
    assert m.brief_admission(lambda:events.append('admission') or 'ok',2000.,registration_lock=lock,evidence=fixture['case']/'lock',wall=lambda:1000.)=='ok'
    assert held==[] and events==['enter','admission','exit']


def test_lock_failure_never_retries_admission(fixture):
    calls=[]
    def lock(evidence,deadline,*,role):raise TimeoutError('busy')
    with pytest.raises(TimeoutError):m.brief_admission(lambda:calls.append(1),2000.,registration_lock=lock,evidence=fixture['case']/'lock',wall=lambda:1000.)
    assert calls==[]


def native_fixture(f,monkeypatch):
    root=f['root'];folder=root/'plans/census';folder.mkdir(parents=True);script=folder/'census.sbatch';script.write_text('synthetic registered script')
    task={'name':'census','script':str(script),'script_sha256':m.canonical_hash(script.read_text()),
        'resources':{'cpus':4,'memory':'16G','hours':4,'gpu':False,'array':None}}
    plan={'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]};(folder/'plan.json').write_text(json.dumps(plan))
    ledger={'source_sha256':m.SOURCE,'plan_sha256':m.canonical_hash(plan),'tasks':{'census':{
        'job_id':'123','state':'submitted','script_sha256':task['script_sha256']}}}
    (root/'state/census-submission.json').write_text(json.dumps(ledger))
    end=datetime.fromtimestamp(20000.,timezone.utc).isoformat().removesuffix('+00:00')
    fields={'JobId':'123','UserId':'pwa209(3164768)','JobState':'RUNNING','Account':'def-ptewarie_cpu',
        'NumCPUs':'4','NumNodes':'1','TimeLimit':'04:00:00','MinMemoryNode':'16G',
        'Command':str(script),'JobName':'hhw-census','EndTime':end,'ReqTRES':'cpu=4,mem=16G,node=1',
        'AllocTRES':'cpu=4,mem=16G,node=1'}
    monkeypatch.setenv('SLURM_JOB_END_TIME','20000');monkeypatch.setattr(m.time,'time',lambda:1000.)
    def reader(job,evidence):return subprocess.CompletedProcess([],0,' '.join(k+'='+v for k,v in fields.items())+'\n','')
    return script,fields,reader,plan,folder


def test_exact_native_4cpu_16g_4h_plan_is_required(fixture,monkeypatch):
    script,fields,reader,_,_=native_fixture(fixture,monkeypatch)
    result=m.native_allocation('123',script,reader)
    assert result['allocation_end_unix']==20000. and result['fields']==fields


@pytest.mark.parametrize('field,value',[('NumCPUs','1'),('NumNodes','2'),('TimeLimit','02:00:00'),
    ('MinMemoryNode','8G'),('Account','foreign'),('UserId','other(1)'),('JobState','PENDING'),
    ('JobId','124'),('ReqTRES','cpu=4,gres/gpu=1'),('AllocTRES','cpu=4,gres/gpu:a100=1')])
def test_foreign_or_different_native_resources_refused(fixture,monkeypatch,field,value):
    script,fields,reader,_,_=native_fixture(fixture,monkeypatch);fields[field]=value
    with pytest.raises(ValueError):m.native_allocation('123',script,reader)


def test_qualification_contracts_remain_exact_reviewed_4f_and100():
    original=Path(__file__).with_name('run-vtl-track-schema-audit-v2-20261021.py')
    old=ast.parse(original.read_text());new=ast.parse(SOURCE.read_text())
    for name in ('qualification_proof','registry_qualification','completed_registry_qualification'):
        a=next(n for n in old.body if isinstance(n,ast.FunctionDef) and n.name==name)
        b=next(n for n in new.body if isinstance(n,ast.FunctionDef) and n.name==name)
        assert ast.dump(a,include_attributes=False)==ast.dump(b,include_attributes=False)


def test_main_has_original_function_call_and_no_clock_or_function_monkeypatch():
    tree=ast.parse(SOURCE.read_text());text=SOURCE.read_text()
    assert 'run_snapshot(budget.storage_snapshot,ROOT,SUPPORT,end)' in text
    assert not any(isinstance(n,ast.Attribute) and isinstance(n.ctx,ast.Store)
        and n.attr in {'storage_snapshot','time','datetime','_measure_personal_quota'} for n in ast.walk(tree))


@pytest.mark.parametrize('end',[float('nan'),float('inf'),True,1200.])
def test_invalid_or_insufficient_native_margin_stops_before_lock_and_callback(end):
    def forbidden(*a,**k):pytest.fail('No lock or operation after invalid native margin')
    with pytest.raises(ValueError):
        m.brief_admission(forbidden,end,registration_lock=forbidden,evidence=Path('/unused'),wall=lambda:1000.)


def test_baseline_callback_failure_is_never_replayed_and_lock_released(fixture):
    calls=[];held=[]
    @contextmanager
    def lock(evidence,deadline,*,role):
        assert role=='controller-start' and deadline==2000.;held.append(True)
        try:yield
        finally:held.clear()
    def callback():calls.append(1);raise RuntimeError('Original admission failed')
    with pytest.raises(RuntimeError,match='Original admission failed'):
        m.brief_admission(callback,2000.,registration_lock=lock,evidence=fixture['case']/'lock',wall=lambda:1000.)
    assert calls==[1] and held==[]


def test_current100_source_guard_constants_are_exact_and_no_legacy84_path():
    assert m.REGISTRY_SUPPORT==m.ROOT/'maintenance/study-af-registry-repair-support-v4-20261021'
    assert m.REGISTRY_GUARD.name=='registered-snapshot-read-guard-v3-20261021.py'
    assert m.REGISTRY_GUARD_SHA=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'
    assert m.REGISTRY_Q_SHA=='1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca'
    assert m.REGISTRY_Q_JOB=='6699561'
    assert m.REGISTRY_Q_HELPER_SHA=='7fd2348c3370811cb5ca2933357a007447b18c2ea57be1cef502fdd035fdda51'
    assert m.REGISTRY_CONTROLLER_SHA=='475a170aab2d158cfd17adb250707e1d0767086d4c50b95516c360f2222560f8'
    assert '42fd488d' not in SOURCE.read_text() and 'private84' not in SOURCE.read_text()


@pytest.mark.parametrize('change',['sha','job'])
def test_old_or_unbound_registry_identity_refused_before_read(change,monkeypatch):
    def forbidden(*a,**k):pytest.fail('No unbound registry source read')
    monkeypatch.setattr(m,'digest',forbidden)
    sha=m.REGISTRY_Q_SHA if change!='sha' else 'a'*64
    job=m.REGISTRY_Q_JOB if change!='job' else '6696233'
    with pytest.raises(ValueError):m.registry_qualification(m.REGISTRY_Q,sha,job)


def test_guard_order_mode_and_sweep_outside_registration_callback_are_explicit():
    text=SOURCE.read_text();tree=ast.parse(text);main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
    body=ast.get_source_segment(text,main)
    assert body.index("runpy.run_path(str(GUARD))['install']")<body.index("registry_api=runpy.run_path(str(REGISTRY_GUARD))")<body.index('from horsehair import campaign,repair,budget')
    assert "registration_mode='controller-already-locked'" in body
    assert "registration_lock=registry_api['registration_lock']" in body
    brief=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='brief_admission')
    calls=[n for n in ast.walk(brief) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='callback']
    assert len(calls)==1 and "role='controller-start'" in ast.get_source_segment(text,brief)
    assert not any(isinstance(n,(ast.For,ast.While)) for n in ast.walk(brief))
    assert 'storage_snapshot' not in ast.get_source_segment(text,brief)
    assert body.index('admission=brief_admission')<body.index('result=run_snapshot(budget.storage_snapshot')
    assert '_storage(' not in body and '21600' not in body and 'freshness' not in body
