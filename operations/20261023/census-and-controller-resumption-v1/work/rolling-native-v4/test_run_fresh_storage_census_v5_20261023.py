"""Native census wrapper contracts using tiny synthetic inventories only."""
from contextlib import contextmanager
from datetime import datetime,timezone
import ast
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import hashlib
import shlex
import stat
import time
from types import SimpleNamespace

import pytest

SOURCE=Path(__file__).with_name('run-fresh-storage-census-v5-20261023.py')
spec=importlib.util.spec_from_file_location('fresh_census_private',SOURCE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    root=tmp_path/'personal';(root/'state').mkdir(parents=True);case=root/'maintenance/census';case.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',case)
    mono=iter([2.,22.]);wall=iter([1000.,1020.,1021.])
    return {'root':root,'case':case,'mono':lambda:next(mono),'wall':lambda:next(wall),'now':1000.}


def measured_item(path,size):
    sha=hashlib.sha256(b'x'*size).hexdigest()
    return {'schema_version':1,'path':str(path),'status':'measured','method':'owned_regular_length','bytes':size,
        'max_bytes':6_000_000_000_000,'prefix_cap_bytes':65536,'prefix_bytes':size,'prefix_sha256':sha,
        'content_sha256':sha,'eof_observed':True,
        'identity':{'inode':1,'uid':42,'mode':stat.S_IFREG|0o600,'nlink':1,'device_major':1,'device_minor':2,'ctime_ns':3},
        'size_proof':{'kind':'prefix_eof','probes':([{'offset':size-1,'bytes':1}] if size else [])+[{'offset':size,'bytes':0}]}}


def synthetic_snapshot(f,mutate=None):
    def snapshot(root,**kwargs):
        assert root==f['root'] and kwargs=={'exact_sizes':True}
        quota=root/'state/quota-synthetic.txt';quota.write_bytes(b'Only synthetic quota evidence')
        entries={'schema_version':1,'status':'measured','root':str(root),'method':'owned_regular_length',
            'count':2,'bytes':15,'items':[{'relative_path':'synthetic-a','status':'measured','method':'owned_regular_length','eof_observed':True,'bytes':7},
            {'relative_path':'synthetic-b','status':'measured','method':'owned_regular_length','eof_observed':True,'bytes':8}]}
        for item in entries['items']:item.update(measured_item(root/item['relative_path'],item['bytes']))
        detail=root/'state/storage-entry-sizes.json';detail.write_text(json.dumps(entries))
        record={'status':'measured','root':str(root),'measured_unix':1001.,
            'captured_utc':datetime.fromtimestamp(1001.,timezone.utc).isoformat(),'personal_uid':42,'study_files':2,'study_bytes':15,
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
    assert start['registration_mutex_held'] is False and start['budget_function_body_unchanged'] is True
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
    ticks=iter([1000.,1800.,1801.])
    with pytest.raises(ValueError,match='finalization'):
        m.run_snapshot(synthetic_snapshot(f),f['root'],f['case'],2500.,wall=lambda:next(ticks),mono=f['mono'])
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
    root=f['root'];folder=root/'plans/census';folder.mkdir(parents=True);script=folder/'census.sbatch';command=shlex.join(['python','-s',str(SOURCE),'--synthetic','x']);script.write_text(command)
    task={'name':'census','command':command,'script':str(script),'script_sha256':m.canonical_hash(script.read_text()),
        'resources':{'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None}}
    plan={'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]};(folder/'plan.json').write_text(json.dumps(plan))
    ledger={'source_sha256':m.SOURCE,'plan_sha256':m.canonical_hash(plan),'tasks':{'census':{
        'job_id':'123','state':'submitted','script_sha256':task['script_sha256']}}}
    (root/'state/census-submission.json').write_text(json.dumps(ledger))
    end=datetime.fromtimestamp(20000.,timezone.utc).isoformat().removesuffix('+00:00')
    fields={'JobId':'123','UserId':'pwa209(3164768)','JobState':'RUNNING','Account':'def-ptewarie_cpu',
        'NumCPUs':'8','NumNodes':'1','TimeLimit':'06:00:00','MinMemoryNode':'64G',
        'Command':str(script),'JobName':'hhw-census','EndTime':end,'ReqTRES':'cpu=8,mem=64G,node=1',
        'AllocTRES':'cpu=8,mem=64G,node=1'}
    monkeypatch.setenv('SLURM_JOB_END_TIME','20000');monkeypatch.setattr(m.time,'time',lambda:1000.)
    def reader(job,evidence):return subprocess.CompletedProcess([],0,' '.join(k+'='+v for k,v in fields.items())+'\n','')
    return script,fields,reader,plan,folder


def test_exact_native_8cpu_64g_6h_plan_is_required(fixture,monkeypatch):
    script,fields,reader,_,_=native_fixture(fixture,monkeypatch)
    result=m.native_allocation('123',script,reader,plan_path=script.parent/'plan.json',ledger_path=fixture['root']/'state/census-submission.json',task_name='census',argv=['--synthetic','x'])
    assert result['allocation_end_unix']==20000. and result['fields']==fields


@pytest.mark.parametrize('field,value',[('NumCPUs','1'),('NumNodes','2'),('TimeLimit','02:00:00'),
    ('MinMemoryNode','8G'),('Account','foreign'),('UserId','other(1)'),('JobState','PENDING'),
    ('JobId','124'),('ReqTRES','cpu=4,gres/gpu=1'),('AllocTRES','cpu=4,gres/gpu:a100=1')])
def test_foreign_or_different_native_resources_refused(fixture,monkeypatch,field,value):
    script,fields,reader,_,_=native_fixture(fixture,monkeypatch);fields[field]=value
    with pytest.raises(ValueError):m.native_allocation('123',script,reader,plan_path=script.parent/'plan.json',ledger_path=fixture['root']/'state/census-submission.json',task_name='census',argv=['--synthetic','x'])


def test_qualification_contracts_remain_exact_reviewed_4f_and100():
    assert m.QUALIFICATION_SHA=='29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f'
    assert m.REGISTRY_Q_SHA=='1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca'


def test_main_retains_budget_function_and_only_scoped_iterator_assignment():
    tree=ast.parse(SOURCE.read_text());text=SOURCE.read_text()
    assert 'run_snapshot(budget.storage_snapshot,ROOT,SUPPORT,end,adapter=adapter)' in text
    assert not any(isinstance(n,ast.Attribute) and isinstance(n.ctx,ast.Store)
        and n.attr in {'storage_snapshot','time','datetime','_measure_personal_quota'} for n in ast.walk(tree))
    assigns=[n for n in ast.walk(tree) if isinstance(n,ast.Attribute) and isinstance(n.ctx,ast.Store) and n.attr=='iter_regular_lengths']
    assert len(assigns)==2


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
    assert body.index('registry=registry_qualification(')<body.index('extension=registry_read_extension()')<body.index('registry_api=runpy.run_path(str(EXTENDED_REGISTRY_GUARD))')
    assert body.index("runpy.run_path(str(GUARD))['install']")<body.index("registry_api=runpy.run_path(str(EXTENDED_REGISTRY_GUARD))")<body.index('from horsehair import campaign,repair,budget')
    assert "registration_mode='controller-already-locked'" in body
    assert "registration_lock=registry_api['registration_lock']" in body
    brief=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='brief_admission')
    calls=[n for n in ast.walk(brief) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='callback']
    assert len(calls)==1 and "role='controller-start'" in ast.get_source_segment(text,brief)
    assert not any(isinstance(n,(ast.For,ast.While)) for n in ast.walk(brief))
    assert 'storage_snapshot' not in ast.get_source_segment(text,brief)
    assert body.index('admission=brief_admission')<body.index('result=run_snapshot(budget.storage_snapshot')
    assert '_storage(' not in body and 'storage_snapshot=' not in body
    assert body.index('admission=brief_admission')<body.index('streaming_q=kernel_qualification')<body.index('kernel=runpy.run_path')
    assert body.index('freshness_before_quota=')<body.index('quota_after=')<body.index('freshness_after_quota=')<body.index('freshness_at_receipt=')


def python311_ast_dump(tree):
    """Strict 3.11 representation; only its absent empty type_params is omitted."""
    tree=copy.deepcopy(tree)
    for node in ast.walk(tree):
        if hasattr(node,'type_params'):
            if not isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)) or type(node.type_params) is not list or node.type_params:
                raise ValueError('Nonempty or unexpected type parameters are not Python3.11 source')
            del node.type_params
    return ast.dump(tree,include_attributes=False)


def test_legacy_positive_proofs_and_native_guard_sources_are_preserved():
    assert hashlib.sha256(SOURCE.read_bytes()).hexdigest()=='b448607b00512ef865a3369da56c18bf6f98ab589b6a524795e5bbed9c64e569'
    expected={'qualification_proof': '68bd48e464f7b9a85691f936cf5458bef1d153bb29452f926c7adc2c077f5d36', 'registry_qualification': '20d66af05da0652a5b93126044934640ba5d9037a46a1e65ae3e6b34c79317eb', 'completed_registry_qualification': '0ea782feed8d7c1e1e80679a11489b48ce03f01f9a0c8af1c58e3963b630bc55', 'brief_admission': 'd21d78dd7f743a984689ed077ead366487a7f33ef03717c603c59c0e9658e898', 'snapshot_clocks': '7c2e9ab7af427fd49fd0e45507e2840923c02aacafcf70d7ca2911dc1c9e34e2'}
    new=ast.parse(SOURCE.read_text(),feature_version=(3,11))
    for name,sha in expected.items():
        function=next(n for n in new.body if isinstance(n,ast.FunctionDef) and n.name==name)
        assert hashlib.sha256(python311_ast_dump(function).encode()).hexdigest()==sha
    assert m.SUPPORT.name=='study-af-fresh-storage-census-v5-20261023'
    assert m.GUARD_SHA=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
    assert m.EXTENDED_REGISTRY_SHA=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'


@pytest.mark.parametrize('age',[-1,21600,21601,float('nan'),float('inf')])
def test_original_quota_clock_refuses_future_expired_or_nonfinite_age(age):
    result={'unchanged_snapshot_measured_unix':1000.,'unchanged_snapshot_captured_utc':datetime.fromtimestamp(1000.,timezone.utc).isoformat()}
    with pytest.raises(ValueError):m.fresh_original_clocks(result,1000.+age)


def test_after_quota_wait_cannot_refresh_original_clock(fixture):
    f=fixture;result=m.run_snapshot(synthetic_snapshot(f),f['root'],f['case'],2000.,wall=f['wall'],mono=f['mono'])
    assert m.final_snapshot_check(result,f['root'],f['case'],wall=lambda:22600.)['original_measured_age_seconds']==21599.
    with pytest.raises(ValueError,match='six hours'):
        m.final_snapshot_check(result,f['root'],f['case'],wall=lambda:22601.)
    assert result['unchanged_snapshot_measured_unix']==1001.


@pytest.mark.parametrize('target',['storage-budget.json','storage-entry-sizes.json'])
def test_final_snapshot_pair_cannot_be_changed_by_another_publication(fixture,target):
    f=fixture;result=m.run_snapshot(synthetic_snapshot(f),f['root'],f['case'],2000.,wall=f['wall'],mono=f['mono'])
    (f['root']/'state'/target).write_bytes(b'other publication')
    with pytest.raises(ValueError,match='pair changed'):m.final_snapshot_check(result,f['root'],f['case'],wall=lambda:1022.)


@pytest.mark.parametrize('field,value',[('size_proof',{'kind':'prefix_eof','probes':[]}),('identity',{}),
    ('path','/foreign'),('relative_path','../foreign'),('prefix_cap_bytes',1)])
def test_resealed_incomplete_entry_proof_refused(fixture,field,value):
    f=fixture
    def mutate(root,record,entries):
        entries['items'][0][field]=value
        path=root/'state/storage-entry-sizes.json';path.write_text(json.dumps(entries))
        record['exact_entry_measurements']['receipt_sha256']=m.digest(path)
        (root/'state/storage-budget.json').write_text(json.dumps(record))
    with pytest.raises(ValueError):m.run_snapshot(synthetic_snapshot(f,mutate),f['root'],f['case'],2000.,wall=f['wall'],mono=f['mono'])
    assert not (f['case']/'sweep-result.json').exists()


def adapter_kernel(artifacts,calls):
    def iterator(root,max_bytes,expected_uid=None,**kwargs):
        calls.append((root,max_bytes,expected_uid,kwargs));yield 'synthetic-path',{'status':'synthetic'}
    return {'ARTIFACTS':artifacts,'MAX_WORKERS':64,'MAX_PENDING':128,'iter_regular_lengths':iterator}


def test_scoped_adapter_changes_only_iterator_and_always_restores():
    from horsehair import budget,storage_artifacts as artifacts
    calls=[];original=artifacts.iter_regular_lengths;snapshot=budget.storage_snapshot;before=m.primitive_identity()
    progress=lambda row:None
    with m.operational_adapter(adapter_kernel(artifacts,calls),deadline=1234.,progress=progress) as proof:
        assert artifacts.iter_regular_lengths is not original and budget.storage_snapshot is snapshot
        assert list(artifacts.iter_regular_lengths('/synthetic',10,42,workers=16))==[('synthetic-path',{'status':'synthetic'})]
        assert proof['restored'] is False and proof['other_measurements_may_remain_active_across_yield'] is True
    assert artifacts.iter_regular_lengths is original and before==m.primitive_identity()
    assert proof['before']==proof['after'] and proof['restored'] is True
    kwargs=calls[0][3];assert kwargs['workers']==64 and kwargs['pending_limit']==128 and kwargs['deadline']==1234. and kwargs['progress'] is progress


def test_scoped_adapter_restores_after_consumer_exception():
    from horsehair import storage_artifacts as artifacts
    original=artifacts.iter_regular_lengths
    with pytest.raises(RuntimeError,match='consumer'):
        with m.operational_adapter(adapter_kernel(artifacts,[]),deadline=1234.) as proof:raise RuntimeError('consumer')
    assert artifacts.iter_regular_lengths is original and proof['restored'] is True


def test_scoped_adapter_detects_primitive_replacement_even_after_iterator_restore(monkeypatch):
    from horsehair import storage_artifacts as artifacts
    original=artifacts.iter_regular_lengths
    with pytest.raises(ValueError,match='identity/source'):
        with m.operational_adapter(adapter_kernel(artifacts,[]),deadline=1234.):
            monkeypatch.setattr(artifacts,'_metadata',m.require)
    assert artifacts.iter_regular_lengths is original


@pytest.mark.parametrize('key,value',[('ARTIFACTS',None),('MAX_WORKERS',16),('MAX_PENDING',256),('iter_regular_lengths',None)])
def test_unbound_kernel_refused_before_iterator_changes(key,value):
    from horsehair import storage_artifacts as artifacts
    original=artifacts.iter_regular_lengths;kernel=adapter_kernel(artifacts,[]);kernel[key]=value
    with pytest.raises(ValueError):
        with m.operational_adapter(kernel,deadline=1234.):pytest.fail('Unbound adapter entered')
    assert artifacts.iter_regular_lengths is original


def test_original_budget_consumer_uses_scoped_iterator_and_unchanged_quota_clock(fixture,monkeypatch):
    from horsehair import budget,storage_artifacts as artifacts
    f=fixture;root=f['root'];original=artifacts.iter_regular_lengths;snapshot=budget.storage_snapshot
    monkeypatch.setattr(budget,'PERSONAL_STUDY_ROOT',root)
    monkeypatch.setattr(budget,'_measure_personal_quota',lambda runner:{'native_stdout':'synthetic quota only',
        'measured_unix':1001.,'captured_utc':datetime.fromtimestamp(1001.,timezone.utc).isoformat(),'personal_uid':42})
    def iterator(root,max_bytes,expected_uid=None,**kwargs):
        assert kwargs['workers']==64 and kwargs['pending_limit']==128
        yield root/'z',measured_item(root/'z',8)
        yield root/'a',measured_item(root/'a',7)
    kernel=adapter_kernel(artifacts,[]);kernel['iter_regular_lengths']=iterator
    with m.operational_adapter(kernel,deadline=time.monotonic()+100.) as proof:
        value=snapshot(root,exact_sizes=True)
    assert artifacts.iter_regular_lengths is original and budget.storage_snapshot is snapshot
    assert proof['restored'] is True and value['study_bytes']==15 and value['study_files']==2 and value['measured_unix']==1001.
    items=json.loads((root/'state/storage-entry-sizes.json').read_text())['items']
    assert [item['relative_path'] for item in items]==['a','z']


@pytest.mark.parametrize('text',['cpu=8,mem=64G,node=1,gres/gpu=0','cpu=4,mem=64G,node=1',
    'cpu=8,mem=16G,node=1','cpu=8,mem=64G,node=2','cpu=8,mem=64G,node=1,cpu=8','cpu=8,node=1',''])
def test_missing_or_wrong_actual_tres_refused(text):
    with pytest.raises(ValueError):m.cpu_only_tres(text)


@pytest.mark.parametrize('text',['cpu=8,mem=64G,node=1','cpu=8,mem=65536M,node=1','cpu=8,mem=67108864K,node=1'])
def test_native_equivalent_memory_units_preserve_exact_64g_requirement(text):
    assert m.cpu_only_tres(text)['cpu']=='8'


@pytest.fixture
def qualified_bundle(fixture,monkeypatch):
    f=fixture;root=f['root'];support=root/'maintenance/streaming-source';support.mkdir()
    framework=root/'framework';framework.mkdir();qpath=root/'maintenance/streaming-q/qualification.json';qpath.parent.mkdir()
    monkeypatch.setattr(m,'SOURCE_SUPPORT',support);monkeypatch.setattr(m,'FRAMEWORK',framework);monkeypatch.setattr(m,'KERNEL_Q',qpath)
    for name in m.PRIVATE_FILES:(support/name).write_text('synthetic private source '+name)
    for name in [*['tests/test_'+name+'.py' for name in m.BASELINE_NAMES],
        'tests/conftest.py','src/horsehair/budget.py','src/horsehair/storage_artifacts.py']:
        path=framework/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('synthetic public source '+name)
    manifest={'kind':'streaming-census-operational-source-manifest-v1',
        'files':{name:m.digest(support/name) for name in m.PRIVATE_FILES},'suites':{}}
    suites=[]
    for scope,name in m.SUITE_SOURCES.items():
        source=framework/name if scope.startswith('baseline_') else support/name
        roster=m.canonical_hash(['test_'+scope+'::positive'])
        manifest['suites'][scope]={'source':name,'source_sha256':m.digest(source),'tests':1,'roster_sha256':roster}
        junit=qpath.parent/(scope+'.xml')
        junit.write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"><testcase classname="work.test_'+scope+'" name="positive"/></testsuite></testsuites>')
        suites.append({'scope':scope,'source':{'path':str(source),'sha256':m.digest(source)},
            'junit':{'path':str(junit),'sha256':m.digest(junit)},'test_counts':{'tests':1,'failures':0,'errors':0,'skipped':0},'test_roster_sha256':roster})
    manifest_path=support/'source-manifest.json';manifest_path.write_text(json.dumps(manifest));manifest_sha=m.digest(manifest_path)
    _,pins=m.source_manifest(manifest_path,manifest_sha)
    plan_dir=root/'plans/kernel-q';plan_dir.mkdir(parents=True);plan_path=plan_dir/'plan.json';script=plan_dir/'q.sbatch';ledger_path=root/'state/kernel-q-submission.json'
    args=['--native-plan',str(plan_path),'--native-ledger',str(ledger_path),'--native-task','kernel-q','--native-script',str(script),
        '--source-manifest',str(manifest_path),'--source-manifest-sha256',manifest_sha]
    command=shlex.join(['python','-s',str(support/m.QUALIFIER_NAME),*args]);script.write_text(command)
    task={'name':'kernel-q','command':command,'script':str(script),'script_sha256':m.canonical_hash(script.read_text()),
        'resources':{'cpus':8,'memory':'64G','hours':1,'gpu':False,'array':None},
        'streaming_census_qualification':{'helper':{'path':str(support/m.QUALIFIER_NAME),'sha256':pins[str(support/m.QUALIFIER_NAME)]},
            'source_manifest':{'path':str(manifest_path),'sha256':manifest_sha}}}
    plan={'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]};plan_path.write_text(json.dumps(plan))
    ledger={'source_sha256':m.SOURCE,'plan_sha256':m.canonical_hash(plan),'tasks':{'kernel-q':{'job_id':'456','state':'submitted','script_sha256':task['script_sha256']}}}
    ledger_path.write_text(json.dumps(ledger))
    registration,_=m.registered_task(plan_path,ledger_path,'kernel-q',script,'456',hours=1,helper=support/m.QUALIFIER_NAME)
    q={'status':'complete_streaming_census_operational_qualification','returncode':0,'slurm_job_id':'456',
        'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,'source_verified_unchanged':True,'source_pins':pins,
        'source_manifest':{'path':str(manifest_path),'sha256':manifest_sha},
        'qualification_helper':{'path':str(support/m.QUALIFIER_NAME),'sha256':pins[str(support/m.QUALIFIER_NAME)]},
        'suites':suites,'baseline_iterator_policy':'original_canonical_iterator','private_iterator_policy':'scoped_private_kernel_only',
        'primitive_identity':m.primitive_contract(m.primitive_identity()),'native_registration':registration,
        'native_completion_required_separately':True,**{k:False for k in m.FALSE_FLAGS}}
    identity=m.primitive_identity()
    canary={'status':'complete_native_local_syscall_canary','owner_uid':3164768,'workers':64,'pending_limit':128,
        **{key:True for key in ('serial_equals_streaming','symlink_not_followed','special_not_opened','original_iterator_restored',
            'original_measurement_functions_preserved','fixture_quota_simulated')},'fixture_quota_is_admission_authority':False,
        'primitive_identity':{'before':identity,'after':identity,'restored':True},'primitive_contract':m.primitive_contract(identity),
        **{k:False for k in m.FALSE_FLAGS}}
    canary_path=qpath.parent/'native-canary.json';canary_path.write_text(json.dumps(canary))
    q['native_canary']={'path':str(canary_path),'sha256':m.digest(canary_path)}
    observed={'456':{'state':'COMPLETED','exit_code':'0:0','user':'pwa209','account':'def-ptewarie_cpu','job_id_raw':'456',
        'unallocated_metadata':False,'cpus':8,'elapsed_seconds':1,'alloc_tres':'cpu=8,mem=64G,node=1'}}
    monkeypatch.setattr(m,'completed_native_dependency',lambda root,job:{'actual_positive_persisted_native':job})
    f.update(support=support,framework=framework,qpath=qpath,manifest_path=manifest_path,manifest_sha=manifest_sha,manifest=manifest,q=q,
        observed=observed,script=script,plan_path=plan_path,ledger_path=ledger_path,plan=plan,ledger=ledger,canary=canary,canary_path=canary_path)
    def verify():
        qpath.write_text(json.dumps(q));return m.kernel_qualification(qpath,m.digest(qpath),'456',manifest_path,manifest_sha,observed)
    f['verify']=verify;return f


def test_full_separately_completed_source_bound_streaming_qualification(qualified_bundle):
    f=qualified_bundle;result=f['verify']()
    assert result['job_id']=='456' and result['completion']=={'actual_positive_persisted_native':'456'}
    assert len(result['source_pins'])==17


@pytest.mark.parametrize('change',['status','returncode','source','config','scientific','policy','primitive','suites_missing','suite_duplicate',
    'suite_roster','suite_count','suite_source','helper','manifest','pins'])
def test_resealed_wrong_qualification_contract_refused(qualified_bundle,change):
    f=qualified_bundle;q=f['q']
    if change=='status':q['status']='pending'
    if change=='returncode':q['returncode']=False
    if change=='source':q['source_sha256']='0'*64
    if change=='config':q['config_sha256']='0'*64
    if change=='scientific':q['scientific_gates_cleared']=True
    if change=='policy':q['baseline_iterator_policy']='private_kernel'
    if change=='primitive':q['primitive_identity']['functions']={}
    if change=='suites_missing':q['suites'].pop()
    if change=='suite_duplicate':q['suites'][0]=q['suites'][1]
    if change=='suite_roster':q['suites'][0]['test_roster_sha256']='0'*64
    if change=='suite_count':q['suites'][0]['test_counts']['tests']=True
    if change=='suite_source':q['suites'][0]['source']['sha256']='0'*64
    if change=='helper':q['qualification_helper']['path']=str(f['support']/'foreign.py')
    if change=='manifest':q['source_manifest']['sha256']='0'*64
    if change=='pins':q['source_pins'].pop(next(iter(q['source_pins'])))
    with pytest.raises(ValueError):f['verify']()


@pytest.mark.parametrize('field,value',[('state','FAILED'),('state','RUNNING'),('exit_code','1:0'),('user','foreign'),
    ('account','foreign'),('job_id_raw','457'),('unallocated_metadata',True),('cpus',4),('cpus',True),('elapsed_seconds',0),
    ('elapsed_seconds',3601),('elapsed_seconds',True),('alloc_tres','cpu=8,mem=64G,node=1,gres/gpu=1')])
def test_wrong_native_qualifier_cannot_authorize_private_kernel(qualified_bundle,field,value):
    f=qualified_bundle;f['observed']['456'][field]=value
    with pytest.raises(ValueError):f['verify']()


def test_missing_final_persisted_qualifier_proof_refused(qualified_bundle,monkeypatch):
    monkeypatch.setattr(m,'completed_native_dependency',lambda *a:None)
    with pytest.raises(ValueError,match='pending'):qualified_bundle['verify']()


@pytest.mark.parametrize('change',['junit_tamper','junit_resealed_skip','wrong_roster_resealed','source_tamper','unbound_file','unbound_suite'])
def test_actual_source_and_junit_are_checked_after_resealing(qualified_bundle,change):
    f=qualified_bundle;q=f['q'];suite=q['suites'][0];junit=Path(suite['junit']['path'])
    if change=='junit_tamper':junit.write_text(junit.read_text()+' ')
    if change=='junit_resealed_skip':
        junit.write_text(junit.read_text().replace('/>','><skipped/></testcase>'));suite['junit']['sha256']=m.digest(junit)
    if change=='wrong_roster_resealed':
        junit.write_text(junit.read_text().replace('name="positive"','name="unqualified"'));suite['junit']['sha256']=m.digest(junit)
    if change=='source_tamper':(f['support']/m.KERNEL_NAME).write_bytes(b'changed kernel')
    if change in {'unbound_file','unbound_suite'}:
        value=f['manifest']
        if change=='unbound_file':value['files'].pop(m.KERNEL_NAME)
        else:value['suites'].pop('kernel')
        f['manifest_path'].write_text(json.dumps(value))
        with pytest.raises(ValueError):m.source_manifest(f['manifest_path'],m.digest(f['manifest_path']))
        return
    with pytest.raises(ValueError):f['verify']()


def test_qualifier_registered_arguments_cannot_point_to_other_native_task(qualified_bundle):
    f=qualified_bundle;task=f['plan']['tasks'][0]
    task['command']=task['command'].replace('--native-task kernel-q','--native-task foreign')
    f['script'].write_text(task['command']);task['script_sha256']=m.canonical_hash(f['script'].read_text())
    f['plan_path'].write_text(json.dumps(f['plan']));f['ledger']['plan_sha256']=m.canonical_hash(f['plan'])
    f['ledger']['tasks']['kernel-q']['script_sha256']=task['script_sha256'];f['ledger_path'].write_text(json.dumps(f['ledger']))
    registration,_=m.registered_task(f['plan_path'],f['ledger_path'],'kernel-q',f['script'],'456',hours=1,helper=f['support']/m.QUALIFIER_NAME)
    f['q']['native_registration']=registration
    with pytest.raises(ValueError,match='every exact actual'):f['verify']()


def test_main_qualification_precedes_private_kernel_and_leaves_final_native_margin():
    tree=ast.parse(SOURCE.read_text());main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
    text=ast.get_source_segment(SOURCE.read_text(),main)
    assert text.index('streaming_q=kernel_qualification')<text.index('kernel=runpy.run_path')<text.index('with operational_adapter(')
    assert 'end-time.time()-FINALIZATION_SECONDS' in text and m.FINALIZATION_SECONDS==900
    assert 'signal.alarm(max(1,int(end-time.time()-180)))' in text
    assert 'resource_lease(ROOT,config,cpus=8,gpus=0,max_hours=6' in text
    assert not any(word in text for word in ('sbatch','scancel','unlink(','.rmtree('))


@pytest.mark.parametrize('change',['restored','owner','serial','authority','identity','bytes'])
def test_actual_native_canary_is_required_even_when_qualification_resealed(qualified_bundle,change):
    f=qualified_bundle;value=f['canary']
    if change=='restored':value['original_iterator_restored']=False
    if change=='owner':value['owner_uid']=1
    if change=='serial':value['serial_equals_streaming']=False
    if change=='authority':value['fixture_quota_is_admission_authority']=True
    if change=='identity':value['primitive_contract']['functions']={}
    if change=='bytes':f['canary_path'].write_bytes(b'changed')
    else:
        f['canary_path'].write_text(json.dumps(value));f['q']['native_canary']['sha256']=m.digest(f['canary_path'])
    with pytest.raises(ValueError):f['verify']()


def test_progress_metadata_is_bounded_and_never_authorizes_partial_inventory(tmp_path):
    path=tmp_path/'progress.jsonl'
    with path.open('xb') as stream:
        progress,latest=m.progress_recorder(stream)
        for count in range(1,2501):progress({'entries':count,'partial_census_authority':False,'pending_limit':128})
    rows=[json.loads(line) for line in path.read_text().splitlines()]
    assert [row['entries'] for row in rows]==[1,1001,2001] and latest['entries']==2500
    assert all(row['partial_census_authority'] is False for row in rows)


def test_live_progress_path_is_fresh_owned_and_outside_measured_root(fixture,monkeypatch):
    f=fixture;temporary=f['root'].parent/'allocation';temporary.mkdir();monkeypatch.setenv('SLURM_TMPDIR',str(temporary))
    original=Path.stat
    def owned(path,*args,**kwargs):
        record=original(path,*args,**kwargs)
        if path==temporary:
            fields=list(record);fields[4]=3164768;return m.os.stat_result(fields)
        return record
    monkeypatch.setattr(Path,'stat',owned)
    path=m.progress_path('123');assert path.parent==temporary and not path.is_relative_to(f['root'])
    path.write_bytes(b'prior evidence')
    with pytest.raises(ValueError,match='Fresh'):m.progress_path('123')
    monkeypatch.setenv('SLURM_TMPDIR',str(f['root']))
    with pytest.raises(ValueError,match='outside census'):m.progress_path('123')
