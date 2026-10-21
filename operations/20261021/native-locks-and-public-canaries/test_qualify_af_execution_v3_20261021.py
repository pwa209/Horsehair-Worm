"""Private native qualification contracts; no network or scheduler operations."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import copy
import importlib.util
import time
from datetime import datetime, timezone

import pytest

from horsehair import agents
from horsehair.core import atomic_json, canonical_hash, digest, read_json

spec = importlib.util.spec_from_file_location('af_qualification_v3', Path(__file__).with_name('qualify-af-execution-v3-20261021.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def policy():
    return SimpleNamespace(execution_source_sha256=m.FRAMEWORK_SHA, base_config_sha256=m.CONFIG_SHA,
        primary_scientific_source_sha256=m.PRIMARY_SHA,
        receipt=lambda: {'path':str(m.POLICY),'sha256':'a'*64,'execution_source_sha256':m.FRAMEWORK_SHA})


def test_explicit_new_policy_path_sha_and_source_are_passed_to_public_loader(tmp_path, monkeypatch):
    path = tmp_path/'policy.json'; path.write_text('{}'); monkeypatch.setattr(m,'POLICY',path)
    expected=policy();calls=[]
    def load(path,sha,**kwargs):calls.append((path,sha,kwargs));return expected
    monkeypatch.setattr(m,'load_resource_policy',load)
    assert m.approved_policy(path,'a'*64,{'frozen':'config'}) is expected
    assert calls == [(path,'a'*64,{'root':m.ROOT,'config':{'frozen':'config'}})]


@pytest.mark.parametrize('change',['path','hash','execution_source','config_source','primary_source'])
def test_old_or_changed_policy_is_refused(tmp_path,monkeypatch,change):
    path=tmp_path/'policy.json';path.write_text('{}');monkeypatch.setattr(m,'POLICY',path)
    value=policy();given=path;sha='a'*64
    if change=='path':given=tmp_path/'v1-policy.json'
    elif change=='hash':sha='not-a-checksum'
    elif change=='execution_source':value.execution_source_sha256='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
    elif change=='config_source':value.base_config_sha256='b'*64
    else:value.primary_scientific_source_sha256='b'*64
    monkeypatch.setattr(m,'load_resource_policy',lambda *a,**k:value)
    with pytest.raises(ValueError):m.approved_policy(given,sha,{})


def test_guard_precedes_campaign_binding_and_current_job_is_not_counted_twice(monkeypatch, tmp_path):
    from horsehair import campaign,repair
    events=[];reader=object();value=policy()
    monkeypatch.setattr(m,'SUPPORT',tmp_path)
    monkeypatch.setattr(campaign,'_repair_native_states',reader)
    monkeypatch.setattr(repair,'_repair_native_states',reader)
    monkeypatch.setattr(m.runpy,'run_path',lambda path:{'install':lambda evidence:events.append(('guard',evidence))})
    def admission(*args,**kwargs):events.append(('admission',args,kwargs));return {'status':'admitted'}
    monkeypatch.setattr(campaign,'combined_admission',admission)
    @contextmanager
    def mutex(path,timeout):
        assert path==m.ROOT/'state/af-campaign-registration.mutex' and timeout==120
        events.append(('lock',));yield;events.append(('unlock',))
    assert m.expanded_baseline({'config':True},{'release_id':'primary'},value,2000,lock_factory=mutex,clock=lambda:1000)=={'status':'admitted'}
    assert [e[0] for e in events]==['guard','lock','admission','unlock']
    assert events[2][2]=={'resource_policy':value,'cpus':0,'gpus':0,'hours':1}
    assert read_json(tmp_path/'registration-protection.json')['released_before_tests_and_scientific_preprocessing'] is True
    monkeypatch.setattr(campaign,'_repair_native_states',object())
    with pytest.raises(ValueError,match='guard must precede'):m.expanded_baseline({}, {'release_id':'primary'},value,2000)


def junit_result(tmp_path,monkeypatch):
    root=tmp_path/'study';directory=root/'state/qualification-evidence/native-attempt';directory.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root)
    path=directory/'junit.xml'
    path.write_text('<testsuites><testsuite tests="1826" failures="0" errors="0" skipped="0">'+
        ''.join('<testcase name="actual-'+str(i)+'"/>' for i in range(1826))+'</testsuite></testsuites>')
    result={'status':'complete','returncode':0,'test_counts':{'tests':1826,'failures':0,'errors':0,'skipped':0},
        'slurm_job_id':'123','source_sha256':m.FRAMEWORK_SHA,'config_sha256':m.CONFIG_SHA,
        'source_verified_unchanged':True,'artifacts':{'junit':{'path':str(path),'bytes':path.stat().st_size,'sha256':digest(path)}}}
    return result,path


def test_exact_full_native_suite_has_bound_actual_junit(tmp_path,monkeypatch):
    result,path=junit_result(tmp_path,monkeypatch)
    m.verify_suite(result,'123')
    assert m.EXPECTED_TESTS==1826


@pytest.mark.parametrize('change',['old_count','failed','wrong_job','wrong_source','wrong_config','source_changed',
    'junit_changed','attributes_only','missing_junit'])
def test_selected_incomplete_or_unbound_suite_never_qualifies(tmp_path,monkeypatch,change):
    result,path=junit_result(tmp_path,monkeypatch)
    if change=='old_count':result['test_counts']['tests']=1694
    elif change=='failed':result['returncode']=1
    elif change=='wrong_job':result['slurm_job_id']='124'
    elif change=='wrong_source':result['source_sha256']='b'*64
    elif change=='wrong_config':result['config_sha256']='b'*64
    elif change=='source_changed':result['source_verified_unchanged']=False
    elif change=='junit_changed':path.write_text('changed bytes')
    elif change=='missing_junit':result['artifacts']={}
    else:
        path.write_text('<testsuite tests="1826" failures="0" errors="0" skipped="0"/>')
        result['artifacts']['junit'].update(bytes=path.stat().st_size,sha256=digest(path))
    with pytest.raises(ValueError):m.verify_suite(result,'123')


def test_main_uses_explicit_policy_for_public_admission_and_finite_lease(tmp_path,monkeypatch):
    import horsehair
    from horsehair import cli,pipeline
    config=read_json(Path(agents.__file__).resolve().parents[2]/'configs/study.json')
    root=tmp_path/'root';root.mkdir();framework=root/'releases/new';primary=root/'releases/primary'
    support=root/'maintenance/support-v3';support.parent.mkdir();framework.mkdir(parents=True);primary.mkdir()
    identities={framework:{'sha256':m.FRAMEWORK_SHA},primary:{'sha256':m.PRIMARY_SHA}}
    for path in identities:
        atomic_json(path/'release.json',{'source_identity':identities[path],'config_sha256':m.CONFIG_SHA,'release_id':'primary'})
        atomic_json(path/'configs/study.json',config)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'PRIMARY',primary);monkeypatch.setattr(m,'FRAMEWORK',framework)
    monkeypatch.setattr(m,'SUPPORT',support);monkeypatch.setattr(horsehair,'__file__',str(framework/'src/horsehair/__init__.py'))
    monkeypatch.setattr(m,'source_identity',lambda path:identities[Path(path)])
    monkeypatch.setattr(m,'native_job_id',lambda:'123');monkeypatch.setattr(m.os,'geteuid',lambda:3164768,raising=False)
    monkeypatch.setenv('SLURM_CPUS_PER_TASK','4')
    monkeypatch.setattr(m.subprocess,'check_output',lambda command,**kwargs:'pwa209' if command[0]=='id' else 'nc0001.narval.calcul.quebec')
    driver=root/'driver.py';driver.write_text('bound driver')
    monkeypatch.setattr(m,'DRIVER_SHA',digest(driver));monkeypatch.setattr(m,'GUARD_SHA',digest(driver));monkeypatch.setattr(m,'QUOTA_SHA',digest(driver))
    monkeypatch.setattr(m,'GUARD',driver);monkeypatch.setattr(m,'QUOTA',driver)
    args=SimpleNamespace(framework=framework,driver=driver,expected_tests=1826,resource_policy=root/'policy.json',resource_policy_sha256='a'*64)
    monkeypatch.setattr(m.argparse.ArgumentParser,'parse_args',lambda self:args)
    value=policy();events=[]
    def approved(*args):events.append('policy');return value
    monkeypatch.setattr(m,'approved_policy',approved)
    monkeypatch.setattr(m,'native_allocation',lambda job:{'allocation_end_unix':time.time()+3600})
    def baseline(config,science,given,end):
        assert given is value;events.append('admission');atomic_json(support/'native-qualification-baseline.json',{'status':'admitted'})
        atomic_json(support/'registration-protection.json',{'released':True})
    monkeypatch.setattr(m,'expanded_baseline',baseline)
    @contextmanager
    def lease(*args,**kwargs):
        assert kwargs=={'cpus':4,'gpus':0,'max_hours':1,'job_id':'123','resource_policy':value}
        events.append('lease');yield;events.append('finalized')
    monkeypatch.setattr(m,'resource_lease',lease)
    monkeypatch.setattr(m,'fresh_storage',lambda config,quota:{'fresh':True})
    monkeypatch.setattr(m.runpy,'run_path',lambda path:{'require_storage_headroom_with_wait':lambda *a,**k:{'ready':True}})
    result,_=junit_result(tmp_path,monkeypatch)
    monkeypatch.setattr(m,'ROOT',root)
    path=root/'state/qualification-evidence/attempt/junit.xml';path.parent.mkdir(parents=True)
    source=Path(result['artifacts']['junit']['path']);path.write_bytes(source.read_bytes())
    result['artifacts']['junit'].update(path=str(path),sha256=digest(path),bytes=path.stat().st_size)
    monkeypatch.setattr(cli,'qualification',lambda got_config,got_root:copy.deepcopy(result))
    def smoke(config,root,primary,*args):
        destination=args[-1];destination.mkdir(parents=True);atomic_json(destination/'plan.json',{'bound':True})
        return {'release_identity':identities[primary],'config_sha256':m.CONFIG_SHA}
    monkeypatch.setattr(pipeline,'_primary_execution',smoke)
    m.main()
    record=read_json(support/'qualification.json')
    assert record['test_counts']['tests']==1826 and record['resource_policy']==value.receipt()
    assert record['scientific_result'] is record['admission_authority'] is False
    assert events==['policy','admission','lease','policy','finalized']


@pytest.mark.parametrize('failure',['callback','timeout','deadline'])
def test_registration_lock_failures_never_run_tests_or_leave_lock_held(tmp_path,monkeypatch,failure):
    from horsehair import campaign,repair
    events=[];reader=object();held=[False]
    monkeypatch.setattr(m,'SUPPORT',tmp_path)
    monkeypatch.setattr(campaign,'_repair_native_states',reader);monkeypatch.setattr(repair,'_repair_native_states',reader)
    monkeypatch.setattr(m.runpy,'run_path',lambda p:{'install':lambda e:None})
    @contextmanager
    def mutex(path,timeout):
        if failure=='timeout':raise TimeoutError('bounded acquisition expired')
        held[0]=True
        try:yield
        finally:held[0]=False;events.append('released')
    def admission(*a,**k):
        assert held[0];events.append('admission')
        if failure=='callback':raise ValueError('original admission refused')
        return {}
    monkeypatch.setattr(campaign,'combined_admission',admission)
    with pytest.raises((TimeoutError,ValueError)):
        m.expanded_baseline({}, {'release_id':'original'},policy(),1100 if failure=='deadline' else 2000,
                            lock_factory=mutex,clock=lambda:1000)
    assert not held[0] and not (tmp_path/'registration-protection.json').exists()
    if failure=='callback':assert events==['admission','released']
    else:assert events==[]


def test_actual_registration_mutex_contention_times_out_without_admission(tmp_path,monkeypatch):
    from horsehair import campaign,repair
    from horsehair.core import exclusive_file_lock,ReceiptLockTimeout
    root=tmp_path/'root';(root/'state').mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',tmp_path/'evidence')
    reader=object();monkeypatch.setattr(campaign,'_repair_native_states',reader);monkeypatch.setattr(repair,'_repair_native_states',reader)
    monkeypatch.setattr(m.runpy,'run_path',lambda p:{'install':lambda e:None})
    monkeypatch.setattr(campaign,'combined_admission',lambda *a,**k:pytest.fail('admission while publisher holds lock'))
    @contextmanager
    def bounded(path,timeout):
        with exclusive_file_lock(path,timeout=.1):yield
    with exclusive_file_lock(root/'state/af-campaign-registration.mutex',timeout=1):
        with pytest.raises(ReceiptLockTimeout):m.expanded_baseline({}, {'release_id':'primary'},policy(),2000,
            lock_factory=bounded,clock=lambda:1000)
    with exclusive_file_lock(root/'state/af-campaign-registration.mutex',timeout=1):pass


@pytest.fixture
def storage(tmp_path,monkeypatch):
    root=tmp_path/'root';support=root/'maintenance/support-v3';support.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',support)
    now=2000000000
    record={'measured_unix':now-10,'captured_utc':datetime.fromtimestamp(now-10,timezone.utc).isoformat()}
    path=root/'state/storage-budget.json';atomic_json(path,record)
    from horsehair import admission
    calls=[]
    def check(root,config,forecast_bytes,forecast_files,**kw):
        calls.append((root,config,forecast_bytes,forecast_files,kw));return 100,200,[path]
    monkeypatch.setattr(admission,'_storage',check)
    return root,support,path,record,now,calls


def test_six_hour_storage_check_calls_unchanged_contract_with_fresh_personal_quota(storage):
    root,support,path,record,now,calls=storage
    value=m.fresh_storage({'original':True},{'actual_quota':True},now=now)
    assert calls==[(root,{'original':True},5_000_000_000,15000,{'personal_headroom':{'actual_quota':True}})]
    assert value['census']['sha256']==digest(path) and value['maximum_census_age_seconds']==21600


@pytest.mark.parametrize('bad',['stale','future','nan','bool','naive','stale_capture'])
def test_storage_rejects_invalid_freshness_before_unchanged_contract(storage,bad):
    root,support,path,record,now,calls=storage
    if bad=='stale':record['measured_unix']=now-21600
    elif bad=='future':record['measured_unix']=now+1
    elif bad=='nan':record['measured_unix']=float('nan')
    elif bad=='bool':record['measured_unix']=True
    elif bad=='naive':record['captured_utc']='2033-05-18T03:33:20'
    else:record['captured_utc']=datetime.fromtimestamp(now-21600,timezone.utc).isoformat()
    path.write_text(__import__('json').dumps(record))
    with pytest.raises(ValueError):m.fresh_storage({}, {},now=now)
    assert calls==[] and not (support/'storage-proof.json').exists()


def test_storage_pair_change_fails_without_publishing_proof(storage,monkeypatch):
    root,support,path,record,now,calls=storage
    from horsehair import admission
    def check(*a,**k):atomic_json(path,dict(record,changed=True));return 100,200,[path]
    monkeypatch.setattr(admission,'_storage',check)
    with pytest.raises(ValueError,match='census changed'):m.fresh_storage({}, {},now=now)
    assert not (support/'storage-proof.json').exists()


def test_primary_smoke_publishes_only_under_shared_lock_and_releases_before_return(tmp_path,monkeypatch):
    from horsehair import pipeline
    root=tmp_path/'root';root.mkdir();monkeypatch.setattr(m,'ROOT',root)
    events=[];held=[False]
    @contextmanager
    def lock(path,timeout):
        assert path==root/'state/af-campaign-registration.mutex' and timeout==120
        held[0]=True;events.append('lock')
        try:yield
        finally:held[0]=False;events.append('unlock')
    def smoke(config,actual_root,primary,*args):
        assert held[0] and actual_root==root and primary==m.PRIMARY and args[:2]==('plan','behaviour-benchmark')
        events.append('publish');return {'primary':True}
    monkeypatch.setattr(m,'exclusive_file_lock',lock);monkeypatch.setattr(pipeline,'_primary_execution',smoke)
    result,path=m.primary_smoke({},2000,clock=lambda:1000)
    assert result=={'primary':True} and not held[0] and events==['lock','publish','unlock']
    assert path==root/'plans/native-primary-context-study-af-execution-v3-20261021'


def test_primary_smoke_failure_releases_lock_without_retry_or_overwrite(tmp_path,monkeypatch):
    from horsehair import pipeline
    root=tmp_path/'root';root.mkdir();monkeypatch.setattr(m,'ROOT',root)
    held=[False];calls=[]
    @contextmanager
    def lock(*a,**k):
        held[0]=True
        try:yield
        finally:held[0]=False
    def fail(*a,**k):calls.append(1);raise ValueError('actual primary planning refused')
    monkeypatch.setattr(m,'exclusive_file_lock',lock);monkeypatch.setattr(pipeline,'_primary_execution',fail)
    with pytest.raises(ValueError):m.primary_smoke({},2000,clock=lambda:1000)
    assert not held[0] and calls==[1]
