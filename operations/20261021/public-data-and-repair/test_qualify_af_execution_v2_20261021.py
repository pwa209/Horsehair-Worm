"""Private native qualification contracts; no network or scheduler operations."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import copy
import importlib.util
import time

import pytest

from horsehair import agents
from horsehair.core import atomic_json, canonical_hash, digest, read_json

spec = importlib.util.spec_from_file_location('af_qualification_v2', Path(__file__).with_name('qualify-af-execution-v2-20261021.py'))
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


def test_guard_precedes_campaign_binding_and_current_job_is_not_counted_twice(monkeypatch):
    from horsehair import campaign,repair
    events=[];reader=object();value=policy()
    monkeypatch.setattr(campaign,'_repair_native_states',reader)
    monkeypatch.setattr(repair,'_repair_native_states',reader)
    monkeypatch.setattr(m.runpy,'run_path',lambda path:{'install':lambda evidence:events.append(('guard',evidence))})
    def admission(*args,**kwargs):events.append(('admission',args,kwargs));return {'status':'admitted'}
    monkeypatch.setattr(campaign,'combined_admission',admission)
    assert m.expanded_baseline({'config':True},{'release_id':'primary'},value)=={'status':'admitted'}
    assert events[0][0]=='guard' and events[1][0]=='admission'
    assert events[1][2]=={'resource_policy':value,'cpus':0,'gpus':0,'hours':1}
    monkeypatch.setattr(campaign,'_repair_native_states',object())
    with pytest.raises(ValueError,match='guard must precede'):m.expanded_baseline({}, {'release_id':'primary'},value)


def junit_result(tmp_path,monkeypatch):
    root=tmp_path/'study';directory=root/'state/qualification-evidence/native-attempt';directory.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root)
    path=directory/'junit.xml'
    path.write_text('<testsuites><testsuite tests="1806" failures="0" errors="0" skipped="0">'+
        ''.join('<testcase name="actual-'+str(i)+'"/>' for i in range(1806))+'</testsuite></testsuites>')
    result={'status':'complete','returncode':0,'test_counts':{'tests':1806,'failures':0,'errors':0,'skipped':0},
        'slurm_job_id':'123','source_sha256':m.FRAMEWORK_SHA,'config_sha256':m.CONFIG_SHA,
        'source_verified_unchanged':True,'artifacts':{'junit':{'path':str(path),'bytes':path.stat().st_size,'sha256':digest(path)}}}
    return result,path


def test_exact_full_native_suite_has_bound_actual_junit(tmp_path,monkeypatch):
    result,path=junit_result(tmp_path,monkeypatch)
    m.verify_suite(result,'123')
    assert m.EXPECTED_TESTS==1806


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
        path.write_text('<testsuite tests="1806" failures="0" errors="0" skipped="0"/>')
        result['artifacts']['junit'].update(bytes=path.stat().st_size,sha256=digest(path))
    with pytest.raises(ValueError):m.verify_suite(result,'123')


def test_main_uses_explicit_policy_for_public_admission_and_finite_lease(tmp_path,monkeypatch):
    import horsehair
    from horsehair import cli,pipeline
    config=read_json(Path(agents.__file__).resolve().parents[2]/'configs/study.json')
    root=tmp_path/'root';root.mkdir();framework=root/'releases/new';primary=root/'releases/primary'
    support=root/'maintenance/support-v2';support.parent.mkdir();framework.mkdir(parents=True);primary.mkdir()
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
    args=SimpleNamespace(framework=framework,driver=driver,expected_tests=1806,resource_policy=root/'policy.json',resource_policy_sha256='a'*64)
    monkeypatch.setattr(m.argparse.ArgumentParser,'parse_args',lambda self:args)
    value=policy();events=[]
    def approved(*args):events.append('policy');return value
    monkeypatch.setattr(m,'approved_policy',approved)
    monkeypatch.setattr(m,'native_allocation',lambda job:{'allocation_end_unix':time.time()+3600})
    def baseline(config,science,given):
        assert given is value;events.append('admission');atomic_json(support/'native-qualification-baseline.json',{'status':'admitted'})
    monkeypatch.setattr(m,'expanded_baseline',baseline)
    @contextmanager
    def lease(*args,**kwargs):
        assert kwargs=={'cpus':4,'gpus':0,'max_hours':1,'job_id':'123','resource_policy':value}
        events.append('lease');yield;events.append('finalized')
    monkeypatch.setattr(m,'resource_lease',lease)
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
    assert record['test_counts']['tests']==1806 and record['resource_policy']==value.receipt()
    assert record['scientific_result'] is record['admission_authority'] is False
    assert events==['policy','admission','lease','policy','finalized']
