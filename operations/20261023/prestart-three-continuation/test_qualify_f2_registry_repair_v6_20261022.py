"""Mocked qualification contracts only; no native reads or submissions."""
from pathlib import Path
import copy
from datetime import datetime,timezone
import importlib.util
import os
import subprocess
import pytest
from horsehair.core import atomic_json,canonical_hash,digest

spec=importlib.util.spec_from_file_location('private_f2_q_v6',Path(__file__).with_name('qualify-f2-registry-repair-v6-20261022.py'))
q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)


@pytest.fixture
def xml(tmp_path):
    p=tmp_path/'actual.xml';p.write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0">'
        '<testcase classname="native.test_overlay" name="test_a"/></testsuite></testsuites>')
    return p


def test_junit_actual_roster_not_only_suite_attributes(xml):
    h=canonical_hash([('test_overlay','test_a')]);assert q.junit_roster(xml,1,h)['sha256']==digest(xml)
    xml.write_text(xml.read_text().replace('test_a','test_b'))
    with pytest.raises(ValueError):q.junit_roster(xml,1,h)


@pytest.mark.parametrize('bad',['empty','duplicate','skip','failure','dtd','false_count'])
def test_junit_rejects_incomplete_unsafe_or_nonpassing_roster(xml,bad):
    text=xml.read_text()
    if bad=='empty':text='<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"/></testsuites>'
    if bad=='duplicate':text=text.replace('</testsuite>','<testcase classname="test_overlay" name="test_a"/></testsuite>')
    if bad=='skip':text=text.replace('/></testsuite>','><skipped/></testcase></testsuite>')
    if bad=='failure':text=text.replace('/></testsuite>','><failure/></testcase></testsuite>')
    if bad=='dtd':text='<!DOCTYPE x [<!ENTITY y "z">]>'+text
    if bad=='false_count':text=text.replace('tests="1"','tests="9"')
    xml.write_text(text)
    with pytest.raises(ValueError):q.junit_roster(xml,1,canonical_hash([('test_overlay','test_a')]))


@pytest.fixture
def allocation(monkeypatch,tmp_path):
    end=2000000000
    v={'JobId':'123','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'NumCPUs':'4','NumNodes':'1-1','TimeLimit':'01:00:00','MinMemoryNode':'16G',
        'EndTime':datetime.fromtimestamp(end,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),'AllocTRES':'cpu=4,mem=16G'}
    monkeypatch.setattr(q.time,'time',lambda:end-1000);monkeypatch.setenv('SLURM_JOB_END_TIME',str(end))
    calls=[]
    def read(job,path):
        calls.append((job,path));return subprocess.CompletedProcess([],0,' '.join(k+'='+s for k,s in v.items()),'')
    monkeypatch.setattr(q.runpy,'run_path',lambda *a,**kw:{'run_scontrol_job':read})
    monkeypatch.setattr(q,'CASE',tmp_path/'evidence')
    return v,calls


def test_native_owner_requires_exact_finite_allocation(allocation):
    v,calls=allocation;assert q.native_owner('123')[0]==v
    assert len(calls)==1 and calls[0][0]=='123'


@pytest.mark.parametrize('key,value',[('JobId','124'),('UserId','foreign(7)'),('Account','other'),('JobState','PENDING'),
    ('NumCPUs','5'),('NumNodes','1-2'),('TimeLimit','02:00:00'),('MinMemoryNode','8G'),('AllocTRES','cpu=4,gres/gpu=1')])
def test_native_owner_rejects_wrong_owner_resources_or_gpu(allocation,key,value):
    allocation[0][key]=value
    with pytest.raises(ValueError):q.native_owner('123')


def test_native_owner_rejects_nonfinite_or_disagreeing_end(allocation,monkeypatch):
    monkeypatch.setenv('SLURM_JOB_END_TIME','nan')
    with pytest.raises(ValueError):q.native_owner('123')


def test_pytest_environment_removes_selectors_and_plugins(monkeypatch):
    monkeypatch.setenv('PYTEST_ADDOPTS','-k only_one');monkeypatch.setenv('PYTEST_PLUGINS','foreign')
    env=q.pytest_environment()
    assert 'PYTEST_ADDOPTS' not in env and 'PYTEST_PLUGINS' not in env
    assert env['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1' and env['TZ']=='UTC'


def test_storage_census_requires_both_actual_times_within_six_hours():
    q.require_fresh_census({'measured_unix':100000.,'captured_utc':datetime.fromtimestamp(100000.,timezone.utc).isoformat()},now=100001.)


@pytest.mark.parametrize('failure',['stale','future','nan','bool','naive','stale_capture'])
def test_storage_census_refuses_stale_future_or_malformed_actual_times(failure):
    record={'measured_unix':100000.,'captured_utc':datetime.fromtimestamp(100000.,timezone.utc).isoformat()};now=100001.
    if failure=='stale':record['measured_unix']=now-21600
    if failure=='future':record['measured_unix']=now+1
    if failure=='nan':record['measured_unix']=float('nan')
    if failure=='bool':record['measured_unix']=True
    if failure=='naive':record['captured_utc']='1970-01-02T00:00:00'
    if failure=='stale_capture':record['captured_utc']=datetime.fromtimestamp(now-21600,timezone.utc).isoformat()
    with pytest.raises(ValueError):q.require_fresh_census(record,now=now)


@pytest.fixture
def registration(tmp_path,monkeypatch):
    root=tmp_path/'root';directory=root/'plans/f2-q';directory.mkdir(parents=True);(root/'state').mkdir()
    helper=Path(q.__file__)
    command=q.shlex.join(['python',str(helper)]);script=directory/'task.sbatch';script.write_text('#!/bin/bash\n'+command+'\n')
    task={'name':'task','script':str(script),'script_sha256':canonical_hash(script.read_text()),'command':command,
        'resources':{'cpus':4,'gpu':False,'hours':1,'memory':'16G'}}
    plan={'release_identity':{'sha256':q.SOURCE},'tasks':[task]};ledger={'tasks':{'task':{'job_id':'123','state':'submitted','script_sha256':task['script_sha256']}}}
    pp=directory/'plan.json';lp=root/'state/q-submission.json';atomic_json(pp,plan);atomic_json(lp,ledger)
    monkeypatch.setattr(q,'ROOT',root)
    monkeypatch.setattr(q.runpy,'run_path',lambda *a,**kw:{'checked_pair':lambda *a:(plan,ledger)})
    return pp,lp,script,plan,ledger


def test_registered_native_task_binds_source_script_owner_and_command(registration):
    pp,lp,script,*_=registration
    assert q.registered_task(pp,lp,'task',script,'123',{})['native_job_id']=='123'


@pytest.mark.parametrize('bad',['foreign_job','different_source','changed_script','wrong_resources'])
def test_registered_native_task_rejects_changed_inputs(registration,bad):
    pp,lp,script,plan,ledger=registration
    if bad=='foreign_job':ledger['tasks']['task']['job_id']='124'
    if bad=='different_source':plan['release_identity']['sha256']='a'*64
    if bad=='changed_script':script.write_text('changed')
    if bad=='wrong_resources':plan['tasks'][0]['resources']['gpu']=True
    with pytest.raises(ValueError):q.registered_task(pp,lp,'task',script,'123',{})


def test_new_sources_and_complete_operational_recovery_activation_rosters_are_frozen():
    assert q.SUPPORT.name == 'study-af-registry-repair-support-v6-20261022'
    controller=Path(__file__).with_name(q.CONTROLLER)
    ns=q.runpy.run_path(str(controller),run_name='private_contract_test')
    assert type(ns['OPERATIONAL_TEST_COUNT']) is int and ns['OPERATIONAL_TEST_COUNT']>0
    assert q.re.fullmatch('[0-9a-f]{64}',ns['OPERATIONAL_TEST_ROSTER_SHA256'])
    assert ns['OPERATIONAL_TEST_SOURCES']=={n:q.PINS[n] for n in q.OVERLAY_TEST_NAMES}
    assert type(q.NEW_RECOVERY_COUNT) is int and q.NEW_RECOVERY_COUNT>0
    assert q.re.fullmatch('[0-9a-f]{64}',q.NEW_RECOVERY_ROSTER)
    assert type(q.ACTIVATION_COUNT) is int and q.ACTIVATION_COUNT>0
    assert q.re.fullmatch('[0-9a-f]{64}',q.ACTIVATION_ROSTER)
    for name in q.PINS:
        assert digest(Path(__file__).with_name(name)) == q.PINS[name]
    assert 'registered-snapshot-read-guard-20261021.py' not in q.PINS
    assert 'run-af-agent-controller-registry-v2-20261021.py' not in q.PINS


@pytest.fixture
def frozen_contract(monkeypatch):
    calls=[];monkeypatch.setattr(q,'verify_pins',lambda:calls.append('pins'))
    monkeypatch.setattr(q,'NEW_RECOVERY_COUNT',2);monkeypatch.setattr(q,'NEW_RECOVERY_ROSTER','a'*64)
    monkeypatch.setattr(q,'ACTIVATION_COUNT',3);monkeypatch.setattr(q,'ACTIVATION_ROSTER','b'*64)
    controller={'OPERATIONAL_TEST_COUNT':5,'OPERATIONAL_TEST_ROSTER_SHA256':'c'*64,
        'OPERATIONAL_TEST_SOURCES':{n:q.PINS[n] for n in q.OVERLAY_TEST_NAMES}}
    def load(path,**kwargs):
        calls.append(('controller',path,kwargs));return controller
    monkeypatch.setattr(q.runpy,'run_path',load)
    return calls,controller


def test_operational_contract_is_derived_only_after_all_byte_pins(frozen_contract):
    calls,controller=frozen_contract
    assert q.qualification_contract()==(controller,5,'c'*64)
    assert calls[0]=='pins' and calls[1][0]=='controller' and len(calls)==2
    assert calls[1][1]==str(q.SUPPORT/q.CONTROLLER)


@pytest.mark.parametrize('field,value',[('NEW_RECOVERY_COUNT',0),('NEW_RECOVERY_COUNT',True),
    ('NEW_RECOVERY_ROSTER','UNFROZEN'),('ACTIVATION_COUNT',0),('ACTIVATION_COUNT',True),('ACTIVATION_ROSTER','UNFROZEN')])
def test_unfrozen_separate_test_rosters_refuse_controller_loading(frozen_contract,monkeypatch,field,value):
    calls,_=frozen_contract;monkeypatch.setattr(q,field,value)
    with pytest.raises(ValueError,match='not frozen'):q.qualification_contract()
    assert calls==['pins']


@pytest.mark.parametrize('change',['count','boolean_count','roster','source','extra_source'])
def test_controller_operational_contract_refuses_unfrozen_or_changed_roster(frozen_contract,change):
    _,controller=frozen_contract
    if change=='count':controller['OPERATIONAL_TEST_COUNT']=0
    if change=='boolean_count':controller['OPERATIONAL_TEST_COUNT']=True
    if change=='roster':controller['OPERATIONAL_TEST_ROSTER_SHA256']='UNFROZEN'
    if change=='source':controller['OPERATIONAL_TEST_SOURCES'][q.OVERLAY_TEST_NAMES[0]]='d'*64
    if change=='extra_source':controller['OPERATIONAL_TEST_SOURCES']['foreign.py']='d'*64
    with pytest.raises(ValueError,match='not frozen'):q.qualification_contract()


def test_unfrozen_byte_pin_refuses_all_file_reads(monkeypatch):
    monkeypatch.setattr(q,'PINS',{**q.PINS,q.ACTIVATION_HELPER:'UNFROZEN'})
    def forbidden(*args,**kwargs):raise AssertionError('No file may be read before byte pins are frozen')
    monkeypatch.setattr(q,'digest',forbidden)
    with pytest.raises(ValueError,match='byte pins are not frozen'):q.verify_pins()


def test_qualification_main_orders_frozen_contracts_native_reads_and_separate_test_suites():
    import ast
    tree=ast.parse(Path(q.__file__).read_bytes());main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
    calls=list(n for n in ast.walk(main) if isinstance(n,ast.Call))
    names=lambda name:sorted(n.lineno for n in calls if isinstance(n.func,ast.Name) and n.func.id==name)
    assert names('qualification_contract')[0]<names('native_owner')[0]<names('registered_task')[0]<names('run_tests')[0]
    suites=[n for n in calls if isinstance(n.func,ast.Name) and n.func.id=='run_tests']
    assert len(suites)==4
    assert ast.literal_eval(suites[1].args[0])==['test_recover_f2_training0000_state_20261021.py']
    assert isinstance(suites[2].args[0].elts[0],ast.Name) and suites[2].args[0].elts[0].id=='NEW_RECOVERY_TEST'
    assert isinstance(suites[3].args[0].elts[0],ast.Name) and suites[3].args[0].elts[0].id=='ACTIVATION_TEST'
    text=ast.get_source_segment(Path(q.__file__).read_text(),main)
    assert "result['migration_qualification']" in text and "result['new_recovery_qualification']" in text
    assert "result['activation_qualification']" in text and "junit_roster(migration_xml,42,MIGRATION_ROSTER)" in text
    assert "required_files=2000" in text and "required_bytes=10_000_000_000" in text
    assert names('require_fresh_census')[0]<names('run_tests')[0]
    assert all(not (isinstance(n,ast.Constant) and n.value in {'sbatch','scancel','release','compare_and_swap','activate'}) for n in ast.walk(main))


def test_original_native_registration_allocation_and_freshness_contracts_are_unchanged():
    import ast
    import hashlib
    # Normalized source hashes freeze the v5 functions without requiring its
    # unstaged file or a particular Python version's AST fields on native nodes.
    before={'ordinary':'1ba1db6186e94a9adb40663d33736dd1a9f35f7219acad6dc20471e8ff3d807e',
        'junit_roster':'f65c07fdbb6d5064b87486c76af480b01bef9a4d6d8c3375ee8c6d0f1d16525f',
        'registered_task':'6775775ed12d7a499079222bd33cd28bf03ae433c2c6445e6db874e679103d88',
        'native_owner':'595eb2fbe9ae6f52d0a80b29119b0291e10f4d0d8f5d0787470e88323264c280',
        'pytest_environment':'124f7c11fb94934aee70ee3168b864e85462acc637171b2f0ba27eb8558ebec4',
        'require_fresh_census':'c1c25c64e1bf55d3b8907e1cef0584c24fda7dea1c1ff6c947e4018d870a279c',
        'run_tests':'168295854ff45e6f20b6df26fb575d3e3c6b92f85a5310834fa1f3c0005dd275'}
    source=Path(q.__file__).read_text();new=ast.parse(source)
    after={n.name:hashlib.sha256(ast.get_source_segment(source,n).encode()).hexdigest()
        for n in new.body if isinstance(n,ast.FunctionDef) and n.name in before}
    assert before==after
