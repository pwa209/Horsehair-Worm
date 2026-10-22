"""Mocked qualification contracts only; no native reads or submissions."""
from pathlib import Path
import copy
from datetime import datetime,timezone
import importlib.util
import os
import subprocess
import pytest
from horsehair.core import atomic_json,canonical_hash,digest

spec=importlib.util.spec_from_file_location('private_f2_q',Path(__file__).with_name('qualify-f2-registry-repair-v5-20261022.py'))
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


def test_new_sources_and_full103_operational_roster_are_frozen():
    assert q.SUPPORT.name == 'study-af-registry-repair-support-v5-20261022'
    controller=Path(__file__).with_name('run-af-agent-controller-registry-v5-20261022.py')
    ns=q.runpy.run_path(str(controller),run_name='private_contract_test')
    assert ns['OPERATIONAL_TEST_COUNT']==103
    assert ns['OPERATIONAL_TEST_ROSTER_SHA256']=='154680646a7d4a8e68eb29174f1a11a6702326597aa56923a1ec69ef1736c5c1'
    for name in q.PINS:
        assert digest(Path(__file__).with_name(name)) == q.PINS[name]
    assert 'registered-snapshot-read-guard-20261021.py' not in q.PINS
    assert 'run-af-agent-controller-registry-v2-20261021.py' not in q.PINS
