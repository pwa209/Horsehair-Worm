"""Generic native-registration, dependency, freshness and ambiguity fixtures."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from datetime import datetime,timezone
import ast
import importlib.util
import json
import os
import shlex
import subprocess
import sys
import time
import pytest

HERE=Path(__file__).resolve().parent


def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


@pytest.fixture
def m():return load(HERE/'run-behaviour-continuation-activation-v1-20261024.py','generic_activation_test')


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes((json.dumps(value,sort_keys=True)+'\n').encode());return path


@pytest.mark.parametrize('raw',['{"x":1,"x":2}','{"x":NaN}','{"x":Infinity}'])
def test_strict_json_rejects_ambiguity(m,raw):
    with pytest.raises(ValueError):m.strict(raw)


def native(m):
    return {'JobId':'900000001','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'JobName':'hhw-'+m.TASK,'Command':'/fixture/activation.sbatch','NumCPUs':'1','NumNodes':'1',
        'MinMemoryNode':'16G','TimeLimit':'04:00:00','EndTime':'2030-01-01T04:00:00',
        'AllocTRES':'cpu=1,mem=16G,node=1','ReqTRES':'cpu=1,mem=16G,node=1'}


def test_actual_native_four_hour_contract(m,monkeypatch):
    f=native(m);monkeypatch.setenv('SLURM_JOB_END_TIME','1893470400')
    assert m.allocation(f,'900000001',{'script':f['Command']},wall=lambda:1893456000)==1893470400


@pytest.mark.parametrize('key,value',[('NumCPUs','4'),('MinMemoryNode','64G'),('TimeLimit','01:00:00'),
    ('UserId','other(1)'),('Account','other'),('AllocTRES','cpu=1,gres/gpu=1'),('Command','/wrong.sbatch')])
def test_actual_native_wrong_resource_or_owner_refused(m,monkeypatch,key,value):
    f=native(m);f[key]=value;monkeypatch.setenv('SLURM_JOB_END_TIME','1893470400')
    with pytest.raises(ValueError):m.allocation(f,'900000001',{'script':'/fixture/activation.sbatch'},wall=lambda:1893456000)


def test_native_cleanup_margin_refused(m,monkeypatch):
    f=native(m);monkeypatch.setenv('SLURM_JOB_END_TIME','1893470400')
    with pytest.raises(ValueError,match='cleanup'):m.allocation(f,'900000001',{'script':f['Command']},wall=lambda:1893469900)


def test_logical_dependencies_allow_individually_verified_duplicates(m):
    record={'state':'COMPLETED','exit_code':'0:0'};proof={'job_id':'123','dependency_kind':'afterok','native_records':{'123':record}}
    calls=[];m.completed_set([proof,dict(proof)],{'123'},lambda job:{'native_records':{'123':record}},lambda p:calls.append(p))
    assert len(calls)==2


@pytest.mark.parametrize('change',['missing','failed_kind','changed_records'])
def test_logical_dependency_bad_proofs_refused(m,change):
    proof={'job_id':'123','dependency_kind':'afterok','native_records':{'123':{'state':'COMPLETED'}}}
    proofs=[] if change=='missing' else [proof]
    if change=='failed_kind':proof['dependency_kind']='afterany'
    current={'native_records':{'123':{'state':'FAILED' if change=='changed_records' else 'COMPLETED'}}}
    with pytest.raises(ValueError):m.completed_set(proofs,{'123'},lambda job:current,lambda proof:None)


def test_bounded_transport_does_not_retry_or_permit_mutations(m):
    calls=[];run=m.bounded_transport(3000,runner=lambda c,**kw:calls.append((c,kw)) or 'ok',wall=lambda:1000)
    assert run(['sacct','-j','123'],text=True)=='ok' and calls==[(['sacct','-j','123'],{'text':True,'timeout':45.})]
    with pytest.raises(ValueError):run(['scontrol','release','123'])
    with pytest.raises(ValueError):m.bounded_transport(1500,wall=lambda:1000)(['sacct'])


def producer_clock(m):
    path=m.PROVIDER if m.PROVIDER.exists() else HERE.parent/'census-resilience-v1'/m.PROVIDER.name
    assert m.digest(path)==m.PROVIDER_SHA
    return load(path,'actual_qualified_provider_clock').fresh_original_clocks


@pytest.mark.parametrize('change',['positive','expired','future','detail_changed','quota_changed'])
def test_original_current_clock_and_pair_bytes(m,tmp_path,change):
    clock=producer_clock(m);m.ROOT=tmp_path;now=1792845000.;stamp=now-(21600 if change=='expired' else -10 if change=='future' else 100)
    detail=write(tmp_path/'state/storage-entry-sizes.json',{'opaque':'inert fixture, no scientific values'})
    quota=tmp_path/'state/quota-123.txt';quota.write_text('inert pinned original quota fixture\n')
    budget={'status':'measured','root':str(tmp_path),'personal_uid':3164768,'measured_unix':stamp,
        'captured_utc':datetime.fromtimestamp(stamp,timezone.utc).isoformat(),'study_files':1,'study_bytes':10,
        'exact_entry_measurements':{'receipt_path':str(detail),'receipt_sha256':m.digest(detail),'method':'owned_regular_length','count':1,'bytes':10},
        'native_source_paths':[str(quota)],'native_source_hashes':{str(quota):m.digest(quota)}}
    write(tmp_path/'state/storage-budget.json',budget)
    if change=='detail_changed':detail.write_text('changed')
    if change=='quota_changed':quota.write_text('changed')
    producer=SimpleNamespace(fresh_original_clocks=clock)
    if change=='positive':
        proof=m.current_pair(producer,wall=lambda:now)
        assert proof['entry_sizes']['path']==str(detail) and proof['freshness_metadata_only'] is True
        assert proof['clocks']['original_measured_age_seconds']==100 and proof['admission_authority'] is False
    else:
        with pytest.raises(ValueError):m.current_pair(producer,wall=lambda:now)


def test_real_pytest_accepts_child_fixture_basetemp(m,tmp_path):
    workspace,basetemp=m.fixture_workspace(tmp_path,'900000001')
    source=workspace/'test_generic_fixture.py';source.write_text('def test_fixture(tmp_path):\n    assert tmp_path.is_dir()\n')
    result=subprocess.run([sys.executable,'-s','-B','-m','pytest',str(source),'-q','-p','no:cacheprovider','--basetemp',str(basetemp)],
        cwd=workspace,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},capture_output=True,text=True,timeout=60,check=False)
    assert result.returncode==0 and not result.stderr,(result.stdout,result.stderr)


def test_actual_sealed_behaviour_builder_matches_complete_worker_argument_contract(m):
    path=m.BEHAVIOUR/'build-native-plan.py'
    if not path.exists():path=HERE.parent/'behaviour-full-cohort-continuation-v2/build-native-plan.py'
    assert m.digest(path)=='14d01d927baf921bb9c657905005b25003af725e08916c5f7d59ae2934bccc61'
    builder=load(path,'actual_sealed_behaviour_builder_contract')
    config_path=m.FRAMEWORK/'configs/study.json'
    if not config_path.exists():config_path=HERE.parents[2]/'configs/study.json'
    config=json.loads(config_path.read_text());receipt='a'*64
    provider={'job_id':m.FIRST,'schedule':m.SCHEDULE,'receipt':{'path':'/opaque/first-receipt.json','sha256':receipt}}
    proofs=[{'job_id':job,'dependency_kind':'afterok'} for job in m.DEPENDENCIES]
    plan,_=builder.render(config,{'sha256':m.BEHAVIOUR_SOURCE},m.BEHAVIOUR_MANIFEST_SHA,m.VERIFIER,
        m.VERIFIER_RECEIPT_SHA,m.VERIFIER_MANIFEST_SHA,provider,proofs,lambda cfg,release,name,command,**kw:command+'\n')
    tail=shlex.split(plan['tasks'][0]['command'])[4:]
    assert dict(zip(tail[::2],tail[1::2]))==m.expected_worker_flags(receipt)


def dispatch_fixture(m,tmp_path):
    from horsehair import slurm
    m.ROOT=tmp_path;m.BEHAVIOUR=tmp_path/'maintenance/behaviour';m.BEHAVIOUR_FRAMEWORK=tmp_path/'releases/8495'
    m.BEHAVIOUR_PLAN=tmp_path/'plans/behaviour/plan.json';m.BEHAVIOUR_LEDGER=tmp_path/'state/behaviour-submission.json'
    records={job:{'state':'COMPLETED','exit_code':'0:0','elapsed_seconds':100,'cpus':8 if job==m.FIRST else 4,
        'gpus':0,'basis':'sacct final allocated wall time'} for job in m.DEPENDENCIES}
    write(tmp_path/'state/resource-budget.json',{'native_jobs':records})
    completion=lambda job:slurm.completed_native_dependency(tmp_path,job)
    proofs=[completion(job) for job in m.DEPENDENCIES]
    script=m.BEHAVIOUR_PLAN.parent/(m.BEHAVIOUR_TASK+'.sbatch');command=shlex.join(['python','-s','-u',str(m.BEHAVIOUR/'run-full-cohort-continuation-v2-20261024.py'),
        *[value for pair in m.expected_worker_flags('a'*64).items() for value in pair]])
    text='#!/bin/bash\n'+command+'\n';script.parent.mkdir(parents=True);script.write_bytes(text.encode())
    task={'name':m.BEHAVIOUR_TASK,'command':command,'script':str(script),'script_sha256':m.canonical(text),
        'resources':{'cpus':1,'memory':'64G','hours':24,'gpu':False,'array':None},'depends':[],'external_dependencies':[],
        'dependency_kind':'afterok','completed_external_dependencies':proofs,'scientific_result':False,'complete_declared_workload':False,'admission_authority':False}
    plan={'root':str(tmp_path),'phase':'behaviour-full-cohort-continuation-v2-20261024','release':str(m.BEHAVIOUR_FRAMEWORK),
        'release_identity':{'sha256':m.BEHAVIOUR_SOURCE},'config_sha256':m.CONFIG,'source_manifest_sha256':m.BEHAVIOUR_MANIFEST_SHA,
        'tasks':[task],'scientific_result':False,'complete_declared_workload':False,'admission_authority':False}
    write(m.BEHAVIOUR_PLAN,plan);case=tmp_path/'maintenance/activation';case.mkdir(parents=True)
    verify=lambda proof:slurm._verify_completed_dependency(tmp_path,proof)
    return case,plan,task,completion,verify,slurm


@pytest.mark.parametrize('change',['positive','submit_timeout','wrong_held_memory','expired_before_release'])
def test_actual_slurm_submit_and_single_release_boundaries(m,tmp_path,monkeypatch,change):
    case,plan,task,completion,verify,slurm=dispatch_fixture(m,tmp_path);monkeypatch.delenv('SBATCH_EXCLUDE',raising=False)
    assert m.worker_plan('a'*64,completion,verify)==(plan,task)
    calls=[];checks=[]
    def freshness():
        checks.append(True)
        if change=='expired_before_release' and len(checks)==3:raise ValueError('fixture original clock expired')
    def runner(command,**options):
        calls.append(command)
        if command[0]=='sbatch':
            if change=='submit_timeout':raise subprocess.TimeoutExpired(command,45)
            assert command==['sbatch','--hold','--parsable',task['script']]
            return subprocess.CompletedProcess(command,0,'900000099\n','')
        if command[1:3]==['show','job']:
            fields={'JobId':'900000099','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'PENDING','Reason':'JobHeldUser',
                'Command':task['script'],'JobName':'hhw-'+m.BEHAVIOUR_TASK,'NumCPUs':'1','NumNodes':'1','MinMemoryNode':'32G' if change=='wrong_held_memory' else '64G',
                'TimeLimit':'1-00:00:00','RunTime':'00:00:00','Restarts':'0','Dependency':'(null)','ReqTRES':'cpu=1,mem=64G,node=1','AllocTRES':'(null)'}
            return subprocess.CompletedProcess(command,0,' '.join(k+'='+shlex.quote(v) for k,v in fields.items()),'')
        assert command==['scontrol','release','900000099'];return subprocess.CompletedProcess(command,0,'','')
    invoke=lambda:m.dispatch(case,plan,task,time.time()+3600,slurm.submit,completion,verify,freshness,runner=runner)
    if change=='positive':
        result=invoke();assert result['worker_native_job_id']=='900000099' and result['native_qualification_tests_required_by_worker']==179
        assert len([c for c in calls if c[:2]==['scontrol','release']])==1
    else:
        with pytest.raises((ValueError,subprocess.TimeoutExpired)):invoke()
        assert not any(c[:2]==['scontrol','release'] for c in calls)
        assert len([c for c in calls if c[0]=='sbatch'])==1
    assert (case/'submit-intent.json').exists()
    with pytest.raises(ValueError,match='Existing ledger'):invoke()


def test_activation_plan_is_one_finite_afterok_job(m):
    builder=load(HERE/'build-native-plan.py','activation_plan_contract')
    config_path=m.FRAMEWORK/'configs/study.json'
    if not config_path.exists():config_path=HERE.parents[2]/'configs/study.json'
    config=json.loads(config_path.read_text());manifest={'files':{m.SELF:'a'*64},'suite':{'tests':1}}
    binding={'path':str(m.SUPPORT/'source-manifest.json'),'sha256':'b'*64};captured=[]
    plan,text=builder.render(config,{'sha256':m.SOURCE},binding,manifest,lambda cfg,release,name,command,**kw:captured.append(kw) or command+'\n')
    assert len(plan['tasks'])==1 and plan['tasks'][0]['external_dependencies']==[m.FIRST]
    assert plan['tasks'][0]['dependency_kind']=='afterok' and captured==[{'cpus':1,'memory':'16G','hours':4,'gpu':False,'array':None}]
    assert m.SOURCE==plan['release_identity']['sha256'] and str(m.SUPPORT/m.SELF) in text


def test_changed_plan_is_refused_before_any_submission(m,tmp_path):
    case,plan,task,completion,verify,slurm=dispatch_fixture(m,tmp_path)
    changed=json.loads(m.BEHAVIOUR_PLAN.read_text());changed['tasks'][0]['resources']['cpus']=2;write(m.BEHAVIOUR_PLAN,changed)
    calls=[]
    with pytest.raises(ValueError,match='changed before submission'):
        m.dispatch(case,plan,task,time.time()+3600,lambda *a,**kw:calls.append(True),completion,verify,lambda:None)
    assert calls==[] and not (case/'submit-intent.json').exists()


def test_source_has_no_numerical_dispatch_or_unbounded_mutation_retry(m):
    tree=ast.parse(Path(m.__file__).read_bytes());text=ast.unparse(tree)
    assert 'import torch' not in text and 'optimizer' not in text and 'storage_snapshot(' not in text
    assert "budget.native_accounting(ROOT, [FIRST], runner=transport)" in text
    assert text.index('budget.native_accounting(')<text.index('provider = provider_evidence(')
    assert 'min(7200.0, end - time.time() - 600)' in text
    assert 'producer.validate_schedule_binding' in text and 'slurm.submit' in text
