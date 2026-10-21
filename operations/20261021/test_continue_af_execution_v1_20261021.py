"""Mocked continuation orchestration; no native calls or scheduler side effects."""
import copy
import importlib.util
import json
import math
from pathlib import Path
import subprocess
import time

import pytest

from horsehair import agents
from horsehair.core import atomic_json,canonical_hash,digest,read_json


def load(name,filename):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(filename))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


m=load('private_af_continuation','continue-af-execution-v1-20261021.py')
cmod=load('private_af_continuation_controller','run-af-agent-controller-v1-20261021.py')


class Policy:
    def receipt(self):return {'sha256':'a'*64,'scientific_gates_changed':False}


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    root=tmp_path/'scratch';case=root/'maintenance/study-af-campaign-v1-20261021';area=case/'precision-and-f2-continuation-v1'
    area.mkdir(parents=True);(root/'state').mkdir();(root/'plans').mkdir();framework=root/'releases/qualified';framework.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'CASE',case);monkeypatch.setattr(m,'FRAMEWORK',framework)
    monkeypatch.setattr(cmod,'ROOT',root)
    monkeypatch.setattr(m,'source_identity',lambda path:{'sha256':m.SOURCE,'files':{}})
    monkeypatch.setattr(m,'completed_native_dependency',lambda root,job:{'job_id':job})
    controller_path=area/'run-af-agent-controller-v1-20261021.py';controller_path.write_text('qualified controller fixture')
    monkeypatch.setattr(m,'CONTROLLER_SHA',digest(controller_path))
    tests_path=area/'test_af_agent_controller_v1_20261021.py';tests_path.write_text('qualified tests fixture')
    canary=case/'canary';canary.mkdir();plan={'canary':True,'conditions':list(range(52))};atomic_json(canary/'plan.json',plan)
    atomic_json(canary/'plan-binding.json',{'source_bound':'fixture'})
    request_path=area/'request.json';atomic_json(request_path,{'case':str(canary),'required_conditions':52})
    d={'case':str(case),'framework_release':str(framework),'execution_source_sha256':m.SOURCE,
        'worker_path':str(area/'worker.py'),'worker_sha256':m.WORKER_SHA,
        'qualification':{'path':'qualified.json','sha256':'q'*64},'resource_policy':{'sha256':'p'*64},
        'frozen_protocol_sha256':'f'*64,'agent_campaign_case':str(case/'agent-final-f64-v1'),
        'canary_request':{'path':str(request_path),'sha256':digest(request_path)}}
    policy=Policy();events=[];counter=[100];tasks={}
    original_fields=cmod.fields
    monkeypatch.setattr(cmod,'fields',lambda response:getattr(response,'fixture_fields',None) or original_fields(response))
    def runner(command,**kw):
        events.append(('run',command,kw))
        if '-m' in command and command[command.index('-m')+1]=='pytest':
            path=Path(command[command.index('--junitxml')+1]);path.write_text('<testsuites><testsuite tests="48" failures="0" errors="0" skipped="0">'+''.join('<testcase name="case-'+str(i)+'"/>' for i in range(48))+'</testsuite></testsuites>')
            return subprocess.CompletedProcess(command,0,'48 passed','')
        if command[0]=='sbatch':
            assert command[1]=='--hold' and kw['timeout']==30
            script=Path(command[-1]);plan=read_json(script.parent/'plan.json');task=next(t for t in plan['tasks'] if t['script']==str(script))
            counter[0]+=1;job=str(counter[0]);tasks[job]=task
            return subprocess.CompletedProcess(command,0,job+'\n','')
        assert command[:2]==['scontrol','release'];assert kw['timeout']==30
        return subprocess.CompletedProcess(command,0,'','')
    def scheduler(job,evidence):
        evidence.mkdir(parents=True,exist_ok=False);task=tasks[job];r=task['resources'];events.append(('held',job))
        response=subprocess.CompletedProcess([],0,'fixture','')
        response.fixture_fields={'JobId':job,'UserId':'pwa209(3164768)','JobName':'hhw-'+task['name'],
            'JobState':'PENDING','Reason':'JobHeldUser','Command':task['script'],'Account':'def-ptewarie_cpu',
            'NumCPUs':str(r['cpus']),'NumNodes':'1-1','TimeLimit':str(r['hours'])+':00:00',
            'MinMemoryNode':str(int(r['memory'][:-1])*1024)+'M','ReqTRES':'cpu='+str(r['cpus']),'AllocTRES':'(null)'}
        return response
    native={}
    for job,mode,gpus in [('11','gpu-qualify',1),('12','precision-plan',0)]:
        native[job]={'state':'COMPLETED','exit_code':'0:0','cpus':4,'alloc_tres':'cpu=4,gres/gpu='+str(gpus),
            'elapsed_seconds':10.,'end':'2026-10-21T15:00:00','user':'pwa209','account':'def-ptewarie_gpu' if gpus else 'def-ptewarie_cpu'}
        result={'status':'complete','iterations':20,'device':'A100','cuda':'12.6','torch':'2.6.0',
            'elapsed_seconds':1.,'loss':.5,'cpu_cuda_regression':{a:{'max_action_probability_difference':1e-8,'tolerance':3e-5}
                for a in ['rnn','gru','modular','feedforward']}} if gpus else plan
        receipt={'status':'complete_native_work','mode':mode,'index':None,'native_job_id':job,
            'descriptor_sha256':'d'*64,'source_sha256':m.SOURCE,'full_config_sha256':m.CONFIG,
            'resource_policy':policy.receipt(),'scientific_result':False,'admission_authority':False,'result':result,
            'allocation':{'JobId':job,'UserId':'pwa209(3164768)','NumCPUs':'4','Account':native[job]['account']}}
        if not gpus:receipt.update(plan_sha256=digest(canary/'plan.json'),plan_binding_sha256=digest(canary/'plan-binding.json'))
        path=case/('native-'+job)/'receipt.json';atomic_json(path,receipt)
    ctx={'job':'99','descriptor':d,'descriptor_sha256':'d'*64,'policy':policy,
        'config':read_json(Path(agents.__file__).resolve().parents[2]/'configs/study.json'),
        'area':area,'controller':cmod,'controller_path':controller_path,'tests_path':tests_path,'tests_sha256':digest(tests_path),
        'gpu_job':'11','plan_job':'12','native_reader':lambda *args,**kwargs:copy.deepcopy(native),
        'scheduler':scheduler,'end':time.time()+3600,
        'admission':lambda label,**kwargs:events.append(('admission',label,kwargs)) or {'status':'admitted'}}
    return ctx,events,runner,tasks,native


def test_native_qualification_then_seven_held_registered_verified_released(fixture):
    ctx,events,runner,tasks,_=fixture;result=m.orchestrate(ctx,runner=runner)
    assert len(result['jobs'])==7 and result['precision_gate_complete'] is False
    assert result['scientific_result'] is result['admission_authority'] is False and result['H4_inference_status']=='pending'
    assert '-m' in events[0][1] and 'pytest' in events[0][1]
    release=[event for event in events if event[0]=='run' and event[1][:2]==['scontrol','release']]
    assert len(release)==7
    before=events[:events.index(release[0])]
    assert len([event for event in before if event[0]=='held'])==7
    assert [event for event in before if event[0]=='admission'][-1]==('admission','all-continuation-registered',{'cpus':0,'gpus':0,'hours':1})
    plans=[read_json(v['path']) for v in result['plans']];canary,controllers=plans
    assert [t['resources']['hours'] for t in canary['tasks']]==[12,12,1]
    assert all(t['resources']['cpus']==4 and t['dependency_kind']=='afterok' for t in canary['tasks'])
    assert [t['depends'] for t in canary['tasks']]==[[],['af-precision-diagnostics-v1'],['af-precision-chunk-v1']]
    assert all(t['resources']['cpus']==1 and t['resources']['hours']==96 and t['dependency_kind']=='afterany' for t in controllers['tasks'])
    d2=read_json(result['descriptor_v2_path']);d3=read_json(result['descriptor_v3_path'])
    assert d3['worker_descriptor']=={'path':result['descriptor_v2_path'],'sha256':result['descriptor_v2_sha256']}
    assert d3['precision_gate']['descriptor_sha256']==result['descriptor_v2_sha256']
    assert all(result['descriptor_v2_sha256'] in t['command'] for t in canary['tasks'])
    assert all(result['descriptor_v3_sha256'] in t['command'] for t in controllers['tasks'])
    assert d2['gpu_qualification']['descriptor_sha256']==ctx['descriptor_sha256']
    assert all(read_json(p)['returncode']==0 for p in ctx['area'].glob('release-*-result.json'))


def test_private_qualification_failure_prevents_all_submissions(fixture):
    ctx,events,runner,*_=fixture
    def failed(command,**kw):return subprocess.CompletedProcess(command,1,'failed','failure')
    with pytest.raises(ValueError,match='qualification failed'):m.orchestrate(ctx,runner=failed)
    assert not list(m.ROOT.glob('plans/*/plan.json'))


@pytest.mark.parametrize('changed', ['skipped','count','changed_test_bytes','attributes_only'])
def test_incomplete_or_changed_private_qualification_cannot_launch(fixture,changed):
    ctx,events,runner,*_=fixture
    def altered(command,**kw):
        value=runner(command,**kw)
        if '-m' in command:
            path=Path(command[command.index('--junitxml')+1])
            if changed=='skipped':path.write_text('<testsuite tests="48" failures="0" errors="0" skipped="1"/>')
            elif changed=='count':path.write_text('<testsuite tests="47" failures="0" errors="0" skipped="0"/>')
            elif changed=='attributes_only':path.write_text('<testsuite tests="48" failures="0" errors="0" skipped="0"/>')
            else:ctx['tests_path'].write_text('changed')
        return value
    with pytest.raises(ValueError):m.orchestrate(ctx,runner=altered)
    assert not [e for e in events if e[0]=='run' and e[1][0]=='sbatch']


def test_private_qualification_clears_external_selectors_and_explicit_plugins(fixture,monkeypatch):
    ctx,events,runner,*_=fixture
    monkeypatch.setenv('PYTEST_ADDOPTS','-k never_run_anything')
    monkeypatch.setenv('PYTEST_PLUGINS','unreviewed_plugin')
    receipt=m.qualify_controller(ctx,runner=runner)
    environment=events[0][2]['env']
    assert 'PYTEST_ADDOPTS' not in environment and 'PYTEST_PLUGINS' not in environment
    assert environment['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1' and receipt['test_counts']['tests']==48


@pytest.mark.parametrize('field,value',[('NumNodes','1-2'),('UserId','other(4)'),('ReqTRES','cpu=4,gres/gpu=1'),
    ('Reason','Resources'),('MinMemoryNode','64G'),('TimeLimit','168:00:00'),('Account','other_cpu')])
def test_changed_held_native_contract_preserves_all_registered_jobs(fixture,field,value):
    ctx,events,runner,tasks,_=fixture;original=ctx['scheduler']
    def changed(job,evidence):
        response=original(job,evidence);response.fixture_fields[field]=value;return response
    ctx['scheduler']=changed
    with pytest.raises(ValueError):m.orchestrate(ctx,runner=runner)
    assert len(tasks)==7 and not [e for e in events if e[0]=='run' and e[1][0]=='scontrol']
    assert not (ctx['area']/'release-intent.json').exists()


def test_ambiguous_sbatch_is_not_replayed(fixture):
    ctx,events,runner,tasks,_=fixture
    def timeout(command,**kw):
        if command[0]=='sbatch':events.append(('attempt',command));raise subprocess.TimeoutExpired(command,30)
        return runner(command,**kw)
    with pytest.raises(subprocess.TimeoutExpired):m.orchestrate(ctx,runner=timeout)
    ledger=read_json(next((m.ROOT/'state').glob('*-submission.json')))
    assert next(iter(ledger['tasks'].values()))['state']=='submitting'
    with pytest.raises(FileExistsError):m.orchestrate(ctx,runner=runner)
    assert len([e for e in events if e[0]=='attempt'])==1


def test_failed_postregistration_admission_releases_nothing(fixture):
    ctx,events,runner,tasks,_=fixture;original=ctx['admission']
    def denied(label,**kw):
        if label=='all-continuation-registered':raise ValueError('global capacity changed')
        return original(label,**kw)
    ctx['admission']=denied
    with pytest.raises(ValueError,match='capacity changed'):m.orchestrate(ctx,runner=runner)
    assert len(tasks)==7 and not [e for e in events if e[0]=='run' and e[1][0]=='scontrol']


def test_release_timeout_preserves_explicit_intent_and_no_replay(fixture):
    ctx,events,runner,tasks,_=fixture
    def timeout(command,**kw):
        if command[0]=='scontrol':events.append(('attempt_release',command));raise subprocess.TimeoutExpired(command,30)
        return runner(command,**kw)
    with pytest.raises(subprocess.TimeoutExpired):m.orchestrate(ctx,runner=timeout)
    assert (ctx['area']/'release-intent.json').is_file()
    assert len(list(ctx['area'].glob('release-*-intent.json')))==1
    with pytest.raises(FileExistsError):m.orchestrate(ctx,runner=runner)
    assert len([e for e in events if e[0]=='attempt_release'])==1


@pytest.mark.parametrize('failure',['pending','native_failed','wrong_descriptor','nonfinite_gpu','changed_plan','wrong_gpu_count'])
def test_original_native_prerequisites_are_required_before_any_submission(fixture,failure):
    ctx,events,runner,tasks,native=fixture
    if failure=='pending':native['11']['state']='RUNNING'
    elif failure=='native_failed':native['12']['exit_code']='1:0'
    elif failure=='wrong_gpu_count':native['11']['alloc_tres']='cpu=4,gres/gpu=2'
    else:
        path=m.CASE/'native-11/receipt.json';value=read_json(path)
        if failure=='wrong_descriptor':value['descriptor_sha256']='0'*64
        elif failure=='nonfinite_gpu':value['result']['loss']=float('nan')
        else:
            request=read_json(ctx['descriptor']['canary_request']['path']);atomic_json(Path(request['case'])/'plan.json',{'changed':True})
        if failure=='nonfinite_gpu':path.write_text(json.dumps(value))
        elif failure!='changed_plan':atomic_json(path,value)
    with pytest.raises(ValueError):m.orchestrate(ctx,runner=runner)
    assert not tasks


def test_deadline_before_submission_or_release_preserves_evidence(fixture):
    ctx,events,runner,tasks,_=fixture;ctx['end']=time.time()+100
    with pytest.raises(ValueError,match='deadline'):m.orchestrate(ctx,runner=runner)
    assert not tasks
