"""Finite handoff contracts; synthetic scheduler and disposable state only."""
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from datetime import datetime,timezone
import ast
import gzip
import importlib.util
import json
import os
import shlex
import subprocess
import time

import pytest

spec=importlib.util.spec_from_file_location('finite_external_census_handoff',Path(__file__).with_name('handoff-f2-external-census-v1-20261023.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


@pytest.fixture
def actor(tmp_path,monkeypatch):
    root=tmp_path/'root';area=root/'maintenance/handoff/native-999';area.mkdir(parents=True)
    (root/'plans').mkdir();(root/'state').mkdir();q2=root/'maintenance/q2';q2.mkdir()
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'Q2_SUPPORT',q2)
    monkeypatch.setattr(m,'SUPPORT',area.parent);monkeypatch.setattr(m,'FRAMEWORK',root/'framework')
    monkeypatch.delenv('SBATCH_EXCLUDE',raising=False)
    monkeypatch.setattr(m,'source_identity',lambda p:{'sha256':m.PRIMARY_SHA if Path(p)==m.PRIMARY else m.SOURCE})
    helper=area.parent/m.SELF_NAME;helper.write_text('synthetic frozen helper')
    case=root/'maintenance/campaign/agent-final-f64-v1';case.mkdir(parents=True)
    worker=root/'maintenance/worker-output';worker.mkdir();(worker/'receipt.json').write_bytes(b'protected scientific worker evidence')
    worker_descriptor=root/'maintenance/worker-descriptor.json';worker_descriptor.write_text('immutable worker descriptor')
    wd=m.locator(worker_descriptor);intent={'intent_id':'training-0001','intent_sha256':'a'*64,'kind':'training'}
    wdir=root/'plans/af-f2-training-0001';wdir.mkdir();wscript=wdir/'worker.sbatch'
    wcommand=shlex.join(['python','worker.py','--descriptor',wd['path'],'--descriptor-sha256',wd['sha256']]);wscript.write_text(wcommand)
    wtask={'name':'af-f2-training-0001','script':str(wscript),'script_sha256':m.canonical_hash(wscript.read_text()),'command':wcommand}
    wplan={'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'campaign_plan_sha256':'b'*64,
        'intent_sha256':intent['intent_sha256'],'worker_descriptor_sha256':wd['sha256'],'tasks':[wtask]}
    wp=wdir/'plan.json';wp.write_text(json.dumps(wplan));wl=root/'state/af-f2-training-0001-submission.json'
    wl.write_text(json.dumps({'source_sha256':m.SOURCE,'plan_sha256':m.canonical_hash(wplan),
        'tasks':{wtask['name']:{'job_id':'201','state':'submitted','script_sha256':wtask['script_sha256']}}}))
    journal=case/'submission-training-0001.json';jvalue={'status':'released','job_id':'201','intent':intent,
        'plan_path':str(wp),'plan_sha256':m.digest(wp),'ledger_path':str(wl),'scientific_result':False,'admission_authority':False}
    journal.write_text(json.dumps(jvalue));campaign_plan={'plan_sha256':'b'*64};state={'plan_sha256':'b'*64,'state_sha256':'c'*64,'revision':1,
        'jobs':{'training-0001':{'intent':intent,'job_id':'201'}}}
    (case/'campaign-plan.json').write_text(json.dumps(campaign_plan));(case/'state.json.gz').write_bytes(gzip.compress(json.dumps(state).encode(),mtime=0))
    (case/'state-binding.json').write_text(json.dumps({'state_file_sha256':m.digest(case/'state.json.gz'),'state_sha256':'c'*64,
        'campaign_plan_sha256':'b'*64,'revision':1}))
    descriptor=area.parent/'ACT-descriptor.json';descriptor_value={'agent_campaign_case':str(case),'worker_descriptor':wd}
    descriptor.write_text(json.dumps(descriptor_value))
    base_source=area.parent/'run-af-agent-controller-registry-v6-20261022.py';base_source.write_text('frozen base fixture')
    old_dir=root/'plans'/m.OLD_PHASE;old_dir.mkdir();old_plan_path=old_dir/'plan.json';old_ledger_path=root/'state'/(m.OLD_PHASE+'-submission.json')
    jobs=['101','102','103','104'];tasks=[];rows={};by_job={};states={};held={};overrides={};events=[];lock=[False]
    for index,job in enumerate(jobs):
        name='af-f2-registry-r2-v1-controller-'+str(index);script=old_dir/(name+'.sbatch')
        command=shlex.join(['python','-s','-u',str(base_source),'--descriptor',str(descriptor),'--descriptor-sha256',m.digest(descriptor),'--segment',str(index)])
        script.write_text(command)
        task={'name':name,'command':command,'script':str(script),'script_sha256':m.canonical_hash(script.read_text()),
            'resources':{'cpus':1,'gpu':False,'hours':96,'memory':'16G','array':None},
            'depends':[] if index==0 else [tasks[-1]['name']],'dependency_kind':'afterok' if index==0 else 'afterany',
            'external_dependencies':['888'] if index==0 else []}
        tasks.append(task);rows[name]={'job_id':job,'state':'submitted','script_sha256':task['script_sha256']};by_job[job]=task
        states[job]='RUNNING' if index==0 else 'PENDING';held[job]=False
    old_plan={'phase':m.OLD_PHASE,'release_identity':{'sha256':m.SOURCE},'config_sha256':m.CONFIG,'tasks':tasks,
        'descriptor_path':str(descriptor),'descriptor_sha256':m.digest(descriptor)}
    old_ledger={'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,'plan_sha256':m.canonical_hash(old_plan),'tasks':rows}
    old_plan_path.write_text(json.dumps(old_plan));old_ledger_path.write_text(json.dumps(old_ledger))
    config={'paths':{'root':str(root),'cpu_account':'def-ptewarie_cpu','gpu_account':'def-ptewarie_gpu','environment':'/synthetic/environment'}}
    @contextmanager
    def registration_lock(*args,**kwargs):
        assert kwargs['role']=='controller-start' and not lock[0];lock[0]=True;events.append(('lock',))
        try:yield
        finally:lock[0]=False;events.append(('unlock',))
    def scheduler(job,path):
        task=by_job[job];memory=task['resources']['memory'];index=list(by_job).index(job)
        value={'JobId':job,'UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobName':'hhw-'+task['name'],
            'Command':task['script'],'JobState':states[job],'Reason':'JobHeldUser' if held[job] else 'Dependency',
            'NumCPUs':'1','NumNodes':'1','MinMemoryNode':memory,'TimeLimit':'4-00:00:00',
            'ReqTRES':'cpu=1,mem='+memory+',node=1','AllocTRES':'',
            'Dependency':('afterok:999(unfulfilled)' if job=='1000' else 'afterany:'+str(int(job)-1)+'(unfulfilled)')}
        value.update(overrides.get(job,{}));value.update({'Socks/Node':'4','NtasksPerN:B:S:C':'0:0:*:*'})
        return subprocess.CompletedProcess([],0,' '.join(key+'='+item for key,item in value.items()),'')
    def native_reader(selected,rosters,**kwargs):
        assert kwargs=={'owner':'pwa209','account':'def-ptewarie_cpu'}
        return {job:{'state':states[job],'job_id_raw':job,'user':'pwa209','account':'def-ptewarie_cpu','exit_code':'0:0',
            'cpus':1 if states[job]=='RUNNING' else 0,'elapsed_seconds':1 if states[job]=='RUNNING' else 0} for job in selected}
    def admission(label,**kwargs):assert lock[0];events.append(('admission',label))
    counter=[1000]
    def runner(command,**kwargs):
        events.append(tuple(command))
        if command[:2]==['scontrol','hold']:
            assert lock[0] and command[2] in jobs;held[command[2]]=True
        elif command[0]=='scancel':
            job=command[1];assert job in by_job
            if job in jobs:assert lock[0]
            states[job]='CANCELLED'
        elif command[:2]==['scontrol','release']:
            assert not lock[0] and command[2] not in jobs and held[command[2]];held[command[2]]=False
        elif command[0]=='sbatch':
            assert lock[0] and '--hold' in command
            plan=json.loads((root/'plans'/m.PHASE/'plan.json').read_text());task=next(t for t in plan['tasks'] if t['script']==command[-1])
            job=str(counter[0]);counter[0]+=1;by_job[job]=task;states[job]='PENDING';held[job]=True
            return subprocess.CompletedProcess(command,0,job+'\n','')
        else:pytest.fail('Unexpected scheduler operation '+str(command))
        return subprocess.CompletedProcess(command,0,'','')
    outer=SimpleNamespace(BASE=base_source,qualification=lambda policy:events.append(('qualification',)))
    c={'job':'999','area':area,'end':time.time()+10000,'config':config,'policy':SimpleNamespace(receipt=lambda:{'sha256':'a'*64}),
        'pins':{str(helper):m.digest(helper)},'base':SimpleNamespace(seconds=lambda value:96*3600 if value=='4-00:00:00' else 0,
            dispatch=SimpleNamespace(_state=lambda plan,state:None),
            submission_journal=lambda context,name:case/('submission-'+name+'.json'),native_namespace=lambda context,name:'af-f2-'+name),
        'provider':SimpleNamespace(memory=lambda value:int(value[:-1])*1024**(3 if value[-1]=='G' else 2)),
        'outer':outer,'registry':{'registration_lock':registration_lock},'scheduler':scheduler,'native_reader':native_reader,'admission':admission,
        'activation':{'plan':{'path':str(old_plan_path),'sha256':m.digest(old_plan_path),'ledger_path':str(old_ledger_path)},'jobs':jobs},
        'activation_job':'888','base_descriptor':m.locator(descriptor),'descriptor':descriptor_value,'journal_context':{},'old_jobs':[],
        'outer_policy':{'fixture':'qualified outer policy'},'manifest_binding':{'path':'synthetic-manifest','sha256':'b'*64},
        'qualification':{'path':'synthetic-q2','sha256':'c'*64,'job_id':'777'},'activation_binding':{'path':'synthetic-ACT','sha256':'d'*64,'job_id':'888'},
        'selftest_proof':{'status':'complete_native_handoff_contracts_completion_pending'}}
    m.exclusive(area/'native-handoff-qualification.json',c['selftest_proof'])
    return SimpleNamespace(c=c,root=root,area=area,old_plan=old_plan,old_ledger=old_ledger,old_plan_path=old_plan_path,
        old_ledger_path=old_ledger_path,jobs=jobs,by_job=by_job,states=states,held=held,overrides=overrides,events=events,
        lock=lock,runner=runner,case=case,worker=worker,counter=counter,helper=helper,journal=journal,jvalue=jvalue,
        state=state,worker_plan=wplan,worker_plan_path=wp,worker_ledger_path=wl)


def test_real_submit_ledger_holds_all_new_jobs_and_release_is_outside_permanent_lock(actor):
    f=actor;before={p.name:p.read_bytes() for p in f.case.iterdir()};worker=(f.worker/'receipt.json').read_bytes()
    result=m.orchestrate(f.c,runner=f.runner)
    assert result['old_jobs']==f.jobs and result['new_jobs']==['1000','1001','1002','1003']
    assert all(f.states[job]=='CANCELLED' for job in f.jobs) and all(not f.held[job] for job in result['new_jobs'])
    assert {p.name:p.read_bytes() for p in f.case.iterdir()}==before and (f.worker/'receipt.json').read_bytes()==worker
    assert result['campaign_state_changed'] is False and result['worker_jobs_changed'] is False and result['journals_changed'] is False
    mutations=[event for event in f.events if event and event[0] in {'scontrol','scancel','sbatch'}]
    assert mutations[:3]==[('scontrol','hold',job) for job in f.jobs[1:]]
    assert mutations[3:7]==[('scancel',job) for job in f.jobs]
    assert all(event[0]=='sbatch' and '--hold' in event for event in mutations[7:11])
    assert mutations[-4:]==[('scontrol','release',job) for job in result['new_jobs']]
    plan=json.loads((f.root/'plans'/m.PHASE/'plan.json').read_text());ledger=json.loads((f.root/'state'/(m.PHASE+'-submission.json')).read_text())
    assert all(t['resources']=={'cpus':1,'gpu':False,'hours':96,'memory':'64G','array':None} for t in plan['tasks'])
    assert plan['tasks'][0]['external_dependencies']==['999'] and ledger['tasks'][plan['tasks'][0]['name']]['issued_native_dependencies']==['999']
    assert all(t['dependency_kind']=='afterany' for t in plan['tasks'][1:])
    assert all(m.digest(p)==sha for p,sha in result['protected_state_sha256'].items())


def test_no_selftest_proof_means_no_scheduler_action(actor):
    actor.c['selftest_proof']={}
    with pytest.raises(ValueError,match='selftests'):m.orchestrate(actor.c,runner=actor.runner)
    assert not any(event and event[0] in {'scontrol','scancel','sbatch'} for event in actor.events)


@pytest.mark.parametrize('field,value',[('UserId','foreign(1)'),('Account','foreign'),('Command','/foreign/script'),
    ('JobName','hhw-worker'),('NumCPUs','4'),('MinMemoryNode','64G'),('TimeLimit','2:00:00'),('ReqTRES','cpu=1,gres/gpu=1')])
def test_wrong_old_owner_script_or_resources_refused_before_mutations(actor,field,value):
    actor.overrides['101']={field:value}
    with pytest.raises(ValueError):m.orchestrate(actor.c,runner=actor.runner)
    assert not any(event and event[0] in {'scontrol','scancel','sbatch'} for event in actor.events)


@pytest.mark.parametrize('change',['phase','source','ledger_source','descriptor','jobs','script','dependency','worker_command'])
def test_only_the_exact_four_ACT_created_controller_phase_can_be_retired(actor,change):
    f=actor
    if change=='phase':f.old_plan['phase']='unrelated-workers'
    if change=='source':f.old_plan['release_identity']['sha256']='0'*64
    if change=='ledger_source':f.old_ledger['source_sha256']='0'*64
    if change=='descriptor':f.old_plan['descriptor_sha256']='0'*64
    if change=='jobs':f.c['activation']['jobs'][0]='9000'
    if change=='script':Path(f.old_plan['tasks'][0]['script']).write_text('changed')
    if change=='dependency':f.old_plan['tasks'][1]['depends']=[]
    if change=='worker_command':f.old_plan['tasks'][0]['command']='python worker.py'
    f.old_plan_path.write_text(json.dumps(f.old_plan));f.old_ledger['plan_sha256']=m.canonical_hash(f.old_plan);f.old_ledger_path.write_text(json.dumps(f.old_ledger))
    f.c['activation']['plan']['sha256']=m.digest(f.old_plan_path)
    with pytest.raises((ValueError,KeyError)):m.orchestrate(f.c,runner=f.runner)
    assert not any(event and event[0] in {'scontrol','scancel','sbatch'} for event in f.events)


def test_queue_delay_can_advance_old_chain_without_cancelling_terminal_jobs(actor):
    f=actor;f.states['101']='COMPLETED';f.states['102']='RUNNING'
    result=m.orchestrate(f.c,runner=f.runner)
    assert result['new_jobs']==['1000','1001','1002','1003']
    assert ('scancel','101') not in f.events and f.states['101']=='COMPLETED'
    assert [('scontrol','hold',job) for job in ['103','104']]==[e for e in f.events if e[:2]==('scontrol','hold')]


def test_conflicting_active_old_controllers_refused_before_any_hold(actor):
    actor.states['102']='RUNNING'
    with pytest.raises(ValueError,match='conflicting active'):m.orchestrate(actor.c,runner=actor.runner)
    assert not any(event and event[0] in {'scontrol','scancel','sbatch'} for event in actor.events)


def test_protected_campaign_state_change_stops_before_new_registration(actor):
    f=actor
    def runner(command,**kwargs):
        result=f.runner(command,**kwargs)
        if command==['scancel','101']:(f.case/'state.json.gz').write_bytes(b'changed concurrently')
        return result
    with pytest.raises(ValueError,match='Protected campaign'):m.orchestrate(f.c,runner=runner)
    assert not any(e and e[0]=='sbatch' for e in f.events)


def test_failure_after_first_cancel_never_automatically_releases_old_holds(actor):
    f=actor
    def runner(command,**kwargs):
        if command==['scancel','102']:raise subprocess.TimeoutExpired(command,30)
        return f.runner(command,**kwargs)
    with pytest.raises(subprocess.TimeoutExpired):m.orchestrate(f.c,runner=runner)
    assert f.states['101']=='CANCELLED' and all(f.held[job] for job in f.jobs[1:])
    assert not any(e[:2]==('scontrol','release') for e in f.events)
    assert (f.area/'cancel-old-102-intent.json').exists() and (f.area/'handoff-failure.json').exists()


def test_ambiguous_new_sbatch_cleans_only_known_positive_still_held_jobs(actor):
    f=actor;submitted=[0]
    def runner(command,**kwargs):
        if command[0]=='sbatch':
            submitted[0]+=1
            if submitted[0]==2:
                f.runner(command,**kwargs)
                raise subprocess.TimeoutExpired(command,30)
        return f.runner(command,**kwargs)
    with pytest.raises(subprocess.TimeoutExpired):m.orchestrate(f.c,runner=runner)
    assert f.states['1000']=='CANCELLED' and f.states['1001']=='PENDING' and f.held['1001']
    assert ('scancel','1001') not in f.events
    ledger=json.loads((f.root/'state'/(m.PHASE+'-submission.json')).read_text())
    assert list(ledger['tasks'].values())[1]['state']=='submitting'
    assert not any(e[:2]==('scontrol','release') for e in f.events)


def test_ambiguous_release_is_not_replayed_or_cancelled_as_if_still_held(actor):
    f=actor
    def runner(command,**kwargs):
        if command==['scontrol','release','1000']:
            f.runner(command,**kwargs);return subprocess.CompletedProcess(command,1,'applied but response failed','ambiguous')
        return f.runner(command,**kwargs)
    with pytest.raises(subprocess.CalledProcessError):m.orchestrate(f.c,runner=runner)
    assert not f.held['1000'] and f.states['1000']=='PENDING' and ('scancel','1000') not in f.events
    assert all(f.states[job]=='CANCELLED' for job in ['1001','1002','1003'])
    assert f.events.count(('scontrol','release','1000'))==1


def test_unknown_new_native_ownership_is_never_cancelled_during_cleanup(actor):
    f=actor
    def runner(command,**kwargs):
        result=f.runner(command,**kwargs)
        if command[0]=='sbatch' and result.stdout.strip()=='1003':f.overrides['1000']={'UserId':'foreign(1)'}
        return result
    with pytest.raises(ValueError,match='owned'):m.orchestrate(f.c,runner=runner)
    assert ('scancel','1000') not in f.events and f.states['1000']=='PENDING'
    assert all(f.states[job]=='CANCELLED' for job in ['1001','1002','1003'])


def test_source_change_blocks_next_scheduler_mutation_and_preserves_intents(actor):
    f=actor
    def runner(command,**kwargs):
        result=f.runner(command,**kwargs)
        if command==['scontrol','hold','102']:f.helper.write_bytes(b'changed source')
        return result
    with pytest.raises(ValueError,match='changed'):m.orchestrate(f.c,runner=runner)
    assert ('scontrol','hold','103') not in f.events and not any(e and e[0]=='scancel' for e in f.events)


def test_scheduler_mutation_intent_cannot_be_replayed(actor):
    f=actor;m.mutation(f.c,'hold-fixture',['scontrol','hold','102'],runner=lambda command,**kwargs:subprocess.CompletedProcess(command,0,'',''))
    calls=[]
    with pytest.raises(ValueError,match='replay'):
        m.mutation(f.c,'hold-fixture',['scontrol','hold','102'],runner=lambda *a,**kw:calls.append(a))
    assert calls==[]


def test_old_jobs_cannot_be_accepted_as_new_held_jobs(actor):
    f=actor;f.c['old_jobs']=f.jobs
    with pytest.raises(ValueError,match='Only new'):m.new_held(f.c,f.old_plan['tasks'][0],'101')


def test_final_proof_timeout_never_registers_or_releases_new_controllers(actor,monkeypatch):
    f=actor;original=f.c['native_reader']
    def reader(ids,*args,**kwargs):
        value=original(ids,*args,**kwargs)
        for job,row in value.items():
            if f.states[job]=='CANCELLED':row['state']='RUNNING'
        return value
    f.c['native_reader']=reader;quiesce=m.quiesce_old
    monkeypatch.setattr(m,'quiesce_old',lambda c,plan,jobs,**kwargs:quiesce(c,plan,jobs,sleeper=lambda seconds:None,**kwargs))
    with pytest.raises(ValueError,match='not positively terminal'):m.orchestrate(f.c,runner=f.runner)
    assert not any(e and e[0]=='sbatch' for e in f.events)


@pytest.fixture
def own_native(actor,monkeypatch):
    end=20000.;monkeypatch.setenv('SLURM_JOB_END_TIME',str(end))
    fields={'JobId':'999','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING','NumCPUs':'4',
        'NumNodes':'1','MinMemoryNode':'16G','TimeLimit':'02:00:00','Command':'/synthetic/handoff.sbatch','JobName':'hhw-handoff',
        'ReqTRES':'cpu=4,mem=16G,node=1','AllocTRES':'cpu=4,mem=16G,node=1',
        'EndTime':datetime.fromtimestamp(end,timezone.utc).isoformat().removesuffix('+00:00')}
    def reader(job,path):return subprocess.CompletedProcess([],0,' '.join(key+'='+value for key,value in fields.items()),'')
    return fields,reader


def test_exact_actual_native_handoff_context_is_four_cpu_sixteen_g_two_hours(actor,own_native):
    fields,reader=own_native;value,end=m.native_allocation('999','handoff',fields['Command'],reader,actor.area,wall=lambda:1000.)
    assert value==fields and end==20000.


@pytest.mark.parametrize('key,value',[('JobId','998'),('UserId','foreign(1)'),('Account','foreign'),('JobState','PENDING'),
    ('NumCPUs','1'),('NumNodes','2'),('MinMemoryNode','64G'),('TimeLimit','01:00:00'),('Command','/foreign'),
    ('ReqTRES','cpu=4,mem=16G,node=1,gres/gpu=0'),('AllocTRES','cpu=4,mem=64G,node=1'),('ArrayJobId','999')])
def test_foreign_or_wrong_native_handoff_context_refused(actor,own_native,key,value):
    fields,reader=own_native;fields[key]=value
    with pytest.raises(ValueError):m.native_allocation('999','handoff','/synthetic/handoff.sbatch',reader,actor.area,wall=lambda:1000.)


def test_native_parser_preserves_actual_slash_colon_fields_without_swallowing_memory(actor,own_native):
    fields,reader=own_native
    fields['Socks/Node']='4';fields['NtasksPerN:B:S:C']='0:0:*:*'
    value,_=m.native_allocation('999','handoff',fields['Command'],reader,actor.area,wall=lambda:1000.)
    assert value['MinMemoryNode']=='16G' and value['Socks/Node']=='4' and value['NtasksPerN:B:S:C']=='0:0:*:*'
    native=m.native_job(actor.c,actor.old_plan['tasks'][0],'101',memory=16)
    assert native['MinMemoryNode']=='16G' and native['NtasksPerN:B:S:C']=='0:0:*:*'


def test_duplicate_native_fields_are_refused():
    row=subprocess.CompletedProcess([],0,'JobId=999 MinMemoryNode=16G Socks/Node=4 MinMemoryNode=64G','')
    with pytest.raises(ValueError,match='Duplicate'):m.native_fields(row)


@pytest.mark.parametrize('tres',['cpu=2,mem=16G,node=1','cpu=1,mem=64G,node=1','cpu=1,mem=16G,node=2',
    'cpu=1,mem=16G,node=1,cpu=1'])
def test_controller_requested_tres_must_match_actual_scalar_contract(actor,tres):
    actor.overrides['101']={'ReqTRES':tres}
    with pytest.raises(ValueError,match='TRES'):m.orchestrate(actor.c,runner=actor.runner)
    assert not any(e and e[0] in {'scontrol','scancel','sbatch'} for e in actor.events)


@pytest.fixture
def qualification_actor(actor,monkeypatch):
    f=actor;temp=f.root.parent/'allocation-local';temp.mkdir();monkeypatch.setenv('SLURM_TMPDIR',str(temp))
    (m.SUPPORT/m.TEST_NAME).write_text('synthetic immutable handoff contract source')
    (f.area/'native-handoff-qualification.json').unlink()
    original_stat=Path.stat
    def stat(path,*args,**kwargs):
        result=original_stat(path,*args,**kwargs)
        if path==temp:
            values=list(result);values[4]=3164768;return os.stat_result(values)
        return result
    monkeypatch.setattr(Path,'stat',stat)
    f.c['manifest']={'tests':{'count':1,'roster_sha256':m.canonical_hash(['test_contract::test_native'])}}
    f.native_xml='<testsuites tests="1" failures="0" errors="0" skipped="0"><testsuite tests="1" failures="0" errors="0" skipped="0"><testcase classname="work.test_contract" name="test_native"/></testsuite></testsuites>'
    f.qualification_calls=[]
    def runner(command,**kwargs):
        f.qualification_calls.append((command,kwargs));Path(command[command.index('--junitxml')+1]).write_text(f.native_xml)
        return subprocess.CompletedProcess(command,0,'','')
    f.qualification_runner=runner;return f


def test_actual_self_qualification_preserves_inherited_cvmfs_dependencies(qualification_actor,monkeypatch):
    f=qualification_actor;inherited='/cvmfs/qualified-pygments'+os.pathsep+'/cvmfs/qualified-pytest'
    monkeypatch.setenv('PYTHONPATH',inherited);monkeypatch.setenv('PYTEST_ADDOPTS','--dangerous-plugin');monkeypatch.setenv('PYTEST_PLUGINS','external')
    value=m.self_qualification(f.c,runner=f.qualification_runner);command,kwargs=f.qualification_calls[0]
    assert kwargs['env']['PYTHONPATH'].split(os.pathsep)==[str(m.FRAMEWORK/'src'),*inherited.split(os.pathsep)]
    assert kwargs['env']['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1' and 'PYTEST_ADDOPTS' not in kwargs['env'] and 'PYTEST_PLUGINS' not in kwargs['env']
    assert command[:4]==[m.sys.executable,'-s','-m','pytest'] and kwargs['cwd']==m.FRAMEWORK
    assert value['test_counts']=={'tests':1,'failures':0,'errors':0,'skipped':0}
    assert value['q2_coverage_claimed_for_handoff'] is False and value['native_completion_required_before_controllers_start'] is True
    assert f.events==[]


@pytest.mark.parametrize('change',['roster','count','summary','failure','skip','duplicate','unsafe'])
def test_incomplete_native_selftest_evidence_never_allows_scheduler_changes(qualification_actor,change):
    f=qualification_actor
    if change=='roster':f.c['manifest']['tests']['roster_sha256']='0'*64
    if change=='count':f.c['manifest']['tests']['count']=2
    if change=='summary':f.native_xml=f.native_xml.replace('tests="1"','tests="2"')
    if change=='failure':f.native_xml=f.native_xml.replace('/>','><failure/></testcase>')
    if change=='skip':f.native_xml=f.native_xml.replace('/>','><skipped/></testcase>')
    if change=='duplicate':f.native_xml=f.native_xml.replace('</testsuite>','<testcase classname="test_contract" name="test_native"/></testsuite>')
    if change=='unsafe':f.native_xml='<!DOCTYPE dangerous>'+f.native_xml
    with pytest.raises(ValueError):m.self_qualification(f.c,runner=f.qualification_runner)
    assert not (f.area/'native-handoff-qualification.json').exists() and f.events==[]


def test_nonzero_native_selftest_process_never_publishes_qualification(qualification_actor):
    f=qualification_actor
    def runner(command,**kwargs):
        f.qualification_runner(command,**kwargs);return subprocess.CompletedProcess(command,1,'','')
    with pytest.raises(ValueError,match='suite failed'):m.self_qualification(f.c,runner=runner)
    assert not (f.area/'native-handoff-qualification.json').exists() and f.events==[]


def test_outside_lock_worker_release_finishes_before_any_old_controller_mutation(actor,monkeypatch):
    f=actor;f.jvalue['status']='registered_held_verified';f.journal.write_text(json.dumps(f.jvalue));waits=[]
    original=m.await_released_journals;clock=[1000.]
    def sleeper(seconds):
        assert f.lock[0] and not any(e and e[0] in {'scontrol','scancel','sbatch'} for e in f.events)
        waits.append(f.jvalue['status']);clock[0]+=seconds
        f.jvalue['status']='releasing' if len(waits)==1 else 'released';f.journal.write_text(json.dumps(f.jvalue))
    monkeypatch.setattr(m,'await_released_journals',lambda c:original(c,clock=lambda:clock[0],sleeper=sleeper))
    result=m.orchestrate(f.c,runner=f.runner)
    assert waits==['registered_held_verified','releasing'] and result['new_jobs']==['1000','1001','1002','1003']
    assert ('scancel','201') not in f.events and json.loads(f.journal.read_text())['status']=='released'


@pytest.mark.parametrize('status',['before_sbatch','registered_held','unknown','failed'])
def test_partial_or_ambiguous_worker_submission_forbids_old_controller_cancellation(actor,status):
    f=actor;f.jvalue['status']=status;f.journal.write_text(json.dumps(f.jvalue))
    with pytest.raises(ValueError,match='Partial or ambiguous'):m.orchestrate(f.c,runner=f.runner)
    assert not any(e and e[0] in {'scontrol','scancel','sbatch'} for e in f.events)


def test_releasing_worker_timeout_preserves_all_old_controllers(actor,monkeypatch):
    f=actor;f.jvalue['status']='releasing';f.journal.write_text(json.dumps(f.jvalue));clock=[1000.];original=m.await_released_journals
    def sleeper(seconds):clock[0]+=seconds
    monkeypatch.setattr(m,'await_released_journals',lambda c:original(c,clock=lambda:clock[0],sleeper=sleeper))
    with pytest.raises(ValueError,match='pending|wait budget'):m.orchestrate(f.c,runner=f.runner)
    assert not any(e and e[0] in {'scontrol','scancel','sbatch'} for e in f.events)


@pytest.mark.parametrize('change',['job','intent','namespace','ledger','source','descriptor','missing'])
def test_released_worker_journal_requires_source_bound_exact_state_mapping(actor,change):
    f=actor
    if change=='job':f.jvalue['job_id']='9000'
    if change=='intent':f.jvalue['intent']['intent_sha256']='0'*64
    if change=='namespace':f.c['base'].native_namespace=lambda context,name:'af-f2-r2-'+name
    if change=='ledger':
        ledger=json.loads(f.worker_ledger_path.read_text());ledger['tasks']['af-f2-training-0001']['job_id']='9000';f.worker_ledger_path.write_text(json.dumps(ledger))
    if change in {'source','descriptor'}:
        f.worker_plan['release_identity' if change=='source' else 'worker_descriptor_sha256']=({'sha256':'0'*64} if change=='source' else '0'*64)
        f.worker_plan_path.write_text(json.dumps(f.worker_plan));f.jvalue['plan_sha256']=m.digest(f.worker_plan_path)
    f.journal.write_text(json.dumps(f.jvalue))
    if change=='missing':f.journal.unlink()
    with pytest.raises(ValueError):m.orchestrate(f.c,runner=f.runner)
    assert not any(e and e[0] in {'scontrol','scancel','sbatch'} for e in f.events)


@pytest.mark.parametrize('change',['state','journal','new_journal'])
def test_campaign_or_flat_journal_mutation_after_old_cancel_prevents_registration(actor,change):
    f=actor
    def runner(command,**kwargs):
        result=f.runner(command,**kwargs)
        if command==['scancel','104']:
            if change=='state':(f.case/'state-binding.json').write_text('{}')
            if change=='journal':f.journal.write_text('{}')
            if change=='new_journal':(f.case/'submission-unexpected.json').write_text('{}')
        return result
    with pytest.raises(ValueError,match='Protected|inventory'):m.orchestrate(f.c,runner=runner)
    assert not any(e and e[0]=='sbatch' for e in f.events)


@pytest.mark.parametrize('artifact',['plan','script','ledger','descriptor'])
def test_final_registered_artifacts_are_pinned_between_unlock_and_each_release(actor,artifact):
    f=actor;original=f.c['registry']['registration_lock']
    @contextmanager
    def lock(*args,**kwargs):
        with original(*args,**kwargs):yield
        plan_path=f.root/'plans'/m.PHASE/'plan.json';plan=json.loads(plan_path.read_text())
        selected={'plan':plan_path,'script':Path(plan['tasks'][0]['script']),
            'ledger':f.root/'state'/(m.PHASE+'-submission.json'),'descriptor':f.area/'external-census-policy.json'}[artifact]
        selected.write_bytes(selected.read_bytes()+b' ')
    f.c['registry']['registration_lock']=lock
    with pytest.raises(ValueError,match='changed'):m.orchestrate(f.c,runner=f.runner)
    assert not any(e[:2]==('scontrol','release') for e in f.events) and not (f.area/'receipt.json').exists()


def test_fresh_native_dependency_mutation_is_refused_at_release_and_cleanup(actor):
    f=actor;original=f.c['registry']['registration_lock']
    @contextmanager
    def lock(*args,**kwargs):
        with original(*args,**kwargs):yield
        f.overrides['1000']={'Dependency':'afterok:998(unfulfilled)'}
    f.c['registry']['registration_lock']=lock
    with pytest.raises(ValueError,match='dependency'):m.orchestrate(f.c,runner=f.runner)
    assert ('scancel','1000') not in f.events and not any(e[:2]==('scontrol','release') for e in f.events)


@pytest.mark.parametrize('changed',['source','journal'])
def test_final_check_after_last_release_prevents_success_publication_on_change(actor,changed):
    f=actor
    def runner(command,**kwargs):
        result=f.runner(command,**kwargs)
        if command==['scontrol','release','1003']:
            (f.helper if changed=='source' else f.journal).write_bytes(b'changed after last release')
        return result
    with pytest.raises(ValueError,match='changed'):m.orchestrate(f.c,runner=runner)
    assert not (f.area/'receipt.json').exists() and (f.area/'handoff-failure.json').exists()
    assert all(not f.held[job] for job in ['1000','1001','1002','1003'])
    assert not any(e[0]=='scancel' and e[1] in {'1000','1001','1002','1003'} for e in f.events if e)


@pytest.fixture
def manifest_actor(actor,monkeypatch):
    f=actor;test=m.SUPPORT/m.TEST_NAME;test.write_text('separately frozen contract source')
    files={}
    for name in [m.OUTER_NAME,m.PROVIDER_NAME,*['private-'+str(n)+'.py' for n in range(12)]]:
        path=m.Q2_SUPPORT/name;path.write_text('frozen private source '+name);files[name]=m.digest(path)
    qmanifest={'kind':'streaming-census-operational-source-manifest-v2','files':files,'suites':{str(n):{} for n in range(13)}}
    qpath=m.Q2_SUPPORT/'source-manifest.json';qpath.write_text(json.dumps(qmanifest));monkeypatch.setattr(m,'__file__',str(f.helper))
    binding={'plan_path':'explicit-plan','ledger_path':'explicit-ledger','task_name':'explicit-task','script_path':'explicit-script'}
    value={'kind':'f2-external-census-handoff-source-manifest-v1','files':{m.SELF_NAME:m.digest(f.helper),m.TEST_NAME:m.digest(test)},
        'tests':{'count':1,'roster_sha256':'a'*64},'q2_source_manifest':m.locator(qpath),'q2_registration':dict(binding),
        'activation_source':{'path':str(m.ACT_SOURCE),'sha256':m.ACT_SHA},'activation_registration':dict(binding),
        'provider_registration':{'plan_path':'finite-provider-plan','ledger_path':'finite-provider-ledger'}}
    f.manifest_value=value;f.qmanifest=qmanifest;f.qmanifest_path=qpath;f.manifest_path=m.SUPPORT/'source-manifest.json';return f


def test_handoff_manifest_binds_own_sources_and_all_fourteen_Q_sources(manifest_actor):
    f=manifest_actor;f.manifest_path.write_text(json.dumps(f.manifest_value))
    value,qmanifest=m.manifest(f.manifest_path,m.digest(f.manifest_path))
    assert value==f.manifest_value and qmanifest==f.qmanifest and len(qmanifest['files'])==14


@pytest.mark.parametrize('change',['self_sha','q_sha','private_bytes','q_closure','boolean_count','extra_file','extra_key','activation'])
def test_handoff_manifest_refuses_inexact_or_unbound_source_closure(manifest_actor,change):
    f=manifest_actor;value=f.manifest_value
    if change=='self_sha':value['files'][m.SELF_NAME]=None
    if change=='q_sha':value['q2_source_manifest']['sha256']=None
    if change=='private_bytes':(m.Q2_SUPPORT/m.OUTER_NAME).write_text('tampered source')
    if change=='q_closure':
        f.qmanifest['suites'].pop('0');f.qmanifest_path.write_text(json.dumps(f.qmanifest));value['q2_source_manifest']=m.locator(f.qmanifest_path)
    if change=='boolean_count':value['tests']['count']=True
    if change=='extra_file':value['files']['unqualified.py']='a'*64
    if change=='extra_key':value['allow_fallback']=True
    if change=='activation':value['activation_source']['sha256']='0'*64
    f.manifest_path.write_text(json.dumps(value))
    with pytest.raises(ValueError):m.manifest(f.manifest_path,m.digest(f.manifest_path))
    assert not any(e and e[0] in {'scontrol','scancel','sbatch'} for e in f.events)


def test_registered_producer_pair_binds_config_source_script_and_actual_native_id(actor):
    f=actor;task=f.old_plan['tasks'][0];binding={'plan_path':str(f.old_plan_path),'ledger_path':str(f.old_ledger_path),
        'task_name':task['name'],'script_path':task['script']}
    actual,job,proof=m.registration_record(binding)
    assert actual==task and job=='101' and proof['native_job_id']=='101'
    f.old_ledger['config_sha256']='0'*64;f.old_ledger_path.write_text(json.dumps(f.old_ledger))
    with pytest.raises(ValueError,match='source/config'):m.registration_record(binding)


def test_main_selftests_and_actual_producer_refresh_precede_orchestration():
    text=Path(m.__file__).read_text();tree=ast.parse(text);main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main');body=ast.get_source_segment(text,main)
    assert body.index('self_qualification(c)')<body.index("admission('handoff-finalized-producer-accounting'")<body.index('observed=repair._repair_native_states')<body.index('result=orchestrate(c)')
    assert "resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=2" in body
    assert "packet(descriptor_binding['path'],descriptor_binding['sha256'])" in body
    assert not any(isinstance(n,ast.Attribute) and n.attr in {'unlink','save_state','dispatch_tick','rmtree'} for n in ast.walk(tree))
    assert not any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='replace'
        and isinstance(n.func.value,ast.Name) and n.func.value.id=='os' for n in ast.walk(tree))
    assert m.Q2_SUPPORT.name=='study-af-rolling-census-support-v3-20261023'
    assert m.Q2_PATH.parent.name=='study-af-rolling-census-qualification-v4-20261023'
