"""Primary handoff contracts with synthetic native rows and disposable files."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import ast
import importlib.util
import json
import os
import shlex
import subprocess
import time

import pytest

spec=importlib.util.spec_from_file_location('primary_external_census_handoff',Path(__file__).with_name('handoff-primary-external-census-v1-20261023.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def test_native_parser_preserves_real_slash_colon_scheduler_keys():
    row=subprocess.CompletedProcess([],0,'JobId=999 MinMemoryNode=16G Socks/Node=4 NtasksPerN:B:S:C=0:0:*:* Command=/exact/script','')
    fields=m.native_fields(row)
    assert fields['MinMemoryNode']=='16G' and fields['Socks/Node']=='4'
    assert fields['NtasksPerN:B:S:C']=='0:0:*:*' and fields['Command']=='/exact/script'


@pytest.mark.parametrize('text',['JobId=999 JobId=1000','JobId=999\nJobId=1000',''])
def test_native_parser_refuses_duplicate_or_incomplete_rows(text):
    with pytest.raises(ValueError):m.native_fields(subprocess.CompletedProcess([],0,text,''))


def test_native_read_nonzero_exit_is_not_positive_proof():
    with pytest.raises(subprocess.CalledProcessError):m.native_fields(subprocess.CompletedProcess([],1,'JobId=999','read failure'))


@pytest.fixture
def qualification_actor(tmp_path,monkeypatch):
    root=tmp_path/'root';support=root/'maintenance/handoff';area=support/'native-999';area.mkdir(parents=True)
    temp=tmp_path/'allocation-local';temp.mkdir();framework=root/'framework';framework.mkdir()
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',support);monkeypatch.setattr(m,'FRAMEWORK',framework)
    monkeypatch.setenv('SLURM_TMPDIR',str(temp))
    (support/m.SELF_NAME).write_text('immutable helper fixture');(support/m.TEST_NAME).write_text('immutable test fixture')
    original_stat=Path.stat
    def stat(path,*args,**kwargs):
        result=original_stat(path,*args,**kwargs)
        if path==temp:
            values=list(result);values[4]=3164768;return os.stat_result(values)
        return result
    monkeypatch.setattr(Path,'stat',stat)
    calls=[]
    def environment(framework):
        return {'PYTHONPATH':os.pathsep.join([str(framework/'src'),*os.environ.get('PYTHONPATH','').split(os.pathsep)])}
    c={'area':area,'job':'999','end':time.time()+4000,'primary':SimpleNamespace(environment=environment),
        'manifest':{'tests':{'count':1,'roster_sha256':m.canonical_hash(['test_contract::test_native'])}}}
    f=SimpleNamespace(c=c,area=area,root=root,temp=temp,calls=calls,
        xml='<testsuites tests="1" failures="0" errors="0" skipped="0"><testsuite tests="1" failures="0" errors="0" skipped="0"><testcase classname="work.test_contract" name="test_native"/></testsuite></testsuites>')
    def runner(command,**kwargs):
        calls.append((command,kwargs));Path(command[command.index('--junitxml')+1]).write_text(f.xml)
        return subprocess.CompletedProcess(command,0,'','')
    f.runner=runner;return f


def test_native_selftests_preserve_dependency_paths_and_publish_only_pending_completion(qualification_actor,monkeypatch):
    f=qualification_actor;inherited=os.pathsep.join(['/cvmfs/qualified-pygments','/cvmfs/qualified-pytest'])
    monkeypatch.setenv('PYTHONPATH',inherited);monkeypatch.setenv('PYTEST_ADDOPTS','--unqualified');monkeypatch.setenv('PYTEST_PLUGINS','unqualified')
    proof=m.self_qualification(f.c,runner=f.runner);command,kwargs=f.calls[0]
    assert kwargs['env']['PYTHONPATH'].split(os.pathsep)==[str(m.FRAMEWORK/'src'),*inherited.split(os.pathsep)]
    assert kwargs['env']['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1' and 'PYTEST_ADDOPTS' not in kwargs['env'] and 'PYTEST_PLUGINS' not in kwargs['env']
    assert command[:4]==[m.sys.executable,'-s','-m','pytest'] and kwargs['cwd']==m.FRAMEWORK
    assert proof['status']=='complete_native_handoff_contracts_completion_pending'
    assert proof['primary_q_coverage_claimed_for_handoff'] is False and proof['native_completion_required_before_controllers_start'] is True
    assert proof['test_counts']=={'tests':1,'failures':0,'errors':0,'skipped':0}
    assert all(proof[k] is False for k in m.FALSE_FLAGS)
    assert json.loads((f.area/'native-handoff-qualification.json').read_text())==proof


@pytest.mark.parametrize('change',['roster','count','summary','failure','error','skip','duplicate','unsafe','unnamed'])
def test_incomplete_native_selftest_never_publishes_success(qualification_actor,change):
    f=qualification_actor
    if change=='roster':f.c['manifest']['tests']['roster_sha256']='0'*64
    if change=='count':f.c['manifest']['tests']['count']=2
    if change=='summary':f.xml=f.xml.replace('tests="1"','tests="2"')
    if change in {'failure','error'}:f.xml=f.xml.replace('/>','><'+change+'/></testcase>')
    if change=='skip':f.xml=f.xml.replace('/>','><skipped/></testcase>')
    if change=='duplicate':f.xml=f.xml.replace('</testsuite>','<testcase classname="test_contract" name="test_native"/></testsuite>')
    if change=='unsafe':f.xml='<!DOCTYPE dangerous>'+f.xml
    if change=='unnamed':f.xml=f.xml.replace(' name="test_native"','')
    with pytest.raises(ValueError):m.self_qualification(f.c,runner=f.runner)
    assert not (f.area/'native-handoff-qualification.json').exists() and 'selftest_proof' not in f.c


def test_nonzero_native_selftest_does_not_publish_success(qualification_actor):
    f=qualification_actor
    def runner(command,**kwargs):
        f.runner(command,**kwargs);return subprocess.CompletedProcess(command,1,'','')
    with pytest.raises(ValueError,match='suite failed'):m.self_qualification(f.c,runner=runner)
    assert not (f.area/'native-handoff-qualification.json').exists()


def test_selftests_refuse_non_allocation_namespace_before_launch(qualification_actor,monkeypatch):
    f=qualification_actor;monkeypatch.setenv('SLURM_TMPDIR',str(f.root))
    with pytest.raises(ValueError,match='allocation-local'):m.self_qualification(f.c,runner=f.runner)
    assert not f.calls


@pytest.fixture
def actor(tmp_path,monkeypatch):
    root=tmp_path/'root';area=root/'maintenance/handoff/native-999';area.mkdir(parents=True)
    (root/'plans').mkdir();(root/'state').mkdir();old_framework=root/'d89';old_framework.mkdir()
    framework=root/'4f';framework.mkdir();primary=root/'science';primary.mkdir()
    old_directory=root/'plans/original';old_directory.mkdir();old_path=old_directory/'plan.json';old_ledger=root/'state/original-submission.json'
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',area.parent)
    monkeypatch.setattr(m,'FRAMEWORK',framework);monkeypatch.setattr(m,'OLD_FRAMEWORK',old_framework);monkeypatch.setattr(m,'PRIMARY',primary)
    monkeypatch.setattr(m,'OLD_PLAN',old_path);monkeypatch.setattr(m,'OLD_LEDGER',old_ledger)
    monkeypatch.delenv('SBATCH_EXCLUDE',raising=False)
    monkeypatch.setattr(m.slurm,'completed_native_dependency',lambda *a,**kw:None)
    identities={framework:m.SOURCE,old_framework:m.OLD_SOURCE,primary:m.PRIMARY_SHA}
    monkeypatch.setattr(m,'source_identity',lambda path:{'sha256':identities[Path(path)]})
    helper=area.parent/m.SELF_NAME;helper.write_text('immutable synthetic helper')
    protected=root/'state/primary-science-and-worker-journals.json';protected.write_bytes(b'original scientific state and worker journal evidence')
    tasks=[];ledger_rows={};by_job={};states={};held={};overrides={};events=[];lock=[False];edges={}
    for i,job in enumerate(m.OLD_JOBS):
        name='pipeline-controller-'+str(i);script=old_directory/(name+'.sbatch');command='python -m horsehair.cli pipeline-controller --segment '+str(i)
        script.write_bytes(('#!/bin/bash\n'+command+'\n').encode())
        task={'name':name,'command':command,'script':str(script),'script_sha256':m.canonical_hash(script.read_text()),
            'resources':{'cpus':1,'memory':'8G','hours':144,'gpu':False},
            'depends':[] if i==0 else [tasks[-1]['name']],'dependency_kind':'afterok' if i==0 else 'afterany'}
        tasks.append(task);by_job[job]=task;states[job]='FAILED' if i==0 else 'PENDING';held[job]=False
        ledger_rows[name]={'job_id':job,'state':'submitted','script_sha256':task['script_sha256'],'command':['sbatch',str(script)]}
    plan={'root':str(root),'release_identity':{'sha256':m.PRIMARY_SHA},'config_sha256':m.CONFIG,'tasks':tasks}
    ledger={'source_sha256':m.PRIMARY_SHA,'config_sha256':m.CONFIG,'plan_sha256':m.canonical_hash(plan),'tasks':ledger_rows}
    old_path.write_text(json.dumps(plan));old_ledger.write_text(json.dumps(ledger))
    monkeypatch.setattr(m,'OLD_PLAN_SHA',m.digest(old_path));monkeypatch.setattr(m,'OLD_LEDGER_SHA',m.digest(old_ledger))
    config={'paths':{'root':str(root),'cpu_account':'def-ptewarie_cpu','gpu_account':'def-ptewarie_gpu','environment':'/qualified/environment'}}
    @contextmanager
    def registration_lock(*args,**kwargs):
        assert kwargs['role']=='controller-start' and not lock[0];lock[0]=True;events.append(('lock',))
        try:yield
        finally:lock[0]=False;events.append(('unlock',))
    def scheduler(job,path):
        task=by_job[job];new=job not in m.OLD_JOBS
        value={'JobId':job,'UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobName':'hhw-'+task['name'],
            'Command':task['script'],'WorkDir':str(old_framework.resolve()),'JobState':states[job],
            'Reason':'JobHeldUser' if held[job] else 'Dependency','Priority':'0' if held[job] else '100',
            'RunTime':'00:00:00','StartTime':'Unknown','NodeList':'(null)','AllocTRES':'(null)',
            'Restarts':'0','Requeue':'0','ExitCode':'0:0','NumCPUs':'1','NumNodes':'1',
            'MinMemoryNode':'64G' if new else '8G','TimeLimit':'3-00:00:00' if new else '5-22:00:00',
            'ReqTRES':'cpu=1,mem='+('64G' if new else '8G')+',node=1',
            'Dependency':edges.get(job,'(null)'),'Socks/Node':'4','NtasksPerN:B:S:C':'0:0:*:*'}
        value.update(overrides.get(job,{}));events.append(('read',job))
        return subprocess.CompletedProcess([],0,' '.join(k+'='+v for k,v in value.items()),'')
    def native_reader(selected,rosters,**kwargs):
        assert kwargs=={'owner':'pwa209','account':'def-ptewarie_cpu'}
        result={job:{'job_id_raw':job,'user':'pwa209','account':'def-ptewarie_cpu','state':states[job],
            'cpus':1 if job==m.OLD_JOBS[0] else 0,'elapsed_seconds':1200 if job==m.OLD_JOBS[0] else 0,
            'unallocated_metadata':job!=m.OLD_JOBS[0],'alloc_tres':'','start':'Unknown','node_list':'None assigned'} for job in selected}
        return result
    states[m.FIRST_PROVIDER]='RUNNING'
    counter=[1000]
    def runner(command,**kwargs):
        events.append(tuple(command))
        if command[0]=='sbatch':
            assert lock[0] and '--hold' in command
            current=json.loads((root/'plans'/m.PHASE/'plan.json').read_text());task=next(t for t in current['tasks'] if t['script']==command[-1])
            job=str(counter[0]);counter[0]+=1;by_job[job]=task;states[job]='PENDING';held[job]=True
            dep=next((arg.split('=',1)[1] for arg in command if arg.startswith('--dependency=')),'(null)')
            kind,sep,tail=dep.partition(':');edges[job]=kind+':'+':'.join(x+'(unfulfilled)' for x in tail.split(':')) if sep else dep
            return subprocess.CompletedProcess(command,0,job+'\n','')
        if command[0]=='scancel':
            assert lock[0] and command[1:4]==['--ctld','--state=PENDING','--user=pwa209']
            job=command[-1];assert job in m.OLD_JOBS[1:] and states[job]=='PENDING';states[job]='CANCELLED'
        elif command[:2]==['scontrol','release']:
            job=command[2];assert not lock[0] and job not in m.OLD_JOBS and held[job]
            assert all(states[j]=='CANCELLED' for j in m.OLD_JOBS[1:]);held[job]=False
        else:pytest.fail('Unexpected scheduler mutation '+str(command))
        return subprocess.CompletedProcess(command,0,'','')
    def admission(label,**kwargs):
        assert lock[0];events.append(('admission',label))
    c={'job':'999','area':area,'end':time.time()+10000,'config':config,'pins':{str(helper):m.digest(helper)},
        'registry':{'registration_lock':registration_lock},'scheduler':scheduler,'native_reader':native_reader,'admission':admission,
        'policy':SimpleNamespace(receipt=lambda:{'sha256':'a'*64}),'run_policy':{'qualification':{'path':'qualified-primary','sha256':'b'*64,'job_id':m.Q_JOB}},
        'qualification':{'path':'qualified-primary','sha256':'b'*64,'job_id':m.Q_JOB},
        'selftest_proof':{'status':'complete_native_handoff_contracts_completion_pending'}}
    m.exclusive(area/'native-handoff-qualification.json',c['selftest_proof'])
    return SimpleNamespace(c=c,root=root,area=area,old_path=old_path,old_ledger=old_ledger,plan=plan,ledger=ledger,
        states=states,held=held,overrides=overrides,events=events,lock=lock,runner=runner,by_job=by_job,
        counter=counter,edges=edges,helper=helper,protected=protected)


def mutations(f):return [e for e in f.events if e and e[0] in {'sbatch','scancel','scontrol'}]


def test_real_submit_ledger_new_held_before_old_pending_cancel_reverse_release(actor):
    f=actor;original={f.old_path:f.old_path.read_bytes(),f.old_ledger:f.old_ledger.read_bytes(),f.protected:f.protected.read_bytes()}
    result=m.orchestrate(f.c,runner=f.runner)
    assert result['new_jobs']==['1000','1001','1002','1003']
    rows=mutations(f);assert all(row[0]=='sbatch' and '--hold' in row for row in rows[:4])
    assert [row[-1] for row in rows[4:7]]==list(reversed(m.OLD_JOBS[1:]))
    assert all(row[:4]==('scancel','--ctld','--state=PENDING','--user=pwa209') for row in rows[4:7])
    assert rows[-4:]==[('scontrol','release',j) for j in ['1003','1002','1001','1000']]
    assert f.states[m.OLD_JOBS[0]]=='FAILED' and all(f.states[j]=='CANCELLED' for j in m.OLD_JOBS[1:])
    plan=json.loads((f.root/'plans'/m.PHASE/'plan.json').read_text());ledger=json.loads((f.root/'state'/(m.PHASE+'-submission.json')).read_text())
    assert plan['release_identity']['sha256']==m.OLD_SOURCE and plan['original_primary_lease_limits_retained'] is True
    assert all(t['resources']=={'cpus':1,'memory':'64G','hours':72,'gpu':False,'array':None} for t in plan['tasks'])
    assert plan['tasks'][0]['external_dependencies']==['999',m.FIRST_PROVIDER]
    assert ledger['tasks'][plan['tasks'][0]['name']]['issued_native_dependencies']==['999',m.FIRST_PROVIDER]
    assert all(t['dependency_kind']=='afterany' for t in plan['tasks'][1:])
    assert all(t['primary_external_census']['mode']=='run' and '#SBATCH --hold' in Path(t['script']).read_text() for t in plan['tasks'])
    assert all(path.read_bytes()==raw for path,raw in original.items())
    assert result['workers_or_journals_modified'] is False and result['canonical_plan_ledger_scripts_modified'] is False
    assert result['primary_qualification_coverage_claimed_for_handoff'] is False and result['native_handoff_completion_required_before_new_controllers_start'] is True


def test_selftest_required_before_any_scheduler_action(actor):
    actor.c['selftest_proof']={}
    with pytest.raises(ValueError,match='selftests'):m.orchestrate(actor.c,runner=actor.runner)
    assert not mutations(actor)


@pytest.mark.parametrize('field,value',[('UserId','foreign(1)'),('Account','foreign'),('JobName','hhw-worker'),('Command','/other/script'),
    ('WorkDir','/other/release'),('JobState','RUNNING'),('RunTime','00:00:01'),('StartTime','2026-10-23T11:00:00'),
    ('NodeList','nc12345'),('AllocTRES','cpu=1,mem=8G,node=1'),('Restarts','1'),('Requeue','1'),('ExitCode','1:0'),
    ('NumCPUs','2'),('MinMemoryNode','64G'),('TimeLimit','6-00:00:00'),('ArrayJobId','123'),('ReqTRES','cpu=1,mem=8G,node=1,gres/gpu=1')])
def test_wrong_or_already_started_old_successor_refused_before_scheduler_changes(actor,field,value):
    f=actor;f.overrides[m.OLD_JOBS[1]]={field:value}
    with pytest.raises(ValueError):m.orchestrate(f.c,runner=f.runner)
    assert not mutations(f)


@pytest.mark.parametrize('change',['plan_source','config','ledger_source','job','resources','script','dependency'])
def test_only_exact_original_registered_successors_are_eligible(actor,monkeypatch,change):
    f=actor
    if change=='plan_source':f.plan['release_identity']['sha256']='0'*64
    if change=='config':f.plan['config_sha256']='0'*64
    if change=='ledger_source':f.ledger['source_sha256']='0'*64
    if change=='job':f.ledger['tasks']['pipeline-controller-1']['job_id']='555'
    if change=='resources':f.plan['tasks'][1]['resources']['memory']='64G'
    if change=='script':Path(f.plan['tasks'][1]['script']).write_text('changed')
    if change=='dependency':f.plan['tasks'][1]['depends']=[]
    f.old_path.write_text(json.dumps(f.plan));f.ledger['plan_sha256']=m.canonical_hash(f.plan);f.old_ledger.write_text(json.dumps(f.ledger))
    monkeypatch.setattr(m,'OLD_PLAN_SHA',m.digest(f.old_path));monkeypatch.setattr(m,'OLD_LEDGER_SHA',m.digest(f.old_ledger))
    with pytest.raises(ValueError):m.orchestrate(f.c,runner=f.runner)
    assert not mutations(f)


def test_raced_running_successor_stops_before_next_rpc_and_keeps_new_jobs_held(actor):
    f=actor
    def runner(command,**kwargs):
        result=f.runner(command,**kwargs)
        if command[0]=='scancel':f.overrides[m.OLD_JOBS[2]]={'JobState':'RUNNING'}
        return result
    with pytest.raises(ValueError,match='PENDING'):m.orchestrate(f.c,runner=runner)
    cancellations=[e for e in mutations(f) if e[0]=='scancel']
    assert len(cancellations)==1 and cancellations[0][-1]==m.OLD_JOBS[3]
    assert all(f.held[j] for j in ['1000','1001','1002','1003']) and not any(e[:2]==('scontrol','release') for e in f.events)
    evidence=json.loads((f.area/('retire-old-'+m.OLD_JOBS[2]+'-result.json')).read_text())
    assert evidence['mutation_may_have_occurred'] is False


def test_ambiguous_sbatch_is_never_replayed_or_cleaned_up_blindly(actor):
    f=actor;count=[0]
    def runner(command,**kwargs):
        if command[0]=='sbatch':
            count[0]+=1
            if count[0]==2:
                f.runner(command,**kwargs);raise subprocess.TimeoutExpired(command,30)
        return f.runner(command,**kwargs)
    with pytest.raises(subprocess.TimeoutExpired):m.orchestrate(f.c,runner=runner)
    assert len(mutations(f))==2 and all(e[0]=='sbatch' for e in mutations(f))
    assert f.held['1000'] and f.held['1001'] and all(f.states[j]=='PENDING' for j in m.OLD_JOBS[1:])
    failure=json.loads((f.area/'handoff-failure.json').read_text());assert failure['no_automatic_cleanup_retry_release_or_cancel'] is True


def test_release_ack_timeout_stops_after_once_with_current_handoff_dependency_intact(actor):
    f=actor
    def runner(command,**kwargs):
        result=f.runner(command,**kwargs)
        if command[:2]==['scontrol','release']:raise subprocess.TimeoutExpired(command,30)
        return result
    with pytest.raises(subprocess.TimeoutExpired):m.orchestrate(f.c,runner=runner)
    assert [e for e in mutations(f) if e[:2]==('scontrol','release')]==[('scontrol','release','1003')]
    assert f.held['1000'] and f.held['1001'] and f.held['1002'] and not f.held['1003']
    assert 'afterok:999(unfulfilled)' in f.edges['1000']
    assert not (f.area/'receipt.json').exists()


def test_scheduler_mutation_intent_is_non_replayable_and_fresh_precheck_precedes_rpc(actor):
    f=actor;calls=[]
    def precheck():
        assert (f.area/'test-mutation-intent.json').exists();calls.append('fresh');raise ValueError('raced transition')
    with pytest.raises(ValueError,match='raced'):m.mutation(f.c,'test-mutation',['scancel','unsafe'],runner=lambda *a,**kw:calls.append('rpc'),precheck=precheck)
    assert calls==['fresh']
    evidence=json.loads((f.area/'test-mutation-result.json').read_text());assert evidence['mutation_may_have_occurred'] is False
    with pytest.raises(FileExistsError):m.mutation(f.c,'test-mutation',['scancel','unsafe'],runner=lambda *a,**kw:calls.append('rpc'))
    assert calls==['fresh']


@pytest.mark.parametrize('value',['afterok:999:999','afterok:999(foo)','after:999','afterok:0','afterok:999,afterany:999',''])
def test_malformed_native_dependency_graph_refused(value):
    with pytest.raises(ValueError):m.dependency_edges(value)


@pytest.mark.parametrize('value',['cpu=1,mem=64G,node=1,cpu=1','cpu=1,mem=64G,node=2','cpu=1,mem=64G,node=1,gres/gpu=1','cpu=2,mem=64G,node=1'])
def test_duplicate_gpu_or_wrong_requested_resource_proof_refused(value):
    with pytest.raises(ValueError):m.tres(value,1,'64G')


@pytest.mark.parametrize('field,value',[('UserId','foreign(1)'),('Account','foreign'),('Command','/other/script'),
    ('WorkDir','/other/release'),('JobState','RUNNING'),('Reason','Dependency'),('Priority','100'),('RunTime','00:00:01'),
    ('StartTime','2026-10-23T11:00:00'),('NodeList','nc12345'),('AllocTRES','cpu=1,mem=64G,node=1'),
    ('NumCPUs','2'),('MinMemoryNode','16G'),('TimeLimit','4-00:00:00'),('ArrayTaskId','1'),
    ('Dependency','afterok:999(unfulfilled)'),('Dependency','afterany:999(unfulfilled):6864491(unfulfilled)'),
    ('Dependency','afterok:999(fulfilled):6864491(unfulfilled)')])
def test_new_native_contract_or_dag_mismatch_never_cancels_old_successors(actor,field,value):
    f=actor;f.overrides['1000']={field:value}
    with pytest.raises(ValueError):m.orchestrate(f.c,runner=f.runner)
    assert not any(e[0]=='scancel' or e[:2]==('scontrol','release') for e in mutations(f))
    assert all(f.states[j]=='PENDING' for j in m.OLD_JOBS[1:])


@pytest.mark.parametrize('which',['plan','ledger','script','descriptor'])
def test_final_registered_source_mutation_after_unlock_stops_all_release(actor,which):
    f=actor
    @contextmanager
    def lock(*args,**kwargs):
        f.lock[0]=True
        try:yield
        finally:
            f.lock[0]=False
            directory=f.root/'plans'/m.PHASE
            path={'plan':directory/'plan.json','ledger':f.root/'state'/(m.PHASE+'-submission.json'),
                'script':directory/'primary-external-census-controller-0.sbatch','descriptor':f.area/'run-policy.json'}[which]
            path.write_bytes(path.read_bytes()+b' ')
    f.c['registry']={'registration_lock':lock}
    with pytest.raises(ValueError,match='changed'):m.orchestrate(f.c,runner=f.runner)
    assert all(f.states[j]=='CANCELLED' for j in m.OLD_JOBS[1:])
    assert not any(e[:2]==('scontrol','release') for e in mutations(f)) and not (f.area/'receipt.json').exists()


def test_native_edge_race_after_unlock_stops_release_without_cleanup(actor):
    f=actor
    @contextmanager
    def lock(*args,**kwargs):
        f.lock[0]=True
        try:yield
        finally:f.lock[0]=False;f.overrides['1003']={'Dependency':'afterany:777(unfulfilled)'}
    f.c['registry']={'registration_lock':lock}
    with pytest.raises(ValueError,match='dependencies'):m.orchestrate(f.c,runner=f.runner)
    assert not any(e[:2]==('scontrol','release') for e in mutations(f))
    assert [e for e in mutations(f) if e[0]=='scancel']==[e for e in mutations(f)[4:7]]


def test_final_source_check_runs_after_last_release_ack(actor):
    f=actor
    def runner(command,**kwargs):
        result=f.runner(command,**kwargs)
        if command==['scontrol','release','1000']:f.helper.write_text('mutated after last scheduler ack')
        return result
    with pytest.raises(ValueError,match='changed'):m.orchestrate(f.c,runner=runner)
    assert len([e for e in mutations(f) if e[:2]==('scontrol','release')])==4
    assert not (f.area/'receipt.json').exists()


@pytest.mark.parametrize('field,value',[('state','COMPLETED'),('cpus',1),('elapsed_seconds',1),('unallocated_metadata',False),
    ('alloc_tres','cpu=1'),('start','2026-10-23T11:00:00'),('node_list','nc12345'),('user','foreign'),('job_id_raw','555')])
def test_old_terminal_status_alone_cannot_prove_never_started(actor,field,value):
    f=actor;f.states.update({j:'CANCELLED' for j in m.OLD_JOBS[1:]});reader=f.c['native_reader']
    def native(*args,**kwargs):
        result=reader(*args,**kwargs);result[m.OLD_JOBS[1]][field]=value;return result
    f.c['native_reader']=native
    with pytest.raises(ValueError):m.terminal_old(f.c)


def test_original_first_controller_must_already_be_owned_terminal(actor):
    f=actor;f.states[m.OLD_JOBS[0]]='RUNNING'
    with pytest.raises(ValueError,match='terminal'):m.orchestrate(f.c,runner=f.runner)
    assert not mutations(f)


@pytest.fixture
def own_native(actor,monkeypatch):
    from datetime import datetime,timezone
    end=10000
    monkeypatch.setenv('SLURM_JOB_END_TIME',str(end))
    fields={'JobId':'999','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'JobName':'hhw-handoff','Command':str(actor.area/'own.sbatch'),'NumCPUs':'4','NumNodes':'1',
        'MinMemoryNode':'16G','TimeLimit':'01:00:00','ReqTRES':'cpu=4,mem=16G,node=1',
        'AllocTRES':'cpu=4,mem=16G,node=1','EndTime':datetime.fromtimestamp(end,timezone.utc).isoformat(),
        'Socks/Node':'4','NtasksPerN:B:S:C':'0:0:*:*'}
    def reader(job,path):return subprocess.CompletedProcess([],0,' '.join(k+'='+v for k,v in fields.items()),'')
    return fields,reader


def test_own_native_exact_scalar_allocation_and_real_end(own_native,actor):
    fields,reader=own_native
    proof,end=m.native_allocation('999','handoff',fields['Command'],reader,actor.area,wall=lambda:1000)
    assert end==10000 and proof['Socks/Node']=='4' and proof['NtasksPerN:B:S:C']=='0:0:*:*'


@pytest.mark.parametrize('field,value',[('JobState','PENDING'),('UserId','foreign(1)'),('Command','/other/script'),
    ('NumCPUs','1'),('MinMemoryNode','64G'),('TimeLimit','02:00:00'),('AllocTRES','cpu=4,mem=16G,node=1,gres/gpu=1'),('ArrayJobId','100')])
def test_own_native_wrong_allocation_is_not_handoff_authority(own_native,actor,field,value):
    fields,reader=own_native;script=fields['Command'];fields[field]=value
    with pytest.raises(ValueError):m.native_allocation('999','handoff',script,reader,actor.area,wall=lambda:1000)


def test_own_native_end_mismatch_or_expired_margin_refused(own_native,actor,monkeypatch):
    fields,reader=own_native;monkeypatch.setenv('SLURM_JOB_END_TIME','9997')
    with pytest.raises(ValueError,match='end/margin'):m.native_allocation('999','handoff',fields['Command'],reader,actor.area,wall=lambda:1000)
    monkeypatch.setenv('SLURM_JOB_END_TIME','10000')
    with pytest.raises(ValueError,match='end/margin'):m.native_allocation('999','handoff',fields['Command'],reader,actor.area,wall=lambda:9000)


@pytest.fixture
def qualified_actor(actor,monkeypatch):
    import argparse
    f=actor;support=f.root/'maintenance/primary';support.mkdir();directory=f.root/'plans/q469';directory.mkdir()
    path=directory/'plan.json';ledger=f.root/'state/q469.json';script=directory/'recovery-469.sbatch'
    helper=support/'primary.py';helper.write_text('qualified primary helper')
    policy_path=support/'qualification-policy.json';policy_path.write_text(json.dumps({'qualification':None}))
    policy_sha=m.digest(policy_path);monkeypatch.setattr(m,'Q_POLICY_SHA',policy_sha)
    monkeypatch.setattr(m,'PRIMARY_SUPPORT',support);monkeypatch.setattr(m,'PRIMARY_HELPER',helper)
    command=shlex.join(['python','-s','-u',str(helper),'--mode','qualify','--policy-descriptor',str(policy_path),'--policy-descriptor-sha256',policy_sha])
    script.write_bytes((command+'\n').encode())
    task={'name':'recovery-469','script':str(script),'script_sha256':m.canonical_hash(script.read_text()),'command':command,
        'resources':{'cpus':4,'memory':'64G','hours':1,'gpu':False,'array':None}}
    plan={'root':str(f.root),'release_identity':{'sha256':m.SOURCE},'config_sha256':m.CONFIG,'tasks':[task]}
    path.write_text(json.dumps(plan));ledger.write_text(json.dumps({'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,
        'plan_sha256':m.canonical_hash(plan),'tasks':{'recovery-469':{'job_id':m.Q_JOB,'state':'submitted','script_sha256':task['script_sha256']}}}))
    for name,p in [('Q_PLAN',path),('Q_LEDGER',ledger),('Q_SCRIPT',script)]:monkeypatch.setattr(m,name,p);monkeypatch.setattr(m,name+'_SHA',m.digest(p))
    pmanifest={'files':{'primary.py':m.digest(helper)}};qpolicy={'qualification':None,'census':{'fixture':'exact qualified census'}}
    (support/'qualification.json').write_text(json.dumps({'status':'actual primary qualification fixture'}));calls=[]
    monkeypatch.setattr(m,'Q_RECEIPT_SHA',m.digest(support/'qualification.json'))
    def parser():
        p=argparse.ArgumentParser();p.add_argument('--mode');p.add_argument('--segment',type=int,default=0)
        p.add_argument('--policy-descriptor',type=Path);p.add_argument('--policy-descriptor-sha256');return p
    def own(*args):
        calls.append(('own',args));return m.registration(path,ledger,'recovery-469',script)[2],{}
    def final(*args):calls.append(('final',args))
    f.c['primary']=SimpleNamespace(parser=parser,policy_sources=lambda *a:(qpolicy,pmanifest,{str(helper):m.digest(helper)}),
        own_registration=own,finalized_primary_qualification=final)
    f.c['manifest']={'qualification_policy_descriptor':{'path':str(policy_path),'sha256':policy_sha}};f.c['primary_manifest']=pmanifest
    f.c.pop('run_policy');f.c.pop('qualification')
    native={'job_id_raw':m.Q_JOB,'user':'pwa209','account':'def-ptewarie_cpu','state':'COMPLETED','exit_code':'0:0',
        'unallocated_metadata':False,'cpus':4,'elapsed_seconds':138,'alloc_tres':'cpu=4,mem=64G,node=1'}
    f.c['native_reader']=lambda *a,**kw:{m.Q_JOB:native}
    completed=[{'job_id':m.Q_JOB}];monkeypatch.setattr(m.slurm,'completed_native_dependency',lambda *a,**kw:completed[0])
    f.qnative=native;f.completed=completed;f.qcalls=calls;f.qtask=task;f.qplan=plan;f.qpath=path;return f


def test_actual_q469_positive_native_and_registered_source_join_forwarded(qualified_actor):
    f=qualified_actor;binding=m.qualification(f.c)
    assert binding=={'path':str(m.PRIMARY_SUPPORT/'qualification.json'),'sha256':m.digest(m.PRIMARY_SUPPORT/'qualification.json'),'job_id':m.Q_JOB}
    assert [x[0] for x in f.qcalls]==['own','final']
    forwarded=f.qcalls[-1][1]
    assert forwarded[0]['qualification']==binding and forwarded[1]==f.c['primary_manifest'] and forwarded[2] is m.slurm
    assert f.c['run_policy']['qualification']==binding and f.c['pins'][str(m.Q_PLAN)]==m.Q_PLAN_SHA
    assert not mutations(f)


@pytest.mark.parametrize('change',['not_completed','failed_exit','wrong_owner','no_persisted_proof'])
def test_q469_needs_actual_positive_completed_owned_native_proof(qualified_actor,change):
    f=qualified_actor
    if change=='not_completed':f.qnative['state']='RUNNING'
    if change=='failed_exit':f.qnative['exit_code']='1:0'
    if change=='wrong_owner':f.qnative['user']='foreign'
    if change=='no_persisted_proof':f.completed[0]=None
    with pytest.raises(ValueError,match='positive finalized'):m.qualification(f.c)
    assert not any(x[0]=='final' for x in f.qcalls) and 'qualification' not in f.c.get('run_policy',{})
    assert not mutations(f)


def test_q469_finalized_receipt_validation_failure_propagates_before_handoff(qualified_actor):
    f=qualified_actor
    def reject(*args):raise ValueError('finalized JUnit/source closure changed')
    f.c['primary'].finalized_primary_qualification=reject
    with pytest.raises(ValueError,match='JUnit'):m.qualification(f.c)
    assert not mutations(f)


def test_main_orders_native_selftests_accounting_and_separate_qualification_before_mutations():
    import inspect
    source=inspect.getsource(m.main);tree=ast.parse(source)
    assert list(tree.body)[0].name=='main'
    assert source.index('self_qualification(c)')<source.index("admission('primary-handoff-finalized-qualification-accounting'")<source.index('qualification(c);first_provider(c)')<source.index('result=orchestrate(c)')
    assert 'with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1' in source


def test_filtered_cancel_zero_ack_followed_by_running_stops_before_another_cancel(actor):
    f=actor
    def runner(command,**kwargs):
        result=f.runner(command,**kwargs)
        if command[0]=='scancel':f.states[command[-1]]='RUNNING'
        return result
    with pytest.raises(ValueError,match='active or unexpected'):m.orchestrate(f.c,runner=runner)
    assert len([e for e in mutations(f) if e[0]=='scancel'])==1
    assert not any(e[:2]==('scontrol','release') for e in mutations(f)) and all(f.held[j] for j in ['1000','1001','1002','1003'])


@pytest.mark.parametrize('change',['failure','completed_without_proof','completed_nonzero'])
def test_first_provider_must_be_viable_immediately_before_old_retirement(actor,change):
    f=actor;original=f.c['native_reader']
    def reader(selected,*args,**kwargs):
        rows=original(selected,*args,**kwargs)
        if m.FIRST_PROVIDER in selected:
            rows[m.FIRST_PROVIDER].update(state='FAILED' if change=='failure' else 'COMPLETED',cpus=8,elapsed_seconds=10,
                exit_code='1:0' if change=='completed_nonzero' else '0:0')
        return rows
    f.c['native_reader']=reader
    with pytest.raises(ValueError):m.orchestrate(f.c,runner=f.runner)
    assert len([e for e in mutations(f) if e[0]=='sbatch'])==4
    assert not any(e[0]=='scancel' or e[:2]==('scontrol','release') for e in mutations(f))
