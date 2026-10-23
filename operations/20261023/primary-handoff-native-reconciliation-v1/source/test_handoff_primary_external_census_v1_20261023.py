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
from horsehair.repair import _parse_repair_native

spec=importlib.util.spec_from_file_location('primary_external_census_handoff',Path(__file__).with_name('handoff-primary-external-census-v1-20261023.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
REFERENCE_PRIMARY_SUPPORT=m.PRIMARY_SUPPORT
REFERENCE_OLD_FRAMEWORK=m.OLD_FRAMEWORK
REAL_RECONCILE_PARTIAL=m.reconcile_partial


def emitted_scheduler_workdir(script,command,submission_cwd):
    """Only Slurm options set scheduler WorkDir; later body cd is irrelevant."""
    header=[]
    for line in script.splitlines():
        stripped=line.strip()
        if not stripped:continue
        if stripped.startswith('#SBATCH'):header.extend(shlex.split(stripped[len('#SBATCH'):].strip()))
        elif not stripped.startswith('#'):break
    def selected(tokens):
        value=None
        for i,token in enumerate(tokens):
            if token.startswith('--chdir='):value=token.split('=',1)[1]
            elif token in {'--chdir','-D'}:value=tokens[i+1]
        return value
    return selected(command[1:-1]) or selected(header) or str(submission_cwd)


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
    tasks=[];ledger_rows={};by_job={};states={};held={};overrides={};events=[];lock=[False];edges={};workdirs={}
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
            'Command':task['script'],'WorkDir':workdirs[job] if new else str(old_framework.resolve()),'JobState':states[job],
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
        lines=[]
        for job in selected:
            allocated=job in {m.OLD_JOBS[0],m.FIRST_PROVIDER};state=states[job]
            cpus=8 if job==m.FIRST_PROVIDER else 1 if allocated else 0
            fields=[job,job,state,'0:0','1200' if allocated else '0',str(cpus),
                'cpu='+str(cpus)+',mem=64G,node=1' if allocated else '',
                'nc10001' if allocated else 'None assigned','2026-10-23T10:00:00' if allocated else 'None',
                'None' if state in {'PENDING','RUNNING'} else '2026-10-23T11:00:00','pwa209','def-ptewarie_cpu','0']
            lines.append('|'.join(fields))
        return _parse_repair_native('\n'.join(lines),rosters,**kwargs)
    states[m.FIRST_PROVIDER]='RUNNING'
    counter=[1000]
    def runner(command,**kwargs):
        events.append(tuple(command))
        if command[0]=='sbatch':
            assert lock[0] and '--hold' in command
            current=json.loads((root/'plans'/m.PHASE/'plan.json').read_text());task=next(t for t in current['tasks'] if t['script']==command[-1])
            job=str(counter[0]);counter[0]+=1;by_job[job]=task;states[job]='PENDING';held[job]=True
            workdirs[job]=emitted_scheduler_workdir(Path(task['script']).read_text(),command,framework.resolve())
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
    def reconciled(c,**kwargs):
        assert lock[0];events.append(('partial-reconciliation-fixture',))
        m.exclusive(c['area']/'partial-reconciliation.json',{'status':'synthetic previously reconciled fixture'})
        c['partial_reconciliation']={}
    monkeypatch.setattr(m,'reconcile_partial',reconciled)
    c={'job':'999','area':area,'end':time.time()+10000,'config':config,'pins':{str(helper):m.digest(helper)},
        'registry':{'registration_lock':registration_lock},'scheduler':scheduler,'native_reader':native_reader,'admission':admission,
        'policy':SimpleNamespace(receipt=lambda:{'sha256':'a'*64}),'run_policy':{'qualification':{'path':'qualified-primary','sha256':'b'*64,'job_id':m.Q_JOB}},
        'qualification':{'path':'qualified-primary','sha256':'b'*64,'job_id':m.Q_JOB},
        'selftest_proof':{'status':'complete_native_handoff_contracts_completion_pending'}}
    m.exclusive(area/'native-handoff-qualification.json',c['selftest_proof'])
    return SimpleNamespace(c=c,root=root,area=area,old_path=old_path,old_ledger=old_ledger,plan=plan,ledger=ledger,
        states=states,held=held,overrides=overrides,events=events,lock=lock,runner=runner,by_job=by_job,
        counter=counter,edges=edges,helper=helper,protected=protected,workdirs=workdirs)


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


@pytest.mark.parametrize('field,value',[('state','COMPLETED'),('cpus',1),('elapsed_seconds',1),('unallocated_metadata',True),
    ('alloc_tres','cpu=1'),('start','2026-10-23T11:00:00'),('node_list','nc12345'),('user','foreign'),('job_id_raw','555'),
    ('exit_code','1:0'),('max_rss','1K'),('identity_basis','Explicit never-started queue/template metadata, not a physical allocation'),('end','None')])
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


def reviewed_input(canonical,local_relative,expected_sha):
    """The same frozen bytes are supplied by actual native lineage or local fixture."""
    candidate=Path(canonical)
    if not candidate.is_file():candidate=Path(__file__).resolve().parent.parent/local_relative
    raw=candidate.read_bytes()
    assert m.hashlib.sha256(raw).hexdigest()==expected_sha
    return raw


@pytest.fixture
def complete_primary_closure(actor,monkeypatch):
    f=actor;support=f.root/'maintenance/actual-primary-policy';support.mkdir()
    outside=f.root.parent/'original-d89-external';(outside/'tests').mkdir(parents=True)
    helper_name='run-primary-external-census-v1-20261023.py';test_name='test_primary_external_census_v1_20261023.py'
    primary_manifest_raw=reviewed_input(REFERENCE_PRIMARY_SUPPORT/'source-manifest.json',
        'primary-external-census-source-manifest-20261023.json',m.PRIMARY_MANIFEST_SHA)
    primary_manifest=json.loads(primary_manifest_raw);(support/'source-manifest.json').write_bytes(primary_manifest_raw)
    for name in (helper_name,test_name):
        (support/name).write_bytes(reviewed_input(REFERENCE_PRIMARY_SUPPORT/name,name,primary_manifest['files'][name]))
    for scope in ('baseline_pipeline','baseline_budget'):
        contract=primary_manifest['suites'][scope];relative=Path(contract['source'])
        (outside/relative).write_bytes(reviewed_input(REFERENCE_OLD_FRAMEWORK/relative,
            Path('primary-framework-d89-source')/relative,contract['source_sha256']))
    primary_spec=importlib.util.spec_from_file_location('frozen_actual_primary_policy_closure',support/helper_name)
    primary=importlib.util.module_from_spec(primary_spec);primary_spec.loader.exec_module(primary)
    monkeypatch.setattr(primary,'ROOT',f.root);monkeypatch.setattr(primary,'SUPPORT',support);monkeypatch.setattr(primary,'OLD_FRAMEWORK',outside)
    census=f.root/'maintenance/actual-census';census.mkdir()
    (census/'source-manifest.json').write_bytes(reviewed_input(primary.CENSUS_SUPPORT/'source-manifest.json',
        'rolling-native-v5/source-manifest.json',primary.CENSUS_MANIFEST_SHA))
    monkeypatch.setattr(primary,'CENSUS_SUPPORT',census)
    policy=json.loads(reviewed_input(REFERENCE_PRIMARY_SUPPORT/'qualification-policy.json',
        'primary-native-469/qualification-policy.json',m.Q_POLICY_SHA))
    policy['source_manifest']['path']=str(support/'source-manifest.json')
    policy['census']['provider_schedule']['source_manifest']['path']=str(census/'source-manifest.json')
    path=support/'qualification-policy.json';path.write_bytes((json.dumps(policy,sort_keys=True,indent=2)+'\n').encode())
    actual_policy,actual_manifest,pins=primary.policy_sources(path,m.digest(path))
    monkeypatch.setattr(m,'OLD_FRAMEWORK',outside)
    identities={m.FRAMEWORK:m.SOURCE,m.PRIMARY:m.PRIMARY_SHA,outside:m.OLD_SOURCE}
    monkeypatch.setattr(m,'source_identity',lambda source:{'sha256':identities[Path(source)]})
    f.c['pins'].update(pins)
    return SimpleNamespace(actor=f,primary=primary,policy=actual_policy,manifest=actual_manifest,pins=pins,outside=outside,support=support)


def test_complete_actual_primary_policy_sources_closure_passes_handoff_source_guard(complete_primary_closure):
    f=complete_primary_closure;outside={p:h for p,h in f.pins.items() if not Path(p).is_relative_to(f.actor.root)}
    assert len(f.pins)==6 and outside=={
        str(f.outside/'tests/test_pipeline.py'):'5e0c052c596765a32ff4443d8668ba3b627dbeb1d7f7f954479048bbc5a14936',
        str(f.outside/'tests/test_budget.py'):'9ea7346e98203bc098121689f7a4ac4a45c2ef488bd2472d894cc0beee0d65de'}
    m.verify_sources(f.actor.c)
    assert not mutations(f.actor)


@pytest.mark.parametrize('name',['test_pipeline.py','test_budget.py'])
def test_only_exact_expected_sha_reads_each_original_external_test(complete_primary_closure,name):
    f=complete_primary_closure;path=f.outside/'tests'/name;pin=f.pins[str(path)]
    raw,actual=m.packet(path,pin)
    assert raw==path.read_bytes() and actual==pin
    with pytest.raises(ValueError):m.packet(path)
    with pytest.raises(ValueError):m.packet(path,'0'*64)


@pytest.mark.parametrize('relative',['tests/test_future.py','tests/test_pipeline.py.future','src/horsehair/pipeline.py','test_budget.py'])
def test_arbitrary_external_source_is_refused_even_with_its_actual_sha(complete_primary_closure,relative):
    f=complete_primary_closure;path=f.outside/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'unreviewed external file')
    with pytest.raises(ValueError):m.packet(path,m.digest(path))


def test_changed_external_baseline_cannot_be_resealed_into_source_guard(complete_primary_closure):
    f=complete_primary_closure;path=f.outside/'tests/test_budget.py';path.write_bytes(path.read_bytes()+b'\n# altered baseline\n')
    f.actor.c['pins'][str(path)]=m.digest(path)
    with pytest.raises(ValueError):m.verify_sources(f.actor.c)
    assert not mutations(f.actor)


def test_symlink_spelling_for_allowed_external_test_is_refused(complete_primary_closure,monkeypatch):
    f=complete_primary_closure;path=f.outside/'tests/test_pipeline.py';original=Path.is_symlink
    def symlink(candidate):return candidate==path or original(candidate)
    monkeypatch.setattr(Path,'is_symlink',symlink)
    with pytest.raises(ValueError):m.packet(path,f.pins[str(path)])


def test_symlink_ancestor_for_external_tests_is_refused(complete_primary_closure,monkeypatch):
    f=complete_primary_closure;directory=f.outside/'tests';path=directory/'test_budget.py';original=Path.is_symlink
    def symlink(candidate):return candidate==directory or original(candidate)
    monkeypatch.setattr(Path,'is_symlink',symlink)
    with pytest.raises(ValueError):m.packet(path,f.pins[str(path)])


def test_future_external_release_with_identical_baseline_bytes_is_refused(complete_primary_closure):
    f=complete_primary_closure;original=f.outside/'tests/test_budget.py';future=f.outside.parent/'future-d89/tests/test_budget.py'
    future.parent.mkdir(parents=True);future.write_bytes(original.read_bytes())
    with pytest.raises(ValueError):m.packet(future,f.pins[str(original)])


def test_noncanonical_external_alias_is_refused(complete_primary_closure):
    f=complete_primary_closure;path=f.outside/'tests/../tests/test_budget.py'
    with pytest.raises(ValueError):m.packet(path,f.pins[str(f.outside/'tests/test_budget.py')])


def test_actual_483_script_and_native_row_reproduce_submission_cwd_mismatch():
    import base64
    script=base64.b64decode(HISTORICAL_483_SCRIPT_BASE64);raw=base64.b64decode(HISTORICAL_483_NATIVE_BASE64)
    assert m.hashlib.sha256(script).hexdigest()=='fbe69f82abce43e936a70be0aeb27835e7bb915fadb1cef59b1c06d2d39b4938'
    assert m.hashlib.sha256(raw).hexdigest()=='51758a668fa1a70dd105c6a4f3ef7d7d57a46ff250797ea53d6cbcde03b32115'
    native=m.native_fields(subprocess.CompletedProcess([],0,raw.decode(),''))
    expected='/lustre06/project/6101854/pwa209/horsehair-worm/releases/20261021-57e0eb8c4c6c-d89b31180d4c'
    caller='/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e'
    assert '\ncd '+expected+'\n' in script.decode() and native['WorkDir']==caller
    assert emitted_scheduler_workdir(script.decode(),shlex.split(native['SubmitLine']),caller)==caller
    assert not any(line.startswith('#SBATCH') and '--chdir' in line for line in script.decode().splitlines())


def test_real_new_script_chdir_drives_scheduler_workdir_before_any_body_command(actor):
    f=actor;result=m.orchestrate(f.c,runner=f.runner)
    for job in result['new_jobs']:
        task=f.by_job[job];text=Path(task['script']).read_text();lines=text.splitlines()
        directives=[line for line in lines if line.startswith('#SBATCH') and '--chdir=' in line]
        assert len(directives)==1 and shlex.split(directives[0][len('#SBATCH'):].strip())==['--chdir='+str(m.OLD_FRAMEWORK.resolve())]
        assert lines.index(directives[0])<lines.index('set -euo pipefail')
        assert f.workdirs[job]==str(m.OLD_FRAMEWORK.resolve()) and f.workdirs[job]!=str(m.FRAMEWORK.resolve())
    assert all('--chdir='+str(m.OLD_FRAMEWORK.resolve()) in event for event in mutations(f) if event[0]=='sbatch')


def test_removing_explicit_chdir_reproduces_real_failure_despite_unchanged_body_cd(actor,monkeypatch):
    f=actor;original=m.freeze_new
    def missing_directive(c,descriptor):
        pack=original(c,descriptor)
        for task in pack['value']['tasks']:
            path=Path(task['script']);text='\n'.join(line for line in path.read_text().splitlines() if not (line.startswith('#SBATCH') and '--chdir=' in line))+'\n'
            assert any(shlex.split(line)==['cd',str(m.OLD_FRAMEWORK.resolve())] for line in text.splitlines() if line.startswith('cd '))
            path.write_bytes(text.encode());task['script_sha256']=m.canonical_hash(text)
        pack['path'].write_bytes((json.dumps(pack['value'],sort_keys=True,indent=2)+'\n').encode())
        return pack
    monkeypatch.setattr(m,'freeze_new',missing_directive)
    def caller_without_cli_override(command,**kwargs):
        return f.runner([token for token in command if not token.startswith('--chdir=')],**kwargs)
    with pytest.raises(ValueError,match='Exact held unstarted new D89'):m.orchestrate(f.c,runner=caller_without_cli_override)
    assert all(f.workdirs[job]==str(m.FRAMEWORK.resolve()) for job in ['1000','1001','1002','1003'])
    assert len([e for e in mutations(f) if e[0]=='sbatch'])==4
    assert not any(e[0]=='scancel' or e[:2]==('scontrol','release') for e in mutations(f))


def captured_491():
    import base64
    raw=base64.b64decode(HISTORICAL_491_NATIVE_BASE64)
    assert m.hashlib.sha256(raw).hexdigest()=='bf0e83b0f1aa02f6a9fdde520ccca5f14f6f8f494bb73876dbe951dd580ce9a7'
    return _parse_repair_native(raw.decode(),{},owner='pwa209',account='def-ptewarie_cpu')


@pytest.fixture
def partial_actor(actor,monkeypatch):
    import base64
    import copy
    f=actor;monkeypatch.setattr(m,'reconcile_partial',REAL_RECONCILE_PARTIAL)
    original_root='/scratch/pwa209/horsehair-worm';directory=f.root/'plans'/m.PARTIAL_PHASE;directory.mkdir()
    plan_entry=HISTORICAL_PARTIAL_PACKETS[original_root+'/plans/'+m.PARTIAL_PHASE+'/plan.json']
    ledger_entry=HISTORICAL_PARTIAL_PACKETS[original_root+'/state/'+m.PARTIAL_PHASE+'-submission.json']
    for entry in HISTORICAL_PARTIAL_PACKETS.values():
        assert m.hashlib.sha256(base64.b64decode(entry['base64'])).hexdigest()==entry['sha256']
    plan=json.loads(base64.b64decode(plan_entry['base64']));ledger=json.loads(base64.b64decode(ledger_entry['base64']))
    descriptor=f.root/'maintenance/historical/native-6868176/run-policy.json';descriptor.parent.mkdir(parents=True)
    descriptor.write_bytes(base64.b64decode(HISTORICAL_PARTIAL_DESCRIPTOR['base64']))
    assert m.digest(descriptor)==m.PARTIAL_DESCRIPTOR_SHA
    source=descriptor.parent.parent/m.SELF_NAME
    source.write_bytes(reviewed_input(m.PARTIAL_SOURCE,Path('primary-handoff-v2')/m.SELF_NAME,m.PARTIAL_SOURCE_SHA))
    path=directory/'plan.json';ledger_path=f.root/'state'/(m.PARTIAL_PHASE+'-submission.json')
    monkeypatch.setattr(m,'PARTIAL_PLAN',path);monkeypatch.setattr(m,'PARTIAL_LEDGER',ledger_path)
    monkeypatch.setattr(m,'PARTIAL_DESCRIPTOR',descriptor);monkeypatch.setattr(m,'PARTIAL_SOURCE',source)
    plan['root']=str(f.root);plan['descriptor_path']=str(descriptor);shas=[]
    for i,task in enumerate(plan['tasks']):
        name=task['name'];script=directory/(name+'.sbatch');row=ledger['tasks'][name];oldcommand=task['command']
        args=['--mode','run','--policy-descriptor',str(descriptor),'--policy-descriptor-sha256',m.PARTIAL_DESCRIPTOR_SHA,
            '--native-plan',str(path),'--native-ledger',str(ledger_path),'--native-task',name,'--native-script',str(script),'--segment',str(i)]
        command=shlex.join(['python','-s','-u',str(m.PRIMARY_HELPER),*args])
        text=base64.b64decode(HISTORICAL_PARTIAL_PACKETS[original_root+'/plans/'+m.PARTIAL_PHASE+'/'+name+'.sbatch']['base64']).decode()
        text=text.replace(oldcommand,command);script.write_bytes(text.encode());shas.append(m.digest(script))
        task.update(command=command,script=str(script),script_sha256=m.canonical_hash(text))
        task['primary_external_census']['helper']['path']=str(m.PRIMARY_HELPER)
        task['primary_external_census']['policy_descriptor']['path']=str(descriptor)
        row['script_sha256']=task['script_sha256'];row['command'][-1]=str(script)
        job=m.PARTIAL_JOBS[i];f.by_job[job]=task;f.states[job]='CANCELLED' if i==0 else 'PENDING';f.held[job]=True
        f.workdirs[job]=str(m.FRAMEWORK.resolve());f.edges[job]='(null)'
    path.write_bytes((json.dumps(plan,sort_keys=True,indent=2)+'\n').encode());ledger['plan_sha256']=m.canonical_hash(plan)
    ledger_path.write_bytes((json.dumps(ledger,sort_keys=True,indent=2)+'\n').encode())
    monkeypatch.setattr(m,'PARTIAL_PLAN_SHA',m.digest(path));monkeypatch.setattr(m,'PARTIAL_LEDGER_SHA',m.digest(ledger_path))
    monkeypatch.setattr(m,'PARTIAL_SCRIPT_SHAS',tuple(shas))
    original_reader=f.c['native_reader'];actual=captured_491()
    def native(selected,rosters,**kwargs):
        result=original_reader([j for j in selected if j!=m.PARTIAL_PARENT],rosters,**kwargs)
        for job in selected:
            if job==m.PARTIAL_PARENT or job==m.PARTIAL_JOBS[0]:result[job]=copy.deepcopy(actual[job])
        return result
    f.c['native_reader']=native
    original_runner=f.runner
    def runner(command,**kwargs):
        if command[0]=='scancel' and command[-1] in m.PARTIAL_JOBS:
            assert f.lock[0] and command[1:4]==['--ctld','--state=PENDING','--user=pwa209']
            assert command[-1]!=m.PARTIAL_JOBS[0] and f.states[command[-1]]=='PENDING'
            f.events.append(tuple(command));f.states[command[-1]]='CANCELLED';return subprocess.CompletedProcess(command,0,'','')
        return original_runner(command,**kwargs)
    f.runner=runner;f.partial_plan=plan;f.partial_ledger=ledger;f.partial_path=path;f.partial_ledger_path=ledger_path
    f.partial_protected={p:p.read_bytes() for p in [path,ledger_path,descriptor,source,*directory.glob('*.sbatch')]}
    return f


def test_actual_491_scalar_rows_and_k_units_are_accepted_without_array_metadata():
    native=captured_491();first=native[m.PARTIAL_JOBS[0]];parent=native[m.PARTIAL_PARENT]
    assert m.unused_cancelled(first,m.PARTIAL_JOBS[0]) is first
    assert first['unallocated_metadata'] is False and first['identity_basis']=='Unique scalar native job; zero-allocation queue record'
    assert parent['elapsed_seconds']==864 and parent['cpus']==4 and parent['max_rss']=='85520K'
    m.tres(parent['alloc_tres'],4,'16G')


def test_full_real_partial_bindings_accept_actual_parent_and_preserved_historical_packet_schema(partial_actor):
    f=partial_actor;pack=m.partial_bindings(f.c)
    assert pack['jobs']==list(m.PARTIAL_JOBS) and len(pack['tasks'])==4
    assert f.c['pins'][str(m.PARTIAL_PLAN)]==m.PARTIAL_PLAN_SHA and not mutations(f)


def test_partial_reconciliation_reverse_pending_only_and_first_already_cancelled_no_mutation(partial_actor):
    f=partial_actor;f.c['registration_locked']=True;f.lock[0]=True
    proof=m.reconcile_partial(f.c,runner=f.runner)
    assert list(proof)==list(m.PARTIAL_JOBS)
    assert [e[-1] for e in mutations(f)]==list(reversed(m.PARTIAL_JOBS[1:]))
    assert all(e[:4]==('scancel','--ctld','--state=PENDING','--user=pwa209') for e in mutations(f))
    assert all(p.read_bytes()==raw for p,raw in f.partial_protected.items())
    assert all(f.states[j]=='PENDING' for j in m.OLD_JOBS[1:])
    record=json.loads((f.area/'partial-reconciliation.json').read_text())
    assert record['already_cancelled_job_not_mutated']==m.PARTIAL_JOBS[0] and record['failed_phase_bytes_preserved'] is True


def test_reconciliation_precedes_new_registration_and_original_primary_retirement(partial_actor):
    f=partial_actor;result=m.orchestrate(f.c,runner=f.runner)
    rows=mutations(f)
    assert [x[-1] for x in rows[:3]]==list(reversed(m.PARTIAL_JOBS[1:]))
    assert all(x[0]=='sbatch' for x in rows[3:7])
    assert result['partial_reconciliation']['sha256']==m.digest(f.area/'partial-reconciliation.json')


@pytest.mark.parametrize('field,value',[('JobState','RUNNING'),('Reason','Dependency'),('UserId','foreign(1)'),('WorkDir','/foreign'),('AllocTRES','cpu=1,mem=64G,node=1')])
def test_partial_preflight_refuses_unknown_started_or_unheld_target_before_any_rpc(partial_actor,field,value):
    f=partial_actor;f.overrides[m.PARTIAL_JOBS[2]]={field:value}
    with pytest.raises(ValueError):m.orchestrate(f.c,runner=f.runner)
    assert not mutations(f)


@pytest.mark.parametrize('phase',['before_rpc','after_rpc'])
def test_partial_running_race_stops_before_another_cancel_or_new_registration(partial_actor,phase):
    f=partial_actor;original=f.runner
    def runner(command,**kwargs):
        response=original(command,**kwargs)
        if command[0]=='scancel':
            if phase=='before_rpc':f.overrides[m.PARTIAL_JOBS[2]]={'JobState':'RUNNING'}
            else:f.states[command[-1]]='RUNNING'
        return response
    with pytest.raises(ValueError):m.orchestrate(f.c,runner=runner)
    assert len(mutations(f))==1 and mutations(f)[0][-1]==m.PARTIAL_JOBS[3]
    assert not (f.root/'plans'/m.PHASE).exists()


@pytest.mark.parametrize('field,value',[('cpus',1),('max_rss','1K'),('unallocated_metadata',True),('start','2026-10-23T11:00:00')])
def test_already_cancelled_first_orphan_requires_positive_zero_allocation(partial_actor,field,value):
    f=partial_actor;original=f.c['native_reader']
    def native(*args,**kwargs):
        rows=original(*args,**kwargs)
        if m.PARTIAL_JOBS[0] in rows:rows[m.PARTIAL_JOBS[0]][field]=value
        return rows
    f.c['native_reader']=native
    with pytest.raises(ValueError):m.orchestrate(f.c,runner=f.runner)
    assert not mutations(f)


@pytest.mark.parametrize('change',['wrong_job','wrong_intent','plan_source','stale_script','parent_duration','parent_k_memory'])
def test_partial_source_binding_or_exact_parent_allocation_mismatch_refused(partial_actor,monkeypatch,change):
    f=partial_actor
    if change=='wrong_job':f.partial_ledger['tasks']['primary-external-census-controller-1']['job_id']='99999'
    if change=='wrong_intent':f.partial_plan['tasks'][1]['primary_external_census']['mode']='qualify'
    if change=='plan_source':f.partial_plan['release_identity']['sha256']='0'*64
    if change=='stale_script':Path(f.partial_plan['tasks'][1]['script']).write_bytes(b'changed')
    if change in {'parent_duration','parent_k_memory'}:
        original=f.c['native_reader']
        def native(*args,**kwargs):
            rows=original(*args,**kwargs)
            if m.PARTIAL_PARENT in rows:
                if change=='parent_duration':rows[m.PARTIAL_PARENT]['elapsed_seconds']=863
                else:rows[m.PARTIAL_PARENT]['alloc_tres']='billing=4000,cpu=4,mem=16777215K,node=1'
            return rows
        f.c['native_reader']=native
    f.partial_path.write_bytes((json.dumps(f.partial_plan,sort_keys=True,indent=2)+'\n').encode())
    f.partial_ledger['plan_sha256']=m.canonical_hash(f.partial_plan);f.partial_ledger_path.write_bytes((json.dumps(f.partial_ledger,sort_keys=True,indent=2)+'\n').encode())
    monkeypatch.setattr(m,'PARTIAL_PLAN_SHA',m.digest(f.partial_path));monkeypatch.setattr(m,'PARTIAL_LEDGER_SHA',m.digest(f.partial_ledger_path))
    with pytest.raises(ValueError):m.orchestrate(f.c,runner=f.runner)
    assert not mutations(f)


# Exact own-job bytes captured by read-only evidence488; no live scheduler call.
HISTORICAL_483_SCRIPT_BASE64='IyEvYmluL2Jhc2gKI1NCQVRDSCAtLWFjY291bnQ9ZGVmLXB0ZXdhcmllX2NwdQojU0JBVENIIC0tam9iLW5hbWU9aGh3LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMAojU0JBVENIIC0tY3B1cy1wZXItdGFzaz0xCiNTQkFUQ0ggLS1tZW09NjRHCiNTQkFUQ0ggLS10aW1lPTcyOjAwOjAwCiNTQkFUQ0ggLS1vdXRwdXQ9L3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL2xvZ3MvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wLSVBXyVhLmxvZwojU0JBVENIIC0taG9sZApzZXQgLWV1byBwaXBlZmFpbApleHBvcnQgVFo9VVRDCmV4cG9ydCBQWVRIT05ET05UV1JJVEVCWVRFQ09ERT0xCnRlc3QgIiQoaWQgLXVuKSIgPSBwd2EyMDkKbW9kdWxlIGxvYWQgU3RkRW52LzIwMjMgcHl0aG9uLzMuMTEuNSBjdWRhLzEyLjYgYXJyb3cvMTkuMC4xCnNvdXJjZSAvaG9tZS9wd2EyMDkvaGh3LWVudmlyb25tZW50L2Jpbi9hY3RpdmF0ZQpjZCAvbHVzdHJlMDYvcHJvamVjdC82MTAxODU0L3B3YTIwOS9ob3JzZWhhaXItd29ybS9yZWxlYXNlcy8yMDI2MTAyMS01N2UwZWI4YzRjNmMtZDg5YjMxMTgwZDRjCmV4cG9ydCBQWVRIT05QQVRIPS9sdXN0cmUwNi9wcm9qZWN0LzYxMDE4NTQvcHdhMjA5L2hvcnNlaGFpci13b3JtL3JlbGVhc2VzLzIwMjYxMDIxLTU3ZTBlYjhjNGM2Yy1kODliMzExODBkNGMvc3JjOiIke1BZVEhPTlBBVEg6LX0iCmV4cG9ydCBNUExCQUNLRU5EPUFnZwpleHBvcnQgT01QX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE9QRU5CTEFTX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE1LTF9OVU1fVEhSRUFEUz0ke1NMVVJNX0NQVVNfUEVSX1RBU0t9CmV4cG9ydCBQWVRIT05VTkJVRkZFUkVEPTEKZXhwb3J0IENVQkxBU19XT1JLU1BBQ0VfQ09ORklHPTo0MDk2OjgKZXhwb3J0IEhIV19HUFVfQ09VTlQ9MApleHBvcnQgSEhXX01BWF9IT1VSUz03MgpweXRob24gLW0gaG9yc2VoYWlyLmNsaSAtLWNvbmZpZyBjb25maWdzL3N0dWR5Lmpzb24gLS1yb290IC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybSB2ZXJpZnktc291cmNlIC0taWRlbnRpdHkgcmVsZWFzZS5qc29uCi9ob21lL3B3YTIwOS9oaHctZW52aXJvbm1lbnQvYmluL3B5dGhvbiAtcyAtdSAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxLTIwMjYxMDIzL3J1bi1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy12MS0yMDI2MTAyMy5weSAtLW1vZGUgcnVuIC0tcG9saWN5LWRlc2NyaXB0b3IgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWhhbmRvZmYtdjItMjAyNjEwMjMvbmF0aXZlLTY4NjgxNzYvcnVuLXBvbGljeS5qc29uIC0tcG9saWN5LWRlc2NyaXB0b3Itc2hhMjU2IGFkNzBhMTQwY2RkNTAzODRmODYwY2I0YWRjMmRjNGZjMzhmZWIwYTdlYWE2MGJlMjQwMzQ1NjM3M2M5YjIyNjcgLS1uYXRpdmUtcGxhbiAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcGxhbi5qc29uIC0tbmF0aXZlLWxlZGdlciAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vc3RhdGUvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEtc3VibWlzc2lvbi5qc29uIC0tbmF0aXZlLXRhc2sgcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wIC0tbmF0aXZlLXNjcmlwdCAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wLnNiYXRjaCAtLXNlZ21lbnQgMAo='
HISTORICAL_483_NATIVE_BASE64='Sm9iSWQ9Njg2ODg4NCBKb2JOYW1lPWhody1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTAgVXNlcklkPXB3YTIwOSgzMTY0NzY4KSBHcm91cElkPXB3YTIwOSgzMTY0NzY4KSBNQ1NfbGFiZWw9Ti9BIFByaW9yaXR5PTAgTmljZT0wIEFjY291bnQ9ZGVmLXB0ZXdhcmllX2NwdSBRT1M9bm9ybWFsIEpvYlN0YXRlPVBFTkRJTkcgUmVhc29uPUpvYkhlbGRVc2VyIERlcGVuZGVuY3k9YWZ0ZXJvazo2ODY4MTc2KHVuZnVsZmlsbGVkKSBSZXF1ZXVlPTAgUmVzdGFydHM9MCBCYXRjaEZsYWc9MSBSZWJvb3Q9MCBFeGl0Q29kZT0wOjAgUnVuVGltZT0wMDowMDowMCBUaW1lTGltaXQ9My0wMDowMDowMCBUaW1lTWluPU4vQSBTdWJtaXRUaW1lPTIwMjYtMTAtMjNUMTE6MzQ6MjQgRWxpZ2libGVUaW1lPVVua25vd24gQWNjcnVlVGltZT1Vbmtub3duIFN0YXJ0VGltZT1Vbmtub3duIEVuZFRpbWU9VW5rbm93biBEZWFkbGluZT1OL0EgU3VzcGVuZFRpbWU9Tm9uZSBTZWNzUHJlU3VzcGVuZD0wIExhc3RTY2hlZEV2YWw9MjAyNi0xMC0yM1QxMTozNDoyNCBTY2hlZHVsZXI9TWFpbiBQYXJ0aXRpb249Y3B1bGFyZ2VfYnljb3JlX2I0IEFsbG9jTm9kZTpTaWQ9bmMzMTEwOTozNDYwMzM5IFJlcU5vZGVMaXN0PShudWxsKSBFeGNOb2RlTGlzdD0obnVsbCkgTm9kZUxpc3Q9IE51bU5vZGVzPTEtMSBOdW1DUFVzPTEgTnVtVGFza3M9MSBDUFVzL1Rhc2s9MSBSZXFCOlM6QzpUPTA6MDoqOiogUmVxVFJFUz1jcHU9MSxtZW09NjRHLG5vZGU9MSxiaWxsaW5nPTE2MDAwIEFsbG9jVFJFUz0obnVsbCkgU29ja3MvTm9kZT0qIE50YXNrc1Blck46QjpTOkM9MDowOio6KiBDb3JlU3BlYz0qIE1pbkNQVXNOb2RlPTEgTWluTWVtb3J5Tm9kZT02NEcgTWluVG1wRGlza05vZGU9MCBGZWF0dXJlcz1bcm9tZXxtaWxhbnxnZW5vYV0gRGVsYXlCb290PTAwOjAwOjAwIE92ZXJTdWJzY3JpYmU9T0sgQ29udGlndW91cz0wIExpY2Vuc2VzPShudWxsKSBMaWNlbnNlc0FsbG9jPShudWxsKSBOZXR3b3JrPShudWxsKSBDb21tYW5kPS9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9wbGFucy9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVycy12MS9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTAuc2JhdGNoIFN1Ym1pdExpbmU9c2JhdGNoIC0taG9sZCAtLXBhcnNhYmxlIC0tZGVwZW5kZW5jeT1hZnRlcm9rOjY4NjgxNzYgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL3BsYW5zL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXJzLXYxL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMC5zYmF0Y2ggV29ya0Rpcj0vc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcmVsZWFzZXMvMjAyNjEwMjEtNmVkMjAxNjUzZGZhLTRmMzkwM2Q3YzI2ZSBTdGRFcnI9IFN0ZEluPS9kZXYvbnVsbCBTdGRPdXQ9L3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL2xvZ3MvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wLTY4Njg4ODRfNDI5NDk2NzI5NC5sb2cgVHJlc1BlclRhc2s9Y3B1PTEK'


HISTORICAL_PARTIAL_PACKETS={'/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v1/plan.json': {'base64': 'ewogICJjb25maWdfc2hhMjU2IjogIjQ0NmI1ZDYyZmMyMDZjZjEzOTJiOTBmZjljZjE2YWRhZmM2NmM1N2E2ZDM4ZTg0YzAyOWNkM2IyZWJiZjFmMzUiLAogICJjcHVfY2VpbGluZyI6IDI1NiwKICAiZGVzY3JpcHRvcl9wYXRoIjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9tYWludGVuYW5jZS9zdHVkeS1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1oYW5kb2ZmLXYyLTIwMjYxMDIzL25hdGl2ZS02ODY4MTc2L3J1bi1wb2xpY3kuanNvbiIsCiAgImRlc2NyaXB0b3Jfc2hhMjU2IjogImFkNzBhMTQwY2RkNTAzODRmODYwY2I0YWRjMmRjNGZjMzhmZWIwYTdlYWE2MGJlMjQwMzQ1NjM3M2M5YjIyNjciLAogICJncHVfY29uY3VycmVuY3kiOiA0LAogICJoYW5kb2ZmX25hdGl2ZV9qb2JfaWQiOiAiNjg2ODE3NiIsCiAgIm1heGltdW1fc2ltdWx0YW5lb3VzX3Jlc291cmNlcyI6IHsKICAgICJjcHVzIjogMSwKICAgICJncHVzIjogMAogIH0sCiAgIm9yaWdpbmFsX3ByaW1hcnlfbGVhc2VfbGltaXRzX3JldGFpbmVkIjogdHJ1ZSwKICAicGhhc2UiOiAicHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEiLAogICJyZWxlYXNlX2lkZW50aXR5IjogewogICAgImZpbGVzIjogewogICAgICAiY29uZmlncy9zdHVkeS5qc29uIjogImI4NzJmMTIxYjZjZWYxMWE3NzI2ZmM1NmI0YzIzN2M3MTRmYWI0MWJiYWZhMmE2MDkwZDdlMmMxZTJmOTEzNTMiLAogICAgICAicHlwcm9qZWN0LnRvbWwiOiAiNzRkNjhiNWE4NjgyZGVhMDdlNGQ4NDg3NDllZDdmYmYyMzI0NDI2MmRlYjZmN2FmMWVjNzU4NWY2NjQ0ZTMzOCIsCiAgICAgICJyZXF1aXJlbWVudHMtbmFydmFsLnR4dCI6ICIxNjQ1ZDc3ZGU2YjcwNWExODI2N2JjNGU2MTU5NjViOGEzNjFiZjJmY2NiNzM4MzM1NGMyOWZiYjVmNTVjMzhiIiwKICAgICAgInNjcmlwdHMvYW1lbmRfcGlwZWxpbmUucHkiOiAiY2M3ZWVkMmMxYmI3M2Q5ZjhlY2FjODhjYTFkNWZlMTY1NzlkMTRhOTU4ZTNhOTQ2ZTAzYmFhZDg3NGNmMjFhNCIsCiAgICAgICJzY3JpcHRzL2Jvb3RzdHJhcF9uYXJ2YWwuc2giOiAiZDkyNjZhNDcwMTU4YjRlYzAwYTM0MGFlNjg0MGNiNjUwZjdmODIwYWQ4ODRjMWY2YjkwNTkzN2Y1ZjI2YzNlYiIsCiAgICAgICJzY3JpcHRzL2J1aWxkX2Jvb3RzdHJhcF9yZXF1ZXN0LnB5IjogIjljYTQ5N2ZhOWU0OWM1YWIxNGZkNGMxNWEyMDljNjEyMGMxYzljNWE5NGQzZmM5MDViNTllMTZiNjg0ODJhZDUiLAogICAgICAic2NyaXB0cy9idWlsZF9yZWxlYXNlLnB5IjogIjM5YTNmZTA1NDc0MmNkMWJiYjBiZmQ0NGI1MzIzZDYxYjQ5NTE2OTU0ZGY5MGEwOTljYTRlNmM0ZmRlZWE4YTYiLAogICAgICAic2NyaXB0cy9uYXJ2YWxfdHJhbnNwb3J0LnB5IjogImU4NDE0YTU1ZDgxNjY4NjMzNDQ0MTY3M2RkNmZhYTRkZDI3YjBlM2M2N2RlNDU0ZTI3NDgyYjkwYzM2MGQwZjciLAogICAgICAic2NyaXB0cy9xdWFsaWZ5X3IucHkiOiAiNzMxOWZiZDRjMGE5Y2JlZmVhY2Q4MDZiMWU3ZDE1ZDlhZmVjM2I3MjYwN2Y5NGM0ZWRlZTVjMDI5YmRiMDM1MSIsCiAgICAgICJzY3JpcHRzL3JlcGFpcl90cmFuc2Zlci5weSI6ICJkOTI2MmQwMzI0MDFhMmNmNDIwZGQ5NjI4OTcxZTQ0YWQ5ZDU4NWNlOWE4NWNhYmMzNjNkYmU0MzYxNGNmNzg5IiwKICAgICAgInNjcmlwdHMvcm5hX2NvdW50cy5SIjogIjk4MGY5YjgxMTlmYTFiMDI1YWMzMTZhZDQ0ZDM2NjgwMDllNzc4OTc1OGQ2NGU5ZDNjNWE5ODkyN2NkY2ZkNGMiLAogICAgICAic2NyaXB0cy9zdGFydF90cmFuc2ZlcnMucHkiOiAiZmVjMDkzOWU1MDhjN2VhNDQ3NzNjYWFlZmIzNTQyMWQ5NzE1ZDNlNDI4NTg5ODllMGU2NDNlNDUzMjYwZDA2ZiIsCiAgICAgICJzY3JpcHRzL3RyYW5zZmVyX3JuYV9jdXJsLnB5IjogIjk0MWE4ZTU3ZDNlOWJlMzdmZTQzYjNmNjRlZGI1NTRiODY3OTNiNTE4YzAwYTc5YjI0OTZhMTM3YmYwODQ0NWIiLAogICAgICAic2NyaXB0cy90cmFuc2Zlcl9ybmFfZnRwLnB5IjogIjAxYmYwM2RkNjViNjE1ZjVlMTUyYTBmNzZjMjFjNjUxZTkwNGM1NTI2ODE0YmUwMzgzNDc4M2M0ZDQ5MWQxZmQiLAogICAgICAic2NyaXB0cy90cmFuc2Zlcl9zb3VyY2VzLnB5IjogIjQ5MTYwMmY4MjY1NzI1MDY4ZGJmODc5YmY0MGUwMmI4MWRhMDVkOWQ4YWNlY2UwMzU3NGJkMTM0MGZlMjRkYzgiLAogICAgICAic3JjL2hvcnNlaGFpci9fX2luaXRfXy5weSI6ICJhNTIxYTVhMGY4ZDRkMzY0YjhhYzMyNGI4YTY2ZDA1NDRlMzViNzU1ODNmNTM3YWVlZWFjZGE3YTliOGM4MGZkIiwKICAgICAgInNyYy9ob3JzZWhhaXIvYWNxdWlyZS5weSI6ICJhOGI0YWUwY2Q4YjM5ZTJhNGVmYzgyY2FhYzk5YmM2ODUzYTRiYWNmNjc0N2NmNTQxOTY5NjYyNzY1ZDk1MmYzIiwKICAgICAgInNyYy9ob3JzZWhhaXIvYWRtaXNzaW9uLnB5IjogIjhkYWViZmQzZjVkZjAwYzU0YmZkMDFmYjA4MjFjYmI3N2FhNTc5MjFkYjUxYjE0Y2Q0M2UwMDYzNjdhMjFjNzEiLAogICAgICAic3JjL2hvcnNlaGFpci9hZ2VudF9hbmFseXNpcy5weSI6ICIyNWIyMWNmMTRhYmQ5NjkxMjc5ZTc3MGUxMmY3YjMxY2JkMThmMGJkMmFiZDZiMzk0OTI1ZGMyODc0ODBhZjA2IiwKICAgICAgInNyYy9ob3JzZWhhaXIvYWdlbnRzLnB5IjogImZiYzk4YmM1NzY1YjJkMWIyZmM5ZjQ2YjQ4YjEwNGRhMmNmYmE3YTIxMWU2OTZmN2IyM2UyMzc0NWU2N2FjMTQiLAogICAgICAic3JjL2hvcnNlaGFpci9hcmNoaXZlLnB5IjogIjJkMDQxMTljNTFiY2JjNTE0OTcxOGU5Zjk2M2E1MjNmYzRiNDM5ZmMwYzgzY2NlZWQ3NmIzZDEyZTdiYzA5ZjciLAogICAgICAic3JjL2hvcnNlaGFpci9iYXRjaGVkX2R1cmF0aW9uLnB5IjogImQ5NDIxZTMzZjc3ZTQ0YmQxN2E1OWRmMTMyZGVkZDkyMjM4NjNiOGEwZjE4NjAyNTg3NDQxYjk3YzI5YmM3MWEiLAogICAgICAic3JjL2hvcnNlaGFpci9iZWhhdmlvdXIucHkiOiAiNzZhN2E1MDhhYjAyMWFlNzhkODlmNWY1NzRiYjJmMjY3MTMzY2ExMDJkNTEwNzM2Y2IwZDAyODJhMWIzNzk0MCIsCiAgICAgICJzcmMvaG9yc2VoYWlyL2JlaGF2aW91cl93b3JrbG9hZC5weSI6ICI5ZjI4NWNiMDc0ZTY4OGI3ZmE4ZTNmOTQ5Mjg0MWMxMTU2N2JhOTAwM2IyNWRkZmY4NzljODBlNzhhZmFkOGYxIiwKICAgICAgInNyYy9ob3JzZWhhaXIvYnVkZ2V0LnB5IjogIjU5YzBlYWJhYWM0ZTYzMDc3YjNjNTY3ZDJlYzhmNjg5NTk0MGE0NjMwNWVhM2Q4MWY4ZGRmM2NlNDhiZjA3NDUiLAogICAgICAic3JjL2hvcnNlaGFpci9jbGkucHkiOiAiYzY2NzRlNDE2YTM5YmMxNGJhOWE3MWY2MjI0YjI4ZjAwZjFkM2NmYjljZWU1ZmEyNDRjYmUyZWY4YWQ0YTI2ZCIsCiAgICAgICJzcmMvaG9yc2VoYWlyL2NvcmUucHkiOiAiMWZhOGVhZGZiNzIwNmQyMWVhZDYxN2JkNWMwZWVlMjgxNDdkOGUyNzI3MWVhNTViYmFmMmI5NTdiOGJiYmE3MCIsCiAgICAgICJzcmMvaG9yc2VoYWlyL2luZmVyZW5jZS5weSI6ICI5YmQ5MjFhMjM4MDZiMGVhMjZmMWJlYWY0OWVmMGY5ZmM2MjY4YmRlMGZmNGI3NTQyOTUxMzZmODQxMjRmYjEzIiwKICAgICAgInNyYy9ob3JzZWhhaXIvaW50ZXJ2ZW50aW9ucy5weSI6ICIxOTM3OWE2OGI2ZjliYWUyYzM5NTA2NTg0ODQ1MDIwY2RiNTNlM2Q4Y2RmNjlhNTkzNDE3NTBiNGZjNGE5ZjQzIiwKICAgICAgInNyYy9ob3JzZWhhaXIvbWFudGlzX3JlY292ZXJ5LnB5IjogIjAyYTNmMWFjMzYxNjFjZGVkMjVhNGFmOGQ3MTA0ZTY2ZmU4NmY5M2ExODE4YTdhZjg2ZTA2MTU5NmNmNTgyYjMiLAogICAgICAic3JjL2hvcnNlaGFpci9tb2xlY3VsYXIucHkiOiAiZThmZWY2MzQzMzVjYjM3NzhmN2JlYjRjMTg3MWYxNjNmZjMyZjAzN2RkMmNmMDU1YjgxZTAxNDk1ZjQyNTM3OSIsCiAgICAgICJzcmMvaG9yc2VoYWlyL21vbGVjdWxhcl9waXBlbGluZS5weSI6ICI1OGM1OGQwNGViNTk5Y2M1ZDVjZjRmMWM1MTRkZGZjY2FlMzYzYmFmYTJjN2QxMmRhNGEwMzNhM2RhNGQyYmRjIiwKICAgICAgInNyYy9ob3JzZWhhaXIvcGlwZWxpbmUucHkiOiAiZWY5NTRmZTI0OWUzNzZlMGQ5MTFiNjRiNjdmYWUxNjI5MDIxNmI2MmYyZWZiOTNlY2FjZTI1NTk4ZmJjYzU3NSIsCiAgICAgICJzcmMvaG9yc2VoYWlyL3JlcGFpci5weSI6ICJiZTExZjYyMzk4NzVkMDZlYWEzNDkxOWM5NmY2OTJjMzMwMTQ1OTcwNzQ1M2YwZTc2ZDkzZjljMTI5NTgxNjE3IiwKICAgICAgInNyYy9ob3JzZWhhaXIvcmVwb3J0LnB5IjogIjVmZmNlNGM2ZWM2Nzk5MDFmYzYzYzU0ZTI0YWNjOTllODk5NzMxZWIyNTg2NjdiOGZhMjJiNmY1Njg2MDI3YTgiLAogICAgICAic3JjL2hvcnNlaGFpci9ybmFfZm9sbG93dXAucHkiOiAiODI3YjA4ZTkwYTZhODJhZDBlYjNlOWJiMzk1ZjU1MzYzNGIwYjM1NTBkOTE5MTkzZjUwNTFlZjJlYTI2MmRjYyIsCiAgICAgICJzcmMvaG9yc2VoYWlyL3JuYV9yZWNvdmVyeS5weSI6ICJlYzM1MGFhNDRkODZjNjYxYzQ5NTM0ZjcxYjIyMDdlM2JhZmJlNzM2NmJhMmI0MTk4NWZjOTdhZWY1NjY3NzFhIiwKICAgICAgInNyYy9ob3JzZWhhaXIvc2VtaW1hcmtvdi5weSI6ICJkOTk3YTM1OGI5NjhhOTI0MDgyZDQzZWFmM2Y0MTkyZDU3YzBmYTJmOGRmMGEzYWI0ZjFjNjc1YWJiY2UwYzFiIiwKICAgICAgInNyYy9ob3JzZWhhaXIvc2x1cm0ucHkiOiAiMTk0NzM1MjhkOTY4NGJiYmM5YTI0MWE2MjEyODNiMTA1YmViOGJmYTEzNTJmMTY4YmYyMWU2MjgzMjdlOGQzNyIsCiAgICAgICJzcmMvaG9yc2VoYWlyL3N0b3JhZ2VfYXJ0aWZhY3RzLnB5IjogImMyZTFjYmU3ODUwMjdmOTJmYjMwODMzYTlmZGZiNThlN2NkZWU5YzM5MDdhMWQyZWVmYjZkZWI4ZWZhMTE5NjgiLAogICAgICAidGVzdHMvY29uZnRlc3QucHkiOiAiMzViN2RlOGRjMWJmMTc0MzI1MjY0MzM0ZDMzN2IxNzFmNzI5NmMyNzU1MjRhOGM0NGQ5MDJmNDlkN2E1OTVlOCIsCiAgICAgICJ0ZXN0cy90ZXN0X2FjcXVpcmUucHkiOiAiNzliNWEzNTdkOTk2MzM0MjBjNTA0NmUwZDhkNTQwZjNiODZlMmViNWMyNzZjOTRjYjg2NDBiNGEwZDBhYjM4YSIsCiAgICAgICJ0ZXN0cy90ZXN0X2FkbWlzc2lvbi5weSI6ICJlNzk1ZGRkY2MyYWJjNzcxYmYyNzI3YTk2NDg4N2NmMzIzNDhkYmEyMTE4ZmNkMjY5MDU4NmU2MmEzNWE2MzlmIiwKICAgICAgInRlc3RzL3Rlc3RfYWdlbnRfYW5hbHlzaXMucHkiOiAiNjk5MjFkNjQxODFlYzQ2ZjY5YWZkNzViZTFlMTNkYzMxYWY3N2UxMGNkZWUxODAyOGNiMTU4NmNhZTE3NWNlMSIsCiAgICAgICJ0ZXN0cy90ZXN0X2FnZW50cy5weSI6ICJkNDQ0YzUyM2UxNzdlYTZkMDIwN2JhYzVmYThhOGRhZTFjZTdkODljYzgyNDUxOWQ2MDhlZjcwMWEwMjI1NjI0IiwKICAgICAgInRlc3RzL3Rlc3RfYXJjaGl2ZS5weSI6ICI1ZWMwYmVkYmQ2YWUyNjMyYzI0YmE0NTA5MmEzZTBmNjdmZGIyZmNkMTg2YmU0YzQyMDNmMjczNjM3MzgyNmY1IiwKICAgICAgInRlc3RzL3Rlc3RfYmF0Y2hlZF9kdXJhdGlvbi5weSI6ICJkMTM0ZDdkMmFjMzNiZjE0ODYzMTA0NzdlZDE1NmFhODkwNTM1NWY5MjQyZDAyNmE2YzJlNDY3MDFkNWUwOGQwIiwKICAgICAgInRlc3RzL3Rlc3RfYmVoYXZpb3VyLnB5IjogIjIzNDdhNTEyYmIxYmU4OWUzOTBlODQ3YjNkN2FhOTFlNTZhNzg3Mzk5ZDYxNGYwYjNmYzlkMTlmZGUyYWE0YzAiLAogICAgICAidGVzdHMvdGVzdF9iZWhhdmlvdXJfd29ya2xvYWQucHkiOiAiNWE2Mjg1YTUwYzQ0YWU4YjcyNjhhMDUwMGY1YmYxMGVjYjk0ZTgwNDg4OWY1OTIyZjcyMTgzYjg3YjRmMzVkYiIsCiAgICAgICJ0ZXN0cy90ZXN0X2J1ZGdldC5weSI6ICI5ZWE3MzQ2ZTk4MjAzYmMwOTgxMjE2ODlmN2E0YWM0YTQ1YzJlZjQ4OGJkMjQ3MmQ4OTRjYzBiZWVlMGQ2NWRlIiwKICAgICAgInRlc3RzL3Rlc3RfY29yZS5weSI6ICJmZjVlZTQ0MWU1NWNkOWIyYjYwOGUyNDZiZmMzNzkwNzBjNjE2NTQzMzM0YjE4ZDFmOWM3NzliYzBkYmMzZjRiIiwKICAgICAgInRlc3RzL3Rlc3RfZGVwbG95bWVudC5weSI6ICI5MWM5OThkMzZmYWViODdkYjNmNzBmNDdlODBiYjIyYWI4ZmQyYzBhMGUzYzljMWMwNGIzZmFiNGI5MDg2YTEyIiwKICAgICAgInRlc3RzL3Rlc3RfaW5mZXJlbmNlLnB5IjogIjhjZDc1NTMzYjU5MGM4ZjQ1NWI5OWJmZTkzNmNhMzJhMjE5MDVkZjY1ZGM5MjVjOTI2NTM4M2NhNjFkZTE2NWIiLAogICAgICAidGVzdHMvdGVzdF9tYW50aXNfcmVjb3ZlcnkucHkiOiAiYjA4OGY1NjQ0YWRlMmRkZTliYTMxOGUxMDE0YWY1ZTNhNTBhNjc2NzUxNGE2Yjc0MjI0NGUzMDcwZTBjMGJhZiIsCiAgICAgICJ0ZXN0cy90ZXN0X21hbnRpc19yZXBhaXIucHkiOiAiMDdjNDk5ZDlmZDBkZmVlMDE3ZTZlYTgwZWY1NDliZmFhZTBlMDFlODJjNWE3OTRkYjBlOTM2N2NiOWVhYjNkMiIsCiAgICAgICJ0ZXN0cy90ZXN0X21vbGVjdWxhci5weSI6ICIwY2YwNGY0NWIwYTk5ZTFmMjFmZDhjNGQyZTlhMWJjOGU1OTBiZGFkZDVhMzRmNGViYzEyMTg3ZTMxZWE4Mjg3IiwKICAgICAgInRlc3RzL3Rlc3RfbW9sZWN1bGFyX3BpcGVsaW5lLnB5IjogImIxNzA2MWYxYmExZTA0NGU5MDBhZjEyMDhiNjBhOGYxYWQ1NGE3NTNjYTBkMGU3ZGQ2NDY5ZmRlNjAyYTMxMDYiLAogICAgICAidGVzdHMvdGVzdF9waXBlbGluZS5weSI6ICI1ZTBjMDUyYzU5Njc2NWEzMmZmNDQ0M2Q4NjY4YmEzYjYyN2RiZWIxZDdmN2Y5NTQ0NzkwNDhiYmM1YTE0OTM2IiwKICAgICAgInRlc3RzL3Rlc3RfcXVhbGlmaWNhdGlvbi5weSI6ICJjZGJlZDg2ZGFkMmRjOWQ0ODE1ZDE3ZWEwZmY0ODUxZDQ1MGY5ZDcxZjA3NmE3NjVmYzM0MWUyYTNlY2EyMzhhIiwKICAgICAgInRlc3RzL3Rlc3RfcmVwYWlyLnB5IjogImQ3YjczMTY1Nzk4Njg3ZjhhNTZjYjIyNTg4N2RkNzgyZTkwYTEyNjczMzM2M2I2YmQxNDAwZjZhZDBlZGJhOTYiLAogICAgICAidGVzdHMvdGVzdF9yZXBvcnQucHkiOiAiNjliNDVjNGU2OWNlODQyMzcyMzc3MjAwNGFhNWM0Yjk5MDA2ZTI2ZWY1MmVjNDQ0MDQ5ODUwYjljNTU0Y2ZlZiIsCiAgICAgICJ0ZXN0cy90ZXN0X3JuYV9mb2xsb3d1cC5weSI6ICJlZTVjMDIwYzkxMjI4MDM2YTc5YzEyMWI5NmI5MTY0Mjc3ZjcwOGU0NWFhOTFjYmFhOTUzMDY0OTdhNTQ1YmM5IiwKICAgICAgInRlc3RzL3Rlc3Rfcm5hX3JlY292ZXJ5LnB5IjogImY1NzNhN2I1MTRmMGNmOGQ1NzAxZjFlYmNmODQ1NGZlODMxMWRmZDEyMDU4NWNiZWI1MzdhNjgyYjkwZjM0OWQiLAogICAgICAidGVzdHMvdGVzdF9ybmFfcmVjb3ZlcnlfcmVnaXN0cmF0aW9uLnB5IjogIjJiMzQxYzU0YTlhNDA4ZjI2YTgxNDNkYjc3YThkMmJlN2Q5YjQ4MWE1YTBhNTRiMjk2ZjE3MTJhMzQ1MDZiYWEiLAogICAgICAidGVzdHMvdGVzdF9ybmFfcmVjb3Zlcnlfc3RhYmlsaXR5LnB5IjogIjU0M2IzY2VhM2I0YWQ0YzU3NmEwZjYzMGMyOTRlNjkxZTE0OThiOWI2YzUwNGY4NGE0ODNjMzdiOTk0YzI0ZTciLAogICAgICAidGVzdHMvdGVzdF9zZW1pbWFya292LnB5IjogIjExM2VkOTNjOWMwZWUxZjA4YmMyYWVhNjI0ZjZiMGFiOTg0ZTE0MmJlODIxODFhNDE3ZmNiNjJiY2MxMTcwMzAiLAogICAgICAidGVzdHMvdGVzdF9zbHVybS5weSI6ICJiODA1ZmU5YjU4Yzc2NWI4ZmRkMTZmNmY2ZWZhN2RjZTQ1YWI1N2M4ZTY2NTlhMTkwY2IxMjc2M2Y3ZDRjZTVlIiwKICAgICAgInRlc3RzL3Rlc3Rfc2x1cm1fZXhjbHVzaW9ucy5weSI6ICI3YmUwZTU2Yjc3NjM1MTRkOWVlNTdjZWMyOTYzODdhYTI4YzNjMjFjZWRmMzE4ZmQwZWE1YzMwY2NkOTlmZWI5IiwKICAgICAgInRlc3RzL3Rlc3Rfc2x1cm1fcGxhY2VtZW50LnB5IjogImU5MjE0NTE5ZmY2MjZkMGRhYTQyZjcxMmNmNzc5MjMwMzg3MzRkY2E3MjZlZDQyZTkxMDhlZDA0ZWE1ODA5ODAiLAogICAgICAidGVzdHMvdGVzdF9zb3VyY2VfcmVwbGljYXRpb24ucHkiOiAiNzBjZTA3M2FiYjFhZDU2ZGJhZGQwMzY5MDM5ZmFlZTI3Njg3MzFlYTQwMDJmYzYzMTBkMzg3ZjA1ZjAzNjBiZiIsCiAgICAgICJ0ZXN0cy90ZXN0X3N0b3JhZ2VfYXJ0aWZhY3RfY2FjaGUucHkiOiAiMTk5MjFiZWVlNzdiZWVhNjJjNjQ5ZTM0ZjBhMTI5YmRkMDdhMDI1NWM5ZDFjNjVmN2NjNzc5ZTU2YzhkOTA2NSIsCiAgICAgICJ0ZXN0cy90ZXN0X3N0b3JhZ2VfYXJ0aWZhY3RzLnB5IjogImE2ZWMyZmI5NDMyM2Y0Mzg4ZWM3ODYyYTdjMzM0YzRiYjJmMGQ1NmRmYmU1YmNjNWY4ZTk5M2M4MTZmMmMxMmIiLAogICAgICAidGVzdHMvdGVzdF9zdG9yYWdlX2JvdW5kYXJ5X2NhY2hlLnB5IjogIjQ5NDAwOTYxMTQ3ZjY1ZjI2ZjJlN2Y2NTk2ZTBmMjgyZDUwNDk4ZWI0NTkwMzU2ZDM5ZGZjNDRiZTVmZmZlYTYiLAogICAgICAidGVzdHMvdGVzdF9zdG9yYWdlX3NuYXBzaG90X2FydGlmYWN0cy5weSI6ICI1OTFkNzAwYjc5NjhkYjI0OGVmYmI3YWY0NmVkMjhkYmQ5ZWY2YjNmZjNmZmYxNjU5YzU1YzVlMGRlY2RiZDI4IiwKICAgICAgInRlc3RzL3Rlc3Rfc3RvcmFnZV9zbmFwc2hvdF9ib3VuZGFyeS5weSI6ICI4MzFlODIwNTRhMTJhYTI4Y2JkYjUwZjI0ZjEzNWZhMTZkNTkwZTgwNjU0NmUzODliNzYwODU4NDhmNTY1M2YyIiwKICAgICAgInRlc3RzL3Rlc3RfdHJhbnNmZXJfcm5hX2N1cmwucHkiOiAiMjlhNjU4ZWY2ZmY4NmY3M2MxZTdiMjM4NTMwYWU3NTRhZmEwMzZhM2EyNjRiNDdkZTg2NGJmOGNhZjY1ODg3YyIsCiAgICAgICJ0ZXN0cy90ZXN0X3RyYW5zZmVyX3JuYV9mdHAucHkiOiAiMjRlNjRlMGUyZjU3MmM2ZTcwNTZhMTMyNmIzYjhmNjUyY2Y1MDI3OTk1N2YwOWNhYjZhYTY5YThmY2MxOWE1MyIKICAgIH0sCiAgICAic2hhMjU2IjogImQ4OWIzMTE4MGQ0YzRmZmU5YzQwMjNkYzU1MzRiOTZmMmQxNTdhM2E4ZTNhZWNhZmYzMDg0OTI4M2QwMGE3MDUiCiAgfSwKICAicmVzb3VyY2VfcG9saWN5IjogewogICAgImFwcHJvdmFsIjogewogICAgICAiYXBwcm92ZWRfYXRfdXRjIjogIjIwMjYtMTAtMjFUMTg6MDY6MTIuOTY1NjE5KzAwOjAwIiwKICAgICAgImF1dGhvcml6ZXNfY3B1X2dwdV9yZXNvdXJjZV9hbWVuZG1lbnQiOiB0cnVlLAogICAgICAiZXZpZGVuY2VfcmVmZXJlbmNlIjogIkN1cnJlbnQgdXNlciBpbnN0cnVjdGlvbiwgMjEgT2N0b2JlciAyMDI2LCBjb21wbGV0ZSBBQkNERUYgYW5kIGxpZnQgQ1BVL0dQVSByZXN0cmljdGlvbnMiLAogICAgICAiaWRlbnRpdHlfdmVyaWZpY2F0aW9uIjogImluc3RydWN0aW9uX3Byb3ZlbmFuY2Vfb25seSIsCiAgICAgICJzb3VyY2UiOiAiY3VycmVudF91c2VyIiwKICAgICAgInN0YXRlbWVudCI6ICJJIHdhbnQgeW91IHRvIGNvbXBsZXRlIEFCQ0RFRi4geW91IGNhbiBsaWZ0IHRoZSByZXN0cmljdGlvbnMgb24gY3B1IG9yIGdwdS4gZm9yIGFueSB1bnN1cHBvcnRlZCBzdHVmZiwgdHJ5IHRvIGNyZWF0ZSBwb3NzaWJpbGl0aWVzIG9mIGNvbXBsZXRpbmcgdGhlbSwgaW5jbHVkaW5nIGJ1dCBub3QgbGltaXRlZCB0byBuZXcgZGF0YSBhY3F1aXNpdGlvbiBldGMuIgogICAgfSwKICAgICJhdXRob3JpdHlfYmFzaXMiOiAiUmVjb3JkZWQgY3VycmVudC11c2VyIGluc3RydWN0aW9uIHByb3ZlbmFuY2U7IG5vdCBjcnlwdG9ncmFwaGljIGlkZW50aXR5IHZlcmlmaWNhdGlvbiIsCiAgICAiYmFzZV9jb25maWdfc2hhMjU2IjogIjQ0NmI1ZDYyZmMyMDZjZjEzOTJiOTBmZjljZjE2YWRhZmM2NmM1N2E2ZDM4ZTg0YzAyOWNkM2IyZWJiZjFmMzUiLAogICAgImV4ZWN1dGlvbl9zb3VyY2Vfc2hhMjU2IjogIjRmMzkwM2Q3YzI2ZTMzNGE3NWM4ZjdmODFhZDhiODY1ZmFjNGU1YTQ0N2ZmODNhOGJmZWFlODc3NDg5MWQ5ZDEiLAogICAgImxpbWl0cyI6IHsKICAgICAgImNwdV9jZWlsaW5nIjogMjU2LAogICAgICAiY3B1X2NvcmVfaG91cnNfY2FwIjogbnVsbCwKICAgICAgImdwdV9jb25jdXJyZW5jeSI6IDQsCiAgICAgICJncHVfaG91cnNfY2FwIjogbnVsbCwKICAgICAgIm1heF9uYXRpdmVfaG91cnMiOiAxNjgsCiAgICAgICJzY3JhdGNoX2J1ZGdldF9ieXRlcyI6IDYwMDAwMDAwMDAwMDAsCiAgICAgICJzY3JhdGNoX2J1ZGdldF9maWxlcyI6IDI1MDAwMCwKICAgICAgInNjcmF0Y2hfcmVzZXJ2ZV9ieXRlcyI6IDIwMDAwMDAwMDAwMDAKICAgIH0sCiAgICAicGF0aCI6ICIvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2UvZXhlY3V0aW9uLXJlc291cmNlLXBvbGljaWVzL2FmLWV4cGFuZGVkLTIwMjYxMDIxLXYxL3BvbGljeS5qc29uIiwKICAgICJwb2xpY3lfaWQiOiAiYWYtZXhwYW5kZWQtMjAyNjEwMjEtdjEiLAogICAgInByaW1hcnlfc2NpZW50aWZpY19zb3VyY2Vfc2hhMjU2IjogIjBlMmIxZWJjZThiZmZlMTA3OTk0NzBiM2FjMjA4MjhlNjIxZjE0MTVmM2ExOGJjYThjZDY3ZDUwMmYxZTg2MzAiLAogICAgInNjaWVudGlmaWNfZ2F0ZXNfY2hhbmdlZCI6IGZhbHNlLAogICAgInNoYTI1NiI6ICJmNDQ0NWZiNzJkNmEwZmQ4ZTYyZmI5YTNjNWVmNDNhNmE2YjFlN2JmNWEzNjkyNTRmZjQwZDYxN2U5OTJiNGU1IgogIH0sCiAgInJvb3QiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtIiwKICAic2NoZW1hX3ZlcnNpb24iOiAyLAogICJzY2llbnRpZmljX2dhdGVzX2NoYW5nZWQiOiBmYWxzZSwKICAidGFza3MiOiBbCiAgICB7CiAgICAgICJjb21tYW5kIjogIi9ob21lL3B3YTIwOS9oaHctZW52aXJvbm1lbnQvYmluL3B5dGhvbiAtcyAtdSAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxLTIwMjYxMDIzL3J1bi1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy12MS0yMDI2MTAyMy5weSAtLW1vZGUgcnVuIC0tcG9saWN5LWRlc2NyaXB0b3IgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWhhbmRvZmYtdjItMjAyNjEwMjMvbmF0aXZlLTY4NjgxNzYvcnVuLXBvbGljeS5qc29uIC0tcG9saWN5LWRlc2NyaXB0b3Itc2hhMjU2IGFkNzBhMTQwY2RkNTAzODRmODYwY2I0YWRjMmRjNGZjMzhmZWIwYTdlYWE2MGJlMjQwMzQ1NjM3M2M5YjIyNjcgLS1uYXRpdmUtcGxhbiAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcGxhbi5qc29uIC0tbmF0aXZlLWxlZGdlciAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vc3RhdGUvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEtc3VibWlzc2lvbi5qc29uIC0tbmF0aXZlLXRhc2sgcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wIC0tbmF0aXZlLXNjcmlwdCAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wLnNiYXRjaCAtLXNlZ21lbnQgMCIsCiAgICAgICJkZXBlbmRlbmN5X2tpbmQiOiAiYWZ0ZXJvayIsCiAgICAgICJkZXBlbmRzIjogW10sCiAgICAgICJleHRlcm5hbF9kZXBlbmRlbmNpZXMiOiBbCiAgICAgICAgIjY4NjgxNzYiLAogICAgICAgICI2ODY0NDkxIgogICAgICBdLAogICAgICAibmFtZSI6ICJwcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTAiLAogICAgICAicHJpbWFyeV9leHRlcm5hbF9jZW5zdXMiOiB7CiAgICAgICAgImhlbHBlciI6IHsKICAgICAgICAgICJwYXRoIjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9tYWludGVuYW5jZS9zdHVkeS1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1wb2xpY3ktdjEtMjAyNjEwMjMvcnVuLXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLXYxLTIwMjYxMDIzLnB5IiwKICAgICAgICAgICJzaGEyNTYiOiAiZGQ1ZjhkMmFlYzkwODk2ZmRjZmUxOTA5NzFlYThiNmMzMmE5NDE1OGEzNjg0NTY2YzJiODQwNTc0OGU3Yzc5NCIKICAgICAgICB9LAogICAgICAgICJtb2RlIjogInJ1biIsCiAgICAgICAgInBvbGljeV9kZXNjcmlwdG9yIjogewogICAgICAgICAgInBhdGgiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWhhbmRvZmYtdjItMjAyNjEwMjMvbmF0aXZlLTY4NjgxNzYvcnVuLXBvbGljeS5qc29uIiwKICAgICAgICAgICJzaGEyNTYiOiAiYWQ3MGExNDBjZGQ1MDM4NGY4NjBjYjRhZGMyZGM0ZmMzOGZlYjBhN2VhYTYwYmUyNDAzNDU2MzczYzliMjI2NyIKICAgICAgICB9CiAgICAgIH0sCiAgICAgICJyZXNvdXJjZXMiOiB7CiAgICAgICAgImFycmF5IjogbnVsbCwKICAgICAgICAiY3B1cyI6IDEsCiAgICAgICAgImdwdSI6IGZhbHNlLAogICAgICAgICJob3VycyI6IDcyLAogICAgICAgICJtZW1vcnkiOiAiNjRHIgogICAgICB9LAogICAgICAic2NyaXB0IjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9wbGFucy9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVycy12MS9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTAuc2JhdGNoIiwKICAgICAgInNjcmlwdF9zaGEyNTYiOiAiYjE3OTJmOWQ0ZTJlYTZhMzY2MzdkYWJjMDc5YTk2MjUxMGI4MGQyM2E4MzIyOTM2NTY5NGI3NzA0YTc3MjE1MSIKICAgIH0sCiAgICB7CiAgICAgICJjb21tYW5kIjogIi9ob21lL3B3YTIwOS9oaHctZW52aXJvbm1lbnQvYmluL3B5dGhvbiAtcyAtdSAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxLTIwMjYxMDIzL3J1bi1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy12MS0yMDI2MTAyMy5weSAtLW1vZGUgcnVuIC0tcG9saWN5LWRlc2NyaXB0b3IgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWhhbmRvZmYtdjItMjAyNjEwMjMvbmF0aXZlLTY4NjgxNzYvcnVuLXBvbGljeS5qc29uIC0tcG9saWN5LWRlc2NyaXB0b3Itc2hhMjU2IGFkNzBhMTQwY2RkNTAzODRmODYwY2I0YWRjMmRjNGZjMzhmZWIwYTdlYWE2MGJlMjQwMzQ1NjM3M2M5YjIyNjcgLS1uYXRpdmUtcGxhbiAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcGxhbi5qc29uIC0tbmF0aXZlLWxlZGdlciAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vc3RhdGUvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEtc3VibWlzc2lvbi5qc29uIC0tbmF0aXZlLXRhc2sgcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0xIC0tbmF0aXZlLXNjcmlwdCAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0xLnNiYXRjaCAtLXNlZ21lbnQgMSIsCiAgICAgICJkZXBlbmRlbmN5X2tpbmQiOiAiYWZ0ZXJhbnkiLAogICAgICAiZGVwZW5kcyI6IFsKICAgICAgICAicHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wIgogICAgICBdLAogICAgICAiZXh0ZXJuYWxfZGVwZW5kZW5jaWVzIjogW10sCiAgICAgICJuYW1lIjogInByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMSIsCiAgICAgICJwcmltYXJ5X2V4dGVybmFsX2NlbnN1cyI6IHsKICAgICAgICAiaGVscGVyIjogewogICAgICAgICAgInBhdGgiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLXBvbGljeS12MS0yMDI2MTAyMy9ydW4tcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtdjEtMjAyNjEwMjMucHkiLAogICAgICAgICAgInNoYTI1NiI6ICJkZDVmOGQyYWVjOTA4OTZmZGNmZTE5MDk3MWVhOGI2YzMyYTk0MTU4YTM2ODQ1NjZjMmI4NDA1NzQ4ZTdjNzk0IgogICAgICAgIH0sCiAgICAgICAgIm1vZGUiOiAicnVuIiwKICAgICAgICAicG9saWN5X2Rlc2NyaXB0b3IiOiB7CiAgICAgICAgICAicGF0aCI6ICIvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtaGFuZG9mZi12Mi0yMDI2MTAyMy9uYXRpdmUtNjg2ODE3Ni9ydW4tcG9saWN5Lmpzb24iLAogICAgICAgICAgInNoYTI1NiI6ICJhZDcwYTE0MGNkZDUwMzg0Zjg2MGNiNGFkYzJkYzRmYzM4ZmViMGE3ZWFhNjBiZTI0MDM0NTYzNzNjOWIyMjY3IgogICAgICAgIH0KICAgICAgfSwKICAgICAgInJlc291cmNlcyI6IHsKICAgICAgICAiYXJyYXkiOiBudWxsLAogICAgICAgICJjcHVzIjogMSwKICAgICAgICAiZ3B1IjogZmFsc2UsCiAgICAgICAgImhvdXJzIjogNzIsCiAgICAgICAgIm1lbW9yeSI6ICI2NEciCiAgICAgIH0sCiAgICAgICJzY3JpcHQiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL3BsYW5zL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXJzLXYxL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMS5zYmF0Y2giLAogICAgICAic2NyaXB0X3NoYTI1NiI6ICIxZDQzZDdjZjkwODQ2YzJjMTEzNzY0YjAzMjJhMzE1NGFiZTkyZGVhNjZmODg3ZGM4M2U5YWI4OWE3MWVhNGM3IgogICAgfSwKICAgIHsKICAgICAgImNvbW1hbmQiOiAiL2hvbWUvcHdhMjA5L2hody1lbnZpcm9ubWVudC9iaW4vcHl0aG9uIC1zIC11IC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9tYWludGVuYW5jZS9zdHVkeS1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1wb2xpY3ktdjEtMjAyNjEwMjMvcnVuLXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLXYxLTIwMjYxMDIzLnB5IC0tbW9kZSBydW4gLS1wb2xpY3ktZGVzY3JpcHRvciAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtaGFuZG9mZi12Mi0yMDI2MTAyMy9uYXRpdmUtNjg2ODE3Ni9ydW4tcG9saWN5Lmpzb24gLS1wb2xpY3ktZGVzY3JpcHRvci1zaGEyNTYgYWQ3MGExNDBjZGQ1MDM4NGY4NjBjYjRhZGMyZGM0ZmMzOGZlYjBhN2VhYTYwYmUyNDAzNDU2MzczYzliMjI2NyAtLW5hdGl2ZS1wbGFuIC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9wbGFucy9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVycy12MS9wbGFuLmpzb24gLS1uYXRpdmUtbGVkZ2VyIC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9zdGF0ZS9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVycy12MS1zdWJtaXNzaW9uLmpzb24gLS1uYXRpdmUtdGFzayBwcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTIgLS1uYXRpdmUtc2NyaXB0IC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9wbGFucy9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVycy12MS9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTIuc2JhdGNoIC0tc2VnbWVudCAyIiwKICAgICAgImRlcGVuZGVuY3lfa2luZCI6ICJhZnRlcmFueSIsCiAgICAgICJkZXBlbmRzIjogWwogICAgICAgICJwcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTEiCiAgICAgIF0sCiAgICAgICJleHRlcm5hbF9kZXBlbmRlbmNpZXMiOiBbXSwKICAgICAgIm5hbWUiOiAicHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0yIiwKICAgICAgInByaW1hcnlfZXh0ZXJuYWxfY2Vuc3VzIjogewogICAgICAgICJoZWxwZXIiOiB7CiAgICAgICAgICAicGF0aCI6ICIvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxLTIwMjYxMDIzL3J1bi1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy12MS0yMDI2MTAyMy5weSIsCiAgICAgICAgICAic2hhMjU2IjogImRkNWY4ZDJhZWM5MDg5NmZkY2ZlMTkwOTcxZWE4YjZjMzJhOTQxNThhMzY4NDU2NmMyYjg0MDU3NDhlN2M3OTQiCiAgICAgICAgfSwKICAgICAgICAibW9kZSI6ICJydW4iLAogICAgICAgICJwb2xpY3lfZGVzY3JpcHRvciI6IHsKICAgICAgICAgICJwYXRoIjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9tYWludGVuYW5jZS9zdHVkeS1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1oYW5kb2ZmLXYyLTIwMjYxMDIzL25hdGl2ZS02ODY4MTc2L3J1bi1wb2xpY3kuanNvbiIsCiAgICAgICAgICAic2hhMjU2IjogImFkNzBhMTQwY2RkNTAzODRmODYwY2I0YWRjMmRjNGZjMzhmZWIwYTdlYWE2MGJlMjQwMzQ1NjM3M2M5YjIyNjciCiAgICAgICAgfQogICAgICB9LAogICAgICAicmVzb3VyY2VzIjogewogICAgICAgICJhcnJheSI6IG51bGwsCiAgICAgICAgImNwdXMiOiAxLAogICAgICAgICJncHUiOiBmYWxzZSwKICAgICAgICAiaG91cnMiOiA3MiwKICAgICAgICAibWVtb3J5IjogIjY0RyIKICAgICAgfSwKICAgICAgInNjcmlwdCI6ICIvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0yLnNiYXRjaCIsCiAgICAgICJzY3JpcHRfc2hhMjU2IjogImY2ZTA0ZGFmY2ZjOTc2ZmJkNWE3MmQzZDE5NGFmOThjNTE3NWI0YjYzZGU1NGNmMmM4YTkwMmYwOGI5ZDIyNTMiCiAgICB9LAogICAgewogICAgICAiY29tbWFuZCI6ICIvaG9tZS9wd2EyMDkvaGh3LWVudmlyb25tZW50L2Jpbi9weXRob24gLXMgLXUgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLXBvbGljeS12MS0yMDI2MTAyMy9ydW4tcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtdjEtMjAyNjEwMjMucHkgLS1tb2RlIHJ1biAtLXBvbGljeS1kZXNjcmlwdG9yIC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9tYWludGVuYW5jZS9zdHVkeS1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1oYW5kb2ZmLXYyLTIwMjYxMDIzL25hdGl2ZS02ODY4MTc2L3J1bi1wb2xpY3kuanNvbiAtLXBvbGljeS1kZXNjcmlwdG9yLXNoYTI1NiBhZDcwYTE0MGNkZDUwMzg0Zjg2MGNiNGFkYzJkYzRmYzM4ZmViMGE3ZWFhNjBiZTI0MDM0NTYzNzNjOWIyMjY3IC0tbmF0aXZlLXBsYW4gL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL3BsYW5zL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXJzLXYxL3BsYW4uanNvbiAtLW5hdGl2ZS1sZWRnZXIgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL3N0YXRlL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXJzLXYxLXN1Ym1pc3Npb24uanNvbiAtLW5hdGl2ZS10YXNrIHByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMyAtLW5hdGl2ZS1zY3JpcHQgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL3BsYW5zL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXJzLXYxL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMy5zYmF0Y2ggLS1zZWdtZW50IDMiLAogICAgICAiZGVwZW5kZW5jeV9raW5kIjogImFmdGVyYW55IiwKICAgICAgImRlcGVuZHMiOiBbCiAgICAgICAgInByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMiIKICAgICAgXSwKICAgICAgImV4dGVybmFsX2RlcGVuZGVuY2llcyI6IFtdLAogICAgICAibmFtZSI6ICJwcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTMiLAogICAgICAicHJpbWFyeV9leHRlcm5hbF9jZW5zdXMiOiB7CiAgICAgICAgImhlbHBlciI6IHsKICAgICAgICAgICJwYXRoIjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9tYWludGVuYW5jZS9zdHVkeS1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1wb2xpY3ktdjEtMjAyNjEwMjMvcnVuLXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLXYxLTIwMjYxMDIzLnB5IiwKICAgICAgICAgICJzaGEyNTYiOiAiZGQ1ZjhkMmFlYzkwODk2ZmRjZmUxOTA5NzFlYThiNmMzMmE5NDE1OGEzNjg0NTY2YzJiODQwNTc0OGU3Yzc5NCIKICAgICAgICB9LAogICAgICAgICJtb2RlIjogInJ1biIsCiAgICAgICAgInBvbGljeV9kZXNjcmlwdG9yIjogewogICAgICAgICAgInBhdGgiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWhhbmRvZmYtdjItMjAyNjEwMjMvbmF0aXZlLTY4NjgxNzYvcnVuLXBvbGljeS5qc29uIiwKICAgICAgICAgICJzaGEyNTYiOiAiYWQ3MGExNDBjZGQ1MDM4NGY4NjBjYjRhZGMyZGM0ZmMzOGZlYjBhN2VhYTYwYmUyNDAzNDU2MzczYzliMjI2NyIKICAgICAgICB9CiAgICAgIH0sCiAgICAgICJyZXNvdXJjZXMiOiB7CiAgICAgICAgImFycmF5IjogbnVsbCwKICAgICAgICAiY3B1cyI6IDEsCiAgICAgICAgImdwdSI6IGZhbHNlLAogICAgICAgICJob3VycyI6IDcyLAogICAgICAgICJtZW1vcnkiOiAiNjRHIgogICAgICB9LAogICAgICAic2NyaXB0IjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9wbGFucy9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVycy12MS9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTMuc2JhdGNoIiwKICAgICAgInNjcmlwdF9zaGEyNTYiOiAiYTM0MzNjM2FmY2UyYjAzNjA5OGY5MTg1ZTUyMmU1OTM4YjQxYTlhZTAxMjBlNGRhZmQyZjg5ZWQzZWJiOWFlNiIKICAgIH0KICBdCn0K', 'sha256': '6a33e4ee89f4f4ef380e712aec7d488bdd7b5acfbb39d28301cabcb7c7e7cdf3'}, '/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v1/primary-external-census-controller-0.sbatch': {'base64': 'IyEvYmluL2Jhc2gKI1NCQVRDSCAtLWFjY291bnQ9ZGVmLXB0ZXdhcmllX2NwdQojU0JBVENIIC0tam9iLW5hbWU9aGh3LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMAojU0JBVENIIC0tY3B1cy1wZXItdGFzaz0xCiNTQkFUQ0ggLS1tZW09NjRHCiNTQkFUQ0ggLS10aW1lPTcyOjAwOjAwCiNTQkFUQ0ggLS1vdXRwdXQ9L3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL2xvZ3MvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wLSVBXyVhLmxvZwojU0JBVENIIC0taG9sZApzZXQgLWV1byBwaXBlZmFpbApleHBvcnQgVFo9VVRDCmV4cG9ydCBQWVRIT05ET05UV1JJVEVCWVRFQ09ERT0xCnRlc3QgIiQoaWQgLXVuKSIgPSBwd2EyMDkKbW9kdWxlIGxvYWQgU3RkRW52LzIwMjMgcHl0aG9uLzMuMTEuNSBjdWRhLzEyLjYgYXJyb3cvMTkuMC4xCnNvdXJjZSAvaG9tZS9wd2EyMDkvaGh3LWVudmlyb25tZW50L2Jpbi9hY3RpdmF0ZQpjZCAvbHVzdHJlMDYvcHJvamVjdC82MTAxODU0L3B3YTIwOS9ob3JzZWhhaXItd29ybS9yZWxlYXNlcy8yMDI2MTAyMS01N2UwZWI4YzRjNmMtZDg5YjMxMTgwZDRjCmV4cG9ydCBQWVRIT05QQVRIPS9sdXN0cmUwNi9wcm9qZWN0LzYxMDE4NTQvcHdhMjA5L2hvcnNlaGFpci13b3JtL3JlbGVhc2VzLzIwMjYxMDIxLTU3ZTBlYjhjNGM2Yy1kODliMzExODBkNGMvc3JjOiIke1BZVEhPTlBBVEg6LX0iCmV4cG9ydCBNUExCQUNLRU5EPUFnZwpleHBvcnQgT01QX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE9QRU5CTEFTX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE1LTF9OVU1fVEhSRUFEUz0ke1NMVVJNX0NQVVNfUEVSX1RBU0t9CmV4cG9ydCBQWVRIT05VTkJVRkZFUkVEPTEKZXhwb3J0IENVQkxBU19XT1JLU1BBQ0VfQ09ORklHPTo0MDk2OjgKZXhwb3J0IEhIV19HUFVfQ09VTlQ9MApleHBvcnQgSEhXX01BWF9IT1VSUz03MgpweXRob24gLW0gaG9yc2VoYWlyLmNsaSAtLWNvbmZpZyBjb25maWdzL3N0dWR5Lmpzb24gLS1yb290IC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybSB2ZXJpZnktc291cmNlIC0taWRlbnRpdHkgcmVsZWFzZS5qc29uCi9ob21lL3B3YTIwOS9oaHctZW52aXJvbm1lbnQvYmluL3B5dGhvbiAtcyAtdSAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxLTIwMjYxMDIzL3J1bi1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy12MS0yMDI2MTAyMy5weSAtLW1vZGUgcnVuIC0tcG9saWN5LWRlc2NyaXB0b3IgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWhhbmRvZmYtdjItMjAyNjEwMjMvbmF0aXZlLTY4NjgxNzYvcnVuLXBvbGljeS5qc29uIC0tcG9saWN5LWRlc2NyaXB0b3Itc2hhMjU2IGFkNzBhMTQwY2RkNTAzODRmODYwY2I0YWRjMmRjNGZjMzhmZWIwYTdlYWE2MGJlMjQwMzQ1NjM3M2M5YjIyNjcgLS1uYXRpdmUtcGxhbiAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcGxhbi5qc29uIC0tbmF0aXZlLWxlZGdlciAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vc3RhdGUvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEtc3VibWlzc2lvbi5qc29uIC0tbmF0aXZlLXRhc2sgcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wIC0tbmF0aXZlLXNjcmlwdCAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0wLnNiYXRjaCAtLXNlZ21lbnQgMAo=', 'sha256': 'fbe69f82abce43e936a70be0aeb27835e7bb915fadb1cef59b1c06d2d39b4938'}, '/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v1/primary-external-census-controller-1.sbatch': {'base64': 'IyEvYmluL2Jhc2gKI1NCQVRDSCAtLWFjY291bnQ9ZGVmLXB0ZXdhcmllX2NwdQojU0JBVENIIC0tam9iLW5hbWU9aGh3LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMQojU0JBVENIIC0tY3B1cy1wZXItdGFzaz0xCiNTQkFUQ0ggLS1tZW09NjRHCiNTQkFUQ0ggLS10aW1lPTcyOjAwOjAwCiNTQkFUQ0ggLS1vdXRwdXQ9L3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL2xvZ3MvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0xLSVBXyVhLmxvZwojU0JBVENIIC0taG9sZApzZXQgLWV1byBwaXBlZmFpbApleHBvcnQgVFo9VVRDCmV4cG9ydCBQWVRIT05ET05UV1JJVEVCWVRFQ09ERT0xCnRlc3QgIiQoaWQgLXVuKSIgPSBwd2EyMDkKbW9kdWxlIGxvYWQgU3RkRW52LzIwMjMgcHl0aG9uLzMuMTEuNSBjdWRhLzEyLjYgYXJyb3cvMTkuMC4xCnNvdXJjZSAvaG9tZS9wd2EyMDkvaGh3LWVudmlyb25tZW50L2Jpbi9hY3RpdmF0ZQpjZCAvbHVzdHJlMDYvcHJvamVjdC82MTAxODU0L3B3YTIwOS9ob3JzZWhhaXItd29ybS9yZWxlYXNlcy8yMDI2MTAyMS01N2UwZWI4YzRjNmMtZDg5YjMxMTgwZDRjCmV4cG9ydCBQWVRIT05QQVRIPS9sdXN0cmUwNi9wcm9qZWN0LzYxMDE4NTQvcHdhMjA5L2hvcnNlaGFpci13b3JtL3JlbGVhc2VzLzIwMjYxMDIxLTU3ZTBlYjhjNGM2Yy1kODliMzExODBkNGMvc3JjOiIke1BZVEhPTlBBVEg6LX0iCmV4cG9ydCBNUExCQUNLRU5EPUFnZwpleHBvcnQgT01QX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE9QRU5CTEFTX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE1LTF9OVU1fVEhSRUFEUz0ke1NMVVJNX0NQVVNfUEVSX1RBU0t9CmV4cG9ydCBQWVRIT05VTkJVRkZFUkVEPTEKZXhwb3J0IENVQkxBU19XT1JLU1BBQ0VfQ09ORklHPTo0MDk2OjgKZXhwb3J0IEhIV19HUFVfQ09VTlQ9MApleHBvcnQgSEhXX01BWF9IT1VSUz03MgpweXRob24gLW0gaG9yc2VoYWlyLmNsaSAtLWNvbmZpZyBjb25maWdzL3N0dWR5Lmpzb24gLS1yb290IC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybSB2ZXJpZnktc291cmNlIC0taWRlbnRpdHkgcmVsZWFzZS5qc29uCi9ob21lL3B3YTIwOS9oaHctZW52aXJvbm1lbnQvYmluL3B5dGhvbiAtcyAtdSAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxLTIwMjYxMDIzL3J1bi1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy12MS0yMDI2MTAyMy5weSAtLW1vZGUgcnVuIC0tcG9saWN5LWRlc2NyaXB0b3IgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWhhbmRvZmYtdjItMjAyNjEwMjMvbmF0aXZlLTY4NjgxNzYvcnVuLXBvbGljeS5qc29uIC0tcG9saWN5LWRlc2NyaXB0b3Itc2hhMjU2IGFkNzBhMTQwY2RkNTAzODRmODYwY2I0YWRjMmRjNGZjMzhmZWIwYTdlYWE2MGJlMjQwMzQ1NjM3M2M5YjIyNjcgLS1uYXRpdmUtcGxhbiAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcGxhbi5qc29uIC0tbmF0aXZlLWxlZGdlciAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vc3RhdGUvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEtc3VibWlzc2lvbi5qc29uIC0tbmF0aXZlLXRhc2sgcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0xIC0tbmF0aXZlLXNjcmlwdCAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0xLnNiYXRjaCAtLXNlZ21lbnQgMQo=', 'sha256': '0831e0ec643aad2493f34475a62963cf6bffc8419b5ad2a538f9c33369d1539d'}, '/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v1/primary-external-census-controller-2.sbatch': {'base64': 'IyEvYmluL2Jhc2gKI1NCQVRDSCAtLWFjY291bnQ9ZGVmLXB0ZXdhcmllX2NwdQojU0JBVENIIC0tam9iLW5hbWU9aGh3LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMgojU0JBVENIIC0tY3B1cy1wZXItdGFzaz0xCiNTQkFUQ0ggLS1tZW09NjRHCiNTQkFUQ0ggLS10aW1lPTcyOjAwOjAwCiNTQkFUQ0ggLS1vdXRwdXQ9L3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL2xvZ3MvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0yLSVBXyVhLmxvZwojU0JBVENIIC0taG9sZApzZXQgLWV1byBwaXBlZmFpbApleHBvcnQgVFo9VVRDCmV4cG9ydCBQWVRIT05ET05UV1JJVEVCWVRFQ09ERT0xCnRlc3QgIiQoaWQgLXVuKSIgPSBwd2EyMDkKbW9kdWxlIGxvYWQgU3RkRW52LzIwMjMgcHl0aG9uLzMuMTEuNSBjdWRhLzEyLjYgYXJyb3cvMTkuMC4xCnNvdXJjZSAvaG9tZS9wd2EyMDkvaGh3LWVudmlyb25tZW50L2Jpbi9hY3RpdmF0ZQpjZCAvbHVzdHJlMDYvcHJvamVjdC82MTAxODU0L3B3YTIwOS9ob3JzZWhhaXItd29ybS9yZWxlYXNlcy8yMDI2MTAyMS01N2UwZWI4YzRjNmMtZDg5YjMxMTgwZDRjCmV4cG9ydCBQWVRIT05QQVRIPS9sdXN0cmUwNi9wcm9qZWN0LzYxMDE4NTQvcHdhMjA5L2hvcnNlaGFpci13b3JtL3JlbGVhc2VzLzIwMjYxMDIxLTU3ZTBlYjhjNGM2Yy1kODliMzExODBkNGMvc3JjOiIke1BZVEhPTlBBVEg6LX0iCmV4cG9ydCBNUExCQUNLRU5EPUFnZwpleHBvcnQgT01QX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE9QRU5CTEFTX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE1LTF9OVU1fVEhSRUFEUz0ke1NMVVJNX0NQVVNfUEVSX1RBU0t9CmV4cG9ydCBQWVRIT05VTkJVRkZFUkVEPTEKZXhwb3J0IENVQkxBU19XT1JLU1BBQ0VfQ09ORklHPTo0MDk2OjgKZXhwb3J0IEhIV19HUFVfQ09VTlQ9MApleHBvcnQgSEhXX01BWF9IT1VSUz03MgpweXRob24gLW0gaG9yc2VoYWlyLmNsaSAtLWNvbmZpZyBjb25maWdzL3N0dWR5Lmpzb24gLS1yb290IC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybSB2ZXJpZnktc291cmNlIC0taWRlbnRpdHkgcmVsZWFzZS5qc29uCi9ob21lL3B3YTIwOS9oaHctZW52aXJvbm1lbnQvYmluL3B5dGhvbiAtcyAtdSAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxLTIwMjYxMDIzL3J1bi1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy12MS0yMDI2MTAyMy5weSAtLW1vZGUgcnVuIC0tcG9saWN5LWRlc2NyaXB0b3IgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWhhbmRvZmYtdjItMjAyNjEwMjMvbmF0aXZlLTY4NjgxNzYvcnVuLXBvbGljeS5qc29uIC0tcG9saWN5LWRlc2NyaXB0b3Itc2hhMjU2IGFkNzBhMTQwY2RkNTAzODRmODYwY2I0YWRjMmRjNGZjMzhmZWIwYTdlYWE2MGJlMjQwMzQ1NjM3M2M5YjIyNjcgLS1uYXRpdmUtcGxhbiAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcGxhbi5qc29uIC0tbmF0aXZlLWxlZGdlciAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vc3RhdGUvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEtc3VibWlzc2lvbi5qc29uIC0tbmF0aXZlLXRhc2sgcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0yIC0tbmF0aXZlLXNjcmlwdCAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0yLnNiYXRjaCAtLXNlZ21lbnQgMgo=', 'sha256': '6fc9ba93d17242e2697cfa6a52a0ba5315902be7e596a3175289de520642ce89'}, '/scratch/pwa209/horsehair-worm/plans/primary-external-census-controllers-v1/primary-external-census-controller-3.sbatch': {'base64': 'IyEvYmluL2Jhc2gKI1NCQVRDSCAtLWFjY291bnQ9ZGVmLXB0ZXdhcmllX2NwdQojU0JBVENIIC0tam9iLW5hbWU9aGh3LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMwojU0JBVENIIC0tY3B1cy1wZXItdGFzaz0xCiNTQkFUQ0ggLS1tZW09NjRHCiNTQkFUQ0ggLS10aW1lPTcyOjAwOjAwCiNTQkFUQ0ggLS1vdXRwdXQ9L3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL2xvZ3MvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0zLSVBXyVhLmxvZwojU0JBVENIIC0taG9sZApzZXQgLWV1byBwaXBlZmFpbApleHBvcnQgVFo9VVRDCmV4cG9ydCBQWVRIT05ET05UV1JJVEVCWVRFQ09ERT0xCnRlc3QgIiQoaWQgLXVuKSIgPSBwd2EyMDkKbW9kdWxlIGxvYWQgU3RkRW52LzIwMjMgcHl0aG9uLzMuMTEuNSBjdWRhLzEyLjYgYXJyb3cvMTkuMC4xCnNvdXJjZSAvaG9tZS9wd2EyMDkvaGh3LWVudmlyb25tZW50L2Jpbi9hY3RpdmF0ZQpjZCAvbHVzdHJlMDYvcHJvamVjdC82MTAxODU0L3B3YTIwOS9ob3JzZWhhaXItd29ybS9yZWxlYXNlcy8yMDI2MTAyMS01N2UwZWI4YzRjNmMtZDg5YjMxMTgwZDRjCmV4cG9ydCBQWVRIT05QQVRIPS9sdXN0cmUwNi9wcm9qZWN0LzYxMDE4NTQvcHdhMjA5L2hvcnNlaGFpci13b3JtL3JlbGVhc2VzLzIwMjYxMDIxLTU3ZTBlYjhjNGM2Yy1kODliMzExODBkNGMvc3JjOiIke1BZVEhPTlBBVEg6LX0iCmV4cG9ydCBNUExCQUNLRU5EPUFnZwpleHBvcnQgT01QX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE9QRU5CTEFTX05VTV9USFJFQURTPSR7U0xVUk1fQ1BVU19QRVJfVEFTS30KZXhwb3J0IE1LTF9OVU1fVEhSRUFEUz0ke1NMVVJNX0NQVVNfUEVSX1RBU0t9CmV4cG9ydCBQWVRIT05VTkJVRkZFUkVEPTEKZXhwb3J0IENVQkxBU19XT1JLU1BBQ0VfQ09ORklHPTo0MDk2OjgKZXhwb3J0IEhIV19HUFVfQ09VTlQ9MApleHBvcnQgSEhXX01BWF9IT1VSUz03MgpweXRob24gLW0gaG9yc2VoYWlyLmNsaSAtLWNvbmZpZyBjb25maWdzL3N0dWR5Lmpzb24gLS1yb290IC9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybSB2ZXJpZnktc291cmNlIC0taWRlbnRpdHkgcmVsZWFzZS5qc29uCi9ob21lL3B3YTIwOS9oaHctZW52aXJvbm1lbnQvYmluL3B5dGhvbiAtcyAtdSAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxLTIwMjYxMDIzL3J1bi1wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy12MS0yMDI2MTAyMy5weSAtLW1vZGUgcnVuIC0tcG9saWN5LWRlc2NyaXB0b3IgL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWhhbmRvZmYtdjItMjAyNjEwMjMvbmF0aXZlLTY4NjgxNzYvcnVuLXBvbGljeS5qc29uIC0tcG9saWN5LWRlc2NyaXB0b3Itc2hhMjU2IGFkNzBhMTQwY2RkNTAzODRmODYwY2I0YWRjMmRjNGZjMzhmZWIwYTdlYWE2MGJlMjQwMzQ1NjM3M2M5YjIyNjcgLS1uYXRpdmUtcGxhbiAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcGxhbi5qc29uIC0tbmF0aXZlLWxlZGdlciAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vc3RhdGUvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEtc3VibWlzc2lvbi5qc29uIC0tbmF0aXZlLXRhc2sgcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0zIC0tbmF0aXZlLXNjcmlwdCAvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0zLnNiYXRjaCAtLXNlZ21lbnQgMwo=', 'sha256': '04f55706c737a29b3ef1137c8605b74062346c2baae939bbed8061358d4fbaba'}, '/scratch/pwa209/horsehair-worm/state/primary-external-census-controllers-v1-submission.json': {'base64': 'ewogICJjb25maWdfc2hhMjU2IjogIjQ0NmI1ZDYyZmMyMDZjZjEzOTJiOTBmZjljZjE2YWRhZmM2NmM1N2E2ZDM4ZTg0YzAyOWNkM2IyZWJiZjFmMzUiLAogICJlbnZpcm9ubWVudF9qb2IiOiBudWxsLAogICJleGNsdWRlZF9ub2RlcyI6IFtdLAogICJwbGFuX3NoYTI1NiI6ICJkNWYyMTJiYmQ1ZDcxMjBmMDA0OTIyZWI1ZTEwNDJiOTJlMDExMGJkZjkwNmY1ZmJhOWE5MTI3N2Y4Mzc0YzcyIiwKICAic2NoZW1hX3ZlcnNpb24iOiAyLAogICJzb3VyY2Vfc2hhMjU2IjogImQ4OWIzMTE4MGQ0YzRmZmU5YzQwMjNkYzU1MzRiOTZmMmQxNTdhM2E4ZTNhZWNhZmYzMDg0OTI4M2QwMGE3MDUiLAogICJ0YXNrcyI6IHsKICAgICJwcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTAiOiB7CiAgICAgICJjb21tYW5kIjogWwogICAgICAgICJzYmF0Y2giLAogICAgICAgICItLXBhcnNhYmxlIiwKICAgICAgICAiLS1kZXBlbmRlbmN5PWFmdGVyb2s6Njg2ODE3NiIsCiAgICAgICAgIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9wbGFucy9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVycy12MS9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTAuc2JhdGNoIgogICAgICBdLAogICAgICAiY29tcGxldGVkX25hdGl2ZV9kZXBlbmRlbmNpZXMiOiBbCiAgICAgICAgewogICAgICAgICAgImFycmF5IjogbnVsbCwKICAgICAgICAgICJiYXNpcyI6ICJQb3NpdGl2ZSBwZXJzaXN0ZWQgZmluYWwgc2FjY3QgcmVjb3JkcyBmb3IgZXZlcnkgZGVjbGFyZWQgdGFzazsgcmVxdWlyZSBDT01QTEVURUQvMDowIGZvciBhZnRlcm9rIiwKICAgICAgICAgICJjb21wbGV0aW9uX3Byb29mX3NoYTI1NiI6ICI0YjEzYTNjM2UxZDMyZmRlNWRjYjJlYWU5NzI4YWMwYWJlNzI3OGJmYTFiNGJhYTA2YWM0YjFiOGI3NTZlNGNmIiwKICAgICAgICAgICJkZXBlbmRlbmN5X2tpbmQiOiAiYWZ0ZXJvayIsCiAgICAgICAgICAiam9iX2lkIjogIjY4NjQ0OTEiLAogICAgICAgICAgIm5hdGl2ZV9yZWNvcmRzIjogewogICAgICAgICAgICAiNjg2NDQ5MSI6IHsKICAgICAgICAgICAgICAiYmFzaXMiOiAic2FjY3QgZmluYWwgYWxsb2NhdGVkIHdhbGwgdGltZSIsCiAgICAgICAgICAgICAgImNwdV9jb3JlX2hvdXJzIjogNi4xNjY2NjY2NjY2NjY2NjcsCiAgICAgICAgICAgICAgImNwdXMiOiA4LAogICAgICAgICAgICAgICJlbGFwc2VkX3NlY29uZHMiOiAyNzc1LAogICAgICAgICAgICAgICJleGl0X2NvZGUiOiAiMDowIiwKICAgICAgICAgICAgICAiZ3B1X2hvdXJzIjogMC4wLAogICAgICAgICAgICAgICJncHVzIjogMCwKICAgICAgICAgICAgICAiam9iX2lkIjogIjY4NjQ0OTEiLAogICAgICAgICAgICAgICJqb2JfaWRfcmF3IjogIjY4NjQ0OTEiLAogICAgICAgICAgICAgICJzdGF0ZSI6ICJDT01QTEVURUQiCiAgICAgICAgICAgIH0KICAgICAgICAgIH0sCiAgICAgICAgICAibmF0aXZlX3JlY29yZHNfc2hhMjU2IjogImEyYTZmNDlkY2U0ZjQ4N2RlOTExZTE2MDU5YzIwY2FkMDdhZWNiNmM2MGM5OWVjMjFjZWMzMGIyZjc5OGIyZjMiLAogICAgICAgICAgInNvdXJjZV9maWxlX3NoYTI1Nl9hdF9jYXB0dXJlIjogImQxODZhZGU3MTg2YTZmY2ZlZTlkNjU0YjFhNzUzYmJlOTA2NjU3YTI5NWVkOWI4M2YxNjRlZTRlZDRlMjRhZDYiLAogICAgICAgICAgInNvdXJjZV9wYXRoIjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9zdGF0ZS9yZXNvdXJjZS1idWRnZXQuanNvbiIKICAgICAgICB9CiAgICAgIF0sCiAgICAgICJkZXBlbmRlbmN5X2tpbmQiOiAiYWZ0ZXJvayIsCiAgICAgICJkZXBlbmRlbmN5X3Jlc29sdXRpb25fc2hhMjU2IjogImZmNWJmMzE1YjY2ZGQ0ODg5ODI3MmJkNTJlNjQ5MWE5NDI3NGUzOTIzMjBlMDVhOTMwODEwYzc0NmNlOWVjZTUiLAogICAgICAiaXNzdWVkX25hdGl2ZV9kZXBlbmRlbmNpZXMiOiBbCiAgICAgICAgIjY4NjgxNzYiCiAgICAgIF0sCiAgICAgICJqb2JfaWQiOiAiNjg2ODg4NCIsCiAgICAgICJzY3JpcHRfc2hhMjU2IjogImIxNzkyZjlkNGUyZWE2YTM2NjM3ZGFiYzA3OWE5NjI1MTBiODBkMjNhODMyMjkzNjU2OTRiNzcwNGE3NzIxNTEiLAogICAgICAic3RhdGUiOiAic3VibWl0dGVkIgogICAgfSwKICAgICJwcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTEiOiB7CiAgICAgICJjb21tYW5kIjogWwogICAgICAgICJzYmF0Y2giLAogICAgICAgICItLXBhcnNhYmxlIiwKICAgICAgICAiLS1kZXBlbmRlbmN5PWFmdGVyYW55OjY4Njg4ODQiLAogICAgICAgICIvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vcGxhbnMvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlcnMtdjEvcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0xLnNiYXRjaCIKICAgICAgXSwKICAgICAgImNvbXBsZXRlZF9uYXRpdmVfZGVwZW5kZW5jaWVzIjogW10sCiAgICAgICJkZXBlbmRlbmN5X2tpbmQiOiAiYWZ0ZXJhbnkiLAogICAgICAiZGVwZW5kZW5jeV9yZXNvbHV0aW9uX3NoYTI1NiI6ICJlMTc2YTRkMDI5OWQzZDUxMGJiNDk1OWUyOGJiNDdjODBiNzcyNjM1MjE0MzUzMWFjZThlNTY2ODcwODdmMzZkIiwKICAgICAgImlzc3VlZF9uYXRpdmVfZGVwZW5kZW5jaWVzIjogWwogICAgICAgICI2ODY4ODg0IgogICAgICBdLAogICAgICAiam9iX2lkIjogIjY4Njg4ODYiLAogICAgICAic2NyaXB0X3NoYTI1NiI6ICIxZDQzZDdjZjkwODQ2YzJjMTEzNzY0YjAzMjJhMzE1NGFiZTkyZGVhNjZmODg3ZGM4M2U5YWI4OWE3MWVhNGM3IiwKICAgICAgInN0YXRlIjogInN1Ym1pdHRlZCIKICAgIH0sCiAgICAicHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtY29udHJvbGxlci0yIjogewogICAgICAiY29tbWFuZCI6IFsKICAgICAgICAic2JhdGNoIiwKICAgICAgICAiLS1wYXJzYWJsZSIsCiAgICAgICAgIi0tZGVwZW5kZW5jeT1hZnRlcmFueTo2ODY4ODg2IiwKICAgICAgICAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL3BsYW5zL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXJzLXYxL3ByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMi5zYmF0Y2giCiAgICAgIF0sCiAgICAgICJjb21wbGV0ZWRfbmF0aXZlX2RlcGVuZGVuY2llcyI6IFtdLAogICAgICAiZGVwZW5kZW5jeV9raW5kIjogImFmdGVyYW55IiwKICAgICAgImRlcGVuZGVuY3lfcmVzb2x1dGlvbl9zaGEyNTYiOiAiNDlhYzMyYWNlOGQ3YTQyY2E3MDg0YTNmOWQ0MjYxYTZhYjYwYzQ5MDljYTFmMTA4Mzk3OWE5MGI1ODk0MjMzNiIsCiAgICAgICJpc3N1ZWRfbmF0aXZlX2RlcGVuZGVuY2llcyI6IFsKICAgICAgICAiNjg2ODg4NiIKICAgICAgXSwKICAgICAgImpvYl9pZCI6ICI2ODY4ODg4IiwKICAgICAgInNjcmlwdF9zaGEyNTYiOiAiZjZlMDRkYWZjZmM5NzZmYmQ1YTcyZDNkMTk0YWY5OGM1MTc1YjRiNjNkZTU0Y2YyYzhhOTAyZjA4YjlkMjI1MyIsCiAgICAgICJzdGF0ZSI6ICJzdWJtaXR0ZWQiCiAgICB9LAogICAgInByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLWNvbnRyb2xsZXItMyI6IHsKICAgICAgImNvbW1hbmQiOiBbCiAgICAgICAgInNiYXRjaCIsCiAgICAgICAgIi0tcGFyc2FibGUiLAogICAgICAgICItLWRlcGVuZGVuY3k9YWZ0ZXJhbnk6Njg2ODg4OCIsCiAgICAgICAgIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9wbGFucy9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVycy12MS9wcmltYXJ5LWV4dGVybmFsLWNlbnN1cy1jb250cm9sbGVyLTMuc2JhdGNoIgogICAgICBdLAogICAgICAiY29tcGxldGVkX25hdGl2ZV9kZXBlbmRlbmNpZXMiOiBbXSwKICAgICAgImRlcGVuZGVuY3lfa2luZCI6ICJhZnRlcmFueSIsCiAgICAgICJkZXBlbmRlbmN5X3Jlc29sdXRpb25fc2hhMjU2IjogIjY2ODhlNDk2MTE3MzRjYjhiNmJlNWRkY2Q3ZGI2Y2ZmMzFjYTg1Yzc5ZjA5ZmYyOTVmNGExYjRhMjMxMGZjYzIiLAogICAgICAiaXNzdWVkX25hdGl2ZV9kZXBlbmRlbmNpZXMiOiBbCiAgICAgICAgIjY4Njg4ODgiCiAgICAgIF0sCiAgICAgICJqb2JfaWQiOiAiNjg2ODg4OSIsCiAgICAgICJzY3JpcHRfc2hhMjU2IjogImEzNDMzYzNhZmNlMmIwMzYwOThmOTE4NWU1MjJlNTkzOGI0MWE5YWUwMTIwZTRkYWZkMmY4OWVkM2ViYjlhZTYiLAogICAgICAic3RhdGUiOiAic3VibWl0dGVkIgogICAgfQogIH0KfQo=', 'sha256': '26efafe65339ec349a65773135d36502a3baa7d3f4523d56f704b16dba3c5be6'}}
HISTORICAL_PARTIAL_DESCRIPTOR={'base64': 'ewogICJjZW5zdXMiOiB7CiAgICAicG9saWN5X3NvdXJjZSI6IHsKICAgICAgInBhdGgiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LWFmLXJvbGxpbmctY2Vuc3VzLXN1cHBvcnQtdjQtMjAyNjEwMjMvcnVuLWFmLWFnZW50LWNvbnRyb2xsZXItY2Vuc3VzLXBvbGljeS12MS0yMDI2MTAyMy5weSIsCiAgICAgICJzaGEyNTYiOiAiMTIwZjAyZDA5OGI2YzlhZmEwZWEwZGY1NGY1OTBhZTM4MWI0MjI3ZDQ4OTJiYTBmOWVlNGFjYzkzZTZmZWMwZiIKICAgIH0sCiAgICAicHJvdmlkZXJfc2NoZWR1bGUiOiB7CiAgICAgICJjYXNlX3Jvb3QiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LWFmLXJvbGxpbmctc3RvcmFnZS1jZW5zdXMtdjQtMjAyNjEwMjMiLAogICAgICAiaGVscGVyIjogewogICAgICAgICJwYXRoIjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9tYWludGVuYW5jZS9zdHVkeS1hZi1yb2xsaW5nLWNlbnN1cy1zdXBwb3J0LXY0LTIwMjYxMDIzL3J1bi1mcmVzaC1zdG9yYWdlLWNlbnN1cy12Ni0yMDI2MTAyMy5weSIsCiAgICAgICAgInNoYTI1NiI6ICI0ODdiZjJkZWEyZjVjODkyYzM2OTU4NmUxMWM0ODg1ZTQ0ZTRjNWJiZGRiZGExZTllM2I4MmYxODU2NjZmOWYzIgogICAgICB9LAogICAgICAibGVkZ2VyIjogewogICAgICAgICJwYXRoIjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9zdGF0ZS9yZWNvdmVyeS1zdXBwb3J0LTIwMjYxMDIxLTQyOS1zdWJtaXNzaW9uLmpzb24iLAogICAgICAgICJzaGEyNTYiOiAiN2VhYzQxNDBhNGEzMTQwNTM2MGFkMDAxYjExNTUxNzIwM2NlNTdiY2I1N2RlMGVkODAxOGUzMTU3Y2ZmYjE5MiIKICAgICAgfSwKICAgICAgInBsYW4iOiB7CiAgICAgICAgInBhdGgiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL3BsYW5zL3JlY292ZXJ5LXN1cHBvcnQtMjAyNjEwMjEtNDI5L3BsYW4uanNvbiIsCiAgICAgICAgInNoYTI1NiI6ICJhODQ4ZGZkZDAxYjE0OGIwYmY2OGM1OWIwZmU2YTg4MWQ4Mjg0Y2UzMGQ0OTI4MmUxNTdjYTg4ZmQxN2MzNmNjIgogICAgICB9LAogICAgICAicXVhbGlmaWNhdGlvbiI6IHsKICAgICAgICAiam9iX2lkIjogIjY4NjM5NjYiLAogICAgICAgICJwYXRoIjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9tYWludGVuYW5jZS9zdHVkeS1hZi1yb2xsaW5nLWNlbnN1cy1xdWFsaWZpY2F0aW9uLXY1LTIwMjYxMDIzL3F1YWxpZmljYXRpb24uanNvbiIsCiAgICAgICAgInNoYTI1NiI6ICI4YjBkYzYxMmNlZTY0NDYxYjk5MWE5ZGZiOWE5NzVjZjVhZmUxMzg5YTAzN2IxMzU3MjQ0ZDgyOWY1YWNhNzZjIgogICAgICB9LAogICAgICAic291cmNlX21hbmlmZXN0IjogewogICAgICAgICJwYXRoIjogIi9zY3JhdGNoL3B3YTIwOS9ob3JzZWhhaXItd29ybS9tYWludGVuYW5jZS9zdHVkeS1hZi1yb2xsaW5nLWNlbnN1cy1zdXBwb3J0LXY0LTIwMjYxMDIzL3NvdXJjZS1tYW5pZmVzdC5qc29uIiwKICAgICAgICAic2hhMjU2IjogIjFiMjE3NDJmNmFkNWI3YjQ4ODk5YTQ4OTA5ODhkYTdkZGJhNDM5Nzk4YmE4YjEyYTJiYWQ1N2M3ZjE4OThlNmUiCiAgICAgIH0KICAgIH0sCiAgICAicXVhbGlmaWNhdGlvbiI6IHsKICAgICAgImpvYl9pZCI6ICI2ODYzOTY2IiwKICAgICAgInBhdGgiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LWFmLXJvbGxpbmctY2Vuc3VzLXF1YWxpZmljYXRpb24tdjUtMjAyNjEwMjMvcXVhbGlmaWNhdGlvbi5qc29uIiwKICAgICAgInNoYTI1NiI6ICI4YjBkYzYxMmNlZTY0NDYxYjk5MWE5ZGZiOWE5NzVjZjVhZmUxMzg5YTAzN2IxMzU3MjQ0ZDgyOWY1YWNhNzZjIgogICAgfQogIH0sCiAgImtpbmQiOiAicHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxIiwKICAicXVhbGlmaWNhdGlvbiI6IHsKICAgICJqb2JfaWQiOiAiNjg2NjQ1NyIsCiAgICAicGF0aCI6ICIvc2NyYXRjaC9wd2EyMDkvaG9yc2VoYWlyLXdvcm0vbWFpbnRlbmFuY2Uvc3R1ZHktcHJpbWFyeS1leHRlcm5hbC1jZW5zdXMtcG9saWN5LXYxLTIwMjYxMDIzL3F1YWxpZmljYXRpb24uanNvbiIsCiAgICAic2hhMjU2IjogImQ3OTY2YWZlMDA0OThlYmM0YzFlMjNjMzRkYThiNzU0MDdmYzBmYjY5MWFmYjQ4OGIxMWVkOThlMGM1M2Y3MTEiCiAgfSwKICAic291cmNlX21hbmlmZXN0IjogewogICAgInBhdGgiOiAiL3NjcmF0Y2gvcHdhMjA5L2hvcnNlaGFpci13b3JtL21haW50ZW5hbmNlL3N0dWR5LXByaW1hcnktZXh0ZXJuYWwtY2Vuc3VzLXBvbGljeS12MS0yMDI2MTAyMy9zb3VyY2UtbWFuaWZlc3QuanNvbiIsCiAgICAic2hhMjU2IjogImZkNWQ3Yjk1MWIyZGYwMDk0ZTgyYzdjNjcwZDZlNTc4YTMwZWZhOTEzYjRkY2M2OTIzY2YzMzU2MjdkMWRkZTkiCiAgfQp9Cg==', 'sha256': 'ad70a140cdd50384f860cb4adc2dc4fc38feb0a7eaa60be2403456373c9b2267'}

HISTORICAL_491_NATIVE_BASE64='NjYzMjE2NHw2NjMyMTY0fEZBSUxFRHwxOjB8MTYxNjk1fDF8YmlsbGluZz0yMDAwLGNwdT0xLG1lbT04Mzg4NjA4Syxub2RlPTF8bmMxMTAwNXwyMDI2LTEwLTIxVDEwOjQ5OjU4fDIwMjYtMTAtMjNUMDc6NDQ6NTN8cHdhMjA5fGRlZi1wdGV3YXJpZV9jcHV8CjY2MzIxNjQuYmF0Y2h8NjYzMjE2NC5iYXRjaHxGQUlMRUR8MTowfDE2MTY5NXwxfGNwdT0xLG1lbT04Mzg4NjA4Syxub2RlPTF8bmMxMTAwNXwyMDI2LTEwLTIxVDEwOjQ5OjU4fDIwMjYtMTAtMjNUMDc6NDQ6NTN8fGRlZi1wdGV3YXJpZV9jcHV8ODM4NjEzNksKNjYzMjE2NC5leHRlcm58NjYzMjE2NC5leHRlcm58Q09NUExFVEVEfDA6MHwxNjE2OTV8MXxiaWxsaW5nPTIwMDAsY3B1PTEsbWVtPTgzODg2MDhLLG5vZGU9MXxuYzExMDA1fDIwMjYtMTAtMjFUMTA6NDk6NTh8MjAyNi0xMC0yM1QwNzo0NDo1M3x8ZGVmLXB0ZXdhcmllX2NwdXwKNjYzMjIwOXw2NjMyMjA5fFBFTkRJTkd8MDowfDB8MHx8Tm9uZSBhc3NpZ25lZHxVbmtub3dufFVua25vd258cHdhMjA5fGRlZi1wdGV3YXJpZV9jcHV8CjY2MzIyMjV8NjYzMjIyNXxQRU5ESU5HfDA6MHwwfDB8fE5vbmUgYXNzaWduZWR8VW5rbm93bnxVbmtub3dufHB3YTIwOXxkZWYtcHRld2FyaWVfY3B1fAo2NjMyMjM5fDY2MzIyMzl8UEVORElOR3wwOjB8MHwwfHxOb25lIGFzc2lnbmVkfFVua25vd258VW5rbm93bnxwd2EyMDl8ZGVmLXB0ZXdhcmllX2NwdXwKNjg2ODE3Nnw2ODY4MTc2fEZBSUxFRHwxOjB8ODY0fDR8YmlsbGluZz00MDAwLGNwdT00LG1lbT0xNjc3NzIxNkssbm9kZT0xfG5jMzExMDl8MjAyNi0xMC0yM1QxMToyMDo0MHwyMDI2LTEwLTIzVDExOjM1OjA0fHB3YTIwOXxkZWYtcHRld2FyaWVfY3B1fAo2ODY4MTc2LmJhdGNofDY4NjgxNzYuYmF0Y2h8RkFJTEVEfDE6MHw4NjR8NHxjcHU9NCxtZW09MTY3NzcyMTZLLG5vZGU9MXxuYzMxMTA5fDIwMjYtMTAtMjNUMTE6MjA6NDB8MjAyNi0xMC0yM1QxMTozNTowNHx8ZGVmLXB0ZXdhcmllX2NwdXw4NTUyMEsKNjg2ODE3Ni5leHRlcm58Njg2ODE3Ni5leHRlcm58Q09NUExFVEVEfDA6MHw4NjR8NHxiaWxsaW5nPTQwMDAsY3B1PTQsbWVtPTE2Nzc3MjE2Syxub2RlPTF8bmMzMTEwOXwyMDI2LTEwLTIzVDExOjIwOjQwfDIwMjYtMTAtMjNUMTE6MzU6MDR8fGRlZi1wdGV3YXJpZV9jcHV8CjY4Njg4ODR8Njg2ODg4NHxDQU5DRUxMRUR8MDowfDB8MHx8Tm9uZSBhc3NpZ25lZHxOb25lfDIwMjYtMTAtMjNUMTE6MzU6MDh8cHdhMjA5fGRlZi1wdGV3YXJpZV9jcHV8CjY4Njg4ODZ8Njg2ODg4NnxQRU5ESU5HfDA6MHwwfDB8fE5vbmUgYXNzaWduZWR8VW5rbm93bnxVbmtub3dufHB3YTIwOXxkZWYtcHRld2FyaWVfY3B1fAo2ODY4ODg4fDY4Njg4ODh8UEVORElOR3wwOjB8MHwwfHxOb25lIGFzc2lnbmVkfFVua25vd258VW5rbm93bnxwd2EyMDl8ZGVmLXB0ZXdhcmllX2NwdXwKNjg2ODg4OXw2ODY4ODg5fFBFTkRJTkd8MDowfDB8MHx8Tm9uZSBhc3NpZ25lZHxVbmtub3dufFVua25vd258cHdhMjA5fGRlZi1wdGV3YXJpZV9jcHV8Cg=='
