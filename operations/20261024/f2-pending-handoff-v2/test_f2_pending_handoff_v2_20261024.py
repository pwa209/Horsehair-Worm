"""Short generic guard/retirement tests. Every scheduler/accounting row is fake."""
from pathlib import Path
import copy
import importlib.util
import json
import subprocess

import pytest

SOURCE=Path(__file__).with_name('handoff-f2-pending-backfill-v2-20261024.py')
spec=importlib.util.spec_from_file_location('pending_only_handoff_contracts',SOURCE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def unused(job,state='PENDING'):
    return {'job_id_raw':job,'user':'pwa209','account':'def-ptewarie_cpu','state':state,'exit_code':'0:0',
        'cpus':0,'elapsed_seconds':0,'alloc_tres':'','node_list':'None assigned','start':'Unknown',
        'end':'Unknown' if state=='PENDING' else '2026-10-24T12:00:00','max_rss':'','unallocated_metadata':False,
        'identity_basis':'Unique scalar native job; zero-allocation queue record'}


def pending(task,job,index=0,*,backfill=False):
    return {'JobId':job,'UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobName':'hhw-'+task['name'],
        'Command':task['script'],'WorkDir':str(m.FRAMEWORK.resolve()),'JobState':'PENDING','RunTime':'00:00:00',
        'StartTime':'Unknown','NodeList':'','AllocTRES':'(null)','Restarts':'0','Requeue':'0','ExitCode':'0:0',
        'NumCPUs':'1','NumNodes':'1-1','MinMemoryNode':'64G','TimeLimit':'4-00:00:00','TimeMin':'08:00:00' if backfill else 'N/A',
        'ReqTRES':'cpu=1,mem=64G,node=1,billing=16000','Reason':'Priority' if index==0 else 'Dependency','Priority':'10',
        'Dependency':'(null)' if index==0 else 'afterany:'+m.OLD_IDS[index-1]+'(unfulfilled)'}


@pytest.fixture
def fixture(tmp_path):
    tasks=[{'name':'af-f2-external-census-v1-controller-'+str(i),'script':'/virtual/old-'+str(i)+'.sbatch'} for i in range(4)]
    values={job:pending(task,job,index) for index,(task,job) in enumerate(zip(tasks,m.OLD_IDS))}
    rows={job:unused(job) for job in m.OLD_IDS};commands=[]
    def scheduler(job,path):
        text=' '.join(key+'='+value for key,value in values[job].items())
        return subprocess.CompletedProcess([],0,text,'')
    def reader(ids,*args,**kwargs):return {job:copy.deepcopy(rows[job]) for job in ids}
    context={'area':tmp_path,'end':m.time.time()+3600,'registration_locked':True,'scheduler':scheduler,
        'native_reader':reader,'no_old_leases':lambda **kwargs:True,'verify':lambda:None,'job':'9999'}
    def operation(c,name,command,*,precheck=None):
        if precheck:precheck()
        commands.append(command)
        job=command[-1]
        if command[:2]==['scontrol','hold']:values[job].update(Reason='JobHeldUser',Priority='0')
        elif command[:2]==['scontrol','release']:
            values[job].update(Reason='Priority' if job==m.OLD_IDS[0] else 'Dependency',Priority='10')
        elif command[0]=='scancel':rows[job]=unused(job,'CANCELLED')
        else:raise AssertionError(command)
        return subprocess.CompletedProcess(command,0,'','')
    return context,tasks,values,rows,commands,operation


def test_exact_old_pending_guard_uses_scheduler_and_zero_allocation_accounting(fixture):
    c,tasks,values,*_=fixture
    assert m.old_pending(c,tasks[0],m.OLD_IDS[0])==values[m.OLD_IDS[0]]


@pytest.mark.parametrize('change',['owner','account','script','name','elapsed','start','allocated','gpu'])
def test_old_pending_guard_refuses_changed_identity_or_any_allocation(fixture,change):
    c,tasks,values,*_=fixture;value=values[m.OLD_IDS[0]]
    if change=='owner':value['UserId']='other(1)'
    if change=='account':value['Account']='foreign'
    if change=='script':value['Command']='/foreign/script'
    if change=='name':value['JobName']='foreign'
    if change=='elapsed':value['RunTime']='00:00:01'
    if change=='start':value['StartTime']='2026-10-24T12:00:00'
    if change=='allocated':value['AllocTRES']='cpu=1,mem=64G,node=1'
    if change=='gpu':value['ReqTRES']+=',gres/gpu=1'
    with pytest.raises(ValueError):m.old_pending(c,tasks[0],m.OLD_IDS[0])


@pytest.mark.parametrize('state',['PENDING','CANCELLED'])
def test_native_unused_proof_requires_positive_scalar_record(state,tmp_path,monkeypatch):
    row=unused('123',state)
    assert m.unused_native(row,'123',state=state)==row
    monkeypatch.setattr(m,'SUPPORT',tmp_path)
    calls=[]
    def transport(command,**kwargs):
        calls.append(kwargs['timeout'])
        if state=='CANCELLED':raise subprocess.TimeoutExpired(command,kwargs['timeout'])
        return subprocess.CompletedProcess(command,0,'native fixture row','')
    def original(ids,rosters,*,runner,owner,account):
        assert ids==['123'] and owner=='pwa209' and account=='def-ptewarie_cpu'
        response=runner(['sacct','--array','-j','123','--format=JobIDRaw','--units=K','--noheader','--parsable2'],text=True,capture_output=True)
        response.check_returncode();return {'123':row}
    original.__hhw_native_accounting_read_guard__={'path':str(m.GUARD),'sha256':m.GUARD_SHA}
    bounded=m.bounded_native_reader(original,tmp_path/'native-read',10000,transport=transport,wall=lambda:1000,mono=lambda:10)
    if state=='CANCELLED':
        with pytest.raises(subprocess.TimeoutExpired):bounded(['123'],{},owner='pwa209',account='def-ptewarie_cpu')
    else:assert bounded(['123'],{},owner='pwa209',account='def-ptewarie_cpu')=={'123':row}
    assert calls==[45.0]
    intent=m.read_json(tmp_path/'native-read/query-000001-intent.json')
    assert intent['new_layer_retries']==0 and intent['maximum_original_read_seconds']==150
    assert len(list((tmp_path/'native-read').glob('*-result.json')))==1


@pytest.mark.parametrize('change',['cpu','elapsed','state','metadata','start','node'])
def test_native_unused_proof_refuses_allocated_or_template_metadata(change):
    row=unused('123','CANCELLED')
    if change=='cpu':row['cpus']=1
    if change=='elapsed':row['elapsed_seconds']=1
    if change=='state':row['state']='RUNNING'
    if change=='metadata':row['unallocated_metadata']=True
    if change=='start':row['start']='2026-10-24T11:00:00'
    if change=='node':row['node_list']='node01'
    with pytest.raises(ValueError):m.unused_native(row,'123',state='CANCELLED')


def test_all_old_jobs_are_held_before_reverse_order_pending_only_retirement(fixture):
    c,tasks,values,rows,commands,operation=fixture
    assert m.hold_all_old(c,tasks,operation=operation)['status']=='all_four_original_controllers_positively_held'
    proof=m.retire_old(c,tasks,operation=operation,sleeper=lambda delay:pytest.fail('Immediate fake terminal proof should not wait'))
    assert [command[-1] for command in commands[:4]]==list(m.OLD_IDS)
    assert all(command[:2]==['scontrol','hold'] for command in commands[:4])
    assert [command[-1] for command in commands[4:]]==list(reversed(m.OLD_IDS))
    assert all(command[:4]==['scancel','--ctld','--state=PENDING','--user=pwa209'] for command in commands[4:])
    assert set(proof)==set(m.OLD_IDS) and all(row['state']=='CANCELLED' for row in proof.values())


def test_start_race_restores_only_confirmed_old_holds_without_any_cancel(fixture,tmp_path,monkeypatch):
    c,tasks,values,rows,commands,operation=fixture
    root=tmp_path/'study';case=root/'campaign';case.mkdir(parents=True);(root/'state').mkdir()
    monkeypatch.setattr(m,'ROOT',root)
    plan=case/'campaign-plan.json';state=case/'state.json.gz';binding=case/'state-binding.json';worker=case/'worker.py'
    plan.write_text(json.dumps({'plan_sha256':'a'*64}),encoding='utf8');worker.write_text('unchanged worker\n',encoding='utf8')
    budget=root/'state/resource-budget.json';budget.write_text(json.dumps({'leases':{}}),encoding='utf8')
    def publish(revision):
        state.write_bytes(('native original publication '+str(revision)).encode())
        binding.write_text(json.dumps({'state_file_sha256':m.digest(state),'state_sha256':'b'*64,
            'campaign_plan_sha256':'a'*64,'revision':revision,'scientific_result':False,'admission_authority':False}),encoding='utf8')
    publish(4)
    inventory=lambda context:{str(path):m.digest(path) for path in (plan,state,binding,worker)}
    monkeypatch.setattr(m,'protected_inventory',inventory)
    monkeypatch.setattr(m,'source_identity',lambda path:{'sha256':m.SOURCE if path==m.FRAMEWORK else m.PRIMARY_SHA})
    c.update(pins={str(worker):m.digest(worker)},protected=inventory(c),protected_binding_revision=4)
    c['verify']=lambda:m.verify_sources(c);c['no_old_leases']=lambda **kwargs:m.no_old_leases(c,**kwargs)
    c['verify']()
    def runner(command,**kwargs):
        assert kwargs['timeout']==30
        return operation(c,'actual-command',command)
    def race(c,name,command,*,precheck=None):
        if command[:2]==['scontrol','hold'] and command[-1]==m.OLD_IDS[1]:
            job=m.OLD_IDS[1];values[job]['JobState']='RUNNING'
            rows[job].update(state='RUNNING',cpus=1,elapsed_seconds=1,alloc_tres='cpu=1,mem=64G,node=1',
                node_list='native-node',start='2026-10-24T12:00:00',identity_basis='Actual native allocation identity')
            budget.write_text(json.dumps({'leases':{job:{'state':'running','cpus':1,'gpus':0,'max_hours':96}}}),encoding='utf8')
            publish(5)
        return m.mutation(c,name,command,precheck=precheck,runner=runner)
    result=m.hold_all_old(c,tasks,operation=race)
    assert result['status']=='declined_original_job_no_longer_pending'
    assert result['restored_positive_old_holds']==[m.OLD_IDS[0]]
    assert commands==[['scontrol','hold',m.OLD_IDS[0]],['scontrol','release',m.OLD_IDS[0]]]
    assert c['positively_started_old']=={m.OLD_IDS[1]} and m.read_json(binding)['revision']==5
    assert m.read_json(c['area']/('hold-old-'+m.OLD_IDS[1]+'-result.json'))['mutation_may_have_occurred'] is False
    assert m.read_json(c['area']/('restore-old-'+m.OLD_IDS[0]+'-result.json'))['status']=='native_command_returned_zero'
    assert c['start_race_protected_observation'][str(worker)]==c['protected'][str(worker)]
    c['restoring_after_positive_start_race']=True
    m.verify_sources(c)
    tampered=m.read_json(binding);tampered['campaign_plan_sha256']='c'*64;binding.write_text(json.dumps(tampered),encoding='utf8')
    with pytest.raises(ValueError,match='source-bound original'):m.verify_sources(c)
    tampered['campaign_plan_sha256']='a'*64;binding.write_text(json.dumps(tampered),encoding='utf8')
    budget.write_text(json.dumps({'leases':{m.OLD_IDS[0]:{'state':'running'}}}),encoding='utf8')
    with pytest.raises(ValueError):m.no_old_leases(c,target=m.OLD_IDS[0])


def test_ambiguous_mutation_is_journaled_once_and_cannot_replay(fixture):
    c,*_=fixture;calls=[]
    def timeout(command,**kwargs):
        calls.append(command);raise subprocess.TimeoutExpired(command,30)
    with pytest.raises(subprocess.TimeoutExpired):m.mutation(c,'one-cancel',['scancel','--state=PENDING','123'],runner=timeout)
    with pytest.raises(FileExistsError):m.mutation(c,'one-cancel',['scancel','--state=PENDING','123'],runner=timeout)
    assert len(calls)==1
    record=m.read_json(c['area']/'one-cancel-result.json')
    assert record['status']=='stopped_ambiguous' and record['mutation_may_have_occurred'] is True


def test_pending_filter_race_cannot_kill_running_job_or_cancel_second_target(fixture):
    c,tasks,values,rows,commands,operation=fixture
    m.hold_all_old(c,tasks,operation=operation);commands.clear()
    def race(c,name,command,*,precheck=None):
        if precheck:precheck()
        commands.append(command)
        assert command[0]=='scancel' and '--state=PENDING' in command
        rows[command[-1]].update(state='RUNNING',cpus=1,elapsed_seconds=1,alloc_tres='cpu=1,mem=64G,node=1')
        return subprocess.CompletedProcess(command,0,'','')
    with pytest.raises(ValueError):m.retire_old(c,tasks,operation=race,sleeper=lambda delay:None)
    assert len(commands)==1 and commands[0][-1]==m.OLD_IDS[-1]


@pytest.mark.parametrize('change',['dependency','not_held','time_min'])
def test_new_held_contract_refuses_wrong_dag_hold_or_time_min(fixture,monkeypatch,change):
    c,old_tasks,values,rows,commands,operation=fixture;jobs=['7001','7002','7003','7004'];tasks=[]
    text='#SBATCH --time-min=08:00:00\npython /virtual/new-wrapper.py\n'
    for index,job in enumerate(jobs):
        tasks.append({'name':'new-controller-'+str(index),'script':'/virtual/new-'+str(index)+'.sbatch',
            'script_sha256':m.canonical_hash(text),'command':'python /virtual/new-wrapper.py',
            'resources':{'cpus':1,'memory':'64G','hours':96,'gpu':False,'array':None},
            'depends':[] if index==0 else ['new-controller-'+str(index-1)],'external_dependencies':['9999'] if index==0 else [],
            'dependency_kind':'afterok' if index==0 else 'afterany'})
        values[job]=pending(tasks[-1],job,index,backfill=True)
        values[job].update(Reason='JobHeldUser',Priority='0',Dependency=('afterok:9999' if index==0 else 'afterany:'+jobs[index-1])+'(unfulfilled)')
        rows[job]=unused(job)
    c['new_plan']={'tasks':tasks};task=tasks[0];job=jobs[0]
    ledger={'plan_sha256':m.canonical_hash(c['new_plan']),'source_sha256':m.SOURCE,'tasks':{
        task['name']:{'state':'submitted','job_id':job,'script_sha256':task['script_sha256'],'dependency_kind':'afterok',
            'issued_native_dependencies':['9999'],'completed_native_dependencies':[],
            'command':['sbatch','--parsable','--dependency=afterok:9999',task['script']]}}}
    monkeypatch.setattr(m,'packet',lambda *args,**kwargs:(text.encode(),'0'*64))
    if change=='dependency':values[job]['Dependency']='afterok:foreign(unfulfilled)'
    if change=='not_held':values[job].update(Reason='Priority',Priority='10')
    if change=='time_min':values[job]['TimeMin']='N/A'
    with pytest.raises(ValueError):m.new_held(c,task,job,0,jobs,ledger)


def test_start_race_restoration_never_releases_foreign_or_unverified_hold(fixture):
    c,tasks,values,rows,commands,operation=fixture
    values[m.OLD_IDS[0]].update(Reason='JobHeldUser',Priority='0',UserId='foreign(1)')
    with pytest.raises(ValueError):m.restore_confirmed_old_holds(c,tasks,[m.OLD_IDS[0]],operation=operation)
    assert commands==[]
