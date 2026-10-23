"""Mocked native activation, byte preservation and failure reconciliation contracts."""
from pathlib import Path
from contextlib import contextmanager
import copy
import gzip
import importlib.util
import json
import shlex
import subprocess
import time
import pytest
from horsehair.core import canonical_hash,digest

spec=importlib.util.spec_from_file_location('private_f2_three_activation',Path(__file__).with_name('activate-f2-prestart-three-v1-20261022.py'))
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)


@pytest.fixture
def controller_fixture(tmp_path,monkeypatch):
    root=tmp_path/'root';area=root/'maintenance/support/activation';area.mkdir(parents=True)
    (root/'plans').mkdir();(root/'state').mkdir();amendment=root/'maintenance/amendment';amendment.mkdir()
    monkeypatch.setattr(a,'ROOT',root);monkeypatch.setattr(a,'AREA',area);monkeypatch.setattr(a,'AMENDMENT',amendment)
    monkeypatch.setattr(a,'MUTEX',root/'state/registration.mutex');monkeypatch.setattr(a,'CONTROLLER',root/'maintenance/run-af-agent-controller-registry-v6-20261022.py')
    a.CONTROLLER.write_text('synthetic source')
    lock=[False];events=[]
    @contextmanager
    def mutex(*args,**kw):
        assert not lock[0];lock[0]=True
        try:yield
        finally:lock[0]=False
    a.MUTEX.write_text('permanent mutex')
    monkeypatch.setattr(a,'source_identity',lambda p:{'sha256':a.SOURCE})
    monkeypatch.setattr(a.slurm,'_script',lambda config,framework,name,command,**kw:'#!/bin/bash\nset -euo pipefail\n'+command+'\n')
    status={};tasks={}
    def scheduler(job,path):
        path.mkdir(parents=True,exist_ok=False)
        task=tasks[job]
        return {'JobId':job,'UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobName':'hhw-'+task['name'],
            'Command':task['script'],'JobState':status[job],'Reason':'JobHeldUser' if status[job]=='PENDING' else 'None',
            'NumCPUs':'1','NumNodes':'1-1','MinMemoryNode':'16G','TimeLimit':'4-00:00:00','ReqTRES':'cpu=1','AllocTRES':'',
            'Dependency':'afterok:999(unfulfilled)' if task['name'].endswith('-0') else 'afterany:'+str(int(job)-1)+'(unfulfilled)'}
    controller={'fields':lambda x:x,'seconds':lambda s:96*3600 if s=='4-00:00:00' else 0,
        'gpu_count':lambda s:1 if 'gpu' in s else 0,'worker_binding':lambda d:None,'registry_binding':lambda d:None,'validate_retry_value':lambda d,v:None}
    def admission(label,**kw):assert lock[0];events.append(('admission',label))
    config={'paths':{'cpu_account':'def-ptewarie_cpu'}}
    c={'job':'999','config':config,'policy':type('Policy',(),{'receipt':lambda s:{'sha256':'d'*64}})(),
        'controller':controller,'scheduler':scheduler,'registry_api':{'registration_lock':mutex},'end':time.time()+10000,'recovery':{},
        'pins':{'run-af-agent-controller-registry-v6-20261022.py':'e'*64,'registered-snapshot-read-guard-v5-20261022.py':'f'*64,
            'run-f00d-worker-registry-overlay-20261021.py':'a'*64},'qpath':area/'q.json','qsha':'b'*64,'q':{'slurm_job_id':'123'},
        'admission':admission,'native_reader':lambda *args,**kw:{},'verify_sources':lambda:events.append(('source-check',)),
        'old_descriptor':{'registry_overlay':{'recovery':{'path':str(a.OLD_RECOVERY),'sha256':a.OLD_RECOVERY_SHA}}}}
    result={'state':{'state_sha256':'c'*64},'amendment':{'amendment_sha256':'a'*64}}
    def prepare(*args):
        assert lock[0];a.exclusive(amendment/'amendment.json',result['amendment']);return result,{},{},[],{},[]
    monkeypatch.setattr(a,'prepare_recovery',prepare)
    def install(*args):assert lock[0];events.append(('cas',));a.exclusive(amendment/'installation.json',{'status':'synthetic'})
    monkeypatch.setattr(a,'install_state',install)
    def submit(path,ledger,runner):
        assert lock[0];plan=json.loads(path.read_text());entries={}
        for i,task in enumerate(plan['tasks']):
            job=str(1000+i);tasks[job]=task;status[job]='PENDING';entries[task['name']]={'job_id':job,'state':'submitted','script_sha256':task['script_sha256']}
        value={'plan_sha256':canonical_hash(plan),'source_sha256':a.SOURCE,'tasks':entries};a.exclusive(ledger,value)
        events.append(('submit',));return value
    monkeypatch.setattr(a.slurm,'submit',submit)
    def runner(command,**kwargs):
        events.append(tuple(command))
        if command[:2]==['scontrol','release']:
            assert not lock[0],'Startup lock must be available before release';status[command[2]]='RUNNING'
        elif command[0]=='scancel':status[command[1]]='CANCELLED'
        return subprocess.CompletedProcess(command,0,'','')
    return c,events,status,tasks,runner,lock


def test_four_new_controllers_release_only_after_registered_admission_cas_and_mutex_exit(controller_fixture):
    c,events,status,tasks,runner,lock=controller_fixture
    result=a.orchestrate(c,{'source':'current_user'},runner=runner)
    assert len(result['jobs'])==4 and all(v=='RUNNING' for v in status.values()) and not lock[0]
    assert events.index(('cas',))<next(i for i,v in enumerate(events) if v[:2]==('scontrol','release'))
    assert events.count(('submit',))==1 and not any(v[0]=='scancel' for v in events)
    plan=json.loads((a.ROOT/'plans'/a.PHASE/'plan.json').read_text())
    assert plan['tasks'][0]['depends']==[] and plan['tasks'][0]['external_dependencies']==['999']
    assert plan['tasks'][0]['dependency_kind']=='afterok' and all(t['dependency_kind']=='afterany' for t in plan['tasks'][1:])
    assert all(t['resources']=={'cpus':1,'gpu':False,'hours':96,'memory':'16G','array':None} for t in plan['tasks'])


def test_pre_cas_failure_cancels_only_new_positively_held_jobs(controller_fixture):
    c,events,status,tasks,runner,lock=controller_fixture
    def admission(label,**kw):
        if label=='activation-all-new-controllers-registered':raise ValueError('synthetic post-registration refusal')
    c['admission']=admission
    with pytest.raises(ValueError):a.orchestrate(c,{},runner=runner)
    assert ('cas',) not in events and set(status.values())=={'CANCELLED'}
    assert len([v for v in events if v[0]=='scancel'])==4 and events.count(('submit',))==1
    assert not any(v[1] in a.OLD_IDS for v in events if v[0]=='scancel')


def test_ambiguous_release_never_replays_or_cancels_the_released_running_job(controller_fixture):
    c,events,status,tasks,runner,lock=controller_fixture
    def ambiguous(command,**kwargs):
        value=runner(command,**kwargs)
        if command[:2]==['scontrol','release']:raise subprocess.TimeoutExpired(command,30)
        return value
    with pytest.raises(subprocess.TimeoutExpired):a.orchestrate(c,{},runner=ambiguous)
    assert status['1000']=='RUNNING' and all(status[j]=='CANCELLED' for j in ['1001','1002','1003'])
    assert len([e for e in events if e[:2]==('scontrol','release')])==1
    assert ('scancel','1000') not in events and events.count(('submit',))==1
    assert json.loads((a.AREA/'activation-failure.json').read_text())['automatic_replay_permitted'] is False


@pytest.mark.parametrize('bad',['owner','script','gpu','old_job','wrong_state'])
def test_no_cancel_without_positive_new_owned_still_held_proof(controller_fixture,bad,monkeypatch):
    c,events,status,tasks,runner,lock=controller_fixture
    descriptor=a.AREA/'descriptor.json';a.exclusive(descriptor,{})
    pack=a.freeze_controllers(c,descriptor);task=pack['value']['tasks'][0];job='1000'
    tasks[job]=task;status[job]='PENDING';original=c['scheduler']
    def changed(ident,path):
        v=original(ident,path)
        if bad=='owner':v['UserId']='foreign(1)'
        if bad=='script':v['Command']='/unrelated/script'
        if bad=='gpu':v['ReqTRES']='gres/gpu=1'
        if bad=='wrong_state':v['JobState']='RUNNING';v['Reason']='None'
        return v
    c['scheduler']=changed
    if bad=='old_job':job=next(iter(a.OLD_IDS))
    a.exclusive(pack['ledger'],{'plan_sha256':canonical_hash(pack['value']),'source_sha256':a.SOURCE,
        'tasks':{task['name']:{'job_id':job,'state':'submitted','script_sha256':task['script_sha256']}}})
    a.cleanup_held(c,pack,runner=runner)
    assert not any(v[0]=='scancel' for v in events)


def test_immutable_evidence_cannot_be_overwritten(tmp_path):
    path=tmp_path/'intent.json';a.exclusive(path,{'before':True})
    with pytest.raises(FileExistsError):a.exclusive(path,{'after':True})
    assert json.loads(path.read_text())=={'before':True}


@pytest.fixture
def cas(tmp_path,monkeypatch):
    root=tmp_path/'r';campaign=root/'campaign';campaign.mkdir(parents=True);case=root/'amendment';case.mkdir()
    mutex=root/'permanent.mutex';mutex.write_text('permanent')
    for name,value in [('ROOT',root),('CAMPAIGN',campaign),('AMENDMENT',case),('MUTEX',mutex)]:monkeypatch.setattr(a,name,value)
    old=gzip.compress(b'{"old":true}',mtime=0);binding=b'{"old_binding":true}\n'
    (campaign/'state.json.gz').write_bytes(old);(campaign/'state-binding.json').write_bytes(binding)
    plan={'plan_sha256':'c'*64};planraw=json.dumps(plan).encode();(campaign/'campaign-plan.json').write_bytes(planraw)
    originals={'original_state_gzip':{'sha256':a.sha(old)},'original_state_binding':{'sha256':a.sha(binding)},
        'original_campaign_plan':{'raw':planraw}}
    result={'state':{'state_sha256':'a'*64,'revision':406,'value':'synthetic derivative'},'amendment':{'amendment_sha256':'b'*64}}
    paths=[root/'new_retry_path'];stat=mutex.stat();calls=[]
    c={'job':'123','registration_locked':True,'mutex_identity':(stat.st_dev,stat.st_ino),
        'verify_sources':lambda:calls.append('source'), 'recovery':{'absence_paths':lambda plan:paths}}
    monkeypatch.setattr(a,'evaluation_inventory',lambda plan,state:[])
    state={'state_sha256':a.ORIGINAL_SHA,'revision':405}
    return c,result,plan,state,paths,originals,old,binding,calls


def test_actual_cas_publishes_exact_state_pair_with_original_evidence(cas):
    c,result,plan,state,paths,originals,old,binding,calls=cas
    a.install_state(c,result,plan,state,paths,originals,[])
    assert json.loads(gzip.decompress((a.CAMPAIGN/'state.json.gz').read_bytes()))==result['state']
    new=json.loads((a.CAMPAIGN/'state-binding.json').read_text())
    assert new['state_file_sha256']==digest(a.CAMPAIGN/'state.json.gz') and new['revision']==406
    intent=json.loads((a.AMENDMENT/'cas-intent.json').read_text())
    assert intent['old_state_file_sha256']==a.sha(old) and intent['old_binding_file_sha256']==a.sha(binding)
    assert intent['automatic_replay_permitted'] is False and calls==['source']
    assert (a.AMENDMENT/'installation.json').is_file()


@pytest.mark.parametrize('bad',['state','binding','plan','retry_exists','unlocked','inode'])
def test_actual_cas_refuses_stale_source_and_guard_before_replacement(cas,bad):
    c,result,plan,state,paths,originals,*_=cas
    if bad=='state':(a.CAMPAIGN/'state.json.gz').write_bytes(b'changed')
    if bad=='binding':(a.CAMPAIGN/'state-binding.json').write_bytes(b'changed')
    if bad=='plan':(a.CAMPAIGN/'campaign-plan.json').write_bytes(b'changed')
    if bad=='retry_exists':paths[0].mkdir()
    if bad=='unlocked':c['registration_locked']=False
    if bad=='inode':c['mutex_identity']=(0,0)
    with pytest.raises(ValueError):a.install_state(c,result,plan,state,paths,originals,[])
    assert not (a.AMENDMENT/'cas-intent.json').exists()


@pytest.mark.parametrize('failure',['second_replace','guard_replaced'])
def test_partial_actual_two_file_cas_retains_intent_and_never_rolls_back_or_replays(cas,failure):
    c,result,plan,state,paths,originals,old,binding,calls=cas;replacements=[]
    def interrupted(source,target):
        replacements.append((source,target))
        if len(replacements)==2:raise OSError('synthetic second replacement failure')
        a.os.replace(source,target)
        if failure=='guard_replaced':
            a.MUTEX.rename(a.MUTEX.with_name('old.mutex'));a.MUTEX.write_text('replacement guard')
    with pytest.raises((OSError,ValueError)):a.install_state(c,result,plan,state,paths,originals,[],replace=interrupted)
    assert (a.AMENDMENT/'cas-intent.json').is_file() and not (a.AMENDMENT/'installation.json').exists()
    assert (a.CAMPAIGN/'state-binding.json').read_bytes()==binding and (a.CAMPAIGN/'state.json.gz').read_bytes()!=old
    assert (a.CAMPAIGN/'.binding-prestart-r2-123.tmp').is_file()
    with pytest.raises(ValueError):a.install_state(c,result,plan,state,paths,originals,[])
    assert len(replacements)==(2 if failure=='second_replace' else 1)


def test_absence_inventory_times_all_filesystem_checks_and_refuses_slow_inventory(cas,monkeypatch):
    c,result,plan,state,paths,originals,*_=cas;clock=[1000.]
    monkeypatch.setattr(a.time,'time',lambda:clock[0])
    def slow(plan,state):clock[0]+=31;return []
    monkeypatch.setattr(a,'evaluation_inventory',slow)
    with pytest.raises(ValueError,match='freshness'):a.absence_snapshot(c,plan,state,'d'*64)


@pytest.mark.parametrize('bad',['extra','missing','changed'])
def test_complete_evaluation_inventory_refuses_outputs_or_changed_plan(tmp_path,monkeypatch,bad):
    root=tmp_path/'r';directory=root/'evaluation';directory.mkdir(parents=True);monkeypatch.setattr(a,'ROOT',root)
    (directory/'plan.json').write_text('{}');(directory/'plan-binding.json').write_text('{}')
    plan={'cells':[{} for _ in range(74)]};plan['cells'][73]={'evaluation_case':str(directory)}
    state={'cells':[{} for _ in range(74)]};state['cells'][73]={'evaluation_plan':{'case':str(directory),
        'plan_sha256':digest(directory/'plan.json'),'plan_binding_sha256':digest(directory/'plan-binding.json')}}
    assert len(a.evaluation_inventory(plan,state))==2
    if bad=='extra':(directory/'partial-output').mkdir()
    if bad=='missing':(directory/'plan.json').unlink()
    if bad=='changed':(directory/'plan.json').write_text('{"changed":true}')
    with pytest.raises(ValueError):a.evaluation_inventory(plan,state)


def test_freeze_all_packet_bytes_before_derivation_and_revalidate_originals(tmp_path,monkeypatch):
    root=tmp_path/'r';case=root/'case';case.mkdir(parents=True);source=root/'original';source.write_bytes(b'original bytes')
    monkeypatch.setattr(a,'ROOT',root);monkeypatch.setattr(a,'AMENDMENT',case)
    proof={'one':a.packet(source),'nested':[{'two':a.packet(source)}]};index=a.freeze_packets(proof)
    assert len(index)==2 and all(Path(v['preserved_path']).read_bytes()==b'original bytes' for v in index)
    assert (case/'source-packet-index.json').is_file()
    a.verify_preserved_sources(index);source.write_bytes(b'changed')
    with pytest.raises(ValueError):a.verify_preserved_sources(index)
    assert all(Path(v['preserved_path']).read_bytes()==b'original bytes' for v in index)


@pytest.mark.parametrize('bad',['owner','name','account','cpus','nodes','memory','hours','gpu','script'])
def test_exact_new_held_native_contract_refuses_foreign_resources(controller_fixture,bad):
    c,events,status,tasks,runner,lock=controller_fixture;descriptor=a.AREA/'d.json';a.exclusive(descriptor,{})
    pack=a.freeze_controllers(c,descriptor);task=pack['value']['tasks'][0];tasks['1000']=task;status['1000']='PENDING'
    original=c['scheduler']
    values={'owner':('UserId','other(1)'),'name':('JobName','other'),'account':('Account','other'),'cpus':('NumCPUs','2'),
        'nodes':('NumNodes','2'),'memory':('MinMemoryNode','32G'),'hours':('TimeLimit','1:00:00'),
        'gpu':('ReqTRES','gres/gpu=1'),'script':('Command','/other/script')}
    def changed(job,path):
        v=original(job,path);key,value=values[bad];v[key]=value;return v
    c['scheduler']=changed
    with pytest.raises(ValueError):a.read_held(c,task,'1000')


@pytest.mark.parametrize('bad',['foreign_dependency','extra_dependency','wrong_kind'])
def test_all_four_held_dependencies_are_verified_before_cas(controller_fixture,bad):
    c,events,status,tasks,runner,lock=controller_fixture;original=c['scheduler']
    def changed(job,path):
        v=original(job,path)
        if job=='1000':v['Dependency']={'foreign_dependency':'afterok:888(unfulfilled)',
            'extra_dependency':'afterok:999(unfulfilled),afterok:888(unfulfilled)','wrong_kind':'afterany:999(unfulfilled)'}[bad]
        return v
    c['scheduler']=changed
    with pytest.raises(ValueError,match='dependency'):a.orchestrate(c,{},runner=runner)
    assert ('cas',) not in events and not [e for e in events if e[:2]==('scontrol','release')]


@pytest.mark.parametrize('record',[{}, {'state':'submitting'}, {'state':'submitted','job_id':'1000','script_sha256':'foreign'}])
def test_cleanup_does_not_guess_unknown_or_ambiguous_job_ids(controller_fixture,record):
    c,events,status,tasks,runner,lock=controller_fixture;descriptor=a.AREA/'d.json';a.exclusive(descriptor,{})
    pack=a.freeze_controllers(c,descriptor);task=pack['value']['tasks'][0]
    a.exclusive(pack['ledger'],{'plan_sha256':canonical_hash(pack['value']),'source_sha256':a.SOURCE,'tasks':{task['name']:record}})
    assert a.cleanup_held(c,pack,runner=runner)==[]
    assert not [v for v in events if v[0]=='scancel']


@pytest.fixture
def qualification(tmp_path,monkeypatch):
    root=tmp_path/'r';support=root/'maintenance/support';support.mkdir(parents=True)
    monkeypatch.setattr(a,'ROOT',root);monkeypatch.setattr(a,'SUPPORT',support)
    names=[a.QHELPER.name,a.CONTROLLER.name,a.GUARD.name,'run-f00d-worker-registry-overlay-20261021.py',a.RECOVERY.name,
        'test_recover_f2_prestart_three_state_v1_20261022.py',Path(a.__file__).name,'test_activate_f2_prestart_three_v1_20261022.py']
    pins={name:'a'*64 for name in names};calls=[]
    api={'NEW_RECOVERY_COUNT':1,'NEW_RECOVERY_ROSTER':'b'*64,'ACTIVATION_COUNT':1,'ACTIVATION_ROSTER':'c'*64,
        'junit_roster':lambda *args:calls.append(('junit',args))}
    registration={'plan':{'path':'plan','sha256':'d'*64},'ledger':{'path':'ledger','sha256':'e'*64},
        'script':{'path':'script','sha256':'f'*64},'task_name':'q','native_job_id':'12'}
    api['registered_task']=lambda *args:copy.deepcopy(registration)
    q={'status':'complete_f2_registry_overlay_qualification','returncode':0,'slurm_job_id':'12','source_sha256':a.SOURCE,
        'config_sha256':a.CONFIG,'qualification_helper_sha256':pins[a.QHELPER.name],
        'controller_sha256':pins[a.CONTROLLER.name],'registry_guard_sha256':pins[a.GUARD.name],
        'worker_shim_sha256':pins['run-f00d-worker-registry-overlay-20261021.py'],'native_registration':registration,
        'scientific_result':False,'admission_authority':False}
    for group,helper,test,roster in [('new_recovery_qualification',a.RECOVERY.name,'test_recover_f2_prestart_three_state_v1_20261022.py','b'*64),
        ('activation_qualification',Path(a.__file__).name,'test_activate_f2_prestart_three_v1_20261022.py','c'*64)]:
        path=support/(group+'.xml');path.write_text('<actual-junit/>')
        q[group]={'helper':{'path':str(support/helper),'sha256':pins[helper]},'test_source':{'path':str(support/test),'sha256':pins[test]},
            'junit':{'path':str(path),'sha256':digest(path)},'test_counts':{'tests':1,'failures':0,'errors':0,'skipped':0},'test_roster_sha256':roster}
    controller={'verify_operational_tests':lambda value:calls.append(('operational',))}
    monkeypatch.setattr(a.slurm,'completed_native_dependency',lambda *args:{'positive':True})
    return api,controller,pins,q,calls


def save_qualification(q):a.exclusive(a.SUPPORT/'private-qualification.json',q)


def test_actual_q_bytes_and_both_native_test_groups_are_required(qualification):
    api,controller,pins,q,calls=qualification;save_qualification(q)
    actual,h=a.verify_qualification(api,controller,pins,'12',{})
    assert actual==q and h==digest(a.SUPPORT/'private-qualification.json')
    assert calls[0]==('operational',) and len([v for v in calls if v[0]=='junit'])==2


@pytest.mark.parametrize('bad',['source','helper','returncode','scientific','recovery_missing','activation_missing','count','roster','junit_bytes','registration','native_missing'])
def test_q_refuses_source_tamper_partial_tests_or_missing_positive_native_producer(qualification,bad,monkeypatch):
    api,controller,pins,q,calls=qualification
    if bad=='source':q['source_sha256']='foreign'
    if bad=='helper':q['qualification_helper_sha256']='foreign'
    if bad=='returncode':q['returncode']=True
    if bad=='scientific':q['scientific_result']=True
    if bad=='recovery_missing':q.pop('new_recovery_qualification')
    if bad=='activation_missing':q.pop('activation_qualification')
    if bad=='count':q['activation_qualification']['test_counts']['tests']=0
    if bad=='roster':q['activation_qualification']['test_roster_sha256']='foreign'
    if bad=='junit_bytes':Path(q['activation_qualification']['junit']['path']).write_text('changed')
    if bad=='registration':api['registered_task']=lambda *args:{}
    if bad=='native_missing':monkeypatch.setattr(a.slurm,'completed_native_dependency',lambda *args:None)
    save_qualification(q)
    with pytest.raises(ValueError):a.verify_qualification(api,controller,pins,'12',{})


@pytest.fixture
def manifest(tmp_path,monkeypatch):
    root=tmp_path/'r';support=root/'maintenance/support';support.mkdir(parents=True)
    monkeypatch.setattr(a,'ROOT',root);monkeypatch.setattr(a,'SUPPORT',support)
    files={Path(a.__file__).name:Path(a.__file__).read_bytes(),a.QHELPER.name:b'# qualified helper',a.QTEST:b'# qualifier tests',
        'other.py':b'# pinned source','test_other.py':b'# pinned tests'}
    for name,raw in files.items():(support/name).write_bytes(raw)
    value={'kind':a.MANIFEST_KIND,'files':{name:digest(support/name) for name in files}}
    path=support/'source-manifest.json';a.exclusive(path,value)
    return path,value


def test_manifest_bytebinds_own_source_before_loading_qualified_code(manifest):
    path,value=manifest
    assert a.verify_manifest(path,digest(path))==value['files']


@pytest.mark.parametrize('bad',['raw_file','resealed_own_source','extra_key','wrong_kind','wrong_path'])
def test_source_manifest_refuses_tamper_and_expansion(manifest,bad):
    path,value=manifest
    if bad=='raw_file':(a.SUPPORT/'other.py').write_text('changed')
    if bad=='resealed_own_source':
        own=a.SUPPORT/Path(a.__file__).name;own.write_text('# different executable');value['files'][own.name]=digest(own)
    if bad=='extra_key':value['admission_authority']=False
    if bad=='wrong_kind':value['kind']='foreign'
    if bad in {'resealed_own_source','extra_key','wrong_kind'}:path.write_text(json.dumps(value))
    if bad=='wrong_path':
        other=a.SUPPORT/'other-manifest.json';other.write_bytes(path.read_bytes());path=other
    with pytest.raises(ValueError):a.verify_manifest(path,digest(path))


@pytest.fixture
def own_registration(tmp_path,monkeypatch):
    root=tmp_path/'r';directory=root/'plans/activation';directory.mkdir(parents=True);(root/'state').mkdir()
    monkeypatch.setattr(a,'ROOT',root)
    script=directory/'activation.sbatch';argv=['--native-task','activation','--source-manifest-sha256','a'*64]
    command=shlex.join(['python','-s',str(Path(a.__file__)),*argv]);script.write_text('#!/bin/bash\n'+command+'\n')
    task={'name':'activation','command':command,'script':str(script),'script_sha256':canonical_hash(script.read_text()),
        'resources':{'cpus':4,'gpu':False,'hours':4,'memory':'16G'}}
    plan={'tasks':[task],'release_identity':{'sha256':a.SOURCE},'config_sha256':a.CONFIG}
    ledger={'tasks':{'activation':{'job_id':'123','state':'submitted','script_sha256':task['script_sha256']}}}
    pp=directory/'plan.json';lp=root/'state/activation-submission.json';a.exclusive(pp,plan);a.exclusive(lp,ledger)
    monkeypatch.setattr(a.runpy,'run_path',lambda *args,**kwargs:{'checked_pair':lambda *args:(plan,ledger)})
    return {'DRIVER':root/'driver.py'},pp,lp,script,argv,plan,ledger


def test_actual_own_registered_task_must_bind_exact_cli_and_four_hour_resources(own_registration):
    api,pp,lp,script,argv,plan,ledger=own_registration
    assert a.registered_activation(api,pp,lp,'activation',script,'123',{},argv)['native_job_id']=='123'


@pytest.mark.parametrize('bad',['argv','resources','source','job','ambiguous','script'])
def test_own_native_plan_binding_refuses_changed_args_resources_or_identity(own_registration,bad):
    api,pp,lp,script,argv,plan,ledger=own_registration
    if bad=='argv':argv=[*argv,'--unplanned']
    if bad=='resources':plan['tasks'][0]['resources']['hours']=1
    if bad=='source':plan['release_identity']['sha256']='foreign'
    if bad=='job':ledger['tasks']['activation']['job_id']='999'
    if bad=='ambiguous':ledger['tasks']['activation']['state']='submitting'
    if bad=='script':script.write_text('changed')
    with pytest.raises(ValueError):a.registered_activation(api,pp,lp,'activation',script,'123',{},argv)


def test_prepare_recovery_passes_exact_byte_packet_schema_after_immutable_index(tmp_path,monkeypatch):
    root=tmp_path/'r';campaign=root/'campaign';campaign.mkdir(parents=True);case=root/'case'
    mutex=root/'permanent.mutex';mutex.write_text('permanent')
    for key,value in [('ROOT',root),('CAMPAIGN',campaign),('AMENDMENT',case),('MUTEX',mutex),
        ('WORKER_DESCRIPTOR',root/'worker.json'),('OLD_RECOVERY',root/'old-recovery.json')]:monkeypatch.setattr(a,key,value)
    a.WORKER_DESCRIPTOR.write_text('{}');a.OLD_RECOVERY.write_text('{}')
    monkeypatch.setattr(a,'WORKER_DESCRIPTOR_SHA',digest(a.WORKER_DESCRIPTOR));monkeypatch.setattr(a,'OLD_RECOVERY_SHA',digest(a.OLD_RECOVERY))
    cells=[{'evaluation_case':str(root/('eval-'+str(i))),'training_case':str(root/('train-'+str(i)))} for i in range(78)]
    plan={'cells':cells};plan['plan_sha256']=canonical_hash(plan);monkeypatch.setattr(a,'PLAN_SHA',plan['plan_sha256'])
    evaluation=Path(cells[73]['evaluation_case']);evaluation.mkdir();(evaluation/'plan.json').write_text('{}');(evaluation/'plan-binding.json').write_text('{}')
    statecells=[{} for _ in range(78)];statecells[73]={'evaluation_plan':{'case':str(evaluation),
        'plan_sha256':digest(evaluation/'plan.json'),'plan_binding_sha256':digest(evaluation/'plan-binding.json')}}
    state={'revision':405,'state_sha256':a.ORIGINAL_SHA,'jobs':{str(i):{} for i in range(219)},'cells':statecells}
    raw=gzip.compress(json.dumps(state).encode(),mtime=0);(campaign/'state.json.gz').write_bytes(raw)
    monkeypatch.setattr(a,'ORIGINAL_FILE_SHA',a.sha(raw));(campaign/'state-binding.json').write_text('{}')
    a.exclusive(campaign/'campaign-plan.json',plan)
    (root/'state').mkdir();(root/'logs').mkdir()
    for ident,job in a.TARGETS.items():
        name='af-f2-'+ident;directory=root/'plans'/name;directory.mkdir(parents=True)
        (directory/'plan.json').write_text('{}');(directory/(name+'.sbatch')).write_text('# original')
        (root/'state'/(name+'-submission.json')).write_text('{}')
        (campaign/('submission-'+ident+'.json')).write_text('{}')
        (root/'logs'/(name+'-'+job+'_4294967294.log')).write_text('failure traceback')
    stat=mutex.stat();events=[]
    def pure(plan_value,state_value,proof):
        assert (case/'source-packet-index.json').is_file() and not (case/'amendment.json').exists()
        assert set(proof)=={'case','dispatcher_sha256','approval','original_state_gzip','original_state_binding','original_campaign_plan',
            'worker_descriptor','failed','old_controllers','absence','old_recovery','producer'}
        assert proof['case']==str(case) and set(proof['failed'])==set(a.TARGETS) and set(proof['old_controllers'])==a.CONTROLLERS
        assert all(set(p)=={'path','sha256','raw'} for p in proof['old_controllers'].values())
        for ident,evidence in proof['failed'].items():
            expected={'native','native_plan','submission_ledger','script','submission_journal','traceback'}
            if ident.startswith('evaluation-'):expected|={'evaluation_plan','evaluation_plan_binding'}
            assert set(evidence)==expected
            assert all(set(p)=={'path','sha256','raw'} and a.sha(p['raw'])==p['sha256'] for p in evidence.values())
        assert json.loads(proof['producer']['raw'])['source_sha256']==a.SOURCE
        assert json.loads(proof['producer']['raw'])['native']['job_id']=='123'
        absence=json.loads(proof['absence']['raw']);assert absence['mutex_inode']==stat.st_ino and len(absence['evaluation_top_level'])==2
        assert (case/'original-state.json.gz').read_bytes()==raw
        events.append('pure-after-byte-index')
        return {'state':{'revision':406,'state_sha256':'b'*64},'amendment':{'original_state_sha256':a.ORIGINAL_SHA,'amendment_sha256':'c'*64}}
    def observed(ids,*args,**kwargs):
        return {job:{'job_id':job,'state':'RUNNING' if job=='123' else 'FAILED','exit_code':'0:0' if job=='123' else '1:0',
            'cpus':4,'gpus':0,'elapsed_seconds':1,'end':'Unknown','user':'pwa209','account':'def-ptewarie_cpu'} for job in ids}
    c={'job':'123','end':time.time()+10000,'registration_locked':True,'mutex_identity':(stat.st_dev,stat.st_ino),
        'verify_sources':lambda:events.append('source-check'),'old_descriptor':{},'native_reader':observed,
        'controller':{'normalized_native':lambda job,record:record,'validate_retry_value':lambda *args:events.append('metadata-check')},
        'recovery':{'absence_paths':lambda plan:[root/'missing-target'],'recover':pure,'DISPATCH_SHA':'d'*64}}
    result,new_plan,old_state,paths,originals,index=a.prepare_recovery(c,{'source':'current_user'})
    assert result['state']['revision']==406 and new_plan==plan and old_state==state and len(index)>=30
    assert events==['source-check','pure-after-byte-index','metadata-check']
    assert (case/'amendment.json').is_file() and (campaign/'state.json.gz').read_bytes()==raw
    a.verify_preserved_sources(index)
