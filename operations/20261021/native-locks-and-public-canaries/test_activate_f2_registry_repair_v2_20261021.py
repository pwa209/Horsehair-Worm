"""Synthetic CAS/scheduler contracts; no live files, jobs or checkpoints."""
from pathlib import Path
from contextlib import contextmanager
import copy
import gzip
import importlib.util
import json
import subprocess
import time
import base64
import pytest
from horsehair.core import canonical_hash,digest

spec=importlib.util.spec_from_file_location('private_f2_activation',Path(__file__).with_name('activate-f2-registry-repair-v2-20261021.py'))
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)


@pytest.fixture
def cas(tmp_path,monkeypatch):
    campaign=tmp_path/'campaign';campaign.mkdir();case=tmp_path/'amendment';case.mkdir()
    monkeypatch.setattr(a,'CAMPAIGN',campaign);monkeypatch.setattr(a,'AMENDMENT',case)
    old=gzip.compress(b'{"old":true}',mtime=0);binding=b'{"old_binding":true}\n'
    (campaign/'state.json.gz').write_bytes(old);(campaign/'state-binding.json').write_bytes(binding)
    originals={'original_state_gzip':{'sha256':a.sha(old)},'original_state_binding':{'sha256':a.sha(binding)}}
    result={'state':{'state_sha256':'a'*64,'revision':2,'value':'synthetic derivative'},'amendment':{'amendment_sha256':'b'*64}}
    plan={'plan_sha256':'c'*64};paths=[tmp_path/'new_retry_path']
    return result,plan,paths,originals,old,binding


def test_lock_held_cas_preserves_explicit_intent_and_verifies_both_installed_files(cas):
    result,plan,paths,originals,old,binding=cas
    a.install_state(result,plan,paths,originals,'123')
    assert json.loads(gzip.decompress((a.CAMPAIGN/'state.json.gz').read_bytes()))==result['state']
    new=json.loads((a.CAMPAIGN/'state-binding.json').read_bytes())
    assert new['state_file_sha256']==digest(a.CAMPAIGN/'state.json.gz')
    assert json.loads((a.AMENDMENT/'cas-intent.json').read_text())['old_state_file_sha256']==a.sha(old)
    assert json.loads((a.AMENDMENT/'installation.json').read_text())['healthy_policies_retrained'] is False


@pytest.mark.parametrize('bad',['state','binding','retry_exists'])
def test_cas_refuses_changed_original_or_existing_retry_before_any_replacement(cas,bad):
    result,plan,paths,originals,*_=cas
    if bad=='state':(a.CAMPAIGN/'state.json.gz').write_bytes(b'changed')
    if bad=='binding':(a.CAMPAIGN/'state-binding.json').write_bytes(b'changed')
    if bad=='retry_exists':paths[0].mkdir()
    with pytest.raises(ValueError):a.install_state(result,plan,paths,originals,'123')
    assert not (a.AMENDMENT/'cas-intent.json').exists()


def test_interrupted_state_pair_stays_explicit_never_automatically_replayed_or_rolled_back(cas):
    result,plan,paths,originals,old,binding=cas;calls=[]
    def interrupted(source,target):
        calls.append((source,target))
        if len(calls)==2:raise OSError('synthetic interruption after state publication')
        a.os.replace(source,target)
    with pytest.raises(OSError):a.install_state(result,plan,paths,originals,'123',replace=interrupted)
    assert (a.AMENDMENT/'cas-intent.json').exists() and not (a.AMENDMENT/'installation.json').exists()
    assert (a.CAMPAIGN/'state-binding.json').read_bytes()==binding
    assert (a.CAMPAIGN/'state.json.gz').read_bytes()!=old
    with pytest.raises(ValueError):a.install_state(result,plan,paths,originals,'123')
    assert len(calls)==2


@pytest.fixture
def controller_fixture(tmp_path,monkeypatch):
    root=tmp_path/'root';area=root/'maintenance/support/activation';area.mkdir(parents=True)
    (root/'plans').mkdir();(root/'state').mkdir();amendment=root/'maintenance/amendment';amendment.mkdir()
    monkeypatch.setattr(a,'ROOT',root);monkeypatch.setattr(a,'AREA',area);monkeypatch.setattr(a,'AMENDMENT',amendment)
    monkeypatch.setattr(a,'MUTEX',root/'state/registration.mutex');monkeypatch.setattr(a,'CONTROLLER',root/'maintenance/controller.py')
    a.CONTROLLER.write_text('synthetic source')
    lock=[False];events=[]
    @contextmanager
    def mutex(*args,**kw):
        assert not lock[0];lock[0]=True
        try:yield
        finally:lock[0]=False
    monkeypatch.setattr(a,'exclusive_file_lock',mutex)
    monkeypatch.setattr(a,'source_identity',lambda p:{'sha256':a.SOURCE})
    monkeypatch.setattr(a.slurm,'_script',lambda config,framework,name,command,**kw:'#!/bin/bash\nset -euo pipefail\n'+command+'\n')
    status={};tasks={}
    def scheduler(job,path):
        path.mkdir(parents=True,exist_ok=False)
        task=tasks[job]
        return {'JobId':job,'UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobName':'hhw-'+task['name'],
            'Command':task['script'],'JobState':status[job],'Reason':'JobHeldUser' if status[job]=='PENDING' else 'None',
            'NumCPUs':'1','NumNodes':'1-1','MinMemoryNode':'16G','TimeLimit':'4-00:00:00','ReqTRES':'cpu=1','AllocTRES':''}
    controller={'fields':lambda x:x,'seconds':lambda s:96*3600 if s=='4-00:00:00' else 0,
        'gpu_count':lambda s:1 if 'gpu' in s else 0,'worker_binding':lambda d:None,'registry_binding':lambda d:None}
    def admission(label,**kw):assert lock[0];events.append(('admission',label))
    config={'paths':{'cpu_account':'def-ptewarie_cpu'}}
    c={'job':'999','config':config,'policy':type('Policy',(),{'receipt':lambda s:{'sha256':'d'*64}})(),
        'controller':controller,'scheduler':scheduler,'registry_api':{'registration_lock':mutex},'end':time.time()+3000,'migration':{},
        'pins':{'run-af-agent-controller-registry-v3-20261021.py':'e'*64,'registered-snapshot-read-guard-v3-20261021.py':'f'*64,
            'run-f00d-worker-registry-overlay-20261021.py':'a'*64},'qpath':area/'q.json','qsha':'b'*64,'q':{'slurm_job_id':'123'},
        'admission':admission,'native_reader':lambda *args,**kw:{},'verify_sources':lambda:events.append(('source-check',))}
    result={'state':{'state_sha256':'c'*64},'amendment':{'amendment_sha256':'a'*64}}
    def prepare(*args):
        assert lock[0];a.exclusive(amendment/'amendment.json',result['amendment']);return result,{},[],{}
    monkeypatch.setattr(a,'prepare_migration',prepare);monkeypatch.setattr(a,'verify_old_files',lambda s:None)
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
    result=a.orchestrate(c,{}, {},{}, {'source':'current_user'},runner=runner)
    assert len(result['jobs'])==4 and all(v=='RUNNING' for v in status.values()) and not lock[0]
    assert events.index(('cas',))<next(i for i,v in enumerate(events) if v[:2]==('scontrol','release'))
    assert events.count(('submit',))==1 and not any(v[0]=='scancel' for v in events)
    plan=json.loads((a.ROOT/'plans'/a.PHASE/'plan.json').read_text())
    assert plan['tasks'][0]['depends']==[] and all(t['dependency_kind']=='afterany' for t in plan['tasks'])
    assert all(t['resources']=={'cpus':1,'gpu':False,'hours':96,'memory':'16G','array':None} for t in plan['tasks'])


def test_pre_cas_failure_cancels_only_new_positively_held_jobs(controller_fixture):
    c,events,status,tasks,runner,lock=controller_fixture
    def admission(label,**kw):
        if label=='activation-all-new-controllers-registered':raise ValueError('synthetic post-registration refusal')
    c['admission']=admission
    with pytest.raises(ValueError):a.orchestrate(c,{}, {},{}, {},runner=runner)
    assert ('cas',) not in events and set(status.values())=={'CANCELLED'}
    assert len([v for v in events if v[0]=='scancel'])==4 and events.count(('submit',))==1
    assert not any(v[1] in a.OLD_IDS for v in events if v[0]=='scancel')


def test_ambiguous_release_never_replays_or_cancels_the_released_running_job(controller_fixture):
    c,events,status,tasks,runner,lock=controller_fixture
    def ambiguous(command,**kwargs):
        value=runner(command,**kwargs)
        if command[:2]==['scontrol','release']:raise subprocess.TimeoutExpired(command,30)
        return value
    with pytest.raises(subprocess.TimeoutExpired):a.orchestrate(c,{}, {},{}, {},runner=ambiguous)
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
        'tasks':{task['name']:{'job_id':job,'state':'submitted'}}})
    a.cleanup_held(c,pack,runner=runner)
    assert not any(v[0]=='scancel' for v in events)


def test_immutable_evidence_cannot_be_overwritten(tmp_path):
    path=tmp_path/'intent.json';a.exclusive(path,{'before':True})
    with pytest.raises(FileExistsError):a.exclusive(path,{'after':True})
    assert json.loads(path.read_text())=={'before':True}


def actual253_source():
    """The actual immutable metadata roster, never synthetic science outcomes."""
    native=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-f2-race-repair-evidence-v1-20261021/evidence-packet.json')
    if native.is_file():
        assert digest(native)=='b505d32ecb47f4365764acdfb6c2241312a9f122c09fa00464f2eed2c2849d37'
        return json.loads(native.read_bytes())
    local=Path(__file__).parent/'narval-deployment/hhw-af-f2-repair-evidence-20261021-253.txt'
    value=json.loads(local.read_text().split('AF_F2_REPAIR_PACKET ',1)[1].splitlines()[0])
    raw=(json.dumps(value,indent=2,sort_keys=True)+'\n').encode()
    assert a.sha(raw)=='b505d32ecb47f4365764acdfb6c2241312a9f122c09fa00464f2eed2c2849d37'
    return value


@pytest.fixture
def complete253_metadata(tmp_path,monkeypatch):
    original=actual253_source();root=tmp_path/'scratch';root.mkdir();primary=tmp_path/'project'/'primary';primary.mkdir(parents=True)
    old_root=a.ROOT;old_primary=a.PRIMARY;source=copy.deepcopy(original)
    assert len(source['files'])==27 and 'full_config' in source['files']
    for role,entry in source['files'].items():
        original_path=Path(entry['path'])
        target=(primary/'configs/study.json') if role=='full_config' else root/original_path.relative_to(old_root)
        if role=='full_config':assert original_path==old_primary/'configs/study.json'
        raw=base64.b64decode(entry['base64'],validate=True);assert a.sha(raw)==entry['sha256'] and len(raw)==entry['bytes']
        target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw);entry['path']=str(target)
    source['checkpoint_identities']={}  # Actual checkpoint bytes are not metadata fixtures.
    monkeypatch.setattr(a,'ROOT',root);monkeypatch.setattr(a,'PRIMARY',primary)
    # Model only the exact Alliance project alias; no actual symlink privilege is needed.
    alias=primary.parent;target=tmp_path/'resolved-project';target.mkdir()
    monkeypatch.setattr(a,'PROJECT_ALIAS',alias);monkeypatch.setattr(a,'PROJECT_TARGET',target)
    monkeypatch.setattr(a,'PRIMARY_RESOLVED',target/primary.name)
    old_resolve=Path.resolve;old_symlink=Path.is_symlink
    def resolve(path,*args,**kwargs):
        return target/path.relative_to(alias) if path.is_relative_to(alias) else old_resolve(path,*args,**kwargs)
    monkeypatch.setattr(Path,'resolve',resolve)
    monkeypatch.setattr(Path,'is_symlink',lambda path:path==alias or old_symlink(path))
    identity={'sha256':a.PRIMARY_SHA,'files':{'configs/study.json':source['files']['full_config']['sha256']}}
    monkeypatch.setattr(a,'source_identity',lambda path:copy.deepcopy(identity))
    return source,identity


def test_actual253_complete27_role_roster_accepts_exact_primary_full_config(complete253_metadata):
    source,identity=complete253_metadata
    assert not Path(source['files']['full_config']['path']).is_relative_to(a.ROOT)
    with pytest.raises(ValueError,match='Ordinary confined'):a.packet(source['files']['full_config']['path'])
    a.verify_old_files(source)
    assert canonical_hash(json.loads(base64.b64decode(source['files']['full_config']['base64'])))==a.CONFIG


@pytest.mark.parametrize('bad',['other_role','other_primary_file','foreign_primary','wrong_raw_sha','wrong_config','changed_source'])
def test_primary_exception_cannot_expand_source_or_protocol_scope(complete253_metadata,monkeypatch,bad):
    source,identity=complete253_metadata;entry=source['files']['full_config'];path=Path(entry['path'])
    if bad=='other_role':source['files']['not_full_config']=source['files'].pop('full_config')
    if bad=='other_primary_file':
        other=a.PRIMARY/'configs/other.json';other.write_bytes(path.read_bytes());entry['path']=str(other)
    if bad=='foreign_primary':identity['sha256']='a'*64
    if bad=='wrong_raw_sha':identity['files']['configs/study.json']='a'*64
    if bad=='wrong_config':
        raw=b'{"foreign_config":true}\n';path.write_bytes(raw);entry.update(sha256=a.sha(raw),bytes=len(raw));identity['files']['configs/study.json']=entry['sha256']
    if bad=='changed_source':
        calls=[]
        def changing(_):
            calls.append(None);value=copy.deepcopy(identity)
            if len(calls)>1:value['sha256']='b'*64
            return value
        monkeypatch.setattr(a,'source_identity',changing)
    with pytest.raises(ValueError):a.verify_old_files(source)


def test_frozen_failed_v1_sources_and_fresh_namespaces_are_preserved():
    assert a.AREA.name=='native-activation-v2'
    assert a.SUPPORT.name=='study-af-registry-repair-support-v3-20261021'
    assert a.PHASE=='study-af-f2-registry-r1-controllers-v2-20261021'
    assert a.FAILED_ACTIVATION.parent.name=='native-activation'
    assert a.FAILED_ACTIVATION_SHA=='34ff2e73aa3909d4a6bac87fd3fafb04fd7cc3fb4fa353e34835c3a15478c943'


@pytest.fixture
def pristine275(complete253_metadata,monkeypatch):
    source,_=complete253_metadata
    native=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-postactivation-evidence-v1-20261021/evidence.json')
    if native.is_file():observed=json.loads(native.read_bytes())
    else:
        local=Path(__file__).parent/'narval-deployment/hhw-af-postactivation-evidence-20261021-275.txt'
        observed=json.loads(local.read_text().split('AF_POST_ACTIVATION_EVIDENCE ',1)[1].splitlines()[0])
    path=a.ROOT/'maintenance/old-support/native-activation/activation-failure.json';path.parent.mkdir(parents=True)
    a.exclusive(path,observed['activation_failure']['record']);assert digest(path)==a.FAILED_ACTIVATION_SHA
    monkeypatch.setattr(a,'FAILED_ACTIVATION',path);observed['activation_failure']['path']=str(path)
    monkeypatch.setattr(a,'AMENDMENT',a.ROOT/'maintenance/state-amendment')
    observed['amendment']['path']=str(a.AMENDMENT)
    old_phase='study-af-f2-registry-r1-controllers-v1-20261021'
    observed['new_controller_plan']['path']=str(a.ROOT/'plans'/old_phase)
    observed['new_controller_ledger']['path']=str(a.ROOT/'state'/(old_phase+'-submission.json'))
    observed['failed_training_case']['path']=str(Path(observed['failed_training_case']['path']))
    observed['current_original_state']['path']=source['files']['original_state_gzip']['path']
    return source,observed


def test_actual275_history_proves_failed_v1_did_not_install_or_retrain(pristine275):
    a.verify_pristine_postactivation(*pristine275)


@pytest.mark.parametrize('bad',['installed','revision','healthy_job','failure_bytes','old_plan_exists'])
def test_no_pristine_replay_after_installation_history_or_state_change(pristine275,bad):
    source,observed=pristine275
    if bad=='installed':observed['amendment']['exists']=True
    if bad=='revision':observed['current_original_state']['revision']+=1
    if bad=='healthy_job':observed['current_original_state']['jobs']['training-0001']['job_id']='7'
    if bad=='failure_bytes':a.FAILED_ACTIVATION.write_bytes(b'changed')
    if bad=='old_plan_exists':Path(observed['new_controller_plan']['path']).mkdir(parents=True)
    with pytest.raises(ValueError):a.verify_pristine_postactivation(source,observed)


@pytest.mark.parametrize('bad',['other_alias_target','alias_not_symlink','redirected_lower_ancestor'])
def test_only_actual280_project_alias_is_allowed(complete253_metadata,monkeypatch,bad):
    source,_=complete253_metadata
    if bad=='other_alias_target':monkeypatch.setattr(a,'PROJECT_TARGET',a.PROJECT_TARGET/'other')
    if bad=='alias_not_symlink':monkeypatch.setattr(Path,'is_symlink',lambda path:False)
    if bad=='redirected_lower_ancestor':
        previous=Path.is_symlink
        monkeypatch.setattr(Path,'is_symlink',lambda path:path==a.PRIMARY/'configs' or previous(path))
    with pytest.raises(ValueError):a.verify_old_files(source)
