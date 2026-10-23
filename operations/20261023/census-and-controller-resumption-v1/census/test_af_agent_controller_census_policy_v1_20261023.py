"""External census policy contracts; all inventories and native rows synthetic."""
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from datetime import datetime,timezone
import ast
import hashlib
import importlib.util
import json
import shlex
import stat

import pytest

SOURCE=Path(__file__).with_name('run-af-agent-controller-census-policy-v1-20261023.py')
spec=importlib.util.spec_from_file_location('external_census_policy',SOURCE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class PendingStorage(ValueError):pass


def measured(path,size=7):
    sha=hashlib.sha256(b'x'*size).hexdigest()
    return {'schema_version':1,'path':str(path),'relative_path':path.name,'status':'measured','method':'owned_regular_length',
        'bytes':size,'max_bytes':6_000_000_000_000,'prefix_cap_bytes':65536,'prefix_bytes':size,'prefix_sha256':sha,
        'content_sha256':sha,'eof_observed':True,'identity':{'inode':1,'uid':3164768,'mode':stat.S_IFREG|0o600,
            'nlink':1,'device_major':1,'device_minor':2,'ctime_ns':3},
        'size_proof':{'kind':'prefix_eof','probes':[{'offset':size-1,'bytes':1},{'offset':size,'bytes':0}]}}


@pytest.fixture
def pair(tmp_path,monkeypatch):
    root=tmp_path/'study';(root/'state').mkdir(parents=True);monkeypatch.setattr(m,'ROOT',root)
    detail={'root':str(root),'status':'measured','method':'owned_regular_length','count':1,'bytes':7,'items':[measured(root/'sample')]}
    dpath=root/'state/storage-entry-sizes.json';dpath.write_text(json.dumps(detail))
    quota=root/'state/quota.txt';quota.write_bytes(b'synthetic quota')
    value={'root':str(root),'status':'measured','personal_uid':3164768,'measured_unix':1000.,
        'captured_utc':datetime.fromtimestamp(1000.,timezone.utc).isoformat(),'study_files':1,'study_bytes':7,
        'exact_entry_measurements':{'method':'owned_regular_length','count':1,'bytes':7,'receipt_path':str(dpath),'receipt_sha256':m.digest(dpath)},
        'native_source_paths':[str(quota)],'native_source_hashes':{str(quota):m.digest(quota)}}
    budget=root/'state/storage-budget.json';budget.write_text(json.dumps(value))
    return SimpleNamespace(root=root,budget=budget,detail=dpath,quota=quota,value=value,inventory=detail,base=SimpleNamespace(PendingStorage=PendingStorage))


def reseal_pair(f):
    f.detail.write_text(json.dumps(f.inventory));f.value['exact_entry_measurements']['receipt_sha256']=m.digest(f.detail)
    f.budget.write_text(json.dumps(f.value))


def test_strict_original_global_pair_is_consumed_without_scanning(pair):
    value,hashes=m.fresh_pair(pair.base,clock=lambda:1001.)
    assert value==pair.value and hashes=={'budget':m.digest(pair.budget),'entry_sizes':m.digest(pair.detail)}
    assert value['measured_unix']==1000.


@pytest.mark.parametrize('clock',[999.,22600.,22601.,float('nan'),float('inf')])
def test_stale_future_and_nonfinite_clock_wait_without_inline_scan(pair,clock):
    with pytest.raises(PendingStorage):m.fresh_pair(pair.base,clock=lambda:clock)


@pytest.mark.parametrize('change',['missing_budget','missing_detail','detail_bytes','quota_bytes','count','total','root','uid','path',
    'proof','owner','eof','duplicate','clock_zone','clock_captured'])
def test_partial_mixed_or_resealed_invalid_inventory_never_admits(pair,change):
    f=pair
    if change=='missing_budget':f.budget.unlink()
    if change=='missing_detail':f.detail.unlink()
    if change=='detail_bytes':f.detail.write_bytes(f.detail.read_bytes()+b' ')
    if change=='quota_bytes':f.quota.write_bytes(b'changed')
    if change=='count':f.inventory['count']=2;reseal_pair(f)
    if change=='total':f.inventory['bytes']=8;reseal_pair(f)
    if change=='root':f.value['root']='/foreign';f.budget.write_text(json.dumps(f.value))
    if change=='uid':f.value['personal_uid']=42;f.budget.write_text(json.dumps(f.value))
    if change=='path':f.inventory['items'][0]['relative_path']='../outside';reseal_pair(f)
    if change=='proof':f.inventory['items'][0]['size_proof']['probes']=[];reseal_pair(f)
    if change=='owner':f.inventory['items'][0]['identity']['uid']=42;reseal_pair(f)
    if change=='eof':f.inventory['items'][0]['eof_observed']=False;reseal_pair(f)
    if change=='duplicate':f.inventory['items']*=2;f.inventory['count']=f.value['study_files']=f.value['exact_entry_measurements']['count']=2;f.inventory['bytes']=f.value['study_bytes']=f.value['exact_entry_measurements']['bytes']=14;reseal_pair(f)
    if change=='clock_zone':f.value['captured_utc']='1970-01-01T00:16:40';f.budget.write_text(json.dumps(f.value))
    if change=='clock_captured':f.value['captured_utc']=datetime.fromtimestamp(0.,timezone.utc).isoformat();f.budget.write_text(json.dumps(f.value))
    with pytest.raises(PendingStorage):m.fresh_pair(f.base,clock=lambda:22000.)


def test_clock_expiry_during_pair_validation_waits(pair):
    times=iter([1001.,22600.,22600.])
    with pytest.raises(PendingStorage):m.fresh_pair(pair.base,clock=lambda:next(times))


def test_mutable_pair_replacement_is_rejected(pair,monkeypatch):
    original=m.digest
    def changed(path):
        if Path(path)==pair.budget:return '0'*64
        return original(path)
    monkeypatch.setattr(m,'digest',changed)
    with pytest.raises(PendingStorage,match='changed'):m.fresh_pair(pair.base,clock=lambda:1001.)


def frozen_base():
    path=m.BASE if m.BASE.exists() else SOURCE.with_name('run-af-agent-controller-registry-v6-20261022.py')
    assert m.digest(path)==m.BASE_SHA
    spec=importlib.util.spec_from_file_location('frozen_controller6_census_fixture',path)
    base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base);return base,path


def test_install_changes_only_fresh_census_all_base_functions_objects_and_bodies_retained(pair,monkeypatch):
    base,path=frozen_base();objects={k:v for k,v in vars(base).items() if callable(v)}
    tree_before=ast.dump(ast.parse(path.read_text()),include_attributes=False)
    c={};context={'base_context':c};monkeypatch.setattr(m,'policy_guard',lambda context:None)
    monkeypatch.setattr(m,'provider_proof',lambda *args:{'positive_finalized':'synthetic'})
    installed=m.install(base,context)
    def forbidden(*a,**k):pytest.fail('No inline scan/subprocess/sleep')
    value=base.fresh_census(c,clock=lambda:1001.,popen=forbidden,sleeper=forbidden)
    assert value==pair.value and c['external_census_proof']=={'positive_finalized':'synthetic'}
    assert base.fresh_census is installed['installed_fresh_census'] and base.fresh_census is not objects['fresh_census']
    assert all(getattr(base,name) is obj for name,obj in objects.items() if name!='fresh_census')
    assert ast.dump(ast.parse(path.read_text()),include_attributes=False)==tree_before


def test_installed_policy_stale_wait_does_not_reach_provider_or_original_scan(pair,monkeypatch):
    base,_=frozen_base();c={};monkeypatch.setattr(m,'policy_guard',lambda context:None)
    def forbidden(*a,**k):pytest.fail('Stale pair must wait before any scan/provider')
    monkeypatch.setattr(m,'provider_proof',forbidden);m.install(base,{'base_context':c})
    with pytest.raises(base.PendingStorage):base.fresh_census(c,clock=lambda:22600.,popen=forbidden,sleeper=forbidden)


def test_provider_publication_change_after_completion_proof_waits(pair,monkeypatch):
    base,_=frozen_base();c={};monkeypatch.setattr(m,'policy_guard',lambda context:None)
    def replace(*args):
        pair.value['study_bytes']=8;pair.inventory['bytes']=8;pair.inventory['items']=[measured(pair.root/'sample',8)]
        pair.value['exact_entry_measurements']['bytes']=8;reseal_pair(pair);return {'positive_finalized':'synthetic'}
    monkeypatch.setattr(m,'provider_proof',replace);m.install(base,{'base_context':c})
    with pytest.raises(base.PendingStorage,match='publication changed'):base.fresh_census(c,clock=lambda:1001.)


def test_original_retry_namespace_journal_and_producer_refresh_are_not_replaced(pair,monkeypatch):
    base,_=frozen_base();names=('retry_binding','retry_overrides','native_namespace','submission_journal','refresh_retry_producer_accounting','source_guard','storage_guard','tick','run')
    original={n:getattr(base,n) for n in names};m.install(base,{'base_context':{}})
    assert all(getattr(base,n) is function for n,function in original.items())


def test_wrong_controller_context_cannot_use_installed_policy(pair,monkeypatch):
    base,_=frozen_base();m.install(base,{'base_context':{}})
    with pytest.raises(ValueError,match='one verified'):base.fresh_census({},clock=lambda:1001.)


def test_no_scanning_or_scheduler_mutation_calls_in_outer_source():
    tree=ast.parse(SOURCE.read_text())
    names={n.id for n in ast.walk(tree) if isinstance(n,ast.Name)}
    assert not names.intersection({'storage_snapshot','Popen','subprocess','sbatch','scancel'})
    stores=[n for n in ast.walk(tree) if isinstance(n,ast.Attribute) and isinstance(n.ctx,ast.Store)]
    assert [n.attr for n in stores]==['fresh_census']
    main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main');text=ast.get_source_segment(SOURCE.read_text(),main)
    assert text.index('registered(')<text.index('c=base.load_context')<text.index('qualification(policy)')<text.index('install(base,context)')<text.index('return base.run(c)')


@pytest.mark.parametrize('memory,accept',[('64G',True),('65536M',True),('16G',False),('32G',False)])
def test_new_outer_native_allocation_requires_64g_while_base_is_unchanged(memory,accept):
    base,path=frozen_base();before=m.digest(path)
    fields={'JobId':'999','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'Command':'/synthetic/outer.sbatch','JobName':'hhw-outer','NumCPUs':'1','NumNodes':'1','TimeLimit':'4-00:00:00',
        'MinMemoryNode':memory,'AllocTRES':'cpu=1,mem='+memory+',node=1','ReqTRES':'cpu=1,mem='+memory+',node=1'}
    if accept:m.outer_native(base,fields,'999','outer',fields['Command'])
    else:
        with pytest.raises(ValueError,match='64G'):m.outer_native(base,fields,'999','outer',fields['Command'])
    assert m.digest(path)==before==m.BASE_SHA


@pytest.fixture
def provider(pair,monkeypatch):
    f=pair;root=f.root;source=root/'maintenance/provider.py';source.parent.mkdir()
    selected={'case_id':'rolling-01','native_job_id':'456','predecessor_job_id':None,'slot_count':1,'maximum_cpu_core_hours':48}
    source.write_text('''def provider_registration(args,job,config):
    assert args.case_id=='rolling-01' and job=='456'
    return {'case_id':'rolling-01','native_job_id':'456','predecessor_job_id':None,'slot_count':1,'maximum_cpu_core_hours':48}
def memory(value):
    if value!='64G':raise ValueError('memory')
    return 64*1024**3
def cpu_only_tres(value):
    if value!='cpu=8,mem=64G,node=1':raise ValueError('tres')
''')
    pdir=root/'plans/provider';pdir.mkdir(parents=True);plan_path=pdir/'plan.json';script=pdir/'provider.sbatch';ledger_path=root/'state/provider-submission.json'
    flags={'--case-id':'rolling-01','--source-manifest':'synthetic-manifest','--source-manifest-sha256':'a'*64,
        '--kernel-qualification':'synthetic-q','--kernel-qualification-sha256':'b'*64,'--kernel-qualification-job-id':'123',
        '--native-plan':str(plan_path),'--native-ledger':str(ledger_path),'--native-task':'rolling-storage-census-01',
        '--native-script':str(script),'--registry-qualification':'old-q','--registry-qualification-sha256':'c'*64,'--registry-qualification-job-id':'789'}
    command=shlex.join(['python',str(source),*[item for pair in flags.items() for item in pair]]);script.write_text(command)
    task={'name':'rolling-storage-census-01','script':str(script),'command':command,'script_sha256':m.canonical_hash(script.read_text()),
        'resources':{'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None}}
    plan={'release_identity':{'sha256':m.SOURCE},'config_sha256':m.CONFIG,'tasks':[task]};plan_path.write_text(json.dumps(plan))
    ledger={'source_sha256':m.SOURCE,'plan_sha256':m.canonical_hash(plan),'tasks':{task['name']:{'job_id':'456','state':'submitted','script_sha256':task['script_sha256']}}};ledger_path.write_text(json.dumps(ledger))
    cases=root/'maintenance/provider-results';case=cases/'native-456';case.mkdir(parents=True);monkeypatch.setattr(m,'PROVIDER_CASE',cases)
    copied={}
    for key,path in (('budget',f.budget),('entry_sizes',f.detail)):
        target=case/('original-storage-budget.json' if key=='budget' else 'original-storage-entry-sizes.json');target.write_bytes(path.read_bytes());copied[key]=m.locator(target)
    sweep=case/'sweep-result.json';sweep.write_text(json.dumps(copied))
    schedule={'plan':m.locator(plan_path),'ledger':m.locator(ledger_path),'helper':m.locator(source),
        'source_manifest':{'path':'synthetic-manifest','sha256':'a'*64},'qualification':{'path':'synthetic-q','sha256':'b'*64,'job_id':'123'},'case_root':str(cases)}
    registration={'plan':schedule['plan'],'ledger':schedule['ledger'],'script':m.locator(script),'task_name':task['name'],'native_job_id':'456'}
    fields={'JobId':'456','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING','NumCPUs':'8','NumNodes':'1',
        'Command':str(script),'JobName':'hhw-'+task['name'],'MinMemoryNode':'64G','TimeLimit':'06:00:00','ReqTRES':'cpu=8,mem=64G,node=1','AllocTRES':'cpu=8,mem=64G,node=1'}
    receipt={'status':'complete_exact_size_sweep_with_original_conservative_freshness','native_job_id':'456','wrapper_sha256':schedule['helper']['sha256'],
        'source_manifest':schedule['source_manifest'],'qualification':schedule['qualification'],'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,
        'sweep':m.locator(sweep),'allocation':{'native_registration':registration,'fields':fields},'case_id':'rolling-01','provider_registration':selected,**{key:False for key in m.FALSE_FLAGS}}
    path=case/'receipt.json';path.write_text(json.dumps(receipt))
    row={'state':'COMPLETED','exit_code':'0:0','cpus':8,'gpus':0,'elapsed_seconds':100}
    proof={'native_records':{'456':row}};monkeypatch.setattr(m.slurm,'completed_native_dependency',lambda *args:proof)
    events=[]
    @contextmanager
    def guard(c,role):events.append('lock');yield;events.append('unlock')
    base=SimpleNamespace(PendingStorage=PendingStorage,registration_guard=guard,admission=lambda *a,**k:events.append('admission'))
    context={'policy':{'provider_schedule':schedule},'base_context':{'config':{}}}
    f.provider=SimpleNamespace(path=path,receipt=receipt,schedule=schedule,context=context,base=base,row=row,proof=proof,events=events,fields=fields)
    return f


def test_current_global_pair_requires_finalized_exact_registered_provider(provider):
    f=provider;_,pair=m.fresh_pair(f.provider.base,clock=lambda:1001.)
    value=m.provider_proof(f.provider.base,f.provider.context,pair)
    assert value['job_id']=='456' and value['completion']==f.provider.proof and f.provider.events==[]


@pytest.mark.parametrize('change',['no_receipt','wrong_pair','failed','zero_time','cpu','gpu','own_id','case_id','wrapper','manifest','source',
    'owner','memory','native_script','native_cpu','native_gpu','scientific'])
def test_wrong_or_unfinalized_provider_never_admits(provider,change):
    f=provider;p=f.provider
    if change=='no_receipt':p.path.unlink()
    if change=='wrong_pair':p.receipt['sweep']['sha256']='0'*64
    if change=='failed':p.row['state']='FAILED'
    if change=='zero_time':p.row['elapsed_seconds']=0
    if change=='cpu':p.row['cpus']=4
    if change=='gpu':p.row['gpus']=1
    if change=='own_id':p.receipt['native_job_id']='457'
    if change=='case_id':p.receipt['case_id']='rolling-02'
    if change=='wrapper':p.receipt['wrapper_sha256']='0'*64
    if change=='manifest':p.receipt['source_manifest']={}
    if change=='source':p.receipt['source_sha256']='0'*64
    if change=='owner':p.fields['UserId']='foreign(1)'
    if change=='memory':p.fields['MinMemoryNode']='16G'
    if change=='native_script':p.fields['Command']='/foreign'
    if change=='native_cpu':p.fields['NumCPUs']='4'
    if change=='native_gpu':p.fields['AllocTRES']+='gres/gpu=1'
    if change=='scientific':p.receipt['scientific_gates_cleared']=True
    if change!='no_receipt':p.path.write_text(json.dumps(p.receipt))
    _,pair=m.fresh_pair(p.base,clock=lambda:1001.)
    with pytest.raises(ValueError):m.provider_proof(p.base,p.context,pair)


def test_missing_persisted_provider_accounting_refreshes_real_locked_admission_once(provider,monkeypatch):
    f=provider;p=f.provider;calls=[]
    def completed(*args):calls.append(True);return None if len(calls)==1 else p.proof
    monkeypatch.setattr(m.slurm,'completed_native_dependency',completed)
    _,pair=m.fresh_pair(p.base,clock=lambda:1001.);m.provider_proof(p.base,p.context,pair)
    assert len(calls)==2 and p.events==['lock','admission','unlock']


def test_still_pending_provider_after_actual_refresh_waits_without_fake_record(provider,monkeypatch):
    p=provider.provider;monkeypatch.setattr(m.slurm,'completed_native_dependency',lambda *args:None)
    _,pair=m.fresh_pair(p.base,clock=lambda:1001.)
    with pytest.raises(PendingStorage,match='not finalized'):m.provider_proof(p.base,p.context,pair)
    assert p.events==['lock','admission','unlock']


def test_unfinalized_partial_receipt_is_never_read_or_parsed(provider,monkeypatch):
    p=provider.provider;p.path.write_bytes(b'{')
    monkeypatch.setattr(m.slurm,'completed_native_dependency',lambda *args:None)
    original=m.packet
    def no_partial(path,*args,**kwargs):
        if Path(path)==p.path:pytest.fail('Unfinalized partial receipt must not be read or parsed')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(m,'packet',no_partial)
    _,pair=m.fresh_pair(p.base,clock=lambda:1001.)
    with pytest.raises(PendingStorage,match='not finalized'):m.provider_proof(p.base,p.context,pair)
    assert p.events==['lock','admission','unlock']


def test_finalized_malformed_receipt_is_a_hard_refusal(provider):
    p=provider.provider;p.path.write_bytes(b'{')
    _,pair=m.fresh_pair(p.base,clock=lambda:1001.)
    with pytest.raises(json.JSONDecodeError):m.provider_proof(p.base,p.context,pair)


def test_nonfinite_or_unbounded_provider_roster_refused(provider):
    p=provider.provider;plan=json.loads(Path(p.schedule['plan']['path']).read_text());plan['tasks']*=65
    Path(p.schedule['plan']['path']).write_text(json.dumps(plan));p.schedule['plan']['sha256']=m.digest(p.schedule['plan']['path'])
    with pytest.raises(ValueError,match='Finite'):m.provider_proof(p.base,p.context,{'budget':'0'*64,'entry_sizes':'1'*64})


@pytest.fixture
def qualified(pair,monkeypatch):
    f=pair;support=f.root/'maintenance/sources';support.mkdir(parents=True);framework=f.root/'framework';framework.mkdir()
    qpath=f.root/'maintenance/q2/qualification.json';qpath.parent.mkdir()
    monkeypatch.setattr(m,'SUPPORT',support);monkeypatch.setattr(m,'FRAMEWORK',framework);monkeypatch.setattr(m,'QUALIFICATION',qpath)
    base=support/'frozen-base.py';base.write_bytes(SOURCE.with_name('run-af-agent-controller-registry-v6-20261022.py').read_bytes());monkeypatch.setattr(m,'BASE',base)
    for name in m.PRIVATE_FILES:(support/name).write_text('synthetic private '+name)
    for name in [*[name for key,name in m.SUITE_SOURCES.items() if key.startswith('baseline_')],
        'tests/conftest.py','src/horsehair/budget.py','src/horsehair/storage_artifacts.py']:
        p=framework/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('synthetic public '+name)
    manifest={'kind':'streaming-census-operational-source-manifest-v2',
        'files':{name:m.digest(support/name) for name in m.PRIVATE_FILES},'suites':{}}
    pins={str(support/name):sha for name,sha in manifest['files'].items()};suites=[]
    for scope,name in m.SUITE_SOURCES.items():
        p=framework/name if scope.startswith('baseline_') else support/name;sha=m.digest(p);pins[str(p)]=sha
        roster=m.canonical_hash([Path(name).stem+'::positive'])
        manifest['suites'][scope]={'source':name,'source_sha256':sha,'tests':1,'roster_sha256':roster}
        xml=qpath.parent/(scope+'.xml');xml.write_text('<testsuites><testsuite tests="1" errors="0" failures="0" skipped="0"><testcase classname="work.'+Path(name).stem+'" name="positive"/></testsuite></testsuites>')
        suites.append({'scope':scope,'source':{'path':str(p),'sha256':sha},'junit':m.locator(xml),
            'test_counts':{'tests':1,'failures':0,'errors':0,'skipped':0},'test_roster_sha256':roster})
    for name in ('tests/conftest.py','src/horsehair/budget.py','src/horsehair/storage_artifacts.py'):pins[str(framework/name)]=m.digest(framework/name)
    manifest_path=support/'source-manifest.json';manifest_path.write_text(json.dumps(manifest));manifest_binding=m.locator(manifest_path)
    pdir=f.root/'plans/q2';pdir.mkdir(parents=True);plan_path=pdir/'plan.json';ledger_path=f.root/'state/q2-submission.json';script=pdir/'q2.sbatch'
    flags={'--native-plan':str(plan_path),'--native-ledger':str(ledger_path),'--native-task':'q2','--native-script':str(script),
        '--source-manifest':str(manifest_path),'--source-manifest-sha256':manifest_binding['sha256']}
    helper={'path':str(support/m.QUALIFIER_NAME),'sha256':manifest['files'][m.QUALIFIER_NAME]}
    command=shlex.join(['python',helper['path'],*[item for entry in flags.items() for item in entry]]);script.write_text(command)
    task={'name':'q2','command':command,'script':str(script),'script_sha256':m.canonical_hash(script.read_text()),
        'resources':{'cpus':8,'memory':'64G','hours':1,'gpu':False,'array':None},
        'rolling_census_qualification':{'helper':helper,'source_manifest':manifest_binding}}
    plan={'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]};plan_path.write_text(json.dumps(plan))
    ledger={'source_sha256':m.SOURCE,'plan_sha256':m.canonical_hash(plan),'tasks':{'q2':{'job_id':'123','state':'submitted','script_sha256':task['script_sha256']}}};ledger_path.write_text(json.dumps(ledger))
    registration={'plan':m.locator(plan_path),'ledger':m.locator(ledger_path),'script':m.locator(script),'task_name':'q2','native_job_id':'123'}
    q={'status':'complete_rolling_census_operational_qualification','returncode':0,'slurm_job_id':'123','source_sha256':m.SOURCE,
        'config_sha256':m.CONFIG,'source_verified_unchanged':True,'source_manifest':manifest_binding,'source_pins':pins,
        'qualification_helper':helper,
        'base_controller':{'path':str(base),'sha256':m.BASE_SHA},'baseline_iterator_policy':'original_canonical_iterator',
        'private_iterator_policy':'scoped_private_kernel_only','native_completion_required_separately':True,'suites':suites,
        'native_registration':registration,**{key:False for key in m.FALSE_FLAGS}}
    qpath.write_text(json.dumps(q));qbinding=m.locator(qpath)|{'job_id':'123'}
    policy={'policy_source':{'path':str(support/m.SELF_NAME),'sha256':manifest['files'][m.SELF_NAME]},'qualification':qbinding,
        'provider_schedule':{'source_manifest':manifest_binding,'helper':{'path':str(support/m.PROVIDER_NAME),'sha256':manifest['files'][m.PROVIDER_NAME]}}}
    proof={'native_records':{'123':{'state':'COMPLETED','exit_code':'0:0','cpus':8,'gpus':0,'elapsed_seconds':100}}}
    monkeypatch.setattr(m.slurm,'completed_native_dependency',lambda *args:proof)
    f.q=SimpleNamespace(path=qpath,value=q,policy=policy,manifest=manifest,manifest_path=manifest_path,support=support,
        proof=proof,task=task,plan=plan,ledger=ledger,plan_path=plan_path,ledger_path=ledger_path,script=script)
    def verify():
        qpath.write_text(json.dumps(q));policy['qualification']['sha256']=m.digest(qpath);return m.qualification(policy)
    f.q.verify=verify;return f


def test_new_outer_policy_requires_its_own_full_native_q2_suite_and_source_closure(qualified):
    paths=qualified.q.verify()
    assert str(qualified.q.support/m.SELF_NAME) in paths and str(qualified.q.support/m.TEST_NAME) in paths
    assert len(paths)==39


@pytest.mark.parametrize('change',['source','status','returncode','base','missing_suite','old_coverage','bad_roster','bad_count',
    'bad_junit','wrong_helper','wrong_native','pending_native','bad_intent','bad_manifest_kind'])
def test_resealed_wrong_q2_never_qualifies_new_outer_policy(qualified,monkeypatch,change):
    f=qualified.q;q=f.value
    if change=='source':q['source_sha256']='0'*64
    if change=='status':q['status']='complete_old_qualification'
    if change=='returncode':q['returncode']=False
    if change=='base':q['base_controller']['sha256']='0'*64
    if change=='missing_suite':q['suites'].pop()
    if change=='old_coverage':
        f.manifest['suites']['external_census_policy']['source']='test_run_fresh_storage_census_v5_20261023.py'
        f.manifest_path.write_text(json.dumps(f.manifest));f.policy['provider_schedule']['source_manifest']['sha256']=m.digest(f.manifest_path)
    if change=='bad_roster':q['suites'][0]['test_roster_sha256']='0'*64
    if change=='bad_count':q['suites'][0]['test_counts']['tests']=True
    if change=='bad_junit':Path(q['suites'][0]['junit']['path']).write_bytes(b'changed')
    if change=='wrong_helper':(f.support/m.PROVIDER_NAME).write_bytes(b'changed')
    if change=='wrong_native':f.proof['native_records']['123']['cpus']=4
    if change=='pending_native':monkeypatch.setattr(m.slurm,'completed_native_dependency',lambda *a:None)
    if change=='bad_intent':
        f.task['rolling_census_qualification']['helper']['sha256']='0'*64
        f.plan_path.write_text(json.dumps(f.plan));f.ledger['plan_sha256']=m.canonical_hash(f.plan);f.ledger_path.write_text(json.dumps(f.ledger))
        q['native_registration']['plan']=m.locator(f.plan_path);q['native_registration']['ledger']=m.locator(f.ledger_path)
    if change=='bad_manifest_kind':
        f.manifest['kind']='streaming-census-operational-source-manifest-v1';f.manifest_path.write_text(json.dumps(f.manifest))
        f.policy['provider_schedule']['source_manifest']['sha256']=m.digest(f.manifest_path)
    with pytest.raises(ValueError):f.verify()
