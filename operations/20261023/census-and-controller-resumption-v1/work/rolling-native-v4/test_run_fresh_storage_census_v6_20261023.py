"""Finite repeatable provider policy, unchanged proof bodies, failure progress."""
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
import ast
import copy
import importlib.util
import json
import shlex

import pytest
from horsehair.core import canonical_hash,digest

spec=importlib.util.spec_from_file_location('repeatable_census_v6',Path(__file__).with_name('run-fresh-storage-census-v6-20261023.py'))
c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)


def function_nodes(path):
    return {node.name:ast.dump(node,include_attributes=False) for node in ast.parse(path.read_text()).body
        if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef))}


def test_every_original_operational_proof_body_except_explicit_new_bindings_is_unchanged():
    base=Path(__file__).with_name(c.BASE_NAME);assert digest(base)==c.BASE_SHA
    old=function_nodes(base);new=function_nodes(Path(c.__file__))
    changed={'main','source_manifest','kernel_qualification'}
    assert len(old)>20 and all(new[name]==body for name,body in old.items() if name not in changed)


@pytest.fixture
def case_root(tmp_path,monkeypatch):
    root=tmp_path/'study';(root/'maintenance').mkdir(parents=True)
    monkeypatch.setattr(c,'ROOT',root);monkeypatch.setattr(c,'CASE_ROOT',root/'maintenance/rolling')
    return root


def test_repeatable_cases_are_fresh_actual_native_job_directories(case_root):
    a=c.create_case('rolling-01','101');b=c.create_case('rolling-01','102')
    assert a.name=='native-101' and b.name=='native-102' and a.parent==b.parent==c.CASE_ROOT
    (a/'historical.json').write_text('preserved')
    with pytest.raises(ValueError):c.create_case('rolling-01','101')
    assert (a/'historical.json').read_text()=='preserved'


def test_last_declared_slot_sixty_four_is_valid(case_root):
    assert c.create_case('rolling-64','164').name=='native-164'


@pytest.mark.parametrize('case_id,job',[('rolling-00','1'),('rolling-65','1'),('rolling-1','1'),('../rolling-01','1'),
    ('rolling-01','0'),('rolling-01','1_2'),('rolling-01','../1'),('rolling-01',True)])
def test_only_sixty_four_explicit_slots_and_positive_scalar_native_jobs_are_allowed(case_root,case_id,job):
    with pytest.raises(ValueError):c.create_case(case_id,job)
    assert not c.CASE_ROOT.exists()


@pytest.fixture
def provider(case_root,monkeypatch):
    driver=case_root/'maintenance/driver.py';driver.write_text('fixture')
    monkeypatch.setattr(c,'DRIVER',driver);monkeypatch.setattr(c,'DRIVER_SHA',digest(driver))
    directory=case_root/'plans'/c.PHASE;directory.mkdir(parents=True);(case_root/'state').mkdir()
    plan_path=directory/'plan.json';ledger_path=case_root/'state/rolling-submission.json'
    manifest=case_root/'maintenance/source-manifest.json';manifest.write_text('{}')
    qpath=case_root/'maintenance/qualification.json';qpath.write_text('{}')
    tasks=[];records={}
    for index in range(1,4):
        slot='rolling-'+str(index).zfill(2);name='rolling-storage-census-'+str(index).zfill(2)
        script=directory/(name+'.sbatch');flags={'--native-plan':str(plan_path),'--native-ledger':str(ledger_path),
            '--native-task':name,'--native-script':str(script),'--case-id':slot,
            '--source-manifest':str(manifest),'--source-manifest-sha256':digest(manifest),
            '--kernel-qualification':str(qpath),'--kernel-qualification-sha256':digest(qpath),'--kernel-qualification-job-id':'99',
            '--registry-qualification':'/old/registry.json','--registry-qualification-sha256':'a'*64,'--registry-qualification-job-id':'98'}
        tail=[part for pair in flags.items() for part in pair];command=shlex.join(['python','-s',str(Path(c.__file__)),*tail])
        script.write_text('#!/bin/sh\n'+command+'\n')
        task={'name':name,'script':str(script),'command':command,'script_sha256':canonical_hash(script.read_text()),
            'resources':{'cpus':8,'gpu':False,'hours':6,'memory':'64G','array':None},
            'rolling_census_provider':{'helper':{'path':str(Path(c.__file__)),'sha256':digest(c.__file__)},
                'source_manifest':{'path':str(manifest),'sha256':digest(manifest)},'case_id':slot},
            'depends':[] if index==1 else ['rolling-storage-census-'+str(index-1).zfill(2)],
            'external_dependencies':['99'] if index==1 else [],'dependency_kind':'afterok'}
        tasks.append(task);records[name]={'job_id':str(100+index),'state':'submitted','script_sha256':task['script_sha256']}
    plan={'phase':c.PHASE,'tasks':tasks};ledger={'tasks':records}
    plan_path.write_text(json.dumps(plan));ledger_path.write_text(json.dumps(ledger))
    args=SimpleNamespace(native_plan=plan_path,native_ledger=ledger_path,native_task=tasks[0]['name'],native_script=Path(tasks[0]['script']),
        case_id='rolling-01',source_manifest=manifest,source_manifest_sha256=digest(manifest),kernel_qualification=qpath,
        kernel_qualification_sha256=digest(qpath),kernel_qualification_job_id='99',registry_qualification='/old/registry.json',
        registry_qualification_sha256='a'*64,registry_qualification_job_id='98')
    monkeypatch.setattr(c.runpy,'run_path',lambda *a,**k:{'checked_pair':lambda p,l,*a:(json.loads(p.read_text()),json.loads(l.read_text()))})
    return args,plan,ledger


def test_whole_provider_plan_is_source_bound_finite_and_strictly_sequential(provider):
    args,plan,ledger=provider;proof=c.provider_registration(args,'101',{})
    assert proof=={'case_id':'rolling-01','native_job_id':'101','predecessor_job_id':None,'slot_count':3,'maximum_cpu_core_hours':144}
    args.native_task=plan['tasks'][1]['name'];args.native_script=Path(plan['tasks'][1]['script']);args.case_id='rolling-02'
    assert c.provider_registration(args,'102',{})['predecessor_job_id']=='101'


def test_full_sixty_four_slot_chain_has_3072_core_hour_bound_and_eight_cpu_capacity(provider):
    from horsehair.pipeline import prospective_capacity,prospective_registered_budget
    args,plan,ledger=provider;template=plan['tasks'][-1]
    for index in range(4,65):
        task=copy.deepcopy(template);name='rolling-storage-census-'+str(index).zfill(2);slot='rolling-'+str(index).zfill(2)
        script=args.native_plan.parent/(name+'.sbatch');command=shlex.split(task['command'])
        for flag,value in {'--native-task':name,'--native-script':str(script),'--case-id':slot}.items():command[command.index(flag)+1]=value
        task.update(name=name,script=str(script),command=shlex.join(command),depends=['rolling-storage-census-'+str(index-1).zfill(2)])
        task['rolling_census_provider']['case_id']=slot
        script.write_text('#!/bin/sh\n'+task['command']+'\n');task['script_sha256']=canonical_hash(script.read_text())
        ledger['tasks'][name]={'job_id':str(100+index),'state':'submitted','script_sha256':task['script_sha256']};plan['tasks'].append(task)
    args.native_plan.write_text(json.dumps(plan));args.native_ledger.write_text(json.dumps(ledger))
    assert c.provider_registration(args,'101',{})['maximum_cpu_core_hours']==3072
    assert prospective_capacity({'rolling':plan},{'rolling':ledger},{})=={'cpus':8,'gpus':0}
    cost=prospective_registered_budget({'rolling':plan},{'rolling':ledger},{},{'spent_cpu_core_hours':0,'spent_gpu_hours':0})
    assert cost['cpu_core_hours']==3072 and cost['gpu_hours']==0


@pytest.mark.parametrize('change',['phase','empty','sixty_five','name','helper_sha','manifest_sha','case_id','resource',
    'dependency','afterany','external','argv','script','unsubmitted','duplicate_job','selected_slot'])
def test_provider_rejects_changed_or_unbounded_or_parallel_schedule(provider,change):
    args,plan,ledger=provider;task=plan['tasks'][1]
    if change=='phase':plan['phase']='arbitrary'
    elif change=='empty':plan['tasks']=[]
    elif change=='sixty_five':plan['tasks']=[copy.deepcopy(task) for _ in range(65)]
    elif change=='name':task['name']='unbound'
    elif change=='helper_sha':task['rolling_census_provider']['helper']['sha256']='0'*64
    elif change=='manifest_sha':task['rolling_census_provider']['source_manifest']['sha256']='0'*64
    elif change=='case_id':task['rolling_census_provider']['case_id']='rolling-03'
    elif change=='resource':task['resources']['cpus']=64
    elif change=='dependency':task['depends']=[]
    elif change=='afterany':task['dependency_kind']='afterany'
    elif change=='external':task['external_dependencies']=['77']
    elif change=='argv':task['command']+=' --ignored true'
    elif change=='script':Path(task['script']).write_text('changed')
    elif change=='unsubmitted':ledger['tasks'][task['name']]['state']='before_sbatch'
    elif change=='duplicate_job':ledger['tasks'][task['name']]['job_id']='101'
    elif change=='selected_slot':args.case_id='rolling-02'
    args.native_plan.write_text(json.dumps(plan));args.native_ledger.write_text(json.dumps(ledger))
    with pytest.raises(ValueError):c.provider_registration(args,'101',{})


def test_source_closure_and_new_q_are_separate_from_old_qualified_bytes():
    assert len(c.PRIVATE_FILES)==14 and len(c.SUITE_SOURCES)==13
    assert c.BASE_NAME in c.PRIVATE_FILES and c.OUTER in c.PRIVATE_FILES
    assert c.WRAPPER_NAME=='run-fresh-storage-census-v6-20261023.py'
    assert c.QUALIFIER_NAME=='qualify-streaming-census-v2-20261023.py'


@pytest.mark.parametrize('error',[ValueError('measurement refused'),TimeoutError('finite deadline')])
def test_closed_partial_progress_is_preserved_without_inventory_authority(tmp_path,error):
    local=tmp_path/'local.jsonl';local.write_bytes(b'{"entries":1000,"partial_census_authority":false}\n')
    case=tmp_path/'case';case.mkdir();proof=c.preserve_closed_progress(local,case,error=error)
    assert (case/'partial-progress.jsonl').read_bytes()==local.read_bytes()
    assert proof['partial_census_authority'] is False and proof['progress_is_complete_inventory'] is False
    saved=json.loads((case/'progress-failure.json').read_text())
    assert saved['error_type']==type(error).__name__ and saved['automatic_replay_permitted'] is False
    assert saved['scientific_result'] is False and saved['admission_authority'] is False


def test_success_progress_copy_is_immutable_and_also_has_no_inventory_authority(tmp_path):
    local=tmp_path/'local.jsonl';local.write_bytes(b'prefix\n');case=tmp_path/'case';case.mkdir()
    proof=c.preserve_closed_progress(local,case)
    assert proof['path']==str(case/'sweep-progress.jsonl') and not (case/'progress-failure.json').exists()
    with pytest.raises(FileExistsError):c.preserve_closed_progress(local,case)


@pytest.mark.parametrize('failure',[None,ValueError('late measurement refusal'),TimeoutError('native deadline')])
def test_measurement_context_and_progress_stream_close_before_copy(tmp_path,monkeypatch,failure):
    local=tmp_path/'local.jsonl';case=tmp_path/'case';case.mkdir();closed=[False];events=[]
    monkeypatch.setattr(c,'progress_path',lambda job:local)
    @contextmanager
    def adapter(*a,**kw):
        events.append('adapter-open')
        try:yield {'restored':True}
        finally:closed[0]=True;events.append('adapter-closed')
    monkeypatch.setattr(c,'operational_adapter',adapter)
    def snapshot(*a,**kw):
        assert not closed[0];local.write_bytes(b'{"entries":1}\n')
        if failure:raise failure
        return {'status':'complete'}
    monkeypatch.setattr(c,'run_snapshot',snapshot)
    real=c.preserve_closed_progress
    def copy_progress(*a,**kw):
        assert closed[0];events.append('copy');return real(*a,**kw)
    monkeypatch.setattr(c,'preserve_closed_progress',copy_progress)
    if failure:
        with pytest.raises(type(failure)):c.measured_sweep(None,{},tmp_path,case,c.time.time()+3600,'101')
        assert (case/'partial-progress.jsonl').exists()
    else:
        result,_,_=c.measured_sweep(None,{},tmp_path,case,c.time.time()+3600,'101');assert result['status']=='complete'
        assert (case/'sweep-progress.jsonl').exists()
    assert events==['adapter-open','adapter-closed','copy']


@pytest.fixture
def qualified_v6(case_root,monkeypatch):
    (case_root/'state').mkdir()
    support=case_root/'maintenance/source';support.mkdir();framework=case_root/'framework';framework.mkdir()
    qpath=case_root/'maintenance/q2/qualification.json';qpath.parent.mkdir()
    monkeypatch.setattr(c,'SOURCE_SUPPORT',support);monkeypatch.setattr(c,'FRAMEWORK',framework);monkeypatch.setattr(c,'KERNEL_Q',qpath)
    base=case_root/'maintenance/base-controller.py';base.write_text('qualified synthetic base')
    monkeypatch.setattr(c,'BASE_CONTROLLER',base);monkeypatch.setattr(c,'BASE_CONTROLLER_SHA',digest(base))
    for name in c.PRIVATE_FILES:(support/name).write_text('synthetic private '+name)
    (support/c.BASE_NAME).write_bytes(Path(__file__).with_name(c.BASE_NAME).read_bytes())
    for name in [*['tests/test_'+n+'.py' for n in c.BASELINE_NAMES],
        'tests/conftest.py','src/horsehair/budget.py','src/horsehair/storage_artifacts.py']:
        path=framework/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('synthetic public '+name)
    manifest={'kind':'streaming-census-operational-source-manifest-v2',
        'files':{n:digest(support/n) for n in c.PRIVATE_FILES},'suites':{}};suites=[]
    for scope,name in c.SUITE_SOURCES.items():
        source=framework/name if scope.startswith('baseline_') else support/name;roster=canonical_hash(['test_'+scope+'::positive'])
        manifest['suites'][scope]={'source':name,'source_sha256':digest(source),'tests':1,'roster_sha256':roster}
        xml=qpath.parent/(scope+'.xml');xml.write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"><testcase classname="test_'+scope+'" name="positive"/></testsuite></testsuites>')
        suites.append({'scope':scope,'source':{'path':str(source),'sha256':digest(source)},'junit':{'path':str(xml),'sha256':digest(xml)},
            'test_counts':{'tests':1,'failures':0,'errors':0,'skipped':0},'test_roster_sha256':roster})
    manifest_path=support/'source-manifest.json';manifest_path.write_text(json.dumps(manifest));msha=digest(manifest_path)
    _,pins=c.source_manifest(manifest_path,msha)
    directory=case_root/'plans/q2';directory.mkdir(parents=True);script=directory/'q.sbatch'
    plan_path=directory/'plan.json';ledger_path=case_root/'state/q-submission.json'
    flags={'--native-plan':str(plan_path),'--native-ledger':str(ledger_path),'--native-task':'q',
        '--native-script':str(script),'--source-manifest':str(manifest_path),'--source-manifest-sha256':msha}
    command=shlex.join(['python','-s',str(support/c.QUALIFIER_NAME),*[x for pair in flags.items() for x in pair]])
    script.write_text(command);task={'name':'q','script':str(script),'command':command,'script_sha256':canonical_hash(script.read_text()),
        'resources':{'cpus':8,'memory':'64G','hours':1,'gpu':False,'array':None},
        'rolling_census_qualification':{'helper':{'path':str(support/c.QUALIFIER_NAME),'sha256':pins[str(support/c.QUALIFIER_NAME)]},
            'source_manifest':{'path':str(manifest_path),'sha256':msha}}}
    plan={'config_sha256':c.CONFIG,'release_identity':{'sha256':c.SOURCE},'tasks':[task]};plan_path.write_text(json.dumps(plan))
    ledger={'source_sha256':c.SOURCE,'plan_sha256':canonical_hash(plan),
        'tasks':{'q':{'job_id':'456','state':'submitted','script_sha256':task['script_sha256']}}};ledger_path.write_text(json.dumps(ledger))
    registration,_=c.registered_task(plan_path,ledger_path,'q',script,'456',hours=1,helper=support/c.QUALIFIER_NAME)
    identity=c.primitive_identity();canary={'status':'complete_native_local_syscall_canary','owner_uid':3164768,
        'workers':64,'pending_limit':128,**{k:True for k in ('serial_equals_streaming','symlink_not_followed','special_not_opened',
            'original_iterator_restored','original_measurement_functions_preserved','fixture_quota_simulated')},
        'fixture_quota_is_admission_authority':False,'primitive_identity':{'before':identity,'after':identity,'restored':True},
        'primitive_contract':c.primitive_contract(identity),**{k:False for k in c.FALSE_FLAGS}}
    canary_path=qpath.parent/'native-canary.json';canary_path.write_text(json.dumps(canary))
    value={'status':'complete_rolling_census_operational_qualification','returncode':0,'slurm_job_id':'456',
        'source_sha256':c.SOURCE,'config_sha256':c.CONFIG,'source_verified_unchanged':True,'source_pins':pins,
        'source_manifest':{'path':str(manifest_path),'sha256':msha},
        'base_controller':{'path':str(base),'sha256':digest(base)},
        'qualification_helper':{'path':str(support/c.QUALIFIER_NAME),'sha256':pins[str(support/c.QUALIFIER_NAME)]},
        'suites':suites,'baseline_iterator_policy':'original_canonical_iterator','private_iterator_policy':'scoped_private_kernel_only',
        'primitive_identity':c.primitive_contract(identity),'native_registration':registration,'native_completion_required_separately':True,
        'native_canary':{'path':str(canary_path),'sha256':digest(canary_path)},**{k:False for k in c.FALSE_FLAGS}}
    observed={'456':{'state':'COMPLETED','exit_code':'0:0','user':'pwa209','account':'def-ptewarie_cpu','job_id_raw':'456',
        'unallocated_metadata':False,'cpus':8,'elapsed_seconds':1,'alloc_tres':'cpu=8,mem=64G,node=1'}}
    monkeypatch.setattr(c,'completed_native_dependency',lambda *a:{'actual_positive_native':'456'})
    def verify():
        qpath.write_text(json.dumps(value));return c.kernel_qualification(qpath,digest(qpath),'456',manifest_path,msha,observed)
    return value,observed,verify


def test_v6_requires_its_own_full_new_q2_and_actual_23_source_closure(qualified_v6):
    value,observed,verify=qualified_v6;proof=verify()
    assert proof['job_id']=='456' and len(proof['source_pins'])==23 and len(value['suites'])==13


@pytest.mark.parametrize('change',['old_q1','wrong_base','missing_suite','missing_pin','native_pending','native_foreign'])
def test_old_or_resealed_partial_or_nonpositive_new_qualification_is_refused(qualified_v6,change):
    value,observed,verify=qualified_v6
    if change=='old_q1':value['status']='complete_streaming_census_operational_qualification'
    elif change=='wrong_base':value['base_controller']['sha256']='0'*64
    elif change=='missing_suite':value['suites'].pop()
    elif change=='missing_pin':value['source_pins'].pop(next(iter(value['source_pins'])))
    elif change=='native_pending':observed['456']['state']='RUNNING'
    elif change=='native_foreign':observed['456']['user']='foreign'
    with pytest.raises(ValueError):verify()
