"""Disposable exact-source transport fixtures; no native calls or study files."""
from pathlib import Path
from types import SimpleNamespace
import argparse
import ast
import copy
import gzip
import importlib.util
import json
import os
import shlex
import shutil
import subprocess
import sys
import types
from datetime import datetime,timezone

import pytest
from horsehair import agents,budget,core,repair,slurm
from horsehair.core import canonical_hash,digest,read_json

SOURCE_PATH=Path(__file__).with_name('run-f2-policy-archive-pack-v1-20261021.py')
spec=importlib.util.spec_from_file_location('private_f2_pack',SOURCE_PATH)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
FIXTURE_ROOT=Path(os.environ.get('HHW_PACK_FIXTURE_ROOT',str(Path(agents.__file__).resolve().parents[2]))).resolve()
FIXTURE_HASHES={
    'configs/study.json':'b872f121b6cef11a7726fc56b4c237c714fab41bbafa2a6090d7e2c1e2f91353',
    'tests/fixtures/evidence_archive/verified-evidence-archive-20261021.py':m.ARCHIVE_SHA,
    'tests/fixtures/evidence_archive/test_verified_evidence_archive_20261021.py.txt':'ec4131d283d3f3a792a561c6ae6324de853a4301def9d275a03018a9801f5462',
    'src/horsehair/agent_analysis_v2.py':m.VALIDATION_MODULE_SHA}
for relative,sha in FIXTURE_HASHES.items():assert digest(FIXTURE_ROOT/relative)==sha
fixture_path=FIXTURE_ROOT/'tests/fixtures/evidence_archive/test_verified_evidence_archive_20261021.py.txt'
f=types.ModuleType('synthetic_exact045d_transport_fixtures');f.__file__=str(fixture_path)
exec(compile(fixture_path.read_bytes(),str(fixture_path),'exec'),f.__dict__)
policy=f.policy


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,sort_keys=True)+'\n')
    return {'path':str(path),'sha256':digest(path)}


class Policy:
    def receipt(self):return {'path':'synthetic-approved-policy','sha256':'d'*64}


def build_case(root,source,authority,monkeypatch):
    monkeypatch.setattr(m,'ROOT',root)
    support=root/'maintenance/pack-support';support.mkdir()
    module=support/SOURCE_PATH.name;shutil.copyfile(SOURCE_PATH,module);monkeypatch.setattr(m,'__file__',str(module))
    helper_path=support/'verified-evidence-archive-20261021.py';helper_path.write_bytes((FIXTURE_ROOT/'tests/fixtures/evidence_archive'/helper_path.name).read_bytes())
    helper=m.load_archive(m.locator(helper_path));monkeypatch.setattr(helper,'_publish_file',lambda source,destination:Path(source).rename(destination))
    canary=f.canary(root);monkeypatch.setattr(m,'CANARY',canary)
    config=read_json(FIXTURE_ROOT/'configs/study.json')
    if 'native' not in authority:
        authority={**authority,'native':f.native(root,'100'),'policy_inputs':{
            'plan':m.locator(source/'plan.json'),'binding':write(source/'plan-binding.json',{}),
            'summary':write(source/'evaluation_summary.json',{})}}
    # The600-file fixture tests transport bytes; its deliberately simple rows
    # are not claimed to be a scientific campaign. Only the independently
    # qualified pure dispatcher boundary is stubbed here, never production.
    calls=[]
    monkeypatch.setattr(m.dispatch,'_state',lambda plan,state:calls.append((plan,state)) or state)
    plan={'case':str(source.parent),'config':config,'source_binding':{'primary_source_sha256':m.PRIMARY_SHA,
        'execution_source_sha256':m.SOURCE},'cells':[{'index':i,'evaluation_case':str(source.parent/('cell-'+str(i).zfill(4)))} for i in range(240)]}
    plan['plan_sha256']=canonical_hash(plan);campaign=write(source.parent/'campaign-plan.json',plan)
    summary=read_json(Path(authority['policy_inputs']['summary']['path']))
    if summary.get('condition_bindings'):
        packets={str(row['condition_index']):{'sha256':row['receipt_sha256'],'value':read_json(source/row['receipt_name'])}
            for row in summary['condition_bindings']}
    else:packets={str(i):{'sha256':str(i),'value':{'condition_index':i}} for i in range(300)}
    state={'parent':plan['case'],'plan_sha256':plan['plan_sha256'],'cells':[{'index':i,'complete':False,
        'assembly_job':None,'conditions':{},'evaluation_plan':None} for i in range(240)],
        'jobs':{'assembly0':{'job_id':'100','status':'complete'}}}
    state['cells'][0].update(complete=True,assembly_job='assembly0',conditions=packets,evaluation_plan={
        'case':str(source),'plan_sha256':authority['policy_inputs']['plan']['sha256'],
        'plan_binding_sha256':authority['policy_inputs']['binding']['sha256']})
    state['state_sha256']=canonical_hash(state);snapshot=support/'captured-state.json.gz'
    snapshot.write_bytes(gzip.compress(json.dumps(state,sort_keys=True).encode(),mtime=0))
    q=write(support/'private-qualification.json',{'synthetic':'separately tested only'});q['job_id']='800'
    request={'schema_version':1,'kind':'originals_retaining_policy_archive_pack_v1','root':str(root),
        'case':str(root/'maintenance/packs'),'archive_helper':m.locator(helper_path),'archive_canary':canary,
        'operational_qualification':q,'campaign_plan':campaign,'campaign_snapshot':m.locator(snapshot),
        'policies':[{'cell_index':0,'evaluation_case':str(source),'authority':authority}]}
    request_locator=write(support/'request.json',request)
    job='900'
    plan_path=root/'plans/pack/plan.json';script=plan_path.with_name('pack.sbatch');ledger_path=root/'state/pack-submission.json'
    args=argparse.Namespace(mode='pack',request=Path(request_locator['path']),request_sha256=request_locator['sha256'],
        native_plan=plan_path,native_ledger=ledger_path,native_script=script,native_task='pack')
    tokens=[sys.executable,str(module),'--mode','pack','--request',str(args.request),'--request-sha256',args.request_sha256,
        '--native-plan',str(plan_path),'--native-ledger',str(ledger_path),'--native-script',str(script),'--native-task','pack']
    command=shlex.join(tokens);script.parent.mkdir(parents=True);script.write_text('#!/bin/bash\n'+command+'\n')
    task={'name':'pack','command':command,'script':str(script),'script_sha256':canonical_hash(script.read_text()),
        'resources':{'cpus':4,'gpu':False,'hours':1,'memory':'16G'}}
    plan={'release_identity':{'sha256':m.SOURCE},'tasks':[task]}
    ledger={'tasks':{'pack':{'state':'submitted','job_id':job,'script_sha256':task['script_sha256']}}}
    write(plan_path,plan);write(ledger_path,ledger)
    driver=support/'pinned-driver.py';driver.write_text('# synthetic checked_pair transport boundary')
    monkeypatch.setattr(m,'DRIVER',driver);monkeypatch.setattr(m,'DRIVER_SHA',digest(driver))
    original_run=m.runpy.run_path
    def run(path,**kwargs):
        if Path(path)==driver:return {'checked_pair':lambda p,l,*args:(read_json(p),read_json(l))}
        return original_run(path,**kwargs)
    monkeypatch.setattr(m.runpy,'run_path',run)
    registration=m.registered_task(args,job,config)
    context={'case':Path(request['case']),'job_id':job,'end':10**20,'config':config,'policy':Policy(),
        'request':request_locator,'registration':registration}
    return root,source,authority,helper,request,context,args


@pytest.fixture
def case(tmp_path,monkeypatch):
    root=tmp_path/'study';source=root/'maintenance/campaign/cell-0000';source.mkdir(parents=True)
    write(source/'plan.json',{'synthetic':'metadata fixture only'})
    authority={'primary_source_sha256':m.PRIMARY_SHA,'full_config_sha256':m.CONFIG,'execution_source_sha256':m.SOURCE}
    return build_case(root,source,authority,monkeypatch)


@pytest.fixture
def full_case(policy,monkeypatch):return build_case(*policy,monkeypatch)


def test_request_and_full600_roster_forecast_have_no_future_savings(full_case):
    root,source,authority,helper,request,context,args=full_case
    assert m.validate_request(request,'pack')==Path(request['case'])
    forecast=m.plan_packs(helper,request)
    assert len(forecast['policies'][0]['plan']['members'])==600
    assert forecast['additional_peak_files']==2612 and forecast['additional_peak_bytes']>10_000_000_000
    assert forecast['future_storage_savings_credited']==0 and forecast['admission_authority'] is False


@pytest.mark.parametrize('change',['bool_index','outside_index','wrong_case','foreign_config','foreign_source','duplicate','ninth',
    'wrong_canary','wrong_helper','existing_output','unknown_field'])
def test_no_partial_foreign_or_unbounded_policy_request(case,change):
    root,source,authority,helper,request,context,args=case;request=copy.deepcopy(request)
    if change=='bool_index':request['policies'][0]['cell_index']=False
    if change=='outside_index':request['policies'][0]['cell_index']=240
    if change=='wrong_case':request['policies'][0]['cell_index']=1
    if change=='foreign_config':request['policies'][0]['authority']['full_config_sha256']='0'*64
    if change=='foreign_source':request['policies'][0]['authority']['execution_source_sha256']='0'*64
    if change=='duplicate':request['policies']*=2
    if change=='ninth':request['policies']*=9
    if change=='wrong_canary':request['archive_canary']['sha256']='0'*64
    if change=='wrong_helper':request['archive_helper']['sha256']='0'*64
    if change=='existing_output':Path(request['case']).mkdir()
    if change=='unknown_field':request['remove_originals']=True
    with pytest.raises(ValueError):m.validate_request(request,'pack')


@pytest.mark.parametrize('change',['mode','request','request_sha256','native_task','native_script','job','source','resources','script_bytes'])
def test_registered_native_command_binds_every_actual_argument(case,change):
    root,source,authority,helper,request,context,args=case;args=copy.copy(args)
    if change=='mode':args.mode='finalize'
    if change=='request':args.request=args.request.with_name('other.json')
    if change=='request_sha256':args.request_sha256='0'*64
    if change=='native_task':args.native_task='other'
    if change=='native_script':args.native_script.write_text('changed')
    if change=='job':
        ledger=read_json(args.native_ledger);ledger['tasks']['pack']['job_id']='901';write(args.native_ledger,ledger)
    if change in {'source','resources'}:
        plan=read_json(args.native_plan)
        if change=='source':plan['release_identity']['sha256']='0'*64
        else:plan['tasks'][0]['resources']['cpus']=8
        write(args.native_plan,plan)
    if change=='script_bytes':args.native_script.write_text(args.native_script.read_text()+'# changed\n')
    with pytest.raises(ValueError):m.registered_task(args,'900',context['config'])


def native_join_fixture(root,monkeypatch):
    import horsehair
    package=Path(agents.__file__).resolve().parents[2];alternate=root/'releases/validation';(alternate/'src/horsehair').mkdir(parents=True)
    for name in ('core','slurm','repair'):
        shutil.copyfile(package/('src/horsehair/'+name+'.py'),alternate/('src/horsehair/'+name+'.py'))
    shutil.copyfile(FIXTURE_ROOT/'src/horsehair/agent_analysis_v2.py',alternate/'src/horsehair/agent_analysis_v2.py')
    monkeypatch.setattr(m,'FRAMEWORK',package);monkeypatch.setattr(m,'VALIDATION_FRAMEWORK',alternate)
    monkeypatch.setattr(m,'source_identity',lambda path:{'sha256':m.SOURCE if Path(path)==package else m.VALIDATION_SOURCE})
    ledger_path=root/'state/resource-budget.json';ledger=read_json(ledger_path) if ledger_path.exists() else {'native_jobs':{}}
    ledger.setdefault('leases',{});ledger.setdefault('spent_cpu_core_hours',0.);ledger.setdefault('spent_gpu_hours',0.);write(ledger_path,ledger)
    job='900';stdout=job+'|'+job+'|COMPLETED|12|4|billing=4,cpu=4,mem=16G,node=1|0:0|1024K\n'
    budget.native_accounting(root,[job],runner=lambda command,**kwargs:subprocess.CompletedProcess(command,0,stdout,''))
    original=m.subprocess.run
    def transport(command,**kwargs):
        assert command==['sacct','--array','-j',job,'--format='+repair.REPAIR_NATIVE_FORMAT,'--units=K','--noheader','--parsable2']
        assert 0<kwargs['timeout']<=60
        return subprocess.CompletedProcess(command,0,job+'|'+job+'|COMPLETED|0:0|12|4|cpu=4,mem=16G,node=1|nc10000|'
            '2026-10-21T10:00:00|2026-10-21T10:00:12|pwa209|def-ptewarie_cpu|1024K\n','')
    monkeypatch.setattr(m.subprocess,'run',transport)
    join,binding=m.public_native_join();return join,binding,alternate


def test_exact_public_function_ast_executes_with_same_package_dependencies(case,monkeypatch):
    root,*_=case;join,binding,alternate=native_join_fixture(root,monkeypatch)
    value=join('900');cost=value['completion']['native_records']['900']
    assert not {'user','account','unallocated_metadata','alloc_tres'} & set(cost)
    assert value['native']['user']=='pwa209' and value['native']['account']=='def-ptewarie_cpu'
    assert binding['mixed_package_import'] is False
    source=ast.parse((FIXTURE_ROOT/'src/horsehair/agent_analysis_v2.py').read_bytes())
    definitions=[n for n in source.body if isinstance(n,ast.FunctionDef) and n.name in {'_require','_owned_completed_cpu_qualification'}]
    assert binding['function_ast_sha256']==canonical_hash([ast.dump(n,include_attributes=False) for n in definitions])


@pytest.mark.parametrize('changed',['core','slurm','repair','public_module'])
def test_qualified_function_cannot_run_with_mixed_or_changed_dependency_bytes(case,monkeypatch,changed):
    root,*_=case;join,binding,alternate=native_join_fixture(root,monkeypatch)
    path=alternate/'src/horsehair'/('agent_analysis_v2.py' if changed=='public_module' else changed+'.py')
    path.write_bytes(path.read_bytes()+b'\n# changed\n')
    with pytest.raises(ValueError):m.public_native_join()
    if changed=='public_module':
        with pytest.raises(ValueError):join('900')


def test_actual600_member_pack_and_positive_producer_finalizer_retain_every_original(full_case,monkeypatch):
    root,source,authority,helper,request,context,args=full_case
    forecast=m.plan_packs(helper,request);Path(request['case']).mkdir()
    before={row['path']:helper.file_hash(source/row['path'])[0] for row in forecast['policies'][0]['plan']['members']}
    rows=m.execute_packs(helper,request,forecast,context)
    assert rows[0]['native_producer_completion']=='pending' and rows[0]['original_regular_files_removed']==0
    assert {name:helper.file_hash(source/name)[0] for name in before}==before
    result=read_json(Path(rows[0]['result']['path']));assert result['execution_source_sha256']==m.SOURCE
    join,binding,alternate=native_join_fixture(root,monkeypatch)
    final_case=root/'maintenance/finalized';final_case.mkdir()
    final_request={k:copy.deepcopy(request[k]) for k in ('schema_version','root','archive_helper','archive_canary','operational_qualification',
        'campaign_plan','campaign_snapshot')}
    final_request.update(kind='originals_retaining_policy_archive_finalize_v1',case=str(final_case),
        pack_outputs=[{'cell_index':0,'result':rows[0]['result']}],previous_registry=None)
    final_context={**context,'case':final_case,'job_id':'901'}
    finalized=m.finalized_registry(helper,final_request,final_context,join)
    registry=read_json(Path(finalized['registry']['path']));descriptor=registry['cells']['0']
    assert descriptor['pack_producer']['result']==rows[0]['result']
    assert helper.verify_native(descriptor['pack_producer']['native'],root,completed=True)[0]=='900'
    assert len(helper.committed_conditions(descriptor['archive_directory'],descriptor['index_sha256'],descriptor['archive_sha256'],root=root))==300
    assert {name:helper.file_hash(source/name)[0] for name in before}==before
    assert finalized['controller_descriptor_published'] is False and finalized['future_storage_savings_credited']==0
    assert finalized['original_regular_files_removed']==0 and finalized['consumer_full_revalidation_required'] is True


def test_source_and_operation_have_no_unlink_or_scheduler_mutation_api():
    tree=ast.parse(SOURCE_PATH.read_bytes());calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call)]
    forbidden={'unlink','remove_verified_originals','rmtree','rename','submit','release','cancel'}
    assert not any(isinstance(n.func,ast.Attribute) and n.func.attr in forbidden for n in calls)
    assert not any(isinstance(n.func,ast.Attribute) and n.func.attr=='replace' and isinstance(n.func.value,ast.Name)
        and n.func.value.id=='os' for n in calls)
    assert not any(isinstance(n,ast.Constant) and type(n.value) is str and n.value in {'sbatch','scancel','scontrol'} for n in ast.walk(tree))


@pytest.mark.parametrize('value',['NaN','Infinity','-Infinity','1e999','-1e999'])
def test_nonfinite_metadata_including_exponent_overflow_is_refused(value):
    with pytest.raises(ValueError):m.strict('{"x":'+value+'}')


def test_duplicate_metadata_keys_are_refused_without_changing_finite_literals():
    with pytest.raises(ValueError):m.strict('{"x":1,"x":2}')
    assert m.strict('{"x":1e2,"literal":"1e999"}')=={'x':100.,'literal':'1e999'}


def replace_snapshot(request,change):
    path=Path(request['campaign_snapshot']['path']);state=json.loads(gzip.decompress(path.read_bytes()))
    change(state);path.write_bytes(gzip.compress(json.dumps(state,sort_keys=True).encode(),mtime=0))
    request['campaign_snapshot']=m.locator(path)


@pytest.mark.parametrize('change',['incomplete','missing_condition','extra_condition','assembly_absent','assembly_active',
    'assembly_other_job','activation_absent','other_plan','other_binding'])
def test_only_complete_original300_packet_assembly_is_eligible(case,change):
    root,source,authority,helper,request,context,args=case
    def modify(state):
        cell=state['cells'][0]
        if change=='incomplete':cell['complete']=False
        elif change=='missing_condition':cell['conditions'].pop('299')
        elif change=='extra_condition':cell['conditions']['300']={}
        elif change=='assembly_absent':cell['assembly_job']=None
        elif change=='assembly_active':state['jobs']['assembly0']['status']='submitted'
        elif change=='assembly_other_job':state['jobs']['assembly0']['job_id']='101'
        elif change=='activation_absent':cell['evaluation_plan']={'case':'other'}
        elif change=='other_plan':cell['evaluation_plan']['plan_sha256']='0'*64
        elif change=='other_binding':cell['evaluation_plan']['plan_binding_sha256']='0'*64
    replace_snapshot(request,modify)
    with pytest.raises(ValueError):m.validate_request(request,'pack')
    assert not Path(request['case']).exists()


@pytest.mark.parametrize('change',['canonical_live_state','foreign_plan_path','foreign_config','foreign_source','partial_roster'])
def test_original_full_campaign_and_immutable_separate_state_are_required(case,change):
    root,source,authority,helper,request,context,args=case
    if change=='canonical_live_state':
        path=source.parent/'state.json.gz';shutil.copyfile(request['campaign_snapshot']['path'],path)
        request['campaign_snapshot']=m.locator(path)
    else:
        path=Path(request['campaign_plan']['path']);value=read_json(path)
        if change=='foreign_plan_path':
            request['campaign_plan']=write(path.with_name('copied-plan.json'),value)
        else:
            if change=='foreign_config':value['config']['study_title']='foreign'
            elif change=='foreign_source':value['source_binding']['execution_source_sha256']='0'*64
            elif change=='partial_roster':value['cells'].pop()
            request['campaign_plan']=write(path,value)
    with pytest.raises(ValueError):m.campaign_inputs(request)


def native_response(**changes):
    row={'JobId':'900','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'NumCPUs':'4','NumNodes':'1-1','MinMemoryNode':'16G','TimeLimit':'01:00:00',
        'AllocTRES':'cpu=4,mem=16G,node=1','ReqTRES':'cpu=4,mem=16G,node=1','EndTime':'2026-10-21T12:00:00'}
    row.update(changes);return ' '.join(k+'='+v for k,v in row.items())


@pytest.mark.parametrize('change',['valid','owner','account','cpus','gpu','empty_tres','duplicate_tres','wrong_tres_cpu',
    'state','memory','nodes','hours','end_environment'])
def test_actual_native_allocation_is_positive_exact_and_finite(case,monkeypatch,change):
    root,*_=case;end=datetime(2026,10,21,12,tzinfo=timezone.utc).timestamp()
    monkeypatch.setattr(m.time,'time',lambda:end-1800);monkeypatch.setenv('SLURM_JOB_END_TIME',str(end))
    changes={}
    if change=='owner':changes['UserId']='other(1)'
    if change=='account':changes['Account']='def-ptewarie_gpu'
    if change=='cpus':changes['NumCPUs']='8'
    if change=='gpu':changes['AllocTRES']='cpu=4,gres/gpu=1'
    if change=='empty_tres':changes['AllocTRES']=''
    if change=='duplicate_tres':changes['AllocTRES']='cpu=4,cpu=4'
    if change=='wrong_tres_cpu':changes['ReqTRES']='cpu=1'
    if change=='state':changes['JobState']='PENDING'
    if change=='memory':changes['MinMemoryNode']='8G'
    if change=='nodes':changes['NumNodes']='1-2'
    if change=='hours':changes['TimeLimit']='02:00:00'
    if change=='end_environment':monkeypatch.setenv('SLURM_JOB_END_TIME',str(end+60))
    monkeypatch.setattr(m.runpy,'run_path',lambda path:{'run_scontrol_job':lambda job,evidence:
        subprocess.CompletedProcess(['read-only'],0,native_response(**changes),'')})
    if change=='valid':assert m.native_owner('900',root/'maintenance')[1]==end
    else:
        with pytest.raises(ValueError):m.native_owner('900',root/'maintenance')


def source_qualification(case,count=1889):
    root,*_=case;path=root/'state/qualification-evidence/fixture/junit.xml';path.parent.mkdir(parents=True)
    path.write_text('<testsuites><testsuite tests="'+str(count)+'" failures="0" errors="0" skipped="0">'+
        ''.join('<testcase classname="full_source" name="test_'+str(i)+'"/>' for i in range(count))+'</testsuite></testsuites>')
    q={'status':'complete','returncode':0,'source_sha256':m.VALIDATION_SOURCE,'config_sha256':m.CONFIG,
        'source_verified_unchanged':True,'slurm_job_id':'900','test_counts':{'tests':count,'failures':0,'errors':0,'skipped':0},
        'artifacts':{'junit':{**m.locator(path),'bytes':path.stat().st_size}}}
    return q,root/'maintenance/q.json'


@pytest.mark.parametrize('change',['valid','partial','boolean_return','wrong_source','wrong_job','changed_junit'])
def test_complete_source_qualification_requires_actual_full_bound_junit(case,change):
    root,*_=case;q,path=source_qualification(case)
    if change=='partial':q['test_counts']['tests']=1
    if change=='boolean_return':q['returncode']=False
    if change=='wrong_source':q['source_sha256']='0'*64
    if change=='wrong_job':q['slurm_job_id']='901'
    binding={**write(path,q),'job_id':'900'}
    if change=='changed_junit':Path(q['artifacts']['junit']['path']).write_text('<testsuite tests="1"/>')
    calls=[]
    def join(job,**kwargs):calls.append(job);return {'positive':True}
    if change=='valid':
        assert m.qualification(binding,m.VALIDATION_SOURCE,1889,join,end=10**20)=={'positive':True};assert calls==['900']
    else:
        with pytest.raises(ValueError):m.qualification(binding,m.VALIDATION_SOURCE,1889,join,end=10**20)
        assert not calls


@pytest.mark.parametrize('change',['valid','foreign_wrapper','partial_roster','skip','wrong_test_source','unsealed_tests',
    'wrong_producer','wrong_registration_job','wrong_command'])
def test_private_operational_qualification_requires_exact_actual_test_source_and_roster(case,monkeypatch,change):
    root,source,authority,helper,request,context,args=case
    monkeypatch.setattr(m,'PRIMARY',FIXTURE_ROOT)
    name='test_synthetic_pack_contract.py';test=write(root/'maintenance'/name,{'synthetic':'source bytes only'})
    path=root/'maintenance/private-junit.xml'
    path.write_text('<testsuite tests="1" failures="0" errors="0" skipped="0"><testcase classname="'+name[:-3]+
        '" name="test_contract"/></testsuite>')
    expected={'sha256':test['sha256'],'tests':1,'roster_sha256':canonical_hash([(name[:-3],'test_contract')])}
    monkeypatch.setattr(m,'PRIVATE_TESTS',{name:expected})
    counts={'tests':1,'failures':0,'errors':0,'skipped':0}
    producer=write(root/'maintenance/qualify-pack.py',{'synthetic':'source contract only'})
    registration=copy.deepcopy(context['registration'])
    tokens=[sys.executable,producer['path'],'--pack-driver',str(m.__file__),'--pack-driver-sha256',digest(Path(m.__file__)),
        '--test-source',test['path'],'--qualification-case',str(root/'maintenance'),
        '--native-plan',str(args.native_plan),'--native-ledger',str(args.native_ledger),
        '--native-script',str(args.native_script),'--native-task',args.native_task]
    plan=read_json(args.native_plan);ledger=read_json(args.native_ledger);command=shlex.join(tokens)
    args.native_script.write_text('#!/bin/bash\n'+command+'\n')
    plan['tasks'][0].update(command=command,script_sha256=canonical_hash(args.native_script.read_text()))
    ledger['tasks'][args.native_task]['script_sha256']=plan['tasks'][0]['script_sha256']
    registration.update(plan=write(args.native_plan,plan),ledger=write(args.native_ledger,ledger),script=m.locator(args.native_script))
    q={'status':'complete_f2_policy_pack_operational_qualification_v1','returncode':0,'source_sha256':m.SOURCE,
        'config_sha256':m.CONFIG,'slurm_job_id':'900','wrapper_sha256':digest(Path(m.__file__)),
        'archive_helper_sha256':m.ARCHIVE_SHA,'validation_source_sha256':m.VALIDATION_SOURCE,'validation_qualification':m.VALIDATION_Q,
        'scientific_result':False,'admission_authority':False,'suites':{name:{'tests':counts,'test_source':test,'junit':m.locator(path)}},
        'qualification_driver':producer,'native_registration':registration,'pack_driver':m.locator(Path(m.__file__)),
        'fixture_root':str(m.VALIDATION_FRAMEWORK),'framework_qualification':m.FRAMEWORK_Q}
    if change=='foreign_wrapper':q['wrapper_sha256']='0'*64
    if change=='partial_roster':expected['tests']=2
    if change=='skip':path.write_text('<testsuite tests="1" skipped="1"><testcase name="test_contract"><skipped/></testcase></testsuite>')
    if change=='wrong_test_source':q['suites'][name]['test_source']={**test,'sha256':'0'*64}
    if change=='unsealed_tests':monkeypatch.setattr(m,'PRIVATE_TESTS',{})
    if change=='wrong_producer':q['qualification_driver']={**producer,'sha256':'0'*64}
    if change=='wrong_registration_job':q['native_registration']['native_job_id']='901'
    if change=='wrong_command':
        plan['tasks'][0]['command']=plan['tasks'][0]['command'].replace('--pack-driver-sha256','--other-sha256')
        q['native_registration']['plan']=write(args.native_plan,plan)
    binding={**write(root/'maintenance/operation-q.json',q),'job_id':'900','producer':producer};calls=[]
    def join(job,**kwargs):calls.append(job);return 'positive'
    if change=='valid':assert m.private_qualification(binding,join,end=10**20)=='positive' and calls==['900']
    else:
        with pytest.raises(ValueError):m.private_qualification(binding,join,end=10**20)
        assert not calls


@pytest.mark.parametrize('change',['canonical_array_null','reject_array','reject_boolean_hours'])
def test_registered_scalar_resource_schema_accepts_only_exact_canonical_null_array(case,change):
    root,source,authority,helper,request,context,args=case;plan=read_json(args.native_plan)
    resources=plan['tasks'][0]['resources']
    if change=='canonical_array_null':resources['array']=None
    if change=='reject_array':resources['array']='0-3'
    if change=='reject_boolean_hours':resources['hours']=True
    write(args.native_plan,plan)
    if change=='canonical_array_null':assert m.registered_task(args,'900',context['config'])['native_job_id']=='900'
    else:
        with pytest.raises(ValueError):m.registered_task(args,'900',context['config'])


@pytest.mark.parametrize('change',['valid','foreign_command','foreign_jobname'])
def test_native_scheduler_script_and_name_match_exact_registered_task(case,change):
    root,source,authority,helper,request,context,args=case
    allocation={'Command':str(args.native_script),'JobName':'hhw-'+args.native_task}
    if change=='foreign_command':allocation['Command']=str(args.native_script.with_name('other.sbatch'))
    if change=='foreign_jobname':allocation['JobName']='other'
    if change=='valid':m.native_registration_owner(allocation,context['registration'])
    else:
        with pytest.raises(ValueError):m.native_registration_owner(allocation,context['registration'])


@pytest.mark.parametrize('change',['valid','builder_python_u','reordered','prefix','unknown_argument','duplicate_argument',
    'other_interpreter','wrong_helper'])
def test_registered_command_is_exact_python_helper_and_known_pairs(change):
    helper='/scratch/pwa209/horsehair-worm/maintenance/source/driver.py';expected={'--one':'1','--two':'2'}
    tokens=[sys.executable,helper,'--one','1','--two','2']
    if change=='builder_python_u':tokens=['python','-u',*tokens[1:]]
    if change=='reordered':tokens=[sys.executable,helper,'--two','2','--one','1']
    if change=='prefix':tokens=['unrelated','--path',*tokens]
    if change=='unknown_argument':tokens+=['--unknown','value']
    if change=='duplicate_argument':tokens+=['--one','1']
    if change=='other_interpreter':tokens[0]='unapproved-python'
    if change=='wrong_helper':tokens[1]=helper+'.other'
    assert m.exact_command(shlex.join(tokens),helper,expected)==(change in {'valid','builder_python_u','reordered'})


@pytest.mark.parametrize('change',['valid','stale_measured','stale_captured','future','boolean','naive'])
def test_storage_census_uses_both_finite_aware_six_hour_clocks(monkeypatch,change):
    now=datetime(2026,10,21,12,tzinfo=timezone.utc).timestamp();monkeypatch.setattr(m.time,'time',lambda:now)
    record={'measured_unix':now-60,'captured_utc':datetime.fromtimestamp(now-60,timezone.utc).isoformat()}
    if change=='stale_measured':record['measured_unix']=now-21600
    if change=='stale_captured':record['captured_utc']=datetime.fromtimestamp(now-21600,timezone.utc).isoformat()
    if change=='future':record['measured_unix']=now+1
    if change=='boolean':record['measured_unix']=True
    if change=='naive':record['captured_utc']='2026-10-21T11:59:00'
    if change=='valid':m.fresh_census(record)
    else:
        with pytest.raises(ValueError):m.fresh_census(record)


@pytest.mark.parametrize('change',['valid','quota_error','census_changed'])
def test_peak_storage_uses_fresh_quota_and_full_study_reservation_without_discount(case,monkeypatch,change):
    from horsehair import admission
    root,source,authority,helper,request,context,args=case;context['case'].mkdir()
    now=datetime(2026,10,21,12,tzinfo=timezone.utc).timestamp();monkeypatch.setattr(m.time,'time',lambda:now)
    census=root/'state/storage-budget.json';write(census,{'measured_unix':now,'captured_utc':datetime.fromtimestamp(now,timezone.utc).isoformat()})
    calls=[]
    def quota(study,**kwargs):
        calls.append(kwargs)
        if change=='quota_error':raise OSError('ENOSPC remains a real quota failure')
        assert study==root and kwargs['required_files']==2612 and kwargs['required_bytes']==11_000_000_000
        assert kwargs['reserve_files']==247388 and kwargs['reserve_bytes']==2_000_000_000_000
        return {'synthetic_fresh_quota':True}
    monkeypatch.setattr(m.runpy,'run_path',lambda path:{'require_storage_headroom_with_wait':quota})
    def storage(study,config,bytes_,files,**kwargs):
        assert study==root and (bytes_,files)==(11_000_000_000,2612)
        if change=='census_changed':write(census,{'changed':'must stop'})
        return 1_100_000_000_000+bytes_,95000+files,[census]
    monkeypatch.setattr(admission,'_storage',storage)
    if change=='valid':
        proof=m.storage_gate(context,2612,11_000_000_000,1)
        assert proof['future_storage_savings_credited']==0 and proof['future_study_files']==97612 and len(calls)==1
    else:
        with pytest.raises((ValueError,OSError)):m.storage_gate(context,2612,11_000_000_000,1)
        assert not (context['case']/'storage-proof-1.json').exists()


@pytest.mark.parametrize('change',['same_active_job','failed_native_producer','wrong_current_driver'])
def test_finalizer_refuses_uncompleted_or_unbound_producer_before_registry_publication(case,change):
    root,source,authority,helper,request,context,args=case
    final_case=root/'maintenance/finalized';final_case.mkdir()
    final_request={k:copy.deepcopy(request[k]) for k in ('schema_version','root','archive_helper','archive_canary',
        'operational_qualification','campaign_plan','campaign_snapshot')}
    result={'schema_version':1,'status':'complete_verified_policy_archive_pack','native_job_id':'900',
        'private_pack_driver_sha256':'0'*64}
    binding=write(root/'maintenance/bad-result.json',result)
    final_request.update(kind='originals_retaining_policy_archive_finalize_v1',case=str(final_case),
        pack_outputs=[{'cell_index':0,'result':binding}],previous_registry=None)
    final_context={**context,'case':final_case,'job_id':'900' if change=='same_active_job' else '901'}
    def join(job,**kwargs):
        if change=='failed_native_producer':raise ValueError('Real strict native producer is FAILED')
        return {'completion':{'native_records':{'900':{'state':'COMPLETED','exit_code':'0:0','cpus':4,'gpus':0,'elapsed_seconds':12}}},
            'native':{'user':'pwa209','account':'def-ptewarie_cpu'}}
    with pytest.raises(ValueError):m.finalized_registry(helper,final_request,final_context,join)
    assert not (final_case/'condition-archive-registry.json').exists()


def test_minimal_declared_fixture_stage_has_no_private_sibling_dependency(tmp_path):
    stage=tmp_path/'s';stage.mkdir();fixtures=tmp_path/'q';fixtures.mkdir()
    for relative,expected in FIXTURE_HASHES.items():
        path=fixtures/relative;path.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(FIXTURE_ROOT/relative,path)
        assert digest(path)==expected
    for source in (Path(__file__),SOURCE_PATH):shutil.copyfile(source,stage/source.name)
    environment={**os.environ,'HHW_PACK_FIXTURE_ROOT':str(fixtures),'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1'}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):environment.pop(key,None)
    command=[sys.executable,'-s','-m','pytest',str(stage/Path(__file__).name),'-q','-p','no:cacheprovider',
        '--rootdir',str(stage),'--deselect',Path(__file__).name+'::test_minimal_declared_fixture_stage_has_no_private_sibling_dependency',
        '--basetemp',str(tmp_path/'f')]
    result=subprocess.run(command,cwd=stage,text=True,capture_output=True,timeout=420,env=environment)
    assert result.returncode==0,result.stdout+result.stderr
    assert '102 passed' in result.stdout
