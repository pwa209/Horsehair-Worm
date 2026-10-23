"""Disposable producer contract tests; no cluster, scheduler, or study writes."""
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
import ast
import copy
import importlib.util
import json
import subprocess
import time

import pytest
from horsehair.core import canonical_hash,digest,read_json

SOURCE=Path(__file__).with_name('qualify-f2-archive-consumer-v3-20261023.py')
spec=importlib.util.spec_from_file_location('private_archive_v3_qualifier',SOURCE)
q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)


def loc(path,**kwargs):return {'path':str(path),'sha256':digest(Path(path))}


def test_five_file_source_closure_and_exact_136_contract_metadata_are_frozen():
    assert set(q.PINS)=={q.OVERLAY,q.OVERLAY_TEST,q.VERIFIER,q.VERIFIER_TEST,q.BASE_PATH.name}
    for name,expected in q.PINS.items():assert digest(SOURCE.with_name(name))==expected
    module=ast.parse(SOURCE.with_name(q.OVERLAY).read_bytes())
    suites=ast.literal_eval(next(n.value for n in module.body if isinstance(n,ast.Assign)
        and any(isinstance(t,ast.Name) and t.id=='TEST_SUITES' for t in n.targets)))
    assert suites==q.SUITES and sum(v['tests'] for v in suites.values())==136
    assert [suites[n]['tests'] for n in (q.OVERLAY_TEST,q.VERIFIER_TEST)]==[111,25]


@pytest.fixture
def loader(tmp_path,monkeypatch):
    root=tmp_path/'root';stage=root/'maintenance/stage';stage.mkdir(parents=True)
    base=root/'maintenance/canonical'/q.BASE_PATH.name;base.parent.mkdir();base.write_text('# frozen base fixture')
    monkeypatch.setattr(q,'ROOT',root);monkeypatch.setattr(q,'SUPPORT',stage);monkeypatch.setattr(q,'CASE',root/'maintenance/result')
    monkeypatch.setattr(q,'BASE_PATH',base)
    for name in (q.VERIFIER,q.OVERLAY_TEST,q.VERIFIER_TEST): (stage/name).write_text('# disposable '+name)
    (stage/base.name).write_bytes(base.read_bytes())
    suites={name:{'sha256':digest(stage/name),'tests':1,'roster_sha256':'a'*64} for name in (q.OVERLAY_TEST,q.VERIFIER_TEST)}
    monkeypatch.setattr(q,'SUITES',suites)
    raw=('from pathlib import Path\nROOT=Path('+repr(str(root))+')\nFRAMEWORK=Path('+repr(str(q.FRAMEWORK))+')\n'
        +'SOURCE='+repr(q.SOURCE)+'\nCONFIG='+repr(q.CONFIG)+'\nBASE_PATH=Path('+repr(str(base))+')\n'
        +'BASE_SHA='+repr(digest(base))+'\nVERIFIER_SHA='+repr(digest(stage/q.VERIFIER))+'\nTEST_SUITES='+repr(suites)+'\n')
    overlay=stage/q.OVERLAY;overlay.write_text(raw)
    pins={name:digest(stage/name) for name in (q.OVERLAY,q.VERIFIER,q.OVERLAY_TEST,q.VERIFIER_TEST,base.name)}
    monkeypatch.setattr(q,'PINS',pins)
    args=SimpleNamespace(overlay=overlay,overlay_sha256=pins[q.OVERLAY],verifier=stage/q.VERIFIER,
        verifier_sha256=pins[q.VERIFIER],base_controller=base,base_controller_sha256=pins[base.name],
        overlay_tests=stage/q.OVERLAY_TEST,verifier_tests=stage/q.VERIFIER_TEST,qualification_case=q.CASE)
    return args,stage,base


def test_loader_compiles_only_the_verified_complete_stage(loader):
    args,stage,base=loader
    assert q.load_overlay(args).BASE_PATH==base


@pytest.mark.parametrize('change',['overlay_sha','verifier_sha','base_sha','foreign_overlay','foreign_case',
    'changed_test','changed_canonical_base','changed_sibling_base','runtime_contract'])
def test_loader_refuses_wrong_or_changed_inputs_before_qualification(loader,monkeypatch,change):
    args,stage,base=loader
    if change=='overlay_sha':args.overlay_sha256='0'*64
    if change=='verifier_sha':args.verifier_sha256='0'*64
    if change=='base_sha':args.base_controller_sha256='0'*64
    if change=='foreign_overlay':args.overlay=stage.parent/args.overlay.name
    if change=='foreign_case':args.qualification_case=stage/'unapproved'
    if change=='changed_test':args.overlay_tests.write_text('# changed')
    if change=='changed_canonical_base':base.write_text('# changed')
    if change=='changed_sibling_base':(stage/base.name).write_text('# changed')
    if change=='runtime_contract':
        args.overlay.write_text(args.overlay.read_text().replace(q.SOURCE,'b'*64))
        q.PINS[q.OVERLAY]=digest(args.overlay);args.overlay_sha256=q.PINS[q.OVERLAY]
    with pytest.raises(ValueError):q.load_overlay(args)


@pytest.fixture
def stage_intent(loader,monkeypatch):
    args,stage,base=loader;root=q.ROOT;ident='406'
    args.native_plan=root/'plans/recovery-support-20261021-406/plan.json'
    args.native_ledger=root/'state/recovery-support-20261021-406-submission.json'
    args.native_task='recovery-406';args.native_script=args.native_plan.parent/'recovery-406.sbatch'
    helper=stage/'qualify-f2-archive-consumer-v3-20261023.py';helper.write_text('# exact initial helper bytes')
    monkeypatch.setattr(q,'__file__',str(helper));producer=loc(helper)
    mapping={'--overlay':str(args.overlay),'--overlay-sha256':args.overlay_sha256,
        '--verifier':str(args.verifier),'--verifier-sha256':args.verifier_sha256,
        '--base-controller':str(args.base_controller),'--base-controller-sha256':args.base_controller_sha256,
        '--overlay-tests':str(args.overlay_tests),'--verifier-tests':str(args.verifier_tests),
        '--qualification-case':str(args.qualification_case),'--native-plan':str(args.native_plan),
        '--native-ledger':str(args.native_ledger),'--native-script':str(args.native_script),'--native-task':args.native_task}
    value={'id':ident,'helper':helper.name,'helper_sha256':producer['sha256'],'area':str(stage),
        'automatic_replay_permitted':False,'plan_path':str(args.native_plan),'ledger_path':str(args.native_ledger),
        'args':[v for pair in mapping.items() for v in pair]}
    path=stage/'submission-intent.json';path.write_text(json.dumps(value))
    module=SimpleNamespace(locator=loc,strict=json.loads,read_bound=lambda binding:Path(binding['path']).read_bytes())
    return args,helper,producer,path,value,module


def test_published_intent_binds_initial_self_source_and_all_native_arguments(stage_intent):
    args,helper,producer,path,value,module=stage_intent
    assert q.registered_source_intent(module,args,producer)==loc(path)


@pytest.mark.parametrize('change',['self_bytes','helper_sha','helper_name','area','id','plan','ledger','argument',
    'duplicate_argument','extra_argument','replay'])
def test_changed_source_or_stage_intent_never_admits_native_tests(stage_intent,change):
    args,helper,producer,path,value,module=stage_intent
    if change=='self_bytes':helper.write_text('# changed before startup');producer=loc(helper)
    if change=='helper_sha':value['helper_sha256']='0'*64
    if change=='helper_name':value['helper']='foreign.py'
    if change=='area':value['area']=str(path.parent/'foreign')
    if change=='id':value['id']='407'
    if change=='plan':value['plan_path']=str(args.native_plan.parent/'other.json')
    if change=='ledger':value['ledger_path']=str(args.native_ledger.parent/'other.json')
    if change=='argument':value['args'][1]='foreign-overlay.py'
    if change=='duplicate_argument':value['args'][2]=value['args'][0]
    if change=='extra_argument':value['args']+=['--unknown','extra']
    if change=='replay':value['automatic_replay_permitted']=True
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):q.registered_source_intent(module,args,producer)


@pytest.fixture
def junit(tmp_path,monkeypatch):
    test=tmp_path/'test_disposable.py';test.write_text('# fixture source')
    path=tmp_path/'actual.xml';path.write_text('<testsuite tests="1" failures="0" errors="0" skipped="0">'
        '<testcase classname="native.test_disposable" name="test_one"/></testsuite>')
    expected={'sha256':digest(test),'tests':1,'roster_sha256':canonical_hash([('test_disposable','test_one')])}
    monkeypatch.setattr(q,'SUITES',{test.name:expected})
    return path,test,expected


def test_actual_junit_roster_and_executed_source_are_bound(junit):
    path,test,expected=junit;value=q.verify_suite(SimpleNamespace(locator=loc),path,test,expected)
    assert value=={'tests':{'tests':1,'failures':0,'errors':0,'skipped':0},'test_source':loc(test),'junit':loc(path)}


@pytest.mark.parametrize('change',['empty','duplicate','skip','failure','error','false_count','source','wrong_roster','entity','oversize'])
def test_partial_unbound_or_nonpassing_junit_never_qualifies(junit,change):
    path,test,expected=junit;text=path.read_text()
    if change=='empty':text=text.replace('<testcase classname="native.test_disposable" name="test_one"/>','')
    if change=='duplicate':text=text.replace('</testsuite>','<testcase classname="native.test_disposable" name="test_one"/></testsuite>')
    if change in {'skip','failure','error'}:text=text.replace('/></testsuite>','><'+('skipped' if change=='skip' else change)+'/></testcase></testsuite>')
    if change=='false_count':text=text.replace('tests="1"','tests="2"')
    if change=='source':test.write_text('# changed')
    if change=='wrong_roster':text=text.replace('test_one','test_other')
    if change=='entity':text='<!DOCTYPE testsuite [<!ENTITY x "y">]>'+text
    if change=='oversize':text=' '*8_000_001
    path.write_text(text)
    with pytest.raises(ValueError):q.verify_suite(SimpleNamespace(locator=loc),path,test,expected)


def test_child_packages_are_separate_and_fixture_sources_are_explicit(monkeypatch,tmp_path):
    from horsehair import rna_recovery
    seen=[]
    def environment(context):
        seen.append(context);return {'PYTHONPATH':'vendor:'+context['primary_release'],
            'PYTEST_ADDOPTS':'-k one','PYTEST_PLUGINS':'foreign','PYTHONHOME':'foreign','PYTHONINSPECT':'1'}
    monkeypatch.setattr(rna_recovery,'_primary_environment',environment)
    consumer=tmp_path/'whole-f047';module=SimpleNamespace(CONSUMER_FRAMEWORK=consumer)
    first=q.child_environment(module,q.FRAMEWORK);second=q.child_environment(module,consumer)
    assert seen[0]['primary_release']==str(q.FRAMEWORK) and seen[1]['primary_release']==str(consumer)
    for env in (first,second):
        assert not {'PYTEST_ADDOPTS','PYTEST_PLUGINS','PYTHONHOME','PYTHONINSPECT'}&env.keys()
        assert env['HHW_ARCHIVE_SYNTHETIC_FIXTURE_ROOT']==env['HHW_OVERLAY_PUBLIC_FIXTURE_ROOT']==str(consumer)
        assert env['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1' and env['PYTHONNOUSERSITE']=='1'


@pytest.fixture
def allocation(tmp_path,monkeypatch):
    end=2000000000
    row={'JobId':'123','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING','NumCPUs':'4',
        'NumNodes':'1-1','TimeLimit':'01:00:00','MinMemoryNode':'16G','AllocTRES':'cpu=4,mem=16G,node=1',
        'ReqTRES':'cpu=4,mem=16G,node=1','Command':'/registered/q.sbatch','JobName':'hhw-q',
        'EndTime':datetime.fromtimestamp(end,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}
    monkeypatch.setattr(q.time,'time',lambda:end-1000);monkeypatch.setenv('SLURM_JOB_END_TIME',str(end))
    calls=[]
    def read(job,path):
        calls.append((job,path));return subprocess.CompletedProcess([],0,' '.join(k+'='+v for k,v in row.items()),'')
    monkeypatch.setattr(q.runpy,'run_path',lambda *a,**kw:{'run_scontrol_job':read});monkeypatch.setattr(q,'CASE',tmp_path/'case')
    return row,calls,end


def test_native_owner_reads_one_actual_finite_owned_allocation(allocation):
    row,calls,end=allocation
    assert q.native_owner('123')==(row,end) and len(calls)==1


@pytest.mark.parametrize('key,value',[('JobId','124'),('UserId','other(1)'),('Account','other'),('JobState','PENDING'),
    ('NumCPUs','5'),('NumNodes','1-2'),('TimeLimit','02:00:00'),('MinMemoryNode','8G'),
    ('AllocTRES',''),('ReqTRES','cpu=4,gres/gpu=1'),('ReqTRES','cpu=4,cpu=4')])
def test_native_owner_refuses_wrong_identity_or_resources(allocation,key,value):
    allocation[0][key]=value
    with pytest.raises(ValueError):q.native_owner('123')


@pytest.mark.parametrize('value',['nan','2000000003'])
def test_native_owner_refuses_unverified_end_time(allocation,monkeypatch,value):
    monkeypatch.setenv('SLURM_JOB_END_TIME',value)
    with pytest.raises(ValueError):q.native_owner('123')


def test_native_registration_forwards_all_actual_inputs_to_the_real_adapter(tmp_path,monkeypatch):
    root=tmp_path/'root';stage=root/'maintenance/stage';stage.mkdir(parents=True);monkeypatch.setattr(q,'ROOT',root)
    monkeypatch.setattr(q,'CASE',root/'maintenance/result');producer=stage/'qual.py';producer.write_text('# source')
    monkeypatch.setattr(q,'__file__',str(producer))
    args=SimpleNamespace(native_task='actual-q')
    for name in ('native_plan','native_ledger','native_script','overlay','verifier','base_controller','overlay_tests','verifier_tests'):
        path=stage/name;path.write_text('# '+name);setattr(args,name,path)
    seen=[];allocation={'Command':str(args.native_script),'JobName':'hhw-actual-q'}
    module=SimpleNamespace(locator=loc,CONSUMER_FRAMEWORK=root/'releases/f047',
        private_producer_registration=lambda value,binding:seen.append((value,binding)))
    registration,inputs,source=q.registration(module,args,'900',allocation)
    value,binding=seen[0]
    assert registration['native_job_id']=='900' and registration['task_name']=='actual-q'
    assert inputs=={k:loc(getattr(args,k)) for k in ('overlay','verifier','base_controller','overlay_tests','verifier_tests')}|{'fixture_root':str(module.CONSUMER_FRAMEWORK)}
    assert value['producer_allocation']==allocation and binding['producer']==source==loc(producer)
    assert set(value['suites'])=={q.OVERLAY_TEST,q.VERIFIER_TEST}


@pytest.fixture
def public_qualification(tmp_path,monkeypatch):
    root=tmp_path/'root';artifacts=root/'state/qualification-evidence/public';artifacts.mkdir(parents=True)
    monkeypatch.setattr(q,'ROOT',root)
    junit=artifacts/'junit.xml';junit.write_text('<testsuite tests="1" failures="0" errors="0" skipped="0">'
        '<testcase classname="public.test_one" name="test_actual"/></testsuite>')
    path=root/'qualification.json'
    value={'status':'complete','returncode':0,'source_sha256':q.SOURCE,'config_sha256':q.CONFIG,
        'source_verified_unchanged':True,'slurm_job_id':'700','test_counts':{'tests':1,'failures':0,'errors':0,'skipped':0},
        'artifacts':{'junit':{**loc(junit),'bytes':junit.stat().st_size}}}
    path.write_text(json.dumps(value));calls=[]
    def bound(binding,maximum=10_000_000,*,namespace=None):
        target=Path(binding['path']);raw=target.read_bytes()
        assert namespace is None or target.is_relative_to(namespace)
        q.require(len(raw)<=maximum and digest(target)==binding['sha256'],'Actual public source changed')
        return raw
    proof={'completion':{'job_id':'700','native_records_sha256':'a'*64},'native':{'state':'COMPLETED'}}
    def joined(job,**kwargs):calls.append((job,kwargs));return proof
    module=SimpleNamespace(strict=json.loads,read_bound=bound)
    return path,value,junit,module,joined,calls,proof


def test_public_qualification_requires_actual_junit_then_source_checked_native_join(public_qualification):
    path,value,junit,module,joined,calls,proof=public_qualification
    binding={**loc(path),'job_id':'700'}
    assert q.qualification(module,binding,q.SOURCE,1,joined,time.time()+3600)==proof
    assert calls[0][0]=='700' and time.monotonic()<calls[0][1]['deadline']<=time.monotonic()+60


@pytest.mark.parametrize('change',['source','config','counts','boolean_return','missing_actual_case','bytes','job'])
def test_public_qualification_never_borrows_counts_or_accepts_unbound_source(public_qualification,change):
    path,value,junit,module,joined,calls,proof=public_qualification
    if change=='source':value['source_sha256']='b'*64
    if change=='config':value['config_sha256']='b'*64
    if change=='counts':value['test_counts']['tests']=2
    if change=='boolean_return':value['returncode']=False
    if change=='missing_actual_case':
        junit.write_text('<testsuite tests="1" failures="0" errors="0" skipped="0"/>')
        value['artifacts']['junit']={**loc(junit),'bytes':junit.stat().st_size}
    if change=='bytes':value['artifacts']['junit']['bytes']+=1
    if change=='job':value['slurm_job_id']='701'
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):q.qualification(module,{**loc(path),'job_id':'700'},q.SOURCE,1,joined,time.time()+3600)
    assert calls==[]


def test_missing_genuine_public_native_completion_cannot_be_replaced(public_qualification):
    path,value,junit,module,joined,calls,proof=public_qualification
    def missing(job,**kwargs):raise ValueError('Positive native completion is absent')
    with pytest.raises(ValueError,match='completion is absent'):
        q.qualification(module,{**loc(path),'job_id':'700'},q.SOURCE,1,missing,time.time()+3600)


@pytest.mark.parametrize('value',['','relative-fixtures'])
def test_fixture_directory_requires_an_actual_allocated_absolute_directory(monkeypatch,value):
    monkeypatch.setenv('SLURM_TMPDIR',value)
    with pytest.raises(ValueError):q.allocated_fixture_directory('700')


@pytest.fixture
def storage(tmp_path,monkeypatch):
    from horsehair import admission
    root=tmp_path/'root';path=root/'state/storage-budget.json';path.parent.mkdir(parents=True)
    clock=[100000.];captured=100000.-21600.+1
    path.write_text(json.dumps({'measured_unix':captured,'captured_utc':datetime.fromtimestamp(captured,timezone.utc).isoformat()}))
    monkeypatch.setattr(q,'ROOT',root);monkeypatch.setattr(q,'CASE',root/'maintenance/case');monkeypatch.setattr(q.time,'time',lambda:clock[0])
    effects=[]
    def quota(*args,**kwargs):effects.append(('quota',kwargs));return {'headroom':'actual mock'}
    monkeypatch.setattr(q.runpy,'run_path',lambda *a,**kw:{'require_storage_headroom_with_wait':quota})
    monkeypatch.setattr(admission,'_storage',lambda *a,**kw:(12,34,[path]))
    module=SimpleNamespace(strict=json.loads,exclusive=lambda *a:effects.append(('proof',a)))
    return path,clock,effects,module


def test_storage_requires_current_quota_and_unchanged_fresh_census(storage):
    path,clock,effects,module=storage;value=q.storage_gate(module,{},clock[0]+3600)
    assert value['census']==loc(path) and effects[0][0]=='quota' and effects[-1][0]=='proof'
    assert effects[0][1]['required_files']==2000 and effects[0][1]['reserve_bytes']==2_000_000_000_000
    assert value['future_storage_savings_credited']==0


def test_quota_wait_crossing_six_hour_census_clock_refuses_suite_admission(storage,monkeypatch):
    path,clock,effects,module=storage
    def quota(*args,**kwargs):clock[0]+=2;return {}
    monkeypatch.setattr(q.runpy,'run_path',lambda *a,**kw:{'require_storage_headroom_with_wait':quota})
    with pytest.raises(ValueError,match='six hours'):q.storage_gate(module,{},clock[0]+3600)
    assert not effects


def test_changed_census_during_quota_wait_refuses_suite_admission(storage,monkeypatch):
    path,clock,effects,module=storage
    def quota(*args,**kwargs):path.write_text(path.read_text()+'\n');return {}
    monkeypatch.setattr(q.runpy,'run_path',lambda *a,**kw:{'require_storage_headroom_with_wait':quota})
    with pytest.raises(ValueError,match='changed'):q.storage_gate(module,{},clock[0]+3600)
    assert not effects


@pytest.mark.parametrize('change',['old','future','nan','bool','naive'])
def test_fresh_census_refuses_malformed_or_out_of_window_times(change):
    value={'measured_unix':100000.,'captured_utc':datetime.fromtimestamp(100000.,timezone.utc).isoformat()};now=100001.
    if change=='old':value['measured_unix']=now-21600
    if change=='future':value['measured_unix']=now+1
    if change=='nan':value['measured_unix']=float('nan')
    if change=='bool':value['measured_unix']=True
    if change=='naive':value['captured_utc']='1970-01-02T00:00:00'
    with pytest.raises(ValueError):q.fresh_census(value,now=now)


def test_full_suite_command_has_local_basetemp_and_no_test_selector(junit,tmp_path,monkeypatch):
    path,test,expected=junit;case=tmp_path/'case';case.mkdir();stage=test.parent
    monkeypatch.setattr(q,'CASE',case);monkeypatch.setattr(q,'SUPPORT',stage)
    monkeypatch.setattr(q,'child_environment',lambda *a:{'TZ':'UTC'})
    seen=[]
    def runner(command,**kwargs):
        seen.append((command,kwargs));Path(command[command.index('--junitxml')+1]).write_bytes(path.read_bytes())
        return subprocess.CompletedProcess(command,0)
    module=SimpleNamespace(locator=loc,exclusive=lambda *a:None)
    q.run_suite(module,test.name,q.FRAMEWORK,tmp_path/'allocated',time.time()+3600,runner=runner)
    command,kwargs=seen[0]
    assert not {'-k','--deselect','--lf','--last-failed'}&set(command)
    assert command[command.index('--basetemp')+1]==str(tmp_path/'allocated'/test.name)
    assert command[command.index('--rootdir')+1]==str(stage) and 0<kwargs['timeout']<=2400
    assert kwargs['cwd']==q.FRAMEWORK and kwargs['stdin']==subprocess.DEVNULL


def test_timeout_never_returns_a_passing_suite(junit,tmp_path,monkeypatch):
    _,test,_=junit;case=tmp_path/'case';case.mkdir();monkeypatch.setattr(q,'CASE',case);monkeypatch.setattr(q,'SUPPORT',test.parent)
    monkeypatch.setattr(q,'child_environment',lambda *a:{});events=[]
    def timeout(command,**kwargs):raise subprocess.TimeoutExpired(command,kwargs['timeout'])
    with pytest.raises(subprocess.TimeoutExpired):
        q.run_suite(SimpleNamespace(locator=loc,exclusive=lambda *a:events.append(a)),test.name,q.FRAMEWORK,tmp_path/'allocated',time.time()+3600,runner=timeout)
    assert events[-1][1]['status']=='native_test_closure_timeout'


def test_native_producer_is_test_only_and_requires_real_later_completion():
    tree=ast.parse(SOURCE.read_bytes());calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call)]
    assert not any(isinstance(n.func,ast.Attribute) and n.func.attr in {'unlink','rmtree','submit','cancel','release','pack_policy','install_overlay'} for n in calls)
    strings={n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and type(n.value) is str}
    assert not {'-k','--deselect','--lf','sbatch','scancel'}&strings
    assert 'producer_native_completion' in strings and 'pending' in strings
    assert all(value is False for key,value in q.FLAGS.items() if key!='future_storage_savings_credited')
    assert q.FLAGS['future_storage_savings_credited']==0
    main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
    named=[n for n in ast.walk(main) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)]
    line=lambda name:min(n.lineno for n in named if n.func.id==name)
    assert line('load_overlay')<line('native_owner')<line('registration')<line('qualification')<line('storage_gate')<line('run_suite')
