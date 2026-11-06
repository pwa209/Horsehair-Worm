"""Generic read-only routing, native-contract and emitted-registration fixtures."""
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from contextlib import nullcontext
from unittest.mock import patch
import copy
import importlib.util
import json
import shlex
import os
import subprocess
import sys
import pytest

HERE=Path(__file__).resolve().parent
REPOSITORY=next((p for p in HERE.parents if (p/'src/horsehair').is_dir()),Path('/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e'))
OPERATIONS=REPOSITORY/'operations'


def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


@pytest.fixture
def m():return load(HERE/'run-census-resilience-consumer-v2-20261024.py','consumer_runtime_test')


@pytest.fixture
def builder():return load(HERE/'build-consumer-plan.py','consumer_builder_test')


def test_actual_qualifier_pytest_accepts_child_basetemp(tmp_path):
    qualifier=load(HERE/'qualify-census-resilience-consumers-v2-20261024.py','consumer_qualifier_v2_test')
    workspace,basetemp=qualifier.fixture_workspace(tmp_path,'900000001')
    assert workspace.is_dir() and basetemp.is_dir() and basetemp.parent==workspace
    sentinel=workspace/'test_generic_fixture.py'
    sentinel.write_text('def test_allocation_local_fixture(tmp_path):\n    assert tmp_path.is_dir()\n',encoding='utf8')
    response=subprocess.run([sys.executable,'-s','-B','-m','pytest',str(sentinel),'-q','-p','no:cacheprovider',
        '--basetemp',str(basetemp)],cwd=workspace,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},
        text=True,capture_output=True,timeout=60,check=False)
    assert response.returncode==0,(response.stdout,response.stderr)
    assert '1 passed' in response.stdout and not response.stderr


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value),encoding='utf8');return path


def registration_fixture(m,tmp_path):
    m.ROOT=tmp_path;entry=tmp_path/'maintenance/new.py';script=tmp_path/'plans/new/task.sbatch'
    argv=['--x','one'];command=shlex.join(['python',str(entry),*argv]);text='#!/bin/bash\n'+command+'\n'
    script.parent.mkdir(parents=True);script.write_bytes(text.encode())
    task={'name':'one','command':command,'script':str(script),'script_sha256':m.canonical(text),
        'resources':{'cpus':1,'hours':96,'memory':'64G','gpu':False,'array':None}}
    plan={'root':str(tmp_path),'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]}
    pp=write(tmp_path/'plans/new/plan.json',plan)
    ledger={'config_sha256':m.CONFIG,'source_sha256':m.SOURCE,'plan_sha256':m.canonical(plan),
        'tasks':{'one':{'state':'submitted','job_id':'123','script_sha256':task['script_sha256']}}}
    lp=write(tmp_path/'state/new-submission.json',ledger)
    kw=dict(entry=entry,argv=argv,source=m.SOURCE,cpus=1,hours=96,memory='64G')
    return pp,lp,script,task,kw


def test_exact_registration(m,tmp_path):
    pp,lp,script,task,kw=registration_fixture(m,tmp_path)
    assert m.registration(m.locator(pp),m.locator(lp),m.locator(script),'one','123',**kw)==task


@pytest.mark.parametrize('change',['argv','entry','job','script','source','cpus','hours','memory','plan_hash','config','ledger_state','duplicate_helper'])
def test_registration_refuses_mismatch(m,tmp_path,change):
    pp,lp,script,task,kw=registration_fixture(m,tmp_path);job='123'
    if change in {'argv','entry','source','cpus','hours','memory'}:kw[change]={'argv':['--x','two'],'entry':tmp_path/'wrong','source':'0'*64,'cpus':2,'hours':72,'memory':'16G'}[change]
    elif change=='job':job='124'
    elif change=='script':script.write_text(script.read_text()+'changed\n')
    elif change=='duplicate_helper':
        plan=json.loads(pp.read_text());plan['tasks'][0]['command']+=' '+str(kw['entry']);write(pp,plan)
    else:
        ledger=json.loads(lp.read_text())
        if change=='plan_hash':ledger['plan_sha256']='0'*64
        elif change=='config':ledger['config_sha256']='0'*64
        else:ledger['tasks']['one']['state']='prepared'
        write(lp,ledger)
    with pytest.raises(ValueError):m.registration(m.locator(pp),m.locator(lp),m.locator(script),'one',job,**kw)


def native(m):
    return {'JobId':'123','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING','JobName':'hhw-one',
        'Command':'/plan/one.sbatch','NumCPUs':'1','NumNodes':'1','MinMemoryNode':'64G','TimeLimit':'08:00:00','TimeMin':'08:00:00',
        'EndTime':'2030-01-01T12:00:00','ReqTRES':'cpu=1,mem=64G','AllocTRES':'cpu=1,mem=64G'}


def test_native_effective_eight_hours(m,monkeypatch):
    fields=native(m);end=1893499200
    monkeypatch.setenv('SLURM_JOB_END_TIME',str(end))
    assert m.allocation(fields,'123',{'name':'one','script':'/plan/one.sbatch'},'af',wall=lambda:end-28800)==end


@pytest.mark.parametrize('key,value',[('JobId','124'),('UserId','other(42)'),('Account','def-other'),('JobState','PENDING'),
    ('NumCPUs','2'),('MinMemoryNode','16G'),('TimeLimit','07:59:59'),('TimeMin','00:00:00'),('AllocTRES','gpu=1'),('Command','/other')])
def test_native_contract_rejects_changes(m,monkeypatch,key,value):
    fields=native(m);fields[key]=value;monkeypatch.setenv('SLURM_JOB_END_TIME','1893499200')
    with pytest.raises(ValueError):m.allocation(fields,'123',{'name':'one','script':'/plan/one.sbatch'},'af',wall=lambda:1893470400)


def test_primary_keeps_seventy_two_hours(m,monkeypatch):
    fields=native(m);fields['TimeLimit']='3-00:00:00';monkeypatch.setenv('SLURM_JOB_END_TIME','1893499200')
    assert m.allocation(fields,'123',{'name':'one','script':'/plan/one.sbatch'},'primary',wall=lambda:1893400000)==1893499200


def test_verifier_near_shutdown_preserves_startup_reserve(m,monkeypatch):
    fields=native(m);fields['TimeLimit']='3-00:00:00';end=1893499200
    monkeypatch.setenv('SLURM_JOB_END_TIME',str(end));task={'name':'one','script':'/plan/one.sbatch'}
    with pytest.raises(ValueError,match='shutdown margin'):
        m.allocation(fields,'123',task,'primary',wall=lambda:end-14600)
    assert m.allocation(fields,'123',task,'primary',wall=lambda:end-14600,startup=False)==end
    with pytest.raises(ValueError,match='shutdown margin'):
        m.allocation(fields,'123',task,'primary',wall=lambda:end-300,startup=False)


def test_actual_verifier_shutdown_returns_pending_without_scientific_dispatch(m,tmp_path,monkeypatch,capsys):
    import time
    from datetime import datetime,timezone
    m.SUPPORT=tmp_path/'consumer';m.SUPPORT.mkdir();m.ROOT=tmp_path
    end=int(time.time())+14600;fields=native(m)
    fields.update(TimeLimit='3-00:00:00',EndTime=datetime.fromtimestamp(end,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'))
    tail=['--mode','run','--policy-descriptor','/policy','--policy-descriptor-sha256','a'*64,
          '--native-plan','/plan','--native-ledger','/ledger','--native-script','/plan/one.sbatch','--native-task','one']
    binding={'consumer_kind':'primary','legacy_entry':{'path':str(m.PRIMARY),'sha256':m.PRIMARY_SHA},'legacy_policy':{'path':'/policy','sha256':'a'*64}}
    task={'name':'one','script':'/plan/one.sbatch','census_resilience_consumer':{
        'entry':{'path':str(m.SUPPORT/m.SELF_NAME),'sha256':'b'*64},
        'binding':{'path':str(Path('/binding')),'sha256':'c'*64},'consumer_kind':'primary'}}
    legacy=SimpleNamespace(request_context=lambda args:{'argv':tail,'sequence':1},
        parser=lambda:SimpleNamespace(parse_args=lambda argv:None),FALSE_FLAGS={**m.FALSE_FLAGS},
        main=lambda *a:pytest.fail('Shutdown may not dispatch scientific verification'))
    monkeypatch.setattr(m,'check_binding',lambda *a:(binding,{'files':{m.SELF_NAME:'b'*64}},{}))
    monkeypatch.setattr(m,'module',lambda *a:legacy);monkeypatch.setattr(m,'registration',lambda *a,**kw:task)
    monkeypatch.setattr(m,'locator',lambda p:{'path':str(p),'sha256':'d'*64});monkeypatch.setattr(m,'packet',lambda *a,**kw:b'')
    response=SimpleNamespace(stdout=' '.join(k+'='+shlex.quote(v) for k,v in fields.items()),stderr='',check_returncode=lambda:None)
    monkeypatch.setattr(m.runpy,'run_path',lambda p:{'run_scontrol_job':lambda *a:response})
    monkeypatch.setattr(m.os,'geteuid',lambda:3164768,raising=False)
    monkeypatch.setenv('USER','pwa209');monkeypatch.setenv('SLURM_JOB_ID','123');monkeypatch.setenv('SLURM_JOB_END_TIME',str(end))
    argv=m.prefix(Path('/binding'),'c'*64,m.PRIMARY)[1:]+['--mode','verify','--request','/request']
    assert m.main(argv)==75
    value=json.loads(capsys.readouterr().out)
    assert value['status']=='pending_external_storage_census' and value['job_id']=='123'
    assert all(value[k] is False for k in m.FALSE_FLAGS)
    # The exact frozen parent's protocol recognizes75 before its success-only
    # branch and turns it into the existing soft pending exception.
    old=actual_legacy(m,'primary');old.ROOT=tmp_path
    # Production source checking is covered separately against the actual
    # pinned guards; this local protocol fixture has no production filesystem.
    monkeypatch.setattr(old,'source_guard',lambda pins:None)
    temporary=tmp_path/'allocation-local';temporary.mkdir()
    context={'job':'123','argv':tail,'parent_mode':'run','descriptor_sha256':'a'*64,'end':end,
        'allocation':fields,'registration':{},'sequence':0,'pins':{},'temporary':temporary}
    runner=lambda *a,**kw:subprocess.CompletedProcess(a[0],75,json.dumps(value).encode(),b'')
    with pytest.raises(old.PendingExternalCensus,match='shutdown reserve'):
        old.make_consumer(context,runner=runner)(tmp_path)


def test_parse_keeps_exact_legacy_tail(m):
    argv=['--consumer-binding','/binding','--consumer-binding-sha256','a'*64,'--legacy-entry','/old.py','--mode','run','--segment','0']
    assert m.parse(argv).legacy_argv==['--mode','run','--segment','0']


def test_duplicate_entry_boundary_rejected(m):
    with pytest.raises(ValueError):m.parse(['--legacy-entry','/a','--legacy-entry','/b'])


def test_private_provider_route_preserves_old_context_and_code(m,monkeypatch):
    calls=[];old_case=Path('/old-case');new_case=Path('/new-case')
    ns={'PROVIDER_CASE':old_case,'calls':calls}
    exec("def provider_proof(base,context,pair):\n calls.append((PROVIDER_CASE,context,pair))\n return {'job_id':'456','receipt':{'path':'/new-case/receipt.json','sha256':'d'*64}}\n",ns)
    outer=SimpleNamespace(provider_proof=ns['provider_proof']);original=outer.provider_proof
    binding={'provider_schedule':{'helper':{'path':str(m.PROVIDER_HELPER),'sha256':'a'*64},'case_root':str(new_case),'qualification':{'job_id':'new-q'}},'source_manifest':{'path':'/manifest','sha256':'b'*64}}
    original_context={'policy':{'provider_schedule':{'old':True},'qualification':{'job_id':'old-q'}},'base_context':{'config':{'frozen':True}}}
    snapshot=copy.deepcopy(original_context)
    provider=SimpleNamespace(validate_schedule_binding=lambda *a:{'checked_paths':{}},validate_predecessor_receipt=lambda *a:None)
    monkeypatch.setattr(m,'PROVIDER_CASE',new_case);monkeypatch.setattr(m,'module',lambda *a:provider);monkeypatch.setattr(m,'source_guard',lambda p:None)
    monkeypatch.setattr(m,'json_packet',lambda *a,**kw:{'predecessor_terminal':None})
    base=SimpleNamespace(registration_guard=lambda *a:nullcontext(),admission=lambda *a,**kw:None)
    result=m.routed_provider(outer,binding,{},lambda:None)(base,original_context,{'budget':'c'*64})
    assert outer.provider_proof is original and original.__globals__['PROVIDER_CASE']==old_case
    assert original_context==snapshot and calls[0][0]==new_case
    assert calls[0][1]['policy']['provider_schedule']==binding['provider_schedule']
    assert calls[0][1]['policy']['qualification']==snapshot['policy']['qualification']
    assert result['legacy_provider_proof_code_preserved'] is True


def test_provider_validation_cannot_be_skipped(m,monkeypatch):
    def proof(*args):raise AssertionError('must not reach old proof')
    outer=SimpleNamespace(provider_proof=proof)
    def reject(*args):raise ValueError('new provider Q absent')
    monkeypatch.setattr(m,'module',lambda *a:SimpleNamespace(validate_schedule_binding=reject));monkeypatch.setattr(m,'source_guard',lambda p:None)
    binding={'provider_schedule':{'helper':{'path':str(m.PROVIDER_HELPER),'sha256':'a'*64},'case_root':str(m.PROVIDER_CASE),'qualification':{'job_id':'new-q'}}}
    base=SimpleNamespace(registration_guard=lambda *a:nullcontext(),admission=lambda *a,**kw:None)
    with pytest.raises(ValueError,match='new provider Q absent'):m.routed_provider(outer,binding,{},lambda:None)(base,{'base_context':{},'policy':{}},{})


def actual_legacy(m,kind):
    native_path=m.AF if kind=='af' else m.PRIMARY
    local=(OPERATIONS/'20261024/f2-backfill-controller-v2'/m.AF.name) if kind=='af' else REPOSITORY/'work'/m.PRIMARY.name
    path=native_path if native_path.exists() else local
    expected=m.AF_SHA if kind=='af' else m.PRIMARY_SHA
    assert m.digest(path)==expected
    return load(path,'actual_frozen_'+kind)


def test_actual_af_legacy_source_guards_retained(m):
    old=actual_legacy(m,'af');functions={n:v for n,v in vars(old).items() if callable(v)}
    m.routed_provider(old,{}, {},lambda:None)
    assert all(getattr(old,n) is v for n,v in functions.items())
    import inspect
    source=inspect.getsource(old.main)
    assert source.index('checked=qualification(provider_policy)')<source.index('c=base.load_context')
    assert 'backfill_qualification(policy,provider_paths=checked)' in source
    assert 'provider_policy_guard(context)' in inspect.getsource(old.policy_guard)
    assert '0<=now-measured<21600' in inspect.getsource(old.fresh_pair)


def test_actual_primary_child_and_source_guards_retained(m,monkeypatch):
    old=actual_legacy(m,'primary');functions={n:v for n,v in vars(old).items() if callable(v)};captured=[]
    def child(command,**kw):captured.append((command,kw));return 'actual-transport-result'
    old.bounded_child=child
    original_make=old.make_consumer
    def make(context,*,runner):
        command=['python','-s','-u',str(m.PRIMARY),'--mode','verify','--request','/local/request','--request-sha256','c'*64]
        return runner(command,cwd=old.FRAMEWORK,env={'PYTHONPATH':str(old.FRAMEWORK/'src')},timeout=45)
    old.make_consumer=make;monkeypatch.setattr(m,'source_guard',lambda p:None)
    m.install_primary(old,{}, {},lambda:None,Path('/binding'),'b'*64)
    assert old.make_consumer({})=='actual-transport-result'
    command,kw=captured[0];assert command[:3]==['python','-s','-u'] and command.count(str(m.PRIMARY))==1
    assert command[3:]==m.prefix(Path('/binding'),'b'*64,m.PRIMARY)+['--mode','verify','--request','/local/request','--request-sha256','c'*64]
    assert kw['cwd']==old.FRAMEWORK and kw['timeout']==45
    assert all(getattr(old,n) is v for n,v in functions.items() if n not in {'module','make_consumer','bounded_child','derive_run'})
    import inspect
    assert 'source_guard(pins)' in inspect.getsource(old.verify_census)
    assert 'finalized_primary_qualification(policy, manifest, slurm, refresh)' in inspect.getsource(old.verify_census)
    assert 'package_origins(FRAMEWORK)' in inspect.getsource(old.verify_census)
    assert 'source_guard(context[\'pins\'])' in inspect.getsource(original_make)


def test_primary_route_refuses_different_child(m,monkeypatch):
    old=SimpleNamespace(module=lambda *a:None,bounded_child=lambda *a,**kw:None,CENSUS_SUPPORT=Path('/old'),OUTER_NAME='outer.py')
    old.make_consumer=lambda c,runner:runner(['python','/wrong.py','--mode','verify'])
    m.install_primary(old,{}, {},lambda:None,Path('/binding'),'b'*64)
    with pytest.raises(ValueError):old.make_consumer({})


def old_plan_fixture(builder,kind):
    r=builder.runtime();entry=r.AF if kind=='af' else r.PRIMARY;policy={'path':'/scratch/pwa209/horsehair-worm/maintenance/old/policy.json','sha256':'a'*64}
    bound=builder.binding(kind,manifest={'path':'/manifest','sha256':'b'*64},qualification={'job_id':'12'},provider_schedule={},legacy_policy=policy)
    scripts={};tasks=[]
    for i in range(4):
        name='old-'+str(i);script='/old/'+name+'.sbatch';tail=(['--mode','run'] if kind=='primary' else [])+['--policy-descriptor',policy['path'],
            '--policy-descriptor-sha256',policy['sha256'],'--native-plan','/old/plan.json','--native-ledger','/old/ledger.json','--native-task',name,'--native-script',script,'--segment',str(i)]
        command=shlex.join(['python','-s','-u',str(entry),*tail]);text='#!/bin/bash\n#SBATCH --job-name=hhw-'+name+'\n#SBATCH --output=/old/'+name+'.out\n'
        if kind=='af':text+='#SBATCH --time-min=08:00:00\n'
        text+=command+'\n';scripts[script]=text
        task={'name':name,'command':command,'script':script,'script_sha256':builder.canonical(text),'depends':[] if i==0 else ['old-'+str(i-1)],
            'resources':{'cpus':1,'memory':'64G','hours':96 if kind=='af' else 72,'gpu':False,'array':None},'preserved_science_binding':{'sha256':'z'*64}}
        if kind=='primary':task['primary_external_census']={'helper':bound['legacy_entry'],'policy_descriptor':policy,'mode':'run'}
        tasks.append(task)
    return {'tasks':tasks},scripts,bound,{'path':str(builder.SUPPORT/(kind+'-consumer-binding.json')),'sha256':'c'*64},{'files':{builder.SELF_NAME:'d'*64}}


@pytest.mark.parametrize('kind',['af','primary'])
def test_plan_new_chain_held_afterany_exact_old_parent(builder,kind):
    old,scripts,bound,loc,manifest=old_plan_fixture(builder,kind);snapshot=copy.deepcopy(old)
    result=builder.render(old,kind,loc,bound,manifest,script_reader=scripts.__getitem__)
    tasks=result['plan']['tasks'];assert old==snapshot and len(tasks)==4
    for i,task in enumerate(tasks):
        assert task['dependency_kind']=='afterany' and task['external_dependencies']==([builder.PARENTS[kind]] if i==0 else [])
        assert task['depends']==([] if i==0 else [tasks[i-1]['name']])
        assert result['scripts'][task['script']].splitlines().count('#SBATCH --hold')==1
        assert task['preserved_science_binding']==old['tasks'][i]['preserved_science_binding']
        command=shlex.split(task['command']);entry=bound['legacy_entry']['path'];assert command.count(entry)==1
        flags=dict(zip(command[command.index(entry)+1:][::2],command[command.index(entry)+1:][1::2]))
        assert flags['--native-plan']==result['plan_path'] and flags['--native-script']==task['script']
        assert flags['--policy-descriptor']==bound['legacy_policy']['path']
        if kind=='primary':assert task['primary_external_census']==old['tasks'][i]['primary_external_census']


@pytest.mark.parametrize('allowed',[False,True])
def test_primary_capacity_private_tick_defers_before_any_submission_journal(m,tmp_path,allowed):
    path=tmp_path/'pipeline.py'
    path.write_text('''def tick(candidate):
 phases={};ledger_path="/ledger";journal_path="/journal";plan_records={};ledgers={};states={}
 for name in ["phase"]:
  atomic_json(journal_path,{"state":"submitting","phase":name})
  result=submitter(candidate,ledger_path)
  phases[name]={"status":"queued"}
 return phases
''')
    calls=[];namespace={'atomic_json':lambda *a:calls.append(('journal',a)),
        'submitter':lambda *a:calls.append(('submit',a))}
    exec(compile(path.read_text(),str(path),'exec'),namespace)
    original=namespace['tick'];pipeline=SimpleNamespace(__file__=str(path),**namespace)
    checked=[]
    def guard(*args):checked.append(args);return nullcontext({'allowed':allowed})
    private,proof=m.derive_primary_capacity_tick(pipeline,guard,expected_sha=m.digest(path))
    result=private({'phase':'new'})
    assert pipeline.tick is original and len(checked)==1 and proof['guard_precedes_submitting_journal']
    if allowed:assert [v[0] for v in calls]==['journal','submit'] and result['phase']['status']=='queued'
    else:assert calls==[] and result['phase']['status']=='waiting_for_coordinated_capacity'


def primary_pipeline_fixture_source(m,*,repository=None,legacy=None):
    repository=REPOSITORY/'src/horsehair/pipeline.py' if repository is None else Path(repository)
    if repository.is_file():return repository,m.digest(repository)
    legacy=actual_legacy(m,'primary') if legacy is None else legacy
    path=legacy.OLD_FRAMEWORK/'src/horsehair/pipeline.py'
    assert path.is_file() and m.digest(path)==legacy.PIPELINE_SHA
    return path,legacy.PIPELINE_SHA


def test_actual_primary_tick_ast_and_original_functions_preserved(m):
    path,source_sha=primary_pipeline_fixture_source(m)
    pipeline=SimpleNamespace(__file__=str(path),sentinel=lambda:None)
    original=pipeline.sentinel
    private,proof=m.derive_primary_capacity_tick(pipeline,lambda *a:nullcontext({'allowed':False}),expected_sha=source_sha)
    assert callable(private) and pipeline.sentinel is original and proof['normal_primary_tick_ast_preserved']


@pytest.mark.parametrize('tamper',[False,True])
def test_primary_pipeline_fixture_resolves_standalone_capsule_with_exact_frozen_hash(m,tmp_path,tamper):
    path=tmp_path/'original-release/src/horsehair/pipeline.py';path.parent.mkdir(parents=True)
    path.write_text('def tick(): return "original"\n')
    legacy=SimpleNamespace(OLD_FRAMEWORK=tmp_path/'original-release',PIPELINE_SHA=m.digest(path))
    if tamper:path.write_text('def tick(): return "changed"\n')
    if tamper:
        with pytest.raises(AssertionError):primary_pipeline_fixture_source(m,repository=tmp_path/'missing/src/horsehair/pipeline.py',legacy=legacy)
    else:assert primary_pipeline_fixture_source(m,repository=tmp_path/'missing/src/horsehair/pipeline.py',legacy=legacy)==(path,legacy.PIPELINE_SHA)


def test_primary_capacity_state_keeps_pending_held_array_commitments(m):
    resources={'cpus':8,'gpu':False,'hours':2,'array':'0-2%1'}
    plan={'tasks':[{'name':'rna','resources':resources,'script_sha256':'a'*64}]}
    ledger={'plan_sha256':m.canonical(plan),'tasks':{'rna':{'state':'submitted','job_id':'123','script_sha256':'a'*64}}}
    states={'123_0':{'state':'COMPLETED','exit_code':'0:0'},'123_1':{'state':'PENDING'}}
    jobs=m.primary_capacity_state({'rna':plan},{'rna':ledger},states)
    assert set(jobs)=={'123_1','123_2'} and all(j['intent']['resources']['cpus']==8 for j in jobs.values())


def test_primary_capacity_state_refuses_ambiguous_registered_work(m):
    plan={'tasks':[{'name':'science','resources':{'cpus':2,'gpu':False,'hours':1},'script_sha256':'a'*64}]}
    ledger={'plan_sha256':m.canonical(plan),'tasks':{'science':{'state':'submitting'}}}
    with pytest.raises(ValueError,match='Ambiguous'):m.primary_capacity_state({'s':plan},{'s':ledger},{})


@pytest.mark.parametrize('array,cpus',[('0-59%1',32),('0-59%2',64),('0-2',96)])
def test_primary_candidate_capacity_uses_frozen_array_concurrency(m,array,cpus):
    from horsehair.pipeline import prospective_capacity
    candidate={'phase':'proposed','tasks':[{'name':'rna','depends':[],'resources':{'cpus':32,'gpu':False,'hours':2,'array':array}},
                        {'name':'parallel','depends':[],'resources':{'cpus':4,'gpu':False,'hours':1}}]}
    assert m.primary_candidate_resources(candidate,prospective_capacity)=={'cpus':cpus+4,'gpus':0,'hours':2}


def test_primary_candidate_serial_dag_uses_original_peak_not_task_sum(m):
    from horsehair.pipeline import prospective_capacity
    candidate={'phase':'proposed','tasks':[
        {'name':'train','depends':[],'resources':{'cpus':6,'gpu':True,'hours':2}},
        {'name':'evaluate','depends':['train'],'resources':{'cpus':32,'gpu':False,'hours':1}},
        {'name':'analyze','depends':['evaluate'],'resources':{'cpus':4,'gpu':False,'hours':1}}]}
    assert m.primary_candidate_resources(candidate,prospective_capacity)=={'cpus':32,'gpus':1,'hours':2}
    parallel=copy.deepcopy(candidate);parallel['tasks'][1]['depends']=[]
    assert m.primary_candidate_resources(parallel,prospective_capacity)['cpus']==38
    parallel['tasks'].append({'name':'other','depends':[],'resources':{'cpus':8,'gpu':False,'hours':1}})
    assert m.primary_candidate_resources(parallel,prospective_capacity)['cpus']==46


def test_primary_uses_exact_shared_coordinator_capacity_contract(m):
    path=m.COORDINATION if m.COORDINATION.exists() else HERE.parent/'coordination/coordination.py'
    assert m.COORDINATION==m.ROOT/'maintenance/storage-reserve-200k-v2-20261106/coordination/coordination.py'
    shared=load(path,'qualified_shared_primary_capacity')
    native='101|pwa209|hhw-primary-controller|RUNNING|1|N/A|/scratch/pwa209/horsehair-worm\n'
    native+='201|pwa209|hhw-census-provider|PENDING|8|N/A|/scratch/pwa209/horsehair-worm\n'
    chains=[{'role':'controller','jobs':{'101':{'name':'hhw-primary-controller','cpus':1,'gpus':0}}},
            {'role':'provider','jobs':{'201':{'name':'hhw-census-provider','cpus':8,'gpus':0}}}]
    allowed=shared.capacity_totals(native,{'cpus':44,'gpus':0,'hours':2},serial_chains=chains)
    assert allowed['allowed'] and allowed['prospective_hhw_cpus']==53
    chains.append({'role':'controller','jobs':{'102':{'name':'hhw-agent-controller','cpus':1,'gpus':0}}})
    denied=shared.capacity_totals(native,{'cpus':44,'gpus':0,'hours':2},serial_chains=chains)
    assert not denied['allowed'] and denied['prospective_hhw_cpus']==54


def test_shared_snapshot_primary_ambiguity_blocks_before_capacity_math(m,tmp_path,monkeypatch):
    path=m.COORDINATION if m.COORDINATION.exists() else HERE.parent/'coordination/coordination.py'
    assert m.COORDINATION==m.ROOT/'maintenance/storage-reserve-200k-v2-20261106/coordination/coordination.py'
    shared=load(path,'qualified_shared_primary_visibility');calls=[]
    def unresolved(*args,**kwargs):
        calls.append((args,kwargs));raise shared.PendingCapacity('Source-bound primary submitting phase awaits actual acceptance evidence')
    monkeypatch.setattr(shared,'primary_visibility_guard',unresolved)
    monkeypatch.setattr(shared,'capacity_totals',lambda *a,**kw:pytest.fail('An ambiguous primary phase must not reach capacity arithmetic'))
    response=SimpleNamespace(returncode=0,stdout='',stderr='')
    with pytest.raises(shared.PendingCapacity,match='primary submitting'):
        shared.coordinated_snapshot({'cpus':6,'gpus':1,'hours':1},state={'jobs':{}},serial_chains=[],
            native_reader=lambda *a,**kw:{},evidence_dir=tmp_path,intent_id='primary-test',runner=lambda *a,**kw:response)
    assert len(calls)==1


def previous_consumer_plan_fixture(builder):
    old,scripts,bound,loc,manifest=old_plan_fixture(builder,'primary')
    for task in old['tasks']:
        prior=str(builder.PRIOR_SUPPORT/builder.SELF_NAME)
        old_binding={'path':str(builder.PRIOR_SUPPORT/'primary-consumer-binding.json'),'sha256':'e'*64}
        envelope=[prior,'--consumer-binding',old_binding['path'],
                  '--consumer-binding-sha256',old_binding['sha256'],'--legacy-entry']
        previous=task['command'];args=shlex.split(previous);at=args.index(bound['legacy_entry']['path'])
        task['command']=shlex.join(args[:at]+envelope+args[at:])
        scripts[task['script']]=scripts[task['script']].replace(previous,task['command'])
        task['script_sha256']=builder.canonical(scripts[task['script']])
        task['census_resilience_consumer']={'entry':{'path':prior,'sha256':'f'*64},
            'binding':old_binding,'consumer_kind':'primary'}
    return old,scripts,bound,loc,manifest


def test_resume_replaces_prior_consumer_envelope_without_nesting(builder):
    old,scripts,bound,loc,manifest=previous_consumer_plan_fixture(builder)
    result=builder.render(old,'primary',loc,bound,manifest,script_reader=scripts.__getitem__)
    for old_task,task in zip(old['tasks'],result['plan']['tasks']):
        command=shlex.split(task['command']);entry=bound['legacy_entry']['path']
        assert command.count(str(builder.SUPPORT/builder.SELF_NAME))==1
        assert str(builder.PRIOR_SUPPORT/builder.SELF_NAME) not in command
        assert command.count('--legacy-entry')==1 and command.count(entry)==1
        assert task['primary_external_census']==old_task['primary_external_census']
        assert task['external_dependencies']==(['7046404'] if task is result['plan']['tasks'][0] else [])


@pytest.mark.parametrize('change',['binding','duplicate','metadata'])
def test_resume_refuses_ambiguous_prior_consumer_envelope(builder,change):
    old,scripts,bound,loc,manifest=previous_consumer_plan_fixture(builder)
    task=old['tasks'][0];original=task['command'];args=shlex.split(original)
    if change=='binding':args[args.index('--consumer-binding')+1]='/wrong/binding.json'
    elif change=='duplicate':args.insert(args.index('--consumer-binding'),str(builder.PRIOR_SUPPORT/builder.SELF_NAME))
    else:task['census_resilience_consumer']['binding']['sha256']='9'*64
    task['command']=shlex.join(args);scripts[task['script']]=scripts[task['script']].replace(original,task['command'])
    task['script_sha256']=builder.canonical(scripts[task['script']])
    with pytest.raises(ValueError):builder.render(old,'primary',loc,bound,manifest,script_reader=scripts.__getitem__)


@pytest.mark.parametrize('change',['parent','script','resource','segment','policy','dependency','duplicate_helper','embedded_dependency'])
def test_plan_refuses_ambiguous_old_inputs(builder,change):
    old,scripts,bound,loc,manifest=old_plan_fixture(builder,'af')
    if change=='parent':bound['afterany_parent']='999'
    elif change=='script':scripts[old['tasks'][0]['script']]+='changed\n'
    elif change=='resource':old['tasks'][0]['resources']['cpus']=2
    elif change=='dependency':old['tasks'][1]['depends']=[]
    elif change=='policy':bound['legacy_policy']['sha256']='e'*64
    elif change=='segment':old['tasks'][0]['command']=old['tasks'][0]['command'].replace('--segment 0','--segment 2')
    elif change=='duplicate_helper':old['tasks'][0]['command']+=' '+bound['legacy_entry']['path']
    else:
        script=old['tasks'][0]['script'];scripts[script]+='#SBATCH --dependency=afterok:777\n';old['tasks'][0]['script_sha256']=builder.canonical(scripts[script])
    with pytest.raises(ValueError):builder.render(old,'af',loc,bound,manifest,script_reader=scripts.__getitem__)


@pytest.mark.parametrize('raw',['{"x":1,"x":2}','{"x":NaN}'])
def test_strict_json(m,raw):
    with pytest.raises(ValueError):m.strict(raw)


def test_no_eager_horsehair_import(m):
    import ast
    tree=ast.parse((HERE/m.SELF_NAME).read_text())
    assert not any(isinstance(n,ast.ImportFrom) and (n.module or '').startswith('horsehair') for n in tree.body)


def chain_fixture(m):
    binding={'consumer_kind':'af','afterany_parent':'6950429'};tasks=[];rows={}
    for i in range(4):
        name='af-resilient-census-controller-'+str(i);job=str(900+i)
        tasks.append({'name':name,'depends':[] if i==0 else [tasks[-1]['name']],'external_dependencies':['6950429'] if i==0 else [],'dependency_kind':'afterany','script_sha256':'a'*64})
        issued=['6950429'] if i==0 else [str(899+i)]
        rows[name]={'state':'submitted','job_id':job,'script_sha256':'a'*64,'dependency_kind':'afterany','issued_native_dependencies':issued,
            'completed_native_dependencies':[],'dependency_resolution_sha256':m.canonical({'kind':'afterany','issued':issued,'completed':[]})}
    return {'phase':'study-af-census-resilience-consumers-v6-20261103','tasks':tasks},{'tasks':rows},binding


def test_chain_and_native_predecessor_positive(m):
    plan,ledger,binding=chain_fixture(m);assert m.chain_guard(plan,ledger,binding)==['900','901','902','903']
    calls=[]
    def complete(job,**kw):calls.append((job,kw));return {'positive':True}
    m.predecessor_guard({'Dependency':'(null)'},plan['tasks'][0],ledger,binding,complete)
    assert calls and all(k=={'dependency_kind':'afterany'} for _,k in calls)


def accounting_fixture(m):
    plan,ledger,binding=chain_fixture(m);task=plan['tasks'][1]
    final={'job_id_raw':'900','state':'COMPLETED','exit_code':'0:0','cpus':1,'elapsed_seconds':123,
        'user':'pwa209','account':'def-ptewarie_cpu','unallocated_metadata':False,
        'end':'2030-01-01T10:00:00','alloc_tres':'cpu=1,mem=64G,node=1','max_rss':'1024K'}
    return task,ledger,binding,final


@pytest.mark.parametrize('state,exit_code',[('COMPLETED','0:0'),('FAILED','1:0'),('TIMEOUT','0:15')])
def test_predecessor_accounting_lag_refreshes_actual_final_record(m,state,exit_code):
    task,ledger,binding,row=accounting_fixture(m);row.update(state=state,exit_code=exit_code);saved={};calls=[]
    def completion(job,**kw):return {'positive':True} if job==binding['afterany_parent'] or job in saved else None
    def native(ids,rosters,**kw):
        calls.append((ids,rosters,kw));return {'900':row}
    def reconcile(ids,runner):
        response=runner(['sacct','-j',','.join(ids)]);assert response.returncode==0
        assert response.stdout=='|'.join(['900','900',state,'123','1',row['alloc_tres'],exit_code,'1024K'])
        saved['900']=row
    value=m.finalized_predecessors({'Dependency':'(null)'},task,ledger,binding,completion,native,reconcile,
        20000,wall=lambda:1000,monotonic=lambda:0,sleeper=lambda _:pytest.fail('Final evidence needs no sleep'))
    assert value['native_reads']==1 and calls==[(['900'],{},{'owner':'pwa209','account':'def-ptewarie_cpu'})]


@pytest.mark.parametrize('change',['owner','account','job','cpus','gpu','elapsed','unallocated','end','exit'])
def test_predecessor_accounting_refuses_wrong_native_allocation(m,change):
    task,ledger,binding,row=accounting_fixture(m)
    key,value={'owner':('user','other'),'account':('account','def-other'),'job':('job_id_raw','901'),
        'cpus':('cpus',2),'gpu':('alloc_tres','cpu=1,gres/gpu=1'),'elapsed':('elapsed_seconds',0),
        'unallocated':('unallocated_metadata',True),'end':('end','Unknown'),'exit':('exit_code','unknown')}[change]
    row[key]=value
    completion=lambda job,**kw:{'positive':True} if job==binding['afterany_parent'] else None
    with pytest.raises(ValueError,match='Positive owned scalar'):
        m.finalized_predecessors({'Dependency':'(null)'},task,ledger,binding,completion,lambda *a,**kw:{'900':row},
            lambda *a,**kw:pytest.fail('Invalid allocation may not enter accounting'),20000,wall=lambda:1000,monotonic=lambda:0)


@pytest.mark.parametrize('observation',['missing','running','timeout'])
def test_predecessor_missing_or_running_accounting_wait_is_finite(m,observation):
    task,ledger,binding,row=accounting_fixture(m);row['state']='RUNNING';clock=[0.];queries=[]
    def native(*a,**kw):
        queries.append(a)
        if observation=='timeout':raise subprocess.TimeoutExpired(['sacct'],45)
        return {} if observation=='missing' else {'900':row}
    def sleep(seconds):clock[0]+=seconds
    completion=lambda job,**kw:{'positive':True} if job==binding['afterany_parent'] else None
    with pytest.raises(ValueError,match='Finite wait'):
        m.finalized_predecessors({'Dependency':'(null)'},task,ledger,binding,completion,native,
            lambda *a,**kw:pytest.fail('No final proof may be persisted'),20000,
            wall=lambda:1000+clock[0],monotonic=lambda:clock[0],sleeper=sleep)
    assert clock[0]==900 and len(queries)==30


@pytest.mark.parametrize('change',['phase','parallel','parent','duplicate_job','unsubmitted','afterok','wrong_native_edge','nonterminal'])
def test_chain_or_predecessor_rejects_unsafe_state(m,change):
    plan,ledger,binding=chain_fixture(m);fields={'Dependency':'(null)'};complete=lambda *a,**kw:{'positive':True}
    if change=='phase':plan['phase']='old'
    elif change=='parallel':plan['tasks'][1]['depends']=[]
    elif change=='parent':plan['tasks'][0]['external_dependencies']=[]
    elif change=='duplicate_job':ledger['tasks'][plan['tasks'][1]['name']]['job_id']='900'
    elif change=='unsubmitted':ledger['tasks'][plan['tasks'][1]['name']]['state']='planned'
    elif change=='afterok':plan['tasks'][0]['dependency_kind']='afterok'
    elif change=='wrong_native_edge':fields['Dependency']='afterany:123'
    else:complete=lambda *a,**kw:None
    with pytest.raises(ValueError):
        m.chain_guard(plan,ledger,binding);m.predecessor_guard(fields,plan['tasks'][0],ledger,binding,complete)


@pytest.mark.parametrize('kind',['af','primary'])
def test_actual_captured_or_native_plan_renders(builder,monkeypatch,kind):
    # The local capture is byte-preserving diagnostic568. Native Q reads the
    # exact original native plan/scripts instead; it does not copy production.
    r=builder.runtime();root=PurePosixPath('/scratch/pwa209/horsehair-worm')
    for name in ('ROOT','SUPPORT','AF','PRIMARY'):
        setattr(r,name,PurePosixPath(str(getattr(r,name)).replace('\\','/')))
    monkeypatch.setattr(builder,'ROOT',root);monkeypatch.setattr(builder,'SUPPORT',r.SUPPORT);monkeypatch.setattr(builder,'runtime',lambda:r)
    phase='study-af-f2-backfill-controllers-v2-20261024' if kind=='af' else 'primary-external-census-controllers-v3'
    native=root/'plans'/phase/'plan.json';capture=REPOSITORY/'work/census-resilience-captured-inputs'
    def actual(path):
        value=Path(str(path));return value if value.exists() else capture/str(path).removeprefix(str(root)+'/')
    old=json.loads(actual(native).read_text());entry=r.AF if kind=='af' else r.PRIMARY
    tokens=shlex.split(old['tasks'][0]['command']);tail=tokens[tokens.index(str(entry))+1:];flags=dict(zip(tail[::2],tail[1::2]))
    policy={'path':flags['--policy-descriptor'],'sha256':flags['--policy-descriptor-sha256']}
    bound=builder.binding(kind,manifest={'path':'/new/manifest','sha256':'1'*64},qualification={'job_id':'44'},provider_schedule={},legacy_policy=policy)
    loc={'path':str(r.SUPPORT/(kind+'-consumer-binding.json')),'sha256':'2'*64};manifest={'files':{builder.SELF_NAME:'3'*64}}
    result=builder.render(old,kind,loc,bound,manifest,script_reader=lambda path:actual(path).read_text())
    assert len(result['plan']['tasks'])==4
    for task in result['plan']['tasks']:
        text=result['scripts'][task['script']]
        assert text.splitlines().count('#SBATCH --hold')==1 and entry.as_posix() in task['command']
        assert task['resources']['hours']==(96 if kind=='af' else 72)


def qualification_fixture(m,tmp_path,monkeypatch):
    from horsehair import slurm
    m.ROOT=tmp_path;m.SUPPORT=tmp_path/'maintenance/consumers';m.SUPPORT.mkdir(parents=True)
    original=Path(m.__file__).read_bytes();m.__file__=str(m.SUPPORT/m.SELF_NAME)
    (m.SUPPORT/m.SELF_NAME).write_bytes(original)
    for name in m.FILES-{m.SELF_NAME}:(m.SUPPORT/name).write_text('# exact generic fixture\n')
    contract={'source':m.TEST_NAME,'source_sha256':m.digest(m.SUPPORT/m.TEST_NAME),'tests':1,
        'roster_sha256':m.canonical(['Fixture::test_one'])}
    manifest={'kind':'census-resilience-consumer-source-manifest-v2','files':{name:m.digest(m.SUPPORT/name) for name in m.FILES},'suites':{'consumers':contract}}
    manifest_path=write(m.SUPPORT/'source-manifest.json',manifest);mb=m.locator(manifest_path);_,pins=m.source_manifest(mb)
    xml=m.SUPPORT/'native-qualification-777/consumers.xml';xml.parent.mkdir()
    xml.write_bytes(b'<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"><testcase classname="Fixture" name="test_one"/></testsuite></testsuites>')
    pp=tmp_path/'plans/q/plan.json';lp=tmp_path/'state/q-submission.json';sp=tmp_path/'plans/q/q.sbatch';helper=m.SUPPORT/m.QUALIFIER_NAME
    tail=['--native-plan',str(pp),'--native-ledger',str(lp),'--native-task','q','--native-script',str(sp),
        '--source-manifest',str(manifest_path),'--source-manifest-sha256',mb['sha256']]
    command=shlex.join(['python','-s','-u',str(helper),*tail]);text='#!/bin/bash\n'+command+'\n';sp.parent.mkdir(parents=True);sp.write_bytes(text.encode())
    task={'name':'q','command':command,'script':str(sp),'script_sha256':m.canonical(text),
        'resources':{'cpus':4,'memory':'16G','hours':1,'gpu':False,'array':None},
        'census_resilience_consumer_qualification':{'helper':m.locator(helper),'source_manifest':mb,'suite':contract}}
    plan={'root':str(tmp_path),'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]};write(pp,plan)
    ledger={'config_sha256':m.CONFIG,'source_sha256':m.SOURCE,'plan_sha256':m.canonical(plan),
        'tasks':{'q':{'state':'submitted','job_id':'777','script_sha256':task['script_sha256']}}};write(lp,ledger)
    fields=native(m);fields.update(JobId='777',NumCPUs='4',MinMemoryNode='16G',TimeLimit='01:00:00',Command=str(sp),JobName='hhw-q')
    value={'status':'complete_census_resilience_consumer_qualification','returncode':0,'native_job_id':'777','source_manifest':mb,'source_pins':pins,
        'suite':{'source':m.locator(m.SUPPORT/m.TEST_NAME),'test_counts':{'tests':1,'failures':0,'errors':0,'skipped':0},
            'test_roster_sha256':contract['roster_sha256'],'junit':m.locator(xml)},
        'native_registration':{'plan':m.locator(pp),'ledger':m.locator(lp),'script':m.locator(sp),'task_name':'q','native_job_id':'777'},
        'allocation':fields,**m.FALSE_FLAGS}
    qp=write(m.SUPPORT/'qualification.json',value)
    completed={'native_records':{'777':{'state':'COMPLETED','exit_code':'0:0','cpus':4,'gpus':0,'elapsed_seconds':12}}}
    monkeypatch.setattr(slurm,'completed_native_dependency',lambda *a,**kw:completed)
    return qp,mb,value,completed


def test_actual_qualification_schema_roundtrip(m,tmp_path,monkeypatch):
    path,manifest,_,_=qualification_fixture(m,tmp_path,monkeypatch)
    result=m.validate_qualification({**m.locator(path),'job_id':'777'},manifest,lambda:None)
    assert str(path) in result['checked_paths'] and result['completion']['native_records']['777']['state']=='COMPLETED'


@pytest.mark.parametrize('change',['source','counts','junit','native','metadata','qualification_argv'])
def test_qualification_rejects_actual_bound_mismatch(m,tmp_path,monkeypatch,change):
    path,manifest,value,completed=qualification_fixture(m,tmp_path,monkeypatch)
    if change=='source':(m.SUPPORT/m.TEST_NAME).write_text('changed source')
    elif change=='counts':value['suite']['test_counts']['tests']=2;write(path,value)
    elif change=='junit':Path(value['suite']['junit']['path']).write_text('<testsuite/>')
    elif change=='native':completed['native_records']['777']['state']='RUNNING'
    else:
        pp=Path(value['native_registration']['plan']['path']);plan=json.loads(pp.read_text())
        if change=='metadata':plan['tasks'][0]['census_resilience_consumer_qualification']['source_manifest']['sha256']='0'*64
        else:plan['tasks'][0]['command']=plan['tasks'][0]['command'].replace('--source-manifest-sha256','--wrong-manifest-sha256')
        write(pp,plan);value['native_registration']['plan']=m.locator(pp);write(path,value)
    with pytest.raises(ValueError):m.validate_qualification({**m.locator(path),'job_id':'777'},manifest,lambda:None)


def test_actual_bounded_native_transport_preserves_current_guard(m,monkeypatch):
    from horsehair import repair
    calls=[]
    def guard(ids,rosters,*,runner,**kwargs):
        calls.append((ids,rosters,kwargs));return runner(['sacct','--jobs','123'],capture_output=True,text=True)
    monkeypatch.setattr(repair,'_repair_native_states',guard)
    monkeypatch.setattr(m.time,'time',lambda:1000)
    monkeypatch.setattr(m.subprocess,'run',lambda command,**kwargs:(command,kwargs))
    result=m.bounded_native_reader(3000)(['123'],{},owner='pwa209',account='def-ptewarie_cpu')
    assert result==(['sacct','--jobs','123'],{'capture_output':True,'text':True,'timeout':45})
    assert calls==[(['123'],{},{'owner':'pwa209','account':'def-ptewarie_cpu'})]


def test_actual_bounded_transport_refuses_expired_margin(m,monkeypatch):
    from horsehair import repair
    monkeypatch.setattr(repair,'_repair_native_states',lambda ids,rosters,runner,**kw:runner(['sacct']))
    monkeypatch.setattr(m.time,'time',lambda:1000)
    with pytest.raises(ValueError,match='cleanup margin'):m.bounded_native_reader(1599)(['123'],{})


def test_actual_bounded_transport_refuses_foreign_runner(m):
    with pytest.raises(ValueError,match='bounded actual'):m.bounded_native_reader(3000)(['123'],{},runner=lambda:None)


@pytest.mark.parametrize('duplicate',[False,True])
def test_actual_main_registration_native_qualification_and_dispatch(m,tmp_path,monkeypatch,duplicate):
    # Actual ingress, registration, native-field, new-Q and predecessor
    # validators run. Scheduler observations and terminal legacy execution are
    # fixture boundaries; this never claims native qualification or science.
    qp,mb,q,completed=qualification_fixture(m,tmp_path,monkeypatch)
    old_source=actual_legacy(m,'af');old_path=tmp_path/'maintenance/frozen-af.py'
    old_path.write_bytes(Path(old_source.__file__).read_bytes());m.AF=old_path
    old_policy=write(tmp_path/'maintenance/old-policy.json',{'kind':'original-policy-fixture'})
    manifest=json.loads(Path(mb['path']).read_text())
    bound={'kind':'census-resilience-consumer-binding-v2','consumer_kind':'af','source_manifest':mb,
        'qualification':{**m.locator(qp),'job_id':'777'},'provider_schedule':{
            'helper':{'path':str(m.PROVIDER_HELPER),'sha256':m.PROVIDER_HELPER_SHA},
            'source_manifest':{'path':str(m.PROVIDER_SUPPORT/'source-manifest.json'),'sha256':m.PROVIDER_MANIFEST_SHA},'case_root':str(m.PROVIDER_CASE)},
        'legacy_entry':{'path':str(old_path),'sha256':m.AF_SHA},'legacy_policy':m.locator(old_policy),'afterany_parent':'6950429','flags':m.FALSE_FLAGS}
    m.COORDINATION=tmp_path/'maintenance/new-coordination/coordination.py';m.COORDINATION.parent.mkdir()
    m.COORDINATION.write_text('# immutable fixture coordinator\n')
    shared_files={'coordination.py':m.digest(m.COORDINATION)}
    for name in ['registry-wait.py','test_coordination.py','test_registry.py','test_acknowledgements.py']:
        path=m.COORDINATION.parent/name;path.write_text('# immutable source fixture\n');shared_files[name]=m.digest(path)
    shared=write(m.COORDINATION.parent/'source-manifest.json',{'kind':'agent-coordination-repair-v1','files':shared_files})
    bound['coordination']={'entry':m.locator(m.COORDINATION),'source_manifest':m.locator(shared)}
    bp=write(m.SUPPORT/'af-consumer-binding.json',bound);bloc=m.locator(bp)
    plan,ledger,_=chain_fixture(m);plan.update(root=str(tmp_path),config_sha256=m.CONFIG,release_identity={'sha256':m.SOURCE})
    pp=tmp_path/'plans/consumer/plan.json';lp=tmp_path/'state/consumer-submission.json';first_argv=None
    for i,task in enumerate(plan['tasks']):
        sp=tmp_path/'plans/consumer'/(task['name']+'.sbatch')
        tail=['--policy-descriptor',str(old_policy),'--policy-descriptor-sha256',m.digest(old_policy),'--segment',str(i),
            '--native-plan',str(pp),'--native-ledger',str(lp),'--native-task',task['name'],'--native-script',str(sp)]
        if duplicate:tail+=['--segment',str(i)]
        if i==0:first_argv=tail
        args=m.prefix(bp,bloc['sha256'],old_path)[1:]+tail
        command=shlex.join(['python',str(m.SUPPORT/m.SELF_NAME),*args]);text='#!/bin/bash\n'+command+'\n'
        sp.parent.mkdir(parents=True,exist_ok=True);sp.write_bytes(text.encode())
        task.update(command=command,script=str(sp),script_sha256=m.canonical(text),resources={'cpus':1,'memory':'64G','hours':96,'gpu':False,'array':None},
            census_resilience_consumer={'entry':{'path':str(m.SUPPORT/m.SELF_NAME),'sha256':manifest['files'][m.SELF_NAME]},'binding':bloc,'consumer_kind':'af'})
        ledger['tasks'][task['name']]['script_sha256']=task['script_sha256']
    ledger.update(config_sha256=m.CONFIG,source_sha256=m.SOURCE,plan_sha256=m.canonical(plan));write(pp,plan);write(lp,ledger)
    fields=native(m);fields.update(JobId='900',JobName='hhw-'+plan['tasks'][0]['name'],Command=plan['tasks'][0]['script'],Dependency='(null)')
    response=SimpleNamespace(stdout=' '.join(k+'='+shlex.quote(v) for k,v in fields.items()),stderr='',check_returncode=lambda:None)
    m.SCHEDULER=tmp_path/'maintenance/scheduler.py';m.SCHEDULER.write_text('# pinned fixture transport\n');m.SCHEDULER_SHA=m.digest(m.SCHEDULER)
    monkeypatch.setattr(m.runpy,'run_path',lambda p:{'run_scontrol_job':lambda *a:response})
    monkeypatch.setenv('SLURM_JOB_ID','900');monkeypatch.setenv('USER','pwa209');monkeypatch.setenv('SLURM_JOB_END_TIME','1893499200')
    monkeypatch.setattr(m.os,'geteuid',lambda:3164768,raising=False)
    calls=[]
    def original_provider(*a):raise AssertionError('no legacy dispatch in this fixture')
    legacy=SimpleNamespace(provider_proof=original_provider,main=lambda argv:calls.append(argv) or 0)
    monkeypatch.setattr(m,'module',lambda path,sha,name:legacy)
    argv=m.prefix(bp,bloc['sha256'],old_path)[1:]+first_argv
    if duplicate:
        with pytest.raises(ValueError,match='legacy run arguments'):m.main(argv)
        assert calls==[]
    else:
        assert m.main(argv)==0 and calls==[first_argv]
        assert legacy.provider_proof is not original_provider


@pytest.mark.parametrize('files,needed',[(0,200000),(2000,200000),(18565,200000),(199996,200001),(250000,250005)])
def test_primary_private_reserve_preserves_actual_demand_and_bytes(m,tmp_path,monkeypatch,files,needed):
    root=tmp_path/'study';root.mkdir();monkeypatch.setattr(m,'ROOT',root)
    events=[];policy=SimpleNamespace(path=Path('/explicit/policy.json'),sha256='a'*64)
    helper=SimpleNamespace(reserve_for=lambda count,p:max(0,200000-count-5),
        verify_policy=lambda *a,**kw:events.append('policy-after'))
    def factory(original,consumer):
        def checked(*args,**kwargs):
            consumer();value=original(*args,**kwargs);consumer();return value
        return checked
    def reader(where,**kwargs):
        events.append(('quota',kwargs));return kwargs
    fn=m.primary_quota_factory(factory,helper,policy)(reader,lambda:events.append('producer'))
    result=fn(root,required_files=files,reserve_files=max(0,250000-files),required_bytes=123,reserve_bytes=2000000000000)
    assert result['required_files']+result['reserve_files']+5==needed
    assert result['required_bytes']==123 and result['reserve_bytes']==2000000000000
    assert events[0]=='producer' and events[-2:]==['producer','policy-after']


@pytest.mark.parametrize('change',['foreign_root','wrong_reserve','noninteger_demand'])
def test_primary_private_reserve_rejects_unrelated_quota_calls(m,tmp_path,monkeypatch,change):
    root=tmp_path/'study';root.mkdir();monkeypatch.setattr(m,'ROOT',root)
    helper=SimpleNamespace(reserve_for=lambda *a:(_ for _ in ()).throw(AssertionError('no quota mutation')))
    fn=m.primary_quota_factory(lambda original,consumer:original,helper,None)(lambda *a,**kw:None,lambda:None)
    where=root if change!='foreign_root' else root.parent
    files=2000 if change!='noninteger_demand' else True
    reserve=248000 if change!='wrong_reserve' else 0
    with pytest.raises(ValueError):fn(where,required_files=files,reserve_files=reserve)


def test_primary_private_reserve_refuses_policy_changed_after_quota(m,tmp_path,monkeypatch):
    root=tmp_path/'study';root.mkdir();monkeypatch.setattr(m,'ROOT',root)
    policy=SimpleNamespace(path=Path('/explicit/policy.json'),sha256='a'*64)
    helper=SimpleNamespace(reserve_for=lambda *a:197995,
        verify_policy=lambda *a,**kw:(_ for _ in ()).throw(ValueError('policy changed')))
    fn=m.primary_quota_factory(lambda original,consumer:original,helper,policy)(lambda *a,**kw:{'ready':True},lambda:None)
    with pytest.raises(ValueError,match='policy changed'):fn(root,required_files=2000,reserve_files=248000)
