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
import pytest

HERE=Path(__file__).resolve().parent


def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


@pytest.fixture
def m():return load(HERE/'run-census-resilience-consumer-v1-20261024.py','consumer_runtime_test')


@pytest.fixture
def builder():return load(HERE/'build-consumer-plan.py','consumer_builder_test')


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
    local=(HERE.parents[1]/'f2-backfill-controller-v2'/m.AF.name) if kind=='af' else HERE.parents[3]/'work'/m.PRIMARY.name
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
    assert all(getattr(old,n) is v for n,v in functions.items() if n not in {'module','make_consumer','bounded_child'})
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
    return {'phase':'study-af-census-resilience-consumers-v1-20261024','tasks':tasks},{'tasks':rows},binding


def test_chain_and_native_predecessor_positive(m):
    plan,ledger,binding=chain_fixture(m);assert m.chain_guard(plan,ledger,binding)==['900','901','902','903']
    calls=[]
    def complete(job,**kw):calls.append((job,kw));return {'positive':True}
    m.predecessor_guard({'Dependency':'(null)'},plan['tasks'][0],ledger,binding,complete)
    assert calls and all(k=={'dependency_kind':'afterany'} for _,k in calls)


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
    native=root/'plans'/phase/'plan.json';capture=HERE.parents[3]/'work/census-resilience-captured-inputs'
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
    manifest={'kind':'census-resilience-consumer-source-manifest-v1','files':{name:m.digest(m.SUPPORT/name) for name in m.FILES},'suites':{'consumers':contract}}
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
    bound={'kind':'census-resilience-consumer-binding-v1','consumer_kind':'af','source_manifest':mb,
        'qualification':{**m.locator(qp),'job_id':'777'},'provider_schedule':{
            'helper':{'path':str(m.PROVIDER_HELPER),'sha256':m.PROVIDER_HELPER_SHA},
            'source_manifest':{'path':str(m.PROVIDER_SUPPORT/'source-manifest.json'),'sha256':m.PROVIDER_MANIFEST_SHA},'case_root':str(m.PROVIDER_CASE)},
        'legacy_entry':{'path':str(old_path),'sha256':m.AF_SHA},'legacy_policy':m.locator(old_policy),'afterany_parent':'6950429','flags':m.FALSE_FLAGS}
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
