"""Pure native-producer binding tests; allocation-only canary runs in the job."""
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
from contextlib import contextmanager
import copy
import importlib.util
import json
import shlex
import subprocess
import xml.etree.ElementTree as ET

import pytest
from horsehair.core import canonical_hash,digest

spec=importlib.util.spec_from_file_location('rolling_native_qualifier',Path(__file__).with_name('qualify-streaming-census-v2-20261023.py'))
q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)


@pytest.fixture
def frozen(tmp_path,monkeypatch):
    root=tmp_path/'study';support=root/'maintenance/source';framework=root/'release'
    support.mkdir(parents=True);framework.mkdir();case=root/'maintenance/q';case.mkdir()
    monkeypatch.setattr(q,'ROOT',root);monkeypatch.setattr(q,'SUPPORT',support)
    monkeypatch.setattr(q,'FRAMEWORK',framework);monkeypatch.setattr(q,'CASE',case)
    for name in q.PRIVATE_FILES:(support/name).write_text('private fixture '+name)
    monkeypatch.setattr(q,'__file__',str(support/q.HELPER));monkeypatch.setattr(q,'GUARD_SHA',digest(support/q.GUARD_NAME))
    files={name:digest(support/name) for name in q.PRIVATE_FILES};suites={}
    for scope,relative in q.SUITE_SOURCES.items():
        p=framework/relative if scope.startswith('baseline_') else support/relative
        if scope.startswith('baseline_'):p.parent.mkdir(parents=True,exist_ok=True);p.write_text('public fixture '+scope)
        suites[scope]={'source':relative,'source_sha256':digest(p),'tests':1,'roster_sha256':canonical_hash(['test_fixture::test_positive'])}
    for relative in ('tests/conftest.py','src/horsehair/budget.py','src/horsehair/storage_artifacts.py'):
        p=framework/relative;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(relative)
    value={'kind':q.MANIFEST_KIND,'files':files,'suites':suites};manifest=support/'source-manifest.json'
    manifest.write_text(json.dumps(value));return SimpleNamespace(root=root,support=support,framework=framework,case=case,value=value,manifest=manifest)


def rewrite(f,value):
    f.manifest.write_text(json.dumps(value));return digest(f.manifest)


def test_manifest_binds_exact_twenty_three_private_public_and_primitive_sources(frozen):
    value,pins=q.source_manifest(frozen.manifest,digest(frozen.manifest))
    assert value==frozen.value and len(pins)==23
    assert pins[str(frozen.framework/'src/horsehair/storage_artifacts.py')]==digest(frozen.framework/'src/horsehair/storage_artifacts.py')


@pytest.mark.parametrize('target',['kernel','wrapper','registry','qualifier','baseline_budget'])
def test_any_suite_source_byte_change_refuses_manifest(frozen,target):
    relative=q.SUITE_SOURCES[target];p=frozen.framework/relative if target.startswith('baseline_') else frozen.support/relative
    p.write_text('tampered')
    with pytest.raises(ValueError,match='changed'):q.source_manifest(frozen.manifest,digest(frozen.manifest))


@pytest.mark.parametrize('change',['missing_file','extra_file','missing_suite','extra_suite','wrong_kind','zero_tests','bool_tests',
    'wrong_source','invented_source_sha','bad_roster','missing_suite_field','extra_suite_field'])
def test_manifest_rejects_incomplete_or_invented_contract(frozen,change):
    value=copy.deepcopy(frozen.value)
    if change=='missing_file':value['files'].pop(q.KERNEL)
    elif change=='extra_file':value['files']['foreign.py']='a'*64
    elif change=='missing_suite':value['suites'].pop('kernel')
    elif change=='extra_suite':value['suites']['invented']=value['suites']['kernel']
    elif change=='wrong_kind':value['kind']='old'
    else:
        c=value['suites']['kernel']
        if change=='zero_tests':c['tests']=0
        elif change=='bool_tests':c['tests']=True
        elif change=='wrong_source':c['source']='elsewhere.py'
        elif change=='invented_source_sha':c['source_sha256']='0'*64
        elif change=='bad_roster':c['roster_sha256']='not-a-sha'
        elif change=='missing_suite_field':c.pop('roster_sha256')
        elif change=='extra_suite_field':c['skip']=True
    with pytest.raises(ValueError):q.source_manifest(frozen.manifest,rewrite(frozen,value))


@pytest.mark.parametrize('raw',[b'{"x":1,"x":2}',b'{"x":NaN}',b'{"x":Infinity}'])
def test_duplicate_or_nonfinite_json_is_refused(raw):
    with pytest.raises(ValueError):q.strict_json(raw)


def test_manifest_sha_is_not_an_optional_hint(frozen):
    with pytest.raises(ValueError,match='changed'):q.source_manifest(frozen.manifest,'0'*64)


def test_wrong_manifest_path_is_refused_even_with_matching_bytes(frozen):
    other=frozen.support/'other.json';other.write_bytes(frozen.manifest.read_bytes())
    with pytest.raises(ValueError,match='locator'):q.source_manifest(other,digest(other))


def allocation_fields():
    return {'JobId':'101','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'NumCPUs':'8','NumNodes':'1','TimeLimit':'01:00:00','MinMemoryNode':'64G','Command':'/native/q.sbatch',
        'JobName':'hhw-streaming-q','EndTime':datetime.fromtimestamp(2000,timezone.utc).replace(tzinfo=None).isoformat(),
        'ReqTRES':'cpu=8,mem=64G,node=1','AllocTRES':'cpu=8,mem=64G,node=1'}


def allocation_reader(fields,extra=''):
    return lambda job,path:subprocess.CompletedProcess([],0,' '.join(k+'='+v for k,v in fields.items())+extra,'')


def test_actual_owned_native_allocation_is_bound_to_script_and_finite_margin(monkeypatch):
    monkeypatch.setenv('SLURM_JOB_END_TIME','2000')
    value,end=q.native_allocation('101','/native/q.sbatch','streaming-q',allocation_reader(allocation_fields()),wall=lambda:1000)
    assert end==2000 and value['fields']['NumCPUs']=='8'


@pytest.mark.parametrize('key,value',[('JobId','102'),('UserId','foreign(1)'),('Account','other'),('JobState','PENDING'),
    ('NumCPUs','4'),('NumNodes','2'),('TimeLimit','06:00:00'),('MinMemoryNode','16G'),('Command','/wrong'),
    ('JobName','hhw-other'),('ReqTRES','cpu=8,gres/gpu=1'),('ArrayJobId','101'),('ArrayTaskId','1'),
    ('ReqTRES','cpu=4,mem=64G,node=1'),('AllocTRES','cpu=8,mem=16G,node=1'),('AllocTRES','cpu=8,mem=64G,node=2')])
def test_wrong_native_owner_script_resources_or_state_is_refused(monkeypatch,key,value):
    monkeypatch.setenv('SLURM_JOB_END_TIME','2000');fields=allocation_fields();fields[key]=value
    with pytest.raises(ValueError):q.native_allocation('101','/native/q.sbatch','streaming-q',allocation_reader(fields),wall=lambda:1000)


def test_duplicate_native_fields_are_refused(monkeypatch):
    monkeypatch.setenv('SLURM_JOB_END_TIME','2000')
    with pytest.raises(ValueError):q.native_allocation('101','/native/q.sbatch','streaming-q',allocation_reader(allocation_fields(),' NumCPUs=8'),wall=lambda:1000)


@pytest.mark.parametrize('end',['nan','2003','1600'])
def test_unverified_or_exhausted_native_end_is_refused(monkeypatch,end):
    monkeypatch.setenv('SLURM_JOB_END_TIME',end)
    with pytest.raises(ValueError):q.native_allocation('101','/native/q.sbatch','streaming-q',allocation_reader(allocation_fields()),wall=lambda:1500)


@pytest.fixture
def registered(frozen,monkeypatch):
    directory=frozen.root/'plans/q';directory.mkdir(parents=True);(frozen.root/'state').mkdir()
    script=directory/'q.sbatch';plan_path=directory/'plan.json';ledger_path=frozen.root/'state/q-submission.json'
    argv=['--native-plan',str(plan_path),'--native-ledger',str(ledger_path),'--native-task','q','--native-script',str(script),
        '--source-manifest',str(frozen.manifest),'--source-manifest-sha256',digest(frozen.manifest)]
    own=str(Path(q.__file__));command=shlex.join(['python',own,*argv]);script.write_text('#!/bin/sh\n'+command+'\n')
    task={'name':'q','command':command,'script':str(script),'script_sha256':canonical_hash(script.read_text()),
        'resources':{'cpus':8,'gpu':False,'hours':1,'memory':'64G','array':None},
        'rolling_census_qualification':{'helper':{'path':own,'sha256':digest(q.__file__)},
            'source_manifest':{'path':str(frozen.manifest),'sha256':digest(frozen.manifest)}}}
    plan={'release_identity':{'sha256':q.SOURCE},'config_sha256':q.CONFIG,'tasks':[task]}
    ledger={'tasks':{'q':{'job_id':'101','state':'submitted','script_sha256':task['script_sha256']}}}
    plan_path.write_text(json.dumps(plan));ledger_path.write_text(json.dumps(ledger))
    monkeypatch.setattr(q.runpy,'run_path',lambda *a,**k:{'checked_pair':lambda p,l,*a:(json.loads(p.read_text()),json.loads(l.read_text()))})
    return frozen,plan_path,ledger_path,script,argv,plan,ledger


def invoke_registered(f):
    frozen,p,l,s,argv,plan,ledger=f
    return q.registered_task(p,l,'q',s,'101',{},argv,frozen.manifest,digest(frozen.manifest))


def test_registered_intent_binds_own_helper_manifest_exact_argv_and_actual_files(registered):
    value=invoke_registered(registered)
    assert value['native_job_id']=='101' and value['script']['sha256']==digest(registered[3])


@pytest.mark.parametrize('change',['helper_sha','manifest_sha','argv','job','source','script','resources','duplicate_task'])
def test_native_registration_tamper_or_ambiguous_intent_is_refused(registered,change):
    frozen,p,l,s,argv,plan,ledger=registered;t=plan['tasks'][0]
    if change=='helper_sha':t['rolling_census_qualification']['helper']['sha256']='0'*64
    elif change=='manifest_sha':t['rolling_census_qualification']['source_manifest']['sha256']='0'*64
    elif change=='argv':t['command']+=' --ignored true'
    elif change=='job':ledger['tasks']['q']['job_id']='102'
    elif change=='source':plan['release_identity']['sha256']='0'*64
    elif change=='script':s.write_text('changed')
    elif change=='resources':t['resources']['cpus']=4
    elif change=='duplicate_task':plan['tasks'].append(copy.deepcopy(t))
    p.write_text(json.dumps(plan));l.write_text(json.dumps(ledger))
    with pytest.raises(ValueError):invoke_registered(registered)


def junit(path,*,names=('test_positive',),failure=None,summary=None):
    root=ET.Element('testsuites');suite=ET.SubElement(root,'testsuite',tests=str(len(names)),failures='0',errors='0',skipped='0')
    if summary:suite.set(*summary)
    for name in names:
        case=ET.SubElement(suite,'testcase',classname='test_fixture',name=name)
        if failure:ET.SubElement(case,failure)
    path.write_bytes(ET.tostring(root));return canonical_hash(sorted('test_fixture::'+name for name in names))


def test_junit_requires_actual_distinct_full_passing_roster(tmp_path):
    p=tmp_path/'positive.xml';roster=junit(p)
    assert q.junit_roster(p,1,roster)['sha256']==digest(p)


@pytest.mark.parametrize('change',['failure','error','skipped','duplicate','roster','summary','doctype'])
def test_forged_partial_skipped_or_different_junit_is_refused(tmp_path,change):
    p=tmp_path/'q.xml';roster=junit(p)
    if change in ('failure','error','skipped'):junit(p,failure=change)
    elif change=='duplicate':junit(p,names=('test_positive','test_positive'))
    elif change=='roster':roster='0'*64
    elif change=='summary':junit(p,summary=('tests','2'))
    elif change=='doctype':p.write_bytes(b'<!DOCTYPE x>'+p.read_bytes())
    with pytest.raises(ValueError):q.junit_roster(p,1,roster)


def test_test_environment_discards_user_pytest_plugins_and_options(monkeypatch):
    monkeypatch.setenv('PYTEST_ADDOPTS','--ignore=tests');monkeypatch.setenv('PYTEST_PLUGINS','foreign')
    inherited=['native-module-dependencies','another-native-library']
    monkeypatch.setenv('PYTHONPATH',q.os.pathsep.join(inherited))
    env=q.pytest_environment()
    assert 'PYTEST_ADDOPTS' not in env and 'PYTEST_PLUGINS' not in env and env['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1'
    assert env['PYTHONPATH'].split(q.os.pathsep)==[str(q.FRAMEWORK/'src'),*inherited]


def test_actual_suite_command_and_junit_are_bound_with_all_fixtures_local(frozen,tmp_path):
    contract=frozen.value['suites']['kernel'];workspace=tmp_path/'allocation-local';workspace.mkdir();seen=[]
    def runner(command,**kw):
        seen.append((command,kw));junit(Path(command[command.index('--junitxml')+1]))
        assert kw['timeout']==820 and kw['cwd']==frozen.framework
        return subprocess.CompletedProcess(command,0)
    result=q.run_suite('kernel',contract,workspace,2000,runner=runner,wall=lambda:1000)
    assert result['test_counts']=={'tests':1,'failures':0,'errors':0,'skipped':0}
    command,kw=seen[0];assert command[command.index('--basetemp')+1]==str(workspace/'kernel')
    assert '-s' in command and command[command.index('-p')+1]=='no:cacheprovider'


def test_nonzero_native_suite_exit_never_returns_a_positive_receipt(frozen,tmp_path):
    with pytest.raises(ValueError,match='suite failed'):
        q.run_suite('kernel',frozen.value['suites']['kernel'],tmp_path,2000,
            runner=lambda *a,**k:subprocess.CompletedProcess(a[0],1),wall=lambda:1000)


def test_native_canary_checks_complete_exact_paths_and_both_classifications(tmp_path,monkeypatch):
    monkeypatch.setattr(q.os,'geteuid',lambda:3164768,raising=False)
    tree=tmp_path/'canary';sizes={'small':1};calls=[]
    rows={str(tree/'small'):{'status':'measured','bytes':1},str(tree/'unfollowed-link'):{'status':'symlink'},str(tree/'unopened-fifo'):{'status':'special'}}
    def validate(row,**kwargs):calls.append(kwargs);return row['bytes']
    q.validate_canary_rows(rows,copy.deepcopy(rows),tree,sizes,SimpleNamespace(validate_regular_length_measurement=validate))
    assert len(calls)==1 and calls[0]['path']==str(tree/'small')


@pytest.mark.parametrize('change',['missing','invented','wrong_bytes','different_transcript','classification'])
def test_native_canary_partial_or_invented_measurements_are_refused(tmp_path,change,monkeypatch):
    monkeypatch.setattr(q.os,'geteuid',lambda:3164768,raising=False)
    tree=tmp_path/'canary';sizes={'small':1}
    rows={str(tree/'small'):{'status':'measured','bytes':1},str(tree/'unfollowed-link'):{'status':'symlink'},str(tree/'unopened-fifo'):{'status':'special'}}
    other=copy.deepcopy(rows)
    if change=='missing':rows.pop(str(tree/'small'));other=copy.deepcopy(rows)
    elif change=='invented':rows[str(tree/'foreign')]={'status':'measured','bytes':1};other=copy.deepcopy(rows)
    elif change=='wrong_bytes':rows[str(tree/'small')]['bytes']=2;other=copy.deepcopy(rows)
    elif change=='different_transcript':other[str(tree/'small')]['bytes']=2
    elif change=='classification':rows[str(tree/'unopened-fifo')]['status']='symlink';other=copy.deepcopy(rows)
    with pytest.raises(ValueError):q.validate_canary_rows(rows,other,tree,sizes,
        SimpleNamespace(validate_regular_length_measurement=lambda row,**kw:row['bytes']))


def test_producer_has_no_fresh_census_prerequisite_or_scheduler_mutation():
    import ast,inspect
    source=inspect.getsource(q.main);tree=ast.parse(source)
    calls=[node.func.id for node in ast.walk(tree) if isinstance(node,ast.Call) and isinstance(node.func,ast.Name)]
    assert 'require_fresh_census' not in calls and '_storage' not in calls
    assert all(token not in source for token in ("'sbatch'","'scancel'","'scontrol'"))
    assert 'native_canary' in calls and 'run_suite' in calls and 'verify_sources' in calls


@pytest.fixture
def producer(frozen,monkeypatch,tmp_path):
    import horsehair
    from horsehair import campaign,repair
    frozen.case.rmdir();calls=[];locked=[False];config={'synthetic_operational_fixture':True}
    primary=tmp_path/'primary';(primary/'configs').mkdir(parents=True)
    (primary/'configs/study.json').write_text(json.dumps(config));(primary/'release.json').write_text('{"release_id":"fixture"}')
    (frozen.framework/'configs').mkdir();(frozen.framework/'configs/study.json').write_text(json.dumps(config))
    public=frozen.support/'public.json';public.write_text('{"slurm_job_id":"11"}')
    monkeypatch.setattr(q,'PRIMARY',primary);monkeypatch.setattr(q,'CONFIG',canonical_hash(config))
    monkeypatch.setattr(horsehair,'__file__',str(frozen.framework/'src/horsehair/__init__.py'))
    monkeypatch.setattr(q,'source_manifest',lambda *a:(frozen.value,{'pinned':'fixture'}))
    monkeypatch.setattr(q,'verify_sources',lambda pins:calls.append('verify-sources'))
    monkeypatch.setattr(q,'native_job_id',lambda:'101');monkeypatch.setattr(q.os,'geteuid',lambda:3164768,raising=False)
    for key,value in {'USER':'pwa209','SLURM_CPUS_PER_TASK':'8','HHW_GPU_COUNT':'0','HHW_MAX_HOURS':'1'}.items():monkeypatch.setenv(key,value)
    monkeypatch.setattr(q.subprocess,'check_output',lambda *a,**k:'fixture.narval.calcul.quebec\n')
    registration={'plan':{'path':'fixture','sha256':'a'*64},'ledger':{'path':'fixture','sha256':'b'*64},
        'script':{'path':'fixture','sha256':'c'*64},'task_name':'q','native_job_id':'101'}
    monkeypatch.setattr(q,'registered_task',lambda *a:registration)
    monkeypatch.setattr(q,'native_allocation',lambda *a:({'fields':{'NumCPUs':'8'}},q.time.time()+3600))
    def observed(*a,**k):calls.append('actual-lineage-native-read');return {'11':{},'12':{}}
    monkeypatch.setattr(repair,'_repair_native_states',observed);monkeypatch.setattr(campaign,'_repair_native_states',observed)
    @contextmanager
    def lock(*a,**k):
        assert not locked[0];locked[0]=True;calls.append('lock')
        try:yield
        finally:locked[0]=False;calls.append('unlock')
    def admission(root,config,rid,case,label,**kw):
        assert locked[0] and kw['cpus']==kw['gpus']==0;calls.append('actual-combined-admission')
        (case/(label+'.json')).write_text('{"status":"admitted"}');return {'status':'admitted'}
    monkeypatch.setattr(campaign,'combined_admission',admission)
    @contextmanager
    def lease(*a,**kw):
        assert kw['cpus']==8 and kw['gpus']==0 and kw['max_hours']==1;calls.append('lease');yield
    monkeypatch.setattr(q,'resource_lease',lease)
    def proof(*a):calls.append('positive-lineage-proof');return {'positive':True}
    baseline={'registry_qualification':lambda *a:{'job_id':'12'},'REGISTRY_Q':'private','REGISTRY_Q_SHA':'h','REGISTRY_Q_JOB':'12',
        'QUALIFICATION':public,'QUALIFICATION_SHA':'h','qualification_proof':proof,'completed_registry_qualification':proof}
    monkeypatch.setattr(q,'baseline_library',lambda:baseline)
    def load(path,**kw):
        if path==str(q.SCHEDULER):return {'run_scontrol_job':None}
        if path==str(q.NATIVE_GUARD):return {'install':lambda *a:calls.append('358-install')}
        if path==str(q.SUPPORT/q.GUARD_NAME):return {'install':lambda *a,**k:calls.append('900-install'),'registration_lock':lock}
        if path==str(q.QUOTA):return {'require_storage_headroom_with_wait':lambda *a,**k:{'actual_quota_fixture':True}}
        return {}
    monkeypatch.setattr(q.runpy,'run_path',load)
    policy=SimpleNamespace(receipt=lambda:{'policy':'fixture'});monkeypatch.setattr(q,'load_resource_policy',lambda *a,**k:policy)
    workspace=tmp_path/'allocation-local';workspace.mkdir();monkeypatch.setattr(q,'local_workspace',lambda *a:workspace)
    def suite(scope,contract,*a):calls.append(scope);return {'scope':scope,'test_counts':{'tests':1,'failures':0,'errors':0,'skipped':0}}
    monkeypatch.setattr(q,'run_suite',suite)
    def canary(*a,**k):calls.append('native-syscall-canary');return {'primitive_identity':{},'primitive_contract':{'original':True}}
    monkeypatch.setattr(q,'native_canary',canary)
    monkeypatch.setattr(q,'dependency_probe',lambda:{'native_dependency_fixture':True})
    argv=['--native-plan','fixture','--native-ledger','fixture','--native-task','q','--native-script','fixture',
        '--source-manifest',str(frozen.manifest),'--source-manifest-sha256',digest(frozen.manifest)]
    return frozen,calls,argv


def test_complete_producer_orders_real_admission_all_thirteen_suites_canary_and_final_source_check(producer):
    frozen,calls,argv=producer;q.main(argv)
    value=json.loads((frozen.case/'qualification.json').read_text())
    assert value['status']=='complete_rolling_census_operational_qualification'
    assert [s['scope'] for s in value['suites']]==list(q.SUITE_SOURCES)
    assert calls.index('lock')<calls.index('actual-combined-admission')<calls.index('unlock')
    assert calls.index('actual-combined-admission')<calls.index('actual-lineage-native-read')<calls.index('baseline_budget')
    assert calls.index('external_census_policy')<calls.index('native-syscall-canary')<len(calls)-1
    assert calls[-1]=='verify-sources' and value['native_completion_required_separately'] is True
    assert value['workload']['fresh_census_prerequisite'] is False and value['workload']['actual_census_published'] is False


@pytest.mark.parametrize('gate',['suite','canary','final-source'])
def test_failed_required_gate_never_publishes_positive_qualification(producer,monkeypatch,gate):
    frozen,calls,argv=producer
    def fail(*a,**k):raise ValueError('required gate refused')
    if gate=='suite':monkeypatch.setattr(q,'run_suite',fail)
    elif gate=='canary':monkeypatch.setattr(q,'native_canary',fail)
    else:
        def sources(*a):
            if 'native-syscall-canary' in calls:fail()
        monkeypatch.setattr(q,'verify_sources',sources)
    with pytest.raises(ValueError,match='required gate'):q.main(argv)
    assert not (frozen.case/'qualification.json').exists()


def test_native_dependency_probe_uses_exact_pytest_environment_and_qualified_horsehair(frozen,monkeypatch):
    init=frozen.framework/'src/horsehair/__init__.py';init.write_text('qualified package fixture')
    pytest_source=frozen.root/'pytest.py';pytest_source.write_text('pytest fixture')
    pygments_source=frozen.root/'pygments.py';pygments_source.write_text('pygments fixture')
    monkeypatch.setenv('PYTHONPATH','native-cvmfs-module-dependencies')
    raw={'executable':q.sys.executable,'sys_path':[str(frozen.framework/'src'),'native-cvmfs-module-dependencies'],
        'modules':{'horsehair':str(init),'pytest':str(pytest_source),'pygments':str(pygments_source)}}
    def runner(command,**kwargs):
        assert command[:3]==[q.sys.executable,'-s','-c'] and kwargs['timeout']==30
        assert kwargs['env']['PYTHONPATH'].split(q.os.pathsep)==[str(frozen.framework/'src'),'native-cvmfs-module-dependencies']
        return subprocess.CompletedProcess(command,0,json.dumps(raw),'')
    proof=q.dependency_probe(runner=runner)
    assert proof['module_source_sha256']['pygments']==digest(pygments_source)
    assert proof['inherited_dependency_paths_preserved'] is True


@pytest.mark.parametrize('change',['import_error','mixed_horsehair','missing_module','nonfinite','stderr','oversized'])
def test_failed_or_mixed_native_python_imports_never_authorize_tests(frozen,change):
    raw={'executable':q.sys.executable,'sys_path':[],
        'modules':{'horsehair':str(frozen.framework/'src/horsehair/__init__.py'),'pytest':'pytest','pygments':'pygments'}}
    code=0;stderr='';stdout=json.dumps(raw)
    if change=='import_error':code=1;stderr='ModuleNotFoundError: pygments'
    elif change=='mixed_horsehair':raw['modules']['horsehair']=str(frozen.root/'unqualified/__init__.py');stdout=json.dumps(raw)
    elif change=='missing_module':raw['modules'].pop('pygments');stdout=json.dumps(raw)
    elif change=='nonfinite':stdout='{"bad":NaN}'
    elif change=='stderr':stderr='unexpected module environment warning'
    elif change=='oversized':stdout=' '*100_001
    with pytest.raises(ValueError):q.dependency_probe(runner=lambda command,**kw:subprocess.CompletedProcess(command,code,stdout,stderr))


def test_dependency_path_or_source_change_prevents_positive_publication(producer,monkeypatch):
    frozen,calls,argv=producer;count=[0]
    def probe():count[0]+=1;return {'native_dependency_fixture':count[0]}
    monkeypatch.setattr(q,'dependency_probe',probe)
    with pytest.raises(ValueError,match='dependency paths/source bytes changed'):q.main(argv)
    assert not (frozen.case/'qualification.json').exists()


def test_q2_runner_calls_its_own_preserving_environment_and_never_old_q1_runner():
    assert q.run_suite.__globals__['pytest_environment'] is q.pytest_environment
    assert q.HELPER=='qualify-streaming-census-v2-20261023.py'
    assert len(q.PRIVATE_FILES)==14 and len(q.SUITE_SOURCES)==13
