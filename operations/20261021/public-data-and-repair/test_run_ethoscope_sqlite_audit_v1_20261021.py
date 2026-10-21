"""No remote calls: exact profile allocation, qualification and census contracts."""
from datetime import datetime,timezone,timedelta
from pathlib import Path
import importlib.util
import subprocess
import time
from types import SimpleNamespace
import xml.etree.ElementTree as ET

import pytest

from horsehair import agents
from horsehair.core import atomic_json,canonical_hash,digest,read_json

spec=importlib.util.spec_from_file_location('ethoscope_sqlite_wrapper',Path(__file__).with_name('run-ethoscope-sqlite-audit-v1-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


@pytest.fixture
def allocation(tmp_path,monkeypatch):
    root=tmp_path/'study';directory=root/'plans/recovery-support-20261021-240';directory.mkdir(parents=True)
    (root/'state').mkdir();support=root/'maintenance/support';support.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',support)
    script=directory/'recovery-240.sbatch';script.write_text('source-bound exact script')
    task={'name':'recovery-240','script':str(script),'script_sha256':canonical_hash(script.read_text()),
        'resources':{'cpus':1,'memory':'16G','hours':1,'gpu':False,'array':None}}
    plan={'tasks':[task],'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE}}
    atomic_json(directory/'plan.json',plan)
    ledger={'source_sha256':m.SOURCE,'plan_sha256':canonical_hash(plan),
        'tasks':{'recovery-240':{'job_id':'240','state':'submitted','script_sha256':task['script_sha256']}}}
    atomic_json(root/'state/recovery-support-20261021-240-submission.json',ledger)
    end=(datetime.now(timezone.utc)+timedelta(hours=1)).replace(microsecond=0)
    monkeypatch.setenv('SLURM_JOB_END_TIME',str(end.timestamp()))
    fields={'JobId':'240','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'NumCPUs':'1','NumNodes':'1-1','TimeLimit':'01:00:00','MinMemoryNode':'16384M',
        'Command':str(script),'JobName':'hhw-recovery-240','AllocTRES':'cpu=1,mem=16G',
        'ReqTRES':'cpu=1,mem=16G','EndTime':end.strftime('%Y-%m-%dT%H:%M:%S')}
    # The native personal namespace contains no spaces. Local Windows fixture
    # roots do; inject only their parsed native fields, retaining real plan I/O.
    original_findall=m.re.findall
    monkeypatch.setattr(m.re,'findall',lambda pattern,text:list(fields.items())
        if text=='owned-native-fixture' else original_findall(pattern,text))
    def reader(job,evidence):
        evidence.mkdir(exist_ok=False)
        return subprocess.CompletedProcess([],0,'owned-native-fixture','')
    return root,script,fields,reader,plan,ledger


def test_actual_single_cpu16gb_onehour_source_bound_allocation(allocation):
    root,script,fields,reader,*_=allocation
    result=m.native_allocation('240',script,reader)
    assert result['fields']==fields and result['script_sha256']==digest(script)
    assert result['plan_sha256']==canonical_hash(read_json(script.parent/'plan.json'))


@pytest.mark.parametrize('field,value',[('UserId','other(1)'),('NumCPUs','4'),('NumNodes','1-2'),
    ('MinMemoryNode','32G'),('TimeLimit','03:00:00'),('AllocTRES','cpu=1,gres/gpu=1'),
    ('Account','other_cpu'),('JobState','PENDING'),('Command','/elsewhere/script'),('JobName','other')])
def test_changed_native_allocation_is_refused_before_profile(allocation,field,value):
    root,script,fields,reader,*_=allocation;fields[field]=value
    with pytest.raises(ValueError):m.native_allocation('240',script,reader)


@pytest.mark.parametrize('changed',['script','ledger','release','resources'])
def test_native_script_plan_ledger_must_remain_bound(allocation,changed):
    root,script,fields,reader,plan,ledger=allocation
    if changed=='script':script.write_text('different command')
    elif changed=='ledger':ledger['tasks']['recovery-240']['job_id']='241'
    elif changed=='release':plan['release_identity']['sha256']='b'*64
    else:plan['tasks'][0]['resources']['hours']=2
    if changed!='script':
        atomic_json(script.parent/'plan.json',plan)
        ledger['plan_sha256']=canonical_hash(plan)
        atomic_json(root/'state/recovery-support-20261021-240-submission.json',ledger)
    with pytest.raises(ValueError):m.native_allocation('240',script,reader)


@pytest.fixture
def qualified(tmp_path,monkeypatch):
    root=tmp_path/'study';evidence=root/'state/qualification-evidence/239-attempt';evidence.mkdir(parents=True)
    path=root/'maintenance/support/qualification.json';path.parent.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'QUALIFICATION',path)
    junit=evidence/'junit.xml'
    junit.write_text('<testsuite tests="1694" failures="0" errors="0" skipped="0">'+
        ''.join('<testcase name="case-'+str(i)+'"/>' for i in range(1694))+'</testsuite>')
    value={'status':'complete','returncode':0,'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,
        'source_verified_unchanged':True,'slurm_job_id':'239',
        'test_counts':{'tests':1694,'failures':0,'errors':0,'skipped':0},
        'artifacts':{'junit':{'path':str(junit),'bytes':junit.stat().st_size,'sha256':digest(junit)}}}
    atomic_json(path,value)
    observed={'239':{'state':'COMPLETED','exit_code':'0:0','user':'pwa209','account':'def-ptewarie_cpu',
        'job_id_raw':'239','unallocated_metadata':False,'cpus':4,'elapsed_seconds':1200,'alloc_tres':'cpu=4,mem=16G'}}
    monkeypatch.setattr(m,'completed_native_dependency',lambda root,job:{'job_id':job,'positive':'mock'})
    return path,junit,value,observed


def test_positive_actual1694_native_qualification_and_raw_junit_required(qualified):
    path,junit,value,observed=qualified
    proof=m.qualification_proof(path,digest(path),'239',observed)
    assert proof['junit']['sha256']==digest(junit) and proof['native_observed']['cpus']==4


@pytest.mark.parametrize('change',['old_count','source','config','native_pending','native_failed',
    'wrong_owner','physical_alias','unallocated','gpu','too_long','missing_final','junit_bytes'])
def test_invalid_qualification_never_allows_profile(qualified,monkeypatch,change):
    path,junit,value,observed=qualified;record=observed['239']
    if change=='old_count':value['test_counts']['tests']=809
    elif change=='source':value['source_sha256']='b'*64
    elif change=='config':value['config_sha256']='b'*64
    elif change=='native_pending':record['state']='RUNNING'
    elif change=='native_failed':record['exit_code']='1:0'
    elif change=='wrong_owner':record['user']='other'
    elif change=='physical_alias':record['job_id_raw']='239_1'
    elif change=='unallocated':record['unallocated_metadata']=True
    elif change=='gpu':record['alloc_tres']='cpu=4,gres/gpu=1'
    elif change=='too_long':record['elapsed_seconds']=3601
    elif change=='missing_final':monkeypatch.setattr(m,'completed_native_dependency',lambda *args:None)
    else:junit.write_text('corrupted')
    atomic_json(path,value)
    with pytest.raises(ValueError):m.qualification_proof(path,digest(path),'239',observed)


def test_storage_requires_existing_sixhour_census_and_never_scans(tmp_path,monkeypatch):
    from horsehair import admission
    root=tmp_path/'study';(root/'state').mkdir(parents=True);support=root/'maintenance/support';support.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',support)
    calls=[]
    monkeypatch.setattr(admission,'_storage',lambda *a,**k:calls.append((a,k)) or (1000,2100,[]))
    with pytest.raises(m.PendingStorage,match='pending'):m.storage_proof({}, {})
    path=root/'state/storage-budget.json'
    atomic_json(path,{'captured_utc':(datetime.now(timezone.utc)-timedelta(hours=7)).isoformat()})
    with pytest.raises(m.PendingStorage,match='six-hour'):m.storage_proof({}, {})
    assert not calls
    atomic_json(path,{'captured_utc':datetime.now(timezone.utc).isoformat()})
    proof=m.storage_proof({'frozen':True},{'fresh_quota':True})
    assert calls[0][0][2:]==(10_000_000_000,2000)
    assert proof['forecast_additional_files']==2000 and (support/'study-census-snapshot.json').exists()


def write_junit(path,kind,change=None):
    names=m.TEST_NAMES[kind];count=len(names)
    root=ET.Element('testsuites');suite=ET.SubElement(root,'testsuite',tests=str(count),failures='0',errors='0',skipped='0')
    module={'schema':'test_audit_ethoscope_sqlite_schema_20261021','range':'test_acquire_ethoscope_db_canary_20261021'}[kind]
    for name in names:ET.SubElement(suite,'testcase',name=name,classname='work.'+module)
    cases=suite.findall('testcase')
    if change=='missing':suite.remove(cases[-1])
    elif change=='duplicate':cases[-1].set('name',cases[0].get('name'))
    elif change in {'failure','skipped','error'}:ET.SubElement(cases[-1],change)
    elif change=='wrong_class':cases[-1].set('classname','unrelated')
    elif change=='count_only':
        for case in cases:suite.remove(case)
    ET.ElementTree(root).write(path)


@pytest.mark.parametrize('kind',['schema','range'])
def test_every_exact52_and39_case_required_separately(tmp_path,kind):
    path=tmp_path/'junit.xml';write_junit(path,kind)
    assert m.verify_private_junit(path,kind)==canonical_hash(sorted(m.TEST_NAMES[kind]))


@pytest.mark.parametrize('kind',['schema','range'])
@pytest.mark.parametrize('change',['missing','duplicate','failure','skipped','error','wrong_class','count_only'])
def test_selected_skipped_or_unbound_private_tests_refused(tmp_path,kind,change):
    path=tmp_path/'junit.xml';write_junit(path,kind,change)
    with pytest.raises(ValueError):m.verify_private_junit(path,kind)


def private_fixture(tmp_path,monkeypatch):
    support=tmp_path/'support';support.mkdir();local=tmp_path/'local';local.mkdir()
    monkeypatch.setattr(m,'SUPPORT',support);monkeypatch.setenv('SLURM_TMPDIR',str(local))
    monkeypatch.setenv('PYTEST_ADDOPTS','-k selected');monkeypatch.setenv('PYTEST_PLUGINS','foreign_plugin')
    original_stat=Path.stat
    def stat(path,*args,**kwargs):
        actual=original_stat(path,*args,**kwargs)
        return SimpleNamespace(st_uid=3164768,st_mode=actual.st_mode) if path==local else actual
    monkeypatch.setattr(Path,'stat',stat)
    monkeypatch.setattr(m.shutil,'disk_usage',lambda path:SimpleNamespace(free=3_000_000_000))
    monkeypatch.setattr(m.os,'statvfs',lambda path:SimpleNamespace(f_favail=30000),raising=False)
    paths=[]
    for name,pin in [('auditor.py','AUDITOR_SHA'),('test_source.py','TEST_SHA'),('range.py','RANGE_SHA'),('test_range.py','RANGE_TEST_SHA')]:
        p=tmp_path/name;p.write_text('pinned mock source '+name);paths.append(p);monkeypatch.setattr(m,pin,digest(p))
    return support,local,paths


def test_both_private_qualifications_use_node_tmp_clear_selectors_and_remain_separate(tmp_path,monkeypatch):
    support,local,paths=private_fixture(tmp_path,monkeypatch);calls=[]
    def runner(command,**kwargs):
        kind='schema' if len(calls)==0 else 'range';calls.append((command,kwargs))
        write_junit(support/(kind+'-tests-junit.xml'),kind)
        return subprocess.CompletedProcess(command,0)
    result=m.qualify_private(*paths,'248',time.time()+3600,runner=runner)
    assert list(result)==['schema','range'] and len(calls)==2
    assert result['schema']['value']['test_counts']=={'tests':52,'failures':0,'errors':0,'skipped':0}
    assert result['range']['value']['test_counts']=={'tests':39,'failures':0,'errors':0,'skipped':0}
    for command,kwargs in calls:
        assert command[command.index('--basetemp')+1].startswith(str(local))
        assert 'PYTEST_ADDOPTS' not in kwargs['env'] and 'PYTEST_PLUGINS' not in kwargs['env']
        assert kwargs['env']['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1'
        assert kwargs['timeout']<=300
    assert calls[0][0][calls[0][0].index('--basetemp')+1]!=calls[1][0][calls[1][0].index('--basetemp')+1]
    assert result['schema']['value']['actual_provider_database_schema_inspected'] is False
    assert result['range']['value']['actual_provider_database_schema_inspected'] is False


@pytest.mark.parametrize('failure',['schema_return','range_return','schema_skip','range_skip','source_change'])
def test_private_failure_never_authorizes_actual_db_read(tmp_path,monkeypatch,failure):
    support,local,paths=private_fixture(tmp_path,monkeypatch);calls=[]
    def runner(command,**kwargs):
        kind='schema' if len(calls)==0 else 'range';calls.append(kind)
        write_junit(support/(kind+'-tests-junit.xml'),kind,'skipped' if failure==kind+'_skip' else None)
        if failure=='source_change':paths[0].write_text('changed after native suite')
        return subprocess.CompletedProcess(command,1 if failure==kind+'_return' else 0)
    with pytest.raises(ValueError):m.qualify_private(*paths,'248',time.time()+3600,runner=runner)
    assert not (support/'source-audit-intent.json').exists()
    if failure.startswith('schema') or failure=='source_change':assert calls==['schema']


def source_fixture(tmp_path,monkeypatch):
    support=tmp_path/'support';support.mkdir();case=tmp_path/'actual-audit'
    auditor=tmp_path/'auditor.py';auditor.write_text('pinned structural code')
    acquisition=tmp_path/'acquisition-result.json';acquisition.write_text('{}')
    raw=tmp_path/'actual.db';raw.write_bytes(b'NO REAL PROVIDER DATA; synthetic wrapper fixture')
    inputs={}
    for role in ('record','checksum','index','documentation'):
        p=tmp_path/(role+'.fixture');p.write_text('synthetic '+role);inputs[role]=(p,digest(p))
    for key,value in [('SUPPORT',support),('CASE',case),('ACQUISITION',acquisition),('ACQUISITION_SHA',digest(acquisition)),
                      ('RAW_DB',raw),('RAW_SHA',digest(raw)),('INPUTS',inputs),('AUDITOR_SHA',digest(auditor))]:monkeypatch.setattr(m,key,value)
    return support,case,auditor,acquisition,raw,inputs


def actual_receipt(case,auditor,acquisition,raw,inputs):
    inventory={'outcome_values_read':False,'provider_SQL_or_code_executed':False,'clock_units_qualified':False,'expected_grid_missingness_qualified':False}
    atomic_json(case/'schema-and-clock-inventory.json',inventory)
    return {'status':'complete_read_only_structural_inventory','auditor_sha256':digest(auditor),
        'acquisition':{'path':str(acquisition),'sha256':digest(acquisition)},
        'database':{'path':str(raw),'bytes':13594624,'md5':'e261acc31375cb122164c92ed957206e','sha256':digest(raw)},
        'source_pins':[{'role':key,'path':str(p),'sha256':sha} for key,(p,sha) in inputs.items()],
        'source_animal_metadata_rows':[{'region_id':str(i),'date':'2022-04-22','machine_name':'ETHOSCOPE_155'} for i in range(1,21)],
        'all_raw_and_documentation_identities_reverified':True,'unknown_schema_does_not_clear_a_scientific_gate':True,
        'source_metadata_exposure_timestamp_present':False,
        'inventory':{'path':str(case/'schema-and-clock-inventory.json'),'sha256':digest(case/'schema-and-clock-inventory.json')},
        **{key:False for key in m.AUDIT_FALSE_FLAGS}}


@pytest.mark.parametrize('change',[None,'returncode','raw_sha','acquisition','source_pin','source_roi','exposure','H1','XY','inventory','outcomes','clock_grid'])
def test_actual_source_cli_bound_to_a696_raw_bytes_and_false_unknowns(tmp_path,monkeypatch,change):
    support,case,auditor,acquisition,raw,inputs=source_fixture(tmp_path,monkeypatch);calls=[]
    def runner(command,**kwargs):
        calls.append((command,kwargs));case.mkdir();value=actual_receipt(case,auditor,acquisition,raw,inputs)
        if change=='raw_sha':value['database']['sha256']='b'*64
        elif change=='acquisition':value['acquisition']['sha256']='b'*64
        elif change=='source_pin':value['source_pins'][0]['sha256']='b'*64
        elif change=='source_roi':value['source_animal_metadata_rows'].pop()
        elif change=='exposure':value['source_metadata_exposure_timestamp_present']=True
        elif change=='H1':value['h1_cleared']=True
        elif change=='XY':value['physical_2d_units_qualified']=True
        elif change=='inventory':value['inventory']['sha256']='b'*64
        elif change in {'outcomes','clock_grid'}:
            p=case/'schema-and-clock-inventory.json';detail=read_json(p)
            detail['outcome_values_read' if change=='outcomes' else 'expected_grid_missingness_qualified']=True
            atomic_json(p,detail);value['inventory']['sha256']=digest(p)
        atomic_json(case/'receipt.json',value)
        return subprocess.CompletedProcess(command,1 if change=='returncode' else 0)
    if change is None:
        result=m.run_source_audit(auditor,time.time()+3600,runner=runner);assert result['scientific_result'] is False
    else:
        with pytest.raises(ValueError):m.run_source_audit(auditor,time.time()+3600,runner=runner)
    command,kwargs=calls[0]
    assert command[-2:]==['--max-seconds','900'] and kwargs['timeout']<=1080
    assert command[command.index('--acquisition')+1]==str(acquisition)
    assert all(command[command.index('--'+role)+1]==str(p) for role,(p,sha) in inputs.items())


def test_original_qualified_framework_native_guards_are_copied_exactly_from_approved_DAM_wrapper():
    import ast
    parent=Path(__file__).with_name('run-dam-structural-audit-v1-20261021.py').read_text()
    current=Path(m.__file__).read_text()
    originals={n.name:ast.dump(n,include_attributes=False) for n in ast.parse(parent).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
    actual={n.name:ast.dump(n,include_attributes=False) for n in ast.parse(current).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
    for name in ('native_allocation','qualification_proof','storage_proof'):
        assert actual[name]==originals[name]
