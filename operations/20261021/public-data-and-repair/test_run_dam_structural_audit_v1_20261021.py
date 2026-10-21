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

spec=importlib.util.spec_from_file_location('dam_structural_wrapper',Path(__file__).with_name('run-dam-structural-audit-v1-20261021.py'))
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


def write_junit(path,change=None):
    root=ET.Element('testsuites');suite=ET.SubElement(root,'testsuite',tests='37',failures='0',errors='0',skipped='0')
    for name in m.TEST_NAMES:ET.SubElement(suite,'testcase',name=name,classname='work.test_audit_af_dam_secondary_v1_20261021')
    cases=suite.findall('testcase')
    if change=='missing':suite.remove(cases[-1])
    elif change=='duplicate':cases[-1].set('name',cases[0].get('name'))
    elif change in {'failure','skipped'}:ET.SubElement(cases[-1],change)
    elif change=='wrong_class':cases[-1].set('classname','unrelated')
    elif change=='count_only':
        for case in cases:suite.remove(case)
    ET.ElementTree(root).write(path)


def test_every_exact37_case_required(tmp_path):
    path=tmp_path/'junit.xml';write_junit(path)
    assert m.verify_private_junit(path)==canonical_hash(sorted(m.TEST_NAMES))


@pytest.mark.parametrize('change',['missing','duplicate','failure','skipped','wrong_class','count_only'])
def test_selected_or_unbound_private_tests_refused(tmp_path,change):
    path=tmp_path/'junit.xml';write_junit(path,change)
    with pytest.raises(ValueError):m.verify_private_junit(path)


def test_private_qualification_uses_node_tmp_and_clears_external_selectors(tmp_path,monkeypatch):
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
    auditor=tmp_path/'auditor.py';auditor.write_text('pinned fixture')
    tests=tmp_path/'test_source.py';tests.write_text('pinned tests')
    monkeypatch.setattr(m,'AUDITOR_SHA',digest(auditor));monkeypatch.setattr(m,'TEST_SHA',digest(tests))
    calls=[]
    def runner(command,**kwargs):
        calls.append((command,kwargs));write_junit(support/'private-tests-junit.xml')
        return subprocess.CompletedProcess(command,0)
    result=m.qualify_private(auditor,tests,'243',time.time()+3600,runner=runner)
    command,kwargs=calls[0]
    assert command[command.index('--basetemp')+1].startswith(str(local))
    assert 'PYTEST_ADDOPTS' not in kwargs['env'] and 'PYTEST_PLUGINS' not in kwargs['env']
    assert result['actual_provider_source_audit_performed'] is False
    assert result['test_counts']['tests']==37 and not m.CASE.exists()


@pytest.mark.parametrize('change',[None,'effects','gates','manifest','files','returncode'])
def test_actual_source_cli_is_bound_and_never_admits_effects(tmp_path,monkeypatch,change):
    support=tmp_path/'support';support.mkdir();case=tmp_path/'actual-audit'
    manifest=tmp_path/'manifest.json';manifest.write_text('{}')
    auditor=tmp_path/'auditor.py';auditor.write_text('pinned structural code')
    for key,value in [('SUPPORT',support),('CASE',case),('MANIFEST',manifest),('MANIFEST_SHA',digest(manifest)),('AUDITOR_SHA',digest(auditor))]:
        monkeypatch.setattr(m,key,value)
    calls=[]
    def runner(command,**kwargs):
        calls.append((command,kwargs));case.mkdir()
        value={'status':'complete_read_only_structural_audit','files_audited':6,'record_id':'17175344',
            'manifest':{'path':str(manifest),'sha256':digest(manifest)},'auditor_sha256':digest(auditor),
            'all_provider_identities_reverified':True,'original_H1_H2_H3_gates_cleared':False,
            'analysis_admitted':False,'complete_declared_study':False,**{key:False for key in m.FALSE_FLAGS}}
        if change=='effects':value['effects_calculated']=True
        elif change=='gates':value['original_H1_H2_H3_gates_cleared']=True
        elif change=='manifest':value['manifest']['sha256']='b'*64
        elif change=='files':value['files_audited']=5
        atomic_json(case/'receipt.json',value)
        return subprocess.CompletedProcess(command,1 if change=='returncode' else 0)
    if change is None:
        result=m.run_source_audit(auditor,time.time()+3600,runner=runner)
        assert result['scientific_result'] is False
    else:
        with pytest.raises(ValueError):m.run_source_audit(auditor,time.time()+3600,runner=runner)
    assert calls[0][0][-2:]==['--max-seconds','1800']
    assert calls[0][1]['timeout']<=1980

