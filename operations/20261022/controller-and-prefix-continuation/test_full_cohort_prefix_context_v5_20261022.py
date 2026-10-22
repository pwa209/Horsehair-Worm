"""No remote calls: exact profile allocation, qualification and census contracts."""
from datetime import datetime,timezone,timedelta
from pathlib import Path
import importlib.util
import ast
import json
import subprocess
from types import SimpleNamespace

import pytest

from horsehair import agents
from horsehair.core import atomic_json,canonical_hash,digest,read_json

spec=importlib.util.spec_from_file_location('prefix_context_v5',Path(__file__).with_name('run-full-cohort-prefix-context-v5-20261022.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


@pytest.fixture
def allocation(tmp_path,monkeypatch):
    root=tmp_path/'study';directory=root/'plans/recovery-support-20261021-240';directory.mkdir(parents=True)
    (root/'state').mkdir();support=root/'maintenance/support';support.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',support)
    script=directory/'recovery-240.sbatch';script.write_text('source-bound exact script')
    task={'name':'recovery-240','script':str(script),'script_sha256':canonical_hash(script.read_text()),
        'resources':{'cpus':1,'memory':'64G','hours':8,'gpu':False,'array':None}}
    plan={'tasks':[task],'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE}}
    atomic_json(directory/'plan.json',plan)
    ledger={'source_sha256':m.SOURCE,'plan_sha256':canonical_hash(plan),
        'tasks':{'recovery-240':{'job_id':'240','state':'submitted','script_sha256':task['script_sha256']}}}
    atomic_json(root/'state/recovery-support-20261021-240-submission.json',ledger)
    end=(datetime.now(timezone.utc)+timedelta(hours=8)).replace(microsecond=0)
    monkeypatch.setenv('SLURM_JOB_END_TIME',str(end.timestamp()))
    fields={'JobId':'240','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'NumCPUs':'1','NumNodes':'1-1','TimeLimit':'08:00:00','MinMemoryNode':'65536M',
        'Command':str(script),'JobName':'hhw-recovery-240','AllocTRES':'cpu=1,mem=64G',
        'ReqTRES':'cpu=1,mem=64G','EndTime':end.strftime('%Y-%m-%dT%H:%M:%S')}
    # The native personal namespace contains no spaces. Local Windows fixture
    # roots do; inject only their parsed native fields, retaining real plan I/O.
    original_findall=m.re.findall
    monkeypatch.setattr(m.re,'findall',lambda pattern,text:list(fields.items())
        if text=='owned-native-fixture' else original_findall(pattern,text))
    def reader(job,evidence):
        evidence.mkdir(exist_ok=False)
        return subprocess.CompletedProcess([],0,'owned-native-fixture','')
    return root,script,fields,reader,plan,ledger


def test_actual_single_cpu64gb_eighthour_source_bound_allocation(allocation):
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
    else:plan['tasks'][0]['resources']['hours']=1
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
    junit.write_text('<testsuite tests="1806" failures="0" errors="0" skipped="0">'+
        ''.join('<testcase name="case-'+str(i)+'"/>' for i in range(1806))+'</testsuite>')
    value={'status':'complete','returncode':0,'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,
        'source_verified_unchanged':True,'slurm_job_id':'239',
        'test_counts':{'tests':1806,'failures':0,'errors':0,'skipped':0},'resource_policy':{'sha256':m.POLICY_SHA},
        'artifacts':{'junit':{'path':str(junit),'bytes':junit.stat().st_size,'sha256':digest(junit)}}}
    atomic_json(path,value)
    observed={'239':{'state':'COMPLETED','exit_code':'0:0','user':'pwa209','account':'def-ptewarie_cpu',
        'job_id_raw':'239','unallocated_metadata':False,'cpus':4,'elapsed_seconds':1200,'alloc_tres':'cpu=4,mem=16G'}}
    monkeypatch.setattr(m,'completed_native_dependency',lambda root,job:{'job_id':job,'positive':'mock'})
    return path,junit,value,observed


def test_positive_actual1806_native_qualification_and_raw_junit_required(qualified):
    path,junit,value,observed=qualified
    proof=m.qualification_proof(path,digest(path),'239',observed)
    assert proof['junit']['sha256']==digest(junit) and proof['native_observed']['cpus']==4


@pytest.mark.parametrize('change',['old_count','source','policy','native_pending','native_failed',
    'wrong_owner','physical_alias','unallocated','gpu','too_long','missing_final','junit_bytes'])
def test_invalid_qualification_never_allows_profile(qualified,monkeypatch,change):
    path,junit,value,observed=qualified;record=observed['239']
    if change=='old_count':value['test_counts']['tests']=1694
    elif change=='source':value['source_sha256']='b'*64
    elif change=='policy':value['resource_policy']['sha256']='b'*64
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


@pytest.mark.parametrize('old',['02:00:00','2:00:00','0-02:00:00'])
def test_old_twohour_allocation_is_not_relabelled_as_eight_hours(allocation,old):
    root,script,fields,reader,*_=allocation;fields['TimeLimit']=old
    with pytest.raises(ValueError):m.native_allocation('240',script,reader)


def test_context_has_no_profile_or_numerical_launcher():
    tree=ast.parse(Path(m.__file__).read_bytes())
    assert not any(isinstance(n,ast.FunctionDef) and n.name in {'main','original_profile'} for n in tree.body)
    assert not any(isinstance(n,ast.ImportFrom) and n.module=='horsehair.cohort_profile' for n in ast.walk(tree))


@pytest.fixture
def native_reader(tmp_path,monkeypatch):
    support=tmp_path/'support';support.mkdir();monkeypatch.setattr(m,'SUPPORT',support)
    calls=[];clock=[0.];advance=[0.]
    command=['sacct','--array','-j','123','--format=JobID,State','--units=K','--noheader','--parsable2']
    def original(ids,rosters,*,runner,owner,account):
        calls.append((ids,rosters,owner,account));clock[0]+=advance[0];return runner(command,text=True,capture_output=True)
    original.__hhw_native_accounting_read_guard__={'path':str(m.GUARD),'sha256':m.GUARD_SHA}
    def bind(transport,*,end=5000.):
        return m.bounded_native_reader(original,support/'native-reads',end,transport=transport,
            wall=lambda:1000.,mono=lambda:clock[0])
    return SimpleNamespace(support=support,calls=calls,clock=clock,advance=advance,command=command,original=original,bind=bind)


def test_native_read_timeout_is_forwarded_once_with_original_owner_and_raw_parse(native_reader):
    f=native_reader;transports=[]
    def transport(command,**kwargs):
        transports.append((command,kwargs));assert kwargs['timeout']==60.
        return subprocess.CompletedProcess(command,0,'original strict raw output','')
    read=f.bind(transport);response=read(['123'],{},owner='pwa209',account='def-ptewarie_cpu')
    assert response.stdout=='original strict raw output' and len(transports)==len(f.calls)==1
    assert f.calls[0]==(['123'],{},'pwa209','def-ptewarie_cpu')
    assert len(list((f.support/'native-reads').glob('*-intent.json')))==1
    assert json.loads(next((f.support/'native-reads').glob('*-result.json')).read_text())['returncode']==0


def test_timeout_preserves_diagnostics_without_repeating_query_or_admission(native_reader):
    f=native_reader;calls=[]
    def transport(command,**kwargs):calls.append(command);raise subprocess.TimeoutExpired(command,kwargs['timeout'])
    read=f.bind(transport)
    with pytest.raises(subprocess.TimeoutExpired):read(['123'],{},owner='pwa209',account=None)
    assert len(calls)==len(f.calls)==1
    result=json.loads(next((f.support/'native-reads').glob('*-result.json')).read_text())
    assert result['status']=='native_read_failed_no_added_retry' and result['error_type']=='TimeoutExpired'


@pytest.mark.parametrize('wrong',['marker','expired','bool','nan','existing'])
def test_reader_installation_requires_original_guard_finite_end_and_fresh_evidence(native_reader,wrong):
    f=native_reader;end=5000.
    if wrong=='marker':f.original.__hhw_native_accounting_read_guard__['sha256']='f'*64
    elif wrong=='expired':end=1200.
    elif wrong=='bool':end=True
    elif wrong=='nan':end=float('nan')
    else:(f.support/'native-reads').mkdir()
    with pytest.raises(ValueError):f.bind(lambda *a,**k:pytest.fail('No transport during invalid install'),end=end)


def test_native_margin_is_rechecked_before_query_and_other_commands_are_refused(native_reader):
    f=native_reader
    read=f.bind(lambda *a,**k:pytest.fail('Expired or foreign query never executes'))
    f.advance[0]=181.
    with pytest.raises(ValueError,match='deadline'):read(['123'],{},owner='pwa209',account=None)
    f.clock[0]=0.;f.advance[0]=0.;f.command[:]=['scontrol','release','123']
    with pytest.raises(ValueError,match='strict native read'):read(['123'],{},owner='pwa209',account=None)



