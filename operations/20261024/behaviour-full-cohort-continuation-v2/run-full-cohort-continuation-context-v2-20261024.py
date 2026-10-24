"""Operational-only24h full-training-fold comparison context; no full-fit entry point."""
from datetime import datetime, timezone
from pathlib import Path
import argparse
import hashlib
import json
import math
import os
import re
import runpy
import subprocess
import time

from horsehair.core import read_json, source_identity, canonical_hash, digest, native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
from horsehair.slurm import completed_native_dependency

ROOT=Path('/scratch/pwa209/horsehair-worm')
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
FRAMEWORK=ROOT/'releases/20261021-caebc9c1dc77-8495a3d98551'
SOURCE='8495a3d985515bf71f476295da87e663a1e764864ddce47a06794e7e11d88f94'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
CASE=ROOT/'maintenance/behaviour-full-cohort-continuation-v2-20261024'
SUPPORT=ROOT/'maintenance/behaviour-full-cohort-continuation-support-v2-20261024'
QUALIFICATION=ROOT/'maintenance/study-af-execution-support-v2-20261021/qualification.json'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v2/policy.json'
POLICY_SHA='5dac85400e709525458cc34a2edc2bb40b029eb743078672cd721ab444c17015'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'
FALSE_FLAGS=('scientific_result','scientific_fit','complete_declared_workload','admission_authority')


def require(value,message):
    if not value:raise ValueError(message)


def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def memory(value):
    match=re.fullmatch(r'([1-9][0-9]*)([MG])',value)
    require(match is not None,'Exact positive native memory units required')
    return int(match[1])*1024**(2 if match[2]=='M' else 3)


def native_allocation(job,script,reader):
    response=reader(job,SUPPORT/'own-scheduler-reads');response.check_returncode()
    lines=[line for line in response.stdout.splitlines() if line.strip()]
    require(len(lines)==1,'One actual native allocation required')
    pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);fields=dict(pairs)
    require(len(fields)==len(pairs),'Duplicate native allocation fields refused')
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING'
        and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'}
        and fields.get('TimeLimit') in {'24:00:00','1-00:00:00','0-24:00:00'}
        and memory(fields.get('MinMemoryNode',''))==64*1024**3
        and fields.get('Command')==str(script),'Exact owned one-CPU64GB/24-hour training-fold allocation required')
    require(all('gpu' not in fields.get(key,'').lower() for key in
        ('TresPerNode','TresPerJob','TresPerTask','AllocTRES','ReqTRES','Gres')),'CPU-only profile allocation required')
    end=datetime.fromisoformat(fields.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    environment=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(environment) and abs(environment-end)<=2 and time.time()+900<end,'Verified native end/margin required')
    script=Path(script)
    require(script.is_absolute() and script==script.resolve() and not script.is_symlink() and script.resolve().is_relative_to(ROOT/'plans'),
        'Exact immutable registered native script required')
    directory=script.parent;plan=read_json(directory/'plan.json')
    ledger_path=ROOT/'state'/(directory.name+'-submission.json');ledger=read_json(ledger_path)
    tasks=[task for task in plan['tasks'] if task['script']==str(script)]
    require(len(tasks)==1 and plan['config_sha256']==CONFIG and plan['release_identity']['sha256']==SOURCE
        and ledger['source_sha256']==SOURCE and ledger['plan_sha256']==canonical_hash(plan),
        'Source/config-bound registered profile plan required')
    task=tasks[0];registered=ledger['tasks'][task['name']];resources=task['resources']
    require(registered['job_id']==job and registered['state']=='submitted'
        and registered['script_sha256']==task['script_sha256']==canonical_hash(script.read_text())
        and fields.get('JobName')=='hhw-'+task['name']
        and resources=={'cpus':1,'memory':'64G','hours':24,'gpu':False,'array':None},
        'Native allocation does not match exact registered script/resources')
    return {'fields':fields,'native_stdout':response.stdout,'allocation_end_unix':end,
        'plan_path':str(directory/'plan.json'),'plan_sha256':canonical_hash(plan),
        'ledger_path':str(ledger_path),'ledger_sha256':digest(ledger_path),'script_sha256':digest(script)}


def qualification_proof(path,sha,job,observed):
    path=Path(path)
    require(path==QUALIFICATION and not path.is_symlink() and digest(path)==sha
        and re.fullmatch('[1-9][0-9]*',job) is not None,'Exact new qualification path/SHA/scalar job required')
    value=read_json(path);record=observed.get(job,{})
    require(value.get('status')=='complete' and type(value.get('returncode')) is int and value['returncode']==0
        and value.get('source_sha256')==SOURCE and value.get('config_sha256')==CONFIG
        and value.get('source_verified_unchanged') is True and value.get('slurm_job_id')==job
        and value.get('test_counts')=={'tests':1806,'failures':0,'errors':0,'skipped':0}
        and value.get('resource_policy',{}).get('sha256')==POLICY_SHA,
        'Positive actual full1806 new-source qualification required')
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0'
        and record.get('user')=='pwa209' and record.get('account')=='def-ptewarie_cpu'
        and record.get('job_id_raw')==job and record.get('unallocated_metadata') is False
        and record.get('cpus')==4 and type(record.get('elapsed_seconds')) is int
        and 0<record['elapsed_seconds']<=3600 and 'gpu' not in record.get('alloc_tres','').lower(),
        'Positive owned scalar four-CPU qualification native completion required')
    proof=completed_native_dependency(ROOT,job)
    require(proof is not None,'Qualification final native accounting proof is pending')
    artifact=value.get('artifacts',{}).get('junit',{});junit=Path(artifact.get('path',''))
    require(junit.is_absolute() and junit.resolve().is_relative_to(ROOT/'state/qualification-evidence')
        and not junit.is_symlink() and digest(junit)==artifact.get('sha256')
        and junit.stat().st_size==artifact.get('bytes'),'Exact full qualification JUnit bytes required')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(junit)=={'tests':1806,'failures':0,'errors':0,'skipped':0},'Actual1806 native JUnit roster required')
    return {'path':str(path),'sha256':sha,'job_id':job,'native_observed':record,'completion':proof,
        'junit':artifact,'resource_policy':value['resource_policy']}


class PendingStorage(ValueError):pass


def bounded_native_reader(original,evidence,allocation_end,*,transport=subprocess.run,wall=time.time,mono=time.monotonic):
    """Process-local timeout layer around unchanged358, installed before campaign.

    This adds no retry: only the original358 conflict policy can request another
    read. Every actual query has a fresh60s/native-margin bound and evidence.
    """
    marker=getattr(original,'__hhw_native_accounting_read_guard__',None)
    require(type(marker) is dict and marker.get('path')==str(GUARD) and marker.get('sha256')==GUARD_SHA,
        'Exact previously installed358 native reader required')
    require(type(allocation_end) in (int,float) and math.isfinite(allocation_end)
        and allocation_end>wall()+300,'Finite verified native read finalization margin required')
    evidence=Path(evidence)
    require(evidence.is_absolute() and evidence==evidence.resolve() and evidence.is_relative_to(SUPPORT)
        and not evidence.exists() and not evidence.is_symlink(),'Fresh prefix native query evidence required')
    evidence.mkdir(mode=0o700);sequence=[0]
    def read(ids,rosters,*,runner=None,owner=None,account=None):
        actual=transport if runner is None else runner
        deadline=min(mono()+180,mono()+allocation_end-wall()-300)
        def bounded(command,**kwargs):
            require(isinstance(command,(list,tuple)) and len(command)==8 and command[:3]==['sacct','--array','-j']
                and re.fullmatch('[0-9]+(?:_[0-9]+)?(?:,[0-9]+(?:_[0-9]+)?)*',command[3])
                and command[4].startswith('--format=') and command[5:]==['--units=K','--noheader','--parsable2'],
                'Only the exact strict native read command is supported')
            seconds=min(60.,deadline-mono(),allocation_end-wall()-300)
            if 'timeout' in kwargs:
                value=kwargs['timeout'];require(type(value) in (int,float) and math.isfinite(value) and value>0,
                    'Finite positive upstream native read timeout required');seconds=min(seconds,value)
            require(math.isfinite(seconds) and seconds>0,'Native read deadline exhausted before query')
            sequence[0]+=1;prefix=evidence/('query-'+str(sequence[0]).zfill(5))
            exclusive(str(prefix)+'-intent.json',{'command':list(command),'maximum_seconds':seconds,
                'qualified358_source_sha256':GUARD_SHA,'new_layer_retries':0,'native_allocation_end_unix':allocation_end,
                'scientific_result':False,'admission_authority':False})
            kwargs['timeout']=seconds
            try:response=actual(command,**kwargs)
            except BaseException as error:
                exclusive(str(prefix)+'-result.json',{'status':'native_read_failed_no_added_retry','error_type':type(error).__name__,
                    'error':str(error),'scientific_result':False,'admission_authority':False});raise
            exclusive(str(prefix)+'-result.json',{'status':'native_transport_returned_for_original_strict_parse',
                'returncode':response.returncode,'stdout_sha256':hashlib.sha256((response.stdout or '').encode()).hexdigest(),
                'scientific_result':False,'admission_authority':False})
            return response
        return original(ids,rosters,runner=bounded,owner=owner,account=account)
    read.__hhw_native_accounting_read_guard__=dict(marker)
    read.__hhw_prefix_deadline_layer__={'source_sha256':digest(Path(__file__)),'maximum_query_seconds':60,
        'maximum_original_read_seconds':180,'additional_retries':0}
    return read




def storage_proof(config,quota):
    from horsehair.admission import _storage,AdmissionError
    path=ROOT/'state/storage-budget.json'
    if not path.is_file():raise PendingStorage('Exact existing study census is pending; no scan runs in this profile')
    raw=path.read_bytes();value=json.loads(raw)
    captured=datetime.fromisoformat(value.get('captured_utc','').replace('Z','+00:00'))
    if captured.tzinfo is None or not 0<=(datetime.now(timezone.utc)-captured).total_seconds()<=21600:
        raise PendingStorage('Exact study census exceeds its six-hour freshness window')
    try:future_bytes,future_files,paths=_storage(ROOT,config,10_000_000_000,2000,personal_headroom=quota)
    except AdmissionError as error:raise PendingStorage(str(error)) from error
    import hashlib
    sha=hashlib.sha256(raw).hexdigest()
    if digest(path)!=sha:raise PendingStorage('Mutable study census changed during admission; preserve pending evidence')
    exclusive(SUPPORT/'study-census-snapshot.json',value)
    return {'path':str(path),'sha256':sha,'captured_utc':value['captured_utc'],
        'forecast_additional_bytes':10_000_000_000,'forecast_additional_files':2000,
        'projected_study_bytes':future_bytes,'projected_study_files':future_files,
        'native_source_paths':list(map(str,paths)),'headroom':quota}





if __name__=='__main__':raise SystemExit('Operational context library only; no full-fit launcher')
