"""Allocated prepare/activate handoff; ambiguous mutations are never replayed."""
from pathlib import Path
from datetime import datetime,timezone
from contextlib import contextmanager
from types import SimpleNamespace
import argparse
import copy
import gzip
import hashlib
import importlib.util
import json
import math
import os
import re
import runpy
import shlex
import signal
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-census-resilience-handoff-v6-20261024'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
SELF='run-census-resilience-handoff-v6-20261024.py'
FILES={SELF,'old-input-pins.json','partial-prepare-pins.json','prepared-origin-pins.json','retirement-continuation-pins.json','checkpoint-continuation-pins.json','primary-roster.py','primary-history-pins.json','test_census_resilience_handoff_v6_20261024.py','test_primary_roster.py'}
CONSUMER_SUPPORT=ROOT/'maintenance/study-census-resilience-consumers-v2-20261024'
CONSUMER_NAME='run-census-resilience-consumer-v2-20261024.py'
CONSUMER_SHA='d809f8b541ec7f764c00dfeda902b961bbdc28efe076ca20c3e6ff817b9c6efa'
CONSUMER_MANIFEST_SHA='74f512e641d8ddf413b93efc34df360570df190fb90652ccc7fc9308b2be1440'
CONSUMER_Q={'path':str(CONSUMER_SUPPORT/'qualification.json'),'sha256':'7cf95012b0e0d5ade662adae0d4f75e87c8b54c144271a47602e56ae8d33cd09','job_id':'6987709'}
PROVIDER_SUPPORT=ROOT/'maintenance/study-census-resilience-support-v1-20261024'
PROVIDER_NAME='run-fresh-storage-census-resilience-v1-20261024.py'
PROVIDER_SHA='94ac29d11da50412ba3c1f9269ed5762224f93635cd1fae505bfabc5f9b63a93'
FIRST_PROVIDER='6982722'
OLD={'af':('6950429','6950437','6950443','6950449'),'primary':('6873101','6873102','6873105','6873107')}
OLD_PHASE={'af':'study-af-f2-backfill-controllers-v2-20261024','primary':'primary-external-census-controllers-v3'}
AF_CASE=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
PRIMARY_RID='20261013-829e3817bee1-0e2b1ebce8bf'
PIPELINE_CASE=ROOT/'state/pipeline'/PRIMARY_RID
PRIMARY_SUPPORT=ROOT/'maintenance/study-primary-external-census-policy-v1-20261023'
PRIMARY_MARKER_SHA='a872578670f4e25d68cabf9408439ff5c667e5dc77154c333947c67b13417bce'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
TERMINAL={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
FALSE_FLAGS={k:False for k in ('scientific_result','admission_authority','scientific_gates_cleared','worker_jobs_changed','campaign_state_changed','automatic_replay_permitted')}
MEMORY_NOTE='sbatch: NOTE: Your memory request of 65536.0M was likely submitted as 64.0G. Please note that Slurm interprets memory requests denominated in G as multiples of 1024M, not 1000M.\n'


def require(value,message):
    if not value:raise ValueError(message)


def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        while block:=stream.read(1024**2):h.update(block)
    return h.hexdigest()
def locator(path):return {'path':str(path),'sha256':digest(path)}


def strict(raw):
    def pairs(items):
        value={}
        for key,item in items:require(key not in value,'Duplicate JSON field refused');value[key]=item
        return value
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('Nonfinite JSON refused')))


def packet(path,sha=None,limit=8_000_000):
    p=Path(path)
    require(p.is_absolute() and p==p.resolve() and p.is_relative_to(ROOT) and p!=ROOT and p.is_file()
        and all(not q.is_symlink() for q in (p,*p.parents) if q.is_relative_to(ROOT)),'Exact ordinary confined packet required')
    with p.open('rb') as f:raw=f.read(limit+1)
    require(len(raw)<=limit and (sha is None or hashlib.sha256(raw).hexdigest()==sha),'Bounded immutable evidence changed')
    return raw


def read(binding):
    require(type(binding) is dict and set(binding)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',str(binding['sha256'])),'Exact file locator required')
    return strict(packet(binding['path'],binding['sha256']))


def save(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())


def loaded(path,sha,name):
    packet(path,sha,300_000);spec=importlib.util.spec_from_file_location(name,path)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);require(digest(path)==sha,'Imported source changed');return m


def source_guard(pins,absent=()):
    for path,sha in pins.items():
        p=Path(path);require(p.is_file() and not p.is_symlink() and digest(p)==sha,'Source/checkpoint changed: '+str(path))
    for path in absent:require(not Path(path).exists() and not Path(path).is_symlink(),'Previously absent checkpoint appeared: '+str(path))


def source_manifest(binding):
    require(binding['path']==str(SUPPORT/'source-manifest.json'),'Exact own source manifest required')
    value=read(binding);require(set(value)=={'kind','files','suites'} and value['kind']=='census-resilience-handoff-source-manifest-v6'
        and set(value['files'])==FILES and set(value['suites'])=={'handoff','primary_roster'},'Complete exact handoff source/test closure required')
    pins={binding['path']:binding['sha256']}
    for name,sha in value['files'].items():packet(SUPPORT/name,sha,300_000);pins[str(SUPPORT/name)]=sha
    require(Path(__file__)==SUPPORT/SELF and digest(__file__)==value['files'][SELF],'Exact qualified native handoff source required')
    for scope,name in [('handoff','test_census_resilience_handoff_v6_20261024.py'),('primary_roster','test_primary_roster.py')]:
        contract=value['suites'][scope]
        require(set(contract)=={'source','source_sha256','tests','roster_sha256'} and contract['source']==name
            and contract['source_sha256']==value['files'][name] and type(contract['tests']) is int and contract['tests']>0
            and re.fullmatch('[0-9a-f]{64}',str(contract['roster_sha256'])),'Exact actual suite contract required')
    return value,pins


def fields(response):
    response.check_returncode();require(not response.stderr,'Native scheduler stderr refused')
    lines=[x for x in response.stdout.splitlines() if x.strip()];require(len(lines)==1,'One exact native job record required')
    pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);value=dict(pairs)
    require(len(value)==len(pairs),'Duplicate scheduler fields refused');return value


def seconds(value):
    m=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',value);require(m is not None,'Finite exact scheduler duration required')
    d,h,n,s=(int(x or 0) for x in m.groups());return d*86400+h*3600+n*60+s


def scalar_fields(value,task,job,*,hours=None,held=False):
    require(value.get('JobId')==job and value.get('UserId')=='pwa209(3164768)' and value.get('Account')=='def-ptewarie_cpu'
        and value.get('JobName')=='hhw-'+task['name'] and value.get('Command')==task['script']
        and value.get('NumCPUs')=='1' and value.get('NumNodes') in {'1','1-1'} and value.get('MinMemoryNode') in {'64G','65536M'}
        and 'ArrayJobId' not in value and 'ArrayTaskId' not in value
        and all('gpu' not in value.get(k,'').lower() for k in ('ReqTRES','AllocTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact owned scalar1CPU64G controller source/native contract required')
    maximum=seconds(value.get('TimeLimit',''))
    require((hours==96 and 28800<=maximum<=345600 and seconds(value.get('TimeMin',''))==28800)
        or (hours==72 and maximum==259200),'Exact inherited controller runtime required')
    if held:
        require(value.get('JobState')=='PENDING' and value.get('Reason')=='JobHeldUser' and value.get('Priority')=='0'
            and value.get('RunTime')=='00:00:00' and value.get('Restarts')=='0' and value.get('AllocTRES')=='(null)'
            and value.get('NodeList') in {'','(null)','None'},'Positive held never-allocated controller required')
        # StartTime is an estimate on pending jobs, including held future jobs;
        # zero allocation is established by strict native accounting below.
    return value


def owned_record(record,job):
    require(record.get('job_id_raw')==job and record.get('user')=='pwa209' and record.get('account')=='def-ptewarie_cpu'
        and 'gpu' not in record.get('alloc_tres','').lower() and type(record.get('elapsed_seconds')) is int,
        'Exact scalar owned no-GPU native allocation identity required')
    return record


def zero_pending(record,job):
    owned_record(record,job)
    require(record.get('state')=='PENDING' and record.get('cpus')==0 and record['elapsed_seconds']==0
        and record.get('exit_code')=='0:0' and record.get('unallocated_metadata') is False
        and record.get('identity_basis')=='Unique scalar native job; zero-allocation queue record'
        and record.get('alloc_tres')=='' and record.get('node_list')=='None assigned'
        and record.get('start') in {'None','Unknown'} and record.get('end') in {'None','Unknown'}
        and record.get('max_rss') in {'','0','0K'},'Positive exact scalar never-allocated pending proof required')


def dependency_fields(value,task,ledger,completion):
    row=ledger['tasks'][task['name']];issued=row.get('issued_native_dependencies');done=row.get('completed_native_dependencies',[])
    expected=set(task.get('external_dependencies',[]))|{ledger['tasks'][name]['job_id'] for name in task.get('depends',[])}
    require(task.get('dependency_kind')==row.get('dependency_kind')=='afterany' and type(issued) is list
        and len(issued)==len(set(issued)) and type(done) is list
        and row.get('dependency_resolution_sha256')==canonical({'kind':'afterany','issued':issued,'completed':done})
        and set(issued)|{p.get('job_id') for p in done}==expected,'Exact actual afterany dependency resolution required')
    for proof in done:
        current=completion(proof['job_id'])
        require(proof.get('dependency_kind')=='afterany' and current is not None
            and current.get('native_records')==proof.get('native_records'),'Pruned dependency lacks its exact final native proof')
    observed=set()
    for part in value.get('Dependency','').split(','):
        if part in {'','(null)','None'}:continue
        match=re.fullmatch(r'afterany:([1-9][0-9]*)(?:\((?:fulfilled|unfulfilled)\))?',part)
        require(match is not None and match[1] not in observed,'Only exact unique afterany native edges permitted');observed.add(match[1])
    require(observed<=set(issued),'Native edges differ from exact issued predecessors')
    for job in set(issued)-observed:require(completion(job) is not None,'Disappeared native dependency is not positively terminal')


def cancelled_zero(record,job):
    owned_record(record,job)
    require(record.get('state')=='CANCELLED' and record.get('exit_code')=='0:0' and record.get('cpus')==0
        and record['elapsed_seconds']==0 and record.get('unallocated_metadata') is False
        and record.get('identity_basis')=='Unique scalar native job; zero-allocation queue record'
        and record.get('alloc_tres')=='' and record.get('node_list')=='None assigned'
        and record.get('start') in {'None','Unknown'} and re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]+',str(record.get('end','')))
        and record.get('max_rss') in {'','0','0K'},'Positive exact scalar zero-allocation cancelled backup proof required')


def parent_terminal(record,job,*,cancelled=False):
    owned_record(record,job)
    require(record.get('state')==('CANCELLED' if cancelled else 'COMPLETED')
        and (re.fullmatch('[0-9]+:[0-9]+',str(record.get('exit_code',''))) if cancelled else record.get('exit_code')=='0:0')
        and record.get('cpus')==1 and 0<record['elapsed_seconds']<=345600 and record.get('unallocated_metadata') is False,
        'Positive actual allocated final controller accounting required')


def lease_guard(budget,parents,backups,*,final=False):
    leases=budget.get('leases',{})
    for job in backups:require(job not in leases,'A backup owns a durable lease')
    for job,hours in parents.items():
        entry=leases.get(job);require(type(entry) is dict and entry.get('cpus')==1 and entry.get('gpus')==0 and entry.get('max_hours')==hours,
            'Exact original conservative controller lease required')
        require(entry.get('state')==('native_reconciled' if final else 'running'),'Original controller lease is not in the required native state')
        if final:require(entry.get('native')==budget.get('native_jobs',{}).get(job),'Lease does not retain its exact native final accounting')


def pending_primary_status(primary,*,now=time.time):
    require(primary.get('status')=='pending_external_storage_census' and str(primary.get('controller_job_id'))==OLD['primary'][0]
        and primary.get('config_sha256')==CONFIG and primary.get('scheduling_paused') is True
        and primary.get('source_sha256')=='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
        and primary.get('controller_framework_source_sha256')=='d89b31180d4c4ffe9c4023dc5534b96f2d157a3a8e3aecaff30849283d00a705'
        and primary.get('scientific_results_complete') is False and primary.get('publication_ready') is False
        and primary.get('scientific_result') is False and primary.get('admission_authority') is False
        and type(primary.get('checked_unix')) in (float,int) and 0<=now()-primary['checked_unix']<=180,
        'Fresh exact already-entered primary external-census gate required')


def pending_status(af,primary,*,now=time.time):
    require(af.get('status')=='resource_pending_storage' and str(af.get('native_controller_job'))==OLD['af'][0]
        and af.get('scientific_result') is False and af.get('admission_authority') is False,'Current AF storage gate required')
    pending_primary_status(primary,now=now)


def expired_original_budget(value,*,now=time.time):
    measured=value.get('measured_unix');captured=datetime.fromisoformat(value.get('captured_utc','').replace('Z','+00:00'))
    require(type(measured) in (int,float) and math.isfinite(measured) and captured.tzinfo is not None
        and now()-measured>=21600 and now()-captured.timestamp()>=21600,'Every completed old producer must have both immutable original clocks expired')


def closed_old_providers(schedule,records,*,now=time.time):
    """A frozen old reader cannot regain authority from these finite producers."""
    plan=read(schedule['plan']);ledger=read(schedule['ledger']);require(len(plan.get('tasks',[]))==64,'Exact full old64 roster required')
    pins={schedule[k]['path']:schedule[k]['sha256'] for k in ('plan','ledger','helper','source_manifest','qualification')};completed=[]
    jobs=[str(ledger['tasks'][t['name']]['job_id']) for t in plan['tasks']]
    require(len(set(jobs))==64 and set(records)==set(jobs),'Exactly every old provider must have fresh accounting')
    for task,job in zip(plan['tasks'],jobs):
        row=owned_record(records[job],job)
        require(row.get('state') in TERMINAL and row.get('cpus') in {0,8} and 0<=row['elapsed_seconds']<=21600
            and re.fullmatch('[0-9]+:[0-9]+',str(row.get('exit_code',''))),'Every old provider must be strictly terminal')
        require((row['cpus']==0 and row['elapsed_seconds']==0 and row.get('unallocated_metadata') is False
                and row.get('identity_basis')=='Unique scalar native job; zero-allocation queue record')
            or (row['cpus']==8 and row['elapsed_seconds']>0 and row.get('unallocated_metadata') is False),'Old producer cost/allocation proof is ambiguous')
        if row['cpus']==0:cancelled_zero(row,job)
        if row['state']!='COMPLETED' or row['exit_code']!='0:0':continue
        case=Path(schedule['case_root'])/('native-'+job);path=case/'receipt.json';receipt=read(locator(path));pins[str(path)]=digest(path)
        require(receipt.get('status')=='complete_exact_size_sweep_with_original_conservative_freshness'
            and receipt.get('native_job_id')==job and receipt.get('wrapper_sha256')==schedule['helper']['sha256']
            and receipt.get('source_manifest')==schedule['source_manifest'] and receipt.get('qualification')==schedule['qualification'],
            'A completed old producer needs its exact immutable qualified receipt')
        sweep=read(receipt['sweep']);require(receipt['sweep']['path']==str(case/'sweep-result.json'),'Exact old sweep path required')
        pins[receipt['sweep']['path']]=receipt['sweep']['sha256']
        budget=sweep['budget'];require(budget['path']==str(case/'original-storage-budget.json'),'Exact conservative original saved budget required')
        value=read(budget);expired_original_budget(value,now=now);pins[budget['path']]=budget['sha256'];completed.append(job)
    source_guard(pins)
    return {'jobs':jobs,'completed_receipts_with_expired_original_clocks':completed,'checked_paths':pins,
        'all_old_producers_terminal':True,'old_reader_cannot_regain_fresh_pair_authority':True,**FALSE_FLAGS}


class ControllerNotQuiescent(ValueError):
    """Owned identity is valid but a stable sleeping window is not present."""


def proc_sample(pid,job,expected_command,*,proc=Path('/proc')):
    require(os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',str(pid)),'Exact owned process probe required')
    directory=proc/str(pid);status=(directory/'status').read_text();info=dict(line.split(':',1) for line in status.splitlines() if ':' in line)
    require(info.get('Uid','').split()==['3164768']*4 and int(info['Threads'])==1,'Owned single-thread controller process required')
    command=(directory/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')
    require(command==shlex.split(expected_command),'Live process arguments differ from immutable registered source invocation')
    require(re.search(r'(?:^|/)job[_-]'+job+r'(?:/|$)',(directory/'cgroup').read_text()) is not None,'Process does not belong to exact Slurm allocation')
    stat=(directory/'stat').read_text();rest=stat[stat.rindex(')')+2:].split()
    children=[]
    for task in (directory/'task').iterdir():children.extend((task/'children').read_text().split())
    wchan=(directory/'wchan').read_text().strip()
    if not (rest[0]=='S' and wchan=='hrtimer_nanosleep' and not children):
        raise ControllerNotQuiescent('Controller is not positively sleeping without children')
    return {'pid':int(pid),'job_id':job,'start_ticks':int(rest[19]),'cpu_ticks':int(rest[11])+int(rest[12]),
        'state':rest[0],'wchan':wchan,'children':children,'command':command,'sampled_unix':time.time()}


def two_samples(first,second):
    require(first['pid']==second['pid'] and first['job_id']==second['job_id'] and first['start_ticks']==second['start_ticks']
        and first['command']==second['command'],'Controller identity changed across quiescence samples')
    if not (first['cpu_ticks']==second['cpu_ticks']
        and not first['children'] and not second['children'] and first['state']==second['state']=='S'
        and first['wchan']==second['wchan']=='hrtimer_nanosleep'
        and .5<=second['sampled_unix']-first['sampled_unix']<=10):
        raise ControllerNotQuiescent('Two stable fresh sleeping samples without process/CPU/child change required')


def quiescent_probe(pid,job,command,*,clock=time.monotonic,sleeper=time.sleep,maximum_seconds=20):
    """Wait only for transient activity; identity and inventory failures abort."""
    require(type(maximum_seconds) in {int,float} and 1<=maximum_seconds<=20,'Finite quiescence observation bound required')
    deadline=clock()+maximum_seconds;attempts=[]
    while True:
        try:
            first=proc_sample(pid,job,command);members=same_job_processes(pid,job)
            if clock()+1>deadline:raise TimeoutError('Stable quiescence observation deadline reached')
            sleeper(1);second=proc_sample(pid,job,command);after=same_job_processes(pid,job)
            require(members==after,'Owned same-allocation membership/roles changed across process samples')
            two_samples(first,second)
            require(clock()<=deadline,'Stable quiescence observation deadline reached')
            return {'first':first,'second':second,'same_job_processes':members,'transient_observations':attempts,
                'maximum_observation_seconds':maximum_seconds}
        except ControllerNotQuiescent as error:
            attempts.append({'observation':len(attempts)+1,'reason':str(error)})
            remaining=deadline-clock()
            if remaining<1:raise TimeoutError('Stable quiescence observation deadline reached') from error
            sleeper(min(1,remaining))


def same_job_processes(controller,job,*,proc=Path('/proc'),probe_pid=None,uid=3164768):
    """Reject detached/reparented same-allocation processes, not just children."""
    probe_pid=os.getpid() if probe_pid is None else probe_pid;members={}
    entries=list(proc.iterdir());require(len(entries)<=5000,'Bounded native process inventory exceeded')
    for directory in entries:
        if not directory.name.isdigit():continue
        try:
            if directory.stat().st_uid!=uid:continue
            cgroup=(directory/'cgroup').read_text()
            if not re.search(r'(?:^|/)job[_-]'+job+r'(?:/|$)',cgroup):continue
            raw=(directory/'stat').read_text();rest=raw[raw.rindex(')')+2:].split()
            command=(directory/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')
            members[int(directory.name)]={'parent':int(rest[1]),'state':rest[0],'command':command}
        except FileNotFoundError:
            raise ValueError('An owned process disappeared during same-job classification')
    allowed={int(controller),int(probe_pid)}
    for origin in tuple(allowed):
        seen=set();current=origin
        while current in members:
            require(current not in seen and len(seen)<=32,'Cyclic/unbounded owned process ancestry');seen.add(current)
            parent=members[current]['parent']
            if parent not in members:break
            allowed.add(parent);current=parent
    require(int(controller) in members and int(probe_pid) in members and set(members)<=allowed,
        'Detached or unclassified same-allocation process prevents retirement')
    for pid,value in members.items():
        if pid in {int(controller),int(probe_pid)}:continue
        require(value['state'] in {'S','I'} and value['command'] and Path(value['command'][0]).name in {'bash','sh','slurmstepd'},
            'Only sleeping batch/step ancestors may accompany the controller and read-only probe')
    return members


def checkpoint(case,*,primary=False):
    """Only controller state/journals; never enumerate worker artifact trees."""
    paths={};absent=[]
    require(case.is_dir() and not case.is_symlink(),'Exact existing controller checkpoint directory required')
    entries=list(case.iterdir());require(len(entries)<=5000,'Bounded checkpoint directory exceeded')
    for p in entries:
        if p.name.endswith('.partial') or p.name.endswith('.tmp'):raise ValueError('Unfinished checkpoint exists')
        selected=(p.name.endswith('-plan-journal.json') if primary else p.name in {'campaign-plan.json','state.json.gz','state-binding.json','precision-gate.json'} or p.name.startswith('submission-'))
        if not selected:continue
        packet(p,limit=64_000_000);paths[str(p)]=digest(p)
        if p.name.startswith('submission-'):
            value=strict(packet(p));require(value.get('status')=='released' and re.fullmatch('[1-9][0-9]*',str(value.get('job_id',''))),
                'Ambiguous or unreleased worker submission journal prevents retirement')
    if not primary:
        for name in ('campaign-plan.json','state.json.gz','state-binding.json'):require(str(case/name) in paths,'Complete AF checkpoint binding required')
        binding=strict(packet(case/'state-binding.json'));require(binding.get('state_file_sha256')==digest(case/'state.json.gz')
            and binding.get('scientific_result') is False and binding.get('admission_authority') is False,'Exact AF state binding required')
    return {'checked_paths':paths,'absent_paths':absent}


def accepted_memory_notice(command,result):
    return bool(command and command[0]=='sbatch' and '--hold' in command and result.returncode==0
        and re.fullmatch(r'[1-9][0-9]*\n',result.stdout) and result.stderr==MEMORY_NOTE)


def mutate(c,label,command,precheck):
    require(time.time()+600<c['end'],'Native mutation finalization margin exhausted');precheck()
    # Source/native/process prechecks may consume most of the allocation.
    # No mutator may be spawned after the finalization reserve is reached.
    remaining=c['end']-time.time()-600
    require(remaining>0,'Native mutation finalization margin exhausted after precheck')
    timeout=min(45,remaining);require(timeout>0,'Positive bounded mutation timeout required')
    c['sequence']+=1;prefix=c['area']/(str(c['sequence']).zfill(5)+'-'+label)
    save(str(prefix)+'-intent.json',{'command':command,'native_job_id':c['job'],'automatic_replay_permitted':False})
    try:
        timeout=min(timeout,c['end']-time.time()-600)
        require(timeout>0,'Native mutation finalization margin exhausted before command')
        result=subprocess.run(command,text=True,capture_output=True,timeout=timeout)
    except BaseException as error:
        save(str(prefix)+'-response.json',{'status':'ambiguous_action_no_replay','error_type':type(error).__name__,'error':str(error)});raise
    save(str(prefix)+'-response.json',{'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr})
    result.check_returncode()
    require(not result.stderr or accepted_memory_notice(command,result),'Mutation stderr requires explicit reconciliation')
    return result


def retirement(c):
    """Context supplies real source/native/checkpoint/process verifiers."""
    c['verify_quiescent']();save(c['area']/'pre-retirement-checkpoint.json',c['checkpoint_proof'])
    # Backups cannot run after active cancellation, even if a later action is
    # ambiguous. Their zero-allocation terminal proof precedes active retirement.
    for kind in ('af','primary'):
        for job in reversed(OLD[kind][1:]):
            if job in c.get('reconciled_retired_backups',set()):
                c['verify_retired_backup'](job)
                save(c['area']/('prior-backup-retirement-verified-'+job+'.json'),
                    {'job_id':job,'status':'prior_positive_terminal_retirement_preserved','scheduler_mutation_repeated':False})
                continue
            task=c['old_tasks'][job]
            mutate(c,'cancel-backup-'+job,['scancel','--ctld','--state=PENDING','--user=pwa209','--name=hhw-'+task['name'],job],
                lambda job=job:c['verify_backup'](job))
            c['wait_terminal'](job,False)
    for kind in ('af','primary'):
        job=OLD[kind][0]
        if c['already_completed'](job):continue
        c['verify_quiescent']()
        task=c['old_tasks'][job]
        mutate(c,'cancel-active-'+job,['scancel','--ctld','--state=RUNNING','--user=pwa209','--name=hhw-'+task['name'],job],c['verify_quiescent'])
        c['cancelled_parents'].add(job);c['wait_terminal'](job,True)
    c['final_proof']();save(c['area']/'retirement-proof.json',{'old_jobs':OLD,'cancelled_parents':sorted(c['cancelled_parents']),
        'checkpoint':c['checkpoint_proof'],'all_positive_terminal_and_native_reconciled':True,**FALSE_FLAGS})


def release_replacements(c):
    require(not c.get('registration_locked'),'Replacement release must be outside AF and primary recovery guards')
    c['final_proof']()
    for kind in ('af','primary'):
        for task,job in reversed(c['new_jobs'][kind]):
            mutate(c,'release-new-'+job,['scontrol','release',job],lambda task=task,job=job:c['verify_new_held'](task,job))
    save(c['area']/'receipt.json',{'status':'controlled_consumer_replacement_released','new_jobs':{k:[j for _,j in v] for k,v in c['new_jobs'].items()},
        'old_jobs':OLD,'retirement_proof':locator(c['area']/'retirement-proof.json'),**FALSE_FLAGS})


def old_inputs():
    value=read(locator(SUPPORT/'old-input-pins.json'));require(value.get('kind')=='census-resilience-old-consumer-inputs-v1' and len(value.get('files',{}))==14,'Exact14 original immutable inputs required')
    pins={str(ROOT/relative):sha for relative,sha in value['files'].items()};source_guard(pins);result={}
    for kind,ids in OLD.items():
        pp=ROOT/'plans'/OLD_PHASE[kind]/'plan.json';lp=ROOT/'state'/(OLD_PHASE[kind]+'-submission.json')
        plan=read(locator(pp));ledger=read(locator(lp));require(canonical(plan)==ledger.get('plan_sha256') and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG
            and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256')==(SOURCE if kind=='af' else 'd89b31180d4c4ffe9c4023dc5534b96f2d157a3a8e3aecaff30849283d00a705')
            and len(plan.get('tasks',[]))==4,'Exact old source/config/plan/ledger required')
        for task,job in zip(plan['tasks'],ids):
            row=ledger['tasks'][task['name']];text=packet(task['script']).decode()
            require(row.get('state')=='submitted' and row.get('job_id')==job and canonical(text)==task.get('script_sha256')==row.get('script_sha256')
                and task['command'] in text,'Exact old controller job/script registration required')
        result[kind]={'plan':plan,'ledger':ledger}
    return result,pins


def bounded_reader(original,end):
    marker=getattr(original,'__hhw_native_accounting_read_guard__',None)
    require(type(marker) is dict and marker.get('sha256')==GUARD_SHA,'Exact original358 native guard required')
    def read_native(ids,rosters,**kwargs):
        require('runner' not in kwargs,'Only bounded original accounting transport allowed')
        def run(command,**options):
            remaining=end-time.time()-600;require(remaining>0 and command[0]=='sacct','Native read deadline or command refused')
            options['timeout']=min(45,remaining);return subprocess.run(command,**options)
        return original(ids,rosters,runner=run,**kwargs)
    read_native.__hhw_native_accounting_read_guard__=marker
    return read_native


def native(c,job,label):
    c['sequence']+=1;return fields(c['scheduler'](job,c['area']/(str(c['sequence']).zfill(5)+'-'+label)))


def admission(c,label):
    require(c['registration_locked'],'Actual permanent AF mutex must be held for registration/admission')
    c['sequence']+=1
    return c['campaign'].combined_admission(ROOT,c['config'],PRIMARY_RID,c['area'],label+'-'+str(c['sequence']),
        resource_policy=c['policy'],cpus=0,gpus=0,hours=1)


@contextmanager
def already_locked(c):
    from horsehair.core import read_json
    owner=read_json(ROOT/'state/af-campaign-registration.mutex.owner.json')
    require(c['registration_locked'] and owner.get('pid')==os.getpid() and str(owner.get('slurm_job_id'))==c['job']
        and owner.get('state')=='held','Actual locally acquired AF kernel guard and its current owner required')
    yield


def libraries(c):
    consumer=loaded(CONSUMER_SUPPORT/CONSUMER_NAME,CONSUMER_SHA,'exact_new_consumer_v2')
    manifest_binding={'path':str(CONSUMER_SUPPORT/'source-manifest.json'),'sha256':CONSUMER_MANIFEST_SHA}
    require(c['descriptor']['consumer_qualification']==CONSUMER_Q,'Exact independently qualified new consumer69 binding required')
    q=consumer.validate_qualification(CONSUMER_Q,manifest_binding,c['native_reader']);c['pins'].update(q['checked_paths'])
    c['consumer_pins']=dict(q['checked_paths'])
    manifest,_=consumer.source_manifest(manifest_binding)
    builder=loaded(CONSUMER_SUPPORT/'build-consumer-plan.py',manifest['files']['build-consumer-plan.py'],'exact_new_consumer_builder')
    provider=loaded(PROVIDER_SUPPORT/PROVIDER_NAME,PROVIDER_SHA,'exact_new_provider')
    schedule=read(c['descriptor']['provider_schedule'])
    checked=provider.validate_schedule_binding(schedule,schedule['qualification'],c['native_reader']);c['pins'].update(checked['checked_paths'])
    c['pins'][c['descriptor']['provider_schedule']['path']]=c['descriptor']['provider_schedule']['sha256']
    old,captured=old_inputs();c['pins'].update(captured)
    af_policy_path=shlex.split(old['af']['plan']['tasks'][0]['command']);i=af_policy_path.index('--policy-descriptor')
    af_policy_binding={'path':af_policy_path[i+1],'sha256':af_policy_path[af_policy_path.index('--policy-descriptor-sha256')+1]};af_policy=read(af_policy_binding)
    outer=loaded(consumer.AF,consumer.AF_SHA,'original_backfill_outer')
    old_provider_policy=read(af_policy['provider_policy']);paths=outer.qualification(old_provider_policy)
    paths.update(outer.backfill_qualification(af_policy,provider_paths=paths)[0]);c['pins'].update(paths)
    primary=loaded(consumer.PRIMARY,consumer.PRIMARY_SHA,'original_primary_policy')
    command=shlex.split(old['primary']['plan']['tasks'][0]['command'])
    primary_policy_binding={'path':command[command.index('--policy-descriptor')+1],'sha256':command[command.index('--policy-descriptor-sha256')+1]}
    primary_policy,pmanifest,ppins=primary.policy_sources(primary_policy_binding['path'],primary_policy_binding['sha256'])
    primary.source_guard(ppins);c['pins'].update(ppins)
    primary.finalized_primary_qualification(primary_policy,pmanifest,__import__('horsehair.slurm',fromlist=['x']),lambda label:admission(c,label))
    require(primary_policy['census']['provider_schedule']==old_provider_policy['provider_schedule'],'Both old readers must bind the same exact closed provider schedule')
    c.update(consumer=consumer,consumer_manifest=manifest,consumer_builder=builder,provider=provider,schedule=schedule,
        old=old,outer=outer,old_schedule=old_provider_policy['provider_schedule'],old_provider_policy=old_provider_policy,
        legacy_policy_bindings={'af':af_policy_binding,'primary':primary_policy_binding},primary=primary)
    c['old_tasks']={job:task for kind in OLD for job,task in zip(OLD[kind],old[kind]['plan']['tasks'])}
    source_guard(c['pins'])


def old_provider_negative(c):
    schedule=c['old_schedule'];plan=read(schedule['plan']);ledger=read(schedule['ledger'])
    jobs=[ledger['tasks'][t['name']]['job_id'] for t in plan['tasks']]
    observed=c['native_reader'](jobs,{},owner='pwa209',account='def-ptewarie_cpu')
    proof=closed_old_providers(schedule,observed);c['pins'].update(proof['checked_paths']);return proof


def consumer_routing_scope(c):
    """Keep the narrow consumer verifier separate from project-source checks."""
    pins=c['consumer_pins']
    require(type(pins) is dict and pins and all(c['pins'].get(path)==sha for path,sha in pins.items()),
        'Exact consumer qualification pins must remain in the complete source closure')
    expected={str(CONSUMER_SUPPORT/CONSUMER_NAME):CONSUMER_SHA,
        str(CONSUMER_SUPPORT/'source-manifest.json'):CONSUMER_MANIFEST_SHA,CONSUMER_Q['path']:CONSUMER_Q['sha256']}
    require(all(pins.get(path)==sha for path,sha in expected.items()),'Complete independently qualified consumer scope required')
    source_guard(c['pins']);c['consumer'].source_guard(pins)
    return dict(pins)


def fresh_new_provider(c):
    from horsehair import slurm
    proof=slurm.completed_native_dependency(ROOT,FIRST_PROVIDER);require(proof is not None,'First new provider has no positive finalized afterok proof')
    records=c['native_reader']([FIRST_PROVIDER],{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(records)=={FIRST_PROVIDER},'Exactly the actual first new producer native identity required')
    record=owned_record(records[FIRST_PROVIDER],FIRST_PROVIDER)
    require(record.get('state')=='COMPLETED' and record.get('exit_code')=='0:0' and record.get('cpus')==8
        and 0<record['elapsed_seconds']<=21600 and record.get('unallocated_metadata') is False,
        'First new provider requires actual allocated8CPU/6h positive final evidence')
    first=read(c['schedule']['plan'])['tasks'][0];ledger=read(c['schedule']['ledger'])
    require(ledger['tasks'][first['name']]['job_id']==FIRST_PROVIDER,'Exact actually registered first new provider required')
    class PendingCensus(ValueError):pass
    @contextmanager
    def guard(context,role):
        require(role=='controller-start','Only original census accounting role allowed')
        with already_locked(c):yield
    base=SimpleNamespace(PendingStorage=PendingCensus,registration_guard=guard,
        admission=lambda context,label,**resources:admission(c,label))
    binding={'provider_schedule':c['schedule'],'source_manifest':{'path':str(CONSUMER_SUPPORT/'source-manifest.json'),'sha256':CONSUMER_MANIFEST_SHA}}
    route=c['consumer'].routed_provider(c['outer'],binding,consumer_routing_scope(c),c['native_reader'])
    before,pair=c['outer'].fresh_pair(base);producer=route(base,{'policy':c['old_provider_policy'],'base_context':{'config':c['config']}},pair)
    current,after=c['outer'].fresh_pair(base);require(pair==after,'Actual fresh provider/global pair changed during final proof')
    source_guard(c['pins'])
    return {'first_provider_completion':proof,'first_provider_native_record':record,'current_finalized_producer':producer,'global_pair':pair,
        'original_measured_unix':current['measured_unix'],'original_captured_utc':current['captured_utc']}


def selected_held(c,task,job,ledger):
    from horsehair import slurm
    observation=native(c,job,'new-held');scalar_fields(observation,task,job,hours=task['resources']['hours'],held=True)
    dependency_fields(observation,task,ledger,lambda job:slurm.completed_native_dependency(ROOT,job,dependency_kind='afterany'))
    records=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu');require(set(records)=={job},'One exact held new native record required');zero_pending(records[job],job)
    # Registration validator is the real new envelope entry validator; only the
    # native RUNNING branch is deliberately absent for a held preparation.
    tokens=shlex.split(task['command']);entry=str(CONSUMER_SUPPORT/CONSUMER_NAME);require(tokens.count(entry)==1,'Exact new entry once required')
    tail=tokens[tokens.index(entry)+1:];args=c['consumer'].parse(tail);flags=dict(zip(args.legacy_argv[::2],args.legacy_argv[1::2]))
    kind=task['census_resilience_consumer']['consumer_kind'];binding=read(task['census_resilience_consumer']['binding'])
    selected=c['consumer'].registration(locator(flags['--native-plan']),locator(flags['--native-ledger']),locator(flags['--native-script']),task['name'],job,
        entry=entry,argv=tail,source=SOURCE if kind=='af' else c['primary'].OLD_SOURCE,cpus=1,hours=96 if kind=='af' else 72,memory='64G')
    plan=read(locator(flags['--native-plan']))
    legacy=c['outer'] if kind=='af' else c['primary']
    if kind=='af':
        legacy.registered(locator(flags['--native-plan']),locator(flags['--native-ledger']),task['name'],locator(task['script']),job,
            helper=legacy.__file__,hours=96,cpus=1,memory='64G',argv=args.legacy_argv)
    else:
        legacy.own_registration(legacy.parser().parse_args(args.legacy_argv),job,args.legacy_argv,'run',binding['legacy_policy']['sha256'])
    require(selected==task,'Actual new registered task changed')
    return plan,binding


def new_held(c,task,job,ledger):
    plan,binding=selected_held(c,task,job,ledger)
    c['consumer'].chain_guard(plan,ledger,binding)


def amend_first_af(c,task,job,ledger):
    """One owned held scheduler maximum; conservative plan/lease remain96h."""
    new_held(c,task,job,ledger);before=native(c,job,'eight-hour-before')
    require(seconds(before['TimeLimit'])==345600 and seconds(before.get('TimeMin',''))==28800,
        'Only the original96h maximum with8h minimum may receive this one amendment')
    save(c['area']/'first-af-eight-hour-before.json',{'job_id':job,'fields':before,'registered_hours':96})
    mutate(c,'first-af-eight-hour-'+job,['scontrol','update','JobId='+job,'TimeLimit=08:00:00'],lambda:new_held(c,task,job,ledger))
    after=native(c,job,'eight-hour-after');new_held(c,task,job,ledger)
    # Scheduler evaluation timestamps may advance during a read; every field
    # expressing source, ownership, held state, allocation and DAG is retained.
    ignored={'LastSchedEval','TimeLimit'}
    require({k:v for k,v in before.items() if k not in ignored}=={k:v for k,v in after.items() if k not in ignored}
        and seconds(after.get('TimeLimit',''))==28800 and seconds(after.get('TimeMin',''))==28800,
        'A field other than the exact first-controller maximum changed')
    save(c['area']/'first-af-eight-hour-after.json',{'job_id':job,'fields':after,'registered_hours':96,
        'amended_native_maximum_seconds':28800,'conservative_lease_hours':96,'source_and_dag_preserved':True})


def accepted_registration_candidate(plan,ledger,intent,response,partial):
    """Resolve one acknowledged held submission; never issue sbatch here."""
    require(partial.get('kind')=='exact-accepted-held-prepare-reconciliation-v1'
        and partial.get('failed_job_id')=='7027712' and partial.get('accepted_job_id')=='7028292',
        'Exact preserved partial attempt required')
    task=plan['tasks'][0];row=ledger.get('tasks',{}).get(task['name'],{})
    command=['sbatch','--hold','--parsable',task['script']]
    require(set(ledger.get('tasks',{}))=={task['name']} and row.get('state')=='submitting' and 'job_id' not in row
        and row.get('command')==['sbatch','--parsable',task['script']]
        and row.get('script_sha256')==task['script_sha256'] and row.get('dependency_kind')=='afterany'
        and row.get('issued_native_dependencies')==[] and len(row.get('completed_native_dependencies',[]))==1
        and row['completed_native_dependencies'][0].get('job_id')==OLD['af'][0]
        and ledger.get('plan_sha256')==canonical(plan),'Only the exact first ambiguous held row may be reconciled')
    require(intent=={'command':command,'native_job_id':partial['failed_job_id'],'automatic_replay_permitted':False}
        and set(response)=={'returncode','stdout','stderr'}
        and response=={'returncode':0,'stdout':partial['accepted_job_id']+'\n','stderr':MEMORY_NOTE},
        'Exact acknowledged job ID and informational memory note required')
    result=copy.deepcopy(ledger);result['tasks'][task['name']].update(state='submitted',job_id=partial['accepted_job_id'])
    return result


def continued_registration_candidate(plan,original,current,intent,response,partial,reconciliation):
    candidate=accepted_registration_candidate(plan,original,intent,response,partial)
    continued=partial.get('continued_attempt',{})
    require(continued.get('job_id')=='7035171' and current==candidate
        and reconciliation.get('accepted_job_id')==partial['accepted_job_id']
        and reconciliation.get('old_ledger')==partial['af_ledger']
        and reconciliation.get('candidate_canonical_sha256')==canonical(candidate)
        and reconciliation.get('submit_command_replayed') is False
        and all(reconciliation.get(k) is False for k in FALSE_FLAGS),
        'Exact already persisted first-row reconciliation required')
    return candidate


def validate_continued_attempt(c,continued):
    from horsehair import slurm
    manifest=read(continued['source_manifest']);support=Path(continued['source_manifest']['path']).parent
    name='run-census-resilience-handoff-v2-20261024.py'
    old=loaded(support/name,manifest['files'][name],'preserved_v2_prepare_source')
    previous={'descriptor':{'handoff_qualification':continued['qualification']},
        'manifest_binding':continued['source_manifest'],'pins':{}}
    old.validate_own_qualification(previous);c['pins'].update(previous['pins'])
    bindings=read(continued['native_bindings']);reg=bindings['registration'];plan=read(reg['plan'])
    task=next(t for t in plan['tasks'] if t['name']==reg['task_name']);tokens=shlex.split(task['command'])
    require(tokens.count(str(support/name))==1,'Exact V2 failed helper invocation required')
    tail=tokens[tokens.index(str(support/name))+1:];args=old.parser().parse_args(tail)
    actual,_=old.own_registration(args,continued['job_id'],tail,manifest)
    require(actual==reg and args.mode==bindings.get('mode')=='prepare','Original V2 preparation registration changed')
    old.own_fields(bindings['allocation'],continued['job_id'],task,'prepare')
    failure=read(continued['failure'])
    require(failure.get('status')=='needs_explicit_reconciliation' and failure.get('mode')=='prepare'
        and failure.get('error')=='Exact distinct registered replacement jobs required'
        and failure.get('new_jobs')=={} and failure.get('cancelled_parents')==[]
        and all(failure.get(k) is False for k in FALSE_FLAGS),'Exact preserved V2 check-order refusal required')
    proof=slurm.completed_native_dependency(ROOT,continued['job_id'],dependency_kind='afterany')
    row=(proof or {}).get('native_records',{}).get(continued['job_id'],{})
    require(row.get('state')=='FAILED' and row.get('exit_code')=='1:0' and row.get('cpus')==1
        and row.get('gpus')==0 and row.get('elapsed_seconds')==188,'Positive final V2 preparation accounting required')
    return proof


def reconcile_accepted_prepare(c,pack,partial):
    from horsehair import slurm
    from horsehair.core import exclusive_file_lock
    continued=partial['continued_attempt']
    require(locator(pack['plan_path'])==partial['af_plan'] and locator(pack['ledger_path'])==continued['current_ledger']
        and read(partial['af_plan'])==pack['plan'],'Exact unchanged partial AF plan and ledger required')
    old_manifest=read(partial['old_source_manifest']);old_support=Path(partial['old_source_manifest']['path']).parent
    old_name='run-census-resilience-handoff-v1-20261024.py'
    old=loaded(old_support/old_name,old_manifest['files'][old_name],'preserved_failed_prepare_source')
    old_context={'descriptor':{'handoff_qualification':partial['old_qualification']},
        'manifest_binding':partial['old_source_manifest'],'pins':{}}
    old.validate_own_qualification(old_context);c['pins'].update(old_context['pins'])
    bindings=read(partial['native_bindings']);reg=bindings['registration'];old_plan=read(reg['plan'])
    old_task=next(t for t in old_plan['tasks'] if t['name']==reg['task_name']);tokens=shlex.split(old_task['command'])
    require(tokens.count(str(old_support/old_name))==1,'Exact original failed helper invocation required')
    tail=tokens[tokens.index(str(old_support/old_name))+1:];args=old.parser().parse_args(tail)
    actual,_=old.own_registration(args,partial['failed_job_id'],tail,old_manifest)
    require(actual==reg and args.mode==bindings.get('mode')=='prepare','Original failed preparation registration changed')
    old.own_fields(bindings['allocation'],partial['failed_job_id'],old_task,'prepare')
    failure=read(partial['failure'])
    require(failure.get('status')=='needs_explicit_reconciliation' and failure.get('mode')=='prepare'
        and failure.get('error')=='Mutation stderr requires explicit reconciliation'
        and failure.get('new_jobs')=={} and failure.get('cancelled_parents')==[]
        and all(failure.get(k) is False for k in FALSE_FLAGS),'Exact original pre-retirement refusal required')
    proof=slurm.completed_native_dependency(ROOT,partial['failed_job_id'],dependency_kind='afterany')
    row=(proof or {}).get('native_records',{}).get(partial['failed_job_id'],{})
    require(row.get('state')=='FAILED' and row.get('exit_code')=='1:0' and row.get('cpus')==1
        and row.get('gpus')==0 and row.get('elapsed_seconds')==162,'Positive final failed-producer accounting required')
    continued_proof=validate_continued_attempt(c,continued)
    require(continued['preserved_ledger']['sha256']==partial['af_ledger']['sha256'],'Exact original ambiguous ledger bytes required')
    plan=pack['plan'];ledger=read(continued['preserved_ledger']);intent=read(partial['submit_intent']);response=read(partial['submit_response'])
    reconciliation=read(continued['reconciliation_intent'])
    candidate=continued_registration_candidate(plan,ledger,read(continued['current_ledger']),intent,response,partial,reconciliation)
    job=partial['accepted_job_id'];task=plan['tasks'][0]
    observation=native(c,job,'accepted-held-reconciliation');scalar_fields(observation,task,job,hours=96,held=True)
    dependency_fields(observation,task,candidate,lambda ident:slurm.completed_native_dependency(ROOT,ident,dependency_kind='afterany'))
    rows=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(rows)=={job},'One exact held accepted allocation required');zero_pending(rows[job],job)
    for path,text in pack['scripts'].items():require(packet(path).decode()==text,'Exact partial executable bytes changed')
    lp=Path(pack['ledger_path']);require(str(lp) not in c['pins'],'Mutable reconciliation target cannot be an immutable source pin')
    with exclusive_file_lock(lp.with_suffix('.mutex'),timeout=30):
        packet(lp,continued['current_ledger']['sha256']);raw=packet(continued['preserved_ledger']['path'],partial['af_ledger']['sha256']);source_guard(c['pins'])
        with (c['area']/'preserved-ambiguous-ledger.json').open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        save(c['area']/'accepted-ledger-reconciliation-intent.json',{'accepted_job_id':job,'old_ledger':partial['af_ledger'],
            'candidate_canonical_sha256':canonical(candidate),'native_held':observation,'native_zero_allocation':rows[job],
            'submit_command_replayed':False,**FALSE_FLAGS})
        require(read(locator(lp))==candidate,'Previously reconciled row changed; no update or resubmission permitted')
    selected_held(c,task,job,candidate)
    save(c['area']/'accepted-ledger-reconciliation.json',{'status':'accepted_held_job_registered_without_resubmission',
        'accepted_job_id':job,'preserved_ledger':locator(c['area']/'preserved-ambiguous-ledger.json'),
        'reconciled_ledger':locator(lp),'failed_native_completion':proof,'continued_native_completion':continued_proof,**FALSE_FLAGS})


def prepare(c):
    from horsehair import slurm
    source_guard(c['pins']);packs={};new_jobs={};bindings={};partial=read(locator(SUPPORT/'partial-prepare-pins.json'))
    for kind in ('af','primary'):
        b=c['consumer_builder'].binding(kind,manifest={'path':str(CONSUMER_SUPPORT/'source-manifest.json'),'sha256':CONSUMER_MANIFEST_SHA},
            qualification=CONSUMER_Q,provider_schedule=c['schedule'],legacy_policy=c['legacy_policy_bindings'][kind])
        bp=CONSUMER_SUPPORT/(kind+'-consumer-binding.json')
        if kind=='af':require(locator(bp)==partial['af_binding'] and read(partial['af_binding'])==b,'Exact preserved partial consumer binding required')
        else:save(bp,b)
        bindings[kind]=locator(bp)
        pack=c['consumer_builder'].render(c['old'][kind]['plan'],kind,locator(bp),b,c['consumer_manifest'],script_reader=lambda p:packet(p).decode())
        directory=Path(pack['plan_path']).parent
        if kind=='af':reconcile_accepted_prepare(c,pack,partial)
        else:
            require(not directory.exists() and not Path(pack['ledger_path']).exists(),'New consumer phase cannot replay')
            directory.mkdir(mode=0o700)
            for path,text in pack['scripts'].items():
                with Path(path).open('x',encoding='utf8',newline='\n') as f:f.write(text)
            save(pack['plan_path'],pack['plan'])
        def held_runner(command,**kwargs):
            require(command[0]=='sbatch','Only exact source-bound sbatch registration allowed')
            return mutate(c,'submit-held-'+Path(command[-1]).stem,[command[0],'--hold',*command[1:]],lambda:source_guard(c['pins']))
        ledger=slurm.submit(pack['plan_path'],pack['ledger_path'],runner=held_runner)
        pairs=[(task,ledger['tasks'][task['name']]['job_id']) for task in pack['plan']['tasks']]
        for task,job in pairs:new_held(c,task,job,ledger)
        if kind=='af':amend_first_af(c,*pairs[0],ledger)
        new_jobs[kind]=pairs;packs[kind]={'plan':locator(pack['plan_path']),'ledger':locator(pack['ledger_path']),'binding':bindings[kind]}
    jobs=[job for values in new_jobs.values() for _,job in values]
    require(len(set(jobs))==8 and not set(jobs)&{j for ids in OLD.values() for j in ids},'Eight distinct fresh held replacement allocations required')
    result={'status':'eight_source_bound_replacements_held_pending_fresh_provider_and_retirement','native_job_id':c['job'],
        'descriptor':c['descriptor_binding'],'source_manifest':c['manifest_binding'],'packs':packs,
        'native_registration':c['registration'],'allocation':c['allocation'],
        'new_jobs':{kind:[job for _,job in pairs] for kind,pairs in new_jobs.items()},'consumer_qualification':CONSUMER_Q,**FALSE_FLAGS}
    save(c['area']/'prepared-receipt.json',result);return result


def preparation_origin_context(c,origin,manifest,descriptor):
    require(origin.get('kind')=='qualified-v3-prepared-origin-v1'
        and set(descriptor)==set(c['descriptor'])=={'kind','consumer_qualification','provider_schedule','handoff_qualification','processes','flags'}
        and descriptor.get('handoff_qualification')==origin['qualification']
        and {k:v for k,v in descriptor.items() if k!='handoff_qualification'}==
            {k:v for k,v in c['descriptor'].items() if k!='handoff_qualification'},
        'Prepared origin must preserve every provider, consumer, process and flag binding')
    return {**c,'manifest':manifest,'manifest_binding':origin['source_manifest'],
        'descriptor':descriptor,'descriptor_binding':origin['descriptor']}


def load_prepared(c,binding):
    """Use the unchanged qualified V3 receiver for its completed producer."""
    from horsehair import slurm
    origin=read(locator(SUPPORT/'prepared-origin-pins.json'))
    require(binding==origin['prepared_receipt'],'Only the actual completed V3 preparation may be inherited')
    manifest=read(origin['source_manifest']);support=Path(origin['source_manifest']['path']).parent
    name='run-census-resilience-handoff-v3-20261024.py'
    old=loaded(support/name,manifest['files'][name],'qualified_v3_preparation_receiver')
    previous={'descriptor':{'handoff_qualification':origin['qualification']},
        'manifest_binding':origin['source_manifest'],'pins':{}}
    old.validate_own_qualification(previous);c['pins'].update(previous['pins'])
    original_descriptor=read(origin['descriptor'])
    context=preparation_origin_context(c,origin,manifest,original_descriptor)
    failed=origin['failed_activation'];failure=read(failed['failure'])
    require(failed['job_id']=='7049022' and Path(failed['failure']['path'])==support/'native-7049022/failure.json'
        and failure.get('mode')=='activate' and failure.get('error')=='Ordinary confined immutable packet required'
        and failure.get('cancelled_parents')==[] and failure.get('new_jobs')==read(binding)['new_jobs']
        and all(failure.get(k) is False for k in FALSE_FLAGS),'Exact pre-retirement V3 scope refusal required')
    directory=Path(failed['failure']['path']).parent;entries=list(directory.iterdir())
    require(len(entries)<=5000 and not (directory/'pre-retirement-checkpoint.json').exists()
        and not any(re.search(r'-(?:cancel-|release-new-).*intent[.]json$',p.name) for p in entries),
        'Prior retirement or release requires separate explicit reconciliation')
    proof=slurm.completed_native_dependency(ROOT,failed['job_id'],dependency_kind='afterany')
    row=(proof or {}).get('native_records',{}).get(failed['job_id'],{})
    require(row.get('state')=='FAILED' and row.get('exit_code')=='1:0' and row.get('cpus')==1
        and row.get('gpus')==0 and row.get('elapsed_seconds')==213,'Positive final V3 activation accounting required')
    old.load_prepared(context,binding)
    for key in ('new_jobs','new_ledgers','prepared'):c[key]=context[key]
    c['pins'].update(context['pins']);c['pins'][str(SUPPORT/'prepared-origin-pins.json')]=digest(SUPPORT/'prepared-origin-pins.json')
    source_guard(c['pins'])


def validate_prior_backup_command(job,task,intent,response,failed_job):
    require(job in {j for ids in OLD.values() for j in ids[1:]}
        and intent=={'automatic_replay_permitted':False,'native_job_id':failed_job,
            'command':['scancel','--ctld','--state=PENDING','--user=pwa209','--name=hhw-'+task['name'],job]}
        and response=={'returncode':0,'stdout':'','stderr':''},'Exact successful prior pending-only retirement required')


def failed_activation_terminal(row,job):
    owned_record(row,job)
    require(job=='7058744' and row.get('state')=='FAILED' and row.get('exit_code')=='1:0' and row.get('cpus')==1
        and row.get('unallocated_metadata') is False and row.get('elapsed_seconds')==646,
        'Exact positive failed-actor final accounting required')


def rebind_checkpoint_history(c,pre,old_manifest):
    """Move one byte-identical source witness; preserve every other checkpoint."""
    old=ROOT/'maintenance/study-census-resilience-handoff-v4-20261024/primary-history-pins.json'
    new=SUPPORT/'primary-history-pins.json';paths=dict(pre['checked_paths'])
    expected=old_manifest['files']['primary-history-pins.json']
    require(c['manifest']['files']['primary-history-pins.json']==expected and paths.get(str(old))==expected
        and str(new) not in paths,'Only the exact original source-witness namespace may be rebound')
    source_guard(paths,pre['absent_paths']);packet(new,expected)
    del paths[str(old)];paths[str(new)]=expected
    require(len(paths)==len(pre['checked_paths']),'Witness relocation may not add or drop a checkpoint')
    save(c['area']/'checkpoint-witness-relocation.json',{'old':{'path':str(old),'sha256':expected},
        'new':{'path':str(new),'sha256':expected},'all_other_checkpoint_paths_and_hashes_unchanged':True,
        'original_checkpoint_bytes_verified_unchanged':True,**FALSE_FLAGS})
    return paths


def validate_checkpoint_refusal(c):
    binding=locator(SUPPORT/'checkpoint-continuation-pins.json');value=read(binding)
    require(set(value)=={'kind','failed_job_id','source_manifest','qualification','descriptor','native_bindings',
        'registration','failure','prior_retirements','checkpoint_diff','flags'}
        and value['kind']=='checkpoint-witness-relocation-continuation-v1' and value['failed_job_id']=='7069610'
        and value['flags']==FALSE_FLAGS,'Exact nil-mutation checkpoint refusal closure required')
    support=ROOT/'maintenance/study-census-resilience-handoff-v5-20261024';case=support/'native-7069610'
    require(value['source_manifest']=={'path':str(support/'source-manifest.json'),
        'sha256':'3bb6738ff4d1597d94b4fe76295476005859c60e0449dbdd8e14cb585de4aef0'},'Exact preserved V5 source required')
    manifest=read(value['source_manifest']);name='run-census-resilience-handoff-v5-20261024.py'
    previous=loaded(support/name,manifest['files'][name],'qualified_checkpoint_refusal_origin')
    _,pins=previous.source_manifest(value['source_manifest'])
    previous.validate_own_qualification({'descriptor':{'handoff_qualification':value['qualification']},
        'manifest_binding':value['source_manifest'],'pins':pins})
    native=read(value['native_bindings']);reg=value['registration'];descriptor=read(value['descriptor'])
    require(native['mode']=='activate' and native['source_manifest']==value['source_manifest']
        and native['registration']==reg and reg['native_job_id']=='7069610','Exact refused V5 invocation required')
    require({k:v for k,v in descriptor.items() if k!='handoff_qualification'}
        =={k:v for k,v in c['descriptor'].items() if k!='handoff_qualification'}
        and descriptor['handoff_qualification']==value['qualification'],'Checkpoint continuation may change only its own qualification')
    plan=read(reg['plan']);task=next(t for t in plan['tasks'] if t['name']==reg['task_name'])
    tokens=shlex.split(task['command']);entry=str(support/name);require(tokens.count(entry)==1,'Exact refused V5 entry required')
    argv=tokens[tokens.index(entry)+1:];args=previous.parser().parse_args(argv)
    origin=read(locator(SUPPORT/'prepared-origin-pins.json'))
    require(args.mode=='activate' and {'path':str(args.descriptor),'sha256':args.descriptor_sha256}==value['descriptor']
        and {'path':str(args.prepared_receipt),'sha256':args.prepared_receipt_sha256}==origin['prepared_receipt'],
        'Exact preserved descriptor and eight-job preparation required')
    actual,_=previous.own_registration(args,'7069610',argv,manifest)
    require(actual==reg,'Refused V5 registration changed');previous.own_fields(native['allocation'],'7069610',task,'activate')
    row=c['native_reader'](['7069610'],{},owner='pwa209',account='def-ptewarie_cpu')['7069610'];owned_record(row,'7069610')
    require(row.get('state')=='FAILED' and row.get('exit_code')=='1:0' and row.get('cpus')==1
        and row.get('elapsed_seconds')==487 and row.get('unallocated_metadata') is False,'Exact positive V5 final accounting required')
    failure=read(value['failure']);prior=read(value['prior_retirements']);diff=read(value['checkpoint_diff'])
    require(failure.get('status')=='needs_explicit_reconciliation' and failure.get('mode')=='activate'
        and failure.get('cancelled_parents')==[] and failure.get('new_jobs')=={k:[j for _,j in v] for k,v in c['new_jobs'].items()}
        and failure.get('error')=='Controller checkpoint/worker roster changed while held'
        and failure.get('error_type')=='ValueError' and all(failure.get(k) is False for k in FALSE_FLAGS),
        'Exact pre-mutation checkpoint comparison refusal required')
    entries=list(case.iterdir());require(len(entries)<=5000 and not any('-cancel-active-' in p.name or '-release-new-' in p.name for p in entries)
        and not (case/'retirement-proof.json').exists() and not (case/'receipt.json').exists(),'Prior mutation requires separate reconciliation')
    require(prior.get('failed_actor')=='7058744' and prior.get('scheduler_mutations_repeated') is False
        and set(prior.get('retired_jobs',[]))=={j for ids in OLD.values() for j in ids[1:]}
        and all(prior.get(k) is False for k in FALSE_FLAGS),'Exact preserved six-backup reconciliation required')
    history=manifest['files']['primary-history-pins.json']
    require(diff.get('source_manifest')==value['source_manifest'] and diff.get('all_old_checkpoint_bytes_unchanged') is True
        and diff.get('old_checkpoint_paths')==990 and diff.get('no_active_cancel_or_new_release_intent') is True
        and diff.get('scheduler_mutations')==0 and diff.get('changed_common_primary_paths')=={}
        and diff.get('added_primary_paths')=={str(support/'primary-history-pins.json'):history}
        and diff.get('old_history_witness_paths')=={str(ROOT/'maintenance/study-census-resilience-handoff-v4-20261024/primary-history-pins.json'):history},
        'Only the proven byte-identical history-witness relocation is permitted')
    for key in ('source_manifest','descriptor','native_bindings','failure','prior_retirements','checkpoint_diff'):
        pins[value[key]['path']]=value[key]['sha256']
    for key in ('plan','ledger','script'):pins[reg[key]['path']]=reg[key]['sha256']
    pins[binding['path']]=binding['sha256'];source_guard(pins);c['pins'].update(pins)
    save(c['area']/'checkpoint-refusal-reconciled.json',{'origin':binding,'prior_actor':'7069610',
        'prior_active_cancellations':0,'prior_new_releases':0,**FALSE_FLAGS})


def reconcile_retired_backups(c):
    """Preserve the six acknowledged retirements from the exact failed actor."""
    binding=locator(SUPPORT/'retirement-continuation-pins.json');value=read(binding)
    require(set(value)=={'kind','failed_job_id','source_manifest','qualification','descriptor','native_bindings',
        'registration','failure','pre_retirement_checkpoint','failed_process_probe','cancelled_backups','flags'}
        and value['kind']=='six-backup-retirement-continuation-v1' and value['failed_job_id']=='7058744'
        and value['flags']==FALSE_FLAGS,'Exact six-backup continuation closure required')
    support=ROOT/'maintenance/study-census-resilience-handoff-v4-20261024'
    require(value['source_manifest']=={'path':str(support/'source-manifest.json'),
        'sha256':'cdd79adb560680d1a4460ac7098016807d1e4e40b3c3db66a4a0a02ecaa9389a'},'Exact preserved V4 source required')
    old_manifest=read(value['source_manifest']);old_name='run-census-resilience-handoff-v4-20261024.py'
    old=loaded(support/old_name,old_manifest['files'][old_name],'qualified_six_backup_retirement_origin')
    _,pins=old.source_manifest(value['source_manifest'])
    old.validate_own_qualification({'descriptor':{'handoff_qualification':value['qualification']},
        'manifest_binding':value['source_manifest'],'pins':pins})
    native=read(value['native_bindings']);reg=value['registration'];previous=read(value['descriptor'])
    require(native.get('mode')=='activate' and native.get('source_manifest')==value['source_manifest']
        and native.get('registration')==reg and reg.get('native_job_id')==value['failed_job_id'],
        'Exact failed actor native invocation required')
    require({k:v for k,v in previous.items() if k!='handoff_qualification'}
        =={k:v for k,v in c['descriptor'].items() if k!='handoff_qualification'}
        and previous['handoff_qualification']==value['qualification'],'Continuation may change only its own source qualification')
    plan=read(reg['plan']);task=next(t for t in plan['tasks'] if t['name']==reg['task_name'])
    tokens=shlex.split(task['command']);entry=str(support/old_name)
    require(tokens.count(entry)==1,'Exact failed actor entry required')
    argv=tokens[tokens.index(entry)+1:];args=old.parser().parse_args(argv)
    origin=read(locator(SUPPORT/'prepared-origin-pins.json'))
    require(args.mode=='activate' and {'path':str(args.descriptor),'sha256':args.descriptor_sha256}==value['descriptor']
        and {'path':str(args.prepared_receipt),'sha256':args.prepared_receipt_sha256}==origin['prepared_receipt'],
        'Exact failed activation descriptor and original eight-job preparation required')
    actual,_=old.own_registration(args,value['failed_job_id'],argv,old_manifest)
    require(actual==reg,'Failed activation registration changed');old.own_fields(native['allocation'],value['failed_job_id'],task,'activate')
    rows=c['native_reader']([value['failed_job_id']],{},owner='pwa209',account='def-ptewarie_cpu')
    row=rows[value['failed_job_id']];failed_activation_terminal(row,value['failed_job_id'])
    failure=read(value['failure']);probe=read(value['failed_process_probe'])
    require(failure.get('status')=='needs_explicit_reconciliation' and failure.get('mode')=='activate'
        and failure.get('cancelled_parents')==[] and failure.get('new_jobs')=={k:[j for _,j in v] for k,v in c['new_jobs'].items()}
        and failure.get('error_type')=='CalledProcessError' and all(failure.get(k) is False for k in FALSE_FLAGS)
        and probe.get('returncode')==1 and probe.get('stdout')==''
        and 'Controller is not positively sleeping without children' in probe.get('stderr',''),
        'Exact pre-active-retirement process-observation refusal required')
    case=support/('native-'+value['failed_job_id']);entries=list(case.iterdir())
    require(len(entries)<=5000 and not any('-cancel-active-' in p.name or '-release-new-' in p.name for p in entries)
        and not (case/'retirement-proof.json').exists() and not (case/'receipt.json').exists(),
        'Any prior active cancellation or release needs a separate continuation')
    expected={j for ids in OLD.values() for j in ids[1:]}
    require(set(value['cancelled_backups'])==expected,'All six and only six prior backup retirements required')
    for job,commands in value['cancelled_backups'].items():
        require(set(commands)=={'intent','response'},'Both prior retirement intent and response required')
        validate_prior_backup_command(job,c['old_tasks'][job],read(commands['intent']),read(commands['response']),value['failed_job_id'])
        for packet_binding in commands.values():pins[packet_binding['path']]=packet_binding['sha256']
    pre=read(value['pre_retirement_checkpoint'])
    require(pre['primary_workers']['status']=='all_registered_primary_workers_positively_terminal'
        and pre['primary_workers']['workers']==171 and len(pre['af_worker_native_records'])==233
        and len(pre['checked_paths'])==990 and isinstance(pre['absent_paths'],list),
        'Complete original pre-retirement checkpoint required')
    def verify_retired(job):
        require(job in expected,'Unknown prior retirement refused');source_guard(pins)
        observed=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu')
        cancelled_zero(observed[job],job)
        require(job not in strict(packet(ROOT/'state/resource-budget.json')).get('leases',{}),'A retired backup owns a lease')
    for job in sorted(expected):verify_retired(job)
    for key in ('source_manifest','descriptor','native_bindings','failure','pre_retirement_checkpoint','failed_process_probe'):
        pins[value[key]['path']]=value[key]['sha256']
    for key in ('plan','ledger','script'):pins[reg[key]['path']]=reg[key]['sha256']
    pins[binding['path']]=binding['sha256'];source_guard(pins);c['pins'].update(pins)
    validate_checkpoint_refusal(c)
    c['checkpoint_paths']=rebind_checkpoint_history(c,pre,old_manifest);c['checkpoint_absent']=pre['absent_paths']
    c['reconciled_retired_backups']=expected;c['verify_retired_backup']=verify_retired
    save(c['area']/'prior-six-backup-retirements-reconciled.json',{'origin':binding,'failed_actor':value['failed_job_id'],
        'retired_jobs':sorted(expected),'scheduler_mutations_repeated':False,**FALSE_FLAGS})


def remote_probe(c,kind,job):
    hint=c['descriptor']['processes'][kind];require(type(hint)==dict and set(hint)=={'pid'} and re.fullmatch('[1-9][0-9]*',str(hint['pid'])),'Exact root-selected process identity required')
    task=c['old_tasks'][job];observation=native(c,job,'live-parent')
    scalar_fields(observation,task,job,hours=96 if kind=='af' else 72)
    require(observation.get('JobState')=='RUNNING' and re.fullmatch('nl[0-9]+',observation.get('NodeList','')),'One exact live Narval node required')
    command=['srun','--jobid='+job,'--overlap','--nodes=1','--ntasks=1','--cpus-per-task=1','--cpu-bind=none',
        sys.executable,'-s','-B','-u',str(SUPPORT/SELF),'--mode','process-probe','--source-manifest',c['manifest_binding']['path'],
        '--source-manifest-sha256',c['manifest_binding']['sha256'],'--expected-job',job,'--expected-pid',str(hint['pid']),
        '--expected-node',observation['NodeList'],'--expected-command',task['command']]
    c['sequence']+=1;prefix=c['area']/(str(c['sequence']).zfill(5)+'-process-probe-'+job)
    save(str(prefix)+'-intent.json',{'command':command,'operation':'owned allocated read-only process metadata step','maximum_seconds':45})
    response=subprocess.run(command,text=True,capture_output=True,timeout=45)
    save(str(prefix)+'-response.json',{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr})
    response.check_returncode();require(not response.stderr,'Owned process probe stderr refused')
    proof=strict(response.stdout);require(proof.get('job_id')==job and proof.get('node')==observation['NodeList'],'Process proof allocation/node differs')
    two_samples(proof['first'],proof['second']);require(time.time()-proof['second']['sampled_unix']<=15,'Process observation is stale');return proof


def af_workers(c):
    """All historical scalar worker journals, including superseded retries.

    The qualified controller's native_plan creates one scalar task per journal;
    the thirteen most recent jobs are only a subset of this complete history.
    """
    result={};pins={};paths=sorted(AF_CASE.glob('submission-*.json'))
    require(0<len(paths)<=5000,'Complete bounded historical AF journal roster required')
    campaign=read(locator(AF_CASE/'campaign-plan.json'));campaign_sha=campaign['plan_sha256']
    require(canonical({k:v for k,v in campaign.items() if k!='plan_sha256'})==campaign_sha
        and campaign.get('scientific_result') is False and campaign.get('admission_authority') is False,
        'Original sealed AF campaign metadata identity required')
    state_path=AF_CASE/'state.json.gz';state_binding=read(locator(AF_CASE/'state-binding.json'))
    require(state_binding.get('state_file_sha256')==digest(state_path),'Exact AF current state file binding required')
    packet(state_path,state_binding['state_file_sha256'],64_000_000)
    with gzip.open(state_path,'rb') as stream:state_raw=stream.read(64_000_001)
    require(len(state_raw)<=64_000_000,'Bounded AF state metadata exceeded')
    state=strict(state_raw)
    require(state.get('state_sha256')==state_binding.get('state_sha256') and state.get('plan_sha256')==campaign_sha
        and canonical({k:v for k,v in state.items() if k!='state_sha256'})==state.get('state_sha256')
        and state_binding.get('campaign_plan_sha256')==campaign_sha and isinstance(state.get('jobs'),dict)
        and all(value.get(key) is False for value in (state,state_binding) for key in ('scientific_result','admission_authority')),
        'Current AF state/campaign metadata binding required')
    current_ids=[entry.get('job_id') for entry in state['jobs'].values()]
    require(current_ids and all(isinstance(job,str) and re.fullmatch('[1-9][0-9]*',job) for job in current_ids)
        and len(set(current_ids))==len(current_ids),'Complete distinct current AF state native identities required')
    for path in paths:
        record=read(locator(path));require(record.get('status')=='released','Ambiguous AF worker submission/release prevents retirement')
        job=record.get('job_id');require(isinstance(job,str) and re.fullmatch('[1-9][0-9]*',job) and job not in result,'Distinct submitted AF worker identity required')
        pp=Path(record['plan_path']);lp=Path(record['ledger_path']);plan=read(locator(pp));ledger=read(locator(lp))
        phase=plan.get('phase');intent=record.get('intent',{});resources=intent.get('resources',{})
        require(isinstance(phase,str) and re.fullmatch('[a-z0-9-]+',phase) and pp==ROOT/'plans'/phase/'plan.json'
            and lp==ROOT/'state'/(phase+'-submission.json') and plan.get('root')==str(ROOT)
            and record['plan_sha256']==digest(pp) and ledger.get('plan_sha256')==canonical(plan)
            and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG
            and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256')==SOURCE
            and len(plan.get('tasks',[]))==1 and len(ledger.get('tasks',{}))==1,
            'Exact source/config-bound scalar AF journal/plan/ledger required')
        require(plan.get('campaign_plan_sha256')==intent.get('campaign_plan_sha256')==campaign_sha
            and plan.get('intent_sha256')==intent.get('intent_sha256') and re.fullmatch('[0-9a-f]{64}',str(plan.get('intent_sha256')))
            and canonical({k:v for k,v in intent.items() if k!='intent_sha256'})==intent['intent_sha256']
            and intent.get('scientific_result') is False and intent.get('admission_authority') is False
            and intent.get('kind') in {'training','evaluation','assembly'} and type(resources.get('cpus')) is int and resources['cpus']>0
            and type(resources.get('gpus')) is int and resources['gpus'] in {0,1}
            and type(resources.get('hours')) in {int,float} and 0<resources['hours']<=96,
            'Exact original AF campaign intention/resources required')
        task=plan['tasks'][0];row=ledger['tasks'][task['name']]
        expected_resources={'cpus':resources['cpus'],'gpu':bool(resources['gpus']),'hours':resources['hours'],
            'memory':'64G' if intent['kind']=='training' else '32G'}
        text=packet(task['script']).decode()
        require(task.get('name')==phase and task.get('script')==str(pp.parent/(phase+'.sbatch'))
            and task.get('resources')==expected_resources and not task.get('depends') and not task.get('external_dependencies')
            and task.get('dependency_kind')=='afterok' and isinstance(task.get('command'),str) and task['command'] in text
            and '\r' not in text and row.get('state')=='submitted' and row.get('job_id')==job
            and task['script_sha256']==row.get('script_sha256')==canonical(text),
            'AF worker actual registration changed')
        result[job]={'parent_job_id':job,'resources':task['resources']}
        pins.update({str(path):digest(path),str(pp):digest(pp),str(lp):digest(lp),task['script']:digest(task['script'])})
    require(set(current_ids)<=set(result),'A current AF native worker has no confirmed historical submission journal')
    observed=c['native_reader'](sorted(result),{},owner='pwa209',account=None)
    require(set(observed)==set(result),'Every registered AF worker needs exact native proof')
    roster={'status':'complete_source_bound_primary_worker_roster','expected_worker_ids':sorted(result),'workers':result,'array_rosters':{}}
    terminal=c['roster_module'].verify_primary_terminal(roster,observed)
    pins.update({str(AF_CASE/'campaign-plan.json'):digest(AF_CASE/'campaign-plan.json'),str(state_path):digest(state_path),
        str(AF_CASE/'state-binding.json'):digest(AF_CASE/'state-binding.json')})
    return {'checked_paths':pins,'native_records':observed,'terminal_proof':terminal,'historical_journals':len(paths),
        'current_state_workers':len(current_ids)}


def make_activation(c):
    roster_module=loaded(SUPPORT/'primary-roster.py',c['manifest']['files']['primary-roster.py'],'exact_primary_worker_roster')
    c['roster_module']=roster_module
    c['cancelled_parents']=set();c['checkpoint_proof']={}
    def records(ids):return c['native_reader'](list(ids),{},owner='pwa209',account='def-ptewarie_cpu')
    def already(job):
        row=records([job])[job]
        if row.get('state')=='COMPLETED':parent_terminal(row,job);return True
        require(row.get('state')=='RUNNING','Parent state changed unexpectedly; reconcile before cancellation');return False
    def verify_backup(job):
        source_guard(c['pins']);task=c['old_tasks'][job];kind='af' if job in OLD['af'] else 'primary'
        scalar_fields(native(c,job,'old-backup-held'),task,job,hours=96 if kind=='af' else 72,held=True)
        zero_pending(records([job])[job],job)
        require(job not in strict(packet(ROOT/'state/resource-budget.json')).get('leases',{}),'Pending backup acquired a lease')
    def verify_quiescent():
        source_guard(c['pins']);negative=old_provider_negative(c);current=fresh_new_provider(c)
        active={kind:ids[0] for kind,ids in OLD.items() if not already(ids[0])}
        old_status=strict(packet(AF_CASE/'status.json'));primary_status=strict(packet(PIPELINE_CASE/'status.json'))
        if 'af' in active:pending_status(old_status,primary_status)
        else:
            # The AF allocation may finish its qualified four-hour reserve
            # naturally while waiting for the external provider.
            pending_primary_status(primary_status)
        marker=read({'path':str(PIPELINE_CASE/'controller.lock'),'sha256':PRIMARY_MARKER_SHA})
        require(str(marker.get('job_id'))==OLD['primary'][0] and marker.get('segment')==0,'Exact already-entered primary owner required')
        preservation=read(locator(PRIMARY_SUPPORT/('native-'+OLD['primary'][0])/'source-preservation.json'))
        require(preservation.get('normal_branch_ast_preserved') is True and preservation.get('nested_storage_guard_ast_preserved') is True
            and preservation.get('pipeline_source_sha256')==c['primary'].PIPELINE_SHA,'Original primary operational source preservation required')
        primary_roster=roster_module.primary_worker_roster(ROOT,PRIMARY_RID)
        require(primary_roster.get('pipeline_path_override') is False,'Native discovery may not override original primary pipeline')
        primary_records=c['native_reader'](primary_roster['query_ids'],primary_roster['array_rosters'],owner='pwa209',account=None)
        primary_terminal=roster_module.verify_primary_terminal(primary_roster,primary_records)
        af=af_workers(c);snap=checkpoint(AF_CASE);psnap=checkpoint(PIPELINE_CASE,primary=True)
        stable={**snap['checked_paths'],**psnap['checked_paths'],**primary_roster['checked_paths'],**af['checked_paths']}
        if c.get('checkpoint_paths') is None:c['checkpoint_paths']=stable;c['checkpoint_absent']=primary_roster['absent_paths']
        else:require(stable==c['checkpoint_paths'] and primary_roster['absent_paths']==c['checkpoint_absent'],'Controller checkpoint/worker roster changed while held')
        source_guard(c['checkpoint_paths'],c['checkpoint_absent'])
        budget=strict(packet(ROOT/'state/resource-budget.json'))
        for kind,ids in OLD.items():
            entry=budget.get('leases',{}).get(ids[0]);require(type(entry) is dict and entry.get('cpus')==1 and entry.get('gpus')==0
                and entry.get('max_hours')==(96 if kind=='af' else 72),'Exact conservative old parent lease binding required')
            if kind in active:require(entry.get('state')=='running','Live parent original lease changed')
            else:require(entry.get('state')=='native_reconciled','Naturally completed parent needs real native lease reconciliation')
        for ids in OLD.values():
            for job in ids[1:]:require(job not in budget.get('leases',{}),'Old backup owns a lease')
        probes={kind:remote_probe(c,kind,job) for kind,job in active.items()}
        # Fresh post-probe checks close the observation window. The immutable
        # expired-old-provider proof prevents subsequent wakeups admitting work.
        source_guard(c['checkpoint_paths'],c['checkpoint_absent']);source_guard(c['pins'])
        c['checkpoint_proof']={'closed_old_provider':negative,'current_new_provider':current,'processes':probes,
            'primary_workers':primary_terminal,'af_worker_native_records':af['native_records'],'checked_paths':c['checkpoint_paths'],
            'absent_paths':c['checkpoint_absent'],'primary_owner':{'path':str(PIPELINE_CASE/'controller.lock'),'sha256':PRIMARY_MARKER_SHA}}
    def wait_terminal(job,parent):
        deadline=min(time.monotonic()+180,time.monotonic()+c['end']-time.time()-600)
        while True:
            row=records([job])[job]
            if row.get('state') in TERMINAL:
                if parent:parent_terminal(row,job,cancelled=job in c['cancelled_parents'])
                else:cancelled_zero(row,job)
                return row
            require(time.monotonic()<deadline,'Terminal accounting pending; no mutation replay or release');time.sleep(2)
    def final_proof():
        require(c.get('checkpoint_paths') is not None,'Retirement checkpoint has not been established')
        source_guard(c['pins']);source_guard(c['checkpoint_paths'],c['checkpoint_absent'])
        for kind,ids in OLD.items():
            parent_terminal(records([ids[0]])[ids[0]],ids[0],cancelled=ids[0] in c['cancelled_parents'])
            for job in ids[1:]:cancelled_zero(records([job])[job],job)
        # Original combined admission performs the existing native-accounting
        # reconciliation, including old conservative lease finalization.
        if c['registration_locked']:admission(c,'old-final-native-accounting')
        else:
            from horsehair.core import exclusive_file_lock
            with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=30):
                c['registration_locked']=True
                try:admission(c,'old-final-native-accounting-outside')
                finally:c['registration_locked']=False
        lease_guard(strict(packet(ROOT/'state/resource-budget.json')),{OLD['af'][0]:96,OLD['primary'][0]:72},
            [j for ids in OLD.values() for j in ids[1:]],final=True)
        for kind,pairs in c['new_jobs'].items():
            for task,job in pairs:new_held(c,task,job,c['new_ledgers'][kind])
    c.update(verify_quiescent=verify_quiescent,verify_backup=verify_backup,already_completed=already,
        wait_terminal=wait_terminal,final_proof=final_proof,
        verify_new_held=lambda task,job:new_held(c,task,job,c['new_ledgers'][task['census_resilience_consumer']['consumer_kind']]))


def passing_junit(binding,contract):
    raw=packet(binding['path'],binding['sha256']);require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Unsafe JUnit refused')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'));names=sorted(x.get('classname','').split('.')[-1]+'::'+x.get('name','') for x in cases)
    leaves=[x for x in tree.iter('testsuite') if not x.findall('testsuite')]
    require(len(cases)==len(set(names))==contract['tests'] and canonical(names)==contract['roster_sha256']
        and all(x.get('classname') and x.get('name') for x in cases)
        and not any(list(x.iter(k)) for x in cases for k in ('failure','error','skipped'))
        and leaves and sum(int(x.get('tests','-1')) for x in leaves)==contract['tests']
        and all(int(x.get(k,'0'))==0 for x in tree.iter() if x.tag in {'testsuite','testsuites'} for k in ('failures','errors','skipped')),
        'Actual complete passing handoff suite required')


def own_registration(args,job,argv,manifest):
    plan=read(locator(args.native_plan));ledger=read(locator(args.native_ledger));script=packet(args.native_script).decode()
    tasks=[t for t in plan['tasks'] if t['name']==args.native_task];require(len(tasks)==1,'One exact own native task required')
    task=tasks[0];row=ledger.get('tasks',{}).get(args.native_task,{})
    command=shlex.split(task.get('command',''));cpus,memory,hours=(4,'16G',1) if args.mode=='qualify' else (1,'64G',4)
    meta={'helper':{'path':str(SUPPORT/SELF),'sha256':manifest['files'][SELF]},'source_manifest':locator(args.source_manifest),
        'mode':args.mode,'descriptor':locator(args.descriptor) if args.descriptor is not None else None,
        'prepared_receipt':locator(args.prepared_receipt) if args.prepared_receipt is not None else None}
    require(plan.get('root')==str(ROOT) and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG
        and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256')==SOURCE
        and ledger.get('plan_sha256')==canonical(plan) and command.count(str(SUPPORT/SELF))==1
        and command[command.index(str(SUPPORT/SELF))+1:]==argv and task['script']==str(args.native_script)
        and task['command'] in script and canonical(script)==task.get('script_sha256')==row.get('script_sha256')
        and row.get('state')=='submitted' and row.get('job_id')==job
        and task.get('resources')=={'cpus':cpus,'memory':memory,'hours':hours,'gpu':False,'array':None}
        and task.get('census_resilience_handoff')==meta,'Exact own source/argv/mode/descriptor/native/resources registration required')
    if args.mode=='activate':
        issued=row.get('issued_native_dependencies',[]);done=row.get('completed_native_dependencies',[])
        require(task.get('dependency_kind')==row.get('dependency_kind')=='afterok'
            and FIRST_PROVIDER in set(issued)|{p.get('job_id') for p in done}
            and row.get('dependency_resolution_sha256')==canonical({'kind':'afterok','issued':issued,'completed':done}),
            'Actual activation must retain positive afterok first-provider dependency resolution')
    return {'plan':locator(args.native_plan),'ledger':locator(args.native_ledger),'script':locator(args.native_script),
        'task_name':args.native_task,'native_job_id':job},task


def own_fields(value,job,task,mode):
    cpus,memory,hours=(4,{'16G','16384M'},1) if mode=='qualify' else (1,{'64G','65536M'},4)
    require(value.get('JobId')==job and value.get('UserId')=='pwa209(3164768)' and value.get('Account')=='def-ptewarie_cpu'
        and value.get('JobState')=='RUNNING' and value.get('NumCPUs')==str(cpus) and value.get('NumNodes') in {'1','1-1'}
        and value.get('MinMemoryNode') in memory and seconds(value.get('TimeLimit',''))==hours*3600
        and value.get('Command')==task['script'] and value.get('JobName')=='hhw-'+task['name']
        and 'ArrayJobId' not in value and 'ArrayTaskId' not in value
        and all('gpu' not in value.get(k,'').lower() for k in ('ReqTRES','AllocTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact owned scalar native handoff/qualification allocation required')


def own_allocation(value,job,task,mode):
    own_fields(value,job,task,mode)
    end=datetime.fromisoformat(value.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp();env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(end-env)<=2 and time.time()+1200<end,'Actual own native deadline/finalization margin required');return min(end,env)


def qualify(c):
    temporary=Path(os.environ.get('SLURM_TMPDIR',''));require(temporary.is_absolute() and temporary.is_dir() and not temporary.is_symlink()
        and temporary.stat().st_uid==3164768 and not temporary.is_relative_to(ROOT),'Owned allocation-local generic fixtures required')
    work=temporary/('census-handoff-qualification-'+c['job']);work.mkdir(mode=0o700,exist_ok=False)
    suites=[]
    for scope,contract in c['manifest']['suites'].items():
        cwd=work/scope;cwd.mkdir(mode=0o700);xml=c['area']/(scope+'.xml')
        # pytest basetemp must be a strict child, never the child process cwd.
        command=[sys.executable,'-s','-B','-m','pytest',str(SUPPORT/contract['source']),'-q','-p','no:cacheprovider',
            '--basetemp',str(cwd/'fixtures'),'--junitxml',str(xml)]
        save(c['area']/(scope+'-intent.json'),{'command':command,'source_sha256':contract['source_sha256']})
        with (c['area']/(scope+'-stdout.txt')).open('xb') as out,(c['area']/(scope+'-stderr.txt')).open('xb') as err:
            p=subprocess.run(command,cwd=cwd,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},stdout=out,stderr=err,
                timeout=min(1200,c['end']-time.time()-600),check=False)
        require(p.returncode==0,'Actual native handoff generic suite failed; preserve source-bound logs')
        passing_junit(locator(xml),contract)
        suites.append({'scope':scope,'source':locator(SUPPORT/contract['source']),'junit':locator(xml),
            'test_counts':{'tests':contract['tests'],'failures':0,'errors':0,'skipped':0},'test_roster_sha256':contract['roster_sha256']})
    source_guard(c['pins'])
    result={'status':'complete_census_resilience_handoff_qualification','native_job_id':c['job'],'returncode':0,
        'source_manifest':c['manifest_binding'],'source_pins':c['pins'],'native_registration':c['registration'],
        'allocation':c['allocation'],'suites':suites,'positive_final_native_completion_required_before_activation':True,**FALSE_FLAGS}
    save(SUPPORT/'qualification.json',result);return result


def validate_own_qualification(c):
    from horsehair import slurm
    binding=c['descriptor']['handoff_qualification'];require(set(binding)=={'path','sha256','job_id'} and binding['path']==str(SUPPORT/'qualification.json'),'Exact own separately qualified source required')
    q=read({k:binding[k] for k in ('path','sha256')});proof=slurm.completed_native_dependency(ROOT,binding['job_id'])
    row=(proof or {}).get('native_records',{}).get(binding['job_id'],{})
    require(row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0' and row.get('cpus')==4 and row.get('gpus')==0
        and type(row.get('elapsed_seconds')) is int and 0<row['elapsed_seconds']<=3600,'Positive finalized4CPU1h handoff qualification required')
    manifest,pins=source_manifest(c['manifest_binding'])
    # Registration pins belong to the native qualification; their source/helper
    # invocation is revalidated below rather than trusting a completion flag.
    require(q.get('status')=='complete_census_resilience_handoff_qualification' and q.get('native_job_id')==binding['job_id']
        and q.get('returncode')==0 and q.get('source_manifest')==c['manifest_binding'] and all(q.get(k) is False for k in FALSE_FLAGS),
        'Complete actual source-bound qualification report required')
    reg=q['native_registration'];plan=read(reg['plan']);task=next(t for t in plan['tasks'] if t['name']==reg['task_name'])
    tokens=shlex.split(task['command']);require(tokens.count(str(SUPPORT/SELF))==1,'Exact native Q helper required')
    tail=tokens[tokens.index(str(SUPPORT/SELF))+1:];a=parser().parse_args(tail)
    require(a.mode=='qualify','Only generic qualification mode can qualify handoff sources')
    actual,_=own_registration(a,binding['job_id'],tail,manifest);require(actual==reg,'Actual Q registration changed')
    own_fields(q['allocation'],binding['job_id'],task,'qualify')
    expected={**pins,**{reg[k]['path']:reg[k]['sha256'] for k in ('plan','ledger','script')}}
    require(q.get('source_pins')==expected,'Qualification source/native-registration pins differ')
    require(len(q.get('suites',[]))==2 and {s['scope'] for s in q['suites']}==set(manifest['suites']),'Every native generic suite required')
    for suite in q['suites']:
        contract=manifest['suites'][suite['scope']]
        require(suite['source']==locator(SUPPORT/contract['source']) and suite.get('test_counts')=={'tests':contract['tests'],'failures':0,'errors':0,'skipped':0}
            and suite.get('test_roster_sha256')==contract['roster_sha256'],'Complete exact source-bound Q suite required')
        passing_junit(suite['junit'],contract);expected[suite['junit']['path']]=suite['junit']['sha256']
    expected[binding['path']]=binding['sha256'];source_guard(expected);c['pins'].update(expected)


def parser():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--mode',choices=('qualify','prepare','activate','process-probe'),required=True)
    for name in ('native-plan','native-ledger','native-script','source-manifest','descriptor','prepared-receipt'):p.add_argument('--'+name,type=Path)
    for name in ('native-task','source-manifest-sha256','descriptor-sha256','prepared-receipt-sha256','expected-job','expected-pid','expected-node','expected-command'):p.add_argument('--'+name)
    return p


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv);a=parser().parse_args(argv)
    os.environ['TZ']='UTC';time.tzset();require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209','Owned native operational user required')
    require(a.source_manifest is not None and a.source_manifest_sha256 is not None,'Exact own source closure required')
    mb={'path':str(a.source_manifest),'sha256':a.source_manifest_sha256};manifest,pins=source_manifest(mb)
    if a.mode=='process-probe':
        require(a.expected_job==os.environ.get('SLURM_JOB_ID') and socket.gethostname().split('.')[0]==a.expected_node,'Exact allocated old-job/node metadata step required')
        proof=quiescent_probe(a.expected_pid,a.expected_job,a.expected_command);source_guard(pins)
        print(json.dumps({'job_id':a.expected_job,'node':a.expected_node,**proof,'source_manifest':mb},sort_keys=True));return 0
    require(all(getattr(a,k) is not None for k in ('native_plan','native_ledger','native_task','native_script')),'Complete own native registration arguments required')
    if a.mode!='qualify':require(a.descriptor is not None and a.descriptor_sha256 is not None,'Exact allocated production descriptor required')
    if a.mode=='activate':require(a.prepared_receipt is not None and a.prepared_receipt_sha256 is not None,'Exact separate held-chain preparation receipt required')
    import horsehair
    from horsehair.core import native_job_id,source_identity,read_json,exclusive_file_lock
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py' and source_identity(FRAMEWORK)['sha256']==SOURCE,'Exact qualified4f metadata interpreter required')
    require(subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
    job=str(native_job_id() or '');require(re.fullmatch('[1-9][0-9]*',job),'Actual scalar own native job required')
    registration,task=own_registration(a,job,argv,manifest)
    for key in ('plan','ledger','script'):pins[registration[key]['path']]=registration[key]['sha256']
    area=SUPPORT/('native-'+job);require(not area.exists(),'This allocated invocation cannot replay prior evidence');area.mkdir(mode=0o700)
    for path,sha in ((GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA)):packet(path,sha,300_000)
    scheduler=runpy.run_path(str(SCHEDULER))['run_scontrol_job'];allocation=fields(scheduler(job,area/'own-native'))
    end=own_allocation(allocation,job,task,a.mode)
    def expired(signum,frame):raise TimeoutError('Native handoff finalization margin exhausted')
    signal.signal(signal.SIGALRM,expired);signal.alarm(max(1,int(end-time.time()-180)))
    runpy.run_path(str(GUARD))['install'](area/'accounting-reads')
    from horsehair import repair
    repair._repair_native_states=bounded_reader(repair._repair_native_states,end)
    config=read_json(FRAMEWORK/'configs/study.json');require(canonical(config)==CONFIG,'Full original configuration required')
    from horsehair.execution_resources import load_resource_policy
    from horsehair.budget import resource_lease
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    c={'job':job,'area':area,'end':end,'pins':pins,'manifest':manifest,'manifest_binding':mb,'registration':registration,
        'allocation':allocation,'scheduler':scheduler,'native_reader':repair._repair_native_states,'config':config,'policy':policy,
        'registration_locked':False,'sequence':0}
    save(area/'native-bindings.json',{'registration':registration,'allocation':allocation,'source_manifest':mb,'mode':a.mode,**FALSE_FLAGS})
    cpus,hours=(4,1) if a.mode=='qualify' else (1,4)
    try:
        with resource_lease(ROOT,config,cpus=cpus,gpus=0,max_hours=hours,job_id=job,resource_policy=policy):
            if a.mode=='qualify':result=qualify(c)
            else:
                c['descriptor_binding']={'path':str(a.descriptor),'sha256':a.descriptor_sha256};d=read(c['descriptor_binding'])
                require(set(d)=={'kind','consumer_qualification','provider_schedule','handoff_qualification','processes','flags'}
                    and d['kind']=='census-resilience-handoff-descriptor-v1' and d['consumer_qualification']==CONSUMER_Q
                    and d['flags']==FALSE_FLAGS and set(d['processes'])=={'af','primary'},'Exact source-bound controlled handoff descriptor required')
                c['descriptor']=d;c['pins'][str(a.descriptor)]=a.descriptor_sha256
                # Install the exact already-qualified registered-reader guard
                # before campaign imports bind the accounting reader.
                old,_=old_inputs();tokens=shlex.split(old['af']['plan']['tasks'][0]['command'])
                p=read({'path':tokens[tokens.index('--policy-descriptor')+1],'sha256':tokens[tokens.index('--policy-descriptor-sha256')+1]})
                descriptor=read(p['base_descriptor']);guard_binding=descriptor['registry_overlay']['guard']
                packet(guard_binding['path'],guard_binding['sha256'],300_000);registry=runpy.run_path(guard_binding['path'])
                registry['install'](area/'registered-reads',registration_mode='controller-already-locked',allocation_deadline_unix=end)
                from horsehair import campaign
                require(campaign._repair_native_states is repair._repair_native_states,'Qualified native guard must precede campaign binding')
                c['campaign']=campaign
                with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=120):
                    c['registration_locked']=True
                    try:
                        admission(c,'handoff-prerequisites');validate_own_qualification(c);libraries(c)
                        if a.mode=='prepare':result=prepare(c)
                        else:
                            load_prepared(c,{'path':str(a.prepared_receipt),'sha256':a.prepared_receipt_sha256})
                            # This is precisely the original startup/recovery
                            # guard; it does not claim to stop an existing loop.
                            with exclusive_file_lock(PIPELINE_CASE/'controller-recovery.lock',timeout=0):
                                make_activation(c);reconcile_retired_backups(c);retirement(c)
                            result={'status':'old_consumers_retired_before_external_release'}
                    finally:c['registration_locked']=False
                if a.mode=='activate':release_replacements(c);result=read(locator(area/'receipt.json'))
        signal.alarm(0);print(json.dumps(result,sort_keys=True),flush=True);return 0
    except BaseException as error:
        save(area/'failure.json',{'status':'needs_explicit_reconciliation','error_type':type(error).__name__,'error':str(error)[:4000],
            'mode':a.mode,'new_jobs':{k:[j for _,j in v] for k,v in c.get('new_jobs',{}).items()},
            'cancelled_parents':sorted(c.get('cancelled_parents',set())),'no_automatic_replay_or_cleanup':True,**FALSE_FLAGS});raise


if __name__=='__main__':raise SystemExit(main())
