"""Atomic resource leases within one study; stopped jobs require native reconciliation."""
from __future__ import annotations
import json
import math
import os
import sys
import time
import re
import subprocess
import errno
from datetime import datetime,timezone
from contextlib import contextmanager
from pathlib import Path
from .core import atomic_json, read_json,digest,exclusive_file_lock,canonical_hash,ReceiptLockTimeout


PERSONAL_SCRATCH = '/scratch/pwa209'
PERSONAL_STUDY_ROOT = Path(PERSONAL_SCRATCH)/'horsehair-worm'
STORAGE_HEADROOM_RECEIPT_INODES = 5


class StorageHeadroomError(ValueError):
    """A fresh personal quota cannot support the proposed bounded work."""
    def __init__(self, record):
        self.record=record
        super().__init__('Personal scratch storage is pending: '+'; '.join(record['reasons']))


def parse_personal_quota(text):
    """Parse default `lfs quota` units, without inventing unlimited capacity.

    The default block fields are KiB; inode fields are counts. Both positive
    soft and hard limits apply conservatively: an expiring grace period is not
    durable capacity for a new job. A zero limit means unlimited, not zero free.
    """
    rows=[line.split() for line in text.splitlines() if line.split() and line.split()[0]==str(PERSONAL_SCRATCH)]
    if len(rows)!=1 or len(rows[0])!=9:
        raise ValueError('Expected exactly one personal scratch quota row with default KiB/count units')
    fields=rows[0]
    values=[]
    for index in (1,2,3,5,6,7):
        if not re.fullmatch(r'\d+\*?',fields[index]):
            raise ValueError('Personal quota requires nonnegative integer default KiB/count units')
        values.append(int(fields[index].rstrip('*')))
    result={'quota_filesystem':str(PERSONAL_SCRATCH),'quota_units':'default KiB (1024 bytes) and inode counts',
            'quota_limit_policy':'minimum positive soft/hard limit; no grace-period capacity assumed'}
    for kind,offset,multiplier in (('bytes',0,1024),('files',3,1)):
        used,soft,hard=(value*multiplier for value in values[offset:offset+3])
        limits=[value for value in (soft,hard) if value>0]
        limit=min(limits) if limits else None
        result.update({f'personal_used_{kind}':used,f'personal_soft_limit_{kind}':soft or None,
                       f'personal_hard_limit_{kind}':hard or None,f'personal_effective_limit_{kind}':limit,
                       f'personal_available_{kind}':limit-used if limit is not None else None})
    return result


def storage_readiness(quota, *, required_bytes=0, required_files=0, reserve_bytes=0, reserve_files=0):
    """Assess explicit operational demand/reserve, independently of byte space.

    This is a point-in-time guard, not an inode reservation against other
    projects. The caller supplies a measured forecast/configured reserve and
    refreshes again before new write-heavy work; no scientific config changes.
    """
    demand={'required_bytes':required_bytes,'required_files':required_files,
            'reserve_bytes':reserve_bytes,'reserve_files':reserve_files}
    if any(type(value) is not int or value<0 for value in demand.values()):
        raise ValueError('Storage demand and reserve must be nonnegative integer byte/inode counts')
    reasons=[]
    for kind in ('bytes','files'):
        available=quota.get(f'personal_available_{kind}')
        need=demand[f'required_{kind}']+demand[f'reserve_{kind}']
        if available is None:
            reasons.append(f'Finite personal {kind} capacity has not been measured; zero quota limits are unlimited')
        elif type(available) is not int:
            raise ValueError('Personal available storage must contain integer native counts')
        elif available<need:
            label='inode' if kind=='files' else 'byte'
            reasons.append(f'Personal scratch {label} headroom is insufficient: {available} available, {need} required including reserve')
    return {'status':'ready' if not reasons else 'pending','ready':not reasons,'reasons':reasons,**demand,
            'personal_available_bytes':quota.get('personal_available_bytes'),
            'personal_available_files':quota.get('personal_available_files'),'is_capacity_reservation':False}


def _measure_personal_quota(runner):
    uid=os.getuid()
    command=['lfs','quota','-u',str(uid),str(PERSONAL_SCRATCH)]
    response=runner(command,text=True,capture_output=True,timeout=30)
    if response.returncode:raise RuntimeError('Personal quota measurement failed')
    return {**parse_personal_quota(response.stdout),'personal_uid':uid,'native_command':command,
            'native_stdout':response.stdout,'measured_unix':time.time(),
            'captured_utc':datetime.now(timezone.utc).isoformat()}


def require_storage_headroom(root, *, required_files, required_bytes=0,
                             reserve_bytes=0, reserve_files=0, runner=subprocess.run):
    """Refresh personal UID quota immediately and keep one durable receipt.

    Five metadata inodes cover the state directory, permanent kernel guard,
    informational owner record, receipt and one atomic staging file. Repeated
    checks reuse those paths. Concurrent RNA/controller writers serialize their
    fresh measurement and receipt. EDQUOT/ENOSPC are raised once, never retried.
    """
    root=Path(root).resolve()
    if root!=PERSONAL_STUDY_ROOT.resolve():raise ValueError('Storage headroom requires the authorized personal study namespace')
    if any(type(value) is not int or value<0 for value in (required_bytes,required_files,reserve_bytes,reserve_files)):
        raise ValueError('Storage demand and reserve must be nonnegative integer byte/inode counts')
    record=None
    try:
        with exclusive_file_lock(root/'state/storage-headroom.mutex',timeout=30):
            quota=_measure_personal_quota(runner)
            readiness=storage_readiness(quota,required_bytes=required_bytes,
                required_files=required_files+STORAGE_HEADROOM_RECEIPT_INODES,
                reserve_bytes=reserve_bytes,reserve_files=reserve_files)
            record={**quota,**readiness,'root':str(root),'planned_required_files':required_files,
                    'receipt_file_overhead':STORAGE_HEADROOM_RECEIPT_INODES}
            record['self_sha256']=canonical_hash(record)
            atomic_json(root/'state/storage-headroom.json',record)
    except OSError as error:
        if error.errno in (errno.EDQUOT,errno.ENOSPC):
            print(json.dumps({'storage_headroom':'receipt_write_failed','retryable':False,'errno':error.errno,
                              'message':str(error),'measured_storage':record}),file=sys.stderr,flush=True)
        raise
    if not record['ready']:
        raise StorageHeadroomError(record)
    return record


def storage_snapshot(root,runner=subprocess.run,*,exact_sizes=None):
    """Measure this study's occupancy and the user's personal Lustre quota."""
    from .storage_artifacts import artifact_read_cap, measure_regular_eof, iter_regular_lengths
    root=Path(root).resolve()
    if root!=PERSONAL_STUDY_ROOT.resolve():raise ValueError('Storage snapshot requires the authorized personal study namespace')
    if exact_sizes is None:
        # Production Lustre scans must never ask an OST for full getattr merely
        # to classify or size a leaf. Keep the portable stat path for other
        # filesystems and legacy audit fixtures; tests can select either mode.
        exact_sizes=sys.platform.startswith('linux') and root==(Path('/scratch/pwa209/horsehair-worm').resolve())
    if type(exact_sizes) is not bool:raise ValueError('Exact-size measurement selection must be boolean')
    quota=_measure_personal_quota(runner)
    sizes=0;count=0;artifact_items=[];exact_items=[];scan_started=time.monotonic()
    # Offset reads are dominated by remote I/O latency. Bound outstanding work
    # while the allocation's CPU/memory/time limits still govern this process.
    workers=min(16,max(4,4*int(os.environ.get('SLURM_CPUS_PER_TASK','1')))) if exact_sizes else 1
    entries=(iter_regular_lengths(root,6_000_000_000_000,workers=workers) if exact_sizes else
        ((Path(directory)/filename,None) for directory,dirs,files in os.walk(root,followlinks=False) for filename in files))
    for path,measured in entries:
        path=Path(path)
        if exact_sizes:
            if measured['status'] in {'symlink','directory','special'}:continue
            if (measured['status']!='measured' or measured.get('method')!='owned_regular_length'
                    or measured.get('eof_observed') is not True
                    or type(measured.get('bytes')) is not int or measured['bytes']<0):
                raise ValueError('Study entry lacks an exact regular-file length measurement')
            sizes+=measured['bytes'];count+=1
            exact_items.append({'relative_path':path.relative_to(root).as_posix(),**measured})
            if count%1000==0:
                print('STORAGE_CENSUS_PROGRESS '+json.dumps({'files':count,'bytes':sizes,
                    'elapsed_seconds':time.monotonic()-scan_started,'io_workers':workers,
                    'method':'owned_regular_length'},sort_keys=True),flush=True)
            continue
        cap=artifact_read_cap(path,root)
        if cap is not None:
            # Legacy narrow receipts retain their original exact EOF format.
            try:measured=measure_regular_eof(path,cap,root=root)
            except FileNotFoundError:continue
            if measured['status']=='symlink':continue
            if measured['status']!='measured' or measured.get('eof_observed') is not True:
                raise ValueError('Storage artifact lacks an exact EOF measurement')
            sizes+=measured['bytes'];count+=1
            artifact_items.append({'relative_path':str(path.relative_to(root)),**measured})
            continue
        if path.is_symlink():continue
        try:sizes+=path.stat().st_size;count+=1
        except FileNotFoundError:continue
    path=root/'state'/('quota-'+str(time.time_ns())+'.txt')
    path.write_text(quota['native_stdout'],encoding='utf8')
    record={'status':'measured','root':str(root),'measured_unix':time.time(),'captured_utc':datetime.now(timezone.utc).isoformat(),'study_bytes':sizes,'study_files':count,
            **{key:value for key,value in quota.items() if key!='native_stdout'},
            'native_source_paths':[str(path)],'native_source_hashes':{str(path):digest(path)}}
    if exact_sizes:
        exact_items.sort(key=lambda item:item['relative_path'])
        evidence=root/'state/storage-entry-sizes.json'
        atomic_json(evidence,{'schema_version':1,'status':'measured','root':str(root),'method':'owned_regular_length',
            'count':len(exact_items),'bytes':sum(item['bytes'] for item in exact_items),'items':exact_items})
        record['exact_entry_measurements']={'schema_version':1,'method':'owned_regular_length','receipt_path':str(evidence),
            'receipt_sha256':digest(evidence),'count':len(exact_items),'bytes':sum(item['bytes'] for item in exact_items)}
    if artifact_items:
        artifact_items.sort(key=lambda item:item['relative_path'])
        evidence=root/'state/storage-artifacts.json'
        artifact_bytes=sum(item['bytes'] for item in artifact_items)
        atomic_json(evidence,{'schema_version':1,'status':'measured','root':str(root),'method':'owned_regular_eof',
            'count':len(artifact_items),'bytes':artifact_bytes,'items':artifact_items})
        record['artifact_eof_measurements']={'schema_version':1,'method':'owned_regular_eof','receipt_path':str(evidence),
            'receipt_sha256':digest(evidence),'count':len(artifact_items),'bytes':artifact_bytes}
    atomic_json(root/'state/storage-budget.json',record)
    return record


def native_accounting(root, job_ids, runner=subprocess.run):
    """Reconcile allocation cost from final native parent records, once per ID.

    Steps are excluded to avoid double counting. A running or missing job cannot
    release a crashed lease. Bootstrap allocations use the same accounting path.
    """
    ids=list(dict.fromkeys(str(x) for x in job_ids))
    if not ids or any(not re.fullmatch(r'\d+(?:_\d+)?',x) for x in ids):raise ValueError('Invalid native job identifiers')
    response=runner(['sacct','-j',','.join(ids),'--format=JobID%64,JobIDRaw%64,State,ElapsedRaw,AllocCPUS,AllocTRES,ExitCode,MaxRSS','--units=K','--noheader','--parsable2'],text=True,capture_output=True)
    if response.returncode:raise RuntimeError('Native accounting query failed; leases preserved')
    terminal={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
    observed={};peaks={};rows=[];aliases={};logical_raw={};condensed_pending=set()
    def rss_bytes(value):
        match=re.fullmatch(r'(\d+(?:\.\d+)?)([KMGT]?)',value)
        if not match:raise ValueError('Unrecognized native memory measurement')
        return float(match[1])*1024**(' KMGT'.index(match[2]) if match[2] else 0)
    for line in response.stdout.splitlines():
        fields=line.strip().split('|')
        if len(fields)<8:raise ValueError('Native accounting lacks logical and physical allocation identities')
        ident,raw=fields[0].split('.')[0],fields[1].split('.')[0]
        if not re.fullmatch(r'\d+(?:_\d+)?',ident):
            match=re.fullmatch(r'(\d+)_\[.+\]',ident)
            if match:condensed_pending.add(match[1])
            continue  # condensed pending-array rows are not allocations
        if not re.fullmatch(r'\d+',raw):raise ValueError('Invalid physical native allocation identity')
        if raw in aliases and aliases[raw]!=ident or ident in logical_raw and logical_raw[ident]!=raw:
            raise ValueError('Conflicting logical/physical native identities')
        aliases[raw]=ident;logical_raw[ident]=raw
        if fields[7]:
            if ident not in peaks or rss_bytes(fields[7])>rss_bytes(peaks[ident]):peaks[ident]=fields[7]
    for line in response.stdout.splitlines():
        fields=line.strip().split('|')
        ident,raw,state,seconds,cpus,tres,exit_code=fields[:7]
        if '.' in ident or not re.fullmatch(r'\d+(?:_\d+)?',ident):continue
        state=state.split()[0].split('+')[0]
        if state not in terminal:continue
        if not seconds.isdigit() or not cpus.isdigit():raise ValueError('Native accounting returned nonnumeric allocation')
        gpu=0
        for pair in tres.split(','):
            key,sep,value=pair.partition('=')
            if key=='gres/gpu' and sep:gpu=int(value)
        if gpu==0:
            # Older records may only expose the typed GPU field.
            gpu=sum(int(p.split('=',1)[1]) for p in tres.split(',') if p.startswith('gres/gpu:') and '=' in p)
        hours=int(seconds)/3600
        item={'state':state,'exit_code':exit_code,'elapsed_seconds':int(seconds),'cpus':int(cpus),'gpus':gpu,
                         'job_id':ident,'job_id_raw':raw,
                         'cpu_core_hours':hours*int(cpus),'gpu_hours':hours*gpu,'basis':'sacct final allocated wall time'}
        if ident in observed and observed[ident]!=item:raise ValueError('Conflicting final native allocation records')
        observed[ident]=item
        if int(seconds)>0 and int(cpus)>0:rows.append('|'.join([ident,raw,state,seconds,cpus,peaks.get(ident,''),tres,exit_code]))
    path=Path(root)/'state/resource-budget.json';path.parent.mkdir(parents=True,exist_ok=True)
    with exclusive_file_lock(path.with_suffix('.mutex')):
        record=read_json(path) if path.exists() else {'spent_cpu_core_hours':0.,'spent_gpu_hours':0.,'leases':{}}
        before=canonical_hash(record)
        native=record.setdefault('native_jobs',{})
        known=record.setdefault('native_raw_aliases',{})
        if any(raw in known and known[raw]!=ident for raw,ident in aliases.items()):
            raise ValueError('Native allocation identity changed; explicit reconciliation needed')
        def same_allocation(old,new):
            return all(old.get(key)==new.get(key) for key in ('state','exit_code','elapsed_seconds','cpus','gpus','cpu_core_hours','gpu_hours','basis'))
        # Upgrade old JobIDRaw-only receipts using actual paired native evidence.
        # Never infer an array index from row order or a job's name.
        for raw,ident in aliases.items():
            if raw!=ident and raw in native:
                old=native[raw]
                if ident not in observed or not same_allocation(old,observed[ident]):
                    raise ValueError('Legacy physical accounting differs from native allocation evidence')
                if ident in native and not same_allocation(native[ident],old):
                    raise ValueError('Duplicate accounting identities contain conflicting allocations')
                del native[raw]
                record.setdefault('native_identity_reconciliations',[]).append({'previous_job_id_raw_key':raw,'logical_job_id':ident})
            # A historical raw-ID owner still finishes against its original key.
            # The alias is evidence, never permission to rename a live lease.
            if raw!=ident and ident in observed and raw in record['leases']:
                if ident in record['leases']:
                    raise ValueError('Duplicate physical/logical leases require explicit reconciliation')
                record['leases'][ident]=record['leases'].pop(raw)
            known[raw]=ident
        for ident,item in observed.items():
            old=native.get(ident)
            if old and not same_allocation(old,item):raise ValueError('Final native accounting changed; explicit reconciliation needed')
            native[ident]=item
            if ident in record['leases']:
                record['leases'][ident].update(state='native_reconciled',native=item)
        unreconciled=[v for key,v in record['leases'].items() if key not in native and v['state']=='finished']
        record['spent_cpu_core_hours']=sum(v['cpu_core_hours'] for v in native.values())+sum(v['cpus']*v.get('elapsed_hours',0) for v in unreconciled)
        record['spent_gpu_hours']=sum(v['gpu_hours'] for v in native.values())+sum(v['gpus']*v.get('elapsed_hours',0) for v in unreconciled)
        record['accounting_basis']='final sacct allocations where available; provisional command wall time otherwise'
        record['accounting_identity_schema']=2
        if canonical_hash(record)!=before or not path.exists():atomic_json(path,record)
        accounting=Path(root)/'state/native-accounting.tsv'
        old_rows={}
        previous_text=None
        if accounting.exists():
            previous_text=accounting.read_text()
            lines=previous_text.splitlines()
            old_header=lines[0].split('|')
            for line in lines[1:]:
                if not line.strip():continue
                old=dict(zip(old_header,line.split('|')))
                raw=old['JobIDRaw'];ident=known.get(raw,old.get('JobID',raw))
                old_rows[ident]='|'.join([ident,raw,old['State'],old['ElapsedRaw'],old['AllocCPUS'],old.get('MaxRSS',''),old['AllocTRES'],old.get('ExitCode','')])
        for line in rows:old_rows[line.split('|')[0]]=line
        atomic_text='JobID|JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES|ExitCode\n'+'\n'.join(old_rows[key] for key in sorted(old_rows))+'\n'
        if atomic_text!=previous_text:
            temporary=accounting.with_suffix('.tmp');temporary.write_text(atomic_text,encoding='utf8');temporary.replace(accounting)
        pending=[]
        for ident in ids:
            if ident in observed:continue
            children={key for key in logical_raw if key.startswith(ident+'_')}
            if ident in condensed_pending or (children and not children.issubset(observed)) or (not children and aliases.get(ident) not in observed):
                pending.append(ident)
        return {'reconciled':observed,'native_raw_aliases':aliases,'pending_native_records':pending,
                'spent_cpu_core_hours':record['spent_cpu_core_hours'],'spent_gpu_hours':record['spent_gpu_hours']}


@contextmanager
def resource_lease(root, config, *, cpus, gpus, max_hours, job_id, resource_policy=None):
    root=Path(root)
    path=root/'state/resource-budget.json'
    path.parent.mkdir(parents=True,exist_ok=True)
    job_id=str(job_id)
    if not re.fullmatch(r'\d+(?:_\d+)?',job_id):raise ValueError('Invalid native resource lease identity')
    if resource_policy is not None and (type(cpus) is not int or cpus<1
            or type(gpus) is not int or gpus not in (0,1)
            or isinstance(max_hours,bool) or not isinstance(max_hours,(int,float))
            or not math.isfinite(max_hours) or not 0<max_hours<=168):
        raise ValueError('Operational resource policy requires finite native integer CPU/GPU allocations within 168 hours')
    if cpus<1 or gpus not in (0,1) or not math.isfinite(max_hours) or max_hours<=0:
        raise ValueError('Invalid bounded resource allocation')
    policy=None
    if resource_policy is not None:
        from .execution_resources import resolve_resource_policy
        policy=resolve_resource_policy(resource_policy,root=root,config=config)
    def update(callback,timeout=30):
        with exclusive_file_lock(path.with_suffix('.mutex'),timeout=timeout):
            record=read_json(path) if path.exists() else {'spent_cpu_core_hours':0.,'spent_gpu_hours':0.,'leases':{}}
            before=canonical_hash(record)
            callback(record)
            if canonical_hash(record)!=before or not path.exists():atomic_json(path,record)
    start=time.time();started_monotonic=time.monotonic()
    deadline=start+max_hours*3600
    native_end=os.environ.get('SLURM_JOB_END_TIME')
    if native_end:
        native_end=float(native_end)
        if not math.isfinite(native_end):raise ValueError('Invalid native allocation end time')
        deadline=min(deadline,native_end)
    if deadline<=start:raise ValueError('Native resource allocation has no remaining time')
    # Ten ordinary acquisition windows tolerate simultaneous native starts.
    # Short allocations permit only an immediate attempt when less than the
    # completion margin remains. No command may run without its durable lease.
    reservation_deadline=min(started_monotonic+300.,started_monotonic+max(0.,deadline-start-120.))
    reservation_attempts=0
    def reserve(r):
        if time.time()>=deadline:raise ValueError('Native allocation ended before the resource reservation')
        if job_id in r['leases']:raise ValueError('Existing resource lease; reconcile native accounting without replaying')
        current_policy=None
        if policy is not None:
            # Recheck bytes, approval and source/config bindings under the same
            # durable accounting guard as admission. A cached object is not authority.
            current_policy=resolve_resource_policy(policy,root=root,config=config)
        limits=current_policy.receipt()['limits'] if current_policy is not None else dict(config['resources'],gpu_concurrency=1)
        active=[v for v in r['leases'].values() if v['state']=='running']
        if sum(v['cpus'] for v in active)+cpus>limits['cpu_ceiling'] or sum(v['gpus'] for v in active)+gpus>limits['gpu_concurrency']:
            raise ValueError('Study concurrency ceiling reached')
        if limits['cpu_core_hours_cap'] is not None and r['spent_cpu_core_hours']+sum(v['cpus']*v['max_hours'] for v in active)+cpus*max_hours>limits['cpu_core_hours_cap']:
            raise ValueError('Insufficient remaining CPU-hour budget')
        if limits['gpu_hours_cap'] is not None and r['spent_gpu_hours']+sum(v['gpus']*v['max_hours'] for v in active)+gpus*max_hours>limits['gpu_hours_cap']:
            raise ValueError('Insufficient remaining GPU-hour budget')
        r['leases'][job_id]={'state':'running','started_unix':start,'cpus':cpus,'gpus':gpus,'max_hours':max_hours,
            'reservation_attempts':reservation_attempts,'reservation_acquisition_wait_seconds':max(0.,time.monotonic()-started_monotonic)}
        if current_policy is not None:r['leases'][job_id]['resource_policy']=current_policy.receipt()
    for attempt in range(10):
        remaining=max(0.,reservation_deadline-time.monotonic())
        reservation_attempts+=1
        try:
            update(reserve,timeout=min(30.,remaining))
            break
        except ReceiptLockTimeout as error:
            print(json.dumps({'resource_lease_reservation':'waiting_for_live_guard','job_id':job_id,
                'attempt':reservation_attempts,'reservation_wait_seconds':max(0.,time.monotonic()-started_monotonic),
                'retry_limit_seconds':300.,'remaining_retry_seconds':max(0.,reservation_deadline-time.monotonic()),
                'allocation_deadline_unix':deadline,'cleanup_margin_seconds':120.,'guard':error.diagnostics}),file=sys.stderr,flush=True)
            if attempt==9 or time.monotonic()>=reservation_deadline:raise
    reservation_wait=max(0.,time.monotonic()-started_monotonic)
    command_exception=None
    try:
        if time.time()>=deadline:raise ValueError('Native allocation ended before the reserved command could begin')
        lease_receipt={'deadline_unix':deadline,'job_id':job_id,'reservation_attempts':reservation_attempts,
                       'reservation_wait_seconds':reservation_wait}
        if policy is not None:lease_receipt['resource_policy']=policy.receipt()
        yield lease_receipt
    except BaseException as exc:
        command_exception=exc
        raise
    finally:
        command_end=time.time();cleanup_start=time.monotonic()
        finish_deadline=min(cleanup_start+120.,started_monotonic+max(0.,deadline-start))
        journal_path=root/'state/resource-lease-finalizations'/(job_id+'.json')
        journal={'schema_version':1,'status':'finalization_intent','local_finalization':'pending',
                 'job_id':job_id,'root':str(root.resolve()),'config_sha256':canonical_hash(config),
                 'cpus':cpus,'gpus':gpus,'max_hours':max_hours,'started_unix':start,
                 'reservation_attempts':reservation_attempts,'reservation_wait_seconds':reservation_wait,
                 'command_ended_unix':command_end,'command_elapsed_seconds':max(0.,command_end-start),
                 'deadline_unix':deadline,'finish_retry_limit_seconds':120.,'finish_attempts':0,
                 'command_outcome':'raised' if command_exception is not None else 'returned',
                 'command_exception':({'type':type(command_exception).__name__,'module':type(command_exception).__module__,
                                       'message':str(command_exception)} if command_exception is not None else None),
                 'reservation_release_rule':'positive terminal sacct proof in resource-budget.json; this journal never releases a lease or charges hours'}
        if policy is not None:journal['resource_policy']=policy.receipt()
        journal_error=None;cleanup_error=None;last_timeout=None
        def persist_journal():
            nonlocal journal_error
            try:
                payload=dict(journal);payload['self_sha256']=canonical_hash(payload)
                atomic_json(journal_path,payload)
                journal_error=None
            except Exception as exc:
                journal_error=exc
        # Persist before waiting: a native termination during lock acquisition
        # must not erase the original command's scientific error or completion.
        # This separate file requires no shared receipt guard.
        persist_journal()
        def finish(r):
            key=job_id if job_id in r['leases'] else r.get('native_raw_aliases',{}).get(job_id)
            entry=r['leases'].get(key)
            if not entry or entry.get('started_unix')!=start:
                raise ValueError('Resource lease owner changed; preserve its reservation')
            if entry['state'] in ('finished','native_reconciled'):
                return  # An already recorded/native allocation is never charged twice.
            if entry['state']!='running':raise ValueError('Unexpected resource lease state')
            elapsed=max(0.,time.time()-start)/3600
            entry.update(state='finished',elapsed_hours=elapsed)
            r['spent_cpu_core_hours']+=cpus*elapsed
            r['spent_gpu_hours']+=gpus*elapsed
        # At most four full30-second waits, or a final shorter attempt before the
        # allocation deadline. Entry contention has its own bounded wait above;
        # neither path bypasses the durable resource reservation.
        for attempt in range(5):
            remaining=max(0.,finish_deadline-time.monotonic())
            if attempt and remaining<=0:break
            journal['finish_attempts']+=1
            try:
                update(finish,timeout=min(30.,remaining))
                journal['local_finalization']='recorded'
                break
            except ReceiptLockTimeout as exc:
                last_timeout=exc
                journal['last_guard_timeout']=exc.diagnostics
            except BaseException as exc:
                cleanup_error=exc
                journal['local_finalization']='failed'
                journal['finalization_error']={'type':type(exc).__name__,'message':str(exc)}
                break
        journal['finalization_elapsed_seconds']=max(0.,time.monotonic()-cleanup_start)
        journal['finished_unix']=time.time()
        if journal['local_finalization']=='pending':
            journal['status']='pending_native_reconciliation'
        else:
            journal['status']='local_finalization_recorded' if journal['local_finalization']=='recorded' else 'local_finalization_failed'
        persist_journal()
        if journal['local_finalization']!='recorded' or journal_error is not None:
            print(json.dumps({'resource_lease_finalization':journal['status'],'job_id':job_id,
                              'journal_path':str(journal_path),'journal_persisted':journal_error is None,
                              'original_command_exception':journal['command_exception'],
                              'last_guard_timeout':last_timeout.diagnostics if last_timeout else None,
                              'cleanup_error':str(cleanup_error) if cleanup_error else None,
                              'journal_error':str(journal_error) if journal_error else None}),file=sys.stderr,flush=True)
        # A bookkeeping error never replaces the exception raised by the study.
        # Exhausted contention with a durable intent keeps the reservation until
        # the already implemented native terminal reconciliation runs.
        if command_exception is None:
            if cleanup_error is not None:raise cleanup_error
            if journal_error is not None and journal['local_finalization']!='recorded':
                raise RuntimeError('Pending resource finalization could not persist its durable evidence') from journal_error

