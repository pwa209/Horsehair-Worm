"""Atomic resource leases within one study; stopped jobs require native reconciliation."""
from __future__ import annotations
import json
import os
import time
import re
import subprocess
from datetime import datetime,timezone
from contextlib import contextmanager
from pathlib import Path
from .core import atomic_json, read_json,digest,exclusive_file_lock


def storage_snapshot(root,runner=subprocess.run):
    """Measure this study's occupancy and the user's personal Lustre quota."""
    root=Path(root).resolve()
    if root!=Path('/scratch/pwa209/horsehair-worm'):raise ValueError('Storage snapshot requires the authorized personal study namespace')
    response=runner(['lfs','quota','-u',str(os.getuid()),'/scratch/pwa209'],text=True,capture_output=True)
    if response.returncode:raise RuntimeError('Personal quota measurement failed')
    line=next((x for x in response.stdout.splitlines() if x.strip().startswith('/scratch/pwa209 ')),None)
    if line is None:raise ValueError('Expected personal scratch quota row missing')
    fields=line.split()
    if len(fields)!=9:raise ValueError('Unexpected personal quota format; preserve native output for inspection')
    used,limit=int(fields[1].rstrip('*')),int(fields[3].rstrip('*'))
    used_files,limit_files=int(fields[5].rstrip('*')),int(fields[7].rstrip('*'))
    sizes=0;count=0
    for directory,dirs,files in os.walk(root,followlinks=False):
        for filename in files:
            path=Path(directory)/filename
            if path.is_symlink():continue
            try:sizes+=path.stat().st_size;count+=1
            except FileNotFoundError:continue
    path=root/'state'/('quota-'+str(time.time_ns())+'.txt')
    path.write_text(response.stdout,encoding='utf8')
    record={'status':'measured','root':str(root),'measured_unix':time.time(),'captured_utc':datetime.now(timezone.utc).isoformat(),'study_bytes':sizes,'study_files':count,
            'personal_available_bytes':(limit-used)*1024,'personal_available_files':limit_files-used_files,
            'native_source_paths':[str(path)],'native_source_hashes':{str(path):digest(path)}}
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
        atomic_json(path,record)
        accounting=Path(root)/'state/native-accounting.tsv'
        old_rows={}
        if accounting.exists():
            lines=accounting.read_text().splitlines()
            old_header=lines[0].split('|')
            for line in lines[1:]:
                if not line.strip():continue
                old=dict(zip(old_header,line.split('|')))
                raw=old['JobIDRaw'];ident=known.get(raw,old.get('JobID',raw))
                old_rows[ident]='|'.join([ident,raw,old['State'],old['ElapsedRaw'],old['AllocCPUS'],old.get('MaxRSS',''),old['AllocTRES'],old.get('ExitCode','')])
        for line in rows:old_rows[line.split('|')[0]]=line
        atomic_text='JobID|JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES|ExitCode\n'+'\n'.join(old_rows[key] for key in sorted(old_rows))+'\n'
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
def resource_lease(root, config, *, cpus, gpus, max_hours, job_id):
    root=Path(root)
    path=root/'state/resource-budget.json'
    path.parent.mkdir(parents=True,exist_ok=True)
    def update(callback):
        with exclusive_file_lock(path.with_suffix('.mutex')):
            record=read_json(path) if path.exists() else {'spent_cpu_core_hours':0.,'spent_gpu_hours':0.,'leases':{}}
            callback(record)
            atomic_json(path,record)
    start=time.time()
    def reserve(r):
        if job_id in r['leases']:raise ValueError('Existing resource lease; reconcile native accounting without replaying')
        active=[v for v in r['leases'].values() if v['state']=='running']
        if sum(v['cpus'] for v in active)+cpus>config['resources']['cpu_ceiling'] or sum(v['gpus'] for v in active)+gpus>1:
            raise ValueError('Study concurrency ceiling reached')
        if r['spent_cpu_core_hours']+sum(v['cpus']*v['max_hours'] for v in active)+cpus*max_hours>config['resources']['cpu_core_hours_cap']:
            raise ValueError('Insufficient remaining CPU-hour budget')
        if r['spent_gpu_hours']+sum(v['gpus']*v['max_hours'] for v in active)+gpus*max_hours>config['resources']['gpu_hours_cap']:
            raise ValueError('Insufficient remaining GPU-hour budget')
        r['leases'][job_id]={'state':'running','started_unix':start,'cpus':cpus,'gpus':gpus,'max_hours':max_hours}
    update(reserve)
    try:
        yield {'deadline_unix':start+max_hours*3600,'job_id':job_id}
    finally:
        elapsed=(time.time()-start)/3600
        def finish(r):
            r['leases'][job_id].update(state='finished',elapsed_hours=elapsed)
            r['spent_cpu_core_hours']+=cpus*elapsed
            r['spent_gpu_hours']+=gpus*elapsed
        update(finish)

