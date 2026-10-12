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
from .core import atomic_json, read_json,digest


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
    response=runner(['sacct','-j',','.join(ids),'--format=JobIDRaw,State,ElapsedRaw,AllocCPUS,AllocTRES,ExitCode,MaxRSS','--units=K','--noheader','--parsable2'],text=True,capture_output=True)
    if response.returncode:raise RuntimeError('Native accounting query failed; leases preserved')
    terminal={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
    observed={};peaks={};rows=[]
    def rss_bytes(value):
        match=re.fullmatch(r'(\d+(?:\.\d+)?)([KMGT]?)',value)
        if not match:raise ValueError('Unrecognized native memory measurement')
        return float(match[1])*1024**(' KMGT'.index(match[2]) if match[2] else 0)
    for line in response.stdout.splitlines():
        fields=line.strip().split('|')
        if len(fields)>=7 and fields[6]:
            ident=fields[0].split('.')[0]
            if ident not in peaks or rss_bytes(fields[6])>rss_bytes(peaks[ident]):peaks[ident]=fields[6]
    for line in response.stdout.splitlines():
        fields=line.strip().split('|')
        if len(fields)<6:continue
        ident,state,seconds,cpus,tres,exit_code=fields[:6]
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
        observed[ident]={'state':state,'exit_code':exit_code,'elapsed_seconds':int(seconds),'cpus':int(cpus),'gpus':gpu,
                         'cpu_core_hours':hours*int(cpus),'gpu_hours':hours*gpu,'basis':'sacct final allocated wall time'}
        if int(seconds)>0 and int(cpus)>0:rows.append('|'.join([ident,state,seconds,cpus,peaks.get(ident,''),tres]))
    path=Path(root)/'state/resource-budget.json';path.parent.mkdir(parents=True,exist_ok=True)
    lock=path.with_suffix('.lock')
    fd=os.open(lock,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(fd)
    try:
        record=read_json(path) if path.exists() else {'spent_cpu_core_hours':0.,'spent_gpu_hours':0.,'leases':{}}
        native=record.setdefault('native_jobs',{})
        for ident,item in observed.items():
            old=native.get(ident)
            if old and old!=item:raise ValueError('Final native accounting changed; explicit reconciliation needed')
            native[ident]=item
            if ident in record['leases']:
                record['leases'][ident].update(state='native_reconciled',native=item)
        unreconciled=[v for key,v in record['leases'].items() if key not in native and v['state']=='finished']
        record['spent_cpu_core_hours']=sum(v['cpu_core_hours'] for v in native.values())+sum(v['cpus']*v.get('elapsed_hours',0) for v in unreconciled)
        record['spent_gpu_hours']=sum(v['gpu_hours'] for v in native.values())+sum(v['gpus']*v.get('elapsed_hours',0) for v in unreconciled)
        record['accounting_basis']='final sacct allocations where available; provisional command wall time otherwise'
        atomic_json(path,record)
        accounting=Path(root)/'state/native-accounting.tsv'
        old_rows={}
        if accounting.exists():
            for line in accounting.read_text().splitlines()[1:]:old_rows[line.split('|')[0]]=line
        for line in rows:old_rows[line.split('|')[0]]=line
        accounting.write_text('JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES\n'+'\n'.join(old_rows.values())+'\n',encoding='utf8')
        return {'reconciled':observed,'pending_native_records':[i for i in ids if i not in observed],
                'spent_cpu_core_hours':record['spent_cpu_core_hours'],'spent_gpu_hours':record['spent_gpu_hours']}
    finally:lock.unlink(missing_ok=True)


@contextmanager
def resource_lease(root, config, *, cpus, gpus, max_hours, job_id):
    root=Path(root)
    path=root/'state/resource-budget.json'
    path.parent.mkdir(parents=True,exist_ok=True)
    lock=path.with_suffix('.lock')
    def update(callback):
        deadline=time.monotonic()+30
        while True:
            try:
                fd=os.open(lock,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                os.close(fd)
                break
            except FileExistsError:
                if time.monotonic()>deadline:raise TimeoutError('Study resource ledger locked; inspect owner before recovery')
                time.sleep(.2)
        try:
            record=read_json(path) if path.exists() else {'spent_cpu_core_hours':0.,'spent_gpu_hours':0.,'leases':{}}
            callback(record)
            atomic_json(path,record)
        finally:
            lock.unlink(missing_ok=True)
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

