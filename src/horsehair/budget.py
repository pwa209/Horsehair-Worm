"""Atomic resource leases within one study; stopped jobs require native reconciliation."""
from __future__ import annotations
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from .core import atomic_json, read_json


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

