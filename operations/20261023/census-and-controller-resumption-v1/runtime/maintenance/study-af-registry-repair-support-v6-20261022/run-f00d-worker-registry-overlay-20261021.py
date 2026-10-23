"""Source-bound operational shim; original f00d main/numerics/descriptor retained."""
from pathlib import Path
import argparse
import hashlib
import json
import math
import os
import re
import runpy
import sys
import time

ROOT=Path('/scratch/pwa209/horsehair-worm')
WORKER=ROOT/'maintenance/study-af-campaign-v1-20261021/native-worker.py'
WORKER_SHA='f00d4265a68a3ace5faa47da4fe2701fff3fe86d9132b09ea748e9f12471e4fd'


def require(value,message):
    if not value:raise ValueError(message)


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def exclusive(path,value):
    with Path(path).open('xb') as stream:
        stream.write((json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode())
        stream.flush();os.fsync(stream.fileno())


def run(guard_path,guard_sha,worker_arguments):
    # Scheduler/accounting clocks only; no input data or biological clock is
    # transformed. Every subsequently launched native query inherits UTC.
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    guard_path=Path(guard_path)
    require(guard_path.is_absolute() and guard_path==guard_path.resolve() and not guard_path.is_symlink()
        and guard_path.is_relative_to(ROOT/'maintenance') and re.fullmatch('[0-9a-f]{64}',guard_sha)
        and sha(guard_path)==guard_sha,'Explicit immutable private registry guard path/SHA required')
    require(WORKER.is_file() and not WORKER.is_symlink() and sha(WORKER)==WORKER_SHA,'Unchanged original f00d worker required')
    deadline=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(deadline),'Verified finite native end required by original worker')
    # Loading the original with a non-main name executes no native work. Its
    # main subsequently creates its area and installs358 exactly as before.
    loaded=runpy.run_path(str(WORKER),run_name='stable_f00d_worker_library')
    main=loaded['main'];namespace=main.__globals__;original=namespace['combined_admission']
    calls=[]
    def admission(*args,**kwargs):
        require(not calls and len(args)>=4,'One original native-start admission only; no whole-stage retry')
        area=Path(args[3]);require(area.is_dir() and area.is_relative_to(ROOT/'maintenance'),'Original worker must create its area first')
        require(sha(WORKER)==WORKER_SHA and sha(guard_path)==guard_sha,'Worker or registry guard source changed')
        guard=runpy.run_path(str(guard_path),run_name='private_registry_overlay_library')
        identity=guard['install'](area/'registered-snapshot-reads',registration_mode='worker-lock',allocation_deadline_unix=deadline)
        calls.append(identity)
        metadata={'status':'operational_registry_overlay_installed','original_worker_path':str(WORKER),
            'original_worker_sha256':WORKER_SHA,'shim_sha256':sha(Path(__file__)),
            'registry_guard_path':str(guard_path),'registry_guard_sha256':guard_sha,'installation':identity,
            'original_worker_arguments':list(worker_arguments),'numeric_functions_replaced':False,
            'stable_worker_descriptor_changed':False,'scheduler_mutations_retried':False,
            'scientific_result':False,'admission_authority':False}
        exclusive(area/'registered-snapshot-overlay.json',metadata)
        result=original(*args,**kwargs)
        require(sha(WORKER)==WORKER_SHA and sha(guard_path)==guard_sha,'Operational source changed during admission')
        return result
    namespace['combined_admission']=admission
    previous=sys.argv
    try:
        sys.argv=[str(WORKER),*worker_arguments]
        return main()
    finally:
        sys.argv=previous;namespace['combined_admission']=original


def main():
    parser=argparse.ArgumentParser(add_help=False)
    parser.add_argument('--registry-read-guard',type=Path,required=True)
    parser.add_argument('--registry-read-guard-sha256',required=True)
    args,remaining=parser.parse_known_args()
    return run(args.registry_read_guard,args.registry_read_guard_sha256,remaining)


if __name__=='__main__':main()
