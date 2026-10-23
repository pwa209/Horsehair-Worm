"""Finite two-worker public file-transfer launcher on Narval.

Dataset bytes never pass through the local workstation. This launcher performs
metadata discovery and bounded transfers only; numerical work runs under Slurm.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys
import time
from horsehair.core import atomic_json,read_json,source_identity,canonical_hash,validate_config
from horsehair.acquire import discover_sources
from horsehair.molecular import tool_runtime_manifest

p=argparse.ArgumentParser()
p.add_argument('--root',required=True,type=Path)
p.add_argument('--config',required=True,type=Path)
p.add_argument('--identity',required=True,type=Path)
a=p.parse_args()
c=read_json(a.config);validate_config(c)
if source_identity(a.identity.resolve().parent)!=read_json(a.identity)['source_identity']:raise ValueError('Release changed')
state=a.root/'state/transfer-launcher.json'
lock=a.root/'state/.transfer-launcher.lock'
lock.parent.mkdir(parents=True,exist_ok=True)
with lock.open('x') as f:f.write(str(time.time()))
record={'status':'metadata_discovery','config_sha256':canonical_hash(c),'started_unix':time.time(),'maximum_workers':2,'execution_class':'finite file transfer, no numerical analysis'}
atomic_json(state,record)
try:
    manifest=a.root/'manifests/source_manifest.json'
    if not manifest.exists():discover_sources(manifest)
    tools=a.root/'manifests/tool_runtime_manifest.json'
    if not tools.exists():atomic_json(tools,tool_runtime_manifest())
    record.update(status='transferring',manifest_sha256=canonical_hash(read_json(manifest)))
    atomic_json(state,record)
    def transfer(group):
        release_id=read_json(a.identity)['release_id']
        log=a.root/'logs'/('transfer-'+group+'-'+release_id+'-'+str(time.time_ns())+'.log')
        with log.open('x') as output:
            result=subprocess.run([sys.executable,'scripts/transfer_sources.py','--root',str(a.root),'--config',str(a.config),'--identity',str(a.identity),'--group',group],stdout=output,stderr=subprocess.STDOUT)
        return {'group':group,'returncode':result.returncode,'log':str(log)}
    # The two tiny groups run first; a provider failure is retained independently.
    with ThreadPoolExecutor(max_workers=2) as pool:
        small=list(pool.map(transfer,['mantis','tools']))
        large=list(pool.map(transfer,['fly','rna']))
    record.update(status='complete' if all(x['returncode']==0 for x in small+large) else 'completed_with_provider_failures',groups=small+large,elapsed_seconds=time.time()-record['started_unix'])
    atomic_json(state,record)
except BaseException as error:
    record.update(status='failed',error_type=type(error).__name__,error=str(error))
    atomic_json(state,record)
    raise
finally:lock.unlink(missing_ok=True)
