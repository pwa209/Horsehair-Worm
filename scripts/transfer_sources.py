"""Finite public-source file transfer on Narval's documented transfer endpoint.

No extraction, alignment, scientific fitting or persistent scheduler controller
runs here. Native Slurm stages independently reverify sources before analysis.
"""
import argparse
import json
import os
import time
from pathlib import Path
from horsehair.core import atomic_json,canonical_hash,read_json,validate_config,source_identity
from horsehair.acquire import discover_sources,download_manifest

p=argparse.ArgumentParser()
p.add_argument('--config',type=Path,required=True)
p.add_argument('--root',type=Path,required=True)
p.add_argument('--identity',type=Path,required=True)
p.add_argument('--group',required=True,choices=['fly','rna','mantis','tools'])
a=p.parse_args()
c=read_json(a.config);validate_config(c)
identity=read_json(a.identity)
if source_identity(a.identity.resolve().parent)!=identity['source_identity']:raise ValueError('Transfer release changed')
groups={'fly':['fly'],'rna':['ant_rna','ant_reference','mantis_rna','fly_reference'],'mantis':['polarization','mating'],'tools':['tool_runtime']}
state=a.root/'state'/('transfer-'+a.group+'.json')
if state.exists() and read_json(state).get('status')=='complete':raise FileExistsError('Transfer already completed')
lock=a.root/'state'/('.transfer-'+a.group+'.lock')
lock.parent.mkdir(parents=True,exist_ok=True)
with lock.open('x') as f:json.dump({'pid':os.getpid(),'started_unix':time.time()},f)
started=time.time()
record={'status':'running','group':a.group,'sources':groups[a.group],'pid':os.getpid(),'started_unix':started,'config_sha256':canonical_hash(c),'source_identity':identity['source_identity']['sha256'],'execution_class':'finite public-data file transfer'}
atomic_json(state,record)
try:
 manifest=a.root/'manifests'/('tool_runtime_manifest.json' if a.group=='tools' else 'source_manifest.json')
 if not manifest.exists():raise FileNotFoundError('Freeze the provider metadata manifest before starting independent transfers')
 result=download_manifest(manifest,a.root,sources=groups[a.group],max_bytes=c['resources']['scratch_budget_bytes'],max_files=c['resources']['scratch_budget_files'],bandwidth_bytes_per_second=64*1024**2,max_seconds=22*3600)
 record.update(status='complete',elapsed_seconds=time.time()-started,source_manifest_sha256=canonical_hash(read_json(manifest)),verified_files=len(result['files']))
 atomic_json(state,record)
except BaseException as e:
 record.update(status='failed',elapsed_seconds=time.time()-started,error_type=type(e).__name__,error=str(e))
 atomic_json(state,record)
 raise
finally:lock.unlink(missing_ok=True)

