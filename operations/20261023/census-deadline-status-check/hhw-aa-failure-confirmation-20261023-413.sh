#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib,subprocess,time,re
assert os.geteuid()==3164768
os.environ['TZ']='UTC';time.tzset()
root=Path('/scratch/pwa209/horsehair-worm');campaign=root/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
out={'read_only':True,'checked_utc':datetime.now(timezone.utc).isoformat(),'host':subprocess.check_output(['hostname','-f'],text=True).strip(),'commands':{},'state':{}}
commands=[('failure_reasons',['sacct','-X','-j','6811498,6819399,6822432,6822568,6822730,6824584','--format=JobID,JobName%64,State%32,Reason%80,ExitCode,ElapsedRaw,TimelimitRaw,Start,End','--noheader','--parsable2']),('main_controller',['scontrol','show','job','6632164','-o'])]
for key,args in commands:
 try:
  p=subprocess.run(args,capture_output=True,text=True,timeout=40);out['commands'][key]={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
 except Exception as e:out['commands'][key]={'error_type':type(e).__name__,'error':str(e)}
try:
 p=campaign/'state.json.gz';assert not p.is_symlink();h=hashlib.sha256();n=0
 with p.open('rb') as f:
  while True:
   block=f.read(65536)
   if not block:break
   n+=len(block)
   if n>64000000:raise ValueError('Compressed state read bound exceeded')
   h.update(block)
 binding=json.loads((campaign/'state-binding.json').read_text());out['state']={'bytes':n,'file_sha256':h.hexdigest(),'binding':binding,'compressed_bytes_match_binding':h.hexdigest()==binding['state_file_sha256'],'prior_405_bytes_unchanged':h.hexdigest()=='d44aa7c09bcd09c007009d9d5af58c2976f1d53eabbdd93c8f0daf92d9afa684'}
except Exception as e:out['state']={'error_type':type(e).__name__,'errno':getattr(e,'errno',None),'error':str(e)}
try:
 text=out['commands']['main_controller'].get('stdout','');m=re.search(r'(?:^|\s)StdOut=(\S+)',text);assert m
 p=Path(m.group(1));assert p.is_relative_to(root/'logs') and not p.is_symlink()
 with p.open('rb') as f:f.seek(0,2);n=f.tell();f.seek(max(0,n-6000));out['main_controller_log_tail']={'path':str(p),'bytes':n,'tail':f.read(6000).decode(errors='replace')}
except Exception as e:out['main_controller_log_tail']={'error_type':type(e).__name__,'error':str(e)}
print('NARVAL_FAILURE_CONFIRMATION '+json.dumps(out,sort_keys=True),flush=True)
PY
