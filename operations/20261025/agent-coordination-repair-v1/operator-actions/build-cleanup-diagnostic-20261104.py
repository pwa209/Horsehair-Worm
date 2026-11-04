from pathlib import Path
import base64

diagnostic = r'''from pathlib import Path
import hashlib,importlib.util,json,traceback,os
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/terminal-controller-diagnostic-support-v3-20261103'
p=s/'compact-terminal-controller-reads-20261103.py'
assert hashlib.sha256(p.read_bytes()).hexdigest()=='5019e6f543cd6a93195edf6e8db0319eaa7fc2d1abd8cc48e74db06465ff7550'
spec=importlib.util.spec_from_file_location('compactor',p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
request=json.loads((s/'request.json').read_bytes())
assert hashlib.sha256((s/'request.json').read_bytes()).hexdigest()=='e5616c613b646818c9647087583ce22f3a844b1466992276ae3f9f5950ec88f6'
done=json.loads((r/'maintenance/terminal-controller-diagnostic-compaction-v3-20261103/progress.json').read_bytes())['completed']
assert [x['job'] for x in done]==['7046213','6689295','6701493']
item=next(x for x in request['sources'] if x['job'] not in {y['job'] for y in done})
source=Path(item['path']);registration=m.source_registration(source,item['job'],root=r)
docs=request['binding_documents']+item['binding_documents']+[registration['source_document']]+[registration['registration'][k] for k in ('plan','ledger')]
try:
 rows,bound=m.roster(source,docs,request['protected_paths']+item['protected_paths'],root=r)
 out={'status':'source_roster_now_valid','job':item['job'],'files':len(rows),'bound_documents':len(bound)}
except ValueError as e:
 out={'status':'source_ineligible_no_files_modified','source':str(source),'job':item['job'],'error':str(e)}
 tb=e.__traceback__
 while tb:
  if tb.tb_frame.f_code.co_name=='bound_exclusions':
   v=tb.tb_frame.f_locals
   out.update(bound_document=str(v.get('path')),expected=v.get('item',{}).get('sha256'),actual=v.get('digest'),seen_documents=len(v.get('seen',{})))
  tb=tb.tb_next
(Path(__file__).parent/'result.json').write_text(json.dumps(out,indent=2))
print(json.dumps(out),flush=True)
'''
script = '''#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-cleanup-reference-diagnostic-20261104
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=00:30:00
#SBATCH --hold
#SBATCH --no-requeue
#SBATCH --output=/scratch/pwa209/horsehair-worm/maintenance/cleanup-reference-diagnostic-20261104/native.log
set -euo pipefail
test "$(id -un)" = pwa209
module load StdEnv/2023 python/3.11.5
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC
python -s -B /scratch/pwa209/horsehair-worm/maintenance/cleanup-reference-diagnostic-20261104/diagnostic.py
'''
body = '''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -B -u - <<'PY'
from pathlib import Path
import base64,json,subprocess,shlex,os
assert os.geteuid()==3164768
s=Path('/scratch/pwa209/horsehair-worm/maintenance/cleanup-reference-diagnostic-20261104');s.mkdir()
(s/'diagnostic.py').write_bytes(base64.b64decode(DIAGNOSTIC))
(s/'diagnostic.sbatch').write_bytes(base64.b64decode(SCRIPT))
cmd=['sbatch','--parsable',str(s/'diagnostic.sbatch')];(s/'submit-intent.json').write_text(json.dumps(cmd))
p=subprocess.run(cmd,capture_output=True,text=True,timeout=30);(s/'submit-response.json').write_text(json.dumps({'code':p.returncode,'stdout':p.stdout,'stderr':p.stderr}));p.check_returncode();job=p.stdout.strip().split(';')[0];assert job.isdigit()
def query():
 p=subprocess.run(['scontrol','show','job',job,'-o'],capture_output=True,text=True,timeout=30);p.check_returncode();assert not p.stderr
 f=dict(x.split('=',1) for x in shlex.split(p.stdout) if '=' in x)
 assert f['JobId']==job and f['UserId']=='pwa209(3164768)' and f['Command']==str(s/'diagnostic.sbatch') and f['JobName']=='hhw-cleanup-reference-diagnostic-20261104' and f['NumCPUs']=='1' and f['TimeLimit']=='00:30:00'
 return f
f=query();assert f['JobState']=='PENDING' and f['Reason']=='JobHeldUser'
(s/'registration.json').write_text(json.dumps({'job_id':job,'held':f},indent=2))
(s/'release-intent.json').write_text(json.dumps(['scontrol','release',job]));p=subprocess.run(['scontrol','release',job],capture_output=True,text=True,timeout=30);(s/'release-response.json').write_text(json.dumps({'code':p.returncode,'stdout':p.stdout,'stderr':p.stderr}))
f=query();assert f['JobState']=='RUNNING' or f['JobState']=='PENDING' and f['Reason'] not in {'JobHeldUser','JobHeldAdmin'} and int(f['Priority'])>0
print(json.dumps({'job_id':job,'actual_unheld':f}),flush=True)
PY
'''
body=body.replace('DIAGNOSTIC',repr(base64.b64encode(diagnostic.encode()).decode())).replace('SCRIPT',repr(base64.b64encode(script.encode()).decode()))
Path('work/hhw-cleanup-diagnostic-20261104-944.sh').write_text(body,newline='\n')
