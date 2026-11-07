from pathlib import Path
import base64,json,hashlib
local=Path('operations/20261106/storage-reserve-200k')
payload={n:{'data':base64.b64encode((local/n).read_bytes()).decode(),'sha':hashlib.sha256((local/n).read_bytes()).hexdigest()} for n in ['activate_controllers.py','test_activate_controllers.py']}
script=r'''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src
export PYTHONDONTWRITEBYTECODE=1 TZ=UTC
python3 -B -u - <<'PY'
from pathlib import Path
import base64,json,hashlib,shlex,subprocess,os
from horsehair import slurm,budget
from horsehair.core import exclusive_file_lock,atomic_json,canonical_hash
r=Path('/scratch/pwa209/horsehair-worm');b=r/'maintenance/storage-reserve-200k-v3-20261106'
payload=__PAYLOAD__
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def native(j):
 p=subprocess.run(['scontrol','show','job',j,'-o'],text=True,capture_output=True,timeout=45);p.check_returncode()
 return dict(x.split('=',1) for x in shlex.split(p.stdout) if '=' in x)
oldplan=json.loads((r/'plans/storage-reserve-200k-v3-activation-20261106/plan.json').read_bytes());oldtask=oldplan['tasks'][0]
oldledger=json.loads((r/'state/storage-reserve-200k-v3-activation-20261106-submission.json').read_bytes())
assert oldledger['tasks'][oldtask['name']]['job_id']=='8634125' and oldledger['plan_sha256']==canonical_hash(oldplan)
p=subprocess.run(['sacct','-X','-n','-P','-j','8634125','--format=JobIDRaw,User,State,ExitCode,ElapsedRaw'],text=True,capture_output=True,timeout=45);p.check_returncode()
assert p.stdout.strip().split('|')[:4]==['8634125','pwa209','FAILED','1:0']
assert {p.name for p in (b/'activation/native-8634125').iterdir()}=={'positive-prerequisites.json'}
assert not (r/'plans/storage-reserve-200k-v3-primary-20261106').exists()
assert not (r/'plans/agent-coordination-storage-reserve-200k-v3-20261106').exists()
request=json.loads((b/'controller-activation-request.json').read_bytes())
text=Path(oldtask['script']).read_text();assert canonical_hash(text)==oldtask['script_sha256']
phase='storage-reserve-binding-repair-20261107';name='storage-reserve-binding-repair'
d=r/'plans'/phase;ledger=r/'state'/(phase+'-submission.json');history=b/'binding-repair-20261107'
assert not d.exists() and not ledger.exists() and not history.exists()
oldsource='97ee6265bfdc736052c8877feccd9866d88de611fb1099adab994ea557cad4b2'
oldtest='6e0a7f0fd93a7b77c4b2869aba2e4295511eae14b8307b54a555d25bad8210f9'
assert sha(b/'activate_controllers.py')==oldsource and sha(b/'test_activate_controllers.py')==oldtest
stale=b/'consumers/primary-consumer-binding.json';value=json.loads(stale.read_bytes());assert value['afterany_parent']=='6873101'
assert value['consumer_kind']=='primary' and value['legacy_entry']['sha256']=='dd5f8d2aec90896fdcfe190971ea8b6c32a94158a3684566c2b8405748e7c794'
for j in ['8559679','8559680','8559681','8559683','8559708','8559725']:
 f=native(j);assert f['UserId']=='pwa209(3164768)' and f['JobState']=='PENDING' and f['Reason']=='JobHeldUser' and f['RunTime']=='00:00:00'
with exclusive_file_lock(r/'state/af-campaign-registration.mutex',timeout=180):
 history.mkdir(mode=0o700)
 atomic_json(history/'before.json',{'failed_job':'8634125','old_source_sha':oldsource,'old_test_sha':oldtest,'stale_descriptor_sha':sha(stale),'new_sources':{n:v['sha'] for n,v in payload.items()},'scientific_functions_changed':False,'new_supervisor_layers':0})
 for n,v in payload.items():
  raw=base64.b64decode(v['data']);assert hashlib.sha256(raw).hexdigest()==v['sha']
  (b/n).rename(history/n)
  with (b/n).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
  (b/n).chmod(0o444)
 stale.rename(history/'primary-consumer-binding-stale.json')
 request['native_tests']['source']={'path':str(b/'test_activate_controllers.py'),'sha256':payload['test_activate_controllers.py']['sha']}
 reqpath=history/'controller-activation-request.json';atomic_json(reqpath,request)
 args=shlex.split(oldtask['command']);flags=args[args.index(str(b/'activate_controllers.py'))+1:]
 updates={'--request':str(reqpath),'--request-sha256':sha(reqpath),'--source-sha256':payload['activate_controllers.py']['sha'],'--native-plan':str(d/'plan.json'),'--native-ledger':str(ledger),'--native-script':str(d/(name+'.sbatch')),'--native-task':name}
 for flag,v in updates.items():flags[flags.index(flag)+1]=v
 command=shlex.join(args[:args.index(str(b/'activate_controllers.py'))+1]+flags)
 text=text.replace(oldtask['command'],command).replace(oldsource,payload['activate_controllers.py']['sha']).replace(oldtest,payload['test_activate_controllers.py']['sha'])
 text=text.replace('hhw-'+oldtask['name'],'hhw-'+name)
 text=text.replace(oldtask['name']+'-%A_%a',name+'-%A_%a')
 assert text.count(command)==1 and oldsource not in text and oldtest not in text
 syntax=subprocess.run(['bash','-n'],input=text,text=True,capture_output=True,timeout=30);syntax.check_returncode()
 task={**oldtask,'name':name,'script':str(d/(name+'.sbatch')),'script_sha256':canonical_hash(text),'command':command}
 # Refresh actual final records before resolving parents no longer live in Slurm.
 budget.native_accounting(r,['8634115','8633757','6950429','7046404'],runner=lambda cmd,**kw:subprocess.run(cmd,**{**kw,'timeout':60}))
 plan={**oldplan,'phase':phase,'tasks':[task]};d.mkdir(mode=0o700)
 Path(task['script']).write_text(text);atomic_json(d/'plan.json',plan)
 counter=[0]
 def runner(cmd,**kw):
  counter[0]+=1;prefix=history/('mutation-'+str(counter[0]));atomic_json(prefix.with_suffix('.intent.json'),{'command':cmd,'automatic_replay_permitted':False})
  response=subprocess.run(cmd,**{**kw,'timeout':120});atomic_json(prefix.with_suffix('.response.json'),{'returncode':response.returncode,'stdout':response.stdout,'stderr':response.stderr});return response
 result=slurm.submit(d/'plan.json',ledger,runner=runner);job=result['tasks'][name]['job_id'];f=native(job)
 assert f['UserId']=='pwa209(3164768)' and f['JobState']=='PENDING' and f['Reason']=='JobHeldUser' and f['Command']==task['script'] and f['NumCPUs']=='2' and f['Requeue']=='0'
 atomic_json(history/'accepted-held.json',{'job_id':job,'native':f,'plan':str(d/'plan.json'),'ledger':str(ledger)})
 a=runner(['scontrol','release',job],text=True,capture_output=True);a.check_returncode();f=native(job)
 assert f['JobId']==job and (f['JobState'] in {'RUNNING','COMPLETING','COMPLETED'} or f['JobState']=='PENDING' and f.get('Reason') not in {'JobHeldUser','JobHeldAdmin'})
 receipt={'status':'binding_repair_activation_registered_and_released','job_id':job,'plan':str(d/'plan.json'),'ledger':str(ledger),'source_sha':payload['activate_controllers.py']['sha'],'native':f}
 atomic_json(history/'complete.json',receipt)
 print(json.dumps(receipt),flush=True)
PY
'''.replace('__PAYLOAD__',repr(payload))
Path('work/hhw-deploy-binding-repair-20261107-1017.sh').write_text(script,encoding='utf-8',newline='\n')
print('Built reviewed finite activation repair; no additional persistent supervisor.')
