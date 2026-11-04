#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
export TZ=UTC PYTHONDONTWRITEBYTECODE=1
python3 -B -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone,timedelta
import hashlib,json,os,re,shlex,subprocess,time
assert os.geteuid()==3164768
if hasattr(time,'tzset'):time.tzset()
r=Path('/scratch/pwa209/horsehair-worm');phase='study-census-resilience-v1-20261024'
directory=r/'plans'/phase;planpath=directory/'plan.json';ledgerpath=r/'state'/(phase+'-submission.json')
area=r/'maintenance/census-cadence-20261103-925';area.mkdir(mode=0o700,exist_ok=False)
targets=[('6983056',62,4),('6983059',63,8),('6983060',64,12)]
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def save(name,value):
 with (area/name).open('x') as f:json.dump(value,f,sort_keys=True,indent=2);f.flush();os.fsync(f.fileno())
def ordinary(path):
 p=Path(path);assert p.is_absolute() and p.is_relative_to(r) and p.resolve()==p and p.is_file()
 assert all(not x.is_symlink() for x in [p,*p.parents] if x.is_relative_to(r))
 assert p.stat().st_uid==3164768;return p
def invoke(label,command):
 save(label+'-intent.json',{'command':command,'timeout_seconds':30,'automatic_retry_permitted':False})
 try:
  p=subprocess.run(command,text=True,capture_output=True,timeout=30)
  result={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr,'timed_out':False}
 except subprocess.TimeoutExpired as e:
  def decoded(value):return value.decode(errors='replace') if isinstance(value,bytes) else value
  result={'returncode':None,'stdout':decoded(e.stdout),'stderr':decoded(e.stderr),'timed_out':True}
 save(label+'-response.json',result);return result
def fields(label,job):
 response=invoke(label,['scontrol','show','job',job,'-o'])
 assert response['returncode']==0 and not response['stderr'],'Native observation failed; preserve evidence and do not repeat mutations'
 parsed=dict(x.split('=',1) for x in shlex.split(response['stdout']) if '=' in x)
 assert parsed.get('JobId')==job;save(label+'-fields.json',parsed);return parsed
plan=json.loads(ordinary(planpath).read_bytes());ledger=json.loads(ordinary(ledgerpath).read_bytes())
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1';CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
assert plan['root']==str(r) and plan['phase']==phase and plan['config_sha256']==ledger['config_sha256']==CONFIG
assert plan['release_identity']['sha256']==ledger['source_sha256']==SOURCE and ledger['plan_sha256']==canonical(plan)
assert len(plan['tasks'])==64 and set(ledger['tasks'])=={t['name'] for t in plan['tasks']}
pins={str(planpath):sha(planpath),str(ledgerpath):sha(ledgerpath)};jobs={};tasks={}
for index,task in enumerate(plan['tasks'],1):
 name='rolling-storage-census-'+str(index).zfill(2);assert task['name']==name
 assert task['resources']=={'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None}
 assert task['depends']==([] if index==1 else [plan['tasks'][index-2]['name']])
 assert task['dependency_kind']==('afterok' if index==1 else 'afterany')
 script=ordinary(task['script']);assert script==directory/(name+'.sbatch')
 text=script.read_text();row=ledger['tasks'][name];job=row['job_id']
 assert row['state']=='submitted' and re.fullmatch('[1-9][0-9]*',job) and job not in jobs
 assert task['script_sha256']==row['script_sha256']==canonical(text)
 assert '#SBATCH --cpus-per-task=8' in text.splitlines() and '#SBATCH --mem=64G' in text.splitlines()
 assert '#SBATCH --time=6:00:00' in text.splitlines() and task['command'] in text
 provider=task['rolling_census_provider'];helper=ordinary(provider['helper']['path']);manifest=ordinary(provider['source_manifest']['path'])
 assert sha(helper)==provider['helper']['sha256'] and sha(manifest)==provider['source_manifest']['sha256']
 assert provider['case_id']=='rolling-'+str(index).zfill(2)
 argv=shlex.split(task['command']);at=argv.index(str(helper));tail=argv[at+1:];assert len(tail)%2==0
 flags=dict(zip(tail[::2],tail[1::2]));assert len(flags)*2==len(tail)
 assert flags['--native-plan']==str(planpath) and flags['--native-ledger']==str(ledgerpath) and flags['--native-task']==name and flags['--native-script']==str(script)
 assert flags['--source-manifest']==str(manifest) and flags['--source-manifest-sha256']==provider['source_manifest']['sha256'] and flags['--case-id']==provider['case_id']
 pins.update({str(script):sha(script),str(helper):sha(helper),str(manifest):sha(manifest)});jobs[job]=index;tasks[job]=task
def source_guard():
 for path,pin in pins.items():assert sha(path)==pin,'Original plan, ledger or source changed'
def dependency_guard(f,job,label):
 task=tasks[job];index=jobs[job];predecessor=ledger['tasks'][plan['tasks'][index-2]['name']]['job_id']
 row=ledger['tasks'][task['name']];assert row['dependency_kind']=='afterany' and predecessor in row['issued_native_dependencies']
 dep=f.get('Dependency','')
 if re.fullmatch('afterany:'+predecessor+r'(?:\((?:unfulfilled|fulfilled)\))?',dep):return
 assert dep in {'(null)','None',''},'Native dependency differs from the original source-bound chain'
 response=invoke(label+'-predecessor-final',['sacct','-X','-n','-P','-j',predecessor,'--format=JobIDRaw,State,ExitCode,ElapsedRaw,AllocCPUS,User,Account'])
 assert response['returncode']==0 and not response['stderr']
 rows=[line.split('|') for line in response['stdout'].splitlines() if line.strip()];assert len(rows)==1
 v=rows[0];assert len(v)==7 and v[0]==predecessor and v[1].split()[0].split('+')[0] in {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'} and re.fullmatch('[0-9]+:[0-9]+',v[2]) and v[3].isdigit() and v[4].isdigit() and v[5:]==['pwa209','def-ptewarie_cpu'],'Missing positive final predecessor observation'
def held_guard(f,job,label):
 task=tasks[job]
 assert f.get('UserId')=='pwa209(3164768)' and f.get('Account')=='def-ptewarie_cpu' and f.get('JobName')=='hhw-'+task['name'] and f.get('Command')==task['script']
 assert f.get('JobState')=='PENDING' and f.get('Reason')=='JobHeldUser' and f.get('Priority')=='0'
 assert f.get('NumCPUs')=='8' and f.get('NumNodes') in {'1','1-1'} and f.get('MinMemoryNode') in {'64G','65536M'} and f.get('TimeLimit') in {'06:00:00','6:00:00','0-06:00:00'} and f.get('TimeMin') in {'N/A','(null)','None'}
 assert 'ArrayJobId' not in f and 'ArrayTaskId' not in f and all('gpu' not in f.get(k,'').lower() for k in ('ReqTRES','AllocTRES','Gres','TresPerNode','TresPerTask'))
 dependency_guard(f,job,label)
def epoch(value):
 if isinstance(value,bool):return None
 if isinstance(value,(int,float)):return float(value)
 if isinstance(value,str):
  try:
   parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
   return (parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed).timestamp()
  except ValueError:return None
 return None
def text_time_proof(f,wanted):
 for key in ('EligibleTime','StartTime','BeginTime'):
  found=epoch(f.get(key))
  if found is not None and abs(found-wanted)<=1:return {'basis':'actual_native_text','field':key,'value':f[key],'epoch':found}
 return None
def json_time_proof(value,job,wanted):
 rows=value.get('jobs',[]);assert isinstance(rows,list) and len(rows)==1
 record=rows[0];assert str(record.get('job_id'))==job,'JSON observation belongs to another job'
 matches=[]
 def values(node,path):
  if isinstance(node,dict):
   if node.get('set') is False or node.get('infinite') is True:return
   for k,v in node.items():
    if k in {'number','value','timestamp','time'}:values(v,path+'.'+k)
  else:
   found=epoch(node)
   if found is not None and abs(found-wanted)<=1:matches.append({'basis':'actual_native_json','field':path,'value':node,'epoch':found})
 def visit(node,path):
  if isinstance(node,dict):
   for key,v in node.items():
    if re.sub('[^a-z]','',key.lower()) in {'eligible','eligibletime','eligibilitytime','start','starttime','begin','begintime'}:values(v,path+'.'+key)
    visit(v,path+'.'+key)
  elif isinstance(node,list):
   for i,v in enumerate(node):visit(v,path+'['+str(i)+']')
 visit(record,'jobs[0]');return matches[0] if matches else None
before={}
for job,slot,hours in targets:
 assert jobs[job]==slot
 f=fields('before-all-'+job,job);held_guard(f,job,'before-all-'+job);before[job]=f
source_guard();anchor=datetime.now(timezone.utc).replace(microsecond=0)
desired={job:{'utc':(anchor+timedelta(hours=hours)).strftime('%Y-%m-%dT%H:%M:%S'),'epoch':(anchor+timedelta(hours=hours)).timestamp(),'slot':slot} for job,slot,hours in targets}
save('validated-operation.json',{'source_pins':pins,'remote_anchor_utc':anchor.isoformat(),'desired':desired,'unchanged_job_61':'6983037','unchanged_quota_releaser':'8421133','new_jobs_submitted':0,'automatic_replay_permitted':False})
results={}
for job,slot,hours in targets:
 source_guard();pre=fields('before-update-'+job,job);held_guard(pre,job,'before-update-'+job)
 command=['scontrol','update','JobId='+job,'StartTime='+desired[job]['utc']]
 response=invoke('update-once-'+job,command)
 post=fields('after-update-'+job,job);held_guard(post,job,'after-update-'+job);source_guard()
 for key in ('JobId','UserId','Account','JobName','Command','NumCPUs','NumNodes','MinMemoryNode','TimeLimit','TimeMin','ReqTRES','WorkDir','StdOut','StdErr'):
  assert pre.get(key)==post.get(key),'Unrelated native contract changed: '+key
 proof=text_time_proof(post,desired[job]['epoch'])
 if proof is None:
  observed=invoke('after-json-'+job,['scontrol','--json','show','job',job])
  if observed['returncode']==0 and not observed['stderr']:
   try:proof=json_time_proof(json.loads(observed['stdout']),job,desired[job]['epoch'])
   except (ValueError,AssertionError,TypeError):proof=None
 results[job]={'status':'native_earliest_start_verified' if proof else 'native_time_not_observable_requires_review','desired':desired[job],'proof':proof,'update_response_returncode':response['returncode'],'update_timed_out':response['timed_out'],'held_priority_zero_verified':True,'source_and_resource_contract_preserved':True,'dependency_before':pre.get('Dependency'),'dependency_after':post.get('Dependency'),'automatic_replay_permitted':False}
 save('result-'+job+'.json',results[job])
result={'status':'three_existing_held_census_times_verified' if all(row['proof'] for row in results.values()) else 'update_commands_issued_native_time_proof_incomplete','jobs':results,'source_pins_unchanged':True,'jobs_submitted':0,'job_61_touched':False,'quota_releaser_touched':False,'audit_directory':str(area),'automatic_replay_permitted':False}
save('result.json',result);print('CENSUS_CADENCE_925 '+json.dumps(result),flush=True)
PY
