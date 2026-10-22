"""One explicit native repair of positively never-submitted state references."""
from pathlib import Path
from datetime import datetime,timezone
import argparse,base64,gzip,hashlib,io,json,os,re,runpy,shlex,subprocess,sys,time,xml.etree.ElementTree as ET,zlib
from horsehair.core import canonical_hash,digest,read_json,source_identity,exclusive_file_lock
ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CASE=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
AREA=ROOT/'maintenance/study-af-unsubmitted-reconciliation-stage-v1-20261022'
PHASE='study-af-f2-registry-r1-controllers-v3-20261021'
CONTROLLER=ROOT/'maintenance/study-af-registry-repair-support-v4-20261021/run-af-agent-controller-registry-v4-20261021.py'
CONTROLLER_SHA='475a170aab2d158cfd17adb250707e1d0767086d4c50b95516c360f2222560f8'
DESCRIPTOR=CONTROLLER.parent/'native-activation-v3/descriptor-v6-registry-controller.json'
DESCRIPTOR_SHA='9ef405cf3f012f21bdec79da147c3fc57ca89e90dbe6124cacf8a5b4a0f7c9d8'
IDS=['6701494','6701495','6701498']
FALSE={'scientific_result':False,'admission_authority':False,'original_files_removed':False,'automatic_replay_permitted':False}

def require(v,message):
 if not v:raise ValueError(message)

def raw(path,maximum=2000000):
 require(path.is_file() and not path.is_symlink() and path.stat().st_uid==3164768 and path.stat().st_size<=maximum,'Bounded owned ordinary metadata required')
 with path.open('rb') as stream:value=stream.read(maximum+1)
 require(len(value)<=maximum,'Bounded metadata required');return value

def save(name,value):
 body=value if type(value) is bytes else (json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
 with (AREA/name).open('xb') as stream:stream.write(body);stream.flush();os.fsync(stream.fileno())

def native(job):
 p=subprocess.run(['scontrol','show','job',job,'-o'],capture_output=True,text=True,check=True,timeout=45)
 lines=[x for x in p.stdout.splitlines() if x.strip()];require(len(lines)==1,'One exact native allocation required')
 pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);f=dict(pairs);require(len(f)==len(pairs),'Unique native fields required')
 require(f.get('JobId')==job and f.get('UserId')=='pwa209(3164768)' and f.get('Account')=='def-ptewarie_cpu','Exact owned native job required')
 require(all('gpu' not in f.get(k,'').lower() for k in ['ReqTRES','AllocTRES','TresPerNode','TresPerJob','TresPerTask','Gres']),'CPU only required')
 return f

def guards(check_source=False):
 if check_source:require(source_identity(FRAMEWORK)['sha256']==SOURCE and digest(CONTROLLER)==CONTROLLER_SHA and digest(DESCRIPTOR)==DESCRIPTOR_SHA,'Unchanged qualified public/controller source required')
 require(digest(CONTROLLER)==CONTROLLER_SHA and digest(DESCRIPTOR)==DESCRIPTOR_SHA,'Unchanged controller/descriptor bytes required')
 queue=subprocess.run(['squeue','-h','-u','pwa209','--name',','.join('hhw-af-f2-registry-r1-v3-controller-'+str(i) for i in range(4)),'-o','%i|%T'],capture_output=True,text=True,check=True,timeout=45)
 require(all(row.split('|')==[job,'PENDING'] for row in queue.stdout.splitlines() if row.strip() for job in [row.split('|')[0]]) and {row.split('|')[0] for row in queue.stdout.splitlines() if row.strip()}==set(IDS),'Only the three held successors may exist in the live controller queue')
 plan=json.loads(raw(ROOT/'plans'/PHASE/'plan.json'));ledger=json.loads(raw(ROOT/'state'/(PHASE+'-submission.json')))
 require(canonical_hash(plan)==ledger['plan_sha256'] and plan['release_identity']['sha256']==ledger['source_sha256']==SOURCE,'Controller registration changed')
 output={}
 for i,job in enumerate(IDS,1):
  f=native(job);script=ROOT/'plans'/PHASE/('af-f2-registry-r1-v3-controller-'+str(i)+'.sbatch')
  require(f.get('JobState')=='PENDING' and f.get('Reason')=='JobHeldUser' and f.get('Command')==str(script) and f.get('JobName')=='hhw-af-f2-registry-r1-v3-controller-'+str(i),'Only three exact held successors permitted')
  require(f.get('NumCPUs')=='1' and f.get('MinMemoryNode')=='16G' and f.get('TimeLimit')=='4-00:00:00','Unchanged controller resources required')
  task=next(t for t in plan['tasks'] if t['script']==str(script))
  require(ledger['tasks'][task['name']]['job_id']==job and canonical_hash(script.read_text())==task['script_sha256'],'Exact immutable registered controller script required')
  output[job]=f
 return output

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--native-script',type=Path,required=True);a=parser.parse_args()
 require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209' and Path(__file__).resolve().parent==AREA,'Exact owned native maintenance namespace required')
 os.environ['TZ']='UTC';time.tzset();job=os.environ.get('SLURM_JOB_ID','');require(re.fullmatch('[1-9][0-9]*',job) is not None,'Scalar native job required')
 intent=json.loads(raw(AREA/'submission-intent.json'));require(intent['helper_sha256']==digest(Path(__file__)) and intent['args']==['--native-script',str(a.native_script)] and intent['cpus']==1 and intent['hours']==1 and intent['memory']=='16G' and intent['framework']==str(FRAMEWORK),'Registered helper bytes and resources required')
 f=native(job);require(f.get('JobState')=='RUNNING' and f.get('NumCPUs')=='1' and f.get('MinMemoryNode')=='16G' and f.get('TimeLimit')=='01:00:00' and f.get('Command')==str(a.native_script),'Exact running 1CPU16G1h repair allocation required')
 plan=json.loads(raw(a.native_script.parent/'plan.json'));ledger=json.loads(raw(ROOT/'state'/(a.native_script.parent.name+'-submission.json')));task=next(t for t in plan['tasks'] if t['script']==str(a.native_script))
 require(canonical_hash(plan)==ledger['plan_sha256'] and ledger['source_sha256']==plan['release_identity']['sha256']==SOURCE and ledger['tasks'][task['name']]['job_id']==job and canonical_hash(a.native_script.read_text())==task['script_sha256'],'Exact own registered script/plan/ledger required')
 require(task['command']==shlex.join(['python','-u',str(Path(__file__)),'--native-script',str(a.native_script)]),'Direct exact repair task required')
 require(Path(sys.modules['horsehair'].__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py','Qualified package interpreter required')
 end=float(os.environ['SLURM_JOB_END_TIME']);require(time.time()+900<end,'Finite repair margin required')
 save('own-allocation.json',{'native':f,'plan_sha256':canonical_hash(plan),'helper_sha256':digest(Path(__file__)),**FALSE})
 # All source-only contracts execute before observing or changing campaign state.
 bootstrap_sources={'reconcile-unsubmitted-f2-intents-v1-20261022.py': {'sha256': '6cb420833a40fef8a1053b24602de9e9598ff5a0c405955d28ffa7b091f788fd', 'data': 'eJyFVm1v2zYQ/m7A/4HLF9mbGuiFekugD0XnYR+GrkDTYUBgGBR5ilXLlCrJbrIg/31H0pJsx2kRwBHJ4/G555478urqavFYlwUvOlLIPTQtkConebVrSI2jou1A2KSu2qIr9lA+EQlo9a7dZduiwzXc1YHsikq219PJdPK3RJsGcmhAciBZVW02AHUhHwhfM/kA7TVZPKJbNfO1ylqbVLuOV1toCZPobrvddSwrYToxnnG6AVI30EKz1+eRbo0gm+KhkKwkbcc6IK1kdbuuOpJBXqE9k08EnSJGRHV1dYXetnXVdDhZP00neVNtybrCaNesaK652tIbMFnJgrNytWbtejpZ/Ptp8eFu8Xv6PJ0Qi7UtbLPy6Z3j+IF1YwV+mNDYcRJfeIIHDsVfSiNIRO7nLPbyjHmeAw7lIhZB5mdh4LtxmDEWJCH+s+xzr7Hy6grhu2FMnYCzMPEBMpfRwOHCpyJEHwHNgijMBMODGADNwWE0DCEQ4HKfGq+wZ+WOqdQov8k7x0XXuUv9wIkDoJHrUN+j1GMZj+Lci6NYOLkX+eBRiOMgDgT+ck5xB49o7DHugGtcdw0rJGYQHVMfvca+4PjnBSIIQwSDKHicZ8Ln3KVRlFAe5H5Cqet6Thh7LhOJEwZxFnm56wLPrJfp5PPd+7vF6vOf71PLyRwQWegnApwYic6CLIwZoL3vZpHr5ZB5gYcUBdxJIsFDV8QUqB9BECfc5dZ08umv9x+NMz/MI5EFgkeJ46I76gANaBhRxwmp57qIKqYiS7wk8wUGzoB7WcISx4kjLwLHUrIWkKOqv+2KBmaKVrBRsC17gPkN8lHkRKL29MINUoNF9I/6XjRN1cx6y9EPryQvSphp7dosa1Wt2L/a8FgDx6Ja6YVVu2bpwMq4eKiLtFemhtCD655qmB08zknRElHwTldWC92wkKZqdO5xblsfqm1dAhYUPDK1z9gT2BdCfxyOEdb86EyN9t4aQHtBaC3T9HU0Bsdl69O6mz1vbvbYhhqysfeq5rX5ddHBtp3NFeGbX9JTHy8If6FR76RpNYezfgi6Lpk8QtHL5hzprlWrFnrHLmgdr0oA0a4aqLGRKJv7pW19GQCYDQT7zX8gsbds0exBXkb0vHkV8L2lWqS1PA58rydXhbCWKr8fKwkvb+bzi+wnkRXGN7L6XoJ4OO7bqlvv2gFLuyu7VHXJa4F9W30YqtSqgifZFmyVS8R4fmAPUwkSW3ZV5elBQfdq21JND/wjYG2CYnw2adC2nWVbGmIzjr/idYS9fpyQTNG6+raDHaxgW3dP4+QaL5eqeTpMv+hcsbKc7RVbd80OdBiaY33+tS5bRI10fTrcc+ruMHXQV4A2Pc2bur5SQ1ifJxPmraEjxbl7y3xby1sOZTmYq4GyN6tmjCwKeLSWr2hCP5qkgyu7z7/dS3OIXaPEIbYYKOruEL3GcazibVY87CocDeQok82gJ6NDxdD9eNYbRyyRNn3pm4fBicZ0Co6Uhi8K82ow/PUx9iSY/1rZaaqYNC+C09WxVvuG8pPOYfadt45Tby/z0d2QkkOxrt5oEX23GV4sGJ4U6llzXFrHetlggtPevRqgKnoOlABUjFoBaXpJFyZYbdfL07SAP1jZwgBHDtod3khq0xkWRYOCkw53uaWrlpwC6hdXKIOTrJjl8YmhWRo70mUbUySqRx6t948fc8QFB3hbikK/MNXe5xfbujugIphwIkDxjmwNt5SJkFyMQPlWq1CODIwIL3JwMUh135/h/Gkgr5j40dr9O/eY7xLkazjGcj7K5TvD22l5KMiStd2gyXHX0ev8rB7fgnJdV/Vsfs5aH+5Fzk65MHH0Cu0h9TZHgEaN3l70M2avvfDQwttONR953HFQIQc9CCjJpW493Ho6zNM3xfy239GguFqlkOVvqTvMnj1izjqRsTLXaoc32OGc6eR/o6mSxg=='}, 'test_reconcile_unsubmitted_f2_intents_v1_20261022.py': {'sha256': '1d4f1e906bfa935566c97897f8eecdaa6fd787107d6c1efa931a2c882501d4c5', 'data': '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'}}
 for name,item in bootstrap_sources.items():
  value=zlib.decompress(base64.b64decode(item['data']));require(hashlib.sha256(value).hexdigest()==item['sha256'],'Frozen private contract bytes required')
  save(name,value)
 tmp=Path(os.environ['SLURM_TMPDIR'])/('hhw-never-submitted-'+job);tmp.mkdir(mode=0o700,exist_ok=False)
 env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','PYTHONNOUSERSITE':'1'}
 for k in ['PYTEST_ADDOPTS','PYTEST_PLUGINS']:env.pop(k,None)
 xml=AREA/'contracts34.xml'
 subprocess.run([sys.executable,'-s','-m','pytest',str(AREA/'test_reconcile_unsubmitted_f2_intents_v1_20261022.py'),'-q','--noconftest','-p','no:cacheprovider','--rootdir',str(AREA),'--basetemp',str(tmp/'fixtures'),'--junitxml',str(xml)],env=env,check=True,timeout=180)
 cases=list(ET.fromstring(raw(xml)).iter('testcase'));rows=sorted((x.get('classname','').split('.')[-1],x.get('name','')) for x in cases)
 require(len(cases)==len(set(rows))==34 and canonical_hash(rows)=='6715cc0f2b73194a437c58b5212ee50e7644477f4049b9e4137312908d96262e' and all(not any(x.find(k) is not None for k in ['failure','error','skipped']) for x in cases),'Actual complete passing 34-test roster required')
 for name,item in bootstrap_sources.items():require(digest(AREA/name)==item['sha256'],'Private source changed during tests')
 repair=runpy.run_path(str(AREA/'reconcile-unsubmitted-f2-intents-v1-20261022.py'))
 from horsehair import agent_campaign as dispatch
 held=guards(check_source=True);save('held-successors-before.json',held)
 # No accounting or source scan occurs inside this existing kernel mutex.
 with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=120):
  guards()
  binding_raw=raw(CASE/'state-binding.json');binding=json.loads(binding_raw);compressed=raw(CASE/'state.json.gz',8000000)
  require(hashlib.sha256(compressed).hexdigest()==binding['state_file_sha256'],'Original compressed state binding required')
  with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:plain=stream.read(64000001)
  require(len(plain)<=64000000,'Bounded state required');state=json.loads(plain);plan_raw=raw(CASE/'campaign-plan.json');campaign_plan=json.loads(plan_raw)
  require(state['state_sha256']==binding['state_sha256'] and state['revision']==binding['revision']==213 and state['plan_sha256']==binding['campaign_plan_sha256'],'Exact original revision213 required')
  dispatch._state(campaign_plan,state)
  for name,value in state['jobs'].items():
   if value['job_id'] is None:continue
   journal_path=CASE/('submission-r1-training-0000.json' if name=='training-0000' else 'submission-'+name+'.json')
   journal=json.loads(raw(journal_path))
   require(journal.get('status')=='released' and journal.get('job_id')==value['job_id'] and journal.get('intent')==value['intent'],'Every acknowledged job must have its exact released journal')
  absence={};native_evidence={}
  for name in repair['EXPECTED']:
   namespace='af-f2-'+name
   paths=[ROOT/'plans'/namespace,ROOT/'state'/(namespace+'-submission.json'),CASE/('submission-'+name+'.json')]
   require(all(not p.exists() and not p.is_symlink() for p in paths),'Any pre-existing submission artifact requires separate reconciliation')
   queue=subprocess.run(['squeue','-h','-u','pwa209','--name','hhw-'+namespace,'-o','%i|%j|%T'],capture_output=True,text=True,check=True,timeout=45)
   history=subprocess.run(['sacct','-X','-u','pwa209','--name','hhw-'+namespace,'-S','2026-10-21T00:00:00','--format=JobIDRaw,JobName%128,User,Account,State','--noheader','--parsable2'],capture_output=True,text=True,check=True,timeout=45)
   require(not queue.stdout.strip() and not history.stdout.strip(),'A matching native job exists; never resubmit')
   absence[name]={k:True for k in ['plan_absent','ledger_absent','journal_absent','native_queue_empty','native_history_empty']};native_evidence[name]={'paths':[str(p) for p in paths],'queue':queue.stdout,'history':history.stdout,'queue_stderr':queue.stderr,'history_stderr':history.stderr}
  candidate=repair['reconcile'](state,absence);dispatch._state(campaign_plan,candidate)
  regenerated,intents=dispatch.dispatch_tick(campaign_plan,candidate,{}, {})
  require({v['intent_id']:v for v in intents}=={name:state['jobs'][name]['intent'] for name in repair['EXPECTED']},'Unchanged dispatcher must regenerate exactly the four identical frozen intentions')
  # The diagnostic dispatch is never saved or submitted.
  require(raw(CASE/'state-binding.json')==binding_raw and raw(CASE/'state.json.gz',8000000)==compressed and raw(CASE/'campaign-plan.json')==plan_raw,'State changed during reconciliation')
  guards()
  for name,value in [('original-state.json.gz',compressed),('original-state-binding.json',binding_raw),('original-campaign-plan.json',plan_raw),('absence-evidence.json',native_evidence),('candidate-state.json',candidate)]:save(name,value)
  controller=runpy.run_path(str(CONTROLLER),run_name='explicit_state_save_only')
  require(digest(CONTROLLER)==CONTROLLER_SHA,'Original state writer bytes required')
  save('commit-intent.json',{'before_state_sha256':state['state_sha256'],'after_state_sha256':candidate['state_sha256'],'removed_unsubmitted_references':repair['EXPECTED'],'native_jobs_submitted':[],**FALSE})
  controller['save_state']({'case':CASE},candidate)
  saved_plan,saved_state=controller['initialize']({'case':CASE})
  require(saved_plan==campaign_plan and saved_state==candidate,'Qualified writer/readback failed')
  save('state-commit-verified.json',{'state_sha256':candidate['state_sha256'],'revision':candidate['revision'],'remaining_jobs':len(candidate['jobs']),'bound_jobs_unchanged':candidate['jobs']=={k:v for k,v in state['jobs'].items() if k not in repair['EXPECTED']},**FALSE})
 # The three previously held successors are released once, last segment first.
 guards(check_source=True)
 for successor in reversed(IDS):
  save('release-'+successor+'-intent.json',{'job_id':successor,'command':['scontrol','release',successor],**FALSE})
  p=subprocess.run(['scontrol','release',successor],capture_output=True,text=True,timeout=45)
  save('release-'+successor+'-response.json',{'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr});p.check_returncode()
 save('receipt.json',{'status':'reconciled_never_submitted_intentions_and_released_successors','native_job_id':job,'state_revision':candidate['revision'],'state_sha256':candidate['state_sha256'],'original_state_preserved':True,'native_scientific_jobs_submitted_by_repair':[],'released_controller_successors':IDS,'test_counts':{'tests':34,'failures':0,'errors':0,'skipped':0},**FALSE})
 print('F2_NEVER_SUBMITTED_RECONCILIATION '+(AREA/'receipt.json').read_text(),flush=True)

if __name__=='__main__':main()
