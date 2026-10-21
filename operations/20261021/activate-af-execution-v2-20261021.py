"""One-time source-bound policy and held/reviewed initial A-F support launch."""
from pathlib import Path
from datetime import datetime, timezone
import argparse, base64, json, os, re, runpy, shlex, subprocess, time, zlib
from horsehair.core import read_json,digest,source_identity,canonical_hash,atomic_json,exclusive_file_lock,native_job_id
from horsehair.slurm import _script,submit,completed_native_dependency
from horsehair.budget import resource_lease
from horsehair.execution_resources import LIMITS,load_resource_policy

ROOT=Path('/scratch/pwa209/horsehair-worm')
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
Q=ROOT/'maintenance/study-af-execution-support-v1-20261021/qualification.json'
Q_SHA='29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f'
CASE=ROOT/'maintenance/study-af-campaign-v1-20261021'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'

def require(value,message):
 if not value:raise ValueError(message)

def combined_admission(*args,**kwargs):
 from horsehair.campaign import combined_admission as admitted
 from horsehair import campaign,repair
 require(campaign._repair_native_states is repair._repair_native_states,'Campaign native reader was bound before the scoped guard')
 return admitted(*args,**kwargs)

def exclusive(path,raw):
 with Path(path).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())

def json_bytes(value):return (json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()

def seconds(value):
 match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',value);require(match is not None,'Finite native TimeLimit required');day,hour,minute,second=match.groups();return int(day or 0)*86400+int(hour)*3600+int(minute)*60+int(second)

def memory_bytes(value):
 match=re.fullmatch(r'(\d+)([MG])',value);require(match is not None,'Exact finite native node-memory units required');return int(match[1])*(1024**(2 if match[2]=='M' else 3))

def native_resources(fields,cpus,gpus,hours,memory):
 require(fields.get('NumCPUs')==str(cpus) and fields.get('NumNodes')=='1' and seconds(fields.get('TimeLimit',''))==hours*3600 and memory_bytes(fields.get('MinMemoryNode',''))==memory_bytes(memory),'Native CPU/node/time/memory request differs')
 for key in ['ReqTRES','AllocTRES']:
  text=fields.get(key,'')
  if not text or text=='(null)':
   require(key=='AllocTRES','Positive native requested resources missing');continue
  tres=dict(x.split('=',1) for x in text.split(',') if '=' in x)
  require(tres.get('gres/gpu','0').isdigit() and int(tres.get('gres/gpu','0'))==gpus,'Native total GPU request differs')
  typed=[int(v) for k,v in tres.items() if k.startswith('gres/gpu:') and v.isdigit()]
  require(not typed or sum(typed)==gpus,'Native typed GPU request differs')

def main():
 p=argparse.ArgumentParser();p.add_argument('--payload',required=True,type=Path);p.add_argument('--payload-sha256',required=True);a=p.parse_args()
 require(os.geteuid()==3164768 and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and str(native_job_id()).isdigit(),'Owned native one-CPU management allocation required')
 require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,'Unchanged qualified operational and primary scientific source required')
 config=read_json(PRIMARY/'configs/study.json');require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Scientific configuration changed')
 require(digest(Q)==Q_SHA and digest(GUARD)==GUARD_SHA and digest(SCHEDULER)==SCHEDULER_SHA,'Qualification or native query guard changed')
 q=read_json(Q);require(q['status']=='complete' and q['returncode']==0 and q['source_sha256']==SOURCE and q['config_sha256']==CONFIG and q['source_verified_unchanged'] is True and q['test_counts']=={'tests':1694,'failures':0,'errors':0,'skipped':0} and completed_native_dependency(ROOT,'6684484') is not None,'Full native 1694-test qualification required')
 require(digest(a.payload)==a.payload_sha256 and a.payload.resolve().is_relative_to(ROOT/'maintenance'),'Exact private support payload required');payload=read_json(a.payload)
 require(not CASE.exists(),'One-time activation case already exists; never replay')
 CASE.mkdir(mode=0o700)
 runpy.run_path(str(GUARD))['install'](CASE/'activation-accounting-reads')
 reader=runpy.run_path(str(SCHEDULER))['run_scontrol_job'];parent=reader(str(native_job_id()),CASE/'activation-scheduler-reads');parent.check_returncode();parent_fields=dict(re.findall(r'(?<!\S)(\w+)=([^\s]*)',parent.stdout))
 require(parent_fields.get('JobId')==str(native_job_id()) and parent_fields.get('JobName')=='hhw-recovery-207' and parent_fields.get('UserId')=='pwa209(3164768)' and parent_fields.get('Account')=='def-ptewarie_cpu' and parent_fields.get('JobState')=='RUNNING' and parent_fields.get('Command')==str(ROOT/'plans/recovery-support-20261021-207/recovery-207.sbatch'),'Activator native identity/script differs')
 native_resources(parent_fields,1,0,1,'16G');atomic_json(CASE/'activation-native-proof.json',parent_fields)
 for name,item in payload.items():
  require(re.fullmatch(r'[A-Za-z0-9_.-]+',name),'Unsafe payload name');raw=zlib.decompress(base64.b64decode(item['data']));require(len(raw)==item['bytes'] and __import__('hashlib').sha256(raw).hexdigest()==item['sha256'],'Support payload changed');exclusive(CASE/name,raw)
 worker=CASE/'native-worker.py';request=CASE/'agent-v2-native-canary-request.json'
 require(digest(worker)==payload[worker.name]['sha256']=='f00d4265a68a3ace5faa47da4fe2701fff3fe86d9132b09ea748e9f12471e4fd' and digest(request)=='c2d40326a7c24e09794647f725c24cf9682ddfc4d66709db4113e5d05fa1c9ae','Exact reviewed worker/canary request required')
 approval={'source':'current_user','statement':'I want you to complete ABCDEF. you can lift the restrictions on cpu or gpu. for any unsupported stuff, try to create possibilities of completing them, including but not limited to new data acquisition etc.','evidence_reference':'Current user instruction, 21 October 2026, complete ABCDEF and lift CPU/GPU restrictions','approved_at_utc':datetime.now(timezone.utc).isoformat(),'authorizes_cpu_gpu_resource_amendment':True,'identity_verification':'instruction_provenance_only'}
 policy_id='af-expanded-20261021-v1';policy_path=ROOT/'maintenance/execution-resource-policies'/policy_id/'policy.json';require(not policy_path.parent.exists(),'Operational resource policy already exists')
 policy_path.parent.mkdir(parents=True,mode=0o700)
 record={'schema_version':1,'kind':'approved-operational-resource-policy','policy_id':policy_id,'study_root':str(ROOT),'base_config_sha256':CONFIG,'primary_scientific_source_sha256':PRIMARY_SHA,'execution_source_sha256':SOURCE,'limits':dict(LIMITS),'approval':approval,'scientific_gates_changed':False};exclusive(policy_path,json_bytes(record));policy_path.chmod(0o444)
 policy=load_resource_policy(policy_path,digest(policy_path),root=ROOT,config=config)
 descriptor={'schema_version':1,'case':str(CASE),'framework_release':str(FRAMEWORK),'execution_source_sha256':SOURCE,'primary_source_sha256':PRIMARY_SHA,'full_config_sha256':CONFIG,'qualification':{'path':str(Q),'sha256':Q_SHA,'job_id':'6684484','test_counts':q['test_counts']},'resource_policy':{'path':str(policy.path),'sha256':policy.sha256},'worker_path':str(worker),'worker_sha256':digest(worker),'canary_request':{'path':str(request),'sha256':digest(request)},'frozen_protocol_sha256':'7da54fc8ef538ed4593b5cd20227647c6f7fa1bee340f57df31daab51ac357c4','cohort_case':str(ROOT/'maintenance/behaviour-full-cohort-profile-v1-20261021'),'agent_campaign_case':str(CASE/'agent-final-f64-v1'),'scientific_gates_changed':False}
 dpath=CASE/'descriptor-v1.json';exclusive(dpath,json_bytes(descriptor));dsha=digest(dpath)
 with resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=1,job_id=str(native_job_id()),resource_policy=policy):
  with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=120):
   combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],CASE,'pre-registration-admission',resource_policy=policy,cpus=9,gpus=1,hours=2)
   directory=ROOT/'plans/study-af-canaries-v1-20261021';ledger_path=ROOT/'state/study-af-canaries-v1-20261021-submission.json';require(not directory.exists() and not ledger_path.exists(),'Immutable canary plan already registered');directory.mkdir(mode=0o700)
   tasks=[]
   for name,mode,cpus,gpus,hours,memory in [('af-gpu-qualification','gpu-qualify',4,1,1,'32G'),('af-cohort-profile','cohort-profile',1,0,2,'64G'),('af-precision-plan','precision-plan',4,0,1,'32G')]:
    command=shlex.join(['python','-u',str(worker),'--descriptor',str(dpath),'--descriptor-sha256',dsha,'--mode',mode,'--cpus',str(cpus),'--gpus',str(gpus),'--hours',str(hours)])
    text=_script(config,FRAMEWORK,name,command,cpus=cpus,memory=memory,hours=hours,gpu=bool(gpus));text=text.replace('set -euo pipefail','set -euo pipefail\nexport PYTHONDONTWRITEBYTECODE=1',1);script=directory/(name+'.sbatch');exclusive(script,text.encode())
    tasks.append({'name':name,'script':str(script),'script_sha256':canonical_hash(text),'command':command,'depends':[],'resources':{'cpus':cpus,'gpu':bool(gpus),'hours':hours,'memory':memory,'array':None}})
   plan={'schema_version':2,'phase':'af-execution-support','root':str(ROOT),'config_sha256':CONFIG,'release_identity':source_identity(FRAMEWORK),'tasks':tasks,'cpu_ceiling':256,'gpu_concurrency':4,'maximum_simultaneous_resources':{'cpus':9,'gpus':1},'resource_policy':policy.receipt()};exclusive(directory/'plan.json',json_bytes(plan))
   exclusive(CASE/'submission-intent.json',json_bytes({'descriptor_path':str(dpath),'descriptor_sha256':dsha,'plan_path':str(directory/'plan.json'),'plan_sha256':canonical_hash(plan),'ledger_path':str(ledger_path),'submission_mode':'held; exact owner/command/resources then global registration admission before release','automatic_replay_permitted':False}))
   def held(command,**kwargs):
    require(command[0]=='sbatch' and '--hold' not in command,'Only single reviewed held sbatch calls allowed')
    return subprocess.run([command[0],'--hold',*command[1:]],timeout=30,**kwargs)
   ledger=submit(directory/'plan.json',ledger_path,runner=held);held_records={}
   reader=runpy.run_path(str(SCHEDULER))['run_scontrol_job']
   for task in tasks:
    job=ledger['tasks'][task['name']]['job_id'];response=reader(job,CASE/('held-scheduler-'+job));response.check_returncode();fields=dict(re.findall(r'(?<!\S)(\w+)=([^\s]*)',response.stdout));resources=task['resources'];account=config['paths']['gpu_account' if resources['gpu'] else 'cpu_account']
    require(fields.get('JobId')==job and fields.get('JobName')=='hhw-'+task['name'] and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')==account and fields.get('Command')==task['script'] and fields.get('NumCPUs')==str(resources['cpus']) and fields.get('JobState')=='PENDING' and fields.get('Reason')=='JobHeldUser','Held native job does not match exact registered source/resource/owner contract')
    native_resources(fields,resources['cpus'],int(resources['gpu']),resources['hours'],resources['memory'])
    held_records[job]=fields
   atomic_json(CASE/'held-native-proof.json',held_records)
   combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],CASE,'post-registration-admission',resource_policy=policy)
   atomic_json(CASE/'release-intent.json',{'job_ids':list(held_records),'descriptor_sha256':dsha,'held_native_proof_sha256':digest(CASE/'held-native-proof.json'),'plan_sha256':canonical_hash(plan),'automatic_replay_permitted':False})
   for job in held_records:subprocess.run(['scontrol','release',job],capture_output=True,text=True,check=True,timeout=30)
   receipt={'status':'initial_af_canaries_registered_and_released','native_activation_job_id':str(native_job_id()),'descriptor_path':str(dpath),'descriptor_sha256':dsha,'worker_sha256':digest(worker),'resource_policy':policy.receipt(),'qualification_sha256':Q_SHA,'plan_path':str(directory/'plan.json'),'ledger_path':str(ledger_path),'tasks':{name:row['job_id'] for name,row in ledger['tasks'].items()},'source_sha256':SOURCE,'scientific_gates_changed':False,'created_utc':datetime.now(timezone.utc).isoformat()};atomic_json(CASE/'activation-receipt.json',receipt)
 print('AF_ACTIVATED '+json.dumps({k:v for k,v in receipt.items() if k!='resource_policy'},sort_keys=True),flush=True)

if __name__=='__main__':main()
