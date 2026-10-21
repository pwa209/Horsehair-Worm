"""Explicit reconciliation of three registered, unreleased initial A-F jobs."""
from pathlib import Path
import json,os,re,runpy,subprocess,time
from horsehair.core import read_json,digest,source_identity,canonical_hash,atomic_json,exclusive_file_lock,native_job_id
from horsehair.execution_resources import load_resource_policy
from horsehair.budget import resource_lease

ROOT=Path('/scratch/pwa209/horsehair-worm')
CASE=ROOT/'maintenance/study-af-campaign-v1-20261021'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
DESCRIPTOR_SHA='987c4c3bec38ca3a28d14cb1fbe22675bfcd6e5d2a465c8eb9adf01270b5b4bc'
ACTIVATOR=ROOT/'maintenance/study-af-activation-stage-v2-20261021/activate-af.py'
ACTIVATOR_SHA='d4665603e04dad0d928af69558e2297ad15615cecf6808702b7251d3ccb32af1'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
SCHEDULER=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
JOBS={'af-gpu-qualification':'6685917','af-cohort-profile':'6685918','af-precision-plan':'6685919'}

def require(value,message):
 if not value:raise ValueError(message)

def exact_one_node(value):
 return value in {'1','1-1'}

def parse_fields(text):
 pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',text);fields=dict(pairs)
 require(len(fields)==len(pairs),'Duplicate scheduler fields')
 return fields

def native_resources(fields,resources,helpers):
 require(fields.get('NumCPUs')==str(resources['cpus']) and exact_one_node(fields.get('NumNodes')) and helpers['seconds'](fields.get('TimeLimit',''))==resources['hours']*3600 and helpers['memory_bytes'](fields.get('MinMemoryNode',''))==helpers['memory_bytes'](resources['memory']),'CPU/node/time/memory request differs')
 for key in ['ReqTRES','AllocTRES']:
  value=fields.get(key,'')
  if not value or value=='(null)':require(key=='AllocTRES','Requested TRES missing');continue
  pairs=[x.split('=',1) for x in value.split(',') if '=' in x];tres=dict(pairs)
  require(len(tres)==len(pairs),'Duplicate native TRES')
  gpus=int(resources['gpu']);require(tres.get('gres/gpu','0').isdigit() and int(tres.get('gres/gpu','0'))==gpus,'GPU total differs')
  typed=[v for k,v in tres.items() if k.startswith('gres/gpu:')]
  require(all(v.isdigit() for v in typed) and (not typed or sum(map(int,typed))==gpus),'GPU typed request differs')

def main():
 job=str(native_job_id());require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209' and job.isdigit() and os.environ.get('SLURM_CPUS_PER_TASK')=='1','Owned native management allocation required')
 require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval context required')
 require(source_identity(FRAMEWORK)['sha256']==SOURCE and digest(ACTIVATOR)==ACTIVATOR_SHA,'Exact qualified/reviewed source required')
 require(digest(GUARD)=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91' and digest(SCHEDULER)=='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3','Reader bytes changed')
 dpath=CASE/'descriptor-v1.json';require(digest(dpath)==DESCRIPTOR_SHA,'Initial descriptor changed');d=read_json(dpath)
 config=read_json(PRIMARY/'configs/study.json');require(canonical_hash(config)==d['full_config_sha256'] and source_identity(PRIMARY)['sha256']==d['primary_source_sha256'] and read_json(FRAMEWORK/'configs/study.json')==config,'Original scientific bindings changed')
 require(digest(Path(d['worker_path']))==d['worker_sha256']=='f00d4265a68a3ace5faa47da4fe2701fff3fe86d9132b09ea748e9f12471e4fd','Reviewed worker changed')
 area=CASE/'initial-held-reconciliation-v1';require(not area.exists() and not (CASE/'activation-receipt.json').exists() and not (CASE/'release-intent.json').exists(),'Explicit one-time reconciliation cannot replay an existing release')
 area.mkdir(mode=0o700)
 runpy.run_path(str(GUARD))['install'](area/'accounting-reads')
 from horsehair.campaign import combined_admission
 from horsehair import campaign,repair
 require(campaign._repair_native_states is repair._repair_native_states,'Guard must precede campaign import')
 helpers=runpy.run_path(str(ACTIVATOR),run_name='held_resource_helpers_only');reader=runpy.run_path(str(SCHEDULER))['run_scontrol_job']
 own=reader(job,area/'own-scheduler');own.check_returncode();ownfields=parse_fields(own.stdout)
 require(ownfields.get('JobId')==job and ownfields.get('UserId')=='pwa209(3164768)' and ownfields.get('JobState')=='RUNNING' and ownfields.get('Account')==config['paths']['cpu_account'] and ownfields.get('Command')==str(ROOT/'plans/recovery-support-20261021-211/recovery-211.sbatch'),'Reconciliation allocation differs')
 native_resources(ownfields,{'cpus':1,'gpu':False,'hours':1,'memory':'16G'},helpers)
 old=reader('6685623',area/'failed-activation-scheduler');old.check_returncode();failed=parse_fields(old.stdout)
 require(failed.get('UserId')=='pwa209(3164768)' and failed.get('JobState')=='FAILED' and failed.get('ExitCode')=='1:0' and failed.get('Command')==str(ROOT/'plans/recovery-support-20261021-207/recovery-207.sbatch'),'Original exact failed activation proof differs')
 policy=load_resource_policy(Path(d['resource_policy']['path']),d['resource_policy']['sha256'],root=ROOT,config=config)
 directory=ROOT/'plans/study-af-canaries-v1-20261021';plan=read_json(directory/'plan.json');ledger_path=ROOT/'state/study-af-canaries-v1-20261021-submission.json';ledger=read_json(ledger_path)
 require(canonical_hash(plan)==ledger['plan_sha256']=='fd685add1fa7e2a12727a3558d49d40ccf79d1834512991fd06905c791401b38' and ledger['source_sha256']==SOURCE,'Initial plan/ledger changed')
 require(set(ledger['tasks'])==set(JOBS) and len(plan['tasks'])==3 and {x['name'] for x in plan['tasks']}==set(JOBS),'Exact original task roster required')
 with resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
  with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=120):
   proof={}
   for task in plan['tasks']:
    name=task['name'];ident=JOBS[name];entry=ledger['tasks'][name];require(entry['job_id']==ident and entry['state']=='submitted' and entry['script_sha256']==task['script_sha256'] and canonical_hash(Path(task['script']).read_text())==task['script_sha256'],'Registered exact script/job changed')
    response=reader(ident,area/('held-'+ident));response.check_returncode();fields=parse_fields(response.stdout);resources=task['resources']
    require(fields.get('JobId')==ident and fields.get('UserId')=='pwa209(3164768)' and fields.get('JobName')=='hhw-'+name and fields.get('JobState')=='PENDING' and fields.get('Reason')=='JobHeldUser' and fields.get('Account')==config['paths']['gpu_account' if resources['gpu'] else 'cpu_account'] and fields.get('Command')==task['script'],'Exact existing held job differs')
    native_resources(fields,resources,helpers);proof[ident]=fields
   atomic_json(area/'reconciled-held-native-proof.json',{'original_failed_activation':failed,'jobs':proof,'semantic_node_amendment':{'accepted':['1','1-1'],'meaning':'exactly one node; all other requested resources unchanged'}})
   combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],area,'registered-before-release',resource_policy=policy)
   intent={'job_ids':list(proof),'descriptor_sha256':DESCRIPTOR_SHA,'plan_sha256':canonical_hash(plan),'proof_sha256':digest(area/'reconciled-held-native-proof.json'),'automatic_replay_permitted':False,'reconciliation_native_job_id':job}
   atomic_json(CASE/'release-intent.json',intent)
   for ident in proof:
    atomic_json(area/('release-'+ident+'-intent.json'),{'job_id':ident,'automatic_replay_permitted':False})
    result=subprocess.run(['scontrol','release',ident],capture_output=True,text=True,timeout=30)
    atomic_json(area/('release-'+ident+'-result.json'),{'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr});result.check_returncode()
   receipt={'status':'initial_af_canaries_registered_and_released','native_activation_job_id':job,'original_failed_activation_job_id':'6685623','descriptor_path':str(dpath),'descriptor_sha256':DESCRIPTOR_SHA,'worker_sha256':d['worker_sha256'],'resource_policy':policy.receipt(),'qualification_sha256':d['qualification']['sha256'],'plan_path':str(directory/'plan.json'),'ledger_path':str(ledger_path),'tasks':JOBS,'source_sha256':SOURCE,'scientific_gates_changed':False,'reconciliation_proof_sha256':digest(area/'reconciled-held-native-proof.json'),'created_unix':time.time()}
   atomic_json(CASE/'activation-receipt.json',receipt)
 print('AF_RECONCILED '+json.dumps(receipt,sort_keys=True),flush=True)

if __name__=='__main__':main()
