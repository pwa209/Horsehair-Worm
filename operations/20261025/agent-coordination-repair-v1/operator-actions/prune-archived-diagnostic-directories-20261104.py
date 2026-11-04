"""Prune only empty ancestor directories of thirty positively archived reader transactions."""
from pathlib import Path,PurePosixPath
import argparse,errno,hashlib,importlib.util,json,os,re,shlex,stat,subprocess,time

ROOT=Path('/scratch/pwa209/horsehair-worm')
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
HELPER_SHA='e8a599304f4ac2561339114e7965f3cca2f06c87d565822bb32aabbdf322d979'
PRODUCERS={
 '8425411':{'support':'terminal-controller-diagnostic-support-v3-20261103','case':'terminal-controller-diagnostic-compaction-v3-20261103','wrapper':'5019e6f543cd6a93195edf6e8db0319eaa7fc2d1abd8cc48e74db06465ff7550','helper':'compact-terminal-controller-reads-20261103.py','state':'FAILED','exit':'1:0','proof':'progress.json','count':3},
 '8463111':{'support':'terminal-controller-diagnostic-support-v4-20261104','case':'terminal-controller-diagnostic-compaction-v4-20261104','wrapper':HELPER_SHA,'helper':'compact-terminal-controller-reads-20261104.py','state':'COMPLETED','exit':'0:0','proof':'result.json','count':27}}

def require(value,message):
 if not value:raise ValueError(message)

def sha(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as f:
  for block in iter(lambda:f.read(1048576),b''):h.update(block)
 return h.hexdigest()

def load_helper(locator,*,root=ROOT):
 path=Path(locator['path']);require(locator['sha256']==HELPER_SHA and sha(path)==HELPER_SHA,'Frozen archive verifier changed')
 spec=importlib.util.spec_from_file_location('verified_controller_archive',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 module.ordinary(path,root);return module

def pinned(locator,helper,*,root):
 require(isinstance(locator,dict) and set(locator)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',str(locator['sha256'])),'Exact immutable metadata pin required')
 path=helper.ordinary(locator['path'],root);require(sha(path)==locator['sha256'],'Pinned archive/request proof changed: '+str(path));return path

def verify_bindings(request,helper,*,root):
 required={str(root/CAMPAIGN.relative_to(ROOT)/name) for name in ('state.json.gz','campaign-plan.json','state-binding.json')}
 require(required<={x['path'] for x in request['binding_documents']},'Three canonical source pins required')
 for item in request['binding_documents']:pinned(item,helper,root=root)

def producers(request,native,costs,helper,*,root):
 entries=request['producers'];require(len(entries)==2 and {x['job'] for x in entries}==set(PRODUCERS),'Exactly the two original compaction producers required')
 result={}
 for entry in entries:
  job=entry['job'];expected=PRODUCERS[job];base=root/'maintenance'/expected['case'];support=root/'maintenance'/expected['support']
  require(Path(entry['archive_base'])==base,'Producer archive namespace changed')
  paths={k:pinned(entry[k],helper,root=root) for k in ('registration','request','script','proof')}
  require(paths['registration']==support/'registration.json' and paths['request']==support/'request.json' and paths['script']==support/'controller-compact.sbatch' and paths['proof']==base/expected['proof'],'Exact original producer metadata paths required')
  registration=json.loads(paths['registration'].read_bytes());original=json.loads(paths['request'].read_bytes());proof=json.loads(paths['proof'].read_bytes())
  require(registration.get('job_id')==job and registration.get('request_sha256')==entry['request']['sha256'] and original['case']==str(base),'Producer actual registration/request differs')
  script=paths['script'].read_text();require(expected['wrapper'] in script and entry['request']['sha256'] in script and expected['helper'] in script,'Producer original script/source/request binding differs')
  owner=helper.terminal_owner(job,native.get(job,{}),costs[job]);require(owner['state']==expected['state'] and owner['exit_code']==expected['exit'],'Producer actual terminal outcome differs')
  if job=='8463111':
   require(proof.get('status')=='terminal_controller_diagnostic_recovery_incomplete' and proof.get('minimum_met') is False and proof.get('files_removed')==33782 and proof.get('prior_verified_files_removed')==13130 and proof.get('current_pass_files_removed')==20652,'Original v4 incomplete outcome must remain truthful')
   require(len(proof.get('ineligible_sources',[]))==2 and {x['job'] for x in proof['ineligible_sources']}=={'6950429','7028292'} and {x['job'] for x in proof['prior_verified']}=={'7046213','6689295','6701493'},'Two preserved stale sources and exact prior verification required')
  completed=proof['completed'];require(len(completed)==expected['count'] and len({x['job'] for x in completed})==len(completed),'Producer completed archive roster differs')
  result[job]={'request_sha256':entry['request']['sha256'],'wrapper_sha256':expected['wrapper'],'base':base,'completed':{x['job']:x for x in completed},'native':owner,'proof_sha256':entry['proof']['sha256'],'proof_status':proof.get('status'),'canonical_state_sha256':proof.get('canonical_state_sha256'),'ineligible_source_jobs':sorted(x['job'] for x in proof.get('ineligible_sources',[]))}
 return result

def parent_candidates(source,rows,helper,*,root):
 source=helper.ordinary(source,root,directory=True);helper.validate_rows(source,rows,root);candidates={}
 for row in rows:
  parent=PurePosixPath(row['path']).parent
  while str(parent)!='.':
   candidate=source.joinpath(*parent.parts)
   require(candidate!=source and candidate.is_relative_to(source),'Candidate ancestor escaped its exact controller')
   candidates[candidate]=source;parent=parent.parent
 require(len(candidates)<=100000,'Finite derived ancestor roster exceeded')
 return candidates

def verify_transactions(request,producer_map,native,costs,helper,*,root,checkpoint=lambda:None):
 entries=request['transactions'];require(len(entries)==30 and len({x['job'] for x in entries})==30,'Exactly thirty unique completed transactions required')
 expected={(producer,job) for producer,p in producer_map.items() for job in p['completed']}
 require({(x['producer_job'],x['job']) for x in entries}==expected,'Transaction roster differs from original producers')
 candidates={};verified=[]
 for item in entries:
  checkpoint();job=item['job'];producer=producer_map[item['producer_job']];directory=helper.ordinary(item['archive_directory'],root,directory=True)
  require(directory==producer['base']/('controller-'+job),'Transaction archive namespace differs')
  values={}
  for key in ('intent','receipt','removal'):
   path=pinned(item[key],helper,root=root);require(path==directory/(key+'.json'),'Exact archived transaction metadata required');values[key]=json.loads(path.read_bytes())
  intent,receipt,journal=(values[k] for k in ('intent','receipt','removal'));source=helper.controller_identity(intent['source'],job,root=root);rows=intent['members'];derived=parent_candidates(source,rows,helper,root=root)
  owner=helper.terminal_owner(job,native.get(job,{}),costs[job]);authority=intent['authority'];record=authority['native']
  require(intent['kind']=='terminal-controller-raw-reader-evidence' and authority['kind']=='actual_terminal_controller_diagnostics' and record['job_id']==job and all(record[k]==owner[k] for k in ('state','exit_code','cpus','elapsed_seconds','user','account','job_id_raw')),'Original actual terminal controller provenance differs')
  require(authority['wrapper_sha256']==producer['wrapper_sha256'] and authority['request_sha256']==producer['request_sha256'],'Original compaction source/request authority differs')
  helper.registration_binding(authority['original_registration']['registration'],job,root=root)
  require(receipt['status']=='independent_roundtrip_verified_originals_retained' and receipt['intent_sha256']==item['intent']['sha256'] and receipt['files']==len(rows),'Archived complete receipt differs')
  require(rows and journal['status']=='complete_terminal_diagnostic_compaction' and journal['intent_sha256']==receipt['intent_sha256'] and journal['archive_sha256']==receipt['archive_sha256'] and journal['removed_count']==len(rows) and journal['last_removed']==rows[-1]['path'],'Completed archive removal journal differs')
  produced=producer['completed'][job];require(produced['files_removed']==len(rows) and produced['archive']==str(directory) and produced['archive_sha256']==receipt['archive_sha256'],'Original producer completed frontier differs')
  archive=helper.ordinary(directory/'evidence.tar',root);helper.verify_tar(archive,rows,receipt['archive_sha256'],checkpoint=checkpoint)
  for row in rows:
   checkpoint();path=source/row['path'];require(not path.exists() and not path.is_symlink(),'Archived original unexpectedly present: '+str(path))
  candidates.update(derived);verified.append({'job':job,'producer_job':item['producer_job'],'members':len(rows),'archive_sha256':receipt['archive_sha256']})
 return candidates,verified

def remove_empty_directory(path,*,root):
 # Native Linux descends through directory descriptors with O_NOFOLLOW. An
 # ancestor symlink race cannot redirect rmdir outside the admitted root.
 if os.name!='posix':
  os.rmdir(path);return # local Windows fixtures; main requires native Narval
 descriptors=[]
 try:
  parent=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW);descriptors.append(parent)
  parts=Path(path).relative_to(root).parts
  for name in parts[:-1]:
   parent=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent);descriptors.append(parent)
  info=os.stat(parts[-1],dir_fd=parent,follow_symlinks=False)
  require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid(),'Ordinary owned final directory required')
  os.rmdir(parts[-1],dir_fd=parent)
 finally:
  for descriptor in reversed(descriptors):os.close(descriptor)

def prune(candidates,helper,*,root,protected=(),checkpoint=lambda:None,after_remove=lambda path:None):
 require(isinstance(candidates,dict),'Verified candidate-to-source mapping required')
 protected={Path(x) for x in protected};counts={'considered':0,'removed':0,'not_empty':0,'already_absent':0,'protected':0}
 for path in sorted(candidates,key=lambda p:(-len(p.parts),str(p))):
  checkpoint();counts['considered']+=1;source=Path(candidates[path]);path=Path(path)
  match=re.fullmatch(r'(?:controller-(?:[0-3]-)?|native-)([1-9][0-9]*)',source.name)
  require(match is not None,'Exact source controller required');helper.controller_identity(source,match[1],root=root)
  require(path!=source and path.is_relative_to(source) and path!=root and path.is_relative_to(root),'Confined member ancestor required')
  if any(path==p or p.is_relative_to(path) for p in protected):counts['protected']+=1;continue
  try:helper.ordinary(path,root,directory=True)
  except FileNotFoundError:counts['already_absent']+=1;continue
  try:remove_empty_directory(path,root=root)
  except OSError as error:
   if error.errno in {errno.ENOTEMPTY,errno.EEXIST}:counts['not_empty']+=1;continue
   raise
  counts['removed']+=1;after_remove(path)
 return counts

def maintenance_allocation(fields,job,script,environment_end,*,wall=time.time):
 from datetime import datetime,timezone
 require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('JobState')=='RUNNING' and fields.get('Account')=='def-ptewarie_cpu' and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'} and fields.get('MinMemoryNode') in {'8G','8192M'} and fields.get('TimeLimit') in {'02:00:00','0-02:00:00'} and fields.get('Command')==str(script) and fields.get('JobName')=='hhw-empty-diagnostic-directory-prune-20261104' and 'ArrayJobId' not in fields and all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask')),'Exact owned scalar1CPU8G2h diagnostic directory maintenance required')
 end=datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp();require(abs(end-environment_end)<=2 and wall()+600<end,'Actual finite maintenance end required');return end

def main(argv=None):
 import horsehair
 from horsehair import budget,core,repair
 from horsehair.execution_resources import load_resource_policy
 parser=argparse.ArgumentParser();parser.add_argument('--request',type=Path,required=True);parser.add_argument('--request-sha256',required=True);parser.add_argument('--wrapper-sha256',required=True);args=parser.parse_args(argv)
 require(sha(__file__)==args.wrapper_sha256 and sha(args.request)==args.request_sha256,'Frozen maintenance/request changed')
 require(os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',os.environ.get('SLURM_JOB_ID','')),'Owned native allocation required')
 require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
 os.environ['TZ']='UTC'
 if hasattr(time,'tzset'):time.tzset()
 def run(cmd,**kw):return subprocess.run(cmd,**{**kw,'timeout':90})
 result=run(['scontrol','show','job',os.environ['SLURM_JOB_ID'],'-o'],capture_output=True,text=True);result.check_returncode();require(not result.stderr,'Actual own allocation required')
 fields=dict(x.split('=',1) for x in shlex.split(result.stdout) if '=' in x);end=maintenance_allocation(fields,os.environ['SLURM_JOB_ID'],Path(__file__).with_name('prune.sbatch'),float(os.environ['SLURM_JOB_END_TIME']))
 def deadline():require(time.time()+300<end,'Finite maintenance finalization margin exhausted')
 framework=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e';require(Path(horsehair.__file__).resolve()==framework/'src/horsehair/__init__.py' and core.source_identity(framework)['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1','Original complete qualified execution framework required')
 request=json.loads(args.request.read_bytes());helper=load_helper(request['helper']);verify_bindings(request,helper,root=ROOT)
 case=Path(request['case']);require(case.is_relative_to(ROOT/'maintenance') and case.resolve()==case and not case.exists(),'Fresh private maintenance case required');case.mkdir();helper.sync_directory(case.parent)
 policy=load_resource_policy(ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5',root=ROOT,config=core.read_json(framework/'configs/study.json'))
 ids=list(PRODUCERS)+[x['job'] for x in request['transactions']];require(len(set(ids))==len(ids),'Duplicate native allocation refused')
 budget.native_accounting(ROOT,ids,runner=run);native=repair._repair_native_states(ids,{},owner='pwa209',account='def-ptewarie_cpu',runner=run);costs=core.read_json(ROOT/'state/resource-budget.json')['native_jobs']
 producer_map=producers(request,native,costs,helper,root=ROOT)
 with budget.resource_lease(ROOT,core.read_json(framework/'configs/study.json'),cpus=1,gpus=0,max_hours=2,job_id=os.environ['SLURM_JOB_ID'],resource_policy=policy),core.exclusive_file_lock(CAMPAIGN/'coordination-controller.mutex',timeout=60):
  verify_bindings(request,helper,root=ROOT);pins={item['path']:item['sha256'] for item in request['binding_documents']};guard=helper.binding_guard(pins,root=ROOT)
  def checkpoint():deadline();guard()
  require(producer_map['8463111']['canonical_state_sha256']==pins[str(CAMPAIGN/'state.json.gz')],'Original canonical campaign bytes changed since completed compaction')
  candidates,verified=verify_transactions(request,producer_map,native,costs,helper,root=ROOT,checkpoint=checkpoint)
  archived_member_files=sum(x['members'] for x in verified);require(archived_member_files==33782,'Exact positively completed archive member count differs')
  helper.atomic(case/'verified-transactions.json',{'verified':verified,'candidate_directories':len(candidates),'archived_member_files':archived_member_files,'scientific_result':False,'archive_replay':False})
  counts=prune(candidates,helper,root=ROOT,protected=request.get('protected_paths',[]),checkpoint=checkpoint)
  checkpoint();fresh=budget.require_storage_headroom(ROOT,required_files=0,runner=run)
  report={'status':'archived_diagnostic_empty_directory_prune_complete',**counts,'verified_transactions':len(verified),'archived_member_files':archived_member_files,'canonical_state_sha256':pins[str(CAMPAIGN/'state.json.gz')],'wrapper_sha256':args.wrapper_sha256,'request_sha256':args.request_sha256,'helper_sha256':HELPER_SHA,'producer_job_ids':sorted(producer_map),'producer_proofs':{job:{k:value[k] for k in ('proof_sha256','proof_status','canonical_state_sha256','ineligible_source_jobs')} for job,value in producer_map.items()},'personal_available_files':fresh['personal_available_files'],'scientific_result':False,'admission_authority':False,'archive_replay':False,'removed_files':0}
  helper.atomic(case/'result.json',report);print(json.dumps(report),flush=True)

if __name__=='__main__':main()
