"""Source-pinned native dispatch; private wrapper, no implicit science changes."""
from pathlib import Path
from datetime import datetime, timezone
import argparse, json, math, os, re, runpy, subprocess, time
from horsehair.core import read_json, digest, atomic_json, canonical_hash, source_identity, native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy, BASE_CONFIG_SHA256, PRIMARY_SCIENTIFIC_SOURCE_SHA256
from horsehair.slurm import completed_native_dependency

ROOT=Path('/scratch/pwa209/horsehair-worm')
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
AGENTS_SHA='fbc98bc5765b2d1b2fc9f46b48b104da2cfba7a211e696f7b23e23745e67ac14'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'

def require(value,message):
 if not value:raise ValueError(message)

def combined_admission(*args,**kwargs):
 # The pinned read guard must be installed before campaign binds repair readers.
 from horsehair.campaign import combined_admission as admitted
 from horsehair import campaign,repair
 require(campaign._repair_native_states is repair._repair_native_states,'Campaign native reader was imported before the scoped guard')
 return admitted(*args,**kwargs)

def main():
 p=argparse.ArgumentParser();p.add_argument('--descriptor',type=Path,required=True);p.add_argument('--descriptor-sha256',required=True)
 p.add_argument('--mode',choices=['gpu-qualify','agent-train','agent-evaluation','agent-assembly','cohort-profile','precision-plan','precision-diagnostics','precision-chunk','precision-assemble'],required=True)
 p.add_argument('--cpus',required=True,type=int);p.add_argument('--gpus',required=True,type=int);p.add_argument('--hours',required=True,type=int);p.add_argument('--index',type=int)
 p.add_argument('--evaluation-case',type=Path);p.add_argument('--evaluation-plan-sha256');p.add_argument('--evaluation-binding-sha256');a=p.parse_args()
 require(a.descriptor.resolve().is_relative_to(ROOT/'maintenance') and not a.descriptor.is_symlink() and digest(a.descriptor)==a.descriptor_sha256,'Explicit unchanged operational descriptor required')
 d=read_json(a.descriptor);framework=Path(d['framework_release']).resolve();config=read_json(PRIMARY/'configs/study.json');job=str(native_job_id());area=Path(d['case'])/('native-'+job)
 require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209' and job.isdigit(),'Owned single native allocation required')
 require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval context required')
 require(source_identity(PRIMARY)['sha256']==PRIMARY_SCIENTIFIC_SOURCE_SHA256 and canonical_hash(config)==BASE_CONFIG_SHA256,'Frozen primary source/configuration changed')
 import horsehair
 require(Path(horsehair.__file__).resolve()==framework/'src/horsehair/__init__.py' and source_identity(framework)['sha256']==d['execution_source_sha256'],'Exact qualified operational source required')
 require(read_json(framework/'configs/study.json')==config and digest(Path(__file__))==d['worker_sha256'],'Scientific configuration or private wrapper changed')
 require(str(a.cpus)==os.environ.get('SLURM_CPUS_PER_TASK') and str(a.gpus)==os.environ.get('HHW_GPU_COUNT') and str(a.hours)==os.environ.get('HHW_MAX_HOURS'),'Native environment/resource contract differs')
 require(0<a.cpus<=256 and a.gpus in (0,1) and 0<a.hours<=168 and not area.exists() and area.resolve().is_relative_to(ROOT/'maintenance'),'Finite fresh native case required')
 area.mkdir(parents=True,mode=0o700)
 for path,expected in [(GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA)]:require(digest(path)==expected,'Pinned native operational reader changed')
 runpy.run_path(str(GUARD))['install'](area/'accounting-reads')
 response=runpy.run_path(str(SCHEDULER))['run_scontrol_job'](job,area/'scheduler-reads');response.check_returncode();pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',response.stdout);fields=dict(pairs);require(len(fields)==len(pairs),'Duplicate native scheduler fields refused')
 require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('JobState')=='RUNNING' and fields.get('NumCPUs')==str(a.cpus) and fields.get('Account')==config['paths']['gpu_account' if a.gpus else 'cpu_account'],'Native owner/allocation proof differs')
 match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',fields.get('TimeLimit',''));require(match is not None,'Finite native time limit missing');day,hour,minute,second=match.groups();limit=int(day or 0)*86400+int(hour)*3600+int(minute)*60+int(second)
 require(limit==a.hours*3600 and math.isfinite(float(os.environ.get('SLURM_JOB_END_TIME','nan'))) and float(os.environ['SLURM_JOB_END_TIME'])>time.time()+180,'Native bound/finalization margin required')
 gpu_fields=' '.join(fields.get(k,'') for k in ['TresPerNode','TresPerJob','TresPerTask','AllocTRES','ReqTRES','Gres'])
 require(('gpu' in gpu_fields)==bool(a.gpus),'Actual native GPU allocation differs')
 tres_pairs=[item.split('=',1) for item in fields.get('AllocTRES','').split(',') if '=' in item];tres=dict(tres_pairs);require(len(tres)==len(tres_pairs),'Duplicate allocated native TRES keys refused')
 require(tres.get('gres/gpu','0').isdigit() and int(tres.get('gres/gpu','0'))==a.gpus,'Actual native total GPU count differs')
 require(all(v.isdigit() for k,v in tres.items() if k.startswith('gres/gpu:')),'Typed native GPU counts must be nonnegative integers')
 typed=[int(v) for k,v in tres.items() if k.startswith('gres/gpu:')]
 require(not typed or sum(typed)==a.gpus,'Actual typed native GPU count differs')
 policy=load_resource_policy(Path(d['resource_policy']['path']),d['resource_policy']['sha256'],root=ROOT,config=config)
 admission=combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],area,'native-start-admission',resource_policy=policy)
 qpath=Path(d['qualification']['path']);require(digest(qpath)==d['qualification']['sha256'],'Full qualification receipt changed');q=read_json(qpath)
 require(q['status']=='complete' and q.get('returncode')==0 and q.get('config_sha256')==BASE_CONFIG_SHA256 and q['source_sha256']==d['execution_source_sha256'] and q['source_verified_unchanged'] is True and q['test_counts']==d['qualification']['test_counts'] and q['test_counts']['failures']==q['test_counts']['errors']==q['test_counts']['skipped']==0 and completed_native_dependency(ROOT,str(q['slurm_job_id'])) is not None,'Positive complete qualified-source native proof required')
 receipt={'status':'running','mode':a.mode,'index':a.index,'native_job_id':job,'descriptor_sha256':a.descriptor_sha256,'source_sha256':d['execution_source_sha256'],'full_config_sha256':BASE_CONFIG_SHA256,'resource_policy':policy.receipt(),'allocation':fields,'started_unix':time.time(),'scientific_result':False,'admission_authority':False}
 atomic_json(area/'receipt.json',receipt)
 with resource_lease(ROOT,config,cpus=a.cpus,gpus=a.gpus,max_hours=a.hours,job_id=job,resource_policy=policy):
  quota=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,required_bytes=10_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000,evidence_dir=area/'quota-reads',allocation_deadline_unix=float(os.environ['SLURM_JOB_END_TIME']))
  import torch
  torch.set_num_threads(a.cpus)
  if a.gpus:require(torch.cuda.is_available() and torch.cuda.device_count()==1,'Exactly one CUDA-visible allocated GPU required')
  from horsehair import agents,agent_execution_v2 as evaluation
  require(digest(Path(agents.__file__))==AGENTS_SHA,'Original agent implementation changed')
  protocol_path=ROOT/'results/repair-v7-2026-10-13/agents/frozen_protocol.json';require(digest(protocol_path)==d['frozen_protocol_sha256'],'Development-frozen protocol changed');protocol=read_json(protocol_path)
  result=None
  if a.mode=='gpu-qualify':
   require(a.gpus==1,'GPU qualification requires one GPU')
   from horsehair.cli import qualification
   result=qualification(config,ROOT,gpu=True)
  elif a.mode=='agent-train':
   require(a.gpus==1 and torch.cuda.is_available() and type(a.index) is int and 0<=a.index<240,'Original final training requires CUDA and exact final index')
   gpu_binding=d['gpu_qualification'];gpu_proof=Path(gpu_binding['path']);require(gpu_proof.resolve().is_relative_to(ROOT/'maintenance') and not gpu_proof.is_symlink() and digest(gpu_proof)==gpu_binding['sha256'],'Explicit current GPU qualification bytes required');gpu=read_json(gpu_proof)
   require(gpu.get('status')=='complete_native_work' and gpu.get('mode')=='gpu-qualify' and gpu.get('native_job_id')==gpu_binding['job_id'] and gpu.get('descriptor_sha256')==gpu_binding['descriptor_sha256'] and gpu.get('source_sha256')==d['execution_source_sha256'] and gpu.get('full_config_sha256')==BASE_CONFIG_SHA256 and gpu.get('resource_policy',{}).get('sha256')==policy.sha256 and completed_native_dependency(ROOT,gpu_binding['job_id']) is not None,'Positive source-bound native GPU qualification required')
   regression=gpu['result'];comparisons=regression.get('cpu_cuda_regression',{})
   require(regression.get('status')=='complete' and regression.get('iterations')==20 and isinstance(regression.get('device'),str) and regression['device'] and regression.get('cuda') and regression.get('torch') and math.isfinite(regression.get('elapsed_seconds',float('nan'))) and regression['elapsed_seconds']>0 and math.isfinite(regression.get('loss',float('nan'))) and set(comparisons)=={'rnn','gru','modular','feedforward'} and all(math.isfinite(v['max_action_probability_difference']) and 0<=v['max_action_probability_difference']<=3e-5 and v.get('tolerance')==3e-5 for v in comparisons.values()),'Complete actual CUDA regression evidence required')
   result=agents.run(config,ROOT,mode='final',task_index=a.index)
   cell=evaluation.final_cells(config)[a.index];summary_path=agents._directory(ROOT,config['agents'])/'final'/agents._cell_name(cell['task'],cell['architecture'],cell['seed'])/'summary.json'
   receipt.update(training_summary_path=str(summary_path),training_summary_sha256=digest(summary_path),training_summary=read_json(summary_path),cell=cell)
  elif a.mode in {'agent-evaluation','agent-assembly'}:
   require(a.gpus==0 and a.cpus==(4 if a.mode=='agent-evaluation' else 1) and type(a.index) is int and 0<=a.index<240,'Finite final evaluation/assembly allocation required')
   case=Path(d['agent_campaign_case'])/('cell-'+str(a.index).zfill(4));require(a.evaluation_case is not None and a.evaluation_case.resolve()==case.resolve() and case.resolve().is_relative_to(ROOT/'maintenance') and not case.is_symlink(),'Exact declared final policy case required')
   require(digest(case/'plan.json')==a.evaluation_plan_sha256 and digest(case/'plan-binding.json')==a.evaluation_binding_sha256,'Immutable final evaluation plan/binding changed')
   plan=read_json(case/'plan.json');cell=evaluation.final_cells(config)[a.index]
   expected_checkpoint=agents._directory(ROOT,config['agents'])/'final'/agents._cell_name(cell['task'],cell['architecture'],cell['seed'])/'checkpoint.pt'
   require(plan['canary'] is False and plan['benchmark_directions'] is False and all(plan[k]==cell[k] for k in ['task','architecture','seed']) and plan['config']==config and plan['full_config_sha256']==BASE_CONFIG_SHA256 and plan['original_protocol']==protocol and plan['original_protocol_sha256']==agents._digest(protocol) and plan['evaluation_settings']==evaluation._settings(config) and plan['conditions']==evaluation._conditions(evaluation._settings(config),protocol) and len(plan['conditions'])==300 and plan['original_training_steps']==config['agents']['total_steps'] and Path(plan['checkpoint']).resolve()==expected_checkpoint.resolve(),'Final evaluation must bind the unchanged full scientific configuration, protocol, complete checkpoint and entire grid')
   if a.mode=='agent-evaluation':result=evaluation.run_chunk(case,max_conditions=300,max_wall_seconds=max(1.,float(os.environ['SLURM_JOB_END_TIME'])-time.time()-300),expected_plan_sha256=a.evaluation_plan_sha256)
   else:result=evaluation.assemble(case,expected_plan_sha256=a.evaluation_plan_sha256)
   receipt.update(evaluation_case=str(case),evaluation_plan_sha256=a.evaluation_plan_sha256,evaluation_binding_sha256=a.evaluation_binding_sha256)
  elif a.mode=='cohort-profile':
   require(a.gpus==0 and a.cpus==1,'Original CPU cohort profiler requires one CPU')
   from horsehair.cohort_profile import run_profile
   result=run_profile(PRIMARY/'configs/study.json',ROOT,PRIMARY,Path(d['cohort_case']),kind='MX',rate=.001,device='cpu',max_seconds=min(6300.,float(os.environ['SLURM_JOB_END_TIME'])-time.time()-180))
   require(result['status'] in {'declared_fold_profile_complete','bounded_deadline_reached'},'Whole-cohort technical worker failed')
  else:
   require(a.gpus==0 and a.cpus==4,'Paired precision canary retains original four CPU threads')
   request_path=Path(d['canary_request']['path']);require(request_path.resolve().is_relative_to(Path(d['case']).resolve()) and not request_path.is_symlink() and digest(request_path)==d['canary_request']['sha256'],'Native precision request changed');request=read_json(request_path);case=Path(request['case'])
   kw=request['prepare_plan_kwargs'];expected_checkpoint=agents._directory(ROOT,config['agents'])/'benchmark-training'/agents._cell_name(kw['task'],kw['architecture'],kw['seed'])/'checkpoint.pt'
   require(case==ROOT/'maintenance/agent-precision-canary-20261021' and case.resolve()==case and not case.is_symlink() and Path(request['checkpoint_binding']['path']).resolve()==expected_checkpoint.resolve(),'Exact personal canary destination and original pilot checkpoint required')
   require(request['execution_module_sha256']==digest(Path(evaluation.__file__)) and request['frozen_protocol_binding']['sha256']==digest(protocol_path),'Precision source/protocol changed')
   if a.mode=='precision-plan':
    checkpoint=request['checkpoint_binding'];require(digest(Path(checkpoint['path']))==checkpoint['sha256'],'Original failed checkpoint changed')
    result=evaluation.prepare_plan(config,protocol,Path(checkpoint['path']),case,checkpoint_sha256=checkpoint['sha256'],primary_agents_sha256=AGENTS_SHA,**request['prepare_plan_kwargs'])
    require(len(result['conditions'])==request['required_conditions'],'Canary grid differs')
    receipt.update(plan_sha256=digest(case/'plan.json'),plan_binding_sha256=digest(case/'plan-binding.json'))
   else:
    binding=d['canary_plan'];require(digest(case/'plan.json')==binding['plan_sha256'] and digest(case/'plan-binding.json')==binding['plan_binding_sha256'],'Pinned canary plan changed')
    plan=read_json(case/'plan.json')
    require(plan['config']==config and plan['full_config_sha256']==BASE_CONFIG_SHA256 and plan['original_protocol']==protocol and plan['original_protocol_sha256']==agents._digest(protocol) and plan['checkpoint']==str(expected_checkpoint.resolve()) and plan['checkpoint_sha256']==request['checkpoint_binding']['sha256'] and all(plan[k]==kw[k] for k in ['task','architecture','seed']) and plan['canary'] is True and plan['canary_overrides']==kw['canary'] and plan['benchmark_directions'] is kw['benchmark_directions'] and plan['evaluation_settings']==dict(evaluation._settings(config),**kw['canary']),'Canary plan differs from its independent source-bound request')
    if a.mode=='precision-diagnostics':
     result=evaluation.diagnostics(case,expected_plan_sha256=binding['plan_sha256'])
     atomic_json(area/'paired-precision-diagnostics.json',result)
     f32=[r for r in result['reports'] if r['dtype']=='torch.float32'];f64=[r for r in result['reports'] if r['dtype']=='torch.float64']
     require(len(f32)==len(f64)==request['required_conditions'] and any(r['status']=='unavailable' for r in f32),'Original native float32 failure was not reproduced; review before expanding evaluation')
     require(all(r['status']=='available' and r.get('unavailable_rows')==0 for r in f64),'Float64 has not resolved every planned canary condition')
     receipt.update(float32_failure_reproduced=True,float64_all_canary_conditions_available=True,diagnostics_sha256=digest(area/'paired-precision-diagnostics.json'))
    elif a.mode=='precision-chunk':result=evaluation.run_chunk(case,max_conditions=request['required_conditions'],max_wall_seconds=max(1.,float(os.environ['SLURM_JOB_END_TIME'])-time.time()-300),expected_plan_sha256=binding['plan_sha256'])
    else:result=evaluation.assemble(case,expected_plan_sha256=binding['plan_sha256'])
  receipt.update(status='complete_native_work',result=result,finished_unix=time.time(),storage_headroom=quota)
  atomic_json(area/'receipt.json',receipt)
 print('AF_NATIVE_WORK '+json.dumps({'mode':a.mode,'native_job_id':job,'status':receipt['status'],'receipt_path':str(area/'receipt.json'),'receipt_sha256':digest(area/'receipt.json')},sort_keys=True),flush=True)

if __name__=='__main__':main()
