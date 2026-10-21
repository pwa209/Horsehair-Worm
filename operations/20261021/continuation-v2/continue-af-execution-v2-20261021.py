"""One-time native precision continuation and finite four-segment F2 launch.

This management program never retries scheduler mutations. Its immutable case,
submission journals and ordinary qualified Slurm ledgers preserve ambiguous work.
"""
from pathlib import Path
from datetime import datetime, timezone
import argparse
import importlib.util
import json
import math
import os
import re
import runpy
import shlex
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

from horsehair.core import atomic_json, canonical_hash, digest, exclusive_file_lock, native_job_id, read_json, source_identity
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
from horsehair.slurm import _script, submit, completed_native_dependency
from horsehair.admission import _storage
from horsehair.cli import _qualification_junit

ROOT=Path('/scratch/pwa209/horsehair-worm')
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
WORKER_SHA='f00d4265a68a3ace5faa47da4fe2701fff3fe86d9132b09ea748e9f12471e4fd'
CONTROLLER_SHA='3e30441d026706d1a10749ae6f4d892821defb9f0bf99070aec6a5a487710577'
CASE=ROOT/'maintenance/study-af-campaign-v1-20261021'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'
PREVIOUS_JOB='6687007'
PREVIOUS_JUNIT_SHA='1d80f96037439ef9efe7fe95a952aa7bab788314695b7b146801eb1f7cbdedca'


def require(value,message):
    if not value:raise ValueError(message)


def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def frozen(path,sha):
    path=Path(path)
    require(path.resolve().is_relative_to(ROOT/'maintenance') and not path.is_symlink() and digest(path)==sha,'Immutable personal maintenance input differs')
    return read_json(path)


def memory(value):
    match=re.fullmatch(r'([1-9][0-9]*)([MG])',value)
    require(match is not None,'Exact finite native memory units required')
    return int(match[1])*1024**(2 if match[2]=='M' else 3)


def held_proof(c,task,job,reader):
    response=reader(job,c['area']/('held-'+job));fields=c['controller'].fields(response);r=task['resources']
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
            and fields.get('JobName')=='hhw-'+task['name'] and fields.get('JobState')=='PENDING'
            and fields.get('Reason')=='JobHeldUser' and fields.get('Command')==task['script']
            and fields.get('Account')==c['config']['paths']['cpu_account'] and fields.get('NumCPUs')==str(r['cpus'])
            and fields.get('NumNodes') in {'1','1-1'} and memory(fields.get('MinMemoryNode',''))==memory(r['memory'])
            and c['controller'].seconds(fields.get('TimeLimit',''))==r['hours']*3600,
            'Exact registered held owner/script/resources differ')
    require(c['controller'].gpu_count(fields.get('ReqTRES',''))==0,'Unexpected held GPU request')
    alloc=fields.get('AllocTRES','')
    if alloc and alloc!='(null)':require(c['controller'].gpu_count(alloc)==0,'Unexpected held GPU allocation')
    return fields


def qualify_controller(c,*,runner=subprocess.run):
    require(time.time()+180<c['end'],'Finite management margin exhausted before private qualification')
    output=c['area']/'controller-pytest';output.mkdir()
    scratch=Path(os.environ.get('SLURM_TMPDIR',''))
    require(scratch.is_absolute() and scratch.is_dir() and not scratch.is_symlink(),'Existing allocated node-local SLURM_TMPDIR required for synthetic qualification fixtures')
    workspace=scratch/('af-controller-private-'+c['job']+'-v2');workspace.mkdir(mode=0o700)
    command=[sys.executable,'-s','-m','pytest',str(c['tests_path']),'-q','--basetemp',str(workspace/'tmp'),
             '--junitxml',str(output/'junit.xml'),'-o','cache_dir='+str(workspace/'cache')]
    env=os.environ.copy()
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
    env.update(PYTHONDONTWRITEBYTECODE='1',PYTEST_DISABLE_PLUGIN_AUTOLOAD='1')
    started=time.time()
    result=runner(command,capture_output=True,text=True,env=env,timeout=min(1200,c['end']-time.time()-180))
    (output/'stdout.txt').write_text(result.stdout,encoding='utf8');(output/'stderr.txt').write_text(result.stderr,encoding='utf8')
    require(result.returncode==0 and (output/'junit.xml').is_file(),'Native private controller qualification failed')
    counts=_qualification_junit(output/'junit.xml')
    require(counts=={'tests':49,'failures':0,'errors':0,'skipped':0},'Exact complete 49-test private controller qualification required')
    require(digest(c['controller_path'])==CONTROLLER_SHA and digest(c['tests_path'])==c['tests_sha256'],'Private qualification inputs changed')
    receipt={'status':'complete','returncode':0,'test_counts':counts,'controller_sha256':CONTROLLER_SHA,
        'tests_sha256':c['tests_sha256'],'source_sha256':SOURCE,'config_sha256':CONFIG,
        'native_management_job_id':c['job'],'elapsed_seconds':time.time()-started,
        'fixture_workspace':str(workspace),'fixture_workspace_kind':'allocated SLURM_TMPDIR synthetic fixtures only',
        'junit_sha256':digest(output/'junit.xml'),'scientific_result':False,'admission_authority':False}
    atomic_json(output/'receipt.json',receipt);return receipt


def previous_failure(c,native):
    """Reconcile exact failed v1 qualification without reusing its namespaces."""
    record=native.get(PREVIOUS_JOB,{})
    require(record.get('state')=='FAILED' and record.get('exit_code')=='1:0' and record.get('cpus')==1
        and record.get('user')=='pwa209' and record.get('account')==c['config']['paths']['cpu_account']
        and record.get('elapsed_seconds',0)>0 and str(record.get('job_id_raw'))==PREVIOUS_JOB
        and record.get('unallocated_metadata') is False and c['controller'].gpu_count(record.get('alloc_tres',''))==0,
        'Exact failed previous one-CPU native management allocation required')
    directory=ROOT/'plans/recovery-support-20261021-218';plan_path=directory/'plan.json'
    ledger_path=ROOT/'state/recovery-support-20261021-218-submission.json'
    plan,ledger=read_json(plan_path),read_json(ledger_path)
    require(canonical_hash(plan)==ledger['plan_sha256'] and ledger['source_sha256']==SOURCE
        and plan['release_identity']['sha256']==SOURCE
        and len(plan['tasks'])==1 and set(ledger['tasks'])=={'recovery-218'}
        and ledger['tasks']['recovery-218']['job_id']==PREVIOUS_JOB
        and ledger['tasks']['recovery-218']['state']=='submitted'
        and ledger['tasks']['recovery-218']['script_sha256']==plan['tasks'][0]['script_sha256']
        and plan['tasks'][0]['name']=='recovery-218' and plan['tasks'][0]['script']==str(directory/'recovery-218.sbatch')
        and canonical_hash((directory/'recovery-218.sbatch').read_text())==plan['tasks'][0]['script_sha256'],
        'Previous failed native job lost its original immutable plan/ledger/script join')
    old=CASE/'precision-and-f2-continuation-v1';junit=old/'controller-pytest/junit.xml'
    require(digest(junit)==PREVIOUS_JUNIT_SHA and _qualification_junit(junit)=={'tests':48,'failures':2,'errors':0,'skipped':0},
        'Exact original 48-test native qualification failure evidence required')
    failed={case.get('name') for case in ET.parse(junit).findall('.//testcase') if case.find('failure') is not None}
    require(failed=={'test_initial_four_gpu_jobs_are_registered_held_verified_then_released_once',
        'test_semantically_exact_single_node_range_is_verified_and_released'},'Original two positive mock failures changed')
    absent=[old/'descriptor-v2-worker.json',old/'descriptor-v3-controller.json',old/'release-intent.json',old/'receipt.json']
    for name in ('study-af-precision-v1-20261021','study-af-f2-controllers-v1-20261021'):
        absent += [ROOT/'plans'/name,ROOT/'state'/(name+'-submission.json')]
    absent += [CASE/'agent-final-f64-v1/campaign-plan.json',CASE/'agent-final-f64-v1/state-binding.json']
    require(not any(path.exists() for path in absent) and not list(old.glob('*-intent.json')),
        'Prior downstream mutation/ambiguity exists; this continuation cannot replay it')
    result={'status':'verified_failed_qualification_before_any_downstream_submission','native_job_id':PREVIOUS_JOB,
        'native_proof':record,'old_case':str(old),'junit_sha256':PREVIOUS_JUNIT_SHA,'test_counts':_qualification_junit(junit),
        'failed_tests':sorted(failed),'previous_plan_path':str(plan_path),'previous_plan_sha256':canonical_hash(plan),
        'previous_ledger_path':str(ledger_path),'previous_ledger_sha256':digest(ledger_path),
        'verified_absent_paths':list(map(str,absent)),'production_controller_changed':False,
        'scientific_result':False,'admission_authority':False}
    path=c['area']/'previous-failed-no-submission-proof.json';exclusive(path,result)
    return {'path':str(path),'sha256':digest(path)}


def prior_proof(c,job,mode,native):
    require(c['controller'].positive_native(native,job) and completed_native_dependency(ROOT,job) is not None,
            'Positive original GPU/precision-plan native completion required')
    path=CASE/('native-'+job)/'receipt.json';value=read_json(path)
    require(value.get('status')=='complete_native_work' and value.get('mode')==mode and value.get('index') is None
            and value.get('native_job_id')==job and value.get('descriptor_sha256')==c['descriptor_sha256']
            and value.get('source_sha256')==SOURCE and value.get('full_config_sha256')==CONFIG
            and value.get('resource_policy')==c['policy'].receipt() and value.get('scientific_result') is False
            and value.get('admission_authority') is False,'Original qualified worker receipt differs')
    proof=native[job];allocation=value['allocation'];gpus=int(mode=='gpu-qualify')
    require(proof['cpus']==4 and proof['gpus']==gpus and proof['account']==c['config']['paths']['gpu_account' if gpus else 'cpu_account'],
            'Original prerequisite native resource contract differs')
    require(allocation.get('JobId')==job and allocation.get('UserId')=='pwa209(3164768)'
            and allocation.get('NumCPUs')==str(proof['cpus']) and allocation.get('Account')==proof['account'],
            'Original worker allocation differs from terminal native proof')
    return {'path':str(path),'sha256':digest(path),'job_id':job,'descriptor_sha256':c['descriptor_sha256']},value


def stable_descriptor(c,native):
    gpu_binding,gpu=prior_proof(c,c['gpu_job'],'gpu-qualify',native)
    _,prepared=prior_proof(c,c['plan_job'],'precision-plan',native)
    result=gpu['result'];comparisons=result.get('cpu_cuda_regression',{})
    require(result.get('status')=='complete' and result.get('iterations')==20 and result.get('device') and result.get('cuda')
            and result.get('torch') and math.isfinite(result.get('elapsed_seconds',float('nan')))
            and result['elapsed_seconds']>0 and math.isfinite(result.get('loss',float('nan')))
            and set(comparisons)=={'rnn','gru','modular','feedforward'}
            and all(math.isfinite(v['max_action_probability_difference']) and 0<=v['max_action_probability_difference']<=3e-5
                    and v.get('tolerance')==3e-5 for v in comparisons.values()),'Complete original CUDA equivalence regression required')
    request=frozen(c['descriptor']['canary_request']['path'],c['descriptor']['canary_request']['sha256'])
    case=Path(request['case']);binding={'plan_sha256':prepared['plan_sha256'],'plan_binding_sha256':prepared['plan_binding_sha256']}
    require(digest(case/'plan.json')==binding['plan_sha256'] and digest(case/'plan-binding.json')==binding['plan_binding_sha256'],
            'Actual source-bound precision plan/binding differs')
    plan=read_json(case/'plan.json')
    require(plan==prepared['result'] and plan['canary'] is True and len(plan['conditions'])==request['required_conditions']==52,
            'Actual complete original 52-condition precision plan required')
    value=dict(c['descriptor'],gpu_qualification=gpu_binding,canary_plan=binding)
    path=c['area']/'descriptor-v2-worker.json';exclusive(path,value)
    return path,digest(path),value


def freeze_plan(c,phase,definitions,descriptor,sha,*,controller=False):
    directory=ROOT/'plans'/phase;ledger=ROOT/'state'/(phase+'-submission.json')
    require(not directory.exists() and not ledger.exists(),'One-time registered continuation plan already exists')
    directory.mkdir(mode=0o700);tasks=[]
    for name,mode,cpus,hours,previous,kind in definitions:
        if controller:
            command=shlex.join([sys.executable,'-s','-u',str(c['controller_path']),'--descriptor',str(descriptor),
                '--descriptor-sha256',sha,'--segment',str(mode)])
        else:
            command=shlex.join(['python','-u',str(c['descriptor']['worker_path']),'--descriptor',str(descriptor),
                '--descriptor-sha256',sha,'--mode',mode,'--cpus',str(cpus),'--gpus','0','--hours',str(hours)])
        resources={'cpus':cpus,'gpu':False,'hours':hours,'memory':'32G' if not controller else '16G','array':None}
        text=_script(c['config'],FRAMEWORK,name,command,cpus=cpus,memory=resources['memory'],hours=hours,gpu=False)
        text=text.replace('set -euo pipefail','set -euo pipefail\nexport PYTHONDONTWRITEBYTECODE=1',1)
        script=directory/(name+'.sbatch')
        with script.open('x',encoding='utf8',newline='\n') as stream:stream.write(text)
        tasks.append({'name':name,'command':command,'script':str(script),'script_sha256':canonical_hash(text),
            'depends':[previous] if previous else [],'dependency_kind':kind,'resources':resources})
    plan={'schema_version':2,'phase':phase,'root':str(ROOT),'config_sha256':CONFIG,'release_identity':source_identity(FRAMEWORK),
        'tasks':tasks,'maximum_simultaneous_resources':{'cpus':1 if controller else 4,'gpus':0},
        'cpu_ceiling':256,'gpu_concurrency':4,'resource_policy':c['policy'].receipt(),
        'descriptor_path':str(descriptor),'descriptor_sha256':sha,'scientific_gates_changed':False}
    exclusive(directory/'plan.json',plan)
    return {'path':directory/'plan.json','ledger':ledger,'value':plan}


def submit_held(c,pack,*,runner=subprocess.run):
    journal=c['area']/(pack['value']['phase']+'-intent.json')
    exclusive(journal,{'status':'before_sbatch','plan_path':str(pack['path']),'plan_sha256':canonical_hash(pack['value']),
        'ledger_path':str(pack['ledger']),'automatic_replay_permitted':False})
    def held(command,**kwargs):
        require(command[0]=='sbatch' and '--hold' not in command,'Single held sbatch mutation required')
        require(time.time()+210<c['end'],'Management deadline too close for a new held submission')
        return runner(['sbatch','--hold',*command[1:]],timeout=30,**kwargs)
    return submit(pack['path'],pack['ledger'],runner=held)


def orchestrate(c,*,runner=subprocess.run):
    """Context is verified; caller holds the global registration mutex and lease."""
    require(time.time()+180<c['end'],'Management allocation deadline too close for submission')
    qualify_controller(c,runner=runner)
    c['admission']('before-continuation',cpus=5,gpus=0,hours=96)
    raw=c['native_reader']([c['gpu_job'],c['plan_job']],{},owner='pwa209',account=None)
    native={job:c['controller'].normalized_native(job,record) for job,record in raw.items()}
    d2path,d2sha,d2=stable_descriptor(c,native)
    definitions=[('af-precision-diagnostics-v2','precision-diagnostics',4,12,None,'afterok'),
        ('af-precision-chunk-v2','precision-chunk',4,12,'af-precision-diagnostics-v2','afterok'),
        ('af-precision-assemble-v2','precision-assemble',4,1,'af-precision-chunk-v2','afterok')]
    canary=freeze_plan(c,'study-af-precision-v2-20261021',definitions,d2path,d2sha)
    canary_ledger=submit_held(c,canary,runner=runner)
    jobs={name:entry['job_id'] for name,entry in canary_ledger['tasks'].items()}
    gate={'diagnostics_receipt_path':str(CASE/('native-'+jobs['af-precision-diagnostics-v2'])/'receipt.json'),
        'diagnostics_job_id':jobs['af-precision-diagnostics-v2'],
        'assembly_receipt_path':str(CASE/('native-'+jobs['af-precision-assemble-v2'])/'receipt.json'),
        'assembly_job_id':jobs['af-precision-assemble-v2'],'descriptor_path':str(d2path),'descriptor_sha256':d2sha}
    d3=dict(d2,worker_descriptor={'path':str(d2path),'sha256':d2sha},precision_gate=gate,controller_sha256=CONTROLLER_SHA)
    d3path=c['area']/'descriptor-v3-controller.json';exclusive(d3path,d3);d3sha=digest(d3path)
    c['controller'].worker_binding(d3)
    definitions=[('af-f2-v2-controller-'+str(i),i,1,96,'af-f2-v2-controller-'+str(i-1) if i else None,'afterany') for i in range(4)]
    controls=freeze_plan(c,'study-af-f2-controllers-v2-20261021',definitions,d3path,d3sha,controller=True)
    controller_ledger=submit_held(c,controls,runner=runner)
    packs=[(canary,canary_ledger),(controls,controller_ledger)];proof={}
    for pack,ledger in packs:
        for task in pack['value']['tasks']:
            job=ledger['tasks'][task['name']]['job_id'];proof[job]=held_proof(c,task,job,c['scheduler'])
    atomic_json(c['area']/'held-native-proof.json',proof)
    c['admission']('all-continuation-registered',cpus=0,gpus=0,hours=1)
    require(time.time()+30*len(proof)+180<c['end'],'Finite management margin exhausted before first release')
    exclusive(c['area']/'release-intent.json',{'job_ids':list(proof),'held_native_proof_sha256':digest(c['area']/'held-native-proof.json'),
        'descriptor_v2_sha256':d2sha,'descriptor_v3_sha256':d3sha,'automatic_replay_permitted':False})
    for job in proof:
        require(time.time()+210<c['end'],'Finite management margin exhausted before next release')
        exclusive(c['area']/('release-'+job+'-intent.json'),{'job_id':job,'automatic_replay_permitted':False})
        result=runner(['scontrol','release',job],capture_output=True,text=True,timeout=30)
        atomic_json(c['area']/('release-'+job+'-result.json'),{'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr})
        result.check_returncode()
    receipt={'status':'precision_and_four_finite_f2_controllers_registered_and_released','native_management_job_id':c['job'],
        'descriptor_v2_path':str(d2path),'descriptor_v2_sha256':d2sha,'descriptor_v3_path':str(d3path),'descriptor_v3_sha256':d3sha,
        'source_sha256':SOURCE,'worker_sha256':WORKER_SHA,'controller_sha256':CONTROLLER_SHA,
        'plans':[{'path':str(pack['path']),'sha256':canonical_hash(pack['value']),'ledger_path':str(pack['ledger'])} for pack,_ in packs],
        'jobs':{task['name']:ledger['tasks'][task['name']]['job_id'] for pack,ledger in packs for task in pack['value']['tasks']},
        'precision_gate_complete':False,'scientific_result':False,'admission_authority':False,'H4_inference_status':'pending',
        'previous_failed_attempt':c['previous_failure_proof'],'created_utc':datetime.now(timezone.utc).isoformat()}
    exclusive(c['area']/'receipt.json',receipt);return receipt


def main():
    p=argparse.ArgumentParser()
    for name in ('descriptor-v1','controller-path','controller-tests','management-script'):p.add_argument('--'+name,type=Path,required=True)
    for name in ('descriptor-v1-sha256','controller-sha256','controller-tests-sha256','gpu-job-id','plan-job-id'):p.add_argument('--'+name,required=True)
    a=p.parse_args();job=str(native_job_id())
    require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209' and job.isdigit()
        and os.environ.get('SLURM_CPUS_PER_TASK')=='1' and os.environ.get('HHW_GPU_COUNT')=='0'
        and os.environ.get('HHW_MAX_HOURS')=='1','Owned finite one-CPU management allocation required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval context required')
    require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA,'Unchanged qualified execution/primary source required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py','Exact qualified package required')
    config=read_json(PRIMARY/'configs/study.json');require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Frozen scientific configuration changed')
    require(a.descriptor_v1.resolve()==CASE/'descriptor-v1.json','Exact original activation descriptor required')
    d=frozen(a.descriptor_v1,a.descriptor_v1_sha256)
    require(d['execution_source_sha256']==SOURCE and d['framework_release']==str(FRAMEWORK) and d['case']==str(CASE)
        and d['agent_campaign_case']==str(CASE/'agent-final-f64-v1') and d['primary_source_sha256']==PRIMARY_SHA
        and d['full_config_sha256']==CONFIG and digest(Path(d['worker_path']))==d['worker_sha256']==WORKER_SHA,'Original exact worker/source/case bindings required')
    require(digest(ROOT/'results/repair-v7-2026-10-13/agents/frozen_protocol.json')==d['frozen_protocol_sha256'],
        'Original development-frozen protocol changed')
    require(a.controller_sha256==CONTROLLER_SHA and digest(a.controller_path)==CONTROLLER_SHA
        and a.controller_path.resolve().is_relative_to(ROOT/'maintenance') and not a.controller_path.is_symlink()
        and a.controller_path.name=='run-af-agent-controller-v1-20261021.py'
        and a.controller_tests.resolve().parent==a.controller_path.resolve().parent
        and a.controller_tests.name=='test_af_agent_controller_v1_20261021.py'
        and digest(a.controller_tests)==a.controller_tests_sha256,'Exact reviewed private controller/test pair required')
    q=frozen(d['qualification']['path'],d['qualification']['sha256'])
    require(q['status']=='complete' and q['returncode']==0 and q['config_sha256']==CONFIG and q['source_sha256']==SOURCE
        and q['source_verified_unchanged'] is True and q['test_counts']=={'tests':1694,'failures':0,'errors':0,'skipped':0}
        and str(q['slurm_job_id'])==str(d['qualification']['job_id']),'Full 1694-test native qualification binding required')
    for path,sha in [(GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA)]:require(digest(path)==sha,'Pinned native reader changed')
    require(a.gpu_job_id.isdigit() and a.plan_job_id.isdigit() and a.gpu_job_id!=a.plan_job_id,'Two exact distinct original prerequisite jobs required')
    area=CASE/'precision-and-f2-continuation-v2';require(not area.exists(),'One-time continuation cannot replay existing/ambiguous work');area.mkdir(mode=0o700)
    runpy.run_path(str(GUARD))['install'](area/'accounting-reads')
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'Guard must precede campaign/native reader binding')
    require(completed_native_dependency(ROOT,str(q['slurm_job_id'])) is not None,'Positive completed full native qualification required')
    spec=importlib.util.spec_from_file_location('qualified_private_f2_controller',a.controller_path)
    controller=importlib.util.module_from_spec(spec);spec.loader.exec_module(controller)
    reader=runpy.run_path(str(SCHEDULER))['run_scontrol_job'];own=controller.fields(reader(job,area/'own-scheduler'))
    require(own.get('JobId')==job and own.get('UserId')=='pwa209(3164768)' and own.get('JobState')=='RUNNING'
        and own.get('Account')==config['paths']['cpu_account'] and own.get('Command')==str(a.management_script)
        and a.management_script.resolve().is_relative_to(ROOT/'plans') and own.get('NumCPUs')=='1'
        and own.get('NumNodes') in {'1','1-1'} and controller.seconds(own.get('TimeLimit',''))==3600
        and memory(own.get('MinMemoryNode',''))==memory('16G') and controller.gpu_count(own.get('AllocTRES',''))==0,
        'Exact owned finite native management script/resources required')
    end=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    native_end=datetime.fromisoformat(own.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    require(math.isfinite(end) and abs(end-native_end)<=2 and end>time.time()+1500,'Verified management end and qualification/submission margin required')
    policy=load_resource_policy(Path(d['resource_policy']['path']),d['resource_policy']['sha256'],root=ROOT,config=config)
    c={'job':job,'descriptor':d,'descriptor_sha256':a.descriptor_v1_sha256,'policy':policy,'config':config,'area':area,
        'controller':controller,'controller_path':a.controller_path,'tests_path':a.controller_tests,'tests_sha256':a.controller_tests_sha256,
        'gpu_job':a.gpu_job_id,'plan_job':a.plan_job_id,'native_reader':repair._repair_native_states,'scheduler':reader,'end':end}
    c['previous_failure_proof']=previous_failure(c,repair._repair_native_states([PREVIOUS_JOB],{},owner='pwa209',account=config['paths']['cpu_account']))
    c['admission']=lambda label,**kwargs:campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],area,label,resource_policy=policy,**kwargs)
    with resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        headroom=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,required_bytes=10_000_000_000,
            reserve_files=248000,reserve_bytes=2_000_000_000_000,evidence_dir=area/'quota-reads',allocation_deadline_unix=end)
        census=read_json(ROOT/'state/storage-budget.json')
        captured=datetime.fromisoformat(census['captured_utc'].replace('Z','+00:00'))
        require(captured.tzinfo is not None and 0<=(datetime.now(timezone.utc)-captured).total_seconds()<=21600,
            'Fresh six-hour study occupancy required before bounded continuation output')
        _storage(ROOT,config,10_000_000_000,2000,personal_headroom=headroom)
        with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=120):receipt=orchestrate(c)
    print('AF_CONTINUED '+json.dumps(receipt,sort_keys=True),flush=True)


if __name__=='__main__':main()
