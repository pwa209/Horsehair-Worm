"""One-off retirement of proven inode-preflight failures; no scientific retry logic."""
from pathlib import Path
import copy
import gzip
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import time

ROOT=Path('/scratch/pwa209/horsehair-worm')
CASE=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
AREA=ROOT/'maintenance/agent-quota-preflight-recovery-20261025'
BASE=ROOT/'maintenance/study-af-backfill-controller-support-v2-20261024/run-af-agent-controller-backfill-base-v2-20261024.py'
BASE_SHA='cbf01cffbbaa9a983ec1e8e6394b38ac04623483e768398c18f48b5e30e31445'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
DESCRIPTOR='a56965b46c09cdffa4512302dea87ca895372f7668f88748ac2263c4b9a2c6b2'
TARGETS={'assembly-0081':'7191569','assembly-0082':'7274952','assembly-0083':'7275024',
         'evaluation-0085-01':'7275127','evaluation-0086-01':'7275168'}
GPU_TARGETS={'training-0089':'7275326','training-0090':'7275520'}


def require(value,message):
    if not value:raise ValueError(message)


def recover(plan,state,targets,evidence,dispatch):
    """Keep the intention and all scientific cells; archive each failed attempt."""
    result=dispatch._state(plan,state)
    require(targets and set(evidence)==set(targets),'Exact preflight evidence roster required')
    require(set(state['needs_repair'])<=set(targets),'Unrelated failures cannot be cleared')
    for ident,job_id in targets.items():
        job=result['jobs'][ident];item=evidence[ident];proof=item['native_proof'];receipt=item['receipt']
        require(job['job_id']==job_id and job['status'] in {'submitted','needs_repair'}
                and job['receipt'] is None and job['intent']['kind'] in {'assembly','evaluation','training'},
                'Only the exact unfinished attempt may be retired')
        gpus=job['intent']['resources']['gpus']
        require(proof['job_id']==job_id and proof['state']=='FAILED' and proof['exit_code']=='1:0'
                and proof['user']=='pwa209' and proof['account']==('def-ptewarie_gpu' if gpus else 'def-ptewarie_cpu')
                and proof['cpus']==job['intent']['resources']['cpus'] and proof['gpus']==gpus
                and proof['elapsed_seconds']>0 and proof['end']!='Unknown',
                'Actual final owned failure proof required')
        dispatch._native(job,proof)
        require(receipt['status']=='running' and receipt['native_job_id']==job_id
                and receipt['index']==job['intent']['cell_index']
                and receipt['mode']=={'assembly':'agent-assembly','evaluation':'agent-evaluation','training':'agent-train'}[job['intent']['kind']]
                and receipt['source_sha256']==SOURCE and receipt['descriptor_sha256']==DESCRIPTOR
                and not any(k in receipt for k in ('result','training_summary','evaluation_case','completed_unix')),
                'A scientific result or another execution context cannot be retried')
        log=item['quota_traceback']
        require('native-worker.py", line 66' in log and 'quota=runpy.run_path' in log
                and 'StorageHeadroomError: Personal scratch storage is pending:' in log
                and 'inode headroom is insufficient:' in log,
                'Exact failure before scientific execution at the inode preflight required')
        require(item['artifact_archive']['sha256'] and len(item['artifact_archive']['sha256'])==64,
                'Preserved original artifact archive required')
        history=copy.deepcopy(job.get('retired_preflight_attempts',[]))
        history.append({'job_id':job_id,'native_proof':proof,'receipt':receipt,
                        'quota_failure_log':item['quota_failure_log'],
                        'artifact_archive':item['artifact_archive'],
                        'reason':'inode_preflight_before_scientific_execution'})
        job.update(job_id=None,status='ambiguous',native_proof=None,receipt=None,
                   retired_preflight_attempts=history)
    require(result['cells']==state['cells'] and all(result['jobs'][k]==v for k,v in state['jobs'].items() if k not in targets),
            'Unrelated cells and jobs must remain byte-equivalent')
    result.update(status='active',needs_repair=[])
    result=dispatch._finish(result);dispatch._state(plan,result)
    return result


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main(pin,mode='cpu'):
    global AREA,TARGETS
    require(mode in {'cpu','gpu'},'One explicit reviewed preflight roster required')
    if mode=='gpu':
        AREA=ROOT/'maintenance/agent-quota-preflight-gpu-recovery-20261025'
        TARGETS=GPU_TARGETS
    from horsehair import repair,budget
    from horsehair.core import read_json,atomic_json,exclusive_file_lock,canonical_hash
    require(os.geteuid()==3164768 and os.environ['USER']=='pwa209','Owned Narval administration required')
    require(digest(__file__)==pin and digest(BASE)==BASE_SHA,'Exact reviewed action/base source required')
    require(not (AREA/'complete.json').exists(),'Recovery already executed; inspect its receipt instead of replaying')
    retirement=read_json(AREA/'retirement-complete.json')
    require(retirement['permanent_controller_mutex_obtained'] is True,'Previous controllers must be retired first')
    spec=importlib.util.spec_from_file_location('preserved_base_quota_recovery',BASE)
    base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)
    descriptor=read_json(ROOT/'maintenance/study-af-campaign-v1-20261021/precision-and-f2-continuation-v2/descriptor-v2-worker.json')
    require(digest(ROOT/'maintenance/study-af-campaign-v1-20261021/precision-and-f2-continuation-v2/descriptor-v2-worker.json')==DESCRIPTOR,
            'Original frozen worker descriptor required')
    require(digest(CASE.parent/'native-worker.py')==descriptor['worker_sha256'],'Original worker code changed')
    # Missing/stale native records are read-only waits, never manufactured failures.
    stop=time.monotonic()+1200
    while True:
        rows=repair._repair_native_states(list(TARGETS.values()),{},owner='pwa209',account=None,
             runner=lambda cmd,**kw:subprocess.run(cmd,**{**kw,'timeout':90}))
        if set(rows)==set(TARGETS.values()) and all(row['state']=='FAILED' and row['exit_code']=='1:0' for row in rows.values()):break
        require(not any(row['state']=='COMPLETED' for row in rows.values()),'Completed computation cannot be retired')
        require(time.monotonic()+60<stop,'Final failure accounting remains unavailable; no state mutation performed')
        atomic_json(AREA/'native-wait.json',{'status':'waiting_for_actual_final_preflight_failures','observed':rows})
        time.sleep(60)
    native={job:base.normalized_native(job,row) for job,row in rows.items()}
    headroom=budget.require_storage_headroom(ROOT,required_files=2000,required_bytes=10_000_000_000,
                    reserve_files=248000,reserve_bytes=2_000_000_000_000)
    require(headroom['ready'] is True,'The existing worker storage preflight must pass before retrying')
    with exclusive_file_lock(CASE/'coordination-controller.mutex',timeout=180):
      with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=180):
        require(digest(CASE/'state.json.gz')==retirement['state_file_sha256'],'Campaign changed after retirement')
        plan,state=base.initialize({'case':CASE});evidence={};moves=[]
        require(plan['plan_sha256']=='36f7db5dc790108040e454674006421144184db929b3de88aec2b9a9008727e0',
                'Only the existing frozen campaign may be repaired')
        archive=AREA/'failed-attempts';archive.mkdir()
        for ident,job in TARGETS.items():
            phase='af-f2-'+ident;directory=ROOT/'plans'/phase;ledger=ROOT/'state'/(phase+'-submission.json');journal=CASE/('submission-'+ident+'.json')
            p=read_json(directory/'plan.json');l=read_json(ledger);j=read_json(journal);task=p['tasks'][0]
            require(j['status']=='released' and j['job_id']==l['tasks'][task['name']]['job_id']==job
                    and p['intent_sha256']==state['jobs'][ident]['intent']['intent_sha256']
                    and l['plan_sha256']==canonical_hash(p) and task['script_sha256']==canonical_hash(Path(task['script']).read_text()),
                    'Original released plan, ledger and journal must join their failed attempt')
            if state['jobs'][ident]['intent']['kind']=='training':
                training=Path(state['jobs'][ident]['intent']['case'])
                require(training.resolve()==training and training.is_relative_to(ROOT)
                        and not (training/'checkpoint.pt').exists() and not (training/'summary.json').exists(),
                        'A training checkpoint or result must be preserved for explicit resume, not reset')
            paths=[directory,ledger,journal];inventory={}
            for path in paths:
                require(path.resolve()==path and path.is_relative_to(ROOT) and not path.is_symlink(),'Ordinary personal-scratch artifact required')
                files=list(path.rglob('*')) if path.is_dir() else [path]
                for f in files:
                    require(not f.is_symlink(),'Redirected failure artifact refused')
                    if f.is_file():inventory[str(f)]={'sha256':digest(f),'bytes':f.stat().st_size}
                destination=archive/(ident+'-'+('plan' if path==directory else 'ledger.json' if path==ledger else 'journal.json'))
                require(not destination.exists(),'Failure archive already exists; inspect interrupted recovery')
                moves.append((path,destination))
            manifest=archive/(ident+'-manifest.json');atomic_json(manifest,{'job_id':job,'files':inventory,'scientific_data_changed':False})
            receipt_path=CASE.parent/('native-'+job)/'receipt.json';logs=list((ROOT/'logs').glob(phase+'-'+job+'*'))
            require(len(logs)==1,'One exact failed native worker log required')
            evidence[ident]={'native_proof':native[job],'receipt':read_json(receipt_path),'quota_traceback':logs[0].read_text(),
                 'quota_failure_log':{'path':str(logs[0]),'sha256':digest(logs[0])},'artifact_archive':{'path':str(manifest),'sha256':digest(manifest)}}
        result=recover(plan,state,TARGETS,evidence,base.dispatch)
        budget.native_accounting(ROOT,list(TARGETS.values()),runner=lambda cmd,**kw:subprocess.run(cmd,**{**kw,'timeout':90}))
        before=AREA/'state-before-reset.json.gz'
        with before.open('xb') as f:f.write((CASE/'state.json.gz').read_bytes());f.flush();os.fsync(f.fileno())
        atomic_json(AREA/'mutation-intent.json',{'before_state_sha256':state['state_sha256'],'retry_state_sha256':result['state_sha256'],
                    'moves':[{'from':str(a),'to':str(b)} for a,b in moves],'targets':TARGETS})
        for source,destination in moves:os.rename(source,destination)
        base.save_state({'case':CASE},result)
        record={'status':('two' if mode=='gpu' else 'five')+'_proven_quota_preflight_failures_preserved_and_ready_for_retry','targets':TARGETS,
                'before_state_sha256':state['state_sha256'],'retry_state_sha256':result['state_sha256'],
                'revision':result['revision'],'headroom':headroom,'source_unchanged':True,'scientific_gates_changed':False,
                'original_intentions_and_cells_preserved':True,'archive':str(archive),'action_sha256':pin}
        atomic_json(AREA/'complete.json',record)
        atomic_json(CASE/'status.json',{'status':'waiting_for_repaired_controller','revision':result['revision'],
                  'reason':'Proven source-bound inode-preflight attempts preserved for a fresh native attempt',
                  'native_controller_job':None,'scientific_result':False,'admission_authority':False})
    print('QUOTA_PREFLIGHT_RECOVERY '+json.dumps(record),flush=True)


if __name__=='__main__':main(sys.argv[1],sys.argv[2] if len(sys.argv)>2 else 'cpu')
