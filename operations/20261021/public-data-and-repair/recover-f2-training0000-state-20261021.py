"""Pure, single-use operational amendment for failed native training0000.

No filesystem, scheduler, model loading or deletion occurs here. The native
caller freezes original inputs, owns/holds af-campaign-registration.mutex,
checks actual absence and source/qualification, then applies compare-and-swap.
Byte packets bind parsed proof values to the exact supplied source bytes. Old
failure evidence stays in the immutable amendment, outside the new active view.
"""
from pathlib import PurePosixPath
from datetime import datetime,timezone
import copy
import gzip
import hashlib
import json
import math
import re
import shlex

from horsehair import agent_campaign as dispatch
from horsehair.core import canonical_hash

ROOT=PurePosixPath('/scratch/pwa209/horsehair-worm')
CASE=ROOT/'maintenance/study-af-training0000-state-repair-v1-20261021'
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PRIMARY='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
DISPATCH_SHA='70f28e57f0e4a2eee3ebfef2699e751569fe048b550a94fa36a5461269493070'
WORKER_SHA='f00d4265a68a3ace5faa47da4fe2701fff3fe86d9132b09ea748e9f12471e4fd'
WORKER_DESCRIPTOR=ROOT/'maintenance/study-af-campaign-v1-20261021/precision-and-f2-continuation-v2/descriptor-v2-worker.json'
WORKER_DESCRIPTOR_SHA='a56965b46c09cdffa4512302dea87ca895372f7668f88748ac2263c4b9a2c6b2'
NATIVE_EVIDENCE=ROOT/'maintenance/study-af-f2-race-repair-evidence-v1-20261021/evidence-packet.json'
FAILED='6689664'
HEALTHY={'1':'6689708','2':'6689921','3':'6689974'}
CONTROLLERS={'6689295','6689301','6689323','6689324'}
TARGET='training-0000'
NAMESPACE='af-f2-r1-training-0000'
FAILURE_MARKER='ValueError: A source/configuration-bound submission ledger has no unchanged study plan: /scratch/pwa209/horsehair-worm/state/af-f2-training-0001-submission.json'


def require(value,message):
    if not value:raise ValueError(message)


def raw_sha(raw):return hashlib.sha256(raw).hexdigest()


def path(value):
    require(isinstance(value,str) and PurePosixPath(value).is_absolute() and str(PurePosixPath(value))==value
        and '..' not in value.split('/') and '.' not in value.split('/') and PurePosixPath(value).is_relative_to(ROOT),
        'Exact personal POSIX proof path required')
    return PurePosixPath(value)


def packet(value,*,expected=None,encoding='json',maximum=64_000_000):
    require(set(value)=={'path','sha256','raw'} and type(value['raw']) is bytes and len(value['raw'])<=maximum
        and re.fullmatch('[0-9a-f]{64}',value['sha256']) and raw_sha(value['raw'])==value['sha256'],
        'Exact bounded byte-bound proof packet required')
    source=path(value['path'])
    if expected is not None:require(source==expected,'Proof packet belongs to a different original file')
    raw=value['raw']
    if encoding=='gzip-json':
        import io
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:raw=stream.read(maximum+1)
        require(len(raw)<=maximum,'Bounded original compressed state required')
    if encoding in {'json','gzip-json'}:
        def unique(pairs):
            result={}
            for key,item in pairs:
                require(key not in result,'Duplicate JSON proof key refused');result[key]=item
            return result
        def constant(value):raise ValueError('Nonfinite JSON proof constant refused')
        parsed=json.loads(raw,object_pairs_hook=unique,parse_constant=constant)
    elif encoding=='text':parsed=raw.decode('utf8')
    elif encoding=='binary':parsed=None
    else:raise ValueError('Unknown proof encoding')
    return parsed,{'path':str(source),'sha256':value['sha256'],'bytes':len(value['raw']),
        **({'parsed_value_sha256':canonical_hash(parsed)} if parsed is not None else {})}


def native(value,job,*,cpus,gpus,complete=False,unallocated_controller=False):
    require(value.get('job_id')==job and value.get('user')=='pwa209'
        and value.get('account')==('def-ptewarie_gpu' if gpus else 'def-ptewarie_cpu')
        and type(value.get('cpus')) is int and type(value.get('gpus')) is int
        and type(value.get('elapsed_seconds')) is int and value['elapsed_seconds']>=0,
        'Owned exact native allocation/proof required')
    state=value.get('state');exit_code=value.get('exit_code')
    require(isinstance(exit_code,str) and re.fullmatch('[0-9]+:[0-9]+',exit_code),'Exact terminal native exit code required')
    if unallocated_controller and state=='CANCELLED' and value['elapsed_seconds']==0:
        require(value['cpus']==value['gpus']==0,'Unallocated cancelled controller must have zero resources')
    else:
        require(value['cpus']==cpus and value['gpus']==gpus and value['elapsed_seconds']>0,'Actual native resources differ')
    if complete:require(state=='COMPLETED' and exit_code=='0:0','Healthy original training must be positively complete')
    else:require(state in dispatch.TERMINAL_FAILURES|{'COMPLETED'},'Every old controller/failed worker must be terminal')


def normalize_native(job,record):
    require(record.get('job_id_raw')==job and record.get('unallocated_metadata') is False,'Exact scalar native identity required')
    values={}
    for item in record.get('alloc_tres','').split(','):
        if not item:continue
        key,separator,value=item.partition('=');require(separator and key not in values,'Duplicate/malformed native TRES refused');values[key]=value
    generic=values.get('gres/gpu');typed=[value for key,value in values.items() if key.startswith('gres/gpu:')]
    require((generic is None or generic.isdigit()) and all(value.isdigit() for value in typed),'Negative/malformed GPU allocation refused')
    gpus=int(generic) if generic is not None else sum(map(int,typed))
    require(not typed or sum(map(int,typed))==gpus,'Contradictory native GPU evidence refused')
    return {'job_id':job,'state':record['state'],'exit_code':record['exit_code'],'cpus':record['cpus'],'gpus':gpus,
        'elapsed_seconds':record['elapsed_seconds'],'end':record.get('end','Unknown'),'user':record['user'],'account':record['account']}


def clock_only_change(old,new,job,erratum):
    """Validate an explicit timestamp-basis erratum, never a changed outcome."""
    if old==new:return False
    require(isinstance(old,dict) and isinstance(new,dict) and set(old)==set(new) and 'end' in old
        and {k:v for k,v in old.items() if k!='end'}=={k:v for k,v in new.items() if k!='end'},
        'A native clock erratum may change only end, never state/resources/identity/exit')
    entry=erratum.get('jobs',{}).get(job)
    require(erratum.get('status')=='source_time_basis_erratum' and erratum.get('old_timezone_verified') is False
        and erratum.get('old_timezone')=='unverified_naive_slurm_local' and erratum.get('new_timezone')=='UTC'
        and erratum.get('new_query_environment')=={'TZ':'UTC'}
        and entry=={'old_end':old['end'],'new_end':new['end'],'all_other_native_fields_unchanged':True,'observed_shift_seconds':14400},
        'Exact explicit native timestamp-basis erratum required')
    before=datetime.fromisoformat(old['end']);after=datetime.fromisoformat(new['end'])
    require(before.tzinfo is None and (after.tzinfo is None or after.utcoffset().total_seconds()==0),
        'Original naive end and explicitly queried UTC end required')
    # The historical timezone is not established. This four-hour difference is
    # an observed case-specific operational erratum, not an inferred IANA zone
    # or a biological timestamp conversion.
    require((after.replace(tzinfo=None)-before).total_seconds()==14400,
        'Native end-field change differs from the observed case-specific clock erratum')
    return True


def migrate_training0000(plan,state,proof):
    """Return a derivative state and an immutable amendment receipt; no I/O.

    ``proof`` values that refer to files are packets containing exact raw bytes.
    Caller must persist those byte-for-byte under its own immutable case and
    verify real absence under the kernel registration lock before activation.
    """
    require(set(proof)=={'case','dispatcher_sha256','approval','original_state_gzip','original_state_binding',
        'original_campaign_plan','worker_descriptor','failed','old_controllers','healthy','clock_erratum','native_evidence_packet'},'Exact target-only amendment proof schema required')
    require(proof['case']==str(CASE) and proof['dispatcher_sha256']==DISPATCH_SHA,'Exact new amendment namespace and unchanged dispatcher required')
    approval=proof['approval'];require(approval.get('source')=='current_user'
        and approval.get('authorizes_targeted_operational_repair') is True and isinstance(approval.get('request_reference'),str)
        and bool(approval['request_reference'].strip()),'Explicit existing-user repair authorization provenance required')
    dispatch._state(plan,state)
    require(state.get('scientific_result') is False and state.get('admission_authority') is False
        and state.get('H4_inference_status')==plan['H4_inference_status'],'Operational amendment cannot clear inference/admission gates')
    require(plan['case']==str(CAMPAIGN) and plan['source_binding']['execution_source_sha256']==SOURCE
        and plan['source_binding']['primary_source_sha256']==PRIMARY and canonical_hash(plan['config'])==CONFIG,
        'Exact original source/configuration-bound campaign required')
    require(state['status']=='needs_repair' and state['needs_repair']==[TARGET]
        and set(state['jobs'])=={f'training-{i:04d}' for i in range(4)},'Only original failed0000 and three healthy intentions may exist')
    require(all(cell['evaluation_plan'] is None and cell['evaluation_jobs']==[] and cell['assembly_job'] is None
        and cell['conditions']=={} and cell['complete'] is False for cell in state['cells']), 'Evaluation/assembly work cannot be silently rebased')
    require(all(cell['training_job']==(f'training-{i:04d}' if i<4 else None) for i,cell in enumerate(state['cells'])),
        'Original four training references must be unchanged')
    failed_job=state['jobs'][TARGET]
    require(failed_job['job_id']==FAILED and failed_job['status']=='needs_repair' and failed_job.get('receipt') is None,
        'Only the known pre-training failure may be removed from the active view')
    require(all(job['job_id'] is not None and job['status']!='ambiguous' for job in state['jobs'].values()),
        'Ambiguous submissions must be preserved and reconciled, never retried')
    pins={}
    original,pins['original_state_gzip']=packet(proof['original_state_gzip'],expected=CASE/'original-state.json.gz',encoding='gzip-json')
    require(original==state,'Frozen original compressed state differs')
    binding,pins['original_state_binding']=packet(proof['original_state_binding'],expected=CASE/'original-state-binding.json')
    require(binding.get('state_file_sha256')==proof['original_state_gzip']['sha256'] and binding.get('state_sha256')==state['state_sha256']
        and binding.get('campaign_plan_sha256')==plan['plan_sha256'] and binding.get('revision')==state['revision']
        and binding.get('scientific_result') is False and binding.get('admission_authority') is False,'Original compressed state/binding join changed')
    original_plan,pins['original_campaign_plan']=packet(proof['original_campaign_plan'],expected=CASE/'original-campaign-plan.json')
    require(original_plan==plan,'Frozen original campaign plan differs')
    source_native,pins['native_evidence_packet']=packet(proof['native_evidence_packet'],expected=NATIVE_EVIDENCE)
    require(source_native.get('schema_version')==1 and source_native.get('native_timezone')=='UTC'
        and source_native.get('no_source_evidence_modified') is True
        and set(source_native.get('native_observed',{}))==CONTROLLERS|{FAILED,*HEALTHY.values()},'Exact frozen UTC native evidence packet required')
    native_values={job:normalize_native(job,record) for job,record in source_native['native_observed'].items()}
    erratum,pins['clock_erratum']=packet(proof['clock_erratum'],expected=CASE/'clock-erratum.json');corrected={}
    descriptor,pins['worker_descriptor']=packet(proof['worker_descriptor'],expected=WORKER_DESCRIPTOR)
    require(proof['worker_descriptor']['sha256']==WORKER_DESCRIPTOR_SHA and descriptor.get('worker_sha256')==WORKER_SHA
        and descriptor.get('execution_source_sha256')==SOURCE and descriptor.get('agent_campaign_case')==str(CAMPAIGN)
        and descriptor.get('resource_policy',{}).get('sha256')==plan['policy_binding']['sha256']
        and descriptor.get('frozen_protocol_sha256')==plan['protocol_binding']['sha256'], 'Stable original worker descriptor changed')
    require(set(proof['old_controllers'])==CONTROLLERS,'All four exact old F2 controller segments must be terminal')
    pins['old_controllers']={}
    for job,value in proof['old_controllers'].items():
        observed,ref=packet(value,expected=CASE/'native'/('controller-'+job+'.json'))
        require(observed==native_values[job],'Controller proof differs from frozen UTC native evidence')
        native(observed,job,cpus=1,gpus=0,unallocated_controller=True);pins['old_controllers'][job]=ref
    failed=proof['failed'];require(set(failed)=={'native','native_plan','submission_ledger','script','traceback','absence'},'Complete original failed allocation evidence required')
    observed,pins['failed_native']=packet(failed['native'],expected=CASE/'native'/('worker-'+FAILED+'.json'))
    require(observed==native_values[FAILED],'Failed native proof differs from frozen UTC evidence')
    native(observed,FAILED,cpus=6,gpus=1)
    require(observed['state']=='FAILED' and observed['exit_code']=='1:0',
        'Actual original FAILED/1:0 evidence must remain unchanged')
    if clock_only_change(failed_job.get('native_proof'),observed,FAILED,erratum):corrected[FAILED]={'old':copy.deepcopy(failed_job['native_proof']),'new':copy.deepcopy(observed)}
    native_plan,pins['failed_native_plan']=packet(failed['native_plan'],expected=ROOT/'plans/af-f2-training-0000/plan.json')
    ledger,pins['failed_submission_ledger']=packet(failed['submission_ledger'],expected=ROOT/'state/af-f2-training-0000-submission.json')
    script,pins['failed_script']=packet(failed['script'],expected=ROOT/'plans/af-f2-training-0000/af-f2-training-0000.sbatch',encoding='text')
    require(native_plan.get('root')==str(ROOT) and native_plan.get('phase')=='af-f2-training-0000'
        and native_plan.get('config_sha256')==CONFIG and native_plan.get('release_identity',{}).get('sha256')==SOURCE
        and native_plan.get('campaign_plan_sha256')==plan['plan_sha256'] and native_plan.get('intent_sha256')==failed_job['intent']['intent_sha256']
        and native_plan.get('worker_descriptor_sha256')==WORKER_DESCRIPTOR_SHA and len(native_plan.get('tasks',[]))==1,
        'Original failed native plan/source/intention changed')
    task=native_plan['tasks'][0];name='af-f2-training-0000';contract={'cpus':6,'gpu':True,'hours':12,'memory':'64G'}
    registered=ledger.get('tasks',{}).get(name,{})
    require(task.get('name')==name and task.get('script')==failed['script']['path'] and task.get('script_sha256')==canonical_hash(script)
        and task.get('resources')==contract and ledger.get('source_sha256')==SOURCE and ledger.get('plan_sha256')==canonical_hash(native_plan)
        and registered.get('job_id')==FAILED and registered.get('state')=='submitted' and registered.get('script_sha256')==task['script_sha256'],
        'Original failed script/ledger/resources no longer agree')
    argv=shlex.split(task['command']);expected=['python',descriptor['worker_path'],'--descriptor',str(WORKER_DESCRIPTOR),
        '--descriptor-sha256',WORKER_DESCRIPTOR_SHA,'--mode','agent-train','--index','0','--cpus','6','--gpus','1','--hours','12']
    require(argv==expected and task['command'] in script,'Original worker/descriptor/index command changed')
    traceback,pins['failure_traceback']=packet(failed['traceback'],expected=ROOT/'logs/af-f2-training-0000-6689664_4294967294.log',encoding='text')
    require(FAILURE_MARKER in traceback and "area,'native-start-admission'" in traceback and 'all_registered_work' in traceback
        and 'AF_NATIVE_WORK ' not in traceback,'Exact before-admission registry-publication failure is required')
    mapping={'intent_id':TARGET,'plan_name':NAMESPACE,'plan_path':str(ROOT/'plans'/NAMESPACE/'plan.json'),
        'ledger_path':str(ROOT/'state'/(NAMESPACE+'-submission.json')),'journal_path':str(CAMPAIGN/'submission-r1-training-0000.json'),
        'worker_descriptor':{'path':str(WORKER_DESCRIPTOR),'sha256':WORKER_DESCRIPTOR_SHA},'canonical_training_case':plan['cells'][0]['training_case']}
    absent_expected={str(PurePosixPath(plan['cells'][0]['training_case'])/'checkpoint.pt'),str(PurePosixPath(plan['cells'][0]['training_case'])/'summary.json'),
        str(ROOT/'maintenance/study-af-campaign-v1-20261021'/('native-'+FAILED)/'receipt.json'),str(ROOT/'plans'/NAMESPACE),
        mapping['ledger_path'],mapping['journal_path']}
    absence,pins['absence']=packet(failed['absence'],expected=CASE/'verified-absence-under-lock.json')
    entries=absence.get('entries',[])
    require(absence.get('registration_mutex')==str(ROOT/'state/af-campaign-registration.mutex') and absence.get('registration_mutex_held') is True
        and absence.get('native_campaign_controller_ids')==sorted(CONTROLLERS) and absence.get('captured_original_state_sha256')==state['state_sha256']
        and type(absence.get('captured_unix')) in (int,float) and math.isfinite(absence['captured_unix']) and absence['captured_unix']>0
        and len(entries)==len(absent_expected) and {e.get('path') for e in entries}==absent_expected
        and all(e.get('exists') is False and e.get('is_symlink') is False for e in entries),
        'Actual target output/new-attempt absence must be captured under the registration lock')
    require(set(proof['healthy'])==set(HEALTHY),'Exactly original healthy1/2/3 evidence required')
    pins['healthy']={}
    for index,job in HEALTHY.items():
        cell=plan['cells'][int(index)];ident=f'training-{int(index):04d}';old=state['jobs'][ident];values=proof['healthy'][index]
        require(set(values)=={'native','worker_receipt','summary','checkpoint'} and old['job_id']==job and old['status'] in {'submitted','complete'},
            'Healthy original job must remain submitted or proven complete')
        positive,native_pin=packet(values['native'],expected=CASE/'native'/('worker-'+job+'.json'));native(positive,job,cpus=6,gpus=1,complete=True)
        require(positive==native_values[job],'Healthy native proof differs from frozen UTC evidence')
        worker,worker_pin=packet(values['worker_receipt'],expected=ROOT/'maintenance/study-af-campaign-v1-20261021'/('native-'+job)/'receipt.json')
        summary,summary_pin=packet(values['summary'],expected=PurePosixPath(cell['training_case'])/'summary.json')
        _,checkpoint_pin=packet(values['checkpoint'],expected=PurePosixPath(cell['training_case'])/'checkpoint.pt',encoding='binary',maximum=100_000_000)
        require(worker.get('status')=='complete_native_work' and worker.get('mode')=='agent-train' and worker.get('index')==int(index)
            and worker.get('native_job_id')==job and worker.get('descriptor_sha256')==WORKER_DESCRIPTOR_SHA
            and worker.get('source_sha256')==SOURCE and worker.get('full_config_sha256')==CONFIG and worker.get('resource_policy')==plan['policy_binding']
            and worker.get('training_summary_path')==summary_pin['path'] and worker.get('training_summary_sha256')==summary_pin['sha256']
            and worker.get('training_summary')==summary and summary.get('checkpoint_sha256')==checkpoint_pin['sha256']
            and worker.get('scientific_result') is False and worker.get('admission_authority') is False,'Healthy original worker/summary/checkpoint bytes changed')
        allocation=worker.get('allocation',{})
        require(allocation.get('JobId')==job and allocation.get('UserId')=='pwa209(3164768)' and allocation.get('Account')=='def-ptewarie_gpu'
            and allocation.get('NumCPUs')=='6','Healthy native allocation and worker proof disagree')
        receipt={'campaign_plan_sha256':plan['plan_sha256'],'intent_id':ident,'native_job_id':job,'case':cell['training_case'],
            'sha256':worker_pin['sha256'],'value':summary}
        dispatch._training(plan,old,receipt)
        if old['status']=='complete':
            require(old.get('receipt')==receipt,'Historical positive healthy worker receipt changed')
            if clock_only_change(old.get('native_proof'),positive,job,erratum):corrected[job]={'old':copy.deepcopy(old['native_proof']),'new':copy.deepcopy(positive)}
        pins['healthy'][index]={'native':native_pin,'worker':worker_pin,'summary':summary_pin,'checkpoint':checkpoint_pin}
    require(set(erratum.get('jobs',{}))==set(corrected),'Unused or omitted clock corrections are refused')
    candidate=copy.deepcopy(state);del candidate['jobs'][TARGET];candidate['cells'][0]['training_job']=None
    for index,job in HEALTHY.items():
        if job in corrected:candidate['jobs'][f'training-{int(index):04d}']['native_proof']=copy.deepcopy(corrected[job]['new'])
    candidate['needs_repair']=[];candidate['status']='active';candidate=dispatch._finish(candidate)
    dispatch._state(plan,candidate)
    expected_jobs={key:copy.deepcopy(value) for key,value in state['jobs'].items() if key!=TARGET}
    for index,job in HEALTHY.items():
        if job in corrected:expected_jobs[f'training-{int(index):04d}']['native_proof']=copy.deepcopy(corrected[job]['new'])
    require(candidate['revision']==state['revision']+1 and candidate['cells'][1:]==state['cells'][1:]
        and candidate['jobs']==expected_jobs
        and {key:value for key,value in candidate['cells'][0].items() if key!='training_job'}==
            {key:value for key,value in state['cells'][0].items() if key!='training_job'},'Only failed active reference and proven clock errata may change')
    amendment={'schema_version':1,'status':'validated_target_only_state_derivative_not_activated','case':str(CASE),
        'kind':'explicit-prestart-training0000-operational-recovery-v1','failed_native_job_id':FAILED,'recovered_intent_id':TARGET,
        'native_namespace_overrides':{TARGET:NAMESPACE},'submission_journal_overrides':{TARGET:'submission-r1-training-0000.json'},
        'original_state_sha256':state['state_sha256'],'new_state_sha256':candidate['state_sha256'],'original_revision':state['revision'],
        'new_revision':candidate['revision'],'campaign_plan_sha256':plan['plan_sha256'],'failed_original_native_job':FAILED,
        'failed_history_preserved':copy.deepcopy(failed_job),'source_pins':pins,'retry_namespace_mapping':mapping,
        'native_clock_errata_preserving_original_proofs':corrected,
        'approval':copy.deepcopy(approval),'dispatcher_sha256':DISPATCH_SHA,'source_sha256':SOURCE,'full_config_sha256':CONFIG,
        'preserved_healthy_native_jobs':dict(HEALTHY),'healthy_policies_retrained':False,'original_native_failure_relabelled':False,
        'native_submission_performed':False,'source_files_changed_or_deleted':False,'scientific_result':False,'admission_authority':False,
        'activation_requires':['Immutable original-byte preservation','Same kernel registration lock held through state compare-and-swap',
            'Fresh recheck of original state fileSHA and all absence proofs','Separate source-bound held native submission with one explicit target namespace',
            'No ambiguous replay; no healthy retraining; all other plan/ledger/numerical behavior unchanged']}
    amendment['amendment_sha256']=canonical_hash(amendment)
    return {'state':candidate,'amendment':amendment}
