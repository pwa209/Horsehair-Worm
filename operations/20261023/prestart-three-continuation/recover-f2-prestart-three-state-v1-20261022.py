"""Pure, exact revision405 recovery for three registry-read prestart failures.

No files, locks, scheduler, checkpoints or models are read or changed here.
Every file proof is {path, sha256, raw: bytes}; the native caller supplies those
actual bytes, freezes them, and independently verifies the permanent mutex and
compare-and-swap. A byte-bound absence packet documents a fresh inventory under
that mutex. Its validation_unix must be within30s of captured_unix; this pure
function cannot establish the caller's wall clock or kernel lock ownership.

The three old records remain in the sealed amendment. Only their active job
records/references are removed, allowing the unchanged dispatcher to regenerate
the identical intents. All other216 records, cell fields and gates are preserved.
"""
from datetime import datetime
from pathlib import PurePosixPath
import copy
import gzip
import hashlib
import io
import json
import math
import re
import shlex

from horsehair import agent_campaign as dispatch
from horsehair.core import canonical_hash

ROOT=PurePosixPath('/scratch/pwa209/horsehair-worm')
CASE=ROOT/'maintenance/study-af-prestart-three-recovery-v1-20261022'
CAMPAIGN=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
MUTEX=ROOT/'state/af-campaign-registration.mutex'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PRIMARY='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
DISPATCH_SHA='70f28e57f0e4a2eee3ebfef2699e751569fe048b550a94fa36a5461269493070'
WORKER_SHA='f00d4265a68a3ace5faa47da4fe2701fff3fe86d9132b09ea748e9f12471e4fd'
WORKER_DESCRIPTOR=ROOT/'maintenance/study-af-campaign-v1-20261021/precision-and-f2-continuation-v2/descriptor-v2-worker.json'
WORKER_DESCRIPTOR_SHA='a56965b46c09cdffa4512302dea87ca895372f7668f88748ac2263c4b9a2c6b2'
OLD_SHIM=ROOT/'maintenance/study-af-registry-repair-support-v4-20261021/run-f00d-worker-registry-overlay-20261021.py'
OLD_GUARD=ROOT/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'
OLD_GUARD_SHA='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'
OLD_RECOVERY=ROOT/'maintenance/study-af-training0000-state-repair-v1-20261021/amendment.json'
OLD_RECOVERY_SHA='41bebed391a755ead5edee1bdcb56a203d713e533ca2d3d52c4cf2e6ae37b46b'
EXPECTED_STATE_REVISION=405
EXPECTED_STATE_SHA256='80c8d048550f4e0e606ff323e2ce99af177b92cfe6c43144956c3c2b852f792b'
EXPECTED_STATE_JOBS=219
TARGETS={'evaluation-0073-01':'6795585','training-0076':'6795113','training-0077':'6795225'}
CONTROLLERS={'6701493','6701494','6701495','6701498'}
FAILURE_MARKER='ValueError: Native registry-read deadline/margin exhausted'
NATIVE_KEYS={'job_id','state','exit_code','cpus','gpus','elapsed_seconds','end','user','account'}


def require(value,message):
    if not value:raise ValueError(message)


def raw_sha(raw):return hashlib.sha256(raw).hexdigest()


def sha(value):return isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) is not None


def path(value):
    require(isinstance(value,str) and PurePosixPath(value).is_absolute()
        and str(PurePosixPath(value))==value and not {'.','..'}&set(value.split('/'))
        and PurePosixPath(value).is_relative_to(ROOT) and PurePosixPath(value)!=ROOT,
        'Exact personal POSIX proof path required')
    return PurePosixPath(value)


def packet(value,*,expected,encoding='json',maximum=64_000_000,expanded_maximum=512_000_000):
    require(isinstance(value,dict) and set(value)=={'path','sha256','raw'}
        and type(value['raw']) is bytes and len(value['raw'])<=maximum
        and sha(value['sha256']) and raw_sha(value['raw'])==value['sha256'],
        'Exact bounded byte-bound proof packet required')
    require(path(value['path'])==expected,'Proof packet belongs to a different original file')
    raw=value['raw']
    if encoding=='gzip-json':
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:raw=stream.read(expanded_maximum+1)
        require(len(raw)<=expanded_maximum,'Bounded original compressed state required')
    if encoding in {'json','gzip-json'}:
        def unique(pairs):
            result={}
            for key,item in pairs:
                require(key not in result,'Duplicate JSON proof key refused');result[key]=item
            return result
        def constant(value):raise ValueError('Nonfinite JSON proof constant refused')
        result=json.loads(raw,object_pairs_hook=unique,parse_constant=constant)
        # parse_float also rejects finite-looking JSON exponents that overflow.
        canonical_hash(result)
    elif encoding=='text':result=raw.decode('utf8')
    else:raise ValueError('Unknown proof encoding')
    return result,{'path':value['path'],'sha256':value['sha256'],'bytes':len(value['raw']),
        'parsed_value_sha256':canonical_hash(result)}


def native(value,job,*,cpus,gpus,producer=False,controller=False):
    require(isinstance(value,dict) and set(value)==NATIVE_KEYS
        and value['job_id']==job and isinstance(job,str) and re.fullmatch('[0-9]+',job)
        and value['user']=='pwa209' and value['account']==('def-ptewarie_gpu' if gpus else 'def-ptewarie_cpu')
        and type(value['cpus']) is int and type(value['gpus']) is int
        and type(value['elapsed_seconds']) is int and value['elapsed_seconds']>=0
        and isinstance(value['exit_code'],str) and re.fullmatch('[0-9]+:[0-9]+',value['exit_code']),
        'Strict owned scalar native proof required')
    unallocated=controller and value['state']=='CANCELLED' and value['elapsed_seconds']==0
    require((value['cpus']==value['gpus']==0) if unallocated else
        (value['cpus']==cpus and value['gpus']==gpus and (producer or value['elapsed_seconds']>0)),
        'Actual native resources differ')
    if producer:
        require(value['state']=='RUNNING' and value['exit_code']=='0:0' and value['end']=='Unknown',
            'Actual current native producer must be running')
    else:
        require(value['state'] in dispatch.TERMINAL_FAILURES|{'COMPLETED'},'Every old allocation must be terminal')
        require(isinstance(value['end'],str) and value['end']!='Unknown','Exact terminal native end required')
        datetime.fromisoformat(value['end'])
        if not controller:require(value['state']=='FAILED' and value['exit_code']=='1:0','Original worker FAILED/1:0 required')


def retry_maps():
    return ({ident:'af-f2-r2-'+ident for ident in TARGETS},
        {ident:'submission-r2-'+ident+'.json' for ident in TARGETS})


def absence_paths(plan):
    namespaces,journals=retry_maps()
    result={str(PurePosixPath(plan['cells'][index]['training_case'])) for index in (76,77)}
    for ident,job in TARGETS.items():
        result.update({str(CAMPAIGN.parent/('native-'+job)/'receipt.json'),str(ROOT/'plans'/namespaces[ident]),
            str(ROOT/'state'/(namespaces[ident]+'-submission.json')),str(CAMPAIGN/journals[ident])})
    return result


def validate_failed(plan,state,ident,evidence,pins):
    job=TARGETS[ident];old=state['jobs'][ident];intent=old['intent'];kind=intent['kind'];index=intent['cell_index']
    expected_keys={'native','native_plan','submission_ledger','script','submission_journal','traceback'}
    if kind=='evaluation':expected_keys|={'evaluation_plan','evaluation_plan_binding'}
    require(isinstance(evidence,dict) and set(evidence)==expected_keys,'Complete exact original failed allocation evidence required')
    observed,pins['native']=packet(evidence['native'],expected=CASE/'native'/('worker-'+job+'.json'))
    native(observed,job,cpus=intent['resources']['cpus'],gpus=intent['resources']['gpus'])
    if old['native_proof']['state'] in dispatch.TERMINAL_FAILURES|{'COMPLETED'}:
        require(old['native_proof']==observed,'Historical terminal native proof cannot change')
    else:
        require(ident=='training-0077' and old['status']=='submitted' and old['native_proof']['state']=='RUNNING'
            and all(old['native_proof'][k]==observed[k] for k in ('job_id','cpus','gpus','user','account'))
            and old['native_proof']['elapsed_seconds']<=observed['elapsed_seconds'],
            'Only exact third running proof may advance to terminal failure')
    name='af-f2-'+ident;directory=ROOT/'plans'/name
    original,pins['native_plan']=packet(evidence['native_plan'],expected=directory/'plan.json')
    ledger,pins['submission_ledger']=packet(evidence['submission_ledger'],expected=ROOT/'state'/(name+'-submission.json'))
    script,pins['script']=packet(evidence['script'],expected=directory/(name+'.sbatch'),encoding='text')
    journal,pins['submission_journal']=packet(evidence['submission_journal'],expected=CAMPAIGN/('submission-'+ident+'.json'))
    require(original.get('root')==str(ROOT) and original.get('phase')==name and original.get('config_sha256')==CONFIG
        and original.get('release_identity',{}).get('sha256')==SOURCE and original.get('campaign_plan_sha256')==plan['plan_sha256']
        and original.get('intent_sha256')==intent['intent_sha256'] and original.get('worker_descriptor_sha256')==WORKER_DESCRIPTOR_SHA
        and sha(original.get('controller_descriptor_sha256')) and len(original.get('tasks',[]))==1,
        'Original native plan/source/intention changed')
    task=original['tasks'][0];contract={'cpus':intent['resources']['cpus'],'gpu':bool(intent['resources']['gpus']),
        'hours':12,'memory':'64G' if kind=='training' else '32G'}
    require(set(ledger.get('tasks',{}))=={name},'Exactly one original registered worker required')
    registered=ledger['tasks'][name]
    require(task.get('name')==name and task.get('script')==evidence['script']['path']
        and task.get('script_sha256')==canonical_hash(script) and task.get('resources')==contract
        and task.get('depends')==[] and task.get('external_dependencies')==[] and task.get('dependency_kind')=='afterok'
        and ledger.get('config_sha256')==CONFIG and ledger.get('source_sha256')==SOURCE
        and ledger.get('plan_sha256')==canonical_hash(original) and registered.get('job_id')==job
        and registered.get('state')=='submitted' and registered.get('script_sha256')==task['script_sha256']
        and registered.get('command')==['sbatch','--parsable',evidence['script']['path']],
        'Original script/ledger/resources no longer agree')
    argv=['python',str(OLD_SHIM),'--registry-read-guard',str(OLD_GUARD),'--registry-read-guard-sha256',OLD_GUARD_SHA,
        '--descriptor',str(WORKER_DESCRIPTOR),'--descriptor-sha256',WORKER_DESCRIPTOR_SHA,
        '--mode','agent-train' if kind=='training' else 'agent-evaluation','--index',str(index),
        '--cpus',str(intent['resources']['cpus']),'--gpus',str(intent['resources']['gpus']),'--hours','12']
    if kind=='evaluation':
        binding=state['cells'][index]['evaluation_plan'];case=PurePosixPath(binding['case'])
        argv+=['--evaluation-case',binding['case'],'--evaluation-plan-sha256',binding['plan_sha256'],
            '--evaluation-binding-sha256',binding['plan_binding_sha256']]
        evaluation,pins['evaluation_plan']=packet(evidence['evaluation_plan'],expected=case/'plan.json')
        activation,pins['evaluation_plan_binding']=packet(evidence['evaluation_plan_binding'],expected=case/'plan-binding.json')
        require(evidence['evaluation_plan']['sha256']==binding['plan_sha256'] and evaluation==binding['value']
            and evidence['evaluation_plan_binding']['sha256']==binding['plan_binding_sha256']
            and activation.get('plan_sha256')==binding['plan_sha256'], 'Original evaluation plan binding changed')
    require(shlex.split(task.get('command',''))==argv and task['command'] in script and '#SBATCH --hold' in script,
        'Original worker/index/source command changed')
    require(journal.get('status')=='released' and journal.get('job_id')==job and journal.get('intent')==intent
        and journal.get('plan_path')==evidence['native_plan']['path'] and journal.get('plan_sha256')==evidence['native_plan']['sha256']
        and journal.get('ledger_path')==evidence['submission_ledger']['path']
        and journal.get('scientific_result') is False and journal.get('admission_authority') is False
        and type(journal.get('released_unix')) in (float,int) and math.isfinite(journal['released_unix']) and journal['released_unix']>0,
        'Exact positively released original submission journal required')
    held=journal.get('held_allocation',{})
    require(held.get('JobId')==job and held.get('UserId')=='pwa209(3164768)' and held.get('Account')==observed['account']
        and held.get('JobState')=='PENDING' and held.get('Reason')=='JobHeldUser' and held.get('JobName')=='hhw-'+name
        and held.get('NumCPUs')==str(intent['resources']['cpus']) and held.get('Command')==evidence['script']['path']
        and held.get('MinMemoryNode')==contract['memory'] and held.get('TimeLimit')=='12:00:00',
        'Original held allocation differs from worker intention')
    log,pins['traceback']=packet(evidence['traceback'],expected=ROOT/'logs'/(name+'-'+job+'_4294967294.log'),encoding='text')
    require(FAILURE_MARKER in log and 'native-start-admission' in log and 'all_registered_work' in log
        and 'AF_NATIVE_WORK ' not in log,'Exact before-computation registry-read failure required')
    return copy.deepcopy(observed)


def recover(plan,state,proof):
    """Return {'state', 'amendment'} without I/O; caller alone owns activation.

    Required proof keys are checked below. producer is a byte packet with
    {native: normalized scalar allocation, source_sha256, config_sha256}.
    absence is a packet at CASE/verified-absence-under-lock.json containing
    registration_mutex, registration_mutex_held, mutex_device, mutex_inode,
    same_lock_held_through_validation, captured_original_state_sha256,
    captured_original_state_file_sha256, captured_original_revision,
    native_campaign_controller_ids, captured_unix, validation_unix, entries,
    and evaluation_top_level. entries are exact absent path/exists/is_symlink
    records; evaluation_top_level lists the complete two ordinary plan files
    using path/sha256/is_file/is_dir/is_symlink. No partial output is accepted.
    """
    require(isinstance(proof,dict) and set(proof)=={'case','dispatcher_sha256','approval','original_state_gzip',
        'original_state_binding','original_campaign_plan','worker_descriptor','failed','old_controllers','absence','old_recovery','producer'},
        'Exact three-target proof schema required')
    require(proof['case']==str(CASE) and proof['dispatcher_sha256']==DISPATCH_SHA,'Exact case and unchanged dispatcher required')
    dispatch._state(plan,state)
    require(state['revision']==EXPECTED_STATE_REVISION and state['state_sha256']==EXPECTED_STATE_SHA256
        and len(state['jobs'])==EXPECTED_STATE_JOBS and state['status']=='needs_repair'
        and state['needs_repair']==['evaluation-0073-01','training-0076'], 'Only exact original revision405 state may recover')
    require(plan['case']==str(CAMPAIGN) and plan['source_binding']['execution_source_sha256']==SOURCE
        and plan['source_binding']['primary_source_sha256']==PRIMARY and canonical_hash(plan['config'])==CONFIG
        and state['scientific_result'] is False and state['admission_authority'] is False
        and state['H4_inference_status']==plan['H4_inference_status'], 'Original source/configuration/inference gates required')
    require(sum(j['status']=='complete' and j['intent']['kind']=='training' for j in state['jobs'].values())==76
        and sum(c['complete'] for c in state['cells'])==67, 'Original positive completion counters changed')
    require(all(j['job_id'] is not None and j['status']!='ambiguous' for j in state['jobs'].values()),
        'Ambiguous submissions require reconciliation, never retry')
    for ident,job in TARGETS.items():
        old=state['jobs'].get(ident,{})
        require(old.get('job_id')==job and old.get('receipt') is None and 'failure_receipt' not in old
            and old.get('status')==('submitted' if ident=='training-0077' else 'needs_repair'), 'Exact original target status required')
    for index in (76,77):
        cell=state['cells'][index]
        require(cell['training_job']==f'training-{index:04d}' and cell['evaluation_plan'] is None
            and cell['evaluation_jobs']==[] and cell['assembly_job'] is None and cell['conditions']=={} and cell['complete'] is False,
            'Unstarted training cannot have downstream work')
    cell=state['cells'][73]
    require(cell['evaluation_jobs']==['evaluation-0073-01'] and cell['conditions']=={}
        and cell['assembly_job'] is None and cell['complete'] is False and cell['evaluation_plan'] is not None,
        'Only empty first evaluation wave may retry')
    pins={}
    original,pins['original_state_gzip']=packet(proof['original_state_gzip'],expected=CASE/'original-state.json.gz',encoding='gzip-json')
    require(original==state,'Frozen original compressed state differs')
    binding,pins['original_state_binding']=packet(proof['original_state_binding'],expected=CASE/'original-state-binding.json')
    require(binding.get('state_file_sha256')==proof['original_state_gzip']['sha256'] and binding.get('state_sha256')==state['state_sha256']
        and binding.get('campaign_plan_sha256')==plan['plan_sha256'] and binding.get('revision')==state['revision']
        and binding.get('scientific_result') is False and binding.get('admission_authority') is False,
        'Original compressed state/binding join changed')
    frozen_plan,pins['original_campaign_plan']=packet(proof['original_campaign_plan'],expected=CASE/'original-campaign-plan.json')
    require(frozen_plan==plan,'Frozen original campaign plan differs')
    approval,pins['approval']=packet(proof['approval'],expected=CASE/'current-user-authorization.json')
    require(approval.get('source')=='current_user' and approval.get('authorizes_targeted_operational_repair') is True
        and approval.get('targets')==TARGETS and isinstance(approval.get('request_reference'),str)
        and approval['request_reference'].strip(), 'Exact byte-bound current-user authorization required')
    producer,pins['producer']=packet(proof['producer'],expected=CASE/'native/producer.json')
    require(set(producer)=={'native','source_sha256','config_sha256'} and producer['source_sha256']==SOURCE
        and producer['config_sha256']==CONFIG,'Exact source-bound activation producer required')
    native(producer['native'],producer['native'].get('job_id'),cpus=4,gpus=0,producer=True)
    require(producer['native']['job_id'] not in CONTROLLERS|set(TARGETS.values()), 'Activation producer cannot be an old allocation')
    descriptor,pins['worker_descriptor']=packet(proof['worker_descriptor'],expected=WORKER_DESCRIPTOR)
    require(proof['worker_descriptor']['sha256']==WORKER_DESCRIPTOR_SHA and descriptor.get('worker_sha256')==WORKER_SHA
        and descriptor.get('execution_source_sha256')==SOURCE and descriptor.get('agent_campaign_case')==str(CAMPAIGN)
        and descriptor.get('resource_policy',{}).get('sha256')==plan['policy_binding']['sha256']
        and descriptor.get('frozen_protocol_sha256')==plan['protocol_binding']['sha256'], 'Original worker descriptor changed')
    require(set(proof['old_controllers'])==CONTROLLERS,'All four exact old controllers must be terminal')
    pins['old_controllers']={}
    for job,value in proof['old_controllers'].items():
        record,pins['old_controllers'][job]=packet(value,expected=CASE/'native'/('controller-'+job+'.json'))
        native(record,job,cpus=1,gpus=0,controller=True)
    require(set(proof['failed'])==set(TARGETS),'Exactly the three known targets required')
    observed={};pins['failed']={}
    for ident,evidence in proof['failed'].items():
        pins['failed'][ident]={};observed[ident]=validate_failed(plan,state,ident,evidence,pins['failed'][ident])
    old_recovery=proof['old_recovery']
    require(isinstance(old_recovery,dict) and set(old_recovery)=={'path','sha256'} and path(old_recovery['path'])==OLD_RECOVERY
        and old_recovery['sha256']==OLD_RECOVERY_SHA, 'Original training0000 amendment locator required; caller verifies its chain')
    absent,pins['absence']=packet(proof['absence'],expected=CASE/'verified-absence-under-lock.json')
    require(set(absent)=={'registration_mutex','registration_mutex_held','mutex_device','mutex_inode','same_lock_held_through_validation',
        'captured_original_state_sha256','captured_original_state_file_sha256','captured_original_revision',
        'native_campaign_controller_ids','captured_unix','validation_unix','entries','evaluation_top_level'},'Exact lock-bound absence inventory required')
    require(absent['registration_mutex']==str(MUTEX) and absent['registration_mutex_held'] is True
        and absent['same_lock_held_through_validation'] is True and type(absent['mutex_device']) is int and absent['mutex_device']>=0
        and type(absent['mutex_inode']) is int and absent['mutex_inode']>0
        and absent['captured_original_state_sha256']==state['state_sha256']
        and absent['captured_original_state_file_sha256']==proof['original_state_gzip']['sha256']
        and absent['captured_original_revision']==state['revision'] and absent['native_campaign_controller_ids']==sorted(CONTROLLERS),
        'Same permanent mutex and exact original state must bind fresh absence')
    require(all(type(absent[k]) in (float,int) and math.isfinite(absent[k]) and absent[k]>0 for k in ('captured_unix','validation_unix'))
        and 0<=absent['validation_unix']-absent['captured_unix']<=30, 'Stale or future absence inventory refused')
    entries=absent['entries'];expected=absence_paths(plan)
    require(isinstance(entries,list) and len(entries)==len(expected) and {e.get('path') for e in entries}==expected
        and all(set(e)=={'path','exists','is_symlink'} and e['exists'] is False and e['is_symlink'] is False for e in entries),
        'Actual training directories/worker receipts/new retry artifacts must be absent')
    epins=pins['failed']['evaluation-0073-01'];top=absent['evaluation_top_level']
    expected_top={epins[k]['path']:epins[k]['sha256'] for k in ('evaluation_plan','evaluation_plan_binding')}
    require(isinstance(top,list) and len(top)==2 and {e.get('path') for e in top}==set(expected_top)
        and all(set(e)=={'path','sha256','is_file','is_dir','is_symlink'} and e['sha256']==expected_top[e['path']]
            and e['is_file'] is True and e['is_dir'] is False and e['is_symlink'] is False for e in top),
        'Complete evaluation inventory must contain only the unchanged two plan files')
    candidate=copy.deepcopy(state)
    for ident in TARGETS:del candidate['jobs'][ident]
    candidate['cells'][76]['training_job']=None;candidate['cells'][77]['training_job']=None
    candidate['cells'][73]['evaluation_jobs']=[]
    candidate['needs_repair']=[];candidate['status']='active';candidate=dispatch._finish(candidate)
    dispatch._state(plan,candidate)
    regenerated={ident:dispatch._intent(plan,candidate['cells'][state['jobs'][ident]['intent']['cell_index']],
        state['jobs'][ident]['intent']['kind'],state['jobs'][ident]['intent']['wave']) for ident in TARGETS}
    require(all(regenerated[ident]==state['jobs'][ident]['intent'] for ident in TARGETS),'Retry must regenerate every exact frozen intention')
    expected_state=copy.deepcopy(state)
    expected_state['jobs']={ident:job for ident,job in expected_state['jobs'].items() if ident not in TARGETS}
    expected_state['cells'][76]['training_job']=None;expected_state['cells'][77]['training_job']=None
    expected_state['cells'][73]['evaluation_jobs']=[];expected_state['needs_repair']=[];expected_state['status']='active'
    # Independent preservation check: do not invoke the dispatcher mutator a
    # second time to construct the expected result of its own first call.
    expected_state.pop('state_sha256');expected_state['revision']+=1
    expected_state['state_sha256']=canonical_hash(expected_state)
    require(candidate==expected_state,'Unrelated state field changed during recovery')
    namespaces,journals=retry_maps()
    amendment={'schema_version':1,'kind':'explicit-prestart-three-operational-recovery-v1',
        'status':'validated_target_only_state_derivative_not_activated','case':str(CASE),'targets':dict(TARGETS),
        'native_namespace_overrides':namespaces,'submission_journal_overrides':journals,'old_recovery':copy.deepcopy(old_recovery),
        'original_revision':state['revision'],'new_revision':candidate['revision'],'original_state_sha256':state['state_sha256'],
        'new_state_sha256':candidate['state_sha256'],'campaign_plan_sha256':plan['plan_sha256'],
        'source_sha256':SOURCE,'config_sha256':CONFIG,'dispatcher_sha256':DISPATCH_SHA,
        'activation_producer_job_id':producer['native']['job_id'],'original_jobs_preserved':len(state['jobs']),
        'preserved_unrelated_jobs':len(candidate['jobs']),'failed_history_preserved':{ident:copy.deepcopy(state['jobs'][ident]) for ident in TARGETS},
        'observed_failed_native':observed,'regenerated_intents':regenerated,'source_pins':pins,
        'native_submission_performed':False,'source_files_changed_or_deleted':False,'healthy_policies_retrained':False,
        'original_native_failures_relabelled':False,'scientific_result':False,'admission_authority':False,
        'activation_requires':['Immutable original bytes and all source packets retained',
            'Caller verifies same permanent kernel mutex and source guard through paired state/binding compare-and-swap',
            'Caller repeats fresh target output and retry artifact absence before activation',
            'Separate held native submissions preserve exact regenerated intentions and immutable old attempts',
            'Original training0000 recovery chain independently verified; ambiguous submissions never replayed']}
    amendment['amendment_sha256']=canonical_hash(amendment)
    return {'state':candidate,'amendment':amendment}
