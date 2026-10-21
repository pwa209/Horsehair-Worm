"""Pure, finite intentions for the original 240 final computational policies.

This module never reads a filesystem, submits a job, or verifies a provider.
The native caller verifies source files, qualification, policy descriptors,
condition artifacts and accounting, serializes state changes, persists each
intention before submission, and positively acknowledges its unique native ID.
An unacknowledged intention is reserved ambiguous work, never an automatic retry.
"""
from __future__ import annotations

import copy
from pathlib import PurePosixPath
import re

from . import agents, agent_execution_v2 as evaluation
from .core import canonical_hash
from .execution_resources import BASE_CONFIG_SHA256, PRIMARY_SCIENTIFIC_SOURCE_SHA256, LIMITS


ROOT = PurePosixPath('/scratch/pwa209/horsehair-worm')
TERMINAL_FAILURES = {'FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
BOUNDS = {'training': {'cpus':6,'gpus':1,'hours':12},
          'evaluation': {'cpus':4,'gpus':0,'hours':12},
          'assembly': {'cpus':1,'gpus':0,'hours':1}}
WINDOW = {'training':4,'evaluation':32,'assembly':32,'queued':64,'evaluation_waves_per_policy':12}


def _require(value, message):
    if not value: raise ValueError(message)


def _sha(value):
    return isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) is not None


def _path(value, namespace):
    _require(isinstance(value,str) and PurePosixPath(value).is_absolute()
             and '..' not in value.split('/') and '.' not in value.split('/')
             and str(PurePosixPath(value))==value and PurePosixPath(value).is_relative_to(namespace)
             and PurePosixPath(value)!=namespace, 'Exact personal campaign path required')
    return value


def _sealed(value, key):
    value[key]=canonical_hash(value)
    return value


def plan_campaign(config, protocol, *, protocol_binding, source_binding, policy_binding, case):
    """Freeze source metadata and all original final indices; grant no admission."""
    config,protocol,protocol_binding,source_binding,policy_binding=copy.deepcopy(
        (config,protocol,protocol_binding,source_binding,policy_binding))
    _require(canonical_hash(config)==BASE_CONFIG_SHA256 and config['paths']['root']==str(ROOT),
             'Original complete scientific configuration required')
    keys={'primary_source_sha256','primary_agents_sha256','execution_source_sha256','evaluation_module_sha256'}
    _require(set(source_binding)==keys and all(_sha(v) for v in source_binding.values())
             and source_binding['primary_source_sha256']==PRIMARY_SCIENTIFIC_SOURCE_SHA256,
             'Exact original and qualified prospective source bindings required')
    settings=config['agents']
    _require(protocol.get('config_sha256')==agents._digest(settings)
             and protocol.get('full_config_sha256')==BASE_CONFIG_SHA256
             and protocol.get('implementation_sha256')==source_binding['primary_agents_sha256']
             and protocol.get('H4_inference_specification')==agents.h4_specification(config)
             and protocol.get('learning_rate') in settings['lr_grid']
             and protocol.get('final_seeds')==settings['final_seeds']
             and protocol.get('biological_directions') is None, 'Unchanged original synthetic frozen protocol required')
    _require(set(protocol_binding)=={'path','sha256'} and _sha(protocol_binding['sha256']),
             'Original protocol file locator and SHA required')
    _path(protocol_binding['path'],ROOT)
    _require(policy_binding.get('base_config_sha256')==BASE_CONFIG_SHA256
             and policy_binding.get('primary_scientific_source_sha256')==PRIMARY_SCIENTIFIC_SOURCE_SHA256
             and policy_binding.get('execution_source_sha256')==source_binding['execution_source_sha256']
             and policy_binding.get('limits')==LIMITS and _sha(policy_binding.get('sha256'))
             and policy_binding.get('scientific_gates_changed') is False,
             'Explicit source-bound expanded-resource descriptor metadata required')
    _path(policy_binding['path'],ROOT/'maintenance/execution-resource-policies')
    _require(policy_binding.get('approval',{}).get('source')=='current_user'
             and policy_binding['approval'].get('authorizes_cpu_gpu_resource_amendment') is True,
             'Explicit human resource-authorization provenance required')
    case=_path(case,ROOT/'maintenance')
    cells=evaluation.final_cells(config)
    _require(len(cells)==240 and len(settings['final_seeds'])==20,
             'Original240 policy cells and twenty final seeds required')
    for cell in cells:
        name=agents._cell_name(cell['task'],cell['architecture'],cell['seed'])
        cell.update(training_case=str(ROOT/settings['output_dir']/'agents'/'final'/name),
                    evaluation_case=str(PurePosixPath(case)/f"cell-{cell['index']:04d}"))
    return _sealed({'schema_version':1,'case':case,'config':config,'protocol':protocol,
        'protocol_binding':protocol_binding,'protocol_sha256':agents._digest(protocol),
        'source_binding':source_binding,'policy_binding':policy_binding,'cells':cells,
        'conditions':evaluation._conditions(settings,protocol),'bounds':copy.deepcopy(BOUNDS),
        'window':dict(WINDOW),'scientific_result':False,'admission_authority':False,
        'H4_inference_status':'pending_separate_validated_analysis_and_bridge'},'plan_sha256')


def _plan(plan):
    expected=plan_campaign(plan['config'],plan['protocol'],protocol_binding=plan['protocol_binding'],
        source_binding=plan['source_binding'],policy_binding=plan['policy_binding'],case=plan['case'])
    _require(plan==expected,'Immutable campaign plan changed')


def initial_state(plan):
    _plan(plan)
    return _sealed({'schema_version':1,'plan_sha256':plan['plan_sha256'],'revision':0,
        'status':'active','jobs':{},'cells':[{'index':c['index'],'training_job':None,
        'evaluation_plan':None,'evaluation_jobs':[],'assembly_job':None,'conditions':{},'complete':False}
        for c in plan['cells']],'needs_repair':[],'scientific_result':False,'admission_authority':False,
        'H4_inference_status':'pending_separate_validated_analysis_and_bridge'},'state_sha256')


def _intent(plan, cell, kind, wave=0):
    original=plan['cells'][cell['index']]
    ident=f"{kind}-{cell['index']:04d}"+(f'-{wave:02d}' if kind=='evaluation' else '')
    value={'intent_id':ident,'kind':kind,'cell_index':cell['index'],'wave':wave,
        'campaign_plan_sha256':plan['plan_sha256'],'task':original['task'],
        'architecture':original['architecture'],'seed':original['seed'],
        'case':original['training_case'] if kind=='training' else original['evaluation_case'],
        'resources':dict(BOUNDS[kind]),'scientific_result':False,'admission_authority':False}
    if kind!='training':
        _require(cell['evaluation_plan'] is not None,'Bind the completed checkpoint evaluation plan first')
        value['evaluation_plan_sha256']=cell['evaluation_plan']['plan_sha256']
    if kind=='evaluation':
        value.update(max_conditions=len(plan['conditions']),max_wall_seconds=12*3600-300,
                     previous_completed_conditions=len(cell['conditions']))
    return _sealed(value,'intent_sha256')


def _state(plan,state):
    _plan(plan)
    raw={k:v for k,v in state.items() if k!='state_sha256'}
    _require(state.get('state_sha256')==canonical_hash(raw)
             and state.get('plan_sha256')==plan['plan_sha256'] and state.get('schema_version')==1
             and type(state.get('revision')) is int and state['revision']>=0
             and len(state['cells'])==240 and [c['index'] for c in state['cells']]==list(range(240)),
             'Campaign state/plan identity changed')
    references=[];native=[]
    for cell in state['cells']:
        references += [v for v in [cell['training_job'],*cell['evaluation_jobs'],cell['assembly_job']] if v is not None]
        _require(len(cell['evaluation_jobs'])<=WINDOW['evaluation_waves_per_policy'],'Evaluation wave cap exceeded')
        original=plan['cells'][cell['index']]
        for kind,ident,wave in [('training',cell['training_job'],0),('assembly',cell['assembly_job'],0),
                *[('evaluation',ident,index+1) for index,ident in enumerate(cell['evaluation_jobs'])]]:
            if ident is None:continue
            intent=state['jobs'][ident]['intent']
            _require(intent['kind']==kind and intent['cell_index']==cell['index'] and intent['wave']==wave
                     and all(intent[k]==original[k] for k in ('task','architecture','seed'))
                     and intent['case']==original['training_case' if kind=='training' else 'evaluation_case'],
                     'Intention changed original immutable final index or phase')
    _require(len(references)==len(set(references)) and set(references)==set(state['jobs']),
             'Campaign job references are duplicated or unbound')
    for ident,job in state['jobs'].items():
        intent=job['intent'];raw={k:v for k,v in intent.items() if k!='intent_sha256'}
        _require(intent['intent_id']==ident and canonical_hash(raw)==intent['intent_sha256']
                 and intent['campaign_plan_sha256']==plan['plan_sha256'] and intent['resources']==BOUNDS[intent['kind']],
                 'A campaign intention changed its frozen plan/resource binding')
        if job['job_id'] is not None:
            _require(re.fullmatch(r'[0-9]+(?:_[0-9]+)?',job['job_id']) is not None,'Invalid native job identity')
            native.append(job['job_id'])
        _require(job['status'] in {'ambiguous','submitted','complete','needs_repair'}, 'Unknown campaign job status')
        if job['status']=='complete':
            proof=job.get('native_proof',{})
            _require(proof.get('job_id')==job['job_id'] and proof.get('state')=='COMPLETED'
                     and proof.get('exit_code')=='0:0' and job.get('receipt') is not None,
                     'Completed intention lacks positive native/worker proof')
    for cell in state['cells']:
        if cell['evaluation_plan'] is not None:
            _require(cell['training_job'] is not None and state['jobs'][cell['training_job']]['status']=='complete',
                     'Evaluation plan precedes positively completed training')
        if cell['complete']:
            _require(cell['assembly_job'] is not None and state['jobs'][cell['assembly_job']]['status']=='complete'
                     and len(cell['conditions'])==len(plan['conditions']), 'Unproven policy completion')
    _require(len(native)==len(set(native)),'Native allocation bound to multiple intentions')
    active={kind:sum(j['intent']['kind']==kind and j['status']!='complete' for j in state['jobs'].values()) for kind in BOUNDS}
    _require(sum(active.values())<=WINDOW['queued'] and all(active[k]<=WINDOW[k] for k in BOUNDS),
             'Existing campaign state exceeds finite queued/concurrent windows')
    return copy.deepcopy(state)


def _finish(state):
    state.pop('state_sha256',None);state['revision']+=1
    return _sealed(state,'state_sha256')


def record_submission(plan,state,intent_id,job_id,*,expected_revision):
    """Acknowledge one positive native ID, never repeat an ambiguous submission."""
    result=_state(plan,state)
    _require(type(expected_revision) is int and expected_revision==state['revision'] and intent_id in result['jobs'], 'Stale or unknown submission acknowledgment')
    job=result['jobs'][intent_id]
    _require(job['job_id'] is None and isinstance(job_id,str) and re.fullmatch(r'[0-9]+(?:_[0-9]+)?',job_id)
             and job_id not in {v['job_id'] for v in result['jobs'].values()}, 'Existing or duplicate native submission preserved')
    job.update(job_id=job_id,status='submitted')
    return _finish(result)


def _native(job,proof):
    _require(proof.get('job_id')==job['job_id'] and isinstance(proof.get('state'),str),'Native proof identity required')
    if proof['state']=='COMPLETED' or proof['state'] in TERMINAL_FAILURES:
        _require(type(proof.get('cpus')) is int and type(proof.get('gpus')) is int
                 and proof['cpus']==job['intent']['resources']['cpus']
                 and proof['gpus']==job['intent']['resources']['gpus'],'Native allocation differs from intention')
    if job.get('native_proof') and job['native_proof']['state'] in TERMINAL_FAILURES|{'COMPLETED'}:
        _require(job['native_proof']==proof,'Positive terminal native evidence changed')
    job['native_proof']=copy.deepcopy(proof)


def _packet(plan,job,packet):
    intent=job['intent'];value=packet['value']
    _require(_sha(packet.get('sha256')) and packet.get('campaign_plan_sha256')==plan['plan_sha256']
             and packet.get('intent_id')==intent['intent_id'] and packet.get('native_job_id')==job['job_id']
             and packet.get('case')==intent['case'],'Native worker receipt packet is unbound')
    return value


def _training(plan,job,packet):
    value=_packet(plan,job,packet);cell=plan['cells'][job['intent']['cell_index']]
    _require(all(value.get(k)==cell[k] for k in ('task','architecture','seed'))
             and value.get('training_steps')==plan['config']['agents']['total_steps']
             and value.get('complete_training_budget') is True
             and value.get('implementation_sha256')==plan['source_binding']['primary_agents_sha256']
             and value.get('config_sha256')==agents._digest(plan['config']['agents'])
             and value.get('learning_rate')==plan['protocol']['learning_rate']
             and _sha(value.get('checkpoint_sha256')) and _sha(value.get('weight_sha256')),
             'Original complete final-training receipt required')


def bind_evaluation_plan(plan,state,index,binding,*,expected_revision):
    """Bind one native-verified immutable evaluation plan after original training."""
    result=_state(plan,state)
    _require(type(index) is int and 0<=index<240 and type(expected_revision) is int and expected_revision==state['revision'],'Unknown index or stale plan binding')
    cell=result['cells'][index];training=result['jobs'].get(cell['training_job'],{})
    _require(training.get('status')=='complete' and cell['evaluation_plan'] is None,
             'One evaluation plan may bind only after positively completed training')
    value=binding['value'];summary=training['receipt']['value'];original=plan['cells'][index]
    _require(binding.get('case')==original['evaluation_case'] and _sha(binding.get('plan_sha256'))
             and value.get('canary') is False and value.get('benchmark_directions') is False
             and value.get('amendment')==evaluation.AMENDMENT and value.get('evaluation_dtype')=='float64'
             and value.get('execution_module_sha256')==plan['source_binding']['evaluation_module_sha256']
             and value.get('primary_agents_sha256')==plan['source_binding']['primary_agents_sha256']
             and value.get('full_config_sha256')==BASE_CONFIG_SHA256 and value.get('config')==plan['config']
             and value.get('original_protocol')==plan['protocol']
             and value.get('original_protocol_sha256')==plan['protocol_sha256']
             and value.get('conditions')==plan['conditions']
             and value.get('checkpoint_sha256')==summary['checkpoint_sha256']
             and value.get('original_weight_sha256')==summary['weight_sha256']
             and value.get('checkpoint')==str(PurePosixPath(original['training_case'])/'checkpoint.pt')
             and value.get('original_training_steps')==plan['config']['agents']['total_steps']
             and all(value.get(k)==original[k] for k in ('task','architecture','seed')),
             'Evaluation plan changed original checkpoint, full condition roster or qualified amendment')
    cell['evaluation_plan']=copy.deepcopy(binding)
    return _finish(result)


def _evaluation(plan,state,cell,job,packet):
    progress=_packet(plan,job,packet);binding=cell['evaluation_plan'];before=cell['conditions'];after={}
    _require(progress.get('plan_sha256')==binding['plan_sha256']
             and progress.get('status') in {'chunk_complete','condition_grid_complete'}
             and progress.get('required_conditions')==len(plan['conditions'])
             and progress.get('canary') is False and progress.get('scientific_result') is False
             and progress.get('admission_authority') is False,'Exact final-condition chunk receipt required')
    owners={state['jobs'][ident]['job_id'] for ident in cell['evaluation_jobs']}
    for item in packet['condition_receipts']:
        value=item['value'];index=value['condition_index'];key=str(index)
        _require(type(index) is int and 0<=index<len(plan['conditions']) and key not in after
                 and _sha(item.get('sha256')) and value.get('status')=='complete'
                 and value.get('plan_sha256')==binding['plan_sha256'] and value.get('condition')==plan['conditions'][index]
                 and value.get('original_weight_sha256')==binding['value']['original_weight_sha256']
                 and _sha(value.get('evaluation_weight_sha256')) and _sha(value.get('artifact_sha256'))
                 and value.get('slurm_job_id') in owners,'Unbound, duplicate or foreign committed condition')
        if key in before: _require(before[key]==item,'Previously committed condition changed')
        else: _require(value['slurm_job_id']==job['job_id'],'New condition belongs to another native chunk')
        after[key]=copy.deepcopy(item)
    _require(set(before)<=set(after) and len(after)>len(before)
             and progress.get('completed_conditions')==len(after)
             and progress.get('newly_completed_conditions')==len(after)-len(before),
             'Native chunk made no honest monotone condition progress')
    _require(len({v['value']['evaluation_weight_sha256'] for v in after.values()})==1,'Evaluation weights changed between conditions')
    _require((progress['status']=='condition_grid_complete')==(len(after)==len(plan['conditions'])),
             'Condition-grid completion status is inconsistent')
    cell['conditions']=after


def _assembly(plan,cell,job,packet):
    value=_packet(plan,job,packet);binding=cell['evaluation_plan']
    _require(value.get('status')=='complete_amended_evaluation' and value.get('canary') is False
             and value.get('scientific_result') is False and value.get('admission_authority') is False
             and value.get('plan_sha256')==binding['plan_sha256']
             and value.get('conditions')==len(plan['conditions'])==len(cell['conditions'])
             and value.get('checkpoint_sha256')==binding['value']['checkpoint_sha256']
             and value.get('original_weight_sha256')==binding['value']['original_weight_sha256']
             and _sha(value.get('trial_file_sha256')),'Complete source-bound amended assembly required')
    rows=value.get('condition_bindings',[])
    _require(len(rows)==len(cell['conditions']) and len({r['condition_index'] for r in rows})==len(rows)
             and all(str(r['condition_index']) in cell['conditions']
                 and r['receipt_sha256']==cell['conditions'][str(r['condition_index'])]['sha256']
                 and r['artifact_sha256']==cell['conditions'][str(r['condition_index'])]['value']['artifact_sha256'] for r in rows),
             'Assembly does not bind every exact committed condition')
    cell['complete']=True


def dispatch_tick(plan,state,native,receipts):
    """Consume verified snapshots; emit each finite intention exactly once.

    ``native`` maps acknowledged IDs to original positive allocation records.
    ``receipts`` maps intention IDs to SHA/file-verified worker packets, including
    the entire immutable committed-condition snapshot for each evaluation chunk.
    Missing evidence waits. Failure/nonprogress blocks further automatic work.
    """
    result=_state(plan,state);intentions=[]
    for ident,job in result['jobs'].items():
        if job['job_id'] is None: continue
        if job['job_id'] in native: _native(job,native[job['job_id']])
        proof=job.get('native_proof')
        if not proof: continue
        if job['status']=='needs_repair': continue
        if proof['state'] in TERMINAL_FAILURES or proof['state']=='COMPLETED' and proof.get('exit_code')!='0:0':
            job['status']='needs_repair'
            if ident not in result['needs_repair']:result['needs_repair'].append(ident)
            continue
        if proof['state']!='COMPLETED' or ident not in receipts: continue
        if job['receipt'] is not None:
            _require(job['receipt']==receipts[ident],'Previously validated native worker receipt changed')
            continue
        packet=receipts[ident];cell=result['cells'][job['intent']['cell_index']]
        try:
            if job['intent']['kind']=='training': _training(plan,job,packet)
            elif job['intent']['kind']=='evaluation': _evaluation(plan,result,cell,job,packet)
            else: _assembly(plan,cell,job,packet)
        except (ValueError,KeyError,TypeError) as error:
            job.update(status='needs_repair',failure=str(error),failure_receipt=copy.deepcopy(packet))
            if ident not in result['needs_repair']: result['needs_repair'].append(ident)
            continue
        job.update(status='complete',receipt=copy.deepcopy(packet))
    if result['needs_repair']:
        result['status']='needs_repair';return _finish(result),intentions
    for cell in result['cells']:
        if (len(cell['evaluation_jobs'])==WINDOW['evaluation_waves_per_policy'] and len(cell['conditions'])<len(plan['conditions'])
                and result['jobs'][cell['evaluation_jobs'][-1]]['status']=='complete'):
            result['needs_repair'].append(f'policy-{cell["index"]:04d}-wave-cap')
    if result['needs_repair']:
        result['status']='needs_repair';return _finish(result),intentions
    # All unacknowledged intentions count as live work. They are never emitted twice.
    active={kind:sum(j['intent']['kind']==kind and j['status']!='complete' for j in result['jobs'].values()) for kind in BOUNDS}
    for cell in result['cells']:
        if cell['complete']: continue
        kind=None;wave=0
        if cell['training_job'] is None:kind='training'
        elif result['jobs'][cell['training_job']]['status']!='complete':continue
        elif cell['evaluation_plan'] is None:continue
        elif len(cell['conditions'])==len(plan['conditions']):
            if cell['assembly_job'] is None:kind='assembly'
        elif not cell['evaluation_jobs'] or result['jobs'][cell['evaluation_jobs'][-1]]['status']=='complete':
            wave=len(cell['evaluation_jobs'])+1
            if wave>WINDOW['evaluation_waves_per_policy']:
                result['needs_repair'].append(f'policy-{cell["index"]:04d}-wave-cap');continue
            kind='evaluation'
        if kind is None or active[kind]>=WINDOW[kind] or sum(active.values())>=WINDOW['queued']:continue
        intent=_intent(plan,cell,kind,wave);ident=intent['intent_id']
        result['jobs'][ident]={'intent':intent,'job_id':None,'status':'ambiguous','native_proof':None,'receipt':None}
        if kind=='training':cell['training_job']=ident
        elif kind=='evaluation':cell['evaluation_jobs'].append(ident)
        else:cell['assembly_job']=ident
        active[kind]+=1;intentions.append(copy.deepcopy(intent))
    result['status']='needs_repair' if result['needs_repair'] else 'computational_evaluations_complete_inference_pending' if all(c['complete'] for c in result['cells']) else 'active'
    return _finish(result),intentions
