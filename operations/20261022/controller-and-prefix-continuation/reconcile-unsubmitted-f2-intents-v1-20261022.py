"""Explicit inverse of four persisted, positively never-submitted intentions.

Only reference bookkeeping changes. Existing jobs, outcomes and immutable
intents are preserved in the original state snapshot before any commit.
"""
import copy
from horsehair.core import canonical_hash
EXPECTED={
 'assembly-0035':'5369480093d2dc5042dc447e9df3fa82fba220e04cd8d5b3b653186baa5966ba',
 'assembly-0038':'51dd3168405ca693eeb1a450cd34d6b6554b576bdae9daee4fe0a466e5de1c34',
 'evaluation-0039-01':'f1435085e4710432442abc78f2878d0f273e24e88585d885cc4143c7482ac0e1',
 'training-0043':'83dcdcd25d566fe0e1cc8fbd3cc147794c5f394411206821ad90658b72f11ecb'}
STATE_SHA='0b0edb639de08035b5b68ae8b731b712feb252d315c097dc61d84e437e589c1c'
PLAN_SHA='36f7db5dc790108040e454674006421144184db929b3de88aec2b9a9008727e0'

def require(value,message):
 if not value:raise ValueError(message)

def reconcile(state,absence,*,expected_state_sha=STATE_SHA,expected_intents=EXPECTED):
 require(type(absence) is dict and set(absence)==set(expected_intents),'Complete exact absence evidence required')
 require(state['state_sha256']==expected_state_sha and state['state_sha256']==canonical_hash({k:v for k,v in state.items() if k!='state_sha256'}),'Exact unchanged state required')
 require(state['plan_sha256']==PLAN_SHA and state['status']=='active' and state['needs_repair']==[],'Unchanged active frozen campaign required')
 require({k for k,v in state['jobs'].items() if v['job_id'] is None}==set(expected_intents),'Unexpected unacknowledged intention refused')
 result=copy.deepcopy(state)
 for name,sha in expected_intents.items():
  proof=absence[name]
  require(set(proof)=={'plan_absent','ledger_absent','journal_absent','native_queue_empty','native_history_empty'} and all(v is True for v in proof.values()),'Positive complete absence proof required')
  job=result['jobs'][name];intent=job['intent'];cell=result['cells'][intent['cell_index']]
  require(set(job)=={'intent','job_id','status','native_proof','receipt'} and job['status']=='ambiguous' and all(job[k] is None for k in ['job_id','native_proof','receipt']),'Only never-acknowledged empty intention permitted')
  require(intent['intent_id']==name and intent['intent_sha256']==sha and canonical_hash({k:v for k,v in intent.items() if k!='intent_sha256'})==sha and intent['campaign_plan_sha256']==PLAN_SHA,'Exact immutable pending intention required')
  kind=intent['kind'];require(cell['index']==intent['cell_index'] and cell['complete'] is False,'Exact incomplete original cell required')
  if kind=='training':
   require(cell['training_job']==name and cell['evaluation_plan'] is None and cell['evaluation_jobs']==[] and cell['assembly_job'] is None and cell['conditions']=={},'Training has dependent evidence')
   cell['training_job']=None
  elif kind=='evaluation':
   require(cell['evaluation_plan'] is not None and cell['assembly_job'] is None and cell['evaluation_jobs'] and cell['evaluation_jobs'][-1]==name and len(cell['evaluation_jobs'])==intent['wave'],'Only last pending evaluation reference permitted')
   cell['evaluation_jobs'].pop()
  elif kind=='assembly':
   require(cell['assembly_job']==name,'Exact pending assembly reference required');cell['assembly_job']=None
  else:raise ValueError('Unknown intention phase')
  del result['jobs'][name]
 result.pop('state_sha256');result['revision']+=1;result['state_sha256']=canonical_hash(result)
 return result
