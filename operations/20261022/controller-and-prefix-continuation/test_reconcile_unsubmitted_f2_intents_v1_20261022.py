from pathlib import Path
import importlib.util,copy
import pytest
from horsehair.core import canonical_hash
spec=importlib.util.spec_from_file_location('unsubmitted_repair',Path(__file__).with_name('reconcile-unsubmitted-f2-intents-v1-20261022.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

@pytest.fixture
def data():
 cells=[{'index':i,'complete':False,'training_job':None,'evaluation_plan':None,'evaluation_jobs':[],'assembly_job':None,'conditions':{}} for i in range(240)]
 state={'plan_sha256':m.PLAN_SHA,'status':'active','needs_repair':[],'revision':213,'cells':cells,'jobs':{'old':{'job_id':'123','status':'complete','receipt':{'preserved':True}}}}
 expected={};absence={}
 for name,kind,index,wave in [('training-0043','training',43,0),('evaluation-0039-01','evaluation',39,1),('assembly-0035','assembly',35,0),('assembly-0038','assembly',38,0)]:
  intent={'intent_id':name,'kind':kind,'cell_index':index,'wave':wave,'campaign_plan_sha256':m.PLAN_SHA};intent['intent_sha256']=canonical_hash(intent);expected[name]=intent['intent_sha256']
  state['jobs'][name]={'intent':intent,'job_id':None,'status':'ambiguous','native_proof':None,'receipt':None}
  c=cells[index]
  if kind=='training':c['training_job']=name
  elif kind=='evaluation':c['training_job']='old';c['evaluation_plan']={'unchanged':True};c['evaluation_jobs']=[name]
  else:c['training_job']='old';c['evaluation_plan']={'unchanged':True};c['conditions']={'all':{'preserved':True}};c['assembly_job']=name
  absence[name]={k:True for k in ['plan_absent','ledger_absent','journal_absent','native_queue_empty','native_history_empty']}
 state['state_sha256']=canonical_hash(state)
 return state,expected,absence

def invoke(data):
 state,expected,absence=data
 return m.reconcile(state,absence,expected_state_sha=state['state_sha256'],expected_intents=expected)

def reseal(state):
 state.pop('state_sha256');state['state_sha256']=canonical_hash(state)

def test_inverse_changes_only_four_references_and_intentions_and_revision(data):
 before=copy.deepcopy(data[0]);result=invoke(data)
 assert data[0]==before and result['revision']==214 and set(result['jobs'])=={'old'} and result['jobs']['old']==before['jobs']['old']
 assert result['cells'][39]['evaluation_jobs']==[] and result['cells'][43]['training_job'] is None
 assert result['cells'][35]['assembly_job'] is None and result['cells'][38]['assembly_job'] is None
 for i,c in enumerate(result['cells']):
  expected=copy.deepcopy(before['cells'][i])
  if i==39:expected['evaluation_jobs']=[]
  if i==43:expected['training_job']=None
  if i in [35,38]:expected['assembly_job']=None
  assert c==expected
 assert result['state_sha256']==canonical_hash({k:v for k,v in result.items() if k!='state_sha256'})

@pytest.mark.parametrize('field',['plan_absent','ledger_absent','journal_absent','native_queue_empty','native_history_empty'])
@pytest.mark.parametrize('value',[False,None,1,'true'])
def test_any_nonpositive_absence_proof_refuses_without_mutating(data,field,value):
 data[2]['training-0043'][field]=value;before=copy.deepcopy(data[0])
 with pytest.raises(ValueError):invoke(data)
 assert data[0]==before

@pytest.mark.parametrize('field,value',[('job_id','444'),('receipt',{}),('native_proof',{}),('status','submitted')])
def test_acknowledged_or_evidenced_intention_never_removed(data,field,value):
 data[0]['jobs']['training-0043'][field]=value;reseal(data[0])
 with pytest.raises(ValueError):invoke(data)

@pytest.mark.parametrize('mutation',['missing_proof','extra_proof','foreign_plan','stale_hash','changed_intent','dependent_training','nonlast_evaluation','changed_assembly','extra_pending'])
def test_identity_dependencies_and_exact_scope_refused(data,mutation):
 state,expected,absence=data
 if mutation=='missing_proof':absence.pop('training-0043')
 if mutation=='extra_proof':absence['foreign']={}
 if mutation=='foreign_plan':state['plan_sha256']='f'*64
 if mutation=='changed_intent':state['jobs']['training-0043']['intent']['wave']=9
 if mutation=='dependent_training':state['cells'][43]['evaluation_plan']={}
 if mutation=='nonlast_evaluation':state['cells'][39]['evaluation_jobs'].append('other')
 if mutation=='changed_assembly':state['cells'][35]['assembly_job']='other'
 if mutation=='extra_pending':state['jobs']['new']={'job_id':None}
 if mutation!='stale_hash':reseal(state)
 else:state['revision']+=1
 before=copy.deepcopy(state)
 with pytest.raises(ValueError):invoke(data)
 assert state==before
