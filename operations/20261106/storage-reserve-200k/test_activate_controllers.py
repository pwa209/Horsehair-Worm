from pathlib import Path
from types import SimpleNamespace
import copy,hashlib,importlib.util,json,shlex
import pytest

spec=importlib.util.spec_from_file_location('finite_activation_test_runtime',Path(__file__).with_name('activate_controllers.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def test_exact_retirement_roster_protects_workers_and_held_census():
 assert m.RETIRE==('8559679','8559680','8559681','8559683','8559708','8559725')
 assert m.KEEP_HELD=='6983060' and not set(m.OLD_AGENT[:2])&set(m.RETIRE)
 assert m.BASE==m.ROOT/'maintenance/storage-reserve-200k-v3-20261106'
 assert m.CENSUS_BASE==m.ROOT/'maintenance/storage-reserve-200k-v2-20261106/census'
 assert m.POLICY_BASE==m.ROOT/'maintenance/storage-reserve-200k-v2-20261106'

@pytest.mark.parametrize('state',['RUNNING','PENDING','COMPLETING'])
def test_incomplete_native_finality_never_establishes_success(state):
 row={'state':state,'exit_code':'0:0','cpus':1,'elapsed_seconds':3}
 assert not m.successful({'123':row},{'123':1})

@pytest.mark.parametrize('field,value',[('state','FAILED'),('exit_code','1:0'),('cpus',8),('elapsed_seconds',0)])
def test_final_predecessor_identity_and_success_cannot_be_inferred(field,value):
 row={'state':'COMPLETED','exit_code':'0:0','cpus':1,'elapsed_seconds':3};row[field]=value
 with pytest.raises(ValueError):m.successful({'123':row},{'123':1})

def test_positive_scalar_accounting_and_unallocated_cancellation_are_distinct():
 rows=m.parse_accounting('123|pwa209|def-ptewarie_cpu|COMPLETED|0:0|198|2\n124|pwa209|def-ptewarie_cpu|CANCELLED by 3164768|0:0|0|0\n')
 assert m.successful(rows,{'123':2}) and rows['124']=={'state':'CANCELLED','exit_code':'0:0','elapsed_seconds':0,'cpus':0}

@pytest.mark.parametrize('text',['123|other|def-ptewarie_cpu|COMPLETED|0:0|3|1','123_1|pwa209|def-ptewarie_cpu|COMPLETED|0:0|3|1',
 '123|pwa209|other|COMPLETED|0:0|3|1','123|pwa209|def-ptewarie_cpu|COMPLETED|0:0|3|1\n123|pwa209|def-ptewarie_cpu|COMPLETED|0:0|3|1'])
def test_foreign_array_or_duplicate_accounting_is_refused(text):
 with pytest.raises(ValueError):m.parse_accounting(text)

def old_held():
 task={'name':'old-controller','script':'/old/controller.sbatch'}
 f={'JobId':m.RETIRE[0],'UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'PENDING','Reason':'JobHeldUser',
  'RunTime':'00:00:00','JobName':'hhw-old-controller','Command':task['script'],'NumCPUs':'1','MinMemoryNode':'64G','NumNodes':'1','Requeue':'0','Restarts':'0','ReqTRES':'cpu=1,mem=64G,node=1,billing=16000'}
 return f,task

def test_exact_never_started_held_controller_is_eligible():
 f,t=old_held();m.held_controller(f,m.RETIRE[0],t)

@pytest.mark.parametrize('field,value',[('JobState','RUNNING'),('Reason','Priority'),('RunTime','00:00:01'),('UserId','other(1)'),
 ('Command','/worker.sbatch'),('NumCPUs','8'),('JobId',m.KEEP_HELD)])
def test_controller_retirement_refuses_changed_or_started_identity(field,value):
 f,t=old_held();f[field]=value
 with pytest.raises(ValueError):m.held_controller(f,m.RETIRE[0],t)

@pytest.mark.parametrize('text',['afterok:123(unfulfilled)','afterok:123(satisfied)'])
def test_actual_and_dependencies_match_issued_ledger(text):
 m.dependency_contract({'Dependency':text},{'dependency_kind':'afterok','issued_native_dependencies':['123']})

@pytest.mark.parametrize('text',['afterany:123(unfulfilled)','afterok:124(unfulfilled)','afterok:123?afterok:124','(null)','afterok:123(unfulfilled),afterok:124(unfulfilled)'])
def test_changed_or_parallel_dependencies_refuse_release(text):
 with pytest.raises(ValueError):m.dependency_contract({'Dependency':text},{'dependency_kind':'afterok','issued_native_dependencies':['123']})

@pytest.mark.parametrize('files,bytes_',[(249488,6000000000000-1073741824),(192349,1153976118167)])
def test_controller_metadata_forecast_keeps_original_occupancy_and_byte_caps(files,bytes_):
 assert m.metadata_forecast({'study_files':files,'study_bytes':bytes_})=={'required_files':512,'required_bytes':1073741824,'reserve_bytes':2000000000000}

@pytest.mark.parametrize('record',[{'study_files':249489,'study_bytes':0},{'study_files':0,'study_bytes':6000000000000-1073741823},
 {'study_files':True,'study_bytes':0},{'study_files':0,'study_bytes':-1}])
def test_metadata_forecast_cannot_expand_scientific_storage(record):
 with pytest.raises(ValueError):m.metadata_forecast(record)

def agent_fixture():
 tasks=[];scripts={}
 for i in range(4):
  name='old-agent-controller-'+str(i);path='/old/'+name+'.sbatch';command='python /old/coordination.py --segment '+str(i)
  text='#!/bin/bash\n#SBATCH --hold\n#SBATCH --time-min=08:00:00\n#SBATCH --job-name=hhw-'+name+'\n#SBATCH --output=/old/log-%A_%a.log\n'+command+'\n'
  tasks.append({'name':name,'script':path,'script_sha256':m.canonical(text),'command':command,'depends':[] if i==0 else [tasks[-1]['name']],
   'dependency_kind':'afterok','external_dependencies':['777'] if i==0 else [],'resources':{'cpus':1,'memory':'64G','hours':96,'gpu':False,'array':None},'untouched_receipt_semantics':'retained'})
  scripts[path]=text
 return {'tasks':tasks,'root':str(m.ROOT),'phase':'old','config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE}},scripts

def render(plan,scripts):
 return m.render_agent(plan,{'sha256':'a'*64},{'path':'/new/qualification.json','sha256':'b'*64,'job_id':'999'},
  {'path':str(m.BASE/'consumers/af-consumer-binding.json'),'sha256':'c'*64},lambda path:scripts[path])

def test_agent_plan_preserves_science_resources_and_sequential_envelope():
 old,scripts=agent_fixture();before=copy.deepcopy(old);new=render(old,scripts)
 assert old==before and len(new['plan']['tasks'])==4 and new['plan']['phase']==m.AGENT_PHASE
 for i,t in enumerate(new['plan']['tasks']):
  assert t['resources']==old['tasks'][i]['resources'] and t['untouched_receipt_semantics']=='retained'
  assert t['depends']==([] if i==0 else [new['plan']['tasks'][i-1]['name']]) and t['dependency_kind']=='afterok'
  assert t['external_dependencies']==(['999'] if i==0 else [])
  assert '--consumer-binding' in shlex.split(t['command']) and t['script_sha256']==m.canonical(new['scripts'][t['script']])
  assert new['scripts'][t['script']].splitlines().count('#SBATCH --hold')==1

@pytest.mark.parametrize('change',['resources','parallel','script_bytes','missing_hold','missing_minimum'])
def test_agent_plan_rejects_unbound_or_changed_originals(change):
 old,scripts=agent_fixture();t=old['tasks'][1]
 if change=='resources':t['resources']['cpus']=2
 elif change=='parallel':t['depends']=[]
 else:
  path=t['script'];scripts[path]=scripts[path].replace('#SBATCH --hold\n','') if change=='missing_hold' else scripts[path].replace('#SBATCH --time-min=08:00:00\n','') if change=='missing_minimum' else scripts[path]+'# altered\n'
  if change!='script_bytes':t['script_sha256']=m.canonical(scripts[path])
 with pytest.raises(ValueError):render(old,scripts)

def registration_fixture(tmp_path,monkeypatch):
 root=tmp_path/'study';base=root/'maintenance/storage-reserve-200k-v3-20261106';directory=root/'plans/finite-handoff';directory.mkdir(parents=True)
 monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'BASE',base);ordinary=m.ordinary;monkeypatch.setattr(m,'ordinary',lambda path:ordinary(path,root=root))
 temporary=tmp_path/'node-local';temporary.mkdir();monkeypatch.setenv('SLURM_TMPDIR',str(temporary));monkeypatch.setenv('SLURM_JOB_END_TIME','10000')
 sp=directory/'activate.sbatch';pp=directory/'plan.json';lp=root/'state/activation-submission.json';lp.parent.mkdir()
 tail=['--request',str(base/'request.json'),'--request-sha256','a'*64,'--source-sha256','b'*64,'--native-plan',str(pp),'--native-ledger',str(lp),
  '--native-script',str(sp),'--native-task','activation','--native-tests-junit','activation-tests.xml']
 command=shlex.join(['python',str(base/'activate_controllers.py'),*tail]);text='#!/bin/bash\n'+command+'\n';sp.write_text(text)
 task={'name':'activation','command':command,'script':str(sp),'script_sha256':m.canonical(text),'resources':{'cpus':2,'memory':'16G','hours':1,'gpu':False,'array':None}}
 plan={'root':str(root),'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]};pp.write_text(json.dumps(plan))
 ledger={'config_sha256':m.CONFIG,'source_sha256':m.SOURCE,'plan_sha256':m.canonical(plan),'tasks':{'activation':{'state':'submitted','job_id':'900','script_sha256':task['script_sha256']}}};lp.write_text(json.dumps(ledger))
 args=SimpleNamespace(native_plan=pp,native_ledger=lp,native_script=sp,native_task='activation')
 argv=list(tail)
 fields={'JobId':'900','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING','Command':str(sp),'JobName':'hhw-activation',
  'NumCPUs':'2','NumNodes':'1','MinMemoryNode':'16G','TimeLimit':'01:00:00','Requeue':'0','EndTime':'1970-01-01T02:46:40'}
 return args,argv,fields

def test_actual_registration_preserves_literal_node_local_junit_basename(tmp_path,monkeypatch):
 a,argv,fields=registration_fixture(tmp_path,monkeypatch)
 assert m.own_registration(a,argv,'900',fields,wall=lambda:0)[0]==10000

@pytest.mark.parametrize('change',['argv','script','resources','deadline','requeue','owner'])
def test_actual_registration_refuses_mismatch(tmp_path,monkeypatch,change):
 a,argv,fields=registration_fixture(tmp_path,monkeypatch)
 if change=='argv':argv[-1]='/other/activation-tests.xml'
 elif change=='script':a.native_script.write_text(a.native_script.read_text()+'# altered\n')
 elif change=='resources':fields['NumCPUs']='1'
 elif change=='deadline':monkeypatch.setenv('SLURM_JOB_END_TIME','9999.0');fields['EndTime']='1970-01-01T02:46:30'
 elif change=='requeue':fields['Requeue']='1'
 else:fields['UserId']='other(1)'
 with pytest.raises(ValueError):m.own_registration(a,argv,'900',fields,wall=lambda:0)

@pytest.mark.parametrize('bad',['failure','skipped','duplicate','roster'])
def test_native_test_proof_rejects_partial_or_different_cases(tmp_path,monkeypatch,bad):
 path=tmp_path/'tests.xml';xml='<testsuite><testcase classname="activation_test" name="one"/></testsuite>'
 monkeypatch.setattr(m,'TEST_COUNT',1);monkeypatch.setattr(m,'TEST_ROSTER_SHA',m.canonical(['activation_test::one']))
 if bad in {'failure','skipped'}:xml='<testsuite><testcase classname="activation_test" name="one"><'+bad+'/></testcase></testsuite>'
 elif bad=='duplicate':xml='<testsuite><testcase classname="activation_test" name="one"/><testcase classname="activation_test" name="one"/></testsuite>'
 else:xml=xml.replace('name="one"','name="other"')
 path.write_text(xml)
 with pytest.raises(ValueError):m.passing_tests(path,temporary=tmp_path)


@pytest.mark.parametrize('kind,field,status',[
 ('census','slurm_job_id','complete_census_resilience_operational_qualification'),
 ('consumers','native_job_id','complete_census_resilience_consumer_qualification'),
 ('coordination','native_job_id','qualified_agent_coordination_repair')])
def test_actual_qualification_report_schema_is_component_specific(kind,field,status):
    report={field:'123','status':status,'returncode':0};before=copy.deepcopy(report)
    m.qualification_identity(kind,report,'123');assert report==before
    wrong={('native_job_id' if field=='slurm_job_id' else 'slurm_job_id'):'123','status':status,'returncode':0}
    with pytest.raises(ValueError):m.qualification_identity(kind,wrong,'123')


@pytest.mark.parametrize('field,value',[('NumNodes','2'),('Requeue','1'),('Restarts','1'),('ArrayJobId','123'),
 ('ReqTRES','cpu=1,mem=64G,node=1,gres/gpu=1'),('AllocTRES','cpu=1,mem=64G,node=1,gres/gpu=1')])
def test_replacement_scalar_identity_refuses_ambient_overrides(field,value):
    f,t=old_held();f.update(TimeLimit='96:00:00',Dependency='(null)');t['resources']={'hours':96};f[field]=value
    with pytest.raises(ValueError):m.held_replacement(f,m.RETIRE[0],t,{'dependency_kind':'afterok','issued_native_dependencies':[]})


def test_native_test_basename_is_resolved_only_inside_owned_allocation(tmp_path):
    assert m.native_test_path('activation-tests.xml',tmp_path)==tmp_path/'activation-tests.xml'
    for value in ['../activation-tests.xml','/other/activation-tests.xml','wrong.xml','$SLURM_TMPDIR/activation-tests.xml']:
        with pytest.raises(ValueError):m.native_test_path(value,tmp_path)


def test_exact_replacement_scalar_identity_and_no_requeue_render():
    f,t=old_held();f.update(TimeLimit='96:00:00',Dependency='(null)');t['resources']={'hours':96}
    m.held_replacement(f,m.RETIRE[0],t,{'dependency_kind':'afterok','issued_native_dependencies':[]})
    old,scripts=agent_fixture();new=render(old,scripts)
    assert all(text.splitlines().count('#SBATCH --no-requeue')==1 for text in new['scripts'].values())
    assert all(task['script_sha256']==m.canonical(new['scripts'][task['script']]) for task in new['plan']['tasks'])
