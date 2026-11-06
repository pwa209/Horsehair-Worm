"""Tiny native metadata/qualification/DAG fixtures; no submission or census calls."""
from pathlib import Path
import importlib.util,json,shlex,types
import pytest

AREA=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('finite_study_preparation',AREA/'prepare_study.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def render(*args,**kwargs):return '#!/bin/bash\n#SBATCH --time=1:00:00\nset -euo pipefail\nexport HHW_MAX_HOURS=1\n'+args[3]+'\n'

def test_generated_hold_is_explicit_and_original_command_preserved():
 text=render(None,None,None,'python /verified/helper.py');held=m.held_text(text,1)
 assert held.count('#SBATCH --hold\n')==held.count('#SBATCH --no-requeue\n')==1 and held.endswith('python /verified/helper.py\n')
 with pytest.raises(ValueError):m.held_text(held,1)

def test_half_hour_has_actual_native_limit_and_matching_export():
 held=m.held_text(render(None,None,None,'inert'),.5)
 assert '#SBATCH --time=00:30:00\n' in held and 'export HHW_MAX_HOURS=0.5\n' in held and '#SBATCH --time=1:00:00\n' not in held

def test_held_native_owner_resource_and_array_guards():
 task={'name':'fixture','script':'/owned/fixture','resources':{'cpus':1,'memory':'8G','hours':12}}
 native={'JobId':'5','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'PENDING','Reason':'JobHeldUser','Priority':'0',
  'Command':task['script'],'JobName':'hhw-fixture','NumCPUs':'1','NumNodes':'1','MinMemoryNode':'8G','TimeLimit':'12:00:00','Requeue':'0','ReqTRES':'cpu=1,mem=8G,node=1'}
 m.held_native(native,'5',task)
 for change in [{'JobState':'RUNNING'},{'UserId':'other(1)'},{'NumCPUs':'2'},{'ArrayJobId':'5'},{'ReqTRES':'cpu=1,gres/gpu=1'}]:
  with pytest.raises(ValueError):m.held_native({**native,**change},'5',task)

def test_qualifications_require_actual_owned_positive_final_allocations():
 row={'state':'COMPLETED','exit_code':'0:0','user':'pwa209','account':'def-ptewarie_cpu','cpus':4,'job_id_raw':'9','unallocated_metadata':False,'elapsed_seconds':30,'alloc_tres':'cpu=4,mem=16G,node=1'}
 m.successful_native({'9':row},['9'])
 for change in [{'state':'RUNNING'},{'exit_code':'1:0'},{'user':'other'},{'cpus':0},{'unallocated_metadata':True},{'alloc_tres':'cpu=4,gres/gpu=1'}]:
  with pytest.raises(ValueError):m.successful_native({'9':{**row,**change}},['9'])

def test_final_accounting_may_settle_after_scheduler_success():
 now=[0];observed=[];row={'state':'COMPLETED','exit_code':'0:0','user':'pwa209','account':'def-ptewarie_cpu','cpus':4,'job_id_raw':'9','unallocated_metadata':False,'elapsed_seconds':30,'alloc_tres':'cpu=4,mem=16G,node=1'}
 answers=iter([{}, {'9':{**row,'state':'RUNNING'}},{'9':row}])
 def reader(timeout):observed.append(timeout);return next(answers)
 result=m.wait_qualifications(reader,['9'],3600,wall=lambda:now[0],monotonic=lambda:now[0],sleeper=lambda n:now.__setitem__(0,now[0]+n))
 assert result['9']==row and len(observed)==3 and now[0]==10 and all(0<t<=30 for t in observed)

def test_failed_qualification_is_never_retried_or_accepted():
 calls=[]
 def reader(timeout):calls.append(timeout);return {'9':{'state':'FAILED','exit_code':'1:0'}}
 with pytest.raises(ValueError):m.wait_qualifications(reader,['9'],3600,wall=lambda:0,monotonic=lambda:0,sleeper=lambda n:None)
 assert len(calls)==1

def test_pending_final_accounting_has_finite_twelve_minute_bound():
 now=[0];calls=[]
 def reader(timeout):calls.append(timeout);return {}
 with pytest.raises(ValueError):m.wait_qualifications(reader,['9'],3600,wall=lambda:now[0],monotonic=lambda:now[0],sleeper=lambda n:now.__setitem__(0,now[0]+n))
 assert now[0]==720 and len(calls)==144 and max(calls)<=30

def test_unrecognized_native_observation_error_is_not_hidden():
 calls=[]
 def reader(timeout):calls.append(timeout);raise RuntimeError('foreign source binding changed')
 with pytest.raises(RuntimeError):m.wait_qualifications(reader,['9'],3600,wall=lambda:0,monotonic=lambda:0,sleeper=lambda n:None)
 assert len(calls)==1

def test_actual_junit_roster_and_source_job_are_required(tmp_path,monkeypatch):
 xml=tmp_path/'native.xml';xml.write_text('<testsuite tests="1" failures="0" errors="0"><testcase classname="Fixture" name="test_proof"/></testsuite>')
 binding={'path':str(xml),'sha256':m.sha(xml)}
 def pin(b):
  p=Path(b['path']);assert m.sha(p)==b['sha256'];return p
 monkeypatch.setattr(m,'pin',pin)
 contract={'tests':1,'roster_sha256':m.canonical(['Fixture::test_proof'])};manifest={'suites':{'resilience':contract}}
 source={'path':'/approved/manifest','sha256':'a'*64};record={'status':'complete_census_resilience_operational_qualification','scientific_result':False,'returncode':0,'slurm_job_id':'9','source_manifest':source,'suite':{'junit':binding}}
 assert m.passing_report('census',record,manifest,'9',source)==binding
 with pytest.raises(ValueError):m.passing_report('census',{**record,'slurm_job_id':'10'},manifest,'9',source)
 xml.write_text('<testsuite tests="1"><testcase classname="Fixture" name="test_proof"><skipped/></testcase></testsuite>');binding['sha256']=m.sha(xml)
 with pytest.raises(ValueError):m.passing_report('census',record,manifest,'9',source)

def test_direct_dag_render_assigns_no_ids_and_preserves_dependencies(tmp_path,monkeypatch):
 monkeypatch.setattr(m,'ROOT',tmp_path);monkeypatch.setattr(m,'FRAMEWORK',tmp_path/'release')
 resources={'cpus':2,'memory':'16G','hours':1,'gpu':False,'array':None}
 plan,scripts,p,l=m.one_plan({}, {'sha256':m.SOURCE},'phase','task','python /known/helper',resources,['101','102'],render,'pytest /known/test')
 assert plan['tasks'][0]['external_dependencies']==['101','102'] and plan['tasks'][0]['dependency_kind']=='afterok'
 assert 'job_id' not in plan['tasks'][0] and not p.exists() and not l.exists()
 text=next(iter(scripts.values()));assert text.index('pytest /known/test')<text.index('python /known/helper')
 assert plan['tasks'][0]['script_sha256']==m.canonical(text)

def test_source_test_wrapper_quotes_files_and_keeps_node_local_junit():
 prefix=m.source_test_prefix({'/source folder/test.py':'a'*64},['/source folder/test.py'],'$SLURM_TMPDIR/tests.xml','$SLURM_TMPDIR/pytest-temp')
 assert '--junitxml "$SLURM_TMPDIR/tests.xml"' in prefix and '--basetemp "$SLURM_TMPDIR/pytest-temp"' in prefix
 assert "'/source folder/test.py'" in prefix and 'hashlib.sha256' in prefix

def test_activation_cli_is_fixed_literal_and_metadata_not_scientific():
 import ast
 source=(AREA/'prepare_study.py').read_bytes();tree=ast.parse(source)
 literals=[n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and isinstance(n.value,str)]
 assert 'activation-tests.xml' in literals and '$SLURM_TMPDIR/activation-tests.xml' in literals
 assert "'$SLURM_TMPDIR/activation-tests.xml'" not in literals
 assert 'storage_snapshot(' not in source.decode() and "['scancel'" not in source.decode()
