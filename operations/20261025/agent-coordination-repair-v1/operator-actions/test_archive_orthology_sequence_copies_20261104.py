from pathlib import Path
import hashlib,importlib.util,json,os,tarfile
import pytest

def module(name,file):
 spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(file));value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
m=module('sequence_archiver','archive-orthology-sequence-copies-20261104.py')
h=module('original_archive_integrity','compact-terminal-controller-reads-20261104.py')

@pytest.fixture
def case(tmp_path,monkeypatch):
 root=tmp_path/'root';root.mkdir();specs=[(name,1) for name,count in m.SPECS]
 for name,count in specs:
  directory=root/name;directory.mkdir(parents=True);(directory/'OG0000001.fa').write_bytes(b'>copied-sequence\nACDEFGHIK\n')
 retained=root/'retained/N0.tsv';retained.parent.mkdir();retained.write_bytes(b'original table')
 (root/'original-proteome.fa').write_bytes(b'>all-originals\nACDEFGHIK\n');(root/m.PRIMARY/'Gene_Trees').mkdir();(root/m.PRIMARY/'Gene_Trees/OG0000001_tree.txt').write_text('original tree')
 local=tmp_path/'node-local';local.mkdir();monkeypatch.setenv('SLURM_TMPDIR',str(local))
 if os.name!='posix':
  monkeypatch.setattr(h,'sync_directory',lambda path:None)
  # Windows pathstat reports birthtime where fstat reports change time. The
  # production Linux inode/mtime/ctime checks remain unchanged; fixtures make
  # only this incompatible Windows field neutral, native tests use all five.
  original_stamp=m.stamp;monkeypatch.setattr(m,'stamp',lambda info:original_stamp(info)[:4]+[0])
 bindings={str(retained):m.sha(retained)};rows=m.roster(specs,h,root=root);destination=root/'results/storage-archives/archive';destination.parent.mkdir(parents=True)
 return root,specs,rows,destination,bindings

def original_paths(case):
 root,specs,rows,destination,bindings=case;return [root/row['path'] for row in rows]

def test_actual_roundtrip_removes_only_four_flat_copies(case):
 root,specs,rows,destination,bindings=case;receipt,journal=m.pack_and_remove(destination,rows,specs,{'domain':'explicit-offline-fixture'},h,bindings,root=root)
 assert journal['removed_count']==4 and journal['status']=='complete_verified_orthology_sequence_copy_compaction'
 h.verify_tar(destination/'sequences.tar',rows,receipt['archive_sha256'])
 assert not any(p.exists() for p in original_paths(case)) and all((root/name).is_dir() for name,count in specs)
 assert (root/'retained/N0.tsv').read_bytes()==b'original table' and (root/'original-proteome.fa').exists() and (root/m.PRIMARY/'Gene_Trees/OG0000001_tree.txt').read_text()=='original tree'

@pytest.mark.parametrize('bad',['OG0000001.txt','unknown.fa','checkpoint.json','../OG0000001.fa','/outside/OG0000001.fa'])
def test_only_exact_generated_flat_fasta_names(bad,case):
 root,specs,rows,destination,bindings=case;changed=[{**rows[0],'path':str(Path(specs[0][0])/bad).replace('\\','/')},*rows[1:]]
 if bad.startswith('/'):changed[0]['path']=bad
 with pytest.raises(ValueError):m.validate_rows(changed,specs)
 assert all(p.exists() for p in original_paths(case))

def test_directory_wrong_count_or_unknown_leaf_refused(case):
 root,specs,rows,destination,bindings=case;(root/specs[0][0]/'unreviewed.json').write_text('{}')
 with pytest.raises(ValueError,match='count'):m.roster(specs,h,root=root)

def test_hardlinked_copy_is_preserved(case):
 root,specs,rows,destination,bindings=case;path=original_paths(case)[0];link=root/'other-copy'
 try:os.link(path,link)
 except OSError:pytest.skip('Native hardlink permission required')
 with pytest.raises(ValueError,match='single-link'):m.roster(specs,h,root=root)
 assert path.exists() and link.exists()

def test_symlink_leaf_cannot_escape(case):
 root,specs,rows,destination,bindings=case;path=original_paths(case)[0];path.unlink()
 try:path.symlink_to(root/'original-proteome.fa')
 except OSError:pytest.skip('Native symlink permission required')
 with pytest.raises((ValueError,OSError)):m.roster(specs,h,root=root)
 assert (root/'original-proteome.fa').exists()

def test_foreign_owned_leaf_refused(case,monkeypatch):
 root,specs,rows,destination,bindings=case;path=original_paths(case)[0];actual=path.stat().st_uid;monkeypatch.setattr(m.os,'geteuid',lambda:actual+1,raising=False)
 with pytest.raises(ValueError,match='Owned ordinary'):m.file_observation(path.parent,path.name,root=root)
 assert path.exists()

def test_byte_ceiling_fails_before_any_pack(case,monkeypatch):
 root,specs,rows,destination,bindings=case;monkeypatch.setattr(m,'MAX_BYTES',1)
 with pytest.raises(ValueError,match='byte bound'):m.roster(specs,h,root=root)
 assert all(p.exists() for p in original_paths(case)) and not destination.exists()

def test_archive_sync_failure_keeps_every_original(case,monkeypatch):
 root,specs,rows,destination,bindings=case
 def fail(path):raise OSError('archive fsync failed')
 monkeypatch.setattr(h,'sync_archive',fail)
 with pytest.raises(OSError,match='fsync'):m.pack_and_remove(destination,rows,specs,{},h,bindings,root=root)
 assert all(p.exists() for p in original_paths(case)) and not (destination/'removal.json').exists()

def test_corrupted_tar_refused_before_any_unlink(case,monkeypatch):
 root,specs,rows,destination,bindings=case;real=h.sync_archive
 def corrupt(path):
  real(path)
  with Path(path).open('r+b') as stream:stream.write(b'wrong header')
 monkeypatch.setattr(h,'sync_archive',corrupt)
 with pytest.raises((ValueError,tarfile.TarError)):m.pack_and_remove(destination,rows,specs,{},h,bindings,root=root)
 assert all(p.exists() for p in original_paths(case))

def test_changed_original_after_pack_preserves_all_sources(case,monkeypatch):
 root,specs,rows,destination,bindings=case;real=h.verify_tar
 def change(*args,**kwargs):
  real(*args,**kwargs);original_paths(case)[-1].write_bytes(b'changed before first unlink')
 monkeypatch.setattr(h,'verify_tar',change)
 with pytest.raises(ValueError,match='Original changed'):m.pack_and_remove(destination,rows,specs,{},h,bindings,root=root)
 assert all(p.exists() for p in original_paths(case)) and not (destination/'removal.json').exists()

def test_retained_table_change_stops_next_unlink(case):
 root,specs,rows,destination,bindings=case;table=Path(next(iter(bindings)))
 def change(path):table.write_bytes(b'changed retained table')
 with pytest.raises(ValueError,match='Bound source/proof changed'):m.pack_and_remove(destination,rows,specs,{},h,bindings,root=root,after_unlink=change)
 journal=json.loads((destination/'removal.json').read_bytes());assert journal['removed_count']==1 and journal['status']=='interrupted_explicit_reconciliation_required' and sum(p.exists() for p in original_paths(case))==3

def test_interruption_requires_explicit_reconciliation_no_replay(case):
 root,specs,rows,destination,bindings=case
 def interrupt(path):raise RuntimeError('operator interruption')
 with pytest.raises(RuntimeError):m.pack_and_remove(destination,rows,specs,{},h,bindings,root=root,after_unlink=interrupt)
 journal=json.loads((destination/'removal.json').read_bytes());assert journal['removed_count']==1 and journal['last_removed']==rows[0]['path']
 receipt=json.loads((destination/'receipt.json').read_bytes());h.verify_tar(destination/'sequences.tar',rows,receipt['archive_sha256'])
 with pytest.raises(ValueError,match='Fresh archive'):m.pack_and_remove(destination,rows,specs,{},h,bindings,root=root)
 assert sum(p.exists() for p in original_paths(case))==3

@pytest.mark.parametrize('shape',['string','keyhash','locator','aggregate'])
def test_actual_leaf_reference_or_aggregate_binding_refused(case,shape):
 root,specs,rows,destination,bindings=case;scope=root/specs[0][0];leaf=scope/'OG0000001.fa'
 value={'string':{'input':str(leaf)},'keyhash':{str(leaf):'a'*64},'locator':{'path':str(leaf),'sha256':'a'*64},'aggregate':{'path':str(root/m.PRIMARY),'sha256':'a'*64}}[shape]
 with pytest.raises(ValueError):m.reference_check(value,[scope],root/'retained.json',root=root)

def test_exact_historical_output_command_permitted(case):
 root,specs,rows,destination,bindings=case;output=str(root/'data/molecular/orthology-repair-v7-20261013/of_run');value={'commands':[{'command':['x']*10+[output]}]};m.reference_check(value,[root/specs[0][0]],root/'receipt.json',root=root)
 with pytest.raises(ValueError):m.reference_check({'directory':output},[root/specs[0][0]],root/'receipt.json',root=root)

def test_native_owner_state_cpu_conflicts_refused():
 row='5455844|pwa209|def-ptewarie_cpu|COMPLETED|0:0|8|14551|cpu=8,mem=64G,node=1\n';assert m.native_proofs(row,{'5455844':8})['5455844']['elapsed_seconds']==14551
 for changed in [row.replace('pwa209','foreign'),row.replace('COMPLETED','FAILED'),row.replace('|8|','|1|'),row+row]:
  with pytest.raises(ValueError):m.native_proofs(changed,{'5455844':8})

def test_actual_maintenance_allocation_guard():
 from datetime import datetime,timezone
 script=Path('/support/archive.sbatch');end=datetime(2026,11,4,23,tzinfo=timezone.utc).timestamp();f={'JobId':'99','UserId':'pwa209(3164768)','JobState':'RUNNING','Account':'def-ptewarie_cpu','NumCPUs':'1','NumNodes':'1','MinMemoryNode':'8G','TimeLimit':'03:00:00','JobName':'hhw-orthology-sequence-archive-20261104','Command':str(script),'Requeue':'0','EndTime':'2026-11-04T23:00:00'}
 assert m.maintenance_allocation(f,'99',script,end,wall=lambda:end-700)==end
 for key,value in [('Command','wrong'),('Requeue','1'),('TimeLimit','04:00:00'),('NumCPUs','8'),('JobName','wrong')]:
  with pytest.raises(ValueError):m.maintenance_allocation({**f,key:value},'99',script,end,wall=lambda:end-700)

def metadata_fixture(case,monkeypatch):
 root,specs,rows,destination,bindings=case;documents=[];audited={}
 def write(path,value):
  path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,sort_keys=True));loc={'path':str(path),'sha256':m.sha(path)};audited[str(path)]={'status':'read','sha256':loc['sha256']};return loc
 for relative,(job,status) in m.COMPLETIONS.items():
  value={'status':status,'slurm_job_id':job,'source_sha256':m.SOURCE,'config_sha256':m.CONFIG};value['completion_hash']=m.canonical(value);documents.append(write(root/relative,value))
 extra=root/'immutable-source.json';documents.append(write(extra,{'verified_source':True}))
 tables=[]
 for path in sorted(m.expected_tables(root)):
  p=Path(path);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'preserve source table');tables.append({'path':path,'sha256':m.sha(p)})
 audit=root/'maintenance/orthology-storage-audit-20261104/result.json';audit.parent.mkdir(parents=True);audit.write_text(json.dumps({'status':'read_only_orthology_storage_audit_complete','native_job_id':'8508393','input_files_changed':False,'files_removed':0,'metadata':{'problems':[],'documents':audited},'inventory':{'anomalies':[],'required_original_tables':tables}}));monkeypatch.setattr(m,'AUDIT_SHA',m.sha(audit))
 return {'audit':{'path':str(audit),'sha256':m.sha(audit)},'binding_documents':documents,'retained_tables':tables}

def test_complete_bounded_immutable_graph_and_four_table_pins(case,monkeypatch):
 root,*_=case;request=metadata_fixture(case,monkeypatch);bindings,audit=m.metadata_authority(request,h,root=root)
 assert len(bindings)==9 and set(m.expected_tables(root))<=set(bindings)

@pytest.mark.parametrize('change',['omit_source','table_changed','completion_job','completion_hash','candidate_ref'])
def test_metadata_or_completion_conflict_refuses_archival(case,monkeypatch,change):
 root,*_=case;request=metadata_fixture(case,monkeypatch)
 if change=='omit_source':request['binding_documents'].pop()
 elif change=='table_changed':Path(request['retained_tables'][0]['path']).write_bytes(b'changed original source table')
 else:
  loc=request['binding_documents'][0];path=Path(loc['path']);value=json.loads(path.read_bytes())
  if change=='completion_job':value['slurm_job_id']='wrong'
  elif change=='completion_hash':value['completion_hash']='f'*64
  else:value['future_input']=str(root/m.SPECS[0][0]/'OG0000001.fa')
  path.write_text(json.dumps(value));loc['sha256']=m.sha(path)
 with pytest.raises(ValueError):m.metadata_authority(request,h,root=root)

def test_removal_journal_batches_every_128_and_at_completion(case,monkeypatch):
 root,specs,rows,destination,bindings=case;directory=root/specs[0][0]
 for i in range(260):(directory/f'OG{i:07d}.fa').write_bytes(b'>copy\nACD\n')
 specs=[(specs[0][0],260),*specs[1:]];rows=m.roster(specs,h,root=root);writes=[];real=h.atomic
 def record(path,value):
  if Path(path).name=='removal.json':writes.append((value['status'],value['removed_count']))
  return real(path,value)
 monkeypatch.setattr(h,'atomic',record);receipt,journal=m.pack_and_remove(destination,rows,specs,{},h,bindings,root=root)
 assert writes==[('removal_intent',0),('removing_verified_orthology_sequence_copies',128),('removing_verified_orthology_sequence_copies',256),('complete_verified_orthology_sequence_copy_compaction',263)] and journal['removed_count']==263

def tasks_fixture(case):
 root,*_=case;items=[]
 for job,name in [('5455844','molecular-pilot'),('6632058','mantis-orthology')]:
  directory=root/'plans'/name;directory.mkdir(parents=True);script=directory/'worker.sbatch';text='#!/bin/bash\npython original-method.py\n';script.write_text(text)
  task={'name':name,'script':str(script),'script_sha256':m.canonical(text),'resources':{'cpus':8,'gpus':0,'memory_gb':64}}
  plan={'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]};path=directory/'plan.json';path.write_text(json.dumps(plan));ledger=root/'state'/f'{name}-submission.json';ledger.parent.mkdir(exist_ok=True);ledger.write_text(json.dumps({'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,'plan_sha256':m.canonical(plan),'tasks':{name:{'state':'submitted','job_id':job,'script_sha256':m.canonical(text),'command':['sbatch','--parsable',str(script)]},'unrelated':{'job_id':'another'}}}))
  items.append({'job':job,'task_name':name,'ledger_path':str(ledger),'plan':{'path':str(path),'sha256':m.sha(path)},'script':{'path':str(script),'sha256':m.sha(script)}})
 return {'original_tasks':items}

def test_task_projection_accepts_unrelated_ledger_evolution(case):
 root,*_=case;request=tasks_fixture(case);first=m.original_tasks(request,h,root=root)
 path=Path(request['original_tasks'][0]['ledger_path']);value=json.loads(path.read_bytes());value['tasks']['unrelated']={'state':'complete','job_id':'another'};path.write_text(json.dumps(value))
 second=m.original_tasks(request,h,root=root);assert first==second

@pytest.mark.parametrize('change',['selected_job','script','plan_source'])
def test_changed_original_task_projection_refused(case,change):
 root,*_=case;request=tasks_fixture(case);item=request['original_tasks'][0]
 if change=='selected_job':
  path=Path(item['ledger_path']);v=json.loads(path.read_bytes());v['tasks'][item['task_name']]['job_id']='wrong';path.write_text(json.dumps(v))
 elif change=='script':Path(item['script']['path']).write_text('different source')
 else:
  path=Path(item['plan']['path']);v=json.loads(path.read_bytes());v['release_identity']['sha256']='wrong';path.write_text(json.dumps(v));item['plan']['sha256']=m.sha(path)
 with pytest.raises(ValueError):m.original_tasks(request,h,root=root)

def gate_fixture(root):
 script=root/'plans/study-census-resilience-v1-20261024/rolling-storage-census-61.sbatch';script.parent.mkdir(parents=True);script.write_text('original census script')
 gate={'job_id':'6983037','archive_job_id':'99','script':{'path':str(script),'sha256':m.sha(script)}}
 fields={'JobId':'6983037','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'PENDING','JobName':'hhw-rolling-storage-census-61','Command':str(script),'NumCPUs':'8','NumNodes':'1','MinMemoryNode':'64G','TimeLimit':'06:00:00','Dependency':'afterany:99(unfulfilled)'}
 queue='\n'.join(f'{job}|pwa209|def-ptewarie_cpu|PENDING' for job in ['6983037','6983056','6983059','6983060'])
 return gate,fields,queue

@pytest.mark.parametrize('dependency',['afterany:99(unfulfilled)','afterany:6983031(fulfilled),afterany:99(unfulfilled)','afterany:6983031:99'])
def test_only_and_gate_guarantees_census_waits(case,dependency):
 root,*_=case;gate,fields,queue=gate_fixture(root);m.gate_fields({**fields,'Dependency':dependency},queue,gate,'99',root=root)

@pytest.mark.parametrize('dependency',['afterany:99?afterany:6983031','afterany:99(unfulfilled)?afterok:1','afterok:99','afterany:99(unfulfilled),unknown:2','afterany:99garbage','afterany:6983031','afterany:99,afterany:99','afterany:99,afterany:12345'])
def test_or_unknown_missing_or_malformed_dependency_refused(case,dependency):
 root,*_=case;gate,fields,queue=gate_fixture(root)
 with pytest.raises(ValueError):m.gate_fields({**fields,'Dependency':dependency},queue,gate,'99',root=root)

@pytest.mark.parametrize('change',['first_running','later_completing','owner','script','cpu','memory'])
def test_live_census_identity_or_active_state_refused(case,change):
 root,*_=case;gate,fields,queue=gate_fixture(root)
 if change=='first_running':fields['JobState']='RUNNING'
 elif change=='later_completing':queue=queue.replace('6983056|pwa209|def-ptewarie_cpu|PENDING','6983056|pwa209|def-ptewarie_cpu|COMPLETING')
 elif change=='owner':fields['UserId']='foreign(1)'
 elif change=='script':fields['Command']='wrong'
 elif change=='cpu':fields['NumCPUs']='1'
 else:fields['MinMemoryNode']='8G'
 with pytest.raises(ValueError):m.gate_fields(fields,queue,gate,'99',root=root)

def test_runtime_boundary_queries_before_entry_and_at_60_seconds(case):
 import shlex,subprocess
 root,specs,rows,destination,bindings=case;gate,fields,queue=gate_fixture(root);calls=[];clock=[1.]
 registration={'job_id':'99','request_sha256':'a'*64,'wrapper_sha256':'b'*64,'census_gate':gate}
 def runner(command,**kwargs):
  calls.append(command[0]);output=' '.join(k+'='+shlex.quote(v) for k,v in fields.items()) if command[0]=='scontrol' else queue
  return subprocess.CompletedProcess(command,0,output,'')
 checkpoint,boundary=m.checked_boundary(registration,h,runner,root=root,job='99',request_sha='a'*64,wrapper_sha='b'*64,record_path=root/'gate-observation.json',deadline=lambda:None,guard=lambda:None,clock=lambda:clock[0],wall=lambda:100.)
 assert calls==['scontrol','squeue'];checkpoint();assert len(calls)==2
 clock[0]=62.;checkpoint();assert len(calls)==4;boundary();assert len(calls)==6
 fields['JobState']='RUNNING'
 with pytest.raises(ValueError):boundary()

def test_removal_boundary_failure_retains_every_original(case):
 root,specs,rows,destination,bindings=case;calls=[]
 def boundary():
  calls.append(True)
  if len(calls)==2:raise ValueError('census became active before unlink')
 with pytest.raises(ValueError,match='census became active'):m.pack_and_remove(destination,rows,specs,{},h,bindings,root=root,boundary=boundary)
 assert len(calls)==2 and all(p.exists() for p in original_paths(case)) and (destination/'index.json').exists() and not (destination/'removal.json').exists()
