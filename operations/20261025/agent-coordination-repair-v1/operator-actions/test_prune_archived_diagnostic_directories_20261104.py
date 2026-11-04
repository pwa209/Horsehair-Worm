from pathlib import Path
import hashlib,importlib.util,json,os,tarfile
import pytest

def module(name,file):
 spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(file));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
m=module('pruner','prune-archived-diagnostic-directories-20261104.py')
h=module('archive','compact-terminal-controller-reads-20261104.py')

def write(path,value):
 path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,sort_keys=True));return {'path':str(path),'sha256':m.sha(path)}

@pytest.fixture
def fixture(tmp_path,monkeypatch):
 root=tmp_path/'root';root.mkdir();source=root/m.CAMPAIGN.relative_to(m.ROOT)/'controller-0-1000000';source.mkdir(parents=True)
 # Windows cannot fsync a POSIX directory descriptor; native tests use the
 # unchanged real helper implementation and must have no skipped link tests.
 if os.name!='posix':monkeypatch.setattr(h,'sync_directory',lambda path:None)
 return root,source

def row(source,name='accounting-reads/read-abc/attempt-1-result.json'):
 path=source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'{"evidence":"exact bytes"}')
 return {'path':name,**h.observed(path,source.parents[3])}

def owner(job,state='COMPLETED',exit_code='0:0'):
 return {'job_id_raw':job,'user':'pwa209','account':'def-ptewarie_cpu','state':state,'exit_code':exit_code,'cpus':1,'elapsed_seconds':10,'alloc_tres':'cpu=1,mem=8G,node=1','gpus':0}

def thirty(tmp_path,monkeypatch):
 root=tmp_path/'root';root.mkdir();monkeypatch.setattr(h,'registration_binding',lambda registration,job,root: {'checked':job})
 if os.name!='posix':monkeypatch.setattr(h,'sync_directory',lambda path:None)
 native={};costs={};entries=[];maps={};jobs=['7046213','6689295','6701493']+[str(1000000+x) for x in range(27)]
 for index,job in enumerate(jobs):
  producer='8425411' if index<3 else '8463111';expected=m.PRODUCERS[producer];base=root/'maintenance'/expected['case']
  source=root/(m.CAMPAIGN.relative_to(m.ROOT))/('controller-0-'+job);source.mkdir(parents=True)
  path=source/'accounting-reads/read-abc/attempt-1-result.json';path.parent.mkdir(parents=True);path.write_bytes(b'{"a":1}')
  rows=[{'path':path.relative_to(source).as_posix(),**h.observed(path,root)}];record=owner(job);native[job]=record;costs[job]=record
  authority={'kind':'actual_terminal_controller_diagnostics','native':{'job_id':job,**record},'wrapper_sha256':expected['wrapper'],'request_sha256':'1'*64,'original_registration':{'registration':{}}}
  directory=base/('controller-'+job);base.mkdir(parents=True,exist_ok=True);receipt=h.pack(source,directory,rows,authority,root=root);h.remove(source,directory,rows,receipt,root=root)
  entry={'job':job,'producer_job':producer,'archive_directory':str(directory)}
  for key in ('intent','receipt','removal'):entry[key]={'path':str(directory/(key+'.json')),'sha256':m.sha(directory/(key+'.json'))}
  entries.append(entry)
  maps.setdefault(producer,{'base':base,'wrapper_sha256':expected['wrapper'],'request_sha256':'1'*64,'completed':{}})['completed'][job]={'job':job,'files_removed':1,'archive':str(directory),'archive_sha256':receipt['archive_sha256']}
 return root,{'transactions':entries},maps,native,costs

def test_only_exact_derived_parent_chain_removed(fixture):
 root,source=fixture;leaf=source/'accounting-reads/read-a/attempt-1-result.json';leaf.parent.mkdir(parents=True);leaf.write_bytes(b'{}');rows=[{'path':leaf.relative_to(source).as_posix(),**h.observed(leaf,root)}]
 candidates=m.parent_candidates(source,rows,h,root=root);leaf.unlink();result=m.prune(candidates,h,root=root)
 assert result['removed']==2 and source.is_dir() and not (source/'accounting-reads').exists()

def test_nonempty_and_bound_file_ancestor_preserved(fixture):
 root,source=fixture;folder=source/'accounting-reads/read-a';folder.mkdir(parents=True);bound=folder/'bound.json';bound.write_text('keep')
 result=m.prune({folder:source,folder.parent:source},h,root=root)
 assert result['removed']==0 and result['not_empty']==2 and bound.read_text()=='keep'

def test_protected_empty_directory_preserved(fixture):
 root,source=fixture;folder=source/'accounting-reads/read-a';folder.mkdir(parents=True)
 result=m.prune({folder:source,folder.parent:source},h,root=root,protected=[folder]);assert result['protected']==2 and folder.is_dir()

@pytest.mark.parametrize('name',['../outside/attempt-1-result.json','/absolute/attempt-1-result.json','accounting-reads/read-a/checkpoint.json'])
def test_injected_member_escape_or_nonreader_refused(fixture,name):
 root,source=fixture
 with pytest.raises(ValueError):m.parent_candidates(source,[{'path':name,'sha256':'1'*64,'bytes':0,'identity':[0,0,0,0,0]}],h,root=root)

def test_controller_root_cannot_be_pruned(fixture):
 root,source=fixture
 with pytest.raises(ValueError,match='Confined member ancestor'):m.prune({source:source},h,root=root)
 assert source.is_dir()

def test_foreign_mapping_refused(fixture):
 root,source=fixture;outside=root/'other';outside.mkdir()
 with pytest.raises(ValueError,match='Confined member ancestor'):m.prune({outside:source},h,root=root)
 assert outside.is_dir()

def test_symlink_parent_cannot_escape(fixture):
 root,source=fixture;outside=root/'outside';outside.mkdir();link=source/'accounting-reads'
 try:link.symlink_to(outside,target_is_directory=True)
 except OSError:pytest.skip('Native link permissions required')
 with pytest.raises(ValueError):m.prune({link:source},h,root=root)
 assert outside.is_dir() and link.is_symlink()

def test_absent_parent_is_safe_idempotent_observation(fixture):
 root,source=fixture;candidate=source/'accounting-reads/read-missing';result=m.prune({candidate:source},h,root=root)
 assert result['already_absent']==1 and result['removed']==0

def test_atomic_nonempty_race_keeps_file(fixture,monkeypatch):
 root,source=fixture;folder=source/'accounting-reads';folder.mkdir();real=m.remove_empty_directory
 def raced(path,root):
  (path/'new.json').write_text('keep');real(path,root=root)
 monkeypatch.setattr(m,'remove_empty_directory',raced);result=m.prune({folder:source},h,root=root)
 assert result['not_empty']==1 and (folder/'new.json').read_text()=='keep'

def test_real_thirty_archive_roundtrips_and_no_replay(tmp_path,monkeypatch):
 root,request,maps,native,costs=thirty(tmp_path,monkeypatch)
 monkeypatch.setattr(h,'pack',lambda *a,**k:pytest.fail('archive replay'));monkeypatch.setattr(h,'remove',lambda *a,**k:pytest.fail('file removal replay'))
 candidates,verified=m.verify_transactions(request,maps,native,costs,h,root=root)
 assert len(verified)==30 and len(candidates)==60 and all(not any(p.glob('*.json')) for p in candidates)
 result=m.prune(candidates,h,root=root);assert result['removed']==60 and all(Path(x['archive_directory']).is_dir() for x in request['transactions'])

@pytest.mark.parametrize('change',['receipt','journal','archive','reappeared','native','frontier','duplicate'])
def test_changed_transaction_cannot_authorize_any_pruning(tmp_path,monkeypatch,change):
 root,request,maps,native,costs=thirty(tmp_path,monkeypatch);item=request['transactions'][-1];directory=Path(item['archive_directory']);job=item['job'];intent=json.loads((directory/'intent.json').read_bytes())
 if change=='receipt':(directory/'receipt.json').write_text('{}')
 elif change=='journal':
  value=json.loads((directory/'removal.json').read_bytes());value['removed_count']=0;item['removal']=write(directory/'removal.json',value)
 elif change=='archive':
  with (directory/'evidence.tar').open('r+b') as stream:stream.write(b'corruption')
 elif change=='reappeared':Path(intent['source']).joinpath(intent['members'][0]['path']).write_text('new')
 elif change=='native':native[job]={**native[job],'user':'foreign'}
 elif change=='frontier':maps[item['producer_job']]['completed'][job]['files_removed']=2
 elif change=='duplicate':request['transactions'][-1]=request['transactions'][0]
 with pytest.raises((ValueError,KeyError,tarfile.TarError)):m.verify_transactions(request,maps,native,costs,h,root=root)
 assert Path(intent['source']).is_dir()

def test_request_canonical_pin_mismatch_fatal(fixture):
 root,source=fixture;campaign=root/m.CAMPAIGN.relative_to(m.ROOT);documents=[]
 for name in ('state.json.gz','campaign-plan.json','state-binding.json'):documents.append(write(campaign/name,{'same':True}))
 (campaign/'state.json.gz').write_bytes(b'changed')
 with pytest.raises(ValueError,match='Pinned archive/request proof changed'):m.verify_bindings({'binding_documents':documents},h,root=root)

def test_actual_allocation_guard():
 fields={'JobId':'99','UserId':'pwa209(3164768)','JobState':'RUNNING','Account':'def-ptewarie_cpu','NumCPUs':'1','NumNodes':'1','MinMemoryNode':'8G','TimeLimit':'02:00:00','Command':str(Path('/support/prune.sbatch')),'JobName':'hhw-empty-diagnostic-directory-prune-20261104','EndTime':'2026-11-04T20:00:00'}
 end=1793822400.0
 from datetime import datetime,timezone
 end=datetime(2026,11,4,20,tzinfo=timezone.utc).timestamp()
 assert m.maintenance_allocation(fields,'99',Path('/support/prune.sbatch'),end,wall=lambda:end-700)==end
 for key,value in [('Command','/wrong.sbatch'),('JobName','wrong'),('NumCPUs','2'),('MinMemoryNode','16G'),('TimeLimit','04:00:00')]:
  with pytest.raises(ValueError):m.maintenance_allocation({**fields,key:value},'99',Path('/support/prune.sbatch'),end,wall=lambda:end-700)


def producer_fixture(tmp_path):
 root=tmp_path/'root';root.mkdir();entries=[];native={};costs={}
 for job,e in m.PRODUCERS.items():
  base=root/'maintenance'/e['case'];support=root/'maintenance'/e['support'];request_pin=write(support/'request.json',{'case':str(base)})
  registration=write(support/'registration.json',{'job_id':job,'request_sha256':request_pin['sha256']})
  script=support/'controller-compact.sbatch';script.write_text(e['wrapper']+' '+request_pin['sha256']+' '+e['helper'])
  proof={'completed':[{'job':str(1000000+x)} for x in range(e['count'])]}
  if job=='8463111':proof.update(status='terminal_controller_diagnostic_recovery_incomplete',minimum_met=False,files_removed=33782,prior_verified_files_removed=13130,current_pass_files_removed=20652,ineligible_sources=[{'job':'6950429'},{'job':'7028292'}],prior_verified=[{'job':x} for x in ['7046213','6689295','6701493']],canonical_state_sha256='c'*64)
  proof_pin=write(base/e['proof'],proof);entries.append({'job':job,'archive_base':str(base),'registration':registration,'request':request_pin,'script':{'path':str(script),'sha256':m.sha(script)},'proof':proof_pin})
  record=owner(job,e['state'],e['exit']);native[job]=record;costs[job]=record
 return root,{'producers':entries},native,costs

def test_truthful_producer_failure_and_incomplete_outcomes_preserved(tmp_path):
 root,request,native,costs=producer_fixture(tmp_path);result=m.producers(request,native,costs,h,root=root)
 assert result['8425411']['native']['state']=='FAILED' and result['8463111']['proof_status']=='terminal_controller_diagnostic_recovery_incomplete' and result['8463111']['ineligible_source_jobs']==['6950429','7028292']

@pytest.mark.parametrize('change',['wrong_state','false_complete','false_minimum','omitted_stale','wrong_registration','wrong_script'])
def test_producer_conflict_rejected_before_directory_mutation(tmp_path,change):
 root,request,native,costs=producer_fixture(tmp_path);item=request['producers'][-1]
 if change=='wrong_state':native['8463111']={**native['8463111'],'state':'FAILED'}
 elif change=='wrong_registration':item['registration']=write(Path(item['registration']['path']),{'job_id':'another','request_sha256':item['request']['sha256']})
 elif change=='wrong_script':
  path=Path(item['script']['path']);path.write_text('different script');item['script']['sha256']=m.sha(path)
 else:
  path=Path(item['proof']['path']);value=json.loads(path.read_bytes())
  if change=='false_complete':value['status']='terminal_controller_diagnostic_recovery_complete'
  elif change=='false_minimum':value['minimum_met']=True
  else:value['ineligible_sources']=[]
  item['proof']=write(path,value)
 with pytest.raises(ValueError):m.producers(request,native,costs,h,root=root)
