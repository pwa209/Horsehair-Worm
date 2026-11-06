"""Finite release metadata/lineage/native-ack fixtures; never contact Slurm."""
from pathlib import Path
from datetime import datetime,timezone
import copy,importlib.util,json,subprocess
import pytest

AREA=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('finite_census_release',AREA/'release_census.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes((json.dumps(value,sort_keys=True,indent=2)+'\n').encode())
    return {'path':str(path),'sha256':m.sha(path)}

def native(job,script,name,cpus='8',memory='64G',limit='06:00:00'):
    return {'JobId':job,'UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','Command':str(script),'JobName':name,'NumCPUs':cpus,
        'NumNodes':'1','MinMemoryNode':memory,'TimeLimit':limit,'Requeue':'0','JobState':'PENDING','Reason':'JobHeldUser','Priority':'0',
        'ReqTRES':'cpu='+cpus+',mem='+memory+',node=1','Dependency':'afterany:100(unfulfilled)'}

@pytest.fixture
def chain(tmp_path):
    root=tmp_path.resolve();support=root/'maintenance/storage-reserve-200k-v2-20261106/census';support.mkdir(parents=True)
    bindings={}
    for name,key in [('run-fresh-storage-census-resilience-v1-20261024.py','helper'),('source-manifest.json','manifest')]:
        p=support/name;p.write_bytes((AREA/'census'/name).read_bytes());bindings[key]={'path':str(p),'sha256':m.sha(p)}
    directory=root/'plans'/m.PHASE;directory.mkdir(parents=True);planpath=directory/'plan.json';ledgerpath=root/'state'/(m.PHASE+'-submission.json')
    tasks=[];rows={};jobs=[str(1000+i) for i in range(1,65)];anchor=datetime(2026,11,6,12,tzinfo=timezone.utc)
    for index,job in enumerate(jobs,1):
        name='rolling-storage-census-'+str(index).zfill(2);p=directory/(name+'.sbatch')
        flags={'--source-manifest':bindings['manifest']['path'],'--source-manifest-sha256':m.MANIFEST,'--native-plan':str(planpath),
            '--native-ledger':str(ledgerpath),'--native-script':str(p),'--native-task':name,'--kernel-qualification-job-id':'900'}
        import shlex
        command='python -B '+shlex.join([bindings['helper']['path'],*[item for pair in flags.items() for item in pair]])
        start='' if index==1 else '#SBATCH --begin='+datetime.fromtimestamp(anchor.timestamp()+14400*(index-1),timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')+'\n'
        text='#!/bin/bash\n'+start+'#SBATCH --cpus-per-task=8\n#SBATCH --mem=64G\n#SBATCH --time=06:00:00\n'+command+'\n';p.write_bytes(text.encode())
        tasks.append({'name':name,'script':str(p),'command':command,'script_sha256':m.canonical(text),
            'resources':{'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None},'depends':[] if index==1 else [tasks[-1]['name']],
            'dependency_kind':'afterok' if index==1 else 'afterany','rolling_census_provider':{'helper':bindings['helper'],'source_manifest':bindings['manifest'],'case_id':'rolling-'+str(index).zfill(2)}})
        rows[name]={'job_id':job,'state':'submitted','script_sha256':m.canonical(text),'dependency_kind':tasks[-1]['dependency_kind'],
            'issued_native_dependencies':['900' if index==1 else jobs[index-2]],'completed_native_dependencies':[]}
    plan={'root':str(root),'phase':m.PHASE,'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':tasks}
    ledger={'config_sha256':m.CONFIG,'source_sha256':m.SOURCE,'plan_sha256':m.canonical(plan),'tasks':rows}
    bindings.update(plan=write(planpath,plan),ledger=write(ledgerpath,ledger),jobs=jobs)
    return root,bindings,plan,ledger

def test_complete64_source_bound_chain_passes(chain):
    root,b,_,_=chain;tasks,pins=m.verify_plan(b,root);assert len(tasks)==64 and len(pins)==68
    assert tasks[0]['kind']=='afterok' and tasks[1]['dependency']==b['jobs'][0]

def test_duplicate_or_missing_registered_job_refuses(chain):
    root,b,_,_=chain;b['jobs'][-1]=b['jobs'][0]
    with pytest.raises(ValueError):m.verify_plan(b,root)

def test_changed_original_script_refuses_before_mutation(chain):
    root,b,plan,_=chain;Path(plan['tasks'][7]['script']).write_text('# changed fixture\n')
    with pytest.raises(ValueError):m.verify_plan(b,root)

def test_foreign_native_owner_or_unheld_or_gpu_refuses():
    row=native('1','/owned/script','hhw-case')
    for changed in [{'UserId':'other(99)'},{'Reason':'Priority','Priority':'8'},{'ReqTRES':'cpu=8,gres/gpu=1'}]:
        with pytest.raises(ValueError):m.native_identity({**row,**changed},'1','/owned/script','hhw-case')

def test_dependency_cannot_be_replaced_with_or_or_unrelated_job():
    task={'dependency':'20','kind':'afterany'}
    for dep in ['afterany:20?afterany:21','afterany:21(unfulfilled)']:
        with pytest.raises(ValueError):m.dependency_guard({'Dependency':dep},task,lambda job:None)

def test_cleared_dependency_needs_positive_terminal_and_success_for_afterok():
    m.dependency_guard({'Dependency':'(null)'},{'dependency':'20','kind':'afterany'},lambda job:{'job':job,'state':'FAILED','exit':'1:0'})
    with pytest.raises(ValueError):m.dependency_guard({'Dependency':'(null)'},{'dependency':'20','kind':'afterok'},lambda job:{'job':job,'state':'FAILED','exit':'1:0'})

def test_readiness_anchor_never_advances_original_minimum(chain):
    root,b,_,_=chain;tasks,_=m.verify_plan(b,root);anchor=datetime(2026,11,5,0,tzinfo=timezone.utc);wanted=m.targets(tasks,anchor)
    assert len(wanted)==63 and b['jobs'][0] not in wanted
    assert all(wanted[t['job']]>=t['minimum'] and wanted[t['job']]>=anchor.timestamp()+14400*(t['index']-1) for t in tasks[1:])

def test_actual_text_and_json_time_proofs_require_exact_owned_job():
    wanted=datetime(2026,11,6,16,tzinfo=timezone.utc).timestamp();assert m.time_proof({'EligibleTime':'2026-11-06T16:00:00'},wanted)
    record={'jobs':[{'job_id':12,'eligible_time':{'set':True,'number':wanted}}]};assert m.json_time_proof(record,'12',wanted)
    record['jobs'][0]['eligible_time']['set']=False;assert m.json_time_proof(record,'12',wanted) is None
    with pytest.raises(ValueError):m.json_time_proof(record,'13',wanted)

@pytest.fixture
def cleanup(tmp_path):
    root=tmp_path.resolve();area=root/'maintenance/continuation';area.mkdir(parents=True)
    source=area/'continue.py';source.write_bytes(b'# inert source fixture\n');request=write(area/'request.json',{'fixture':True})
    script=area/'continue.sbatch';script.write_bytes(('python '+str(source)+' --request '+request['path']+' --request-sha256 '+request['sha256']+'\n').encode())
    sourcebinding={'path':str(source),'sha256':m.sha(source)};scriptbinding={'path':str(script),'sha256':m.sha(script)}
    job='700';record={'job_id':job,'source_sha256':sourcebinding['sha256'],'request_sha256':request['sha256'],'script_sha256':scriptbinding['sha256'],
        'actual_held_native':native(job,script,'hhw-orthology-archive-continuation-20261106','1','8G','12:00:00')}
    reg=write(area/'registration.json',record);case=root/'results/storage-archives/orthology-sequences-20261104/continuation-20261106'
    receipt=write(case/'continuation-receipt.json',{'status':'independent_existing_tar_roundtrip_verified_originals_retained','files':43145,'original_archive_sha256':m.ARCHIVE})
    journal=write(case/'removal.json',{'status':'complete_verified_existing_orthology_archive_removal','removed_count':43145,'archive_sha256':m.ARCHIVE})
    result={'status':'complete_verified_orthology_sequence_copy_archive_continuation','files_removed':43145,'native_job_id':job,
        'source_sha256':sourcebinding['sha256'],'request_sha256':request['sha256'],'original_archive_sha256':m.ARCHIVE,'original_wrapper_sha256':m.ORIGINAL_SOURCE,
        'original_request_sha256':m.ORIGINAL_REQUEST,'original_inputs_preserved':True,'no_repacking':True,'scientific_result':False,
        'continuation_receipt_sha256':receipt['sha256'],'removal_journal_sha256':journal['sha256']}
    binding=write(case/'result.json',result)
    c={'job_id':job,'source':sourcebinding,'request':request,'registration':reg,'script':scriptbinding,'result_path':binding['path']}
    return root,c,{'job':job,'state':'COMPLETED','exit':'0:0','cpus':1,'elapsed':900},result,case

def test_complete_native_continuation_and_archive_lineage_pass(cleanup):
    root,c,row,result,_=cleanup;observed,binding=m.verify_cleanup(c,row,root);assert observed==result and binding['sha256']==m.sha(c['result_path'])

def test_failed_native_continuation_cannot_authorize_releases(cleanup):
    root,c,row,_,_=cleanup
    with pytest.raises(ValueError):m.verify_cleanup(c,{**row,'state':'FAILED','exit':'1:0'},root)

def test_partial_or_changed_removal_journal_refuses(cleanup):
    root,c,row,_,case=cleanup;write(case/'removal.json',{'status':'interrupted','removed_count':128})
    with pytest.raises(ValueError):m.verify_cleanup(c,row,root)

def test_uncertain_mutation_is_recorded_once_and_never_replayed(tmp_path):
    calls=[]
    def uncertain(cmd,**kw):calls.append(cmd);raise subprocess.TimeoutExpired(cmd,30)
    path=tmp_path/'release-1';assert m.issue_once(path,['scontrol','release','1'],uncertain)['timed_out']
    with pytest.raises(FileExistsError):m.issue_once(path,['scontrol','release','1'],uncertain)
    assert len(calls)==1 and path.with_suffix('.intent.json').exists() and path.with_suffix('.response.json').exists()
