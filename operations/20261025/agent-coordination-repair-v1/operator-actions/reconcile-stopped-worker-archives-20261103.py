"""Finish only a stopped maintenance pass's published archive transactions."""
from pathlib import Path
import hashlib,importlib.util,json,os,subprocess,time
from horsehair import budget,repair,slurm
from horsehair.core import source_identity

r=Path('/scratch/pwa209/horsehair-worm');old_job='8403868'
assert os.geteuid()==3164768 and os.environ['SLURM_JOB_ID']!=old_job
assert subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec')
assert source_identity(r/'releases/20261021-6ed201653dfa-4f3903d7c26e')['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
s=Path(__file__).parent;helper=r/'maintenance/terminal-diagnostic-compaction-support-v2-20261103/verified-evidence-archive-20261021.py'
assert hashlib.sha256(helper.read_bytes()).hexdigest()=='045d8650db62c0fa52d7f56bb7bdb2669729509c23ecfbbc847e73cbbff8d42a'
end=float(os.environ['SLURM_JOB_END_TIME']);stop=min(time.monotonic()+900,time.monotonic()+end-time.time()-600)
def check():
    if time.time()+300>=end:raise TimeoutError('Maintenance finalization reserve exhausted')
def run(cmd,**kwargs):return subprocess.run(cmd,**{**kwargs,'timeout':90})
while True:
    check();rows=repair._repair_native_states([old_job],{},owner='pwa209',account='def-ptewarie_cpu',runner=run);row=rows.get(old_job,{})
    if row.get('state') in {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}:
        assert row['job_id_raw']==old_job and row['user']=='pwa209' and row['account']=='def-ptewarie_cpu' and row['cpus']==1 and row['elapsed_seconds']>0 and 'gpu' not in row['alloc_tres'].lower();break
    assert time.monotonic()+20<stop,'Actual final accounting for the stopped maintenance pass is pending'
    time.sleep(20)
budget.native_accounting(r,[old_job],runner=run)
assert slurm.completed_native_dependency(r,old_job,dependency_kind='afterany') is not None
spec=importlib.util.spec_from_file_location('original_qualified_archive_helper',helper);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
canary={'path':str(r/'maintenance/study-af-evidence-archive-canary-v1-20261021/finalized-native-proof/canary.json'),'sha256':'921635cbeac2b954c7946c7a6ddd3c375a8ecd862630492d1aa0ee26b58ac14b'}
m._verify_canary(canary,r)
out=[];case=r/'maintenance/terminal-diagnostic-compaction-20261103'
for p in sorted(case.glob('worker-*')):
    check();assert p.is_dir() and not p.is_symlink()
    intent=m.strict_json((p/'pack-intent.json').read_bytes());assert intent['kind']=='terminal-native-worker-reader-evidence'
    source=Path(intent['source']);m.verify_authority(intent['authority'],source,r)
    receipt=p/'pack-receipt.json';journal=p/'removal-journal.json'
    if not receipt.exists():
        assert not journal.exists(),'Removal cannot precede a published verified archive'
        for item in intent['members']:
            check();m.reader_evidence_name(item['path']);path=m.confined(source/item['path'],r,file=True)
            observed,_=m.file_hash(path,checkpoint=check);assert observed=={k:item[k] for k in ['bytes','sha256']}
        out.append({'archive':str(p),'status':'unpublished_pack_retained_all_originals_verified','files':len(intent['members'])});continue
    value=m.strict_json(receipt.read_bytes());m.verify_archive(p,value['index_sha256'],value['archive_sha256'],checkpoint=check)
    if journal.exists():
        original=m.strict_json(journal.read_bytes())
        if original['status']=='complete_originals_compacted':
            out.append({'archive':str(p),'status':'previous_compaction_verified','files':len(original['removed'])});continue
        assert original['native_job_id']==old_job
        result=m.remove_verified_originals(p,value['index_sha256'],value['archive_sha256'],removal_canary=canary,root=r,explicit_reconcile=True,expected_journal_sha256=hashlib.sha256(journal.read_bytes()).hexdigest(),checkpoint=check)
        assert result['status']=='complete_originals_compacted'
        out.append({'archive':str(p),'status':'explicit_interrupted_removal_reconciled','files':len(result['removed'])})
    else:
        for item in intent['members']:
            check();path=m.confined(source/item['path'],r,file=True);observed,_=m.file_hash(path,checkpoint=check);assert observed=={k:item[k] for k in ['bytes','sha256']}
        out.append({'archive':str(p),'status':'verified_archive_originals_retained','files':len(intent['members'])})
with (s/'stopped-worker-archive-reconciliation.json').open('x') as f:json.dump({'native_terminal':row,'transactions':out,'scientific_files_changed':False},f,indent=2);f.flush();os.fsync(f.fileno())
print('STOPPED_WORKER_ARCHIVES_VERIFIED '+json.dumps(out),flush=True)
