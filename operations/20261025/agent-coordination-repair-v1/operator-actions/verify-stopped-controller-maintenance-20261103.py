"""Read-only frontier verification before a new finite maintenance allocation."""
from pathlib import Path
import argparse,hashlib,importlib.util,json,os,re,stat,subprocess,time

ROOT=Path('/scratch/pwa209/horsehair-worm')
OLD_JOB='8420757'
OLD_SOURCE_SHA='3e99bc89133cd06f4f3c120afd4653ba60fe9743530f0da8fda3df57e451ca60'
OLD_SUPPORT=ROOT/'maintenance/terminal-controller-diagnostic-support-v2-20261103'
OLD_CASE=ROOT/'maintenance/terminal-controller-diagnostic-compaction-v2-20261103'
AUDIT=ROOT/'maintenance/controller-compaction-deadline-repair-20261103/before.json'

def require(value,message):
    if not value:raise ValueError(message)

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def identity(info):return [info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns]

def present(path):return Path(path).exists() or Path(path).is_symlink()

def ordinary_directory(path,root,uid):
    path=Path(path);root=Path(root)
    require(path.is_absolute() and path.is_relative_to(root),'Confined evidence directory required')
    current=path
    while True:
        info=current.lstat();require(stat.S_ISDIR(info.st_mode) and info.st_uid==uid,'Ordinary owned directory required')
        if current==root:break
        current=current.parent
    return path

def verify_frontier(audit,old_case,old,*,root,uid):
    """Verify metadata only; the replacement must independently hash its roster."""
    root=Path(root);old_case=ordinary_directory(old_case,root,uid)
    expected=root/'maintenance/terminal-controller-diagnostic-compaction-v2-20261103'
    require(old_case==expected,'Exact stopped maintenance case required')
    require(not present(old_case/'result.json'),'A published old result requires explicit reconciliation')
    maintenance=audit['maintenance']
    require(maintenance.get('JobId')==OLD_JOB and maintenance.get('UserId')=='pwa209(3164768)'
        and maintenance.get('Account')=='def-ptewarie_cpu' and maintenance.get('NumCPUs')=='1'
        and maintenance.get('Command')==str(root/'maintenance/terminal-controller-diagnostic-support-v2-20261103/controller-compact.sbatch'),
        'Exact recorded old maintenance allocation required')
    frontier=audit['frontier'];require(type(frontier) is list and frontier,'Original retirement frontier required')
    expected_paths={row['path'] for row in frontier}
    require(len(expected_paths)==len(frontier) and {str(p) for p in old_case.glob('controller-*')}==expected_paths,
        'Unexpected or missing old controller archive case')
    reports=[];total=0
    for saved in frontier:
        destination=ordinary_directory(saved['path'],root,uid)
        require(destination.parent==old_case and re.fullmatch('controller-[1-9][0-9]*',destination.name),
            'Exact stopped controller archive namespace required')
        require(saved.get('receipt_absent') is True and saved.get('removal_absent') is True,'Original no-removal frontier required')
        require(all(not present(destination/name) for name in ('receipt.json','removal.json','result.json')),
            'A published archive verification/removal/result requires explicit reconciliation')
        intent_path=destination/'intent.json';intent_info=intent_path.lstat()
        require(stat.S_ISREG(intent_info.st_mode) and intent_info.st_nlink==1 and intent_info.st_uid==uid
            and sha(intent_path)==saved['intent_sha256'],'Recorded stopped intent changed')
        intent=json.loads(intent_path.read_bytes());source=Path(intent['source']);rows=intent['members']
        require(intent.get('kind')=='terminal-controller-raw-reader-evidence','Exact original archival intent required')
        old.controller_identity(source,destination.name.removeprefix('controller-'),root=root)
        old.validate_rows(source,rows,root)
        parents={source}
        for row in rows:parents.update((source/row['path']).parents)
        for parent in parents:
            if parent.is_relative_to(source):ordinary_directory(parent,root,uid)
        for row in rows:
            info=(source/row['path']).lstat()
            require(stat.S_ISREG(info.st_mode) and info.st_uid==uid and info.st_nlink==1
                and identity(info)==row['identity'] and info.st_size==row['bytes'],
                'Original diagnostic leaf missing or metadata changed; explicit reconciliation required')
        archive=destination/'evidence.tar';partial=None
        if present(archive):
            info=archive.lstat();require(stat.S_ISREG(info.st_mode) and info.st_uid==uid and info.st_nlink==1,'Ordinary retained partial archive required')
            partial={'path':str(archive),'identity':identity(info),'bytes':info.st_size,'verification_claimed':False}
        reports.append({'path':str(destination),'intent_sha256':saved['intent_sha256'],'original_files':len(rows),'partial_archive':partial})
        total+=len(rows)
    return {'status':'stopped_maintenance_frontier_metadata_verified','controllers':reports,'original_files':total,
        'verification_scope':'metadata_only_original_dev_inode_size_mtime_ns_ctime_ns; no content SHA pass',
        'originals_removed':False,'old_partial_archives_changed':False,'independent_new_roster_hashing_required':True}

def actual_terminal(row):
    require(row.get('job_id_raw')==OLD_JOB and row.get('user')=='pwa209' and row.get('account')=='def-ptewarie_cpu'
        and row.get('cpus')==1 and row.get('elapsed_seconds',0)>0 and not row.get('unallocated_metadata')
        and row.get('state') in {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
        and 'gpu' not in row.get('alloc_tres','').lower(),'Positive owned actual terminal old maintenance allocation required')
    return row

def main():
    p=argparse.ArgumentParser();p.add_argument('--source-sha256',required=True);a=p.parse_args()
    require(sha(__file__)==a.source_sha256 and os.geteuid()==3164768,'Exact owned prelude source required')
    require(subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip().endswith('.narval.calcul.quebec'),'Narval context required')
    helper=OLD_SUPPORT/'compact-terminal-controller-reads-20261103.py';require(sha(helper)==OLD_SOURCE_SHA,'Exact original stopped helper required')
    spec=importlib.util.spec_from_file_location('actual_stopped_controller_compactor',helper);old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
    from horsehair import repair,budget,slurm
    stop=time.monotonic()+900;row=None;cost=None
    def runner(cmd,**kwargs):return subprocess.run(cmd,**{**kwargs,'timeout':max(1,min(45,stop-time.monotonic()))})
    while time.monotonic()<stop:
        records=repair._repair_native_states([OLD_JOB],{},owner='pwa209',account='def-ptewarie_cpu',runner=runner)
        candidate=records.get(OLD_JOB,{})
        if candidate.get('state') in {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}:
            row=actual_terminal(candidate)
            budget.native_accounting(ROOT,[OLD_JOB],runner=runner)
            proof=slurm.completed_native_dependency(ROOT,OLD_JOB,dependency_kind='afterany')
            if proof is not None:
                cost=proof['native_records'][OLD_JOB]
                old.terminal_owner(OLD_JOB,row,cost);break
        require(time.monotonic()+20<stop,'Actual final old maintenance accounting did not arrive within15minutes')
        time.sleep(20)
    require(row is not None and cost is not None,'Actual terminal old maintenance accounting is unavailable')
    audit_raw=AUDIT.read_bytes();audit=json.loads(audit_raw)
    report=verify_frontier(audit,OLD_CASE,old,root=ROOT,uid=3164768)
    report.update({'actual_old_terminal':row,'actual_old_cost':cost,'old_helper_sha256':OLD_SOURCE_SHA,
        'retirement_audit':str(AUDIT),'retirement_audit_sha256':hashlib.sha256(audit_raw).hexdigest()})
    proof_path=Path(__file__).with_name('stopped-controller-maintenance-prelude.json')
    with proof_path.open('x') as f:json.dump(report,f,indent=2,sort_keys=True);f.flush();os.fsync(f.fileno())
    print('STOPPED_CONTROLLER_PRELUDE '+json.dumps(report),flush=True)

if __name__=='__main__':main()
