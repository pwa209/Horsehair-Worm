from pathlib import Path
import hashlib,importlib.util,json,os,tarfile,copy,stat
from types import SimpleNamespace
import pytest

spec=importlib.util.spec_from_file_location('controller_compactor',Path(__file__).with_name('compact-terminal-controller-reads-20261104.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

@pytest.fixture(autouse=True)
def native_directory_sync_or_windows_fixture_stub(monkeypatch):
    # Native deployment is Linux-only and must perform real directory fsync.
    # Windows fixture tests cannot open POSIX directory descriptors.
    if os.name=='nt':monkeypatch.setattr(m,'sync_directory',lambda path:None)

def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value));return {'path':str(path),'sha256':m.sha(path)}

@pytest.fixture
def tree(tmp_path):
    root=tmp_path/'study';source=root/m.CAMPAIGN.relative_to(m.ROOT)/'controller-0-100';source.mkdir(parents=True)
    names=['accounting-reads/read-1/attempt-1-stdout.txt','accounting-reads/read-1/attempt-1-query.json','registered-snapshot-reads/read-2/attempt-1-result.json']
    for i,name in enumerate(names):
        path=source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'EXACT RAW DIAGNOSTIC '+str(i).encode())
    canonical=source/'installation.json';write(canonical,{'source':'pinned','scientific_result':False})
    (source/'checkpoint.pt').write_bytes(b'MUST REMAIN')
    return root,source,names

@pytest.mark.parametrize('name',['accounting-reads/read-a/attempt-1-stdout.txt','registered-snapshot-reads/read-1/attempt-1-intent.json','accounting-refresh-000010/native-accounting/read-x/attempt-1-response.json','accounting-refresh-000010/registered-reads/read-x/attempt-1-result.json'])
def test_only_generated_raw_reader_schema_accepted(name):assert m.allowed(name)

@pytest.mark.parametrize('name',['../accounting-reads/attempt-1-stdout.txt','checkpoint.pt','accounting-reads/read-x/protocol.json','accounting-reads/read-x/attempt-9-stdout.txt','accounting-refresh-000010/other/read-x/attempt-1-response.json','accounting-refresh-000010/native-accounting/not-read-x/attempt-1-response.json','accounting-reads/read-x/anything.parquet'])
def test_scientific_and_unknown_paths_preserved(name):assert not m.allowed(name)

def test_exact_native_controller_directory_required(tree):
    root,source,_=tree;assert m.controller_identity(source,'100',root=root)==source
    with pytest.raises(ValueError):m.controller_identity(source,'101',root=root)
    other=root/'maintenance/foreign/controller-0-100';other.mkdir(parents=True)
    with pytest.raises(ValueError):m.controller_identity(other,'100',root=root)

@pytest.mark.parametrize('change',['none','job','script'])
def test_original_registration_positive_join_and_conflicts_refused(tree,change):
    root,source,_=tree;directory=root/'plans/controller';directory.mkdir(parents=True);script=directory/'controller.sbatch'
    text='#!/bin/bash\n#SBATCH --job-name=hhw-controller\npython /frozen/source.py --segment 0\n';script.write_text(text)
    task={'name':'controller','script':str(script),'script_sha256':m.canonical(text),'command':'python /frozen/source.py --segment 0'}
    config='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35';plan={'root':str(root),'tasks':[task],'config_sha256':config,'release_identity':{'sha256':'a'*64}}
    planloc=write(directory/'plan.json',plan);ledger={'plan_sha256':m.canonical(plan),'config_sha256':config,'source_sha256':'a'*64,'tasks':{'controller':{'state':'submitted','job_id':'101' if change=='job' else '100','script_sha256':task['script_sha256']}}}
    ledgerloc=write(root/'state/ledger.json',ledger);registration={'native_job_id':'100','task_name':'controller','plan':planloc,'ledger':ledgerloc,'script':{'path':str(script),'sha256':m.sha(script)}}
    if change=='script':script.write_text('changed')
    if change=='none':assert m.registration_binding(registration,'100',root=root)['task']==task
    else:
        with pytest.raises(ValueError):m.registration_binding(registration,'100',root=root)

@pytest.mark.parametrize('change',['none','job','plan'])
def test_original_controller_admission_binds_source_to_its_own_job(tree,change):
    root,source,_=tree;directory=root/'plans/controller';directory.mkdir(parents=True);script=directory/'controller.sbatch';text='#!/bin/bash\n#SBATCH --job-name=hhw-controller\npython /frozen/source.py --segment 0\n';script.write_text(text)
    task={'name':'controller','script':str(script),'script_sha256':m.canonical(text),'command':'python /frozen/source.py --segment 0','resources':{'cpus':1,'gpu':False,'memory':'64G','hours':96}}
    config='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35';plan={'root':str(root),'tasks':[task],'config_sha256':config,'release_identity':{'sha256':'a'*64}};pl=write(directory/'plan.json',plan)
    ledger={'plan_sha256':m.canonical(plan),'config_sha256':config,'source_sha256':'a'*64,'tasks':{'controller':{'state':'submitted','job_id':'101' if change=='job' else '100','script_sha256':task['script_sha256']}}};le=write(root/'state/ledger.json',ledger)
    start={'unique_allocation_bindings':{'100':{'ledger':le['path'],'task':'controller','resource_sha256':m.canonical(task['resources'])}},'registered_pair_hashes':{'one':{'ledger_path':le['path'],'plan_path':pl['path'],'plan_sha256':m.canonical(plan),'ledger_sha256':m.canonical(ledger)}}};write(source/'controller-start.json',start)
    if change=='plan':Path(pl['path']).write_text('{}')
    if change=='none':assert m.source_registration(source,'100',root=root)['registration']['native_job_id']=='100'
    else:
        with pytest.raises(ValueError):m.source_registration(source,'100',root=root)

@pytest.mark.parametrize('foreign_script',[False,True])
def test_original_primary_controller_allocation_and_registration_join(tree,foreign_script):
    root,_,_=tree;source=root/m.PRIMARY_SUPPORT.relative_to(m.ROOT)/'native-7046213';source.mkdir(parents=True);directory=root/'plans/primary';directory.mkdir(parents=True);script=directory/'primary.sbatch'
    text='#!/bin/bash\n#SBATCH --job-name=hhw-primary\npython /frozen/primary.py\n';script.write_text(text)
    task={'name':'primary','script':str(script),'script_sha256':m.canonical(text),'command':'python /frozen/primary.py'};config='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
    plan={'root':str(root),'tasks':[task],'config_sha256':config,'release_identity':{'sha256':'b'*64}};pl=write(directory/'plan.json',plan)
    le=write(root/'state/primary-ledger.json',{'plan_sha256':m.canonical(plan),'config_sha256':config,'source_sha256':'b'*64,'tasks':{'primary':{'state':'submitted','job_id':'7046213','script_sha256':task['script_sha256']}}})
    registration={'native_job_id':'7046213','task_name':'primary','plan':pl,'ledger':le,'script':{'path':str(script),'sha256':m.sha(script)}}
    write(source/'native-bindings.json',{'job':'7046213','allocation':{'JobId':'7046213','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','NumCPUs':'1','Command':'/foreign/script' if foreign_script else str(script),'JobName':'hhw-primary'},'registration':registration})
    if foreign_script:
        with pytest.raises(ValueError,match='script/name'):m.source_registration(source,'7046213',root=root)
    else:assert m.source_registration(source,'7046213',root=root)['registration']==registration

def test_bound_document_locator_closure_and_installation_references_excluded(tree):
    root,source,names=tree
    selected=source/names[0];nested=write(root/'maintenance/proof.json',{'selected_source':str(selected)})
    doc=write(root/'maintenance/descriptor.json',{'proof':nested})
    write(source/'installation.json',{'owner_read':str(source/names[1])})
    rows,bound=m.roster(source,[doc],[],root=root)
    assert [r['path'] for r in rows]==[names[2]]
    assert set(bound)=={doc['path'],nested['path'],str(source/'installation.json')}

def test_entire_bound_reader_tree_yields_no_removal_candidates(tree):
    root,source,_=tree;rows,_=m.roster(source,[],[str(source/'accounting-reads'),str(source/'registered-snapshot-reads')],root=root)
    assert rows==[]

def test_missing_or_changed_bound_proof_refuses_roster(tree):
    root,source,_=tree;doc=write(root/'maintenance/proof.json',{'source':'original'});Path(doc['path']).write_text('{}')
    with pytest.raises(ValueError,match='changed'):m.roster(source,[doc],[],root=root)

def test_bound_paths_in_dictionary_keys_are_protected_and_followed(tree):
    root,source,names=tree;inner=write(root/'maintenance/selected-native.json',{str(source/names[0]):'a'*64})
    outer=write(root/'maintenance/descriptor.json',{'checked_paths':{inner['path']:inner['sha256']}})
    rows,bound=m.roster(source,[outer],[],root=root)
    assert names[0] not in {r['path'] for r in rows} and inner['path'] in bound

@pytest.mark.parametrize('mapping',[False,True])
def test_missing_declared_hashed_nested_proof_refuses_incomplete_closure(tree,mapping):
    root,source,_=tree;missing=str(root/'maintenance/missing.json')
    doc=write(root/'maintenance/descriptor.json',{'pins':{missing:'a'*64}} if mapping else {'proof':{'path':missing,'sha256':'a'*64}})
    with pytest.raises((ValueError,FileNotFoundError)):m.roster(source,[doc],[],root=root)

def test_informational_reader_directory_reference_does_not_bind_all_descendants(tree):
    root,source,names=tree;write(source/'installation.json',{'evidence_dir':str(source/'accounting-reads')})
    rows,_=m.roster(source,[],[],root=root);assert len(rows)==3

@pytest.mark.parametrize('injected',['/outside/attempt-1-stdout.txt','accounting-reads/../../outside.txt','checkpoint.pt','duplicate'])
def test_injected_roster_cannot_escape_or_archive_protected_file(tree,injected):
    root,source,_=tree;rows,_=m.roster(source,[],[],root=root)
    if injected=='duplicate':rows.append(copy.deepcopy(rows[0]))
    else:rows[0]['path']=injected
    with pytest.raises(ValueError):m.pack(source,root/'maintenance/must-not-exist',rows,{},root=root)
    assert not (root/'maintenance/must-not-exist').exists()

def test_real_archive_disk_roundtrip_removes_only_selected_raw_bytes(tree):
    root,source,names=tree;rows,_=m.roster(source,[],[str(source/names[1])],root=root);dest=root/'maintenance/archive'
    receipt=m.pack(source,dest,rows,{'kind':'actual_terminal_controller_diagnostics'},root=root)
    assert all((source/n).exists() for n in names)
    result=m.remove(source,dest,rows,receipt,root=root)
    assert result['status']=='complete_terminal_diagnostic_compaction' and result['removed_count']==2
    assert (source/names[1]).exists() and (source/'installation.json').exists() and (source/'checkpoint.pt').read_bytes()==b'MUST REMAIN'
    m.verify_tar(dest/'evidence.tar',rows,receipt['archive_sha256'])

def test_native_roundtrip_uses_allocation_local_disk_and_leaves_no_extra_archive_files(tree,monkeypatch):
    root,source,names=tree;local=root/'allocation-local';local.mkdir();monkeypatch.setenv('SLURM_TMPDIR',str(local))
    real=m.tempfile.TemporaryDirectory;seen=[]
    def observe(*args,**kwargs):
        seen.append(Path(kwargs['dir']));assert Path(kwargs['dir'])==local
        return real(*args,**kwargs)
    monkeypatch.setattr(m.tempfile,'TemporaryDirectory',observe)
    rows,_=m.roster(source,[],[],root=root);dest=root/'maintenance/archive';receipt=m.pack(source,dest,rows,{},root=root)
    journal=m.remove(source,dest,rows,receipt,root=root)
    assert seen==[local,local] and journal['removed_count']==3
    assert not list(local.iterdir()) and not list(dest.glob('controller-roundtrip-*'))
    assert {p.name for p in dest.iterdir()}=={'intent.json','evidence.tar','receipt.json','removal.json'}
    assert (source/'checkpoint.pt').read_bytes()==b'MUST REMAIN'

def test_other_project_capacity_recovery_does_not_skip_required_project_cleanup():
    assert not m.recovery_goal(350000,0,20000)
    assert not m.recovery_goal(290000,30000,20000)
    assert m.recovery_goal(350000,20000,20000)
    assert m.recovery_goal(350000,0,0)
    with pytest.raises(ValueError):m.recovery_goal(350000,0,True)
    with pytest.raises(ValueError):m.recovery_goal(350000,0,-1)

def test_corrupted_archive_prevents_every_unlink(tree):
    root,source,names=tree;rows,_=m.roster(source,[],[],root=root);dest=root/'maintenance/archive';receipt=m.pack(source,dest,rows,{},root=root)
    with (dest/'evidence.tar').open('ab') as f:f.write(b'CORRUPT')
    with pytest.raises(ValueError,match='SHA'):m.remove(source,dest,rows,receipt,root=root)
    assert all((source/n).exists() for n in names)

def test_archive_fsync_failure_keeps_every_original_before_any_receipt_or_unlink(tree,monkeypatch):
    root,source,names=tree;rows,_=m.roster(source,[],[],root=root);dest=root/'maintenance/archive';real=m.os.fsync
    def fail_archive(fd):
        if os.fstat(fd).st_size>=10240:raise OSError('injected archive fsync failure')
        return real(fd)
    monkeypatch.setattr(m.os,'fsync',fail_archive)
    with pytest.raises(OSError,match='archive fsync'):m.pack(source,dest,rows,{},root=root)
    assert all((source/n).exists() for n in names) and not (dest/'receipt.json').exists() and not (dest/'removal.json').exists()

def test_directory_sync_failure_prevents_archive_success_and_removal(tree,monkeypatch):
    root,source,names=tree;rows,_=m.roster(source,[],[],root=root);dest=root/'maintenance/archive'
    def fail(path):raise OSError('injected directory fsync failure')
    monkeypatch.setattr(m,'sync_directory',fail)
    with pytest.raises(OSError,match='directory fsync'):m.pack(source,dest,rows,{},root=root)
    assert all((source/n).exists() for n in names) and not (dest/'receipt.json').exists()

def test_progress_journal_writes_are_batched_but_completion_is_exact(tree,monkeypatch):
    root,source,_=tree
    for i in range(130):
        path=source/f'accounting-reads/read-batch-{i}/attempt-1-stdout.txt';path.parent.mkdir();path.write_bytes(b'raw exact reader bytes')
    rows,_=m.roster(source,[],[],root=root);dest=root/'maintenance/archive';receipt=m.pack(source,dest,rows,{},root=root)
    real=m.atomic;written=[]
    def observe(path,value):
        if Path(path).name=='removal.json':written.append(value['removed_count'])
        return real(path,value)
    monkeypatch.setattr(m,'atomic',observe)
    result=m.remove(source,dest,rows,receipt,root=root)
    assert len(rows)==133 and written==[0,128,133]
    assert result['removed_count']==133 and all(not (source/r['path']).exists() for r in rows)
    m.verify_tar(dest/'evidence.tar',rows,receipt['archive_sha256'])

def test_changed_original_refuses_all_removal_after_roundtrip(tree):
    root,source,names=tree;rows,_=m.roster(source,[],[],root=root);dest=root/'maintenance/archive';receipt=m.pack(source,dest,rows,{},root=root)
    (source/names[-1]).write_bytes(b'CHANGED')
    with pytest.raises(ValueError,match='Original changed'):m.remove(source,dest,rows,receipt,root=root)
    assert all((source/n).exists() for n in names)

def test_interrupted_unlink_preserves_archive_and_forbids_blind_replay(tree):
    root,source,names=tree;rows,_=m.roster(source,[],[],root=root);dest=root/'maintenance/archive';receipt=m.pack(source,dest,rows,{},root=root)
    def interrupt(name):raise RuntimeError('interrupt after unlink')
    with pytest.raises(RuntimeError):m.remove(source,dest,rows,receipt,root=root,after_unlink=interrupt)
    assert json.loads((dest/'removal.json').read_bytes())['status']=='interrupted_explicit_reconciliation_required'
    assert sum((source/n).exists() for n in names)==2
    with pytest.raises(ValueError,match='explicit reconciliation'):m.remove(source,dest,rows,receipt,root=root)
    m.verify_tar(dest/'evidence.tar',rows,receipt['archive_sha256'])

def test_bound_proof_change_between_unlinks_stops_before_next_original(tree):
    root,source,names=tree;doc=write(root/'maintenance/bound.json',{'fixed':'original'})
    rows,bound=m.roster(source,[doc],[],root=root);dest=root/'maintenance/archive';receipt=m.pack(source,dest,rows,{},root=root)
    guard=m.binding_guard(bound,root=root)
    def change_proof(name):Path(doc['path']).write_text('{"fixed":"changed"}')
    with pytest.raises(ValueError,match='Bound source/proof changed'):m.remove(source,dest,rows,receipt,root=root,checkpoint=guard,after_unlink=change_proof)
    assert sum((source/n).exists() for n in names)==2
    m.verify_tar(dest/'evidence.tar',rows,receipt['archive_sha256'])


def test_bound_guard_initial_sha_is_still_required(tree):
    root,source,_=tree;doc=write(root/'proofs/bound.json',{'fixed':'original'})
    with pytest.raises(ValueError,match='bytes changed'):m.binding_guard({doc['path']:'0'*64},root=root)


def test_bound_guard_checks_every_leaf_once_and_shared_ancestor_once(tree,monkeypatch):
    root,source,_=tree
    docs=[write(root/'proofs/shared'/('bound-'+str(i)+'.json'),{'index':i}) for i in range(20)]
    guard=m.binding_guard({x['path']:x['sha256'] for x in docs},root=root)
    expected={str(p) for d in docs for p in Path(d['path']).parents}|{d['path'] for d in docs}
    calls=[];original=os.lstat
    def lstat(path,*a,**kw):calls.append(str(path));return original(path,*a,**kw)
    def noresolve(*a,**kw):raise AssertionError('Repeated resolution makes the per-member guard nonlinear')
    monkeypatch.setattr(m.os,'lstat',lstat);monkeypatch.setattr(Path,'resolve',noresolve)
    guard();monkeypatch.undo()
    assert len(calls)==len(expected) and set(calls)==expected


def test_bound_guard_allows_unrelated_sibling_unlink_and_directory_timestamp_change(tree):
    root,source,_=tree;doc=write(root/'proofs/bound.json',{'fixed':'original'})
    sibling=root/'proofs/eligible-reader.txt';sibling.write_text('archived elsewhere')
    guard=m.binding_guard({doc['path']:doc['sha256']},root=root)
    sibling.unlink();os.utime(root/'proofs',None);guard()
    assert m.sha(doc['path'])==doc['sha256']


def test_bound_guard_refuses_changed_bytes(tree):
    root,source,_=tree;doc=write(root/'proofs/bound.json',{'fixed':'original'})
    guard=m.binding_guard({doc['path']:doc['sha256']},root=root)
    Path(doc['path']).write_bytes(b'changed-bound-evidence')
    with pytest.raises(ValueError,match='Bound source/proof changed'):guard()


def test_bound_guard_refuses_replaced_leaf_symlink(tree):
    root,source,_=tree;doc=write(root/'proofs/bound.json',{'fixed':'original'})
    guard=m.binding_guard({doc['path']:doc['sha256']},root=root)
    target=root/'same-bytes.json';target.write_bytes(Path(doc['path']).read_bytes());Path(doc['path']).unlink()
    try:Path(doc['path']).symlink_to(target)
    except OSError:pytest.skip('symlink creation unavailable on this host')
    with pytest.raises(ValueError,match='Bound source/proof changed'):guard()


def test_bound_guard_refuses_replaced_ancestor_symlink(tree):
    root,source,_=tree;doc=write(root/'proofs/bound.json',{'fixed':'original'})
    guard=m.binding_guard({doc['path']:doc['sha256']},root=root)
    folder=root/'proofs';backup=root/'proofs-original';folder.rename(backup)
    try:folder.symlink_to(backup,target_is_directory=True)
    except OSError:pytest.skip('directory symlink creation unavailable on this host')
    with pytest.raises(ValueError,match='ancestor'):guard()


def test_bound_guard_refuses_new_hardlink(tree):
    root,source,_=tree;doc=write(root/'proofs/bound.json',{'fixed':'original'})
    guard=m.binding_guard({doc['path']:doc['sha256']},root=root)
    try:os.link(doc['path'],root/'duplicate-bound.json')
    except OSError:pytest.skip('hardlink creation unavailable on this host')
    with pytest.raises(ValueError,match='Bound source/proof changed'):guard()


@pytest.mark.parametrize('change',['uid','nlink','type'])
def test_bound_guard_rechecks_owner_link_count_and_file_type(tree,monkeypatch,change):
    root,source,_=tree;doc=write(root/'proofs/bound.json',{'fixed':'original'})
    real=os.lstat;original=real(doc['path']);monkeypatch.setattr(m.os,'geteuid',lambda:original.st_uid,raising=False)
    guard=m.binding_guard({doc['path']:doc['sha256']},root=root)
    def observed(path,*args,**kwargs):
        record=real(path,*args,**kwargs)
        if str(path)!=doc['path']:return record
        values={name:getattr(record,name) for name in ['st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns','st_mode','st_nlink','st_uid']}
        if change=='uid':values['st_uid']+=1
        elif change=='nlink':values['st_nlink']=2
        else:values['st_mode']=stat.S_IFLNK|0o777
        return SimpleNamespace(**values)
    monkeypatch.setattr(m.os,'lstat',observed)
    with pytest.raises(ValueError,match='Bound source/proof changed'):guard()


def maintenance_fields():
    return {'JobId':'100','UserId':'pwa209(3164768)','JobState':'RUNNING','Account':'def-ptewarie_cpu',
        'NumCPUs':'1','NumNodes':'1','MinMemoryNode':'16G','TimeLimit':'12:00:00',
        'Command':str(Path('/maintenance/controller-compact.sbatch')),'JobName':'hhw-terminal-controller-read-compaction-20261104',
        'EndTime':'2030-01-01T12:00:00','AllocTRES':'cpu=1,mem=16G,node=1'}


def test_native_maintenance_requires_twelve_hours_and_retains_finalization_margin():
    fields=maintenance_fields();end=1893499200
    assert m.maintenance_allocation(fields,'100',Path(fields['Command']),end,wall=lambda:end-43200)==end
    with pytest.raises(ValueError,match='finite maintenance end'):
        m.maintenance_allocation(fields,'100',Path(fields['Command']),end,wall=lambda:end-600)
    with pytest.raises(ValueError,match='finite maintenance end'):
        m.maintenance_allocation(fields,'100',Path(fields['Command']),end-5,wall=lambda:end-43200)


@pytest.mark.parametrize('key,value',[('TimeLimit','04:00:00'),('NumCPUs','2'),('MinMemoryNode','64G'),
    ('UserId','other(42)'),('JobId','101'),('Command','/other.sbatch'),('AllocTRES','cpu=1,gres/gpu=1')])
def test_native_maintenance_refuses_wrong_allocation(key,value):
    fields=maintenance_fields();fields[key]=value
    with pytest.raises(ValueError,match='scalar1CPU16G12h'):
        m.maintenance_allocation(fields,'100',Path('/maintenance/controller-compact.sbatch'),1893499200,wall=lambda:1893456000)

def test_archive_symlink_member_refused_even_with_consistent_outer_digest(tree):
    root,source,names=tree;rows,_=m.roster(source,[],[],root=root);dest=root/'maintenance/archive';receipt=m.pack(source,dest,rows,{},root=root)
    with tarfile.open(dest/'evidence.tar','w:') as tar:
        item=tarfile.TarInfo(rows[0]['path']);item.type=tarfile.SYMTYPE;item.linkname='/outside';tar.addfile(item)
    receipt['archive_sha256']=m.sha(dest/'evidence.tar');m.atomic(dest/'receipt.json',receipt)
    with pytest.raises(ValueError,match='type'):m.remove(source,dest,rows,receipt,root=root)
    assert all((source/n).exists() for n in names)

def test_hardlinked_original_refused(tree):
    root,source,names=tree
    try:os.link(source/names[0],root/'duplicate')
    except OSError:pytest.skip('hardlink creation unavailable on this host')
    with pytest.raises(ValueError,match='single-link'):m.roster(source,[],[],root=root)

def test_symlink_original_refused(tree):
    root,source,names=tree;p=source/names[0];p.unlink()
    try:p.symlink_to(source/'checkpoint.pt')
    except OSError:pytest.skip('symlink creation unavailable on this host')
    with pytest.raises(ValueError):m.roster(source,[],[],root=root)

@pytest.mark.parametrize('change',[{'user':'other'},{'job_id_raw':'101'},{'state':'RUNNING'},{'cpus':2},{'elapsed_seconds':0},{'alloc_tres':'cpu=1,gres/gpu=1'}])
def test_actual_terminal_owner_or_allocation_conflict_refuses(change):
    row={'job_id_raw':'100','user':'pwa209','account':'def-ptewarie_cpu','state':'FAILED','exit_code':'1:0','cpus':1,'elapsed_seconds':40,'alloc_tres':'cpu=1,mem=64G'}
    cost={k:row[k] for k in ('state','exit_code','cpus','elapsed_seconds')};cost['gpus']=0;row.update(change)
    with pytest.raises(ValueError):m.terminal_owner('100',row,cost)

def test_stale_historical_reference_excludes_entire_source_with_exact_details(tree):
    root,source,names=tree;old=write(root/'historical.json',{'original':True});Path(old['path']).write_text('{"changed":true}');write(source/'installation.json',{'old_proof':old})
    request={'binding_documents':[],'sources':[]};rows,reason=m.eligible_roster(source,[],[],request,root=root)
    assert rows is None and reason['status']=='historical_source_ineligible_stale_locator'
    assert reason['path']==old['path'] and reason['expected_sha256']==old['sha256'] and reason['actual_sha256']==m.sha(old['path']) and reason['referrer']==str(source/'installation.json')
    assert all((source/n).exists() for n in names)

def test_changed_canonical_request_pin_aborts_instead_of_skipping_source(tree):
    root,source,names=tree;pin=write(root/'canonical.json',{'revision':699});Path(pin['path']).write_text('{"revision":700}')
    with pytest.raises(ValueError,match='Canonical/request'):m.eligible_roster(source,[],[],{'binding_documents':[pin],'sources':[]},root=root)
    assert all((source/n).exists() for n in names)

def test_canonical_pin_race_cannot_be_disguised_as_historical_skip(tree,monkeypatch):
    root,source,_=tree;pin=write(root/'canonical.json',{'revision':699})
    def race(*args,**kwargs):
        Path(pin['path']).write_text('{"revision":700}')
        raise m.StaleHistoricalLocator(root/'old.json','a'*64,'b'*64)
    monkeypatch.setattr(m,'roster',race)
    with pytest.raises(ValueError,match='Canonical/request'):m.eligible_roster(source,[],[],{'binding_documents':[pin],'sources':[]},root=root)

def test_non_stale_source_validation_error_still_aborts(tree):
    root,source,_=tree;(source/'installation.json').write_text('INVALID JSON')
    with pytest.raises(json.JSONDecodeError):m.eligible_roster(source,[],[],{'binding_documents':[],'sources':[]},root=root)

def completed_prior_fixture(root):
    entries=[];native={};costs={}
    for job in ['7046213','6689295','6701493']:
        source=root/m.PRIMARY_SUPPORT.relative_to(m.ROOT)/('native-'+job) if job=='7046213' else root/m.CAMPAIGN.relative_to(m.ROOT)/('controller-0-'+job)
        source.mkdir(parents=True);leaf=source/'accounting-reads/read-original/attempt-1-stdout.txt';leaf.parent.mkdir(parents=True);leaf.write_bytes(('original '+job).encode());write(source/'installation.json',{'source':'fixed'})
        rows,_=m.roster(source,[],[],root=root)
        native[job]={'job_id_raw':job,'user':'pwa209','account':'def-ptewarie_cpu','state':'COMPLETED','exit_code':'0:0','cpus':1,'elapsed_seconds':12,'alloc_tres':'cpu=1','unallocated_metadata':False}
        costs[job]={'state':'COMPLETED','exit_code':'0:0','cpus':1,'gpus':0,'elapsed_seconds':12}
        authority={'kind':'actual_terminal_controller_diagnostics','native':{'job_id':job,**native[job]},'wrapper_sha256':'5019e6f543cd6a93195edf6e8db0319eaa7fc2d1abd8cc48e74db06465ff7550','request_sha256':'e5616c613b646818c9647087583ce22f3a844b1466992276ae3f9f5950ec88f6'}
        destination=root/'maintenance/terminal-controller-diagnostic-compaction-v3-20261103'/('controller-'+job);destination.parent.mkdir(parents=True,exist_ok=True)
        receipt=m.pack(source,destination,rows,authority,root=root);m.remove(source,destination,rows,receipt,root=root)
        item={'job':job,'archive_directory':str(destination)}
        for key,name in [('intent','intent.json'),('receipt','receipt.json'),('removal','removal.json')]:item[key]={'path':str(destination/name),'sha256':m.sha(destination/name)}
        entries.append(item)
    request={'prior_completed':entries,'sources':[{'job':'999','binding_documents':[]}],'binding_documents':[]}
    return request,native,costs

def test_prior_completed_archives_verified_without_any_replay(tree,monkeypatch):
    root,_,_=tree;request,native,costs=completed_prior_fixture(root)
    def forbidden(*args,**kwargs):raise AssertionError('Completed sources must never be packed or removed again')
    monkeypatch.setattr(m,'pack',forbidden);monkeypatch.setattr(m,'remove',forbidden)
    verified=m.reconcile_previous(request,native,costs,root=root)
    assert {x['job'] for x in verified}==m.PREVIOUS_JOBS and sum(x['files_removed'] for x in verified)==3 and all(x['replayed'] is False for x in verified)

@pytest.mark.parametrize('failure',['receipt','archive','incomplete_journal','reappeared','native','replay_request'])
def test_prior_proof_mismatch_or_incomplete_source_prevents_credit(tree,failure):
    root,_,_=tree;request,native,costs=completed_prior_fixture(root);first=request['prior_completed'][0];directory=Path(first['archive_directory'])
    if failure=='receipt':Path(first['receipt']['path']).write_text('{}')
    elif failure=='archive':
        with (directory/'evidence.tar').open('ab') as f:f.write(b'CORRUPT')
    elif failure=='incomplete_journal':
        path=Path(first['removal']['path']);value=json.loads(path.read_bytes());value['removed_count']=0;write(path,value);first['removal']['sha256']=m.sha(path)
    elif failure=='reappeared':
        intent=json.loads(Path(first['intent']['path']).read_bytes());path=Path(intent['source'])/intent['members'][0]['path'];path.write_bytes(b'original reappeared')
    elif failure=='native':native[first['job']]['user']='foreign'
    else:request['sources'].append({'job':first['job']})
    with pytest.raises(ValueError):m.reconcile_previous(request,native,costs,root=root)
