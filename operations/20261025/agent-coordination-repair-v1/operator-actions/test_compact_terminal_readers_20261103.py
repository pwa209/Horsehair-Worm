from pathlib import Path
import copy,importlib.util,json
from datetime import datetime,timezone
import pytest

def load(name,file):
    path=Path(__file__).with_name(file)
    if not path.is_file() and file=='test_verified_evidence_archive_20261021.py':
        # Repository tests reuse the existing archive closure; native staging
        # retains the original flat sibling bundle.
        path=Path(__file__).resolve().parents[3]/'20261021/public-data-and-repair'/file
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

w=load('terminal_compactor','compact-terminal-readers-20261103.py')
fixtures=load('existing_archive_fixtures','test_verified_evidence_archive_20261021.py')
policy=fixtures.policy

def native():
    return {'job_id_raw':'100','user':'pwa209','account':'def-ptewarie_gpu','state':'COMPLETED','exit_code':'0:0','elapsed_seconds':12,'cpus':6,'alloc_tres':'cpu=6,gres/gpu:a100=1','unallocated_metadata':False}

def cost():return {'state':'COMPLETED','exit_code':'0:0','elapsed_seconds':12,'cpus':6,'gpus':1}

@pytest.mark.parametrize('change',[{'user':'other'},{'job_id_raw':'101'},{'state':'RUNNING'},{'exit_code':'1:0'},{'elapsed_seconds':0},{'unallocated_metadata':True},{'alloc_tres':'cpu=6,gres/gpu:a100=2'}])
def test_actual_native_conflict_refuses_compaction(change):
    value=native();value.update(change)
    with pytest.raises(ValueError):w.selected_owner('100',value,cost())

def test_actual_gpu_ownership_matches_selected_cost():
    assert w.selected_owner('100',native(),cost())=={'job_id':'100','user':'pwa209','account':'def-ptewarie_gpu',**cost()}

def test_canonical_pending_jobs_never_selected():
    state={'jobs':{'done':{'status':'complete','job_id':'100','intent':{'kind':'training'}},'pending':{'status':'submitted','job_id':'101','intent':{'kind':'evaluation'}}}}
    assert w.canonical_jobs(state)=={'100':'done'}
    state['jobs']['collision']={'status':'complete','job_id':'100','intent':{'kind':'assembly'}}
    with pytest.raises(ValueError,match='Duplicate'):w.canonical_jobs(state)

def test_embedded_diagnostic_reference_keeps_originals():
    with pytest.raises(ValueError,match='references'):w.protected_references({'nested':{'native_owner':'/root/native-100/scheduler-reads/result.json'}},[Path('/root/native-100/scheduler-reads/result.json')])

def test_real_archive_roundtrip_removes_only_allowed_terminal_diagnostics(policy):
    root,source,authority=policy;area=Path(authority['result_receipt']['path']).parent
    leaf=area/'accounting-reads/read-1/attempt-1-stdout.txt';leaf.parent.mkdir(parents=True);leaf.write_bytes(b'actual original diagnostic bytes\x00')
    protected=area/'checkpoint.pt';protected.write_bytes(b'keep checkpoint')
    before=Path(authority['result_receipt']['path']).read_bytes()
    state={'jobs':{'done':{'status':'complete','job_id':'100','intent':{'kind':'assembly'}}}}
    rows=w.candidates(fixtures.a,state,root=root,campaign=source.parent)
    assert len(rows)==1 and rows[0]['files']==1
    result=w.execute_one(fixtures.a,rows[0],authority,root/'maintenance/terminal-archive',root=root,canary=fixtures.canary(root))
    assert result['files_removed']==1 and not leaf.exists()
    assert protected.read_bytes()==b'keep checkpoint'
    assert Path(authority['result_receipt']['path']).read_bytes()==before
    with fixtures.a.ArchiveReader(root/'maintenance/terminal-archive',result['index_sha256'],result['archive_sha256']) as reader:
        assert reader.read_member('accounting-reads/read-1/attempt-1-stdout.txt')==b'actual original diagnostic bytes\x00'
    assert len(list(source.glob('condition-*')))==600

def test_changed_original_receipt_refuses_before_any_archive(policy):
    root,source,authority=policy;area=Path(authority['result_receipt']['path']).parent
    leaf=area/'quota-reads/result.json';leaf.parent.mkdir();leaf.write_text('{}')
    state={'jobs':{'done':{'status':'complete','job_id':'100','intent':{'kind':'assembly'}}}}
    row=w.candidates(fixtures.a,state,root=root,campaign=source.parent)[0]
    Path(row['receipt']).write_text('changed')
    with pytest.raises(ValueError,match='receipt changed'):w.execute_one(fixtures.a,row,authority,root/'maintenance/must-not-exist',root=root)
    assert leaf.exists() and not (root/'maintenance/must-not-exist').exists()

def fields():return {'JobId':'999','UserId':'pwa209(3164768)','JobState':'RUNNING','Account':'def-ptewarie_cpu','NumCPUs':'1','NumNodes':'1','MinMemoryNode':'16G','TimeLimit':'04:00:00','EndTime':'2026-11-03T20:00:00','JobName':'hhw-terminal-diagnostic-compaction-20261103','Command':str(Path(w.__file__).with_name('compact.sbatch'))}

def test_exact_owned_deployment_accepted_and_clock_conflict_refused():
    end=datetime(2026,11,3,20,tzinfo=timezone.utc).timestamp()
    assert w.allocation(fields(),'999',end,end-14400)==end
    with pytest.raises(ValueError,match='finite'):w.allocation(fields(),'999',end+60,end-14400)

@pytest.mark.parametrize('change',[{'UserId':'other(4)'},{'JobState':'PENDING'},{'TimeLimit':'06:00:00'},{'ReqTRES':'cpu=1,gres/gpu=1'},{'NumCPUs':'4'},{'NumNodes':'2'},{'Command':'/other/script'},{'JobName':'other'}])
def test_wrong_own_allocation_never_runs(change):
    f=fields();f.update(change)
    with pytest.raises(ValueError):w.allocation(f,'999',1793736000.,1793721600.)
