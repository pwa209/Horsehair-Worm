from pathlib import Path
from types import SimpleNamespace
import ast,base64,hashlib,importlib.util,json,os
import pytest

HERE=Path(__file__).resolve().parent

def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

@pytest.fixture
def m():return load(HERE/'verify-stopped-controller-maintenance-20261103.py','stopped_controller_frontier')

@pytest.fixture
def old(m,tmp_path):
    path=m.OLD_SUPPORT/'compact-terminal-controller-reads-20261103.py'
    if path.exists():raw=path.read_bytes()
    else:
        script=(HERE/'controller-diagnostic-compaction-v2-registration-20261103.sh').read_text()
        body=script.split("python -s -B -u - <<'PY'\n",1)[1].split('\nPY',1)[0]
        tree=ast.parse(body)
        payload=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign)
            and any(isinstance(t,ast.Name) and t.id=='payload' for t in n.targets))
        raw=base64.b64decode(payload['compact-terminal-controller-reads-20261103.py'])
    assert hashlib.sha256(raw).hexdigest()==m.OLD_SOURCE_SHA
    path=tmp_path/'exact-old.py';path.write_bytes(raw);return load(path,'exact_stopped_compactor')

@pytest.fixture
def fixture(m,old,tmp_path_factory):
    root=tmp_path_factory.mktemp('r');case=root/'maintenance/terminal-controller-diagnostic-compaction-v2-20261103'
    destination=case/'controller-7046213';destination.mkdir(parents=True)
    source=root/'maintenance/study-primary-external-census-policy-v1-20261023/native-7046213'
    original=source/'accounting-refresh-000001/native-accounting/read-1/attempt-1-stdout.txt'
    original.parent.mkdir(parents=True);original.write_bytes(b'actual old diagnostic\n')
    row={'path':original.relative_to(source).as_posix(),'sha256':m.sha(original),'bytes':original.stat().st_size,'identity':m.identity(original.stat())}
    intent=destination/'intent.json';intent.write_text(json.dumps({'kind':'terminal-controller-raw-reader-evidence','source':str(source),'members':[row]}))
    partial=destination/'evidence.tar';partial.write_bytes(b'unverified partial tar bytes')
    audit={'maintenance':{'JobId':m.OLD_JOB,'UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','NumCPUs':'1',
            'Command':str(root/'maintenance/terminal-controller-diagnostic-support-v2-20261103/controller-compact.sbatch')},
        'frontier':[{'path':str(destination),'intent_sha256':m.sha(intent),'receipt_absent':True,'removal_absent':True}]}
    return SimpleNamespace(root=root,case=case,destination=destination,source=source,original=original,partial=partial,
        intent=intent,audit=audit,uid=root.stat().st_uid)

def verify(m,old,f):return m.verify_frontier(f.audit,f.case,old,root=f.root,uid=f.uid)

def test_intact_partial_archive_metadata_allowed_and_preserved_without_deletion(m,old,fixture,monkeypatch):
    f=fixture;partial=f.partial.read_bytes();original=f.original.read_bytes();before=set(f.root.rglob('*'))
    monkeypatch.setattr(Path,'unlink',lambda *a,**kw:pytest.fail('Verifier may not delete any path'))
    result=verify(m,old,f)
    assert result['original_files']==1 and result['originals_removed'] is False
    assert result['old_partial_archives_changed'] is False and result['independent_new_roster_hashing_required'] is True
    assert result['controllers'][0]['partial_archive']['verification_claimed'] is False
    assert f.partial.read_bytes()==partial and f.original.read_bytes()==original and set(f.root.rglob('*'))==before

def test_original_content_is_not_read_for_metadata_only_frontier(m,old,fixture,monkeypatch):
    original=Path.read_bytes
    def read(path):
        if path==fixture.original:pytest.fail('New roster owns content hashing; prelude is metadata only')
        return original(path)
    monkeypatch.setattr(Path,'read_bytes',read)
    assert verify(m,old,fixture)['original_files']==1

@pytest.mark.parametrize('marker',['receipt.json','removal.json','result.json'])
def test_published_old_controller_frontier_requires_explicit_reconciliation(m,old,fixture,marker):
    (fixture.destination/marker).write_text('{}')
    with pytest.raises(ValueError,match='explicit reconciliation'):verify(m,old,fixture)

def test_case_result_requires_explicit_reconciliation(m,old,fixture):
    (fixture.case/'result.json').write_text('{}')
    with pytest.raises(ValueError,match='explicit reconciliation'):verify(m,old,fixture)

def test_missing_original_refused(m,old,fixture):
    fixture.original.unlink()
    with pytest.raises((ValueError,FileNotFoundError)):verify(m,old,fixture)

def test_changed_original_metadata_refused(m,old,fixture):
    fixture.original.write_bytes(b'changed file length')
    with pytest.raises(ValueError,match='metadata changed'):verify(m,old,fixture)

def test_hardlinked_original_refused(m,old,fixture,monkeypatch):
    original=Path.lstat
    def linked(path,*args,**kwargs):
        info=original(path,*args,**kwargs)
        if path==fixture.original:
            fields={name:getattr(info,name) for name in dir(info) if name.startswith('st_')};fields['st_nlink']=2
            return SimpleNamespace(**fields)
        return info
    monkeypatch.setattr(Path,'lstat',linked)
    with pytest.raises(ValueError,match='metadata changed'):verify(m,old,fixture)

def test_unexpected_archive_case_refused(m,old,fixture):
    (fixture.case/'controller-12345').mkdir()
    with pytest.raises(ValueError,match='Unexpected'):verify(m,old,fixture)

def test_changed_intent_hash_refused(m,old,fixture):
    fixture.intent.write_text(fixture.intent.read_text()+'\n')
    with pytest.raises(ValueError,match='intent changed'):verify(m,old,fixture)

def test_wrong_source_case_refused(m,old,fixture):
    other=fixture.root/'maintenance/unexpected-case';other.mkdir()
    with pytest.raises(ValueError,match='Exact stopped'):m.verify_frontier(fixture.audit,other,old,root=fixture.root,uid=fixture.uid)

@pytest.mark.parametrize('change',[{'state':'RUNNING'},{'user':'other'},{'cpus':2},{'elapsed_seconds':0},{'job_id_raw':'123'}])
def test_actual_terminal_identity_does_not_accept_live_or_foreign_allocation(m,change):
    row={'job_id_raw':m.OLD_JOB,'user':'pwa209','account':'def-ptewarie_cpu','state':'FAILED','cpus':1,'elapsed_seconds':100,'alloc_tres':'cpu=1,mem=16G'}
    row.update(change)
    with pytest.raises(ValueError):m.actual_terminal(row)

def test_positive_actual_terminal_identity_accepted(m):
    row={'job_id_raw':m.OLD_JOB,'user':'pwa209','account':'def-ptewarie_cpu','state':'CANCELLED','cpus':1,'elapsed_seconds':100,'alloc_tres':'cpu=1,mem=16G'}
    assert m.actual_terminal(row)==row
