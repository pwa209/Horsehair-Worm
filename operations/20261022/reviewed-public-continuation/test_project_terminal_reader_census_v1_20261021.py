"""Synthetic census metadata only; listed source paths never exist or open."""
from pathlib import Path
from datetime import datetime, timezone
import ast
import copy
import importlib.util
import json
import stat
import pytest

spec=importlib.util.spec_from_file_location('census_projection',Path(__file__).with_name('project-terminal-reader-census-v1-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def item(relative,size=3):
    raw=b'x'*size;hash=m.sha(raw)
    return {'relative_path':relative,'path':str(m.ROOT/relative),'schema_version':1,'status':'measured',
        'method':'owned_regular_length','bytes':size,'max_bytes':m.STUDY_BYTES,'prefix_cap_bytes':65536,
        'prefix_bytes':size,'prefix_sha256':hash,'content_sha256':hash,'eof_observed':True,
        'identity':{'inode':1,'uid':m.UID,'mode':stat.S_IFREG|0o600,'nlink':1,'device_major':0,'device_minor':1,'ctime_ns':123},
        'size_proof':{'kind':'prefix_eof','probes':([{'offset':size-1,'bytes':1}] if size else [])+[{'offset':size,'bytes':0}]}}


def documents(rows=None):
    rows=rows or [item('maintenance/worker/native-123/accounting-reads/read-one/attempt-1-stdout.txt'),
        item('maintenance/worker/native-123/scheduler-reads/result.json'),
        item('maintenance/worker/native-123/quota-reads/attempt-1-after.json'),
        item('maintenance/controller/registered-snapshot-reads/read-one/attempt-1-result.json'),
        item('maintenance/controller/registration-lock-00001/lock-attempt-01-intent.json'),
        item('maintenance/worker/native-123/receipt.json'),item('state/resource-budget.json'),
        item('plans/test/plan.json'),item('maintenance/unresolved/other.txt')]
    entries={'schema_version':1,'status':'measured','method':'owned_regular_length','root':str(m.ROOT),
        'count':len(rows),'bytes':sum(r['bytes'] for r in rows),'items':rows}
    raw=(json.dumps(entries,sort_keys=True)+'\n').encode();entry_sha=m.sha(raw)
    budget={'status':'measured','root':str(m.ROOT),'study_files':entries['count'],'study_bytes':entries['bytes'],
        'measured_unix':1000.,'captured_utc':datetime.fromtimestamp(1000,timezone.utc).isoformat(),
        'exact_entry_measurements':{'receipt_path':str(m.ROOT/'state/storage-entry-sizes.json'),'receipt_sha256':entry_sha,
            'method':'owned_regular_length','count':entries['count'],'bytes':entries['bytes']}}
    return budget,entries,entry_sha,raw


def test_exhaustive_metadata_groups_preserve_unknown_producers_and_zero_credit():
    b,e,h,_=documents();before=copy.deepcopy((b,e));r=m.project_documents(b,e,h)
    assert r['source_regular_files']==9 and r['source_regular_bytes']==27
    assert r['groups']=={'candidate_path_only':{'files':5,'bytes':15},'protected':{'files':3,'bytes':9},'unresolved':{'files':1,'bytes':3}}
    assert len(r['rows'])==9 and r['classification_roster_sha256']==m.canonical(r['rows'])
    assert (b,e)==before and r['credited_regular_files']==r['credited_bytes']==0
    assert r['actual_archive_eligible_regular_files'] is None
    assert all(r[key] is False for key in m.FALSE_FLAGS)
    assert all(p['producer_identity_status']=='unresolved_no_native_proof' for p in r['producer_namespace_groups'])
    assert r['original_census_entry_path']==str(m.ROOT/'state/storage-entry-sizes.json')


def test_projector_never_opens_stats_resolves_or_measures_any_listed_path(monkeypatch):
    b,e,h,_=documents()
    def forbidden(*a,**k):pytest.fail('Metadata projection attempted filesystem I/O')
    with monkeypatch.context() as patch:
        for name in ('open','stat','resolve','read_bytes','read_text'):patch.setattr(Path,name,forbidden)
        patch.setattr(m.os,'open',forbidden);patch.setattr(m.os,'stat',forbidden)
        r=m.project_documents(b,e,h)
    assert r['source_regular_files']==9


@pytest.mark.parametrize('relative',['../state/a','/scratch/a','a/../b','maintenance//a','maintenance/./a','maintenance\\a',''])
def test_noncanonical_or_traversal_path_metadata_refused(relative):
    with pytest.raises(ValueError):m.relative_name(relative)


@pytest.mark.parametrize('path,kind',[
    ('maintenance/n/native-999/accounting-reads/read-1/result.json','candidate_path_only'),
    ('maintenance/n/native-999/accounting-reads/read-1/attempt-4-query.json','unresolved'),
    ('maintenance/n/native-999/accounting-reads/read-1/result.json.partial','unresolved'),
    ('maintenance/n/native-999/registered-snapshot-reads/installation.json','candidate_path_only'),
    ('maintenance/n/accounting-reads/read-1/quota-reads/intent.json','unresolved'),
    ('maintenance/n/native-999/accounting-reads/read-1/checkpoint.pt','protected'),
    ('maintenance/n/native-999/accounting-reads/read-1/execution.mutex.owner.json','protected'),
    ('maintenance/n/descriptor-v2-worker.json','protected'),
    ('results/agents/final/checkpoint.pt','protected'),
    ('raw/rna/file.fastq.gz','protected')])
def test_exact_path_families_do_not_infer_terminal_status_or_protect_by_name_only(path,kind):
    r=m.classify(path);assert r['classification']==kind
    assert not any(k in r for k in ('native_job_id','terminal','owner','eligible'))


@pytest.mark.parametrize('change',['entry_sha','source_path','count','bytes','duplicate','identity_uid','identity_mode','eof','probe','relative_absolute','method'])
def test_changed_or_mixed_size_metadata_refused(change):
    b,e,h,_=documents()
    if change=='entry_sha':h='f'*64
    elif change=='source_path':b['exact_entry_measurements']['receipt_path']=str(m.ROOT/'maintenance/rebased.json')
    elif change=='count':b['study_files']+=1
    elif change=='bytes':e['bytes']+=1
    elif change=='duplicate':e['items'][1]=copy.deepcopy(e['items'][0])
    elif change=='identity_uid':e['items'][0]['identity']['uid']=0
    elif change=='identity_mode':e['items'][0]['identity']['mode']=stat.S_IFLNK|0o777
    elif change=='eof':e['items'][0]['eof_observed']=False
    elif change=='probe':e['items'][0]['size_proof']['probes'][-1]['bytes']=1
    elif change=='relative_absolute':e['items'][0]['path']=str(m.ROOT/'other')
    else:e['items'][0]['method']='estimated'
    with pytest.raises(ValueError):m.project_documents(b,e,h)


def test_large_prefix_is_not_reported_as_whole_content_hash():
    # Reproduce the exact original boundary algorithm on metadata, not a file.
    n=100000;row=item('maintenance/a/native-1/accounting-reads/read-1/attempt-1-stdout.txt')
    probes=[]
    def probe(offset):
        answer=int(offset<n);probes.append({'offset':offset,'bytes':answer});return answer
    assert m._boundary_length(65536,m.STUDY_BYTES,probe)==n
    probes.extend([{'offset':n-1,'bytes':1},{'offset':n,'bytes':0}])
    row.update(bytes=n,prefix_bytes=65536,prefix_sha256='a'*64,size_proof={'kind':'pread_first_eof','probes':probes})
    del row['content_sha256'];b,e,h,_=documents([row]);r=m.project_documents(b,e,h)
    assert r['rows'][0]['content_sha256'] is None and r['rows'][0]['content_hash_status']=='prefix_only_not_a_whole_file_hash'


def test_deadline_interrupts_without_partial_complete_result():
    b,e,h,_=documents();calls=[]
    def check():
        calls.append(1)
        if len(calls)==4:raise TimeoutError('finite metadata deadline')
    with pytest.raises(TimeoutError):m.project_documents(b,e,h,checkpoint=check)


def test_pure_validator_functions_are_ast_identical_to_qualified_source():
    from horsehair import storage_artifacts as original
    raw=Path(original.__file__).read_bytes();assert m.sha(raw)==m.STORAGE_MODULE_SHA
    source=ast.parse(raw);copied=ast.parse(Path(m.__file__).read_bytes())
    names={'StorageArtifactError','_relative_parts','_regular_identity','_length_arguments','_boundary_length','validate_regular_length_measurement'}
    a={n.name:ast.dump(n,include_attributes=False) for n in source.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in names}
    b={n.name:ast.dump(n,include_attributes=False) for n in copied.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in names}
    assert a==b and len(a)==6


@pytest.mark.parametrize('raw',[b'{"a":1,"a":2}',b'{"x":NaN}',b'{"x":Infinity}'])
def test_json_duplicate_or_nonfinite_refused(raw):
    with pytest.raises(ValueError):m.strict(raw)


def test_cli_reads_only_exact_immutable_pair_and_publishes_once(tmp_path,monkeypatch):
    support=tmp_path/'maintenance/census';support.mkdir(parents=True)
    monkeypatch.setattr(m,'CENSUS_SUPPORT',support)
    b,e,h,raw=documents();budget=support/'original-storage-budget.json';entries=support/'original-storage-entry-sizes.json'
    budget.write_text(json.dumps(b));entries.write_bytes(raw)
    monkeypatch.setattr(m,'BUDGET_SHA',m.sha(budget.read_bytes()));monkeypatch.setattr(m,'ENTRIES_SHA',h)
    output=support.parent/'projection.json'
    args=['--budget',str(budget),'--budget-sha256',m.BUDGET_SHA,'--entries',str(entries),'--entries-sha256',h,'--output',str(output)]
    assert m.main(args)==0
    r=json.loads(output.read_text());assert r['credited_regular_files']==0 and r['source_regular_files']==9
    assert r['input_documents']['entries']=={'path':str(entries),'sha256':h}
    assert r['original_census_entry_path']==str(m.ROOT/'state/storage-entry-sizes.json')
    with pytest.raises(ValueError,match='Fresh confined'):m.main(args)
    entries.write_text('changed')
    with pytest.raises(ValueError,match='bytes changed'):m.main(args[:-1]+[str(support.parent/'new.json')])


def test_input_paths_are_not_rebased_to_arbitrary_copies(tmp_path,monkeypatch):
    support=tmp_path/'census';support.mkdir();monkeypatch.setattr(m,'CENSUS_SUPPORT',support)
    other=tmp_path/'arbitrary.json';other.write_text('{}')
    with pytest.raises(ValueError,match='Exact ordinary'):m.bound_input(other,m.sha(b'{}'),'original-storage-budget.json')
