"""Project a pinned census using metadata alone; no source-leaf I/O or credit.

Native qualification, producer ownership, allocation and storage admission are
the caller's responsibility. Path families are candidates only: no producer
job ID, terminal status or transitive unreachability is inferred from a name.
"""
from pathlib import Path, PurePosixPath, PurePath
from collections import defaultdict
from datetime import datetime, timezone
import argparse
import hashlib
import json
import math
import os
import re
import stat
import time

ROOT=PurePosixPath('/scratch/pwa209/horsehair-worm')
CENSUS_SUPPORT=Path(str(ROOT/'maintenance/study-af-fresh-storage-census-v1-20261021'))
BUDGET_SHA='06fcae248ec48ad65a2db122919f1a29bd2aa82d11cd01927712e9aaa18b59f2'
ENTRIES_SHA='95cc190cc2fcfc0c9cf3458ca0dd00d508c16cdd7c9f4a87a413d9a44b8fc6d3'
STORAGE_MODULE_SHA='c2e1cbe785027f92fb30833a9fdfb58e7cdee9c3907a1d2eefb6deb8efa11968'
MAX_INPUT_BYTES=512*1024**2
MAX_OUTPUT_BYTES=256*1024**2
MAX_ROWS=300000
MAX_SECONDS=1800
ENTRY_PREFIX_CAP=65536
MAX_LOGICAL_BYTES=(1<<63)-2
STUDY_BYTES=6_000_000_000_000
UID=3164768
FALSE_FLAGS={'scientific_result':False,'admission_authority':False,
    'source_leaf_files_opened':False,'source_leaf_files_statted':False,
    'producer_native_identity_verified':False,'producer_terminal_status_verified':False,
    'transitive_unreachability_established':False,'archive_eligibility_established':False,
    'original_files_removed':False,'storage_credit_authorized':False}
PROTECTED_NAMES={'receipt.json','summary.json','evaluation_summary.json','plan.json',
    'plan-binding.json','campaign-plan.json','state.json.gz','state-binding.json',
    'frozen_protocol.json','checkpoint.pt','training_metrics.json','precision-gate.json',
    'qualification.json','junit.xml','release.json','descriptor.json'}
FAMILIES=(('accounting-reads','sacct358'),('native-accounting-reads','sacct358'),
    ('scheduler-reads','scontrol100d'),('native-scheduler-reads','scontrol100d'),
    ('held-reads','scontrol100d'),('quota-reads','quota5004'),
    ('qualification-quota-reads','quota5004'),('registered-snapshot-reads','registry2583'),
    ('registration-lock','registration-wait'),('registration-admission-wait','registration-wait'),
    ('registration-primary-plan-wait','registration-wait'))


def require(value,message):
    if not value:raise ValueError(message)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def canonical(value):return sha(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode())


def strict(raw):
    def pairs(items):
        result={}
        for key,value in items:
            require(key not in result,'Duplicate metadata JSON key');result[key]=value
        return result
    def invalid(value):raise ValueError('Nonfinite metadata literal refused')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)


def bound_input(path,expected_sha,name):
    path=Path(path);expected=CENSUS_SUPPORT/name
    require(path==expected and path.is_absolute() and path==path.resolve() and CENSUS_SUPPORT==CENSUS_SUPPORT.resolve() and not path.is_symlink() and path.is_file()
        and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(CENSUS_SUPPORT)),
        'Exact ordinary immutable census copy required')
    require(isinstance(expected_sha,str) and re.fullmatch('[0-9a-f]{64}',expected_sha),'Exact input SHA required')
    with path.open('rb') as stream:raw=stream.read(MAX_INPUT_BYTES+1)
    require(len(raw)<=MAX_INPUT_BYTES and sha(raw)==expected_sha,'Pinned bounded census bytes changed')
    return strict(raw)


def relative_name(value):
    require(isinstance(value,str) and value and '\\' not in value and '\x00' not in value,
        'Canonical POSIX census relative path required')
    p=PurePosixPath(value)
    require(not p.is_absolute() and str(p)==value and all(x not in {'.','..',''} for x in p.parts),
        'Census path traversal/normalization refused')
    return p


def classify(relative):
    """Pure string operations only; no Path.resolve/stat/open of any leaf."""
    p=relative_name(relative);parts=p.parts;name=p.name
    protected=(parts[0]!='maintenance' or name in PROTECTED_NAMES or name.startswith('assembled-')
        or '.mutex' in name or name.endswith(('.py','.pyc','.sh','.sbatch','.pt','.pth','.xml'))
        or 'qualification' in name or 'descriptor' in name)
    if protected:return {'classification':'protected','reason':'canonical_source_state_plan_result_or_outside_maintenance'}
    matched=[]
    for index,part in enumerate(parts):
        for prefix,family in FAMILIES:
            if part==prefix or part.startswith(prefix+'-'):matched.append((index,family))
    if not matched:return {'classification':'unresolved','reason':'no_frozen_reader_family_path'}
    index,family=min(matched)
    if len({(i,f) for i,f in matched})!=1:
        return {'classification':'unresolved','reason':'ambiguous_nested_reader_families'}
    if index<2:return {'classification':'unresolved','reason':'no_explicit_producer_namespace'}
    allowed=name in {'intent.json','result.json'}
    if family=='sacct358':allowed|=bool(re.fullmatch(r'attempt-[1-3]-(query|runner-error|response|verdict)\.json|attempt-[1-3]-(stdout|stderr)\.txt',name))
    elif family=='scontrol100d':allowed|=bool(re.fullmatch(r'attempt-[1-3]-(command|exception|response)\.json|attempt-[1-3]-(stdout|stderr)\.txt',name))
    elif family=='quota5004':allowed|=bool(re.fullmatch(r'attempt-[1-5]-(before|after)\.json',name))
    elif family=='registry2583':allowed|=name=='installation.json' or bool(re.fullmatch(r'attempt-[1-3]-(intent|result)\.json|lock-attempt-(?:[1-9]|[1-5][0-9]|60)-(intent|result)\.json',name)) or name=='pending.json'
    else:allowed|=name=='pending.json' or bool(re.fullmatch(r'lock-attempt-(?:0?[1-9]|[1-5][0-9]|60)-(intent|result)\.json|attempt-(?:0[1-9]|[12][0-9]|3[01])-(intent|result)\.json',name))
    if not allowed:return {'classification':'unresolved','reason':'unknown_or_partial_reader_leaf','reader_family':family}
    return {'classification':'candidate_path_only','reason':'producer_authority_and_reference_graph_unresolved',
        'reader_family':family,'producer_namespace':str(PurePosixPath(*parts[:index])),
        'producer_identity_status':'not_inferred_from_path'}


# Exact pure metadata validators copied from qualified4f storage_artifacts.py
# source c2e1cbe785027f92fb30833a9fdfb58e7cdee9c3907a1d2eefb6deb8efa11968.
# They do no filesystem I/O. Their AST identity is separately regression-tested.
class StorageArtifactError(ValueError):
    """No trustworthy exact size was measured; never substitute an estimate."""


def _relative_parts(path, root):
    """Lexical checks only: never resolve or stat a leaf/ancestor here."""
    raw_path, raw_root = os.fspath(path), os.fspath(root)
    flavor = PurePosixPath if raw_root.startswith('/') and '\\' not in raw_root else PurePath
    path, root = flavor(raw_path), flavor(raw_root)
    if (not path.is_absolute() or not root.is_absolute()
            or '..' in path.parts or '..' in root.parts
            or '\x00' in os.fspath(path) or '\x00' in os.fspath(root)):
        raise StorageArtifactError('Artifact paths require an absolute root without traversal')
    try:
        parts = path.relative_to(root).parts
    except ValueError as error:
        raise StorageArtifactError('Artifact path lies outside the authorized study root') from error
    if not parts:
        raise StorageArtifactError('Artifact path must name a descendant file')
    return path, root, parts


def _regular_identity(identity, expected_uid):
    fields = {'inode', 'uid', 'mode', 'nlink', 'device_major', 'device_minor', 'ctime_ns'}
    if (not isinstance(identity, dict) or set(identity) != fields
            or any(type(identity[key]) is not int or identity[key] < 0 for key in fields)
            or identity['inode'] == 0 or identity['nlink'] == 0
            or identity['uid'] != expected_uid or not stat.S_ISREG(identity['mode'])):
        raise StorageArtifactError('Entry size requires an owned regular-file identity')


def _length_arguments(max_bytes, expected_uid):
    if type(max_bytes) is not int or not 0 <= max_bytes <= MAX_LOGICAL_BYTES:
        raise StorageArtifactError('Entry size ceiling must fit a nonnegative signed pread offset')
    if type(expected_uid) is not int or expected_uid < 0:
        raise StorageArtifactError('Entry owner must be an explicit native effective UID')


def _boundary_length(prefix_bytes, max_bytes, probe):
    """Find the first EOF using regular-file byte-read monotonicity.

    A hole before EOF returns a NUL byte, not an empty read. No SEEK_END or
    cached inode size is used. ``probe`` records or validates each observation.
    """
    low, high = prefix_bytes - 1, prefix_bytes
    while probe(high):
        if high == max_bytes:
            raise StorageArtifactError('Entry exceeds the measured study byte ceiling')
        low, high = high, min(max_bytes, high * 2)
    while high - low > 1:
        middle = (low + high) // 2
        if probe(middle):
            low = middle
        else:
            high = middle
    return high


def validate_regular_length_measurement(item, *, expected_uid, max_bytes, root, path=None):
    """Validate an exact-size receipt without inspecting any measured leaf.

    The transcript replays the fixed prefix/exponential/binary algorithm and
    its final adjacent positive/EOF observations. It is provenance of a native
    point-in-time observation, not a cryptographic proof of immutable content.
    """
    _length_arguments(max_bytes, expected_uid)
    if not isinstance(item, dict) or not isinstance(item.get('path'), str):
        raise StorageArtifactError('Entry size receipt requires its absolute lexical path')
    entry_path, _, _ = _relative_parts(item['path'], root)
    if item['path'] != os.fspath(entry_path):
        raise StorageArtifactError('Entry size receipt path is not canonical lexical text')
    if path is not None and entry_path != _relative_parts(path, root)[0]:
        raise StorageArtifactError('Entry size receipt belongs to another path')
    size, prefix = item.get('bytes'), item.get('prefix_bytes')
    if (type(item.get('schema_version')) is not int or item['schema_version'] != 1
            or item.get('status') != 'measured' or item.get('method') != 'owned_regular_length'
            or type(size) is not int or not 0 <= size <= max_bytes
            or type(item.get('max_bytes')) is not int or item['max_bytes'] != max_bytes
            or type(item.get('prefix_cap_bytes')) is not int or item['prefix_cap_bytes'] != ENTRY_PREFIX_CAP
            or type(prefix) is not int or not 0 <= prefix <= min(ENTRY_PREFIX_CAP, max_bytes + 1)
            or not isinstance(item.get('prefix_sha256'), str)
            or re.fullmatch(r'[0-9a-f]{64}', item['prefix_sha256']) is None
            or item.get('eof_observed') is not True):
        raise StorageArtifactError('Entry size receipt has invalid size, prefix or method provenance')
    _regular_identity(item.get('identity'), expected_uid)
    proof = item.get('size_proof')
    if (not isinstance(proof, dict) or set(proof) != {'kind', 'probes'}
            or not isinstance(proof.get('probes'), list) or len(proof['probes']) > 256):
        raise StorageArtifactError('Entry size receipt lacks a bounded probe transcript')
    position = 0
    def probe(offset):
        nonlocal position
        if position >= len(proof['probes']):
            raise StorageArtifactError('Entry size probe transcript ended before its EOF boundary')
        row = proof['probes'][position]; position += 1
        if (not isinstance(row, dict) or set(row) != {'offset', 'bytes'}
                or type(row['offset']) is not int or row['offset'] != offset
                or not 0 <= row['offset'] <= max_bytes
                or type(row['bytes']) is not int or row['bytes'] not in (0, 1)):
            raise StorageArtifactError('Entry size probe transcript does not match the exact boundary algorithm')
        return row['bytes']
    if proof['kind'] == 'prefix_eof':
        if (size != prefix or prefix >= min(ENTRY_PREFIX_CAP, max_bytes + 1)
                or item.get('content_sha256') != item['prefix_sha256']):
            raise StorageArtifactError('Prefix EOF receipt does not hash its entire observed content')
    elif proof['kind'] == 'pread_first_eof':
        if prefix != ENTRY_PREFIX_CAP or prefix > max_bytes or 'content_sha256' in item:
            raise StorageArtifactError('Boundary receipt must not claim an unread whole-content hash')
        if _boundary_length(prefix, max_bytes, probe) != size:
            raise StorageArtifactError('Entry size does not equal its first observed EOF')
    else:
        raise StorageArtifactError('Entry size receipt uses an unknown boundary proof')
    if size and probe(size - 1) != 1:
        raise StorageArtifactError('Entry last-byte boundary is no longer positive')
    if probe(size) != 0:
        raise StorageArtifactError('Entry EOF boundary is no longer empty')
    if position != len(proof['probes']):
        raise StorageArtifactError('Entry size probe transcript has unexpected trailing observations')
    return size


def project_documents(budget,entries,entries_sha256,*,checkpoint=lambda:None):
    """Validate complete point-in-time metadata and return an exhaustive grouping."""
    checkpoint();pair=budget.get('exact_entry_measurements',{})
    require(budget.get('status')=='measured' and budget.get('root')==str(ROOT)
        and entries.get('schema_version')==1 and type(entries.get('schema_version')) is int
        and entries.get('status')=='measured' and entries.get('root')==str(ROOT)
        and pair.get('receipt_path')==str(ROOT/'state/storage-entry-sizes.json')
        and pair.get('receipt_sha256')==entries_sha256
        and pair.get('method')==entries.get('method')=='owned_regular_length',
        'Unchanged exact census pair/root/method required; original paths are not rebased')
    measured=budget.get('measured_unix');captured=datetime.fromisoformat(budget.get('captured_utc','').replace('Z','+00:00'))
    require(type(measured) in (int,float) and math.isfinite(measured) and measured>=0 and captured.tzinfo is not None,
        'Original finite measured and timezone-aware capture clocks required')
    rows=entries.get('items');require(isinstance(rows,list) and 0<len(rows)<=MAX_ROWS,'Finite complete regular-file roster required')
    for value in (entries.get('count'),pair.get('count'),budget.get('study_files')):
        require(type(value) is int and value==len(rows),'Census count differs from complete roster')
    expected_bytes=entries.get('bytes')
    require(type(expected_bytes) is int and 0<=expected_bytes<=STUDY_BYTES
        and type(pair.get('bytes')) is int and pair['bytes']==expected_bytes
        and type(budget.get('study_bytes')) is int and budget['study_bytes']==expected_bytes,'Census byte totals changed')
    seen=set();total=0;projection=[];groups=defaultdict(lambda:{'files':0,'bytes':0});producers=defaultdict(lambda:{'files':0,'bytes':0,'families':defaultdict(int)})
    for row in rows:
        checkpoint();relative=row.get('relative_path');p=relative_name(relative)
        require(relative not in seen,'Duplicate census relative path');seen.add(relative)
        require(row.get('path')==str(ROOT/p),'Census absolute lexical path differs from relative path')
        size=validate_regular_length_measurement(row,expected_uid=UID,max_bytes=STUDY_BYTES,root=str(ROOT),path=str(ROOT/p))
        require(size==row['bytes'],'Validated size differs');total+=size
        label=classify(relative);group=label['classification']
        groups[group]['files']+=1;groups[group]['bytes']+=size
        if group=='candidate_path_only':
            producer=producers[label['producer_namespace']];producer['files']+=1;producer['bytes']+=size;producer['families'][label['reader_family']]+=1
        projection.append({'relative_path':relative,'bytes':size,'source_record_sha256':canonical(row),
            'content_sha256':row.get('content_sha256'),
            'content_hash_status':'whole_content_observed_in_original_census' if 'content_sha256' in row else 'prefix_only_not_a_whole_file_hash',**label})
    require(total==expected_bytes and sum(x['files'] for x in groups.values())==len(rows),'Complete classification totals differ')
    checkpoint()
    return {'schema_version':1,'status':'complete_metadata_only_census_projection','root':str(ROOT),
        'census_measured_unix':measured,'census_captured_utc':budget['captured_utc'],'freshness_rewritten':False,
        'original_census_entry_path':pair['receipt_path'],'original_census_entry_sha256':entries_sha256,
        'source_regular_files':len(rows),'source_regular_bytes':total,'groups':dict(sorted(groups.items())),
        'producer_namespace_groups':[{'producer_namespace':key,'producer_identity_status':'unresolved_no_native_proof',
            **{k:v for k,v in value.items() if k!='families'},'families':dict(sorted(value['families'].items()))}
            for key,value in sorted(producers.items(),key=lambda item:(-item[1]['files'],item[0]))],
        'rows':projection,'classification_roster_sha256':canonical(projection),
        'credited_regular_files':0,'credited_bytes':0,'actual_archive_eligible_regular_files':None,
        'projected_no_delete_net_savings':0,
        'pending':['fresh producer ownership plus separate final cost proof','current transitive path/SHA consumer graph',
            'exact archive bucket/authority/native-wrapper overhead and peak admission','independent roundtrip and explicit consumer qualification'],
        'scope':'Point-in-time census classification only; no current source-leaf observation, terminal assertion, archive, unlink or admission',**FALSE_FLAGS}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--budget',type=Path,required=True);p.add_argument('--budget-sha256',required=True)
    p.add_argument('--entries',type=Path,required=True);p.add_argument('--entries-sha256',required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--max-seconds',type=int,default=MAX_SECONDS)
    args=p.parse_args(argv)
    require(0<args.max_seconds<=MAX_SECONDS,'Finite metadata projection deadline required')
    require(args.budget_sha256==BUDGET_SHA and args.entries_sha256==ENTRIES_SHA,'Exact root-supplied immutable census SHA pair required')
    output=args.output;namespace=CENSUS_SUPPORT.parent
    require(output.is_absolute() and not output.exists() and output.resolve().is_relative_to(namespace)
        and not output.is_symlink() and all(not q.is_symlink() for q in output.parents if q.is_relative_to(namespace)),
        'Fresh confined metadata output required')
    deadline=time.monotonic()+args.max_seconds
    def checkpoint():require(time.monotonic()<deadline,'Bounded metadata projection deadline exhausted')
    budget=bound_input(args.budget,BUDGET_SHA,'original-storage-budget.json')
    entries=bound_input(args.entries,ENTRIES_SHA,'original-storage-entry-sizes.json')
    result=project_documents(budget,entries,ENTRIES_SHA,checkpoint=checkpoint)
    # Recheck only the two immutable input documents, never their measured leaves.
    bound_input(args.budget,BUDGET_SHA,'original-storage-budget.json');bound_input(args.entries,ENTRIES_SHA,'original-storage-entry-sizes.json');checkpoint()
    result.update(input_documents={'budget':{'path':str(args.budget),'sha256':BUDGET_SHA},
        'entries':{'path':str(args.entries),'sha256':ENTRIES_SHA}},native_wrapper_source_and_qualification_proof_required=True,
        pure_validator_origin_source_sha256=STORAGE_MODULE_SHA,finished_utc=datetime.now(timezone.utc).isoformat())
    raw=(json.dumps(result,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    require(len(raw)<=MAX_OUTPUT_BYTES,'Bounded complete projection output exceeded')
    with output.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    print(json.dumps({'status':result['status'],'source_regular_files':result['source_regular_files'],
        'groups':result['groups'],'output_sha256':sha(raw),'credited_regular_files':0,'scientific_result':False,'admission_authority':False},sort_keys=True))
    return 0


if __name__=='__main__':raise SystemExit(main())
