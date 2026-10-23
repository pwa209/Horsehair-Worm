"""Private completion-order census scheduling with unchanged entry proofs.

The qualified storage_artifacts module still performs every path, identity,
ownership, EOF, and bounded live-race retry check. Only traversal scheduling is
replaced. Completed rows can be yielded while bounded other workers remain
active; each returned row's measurement has already closed its own descriptors.
Directory names and queued-directory identities retain the original policy.
Only pending measurement work/results are bounded here, not the caller's final
receipt list or the original directory-name/queued-directory rosters.

Cancellation stops submission and cancels queued futures. Cleanup joins running
workers and can wait for native filesystem I/O: threads cannot kill D-state I/O.
The enclosing native allocation/process must enforce the hard execution bound.
"""
import math
import os
import stat
import time
from pathlib import PurePath,PurePosixPath
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED

from horsehair import storage_artifacts as ARTIFACTS

MAX_WORKERS=64
MAX_PENDING=128
FD_RESERVE=128
CONTROL_POLL_SECONDS=0.1
ORIGINAL_MEASUREMENT_NAMES=('_require_linux','_relative_parts','_length_arguments',
    '_open_directory','_metadata','_directory_identity','_stable_regular_length',
    'measure_regular_length','measure_regular_eof','validate_regular_length_measurement')


class CensusCancelled(ARTIFACTS.StorageArtifactError):
    """No complete census exists after cooperative cancellation."""


class CensusDeadlineExceeded(TimeoutError):
    """The cooperative census deadline expired; no complete result exists."""


def _control(cancelled,deadline,clock):
    if cancelled is not None and cancelled():
        raise CensusCancelled('Census cancelled; partial measurements do not authorize a census')
    if deadline is not None and clock()>=deadline:
        raise CensusDeadlineExceeded('Cooperative census deadline expired; native I/O still requires process supervision')


def _open_descriptor_count():
    # Local procfs names only. Include the counting iterator's own descriptor
    # conservatively; no entry type/stat call or measured-file getattr is used.
    with os.scandir('/proc/self/fd') as entries:
        return sum(1 for _ in entries)


def _descriptor_limits():
    import resource
    soft,_=resource.getrlimit(resource.RLIMIT_NOFILE)
    return soft,resource.RLIM_INFINITY


def fd_capacity(directory_depth,requested_pending):
    """Reserve the full ancestry demand of every queued or running entry."""
    if type(directory_depth) is not int or directory_depth<0:
        raise ARTIFACTS.StorageArtifactError('Directory depth must be a nonnegative integer')
    if type(requested_pending) is not int or not 1<=requested_pending<=MAX_PENDING:
        raise ARTIFACTS.StorageArtifactError('Pending census work must be an integer within 1..128')
    soft,infinity=_descriptor_limits();opened=_open_descriptor_count();per_entry=directory_depth+2
    if type(soft) is not int or type(opened) is not int or opened<0 or (soft<0 and soft!=infinity):
        raise ARTIFACTS.StorageArtifactError('Actual native file-descriptor capacity is invalid')
    infinite=soft==infinity
    available=None if infinite else max(0,soft-opened-FD_RESERVE)
    limit=requested_pending if infinite else min(requested_pending,available//per_entry)
    if limit<1:
        raise ARTIFACTS.StorageArtifactError('Native file-descriptor capacity cannot safely measure one census entry')
    return {'soft_limit':None if infinite else soft,'infinite_soft_limit':infinite,
        'open_descriptors':opened,'reserved_descriptors':FD_RESERVE,'descriptors_per_entry':per_entry,
        'requested_pending':requested_pending,'effective_pending':limit,
        'queued_entries_reserve_full_ancestry':True}


def _measured(path,root,max_bytes,uid,clock):
    started=clock()
    value=ARTIFACTS._stable_regular_length(path,max_bytes,uid,root=root)
    return value,max(0.,clock()-started)


def _parallel_lengths(paths,root,max_bytes,uid,executor,limit,*,cancelled,deadline,clock,completed):
    """Yield ready measurements from a bounded window, never a directory list."""
    source=iter(paths);pending={};sequence=0;exhausted=False
    def fill():
        nonlocal sequence,exhausted
        while not exhausted and len(pending)<limit:
            _control(cancelled,deadline,clock)
            try:path=next(source)
            except StopIteration:exhausted=True;break
            future=executor.submit(_measured,path,root,max_bytes,uid,clock)
            pending[future]=(sequence,path);sequence+=1
    try:
        fill()
        while pending:
            _control(cancelled,deadline,clock)
            timeout=CONTROL_POLL_SECONDS if deadline is None else min(CONTROL_POLL_SECONDS,max(0.,deadline-clock()))
            done,_=wait(pending,timeout=timeout,return_when=FIRST_COMPLETED)
            if not done:continue
            # Simultaneously ready results have a deterministic tie order.
            # Resolve every ready exception before yielding this bounded batch.
            rows=[]
            for future in sorted(done,key=lambda value:pending[value][0]):
                _,path=pending.pop(future);value,elapsed=future.result()
                rows.append((path,value,elapsed))
            _control(cancelled,deadline,clock);fill()
            for path,value,elapsed in rows:
                _control(cancelled,deadline,clock)
                completed(path,value,elapsed,len(pending))
                yield path,value
    finally:
        for future in pending:future.cancel()


def iter_regular_lengths(root,max_bytes,expected_uid=None,*,workers=16,pending_limit=None,
                         progress=None,cancelled=None,deadline=None,clock=time.monotonic):
    """Yield the original measurements with bounded completion-order work.

    Directory discovery, lexical checks, anchored no-follow opens, queued
    directory identities, and measurement/retry functions retain their original
    behavior. Duplicate directory names are refused rather than counted twice.
    Progress reports bookkeeping only and never makes a partial census valid.
    """
    ARTIFACTS._require_linux()
    raw_root=os.fspath(root);flavor=PurePosixPath if raw_root.startswith('/') and '\\' not in raw_root else PurePath
    root=flavor(raw_root);_,root,_=ARTIFACTS._relative_parts(root/'__entry_anchor__',root)
    uid=os.geteuid() if expected_uid is None else expected_uid
    ARTIFACTS._length_arguments(max_bytes,uid)
    if type(workers) is not int or not 1<=workers<=MAX_WORKERS:
        raise ARTIFACTS.StorageArtifactError('Entry census workers must be an integer within 1..64')
    pending_limit=2*workers if pending_limit is None else pending_limit
    if type(pending_limit) is not int or not 1<=pending_limit<=min(MAX_PENDING,2*workers):
        raise ARTIFACTS.StorageArtifactError('Pending census work must be within 1..min(128, twice workers)')
    if not callable(clock) or (progress is not None and not callable(progress)) or (cancelled is not None and not callable(cancelled)):
        raise ARTIFACTS.StorageArtifactError('Census clock, progress and cancellation hooks must be callable')
    if deadline is not None and (type(deadline) not in (int,float) or not math.isfinite(deadline)):
        raise ARTIFACTS.StorageArtifactError('A census deadline must be a finite monotonic timestamp')
    started=clock();counts={'entries':0,'regular_files':0,'bytes':0};pending_dirs=[((),None)]
    executor=ThreadPoolExecutor(max_workers=workers,thread_name_prefix='private-storage-size') if workers>1 else None
    try:
        while pending_dirs:
            _control(cancelled,deadline,clock);parts,expected=pending_dirs.pop()
            descriptors=[];failed=False
            try:
                parent=ARTIFACTS._open_directory(os.fspath(root));descriptors.append(parent)
                for name in parts:
                    parent=ARTIFACTS._open_directory(name,parent);descriptors.append(parent)
                observed=ARTIFACTS._metadata('',parent)
                if not stat.S_ISDIR(observed['mode']):
                    raise ARTIFACTS.StorageArtifactError('Entry census anchor is not a directory')
                if expected is not None and ARTIFACTS._directory_identity(observed)!=ARTIFACTS._directory_identity(expected):
                    raise ARTIFACTS.StorageArtifactRace(root.joinpath(*parts),'Queued census directory changed before anchored traversal')
                with ARTIFACTS.os.scandir(parent) as entries:
                    names=[entry.name for entry in entries]
            except BaseException:
                failed=True;raise
            finally:
                close_error=None
                for descriptor in reversed(descriptors):
                    try:os.close(descriptor)
                    except OSError as error:
                        if close_error is None:close_error=error
                if close_error is not None and not failed:raise close_error
            paths=[];seen=set()
            for name in names:
                _control(cancelled,deadline,clock)
                if (not isinstance(name,str) or name in ('','.','..') or '/' in name or '\x00' in name
                        or (flavor is not PurePosixPath and '\\' in name)):
                    raise ARTIFACTS.StorageArtifactError('Native directory entry has an unsafe lexical name')
                if name in seen:
                    raise ARTIFACTS.StorageArtifactRace(root.joinpath(*parts),'Directory census roster repeated a name')
                seen.add(name);paths.append(root.joinpath(*parts,name))
            if not paths:continue
            capacity=fd_capacity(len(parts),pending_limit);limit=capacity['effective_pending']
            def completed(path,value,elapsed,pending_work):
                counts['entries']+=1
                if value['status']=='measured':counts['regular_files']+=1;counts['bytes']+=value['bytes']
                if progress is not None:
                    progress({'event':'entry_complete','path':os.fspath(path),'status':value['status'],**counts,
                        'elapsed_seconds':max(0.,clock()-started),'measurement_seconds':elapsed,
                        'requested_workers':workers,'pending_work':pending_work,'pending_limit':limit,
                        'fd_capacity':dict(capacity),'partial_census_authority':False})
            if executor is None:
                def sequential():
                    for path in paths:
                        _control(cancelled,deadline,clock)
                        value,elapsed=_measured(path,root,max_bytes,uid,clock)
                        _control(cancelled,deadline,clock);completed(path,value,elapsed,0)
                        yield path,value
                measurements=sequential()
            else:
                measurements=_parallel_lengths(paths,root,max_bytes,uid,executor,limit,cancelled=cancelled,
                    deadline=deadline,clock=clock,completed=completed)
            try:
                for path,measured in measurements:
                    if measured['status']=='directory':pending_dirs.append(((*parts,path.name),measured['identity']))
                    else:yield path,measured
            finally:
                measurements.close()
    finally:
        if executor is not None:executor.shutdown(wait=True,cancel_futures=True)
