"""Private scheduling/FD/race contracts using disposable synthetic entries."""
from pathlib import Path,PurePosixPath
from types import SimpleNamespace
from contextlib import nullcontext
import ast
import errno
import importlib.util
import os
import stat
import threading
import time

import pytest

SOURCE=Path(__file__).with_name('streaming-storage-census-v1-20261023.py')
spec=importlib.util.spec_from_file_location('private_streaming_census',SOURCE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


@pytest.fixture
def filesystem(monkeypatch):
    root=PurePosixPath('/synthetic/study');tree={root:['a','b','c']};identities={};opened={};calls=[];finished=[]
    lock=threading.Lock();counter=[1000000]
    fs=SimpleNamespace(root=root,tree=tree,identities=identities,opened=opened,calls=calls,finished=finished,
        lock=lock,active=0,max_active=0,hook=None,after=None,statuses={},receipts={})
    def identity(path):
        if path not in identities:
            identities[path]={'inode':len(identities)+1,'uid':42,'mode':stat.S_IFDIR|0o700,'nlink':2,
                'device_major':1,'device_minor':2,'ctime_ns':3}
        return dict(identities[path])
    def open_directory(name,parent=None):
        path=PurePosixPath(name) if parent is None else opened[parent]/name
        if path not in tree:raise FileNotFoundError(str(path))
        counter[0]+=1;opened[counter[0]]=path;return counter[0]
    original_close=os.close;original_scandir=os.scandir
    def close(descriptor):
        if descriptor in opened:del opened[descriptor]
        else:original_close(descriptor)
    def scandir(descriptor):
        if type(descriptor) is int and descriptor in opened:
            # These entries intentionally offer no is_file/is_dir/stat methods.
            return nullcontext(iter(SimpleNamespace(name=name) for name in tree[opened[descriptor]]))
        return original_scandir(descriptor)
    def measure(path,max_bytes,uid,*,root):
        assert not opened,'Directory discovery descriptors must be closed before measurement'
        assert uid==42 and max_bytes==1000 and root==fs.root
        with lock:
            calls.append(path);fs.active+=1;fs.max_active=max(fs.max_active,fs.active)
        try:
            if fs.hook is not None:fs.hook(path)
            if path in tree:
                value={'status':'directory','path':str(path),'identity':identity(path)}
            elif path in fs.statuses:
                value={'status':fs.statuses[path],'path':str(path),'identity':{'unchanged':'explicit'}}
            else:
                value={'status':'measured','method':'owned_regular_length','path':str(path),'bytes':len(path.name),
                    'eof_observed':True,'original_proof_marker':object()}
            fs.receipts[path]=value
            if fs.after is not None:fs.after(path,value)
            return value
        finally:
            with lock:fs.active-=1;finished.append(path)
    monkeypatch.setattr(m.ARTIFACTS,'_require_linux',lambda:None)
    monkeypatch.setattr(m.ARTIFACTS,'_open_directory',open_directory)
    monkeypatch.setattr(m.ARTIFACTS,'_metadata',lambda name,fd:identity(opened[fd]))
    monkeypatch.setattr(m.ARTIFACTS,'_stable_regular_length',measure)
    monkeypatch.setattr(os,'close',close);monkeypatch.setattr(os,'scandir',scandir)
    def capacity(depth,requested):
        return {'soft_limit':4096,'infinite_soft_limit':False,'open_descriptors':8,'reserved_descriptors':128,
            'descriptors_per_entry':depth+2,'requested_pending':requested,'effective_pending':requested,
            'queued_entries_reserve_full_ancestry':True}
    monkeypatch.setattr(m,'fd_capacity',capacity)
    return fs


def invoke(fs,**kwargs):return m.iter_regular_lengths(fs.root,1000,42,**kwargs)


@pytest.mark.parametrize('workers',[1,2,16,64])
def test_every_enumerated_non_directory_path_is_yielded_once_with_original_receipt(filesystem,workers):
    fs=filesystem;sub=fs.root/'sub';fs.tree[fs.root]=['sub','a','link','special'];fs.tree[sub]=['b','c']
    fs.statuses[fs.root/'link']='symlink';fs.statuses[fs.root/'special']='special';events=[]
    rows=list(invoke(fs,workers=workers,progress=events.append));paths=[path for path,_ in rows]
    assert set(paths)=={fs.root/'a',sub/'b',sub/'c',fs.root/'link',fs.root/'special'} and len(paths)==len(set(paths))==5
    assert len(fs.calls)==len(set(fs.calls))==6 and fs.active==0 and not fs.opened
    assert all(value is fs.receipts[path] for path,value in rows)
    assert events[-1]['entries']==6 and events[-1]['regular_files']==3 and events[-1]['bytes']==3
    assert all(event['partial_census_authority'] is False and event['pending_work']<=event['pending_limit']<=2*workers for event in events)
    assert all(path in fs.finished for path in paths)


def test_slow_first_entry_does_not_block_completed_rows_or_window_refill(filesystem):
    fs=filesystem;fs.tree[fs.root]=['slow','fast','later'];release=threading.Event();slow_started=threading.Event()
    def hook(path):
        if path.name=='slow':
            slow_started.set();assert release.wait(5),'Test failed to release synthetic slow I/O'
    fs.hook=hook;events=[];iterator=invoke(fs,workers=2,pending_limit=2,progress=events.append)
    try:
        first,value=next(iterator)
        assert slow_started.is_set() and first.name=='fast' and fs.active>=1
        assert first in fs.finished and not fs.opened and events[-1]['pending_work']==2
        # The freed slot was refilled before returning the completed row.
        assert len(fs.calls)<=3
    finally:release.set()
    rest=list(iterator)
    assert {path.name for path,_ in rest}=={'slow','later'} and fs.active==0


def test_sixty_four_workers_overlap_without_exceeding_the_configured_bound(filesystem):
    fs=filesystem;fs.tree[fs.root]=['f'+str(i) for i in range(64)];release=threading.Event()
    def hook(path):
        with fs.lock:
            if fs.active==64:release.set()
        assert release.wait(10),'All configured I/O workers should enter before release'
    fs.hook=hook
    assert len(list(invoke(fs,workers=64,pending_limit=128)))==64
    assert fs.max_active==64 and fs.active==0


def test_effective_fd_window_limits_actual_concurrency(filesystem,monkeypatch):
    fs=filesystem;events=[];original=m.fd_capacity
    def capacity(depth,requested):return {**original(depth,requested),'effective_pending':1}
    monkeypatch.setattr(m,'fd_capacity',capacity)
    rows=list(invoke(fs,workers=64,pending_limit=128,progress=events.append))
    assert len(rows)==3 and fs.max_active==1
    assert all(event['pending_limit']==1 and event['pending_work']<=1 for event in events)


@pytest.mark.parametrize('field,value',[('inode',999),('uid',99),('mode',stat.S_IFDIR|0o755),('device_major',999)])
def test_queued_directory_identity_replacement_or_access_change_refuses_census(filesystem,field,value):
    fs=filesystem;sub=fs.root/'sub';fs.tree[fs.root]=['sub'];fs.tree[sub]=['leaf']
    def after(path,item):
        if path==sub:fs.identities[sub][field]=value
    fs.after=after
    with pytest.raises(m.ARTIFACTS.StorageArtifactRace,match='Queued census directory changed'):
        list(invoke(fs,workers=2))
    assert sub/'leaf' not in fs.calls and not fs.opened and fs.active==0


def test_legitimate_directory_child_change_keeps_original_identity_policy(filesystem):
    fs=filesystem;sub=fs.root/'sub';fs.tree[fs.root]=['sub'];fs.tree[sub]=['leaf']
    def after(path,item):
        if path==sub:fs.identities[sub].update(ctime_ns=999,nlink=9)
    fs.after=after
    assert [path for path,_ in invoke(fs,workers=2)]==[sub/'leaf']


@pytest.mark.parametrize('name',['','.', '..','bad/name','bad\x00name'])
def test_unsafe_directory_names_are_refused_before_any_measurement(filesystem,name):
    fs=filesystem;fs.tree[fs.root]=['good',name]
    with pytest.raises(m.ARTIFACTS.StorageArtifactError,match='unsafe lexical name'):list(invoke(fs,workers=2))
    assert not fs.calls and not fs.opened


def test_duplicate_directory_names_refuse_instead_of_dropping_or_double_counting(filesystem):
    fs=filesystem;fs.tree[fs.root]=['same','same']
    with pytest.raises(m.ARTIFACTS.StorageArtifactRace,match='repeated a name'):list(invoke(fs,workers=2))
    assert not fs.calls and not fs.opened


@pytest.mark.parametrize('error',[OSError(errno.EIO,'hard I/O failure'),OSError(errno.EMFILE,'too many descriptors'),ValueError('invalid EOF proof')])
def test_hard_measurement_failures_propagate_without_skip_retry_or_partial_success(filesystem,error):
    fs=filesystem;fs.tree[fs.root]=['bad']
    def hook(path):raise error
    fs.hook=hook
    with pytest.raises(type(error)) as caught:list(invoke(fs,workers=4))
    assert caught.value is error and fs.calls==[fs.root/'bad'] and fs.active==0 and not fs.opened


def test_late_failure_never_returns_a_complete_census(filesystem):
    fs=filesystem;fs.tree[fs.root]=['good','bad'];error=OSError(errno.EIO,'late failure')
    def hook(path):
        if path.name=='bad':raise error
    fs.hook=hook;published=[]
    def consumer():
        rows=list(invoke(fs,workers=1))
        published.append(rows)
    with pytest.raises(OSError):consumer()
    assert published==[] and fs.calls==[fs.root/'good',fs.root/'bad'] and fs.active==0


def test_progress_failure_closes_generator_and_joins_measurements(filesystem):
    fs=filesystem;error=RuntimeError('progress sink failed')
    def progress(event):raise error
    with pytest.raises(RuntimeError) as caught:list(invoke(fs,workers=4,progress=progress))
    assert caught.value is error and fs.active==0 and not fs.opened


def test_explicit_generator_close_cancels_queue_and_joins_running_work(filesystem):
    fs=filesystem;fs.tree[fs.root]=['f'+str(i) for i in range(20)]
    iterator=invoke(fs,workers=2,pending_limit=4)
    next(iterator);iterator.close()
    assert fs.active==0 and not fs.opened and len(fs.calls)<20


def test_cancelled_before_start_submits_no_work(filesystem):
    fs=filesystem
    with pytest.raises(m.CensusCancelled):list(invoke(fs,workers=2,cancelled=lambda:True))
    assert not fs.calls and not fs.opened


@pytest.mark.parametrize('workers',[1,2])
def test_post_measurement_deadline_check_refuses_late_rows(filesystem,workers):
    fs=filesystem;clock=[0.];fs.tree[fs.root]=['late']
    fs.hook=lambda path:clock.__setitem__(0,11.)
    with pytest.raises(m.CensusDeadlineExceeded):list(invoke(fs,workers=workers,deadline=10.,clock=lambda:clock[0]))
    assert fs.calls==[fs.root/'late'] and fs.active==0


@pytest.mark.parametrize('kwargs',[{'workers':0},{'workers':65},{'workers':True},{'workers':1.5},
    {'pending_limit':0},{'pending_limit':129},{'workers':2,'pending_limit':5},{'pending_limit':True},
    {'deadline':float('nan')},{'deadline':float('inf')},{'deadline':True},{'progress':1},{'cancelled':1},{'clock':1}])
def test_invalid_execution_controls_refuse_before_discovery_or_measurement(filesystem,kwargs):
    fs=filesystem
    with pytest.raises(m.ARTIFACTS.StorageArtifactError):list(invoke(fs,**kwargs))
    assert not fs.calls and not fs.opened


@pytest.mark.parametrize('soft,opened,depth,requested,expected',[(1024,8,0,128,128),(1024,8,20,128,40),
    (256,8,60,128,1),(-1,800,500,128,128)])
def test_fd_capacity_uses_actual_open_count_reserve_and_full_ancestry(monkeypatch,soft,opened,depth,requested,expected):
    monkeypatch.setattr(m,'_descriptor_limits',lambda:(soft,-1));monkeypatch.setattr(m,'_open_descriptor_count',lambda:opened)
    value=m.fd_capacity(depth,requested)
    assert value['effective_pending']==expected and value['descriptors_per_entry']==depth+2
    assert value['open_descriptors']==opened and value['reserved_descriptors']==128
    assert value['queued_entries_reserve_full_ancestry'] is True
    if soft!=-1:assert expected*(depth+2)+opened+128<=soft


@pytest.mark.parametrize('soft,opened,depth',[(128,1,0),(256,8,200),(64,0,0)])
def test_fd_capacity_refuses_if_no_safe_entry_fits(monkeypatch,soft,opened,depth):
    monkeypatch.setattr(m,'_descriptor_limits',lambda:(soft,-1));monkeypatch.setattr(m,'_open_descriptor_count',lambda:opened)
    with pytest.raises(m.ARTIFACTS.StorageArtifactError,match='cannot safely measure one'):m.fd_capacity(depth,128)


def test_kernel_reuses_original_measurement_and_race_retry_bodies_without_publication():
    tree=ast.parse(SOURCE.read_bytes());definitions={n.name for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
    assert not set(m.ORIGINAL_MEASUREMENT_NAMES)&definitions
    assert {'measure_regular_length','measure_regular_eof','_stable_regular_length','_directory_identity'}<=set(m.ORIGINAL_MEASUREMENT_NAMES)
    calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call)]
    assert not any(isinstance(n.func,ast.Attribute) and n.func.attr in {'unlink','rmtree','replace','write_text','write_bytes','stat','is_file','is_dir'} for n in calls)
    assert not any(isinstance(n.func,ast.Name) and n.func.id in {'atomic_json','measure_regular_eof','measure_regular_length'} for n in calls)
    assert m.MAX_WORKERS==64 and m.MAX_PENDING==128 and m.FD_RESERVE==128
