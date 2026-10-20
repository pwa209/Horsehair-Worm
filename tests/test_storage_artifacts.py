"""Native metadata ABI and exact-EOF safety without an OST getattr fallback."""
import ctypes
import errno
import hashlib
import io
import stat
from copy import deepcopy
import threading
from types import SimpleNamespace

import pytest

from horsehair import storage_artifacts as artifacts


ROOT = '/scratch/pwa209/horsehair-worm'


@pytest.mark.parametrize('relative,expected', [
    ('raw/ant_rna/SRR22989345_1.fastq.gz.curl-v3/attempts/0002/segment-000009.stderr.private', 64*1024**2),
    ('raw/ant_rna/SRR22989292_2.fastq.gz.curl-v3/attempts/0001/segment-000021.stdout.private', 64*1024**2),
    ('tests/qualification-5452921/generated.txt', 1024**2),
    ('tests/qualification-5452921/test_x/scratch-study/results/generated.txt', 1024**2),
    ('raw/ant_rna/SRR1_1.fastq.gz', None),
    ('raw/ant_rna/SRR1_3.fastq.gz.curl-v3/attempts/0001/segment-000001.stdout.private', None),
    ('raw/ant_rna/SRR1_1.fastq.gz.curl-v3/attempts/001/segment-000001.stdout.private', None),
    ('raw/ant_rna/SRR1_1.fastq.gz.curl-v3/attempts/0001/segment-00001.stdout.private', None),
    ('raw/ant_rna/SRR1_1.fastq.gz.curl-v3/attempts/0001/segment-000001.body.private', None),
    ('raw/ant_rna/SRR1_1.fastq.gz.curl-v3/attempts/0001/segment-000001.headers.private', None),
    ('tests/qualification-x/generated.txt', None),
    ('tests/qualification-1234/generated.txt.extra', None),
    ('tests/qualification-1234/data.fastq.gz', None),
    ('data/rna/brain/counts.txt', None),
    ('tests/qualification-1234/../qualification-1234/generated.txt', None),
])
def test_selector_scopes_only_declared_auxiliary_logs_and_text_fixtures(relative, expected):
    assert artifacts.artifact_read_cap(ROOT+'/'+relative, ROOT) == expected


def test_selector_never_accepts_other_roots_relative_paths_or_prefix_lookalikes():
    assert artifacts.artifact_read_cap(ROOT+'-other/tests/qualification-1/a.txt', ROOT) is None
    assert artifacts.artifact_read_cap('tests/qualification-1/a.txt', ROOT) is None
    assert artifacts.artifact_read_cap(ROOT, ROOT) is None
    assert artifacts.artifact_read_cap(ROOT+'/tests/qualification-1/a.txt\0other', ROOT) is None


def identity(**changes):
    return {'inode':1234,'uid':47,'mode':stat.S_IFREG|0o600,'nlink':1,
            'device_major':910,'device_minor':936806,'ctime_ns':123000000456,**changes}


class KernelFixture:
    """A byte stream and explicit pathname/FD snapshots, not scientific data."""
    def __init__(self, monkeypatch, payload=b'abc'):
        self.stream=io.BytesIO(payload);self.metadata=identity();self.snapshots=[]
        self.opened=[];self.closed=[];self.reads=[];self.remaining=set();self.chunk_limit=None
        self.read_error=None;self.close_error=None;self.directory_error=None
        monkeypatch.setattr(artifacts,'_require_linux',lambda:None)
        monkeypatch.setattr(artifacts,'_open_directory',self.open_directory)
        monkeypatch.setattr(artifacts,'_open_leaf',self.open_leaf)
        monkeypatch.setattr(artifacts,'_metadata',self.inspect)
        # Replace this module's syscall view, never the shared Python os module.
        monkeypatch.setattr(artifacts,'os',SimpleNamespace(read=self.read,close=self.close,
            geteuid=lambda:47,fspath=lambda p:str(p)))

    def allocate(self,kind,name,parent):
        descriptor=10+len(self.opened);self.opened.append((kind,name,parent,descriptor))
        self.remaining.add(descriptor);return descriptor

    def open_directory(self,name,dir_fd=None):
        if self.directory_error and dir_fd is not None:raise self.directory_error
        return self.allocate('directory',name,dir_fd)

    def open_leaf(self,name,dir_fd):return self.allocate('file',name,dir_fd)

    def inspect(self,name,dir_fd):
        self.snapshots.append((name,dir_fd))
        return dict(self.metadata)

    def read(self,descriptor,length):
        self.reads.append((descriptor,length))
        if self.read_error:raise self.read_error
        return self.stream.read(min(length,self.chunk_limit) if self.chunk_limit else length)

    def close(self,descriptor):
        self.closed.append(descriptor);self.remaining.remove(descriptor)
        if self.close_error:raise self.close_error


def measure(kernel,cap=100,path=ROOT+'/tests/qualification-1/subdir/a.txt'):
    return artifacts.measure_regular_eof(path,cap,root=ROOT)


@pytest.mark.parametrize('payload,cap', [(b'',0),(b'abc',3),(b'abcd',10),(b'x'*65536,65536)],
                         ids=['empty','exact','under-cap','full-chunk'])
def test_exact_eof_counts_bytes_and_hash_and_anchors_every_directory(monkeypatch,payload,cap):
    kernel=KernelFixture(monkeypatch,payload)
    result=measure(kernel,cap)
    assert result=={'status':'measured','method':'owned_regular_eof',
        'path':ROOT+'/tests/qualification-1/subdir/a.txt','bytes':len(payload),
        'read_cap_bytes':cap,'sha256':hashlib.sha256(payload).hexdigest(),
        'eof_observed':True,'identity':identity()}
    assert kernel.opened==[('directory',ROOT,None,10),('directory','tests',10,11),
        ('directory','qualification-1',11,12),('directory','subdir',12,13),('file','a.txt',13,14)]
    assert kernel.snapshots==[('a.txt',13),('',14),('',14),('a.txt',13)]
    assert kernel.closed==[14,13,12,11,10] and not kernel.remaining
    assert all(1<=length<=65536 for _,length in kernel.reads)
    if len(payload)==cap:assert kernel.reads[-1][1]==1


def test_positive_short_read_is_not_mistaken_for_eof(monkeypatch):
    kernel=KernelFixture(monkeypatch,b'abcdef');kernel.chunk_limit=2
    result=measure(kernel)
    assert result['bytes']==6 and len(kernel.reads)==4 and not kernel.remaining


def test_cap_exceedance_refuses_size_and_closes_all_descriptors(monkeypatch):
    kernel=KernelFixture(monkeypatch,b'abcdef')
    with pytest.raises(artifacts.StorageArtifactError,match='exceeded'):
        measure(kernel,5)
    assert kernel.stream.tell()==6 and not kernel.remaining
    assert len(kernel.snapshots)==2  # No fabricated measured output on refusal.


@pytest.mark.parametrize('changes', [{'uid':48},{'mode':stat.S_IFDIR|0o700},
    {'mode':stat.S_IFIFO|0o600},{'nlink':0}])
def test_foreign_owner_and_nonregular_artifacts_are_refused_before_payload_open(monkeypatch,changes):
    kernel=KernelFixture(monkeypatch);kernel.metadata=identity(**changes)
    with pytest.raises(artifacts.StorageArtifactError,match='ordinary file owned'):
        measure(kernel)
    assert not kernel.reads and not any(row[0]=='file' for row in kernel.opened) and not kernel.remaining


def test_leaf_symlink_has_explicit_skip_marker_without_opening_or_counting_it(monkeypatch):
    kernel=KernelFixture(monkeypatch);kernel.metadata=identity(mode=stat.S_IFLNK|0o777)
    result=measure(kernel)
    assert result['status']=='symlink' and 'bytes' not in result and 'sha256' not in result
    assert not kernel.reads and not kernel.remaining


@pytest.mark.parametrize('call,changes', [(2,{'inode':2000}),(2,{'mode':stat.S_IFLNK|0o777}),
    (3,{'ctime_ns':123000000457}),(3,{'nlink':2}),(4,{'inode':3000}),
    (4,{'uid':48}),(4,{'device_minor':99})])
def test_fd_swap_file_change_or_path_replacement_is_refused(monkeypatch,call,changes):
    kernel=KernelFixture(monkeypatch);inspect=kernel.inspect
    def altered(name,descriptor):
        value=inspect(name,descriptor)
        if len(kernel.snapshots)==call:value.update(changes)
        return value
    monkeypatch.setattr(artifacts,'_metadata',altered)
    with pytest.raises(artifacts.StorageArtifactError,match='changed'):
        measure(kernel)
    assert not kernel.remaining


def test_growth_during_read_is_observed_by_cap_and_never_undercounted(monkeypatch):
    kernel=KernelFixture(monkeypatch,b'abc');read=kernel.read
    def growing(fd,length):
        if len(kernel.reads)==1:
            position=kernel.stream.tell();kernel.stream.seek(0,2);kernel.stream.write(b'def')
            kernel.stream.seek(position);kernel.metadata['ctime_ns']+=1
        return read(fd,min(length,3))
    monkeypatch.setattr(artifacts.os,'read',growing)
    with pytest.raises(artifacts.StorageArtifactError,match='exceeded'):
        measure(kernel,5)
    assert not kernel.remaining


@pytest.mark.parametrize('failure', [OSError(errno.EIO,'fixture I/O failure'),BlockingIOError(errno.EAGAIN,'fixture read unavailable')])
def test_read_failures_never_become_eof_or_retry_and_always_close(monkeypatch,failure):
    kernel=KernelFixture(monkeypatch);kernel.read_error=failure
    with pytest.raises(OSError) as caught:measure(kernel)
    assert caught.value is failure and len(kernel.reads)==1 and not kernel.remaining


def test_failed_ancestor_open_closes_the_root_descriptor(monkeypatch):
    kernel=KernelFixture(monkeypatch);kernel.directory_error=OSError(errno.ELOOP,'fixture symlinked ancestor')
    with pytest.raises(OSError,match='symlinked ancestor'):measure(kernel)
    assert kernel.closed==[10] and not kernel.remaining and not kernel.reads


def test_leaf_symlink_swap_rejected_by_native_open_still_closes_all_ancestors(monkeypatch):
    kernel=KernelFixture(monkeypatch)
    def refuse_leaf(name,descriptor):
        raise OSError(errno.ELOOP,'fixture swapped leaf symlink')
    monkeypatch.setattr(artifacts,'_open_leaf',refuse_leaf)
    with pytest.raises(OSError,match='swapped leaf symlink'):measure(kernel)
    assert kernel.closed==[13,12,11,10] and not kernel.remaining and not kernel.reads


def test_metadata_refusal_during_fd_check_closes_every_open_descriptor(monkeypatch):
    kernel=KernelFixture(monkeypatch);inspect=kernel.inspect
    def unsupported(name,descriptor):
        if not name:raise artifacts.StorageArtifactError('fixture lacks requested identity metadata')
        return inspect(name,descriptor)
    monkeypatch.setattr(artifacts,'_metadata',unsupported)
    with pytest.raises(artifacts.StorageArtifactError,match='lacks requested'):measure(kernel)
    assert kernel.closed==[14,13,12,11,10] and not kernel.remaining and not kernel.reads


def test_close_failure_still_attempts_every_close_and_refuses_measurement(monkeypatch):
    kernel=KernelFixture(monkeypatch);kernel.close_error=OSError(errno.EIO,'fixture close failure')
    with pytest.raises(OSError,match='close failure'):measure(kernel)
    assert kernel.closed==[14,13,12,11,10] and not kernel.remaining


@pytest.mark.parametrize('path,cap', [(ROOT+'/../other/a.txt',1),('/scratch/other/a.txt',1),
    (ROOT+'/a.txt',True),(ROOT+'/a.txt',-1),(ROOT+'/a.txt',64*1024**2+1)])
def test_invalid_paths_or_caps_never_open_a_descriptor(monkeypatch,path,cap):
    kernel=KernelFixture(monkeypatch)
    with pytest.raises(artifacts.StorageArtifactError):measure(kernel,cap,path)
    assert not kernel.opened


@pytest.mark.parametrize('uid',[-1,True,'47'])
def test_invalid_explicit_uid_never_opens_a_descriptor(monkeypatch,uid):
    kernel=KernelFixture(monkeypatch)
    with pytest.raises(artifacts.StorageArtifactError,match='effective UID'):
        artifacts.measure_regular_eof(ROOT+'/tests/qualification-1/a.txt',1,uid,root=ROOT)
    assert not kernel.opened


def test_native_statx_layout_flags_and_unrequested_size_are_not_used(monkeypatch):
    calls=[]
    def native(fd,path,flags,mask,pointer):
        calls.append((fd,path,flags,mask));value=ctypes.cast(pointer,ctypes.POINTER(artifacts._Statx)).contents
        value.mask=mask|0x600;value.ino=1234;value.uid=47;value.mode=stat.S_IFREG|0o600
        value.nlink=1;value.dev_major=910;value.dev_minor=936806;value.ctime.sec=123;value.ctime.nsec=456
        value.size=2**63;value.blocks=2**63;return 0
    monkeypatch.setattr(artifacts,'_statx_function',lambda:native)
    assert ctypes.sizeof(artifacts._Statx)==256
    assert artifacts._Statx.ctime.offset==96 and artifacts._Statx.dev_major.offset==136
    assert artifacts._metadata('a.txt',17)==identity()
    assert artifacts._metadata('',19)==identity()
    assert calls==[(17,b'a.txt',0x2100,0x19f),(19,b'',0x3100,0x19f)]
    assert not artifacts.STATX_IDENTITY_MASK & (0x200|0x400|0x40)


def test_missing_native_statx_fields_are_not_guessed(monkeypatch):
    def native(fd,path,flags,mask,pointer):
        ctypes.cast(pointer,ctypes.POINTER(artifacts._Statx)).contents.mask=mask & ~0x80
        return 0
    monkeypatch.setattr(artifacts,'_statx_function',lambda:native)
    with pytest.raises(artifacts.StorageArtifactError,match='lacks requested'):
        artifacts._metadata('a.txt',17)


def test_statx_syscall_failure_has_no_full_getattr_fallback(monkeypatch):
    calls=[]
    def native(*args):calls.append(args);ctypes.set_errno(errno.ENOSYS);return -1
    monkeypatch.setattr(artifacts,'_statx_function',lambda:native)
    with pytest.raises(OSError) as caught:artifacts._metadata('a.txt',17)
    assert caught.value.errno==errno.ENOSYS and len(calls)==1


def test_native_open_flags_protect_each_ancestor_and_leaf(monkeypatch):
    calls=[]
    def open_file(name,flags,**kwargs):calls.append((name,flags,kwargs));return 17
    monkeypatch.setattr(artifacts,'os',SimpleNamespace(O_RDONLY=0,O_DIRECTORY=0x10000,
        O_NOFOLLOW=0x20000,O_NONBLOCK=0x800,O_CLOEXEC=0x80000,open=open_file))
    assert artifacts._open_directory('/scratch/study')==17
    assert artifacts._open_directory('sub',17)==17
    assert artifacts._open_leaf('a.txt',17)==17
    assert calls==[('/scratch/study',0xb0000,{'dir_fd':None}),('sub',0xb0000,{'dir_fd':17}),
                   ('a.txt',0xa0800,{'dir_fd':17})]


def test_unsupported_platform_refuses_before_any_io(monkeypatch):
    monkeypatch.setattr(artifacts.sys,'platform','unsupported-platform')
    with pytest.raises(artifacts.StorageArtifactError,match='native Linux'):
        artifacts.measure_regular_eof(ROOT+'/a.txt',1,47,root=ROOT)


class LengthKernelFixture(KernelFixture):
    def __init__(self, monkeypatch, size=3, *, sparse=False):
        super().__init__(monkeypatch, (b'\0' if sparse else b'x')*min(size, artifacts.ENTRY_PREFIX_CAP))
        self.logical_size=size; self.preads=[]; self.pread_error=None; self.sparse=sparse
        monkeypatch.setattr(artifacts.os,'pread',self.pread,raising=False)

    def pread(self,descriptor,length,offset):
        self.preads.append((descriptor,length,offset))
        if self.pread_error:raise self.pread_error
        assert length==1
        return (b'\0' if self.sparse else b'x') if offset<self.logical_size else b''


def measure_length(kernel,cap=6_000_000_000_000):
    return artifacts.measure_regular_length(ROOT+'/any-owned-entry.dat',cap,root=ROOT)


@pytest.mark.parametrize('size,cap',[(0,0),(1,1),(3,100),(65535,65535),
    (65536,65536),(65537,100000),(1000000,1000000),(6_000_000_000_000,6_000_000_000_000)])
def test_general_entry_size_is_exact_at_empty_prefix_and_large_boundaries(monkeypatch,size,cap):
    kernel=LengthKernelFixture(monkeypatch,size)
    result=measure_length(kernel,cap)
    assert result['bytes']==size and not kernel.remaining
    assert result['method']=='owned_regular_length' and result['eof_observed'] is True
    assert result['prefix_bytes']==min(size,65536)
    assert len(kernel.preads)<100 and all(row[1]==1 for row in kernel.preads)
    assert sum(row[1] for row in kernel.reads)<=65537
    assert artifacts.validate_regular_length_measurement(result,expected_uid=47,max_bytes=cap,root=ROOT)==size
    if size<65536:
        assert result['size_proof']['kind']=='prefix_eof'
        assert result['content_sha256']==hashlib.sha256(b'x'*size).hexdigest()
    else:
        assert result['size_proof']['kind']=='pread_first_eof' and 'content_sha256' not in result
    assert result['size_proof']['probes'][-1]=={'offset':size,'bytes':0}
    if size:assert result['size_proof']['probes'][-2]=={'offset':size-1,'bytes':1}


def test_sparse_holes_count_as_logical_bytes_without_bulk_reading(monkeypatch):
    kernel=LengthKernelFixture(monkeypatch,2**40,sparse=True)
    result=measure_length(kernel)
    assert result['bytes']==2**40
    assert result['prefix_sha256']==hashlib.sha256(b'\0'*65536).hexdigest()
    assert len(kernel.preads)<100 and not kernel.remaining


def test_general_prefix_short_positive_reads_continue_until_explicit_eof(monkeypatch):
    kernel=LengthKernelFixture(monkeypatch,9);kernel.chunk_limit=2
    result=measure_length(kernel)
    assert result['bytes']==9 and len(kernel.reads)==6 and not kernel.remaining


@pytest.mark.parametrize('size,cap',[(1,0),(1000,999),(65537,65536),(6_000_000_000_001,6_000_000_000_000)])
def test_positive_byte_at_study_ceiling_refuses_instead_of_assuming_ceiling(monkeypatch,size,cap):
    kernel=LengthKernelFixture(monkeypatch,size)
    with pytest.raises(artifacts.StorageArtifactError,match='exceeds'):
        measure_length(kernel,cap)
    assert not kernel.remaining and len(kernel.preads)<100


@pytest.mark.parametrize('mode,status',[(stat.S_IFLNK,'symlink'),(stat.S_IFDIR,'directory'),
    (stat.S_IFIFO,'special'),(stat.S_IFSOCK,'special')])
def test_general_nonregular_entries_are_explicitly_uncounted_before_open(monkeypatch,mode,status):
    kernel=LengthKernelFixture(monkeypatch);kernel.metadata=identity(mode=mode|0o600)
    result=measure_length(kernel)
    assert result['status']==status and 'bytes' not in result and 'method' not in result
    assert not kernel.reads and not kernel.preads and not kernel.remaining
    assert not any(row[0]=='file' for row in kernel.opened)


def test_general_foreign_regular_entry_is_refused_before_payload_open(monkeypatch):
    kernel=LengthKernelFixture(monkeypatch);kernel.metadata=identity(uid=48)
    with pytest.raises(artifacts.StorageArtifactError,match='owned regular-file'):
        measure_length(kernel)
    assert not kernel.reads and not kernel.preads and not kernel.remaining


@pytest.mark.parametrize('failure',[InterruptedError(errno.EINTR,'fixture signal handler interrupted'),
    OSError(errno.EIO,'fixture pread I/O error'),BlockingIOError(errno.EAGAIN,'fixture unavailable read')])
def test_general_pread_errors_are_not_eof_and_close_every_descriptor(monkeypatch,failure):
    kernel=LengthKernelFixture(monkeypatch,65537);kernel.pread_error=failure
    with pytest.raises(OSError) as caught:measure_length(kernel)
    assert caught.value is failure and len(kernel.preads)==1 and not kernel.remaining


def test_general_prefix_read_error_and_close_error_never_admit_a_size(monkeypatch):
    kernel=LengthKernelFixture(monkeypatch,3)
    kernel.read_error=InterruptedError(errno.EINTR,'fixture interrupted prefix')
    kernel.close_error=OSError(errno.EIO,'fixture close error')
    with pytest.raises(InterruptedError,match='interrupted prefix'):measure_length(kernel)
    assert not kernel.preads and not kernel.remaining and len(kernel.closed)==2


@pytest.mark.parametrize('cap',[-1,True,1.0,2**63])
def test_general_invalid_study_ceiling_never_opens_descriptors(monkeypatch,cap):
    kernel=LengthKernelFixture(monkeypatch)
    with pytest.raises(artifacts.StorageArtifactError,match='ceiling'):measure_length(kernel,cap)
    assert not kernel.opened


@pytest.mark.parametrize('new_size,error',[(2,'last-byte'),(4,'grew')])
def test_adjacent_boundary_recheck_rejects_shrink_and_growth_even_without_ctime_change(monkeypatch,new_size,error):
    kernel=LengthKernelFixture(monkeypatch,3);pread=kernel.pread
    def changed(fd,length,offset):
        if (new_size==2 and offset==2) or (new_size==4 and offset==3):kernel.logical_size=new_size
        return pread(fd,length,offset)
    monkeypatch.setattr(artifacts.os,'pread',changed)
    with pytest.raises(artifacts.StorageArtifactError,match=error):measure_length(kernel)
    assert not kernel.remaining


def test_general_identity_change_after_successful_boundary_is_refused(monkeypatch):
    kernel=LengthKernelFixture(monkeypatch,3);inspect=kernel.inspect
    def changed(name,fd):
        value=inspect(name,fd)
        if len(kernel.snapshots)==3:value['ctime_ns']+=1
        return value
    monkeypatch.setattr(artifacts,'_metadata',changed)
    with pytest.raises(artifacts.StorageArtifactError,match='identity/change'):measure_length(kernel)
    assert not kernel.remaining


@pytest.mark.parametrize('change',[
    lambda item:item.update(bytes=2),
    lambda item:item.update(max_bytes=True),
    lambda item:item.update(prefix_bytes=65536),
    lambda item:item.update(prefix_sha256='bad'),
    lambda item:item.update(eof_observed=False),
    lambda item:item.update(path='/scratch/other/file'),
    lambda item:item['identity'].update(uid=48),
    lambda item:item['size_proof']['probes'][0].update(offset=0),
    lambda item:item['size_proof']['probes'][-1].update(bytes=1),
    lambda item:item['size_proof']['probes'].append({'offset':3,'bytes':0}),
],ids=['wrong-size','boolean-cap','false-prefix','bad-prefix-hash','missing-eof','other-root',
       'foreign-owner','wrong-probe-offset','positive-eof','trailing-probe'])
def test_pure_validator_rejects_tampered_small_receipts_without_leaf_io(monkeypatch,change):
    kernel=LengthKernelFixture(monkeypatch,3);item=deepcopy(measure_length(kernel));change(item)
    with pytest.raises(artifacts.StorageArtifactError):
        artifacts.validate_regular_length_measurement(item,expected_uid=47,max_bytes=6_000_000_000_000,root=ROOT)
    assert not kernel.remaining


def test_pure_validator_rejects_false_whole_content_hash_and_binary_transcript(monkeypatch):
    kernel=LengthKernelFixture(monkeypatch,65537);item=measure_length(kernel)
    wrong_hash=deepcopy(item);wrong_hash['content_sha256']=item['prefix_sha256']
    with pytest.raises(artifacts.StorageArtifactError,match='whole-content'):
        artifacts.validate_regular_length_measurement(wrong_hash,expected_uid=47,max_bytes=6_000_000_000_000,root=ROOT)
    wrong_probe=deepcopy(item);wrong_probe['size_proof']['probes'][2]['offset']+=1
    with pytest.raises(artifacts.StorageArtifactError,match='algorithm'):
        artifacts.validate_regular_length_measurement(wrong_probe,expected_uid=47,max_bytes=6_000_000_000_000,root=ROOT)
    with pytest.raises(artifacts.StorageArtifactError,match='another path'):
        artifacts.validate_regular_length_measurement(item,expected_uid=47,max_bytes=6_000_000_000_000,
            root=ROOT,path=ROOT+'/different-file')


class TraversalFixture:
    class Entry:
        def __init__(self,name):self.name=name
        def is_dir(self,*args,**kwargs):raise AssertionError('DirEntry.is_dir must never be used')
        def is_file(self,*args,**kwargs):raise AssertionError('DirEntry.is_file must never be used')
        def is_symlink(self,*args,**kwargs):raise AssertionError('DirEntry.is_symlink must never be used')
        def stat(self,*args,**kwargs):raise AssertionError('DirEntry.stat must never be used')

    class Scan:
        def __init__(self,owner,path):self.owner=owner;self.path=path;self.closed=False
        def __enter__(self):return self
        def __exit__(self,*args):self.close()
        def __iter__(self):
            if self.owner.scan_error:raise self.owner.scan_error
            return iter([TraversalFixture.Entry(name) for name in self.owner.tree[self.path]])
        def close(self):self.closed=True

    def __init__(self,monkeypatch):
        self.tree={ROOT:['data','link-dir','pipe','root.txt'],ROOT+'/data':['nested','a.fa'],
                   ROOT+'/data/nested':['b.fa']}
        self.identities={path:identity(inode=100+index,mode=stat.S_IFDIR|0o700)
                         for index,path in enumerate(self.tree)}
        self.opened=[];self.closed=[];self.remaining={};self.scans=[];self.measured=[]
        self.scan_error=None;self.swap_directory=None;self.open_error=None;self.close_error=None
        monkeypatch.setattr(artifacts,'_require_linux',lambda:None)
        monkeypatch.setattr(artifacts,'_open_directory',self.open_directory)
        monkeypatch.setattr(artifacts,'_metadata',self.metadata)
        monkeypatch.setattr(artifacts,'measure_regular_length',self.measure)
        monkeypatch.setattr(artifacts,'os',SimpleNamespace(fspath=lambda p:str(p),geteuid=lambda:47,
            scandir=self.scandir,close=self.close))

    def open_directory(self,name,dir_fd=None):
        path=name if dir_fd is None else self.remaining[dir_fd]+'/'+name
        if self.open_error and path!=ROOT:raise self.open_error
        assert path in self.tree
        fd=1000+len(self.opened);self.opened.append((path,dir_fd,fd));self.remaining[fd]=path
        return fd

    def metadata(self,name,fd):
        assert name=='';path=self.remaining[fd];value=dict(self.identities[path])
        if path==self.swap_directory:value['inode']+=1
        return value

    def scandir(self,fd):
        assert type(fd) is int and fd in self.remaining
        scan=self.Scan(self,self.remaining[fd]);self.scans.append(scan);return scan

    def close(self,fd):
        self.closed.append(fd);del self.remaining[fd]
        if self.close_error:raise self.close_error

    def measure(self,path,max_bytes,expected_uid=None,*,root):
        assert not self.remaining and all(scan.closed for scan in self.scans)
        assert str(root)==ROOT and expected_uid==47
        path=str(path);self.measured.append(path)
        if path in self.tree:return {'status':'directory','path':path,'identity':self.identities[path]}
        if path==ROOT+'/link-dir':return {'status':'symlink','path':path,'identity':identity(mode=stat.S_IFLNK|0o777)}
        if path==ROOT+'/pipe':return {'status':'special','path':path,'identity':identity(mode=stat.S_IFIFO|0o600)}
        return {'status':'measured','path':path,'bytes':123}


def test_anchored_census_uses_only_entry_names_and_never_follows_symlink_directories(monkeypatch):
    fixture=TraversalFixture(monkeypatch)
    results=list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000))
    by_path={str(path):row for path,row in results}
    assert set(by_path)=={ROOT+'/root.txt',ROOT+'/link-dir',ROOT+'/pipe',
                          ROOT+'/data/a.fa',ROOT+'/data/nested/b.fa'}
    assert by_path[ROOT+'/link-dir']['status']=='symlink'
    assert by_path[ROOT+'/pipe']['status']=='special'
    assert [scan.path for scan in fixture.scans]==[ROOT,ROOT+'/data',ROOT+'/data/nested']
    assert not fixture.remaining and all(scan.closed for scan in fixture.scans)
    assert all('link-dir' not in path for path,_,_ in fixture.opened)


def test_anchored_census_rejects_directory_replacement_before_queued_traversal(monkeypatch):
    fixture=TraversalFixture(monkeypatch);fixture.swap_directory=ROOT+'/data'
    with pytest.raises(artifacts.StorageArtifactError,match='Queued census directory changed'):
        list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000))
    assert not fixture.remaining and all(scan.closed for scan in fixture.scans)
    assert [scan.path for scan in fixture.scans]==[ROOT]


def test_anchored_census_closes_scandir_and_anchor_when_directory_iteration_fails(monkeypatch):
    fixture=TraversalFixture(monkeypatch);fixture.scan_error=OSError(errno.EIO,'fixture directory iteration error')
    with pytest.raises(OSError,match='directory iteration error'):
        list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000))
    assert not fixture.remaining and len(fixture.scans)==1 and fixture.scans[0].closed


def test_anchored_census_closes_root_when_queued_nofollow_open_refuses_symlink_swap(monkeypatch):
    fixture=TraversalFixture(monkeypatch);fixture.open_error=OSError(errno.ELOOP,'fixture queued directory symlink')
    with pytest.raises(OSError,match='queued directory symlink'):
        list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000))
    assert not fixture.remaining and all(scan.closed for scan in fixture.scans)


def test_anchored_census_retains_no_descriptors_across_yield_or_generator_close(monkeypatch):
    fixture=TraversalFixture(monkeypatch);iterator=artifacts.iter_regular_lengths(ROOT,6_000_000_000_000)
    next(iterator)
    assert not fixture.remaining and all(scan.closed for scan in fixture.scans)
    iterator.close()
    assert not fixture.remaining and all(scan.closed for scan in fixture.scans)


def test_anchored_census_does_not_return_measurements_after_anchor_close_failure(monkeypatch):
    fixture=TraversalFixture(monkeypatch);fixture.close_error=OSError(errno.EIO,'fixture anchor close error')
    with pytest.raises(OSError,match='anchor close error'):
        list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000))
    assert not fixture.remaining and not fixture.measured and fixture.scans[0].closed


@pytest.mark.parametrize('name',['..','bad/name','bad\0name'])
def test_anchored_census_rejects_unsafe_entry_names_without_measuring_or_leaking(monkeypatch,name):
    fixture=TraversalFixture(monkeypatch);fixture.tree[ROOT]=[name]
    with pytest.raises(artifacts.StorageArtifactError,match='unsafe lexical name'):
        list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000))
    assert not fixture.remaining and not fixture.measured and fixture.scans[0].closed


def test_parallel_census_has_complete_ordered_coverage_without_direntry_type_queries(monkeypatch):
    fixture=TraversalFixture(monkeypatch)
    rows=list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000,workers=4))
    assert [str(path) for path,_ in rows]==[ROOT+'/link-dir',ROOT+'/pipe',ROOT+'/root.txt',
                                         ROOT+'/data/a.fa',ROOT+'/data/nested/b.fa']
    assert len(fixture.measured)==7 and not fixture.remaining
    assert all(scan.closed for scan in fixture.scans)


def test_parallel_measurements_overlap_but_finish_and_close_before_yield(monkeypatch):
    fixture=TraversalFixture(monkeypatch);fixture.tree={ROOT:['a.txt','b.txt','c.txt']}
    original=fixture.measure;barrier=threading.Barrier(3);lock=threading.Lock()
    active=0;peak=0
    def simultaneous(*args,**kwargs):
        nonlocal active,peak
        with lock:active+=1;peak=max(peak,active)
        try:
            barrier.wait(timeout=3)
            return original(*args,**kwargs)
        finally:
            with lock:active-=1
    monkeypatch.setattr(artifacts,'measure_regular_length',simultaneous)
    iterator=artifacts.iter_regular_lengths(ROOT,6_000_000_000_000,workers=3)
    next(iterator)
    assert peak==3 and active==0 and not fixture.remaining
    iterator.close()
    assert all(scan.closed for scan in fixture.scans)


def test_parallel_worker_failure_joins_running_workers_and_closes_directory_handles(monkeypatch):
    fixture=TraversalFixture(monkeypatch);fixture.tree={ROOT:['bad.txt','b.txt','c.txt']}
    original=fixture.measure;barrier=threading.Barrier(3);lock=threading.Lock();active=0
    def failing(path,*args,**kwargs):
        nonlocal active
        with lock:active+=1
        try:
            barrier.wait(timeout=3)
            if path.name=='bad.txt':raise OSError(errno.EIO,'fixture parallel read error')
            return original(path,*args,**kwargs)
        finally:
            with lock:active-=1
    monkeypatch.setattr(artifacts,'measure_regular_length',failing)
    with pytest.raises(OSError,match='parallel read error'):
        list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000,workers=3))
    assert active==0 and not fixture.remaining and all(scan.closed for scan in fixture.scans)


def test_parallel_census_bounds_future_window_in_a_large_directory(monkeypatch):
    fixture=TraversalFixture(monkeypatch);fixture.tree={ROOT:[f'{n}.txt' for n in range(100)]}
    counters={'outstanding':0,'peak':0,'shutdown':False}
    class Future:
        def __init__(self,function,args,kwargs):self.function=function;self.args=args;self.kwargs=kwargs
        def result(self):
            try:return self.function(*self.args,**self.kwargs)
            finally:counters['outstanding']-=1
        def cancel(self):counters['outstanding']-=1
    class Executor:
        def __init__(self,*,max_workers,thread_name_prefix):assert max_workers==4
        def submit(self,function,*args,**kwargs):
            counters['outstanding']+=1;counters['peak']=max(counters['peak'],counters['outstanding'])
            return Future(function,args,kwargs)
        def shutdown(self,*,wait,cancel_futures):
            assert wait and cancel_futures;counters['shutdown']=True
    monkeypatch.setattr(artifacts,'ThreadPoolExecutor',Executor)
    rows=list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000,workers=4))
    assert len(rows)==100 and counters=={'outstanding':0,'peak':8,'shutdown':True}
    assert not fixture.remaining and all(scan.closed for scan in fixture.scans)


@pytest.mark.parametrize('workers',[0,17,True,1.0])
def test_invalid_parallel_worker_count_is_refused_before_opening_descriptors(monkeypatch,workers):
    fixture=TraversalFixture(monkeypatch)
    with pytest.raises(artifacts.StorageArtifactError,match='workers'):
        list(artifacts.iter_regular_lengths(ROOT,6_000_000_000_000,workers=workers))
    assert not fixture.opened
