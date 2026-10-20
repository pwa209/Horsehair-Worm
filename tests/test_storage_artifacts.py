"""Native metadata ABI and exact-EOF safety without an OST getattr fallback."""
import ctypes
import errno
import hashlib
import io
import stat
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
