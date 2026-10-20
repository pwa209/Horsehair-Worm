"""Exact EOF sizes for narrowly selected small, owned Lustre artifacts.

Full getattr can synchronize an OST size even for an empty historical log.
These measurements request only statx identity/change metadata, then count
actual read bytes through EOF. They are point-in-time observations with race
checks, not claims of immutable content or guarantees of nonblocking disk I/O.
"""
from __future__ import annotations

import ctypes
import hashlib
import os
import re
import stat
import sys
from functools import lru_cache
from pathlib import PurePath, PurePosixPath


CURL_READ_CAP = 64 * 1024**2
FIXTURE_READ_CAP = 1024**2
STATX_IDENTITY_MASK = 0x19f  # TYPE, MODE, NLINK, UID, GID, INO, CTIME only
AT_SYMLINK_NOFOLLOW = 0x100
AT_EMPTY_PATH = 0x1000
AT_STATX_FORCE_SYNC = 0x2000
_CURL_ARTIFACT = re.compile(
    r'raw/ant_rna/SRR[0-9]+_[12]\.fastq\.gz\.curl-v3/attempts/[0-9]{4}/'
    r'segment-[0-9]{6}\.(?:stdout|stderr)\.private\Z')
_FIXTURE_ARTIFACT = re.compile(r'tests/qualification-[0-9]+/(?:[^/]+/)*[^/]+\.txt\Z')


class StorageArtifactError(ValueError):
    """No trustworthy exact size was measured; never substitute an estimate."""


class _Stamp(ctypes.Structure):
    _fields_ = [('sec', ctypes.c_int64), ('nsec', ctypes.c_uint32), ('reserved', ctypes.c_int32)]


class _Statx(ctypes.Structure):
    # Linux's statx ABI is 256 bytes. Fields after dev_minor are deliberately
    # opaque: newer kernels assign formerly reserved trailing fields.
    _fields_ = [
        ('mask', ctypes.c_uint32), ('blksize', ctypes.c_uint32), ('attributes', ctypes.c_uint64),
        ('nlink', ctypes.c_uint32), ('uid', ctypes.c_uint32), ('gid', ctypes.c_uint32),
        ('mode', ctypes.c_uint16), ('spare', ctypes.c_uint16), ('ino', ctypes.c_uint64),
        ('size', ctypes.c_uint64), ('blocks', ctypes.c_uint64), ('attributes_mask', ctypes.c_uint64),
        ('atime', _Stamp), ('btime', _Stamp), ('ctime', _Stamp), ('mtime', _Stamp),
        ('rdev_major', ctypes.c_uint32), ('rdev_minor', ctypes.c_uint32),
        ('dev_major', ctypes.c_uint32), ('dev_minor', ctypes.c_uint32),
        ('padding', ctypes.c_uint64 * 14)]


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


def artifact_read_cap(path, root):
    """Return an exact-read ceiling only for the declared artifact namespaces."""
    try:
        _, _, parts = _relative_parts(path, root)
    except (StorageArtifactError, TypeError):
        return None
    relative = '/'.join(parts)
    if _CURL_ARTIFACT.fullmatch(relative):
        return CURL_READ_CAP
    if _FIXTURE_ARTIFACT.fullmatch(relative):
        return FIXTURE_READ_CAP
    return None


def _require_linux():
    if not sys.platform.startswith('linux'):
        raise StorageArtifactError('Exact artifact metadata requires native Linux statx')


@lru_cache(maxsize=1)
def _statx_function():
    _require_linux()
    if ctypes.sizeof(_Statx) != 256 or _Statx.ctime.offset != 96 or _Statx.dev_major.offset != 136:
        raise StorageArtifactError('Unsupported Linux statx ABI layout')
    try:
        function = ctypes.CDLL(None, use_errno=True).statx
    except (AttributeError, OSError) as error:
        raise StorageArtifactError('Native libc statx is unavailable; no getattr fallback') from error
    function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_uint, ctypes.POINTER(_Statx)]
    function.restype = ctypes.c_int
    return function


def _metadata(name, dir_fd):
    value = _Statx()
    flags = AT_SYMLINK_NOFOLLOW | AT_STATX_FORCE_SYNC | (AT_EMPTY_PATH if name == '' else 0)
    result = _statx_function()(dir_fd, os.fsencode(name), flags, STATX_IDENTITY_MASK, ctypes.byref(value))
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), os.fspath(name))
    if value.mask & STATX_IDENTITY_MASK != STATX_IDENTITY_MASK:
        raise StorageArtifactError('Native statx lacks requested identity/change metadata')
    if value.ctime.nsec >= 10**9:
        raise StorageArtifactError('Native statx returned an invalid change timestamp')
    # SIZE/BLOCKS/MTIME may be returned incidentally. Never inspect/use them.
    return {'inode': value.ino, 'uid': value.uid, 'mode': value.mode, 'nlink': value.nlink,
            'device_major': value.dev_major, 'device_minor': value.dev_minor,
            'ctime_ns': value.ctime.sec * 10**9 + value.ctime.nsec}


def _open_directory(name, dir_fd=None):
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    return os.open(name, flags, dir_fd=dir_fd)


def _open_leaf(name, dir_fd):
    return os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=dir_fd)


def measure_regular_eof(path, cap, expected_uid=None, *, root):
    """Count a small owned regular file exactly, or explicitly skip a symlink.

    O_NONBLOCK is not a disk-I/O timeout for regular files. The caller retains
    its scheduled allocation/subprocess deadline. No payload is returned/logged.
    All paths are anchored below the opened root through no-follow directories.
    """
    _require_linux()
    path, root, parts = _relative_parts(path, root)
    if type(cap) is not int or not 0 <= cap <= CURL_READ_CAP:
        raise StorageArtifactError('Artifact read ceiling must be an integer within 0..64MiB')
    uid = os.geteuid() if expected_uid is None else expected_uid
    if type(uid) is not int or uid < 0:
        raise StorageArtifactError('Artifact owner must be an explicit native effective UID')
    descriptors = []; failed = False
    try:
        parent = _open_directory(os.fspath(root)); descriptors.append(parent)
        for name in parts[:-1]:
            parent = _open_directory(name, parent); descriptors.append(parent)
        name = parts[-1]
        before = _metadata(name, parent)
        if stat.S_ISLNK(before['mode']):
            return {'status': 'symlink', 'path': os.fspath(path), 'identity': before}
        if not stat.S_ISREG(before['mode']) or before['uid'] != uid or before['nlink'] < 1:
            raise StorageArtifactError('Artifact must be an ordinary file owned by the native user')
        descriptor = _open_leaf(name, parent); descriptors.append(descriptor)
        if _metadata('', descriptor) != before:
            raise StorageArtifactError('Artifact changed between pathname inspection and open')
        total = 0; checksum = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, min(65536, cap + 1 - total))
            if not chunk:
                break  # A short positive read is never assumed to be EOF.
            total += len(chunk)
            if total > cap:
                raise StorageArtifactError('Artifact exceeded its exact-read ceiling; no size admitted')
            checksum.update(chunk)
        if _metadata('', descriptor) != before or _metadata(name, parent) != before:
            raise StorageArtifactError('Artifact identity/change metadata changed during exact EOF measurement')
        return {'status': 'measured', 'method': 'owned_regular_eof', 'path': os.fspath(path),
                'bytes': total, 'read_cap_bytes': cap, 'sha256': checksum.hexdigest(),
                'eof_observed': True, 'identity': before}
    except BaseException:
        failed = True
        raise
    finally:
        # Even an observable race, a read error, or a failed intermediate
        # directory lookup closes every descriptor already acquired.
        close_error = None
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError as error:
                if close_error is None:
                    close_error = error
        if close_error is not None and not failed:
            raise close_error
