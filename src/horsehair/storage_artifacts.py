"""Exact logical sizes of owned Lustre entries without full getattr.

Full getattr can synchronize an OST size even for an empty historical log.
Measurements request only statx identity/change metadata, then observe actual
EOF through full small-file reads or logarithmic one-byte boundary probes.
They are point-in-time observations with race checks, not claims of immutable
content or guarantees of nonblocking disk I/O.
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
ENTRY_PREFIX_CAP = 65536
MAX_LOGICAL_BYTES = (1 << 63) - 2  # Every one-byte pread stays in signed off_t range.
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


def measure_regular_length(path, max_bytes, expected_uid=None, *, root):
    """Measure any owned regular entry without requesting a size attribute.

    Small files are read through EOF. Larger files use one-byte pread probes
    to find the exact logical EOF, including sparse holes, in logarithmic I/O.
    The study ceiling is a refusal boundary, never an assumed file size.
    Directory, symlink and special entries have explicit uncounted statuses.
    As with an ordinary snapshot, observable identity/change races are refused;
    the caller still supplies its allocation deadline for blocking disk I/O.
    """
    _require_linux()
    path, root, parts = _relative_parts(path, root)
    uid = os.geteuid() if expected_uid is None else expected_uid
    _length_arguments(max_bytes, uid)
    descriptors = []; failed = False
    try:
        parent = _open_directory(os.fspath(root)); descriptors.append(parent)
        for name in parts[:-1]:
            parent = _open_directory(name, parent); descriptors.append(parent)
        name = parts[-1]; before = _metadata(name, parent)
        for kind, predicate in (('symlink', stat.S_ISLNK), ('directory', stat.S_ISDIR)):
            if predicate(before['mode']):
                return {'status': kind, 'path': os.fspath(path), 'identity': before}
        if not stat.S_ISREG(before['mode']):
            return {'status': 'special', 'path': os.fspath(path), 'identity': before}
        _regular_identity(before, uid)
        descriptor = _open_leaf(name, parent); descriptors.append(descriptor)
        if _metadata('', descriptor) != before:
            raise StorageArtifactError('Entry changed between pathname inspection and open')
        prefix = 0; checksum = hashlib.sha256(); prefix_eof = False
        prefix_limit = min(ENTRY_PREFIX_CAP, max_bytes + 1)
        while prefix < prefix_limit:
            chunk = os.read(descriptor, prefix_limit - prefix)
            if not chunk:
                prefix_eof = True
                break
            prefix += len(chunk)
            if prefix > max_bytes:
                raise StorageArtifactError('Entry exceeds the measured study byte ceiling')
            checksum.update(chunk)
        probes = []
        def probe(offset):
            value = os.pread(descriptor, 1, offset)
            if len(value) not in (0, 1):
                raise StorageArtifactError('Native one-byte pread returned an impossible length')
            probes.append({'offset': offset, 'bytes': len(value)})
            return len(value)
        size = prefix if prefix_eof else _boundary_length(prefix, max_bytes, probe)
        if size and probe(size - 1) != 1:
            raise StorageArtifactError('Entry changed at its last-byte boundary')
        if probe(size) != 0:
            raise StorageArtifactError('Entry grew beyond its observed EOF boundary')
        if _metadata('', descriptor) != before or _metadata(name, parent) != before:
            raise StorageArtifactError('Entry identity/change metadata changed during size measurement')
        result = {'schema_version': 1, 'status': 'measured', 'method': 'owned_regular_length',
                  'path': os.fspath(path), 'bytes': size, 'max_bytes': max_bytes,
                  'prefix_cap_bytes': ENTRY_PREFIX_CAP, 'prefix_bytes': prefix,
                  'prefix_sha256': checksum.hexdigest(), 'eof_observed': True, 'identity': before,
                  'size_proof': {'kind': 'prefix_eof' if prefix_eof else 'pread_first_eof', 'probes': probes}}
        if prefix_eof:
            result['content_sha256'] = result['prefix_sha256']
        validate_regular_length_measurement(result, expected_uid=uid, max_bytes=max_bytes, root=root, path=path)
        return result
    except BaseException:
        failed = True
        raise
    finally:
        close_error = None
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError as error:
                if close_error is None:
                    close_error = error
        if close_error is not None and not failed:
            raise close_error


def iter_regular_lengths(root, max_bytes, expected_uid=None):
    """Yield ``(path, measurement)`` for every non-directory entry below root.

    Directory discovery uses only names from scandir on an anchored directory
    FD. No DirEntry type/stat method is called: DT_UNKNOWN must not trigger a
    size-dependent getattr. Classification uses identity-only statx through
    ``measure_regular_length``. Symlink/special rows are yielded explicitly and
    never traversed or counted as regular files. Queued directories retain
    lexical parts and identity, not open descriptors. Names are collected and
    all directory descriptors closed before yielding or visiting another path.
    A queued directory replacement or observable change refuses the census.
    """
    _require_linux()
    raw_root = os.fspath(root)
    flavor = PurePosixPath if raw_root.startswith('/') and '\\' not in raw_root else PurePath
    root = flavor(raw_root)
    _, root, _ = _relative_parts(root / '__entry_anchor__', root)
    uid = os.geteuid() if expected_uid is None else expected_uid
    _length_arguments(max_bytes, uid)
    pending = [((), None)]
    while pending:
        parts, expected = pending.pop()
        descriptors = []; failed = False
        try:
            parent = _open_directory(os.fspath(root)); descriptors.append(parent)
            for name in parts:
                parent = _open_directory(name, parent); descriptors.append(parent)
            observed = _metadata('', parent)
            if not stat.S_ISDIR(observed['mode']):
                raise StorageArtifactError('Entry census anchor is not a directory')
            if expected is not None and observed != expected:
                raise StorageArtifactError('Queued census directory changed before anchored traversal')
            with os.scandir(parent) as entries:
                names = [entry.name for entry in entries]
        except BaseException:
            failed = True
            raise
        finally:
            close_error = None
            for descriptor in reversed(descriptors):
                try:
                    os.close(descriptor)
                except OSError as error:
                    if close_error is None:
                        close_error = error
            if close_error is not None and not failed:
                raise close_error
        for name in names:
            if (not isinstance(name, str) or name in ('', '.', '..')
                    or '/' in name or '\x00' in name
                    or (flavor is not PurePosixPath and '\\' in name)):
                raise StorageArtifactError('Native directory entry has an unsafe lexical name')
            path = root.joinpath(*parts, name)
            measured = measure_regular_length(path, max_bytes, uid, root=root)
            if measured['status'] == 'directory':
                pending.append(((*parts, name), measured['identity']))
            else:
                yield path, measured
