"""One source-bound public split-ZIP DB acquisition; no SQLite or scientific analysis.

This gateway helper never retries or resumes. A fresh personal maintenance case is
required. Every HTTP range intent, response, partial body and failure is retained.
Only the frozen ETHOSCOPE_155 canary is supported; ZIP descriptors/encryption,
ZIP64 and other compression/flag variants are deliberately refused.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import struct
import time
import zlib

import requests

ROOT = Path('/scratch/pwa209/horsehair-worm')
RECORD_SHA = '2514b6e65c2892c69db797263ee545d546a76beee4ac931fc3b1257f45992cce'
CHECKSUM_SHA = '39d6f17957c394d95b3746df1aa4ff75ba15ae4722ffede293ed0c6de9c729c6'
INDEX_SHA = 'ffc58aa960a5fdaec167107c4fece8fd7789fb5daad79675c9aa85980dee46b6'
TARGET = {
    'name': 'plos_path_results/15517bc8ec75403fb282ef8e166d150a/ETHOSCOPE_155/'
            '2022-04-22_11-21-12/2022-04-22_11-21-12_15517bc8ec75403fb282ef8e166d150a.db',
    'disk_start': 51, 'local_header_offset': 1118302833,
    'compressed_bytes': 6594774, 'uncompressed_bytes': 13594624,
    'crc32': '7a767f04', 'compression': 8, 'flags': 0,
    'raw_provider_md5': 'e261acc31375cb122164c92ed957206e',
}
PART_KEY = 'db_files.z52'
PART_BYTES = 2097152000
PART_MD5 = 'e791a15456530b89c11b88bd9e9f96e5'
PART_URL = 'https://zenodo.org/api/records/6567039/files/db_files.z52/content'
MAX_RANGE = 1_048_576
MAX_SECONDS = 900


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(65536), b''):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path, value: dict) -> None:
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    with path.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    _sync_directory(path.parent)


def _sync_directory(path: Path) -> None:
    if os.name == 'nt':
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _confined(path: Path, *, exists: bool) -> Path:
    path = Path(path)
    if not path.is_absolute():
        raise ValueError('absolute personal path required')
    root = ROOT.resolve(strict=True)
    resolved = path.resolve(strict=exists)
    if resolved == root or root not in resolved.parents:
        raise ValueError('path outside personal scratch')
    # The system-level scratch alias is allowed at ROOT; subordinate redirects
    # and any existing symlink component below it are refused.
    lexical_root = ROOT.absolute()
    if path != lexical_root and lexical_root not in path.parents:
        raise ValueError('path must use the declared personal scratch root')
    relative = path.relative_to(lexical_root)
    current = lexical_root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError('subordinate symlink refused')
    if exists:
        st = path.stat()
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
            raise ValueError('ordinary single-link metadata file required')
    return resolved


def _inputs(record: Path, checksum: Path, index: Path) -> dict:
    pins = [(record, RECORD_SHA), (checksum, CHECKSUM_SHA), (index, INDEX_SHA)]
    for path, expected in pins:
        _confined(path, exists=True)
        if path.stat().st_size > 2_000_000 or _sha(path) != expected:
            raise ValueError('metadata source hash/size mismatch')
    provider = json.loads(record.read_text())
    if str(provider['id']) != '6567039' or provider['doi'] != '10.5281/zenodo.6567039':
        raise ValueError('wrong official Zenodo record')
    part, = [f for f in provider['files'] if f['key'] == PART_KEY]
    if (part['size'], part['checksum'], part['links']['self']) != (
            PART_BYTES, 'md5:' + PART_MD5, PART_URL):
        raise ValueError('official split-part binding mismatch')
    table = json.loads(index.read_text())
    entries = table['entries']
    selected, = [e for e in entries if e['name'] == TARGET['name']]
    for key, expected in TARGET.items():
        if key != 'raw_provider_md5' and selected.get(key) != expected:
            raise ValueError('frozen central-directory member mismatch: ' + key)
    if table['central_directory_disk'] != 58 or table['eocd_disk'] != 58:
        raise ValueError('unsupported split-directory location')
    members = {}
    for line in checksum.read_text().splitlines():
        digest, name = line.split('  ', 1)
        name = name.removeprefix('./')
        if name in members:
            raise ValueError('duplicate provider member checksum')
        members[name] = digest
    relative = TARGET['name'].split('/', 1)[1]
    if members.get(relative) != TARGET['raw_provider_md5']:
        raise ValueError('raw provider member MD5 mismatch')
    next_offsets = [e['local_header_offset'] for e in entries
                    if e['disk_start'] == TARGET['disk_start']
                    and e['local_header_offset'] > TARGET['local_header_offset']]
    if not next_offsets:
        raise ValueError('canary successor header missing')
    return {'selected': selected, 'next_local_header_offset': min(next_offsets),
            'pins': [{'path': str(p), 'sha256': digest} for p, digest in pins]}


class RangeReader:
    def __init__(self, case: Path, session):
        self.folder = case / 'ranges'
        self.folder.mkdir()
        self.session = session
        self.sequence = 0

    def read(self, start: int, count: int, role: str) -> Path:
        if not isinstance(start, int) or not isinstance(count, int) or not (
                0 <= start < PART_BYTES and 0 < count <= MAX_RANGE
                and start + count <= PART_BYTES):
            raise ValueError('exact single-part range bounds refused')
        self.sequence += 1
        stem = f'{self.sequence:04d}-{role}'
        target = self.folder / (stem + '.bin')
        intent = {'url': PART_URL, 'start': start, 'end': start + count - 1,
                  'bytes': count, 'role': role, 'no_retry': True}
        _json(self.folder / (stem + '-intent.json'), intent)
        response_info = {'status': 'not_opened', 'bytes_preserved': 0}
        try:
            with self.session.get(PART_URL, headers={
                    'Range': f'bytes={start}-{start + count - 1}',
                    'Accept-Encoding': 'identity',
                    'User-Agent': 'Horsehair-Worm bounded public canary'},
                    allow_redirects=False, stream=True, timeout=(15, 90)) as response:
                response_info = {'status': response.status_code, 'bytes_preserved': 0,
                    'headers': {key: response.headers.get(key) for key in [
                        'Content-Range', 'Content-Length', 'Content-Encoding',
                        'ETag', 'Last-Modified', 'Location']}}
                if response.status_code != 206:
                    raise ValueError('exact HTTP206 required; non-range body not read')
                if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                    raise ValueError('encoded range response refused')
                if response.headers.get('Content-Range') != (
                        f'bytes {start}-{start + count - 1}/{PART_BYTES}'):
                    raise ValueError('exact Content-Range/total size mismatch')
                if response.headers.get('Content-Length') != str(count):
                    raise ValueError('exact Content-Length mismatch')
                with target.open('xb') as handle:
                    for chunk in response.iter_content(65536):
                        if not chunk:
                            continue
                        # Keep up to the declared bounded response plus one byte
                        # as evidence; never consume an unbounded provider body.
                        remaining = count + 1 - response_info['bytes_preserved']
                        piece = chunk[:remaining]
                        handle.write(piece)
                        response_info['bytes_preserved'] += len(piece)
                        if len(chunk) > remaining or response_info['bytes_preserved'] > count:
                            handle.flush()
                            os.fsync(handle.fileno())
                            raise ValueError('range body exceeded exact declared bound')
                    handle.flush()
                    os.fsync(handle.fileno())
                if response_info['bytes_preserved'] != count:
                    raise ValueError('truncated range body')
                response_info['sha256'] = _sha(target)
                response_info['verified'] = True
        except BaseException as error:
            response_info.update(verified=False, error_type=type(error).__name__, error=str(error))
            raise
        finally:
            _json(self.folder / (stem + '-response.json'), response_info)
        return target


def _header(reader: RangeReader, proof: dict) -> tuple[int, dict]:
    offset = TARGET['local_header_offset']
    raw = reader.read(offset, 30, 'local-header').read_bytes()
    (magic, version, flags, method, mtime, mdate, crc, compressed, uncompressed,
     name_n, extra_n) = struct.unpack('<4s5H3I2H', raw)
    if magic != b'PK\x03\x04' or not 10 <= version <= 20:
        raise ValueError('invalid or unsupported local ZIP header')
    if flags != 0 or flags != TARGET['flags'] or method != 8:
        raise ValueError('encryption/descriptor/flags/compression variant refused')
    if (f'{crc:08x}', compressed, uncompressed) != (
            TARGET['crc32'], TARGET['compressed_bytes'], TARGET['uncompressed_bytes']):
        raise ValueError('descriptor-free local CRC/size disagreement')
    if not 1 <= name_n <= 1024 or extra_n > 4096:
        raise ValueError('local ZIP name/extra bounds refused')
    name_extra = reader.read(offset+30, name_n+extra_n, 'local-name-extra').read_bytes()
    name, extra = name_extra[:name_n], name_extra[name_n:]
    if name != TARGET['name'].encode('ascii'):
        raise ValueError('local member filename disagreement')
    pos = 0
    while pos < len(extra):
        if pos+4 > len(extra):
            raise ValueError('truncated ZIP extra header')
        kind, length = struct.unpack_from('<HH', extra, pos)
        pos += 4
        if pos+length > len(extra) or kind == 1:
            raise ValueError('truncated or ZIP64 extra variant refused')
        pos += length
    data_start = offset + 30 + name_n + extra_n
    if data_start + compressed > PART_BYTES:
        raise ValueError('frozen canary unexpectedly spans split parts')
    # flags=0 means no data descriptor is permitted; its exact data end must be
    # the next independently indexed local header, with no hidden descriptor.
    if data_start + compressed != proof['next_local_header_offset']:
        raise ValueError('descriptor-free member/successor boundary disagreement')
    return data_start, {'flags': flags, 'compression': method,
        'data_descriptor': 'absent; flag0/header sizes and exact indexed successor agree',
        'data_start': data_start, 'compressed_bytes': compressed,
        'uncompressed_bytes': uncompressed, 'crc32': f'{crc:08x}'}


def _inflate(chunks: list[Path], candidate: Path) -> dict:
    decoder = zlib.decompressobj(-15)
    hashes = {name: hashlib.new(name) for name in ['md5', 'sha256']}
    crc, compressed, uncompressed = 0, 0, 0
    with candidate.open('xb') as output:
        for chunk in chunks:
            with chunk.open('rb') as source:
                for block in iter(lambda: source.read(65536), b''):
                    compressed += len(block)
                    if compressed > TARGET['compressed_bytes'] or decoder.eof:
                        raise ValueError('compressed stream extra bytes refused')
                    pending = block
                    while pending:
                        bound = min(65536, TARGET['uncompressed_bytes']-uncompressed+1)
                        raw = decoder.decompress(pending, bound)
                        pending = decoder.unconsumed_tail
                        uncompressed += len(raw)
                        if uncompressed > TARGET['uncompressed_bytes']:
                            raise ValueError('deflate expanded beyond frozen raw bound')
                        output.write(raw)
                        crc = zlib.crc32(raw, crc)
                        for digest in hashes.values():
                            digest.update(raw)
                        if decoder.unused_data:
                            raise ValueError('trailing deflate payload refused')
            output.flush()
            os.fsync(output.fileno())
    if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError('incomplete or extra deflate stream')
    if (compressed, uncompressed, f'{crc:08x}', hashes['md5'].hexdigest()) != (
            TARGET['compressed_bytes'], TARGET['uncompressed_bytes'],
            TARGET['crc32'], TARGET['raw_provider_md5']):
        raise ValueError('full compressed/raw size,CRC,provider-MD5 verification failed')
    return {'compressed_bytes': compressed, 'raw_bytes': uncompressed,
            'crc32': f'{crc:08x}', 'provider_md5': hashes['md5'].hexdigest(),
            'sha256': hashes['sha256'].hexdigest()}


def run(case: Path, record: Path, checksum: Path, index: Path, *, session=None) -> dict:
    case, record, checksum, index = map(Path, [case, record, checksum, index])
    confined_case = _confined(case, exists=False)
    maintenance = (ROOT/'maintenance').resolve(strict=True)
    if maintenance not in confined_case.parents or confined_case == maintenance:
        raise ValueError('fresh case strictly below personal maintenance required')
    proof = _inputs(record, checksum, index)
    case.mkdir(exist_ok=False)
    _sync_directory(case.parent)
    final = case / Path(TARGET['name']).name
    start = {'schema': 'ethoscope_single_db_range_acquisition_v1',
        'helper_sha256': _sha(Path(__file__)), 'started_unix': time.time(),
        'case': str(case), 'source_pins': proof['pins'], 'target': TARGET,
        'part_url': PART_URL, 'no_automatic_retry_or_resume': True,
        'sqlite_or_scientific_analysis_performed': False,
        'whole_part_provider_md5_verified': False,
        'h1_cleared': False, 'source_replication_passed': False,
        'biological_transfer_supported': False}
    _json(case/'acquisition-start.json', start)
    own_session = session is None
    session = session or requests.Session()
    published = False
    try:
        reader = RangeReader(case, session)
        data_start, header = _header(reader, proof)
        _json(case/'verified-local-header.json', header)
        chunks, position = [], 0
        while position < TARGET['compressed_bytes']:
            count = min(MAX_RANGE, TARGET['compressed_bytes']-position)
            chunks.append(reader.read(data_start+position, count, 'compressed-data'))
            position += count
        raw = _inflate(chunks, case/'verified-db-candidate.partial')
        _inputs(record, checksum, index)
        if _sha(Path(__file__)) != start['helper_sha256']:
            raise ValueError('helper source changed during acquisition')
        candidate = case/'verified-db-candidate.partial'
        _confined(candidate, exists=True)
        if candidate.stat().st_size != raw['raw_bytes'] or _sha(candidate) != raw['sha256']:
            raise ValueError('verified candidate changed before promotion')
        _json(case/'verified-full-member.json', raw)
        _json(case/'promotion-intent.json', {'final':str(final), **raw})
        # Exclusive hard-link publication refuses replacing any existing path.
        # A crash between link and unlink is preserved for explicit reconciliation.
        os.link(case/'verified-db-candidate.partial', final)
        published = True
        _sync_directory(case)
        (case/'verified-db-candidate.partial').unlink()
        _sync_directory(case)
        result = {**start, 'status':'provider_verified_member_acquired',
            'finished_unix':time.time(), 'database':str(final), 'verification':raw,
            'final_promoted':True, 'actual_database_schema_verified':False,
            'per_animal_exposure_timestamp_verified':False}
        _json(case/'acquisition-result.json', result)
        return result
    except BaseException as error:
        failure = {**start, 'status':'failed_no_automatic_replay',
            'finished_unix':time.time(), 'error_type':type(error).__name__,
            'error':str(error), 'final_promoted':published,
            'final_path_exists':final.exists(),
            'actual_database_schema_verified':False}
        _json(case/'acquisition-failure.json', failure)
        raise
    finally:
        if own_session:
            session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case',required=True,type=Path)
    parser.add_argument('--record',required=True,type=Path)
    parser.add_argument('--checksum',required=True,type=Path)
    parser.add_argument('--index',required=True,type=Path)
    args = parser.parse_args()
    if not hasattr(signal, 'SIGALRM'):
        raise RuntimeError('native POSIX wall-clock deadline required for real gateway acquisition')
    def deadline(signum, frame):
        raise TimeoutError('bounded900-second canary deadline exceeded; no automatic replay')
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(MAX_SECONDS)
    result = run(args.case,args.record,args.checksum,args.index)
    signal.alarm(0)
    print(json.dumps({'status':result['status'],'database':result['database'],
                      'sha256':result['verification']['sha256']}))
