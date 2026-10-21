"""One bounded publisher ZIP member for a later structural parser canary.

Only synthetic bytes are used in local tests. This helper does not parse track
values, infer identity, repair detections, qualify imputation, or admit a cohort.
The deposited ZIP CRC is checked; no cryptographic provider checksum is known.
The SHA256 of the first complete acquisition is recorded explicitly as such.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import struct
import time
import urllib.request
import zlib

ROOT = Path('/scratch/pwa209/horsehair-worm')
INDEX_SHA = '58209aca20832ec3897e30014393ace5dd25b186d4580c05d14921bb2f92a9e7'
INDEX_BYTES = 1016200
INDEX_HELPER_SHA = '8798a30cc2c7abf38ffa4a9fca6b66359a95b625807a0c25efb627369ce15c2f'
MAPPING_SHA = 'b53bcc2242a6019f6556d217116c98d021878b6d4ed88c281f752d1eb555518d'
MAPPING_BYTES = 571760
URL = 'https://vtl.biocuckoo.cn/static/data/dataset1.zip'
ARCHIVE_BYTES = 5490797248
ETAG = '"14746eac0-5f88f9022eaf8"'
MEMBER_COUNT = 1000
MAX_COMPRESSED = 8 * 1024**2
MAX_RAW = 64 * 1024**2
MAX_RANGE = 1024**2
MAX_SECONDS = 180
FALSE_FLAGS = ('scientific_result', 'admission_authority', 'full_archive_verified',
    'cryptographic_provider_checksum_verified', 'raw_track_values_parsed',
    'representative_cohort_selected', 'gene_control_mapping_verified',
    'individual_identity_clock_units_verified', 'ordered_track_imputation_qualified',
    'data_repaired_or_imputed', 'source_replication_passed', 'biological_transfer_supported')


def require(value, message):
    if not value:
        raise ValueError(message)


def sha_file(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(65536), b''):
            h.update(block)
    return h.hexdigest()


def sync_dir(path):
    if os.name == 'nt':
        return
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def exclusive(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    require(len(raw) <= 2_000_000, 'Bounded receipt size required')
    with Path(path).open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    sync_dir(Path(path).parent)


def confined(path, *, exists):
    path = Path(path)
    require(path.is_absolute(), 'Absolute personal path required')
    lexical = ROOT.absolute()
    root = ROOT.resolve(strict=True)
    result = path.resolve(strict=exists)
    require(path != lexical and lexical in path.parents and root in result.parents,
            'Path must stay strictly inside declared personal scratch')
    cursor = lexical
    for part in path.relative_to(lexical).parts:
        cursor /= part
        require(not cursor.is_symlink(), 'Subordinate symlink refused')
    if exists:
        s = path.stat()
        require(stat.S_ISREG(s.st_mode) and s.st_nlink == 1, 'Ordinary single-link source required')
    return result


def _integer(value, low, high):
    return type(value) is int and low <= value <= high


def ordinary_member(entry):
    attrs = entry.get('external_attributes')
    if not _integer(attrs, 0, 0xffffffff) or attrs & (0x10 | 0x08):
        return False
    mode = (attrs >> 16) & 0xffff
    return mode == 0 or stat.S_ISREG(mode)


def inputs(index, mapping):
    for path, expected, size in ((index, INDEX_SHA, INDEX_BYTES), (mapping, MAPPING_SHA, MAPPING_BYTES)):
        confined(path, exists=True)
        require(path.stat().st_size == size and sha_file(path) == expected, 'Exact source metadata bytes required')
    value = json.loads(index.read_text())
    require(value.get('schema_version') == 1 and value.get('status') == 'complete_publisher_zip_directory_metadata_only'
        and value.get('dataset') == 1 and value.get('url') == URL and value.get('archive_bytes') == ARCHIVE_BYTES
        and value.get('etag') == ETAG and value.get('helper_sha256') == INDEX_HELPER_SHA,
        'Frozen official dataset1 directory source required')
    require(value.get('mapping_workbook') == {'path': str(mapping), 'sha256': MAPPING_SHA, 'bytes': MAPPING_BYTES},
        'Index must bind this exact publisher mapping workbook')
    for key in ('provider_checksums_verified', 'full_archive_verified', 'raw_member_verified', 'raw_track_values_read',
                'gene_control_mapping_verified', 'individual_identity_clock_units_verified', 'scientific_result', 'admission_authority'):
        require(value.get(key) is False, 'Metadata receipt cannot grant raw/scientific authority')
    require(value.get('provider_checksum_available') is False and value.get('provider_checksums_declared') == {},
            'Frozen directory had no declared cryptographic provider checksum')
    members = value.get('members')
    directory = value.get('central_directory', {})
    require(type(members) is list and len(members) == MEMBER_COUNT
        and directory.get('entries') == MEMBER_COUNT
        and _integer(directory.get('offset'), 1, ARCHIVE_BYTES-22), 'Exact complete bounded index roster required')
    names, offsets, eligible = set(), set(), []
    for entry in members:
        require(type(entry) is dict and type(entry.get('name')) is str, 'Index member metadata required')
        name = entry['name']
        offset = entry.get('local_header_offset')
        require(name not in names and _integer(offset, 0, directory['offset']-30) and offset not in offsets,
                'Unique complete names and local offsets required')
        names.add(name)
        offsets.add(offset)
        eligible_name = re.fullmatch(r'[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}_[1-9][0-9]*_[1-9][0-9]*-order\.txt', name)
        if (eligible_name and entry.get('directory') is False and ordinary_member(entry)
            and entry.get('disk') == 0 and entry.get('flags') == 0 and entry.get('method') == 8
            and entry.get('version_needed') == 20 and entry.get('extra_field_ids') in ([], [10])
            and entry.get('comment_bytes') == 0
            and _integer(entry.get('compressed_bytes'), 1, MAX_COMPRESSED)
            and _integer(entry.get('raw_bytes'), 1, MAX_RAW)):
            require(re.fullmatch('[0-9a-f]{8}', entry.get('crc32', ''))
                and re.fullmatch('[0-9a-f]{64}', entry.get('central_record_sha256', ''))
                and entry.get('name_raw_sha256') == hashlib.sha256(name.encode('ascii')).hexdigest(),
                'Exact central CRC/name/record hashes required')
            eligible.append((hashlib.sha256(name.encode('ascii')).hexdigest(), name, entry))
    require(eligible, 'No supported bounded ordinary whole-track member; no fallback')
    eligible.sort(key=lambda row: (row[0], row[1]))
    selected = eligible[0][2]
    end = min([offset for offset in offsets if offset > selected['local_header_offset']] + [directory['offset']])
    require(selected['local_header_offset']+30+selected['compressed_bytes'] < end,
            'Selected complete member must fit before independently indexed successor')
    roster = [{'name': name, 'name_sha256': h} for h, name, _ in eligible]
    return {'selected': selected, 'successor_offset': end,
        'successor_signature': '504b0102' if end == directory['offset'] else '504b0304',
        'selection': {'rule': 'minimum_sha256_of_ascii_filename_among_frozen_bounded_supported_members_v1',
            'selected_filename_sha256': eligible[0][0], 'eligible_members': len(eligible),
            'eligible_filename_roster_sha256': hashlib.sha256(json.dumps(roster, sort_keys=True,
                separators=(',', ':')).encode()).hexdigest(), 'outcome_values_used': False,
            'representative_cohort_claim': False},
        'source_pins': [{'path': str(index), 'bytes': INDEX_BYTES, 'sha256': INDEX_SHA},
                        {'path': str(mapping), 'bytes': MAPPING_BYTES, 'sha256': MAPPING_SHA}]}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Publisher redirect refused')


def network(method, url, headers, timeout):
    return urllib.request.build_opener(NoRedirect()).open(
        urllib.request.Request(url, headers=headers, method=method), timeout=timeout)


def public_headers(response):
    h = {k.lower(): v for k, v in response.headers.items()}
    return {k: h[k] for k in ('content-length', 'content-range', 'content-encoding', 'accept-ranges',
        'etag', 'last-modified', 'content-type', 'digest', 'content-md5') if k in h}


class Reader:
    def __init__(self, case, requester):
        self.folder = case/'ranges'
        self.folder.mkdir()
        self.requester = requester
        self.deadline = time.monotonic()+MAX_SECONDS
        self.number, self.bytes = 0, 0
        response = requester('HEAD', URL, {'Accept-Encoding': 'identity', 'If-Match': ETAG}, self.remaining())
        try:
            h = public_headers(response)
            exclusive(case/'head.json', {'status': response.status, 'url': response.geturl(), 'headers': h})
            require(response.status == 200 and response.geturl() == URL
                and h.get('content-length') == str(ARCHIVE_BYTES) and h.get('etag') == ETAG
                and h.get('content-encoding', 'identity').lower() == 'identity'
                and h.get('accept-ranges', '').lower() == 'bytes', 'Exact frozen identity200 HEAD/ETag required')
            self.provider_checksums = {k: h[k] for k in ('digest', 'content-md5') if k in h}
        finally:
            response.close()

    def remaining(self):
        seconds = self.deadline-time.monotonic()
        require(seconds > 0, 'Finite acquisition deadline reached')
        return min(30, seconds)

    def read(self, start, size, role):
        require(_integer(start, 0, ARCHIVE_BYTES-1) and _integer(size, 1, MAX_RANGE)
            and start+size <= ARCHIVE_BYTES and self.bytes+size <= MAX_COMPRESSED+2*8192,
            'Exact bounded single-archive range required')
        self.remaining()
        self.number += 1
        prefix = self.folder/f'{self.number:04d}-{role}'
        path = prefix.with_suffix('.bin')
        end = start+size-1
        exclusive(str(prefix)+'-intent.json', {'url': URL, 'start': start, 'end': end, 'bytes': size,
            'archive_bytes': ARCHIVE_BYTES, 'etag': ETAG, 'no_automatic_retry': True})
        result = {'status': 'not_opened', 'bytes_preserved': 0, 'verified': False}
        response = None
        try:
            response = self.requester('GET', URL, {'Accept-Encoding': 'identity', 'If-Match': ETAG,
                'Range': f'bytes={start}-{end}'}, self.remaining())
            h = public_headers(response)
            result.update(status=response.status, url=response.geturl(), headers=h)
            require(response.status == 206 and response.geturl() == URL
                and h.get('content-range') == f'bytes {start}-{end}/{ARCHIVE_BYTES}'
                and h.get('content-length') == str(size) and h.get('etag') == ETAG
                and h.get('content-encoding', 'identity').lower() == 'identity',
                'Exact identity206 range/total/length/ETag required; nonrange body not read')
            with path.open('xb') as stream:
                try:
                    while True:
                        self.remaining()
                        allowance = min(65536, size+1-result['bytes_preserved'])
                        block = response.read(allowance)
                        if not block:
                            break
                        # Keep at most the bounded declaration plus one byte,
                        # even if a faulty response object ignores read(n).
                        stream.write(block[:allowance])
                        result['bytes_preserved'] += min(len(block), allowance)
                        if len(block) > allowance or result['bytes_preserved'] > size:
                            raise ValueError('Range response body exceeded exact declared bound')
                finally:
                    stream.flush()
                    os.fsync(stream.fileno())
            require(result['bytes_preserved'] == size, 'Truncated range body')
            result.update(verified=True, sha256=sha_file(path))
            self.bytes += size
            return path
        except BaseException as error:
            result.update(error_type=type(error).__name__, error=str(error))
            raise
        finally:
            if response is not None:
                response.close()
            exclusive(str(prefix)+'-response.json', result)


def local_header(reader, proof):
    selected = proof['selected']
    offset = selected['local_header_offset']
    raw = reader.read(offset, 30, 'local-header').read_bytes()
    magic, version, flags, method, mtime, mdate, crc, compressed, raw_size, n, x = struct.unpack('<4s5H3I2H', raw)
    require(magic == b'PK\x03\x04' and version == selected['version_needed'] == 20
        and flags == selected['flags'] == 0 and method == selected['method'] == 8,
        'Unsupported local header/version/flags/encryption/descriptor/compression')
    require((mtime, mdate, f'{crc:08x}', compressed, raw_size) ==
        (selected['dos_time'], selected['dos_date'], selected['crc32'], selected['compressed_bytes'], selected['raw_bytes']),
        'Local/central time, CRC or exact size disagreement')
    require(1 <= n <= 1024 and 0 <= x <= 4096, 'Bounded local filename/extra required')
    both = reader.read(offset+30, n+x, 'local-name-extra').read_bytes()
    require(both[:n] == selected['name'].encode('ascii'), 'Local filename differs from frozen selection')
    extra, ids, p = both[n:], [], 0
    while p < len(extra):
        require(p+4 <= len(extra), 'Truncated ZIP extra header')
        kind, length = struct.unpack_from('<HH', extra, p)
        p += 4
        require(kind == 10 and kind not in ids and length == 32 and p+length <= len(extra),
                'Only complete single NTFS10 extra is supported; ZIP64/unknown variants refused')
        payload = extra[p:p+length]
        require(payload[:4] == b'\0'*4 and struct.unpack_from('<HH', payload, 4) == (1, 24),
                'Malformed NTFS reserved/tag/length variant refused')
        ids.append(kind)
        p += length
    start = offset+30+n+x
    require(start+compressed == proof['successor_offset'],
        'Descriptor-free payload must end exactly at independently indexed successor')
    boundary = reader.read(proof['successor_offset'], 4, 'successor-signature').read_bytes()
    require(boundary.hex() == proof['successor_signature'], 'Indexed successor signature mismatch')
    return start, {'data_start': start, 'compressed_bytes': compressed, 'raw_bytes': raw_size,
        'crc32': f'{crc:08x}', 'flags': flags, 'method': method, 'local_extra_field_ids': ids,
        'local_extra_sha256': hashlib.sha256(extra).hexdigest(),
        'data_descriptor': 'absent; flag0 sizes and exact indexed successor agree'}


def inflate(chunks, candidate, selected, deadline):
    decoder = zlib.decompressobj(-15)
    h = hashlib.sha256()
    compressed = raw_bytes = crc = 0
    with candidate.open('xb') as out:
        try:
            for path in chunks:
                with path.open('rb') as stream:
                    for block in iter(lambda: stream.read(65536), b''):
                        require(time.monotonic() < deadline, 'Finite inflation deadline reached')
                        compressed += len(block)
                        require(compressed <= selected['compressed_bytes'] and not decoder.eof,
                                'Compressed stream trailing bytes refused')
                        pending = block
                        while pending:
                            require(time.monotonic() < deadline, 'Finite inflation deadline reached')
                            raw = decoder.decompress(pending, min(65536, selected['raw_bytes']-raw_bytes+1))
                            pending = decoder.unconsumed_tail
                            raw_bytes += len(raw)
                            require(raw_bytes <= selected['raw_bytes'] <= MAX_RAW, 'Deflate raw bound exceeded')
                            out.write(raw)
                            h.update(raw)
                            crc = zlib.crc32(raw, crc)
                            require(not decoder.unused_data, 'Trailing deflate payload refused')
            require(decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail,
                    'Incomplete/extra deflate stream refused')
            require((compressed, raw_bytes, f'{crc:08x}') ==
                (selected['compressed_bytes'], selected['raw_bytes'], selected['crc32']),
                'Complete compressed/raw sizes and deposited CRC must agree')
        finally:
            out.flush()
            os.fsync(out.fileno())
    return {'compressed_bytes': compressed, 'raw_bytes': raw_bytes, 'crc32': f'{crc:08x}',
        'sha256': h.hexdigest(), 'sha256_basis': 'first_complete_acquisition_sha256_not_provider_checksum',
        'deposited_zip_crc_verified': True, 'cryptographic_provider_member_checksum_available': False,
        'cryptographic_provider_checksum_verified': False}


def acquire(case, index, mapping, *, requester=network):
    case, index, mapping = map(Path, (case, index, mapping))
    result_case = confined(case, exists=False)
    require((ROOT/'maintenance').resolve(strict=True) in result_case.parents and not case.exists(),
            'Fresh case strictly below personal maintenance required; no replay')
    proof = inputs(index, mapping)
    case.mkdir(exist_ok=False)
    sync_dir(case.parent)
    final = case/proof['selected']['name']
    start = {'schema_version': 1, 'schema': 'vtl_single_whole_member_acquisition_v1',
        'started_unix': time.time(), 'case': str(case), 'url': URL,
        'helper_sha256': sha_file(Path(__file__)), 'archive_bytes': ARCHIVE_BYTES, 'etag': ETAG,
        'selected_member': proof['selected'], 'selection': proof['selection'], 'source_pins': proof['source_pins'],
        'maximum_seconds': MAX_SECONDS, 'no_automatic_retry_or_resume': True,
        'purpose': 'bounded_complete_member_for_later_outcome_free_structural_parser_canary',
        **{key: False for key in FALSE_FLAGS}}
    exclusive(case/'acquisition-start.json', start)
    published = False
    try:
        reader = Reader(case, requester)
        data_start, header = local_header(reader, proof)
        exclusive(case/'verified-local-header.json', header)
        chunks, position = [], 0
        while position < proof['selected']['compressed_bytes']:
            size = min(MAX_RANGE, proof['selected']['compressed_bytes']-position)
            chunks.append(reader.read(data_start+position, size, 'compressed-data'))
            position += size
        candidate = case/'verified-track-candidate.partial'
        verified = inflate(chunks, candidate, proof['selected'], reader.deadline)
        require(inputs(index, mapping) == proof and sha_file(Path(__file__)) == start['helper_sha256'],
                'Metadata/helper source changed during acquisition')
        confined(candidate, exists=True)
        require(candidate.stat().st_size == verified['raw_bytes'] and sha_file(candidate) == verified['sha256'],
                'Full candidate changed before exclusive promotion')
        exclusive(case/'verified-full-member.json', verified)
        exclusive(case/'promotion-intent.json', {'final': str(final), **verified})
        os.link(candidate, final)
        published = True
        sync_dir(case)
        candidate.unlink()
        sync_dir(case)
        confined(final, exists=True)
        require(final.stat().st_size == verified['raw_bytes'] and sha_file(final) == verified['sha256'],
                'Published complete member no longer matches first acquisition')
        result = {**start, 'status': 'complete_whole_member_crc_verified_first_sha256_recorded',
            'finished_unix': time.time(), 'member': str(final), 'verification': verified,
            'final_promoted': True, 'http_provider_checksums_declared': reader.provider_checksums,
            'http_provider_checksums_verified': False, 'requested_range_bytes': reader.bytes}
        exclusive(case/'acquisition-result.json', result)
        return result
    except BaseException as error:
        exclusive(case/'acquisition-failure.json', {**start, 'status': 'failed_no_automatic_replay',
            'finished_unix': time.time(), 'error_type': type(error).__name__, 'error': str(error),
            'final_promoted': published, 'final_path_exists': final.exists()})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', type=Path, required=True)
    parser.add_argument('--index', type=Path, required=True)
    parser.add_argument('--mapping-workbook', type=Path, required=True)
    args = parser.parse_args()
    require(hasattr(signal, 'SIGALRM'), 'Native POSIX hard deadline required for gateway acquisition')
    def deadline(signum, frame):
        raise TimeoutError('Bounded180-second canary deadline exceeded; no automatic replay')
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(MAX_SECONDS)
    result = acquire(args.case, args.index, args.mapping_workbook)
    signal.alarm(0)
    print(json.dumps({'status': result['status'], 'member': result['member'], 'sha256': result['verification']['sha256']}))


if __name__ == '__main__':
    main()
