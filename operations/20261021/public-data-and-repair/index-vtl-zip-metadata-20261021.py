"""Bounded publisher ZIP-directory probe. Never requests a track/local header.

Only HEAD, the fixed22-byte EOCD, a declared ZIP64 footer, and the declared
central directory are fetched. No extraction, scientific values or code run.
The caller owns this private source pin and the native gateway invocation.
"""
from pathlib import Path, PurePosixPath
import argparse
import hashlib
import json
import re
import struct
import time
import urllib.request

ROOT = Path('/scratch/pwa209/horsehair-worm')
MAPPING_SHA256 = 'b53bcc2242a6019f6556d217116c98d021878b6d4ed88c281f752d1eb555518d'
MAPPING_BYTES = 571760
MAX_METADATA_BYTES = 2_000_000
MAX_ARCHIVE_BYTES = 1_000_000_000_000
MAX_ENTRIES = 100_000
MAX_SECONDS = 180


def require(value, message):
    if not value: raise ValueError(message)


def sha(raw): return hashlib.sha256(raw).hexdigest()


def exclusive(path, value):
    import os
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    require(len(raw) <= 16_000_000, 'Finite output metadata bound exceeded')
    with Path(path).open('xb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())


def confined(path, root):
    original = Path(path).absolute(); root = Path(root).resolve(strict=True)
    require(original == original.resolve() and original.is_relative_to(root) and original != root
            and all(not p.is_symlink() for p in [original, *original.parents] if p.is_relative_to(root)),
            'Exact ordinary personal namespace required')
    return original


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): raise ValueError('Publisher redirect refused')


def network(method, url, headers, timeout):
    return urllib.request.build_opener(NoRedirect()).open(
        urllib.request.Request(url, headers=headers, method=method), timeout=timeout)


def _headers(response): return {k.lower(): v for k, v in response.headers.items()}


def _public_headers(headers):
    return {k: headers[k] for k in ('content-length', 'content-range', 'content-type', 'content-encoding',
        'accept-ranges', 'etag', 'last-modified', 'digest', 'content-md5') if k in headers}


class Probe:
    def __init__(self, url, case, *, requester, expected_total=None):
        self.url, self.case, self.requester = url, case, requester
        self.deadline = time.monotonic() + MAX_SECONDS
        self.bytes, self.reads, self.total, self.etag = 0, [], None, None
        self.provider_checksums = {}
        response = requester('HEAD', url, {'Accept-Encoding': 'identity'}, self.remaining())
        try:
            headers = _headers(response)
            exclusive(case / 'head.json', {'status': response.status, 'url': response.geturl(), 'headers': _public_headers(headers)})
            require(response.status == 200 and response.geturl() == url
                    and headers.get('content-encoding', 'identity').lower() == 'identity'
                    and re.fullmatch('[1-9][0-9]*', headers.get('content-length', ''))
                    and headers.get('accept-ranges', '').lower() == 'bytes', 'Exact identity200 byte-range HEAD required')
            self.total = int(headers['content-length']); self.etag = headers.get('etag')
            self.provider_checksums = {k: headers[k] for k in ('digest','content-md5') if k in headers}
            require(22 <= self.total <= MAX_ARCHIVE_BYTES and (expected_total is None or self.total == expected_total),
                    'Publisher archive length differs or exceeds finite bound')
        finally: response.close()

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, 'Finite metadata probe deadline reached')
        return min(30, remaining)

    def read(self, start, size, role):
        require(type(start) is int and type(size) is int and 0 <= start < self.total
                and 0 < size <= self.total - start and self.bytes + size <= MAX_METADATA_BYTES,
                'Declared metadata range exceeds exact archive or2MB total budget')
        self.remaining(); end = start + size - 1; number = len(self.reads)
        headers = {'Range': f'bytes={start}-{end}', 'Accept-Encoding': 'identity'}
        if self.etag and not self.etag.startswith('W/'): headers['If-Match'] = self.etag
        intent = {'role': role, 'url': self.url, 'start': start, 'end': end, 'bytes': size, 'archive_bytes': self.total}
        exclusive(self.case / f'range-{number:02d}-intent.json', intent)
        response = self.requester('GET', self.url, headers, self.remaining())
        try:
            actual = _headers(response)
            exclusive(self.case / f'range-{number:02d}-headers.json', {'status': response.status,
                'url': response.geturl(), 'headers': _public_headers(actual)})
            require(response.status == 206 and response.geturl() == self.url
                    and actual.get('content-range') == f'bytes {start}-{end}/{self.total}'
                    and actual.get('content-length') == str(size)
                    and actual.get('content-encoding', 'identity').lower() == 'identity'
                    and (self.etag is None or actual.get('etag') == self.etag),
                    'Exact identity206 Content-Range/total/length/ETag required; body was not read')
            # Stream to an exclusive retained file before validation, including
            # partial bytes if a network read fails. No body of a200 is read.
            import http.client
            import os
            chunks, received = [], 0
            with (self.case / f'range-{number:02d}-{role}.bin').open('xb') as retained:
                try:
                    while received <= size:
                        self.remaining()
                        try: chunk = response.read(min(65536,size+1-received))
                        except http.client.IncompleteRead as error:
                            if error.partial:
                                retained.write(error.partial); chunks.append(error.partial)
                                received += len(error.partial); self.bytes += len(error.partial)
                            raise
                        if not chunk: break
                        retained.write(chunk); chunks.append(chunk); received += len(chunk); self.bytes += len(chunk)
                finally: retained.flush(); os.fsync(retained.fileno())
            raw = b''.join(chunks)
            require(len(raw) == size, 'Truncated or oversized publisher range preserved')
            require(sha((self.case / f'range-{number:02d}-{role}.bin').read_bytes()) == sha(raw), 'Retained metadata bytes differ')
            record = {**intent, 'sha256': sha(raw), 'path': str(self.case / f'range-{number:02d}-{role}.bin')}
            self.reads.append(record); self.remaining()
            return raw
        finally: response.close()


def footer(probe):
    offset = probe.total - 22
    raw = probe.read(offset, 22, 'eocd')
    signature, disk, cd_disk, disk_entries, entries, size, start, comment = struct.unpack('<4s4H2IH', raw)
    require(signature == b'PK\x05\x06' and comment == 0 and disk == cd_disk == 0,
            'Only unsplit ZIPs with exact zero-comment EOCD are supported; no broad-tail fallback')
    zip64 = entries == 0xffff or disk_entries == 0xffff or size == 0xffffffff or start == 0xffffffff
    boundary = offset
    if zip64:
        locator_at = offset - 20; require(locator_at >= 0, 'Missing ZIP64 locator')
        signature, locator_disk, zip64_at, disks = struct.unpack('<4sIQI', probe.read(locator_at, 20, 'zip64-locator'))
        require(signature == b'PK\x06\x07' and locator_disk == 0 and disks == 1 and 0 <= zip64_at <= locator_at - 56,
                'Exact single-disk ZIP64 locator required')
        values = struct.unpack('<4sQ2H2I4Q', probe.read(zip64_at, 56, 'zip64-eocd'))
        sig, record_size, made, needed, d, cdd, n_disk, n, cd_size, cd_start = values
        require(sig == b'PK\x06\x06' and record_size >= 44 and zip64_at + 12 + record_size == locator_at
                and d == cdd == 0 and n_disk == n, 'Malformed/nonadjacent ZIP64 EOCD')
        if record_size > 44: probe.read(zip64_at + 56, record_size - 44, 'zip64-extension')
        # Classic fields that are not sentinel values must agree exactly.
        for old, new, sentinel in [(disk_entries,n,0xffff),(entries,n,0xffff),(size,cd_size,0xffffffff),(start,cd_start,0xffffffff)]:
            require(old == sentinel or old == new, 'Classic/ZIP64 footer fields disagree')
        entries, size, start, boundary = n, cd_size, cd_start, zip64_at
    else: require(disk_entries == entries, 'Single-disk central entry counts disagree')
    require(0 < entries <= MAX_ENTRIES and 0 < size <= MAX_METADATA_BYTES and 0 <= start < boundary
            and start + size == boundary, 'Exact bounded central-directory-only interval required')
    return {'zip64': zip64, 'entries': entries, 'offset': start, 'bytes': size, 'footer_boundary': boundary}


def central_records(raw, declared, total):
    records, names, cursor = [], set(), 0
    while cursor < len(raw):
        require(cursor + 46 <= len(raw), 'Truncated central directory header')
        fields = struct.unpack_from('<4s6H3I5H2I', raw, cursor)
        (signature,made,needed,flags,method,mtime,mdate,crc,compressed,uncompressed,
         name_size,extra_size,comment_size,disk,internal,external,local) = fields
        end = cursor + 46 + name_size + extra_size + comment_size
        require(signature == b'PK\x01\x02' and name_size > 0 and end <= len(raw), 'Malformed central directory record')
        name_raw = raw[cursor+46:cursor+46+name_size]
        name = name_raw.decode('utf-8' if flags & 0x800 else 'cp437')
        require('\x00' not in name and '\\' not in name and not name.startswith('/')
                and all(p not in ('', '.', '..') for p in name.rstrip('/').split('/'))
                and ':' not in name.split('/')[0] and name not in names, 'Unsafe or duplicate archive member name')
        names.add(name); extra = raw[cursor+46+name_size:cursor+46+name_size+extra_size]
        extras, p = {}, 0
        while p < len(extra):
            require(p+4 <= len(extra), 'Truncated ZIP extra field')
            key,length = struct.unpack_from('<HH',extra,p); p += 4
            require(p+length <= len(extra) and key not in extras, 'Truncated/duplicate ZIP extra field')
            extras[key] = extra[p:p+length]; p += length
        need64 = uncompressed == 0xffffffff or compressed == 0xffffffff or local == 0xffffffff or disk == 0xffff
        if need64:
            require(1 in extras, 'Missing required ZIP64 member extra')
            extended, position = extras[1], 0
            values = [uncompressed, compressed, local, disk]
            for i, sentinel in enumerate((0xffffffff,0xffffffff,0xffffffff,0xffff)):
                if values[i] == sentinel:
                    width = 4 if i == 3 else 8
                    require(position+width <= len(extended), 'Truncated required ZIP64 member size/offset')
                    values[i] = int.from_bytes(extended[position:position+width], 'little'); position += width
            uncompressed, compressed, local, disk = values
        require(disk == 0 and 0 <= local < declared['offset'] and local + 30 + compressed <= declared['offset'],
                'Member offset/size escapes archive body or uses another disk')
        require(len(records) < MAX_ENTRIES, 'Finite member count exceeded')
        records.append({'name': name, 'name_raw_sha256': sha(name_raw), 'central_record_offset': declared['offset']+cursor,
            'central_record_bytes': end-cursor, 'central_record_sha256': sha(raw[cursor:end]),
            'version_made_by': made, 'version_needed': needed, 'flags': flags, 'method': method,
            'dos_time': mtime, 'dos_date': mdate, 'crc32': f'{crc:08x}', 'compressed_bytes': compressed,
            'raw_bytes': uncompressed, 'local_header_offset': local, 'disk': disk,
            'external_attributes': external, 'internal_attributes': internal, 'directory': name.endswith('/'),
            'extra_field_ids': sorted(extras), 'extra_sha256': sha(extra),
            'comment_bytes': comment_size, 'comment_sha256': sha(raw[end-comment_size:end]) if comment_size else sha(b''),
            'unverified_acquisition_candidate': method in (0,8) and not bool(flags & (1|0x40|0x2000))})
        cursor = end
    require(cursor == len(raw) and len(records) == declared['entries'], 'Exact complete central record count/bytes differ')
    return records


def acquire(dataset, case, *, mapping_workbook, expected_total=None, requester=network):
    require(type(dataset) is int and 1 <= dataset <= 24, 'Exact publisher dataset number1..24 required')
    require(expected_total is None or type(expected_total) is int and expected_total > 0, 'Explicit positive expected archive length required')
    root = ROOT.resolve(strict=True); case = confined(case,root)
    require(case.is_relative_to(root/'maintenance') and not case.exists(), 'Fresh personal maintenance case required; no replay')
    mapping = confined(mapping_workbook,root)
    require(mapping.is_file() and mapping.stat().st_size == MAPPING_BYTES and sha(mapping.read_bytes()) == MAPPING_SHA256,
            'Exact publisher filename/gene mapping workbook required')
    case.mkdir(parents=True,exist_ok=False)
    url = f'https://vtl.biocuckoo.cn/static/data/dataset{dataset}.zip'
    source_sha = sha(Path(__file__).read_bytes())
    exclusive(case/'intent.json',{'schema_version':1,'dataset':dataset,'url':url,'helper_sha256':source_sha,
        'mapping_workbook':{'path':str(mapping),'sha256':MAPPING_SHA256,'bytes':MAPPING_BYTES},
        'expected_archive_bytes':expected_total,'maximum_requested_metadata_bytes':MAX_METADATA_BYTES,
        'maximum_seconds':MAX_SECONDS,'raw_track_or_local_header_requests_permitted':False,
        'scientific_result':False,'admission_authority':False})
    try:
        probe = Probe(url,case,requester=requester,expected_total=expected_total)
        directory = footer(probe)
        raw = probe.read(directory['offset'],directory['bytes'],'central-directory')
        records = central_records(raw,directory,probe.total)
        require(sha(mapping.read_bytes()) == MAPPING_SHA256,'Publisher mapping source changed during probe')
        require(sha(Path(__file__).read_bytes()) == source_sha,'Metadata helper source changed during probe')
        result = {'schema_version':1,'status':'complete_publisher_zip_directory_metadata_only','dataset':dataset,'url':url,
            'helper_sha256':source_sha,'archive_bytes':probe.total,'etag':probe.etag,
            'provider_checksum_available':bool(probe.provider_checksums),'provider_checksums_declared':probe.provider_checksums,
            'provider_checksums_verified':False,'full_archive_verified':False,'raw_member_verified':False,
            'mapping_workbook':{'path':str(mapping),'sha256':MAPPING_SHA256,'bytes':MAPPING_BYTES},
            'central_directory':{**directory,'sha256':sha(raw)},'ranges':probe.reads,
            'metadata_bytes_received':probe.bytes,'members':records,'raw_track_values_read':False,
            'gene_control_mapping_verified':False,'individual_identity_clock_units_verified':False,
            'scientific_result':False,'admission_authority':False}
        exclusive(case/'metadata-index.json',result);return result
    except BaseException as error:
        exclusive(case/'failure.json',{'status':'failed_metadata_probe_preserved','error_type':type(error).__name__,
            'error':str(error),'automatic_replay_permitted':False,'scientific_result':False,'admission_authority':False})
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset',type=int,required=True);parser.add_argument('--case',type=Path,required=True)
    parser.add_argument('--mapping-workbook',type=Path,required=True);parser.add_argument('--expected-total',type=int)
    args=parser.parse_args();result=acquire(args.dataset,args.case,mapping_workbook=args.mapping_workbook,expected_total=args.expected_total)
    print(json.dumps({k:result[k] for k in ('status','dataset','archive_bytes','metadata_bytes_received','scientific_result','admission_authority')},sort_keys=True))


if __name__=='__main__':main()
