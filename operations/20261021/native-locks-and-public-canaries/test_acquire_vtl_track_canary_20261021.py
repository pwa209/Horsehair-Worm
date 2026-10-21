"""Synthetic complete ZIP members only; no public requests or track inspection."""
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import stat
import struct
import time
import zlib

import pytest

SOURCE = Path(__file__).with_name('acquire-vtl-track-canary-20261021.py')
spec = importlib.util.spec_from_file_location('vtl_whole_canary_private', SOURCE)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Response:
    def __init__(self, body, headers, status=206):
        self.body, self.headers, self.status = body, headers, status
        self.position, self.read_calls, self.closed = 0, 0, False
        self.url = m.URL
        self.fail_after = None

    def geturl(self):
        return self.url

    def close(self):
        self.closed = True

    def read(self, count):
        self.read_calls += 1
        if self.fail_after is not None and self.position >= self.fail_after:
            raise TimeoutError('synthetic interrupted read')
        count = min(count, 37)
        raw = self.body[self.position:self.position+count]
        self.position += len(raw)
        return raw


class Network:
    def __init__(self, f, change=None):
        self.f, self.change, self.calls, self.responses = f, change, [], []

    def __call__(self, method, url, headers, timeout):
        assert url == m.URL and headers['Accept-Encoding'] == 'identity'
        assert headers['If-Match'] == m.ETAG and 0 < timeout <= 30
        self.calls.append((method, dict(headers)))
        if method == 'HEAD':
            r = Response(b'', {'Content-Length': str(m.ARCHIVE_BYTES), 'ETag': m.ETAG,
                'Accept-Ranges': 'bytes'}, 200)
        else:
            a, b = map(int, headers['Range'].removeprefix('bytes=').split('-'))
            assert b-a+1 <= m.MAX_RANGE
            r = Response(self.f['archive'][a:b+1], {'Content-Length': str(b-a+1),
                'Content-Range': f'bytes {a}-{b}/{m.ARCHIVE_BYTES}', 'ETag': m.ETAG})
        if self.change:
            self.change(r, len(self.calls), method, headers)
        self.responses.append(r)
        return r


def encode(raw):
    obj = zlib.compressobj(wbits=-15)
    return obj.compress(raw)+obj.flush()


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    root = tmp_path/'personal'
    (root/'maintenance').mkdir(parents=True)
    folder = root/'sources'
    folder.mkdir()
    mapping = folder/'vtl_information.xlsx'
    mapping.write_bytes(b'Explicitly synthetic filename-only mapping; no scientific values')
    names = ['2019-11-5_1_1-order.txt', '2019-11-5_1_2-order.txt']
    archive, entries, content = b'PREFIX', [], {}
    rng = random.Random(291)
    ntfs = struct.pack('<HHIHH', 10, 32, 0, 1, 24)+b'\0'*24
    for number, name in enumerate(names):
        raw = bytes(rng.randrange(256) for _ in range(3000+number))
        data = encode(raw)
        offset = len(archive)
        name_raw = name.encode('ascii')
        header = struct.pack('<4s5H3I2H', b'PK\x03\x04', 20, 0, 8, 11, 22,
            zlib.crc32(raw), len(data), len(raw), len(name_raw), len(ntfs))
        start = offset+30+len(name_raw)+len(ntfs)
        archive += header+name_raw+ntfs+data
        entries.append({'name': name, 'local_header_offset': offset, 'directory': False,
            'disk': 0, 'flags': 0, 'method': 8, 'version_needed': 20,
            'extra_field_ids': [10], 'comment_bytes': 0, 'compressed_bytes': len(data),
            'raw_bytes': len(raw), 'crc32': f'{zlib.crc32(raw):08x}',
            'central_record_sha256': hashlib.sha256(b'synthetic-central-'+name_raw).hexdigest(),
            'name_raw_sha256': hashlib.sha256(name_raw).hexdigest(),
            'dos_time': 11, 'dos_date': 22, 'external_attributes': 32})
        content[name] = {'raw': raw, 'payload': data, 'start': start, 'offset': offset,
            'extra_offset': offset+30+len(name_raw)}
    central = len(archive)
    archive += b'PK\x01\x02synthetic-directory-not-needed-by-this-source-bound-reader'
    index = folder/'metadata-index.json'
    value = {'schema_version': 1, 'status': 'complete_publisher_zip_directory_metadata_only',
        'dataset': 1, 'url': m.URL, 'archive_bytes': len(archive), 'etag': m.ETAG,
        'helper_sha256': m.INDEX_HELPER_SHA, 'mapping_workbook': {'path': str(mapping),
            'sha256': m.sha_file(mapping), 'bytes': mapping.stat().st_size},
        'provider_checksum_available': False, 'provider_checksums_declared': {},
        'central_directory': {'entries': len(entries), 'offset': central}, 'members': entries}
    value.update({k: False for k in ('provider_checksums_verified', 'full_archive_verified', 'raw_member_verified',
        'raw_track_values_read', 'gene_control_mapping_verified', 'individual_identity_clock_units_verified',
        'scientific_result', 'admission_authority')})
    index.write_text(json.dumps(value))
    monkeypatch.setattr(m, 'ROOT', root)
    monkeypatch.setattr(m, 'MEMBER_COUNT', len(entries))
    monkeypatch.setattr(m, 'INDEX_BYTES', index.stat().st_size)
    monkeypatch.setattr(m, 'INDEX_SHA', m.sha_file(index))
    monkeypatch.setattr(m, 'MAPPING_BYTES', mapping.stat().st_size)
    monkeypatch.setattr(m, 'MAPPING_SHA', m.sha_file(mapping))
    monkeypatch.setattr(m, 'ARCHIVE_BYTES', len(archive))
    monkeypatch.setattr(m, 'MAX_RANGE', 256)
    selected = min(names, key=lambda n: (hashlib.sha256(n.encode('ascii')).hexdigest(), n))
    return {'root': root, 'index': index, 'mapping': mapping, 'value': value,
        'archive': archive, 'content': content, 'selected': selected,
        'case': root/'maintenance/canary', 'central': central}


def rewrite(f, monkeypatch):
    f['index'].write_text(json.dumps(f['value']))
    monkeypatch.setattr(m, 'INDEX_BYTES', f['index'].stat().st_size)
    monkeypatch.setattr(m, 'INDEX_SHA', m.sha_file(f['index']))


def invoke(f, change=None):
    net = Network(f, change)
    result = m.acquire(f['case'], f['index'], f['mapping'], requester=net)
    return result, net


def test_complete_member_crc_and_first_acquisition_sha_truth(fixture):
    f = fixture
    result, net = invoke(f)
    final = Path(result['member'])
    assert final.name == f['selected'] and final.read_bytes() == f['content'][f['selected']]['raw']
    assert result['verification']['sha256'] == hashlib.sha256(final.read_bytes()).hexdigest()
    assert result['verification']['deposited_zip_crc_verified'] is True
    assert result['verification']['sha256_basis'] == 'first_complete_acquisition_sha256_not_provider_checksum'
    assert result['verification']['cryptographic_provider_member_checksum_available'] is False
    assert result['http_provider_checksums_declared'] == {}
    assert result['http_provider_checksums_verified'] is False
    assert all(result[k] is False for k in m.FALSE_FLAGS)
    assert result['selection']['outcome_values_used'] is False
    assert result['selection']['eligible_members'] == 2
    assert final.stat().st_nlink == 1 and not (f['case']/'verified-track-candidate.partial').exists()
    assert len(net.calls) > 10 and net.calls[0][0] == 'HEAD'
    assert all(r.closed for r in net.responses)
    assert all(json.loads(p.read_text())['verified'] for p in (f['case']/'ranges').glob('*-response.json'))


@pytest.mark.parametrize('delta', [{'ETag': '"changed"'}, {'Content-Length': '1'},
    {'Accept-Ranges': 'none'}, {'Content-Encoding': 'gzip'}])
def test_changed_or_unsupported_head_stops_before_any_get(fixture, delta):
    net = Network(fixture, lambda r,n,method,h: r.headers.update(delta) if method == 'HEAD' else None)
    with pytest.raises(ValueError, match='HEAD'):
        m.acquire(fixture['case'], fixture['index'], fixture['mapping'], requester=net)
    assert len(net.calls) == 1 and net.responses[0].read_calls == 0
    assert json.loads((fixture['case']/'acquisition-failure.json').read_text())['final_promoted'] is False


@pytest.mark.parametrize('status', [200, 302, 401, 403, 500])
def test_non206_body_not_read_or_retried(fixture, status):
    def change(r, number, method, headers):
        if method == 'GET': r.status = status
    net = Network(fixture, change)
    with pytest.raises(ValueError, match='identity206'):
        m.acquire(fixture['case'], fixture['index'], fixture['mapping'], requester=net)
    assert len(net.calls) == 2 and net.responses[-1].read_calls == 0


@pytest.mark.parametrize('delta', [{'ETag': '"changed"'}, {'Content-Range': 'bytes 0-29/999'},
    {'Content-Length': '999'}, {'Content-Encoding': 'gzip'}])
def test_changed_get_headers_stop_before_body(fixture, delta):
    net = Network(fixture, lambda r,n,method,h: r.headers.update(delta) if method == 'GET' else None)
    with pytest.raises(ValueError, match='identity206'):
        m.acquire(fixture['case'], fixture['index'], fixture['mapping'], requester=net)
    assert net.responses[-1].read_calls == 0


def test_redirected_url_refused_without_body(fixture):
    def change(r, n, method, headers):
        if method == 'GET': r.url = 'https://example.invalid/different'
    net = Network(fixture, change)
    with pytest.raises(ValueError): m.acquire(fixture['case'], fixture['index'], fixture['mapping'], requester=net)
    assert net.responses[-1].read_calls == 0


@pytest.mark.parametrize('kind', ['short', 'long', 'timeout'])
def test_partial_range_and_error_preserved_no_promotion(fixture, kind):
    def change(r, n, method, headers):
        if n == 5:
            if kind == 'short': r.body = r.body[:-1]
            if kind == 'long': r.body += b'X'
            if kind == 'timeout': r.fail_after = 37
    net = Network(fixture, change)
    with pytest.raises((ValueError, TimeoutError)):
        m.acquire(fixture['case'], fixture['index'], fixture['mapping'], requester=net)
    assert len(net.calls) == 5 and list((fixture['case']/'ranges').glob('*.bin'))
    failure = json.loads((fixture['case']/'acquisition-failure.json').read_text())
    assert failure['status'] == 'failed_no_automatic_replay' and failure['final_promoted'] is False
    assert len(list((fixture['case']/'ranges').glob('*-response.json'))) == 4


@pytest.mark.parametrize('field,fmt,delta', [(4,'H',21), (6,'H',1), (6,'H',8),
    (6,'H',0x800), (8,'H',0), (10,'H',12), (12,'H',23), (14,'I',1),
    (18,'I',1), (22,'I',1), (26,'H',0), (28,'H',4097)])
def test_unsupported_or_disagreeing_local_header_refused(fixture, field, fmt, delta):
    f = fixture
    raw = bytearray(f['archive'])
    struct.pack_into('<'+fmt, raw, f['content'][f['selected']]['offset']+field, delta)
    f['archive'] = bytes(raw)
    with pytest.raises(ValueError): invoke(f)
    assert not (f['case']/f['selected']).exists()


@pytest.mark.parametrize('kind', ['unknown', 'zip64', 'reserved', 'tag', 'length', 'truncated'])
def test_ntfs_extra_only_and_structural_bounds_refused(fixture, kind):
    f = fixture
    raw = bytearray(f['archive']); p = f['content'][f['selected']]['extra_offset']
    if kind in ('unknown', 'zip64'): struct.pack_into('<H', raw, p, 11 if kind == 'unknown' else 1)
    elif kind == 'reserved': raw[p+4] = 1
    elif kind == 'tag': struct.pack_into('<H', raw, p+8, 2)
    elif kind == 'length': struct.pack_into('<H', raw, p+10, 23)
    else: struct.pack_into('<H', raw, p+2, 33)
    f['archive'] = bytes(raw)
    with pytest.raises(ValueError, match='NTFS|extra'): invoke(f)


def test_local_filename_must_equal_frozen_selection(fixture):
    f = fixture
    raw = bytearray(f['archive']); raw[f['content'][f['selected']]['offset']+30] ^= 1
    f['archive'] = bytes(raw)
    with pytest.raises(ValueError, match='filename'): invoke(f)


def test_empty_local_extra_supported_without_inventing_central_local_equivalence(fixture, monkeypatch):
    f = fixture
    selected = next(e for e in f['value']['members'] if e['name'] == f['selected'])
    p = f['content'][f['selected']]['extra_offset']
    raw = bytearray(f['archive'])
    struct.pack_into('<H', raw, selected['local_header_offset']+28, 0)
    del raw[p:p+36]
    f['archive'] = bytes(raw)
    for entry in f['value']['members']:
        if entry['local_header_offset'] > selected['local_header_offset']:
            entry['local_header_offset'] -= 36
    f['value']['central_directory']['offset'] -= 36
    f['value']['archive_bytes'] = len(raw)
    monkeypatch.setattr(m, 'ARCHIVE_BYTES', len(raw))
    rewrite(f, monkeypatch)
    result, _ = invoke(f)
    assert Path(result['member']).read_bytes() == f['content'][f['selected']]['raw']
    assert json.loads((f['case']/'verified-local-header.json').read_text())['local_extra_field_ids'] == []


def test_successor_magic_prevents_wrong_boundary_or_hidden_descriptor(fixture):
    f = fixture
    proof = m.inputs(f['index'], f['mapping'])
    raw = bytearray(f['archive']); raw[proof['successor_offset']] ^= 1
    f['archive'] = bytes(raw)
    with pytest.raises(ValueError, match='successor signature'): invoke(f)


@pytest.mark.parametrize('field,value', [('flags',1), ('flags',8), ('flags',0x800), ('method',0),
    ('external_attributes',(stat.S_IFLNK|0o777)<<16), ('external_attributes',(stat.S_IFIFO|0o600)<<16),
    ('external_attributes',16), ('external_attributes',8), ('raw_bytes',m.MAX_RAW+1),
    ('compressed_bytes',m.MAX_COMPRESSED+1), ('extra_field_ids',[1])])
def test_no_supported_member_means_refusal_not_scope_or_size_fallback(fixture, monkeypatch, field, value):
    f = fixture
    for entry in f['value']['members']: entry[field] = value
    rewrite(f, monkeypatch)
    with pytest.raises(ValueError, match='No supported'): invoke(f)
    assert not f['case'].exists()


@pytest.mark.parametrize('kind', ['duplicate_name', 'duplicate_offset', 'roster', 'source_flag', 'mapping_path'])
def test_index_integrity_contract_refuses_malformed_frozen_metadata(fixture, monkeypatch, kind):
    f = fixture
    if kind == 'duplicate_name': f['value']['members'][1]['name'] = f['value']['members'][0]['name']
    if kind == 'duplicate_offset': f['value']['members'][1]['local_header_offset'] = f['value']['members'][0]['local_header_offset']
    if kind == 'roster': f['value']['members'].pop()
    if kind == 'source_flag': f['value']['scientific_result'] = True
    if kind == 'mapping_path': f['value']['mapping_workbook']['path'] += '.other'
    rewrite(f, monkeypatch)
    with pytest.raises(ValueError): invoke(f)


@pytest.mark.parametrize('which', ['index', 'mapping'])
def test_changed_source_bytes_refused_before_network(fixture, which):
    f = fixture; f[which].write_bytes(f[which].read_bytes()+b'changed')
    net = Network(f)
    with pytest.raises(ValueError, match='metadata bytes'):
        m.acquire(f['case'], f['index'], f['mapping'], requester=net)
    assert net.calls == []


@pytest.mark.parametrize('kind', ['trailing', 'incomplete', 'crc', 'raw_bound'])
def test_full_deflate_and_raw_bounds_cannot_be_weakened(fixture, tmp_path, kind):
    selected = dict(m.inputs(fixture['index'], fixture['mapping'])['selected'])
    source = fixture['content'][fixture['selected']]
    payload = source['payload']
    if kind == 'trailing': payload += b'X'; selected['compressed_bytes'] += 1
    if kind == 'incomplete': payload = payload[:-1]; selected['compressed_bytes'] -= 1
    if kind == 'crc': selected['crc32'] = '00000000'
    if kind == 'raw_bound': selected['raw_bytes'] -= 1
    p = tmp_path/'compressed'; p.write_bytes(payload)
    with pytest.raises(ValueError): m.inflate([p], tmp_path/'candidate', selected, time.monotonic()+30)
    assert (tmp_path/'candidate').exists()


def test_mid_acquisition_source_change_refused_before_promotion(fixture):
    f = fixture
    def change(r, n, method, headers):
        if n == 5: f['mapping'].write_bytes(f['mapping'].read_bytes()+b'changed')
    with pytest.raises(ValueError, match='metadata bytes'): invoke(f, change)
    assert (f['case']/'verified-track-candidate.partial').exists()
    assert not (f['case']/f['selected']).exists()


def test_candidate_change_after_verification_refused(fixture, monkeypatch):
    original = m.inflate
    def changed(chunks, candidate, selected, deadline):
        proof = original(chunks, candidate, selected, deadline)
        candidate.write_bytes(candidate.read_bytes()+b'changed')
        return proof
    monkeypatch.setattr(m, 'inflate', changed)
    with pytest.raises(ValueError, match='candidate changed'): invoke(fixture)
    assert not (fixture['case']/fixture['selected']).exists()


def test_existing_case_refused_no_ambiguity_replay(fixture):
    fixture['case'].mkdir()
    (fixture['case']/'sentinel').write_bytes(b'preserve')
    net = Network(fixture)
    with pytest.raises(ValueError, match='Fresh case'):
        m.acquire(fixture['case'], fixture['index'], fixture['mapping'], requester=net)
    assert net.calls == [] and (fixture['case']/'sentinel').read_bytes() == b'preserve'


def test_publication_collision_preserves_original_and_verified_candidate(fixture, monkeypatch):
    f = fixture
    original = m.exclusive
    def intent(path, value):
        original(path, value)
        if Path(path).name == 'promotion-intent.json': (f['case']/f['selected']).write_bytes(b'preserve existing')
    monkeypatch.setattr(m, 'exclusive', intent)
    with pytest.raises(FileExistsError): invoke(f)
    assert (f['case']/f['selected']).read_bytes() == b'preserve existing'
    assert (f['case']/'verified-track-candidate.partial').exists()


def test_outside_root_and_subordinate_symlink_refused(fixture, tmp_path):
    f = fixture
    with pytest.raises(ValueError): m.acquire(tmp_path/'outside', f['index'], f['mapping'], requester=Network(f))
    target = f['root']/'maintenance/target'; target.mkdir()
    alias = f['root']/'maintenance/alias'
    try: alias.symlink_to(target, target_is_directory=True)
    except OSError: pytest.skip('Windows symlink privilege unavailable; native suite must have zero skips')
    with pytest.raises(ValueError, match='symlink'): m.acquire(alias/'case', f['index'], f['mapping'], requester=Network(f))


def test_expired_inflation_deadline_refuses_without_parsing(fixture, tmp_path):
    selected = m.inputs(fixture['index'], fixture['mapping'])['selected']
    p = tmp_path/'chunk'; p.write_bytes(fixture['content'][fixture['selected']]['payload'])
    with pytest.raises(ValueError, match='deadline'): m.inflate([p], tmp_path/'candidate', selected, time.monotonic()-1)


def test_gateway_source_has_no_track_parser_or_scientific_imports():
    import ast
    tree = ast.parse(SOURCE.read_text())
    imports = {n.module.split('.')[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    imports |= {a.name.split('.')[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert not imports & {'pandas','numpy','scipy','sqlite3','torch','openpyxl','horsehair'}
