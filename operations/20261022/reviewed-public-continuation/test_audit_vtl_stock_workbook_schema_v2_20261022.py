"""Synthetic OOXML and source receipts only; no real workbook values."""
from pathlib import Path
import copy
import hashlib
import importlib.util
import io
import json
import os
import time
import zipfile

import pytest


spec = importlib.util.spec_from_file_location('stock_audit_v2', Path(__file__).with_name('audit-vtl-stock-workbook-schema-v2-20261022.py'))
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)


def package(*, cells=None, relationships=None, extra=None, shared=None, dimension='A1:C24', sheet_attrs=''):
    cells = cells or '<row r="1"><c r="A1" t="inlineStr"><is><t>Stock ID</t></is></c><c r="B1" t="inlineStr"><is><t>Gene ID</t></is></c><c r="C1" t="inlineStr"><is><t>Control</t></is></c></row><row r="2"><c r="A2"><v>12345</v></c><c r="B2" t="inlineStr"><is><t>SECRET_GENE_OR_OUTCOME</t></is></c><c r="C2"><f>1+2</f><v>3</v></c></row>'
    relationships = relationships or f'<Relationships xmlns="{m.REL_NS}"><Relationship Id="rId1" Type="{m.DOC_REL_NS}/worksheet" Target="worksheets/sheet1.xml"/></Relationships>'
    parts = {
        'xl/workbook.xml': f'<workbook xmlns="{m.NS}" xmlns:r="{m.DOC_REL_NS}"><sheets><sheet name="Stock information" sheetId="1" r:id="rId1" {sheet_attrs}/></sheets></workbook>',
        'xl/_rels/workbook.xml.rels': relationships,
        'xl/worksheets/sheet1.xml': f'<worksheet xmlns="{m.NS}"><dimension ref="{dimension}"/><sheetData>{cells}</sheetData></worksheet>',
    }
    if shared is not None: parts['xl/sharedStrings.xml'] = shared
    parts.update(extra or {})
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in parts.items():
            if '\\' in name:
                item = zipfile.ZipInfo('placeholder'); item.filename = item.orig_filename = name
                archive.writestr(item, value)
            else: archive.writestr(name, value)
    return stream.getvalue()


def audit(raw): return m.member_inventory(raw, deadline=time.monotonic()+30)


def print_package(*, content=None, print_rel=None, binary=None, extra=None):
    if content is None:
        content = f'<Types xmlns="{m.CONTENT_TYPE_NS}"><Default Extension="bin" ContentType="{m.PRINT_CONTENT_TYPE}"/></Types>'
    if print_rel is None:
        print_rel = f'<Relationships xmlns="{m.REL_NS}"><Relationship Id="rId1" Type="{m.DOC_REL_NS}/printerSettings" Target="../printerSettings/printerSettings1.bin"/></Relationships>'
    parts = {'[Content_Types].xml': content, m.PRINT_RELATIONSHIPS: print_rel,
             m.PRINT_PART: b'OPAQUE_PRINT_BINARY_NOT_EXPORTED'.ljust(m.PRINT_DECLARED_BYTES, b'x') if binary is None else binary}
    parts.update(extra or {})
    return package(extra={name: value for name, value in parts.items() if value is not False})


def test_exact_source_specific_print_metadata_is_ignored_without_opening_binary(monkeypatch):
    raw = print_package()
    opened = []; original = zipfile.ZipFile.open
    def guarded(archive, name, *args, **kwargs):
        actual = name.filename if isinstance(name, zipfile.ZipInfo) else name
        assert actual != m.PRINT_PART, 'Opaque print binary was opened'
        opened.append(actual)
        return original(archive, name, *args, **kwargs)
    monkeypatch.setattr(zipfile.ZipFile, 'open', guarded)
    value = audit(raw); row = value['ignored_opaque_print_metadata'][0]
    assert row['member'] == m.PRINT_PART and row['declared_uncompressed_bytes'] == 220
    assert row['content_type_literal'] == m.PRINT_CONTENT_TYPE and row['relationship_member'] == m.PRINT_RELATIONSHIPS
    assert len(row['zip_crc32_metadata']) == 8 and len(row['relationship_xml_sha256']) == 64
    assert row['ignored_print_metadata'] is True and row['advertised_role_independently_verified'] is False
    assert all(row[key] is False for key in ('binary_member_opened', 'binary_payload_parsed', 'binary_payload_exported'))
    assert value['opaque_binary_members_opened'] is False and m.PRINT_PART not in opened
    text = json.dumps(value)
    assert 'OPAQUE_PRINT_BINARY_NOT_EXPORTED' not in text and 'SECRET_GENE_OR_OUTCOME' not in text
    assert all(value[key] is False for key in m.FALSE_FLAGS)


@pytest.mark.parametrize('change', ['absent', 'missing_bin', 'wrong_mime', 'duplicate_default',
    'override', 'encoded_override', 'unknown_node', 'utf16'])
def test_opaque_print_requires_exact_unambiguous_plain_content_type(change):
    default = f'<Default Extension="bin" ContentType="{m.PRINT_CONTENT_TYPE}"/>'
    body = default
    if change == 'missing_bin': body = '<Default Extension="xml" ContentType="application/xml"/>'
    if change == 'wrong_mime': body = default.replace(m.PRINT_CONTENT_TYPE, 'application/octet-stream')
    if change == 'duplicate_default': body += default
    if change == 'override': body += f'<Override PartName="/{m.PRINT_PART}" ContentType="application/octet-stream"/>'
    if change == 'encoded_override': body += f'<Override PartName="/xl/%70rinterSettings/printerSettings1.bin" ContentType="{m.PRINT_CONTENT_TYPE}"/>'
    if change == 'unknown_node': body += '<Unknown/>'
    raw = f'<Types xmlns="{m.CONTENT_TYPE_NS}">{body}</Types>'
    if change == 'absent': raw = False
    if change == 'utf16': raw = raw.encode('utf-16')
    with pytest.raises(ValueError): audit(print_package(content=raw))


@pytest.mark.parametrize('change', ['absent', 'wrong_target', 'wrong_type', 'duplicate', 'sheet2',
    'wrong_id', 'external', 'encoded_target', 'unknown_tag', 'target_mode_internal'])
def test_opaque_print_requires_exact_single_source_sheet_relationship(change):
    attributes = f'Id="rId1" Type="{m.DOC_REL_NS}/printerSettings" Target="../printerSettings/printerSettings1.bin"'
    if change == 'wrong_target': attributes = attributes.replace('printerSettings1.bin', 'printerSettings2.bin')
    if change == 'wrong_type': attributes = attributes.replace('/printerSettings"', '/oleObject"')
    if change == 'wrong_id': attributes = attributes.replace('rId1', 'rId2')
    if change == 'external': attributes += ' TargetMode="External"'
    if change == 'target_mode_internal': attributes += ' TargetMode="Internal"'
    if change == 'encoded_target': attributes = attributes.replace('../printerSettings/', '../%70rinterSettings/')
    tag = 'Other' if change == 'unknown_tag' else 'Relationship'
    body = f'<{tag} {attributes}/>'
    if change == 'duplicate': body += body
    raw = f'<Relationships xmlns="{m.REL_NS}">{body}</Relationships>'
    extra = None
    if change == 'absent': raw = False
    if change == 'sheet2': extra = {m.PRINT_RELATIONSHIPS: False, 'xl/worksheets/_rels/sheet2.xml.rels': raw}
    with pytest.raises(ValueError): audit(print_package(print_rel=raw, extra=extra))


@pytest.mark.parametrize('length', [0, 219, 221, 4097, 8_000_001])
def test_opaque_print_is_bounded_to_the_observed_source_part_size(length):
    with pytest.raises(ValueError): audit(print_package(binary=b'x'*length))


@pytest.mark.parametrize('name', ['xl/printerSettings/printerSettings2.bin', 'xl/embeddings/object.bin',
    'xl/vbaProject.bin', 'xl/unknown.bin'])
def test_admitted_print_part_does_not_allow_any_additional_binary(name):
    with pytest.raises(ValueError): audit(print_package(extra={name: b'NEVER_OPENED'}))


def test_legacy_nonbinary_workbook_retains_an_explicit_empty_ignored_roster():
    value = audit(package())
    assert value['ignored_opaque_print_metadata'] == [] and value['opaque_binary_members_opened'] is False


def test_recognized_headers_only_unknown_numeric_formula_and_body_withheld():
    value = audit(package()); rendered = json.dumps(value)
    sheet = value['worksheets'][0]
    assert sheet['sheet_name_literal'] == 'Stock information'
    assert sheet['candidate_header_identified'] is True
    assert sheet['declared_dimension'] == 'A1:C24'
    assert sheet['maximum_row_index'] == 2 and sheet['maximum_column_index'] == 3
    assert [r['label_literal'] for r in sheet['header_rows'][0]['recognized_header_cells']] == ['Stock ID', 'Gene ID', 'Control']
    assert 'SECRET_GENE_OR_OUTCOME' not in rendered and '12345' not in rendered and '1+2' not in rendered
    assert all(value[k] is False and sheet[k] is False for k in m.FALSE_FLAGS)
    assert sheet['cell_type_counts']['formula'] == 1
    assert all(c['literal_exported'] is False for c in sheet['header_rows'][1]['unclassified_cells'])


def test_shared_strings_only_header_words_exported():
    raw = package(cells='<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c><c r="C1" t="s"><v>2</v></c></row>',
        shared=f'<sst xmlns="{m.NS}"><si><t>Gene name</t></si><si><r><t>Stock </t></r><r><t>number</t></r></si><si><t>HIDDEN_BODY_MAGNITUDE_999</t></si></sst>')
    value = audit(raw)
    assert value['worksheets'][0]['candidate_header_identified'] is True
    assert 'HIDDEN_BODY_MAGNITUDE_999' not in json.dumps(value)


def test_single_hint_is_ambiguous_not_a_header():
    value = audit(package(cells='<row r="1"><c r="A1" t="inlineStr"><is><t>Control</t></is></c><c r="B1" t="inlineStr"><is><t>Control</t></is></c><c r="C1" t="inlineStr"><is><t>0.432_SECRET</t></is></c></row>'))
    sheet = value['worksheets'][0]
    assert sheet['candidate_header_identified'] is False and sheet['unfamiliar_or_ambiguous_headers_require_review'] is True
    assert '0.432_SECRET' not in json.dumps(value)
    assert 'Control' not in json.dumps(value) and not sheet['header_rows'][0]['recognized_header_cells']


def test_no_body_string_or_formula_export_after_header_bound():
    cells = '<row r="21"><c r="A21" t="inlineStr"><is><t>Genotype</t></is></c><c r="B21"><f>PRIVATE_FORMULA</f><v>812</v></c></row>'
    value = audit(package(cells=cells)); text = json.dumps(value)
    assert not value['worksheets'][0]['header_rows']
    assert 'Genotype' not in text and 'PRIVATE_FORMULA' not in text and '812' not in text


@pytest.mark.parametrize('cells', [
    '<row r="1"><c r="A2"><v>1</v></c></row>',
    '<row r="1"><c r="A1"><v>1</v></c><c r="A1"><v>2</v></c></row>',
    '<row r="0"><c r="A0"><v>1</v></c></row>',
    '<row r="100001"><c r="A100001"><v>1</v></c></row>',
    '<row r="1"><c r="ZZZ1"><v>1</v></c></row>',
    '<row r="1"><c r="A1" t="s"><v>999</v></c></row>',
    '<row r="1"><c r="A1"><v>1</v></c></row><row r="1"><c r="B1"><v>2</v></c></row>',
])
def test_ambiguous_or_out_of_bounds_sheet_structure_refused(cells):
    with pytest.raises(ValueError): audit(package(cells=cells))


@pytest.mark.parametrize('relationship', [
    f'<Relationship Id="rId1" Target="https://example.com/data" TargetMode="External"/>',
    '<Relationship Id="rId1" Target="../sheet.xml"/>',
    '<Relationship Id="rId1" Target="/foreign/worksheets/sheet1.xml"/>',
    '<Relationship Id="rId1" Target="worksheets/sheet1.xml"/><Relationship Id="rId1" Target="worksheets/sheet2.xml"/>',
])
def test_unsafe_relationships_refused(relationship):
    with pytest.raises(ValueError): audit(package(relationships=f'<Relationships xmlns="{m.REL_NS}">{relationship}</Relationships>'))


def test_external_sheet_relationship_refused_even_without_header_use():
    raw = package(extra={'xl/worksheets/_rels/sheet1.xml.rels': f'<Relationships xmlns="{m.REL_NS}"><Relationship Id="rId1" Target="https://example.com" TargetMode="External"/></Relationships>'})
    with pytest.raises(ValueError, match='External'): audit(raw)


def test_standard_package_rooted_internal_sheet_relationship_supported():
    raw = package(relationships=f'<Relationships xmlns="{m.REL_NS}"><Relationship Id="rId1" Target="/xl/worksheets/sheet1.xml"/></Relationships>')
    assert audit(raw)['worksheets'][0]['candidate_header_identified'] is True


@pytest.mark.parametrize('extra', [
    {'../evil.xml': '<x/>'}, {'/evil.xml': '<x/>'}, {'xl\\evil.xml': '<x/>'},
    {'xl/vbaProject.bin': b'no macros'}, {'xl/embeddings/object.bin': b'not opened'},
    {'xl/oversized.xml': b'x'*(m.MAX_XML+1)},
])
def test_unsafe_or_oversized_archive_refused(extra):
    with pytest.raises(ValueError): audit(package(extra=extra))


def test_entity_and_doctype_refused():
    raw = package(extra={'xl/sharedStrings.xml': '<!DOCTYPE x [<!ENTITY secret "private">]><sst/>'})
    with pytest.raises(ValueError, match='OOXML'): audit(raw)


def test_encoded_doctype_cannot_bypass_plain_xml_requirement():
    raw = package(extra={'xl/sharedStrings.xml': '<!DOCTYPE x [<!ENTITY secret "private">]><sst/>'.encode('utf-16')})
    with pytest.raises(ValueError, match='UTF8'): audit(raw)


def test_duplicate_archive_members_refused():
    stream = io.BytesIO(package())
    with zipfile.ZipFile(stream, 'a') as archive: archive.writestr('xl/workbook.xml', '<workbook/>')
    with pytest.raises(ValueError, match='unique'): audit(stream.getvalue())


def test_special_archive_member_refused():
    stream = io.BytesIO(package())
    with zipfile.ZipFile(stream, 'a') as archive:
        item = zipfile.ZipInfo('symlink'); item.create_system = 3; item.external_attr = 0o120777 << 16
        archive.writestr(item, 'target')
    with pytest.raises(ValueError, match='Special'): audit(stream.getvalue())


def test_deadline_refusal_and_determinism():
    raw = package()
    assert audit(raw) == audit(raw)
    with pytest.raises(ValueError, match='deadline'): m.member_inventory(raw, deadline=time.monotonic()-1)


def provider_sources(monkeypatch):
    raw = package(); monkeypatch.setattr(m, 'SOURCE_SHA', m.sha(raw)); monkeypatch.setattr(m, 'SOURCE_BYTES', len(raw))
    monkeypatch.setattr(m, 'SOURCE_MD5', hashlib.md5(raw).hexdigest())
    monkeypatch.setattr(m, 'S3', 's3://pmc-oa-opendata/PMC11063160.1/'+m.NAME+'?md5='+m.SOURCE_MD5)
    metadata = {'pmcid': 'PMC11063160', 'version': 1, 'doi': '10.1038/s41467-024-48005-w',
        'is_retracted': False, 'is_manuscript': False, 'is_pmc_openaccess': True, 'license_code': 'CC BY', 'media_urls': [m.S3]}
    transfer = {'status': 'provider_md5_verified_official_stock_workbook_acquired', 'path': str(m.SOURCE),
        'sha256': m.SOURCE_SHA, 'bytes': m.SOURCE_BYTES, 'actual_md5': m.SOURCE_MD5,
        'provider_md5_verified': True, 'publisher_version_verified': True, 'url': m.URL,
        'provider_metadata': {'path': str(m.METADATA), 'sha256': m.METADATA_SHA},
        'scientific_result': False, 'workbook_values_inspected': False, 'author_contact': False,
        'transfer': {'returncode': 0, 'stdout': '200|'+m.URL+'|'+str(m.SOURCE_BYTES)}}
    return metadata, transfer, raw


def test_provider_metadata_checksum_receipt_and_actual_bytes_all_bound(monkeypatch):
    metadata, transfer, raw = provider_sources(monkeypatch)
    value = m.source_bindings(json.dumps(metadata).encode(), json.dumps(transfer).encode(), raw)
    assert value['provider_checksum_verified'] is True
    assert value['workbook']['provider_md5'] == hashlib.md5(raw).hexdigest()


@pytest.mark.parametrize('change', ['version', 'license', 'manuscript', 'retracted', 'md5_url', 'url', 'md5',
    'bool_bytes', 'bool_return', 'metadata_sha', 'values_read', 'author', 'actual_raw'])
def test_wrong_source_version_checksum_or_acquisition_refused(monkeypatch, change):
    metadata, transfer, raw = provider_sources(monkeypatch)
    if change == 'version': metadata['version'] = True
    if change == 'license': metadata['license_code'] = 'unknown'
    if change == 'manuscript': metadata['is_manuscript'] = True
    if change == 'retracted': metadata['is_retracted'] = True
    if change == 'md5_url': metadata['media_urls'] = []
    if change == 'url': transfer['url'] += '?different'
    if change == 'md5': transfer['actual_md5'] = '0'*32
    if change == 'bool_bytes': transfer['bytes'] = True
    if change == 'bool_return': transfer['transfer']['returncode'] = False
    if change == 'metadata_sha': transfer['provider_metadata']['sha256'] = '0'*64
    if change == 'values_read': transfer['workbook_values_inspected'] = True
    if change == 'author': transfer['author_contact'] = True
    if change == 'actual_raw': raw += b'changed'
    with pytest.raises(ValueError): m.source_bindings(json.dumps(metadata).encode(), json.dumps(transfer).encode(), raw)


@pytest.mark.parametrize('raw', [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}'])
def test_strict_json_duplicate_and_all_nonfinite_refused(raw):
    with pytest.raises(ValueError): m.strict_json(raw)


def test_main_requires_native_finite_context_before_sources(tmp_path, monkeypatch):
    monkeypatch.delenv('SLURM_CPUS_PER_TASK', raising=False)
    monkeypatch.setattr(m, 'execute', lambda **kwargs: pytest.fail('No source read before native context'))
    with pytest.raises(ValueError, match='CPU'): m.main(['--output', str(tmp_path/'report.json')])
    monkeypatch.setenv('SLURM_CPUS_PER_TASK', '1'); monkeypatch.setenv('HHW_GPU_COUNT', '0')
    monkeypatch.setenv('SLURM_JOB_END_TIME', 'nan')
    with pytest.raises(ValueError, match='margin'): m.main(['--output', str(tmp_path/'report.json')])


def test_bounded_source_and_fresh_ordinary_output(tmp_path, monkeypatch):
    root = tmp_path.resolve(); area = root/'maintenance'; area.mkdir(); monkeypatch.setattr(m, 'ROOT', root)
    source = area/'metadata.json'; source.write_bytes(b'{}')
    assert m.bounded(source, m.sha(b'{}'), 2) == b'{}'
    with pytest.raises(ValueError): m.bounded(source, m.sha(b'{}'), 1)
    with pytest.raises(ValueError): m.bounded(source, '0'*64, 2)
    with pytest.raises(ValueError): m.ordinary(source, fresh=True)
    with pytest.raises(ValueError): m.ordinary(root/'foreign.json', fresh=True)
