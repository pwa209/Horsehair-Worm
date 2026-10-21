import csv
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import zipfile

import pytest


SPEC = importlib.util.spec_from_file_location('secondary_audit', Path(__file__).with_name('audit-secondary-evidence-20261021.py'))
audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit)


def encoded(value):
    return json.dumps(value).encode()


def archive(parts):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w') as handle:
        for key, value in parts.items():
            handle.writestr(key, value)
    return stream.getvalue()


def workbook(rows, *, headers=None, strings=None, extra=None):
    body = ''.join('<row r="%s">%s</row>' % (r, ''.join(cells)) for r, cells in rows)
    parts = {'xl/workbook.xml': '<workbook xmlns="%s" xmlns:r="%s"><sheets><sheet name="Data" sheetId="1" r:id="rId1"/></sheets></workbook>' % (audit.M[1:-1], audit.R[1:-1]),
             'xl/_rels/workbook.xml.rels': '<Relationships><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>',
             'xl/worksheets/sheet1.xml': '<worksheet xmlns="%s"><sheetData>%s</sheetData></worksheet>' % (audit.M[1:-1], body)}
    if strings:
        parts['xl/sharedStrings.xml'] = '<sst xmlns="%s">%s</sst>' % (audit.M[1:-1], ''.join('<si><t>%s</t></si>' % s for s in strings))
    parts.update(extra or {})
    return archive(parts)


def textcell(coord, value):
    return '<c r="%s" t="inlineStr"><is><t>%s</t></is></c>' % (coord, value)


def numeric(coord, value):
    return '<c r="%s"><v>%s</v></c>' % (coord, value)


def csvraw(header, rows):
    stream = io.StringIO(newline='')
    writer = csv.writer(stream)
    writer.writerow(header); writer.writerows(rows)
    return stream.getvalue().encode()


@pytest.fixture
def source_case(tmp_path, monkeypatch):
    case = tmp_path / 'source'; case.mkdir()
    raw_files = {name: b'header\n' for name in audit.CSV_NAMES}
    raw_files['README.md'] = b'Table of contents for Activity.csv\n* Time: hours after exposure\n'
    for name in audit.DOCX:
        raw_files[name] = archive({'word/document.xml': '<w:document xmlns:w="%s"><w:tbl><w:tr><w:tc><w:p><w:r><w:t>Genotype</w:t></w:r></w:p></w:tc></w:tr></w:tbl></w:document>' % audit.W[1:-1]})
    monkeypatch.setattr(audit, 'DOCX', {n: (len(raw_files[n]), audit.sha(raw_files[n])) for n in audit.DOCX})
    links = lambda **kw: {k: {'href': v} for k, v in kw.items()}
    dataset = {'identifier': audit.DOI, 'curationStatus': 'Published', 'versionNumber': 1,
        '_links': links(self='/api/v2/datasets/doi%3A10.5061%2Fdryad.m37pvmd8r', **{'stash:version': '/api/v2/versions/272919'})}
    version = {'curationStatus': 'Published', 'versionNumber': 1,
        '_links': links(self='/api/v2/versions/272919', **{'stash:dataset': '/api/v2/datasets/doi%3A10.5061%2Fdryad.m37pvmd8r', 'stash:files': '/api/v2/versions/272919/files'})}
    files = []
    for index, name in enumerate(sorted(audit.CSV_NAMES | {'README.md'}), 100):
        algorithm = 'SHA-256' if index % 2 else 'MD5'
        files.append({'path': name, 'size': len(raw_files[name]), 'digestType': algorithm,
            'digest': hashlib.new(algorithm.lower().replace('-', ''), raw_files[name]).hexdigest(),
            '_links': links(self='/api/v2/files/' + str(index))})
    raw_files['dryad-dataset.json'] = encoded(dataset)
    raw_files['dryad-version.json'] = encoded(version)
    raw_files['dryad-files.json'] = encoded({'_embedded': {'stash:files': files}})
    urls = {'dryad-dataset.json': 'https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.m37pvmd8r',
        'dryad-version.json': 'https://datadryad.org/api/v2/versions/272919',
        'dryad-files.json': 'https://datadryad.org/api/v2/versions/272919/files'}
    urls.update({n: 'https://cdn.elifesciences.org/articles/85410/' + n for n in audit.DOCX})
    urls.update({f['path']: 'https://datadryad.org/downloads/file_stream/' + f['_links']['self']['href'].split('/')[-1] for f in files})
    manifest = {'status': 'complete_expected_public_sources', 'dryad_dataset': audit.DOI,
        'missing_dryad_files': [], 'scientific_gates_cleared': False, 'files': []}
    for name, raw in raw_files.items():
        (case / name).write_bytes(raw)
        manifest['files'].append({'name': name, 'bytes': len(raw), 'sha256': audit.sha(raw),
            'source_url': urls[name], 'integrity_basis': 'Mock exact metadata contract', 'scientific_gates_cleared': False})
    def save_manifest():
        raw = encoded(manifest); (case / 'manifest.json').write_bytes(raw)
        return audit.sha(raw)
    return case, manifest, save_manifest


def test_exact_provider_sha256_and_md5_contract(source_case):
    case, manifest, save = source_case
    raw, bindings, proof = audit.verify_source_case(case, save())
    assert len(raw) == len(bindings) == 13
    assert {v['provider']['digest_type'] for v in bindings.values() if 'provider' in v} == {'MD5', 'SHA-256'}
    assert proof['published_version_url'].endswith('/272919')


@pytest.mark.parametrize('mutation', ['foreign_doi', 'unpublished', 'version', 'route', 'digest', 'size', 'pagination', 'duplicate'])
def test_source_provider_binding_fails_closed(source_case, mutation):
    case, manifest, save = source_case
    name = 'dryad-dataset.json' if mutation in {'foreign_doi', 'unpublished'} else 'dryad-version.json' if mutation == 'version' else 'dryad-files.json'
    value = json.loads((case / name).read_bytes())
    if mutation == 'foreign_doi': value['identifier'] = 'doi:foreign'
    elif mutation == 'unpublished': value['curationStatus'] = 'Draft'
    elif mutation == 'version': value['_links']['self']['href'] = '/api/v2/versions/1'
    elif mutation == 'pagination': value['_links'] = {'next': {'href': 'more'}}
    else:
        item = value['_embedded']['stash:files'][0]
        if mutation == 'route': item['_links']['self']['href'] = '/api/v2/files/999'
        elif mutation == 'digest': item['digest'] = '0' * len(item['digest'])
        elif mutation == 'size': item['size'] += 1
        elif mutation == 'duplicate': value['_embedded']['stash:files'][1] = item
    raw = encoded(value); (case / name).write_bytes(raw)
    record = next(r for r in manifest['files'] if r['name'] == name)
    record.update(sha256=audit.sha(raw), bytes=len(raw))
    with pytest.raises(ValueError): audit.verify_source_case(case, save())


def test_transfer_hash_failure_precedes_metadata_parse(source_case):
    case, manifest, save = source_case
    digest = save()
    (case / 'Activity.csv').write_bytes(b'not the original')
    with pytest.raises(ValueError, match='binding failed'): audit.verify_source_case(case, digest)


def test_changed_or_incomplete_manifest_refused(source_case):
    case, manifest, save = source_case
    old = save(); manifest['files'].pop(); new = save()
    with pytest.raises(ValueError, match='binding failed'): audit.verify_source_case(case, old)
    with pytest.raises(ValueError, match='13-file'): audit.verify_source_case(case, new)


def test_fatal_selection_is_association_and_indiv_is_not_animal_key():
    raw = csvraw(['Indiv', 'Week', 'Subject', 'Time', 'Video', 'Day', 'Observation', 'deathday', 'TreatmentType', 'locomotion.no'], [
        ['I1_48_W1', 'W1', 'I1', '48', '1', '2', 'infected', 'DeadDay6', 'sporulating', '987654321'],
        ['I1_51_W1', 'W1', 'I1', '51', '2', '2', 'infected', 'DeadDay6', 'sporulating', '999999999'],
        ['C1_48_W1', 'W1', 'C1', '48', '1', '2', 'control', 'control', 'freeze-killed', 'NA'],
        ['I1_48_W2', 'W2', 'I1', '48', '1', '2', 'infected', 'DeadDay7', 'sporulating', '123456789']])
    report = audit.csv_structure(raw, 'Activity.csv'); design = report['activity_design']
    assert design['unique_candidate_subjects'] == 3
    assert design['subjects_with_repeated_observations'] == 1
    assert design['all_infected_label_rows_are_future_fatal_cases'] is True
    assert design['time_hours_values'] == [48.0, 51.0]
    assert design['observed_time_grid_gaps_hours'] == [3.0]
    assert design['HF1_route'] == 'conditional_infection_health_status_association'
    assert design['exposure_causal_estimand_available'] is False
    assert design['all_exposed_assignment_roster_proven'] is False
    assert report['missingness_by_column']['locomotion.no'] == 1
    assert '987654321' not in json.dumps(report)
    assert '123456789' not in json.dumps(report)
    assert report['outcome_values_exported'] is False


def test_no_fatal_selection_still_does_not_prove_exposure_registry():
    raw = csvraw(['Week', 'Subject', 'Time', 'Observation', 'deathday', 'TreatmentType'],
        [['1', 'I1', '48', 'infected', 'survived', 'exposed']])
    design = audit.csv_structure(raw, 'Activity.csv')['activity_design']
    assert not design['all_infected_label_rows_are_future_fatal_cases']
    assert not design['exposure_causal_estimand_available']


def test_repeated_clock_and_invalid_time_are_explicit():
    header = ['Week', 'Subject', 'Time', 'Video', 'Day', 'Observation', 'deathday', 'TreatmentType']
    row = ['1', 'C1', '48', '1', '2', 'control', 'control', 'freeze-killed']
    raw = csvraw(header, [row, row, ['1', 'C1', 'NaN', '2', '2', 'control', 'control', 'freeze-killed']])
    design = audit.csv_structure(raw, 'Activity.csv')['activity_design']
    assert design['subjects'][0]['repeated_time_video_day_keys'][0]['rows'] == 2
    assert design['invalid_time_csv_records'] == [4]


def test_csv_no_effect_values_even_other_assay_and_multiline_identity():
    raw = csvraw(['Observation', 'Treatment', 'matinglatency_sec', 'status'], [['A\nB', 'infected', '76543210', 'died']])
    report = audit.csv_structure(raw, 'MatingLatency.csv')
    assert report['metadata_records'][0]['physical_lines_1based'] == [2, 3]
    assert report['metadata_records'][0]['metadata_literals'] == {'Observation': 'A\nB', 'Treatment': 'infected'}
    assert '76543210' not in json.dumps(report) and 'died' not in json.dumps(report)


@pytest.mark.parametrize('raw', [b'A,A\n1,2\n', b'A,B\n1\n', b''])
def test_invalid_csv_schema_refused(raw):
    with pytest.raises(ValueError): audit.csv_structure(raw, 'other.csv')


def test_readme_only_exact_variable_definitions_not_result_prose():
    raw = b'The treatment increased activity by SECRET.\nTable of contents for Activity.csv\n* Time: original clock\n* deathday: exact eligible groups\n'
    report = audit.readme_metadata(raw)
    assert report['metadata_statements'][0] == {'source_line': 3, 'dataset_section': 'Activity.csv', 'literal': '* Time: original clock'}
    assert 'SECRET' not in json.dumps(report)


def test_xlsx_proteomic_values_never_exported_or_synthetic_trials():
    raw = workbook([(1, [textcell('A1', 'Protein IDs'), textcell('B1', 'LFQ intensity')]),
        (2, [textcell('A2', 'SECRET_PROTEIN'), numeric('B2', '987654321')])])
    report = audit.xlsx_structure(raw)
    assert report['sheets'][0]['proteomic_schema_hint']
    assert not report['individual_behavior_trial_schema_identified']
    assert report['individual_behavior_trials_available'] == 'not_established_by_structural_audit'
    assert report['CR1_admitted'] is False
    assert 'SECRET_PROTEIN' not in json.dumps(report)
    assert '987654321' not in json.dumps(report)


def test_xlsx_candidate_retains_only_trial_metadata_not_endpoint_values():
    raw = workbook([(1, [textcell('A1', 'Cricket ID'), textcell('B1', 'Treatment'), textcell('C1', 'Water entry')]),
        (2, [textcell('A2', 'CR001'), textcell('B2', 'Infected26dpi'), numeric('C2', '76543210')])])
    report = audit.xlsx_structure(raw)
    assert report['individual_behavior_trial_schema_identified']
    cells = report['sheets'][0]['metadata_records'][0]['cells']
    assert cells == [{'cell': 'A2', 'header_cell': 'A1', 'header_literal': 'Cricket ID', 'literal': 'CR001'},
        {'cell': 'B2', 'header_cell': 'B1', 'header_literal': 'Treatment', 'literal': 'Infected26dpi'}]
    assert '76543210' not in json.dumps(report)
    assert not report['original_eligible_trial_denominators_proven']


def test_shared_strings_and_formula_values_not_executed_or_exported():
    raw = workbook([(1, ['<c r="A1" t="s"><v>0</v></c>', '<c r="B1" t="s"><v>1</v></c>']),
        (2, [textcell('A2', 'P1'), '<c r="B2"><f>2+2</f><v>77777777</v></c>'])], strings=['Protein IDs', 'Intensity'])
    report = audit.xlsx_structure(raw)
    assert report['sheets'][0]['formula_cells_count'] == 1
    assert report['formulas_executed'] is False
    assert '77777777' not in json.dumps(report) and '2+2' not in json.dumps(report)


def test_external_sheet_and_entity_declaration_refused():
    raw = workbook([], extra={'xl/_rels/workbook.xml.rels': '<Relationships><Relationship Id="rId1" Type="x/worksheet" Target="https://example.com/data" TargetMode="External"/></Relationships>'})
    with pytest.raises(ValueError, match='External'): audit.xlsx_structure(raw)
    raw = workbook([], extra={'xl/workbook.xml': '<!DOCTYPE x [<!ENTITY a "unsafe">]><workbook/>'})
    with pytest.raises(ValueError, match='declarations'): audit.xlsx_structure(raw)


def test_finite_cells_deadline_and_unsafe_archive_fail_closed(monkeypatch):
    raw = workbook([(1, [textcell('A1', 'Protein'), textcell('B1', 'Intensity')])])
    monkeypatch.setattr(audit, 'MAX_CELLS', 1)
    with pytest.raises(ValueError, match='cell bound'): audit.xlsx_structure(raw)
    def expired(): raise TimeoutError('deadline')
    with pytest.raises(TimeoutError): audit.xlsx_structure(raw, expired)
    with pytest.raises(ValueError, match='Unsafe'): audit.xlsx_structure(archive({'../evil': 'x'}))


def test_docx_stock_discrepancy_preserves_literal_no_catalog_repair():
    body = '<w:document xmlns:w="%s"><w:tbl>' % audit.W[1:-1]
    for row in [['Genotype', 'Source'], ['RNAi-Lk', 'BDSC:25936'], ['RNAi-LkR', 'BDSC:25836']]:
        body += '<w:tr>' + ''.join('<w:tc><w:p><w:r><w:t>%s</w:t></w:r></w:p></w:tc>' % v for v in row) + '</w:tr>'
    raw = archive({'word/document.xml': body + '</w:tbl></w:document>'})
    report = audit.docx_structure(raw, 'supp1.docx')
    assert report['literal_25936_25836_rows'][0]['row_literals'] == ['RNAi-Lk', 'BDSC:25936']
    assert report['literal_25936_25836_rows'][1]['row_literals'] == ['RNAi-LkR', 'BDSC:25836']
    assert not report['stock_gene_identity_resolved'] and not report['raw_animal_ROI_join_recovered']


def test_immutable_output_and_size_bound(tmp_path):
    path = tmp_path / 'report.json'
    proof = audit.exclusive_json(path, {'finite': 1})
    original = path.read_bytes()
    assert proof['sha256'] == audit.sha(original)
    with pytest.raises(FileExistsError): audit.exclusive_json(path, {'different': 2})
    assert path.read_bytes() == original
    with pytest.raises(ValueError, match='output bound'): audit.exclusive_json(tmp_path / 'too-big.json', {'x': 'long'}, 2)
    assert not (tmp_path / 'too-big.json').exists()


@pytest.mark.parametrize('value', ['{"x":1,"x":2}', '{"x":NaN}', '{"x":1e10000}'])
def test_invalid_json_refused(value):
    with pytest.raises(ValueError): audit.strict_json(value)


def test_missing_data_spec_never_claims_contact_or_recovered_gates():
    spec = audit.missing_data_spec()
    assert spec['no_outbound_contact_performed'] is True
    assert any('survivors' in v for v in spec['housefly_HF1'])
    assert any('denominators' in v for v in spec['cricket_CR1'])


def test_metadata_formula_is_diagnostic_not_evaluated():
    raw = workbook([(1, [textcell('A1', 'Cricket ID'), textcell('B1', 'Treatment')]),
        (2, ['<c r="A2" t="str"><f>SECRET_FORMULA</f><v>SECRET_CACHED</v></c>', textcell('B2', 'control')])])
    report = audit.xlsx_structure(raw)
    diagnostic = report['sheets'][0]['metadata_formula_cells_not_evaluated'][0]
    assert diagnostic['cell'] == 'A2' and diagnostic['header_cell'] == 'A1'
    assert diagnostic['formula_sha256'] == audit.sha(b'SECRET_FORMULA')
    assert 'SECRET_FORMULA' not in json.dumps(report) and 'SECRET_CACHED' not in json.dumps(report)


@pytest.mark.parametrize('status', ['validated_publisher_xlsx', 'validated_publisher_workbook'])
def test_both_exact_cricket_manifest_contracts(tmp_path, monkeypatch, status):
    raw = b'z' * 4650070
    monkeypatch.setattr(audit, 'CRICKET_SHA', audit.sha(raw))
    (tmp_path / 'pone.0332641.s014.xlsx').write_bytes(raw)
    manifest = {'status': status, 'bytes': 4650070, 'sha256': audit.sha(raw),
        'zip_crc_verified': True, 'scientific_gates_cleared': False,
        'integrity_basis': 'First observed official publisher SHA; not independent checksum'}
    encoded_manifest = encoded(manifest); (tmp_path / 'manifest.json').write_bytes(encoded_manifest)
    result, proof = audit.verify_cricket_case(tmp_path, audit.sha(encoded_manifest))
    assert result == raw and proof['acquisition_manifest_sha256'] == audit.sha(encoded_manifest)
    assert not proof['independent_provider_checksum_available']
    (tmp_path / 'pone.0332641.s014.xlsx').write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='binding failed'): audit.verify_cricket_case(tmp_path, audit.sha(encoded_manifest))


def test_no_table_parse_before_cricket_verification(tmp_path, monkeypatch):
    prereg = tmp_path / 'protocol.json'; prereg.write_bytes(b'{}')
    monkeypatch.setattr(audit, 'verify_source_case', lambda *a: ({}, {}, {}))
    def reject(*a): raise ValueError('cricket unverified')
    monkeypatch.setattr(audit, 'verify_cricket_case', reject)
    def forbidden(*a): pytest.fail('No table may be parsed before all original hashes pass')
    monkeypatch.setattr(audit, 'csv_structure', forbidden)
    with pytest.raises(ValueError, match='cricket unverified'):
        audit.audit_sources(tmp_path, 'x', tmp_path, 'x', prereg, audit.sha(b'{}'))


def test_unrelated_tables_cannot_be_joined_into_behavior_trial_schema():
    raw = workbook([(1, [textcell('A1', 'Cricket ID'), textcell('B1', 'Group')]),
        (2, [textcell('A2', 'C001'), textcell('B2', 'control')]),
        (4, [textcell('A4', 'Water entry'), textcell('B4', 'P value')])])
    report = audit.xlsx_structure(raw)
    assert report['sheets'][0]['explicit_individual_identity_header']
    assert report['sheets'][0]['behavioral_endpoint_header_candidate']
    assert not report['individual_behavior_trial_schema_identified']


@pytest.mark.parametrize('hints', [['Protein IDs', 'P value'], ['Protein IDs']])
def test_header_candidate_cannot_export_unclassified_string_outcome(hints):
    cells = [textcell(chr(65+i) + '1', value) for i, value in enumerate(hints + ['SECRET_EFFECT_VALUE'])]
    report = audit.xlsx_structure(workbook([(1, cells)]))
    header = report['sheets'][0]['header_candidates'][0]
    assert [c['literal'] for c in header['cells']] == hints
    assert 'SECRET_EFFECT_VALUE' not in json.dumps(report)
    assert header['unclassified_text_cells_sha256_only'][0]['sha256'] == audit.sha(b'SECRET_EFFECT_VALUE')
    assert not header['all_candidate_row_text_literals_exported']
