"""Outcome-blind, finite structural audit of explicitly pinned public originals.

This private support program never calculates or exports behavioural/proteomic
outcome values. Identity, treatment, ascertainment and clock fields remain exact
source literals. A completed audit is not a scientific admission receipt.
"""
from pathlib import Path, PurePosixPath
from collections import Counter, defaultdict
from datetime import datetime, timezone
import argparse
import csv
import hashlib
import io
import json
import math
import os
import posixpath
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path('/scratch/pwa209/horsehair-worm')
SOURCE_CASE = ROOT / 'maintenance/study-af-source-relay-20261021'
CRICKET_CASE = ROOT / 'maintenance/study-af-cricket-relay-20261021'
CRICKET_MANIFEST_SHA = '35741c6c8de42392e6c96fcf62500dfcf5d24ced1bcbb2f9757185336a8875c0'
SOURCE_MANIFEST_SHA = '96fa66e5be1993e9223d085f80252775c7919a1f3a66e956454781c786663a5c'
PREREG_SHA = '954548eb32d94918df597fef45fab23a5e162ccafbcc2432841f3b6c85da9220'
CRICKET_SHA = '16dc3487dabb72337c97dbb415209c1f254e139c428dc8b4b694ecd557cae9cc'
DOI = 'doi:10.5061/dryad.m37pvmd8r'
DOCX = {
 'elife-85410-supp1-v2.docx': (32596, '00ce4dccbc5776e1e95205d3061a03c7aba28c2530c7ab42c9e4b0fdcf462ebc'),
 'elife-85410-supp2-v2.docx': (27200, '0852955493c0db1e6d1f337533d92d43c38363f2d161b00bff7ceb7133b8b237')}
CSV_NAMES = {'Activity.csv', 'FemaleChoice.csv', 'Male_Housefly_Weight.csv',
 'Mating_Attempts_Duration.csv', 'MatingLatency.csv', 'SpermViability.csv', 'Virulence.csv'}
API_NAMES = {'dryad-dataset.json', 'dryad-version.json', 'dryad-files.json'}
EXPECTED_NAMES = set(DOCX) | CSV_NAMES | API_NAMES | {'README.md'}
MAX_INPUT_BYTES = 10_000_000
MAX_EXPANDED_BYTES = 150_000_000
MAX_MEMBER_BYTES = 40_000_000
MAX_CELLS = 1_000_000
MAX_CSV_ROWS = 100_000
MAX_ROWS = 200_000
MAX_OUTPUT_BYTES = 20_000_000
MAX_SECONDS = 3000
MISSING = {'', 'NA', 'N/A', 'NaN', 'nan', 'NULL', 'null'}
M = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
W = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
R = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}'


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'Duplicate JSON key')
            result[key] = value
        return result
    def constant(value):
        raise ValueError('Nonfinite JSON number')
    def finite_float(value):
        result = float(value)
        require(math.isfinite(result), 'Nonfinite JSON number')
        return result
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant, parse_float=finite_float)


def read_bound(path, expected_sha, maximum=MAX_INPUT_BYTES):
    path = Path(path)
    require(not path.is_symlink() and path.is_file(), 'Missing or symlink source')
    require(re.fullmatch('[0-9a-f]{64}', expected_sha or '') is not None, 'Exact SHA256 required')
    with path.open('rb') as handle:
        raw = handle.read(maximum + 1)
    require(len(raw) <= maximum and sha(raw) == expected_sha, 'Source size/SHA binding failed: ' + path.name)
    return raw


def source_link(obj, relation):
    href = obj.get('_links', {}).get(relation, {}).get('href')
    require(isinstance(href, str), 'Missing provider relation: ' + relation)
    url = urllib.parse.urljoin('https://datadryad.org', href)
    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme == 'https' and parsed.netloc == 'datadryad.org' and not parsed.query
            and not parsed.fragment, 'Foreign provider relation')
    return url


def verify_source_case(case, manifest_sha, checkpoint=lambda: None):
    case = Path(case)
    manifest_raw = read_bound(case / 'manifest.json', manifest_sha, 100_000)
    manifest = strict_json(manifest_raw)
    require(manifest.get('status') == 'complete_expected_public_sources'
            and manifest.get('dryad_dataset') == DOI
            and manifest.get('missing_dryad_files') == []
            and manifest.get('scientific_gates_cleared') is False, 'Incomplete/mismatched source manifest')
    records = manifest.get('files')
    require(isinstance(records, list) and len(records) == 13, 'Exact 13-file source roster required')
    names = [r.get('name') for r in records]
    require(len(set(names)) == 13 and set(names) == EXPECTED_NAMES, 'Exact unique original names required')
    raws, bindings, total = {}, {}, 0
    # All transferred bytes are verified before any table or CSV inspection.
    for record in records:
        checkpoint()
        name = record['name']
        require(type(record.get('bytes')) is int and 0 < record['bytes'] <= MAX_INPUT_BYTES,
                'Malformed byte count')
        raw = read_bound(case / name, record.get('sha256'))
        require(len(raw) == record['bytes'], 'Transferred original byte count mismatch')
        total += len(raw)
        require(total <= MAX_INPUT_BYTES, 'Source aggregate input bound exceeded')
        require(isinstance(record.get('integrity_basis'), str) and record['integrity_basis'],
                'Integrity authority must be stated')
        require(record.get('scientific_gates_cleared') is False, 'Source cannot grant scientific clearance')
        if name in DOCX:
            require((len(raw), sha(raw)) == DOCX[name], 'Publisher original differs from pinned source')
            require(record.get('source_url') == 'https://cdn.elifesciences.org/articles/85410/' + name,
                    'Publisher original URL mismatch')
        raws[name] = raw
        bindings[name] = {k: record[k] for k in ['name', 'bytes', 'sha256', 'source_url', 'integrity_basis']}
    dataset, version, roster = (strict_json(raws[n]) for n in ['dryad-dataset.json', 'dryad-version.json', 'dryad-files.json'])
    require(dataset.get('identifier') == DOI and dataset.get('curationStatus') == 'Published',
            'Exact published DOI required')
    version_url = source_link(dataset, 'stash:version')
    require(re.fullmatch('https://datadryad.org/api/v2/versions/[1-9][0-9]*', version_url), 'Exact provider version required')
    require(source_link(version, 'self') == version_url and version.get('curationStatus') == 'Published'
            and version.get('versionNumber') == dataset.get('versionNumber')
            and source_link(version, 'stash:dataset') == source_link(dataset, 'self'), 'Published version identity mismatch')
    files_url = source_link(version, 'stash:files')
    require(files_url == version_url + '/files' and not roster.get('_links', {}).get('next'), 'Incomplete provider listing')
    expected_urls = {'dryad-dataset.json': source_link(dataset, 'self'),
                     'dryad-version.json': version_url, 'dryad-files.json': files_url}
    require(all(bindings[n]['source_url'] == u for n, u in expected_urls.items()), 'API URL binding mismatch')
    files = roster.get('_embedded', {}).get('stash:files')
    require(isinstance(files, list) and len(files) == 8, 'Exact eight-file published roster required')
    seen, ids = set(), set()
    for file in files:
        checkpoint()
        name = file.get('path')
        require(name in CSV_NAMES | {'README.md'} and name not in seen, 'Unknown/duplicate provider file')
        seen.add(name)
        algorithm = str(file.get('digestType', '')).lower().replace('-', '')
        digest = file.get('digest')
        require(algorithm in {'md5', 'sha256'} and isinstance(digest, str)
                and re.fullmatch('[0-9a-fA-F]{' + str(32 if algorithm == 'md5' else 64) + '}', digest),
                'Exact supported provider digest required')
        raw = raws[name]
        require(type(file.get('size')) is int and len(raw) == file['size']
                and hashlib.new(algorithm, raw).hexdigest() == digest.lower(), 'Provider bytes/digest mismatch')
        self_url = source_link(file, 'self')
        match = re.fullmatch('https://datadryad.org/api/v2/files/([1-9][0-9]*)', self_url)
        require(match is not None and match[1] not in ids, 'Unique provider-issued file ID required')
        ids.add(match[1])
        require(bindings[name]['source_url'] == 'https://datadryad.org/downloads/file_stream/' + match[1],
                'Payload route does not match actual provider file ID')
        bindings[name]['provider'] = {'published_version_url': version_url, 'file_self_url': self_url,
            'digest_type': file['digestType'], 'digest': digest, 'bytes': file['size']}
    require(seen == CSV_NAMES | {'README.md'}, 'Provider roster incomplete')
    return raws, bindings, {'sha256': sha(manifest_raw), 'bytes': len(manifest_raw), 'published_version_url': version_url}


def verify_cricket_case(case, manifest_sha):
    case = Path(case)
    raw_manifest = read_bound(case / 'manifest.json', manifest_sha, 100_000)
    manifest = strict_json(raw_manifest)
    require(manifest.get('status') in {'validated_publisher_xlsx', 'validated_publisher_workbook'}
            and manifest.get('sha256') == CRICKET_SHA and manifest.get('bytes') == 4650070
            and manifest.get('zip_crc_verified') is True
            and manifest.get('scientific_gates_cleared') is False, 'Cricket source manifest mismatch')
    raw = read_bound(case / 'pone.0332641.s014.xlsx', CRICKET_SHA)
    require(len(raw) == 4650070, 'Cricket publisher byte count mismatch')
    return raw, {'name': 'pone.0332641.s014.xlsx', 'sha256': CRICKET_SHA, 'bytes': len(raw),
        'acquisition_manifest_sha256': sha(raw_manifest), 'integrity_basis': manifest.get('integrity_basis'),
        'source_url': 'https://journals.plos.org/plosone/article/file?id=10.1371%2Fjournal.pone.0332641.s014&type=supplementary',
        'independent_provider_checksum_available': False}


def readme_metadata(raw):
    lines = raw.decode('utf-8-sig').splitlines()
    statements = []
    section = None
    for i, line in enumerate(lines, 1):
        match = re.match(r'Table of contents for (\S+\.csv)', line)
        if match:
            section = match[1]
        if line.startswith('*'):
            statements.append({'source_line': i, 'dataset_section': section, 'literal': line})
    return {'source_sha256': sha(raw), 'metadata_statements': statements,
        'activity_clock': 'README Time is hours after first placement with sporulating/freeze-killed cadavers.',
        'identity_caution': 'README Individual includes Subject, Time and Week; actual Activity header is Indiv. This is not a stable animal key.',
        'ascertainment_caution': 'README Observation is health status; deathday labels infected flies that died at 6/7dpi. Neither proves an all-exposed assignment roster.',
        'window_duration_seconds_proven': None, 'all_exposed_assignment_roster_proven': False}


def csv_structure(raw, name, checkpoint=lambda: None):
    text = raw.decode('utf-8-sig')
    require('\x00' not in text, 'NUL in CSV')
    reader = csv.reader(io.StringIO(text, newline=''))
    header = next(reader, None)
    require(header and len(header) <= 256 and len(set(header)) == len(header), 'Missing/duplicate/excessive CSV headers')
    missing, records = Counter(), []
    metadata_names = {'Indiv', 'Subject', 'Time', 'Video', 'Day', 'Week', 'Observation', 'deathday',
        'TreatmentType', 'Treatment', 'dpi', 'Age', 'TimeOfDay', 'Date', 'ID', 'Trial_ID', 'Fly_ID',
        'FlyID', 'Pupae_date', 'infection_date', 'Pot'}
    selected = [h for h in header if h in metadata_names]
    groups = defaultdict(list)
    previous_line = reader.line_num
    for index, row in enumerate(reader, 2):
        checkpoint()
        require(index <= MAX_CSV_ROWS + 1 and len(row) == len(header), 'CSV finite row/width bound failed')
        for h, value in zip(header, row):
            if value in MISSING:
                missing[h] += 1
        values = dict(zip(header, row))
        record = {'csv_record_1based': index, 'physical_lines_1based': [previous_line + 1, reader.line_num],
                  'metadata_literals': {h: values[h] for h in selected}}
        previous_line = reader.line_num
        records.append(record)
        if name == 'Activity.csv':
            require({'Week', 'Subject', 'Time', 'Observation', 'deathday', 'TreatmentType'}.issubset(header),
                    'Actual activity identity/ascertainment schema required')
            groups[(values['Week'], values['Subject'])].append(record)
    result = {'name': name, 'source_sha256': sha(raw), 'headers': header,
        'header_column_1based': {h: i for i, h in enumerate(header, 1)},
        'rows': len(records), 'missingness_by_column': {h: missing[h] for h in header},
        'metadata_records': records, 'outcome_values_exported': False,
        'outcome_statistics_calculated': False, 'missingness_definition': sorted(MISSING),
        'metadata_scope': 'Only exact whitelisted identity, treatment, date and clock literals; no weight, mating, locomotion, survival Status or other outcome values.'}
    if name == 'Activity.csv':
        require(records, 'Activity source has no records')
        result['activity_design'] = activity_design(groups)
    return result


def activity_design(groups):
    subjects, joint, all_times, invalid_time = [], Counter(), set(), []
    infected, fatal = 0, 0
    for (week, subject), records in sorted(groups.items()):
        keys = Counter()
        for record in records:
            value = record['metadata_literals']
            joint[(value['Observation'], value['deathday'], value['TreatmentType'])] += 1
            status = value['Observation'].strip().lower()
            if status == 'infected':
                infected += 1
                fatal += value['deathday'].strip().lower() in {'deadday6', 'deadday7'}
            try:
                hour = float(value['Time'])
                require(math.isfinite(hour) and hour >= 0, 'Nonfinite/negative activity clock')
                all_times.add(hour)
            except (ValueError, OverflowError):
                invalid_time.append(record['csv_record_1based'])
            keys[(value.get('Time'), value.get('Video'), value.get('Day'))] += 1
        subjects.append({'Week': week, 'Subject': subject, 'rows': len(records),
            'csv_records_1based': [r['csv_record_1based'] for r in records],
            'Observation_literals': sorted({r['metadata_literals']['Observation'] for r in records}),
            'deathday_literals': sorted({r['metadata_literals']['deathday'] for r in records}),
            'TreatmentType_literals': sorted({r['metadata_literals']['TreatmentType'] for r in records}),
            'Time_literals': sorted({r['metadata_literals']['Time'] for r in records}),
            'repeated_time_video_day_keys': [{'Time': k[0], 'Video': k[1], 'Day': k[2], 'rows': n}
                                             for k, n in sorted(keys.items()) if n > 1]})
    selected = infected > 0 and fatal == infected
    times = sorted(all_times)
    return {'candidate_biological_key': ['Week', 'Subject'], 'unique_candidate_subjects': len(subjects),
        'candidate_key_validated_as_original_animal_registry': False,
        'subjects_with_repeated_observations': sum(s['rows'] > 1 for s in subjects),
        'blank_identity_keys': sum(not k[0].strip() or not k[1].strip() for k in groups),
        'subjects': subjects, 'time_hours_values': times,
        'observed_time_grid_gaps_hours': sorted({round(b-a, 12) for a, b in zip(times, times[1:])}),
        'invalid_time_csv_records': invalid_time,
        'metadata_joint_counts': [{'Observation': k[0], 'deathday': k[1], 'TreatmentType': k[2], 'rows': n}
                                  for k, n in sorted(joint.items())],
        'infected_label_rows': infected, 'infected_label_rows_with_DeadDay6_or_7': fatal,
        'all_infected_label_rows_are_future_fatal_cases': selected,
        'all_exposed_assignment_roster_proven': False, 'exposure_causal_estimand_available': False,
        'HF1_route': 'conditional_infection_health_status_association',
        'HF1_selection_gate': ('Observed infected-labelled rows are defined by future death at day6/7. '
            'An exposure-causal contrast would condition on subsequent infection/death; all exposed assignments are not established.'
            if selected else 'Original full exposure assignments and linkage to surviving/noninfected exposed flies remain unproven.'),
        'uniform_window_duration_seconds_proven': None,
        'missing_observation_windows_are_not_zeros': True,
        'no_other_csv_join_inferred_from_matching_subject_strings': True}


def archive_reader(raw, checkpoint):
    archive = zipfile.ZipFile(io.BytesIO(raw))
    members = archive.infolist()
    require(len(members) <= 10000 and len({m.filename for m in members}) == len(members)
            and sum(m.file_size for m in members) <= MAX_EXPANDED_BYTES, 'OOXML finite expansion/unique-part bound failed')
    for member in members:
        checkpoint()
        p = PurePosixPath(member.filename)
        require(not p.is_absolute() and '..' not in p.parts and '\\' not in member.filename
                and not member.flag_bits & 1 and member.file_size <= MAX_MEMBER_BYTES, 'Unsafe OOXML part')
        with archive.open(member) as stream:
            while stream.read(65536):
                checkpoint()
    def xml(name):
        checkpoint()
        body = archive.read(name)
        require(b'<!DOCTYPE' not in body.upper() and b'<!ENTITY' not in body.upper(), 'XML entity declarations forbidden')
        node = ET.fromstring(body)
        checkpoint()
        return node
    return archive, xml


def docx_structure(raw, name, checkpoint=lambda: None):
    archive, xml = archive_reader(raw, checkpoint)
    try:
        node = xml('word/document.xml')
        tables, count, flags = [], 0, []
        for tindex, table in enumerate(node.findall('.//' + W + 'tbl'), 1):
            rows = []
            for rindex, row in enumerate(table.findall(W + 'tr'), 1):
                checkpoint()
                values = []
                for cindex, cell in enumerate(row.findall(W + 'tc'), 1):
                    count += 1
                    require(count <= MAX_CELLS, 'DOCX cell bound exceeded')
                    literal = ''.join(t.text or '' for t in cell.iter(W + 't'))
                    values.append(literal)
                    if re.search(r'(?<!\d)(?:25936|25836)(?!\d)', literal):
                        flags.append({'table': tindex, 'row': rindex, 'column': cindex,
                                      'literal': literal, 'row_literals': None})
                rows.append(values)
                for flag in flags:
                    if flag['table'] == tindex and flag['row'] == rindex:
                        flag['row_literals'] = values
            tables.append({'table': tindex, 'rows': len(rows), 'headers': rows[0] if rows else [],
                           'explicit_animal_join_header_present': any(re.search(r'\b(?:ROI|animal.?id|experiment.?id)\b', v, re.I) for v in (rows[0] if rows else []))})
        return {'name': name, 'source_sha256': sha(raw), 'zip_crc_verified': True,
            'tables': tables, 'cells_scanned': count, 'literal_25936_25836_rows': flags,
            'stock_gene_identity_resolved': False, 'raw_animal_ROI_join_recovered': False,
            'interpretation': 'Published design-level genotype/control table; preserve assignments. No stock swap, raw ROI join, clock offset or biological admission inferred.'}
    finally:
        archive.close()


def normalized(value):
    return re.sub('[^a-z0-9]', '', value.lower())


IDENTITY_LABELS = {'animalid', 'cricketid', 'subjectid', 'trialid', 'flyid'}
META_LABELS = IDENTITY_LABELS | {'sampleid', 'treatment', 'condition', 'group', 'sex', 'dpi',
    'dayspostinfection', 'exposuredate', 'exposuretime', 'recordingtime', 'trialstart', 'trialend',
    'recordingduration', 'observationwindow', 'observationwindowseconds'}
ENDPOINT_LABELS = {'waterentry', 'enteredwater', 'headsubmersion', 'initialchoice', 'fullwaterentry'}
HEADER_HINTS = META_LABELS | ENDPOINT_LABELS | {'protein', 'proteinid', 'proteinids', 'gene', 'genes',
    'genenames', 'accession', 'description', 'peptides', 'intensity', 'lfqintensity', 'log2foldchange',
    'foldchange', 'pvalue', 'qvalue', 'fdr', 'go', 'goterm', 'pathway'}


def xlsx_structure(raw, checkpoint=lambda: None):
    archive, xml = archive_reader(raw, checkpoint)
    try:
        strings = []
        if 'xl/sharedStrings.xml' in archive.namelist():
            for item in xml('xl/sharedStrings.xml').findall(M + 'si'):
                checkpoint()
                require(len(strings) < MAX_CELLS, 'Shared-string bound exceeded')
                strings.append(''.join(t.text or '' for t in item.iter(M + 't')))
        rels = {}
        for rel in xml('xl/_rels/workbook.xml.rels'):
            require(rel.get('Id') not in rels, 'Duplicate workbook relationship')
            if rel.get('Type', '').endswith('/worksheet'):
                require(rel.get('TargetMode') != 'External', 'External worksheet not audited')
                target = rel.get('Target', '')
                require('\\' not in target, 'Unsafe worksheet target')
                part = posixpath.normpath(target.lstrip('/') if target.startswith('/') else 'xl/' + target)
                require(part.startswith('xl/worksheets/') and '..' not in PurePosixPath(part).parts,
                        'Foreign worksheet target')
                rels[rel.get('Id')] = part
        sheet_defs = xml('xl/workbook.xml').findall(M + 'sheets/' + M + 'sheet')
        require(0 < len(sheet_defs) <= 256, 'Finite nonempty workbook sheet roster required')
        sheets, total_cells, total_rows = [], 0, 0
        for definition in sheet_defs:
            name, ident = definition.get('name'), definition.get(R + 'id')
            require(ident in rels, 'Worksheet relationship unresolved')
            part = rels[ident]
            headers, metadata, metadata_formulas, missing, formulas, rows_seen = [], [], [], Counter(), 0, 0
            active = None
            for row in xml(part).iter(M + 'row'):
                checkpoint()
                total_rows += 1; rows_seen += 1
                require(total_rows <= MAX_ROWS, 'Workbook row bound exceeded')
                cells = []
                for cell in row.findall(M + 'c'):
                    total_cells += 1
                    require(total_cells <= MAX_CELLS, 'Workbook cell bound exceeded')
                    if total_cells % 256 == 0: checkpoint()
                    coord = cell.get('r')
                    require(re.fullmatch('[A-Z]+[1-9][0-9]*', coord or ''), 'Exact worksheet cell coordinate required')
                    v, inline, formula = cell.find(M + 'v'), cell.find(M + 'is'), cell.find(M + 'f')
                    literal = v.text if v is not None else ''.join(t.text or '' for t in inline.iter(M + 't')) if inline is not None else None
                    if cell.get('t') == 's' and literal is not None:
                        require(re.fullmatch('[0-9]+', literal) and int(literal) < len(strings), 'Invalid shared-string reference')
                        literal = strings[int(literal)]
                    is_text = cell.get('t') in {'s', 'inlineStr', 'str'}
                    formulas += formula is not None
                    col = re.match('[A-Z]+', coord)[0]
                    if literal is None or literal in MISSING:
                        missing[col] += 1
                    cells.append((coord, col, literal, is_text, formula))
                text_cells = [(c, col, v) for c, col, v, text, f in cells if text and v is not None and f is None]
                hints = [v for _, _, v in text_cells if normalized(v) in HEADER_HINTS or normalized(v).startswith('lfqintensity')]
                # Only a real header candidate is exported. Data/protein rows and
                # numerical cached results never become a header preview.
                candidate = bool(hints) and (len(hints) >= 2 or (rows_seen <= 5 and len(text_cells) >= 2))
                if candidate:
                    require(len(headers) < 256 and len(text_cells) <= 256, 'Header export finite bound exceeded')
                    recognized = [(c, col, v) for c, col, v in text_cells
                        if normalized(v) in HEADER_HINTS or normalized(v).startswith('lfqintensity')]
                    unclassified = [(c, col, v) for c, col, v in text_cells
                        if normalized(v) not in HEADER_HINTS and not normalized(v).startswith('lfqintensity')]
                    entry = {'row': row.get('r'), 'cells': [{'cell': c, 'literal': v} for c, _, v in recognized],
                        'unclassified_text_cells_sha256_only': [{'cell': c, 'sha256': sha(v.encode()),
                            'literal_exported': False} for c, _, v in unclassified],
                        'all_candidate_row_text_literals_exported': not unclassified}
                    headers.append(entry)
                    active = {col: (v, c) for c, col, v in text_cells if normalized(v) in META_LABELS}
                elif active:
                    values = []
                    for coord, col, literal, text, formula in cells:
                        if col in active and formula is not None:
                            require(len(metadata_formulas) < 100_000, 'Metadata formula diagnostic bound exceeded')
                            label, header_cell = active[col]
                            metadata_formulas.append({'cell': coord, 'header_cell': header_cell,
                                'header_literal': label, 'formula_sha256': sha((formula.text or '').encode()),
                                'cached_value_exported': False, 'literal_available_only_in_original': True})
                        if col in active and literal is not None and formula is None:
                            label, header_cell = active[col]
                            values.append({'cell': coord, 'header_cell': header_cell, 'header_literal': label, 'literal': literal})
                    if values:
                        require(len(metadata) < 100_000, 'Metadata record finite bound exceeded')
                        metadata.append({'row': row.get('r'), 'cells': values})
            header_literals = [c['literal'] for h in headers for c in h['cells']]
            labels = {normalized(v) for v in header_literals}
            trial_schema = any(({normalized(c['literal']) for c in h['cells']} & IDENTITY_LABELS)
                and ({normalized(c['literal']) for c in h['cells']} & ENDPOINT_LABELS) for h in headers)
            sheets.append({'sheet_name_literal': name, 'part': part, 'rows_scanned': rows_seen,
                'header_candidates': headers, 'metadata_records': metadata,
                'metadata_formula_cells_not_evaluated': metadata_formulas,
                'empty_or_missing_stored_cells_by_column': dict(missing),
                'formula_cells_count': formulas, 'formulas_executed': False,
                'explicit_individual_identity_header': bool(labels & IDENTITY_LABELS),
                'behavioral_endpoint_header_candidate': bool(labels & ENDPOINT_LABELS),
                'individual_behavior_trial_schema_candidate': trial_schema,
                'proteomic_schema_hint': bool(labels & {'protein', 'proteinid', 'proteinids', 'genenames', 'peptides', 'lfqintensity'}),
                'header_detection_scope': 'Only recognized structural/header labels are literal-exported. Unclassified candidate-row text has coordinate/SHA-only diagnostics. Every cell scanned; unfamiliar headers may remain unresolved; no exhaustive semantic absence claim.',
                'missingness_scope': 'Stored OOXML cells only; absent sparse-grid cells are not inferred or treated as zero.'})
        return {'source_sha256': sha(raw), 'zip_crc_verified': True, 'sheets': sheets,
            'cells_scanned': total_cells, 'rows_scanned': total_rows, 'formulas_executed': False,
            'individual_behavior_trial_schema_identified': any(s['individual_behavior_trial_schema_candidate'] for s in sheets),
            'individual_behavior_trials_available': 'not_established_by_structural_audit',
            'CR1_admitted': False, 'original_eligible_trial_denominators_proven': False,
            'outcome_values_exported': False, 'outcome_statistics_calculated': False,
            'interpretation': 'Proteomics/sample summaries cannot be converted into individual behavioral trials. Candidate headers require original trial/condition/outcome/eligibility linkage before CR1 admission.'}
    finally:
        archive.close()


def missing_data_spec():
    return {
      'original_fly_H1_H3': ['Original experiment/ROI/animal registry with reused-animal linkage and sex.',
          'Actual per-animal first exposure/assignment timestamps and recording timestamps with timezone/units.',
          'Original genotype/reagent/stock and matched control for each raw ROI; resolve published25936/25836 against actual used stock records.',
          'Verified reagent-to-gene identifiers and homolog-family mapping with source versions; no filename/catalog imputation.'],
      'housefly_HF1': ['Complete original batch×animal exposure/sham assignment roster, including exposed survivors/noninfected animals, exclusions and losses.',
          'Original infection verification and survival follow-up joined to the same stable animal key, without selecting future death day.',
          'Actual observation start/stop or valid duration per filmed window, per-window missingness/death/censoring reasons and original time units.',
          'Allocation protocol/randomization evidence and original analysis inclusion rules; otherwise interpret treatment contrast as observational.',
          'If complete assignment roster is unavailable, prospectively label the endpoint conditional infection/health-status association and document future-fatal ascertainment before effects.'],
      'cricket_CR1': ['Original one-row-per-tested-cricket behavior table: stable trial/animal ID, original condition and exact exposure/dehydration/recovery timeline.',
          'All original eligible trials/denominators, full-water-entry outcome and trial duration/censoring; retain nonentries and exclusions.',
          'Separate initial-choice/head-submersion/encounter-conditioned endpoints and their denominators; no invented trials from proteomic rows.',
          'Explicit independent or reused-animal identity and mapping to proteomic samples if cross-modal claims are planned.'],
      'acquisition_route': 'Recover original public supplements/archived deposits or request original records only after separate authorization to contact people; otherwise prospectively collect new independently registered data.',
      'new_data_minimum': 'Freeze animal/condition registry, exposure clock, observation windows, missingness and endpoint definitions before collecting outcomes; calculate sample size from an independently specified meaningful effect and precision target.',
      'no_outbound_contact_performed': True}


def audit_sources(source_case, source_manifest_sha256, cricket_case, cricket_manifest_sha256,
                  preregistration_path, preregistration_sha256, checkpoint=lambda: None):
    prereg_raw = read_bound(preregistration_path, preregistration_sha256, 1_000_000)
    strict_json(prereg_raw)
    raws, bindings, manifest = verify_source_case(source_case, source_manifest_sha256, checkpoint)
    cricket, cricket_binding = verify_cricket_case(cricket_case, cricket_manifest_sha256)
    require(sum(len(v) for v in raws.values()) + len(cricket) + len(prereg_raw) <= MAX_INPUT_BYTES,
            'Combined original/protocol input bound exceeded')
    # No structural table parsing occurs until every original has verified.
    checkpoint()
    csv_reports = {n: csv_structure(raws[n], n, checkpoint) for n in sorted(CSV_NAMES)}
    return {'schema_version': 1, 'status': 'complete_structural_audit_only',
        'created_utc': datetime.now(timezone.utc).isoformat(),
        'helper_sha256': sha(Path(__file__).read_bytes()), 'protocol': {
            'information_barrier': 'No outcome values exported, effect estimates, models, condition plots or treatment-effect reading.',
            'preregistration_sha256': sha(prereg_raw), 'preregistration_path': str(preregistration_path),
            'original_H1_H2_H3_remain_separate': True,
            'HF1_prospective_interpretation_addendum': 'Conditional infection/health-status association until an all-exposed assignment roster and eligibility/window protocol are proved.'},
        'source_manifest': manifest, 'source_bindings': bindings,
        'cricket_source_binding': cricket_binding,
        'readme': readme_metadata(raws['README.md']), 'housefly_csv': csv_reports,
        'publisher_tables': [docx_structure(raws[n], n, checkpoint) for n in sorted(DOCX)],
        'cricket_workbook': xlsx_structure(cricket, checkpoint),
        'prospective_missing_data_specification': missing_data_spec(),
        'scientific_gates_cleared': False, 'original_missing_metadata_gates_cleared': False,
        'HF1_exposure_causal_admitted': False, 'CR1_admitted': False,
        'outcome_values_exported': False, 'outcome_statistics_calculated': False,
        'limits': {'input_bytes': MAX_INPUT_BYTES, 'expanded_bytes_per_archive': MAX_EXPANDED_BYTES,
            'member_bytes': MAX_MEMBER_BYTES, 'cells_per_workbook': MAX_CELLS,
            'csv_rows_per_file': MAX_CSV_ROWS, 'workbook_rows': MAX_ROWS,
            'total_output_bytes': MAX_OUTPUT_BYTES, 'seconds': MAX_SECONDS}}


def exclusive_json(path, value, maximum=MAX_OUTPUT_BYTES):
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    require(len(raw) <= maximum, 'Finite output bound exceeded')
    with Path(path).open('xb') as handle:
        handle.write(raw); handle.flush(); os.fsync(handle.fileno())
    return {'name': Path(path).name, 'bytes': len(raw), 'sha256': sha(raw)}


def markdown(report):
    design = report['housefly_csv']['Activity.csv']['activity_design']
    workbook = report['cricket_workbook']
    return ('# Structural public-evidence audit\n\n'
        'This source-bound audit is outcome-blind and grants no scientific admission.\n\n'
        f"Housefly Activity: {report['housefly_csv']['Activity.csv']['rows']} records, "
        f"{design['unique_candidate_subjects']} candidate Week×Subject keys. "
        f"Infected-labelled records with future day6/7 death: {design['infected_label_rows_with_DeadDay6_or_7']}/{design['infected_label_rows']}.\n\n"
        + design['HF1_selection_gate'] + '\n\n'
        'HF1 remains a prospective conditional infection/health-status association; the full exposure-assignment roster, stable identity and per-window duration need proof. Missing windows are not zeros.\n\n'
        f"Cricket: {len(workbook['sheets'])} sheets, {workbook['cells_scanned']} cells scanned. "
        f"Individual behavioral schema candidate identified: {workbook['individual_behavior_trial_schema_identified']}. "
        'This does not establish original individual trials or eligible denominators; proteomic samples cannot become synthetic behavioral trials.\n\n'
        'Exact metadata cells, README line references, original source/provider checksums and manifest/protocol bindings are in the JSON. Original H1–H3 and all biological admission gates remain unresolved.\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['source-case', 'source-manifest-sha256', 'cricket-case', 'cricket-manifest-sha256',
                 'preregistration', 'preregistration-sha256', 'case']:
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    source_case, cricket_case, case = map(Path, [args.source_case, args.cricket_case, args.case])
    prereg = Path(args.preregistration)
    require(source_case == SOURCE_CASE and cricket_case == CRICKET_CASE, 'Only exact native source cases are authorized')
    require(args.source_manifest_sha256 == SOURCE_MANIFEST_SHA
            and args.cricket_manifest_sha256 == CRICKET_MANIFEST_SHA
            and args.preregistration_sha256 == PREREG_SHA,
            'Exact previously frozen source/protocol identity required')
    require(hasattr(os, 'geteuid') and os.geteuid() == 3164768, 'Native personal user required')
    job = os.environ.get('SLURM_JOB_ID', '')
    require(re.fullmatch('[1-9][0-9]*', job) and os.environ.get('SLURM_CPUS_PER_TASK') == '1'
            and os.environ.get('SLURM_JOB_GPUS', '') == ''
            and os.environ.get('SLURM_STEP_GPUS', '') == ''
            and os.environ.get('SLURM_GPUS', '') in {'', '0'}
            and os.environ.get('SLURM_GPUS_PER_TASK', '') in {'', '0'}, 'Native one-CPU/no-GPU parent required')
    end = int(os.environ.get('SLURM_JOB_END_TIME', '0'))
    now = time.time()
    require(now + 120 < end <= now + 3600, 'Native at-most-one-hour hard bound and cleanup margin required')
    maintenance = (ROOT / 'maintenance').resolve(strict=True)
    require(case.parent.resolve(strict=True) == maintenance and not case.exists()
            and not case.is_symlink() and prereg.resolve(strict=True).is_relative_to(maintenance), 'Fresh confined native evidence case required')
    require(source_case.resolve(strict=True) == source_case and cricket_case.resolve(strict=True) == cricket_case,
            'Source case symlinks forbidden')
    case.mkdir(mode=0o700)
    start = time.monotonic(); deadline = start + min(MAX_SECONDS, end - now - 90)
    def checkpoint():
        if time.monotonic() >= deadline:
            raise TimeoutError('Structural audit finite deadline reached')
    native = {'job_id': job, 'uid': os.geteuid(), 'cpus': 1, 'gpus': 0,
        'allocation_proof_authority': 'Registered parent verifies native scheduler/accounting, qualification and resource lease; helper additionally checks exact native environment.',
        'job_end_unix': end, 'cleanup_margin_seconds': 90, 'audit_soft_seconds': deadline-start}
    try:
        report = audit_sources(source_case, args.source_manifest_sha256, cricket_case,
            args.cricket_manifest_sha256, prereg, args.preregistration_sha256, checkpoint)
        report['native_parent'] = native
        report['elapsed_seconds'] = time.monotonic() - start
        record = exclusive_json(case / 'structural-source-facts.json', report)
        md = markdown(report).encode()
        require(len(md) + record['bytes'] <= MAX_OUTPUT_BYTES - 100_000, 'Combined output bound exceeded')
        with (case / 'structural-source-facts.md').open('xb') as handle:
            handle.write(md); handle.flush(); os.fsync(handle.fileno())
        exclusive_json(case / 'receipt.json', {'status': 'complete_structural_audit_only',
            'artifact': record, 'markdown': {'bytes': len(md), 'sha256': sha(md)},
            'native_parent': native, 'scientific_gates_cleared': False,
            'outcome_statistics_calculated': False}, 100_000)
    except Exception as error:
        exclusive_json(case / 'failure.json', {'status': 'failed_no_clearance', 'native_parent': native,
            'error_type': type(error).__name__, 'message': str(error)[:1000],
            'scientific_gates_cleared': False}, 100_000)
        raise


if __name__ == '__main__':
    main()
