"""Bounded source-bound workbook schema inventory; no outcome magnitudes.

Never execute workbook formulas, scripts, macros or external relationships.
Only recognized candidate header text is exported; other cell content is
represented by its coordinate/type/hash. This is not a stock/control join.
The native launcher owns scheduler, qualification, storage and hard deadline.
"""
from pathlib import Path, PurePosixPath
from collections import Counter
import argparse
import hashlib
import io
import json
import math
import os
import re
import stat
import time
from urllib.parse import urlparse, parse_qs
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path('/scratch/pwa209/horsehair-worm')
NAME = '41467_2024_48005_MOESM8_ESM.xlsx'
SOURCE = ROOT/'maintenance/study-af-vtl-stock-workbook-s3-v1-20261021'/NAME
SOURCE_SHA = 'f8fb6db82687d2dad58215df15283a8d8640acec6c722d0966d9f6f5ca183cb8'
SOURCE_MD5 = '38192352700c09694811a8e8e43e8097'
SOURCE_BYTES = 267375
METADATA = ROOT/'maintenance/study-af-vtl-s3-metadata-v1-20261021/PMC11063160.1.json'
METADATA_SHA = '91bde60e247081cd5afef1a1a121bda8577308bb9a78ab81d13f8f03625dfee1'
TRANSFER = SOURCE.with_name('receipt.json')
TRANSFER_SHA = '70da14b0c60ad281352f478d2cb2b7c93344944112217e8acf549877858f23be'
URL = 'https://pmc-oa-opendata.s3.amazonaws.com/PMC11063160.1/'+NAME
S3 = 's3://pmc-oa-opendata/PMC11063160.1/'+NAME+'?md5='+SOURCE_MD5
NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
REL_NS = 'http://schemas.openxmlformats.org/package/2006/relationships'
DOC_REL_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
MAX_XML = 8_000_000
MAX_EXPANDED = 32_000_000
MAX_ROWS = 100_000
MAX_CELLS = 500_000
HEADER_ROWS = 20
FALSE_FLAGS = dict.fromkeys(['scientific_result', 'admission_authority', 'outcome_magnitudes_exported',
    'formulas_executed', 'formula_text_exported', 'cached_formula_values_exported', 'unknown_cell_text_exported',
    'stock_reagent_gene_joins_established', 'control_background_edges_verified', 'gene_effects_estimated',
    'biological_independence_verified', 'original_E2_cleared', 'original_H1_H2_H3_cleared'], False)

# Literal source header vocabulary, not a gene/reagent or control classifier.
HEADER_GROUPS = {
    'stock': {'stock', 'stockid', 'stockno', 'stocknumber', 'stockname', 'stockcenter', 'stockcentre',
              'stockcenterid', 'stockcenterstocknumber', 'blnumber', 'blno', 'bdsc', 'vdrc', 'thnumber', 'thno'},
    'line': {'line', 'lineid', 'lineno', 'linenumber', 'rnailine', 'mutantline'},
    'gene': {'gene', 'geneid', 'genename', 'genesymbol', 'targetgene', 'targetgeneid', 'flybaseid', 'fbgn'},
    'reagent': {'reagent', 'reagentid', 'rnai', 'rnaiid', 'rnaiinformation', 'construct', 'constructid'},
    'genotype': {'genotype', 'geneticbackground', 'background', 'backgroundgenotype'},
    'control': {'control', 'controlid', 'matchedcontrol', 'controlstock', 'controlgenotype', 'parentalcontrol'},
    'driver': {'driver', 'driverline', 'drivergenotype', 'gal4driver'},
}
HEADERS = {label: group for group, labels in HEADER_GROUPS.items() for label in labels}


def require(value, message):
    if not value: raise ValueError(message)


def sha(raw): return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, 'Duplicate source metadata key')
            result[key] = value
        return result
    def nonfinite(value): raise ValueError('Nonfinite metadata refused')
    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)
    def finite(value):
        if isinstance(value, float): require(math.isfinite(value), 'Nonfinite metadata refused')
        elif isinstance(value, dict):
            for item in value.values(): finite(item)
        elif isinstance(value, list):
            for item in value: finite(item)
    finite(value)
    return value


def ordinary(path, *, fresh=False):
    original = Path(path); root = ROOT.resolve(); resolved = original.resolve()
    require(original.is_absolute() and resolved.is_relative_to(root/'maintenance')
            and resolved != root/'maintenance', 'Personal maintenance file required')
    prefixes = [p for p in (ROOT, root) if original.is_relative_to(p)]
    require(prefixes, 'Foreign source namespace refused')
    prefix = max(prefixes, key=lambda p: len(p.parts))
    require(all(not p.is_symlink() for p in [original, *original.parents]
                if p != prefix and p.is_relative_to(prefix)), 'Subordinate source/output symlink refused')
    require(not resolved.exists() if fresh else resolved.is_file(), 'Fresh output required' if fresh else 'Ordinary source file required')
    return resolved


def bounded(path, expected_sha, maximum):
    path = ordinary(path)
    require(path.stat().st_size <= maximum, 'Finite source size exceeded')
    with path.open('rb') as stream: raw = stream.read(maximum+1)
    require(len(raw) <= maximum and sha(raw) == expected_sha, 'Pinned source bytes differ')
    return raw


def source_bindings(metadata_raw, transfer_raw, workbook_raw):
    metadata, transfer = strict_json(metadata_raw), strict_json(transfer_raw)
    require(type(metadata) is dict and metadata.get('pmcid') == 'PMC11063160'
            and type(metadata.get('version')) is int and metadata['version'] == 1
            and metadata.get('doi') == '10.1038/s41467-024-48005-w'
            and metadata.get('is_retracted') is False and metadata.get('is_manuscript') is False
            and metadata.get('is_pmc_openaccess') is True and metadata.get('license_code') == 'CC BY',
            'Exact official final-publication metadata required')
    media = metadata.get('media_urls')
    require(type(media) is list and media.count(S3) == 1, 'Provider MD5 URL for the exact stock workbook required')
    require(transfer.get('status') == 'provider_md5_verified_official_stock_workbook_acquired'
            and transfer.get('path') == str(SOURCE) and transfer.get('sha256') == SOURCE_SHA
            and type(transfer.get('bytes')) is int and transfer['bytes'] == SOURCE_BYTES
            and transfer.get('actual_md5') == SOURCE_MD5 and transfer.get('provider_md5_verified') is True
            and transfer.get('publisher_version_verified') is True and transfer.get('url') == URL
            and transfer.get('provider_metadata') == {'path': str(METADATA), 'sha256': METADATA_SHA}
            and transfer.get('scientific_result') is False and transfer.get('workbook_values_inspected') is False
            and transfer.get('author_contact') is False and type(transfer.get('transfer', {}).get('returncode')) is int
            and transfer['transfer']['returncode'] == 0
            and transfer['transfer'].get('stdout') == '200|'+URL+'|'+str(SOURCE_BYTES),
            'Exact provider-verified acquisition receipt required')
    require(len(workbook_raw) == SOURCE_BYTES and sha(workbook_raw) == SOURCE_SHA
            and hashlib.md5(workbook_raw).hexdigest() == SOURCE_MD5, 'Workbook provider MD5/full SHA/size differ')
    return {'provider': 'NIH NLM NCBI PubMed Central Article Datasets on AWS', 'version': 1,
        'metadata': {'path': str(METADATA), 'sha256': sha(metadata_raw)},
        'transfer': {'path': str(TRANSFER), 'sha256': sha(transfer_raw)},
        'workbook': {'path': str(SOURCE), 'sha256': sha(workbook_raw), 'bytes': len(workbook_raw), 'provider_md5': SOURCE_MD5},
        'provider_checksum_verified': True, 'license_literal': 'CC BY'}


def xml(raw):
    require(len(raw) <= MAX_XML, 'Plain bounded OOXML required')
    try: text = raw.decode('utf-8-sig')
    except UnicodeError as error: raise ValueError('Only plain UTF8 OOXML is supported') from error
    require('<!DOCTYPE' not in text.upper() and '<!ENTITY' not in text.upper(), 'Plain bounded OOXML required')
    declaration = re.match(r'\s*<\?xml\s+[^?]*encoding\s*=\s*["\']([^"\']+)', text, re.I)
    require(declaration is None or declaration[1].lower().replace('-', '') == 'utf8', 'Only declared UTF8 OOXML is supported')
    return ET.fromstring(text)


def normalize_header(value): return re.sub('[^a-z0-9]', '', value.lower())


def member_inventory(raw, *, deadline):
    def check(): require(time.monotonic() < deadline, 'Workbook metadata deadline exceeded')
    check()
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        infos = archive.infolist(); names = [info.filename for info in infos]
        require(0 < len(infos) <= 1024 and len(names) == len(set(names)), 'Bounded unique OOXML member roster required')
        expanded = 0
        for info in infos:
            check(); name = info.filename; parts = PurePosixPath(name).parts
            require(name and name == info.orig_filename and '\\' not in name and not name.startswith('/') and all(p not in {'.', '..'} for p in parts)
                    and '/'.join(parts) == name.rstrip('/') and not info.flag_bits & 1
                    and info.compress_type in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}, 'Unsupported ZIP path/encryption/compression')
            kind = stat.S_IFMT(info.external_attr >> 16)
            require(kind in {0, stat.S_IFREG, stat.S_IFDIR} and (kind != stat.S_IFDIR or info.is_dir()), 'Special ZIP member refused')
            require(0 <= info.file_size <= MAX_XML, 'Expanded member bound exceeded')
            expanded += info.file_size
        require(expanded <= MAX_EXPANDED and not any('vbaproject' in n.lower() or n.lower().endswith('.bin') for n in names),
                'Expanded workbook bound or macro/embedded binary refused')
        def read(name):
            check(); require(name in names, 'Required workbook metadata member missing')
            with archive.open(name) as stream: value = stream.read(MAX_XML+1)
            require(len(value) <= MAX_XML, 'Expanded XML bound exceeded'); return value
        workbook = xml(read('xl/workbook.xml'))
        relationships = xml(read('xl/_rels/workbook.xml.rels'))
        rels = {}
        for relation in relationships:
            require(relation.tag == '{'+REL_NS+'}Relationship' and relation.get('TargetMode') != 'External', 'External/unknown workbook relationship refused')
            key = relation.get('Id'); target = relation.get('Target', '')
            # OPC permits package-rooted internal URIs. Accept only the exact
            # /xl/ namespace; never normalize a foreign root or traversal.
            if target.startswith('/'):
                require(target.startswith('/xl/'), 'Foreign package-rooted relationship refused')
                target = target[len('/xl/'):]
            require(key and key not in rels and target and not target.startswith('/') and '\\' not in target
                    and all(p not in {'.', '..'} for p in PurePosixPath(target).parts)
                    and re.fullmatch('[A-Za-z0-9_./-]+', target), 'Ambiguous workbook relationship')
            rels[key] = target
        # Reject external links anywhere, including relationships unused by the inventory.
        for name in names:
            if name.endswith('.rels') and name != 'xl/_rels/workbook.xml.rels':
                for relation in xml(read(name)):
                    require(relation.get('TargetMode') != 'External', 'External workbook link refused')
        strings = []
        if 'xl/sharedStrings.xml' in names:
            for item in xml(read('xl/sharedStrings.xml')).findall('{'+NS+'}si'):
                require(len(strings) < MAX_CELLS, 'Shared-string metadata bound exceeded')
                strings.append(''.join(t.text or '' for t in item.iter('{'+NS+'}t')))
        sheets = workbook.find('{'+NS+'}sheets')
        require(sheets is not None and 0 < len(sheets) <= 128, 'Bounded worksheet roster required')
        result = []; used_names = set(); used_targets = set()
        for sheet in sheets:
            check(); title = sheet.get('name', ''); rid = sheet.get('{'+DOC_REL_NS+'}id'); target = rels.get(rid, '')
            require(sheet.tag == '{'+NS+'}sheet' and title and len(title) <= 128 and title not in used_names
                    and re.fullmatch(r'worksheets/[A-Za-z0-9_.-]+\.xml', target)
                    and target not in used_targets, 'Unique ordinary sheet metadata required')
            require(sheet.get('state', 'visible') in {'visible', 'hidden', 'veryHidden'}, 'Unknown worksheet state')
            used_names.add(title); used_targets.add(target); path = 'xl/'+target
            tree = xml(read(path)); dimension = tree.find('{'+NS+'}dimension')
            declared = None if dimension is None else dimension.get('ref')
            require(declared is None or re.fullmatch(r'[A-Z]{1,3}[1-9][0-9]*(?::[A-Z]{1,3}[1-9][0-9]*)?', declared), 'Unknown declared dimension syntax')
            data = tree.find('{'+NS+'}sheetData'); require(data is not None, 'Worksheet data structure missing')
            rows = []; types = Counter(); coordinates = set(); row_ids = set(); max_row = max_col = cells = 0
            for row in data:
                check(); require(row.tag == '{'+NS+'}row' and re.fullmatch('[1-9][0-9]*', row.get('r', '')), 'Explicit positive row identity required')
                number = int(row.get('r')); require(number <= MAX_ROWS and number not in row_ids, 'Worksheet unique row bound exceeded')
                row_ids.add(number)
                maximum_row = number; max_row = max(max_row, maximum_row); headers = []; other = []
                for cell in row:
                    cells += 1; require(cells <= MAX_CELLS, 'Worksheet cell bound exceeded')
                    coordinate = cell.get('r', ''); match = re.fullmatch(r'([A-Z]{1,3})([1-9][0-9]*)', coordinate)
                    require(cell.tag == '{'+NS+'}c' and match is not None and int(match[2]) == number
                            and coordinate not in coordinates, 'Unique source cell coordinate required')
                    coordinates.add(coordinate); col = 0
                    for char in match[1]: col = col*26 + ord(char)-64
                    require(col <= 16384, 'Excel column bound exceeded'); max_col = max(max_col, col)
                    kind = cell.get('t', 'n'); formula = cell.find('{'+NS+'}f')
                    types['formula' if formula is not None else kind] += 1
                    if number > HEADER_ROWS: continue
                    literal = None
                    if formula is None and kind == 's':
                        value = cell.findtext('{'+NS+'}v', '')
                        require(re.fullmatch('[0-9]+', value) and int(value) < len(strings), 'Shared-string reference outside bounded roster')
                        literal = strings[int(value)]
                    elif formula is None and kind == 'inlineStr':
                        literal = ''.join(t.text or '' for t in cell.iter('{'+NS+'}t'))
                    recognized = HEADERS.get(normalize_header(literal)) if literal is not None and len(literal) <= 128 else None
                    if recognized:
                        headers.append({'cell': coordinate, 'label_literal': literal, 'category': recognized,
                                        'cell_xml_sha256': sha(ET.tostring(cell))})
                    else:
                        other.append({'cell': coordinate, 'cell_type': 'formula' if formula is not None else kind,
                                      'cell_xml_sha256': sha(ET.tostring(cell)), 'literal_exported': False})
                if number <= HEADER_ROWS:
                    categories = sorted({h['category'] for h in headers})
                    candidate = len(categories) >= 2
                    if not candidate:
                        other.extend({'cell': h['cell'], 'cell_type': 'recognized_word_in_unqualified_row',
                            'cell_xml_sha256': h['cell_xml_sha256'], 'literal_exported': False} for h in headers)
                    rows.append({'row': number, 'candidate_header': candidate,
                        'recognized_header_categories': categories if candidate else [],
                        'recognized_header_cells': headers if candidate else [],
                        'unclassified_cells': other, 'all_unclassified_literals_exported': False})
            result.append({'sheet_name_literal': title, 'sheet_state_literal': sheet.get('state', 'visible'),
                'worksheet_member': path, 'worksheet_xml_sha256': sha(read(path)), 'declared_dimension': declared,
                'maximum_row_index': max_row, 'maximum_column_index': max_col, 'cell_count': cells,
                'cell_type_counts': dict(sorted(types.items())), 'header_scan_maximum_row': HEADER_ROWS,
                'header_rows': rows, 'candidate_header_identified': any(r['candidate_header'] for r in rows),
                'unfamiliar_or_ambiguous_headers_require_review': not any(r['candidate_header'] for r in rows),
                'body_cell_literals_exported': False, **FALSE_FLAGS})
        check()
        return {'schema_version': 1, 'kind': 'stock_workbook_schema_only', 'ZIP_members': len(infos),
            'expanded_declared_bytes': expanded, 'worksheets': result, 'header_vocabulary_sha256': sha(json.dumps(HEADER_GROUPS, default=sorted, sort_keys=True).encode()),
            'workbook_join_or_control_edges_established': False, **FALSE_FLAGS}


def execute(*, deadline):
    require(time.monotonic() < deadline, 'Finite metadata audit deadline required')
    metadata = bounded(METADATA, METADATA_SHA, 20_000)
    transfer = bounded(TRANSFER, TRANSFER_SHA, 20_000)
    workbook = bounded(SOURCE, SOURCE_SHA, SOURCE_BYTES)
    bindings = source_bindings(metadata, transfer, workbook)
    value = member_inventory(workbook, deadline=deadline)
    require(bounded(METADATA, METADATA_SHA, 20_000) == metadata
            and bounded(TRANSFER, TRANSFER_SHA, 20_000) == transfer
            and bounded(SOURCE, SOURCE_SHA, SOURCE_BYTES) == workbook, 'Source changed during metadata audit')
    require(time.monotonic() < deadline, 'Metadata audit deadline exceeded')
    value.update(sources=bindings, audit_helper_sha256=sha(Path(__file__).read_bytes()))
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--max-seconds', type=int, default=900)
    args = parser.parse_args(argv)
    require(os.environ.get('SLURM_CPUS_PER_TASK') == '1' and os.environ.get('HHW_GPU_COUNT') == '0', 'One CPU/no GPU native context required')
    require(0 < args.max_seconds <= 900, 'Finite maximum900-second audit required')
    end = float(os.environ.get('SLURM_JOB_END_TIME', 'nan'))
    require(math.isfinite(end) and time.time()+args.max_seconds+120 < end, 'Native finalization margin required')
    output = ordinary(args.output, fresh=True)
    value = execute(deadline=time.monotonic()+args.max_seconds)
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False)+'\n').encode()
    require(len(raw) <= 5_000_000, 'Finite metadata report exceeded')
    with output.open('xb') as stream: stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    return 0


if __name__ == '__main__': raise SystemExit(main())
