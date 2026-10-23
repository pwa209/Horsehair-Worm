"""Local namespace and real fixture staging contracts, with no native execution."""
from pathlib import Path
import ast
import base64
import hashlib
import importlib.util
import json
import runpy
import zlib

import pytest

W = Path(__file__).resolve().parent
N = W / 'rolling-native-v5'
O = W / 'rolling-native-v4'
BASE = 'run-af-agent-controller-registry-v6-20261022.py'
BASE_SHA = 'e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
BUILDER = W / 'build-native-fixture-qualification-456.py'


def sha(raw): return hashlib.sha256(raw).hexdigest()


def unpack(raw):
    body = raw.decode().split("python -u - <<'PY'\n", 1)[1].rsplit('\nPY', 1)[0]
    tree = ast.parse(body, feature_version=(3, 11))
    values = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in {'payload', 'manifest_raw'} for t in node.targets):
            exec(compile(ast.Module(body=[node], type_ignores=[]), '<static-source-data-only>', 'exec'),
                 {'json': json, 'zlib': zlib, 'base64': base64}, values)
        if isinstance(node, ast.For) and isinstance(node.iter, ast.Call) and isinstance(node.iter.func, ast.Attribute) and isinstance(node.iter.func.value, ast.Dict):
            values['sources'] = ast.literal_eval(node.iter.func.value)
    return body, tree, values


def test_frozen_v4_to_v5_exact_nine_assignments_and_all_test_bytes_are_unchanged():
    spec = importlib.util.spec_from_file_location('namespace_derivation', W / 'build-rolling-native-v5-20261023.py')
    lib = importlib.util.module_from_spec(spec); spec.loader.exec_module(lib)
    packets, manifest_raw, proof = lib.prepare()
    assert manifest_raw == (N / 'source-manifest.json').read_bytes()
    assert sum(len(x) for x in lib.REPLACEMENTS.values()) == 9
    before = json.loads((O / 'source-manifest.json').read_bytes())
    after = json.loads(manifest_raw)
    assert after['suites'] == before['suites'] and sum(x['tests'] for x in after['suites'].values()) == 757
    for name, raw in packets.items():
        assert raw == (N / name).read_bytes()
        if name.startswith('test_'): assert raw == (O / name).read_bytes()
    assert BASE not in after['files'] and len(after['files']) == 14
    assert sha((N / BASE).read_bytes()) == BASE_SHA
    assert proof['native_qualification_claimed'] is False


def test_actual_456_stage_includes_exact_pinned_sibling_and_every_manifest_source_once():
    raw = (N / 'hhw-recovery-stage-20261021-456.sh').read_bytes()
    body, tree, values = unpack(raw)
    manifest = json.loads((N / 'source-manifest.json').read_bytes())
    payload, sources = values['payload'], values['sources']
    assert payload['id'] == '456' and payload['cpus'] == 8 and payload['memory'] == '64G' and payload['hours'] == 1
    assert payload['area'].endswith('study-af-rolling-census-support-v4-20261023')
    assert set(sources) == (set(manifest['files']) - {payload['helper']}) | {BASE}
    assert len(sources) == 14 and BASE not in manifest['files']
    assert zlib.decompress(base64.b64decode(payload['helper_body'])) == (N / payload['helper']).read_bytes()
    for name, item in sources.items():
        actual = zlib.decompress(base64.b64decode(item['data']))
        assert actual == (N / name).read_bytes() and sha(actual) == item['sha256']
    assert sources[BASE]['sha256'] == BASE_SHA
    assert values['manifest_raw'] == (N / 'source-manifest.json').read_bytes()
    flags = dict(zip(payload['args'][::2], payload['args'][1::2]))
    assert len(flags) == 6 and flags['--native-task'] == 'recovery-456'
    assert flags['--native-plan'].endswith('recovery-support-20261021-456/plan.json')
    assert flags['--source-manifest-sha256'] == sha(values['manifest_raw'])
    assert "task['resources']['array']=None" in body and "task['rolling_census_qualification']" in body
    assert body.index("with (area/name).open('xb')") < body.index("with (area/'source-manifest.json').open('xb')") < body.index('submitted=submit')
    assert 'recovery-support-20261021-375' not in body and 'recovery-support-20261021-452' not in body
    assert b'\r' not in raw
    compile(tree, '<local-456-envelope>', 'exec')


@pytest.fixture
def copied_stage(tmp_path, monkeypatch):
    directory = tmp_path / 'work/rolling-native-v5'; directory.mkdir(parents=True)
    manifest = json.loads((N / 'source-manifest.json').read_bytes())
    for name in [*manifest['files'], BASE, 'source-manifest.json']:
        (directory / name).write_bytes((N / name).read_bytes())
    (tmp_path / 'work/hhw-recovery-stage-20261021-394.sh').write_bytes((W / 'hhw-recovery-stage-20261021-394.sh').read_bytes())
    monkeypatch.chdir(tmp_path)
    return directory, manifest


def test_local_rebuild_reproduces_exact_456_bytes_without_registering(copied_stage):
    directory, _ = copied_stage
    runpy.run_path(str(BUILDER), run_name='__main__')
    assert (directory / 'hhw-recovery-stage-20261021-456.sh').read_bytes() == (N / 'hhw-recovery-stage-20261021-456.sh').read_bytes()
    assert not (directory / 'hhw-recovery-stage-20261021-452.sh').exists()


@pytest.mark.parametrize('change', ['missing_fixture', 'changed_fixture', 'changed_private', 'missing_private', 'wrong_manifest_kind', 'missing_manifest_file', 'missing_suite', 'template_changed'])
def test_missing_or_changed_fixture_and_sources_refuse_before_registration_artifact(copied_stage, change):
    directory, manifest = copied_stage
    if change == 'missing_fixture': (directory / BASE).unlink()
    elif change == 'changed_fixture': (directory / BASE).write_bytes((directory / BASE).read_bytes() + b'\n# altered\n')
    elif change == 'changed_private': (directory / next(iter(manifest['files']))).write_text('# altered')
    elif change == 'missing_private': (directory / next(iter(manifest['files']))).unlink()
    elif change == 'wrong_manifest_kind': manifest['kind'] = 'wrong'
    elif change == 'missing_manifest_file': manifest['files'].pop(next(iter(manifest['files'])))
    elif change == 'missing_suite': manifest['suites'].pop(next(iter(manifest['suites'])))
    else: (directory.parent / 'hhw-recovery-stage-20261021-394.sh').write_text('# altered')
    (directory / 'source-manifest.json').write_text(json.dumps(manifest))
    with pytest.raises((AssertionError, FileNotFoundError)): runpy.run_path(str(BUILDER), run_name='__main__')
    assert not (directory / 'hhw-recovery-stage-20261021-456.sh').exists()


def test_preexisting_456_candidate_is_never_overwritten(copied_stage):
    directory, _ = copied_stage
    output = directory / 'hhw-recovery-stage-20261021-456.sh'
    output.write_bytes(b'preserved local candidate\n')
    with pytest.raises(AssertionError): runpy.run_path(str(BUILDER), run_name='__main__')
    assert output.read_bytes() == b'preserved local candidate\n'
