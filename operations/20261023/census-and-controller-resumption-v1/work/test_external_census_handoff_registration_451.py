"""Local envelope contracts with synthetic evidence, never native qualification."""
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
import ast
import base64
import copy
import importlib.util
import json
import shlex
import xml.etree.ElementTree as ET
import zlib

import pytest

W = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('handoff_registration_builder', W / 'build-external-census-handoff-registration-451.py')
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
GUARD = b.R + '/maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py'
GUARD_SHA = '5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'


@pytest.fixture
def fixture_graph(tmp_path, monkeypatch):
    """Explicitly invented local inputs exercise rendering, not test coverage."""
    work = tmp_path / 'work'
    work.mkdir()
    directory = work / 'synthetic-graph'
    directory.mkdir()
    helper = ("# Synthetic fixture only; never qualified or deployed.\n"
              "from pathlib import Path\nROOT=Path('/scratch/pwa209/horsehair-worm')\n"
              "Q2_SUPPORT=ROOT/'maintenance/study-af-rolling-census-support-v3-20261023'\n"
              "Q2_PATH=ROOT/'maintenance/study-af-rolling-census-qualification-v4-20261023/qualification.json'\n")
    (work / b.HELPER).write_text(helper, encoding='utf-8')
    (work / b.TEST).write_text('# Synthetic test source, not actual qualification.\n', encoding='utf-8')
    files = {}
    for i in range(14):
        name = 'fixture_%02d.py' % i
        raw = ('# Synthetic closure fixture %d\n' % i).encode()
        (directory / name).write_bytes(raw)
        files[name] = b.sha(raw)
    manifest = {'kind': 'streaming-census-operational-source-manifest-v2', 'files': files,
                'suites': {'fixture_%02d' % i: {} for i in range(13)}}
    (directory / 'source-manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    junit = tmp_path / 'synthetic-only.xml'
    suite = ET.Element('testsuite', tests='2', failures='0', errors='0', skipped='0')
    for name in ('test_fixture_a', 'test_fixture_b'):
        ET.SubElement(suite, 'testcase', classname=b.TEST[:-3], name=name)
    ET.ElementTree(suite).write(junit)
    monkeypatch.setattr(b, 'TEMPLATE', (W / 'hhw-recovery-stage-20261021-394.sh').resolve())
    monkeypatch.chdir(tmp_path)
    return directory, junit, manifest


def unpack(raw):
    source = raw.decode()
    body = source.split("python -u - <<'PY'\n", 1)[1].rsplit('\nPY', 1)[0]
    tree = ast.parse(body, feature_version=(3, 11))
    values = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in {'payload', 'manifest_raw'} for t in node.targets):
            exec(compile(ast.Module(body=[node], type_ignores=[]), '<static-data-only>', 'exec'),
                 {'json': json, 'base64': base64, 'zlib': zlib}, values)
        if isinstance(node, ast.For) and isinstance(node.iter, ast.Call) and isinstance(node.iter.func, ast.Attribute) and node.iter.func.attr == 'items' and isinstance(node.iter.func.value, ast.Dict):
            values['sources'] = ast.literal_eval(node.iter.func.value)
    return source, body, tree, values


def render(fixture_graph):
    directory, junit, _ = fixture_graph
    raw, manifest_raw, evidence = b.render(directory, junit)
    return raw, json.loads(manifest_raw), evidence, unpack(raw)


def test_exact_embedded_bytes_and_all_manifest_locators(fixture_graph):
    directory, junit, _ = fixture_graph
    raw, manifest, evidence, (_, _, _, values) = render(fixture_graph)
    payload = values['payload']
    helper_raw = (Path('work') / b.HELPER).read_bytes()
    test_raw = (Path('work') / b.TEST).read_bytes()
    assert zlib.decompress(base64.b64decode(payload['helper_body'])) == helper_raw
    assert payload['helper_sha256'] == b.sha(helper_raw) == manifest['files'][b.HELPER]
    assert set(values['sources']) == {b.TEST}
    assert zlib.decompress(base64.b64decode(values['sources'][b.TEST]['data'])) == test_raw
    assert values['sources'][b.TEST]['sha256'] == b.sha(test_raw) == manifest['files'][b.TEST]
    assert json.loads(values['manifest_raw']) == manifest
    assert manifest['q2_source_manifest'] == {'path': b.SUPPORT + '/source-manifest.json', 'sha256': b.sha((directory / 'source-manifest.json').read_bytes())}
    assert manifest['q2_registration'] == b.registration('452')
    assert manifest['activation_source'] == {'path': b.ACT, 'sha256': b.ACT_SHA}
    assert manifest['activation_registration'] == b.registration('432')
    assert manifest['provider_registration'] == {k: v for k, v in b.registration('429').items() if k in {'plan_path', 'ledger_path'}}
    assert payload['id'] == '451' and payload['area'] == b.AREA and payload['helper'] == b.HELPER
    flags = dict(zip(payload['args'][::2], payload['args'][1::2]))
    own = b.registration('451')
    assert flags == {'--native-plan': own['plan_path'], '--native-ledger': own['ledger_path'], '--native-task': own['task_name'],
                     '--native-script': own['script_path'], '--source-manifest': b.AREA + '/source-manifest.json',
                     '--source-manifest-sha256': b.sha(values['manifest_raw'])}
    assert evidence['qualification_claimed'] is evidence['scheduler_submitted'] is False
    assert evidence['junit'] == {'path': str(junit), 'sha256': b.sha(junit.read_bytes())}
    assert evidence['tests'] == {'count': 2, 'roster_sha256': b.canonical([b.TEST[:-3] + '::test_fixture_a', b.TEST[:-3] + '::test_fixture_b'])}
    assert b'\r' not in raw


def test_canonical_existing_guard_loaded_and_hashed_without_staging_copy(fixture_graph):
    _, _, _, (_, _, tree, values) = render(fixture_graph)
    nodes = tree.body
    start = next(i for i, node in enumerate(nodes) if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'api' for t in node.targets))
    observed = []
    def run_path(path):
        observed.append(('load', str(path)))
        return {'install': lambda *args, **kwargs: observed.append(('install', str(args[0]), kwargs))}
    def digest(path):
        observed.append(('hash', str(path)))
        return GUARD_SHA
    ns = {'root': PurePosixPath(b.R), 'area': PurePosixPath(b.AREA), 'runpy': SimpleNamespace(run_path=run_path),
          'digest': digest, 'time': SimpleNamespace(time=lambda: 100.0)}
    exec(compile(ast.Module(body=nodes[start:start + 3], type_ignores=[]), '<guard-binding-only>', 'exec'), ns)
    assert observed[:2] == [('load', GUARD), ('hash', GUARD)]
    assert observed[2] == ('install', b.AREA + '/submission-registry-reads',
                           {'registration_mode': 'controller-already-locked', 'allocation_deadline_unix': 1600.0})
    assert set(values['sources']) == {b.TEST}


def activation_context(payload):
    """Real 432 payload supplies its original four-key resource schema."""
    actual = (W / 'rolling-native-v4/hhw-recovery-stage-20261021-432.sh').read_bytes()
    _, _, _, actual_values = unpack(actual)
    actual_payload = actual_values['payload']
    command = shlex.join(['python', '-u', b.ACT, *actual_payload['args']])
    text = '# Native accounting bootstrap is a separate preceding command.\npython -u -c bootstrap\n' + command + '\n'
    source = 's' * 64
    config = {'synthetic_fixture': True}
    record = {'name': 'recovery-432', 'script': b.registration('432')['script_path'], 'script_sha256': b.canonical(text),
              'command': command, 'resources': {'cpus': actual_payload['cpus'], 'memory': actual_payload['memory'],
                                              'hours': actual_payload['hours'], 'gpu': False}}
    plan = {'tasks': [record], 'release_identity': {'sha256': source}, 'config_sha256': b.canonical(config)}
    ledger = {'plan_sha256': b.canonical(plan), 'source_sha256': source, 'config_sha256': b.canonical(config),
              'tasks': {'recovery-432': {'state': 'submitted', 'job_id': '1234567', 'script_sha256': record['script_sha256']}}}
    state = {'plan': plan, 'ledger': ledger, 'script_text': text, 'symlink': False, 'helper_sha256': b.ACT_SHA}
    class ScriptPath:
        def __init__(self, path): self.path = str(path)
        def __str__(self): return self.path
        def is_symlink(self): return state['symlink']
        def read_text(self): return state['script_text']
    def read_json(path):
        return {b.registration('432')['plan_path']: state['plan'], b.registration('432')['ledger_path']: state['ledger']}[str(path)]
    ns = {'payload': payload, 'root': PurePosixPath(b.R), 'area': PurePosixPath(b.AREA),
          'helper': PurePosixPath(b.AREA) / b.HELPER, 'name': 'recovery-451',
          'script': PurePosixPath(b.registration('451')['script_path']), 'text': '# synthetic451\n',
          'command': shlex.join(['python', '-u', b.AREA + '/' + b.HELPER, *payload['args']]),
          'config': config, 'identity': {'source_identity': {'sha256': source}}, 'read_json': read_json,
          'canonical_hash': b.canonical, 'Path': ScriptPath, 'digest': lambda path: state['helper_sha256'], 'shlex': shlex}
    return ns, state


def task_binding_nodes(tree):
    lock = next(node for node in tree.body if isinstance(node, ast.With) and any(isinstance(child, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'task' for t in child.targets) for child in node.body))
    start = next(i for i, node in enumerate(lock.body) if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'task' for t in node.targets))
    end = next(i for i, node in enumerate(lock.body[start:], start) if isinstance(node, ast.If) and ast.unparse(node.test) == "payload['after_specs']")
    return ast.Module(body=lock.body[start:end], type_ignores=[])


def test_real_432_task_shape_and_prefix_bind_to_exact_451_task_intent(fixture_graph):
    _, manifest, _, (_, body, tree, values) = render(fixture_graph)
    ns, _ = activation_context(values['payload'])
    exec(compile(task_binding_nodes(tree), '<actual-task-and-activation-bindings>', 'exec'), ns)
    task = ns['task']
    assert task['resources'] == {'cpus': 4, 'memory': '16G', 'hours': 2, 'gpu': False, 'array': None}
    assert task['external_census_handoff'] == {'helper': {'path': b.AREA + '/' + b.HELPER, 'sha256': manifest['files'][b.HELPER]},
                                               'source_manifest': {'path': b.AREA + '/source-manifest.json', 'sha256': b.sha(values['manifest_raw'])}}
    assert task['external_dependencies'] == ['1234567'] and task['dependency_kind'] == 'afterok' and task['depends'] == []
    assert shlex.split(task['command']) == ['python', '-u', b.AREA + '/' + b.HELPER, *values['payload']['args']]
    assert 'recovery-support-20261021-375' not in body and 'recovery-375' not in body


@pytest.mark.parametrize('change', ['plan_hash', 'source_identity', 'config_hash', 'duplicate_task', 'wrong_task_name',
                                  'wrong_resources', 'unexpected_array_key', 'ledger_not_submitted', 'wrong_script_path',
                                  'script_symlink', 'changed_script', 'ledger_script_hash', 'wrong_command_prefix',
                                  'changed_activation_helper', 'invalid_job_id'])
def test_source_bound_activation_mutations_refuse_before_submission(fixture_graph, change):
    _, _, _, (_, _, tree, values) = render(fixture_graph)
    ns, state = activation_context(values['payload'])
    plan, ledger = state['plan'], state['ledger']
    task = plan['tasks'][0]
    if change == 'plan_hash': ledger['plan_sha256'] = 'bad'
    elif change == 'source_identity': ledger['source_sha256'] = 'bad'
    elif change == 'config_hash': plan['config_sha256'] = 'bad'
    elif change == 'duplicate_task': plan['tasks'].append(copy.deepcopy(task))
    elif change == 'wrong_task_name': task['name'] = 'recovery-431'
    elif change == 'wrong_resources': task['resources']['memory'] = '32G'
    elif change == 'unexpected_array_key': task['resources']['array'] = None
    elif change == 'ledger_not_submitted': ledger['tasks']['recovery-432']['state'] = 'failed'
    elif change == 'wrong_script_path': task['script'] += '.other'
    elif change == 'script_symlink': state['symlink'] = True
    elif change == 'changed_script': state['script_text'] += '# changed\n'
    elif change == 'ledger_script_hash': ledger['tasks']['recovery-432']['script_sha256'] = 'bad'
    elif change == 'wrong_command_prefix':
        task['command'] = task['command'].replace('python -u ', 'python3 -u ', 1)
        state['script_text'] = task['command'] + '\n'
        task['script_sha256'] = ledger['tasks']['recovery-432']['script_sha256'] = b.canonical(state['script_text'])
    elif change == 'changed_activation_helper': state['helper_sha256'] = 'bad'
    elif change == 'invalid_job_id': ledger['tasks']['recovery-432']['job_id'] = '123_4'
    if change != 'plan_hash': ledger['plan_sha256'] = b.canonical(plan)
    with pytest.raises(AssertionError):
        exec(compile(task_binding_nodes(tree), '<mutated-activation-bindings>', 'exec'), ns)
    assert 'external_dependencies' not in ns.get('task', {})


def test_stage_writes_are_exclusive_and_admission_precedes_plan_and_submit(fixture_graph):
    _, _, _, (_, body, _, _) = render(fixture_graph)
    assert "with helper.open('xb')" in body
    assert "with (area/name).open('xb')" in body
    assert "with (area/'source-manifest.json').open('xb')" in body
    assert body.count("helper.open('xb')") == 1
    assert body.index("api['install']") < body.index("with api['registration_lock']") < body.index('admission=combined_admission')
    assert body.index('admission=combined_admission') < body.index('activation_plan=read_json') < body.index("atomic_json(directory/'plan.json'") < body.index('submitted=submit')
    assert "'Immutable stage cannot be replayed'" in body and "'Fresh handoff registration required'" in body
    assert 'automatic_replay_permitted=False' in body


@pytest.mark.parametrize('change', ['empty', 'duplicate', 'wrong_class', 'failure', 'error', 'skipped', 'summary_failure'])
def test_invalid_local_evidence_is_not_packaged(fixture_graph, change):
    directory, junit, _ = fixture_graph
    tree = ET.parse(junit)
    suite = tree.getroot()
    if change == 'empty':
        for case in list(suite): suite.remove(case)
    elif change == 'duplicate': suite[1].set('name', suite[0].get('name'))
    elif change == 'wrong_class': suite[0].set('classname', 'wrong_test_source')
    elif change in {'failure', 'error', 'skipped'}: ET.SubElement(suite[0], change)
    else: suite.set('failures', '1')
    tree.write(junit)
    with pytest.raises(AssertionError): b.render(directory, junit)
    assert not (directory / 'handoff-source-manifest.json').exists()


@pytest.mark.parametrize('change', ['source_bytes', 'wrong_kind', 'missing_file', 'missing_suite', 'wrong_helper_namespace', 'invalid_helper_ast', 'invalid_test_ast', 'template_changed'])
def test_changed_sources_and_lineage_fail_before_outputs(fixture_graph, change, monkeypatch):
    directory, junit, manifest = fixture_graph
    if change == 'source_bytes': (directory / next(iter(manifest['files']))).write_text('changed')
    elif change == 'wrong_kind': manifest['kind'] = 'other'
    elif change == 'missing_file': manifest['files'].pop(next(iter(manifest['files'])))
    elif change == 'missing_suite': manifest['suites'].pop(next(iter(manifest['suites'])))
    elif change == 'wrong_helper_namespace':
        helper = Path('work') / b.HELPER
        helper.write_text(helper.read_text().replace('support-v3-', 'support-v2-'))
    elif change == 'invalid_helper_ast': (Path('work') / b.HELPER).write_text('def :')
    elif change == 'invalid_test_ast': (Path('work') / b.TEST).write_text('def :')
    else: monkeypatch.setattr(b, 'TEMPLATE_SHA', 'bad')
    (directory / 'source-manifest.json').write_text(json.dumps(manifest))
    with pytest.raises((AssertionError, SyntaxError)): b.render(directory, junit)
    assert not (directory / 'hhw-recovery-stage-20261021-451.sh').exists()


@pytest.mark.parametrize('name', ['hhw-recovery-stage-20261021-451.sh', 'handoff-source-manifest.json', 'handoff-registration-local-evidence.json'])
def test_existing_local_candidate_is_preserved_without_render(fixture_graph, monkeypatch, name):
    directory, junit, _ = fixture_graph
    target = directory / name
    target.write_bytes(b'immutable existing candidate\n')
    monkeypatch.setattr(b, 'render', lambda *args: pytest.fail('Existing candidate must refuse before rendering'))
    with pytest.raises(AssertionError): b.main(['--directory', str(directory), '--junit', str(junit)])
    assert target.read_bytes() == b'immutable existing candidate\n'
