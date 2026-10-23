"""Immutable namespace successor with one strict Python 3.11 AST test repair.

No native qualification is claimed. Operational function bodies are unchanged;
only namespaces and the cross-version representation in one test are adjusted.
"""
from pathlib import Path
import ast
import copy
import hashlib
import importlib.util
import json

WORK = Path(__file__).resolve().parent
OLD = WORK / 'rolling-native-v3'
TARGET = WORK / 'rolling-native-v4'
OLD_MANIFEST_SHA = 'fefcb2cd1e5f7b70ebab7e959955d96c9ea4072dd133370adf2f6a8e6cb03ff0'
BUILDER3 = WORK / 'build-rolling-native-v3-20261023.py'
BUILDER3_SHA = 'b317189bac94e26034d0217307ace7c34e2119c6d8fbe5bc40c917f4f32cc929'
DIAGNOSTIC = WORK / 'narval-deployment/hhw-native-wrapper-failure-20261023-447.txt'
DIAGNOSTIC_SHA = '3232bef94fe85edfdd56673eeea81d1fd75bbbbb5e38c0a38f4ccd66809e1d3c'
WRAPPER = 'run-fresh-storage-census-v5-20261023.py'
WRAPPER_SHA = 'b448607b00512ef865a3369da56c18bf6f98ab589b6a524795e5bbed9c64e569'
TEST = 'test_run_fresh_storage_census_v5_20261023.py'
BASE = 'run-af-agent-controller-registry-v6-20261022.py'
BASE_SHA = 'e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
EXPECTED = {
    'qualification_proof': '68bd48e464f7b9a85691f936cf5458bef1d153bb29452f926c7adc2c077f5d36',
    'registry_qualification': '20d66af05da0652a5b93126044934640ba5d9037a46a1e65ae3e6b34c79317eb',
    'completed_registry_qualification': '0ea782feed8d7c1e1e80679a11489b48ce03f01f9a0c8af1c58e3963b630bc55',
    'brief_admission': 'd21d78dd7f743a984689ed077ead366487a7f33ef03717c603c59c0e9658e898',
    'snapshot_clocks': '7c2e9ab7af427fd49fd0e45507e2840923c02aacafcf70d7ca2911dc1c9e34e2',
}
REPLACEMENTS = {
    'run-fresh-storage-census-v6-20261023.py': {
        'CASE_ROOT': ('maintenance/study-af-rolling-storage-census-v2-20261023', 'maintenance/study-af-rolling-storage-census-v3-20261023'),
        'SOURCE_SUPPORT': ('maintenance/study-af-rolling-census-support-v2-20261023', 'maintenance/study-af-rolling-census-support-v3-20261023'),
        'KERNEL_Q': ('maintenance/study-af-rolling-census-qualification-v3-20261023/qualification.json', 'maintenance/study-af-rolling-census-qualification-v4-20261023/qualification.json'),
        'PHASE': ('study-af-rolling-storage-census-v2-20261023', 'study-af-rolling-storage-census-v3-20261023'),
    },
    'qualify-streaming-census-v2-20261023.py': {
        'SUPPORT': ('maintenance/study-af-rolling-census-support-v2-20261023', 'maintenance/study-af-rolling-census-support-v3-20261023'),
        'CASE': ('maintenance/study-af-rolling-census-qualification-v3-20261023', 'maintenance/study-af-rolling-census-qualification-v4-20261023'),
    },
    'run-af-agent-controller-census-policy-v1-20261023.py': {
        'SUPPORT': ('maintenance/study-af-rolling-census-support-v2-20261023', 'maintenance/study-af-rolling-census-support-v3-20261023'),
        'QUALIFICATION': ('maintenance/study-af-rolling-census-qualification-v3-20261023/qualification.json', 'maintenance/study-af-rolling-census-qualification-v4-20261023/qualification.json'),
        'PROVIDER_CASE': ('maintenance/study-af-rolling-storage-census-v2-20261023', 'maintenance/study-af-rolling-storage-census-v3-20261023'),
    },
}

HELPER = '''def python311_ast_dump(tree):
    """Strict 3.11 representation; only its absent empty type_params is omitted."""
    tree=copy.deepcopy(tree)
    for node in ast.walk(tree):
        if hasattr(node,'type_params'):
            if not isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)) or type(node.type_params) is not list or node.type_params:
                raise ValueError('Nonempty or unexpected type parameters are not Python3.11 source')
            del node.type_params
    return ast.dump(tree,include_attributes=False)


'''


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def checked(path, expected):
    raw = Path(path).read_bytes()
    require(sha(raw) == expected, 'Frozen input changed: ' + str(path))
    return raw


def builder3():
    checked(BUILDER3, BUILDER3_SHA)
    spec = importlib.util.spec_from_file_location('native_namespace_three_builder', BUILDER3)
    lib = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lib)
    lib.REPLACEMENTS = REPLACEMENTS
    return lib


def portable_test(raw):
    text = raw.decode()
    tree = ast.parse(text, feature_version=(3, 11))
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'test_legacy_positive_proofs_and_native_guard_sources_are_preserved')
    previous = ast.get_source_segment(text, function)
    replacement = '''def test_legacy_positive_proofs_and_native_guard_sources_are_preserved():
    assert hashlib.sha256(SOURCE.read_bytes()).hexdigest()==WRAPPER_SHA_LITERAL
    expected=EXPECTED_LITERAL
    new=ast.parse(SOURCE.read_text(),feature_version=(3,11))
    for name,sha in expected.items():
        function=next(n for n in new.body if isinstance(n,ast.FunctionDef) and n.name==name)
        assert hashlib.sha256(python311_ast_dump(function).encode()).hexdigest()==sha
    assert m.SUPPORT.name=='study-af-fresh-storage-census-v5-20261023'
    assert m.GUARD_SHA=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
    assert m.EXTENDED_REGISTRY_SHA=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a' '''.rstrip().replace('WRAPPER_SHA_LITERAL', repr(WRAPPER_SHA)).replace('EXPECTED_LITERAL', repr(EXPECTED))
    newline = '\r\n' if '\r\n' in text else '\n'
    require(text.count(previous) == 1 and text.count('import ast' + newline) == 1, 'Exact original failed test/import required')
    text = text.replace('import ast' + newline, 'import ast' + newline + 'import copy' + newline, 1).replace(previous, (HELPER + replacement).replace('\n', newline), 1)
    result = text.encode()
    before = ast.parse(raw, feature_version=(3, 11))
    after = ast.parse(result, feature_version=(3, 11))
    old_functions = {n.name: ast.dump(n, include_attributes=False) for n in before.body if isinstance(n, ast.FunctionDef)}
    new_functions = {n.name: ast.dump(n, include_attributes=False) for n in after.body if isinstance(n, ast.FunctionDef)}
    changed = 'test_legacy_positive_proofs_and_native_guard_sources_are_preserved'
    require(set(new_functions) == set(old_functions) | {'python311_ast_dump'} and all(new_functions[n] == v for n, v in old_functions.items() if n != changed), 'Another test/helper body changed')
    return result


def ast_audit(manifest):
    evidence = {}
    hardcoded = []
    for scope, contract in manifest['suites'].items():
        path = WORK.parent / contract['source'] if scope.startswith('baseline_') else OLD / contract['source']
        raw = checked(path, contract['source_sha256'])
        tree = ast.parse(raw, feature_version=(3, 11))
        rows = []
        for function in (n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))):
            for node in ast.walk(function):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or not isinstance(node.func.value, ast.Name) or node.func.value.id != 'ast' or node.func.attr != 'dump':
                    continue
                hashes = [parent for parent in ast.walk(function) if isinstance(parent, ast.Call) and isinstance(parent.func, ast.Attribute) and parent.func.attr == 'sha256' and any(child is node for child in ast.walk(parent))]
                row = {'function': function.name, 'ast_dump_line': node.lineno, 'hardcoded_cross_interpreter_sha': bool(hashes)}
                rows.append(row)
                if hashes:
                    hardcoded.append((scope, function.name))
        evidence[scope] = rows
    require(hardcoded == [('wrapper', 'test_legacy_positive_proofs_and_native_guard_sources_are_preserved')], 'Another cross-interpreter AST hash needs explicit review')
    return evidence


def prepare():
    old = json.loads(checked(OLD / 'source-manifest.json', OLD_MANIFEST_SHA))
    require(len(old['files']) == 14 and len(old['suites']) == 13 and sum(x['tests'] for x in old['suites'].values()) == 757, 'Exact prior closure required')
    lib = builder3()
    packets = {name: lib.rebound(name, checked(OLD / name, h)) for name, h in old['files'].items()}
    packets[TEST] = portable_test(packets[TEST])
    audit = ast_audit(old)
    diagnostic_raw = checked(DIAGNOSTIC, DIAGNOSTIC_SHA)
    rows = [line.split(' ', 1)[1] for line in diagnostic_raw.decode().splitlines() if line.startswith('NATIVE_WRAPPER_FAILURE ')]
    require(len(rows) == 1, 'One preserved actual native failure diagnostic required')
    diagnostic = json.loads(rows[0]);native = diagnostic['suites']['wrapper.xml']
    require(diagnostic['read_only'] is True and native['tests'] == 122 and len(native['failures']) == 1
            and native['failures'][0]['name'] == 'test_legacy_positive_proofs_and_native_guard_sources_are_preserved'
            and EXPECTED['qualification_proof'] in native['failures'][0]['details'], 'Actual observed native Python3.11 first AST hash required')
    require(sha(packets[WRAPPER]) == WRAPPER_SHA, 'Operational wrapper bytes must stay unchanged')
    namespace = {'copy': copy, 'ast': ast}
    exec(HELPER, namespace)
    tree = ast.parse(packets[WRAPPER], feature_version=(3, 11))
    hashes = {name: sha(namespace['python311_ast_dump'](next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)).encode()) for name in EXPECTED}
    require(hashes == EXPECTED, 'Strict normalized AST differs from pinned original source')
    manifest = copy.deepcopy(old)
    manifest['files'] = {name: sha(raw) for name, raw in packets.items()}
    manifest['suites']['wrapper']['source_sha256'] = manifest['files'][TEST]
    raw = (json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    proof = {'kind': 'rolling-census-python311-ast-portability-evidence-v1',
             'previous_manifest': {'path': str(OLD / 'source-manifest.json'), 'sha256': OLD_MANIFEST_SHA},
             'source_manifest_sha256': sha(raw), 'source_manifest_native_path': '/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v3-20261023/source-manifest.json',
             'native_diagnostic': {'path': str(DIAGNOSTIC), 'sha256': sha(diagnostic_raw)},
             'native_job_id': '6860420', 'native_wrapper_junit_sha256': native['sha256'], 'native_wrapper_tests': 122,
             'native_wrapper_passed': 121, 'native_failed_test': native['failures'][0]['name'],
             'actual_native_first_ast_sha256': EXPECTED['qualification_proof'], 'python311_ast_sha256': EXPECTED,
             'original_wrapper_source_sha256': WRAPPER_SHA, 'modified_test_source': TEST,
             'namespace_assignment_changes': {name: list(values) for name, values in REPLACEMENTS.items()},
             'all_operational_function_bodies_ast_identical': True,
             'portable_representation_rule': 'deepcopy; delete only empty list type_params on FunctionDef/AsyncFunctionDef/ClassDef; reject nonempty or unexpected; all remaining AST fields unchanged',
             'all_thirteen_suite_ast_audit': audit, 'all_test_ids_counts_unchanged': True,
             'linux_budget_roster_unchanged': manifest['suites']['baseline_budget']['roster_sha256'],
             'native_qualification_claimed': False, 'existing_frozen_files_modified': False}
    return packets, raw, proof


def main():
    packets, raw, proof = prepare()
    packets[BASE] = checked(OLD / BASE, BASE_SHA)
    packets['source-manifest.json'] = raw
    packets['ast-portability-evidence.json'] = (json.dumps(proof, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    require(all(not (TARGET / name).exists() for name in packets), 'Never overwrite existing candidate bytes')
    TARGET.mkdir(exist_ok=True)
    for name, value in packets.items():
        with (TARGET / name).open('xb') as stream:
            stream.write(value)
    print(json.dumps({'directory': str(TARGET), 'source_manifest_sha256': sha(raw), 'files': json.loads(raw)['files'], 'native_qualification_claimed': False}, sort_keys=True))


if __name__ == '__main__':
    main()
