"""Create an immutable namespace successor and preserve the required test fixture.

The fourteen-file qualification manifest and all thirteen suite rosters retain
their contracts. Only nine operational namespace assignments change. The exact
base controller is an additional pinned sibling fixture outside that manifest.
No scheduler or remote operation occurs here.
"""
from pathlib import Path
import ast
import copy
import hashlib
import importlib.util
import json

WORK = Path(__file__).resolve().parent
OLD = WORK / 'rolling-native-v4'
TARGET = WORK / 'rolling-native-v5'
OLD_MANIFEST_SHA = '43660c97e50c5a24d42abb321514405107b9f1cade649a492f1c91355486ecb8'
REBINDS = WORK / 'build-rolling-native-v3-20261023.py'
REBINDS_SHA = 'b317189bac94e26034d0217307ace7c34e2119c6d8fbe5bc40c917f4f32cc929'
BASE = 'run-af-agent-controller-registry-v6-20261022.py'
BASE_SHA = 'e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
DRIVER = 'controller-primary-repair-sixth-20261016.py'
DRIVER_SHA = '49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'
REPLACEMENTS = {
    'run-fresh-storage-census-v6-20261023.py': {
        'CASE_ROOT': ('maintenance/study-af-rolling-storage-census-v3-20261023', 'maintenance/study-af-rolling-storage-census-v4-20261023'),
        'SOURCE_SUPPORT': ('maintenance/study-af-rolling-census-support-v3-20261023', 'maintenance/study-af-rolling-census-support-v4-20261023'),
        'KERNEL_Q': ('maintenance/study-af-rolling-census-qualification-v4-20261023/qualification.json', 'maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json'),
        'PHASE': ('study-af-rolling-storage-census-v3-20261023', 'study-af-rolling-storage-census-v4-20261023'),
    },
    'qualify-streaming-census-v2-20261023.py': {
        'SUPPORT': ('maintenance/study-af-rolling-census-support-v3-20261023', 'maintenance/study-af-rolling-census-support-v4-20261023'),
        'CASE': ('maintenance/study-af-rolling-census-qualification-v4-20261023', 'maintenance/study-af-rolling-census-qualification-v5-20261023'),
    },
    'run-af-agent-controller-census-policy-v1-20261023.py': {
        'SUPPORT': ('maintenance/study-af-rolling-census-support-v3-20261023', 'maintenance/study-af-rolling-census-support-v4-20261023'),
        'QUALIFICATION': ('maintenance/study-af-rolling-census-qualification-v4-20261023/qualification.json', 'maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json'),
        'PROVIDER_CASE': ('maintenance/study-af-rolling-storage-census-v3-20261023', 'maintenance/study-af-rolling-storage-census-v4-20261023'),
    },
}


def sha(raw): return hashlib.sha256(raw).hexdigest()


def checked(path, expected):
    raw = Path(path).read_bytes()
    if sha(raw) != expected: raise ValueError('Frozen input changed: ' + str(path))
    return raw


def prepare():
    old = json.loads(checked(OLD / 'source-manifest.json', OLD_MANIFEST_SHA))
    if len(old['files']) != 14 or len(old['suites']) != 13 or sum(x['tests'] for x in old['suites'].values()) != 757:
        raise ValueError('Exact prior fourteen-file, thirteen-suite closure required')
    checked(REBINDS, REBINDS_SHA)
    spec = importlib.util.spec_from_file_location('strict_namespace_rebinder', REBINDS)
    lib = importlib.util.module_from_spec(spec); spec.loader.exec_module(lib)
    lib.REPLACEMENTS = REPLACEMENTS
    packets = {name: lib.rebound(name, checked(OLD / name, pin)) for name, pin in old['files'].items()}
    for name, raw in packets.items():
        ast.parse(raw, feature_version=(3, 11))
        if name.startswith('test_') and raw != (OLD / name).read_bytes():
            raise ValueError('Test source changed')
    manifest = copy.deepcopy(old)
    manifest['files'] = {name: sha(raw) for name, raw in packets.items()}
    if manifest['suites'] != old['suites']: raise ValueError('Suite contracts changed')
    raw = (json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    proof = {'kind': 'rolling-census-native-v5-namespace-derivation-v1',
             'previous_manifest': {'path': str(OLD / 'source-manifest.json'), 'sha256': OLD_MANIFEST_SHA},
             'source_manifest_sha256': sha(raw),
             'namespace_assignment_changes': {name: list(values) for name, values in REPLACEMENTS.items()},
             'all_operational_function_bodies_ast_identical': True, 'all_test_source_bytes_identical': True,
             'all_thirteen_suite_contracts_identical': True, 'native_expected_tests': 757,
             'additional_native_sibling_fixture': {'name': BASE, 'sha256': BASE_SHA, 'included_in_manifest_files': False},
             'local_only_registry_fixture': {'name': DRIVER, 'sha256': DRIVER_SHA},
             'native_qualification_claimed': False, 'existing_frozen_files_modified': False}
    return packets, raw, proof


def main():
    packets, raw, proof = prepare()
    packets[BASE] = checked(OLD / BASE, BASE_SHA)
    packets[DRIVER] = checked(OLD / DRIVER, DRIVER_SHA)
    packets['source-manifest.json'] = raw
    packets['namespace-derivation.json'] = (json.dumps(proof, sort_keys=True, indent=2) + '\n').encode()
    if any((TARGET / name).exists() for name in packets): raise ValueError('Never overwrite candidate bytes')
    TARGET.mkdir(exist_ok=True)
    for name, value in packets.items():
        with (TARGET / name).open('xb') as stream: stream.write(value)
    print(json.dumps({'directory': str(TARGET), 'source_manifest_sha256': sha(raw), 'files': json.loads(raw)['files'],
                      'native_qualification_claimed': False}, sort_keys=True))


if __name__ == '__main__': main()
