"""Create new, unsent v5 registration builders and a namespace-only coordinator."""
from pathlib import Path
import ast
import hashlib
import json

W = Path(__file__).resolve().parent
OLD = W / 'rolling-native-v4'
NEW = W / 'rolling-native-v5'
BASE = 'run-af-agent-controller-registry-v6-20261022.py'
BASE_SHA = 'e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
OLD_MANIFEST = '43660c97e50c5a24d42abb321514405107b9f1cade649a492f1c91355486ecb8'
OLD_V6 = '61001913e746537da3583c2844c760f6589b48be763a44f83be446d5d204a1da'
NAMESPACES = {'study-af-rolling-census-support-v3-20261023': 'study-af-rolling-census-support-v4-20261023',
              'study-af-rolling-census-qualification-v4-20261023': 'study-af-rolling-census-qualification-v5-20261023',
              'study-af-rolling-census-registration-v3-20261023': 'study-af-rolling-census-registration-v4-20261023',
              'study-af-rolling-storage-census-v3-20261023': 'study-af-rolling-storage-census-v4-20261023'}


def sha(raw): return hashlib.sha256(raw).hexdigest()


def replace(raw, changes):
    report = {}
    for before, after in changes.items():
        count = raw.count(before.encode())
        if not count: raise ValueError('Missing exact replacement: ' + before)
        raw = raw.replace(before.encode(), after.encode())
        report[before] = {'after': after, 'occurrences': count}
    ast.parse(raw, feature_version=(3, 11))
    return raw, report


def main():
    manifest = json.loads((NEW / 'source-manifest.json').read_bytes())
    mh = sha((NEW / 'source-manifest.json').read_bytes())
    v6 = manifest['files']['run-fresh-storage-census-v6-20261023.py']
    changes429 = dict(NAMESPACES, **{'work/rolling-native-v4': 'work/rolling-native-v5',
        'recovery-support-20261021-452': 'recovery-support-20261021-456', 'recovery-452': 'recovery-456'})
    changes430 = {k: v for k, v in NAMESPACES.items() if 'storage-census' not in k}
    changes430.update({"WORK_ROOT/'rolling-native-v4'": "WORK_ROOT/'rolling-native-v5'", "('416','452')": "('416','456')",
                       'q452': 'q456', OLD_MANIFEST: mh, OLD_V6: v6})
    contracts = [
        (OLD / 'build-rolling-census-registration-429.py', NEW / 'build-rolling-census-registration-429.py', changes429),
        (OLD / 'test_rolling_census_registration_429.py', NEW / 'test_rolling_census_registration_429.py', {}),
        (OLD / 'build-rolling-dependents-430-434.py', NEW / 'build-rolling-dependents-430-434.py', changes430),
        (OLD / 'test_rolling_dependents_430_434.py', NEW / 'test_rolling_dependents_430_434.py',
         {OLD_MANIFEST: mh, OLD_V6: v6, 'study-af-rolling-census-qualification-v4-20261023': 'study-af-rolling-census-qualification-v5-20261023'}),
    ]
    packets = {}
    records = []
    for source, target, changes in contracts:
        before = source.read_bytes(); after, explicit = replace(before, changes)
        packets[target] = after
        records.append({'name': target.name, 'before_sha256': sha(before), 'after_sha256': sha(after), 'explicit_replacements': explicit})
    source = W / 'build-native-portability-qualification-452.py'
    changes = {'work/rolling-native-v4': 'work/rolling-native-v5',
               'study-af-rolling-census-support-v3-20261023': 'study-af-rolling-census-support-v4-20261023',
               "ID='452'": "ID='456'", 'recovery-support-20261021-452': 'recovery-support-20261021-456',
               'hhw-recovery-stage-20261021-452.sh': 'hhw-recovery-stage-20261021-456.sh'}
    before = source.read_bytes(); after, explicit = replace(before, changes)
    marker = b'sources={}\n'
    if after.count(marker) != 1: raise ValueError('Exact qualification staging loop required')
    fixture = ("# Additional exact sibling fixture required by the external policy tests.\n"
               "fixture_name=" + repr(BASE) + ";fixture_sha=" + repr(BASE_SHA) + "\n"
               "assert fixture_name not in files\n"
               "fixture_body=(W/fixture_name).read_bytes();assert hashlib.sha256(fixture_body).hexdigest()==fixture_sha\n"
               "ast.parse(fixture_body,feature_version=(3,11))\n"
               "sources[fixture_name]={'data':base64.b64encode(zlib.compress(fixture_body,9)).decode(),'sha256':fixture_sha}\n").encode()
    after = after.replace(marker, marker + fixture, 1)
    ast.parse(after, feature_version=(3, 11))
    packets[W / 'build-native-fixture-qualification-456.py'] = after
    records.append({'name': 'build-native-fixture-qualification-456.py', 'before_sha256': sha(before), 'after_sha256': sha(after),
                    'explicit_replacements': explicit, 'additional_stage_fixture': {'name': BASE, 'sha256': BASE_SHA}})
    csource = W / 'coordinate-reviewed-registrations-v3-20261023.py'
    cbefore = csource.read_bytes()
    cchanges = {'coordinate-reviewed-registrations-v3-20261023.py': 'coordinate-reviewed-registrations-v4-20261023.py',
                'study-af-reviewed-registration-coordinator-v3-20261023': 'study-af-reviewed-registration-coordinator-v4-20261023',
                'study-af-rolling-census-qualification-v4-20261023': 'study-af-rolling-census-qualification-v5-20261023',
                'study-af-rolling-census-support-v3-20261023': 'study-af-rolling-census-support-v4-20261023',
                'recovery-support-20261021-452': 'recovery-support-20261021-456', 'recovery-452': 'recovery-456', 'Q452': 'Q456'}
    cafter, explicit = replace(cbefore, cchanges)
    packets[W / 'coordinate-reviewed-registrations-v4-20261023.py'] = cafter
    records.append({'name': 'coordinate-reviewed-registrations-v4-20261023.py', 'before_sha256': sha(cbefore), 'after_sha256': sha(cafter),
                    'explicit_replacements': explicit})
    # Keep each historical preservation test tied to its actual historical file.
    test = (W / 'test_coordinate_reviewed_registrations_v3_20261023.py').read_text()
    test = test.replace("SOURCE=Path(__file__).with_name('coordinate-reviewed-registrations-v3-20261023.py')",
                        "SOURCE=Path(__file__).with_name('coordinate-reviewed-registrations-v4-20261023.py')", 1)
    old_function = next(n for n in ast.parse(test).body if isinstance(n, ast.FunctionDef) and n.name == 'test_v3_changes_only_namespace_and_q452_literals_from_frozen_v2')
    old_segment = ast.get_source_segment(test, old_function)
    fixed_segment = old_segment.replace('ast.parse(SOURCE.read_text())', "ast.parse(SOURCE.with_name('coordinate-reviewed-registrations-v3-20261023.py').read_text())")
    test = test.replace(old_segment, fixed_segment, 1)
    # Runtime fixtures must use the successor's actual registered Q.
    start = test.index('@pytest.fixture\ndef frozen(')
    test = test[:start] + test[start:].replace('recovery-support-20261021-452', 'recovery-support-20261021-456').replace('recovery-452', 'recovery-456')
    added = "\n\ndef test_v4_changes_only_namespace_and_q456_literals_from_frozen_v3():\n"
    added += "    original=SOURCE.with_name('coordinate-reviewed-registrations-v3-20261023.py')\n"
    added += '    assert digest(original)==' + repr(sha(cbefore)) + '\n'
    added += '    replacements=' + repr(cchanges) + '\n'
    added += "    class Rebind(ast.NodeTransformer):\n        def visit_Constant(self,node):\n            if isinstance(node.value,str):\n                for before,after in replacements.items():node.value=node.value.replace(before,after)\n            return node\n"
    added += "    assert ast.dump(Rebind().visit(ast.parse(original.read_text())),include_attributes=False)==ast.dump(ast.parse(SOURCE.read_text()),include_attributes=False)\n"
    packets[W / 'test_coordinate_reviewed_registrations_v4_20261023.py'] = (test + added).encode()
    proof = {'kind': 'unsubmitted-registration-namespace-derivation-v1', 'new_source_manifest_sha256': mh,
             'sources': records, 'scheduler_submitted': False}
    packets[NEW / 'registration-namespace-derivation.json'] = (json.dumps(proof, sort_keys=True, indent=2) + '\n').encode()
    for target, raw in packets.items():
        if target.exists(): raise ValueError('Never overwrite existing candidate: ' + str(target))
        if target.suffix == '.py': ast.parse(raw, feature_version=(3, 11))
    for target, raw in packets.items():
        with target.open('xb') as stream: stream.write(raw)
    print(json.dumps({'source_manifest_sha256': mh, 'created': [str(p) for p in packets], 'scheduler_submitted': False}, sort_keys=True))


if __name__ == '__main__': main()
