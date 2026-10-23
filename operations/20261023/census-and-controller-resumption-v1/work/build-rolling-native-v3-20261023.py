"""Create a separate namespace and bind the one observed Linux errno test ID.

This local-only builder changes no existing source and claims no new native
qualification. The fourteen existing private source/test filenames stay fixed.
"""
from pathlib import Path
import ast
import copy
import hashlib
import json
import xml.etree.ElementTree as ET

WORK = Path(__file__).resolve().parent
TARGET = WORK / 'rolling-native-v3'
MANIFEST = WORK / 'rolling-census-source-manifest-20261023.json'
MANIFEST_SHA = 'e407578d5838f11e2d12f02c3cc4821f401152ae689305b9fd7c8e7959fba8fc'
LOCAL_XML = WORK / 'rolling-census-complete-local-20261023.xml'
LOCAL_XML_SHA = 'a946904eb247d0335bef1d225354d3a3931121dd074b81c5c2c02bab0549890b'
NATIVE_DIAGNOSTIC = WORK / 'narval-deployment/hhw-rolling-qualification-roster-20261023-443.txt'
NATIVE_DIAGNOSTIC_SHA = 'dca2cf8e8320fd4d8407e2def342b0d2b8ead963abf6deb3349eb6f508690eec'
BASE = 'run-af-agent-controller-registry-v6-20261022.py'
BASE_SHA = 'e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
OLD_ID = 'test_budget::test_quota_receipt_write_failure_is_never_retried_or_reported_ready[10069]'
NEW_ID = 'test_budget::test_quota_receipt_write_failure_is_never_retried_or_reported_ready[122]'
NATIVE_ROSTER_SHA = 'a29d0c2854dafc41da5b6bcb7e31a03545bcad6134612b52a5f64c166bb52e7e'
NATIVE_XML_SHA = '049fd19a207c0c7dff0a426bb8d10dbf43dd6ea22a63e149bc412f5b45f18537'
OLD_SUPPORT = 'maintenance/study-af-rolling-census-support-v1-20261023'
NEW_SUPPORT = 'maintenance/study-af-rolling-census-support-v2-20261023'
OLD_QCASE = 'maintenance/study-af-rolling-census-qualification-v2-20261023'
NEW_QCASE = 'maintenance/study-af-rolling-census-qualification-v3-20261023'
OLD_PROVIDER = 'maintenance/study-af-rolling-storage-census-v1-20261023'
NEW_PROVIDER = 'maintenance/study-af-rolling-storage-census-v2-20261023'
REPLACEMENTS = {
    'run-fresh-storage-census-v6-20261023.py': {
        'CASE_ROOT': (OLD_PROVIDER, NEW_PROVIDER),
        'SOURCE_SUPPORT': (OLD_SUPPORT, NEW_SUPPORT),
        'KERNEL_Q': (OLD_QCASE + '/qualification.json', NEW_QCASE + '/qualification.json'),
        'PHASE': ('study-af-rolling-storage-census-v1-20261023', 'study-af-rolling-storage-census-v2-20261023'),
    },
    'qualify-streaming-census-v2-20261023.py': {
        'SUPPORT': (OLD_SUPPORT, NEW_SUPPORT),
        'CASE': (OLD_QCASE, NEW_QCASE),
    },
    'run-af-agent-controller-census-policy-v1-20261023.py': {
        'SUPPORT': (OLD_SUPPORT, NEW_SUPPORT),
        'QUALIFICATION': (OLD_QCASE + '/qualification.json', NEW_QCASE + '/qualification.json'),
        'PROVIDER_CASE': (OLD_PROVIDER, NEW_PROVIDER),
    },
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())


def checked(path, expected):
    raw = Path(path).read_bytes()
    require(sha(raw) == expected, 'Pinned input changed: ' + str(path))
    return raw


def namespace_only(name, before, after):
    """Prove that only the nine explicitly listed assignment values changed."""
    old = ast.parse(before, feature_version=(3, 11))
    new = ast.parse(after, feature_version=(3, 11))
    require(len(old.body) == len(new.body), 'Source statements changed')
    seen = set()
    for a, b in zip(old.body, new.body):
        key = a.targets[0].id if isinstance(a, ast.Assign) and len(a.targets) == 1 and isinstance(a.targets[0], ast.Name) else None
        if key in REPLACEMENTS.get(name, {}):
            old_value, new_value = REPLACEMENTS[name][key]
            expected = copy.deepcopy(a)
            values = [n for n in ast.walk(expected.value) if isinstance(n, ast.Constant) and n.value == old_value]
            require(len(values) == 1, 'Expected one exact old namespace constant')
            values[0].value = new_value
            require(ast.dump(expected, include_attributes=False) == ast.dump(b, include_attributes=False), 'Unexpected namespace assignment change')
            seen.add(key)
        else:
            require(ast.dump(a, include_attributes=False) == ast.dump(b, include_attributes=False), 'Non-namespace source changed')
    require(seen == set(REPLACEMENTS.get(name, {})), 'Namespace assignment missing')


def rebound(name, raw):
    result = raw
    for key, (old_value, new_value) in REPLACEMENTS.get(name, {}).items():
        lines = result.splitlines(keepends=True)
        selected = [i for i, line in enumerate(lines) if line.startswith((key + '=').encode())]
        require(len(selected) == 1 and lines[selected[0]].count(old_value.encode()) == 1, 'Exact assignment line required')
        lines[selected[0]] = lines[selected[0]].replace(old_value.encode(), new_value.encode())
        result = b''.join(lines)
    namespace_only(name, raw, result)
    return result


def baseline_evidence(original, local_raw, diagnostic_raw):
    xml = ET.fromstring(local_raw)
    cases = [x for x in xml.iter('testcase') if x.get('classname', '').rsplit('.', 1)[-1] == 'test_budget']
    require(len(cases) == 46 and all(not any(n.tag in {'failure', 'error', 'skipped'} for n in x) for x in cases), 'Actual passing local budget baseline required')
    local = sorted(x.get('classname').rsplit('.', 1)[-1] + '::' + x.get('name') for x in cases)
    require(len(set(local)) == 46 and canonical(local) == original['suites']['baseline_budget']['roster_sha256'], 'Original actual baseline roster changed')
    lines = [x[len('ROLLING_Q_ROSTERS '):] for x in diagnostic_raw.decode().splitlines() if x.startswith('ROLLING_Q_ROSTERS ')]
    require(len(lines) == 1, 'One actual native roster diagnostic required')
    value = json.loads(lines[0])
    require(value['read_only'] is True and set(value['suites']) == {'baseline_budget'}, 'Exact native baseline diagnostic required')
    native = value['suites']['baseline_budget']
    require(native['expected'] == original['suites']['baseline_budget'] and native['tests'] == 46 and native['failed'] == []
            and native['sha256'] == NATIVE_XML_SHA and native['roster_sha256'] == NATIVE_ROSTER_SHA
            and '46 passed' in native['-stdout.txt'] and native['-stderr.txt'] == '', 'Actual successful native budget execution required')
    roster = native['roster']
    require(type(roster) is list and len(roster) == len(set(roster)) == 46 and roster == sorted(roster)
            and canonical(roster) == NATIVE_ROSTER_SHA, 'Actual native roster bytes changed')
    require(set(local) - set(roster) == {OLD_ID} and set(roster) - set(local) == {NEW_ID}, 'Only the observed EDQUOT parameter ID may differ')
    require(sorted(NEW_ID if x == OLD_ID else x for x in local) == roster, 'No coverage substitution permitted')
    return {'scope': 'baseline_budget', 'source': original['suites']['baseline_budget']['source'],
            'source_sha256': original['suites']['baseline_budget']['source_sha256'], 'tests': 46,
            'removed_case_id': OLD_ID, 'added_case_id': NEW_ID,
            'original_roster_sha256': canonical(local), 'native_roster_sha256': canonical(roster),
            'native_actual_xml_sha256': native['sha256'], 'native_job_id': '6859130',
            'native_actual_xml_path': '/scratch/pwa209/horsehair-worm/' + OLD_QCASE + '/baseline_budget.xml',
            'test_body_changed': False, 'roster_normalization_applied': False,
            'other_case_ids_identical': True}


def parameter_audit(manifest):
    """Record errno expressions in parameter declarations; numeric ones fail closed."""
    evidence = {}
    direct = []
    for scope, contract in manifest['suites'].items():
        path = WORK.parent / contract['source'] if scope.startswith('baseline_') else WORK / contract['source']
        raw = checked(path, contract['source_sha256'])
        tree = ast.parse(raw, feature_version=(3, 11))
        observed = []
        for function in (n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))):
            for decorator in function.decorator_list:
                if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute) or decorator.func.attr != 'parametrize':
                    continue
                for node in ast.walk(decorator):
                    if not isinstance(node, ast.Attribute) or not isinstance(node.value, ast.Name) or node.value.id != 'errno':
                        continue
                    wrappers = [parent for parent in ast.walk(decorator) if isinstance(parent, ast.Call) and parent is not decorator
                                and any(child is node for child in ast.walk(parent))]
                    enclosed = [ast.unparse(parent.func) for parent in wrappers]
                    numeric = not enclosed
                    row = {'test': function.name, 'errno': node.attr, 'numeric_parameter': numeric, 'enclosing_calls': enclosed}
                    observed.append(row)
                    if numeric:
                        direct.append((scope, function.name, node.attr))
        evidence[scope] = observed
    require(sorted(direct) == [('baseline_budget', 'test_quota_receipt_write_failure_is_never_retried_or_reported_ready', 'EDQUOT'),
                              ('baseline_budget', 'test_quota_receipt_write_failure_is_never_retried_or_reported_ready', 'ENOSPC')],
            'Unexpected numeric errno parameter requires independent native roster review')
    return evidence


def prepare():
    original_raw = checked(MANIFEST, MANIFEST_SHA)
    original = json.loads(original_raw)
    require(len(original['files']) == 14 and len(original['suites']) == 13 and sum(x['tests'] for x in original['suites'].values()) == 757, 'Exact prior fourteen-source/thirteen-suite closure required')
    packets = {name: rebound(name, checked(WORK / name, expected)) for name, expected in original['files'].items()}
    baseline = baseline_evidence(original, checked(LOCAL_XML, LOCAL_XML_SHA), checked(NATIVE_DIAGNOSTIC, NATIVE_DIAGNOSTIC_SHA))
    audit = parameter_audit(original)
    derived = copy.deepcopy(original)
    derived['files'] = {name: sha(raw) for name, raw in packets.items()}
    derived['suites']['baseline_budget']['roster_sha256'] = baseline['native_roster_sha256']
    for scope, contract in derived['suites'].items():
        if not scope.startswith('baseline_'):
            require(contract['source_sha256'] == derived['files'][contract['source']], 'Private test bytes may not change')
    changed = sorted(name for name in packets if derived['files'][name] != original['files'][name])
    require(changed == sorted(REPLACEMENTS), 'Exactly three runtime source files must change')
    manifest_raw = (json.dumps(derived, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    proof = {'kind': 'rolling-census-native-namespace-and-errno-roster-evidence-v1',
             'old_manifest': {'path': str(MANIFEST), 'sha256': MANIFEST_SHA},
             'new_native_manifest_path': '/scratch/pwa209/horsehair-worm/' + NEW_SUPPORT + '/source-manifest.json',
             'source_manifest_sha256': sha(manifest_raw), 'namespace_assignments': {name: list(values) for name, values in REPLACEMENTS.items()},
             'changed_private_sources': changed, 'unchanged_private_sources': sorted(set(packets) - set(changed)),
             'all_function_bodies_ast_identical': True, 'all_private_tests_byte_identical': True,
             'baseline_difference': baseline, 'parameter_errno_audit': audit,
             'local_baseline_junit': {'path': str(LOCAL_XML), 'sha256': LOCAL_XML_SHA},
             'native_diagnostic': {'path': str(NATIVE_DIAGNOSTIC), 'sha256': NATIVE_DIAGNOSTIC_SHA},
             'external_base_local_fixture': {'path': BASE, 'sha256': BASE_SHA, 'outside_fourteen_private_sources': True},
             'native_qualification_claimed': False, 'existing_files_modified': False}
    return packets, manifest_raw, proof


def main():
    packets, manifest_raw, proof = prepare()
    packets[BASE] = checked(WORK / BASE, BASE_SHA)
    packets['source-manifest.json'] = manifest_raw
    packets['namespace-portability-evidence.json'] = (json.dumps(proof, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    require(all(not (TARGET / name).exists() for name in packets), 'Never overwrite existing namespace evidence')
    TARGET.mkdir(exist_ok=True)
    for name, raw in packets.items():
        with (TARGET / name).open('xb') as stream:
            stream.write(raw)
    print(json.dumps({'directory': str(TARGET), 'manifest_sha256': sha(manifest_raw),
                      'files': json.loads(manifest_raw)['files'], 'tests': 757, 'native_qualification_claimed': False}, sort_keys=True))


if __name__ == '__main__':
    main()
