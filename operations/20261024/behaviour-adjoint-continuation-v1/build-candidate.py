"""Build an operational derivative; preserve every numerical function verbatim."""
from pathlib import Path
import ast
import hashlib
import json

AREA = Path(__file__).resolve().parent
BASE = AREA/'run-full-record-adjoint-v3-20261021.py'
PREFIX = AREA.parents[0]/'behaviour-supervisor-reconciliation-v1/lifecycle/run-full-cohort-prefix-v5-exit-race-repaired-20261024.py'
TARGET = AREA/'run-full-record-adjoint-v5prefix-v1-20261024.py'


def node_source(raw, node):
    return b''.join(raw.splitlines(keepends=True)[node.lineno-1:node.end_lineno])


def build():
    original = BASE.read_bytes()
    assert hashlib.sha256(original).hexdigest() == '815292c4323a03eb1e9cd83499afd9ca3adb0eec8cdcecb56913fb05f79c3ea1'
    tree = ast.parse(original)
    source = original.decode()
    replacements = {
        "CONTEXT_SHA='396e0ec6f9cf5936884fdd08d23e5e3d6e92f9bb06c654d747d04b2b27338787'": "CONTEXT_SHA='e055352de535d029c658723b69707c371ab5d89e451cace506d305c344a3fd0f'",
        "PREFIX_HELPER_SHA='b131d6317858eae3c43c2ae7337f46e3173ab64355693980af2e7e489af46d3d'": "PREFIX_HELPER_SHA='dec3ace9591b71759bbd7418b550e657ecb87a85823652e9d1e56547fe19ec60'",
        "PREFIX_TEST_SHA='01b50175e001a44db682befdfd2728602d4ad467b5b8345092e5d3fa6d9ed80c'": "PREFIX_TEST_SHA='874cfce84a7814536e6442a00775d7ad3b652ef7ad12a087e81585edd0b88169'",
        "PREFIX_CASE=ROOT/'maintenance/behaviour-full-cohort-prefix-v3-20261021'": "PREFIX_CASE=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-20261022'",
        "PREFIX_SUPPORT=ROOT/'maintenance/behaviour-full-cohort-prefix-v3-support-20261021'": "PREFIX_SUPPORT=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-support-20261022'",
        "CASE=ROOT/'maintenance/behaviour-full-record-adjoint-v3-20261021'": "CASE=ROOT/'maintenance/behaviour-full-record-adjoint-v5prefix-v1-20261024'",
        "SUPPORT=ROOT/'maintenance/behaviour-full-record-adjoint-v3-support-20261021'": "SUPPORT=ROOT/'maintenance/behaviour-full-record-adjoint-v5prefix-support-v1-20261024'",
    }
    for before, after in replacements.items():
        assert source.count(before) == 1, before
        source = source.replace(before, after)
    # The numerical worker and preparation are unchanged; only these generic
    # lifecycle/ingress/qualification/orchestration functions may differ.
    operations = (AREA/'operational-functions.py.txt').read_text()
    operation_tree = ast.parse(operations)
    by_name = {n.name: ast.get_source_segment(operations, n) for n in operation_tree.body if isinstance(n, ast.FunctionDef)}
    lifecycle = PREFIX.read_bytes()
    lifecycle_nodes = {n.name: n for n in ast.parse(lifecycle).body if isinstance(n, ast.FunctionDef)}
    by_name['stop'] = node_source(lifecycle, lifecycle_nodes['stop']).decode()
    by_name['supervise'] = node_source(lifecycle, lifecycle_nodes['supervise']).decode()
    for name in ('prefix_evidence', 'qualify_private', 'stop', 'supervise'):
        old = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
        before = node_source(original, old).decode()
        assert source.count(before) == 1, name
        source = source.replace(before, by_name[name])
    main_node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
    main = node_source(original, main_node).decode()
    new_main = main.replace('    args=parser.parse_args()',
        "    parser.add_argument('--source-manifest-sha256',required=True)\n"
        "    parser.add_argument('--verifier-source-manifest-sha256',required=True)\n"
        "    args=parser.parse_args()\n"
        "    global CAPSULE_MANIFEST,VERIFIER_SOURCE_MANIFEST_SHA\n"
        "    CAPSULE_MANIFEST=verify_capsule(args.source_manifest_sha256)\n"
        "    VERIFIER_SOURCE_MANIFEST_SHA=args.verifier_source_manifest_sha256\n"
        "    require(re.fullmatch('[0-9a-f]{64}',VERIFIER_SOURCE_MANIFEST_SHA),'Pinned verifier bundle SHA required')")
    new_main = new_main.replace("sorted({args.qualification_job_id,args.prefix_job_id})", "sorted({args.qualification_job_id,args.prefix_job_id,ORIGINAL_JOB_ID})")
    old_reader = """    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'Guard must precede campaign reader binding')
    reader=runpy.run_path(str(context.SCHEDULER))['run_scontrol_job']
    allocation=context.native_allocation(job,args.native_script,reader)"""
    new_reader = """    reader=runpy.run_path(str(context.SCHEDULER))['run_scontrol_job']
    allocation=context.native_allocation(job,args.native_script,reader)
    from horsehair import repair
    repair._repair_native_states=context.bounded_native_reader(repair._repair_native_states,
        SUPPORT/'bounded-native-reads',allocation['allocation_end_unix'])
    from horsehair import campaign
    require(campaign._repair_native_states is repair._repair_native_states,'Bounded accounting must precede campaign import')"""
    assert new_main.count(old_reader) == 1
    new_main = new_main.replace(old_reader,new_reader)
    new_main = new_main.replace("os.environ.get('HHW_MAX_HOURS')=='2'", "os.environ.get('HHW_MAX_HOURS')=='8'")
    new_main = new_main.replace("'Owned finite1CPU native adjoint context required'", "'Owned finite1CPU/eight-hour native adjoint context required'")
    new_main = new_main.replace("resource_policy=policy,cpus=0,gpus=0,hours=2", "resource_policy=policy,cpus=0,gpus=0,hours=8")
    new_main = new_main.replace("cpus=1,gpus=0,max_hours=2", "cpus=1,gpus=0,max_hours=8")
    new_main = new_main.replace("        from horsehair.behaviour_feasibility import runtime_limit\n", "")
    new_main = new_main.replace("seconds=runtime_limit(6300,allocation_end=os.environ['SLURM_JOB_END_TIME'])", "seconds=adjoint_runtime_seconds(os.environ['SLURM_JOB_END_TIME'])")
    new_main = new_main.replace("        result.update(native_job_id=job", "        verify_capsule(args.source_manifest_sha256)\n        result.update(native_job_id=job")
    new_main = new_main.replace("        return 1 if result['status']=='worker_failed' else 0", "        return 1 if result['status'] in {'worker_failed','worker_termination_unconfirmed'} else 0")
    assert new_main != main
    source = source.replace(main, new_main)
    additions = '\n\n' + '\n\n'.join(ast.get_source_segment(operations, n) for n in operation_tree.body
        if not isinstance(n, ast.FunctionDef) or n.name not in {'prefix_evidence', 'qualify_private'}) + '\n\n'
    assert source.count(new_main) == 1
    source = source.replace(new_main, additions + new_main)
    candidate = source.encode()
    parsed = ast.parse(candidate)
    permitted = {'prefix_evidence', 'qualify_private', 'stop', 'supervise', 'main'}
    changed = []
    preserved = {}
    candidates = {n.name:n for n in parsed.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            continue
        other = candidates[node.name]
        identical = node_source(original, node) == node_source(candidate, other)
        if not identical:
            changed.append(node.name)
            assert node.name in permitted, node.name
        else:
            preserved[node.name] = hashlib.sha256(node_source(original, node)).hexdigest()
    assert set(changed) == permitted
    TARGET.write_bytes(candidate)
    (AREA/'preservation.json').write_text(json.dumps({
        'historical_transport_sha256': '120d3ec88267748ad0f36d03527702f7b5f163ce2f4c5b0aa0e455e6a1b3abca',
        'baseline_normalization': 'Historical CRLF snapshot normalized to LF for this new capsule; AST unchanged',
        'baseline_sha256': hashlib.sha256(original).hexdigest(),
        'candidate_sha256': hashlib.sha256(candidate).hexdigest(),
        'changed_existing_functions': changed,
        'preserved_function_and_class_bytes_sha256': preserved,
        'scientific_configuration_changed': False,
        'numerical_functions_executed_by_builder': False,
        'full_coverage_or_scientific_flags_promoted': False,
    }, sort_keys=True, indent=2)+'\n',newline='\n')
    print(json.dumps({'candidate':str(TARGET), 'changed_functions':changed, 'preserved_definitions':len(preserved)}))


if __name__ == '__main__':
    build()
