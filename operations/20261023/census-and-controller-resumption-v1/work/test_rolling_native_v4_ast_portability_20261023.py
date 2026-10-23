"""Adversarial local evidence for the narrow AST-schema portability repair."""
from pathlib import Path
import ast
import copy
import hashlib
import importlib.util
import json

import pytest

WORK = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('native_four_builder', WORK / 'build-rolling-native-v4-20261023.py')
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
spec = importlib.util.spec_from_file_location('portable_wrapper_tests', WORK / 'rolling-native-v4' / b.TEST)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)


@pytest.mark.parametrize('source', [
    '@decorator\ndef f(a: int=3) -> int:\n return a+1\n',
    'async def f(a):\n await a()\n',
    '@decorator\nclass C(Base):\n def f(self):\n  return []\n',
])
def test_only_empty_type_parameter_schema_is_omitted_and_original_tree_is_unchanged(source):
    tree = ast.parse(source, feature_version=(3, 11))
    before = ast.dump(tree, include_attributes=True)
    expected = copy.deepcopy(tree)
    for node in ast.walk(expected):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if hasattr(node, 'type_params'):
                del node.type_params
            node._fields = tuple(field for field in node._fields if field != 'type_params')
    assert t.python311_ast_dump(tree) == ast.dump(expected, include_attributes=False)
    assert ast.dump(tree, include_attributes=True) == before


@pytest.mark.parametrize('source', ['def f():\n pass\n', 'async def f():\n pass\n', 'class C:\n pass\n'])
def test_nonempty_type_parameters_are_rejected_even_when_the_host_does_not_parse_them(source):
    tree = ast.parse(source, feature_version=(3, 11))
    tree.body[0].type_params = [ast.Name(id='T', ctx=ast.Load())]
    with pytest.raises(ValueError, match='Nonempty'):
        t.python311_ast_dump(tree)


def test_type_parameters_on_an_unrelated_node_are_rejected():
    tree = ast.parse('x=1')
    tree.body[0].type_params = []
    with pytest.raises(ValueError, match='unexpected'):
        t.python311_ast_dump(tree)


def test_other_empty_ast_fields_and_semantic_changes_are_preserved():
    a = ast.parse('def f(a=1):\n return a+1\n', feature_version=(3, 11))
    text = t.python311_ast_dump(a)
    assert 'decorator_list=[]' in text and 'kwonlyargs=[]' in text and 'kw_defaults=[]' in text
    mutated = ast.parse('def f(a=1):\n return a+2\n', feature_version=(3, 11))
    assert t.python311_ast_dump(mutated) != text
    mutated = ast.parse('def f(a=2):\n return a+1\n', feature_version=(3, 11))
    assert t.python311_ast_dump(mutated) != text


def test_actual_native_python311_hash_is_reproduced_from_exact_pinned_source():
    raw = b.checked(b.OLD / b.WRAPPER, b.WRAPPER_SHA)
    node = next(n for n in ast.parse(raw, feature_version=(3, 11)).body if isinstance(n, ast.FunctionDef) and n.name == 'qualification_proof')
    actual = hashlib.sha256(t.python311_ast_dump(node).encode()).hexdigest()
    assert actual == '68bd48e464f7b9a85691f936cf5458bef1d153bb29452f926c7adc2c077f5d36'
    diagnostic = json.loads(b.checked(b.DIAGNOSTIC, b.DIAGNOSTIC_SHA).decode().split('NATIVE_WRAPPER_FAILURE ', 1)[1].strip())
    assert actual in diagnostic['suites']['wrapper.xml']['failures'][0]['details']


def test_whole_original_operational_source_hash_is_enforced_before_ast_checks(tmp_path, monkeypatch):
    source = tmp_path / b.WRAPPER
    raw = (b.OLD / b.WRAPPER).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == b.WRAPPER_SHA
    source.write_bytes(raw + b'\nUNREVIEWED_EXTRA=True\n')
    monkeypatch.setattr(t, 'SOURCE', source)
    with pytest.raises(AssertionError):
        t.test_legacy_positive_proofs_and_native_guard_sources_are_preserved()


def test_all_thirteen_suites_have_only_the_one_reviewed_hardcoded_ast_hash_test():
    previous = json.loads(b.checked(b.OLD / 'source-manifest.json', b.OLD_MANIFEST_SHA))
    evidence = b.ast_audit(previous)
    hashes = [(scope, row['function']) for scope, rows in evidence.items() for row in rows if row['hardcoded_cross_interpreter_sha']]
    assert hashes == [('wrapper', 'test_legacy_positive_proofs_and_native_guard_sources_are_preserved')]


def test_exact_manifest_and_fourteen_source_closure_are_preserved_except_reviewed_changes():
    packets, raw, proof = b.prepare()
    old = json.loads((b.OLD / 'source-manifest.json').read_bytes())
    new = json.loads(raw)
    assert len(packets) == 14 and len(new['suites']) == 13 and sum(c['tests'] for c in new['suites'].values()) == 757
    assert new['suites']['baseline_budget'] == old['suites']['baseline_budget']
    for scope, contract in old['suites'].items():
        assert new['suites'][scope] == (dict(contract, source_sha256=new['files'][b.TEST]) if scope == 'wrapper' else contract)
    changed = {name for name, h in new['files'].items() if h != old['files'][name]}
    assert changed == set(b.REPLACEMENTS) | {b.TEST}
    for name in b.REPLACEMENTS:
        b.builder3().namespace_only(name, (b.OLD / name).read_bytes(), packets[name])
    for name in set(packets) - changed:
        assert packets[name] == (b.OLD / name).read_bytes()
    assert proof['all_test_ids_counts_unchanged'] and proof['all_operational_function_bodies_ast_identical']
    assert not proof['native_qualification_claimed']
