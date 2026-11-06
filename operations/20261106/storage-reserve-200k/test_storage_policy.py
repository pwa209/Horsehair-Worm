from pathlib import Path
import hashlib
import importlib.util
import json
import os
import ast
from types import CodeType,FunctionType
import pytest


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


m = load('approved_storage_policy', Path(__file__).with_name('storage_policy.py'))


@pytest.fixture
def policy(tmp_path, monkeypatch):
    root = tmp_path/'study'; directory = root/'maintenance'/m.POLICY_ID
    directory.mkdir(parents=True)
    record = json.loads(Path(__file__).with_name('policy.json').read_bytes())
    record['study_root'] = str(root)
    path = directory/'policy.json'
    path.write_text(json.dumps(record), encoding='utf-8')
    monkeypatch.setattr(m, 'ROOT', root)
    def verify():
        return m.verify_policy(path, hashlib.sha256(path.read_bytes()).hexdigest(), root=root)
    return root, path, record, verify


@pytest.mark.parametrize('required,reserve,total', [(0,199995,200000), (2000,197995,200000),
    (18565,181430,200000), (199994,1,200000), (199995,0,200000),
    (199996,0,200001), (250000,0,250005), (1000000,0,1000005)])
def test_total_floor_includes_receipt_overhead_and_prospective_demand(policy, required, reserve, total):
    p = policy[3]()
    assert m.reserve_for(required,p) == reserve
    assert required+reserve+5 == total
    assert p.receipt()['minimum_personal_free_files'] == 200000
    assert p.receipt()['study_file_cap_changed'] is False
    assert p.receipt()['byte_limits_changed'] is False


@pytest.mark.parametrize('bad', [-1, True, 1.0, '2000', None])
def test_noninteger_demand_rejected(policy,bad):
    with pytest.raises(ValueError):m.reserve_for(bad,policy[3]())


def test_exact_boundary_with_existing_quota_reader_contract(policy):
    reserve=m.reserve_for(2000,policy[3]());needed=2000+reserve+5
    assert 199999 < needed
    assert 200000 >= needed


def test_policy_bytes_rechecked_after_verification(policy):
    root,path,record,verify=policy;p=verify()
    path.write_text(path.read_text()+' ')
    with pytest.raises(ValueError,match='bytes changed'):m.reserve_for(2000,p)


@pytest.mark.parametrize('key,value', [('minimum_personal_free_files',200005),
    ('minimum_personal_free_files',200000.0),('unchanged_study_file_cap',300000),
    ('unchanged_scratch_budget_bytes',7000000000000),('unchanged_scratch_reserve_bytes',0),
    ('execution_source_sha256','0'*64),('base_config_sha256','0'*64),
    ('scientific_gates_changed',True),('receipt_file_overhead',0),('schema_version',True)])
def test_rehashed_descriptor_cannot_change_scope_or_frozen_limits(policy,key,value):
    root,path,record,verify=policy;record[key]=value;path.write_text(json.dumps(record))
    with pytest.raises(ValueError):verify()


def test_explicit_user_approval_required(policy):
    root,path,record,verify=policy
    record['approval']['authorizes_personal_file_reserve_amendment']=False
    path.write_text(json.dumps(record))
    with pytest.raises(ValueError,match='provenance'):verify()


def test_duplicate_policy_fields_rejected_even_with_matching_sha(policy):
    root,path,record,verify=policy
    path.write_text(path.read_text()[:-1]+',"minimum_personal_free_files":200000}')
    with pytest.raises(ValueError,match='Duplicate'):verify()


def test_redirected_policy_namespace_rejected(policy,tmp_path):
    root,path,record,verify=policy;other=tmp_path/'policy.json';other.write_bytes(path.read_bytes())
    with pytest.raises(ValueError,match='namespace'):
        m.verify_policy(other,hashlib.sha256(other.read_bytes()).hexdigest(),root=root)


@pytest.mark.skipif(os.name=='nt',reason='Windows link permission is host-dependent')
def test_symlink_policy_rejected(policy,tmp_path):
    root,path,record,verify=policy;target=tmp_path/'original.json';target.write_bytes(path.read_bytes())
    path.unlink();path.symlink_to(target)
    with pytest.raises(ValueError):verify()


@pytest.mark.skipif(os.name=='nt',reason='Windows link permission is host-dependent')
def test_hardlinked_policy_rejected(policy,tmp_path):
    root,path,record,verify=policy;os.link(path,tmp_path/'second-link.json')
    with pytest.raises(ValueError,match='single-link'):verify()


def original_function(tmp_path):
    path=tmp_path/'source.py'
    path.write_text('''def guard(forecast_files, *, forecast_bytes=700):
    return reader(required_files=forecast_files, reserve_files=max(0,250000-forecast_files),
                  required_bytes=forecast_bytes, reserve_bytes=2000000000000)
''')
    module=load('exact_original_guard',path)
    module.reader=lambda **kwargs:kwargs
    return module,path,hashlib.sha256(path.read_bytes()).hexdigest()


def derive(module,sha,p,**overrides):
    kwargs={'expected_source_sha256':sha,'expected_reserve_expression':'max(0,250000-forecast_files)',
            'required_files_expression':'forecast_files','expected_count':1,**overrides}
    return m.derive_reserved_function(module.guard,p,**kwargs)


def test_private_ast_reserve_amendment_preserves_bytes_demand_and_original_module(policy,tmp_path):
    module,path,sha=original_function(tmp_path);original=module.guard
    private,proof=derive(module,sha,policy[3]())
    assert module.guard is original
    assert original(2000)['reserve_files']==248000
    assert private(2000)=={'required_files':2000,'reserve_files':197995,
        'required_bytes':700,'reserve_bytes':2000000000000}
    assert private(250000)['reserve_files']==0
    assert proof['reverse_transform_ast_equal'] and proof['original_module_unchanged']
    assert '__verified_file_reserve' not in module.__dict__


@pytest.mark.parametrize('override',[{'expected_source_sha256':'0'*64},
    {'expected_reserve_expression':'max(0,200000-forecast_files)'},
    {'required_files_expression':'other_demand'}, {'expected_count':2}, {'expected_count':True}])
def test_ast_change_rejects_wrong_source_expression_demand_or_count(policy,tmp_path,override):
    module,path,sha=original_function(tmp_path)
    with pytest.raises(ValueError):derive(module,sha,policy[3](),**override)


def test_private_ast_reader_revalidates_policy_for_every_quota_call(policy,tmp_path):
    module,path,sha=original_function(tmp_path);private,proof=derive(module,sha,policy[3]())
    assert private(2000)['reserve_files']==197995
    policy[1].write_text(policy[1].read_text()+' ')
    with pytest.raises(ValueError):private(2000)


def test_already_derived_function_is_not_silently_replaced(policy,tmp_path):
    module,path,sha=original_function(tmp_path);p=policy[3]();private,proof=derive(module,sha,p)
    module.guard=private
    with pytest.raises(ValueError,match='Already-derived'):derive(module,sha,p)


def test_actual_frozen_af_base_has_the_single_declared_reserve_boundary(policy):
    path=Path(__file__).resolve().parents[2]/'20261024/f2-backfill-controller-v2/run-af-agent-controller-backfill-base-v2-20261024.py'
    if not path.exists():
        path=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-backfill-controller-support-v2-20261024/run-af-agent-controller-backfill-base-v2-20261024.py')
    assert hashlib.sha256(path.read_bytes()).hexdigest()=='cbf01cffbbaa9a983ec1e8e6394b38ac04623483e768398c18f48b5e30e31445'
    namespace={'__file__':str(path),'__name__':'actual_frozen_storage_guard'}
    compiled=compile(path.read_bytes(),str(path),'exec',dont_inherit=True)
    code=next(item for item in compiled.co_consts if isinstance(item,CodeType) and item.co_name=='storage_guard')
    original=FunctionType(code,namespace);namespace['storage_guard']=original
    private,proof=m.derive_reserved_function(original,policy[3](),
        expected_source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        expected_reserve_expression='max(0,250000-forecast_files)',
        required_files_expression='forecast_files')
    assert namespace['storage_guard'] is original
    assert private is not original and proof['quota_reserve_expressions_changed']==1
