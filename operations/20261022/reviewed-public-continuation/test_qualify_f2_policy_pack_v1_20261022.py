"""Mocked producer contracts; never submit, access policy data, or call Slurm."""
from pathlib import Path
from types import SimpleNamespace
import ast
import hashlib
import importlib.util
import os

import pytest
from horsehair.core import canonical_hash,digest

SOURCE=Path(__file__).with_name('qualify-f2-policy-pack-v1-20261022.py')
spec=importlib.util.spec_from_file_location('private_pack_qualifier',SOURCE)
q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)


def loc(path):return {'path':str(path),'sha256':digest(Path(path))}


def test_complete_actual_junit_roster_is_verified_and_source_bound(tmp_path):
    test=tmp_path/'test_example.py';test.write_text('# disposable contract source')
    junit=tmp_path/'junit.xml';junit.write_text('<testsuite tests="1" failures="0" errors="0" skipped="0">'
        '<testcase classname="qualified.test_example" name="test_one"/></testsuite>')
    expected={'sha256':digest(test),'tests':1,'roster_sha256':canonical_hash([('test_example','test_one')])}
    module=SimpleNamespace(PRIVATE_TESTS={test.name:expected},canonical_hash=canonical_hash,digest=digest,locator=loc)
    value=q.verify_suite(module,junit,test,expected)
    assert value['tests']=={'tests':1,'failures':0,'errors':0,'skipped':0} and value['junit']==loc(junit)


@pytest.mark.parametrize('change',['missing','duplicate','skip','failure','error','counter','source','wrong_name','entity','oversize'])
def test_partial_or_unbound_junit_never_qualifies(tmp_path,change):
    test=tmp_path/'test_example.py';test.write_text('# disposable')
    body='<testcase classname="test_example" name="test_one"/>'
    if change=='missing':body=''
    if change=='duplicate':body*=2
    if change in {'skip','failure','error'}:
        element='skipped' if change=='skip' else change
        body='<testcase classname="test_example" name="test_one"><'+element+'/></testcase>'
    if change=='wrong_name':body=body.replace('test_one','test_other')
    count='2' if change=='counter' else '1'
    raw=('<testsuite tests="'+count+'" failures="0" errors="0" skipped="0">'+body+'</testsuite>').encode()
    if change=='entity':raw=b'<!DOCTYPE testsuite [<!ENTITY x "value">]>'+raw
    if change=='oversize':raw=b' '*8_000_001
    junit=tmp_path/'junit.xml';junit.write_bytes(raw)
    expected={'sha256':digest(test),'tests':1,'roster_sha256':canonical_hash([('test_example','test_one')])}
    module=SimpleNamespace(PRIVATE_TESTS={test.name:expected},canonical_hash=canonical_hash,digest=digest,locator=loc)
    if change=='source':test.write_text('# changed')
    with pytest.raises((ValueError,AssertionError)):q.verify_suite(module,junit,test,expected)


def test_child_environment_preserves_vendor_paths_but_removes_pytest_selectors(monkeypatch,tmp_path):
    from horsehair import rna_recovery
    monkeypatch.setattr(rna_recovery,'_primary_environment',lambda context:{'PYTHONPATH':'vendor:qualified4f',
        'PYTEST_ADDOPTS':'-k skipped','PYTEST_PLUGINS':'foreign','PYTHONHOME':'foreign','PYTHONINSPECT':'1'})
    module=SimpleNamespace(FRAMEWORK=tmp_path/'qualified4f',VALIDATION_FRAMEWORK=tmp_path/'newf047')
    environment=q.child_environment(module)
    assert environment['PYTHONPATH']=='vendor:qualified4f' and environment['HHW_PACK_FIXTURE_ROOT']==str(tmp_path/'newf047')
    assert environment['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1' and environment['PYTHONNOUSERSITE']=='1'
    assert not {'PYTEST_ADDOPTS','PYTEST_PLUGINS','PYTHONHOME','PYTHONINSPECT'}&environment.keys()


@pytest.mark.parametrize('change',['valid','wrong_sha','foreign_parent','wrong_name','wrong_completed_q'])
def test_pack_loader_compiles_only_explicit_hash_bound_supported_source(tmp_path,monkeypatch,change):
    root=tmp_path/'root';support=root/'maintenance/support';support.mkdir(parents=True)
    monkeypatch.setattr(q,'ROOT',root);monkeypatch.setattr(q,'SUPPORT',support)
    path=support/'run-f2-policy-archive-pack-v1-20261021.py'
    source='from pathlib import Path\nROOT=Path('+repr(str(root))+')\nPRIVATE_TESTS={"test":1}\nVALIDATION_Q='+repr({
        'job_id':'6703313','sha256':'8c3e1910deabea6c1061ddb353c3a6881fb70fb9fe86948007557a03ca809176'})+'\n'
    if change=='wrong_completed_q':source=source.replace('6703313','6703314')
    if change=='foreign_parent':path=root/path.name
    if change=='wrong_name':path=path.with_name('unapproved.py')
    path.write_text(source);sha=digest(path)
    if change=='wrong_sha':sha='0'*64
    if change=='valid':assert q.load_pack(path,sha).ROOT==root
    else:
        with pytest.raises(ValueError):q.load_pack(path,sha)


def test_native_registration_packet_binds_qualification_source_and_all_inputs(tmp_path,monkeypatch):
    seen=[];pack=tmp_path/'pack.py';test=tmp_path/'test_pack.py';driver=tmp_path/'qual.py'
    for path in (pack,test,driver):path.write_text('# synthetic source')
    monkeypatch.setattr(q,'CASE',tmp_path/'stage');monkeypatch.setattr(q,'__file__',str(driver))
    args=SimpleNamespace(native_plan=tmp_path/'plan',native_ledger=tmp_path/'ledger',native_script=tmp_path/'script',
        native_task='q',pack_driver=pack,test_source=test)
    for name in ('native_plan','native_ledger','native_script'):getattr(args,name).write_text('{}')
    module=SimpleNamespace(locator=loc,VALIDATION_FRAMEWORK=tmp_path/'qualified-new',FRAMEWORK_Q={'fixed':'Q'},
        private_producer_registration=lambda value,binding:seen.append((value,binding)))
    value=q.registration(module,args,'900')
    assert value['native_job_id']=='900' and seen[0][0]['pack_driver']==loc(pack)
    assert seen[0][1]['producer']==loc(driver) and seen[0][1]['path']==str(tmp_path/'stage/qualification.json')


def test_whole_native_suite_has_no_selector_or_unlink_and_runs_in_allocated_tmp():
    tree=ast.parse(SOURCE.read_bytes());calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call)]
    assert not any(isinstance(n.func,ast.Attribute) and n.func.attr in {'unlink','rmtree','submit','cancel','release','pack_policy'}
        for n in calls)
    strings=[n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and type(n.value) is str]
    assert '-s' in strings and '--basetemp' in strings and '--rootdir' in strings
    assert not {'-k','--deselect','-m','--lf','sbatch','scancel'}&set(strings)-{'-m'}
    assert 'producer_native_completion' in strings and 'pending' in strings
