"""Self-contained wrapper mocks; no sibling references beyond this wrapper."""
import ast
import importlib.util
import json
from pathlib import Path
import subprocess
import time
import xml.etree.ElementTree as ET

import pytest

SOURCE=Path(__file__).with_name('run-vtl-track-schema-audit-v2-20261021.py')
spec=importlib.util.spec_from_file_location('vtl_schema_wrapper_private',SOURCE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def junit(path,kind,change=None):
    names=m.TEST_NAMES[kind]
    suite=ET.Element('testsuite',tests=str(len(names)),failures='0',errors='0',skipped='0')
    base={'schema':'test_audit_vtl_track_schema_20261021','acquisition':'test_acquire_vtl_track_canary_20261021'}[kind]
    for name in names:ET.SubElement(suite,'testcase',classname=base,name=name,time='0.1')
    if change:change(suite)
    path.write_bytes(ET.tostring(suite));return path


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    root=tmp_path/'personal';support=root/'maintenance/support';support.mkdir(parents=True)
    case=root/'maintenance/inventory';docs=root/'maintenance/docs';docs.mkdir()
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',support);monkeypatch.setattr(m,'CASE',case)
    auditor=docs/'audit-vtl-track-schema-20261021.py';auditor.write_bytes(b'synthetic auditor never executed')
    raw=docs/'2019-11-10_2_11-order.txt';raw.write_bytes(b'GENERATED_TEST_BYTES_ONLY')
    acq=docs/'acquisition-result.json';acq.write_text('{}')
    monkeypatch.setattr(m,'AUDITOR_SHA',m.digest(auditor));monkeypatch.setattr(m,'RAW_TRACK',raw);monkeypatch.setattr(m,'RAW_SHA',m.digest(raw))
    monkeypatch.setattr(m,'ACQUISITION',acq);monkeypatch.setattr(m,'ACQUISITION_SHA',m.digest(acq))
    documentation={}
    for kind in ('index','mapping'):
        path=docs/(kind+'.metadata');path.write_bytes(b'SYNTHETIC_'+kind.encode());documentation[kind]=(path,m.digest(path))
    monkeypatch.setattr(m,'DOCUMENTATION',documentation)
    sources={}
    for kind,name in [('writer','VTL-1.vtl.py'),('order','VTL-2.dataorder.py')]:
        path=docs/name;path.write_bytes(b'synthetic documentation never executed');sources[kind]=path
    monkeypatch.setattr(m,'CODE_PINS',{k:(p.name,m.digest(p)) for k,p in sources.items()})
    qualifications={}
    for kind,count,source,tests in [('acquisition',68,m.ACQ_HELPER_SHA,m.ACQ_TEST_SHA),('schema',40,m.AUDITOR_SHA,m.TEST_SHA)]:
        xml=junit(support/(kind+'-tests-junit.xml'),kind)
        value={'status':'complete_private_'+str(count)+'_test_qualification','returncode':0,'native_job_id':'123',
            'module_sha256':source,'tests_sha256':tests,'test_counts':{'tests':count,'failures':0,'errors':0,'skipped':0},
            'actual_provider_track_bytes_read':False,'junit':{'path':str(xml),'sha256':m.digest(xml)},
            'test_roster_sha256':m.verify_private_junit(xml,kind),**{k:False for k in m.FALSE_FLAGS}}
        path=support/(kind+'-test-qualification.json');m.exclusive(path,value)
        qualifications[kind]={'path':str(path),'sha256':m.digest(path),'value':value}
    return {'root':root,'support':support,'case':case,'auditor':auditor,'raw':raw,'acq':acq,
        **sources,'qualified':qualifications,'now':time.time()}


def fake_result(f,change=None):
    f['case'].mkdir()
    detail={'status':'recognized_writer_shape_inventory','bytes_read':30121971,
        'frame_index_metadata':{'expected_grid_missingness_qualified':False,'seconds_or_exposure_times_derived':False},
        **{k:False for k in m.AUDIT_FALSE_FLAGS}}
    inventory=f['case']/'schema-and-clock-inventory.json';m.exclusive(inventory,detail)
    pins={'acquisition':(m.ACQUISITION,m.ACQUISITION_SHA),**m.DOCUMENTATION,
        'writer':(f['writer'],m.CODE_PINS['writer'][1]),'order':(f['order'],m.CODE_PINS['order'][1])}
    value={'status':'complete_bounded_outcome_blind_vtl_schema_inventory','auditor_sha256':m.AUDITOR_SHA,
        'acquisition':{'path':str(m.ACQUISITION),'sha256':m.ACQUISITION_SHA},
        'member':{'path':str(m.RAW_TRACK),'bytes':30121971,'crc32':'abefabb9','sha256':m.RAW_SHA},
        'all_source_bytes_reverified':True,'unknown_schema_does_not_clear_scientific_gates':True,
        'cryptographic_provider_member_checksum_available':False,'first_acquisition_sha256_not_provider_checksum':True,
        'native':{'job_id':'123','cpus':1,'gpus':0},
        'source_pins':[{'role':role,'path':str(path),'sha256':sha} for role,(path,sha) in pins.items()],
        'inventory':{'path':str(inventory),'sha256':m.digest(inventory)},**{k:False for k in m.AUDIT_FALSE_FLAGS}}
    if change:change(value,detail)
    m.exclusive(f['case']/'receipt.json',value)
    return value


@pytest.mark.parametrize('kind',['acquisition','schema'])
def test_complete_actual_junit_roster_required(tmp_path,kind):
    path=junit(tmp_path/'actual.xml',kind)
    assert m.verify_private_junit(path,kind)==m.canonical_hash(sorted(m.TEST_NAMES[kind]))


@pytest.mark.parametrize('kind',['acquisition','schema'])
@pytest.mark.parametrize('bad',['missing','skipped','failed','duplicate','foreign_class','attribute_only'])
def test_subset_skip_failed_duplicate_or_attribute_only_xml_refused(tmp_path,kind,bad):
    def change(suite):
        if bad=='missing':suite.remove(list(suite)[0])
        if bad=='skipped':ET.SubElement(list(suite)[0],'skipped')
        if bad=='failed':ET.SubElement(list(suite)[0],'failure')
        if bad=='duplicate':list(suite)[1].set('name',list(suite)[0].get('name'))
        if bad=='foreign_class':list(suite)[0].set('classname','unrelated')
        if bad=='attribute_only':
            for child in list(suite):suite.remove(child)
    path=junit(tmp_path/'dishonest.xml',kind,change)
    with pytest.raises(ValueError):m.verify_private_junit(path,kind)


def test_full_private_bindings_revalidate_source_job_counts_and_actual_xml(fixture):
    m.check_private_before_track_read(fixture['qualified'],'123')


@pytest.mark.parametrize('bad',['missing_suite','wrong_job','bool_return','partial','source','already_read_raw','false_gate','raw_xml_changed'])
def test_no_raw_byte_read_when_private_binding_is_invalid(fixture,monkeypatch,bad):
    f=fixture;q=f['qualified']
    if bad=='missing_suite':q.pop('schema')
    else:
        value=q['acquisition']['value']
        if bad=='wrong_job':value['native_job_id']='124'
        if bad=='bool_return':value['returncode']=False
        if bad=='partial':value['test_counts']['tests']=67
        if bad=='source':value['module_sha256']='a'*64
        if bad=='already_read_raw':value['actual_provider_track_bytes_read']=True
        if bad=='false_gate':value['scientific_result']=True
        path=Path(q['acquisition']['path']);path.write_text(json.dumps(value));q['acquisition']['sha256']=m.digest(path)
        if bad=='raw_xml_changed':Path(value['junit']['path']).write_bytes(b'changed')
    calls=[];original=m.digest
    def tracked(path):
        calls.append(Path(path));assert Path(path)!=f['raw'],'Actual raw-track bytes were read before complete qualification'
        return original(path)
    monkeypatch.setattr(m,'digest',tracked)
    with pytest.raises(ValueError):m.run_source_audit(f['auditor'],f['writer'],f['order'],time.time()+3600,q,'123')
    assert f['raw'] not in calls and not f['case'].exists()


def test_successful_audit_hard_deadline_and_no_provider_execution(fixture,monkeypatch):
    f=fixture;commands=[]
    original=m.digest
    def metadata_only(path):
        assert Path(path)!=f['raw'],'Raw first/hash/tail reads belong inside the hard-bounded child'
        return original(path)
    monkeypatch.setattr(m,'digest',metadata_only)
    def runner(command,**kwargs):
        commands.append(command)
        assert 0<kwargs['timeout']<=1080 and kwargs['check'] is False
        assert command[1:3]==['-s','-u'] and command[3]==str(f['auditor'])
        assert '--writer' in command and '--order' in command and '--max-seconds' in command
        fake_result(f);return subprocess.CompletedProcess(command,0)
    result=m.run_source_audit(f['auditor'],f['writer'],f['order'],time.time()+3600,f['qualified'],'123',runner=runner)
    assert result['status']=='complete_bounded_outcome_blind_vtl_schema_inventory' and len(commands)==1
    assert all(result[k] is False for k in m.AUDIT_FALSE_FLAGS)


@pytest.mark.parametrize('bad',['status','raw_hash','source_gate','native_job','source_pin','inventory_hash','provider_claim'])
def test_dishonest_actual_audit_receipts_never_become_success(fixture,bad):
    f=fixture
    def runner(command,**kwargs):
        def change(value,detail):
            if bad=='status':value['status']='partial'
            if bad=='raw_hash':value['member']['sha256']='a'*64
            if bad=='source_gate':value['physical_2d_units_qualified']=True
            if bad=='native_job':value['native']['job_id']='124'
            if bad=='source_pin':value['source_pins'][0]['sha256']='a'*64
            if bad=='inventory_hash':value['inventory']['sha256']='a'*64
            if bad=='provider_claim':value['first_acquisition_sha256_not_provider_checksum']=False
        fake_result(f,change);return subprocess.CompletedProcess(command,0)
    with pytest.raises(ValueError):m.run_source_audit(f['auditor'],f['writer'],f['order'],time.time()+3600,f['qualified'],'123',runner=runner)


def test_failed_actual_worker_never_fabricates_success_or_retries(fixture):
    f=fixture;calls=[]
    def runner(command,**kwargs):calls.append(command);return subprocess.CompletedProcess(command,1)
    with pytest.raises(ValueError):m.run_source_audit(f['auditor'],f['writer'],f['order'],time.time()+3600,f['qualified'],'123',runner=runner)
    assert len(calls)==1 and not (f['case']/'receipt.json').exists()


def test_private_group_roster_is_exact_four_selfcontained_files():
    groups=m.private_groups(Path('audit-vtl-track-schema-20261021.py'),Path('test_audit_vtl_track_schema_20261021.py'),
        Path('acquire-vtl-track-canary-20261021.py'),Path('test_acquire_vtl_track_canary_20261021.py'))
    assert list(groups)==['acquisition','schema'] and [g['count'] for g in groups.values()]==[68,40]
    assert groups['acquisition']['module_sha']==m.ACQ_HELPER_SHA and groups['schema']['module_sha']==m.AUDITOR_SHA


def test_main_prequalification_roster_does_not_read_raw_and_qualification_precedes_audit():
    tree=ast.parse(SOURCE.read_text());main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
    qualification_calls=[n for n in ast.walk(main) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='qualify_private']
    audit_calls=[n for n in ast.walk(main) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='run_source_audit']
    assert len(qualification_calls)==len(audit_calls)==1 and qualification_calls[0].lineno<audit_calls[0].lineno
    assert not any(isinstance(n,ast.Name) and n.id=='RAW_TRACK' for n in ast.walk(main))


def test_only_this_wrapper_is_a_test_sibling_dependency():
    tree=ast.parse(Path(__file__).read_text())
    names=[n.args[0].value for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)
        and n.func.attr=='with_name' and n.args and isinstance(n.args[0],ast.Constant)]
    assert names==['run-vtl-track-schema-audit-v2-20261021.py']


@pytest.fixture
def registry_fixture(tmp_path,monkeypatch):
    root=tmp_path/'personal';support=root/'maintenance/registry-v4';support.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'REGISTRY_SUPPORT',support)
    path=support/'private-qualification.json';monkeypatch.setattr(m,'REGISTRY_Q',path)
    for name,filename in [('REGISTRY_Q_HELPER','qualify-f2-registry-repair-v4-20261021.py'),
        ('REGISTRY_GUARD','registered-snapshot-read-guard-v3-20261021.py'),('REGISTRY_CONTROLLER','run-af-agent-controller-registry-v4-20261021.py')]:
        p=support/filename;p.write_text('synthetic pinned source '+name)
        monkeypatch.setattr(m,name,p);monkeypatch.setattr(m,name+'_SHA',m.digest(p))
    q={'status':'complete_f2_registry_overlay_qualification','returncode':0,'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,
        'slurm_job_id':'6699561','qualification_helper_sha256':m.REGISTRY_Q_HELPER_SHA,
        'registry_guard_sha256':m.REGISTRY_GUARD_SHA,'controller_sha256':m.REGISTRY_CONTROLLER_SHA,
        'worker_shim_sha256':'dd39905ce44f89f7f8bbdd325d1a0f1d5ec7950dd89d0e3bb267f6fdfe77a09a',
        'test_counts':{'tests':100,'failures':0,'errors':0,'skipped':0}}
    path.write_text(json.dumps(q));monkeypatch.setattr(m,'REGISTRY_Q_SHA',m.digest(path))
    events=[]
    def run(path,**kwargs):
        if Path(path)==m.REGISTRY_Q_HELPER:return {'verify_pins':lambda:events.append('live-v4-pins')}
        if Path(path)==m.REGISTRY_CONTROLLER:return {'verify_operational_tests':lambda value:events.append(('actual-full100-roster',value))}
        if Path(path)==m.REGISTRY_GUARD:
            def install(evidence,**kw):events.append(('native-worker-lock',evidence,kw));return {'status':'synthetic'}
            return {'install':install}
        raise AssertionError('Unreviewed code must never execute')
    monkeypatch.setattr(m.runpy,'run_path',run)
    return path,q,events


def test_exact_positive100_v4_binding_installs_new_worker_guard(registry_fixture,monkeypatch):
    path,q,events=registry_fixture;monkeypatch.setattr(m,'SUPPORT',path.parent/'vtl-v2')
    binding,installed=m.qualified_registry_read_guard(path,m.REGISTRY_Q_SHA,'6699561',123456.)
    assert binding['job_id']=='6699561' and installed=={'status':'synthetic'}
    assert events[:2]==['live-v4-pins',('actual-full100-roster',q)]
    assert events[2][0]=='native-worker-lock' and events[2][2]=={'registration_mode':'worker-lock','allocation_deadline_unix':123456.}


@pytest.mark.parametrize('bad',['old84','wrong_job','wrong_receipt_hash','old_guard','old_controller','partial100','skipped100','bool_return'])
def test_old_or_partial_registry_qualification_cannot_reintroduce_contention_guard(registry_fixture,monkeypatch,bad):
    path,q,events=registry_fixture
    job='6699561';sha=m.REGISTRY_Q_SHA
    if bad=='old84':q['test_counts']['tests']=84
    if bad=='wrong_job':job='6696233'
    if bad=='wrong_receipt_hash':sha='a'*64
    if bad=='old_guard':q['registry_guard_sha256']='42fd488d27d1bf9aa75b46c113257525e296c526250e75ba12319d6139854db4'
    if bad=='old_controller':q['controller_sha256']='e1852d78a1f00e9ee8855413db1f6599fe31a73c4f4498d7b09158f4c4f61a1c'
    if bad=='partial100':q['test_counts']['tests']=99
    if bad=='skipped100':q['test_counts']['skipped']=1
    if bad=='bool_return':q['returncode']=False
    if bad not in {'wrong_job','wrong_receipt_hash'}:
        path.write_text(json.dumps(q));sha=m.digest(path);monkeypatch.setattr(m,'REGISTRY_Q_SHA',sha)
    with pytest.raises(ValueError):m.registry_qualification(path,sha,job)
    assert events==[]


def test_native_v4_guard_and_receipt_pins_are_explicit_and_no_old_guard_is_selected():
    assert m.REGISTRY_Q_JOB=='6699561' and m.REGISTRY_Q_SHA=='1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca'
    assert m.REGISTRY_GUARD_SHA=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'
    assert m.REGISTRY_Q_HELPER_SHA=='7fd2348c3370811cb5ca2933357a007447b18c2ea57be1cef502fdd035fdda51'
    assert m.REGISTRY_CONTROLLER_SHA=='475a170aab2d158cfd17adb250707e1d0767086d4c50b95516c360f2222560f8'
    assert m.REGISTRY_Q.parent.name=='study-af-registry-repair-support-v4-20261021'
