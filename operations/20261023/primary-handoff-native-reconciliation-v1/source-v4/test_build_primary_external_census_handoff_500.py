from pathlib import Path
import ast,hashlib,importlib.util,json
import pytest

HERE=Path(__file__).parent
spec=importlib.util.spec_from_file_location('primary500_registration_under_test',HERE/'build-primary-external-census-handoff-500.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)

@pytest.fixture
def packets(tmp_path):
    h=b'def operational_handoff():\n    return None\n';t=b'def test_qualification():\n    assert True\n'
    (tmp_path/b.HELPER).write_bytes(h);(tmp_path/b.TEST).write_bytes(t)
    xml=tmp_path/'actual.xml'
    xml.write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"><testcase classname="work.'+b.TEST[:-3]+'" name="test_qualification"/></testsuite></testsuites>')
    return tmp_path,xml,hashlib.sha256(h).hexdigest(),hashlib.sha256(t).hexdigest()

def render(p):
    folder,xml,hs,ts=p
    return b.render(xml,hs,ts,source_directory=folder)

def test_render_preserves_single_qualification_gate_and_fresh_namespace(packets):
    raw,mraw,eraw=render(packets);s=raw.decode();body=s.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
    compile(body,'<reviewed470>','exec');ast.parse(body,feature_version=(3,11))
    assert "task['external_dependencies']=['6866457']" in body
    assert "task['resources']['array']=None" in body
    assert "task['primary_external_census_handoff']=" in body
    assert 'recovery-support-20261021-375' not in body
    assert "root/'maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py'" in body
    assert "area/'administration-registry-read-guard-v4-20261022.py'" not in body
    assert 'scancel' not in body and 'scontrol' not in body
    manifest=json.loads(mraw);e=json.loads(eraw)
    assert set(manifest)=={'kind','files','tests','primary_source_manifest','qualification_policy_descriptor'}
    assert manifest['files']=={b.HELPER:packets[2],b.TEST:packets[3]}
    assert e['qualification_claimed'] is e['scheduler_submitted'] is False
    assert e['shell_sha256']==hashlib.sha256(raw).hexdigest()
    assert e['own_resources']=={'cpus':4,'memory':'16G','hours':1,'gpu':False,'array':None}

@pytest.mark.parametrize('flag',['failure','error','skipped'])
def test_actual_nonpassing_case_refused(packets,flag):
    folder,xml,hs,ts=packets;s=xml.read_text();xml.write_text(s.replace('/></testsuite>',f'><{flag}/></testcase></testsuite>'))
    with pytest.raises(ValueError,match='must pass'):render(packets)

@pytest.mark.parametrize('flag',['failures','errors','skipped'])
def test_nonzero_summary_refused(packets,flag):
    xml=packets[1];xml.write_text(xml.read_text().replace(flag+'="0"',flag+'="1"'))
    with pytest.raises(ValueError,match='JUnit totals'):render(packets)

def test_duplicate_case_refused(packets):
    xml=packets[1];s=xml.read_text();case=s[s.index('<testcase'):s.index('/>')+2];xml.write_text(s.replace('</testsuite>',case+'</testsuite>'))
    with pytest.raises(ValueError,match='unique own'):render(packets)

def test_foreign_suite_refused(packets):
    xml=packets[1];xml.write_text(xml.read_text().replace(b.TEST[:-3],'test_foreign'))
    with pytest.raises(ValueError,match='unique own'):render(packets)

def test_source_changed_after_freeze_refused(packets):
    (packets[0]/b.HELPER).write_text('raise RuntimeError("changed")\n')
    with pytest.raises(ValueError,match='Frozen source changed'):render(packets)

def test_unreviewed_builder_utility_refused(packets,monkeypatch):
    monkeypatch.setattr(b,'LIB_SHA','0'*64)
    with pytest.raises(ValueError,match='utility changed'):render(packets)

def test_xml_external_entity_refused(packets):
    xml=packets[1];xml.write_text('<!DOCTYPE x SYSTEM "file:///unread">'+xml.read_text())
    with pytest.raises(ValueError,match='Unsafe JUnit'):render(packets)

def test_fresh_registration_filename_matches_request_500(packets,tmp_path,monkeypatch):
    target=tmp_path/'fresh500'
    monkeypatch.setattr(b,'render',lambda *args,**kwargs:(b'new-registration\n',b'{}\n',b'{}\n'))
    b.main(['--junit',str(packets[1]),'--helper-sha256',packets[2],'--test-sha256',packets[3],'--output-directory',str(target)])
    assert (target/'hhw-recovery-stage-20261021-500.sh').read_bytes()==b'new-registration\n'
    assert not (target/'hhw-recovery-stage-20261021-470.sh').exists()


def test_completed_history_bound_and_namespace_corrected_fresh_phase(packets):
    import base64,zlib
    raw,_,_=render(packets);body=raw.decode()
    for pin in ['2b5b77e7230b5365de91caf64f72484c456856b0526549a53400320ee7c32cb3','0bd363318e2c4195de7f1a4bc5dc424555e26e8ff8251d4333c5c902649ce55c','9e525425ba063c824eed9fe40e15272d33be5ae88bc208850167b0e2d07523a1','567ff41003556ffaf316db0d9d5499a20287ffb3fbe304537efac1bb745326ed']:
        assert pin in body
    assert "plans/primary-external-census-controllers-v3" in body
    assert "completed-v3-and-prestart-phase-preserved.json" in body
    assert "failed483-preserved" not in body and "replacement_plan_absent" not in body
    line=next(v for v in body.splitlines() if v.startswith('payload='))
    token=next(n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100)
    payload=json.loads(zlib.decompress(base64.b64decode(token)))
    assert payload['area']==b.R+'/maintenance/study-primary-external-census-handoff-v4-20261023'
    assert payload['id']=='500' and payload['args'][1].endswith('recovery-support-20261021-500/plan.json')
