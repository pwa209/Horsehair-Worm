from pathlib import Path
import ast,hashlib,importlib.util,json
import pytest

HERE=Path(__file__).parent
spec=importlib.util.spec_from_file_location('primary490_registration_under_test',HERE/'build-primary-external-census-handoff-490.py')
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

def test_fresh_registration_filename_matches_request_490(packets,tmp_path,monkeypatch):
    target=tmp_path/'fresh490'
    monkeypatch.setattr(b,'render',lambda *args,**kwargs:(b'new-registration\n',b'{}\n',b'{}\n'))
    b.main(['--junit',str(packets[1]),'--helper-sha256',packets[2],'--test-sha256',packets[3],'--output-directory',str(target)])
    assert (target/'hhw-recovery-stage-20261021-490.sh').read_bytes()==b'new-registration\n'
    assert not (target/'hhw-recovery-stage-20261021-470.sh').exists()


def test_preserved_partial_phase_bound_and_fresh_controller_namespace(packets):
    raw,_,_=render(packets);body=raw.decode()
    for pin in ['6a33e4ee89f4f4ef380e712aec7d488bdd7b5acfbb39d28301cabcb7c7e7cdf3','26efafe65339ec349a65773135d36502a3baa7d3f4523d56f704b16dba3c5be6','ad70a140cdd50384f860cb4adc2dc4fc38feb0a7eaa60be2403456373c9b2267']:
        assert pin in body
    assert "plans/primary-external-census-controllers-v2" in body
    assert "failed483-preserved-partial-phase.json" in body
    import base64,zlib
    line=next(v for v in body.splitlines() if v.startswith('payload='))
    token=next(n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100)
    assert json.loads(zlib.decompress(base64.b64decode(token)))['area']==b.R+'/maintenance/study-primary-external-census-handoff-v3-20261023'
    assert "replacement_plan_absent" not in body and "replacement_ledger_absent" not in body
