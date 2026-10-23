"""Synthetic qualified-package metadata and complete600-member archive canary."""
from pathlib import Path
import copy
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import pytest
from horsehair import agents,agent_analysis_v2 as bridge
from horsehair.core import atomic_json,canonical_hash,digest,read_json

spec=importlib.util.spec_from_file_location('private_archive_consumer',Path(__file__).with_name('verify-f2-condition-archives-v2-20261022.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
FIXTURE_SOURCES={
    'configs/study.json':'b872f121b6cef11a7726fc56b4c237c714fab41bbafa2a6090d7e2c1e2f91353',
    'tests/test_agent_analysis_v2.py':'25b2b72392fdaa98997739567e1487a7c913027f16dcb4d8d3f95b6b9368a0b8',
    'tests/fixtures/evidence_archive/README.md':'2e5e1fdd65bc7f19949bd97c82253bc093f8fecf7bef5295f2350ef5c90e9c8c',
    'tests/fixtures/evidence_archive/verified-evidence-archive-20261021.py':'045d8650db62c0fa52d7f56bb7bdb2669729509c23ecfbbc847e73cbbff8d42a',
    'tests/fixtures/evidence_archive/test_verified_evidence_archive_20261021.py.txt':'ec4131d283d3f3a792a561c6ae6324de853a4301def9d275a03018a9801f5462'}
# A test-only override supplies an isolated copy of the exact frozen fixture
# closure. Production helpers neither read this variable nor import fixtures.
FIXTURE_ROOT=Path(os.environ.get('HHW_ARCHIVE_SYNTHETIC_FIXTURE_ROOT',
    str(Path(agents.__file__).resolve().parents[2]))).resolve()
for relative,expected in FIXTURE_SOURCES.items():
    fixture_path=FIXTURE_ROOT/relative
    assert fixture_path.is_file() and not fixture_path.is_symlink() and digest(fixture_path)==expected
public_path=FIXTURE_ROOT/'tests/test_agent_analysis_v2.py'
public_spec=importlib.util.spec_from_file_location('qualified_h4_archive_fixture_source',public_path)
fixtures=importlib.util.module_from_spec(public_spec);public_spec.loader.exec_module(fixtures)
context=fixtures.context
complete_policy=fixtures.complete_policy
archive_support=fixtures.archive_support


@pytest.fixture
def qcontext(tmp_path,monkeypatch):
    import horsehair
    root=tmp_path/'study';release=root/'releases/consumer';(release/'src/horsehair').mkdir(parents=True)
    (release/'src/horsehair/__init__.py').write_text('synthetic package marker')
    module=release/'src/horsehair/agent_analysis_v2.py';module.write_bytes(Path(bridge.__file__).read_bytes())
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'FRAMEWORK',release)
    monkeypatch.setattr(horsehair,'__file__',str(release/'src/horsehair/__init__.py'))
    monkeypatch.setattr(bridge,'__file__',str(module))
    identity={'sha256':m.SOURCE,'files':{'src/horsehair/agent_analysis_v2.py':m.MODULE}}
    monkeypatch.setattr(m,'source_identity',lambda p:identity)
    qpath=root/'maintenance/v3/qualification.json';qpath.parent.mkdir(parents=True);monkeypatch.setattr(m,'QPATH',qpath)
    workspace=root/'state/qualification-evidence/test';workspace.mkdir(parents=True)
    junit=workspace/'junit.xml';tree=ET.Element('testsuite',tests='1889',failures='0',errors='0',skipped='0')
    for i in range(1889):ET.SubElement(tree,'testcase',classname='synthetic_full_roster',name='synthetic_'+str(i))
    junit.write_bytes(ET.tostring(tree))
    q={'status':'complete','returncode':0,'source_sha256':m.SOURCE,'config_sha256':m.CONFIG,'source_verified_unchanged':True,
        'source_identity':identity,'slurm_job_id':'100','test_counts':{'tests':1889,'failures':0,'errors':0,'skipped':0},
        'workspace':str(workspace),'test_selection':bridge.FULL_TEST_SELECTION,
        'command':['python','-u','-m','pytest',str(release/'tests'),'-v','--color=no','--basetemp',str(workspace/'pytest'),
            '--junitxml',str(workspace/'junit.xml'),'-o','cache_dir='+str(workspace/'cache'),'-o','faulthandler_timeout=120'],
        'artifacts':{'junit':{'path':str(junit),'sha256':digest(junit),'bytes':junit.stat().st_size}}}
    atomic_json(qpath,q)
    monkeypatch.setattr(m,'QSHA',digest(qpath));monkeypatch.setattr(m,'QJOB','100')
    record={'user':'pwa209','account':'def-ptewarie_cpu','job_id_raw':'100','unallocated_metadata':False,'cpus':4,
        'elapsed_seconds':100,'alloc_tres':'cpu=4,mem=16G'}
    # The actual budget parser persists a cost-only row. Fresh ownership is
    # read separately through the actual strict13-field parser; never enrich it.
    from horsehair import budget,repair
    native_ledger=root/'state/resource-budget.json';atomic_json(native_ledger,{'native_jobs':{},'leases':{},
        'spent_cpu_core_hours':0.,'spent_gpu_hours':0.})
    budget.native_accounting(root,['100'],runner=lambda command,**kwargs:subprocess.CompletedProcess(command,0,
        '100|100|COMPLETED|100|4|cpu=4,mem=16G,node=1|0:0|1024K\n',''))
    def transport(command,**kwargs):
        assert command==['sacct','--array','-j','100','--format='+repair.REPAIR_NATIVE_FORMAT,
            '--units=K','--noheader','--parsable2']
        assert 0<kwargs['timeout']<=60
        values=['100',record['job_id_raw'],'COMPLETED','0:0',str(record['elapsed_seconds']),str(record['cpus']),
            record['alloc_tres'],'nc10000','2026-10-21T10:00:00','2026-10-21T10:01:40',record['user'],record['account'],'1024K']
        return subprocess.CompletedProcess(command,0,'|'.join(values)+'\n','')
    monkeypatch.setattr(bridge.subprocess,'run',transport)
    return qpath,q,record


def test_complete_qualified_package_source_command_and_native_proof(qcontext):
    path,q,record=qcontext
    result=m.qualified_consumer({'path':str(path),'sha256':digest(path),'job_id':'100'})
    assert result['source_sha256']==m.SOURCE and result['module_sha256']==m.MODULE


@pytest.mark.parametrize('field,value',[('sha256','f'*64),('job_id','101')])
def test_known_native_consumer_producer_and_receipt_are_pinned(qcontext,field,value):
    path,q,record=qcontext
    binding={'path':str(path),'sha256':digest(path),'job_id':'100'};binding[field]=value
    with pytest.raises(ValueError,match='Exact explicit'):
        m.qualified_consumer(binding)


@pytest.mark.parametrize('failure',['wrong_source','partial_count','skips','bool_zero','wrong_job','selector_command','wrong_selection',
    'foreign_owner','gpu','bool_cpus','zero_seconds','wrong_package'])
def test_mixed_or_partial_or_unowned_qualification_never_admits_archive_reads(qcontext,monkeypatch,failure):
    path,q,record=qcontext
    if failure=='wrong_source':q['source_sha256']='a'*64
    if failure=='partial_count':q['test_counts']['tests']=1888
    if failure=='skips':q['test_counts']['skipped']=1
    if failure=='bool_zero':q['test_counts']['failures']=False
    if failure=='wrong_job':q['slurm_job_id']='101'
    if failure=='selector_command':q['command'].extend(['-k','one'])
    if failure=='wrong_selection':q['test_selection']='selected cases'
    if failure=='foreign_owner':record['user']='foreign'
    if failure=='gpu':record['alloc_tres']='cpu=4,gres/gpu=1'
    if failure=='bool_cpus':record['cpus']=True
    if failure=='zero_seconds':record['elapsed_seconds']=0
    if failure=='wrong_package':
        import horsehair
        monkeypatch.setattr(horsehair,'__file__',str(path.parent/'foreign/__init__.py'))
    atomic_json(path,q)
    monkeypatch.setattr(m,'QSHA',digest(path))
    with pytest.raises(ValueError):m.qualified_consumer({'path':str(path),'sha256':digest(path),'job_id':'100'})


@pytest.mark.parametrize('raw',[b'{"a":1,"a":2}',b'{"a":NaN}',b'{"a":Infinity}',b'{"a":1e999}'])
def test_strict_source_json_refuses_duplicates_and_nonfinite_literals(raw):
    with pytest.raises(ValueError):m.strict(raw)


@pytest.mark.parametrize('bad',['bool_version','foreign_root','malformed_seal','unknown_key'])
def test_request_requires_exact_scope_and_strict_scalar_types(bad):
    request={'schema_version':1,'kind':'fresh_complete_archive_controller_verification_v1',
        'root':str(m.ROOT),'consumer_qualification':{},'campaign_plan':{},'state':{},
        'state_sha256':'a'*64,'archive_registry':{}}
    m.validate_request(request)
    if bad=='bool_version':request['schema_version']=True
    if bad=='foreign_root':request['root']='/scratch/foreign'
    if bad=='malformed_seal':request['state_sha256']='not_a_full_sha'
    if bad=='unknown_key':request['admitted']=True
    with pytest.raises(ValueError):m.validate_request(request)


def test_complete600_member_archive_revalidates_all300_original_controller_packets(context,complete_policy,archive_support,monkeypatch):
    """Actual archive parser/hash/tar; disposable synthetic original parts only."""
    root,_,_,campaign,_=context;analysis_plan,cell,_,ep,assembly=complete_policy;helper,registry,save=archive_support
    case=Path(cell['evaluation_case']);inputs={key:bridge._locator(root,case/name) for key,name in
        [('plan','plan.json'),('binding','plan-binding.json'),('summary','evaluation_summary.json')]}
    native_result={'native_job_id':'100','source_sha256':campaign['source_binding']['execution_source_sha256'],
        'full_config_sha256':bridge.BASE_CONFIG_SHA256,'scientific_result':False,'admission_authority':False,
        'status':'complete_native_work','mode':'agent-assembly','evaluation_case':str(case),
        'evaluation_plan_sha256':inputs['plan']['sha256'],'evaluation_binding_sha256':inputs['binding']['sha256'],'result':assembly}
    native_result_path=root/'maintenance/archive-proofs/assembly-result.json';atomic_json(native_result_path,native_result)
    authority={'primary_source_sha256':bridge.PRIMARY_SCIENTIFIC_SOURCE_SHA256,'full_config_sha256':bridge.BASE_CONFIG_SHA256,
        'execution_source_sha256':native_result['source_sha256'],'native':fixtures.archive_native(root,'100'),
        'result_receipt':bridge._locator(root,native_result_path),'policy_inputs':inputs}
    # Only fixture publication uses rename on restricted Windows. The actual
    # reviewed tar/index/roundtrip/native/canonical parsers execute unchanged.
    monkeypatch.setattr(helper,'_publish_file',lambda source,dest:Path(source).rename(dest))
    directory=root/'maintenance/policy-archive';pack=helper.pack_policy(case,directory,authority=authority,root=root)
    descriptor={'evaluation_case':str(case),'policy_inputs':inputs,'archive_directory':str(directory),
        'index_sha256':pack['index_sha256'],'archive_sha256':pack['archive_sha256'],
        **{key:bridge._locator(root,directory/name) for key,name in [('pack_intent','pack-intent.json'),('roundtrip','roundtrip.json'),('pack_receipt','pack-receipt.json')]}}
    producer={'schema_version':1,'status':'complete_verified_policy_archive_pack','native_job_id':'300',
        'archive_helper_sha256':bridge.ARCHIVE_HELPER_SHA256,'evaluation_case':str(case),'archive_directory':str(directory),
        'index_sha256':pack['index_sha256'],'archive_sha256':pack['archive_sha256'],
        'pack_intent_sha256':descriptor['pack_intent']['sha256'],'roundtrip_sha256':descriptor['roundtrip']['sha256'],
        'pack_receipt_sha256':descriptor['pack_receipt']['sha256'],'execution_source_sha256':bridge.ARCHIVE_PACK_FRAMEWORK_SHA256,
        'full_config_sha256':bridge.BASE_CONFIG_SHA256,'primary_scientific_source_sha256':bridge.PRIMARY_SCIENTIFIC_SOURCE_SHA256,
        'scientific_result':False,'admission_authority':False}
    producer_path=root/'maintenance/archive-proofs/pack-result.json';atomic_json(producer_path,producer)
    descriptor['pack_producer']={'native':fixtures.archive_native(root,'300'),'result':bridge._locator(root,producer_path),
        'qualification':fixtures.archive_frame_qualification(root,monkeypatch)}
    registry['cells']['0']=descriptor;binding=save();archives=bridge._archive_context(root,binding,campaign['cells'])
    original=helper.committed_conditions(directory,pack['index_sha256'],pack['archive_sha256'],root=root)
    for member in assembly['condition_bindings']:
        (case/member['receipt_name']).unlink();(case/member['artifact_name']).unlink()
    state={'cells':[{'index':0,'complete':True,'assembly_job':'assembly0','conditions':{str(i):p for i,p in enumerate(original)},
        'evaluation_plan':{'case':str(case),'plan_sha256':inputs['plan']['sha256'],'plan_binding_sha256':inputs['binding']['sha256'],'value':ep}}],
        'jobs':{'assembly0':{'status':'complete','job_id':'100','intent':{'kind':'assembly'}}}}
    monkeypatch.setattr(m.dispatch,'_state',lambda plan,state:state)
    monkeypatch.setattr(m,'CONFIG',bridge.BASE_CONFIG_SHA256)
    rows=m.verify_cells(root,campaign,state,archives)
    assert rows[0]['condition_packets']==300 and rows[0]['packet_roster_sha256']==canonical_hash(original)
    assert rows[0]['all_current_archived_bytes_reverified'] is True
    changed=copy.deepcopy(state);changed['cells'][0]['conditions']['0']['sha256']='e'*64
    with pytest.raises(ValueError,match='frozen controller state'):m.verify_cells(root,campaign,changed,archives)
    changed=copy.deepcopy(state);changed['cells'][0]['complete']=False
    with pytest.raises(ValueError,match='positively assembled'):m.verify_cells(root,campaign,changed,archives)
    changed=copy.deepcopy(state);changed['jobs']['assembly0']['job_id']='101'
    with pytest.raises(ValueError,match='assembly authority'):m.verify_cells(root,campaign,changed,archives)
    changed=copy.deepcopy(state);changed['cells'][0]['evaluation_plan']['plan_binding_sha256']='a'*64
    with pytest.raises(ValueError,match='plan/binding/case'):m.verify_cells(root,campaign,changed,archives)
    before=(directory/'evidence.tar').read_bytes();(directory/'evidence.tar').write_bytes(b'corrupt')
    with pytest.raises(ValueError):m.verify_cells(root,campaign,state,archives)
    (directory/'evidence.tar').write_bytes(before)
    assert (case/assembly['artifact_name']).exists() and Path(cell['training_case'],'checkpoint.pt').exists()


def test_isolated_closure(tmp_path):
    """All real archive fixtures work without any unrelated private sibling."""
    stage=tmp_path/'s';stage.mkdir();copied=tmp_path/'q';copied.mkdir()
    for relative,expected in FIXTURE_SOURCES.items():
        source=FIXTURE_ROOT/relative;target=copied/relative;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(source,target);assert digest(target)==expected
    for source in (Path(__file__),Path(m.__file__)):
        shutil.copyfile(source,stage/source.name)
    assert {p.name for p in stage.iterdir()}=={Path(__file__).name,Path(m.__file__).name}
    environment={**os.environ,'HHW_ARCHIVE_SYNTHETIC_FIXTURE_ROOT':str(copied),
        'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1'}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):environment.pop(key,None)
    command=[sys.executable,'-s','-m','pytest',str(stage/Path(__file__).name),'-q','-p','no:cacheprovider',
        '--rootdir',str(stage),'--deselect',Path(__file__).name+'::test_isolated_closure',
        '--basetemp',str(tmp_path/'f')]
    result=subprocess.run(command,cwd=stage,text=True,capture_output=True,timeout=420,env=environment)
    assert result.returncode==0,result.stdout+result.stderr
    assert '24 passed' in result.stdout
