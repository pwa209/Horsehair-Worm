"""Synthetic archive-view scheduling/identity tests; no cluster or study data."""
from pathlib import Path
from types import SimpleNamespace
import copy
import ast
import inspect
import gzip
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import time
import shlex
import sys
import xml.etree.ElementTree as ET

import pytest
from horsehair import agents,budget,core,repair,slurm
from horsehair.core import canonical_hash,digest,read_json

spec=importlib.util.spec_from_file_location('private_archive_controller',
    Path(__file__).with_name('run-af-agent-controller-archive-v3-20261023.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
FROZEN=Path(__file__).with_name('run-af-agent-controller-registry-v6-20261022.py')
assert digest(FROZEN)==m.BASE_SHA
PUBLIC_FIXTURE_ROOT=Path(os.environ.get('HHW_OVERLAY_PUBLIC_FIXTURE_ROOT',str(Path(agents.__file__).resolve().parents[2])))
assert digest(PUBLIC_FIXTURE_ROOT/'src/horsehair/agent_analysis_v2.py')==m.VALIDATION_MODULE_SHA


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,sort_keys=True,allow_nan=False)+'\n')
    return {'path':str(path),'sha256':digest(path)}


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    root=tmp_path/'study';case=root/'maintenance/campaign';area=case/'controller';area.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'VERIFIER_SHA','a'*64)
    consumer=root/'releases/qualifiedf047';consumer.mkdir(parents=True);monkeypatch.setattr(m,'CONSUMER_FRAMEWORK',consumer)
    qpath=root/'maintenance/full1889.json'
    junit=area/'framework.xml';junit.write_bytes(b'<testsuite/>');junit_locator={'path':str(junit),'sha256':digest(junit)}
    qbinding={**write(qpath,{'slurm_job_id':'1889','artifacts':{'junit':junit_locator}}),'job_id':'1889'}
    monkeypatch.setattr(m,'CONSUMER_Q',qbinding)
    cellcase=case/'cell-0000';cellcase.mkdir()
    checkpoint=root/'results/training/checkpoint.pt';checkpoint.parent.mkdir(parents=True);checkpoint.write_bytes(b'original synthetic checkpoint')
    assembled=cellcase/'assembled-fixture.parquet';assembled.write_bytes(b'synthetic transport only')
    inputs={'plan':write(cellcase/'plan.json',{'checkpoint':str(checkpoint)}),
        'summary':write(cellcase/'evaluation_summary.json',{'artifact_name':assembled.name}),
        'binding':write(cellcase/'plan-binding.json',{'synthetic':'binding'})}
    archive=root/'maintenance/archive';archive.mkdir();(archive/'evidence.tar').write_bytes(b'synthetic archive identity only')
    write(root/'state/resource-budget.json',{'native_jobs':{},'leases':{},'spent_cpu_core_hours':0.,'spent_gpu_hours':0.})
    jobs=['300','301','302','6684484','1889']
    budget.native_accounting(root,jobs,runner=lambda command,**kwargs:subprocess.CompletedProcess(command,0,
        ''.join(job+'|'+job+'|COMPLETED|60|4|cpu=4,mem=16G,node=1|0:0|1024K\n' for job in jobs),''))
    def native(job):
        proof=slurm.completed_native_dependency(root,job)
        return {'completion':write(area/('complete-'+job+'.json'),proof),
            'owner':write(area/('owner-'+job+'.json'),{'synthetic':'strict child verifies original owner','job_id':job})}
    assembly_native=native('300');pack_native=native('301');canary_native=native('302')
    write(archive/'index.json',{'authority':{'native':assembly_native}})
    pack_q=write(area/'pack-q.json',{'slurm_job_id':'6684484','artifacts':{'junit':junit_locator}})
    canary=write(area/'canary.json',{'native':canary_native,'test_source':junit_locator,'junit':junit_locator})
    descriptor={'evaluation_case':str(cellcase),'policy_inputs':inputs,'archive_directory':str(archive),
        'index_sha256':digest(archive/'index.json'),'archive_sha256':digest(archive/'evidence.tar'),
        'pack_producer':{'native':pack_native,'qualification':pack_q}}
    registry={'kind':'final_policy_condition_archive_registry_v1','root':str(root),'cells':{'0':descriptor},'archive_canary':canary}
    reglocator=write(root/'maintenance/registry.json',registry)
    binding={'registry':reglocator,'consumer_qualification':qbinding,
        'verifier':{'path':str(root/'maintenance/verifier.py'),'sha256':m.VERIFIER_SHA}}
    state={'state_sha256':'f'*64,'cells':[{'index':0,'complete':True,'assembly_job':'assembly0',
        'conditions':{str(i):{'sha256':str(i),'value':{'condition_index':i}} for i in range(300)},
        'evaluation_plan':{'case':str(cellcase)}}], 'jobs':{'assembly0':{'job_id':'300'}}}
    plan={'plan_sha256':'e'*64};write(case/'campaign-plan.json',plan)
    (case/'state.json.gz').write_bytes(gzip.compress(json.dumps(state).encode(),mtime=0))
    sb={'state_file_sha256':digest(case/'state.json.gz'),'state_sha256':state['state_sha256'],
        'campaign_plan_sha256':plan['plan_sha256']};write(case/'state-binding.json',sb)
    c={'case':case,'area':area,'framework':root/'releases/original4f','deadline':10000.,
        'descriptor':{'archive_overlay':binding}}
    extra={'binding':binding,'registry':registry}
    monkeypatch.setattr(m,'overlay_binding',lambda *args,**kwargs:extra)
    def runner(command,**kwargs):
        request_path=Path(command[command.index('--request')+1]);request=m.strict(request_path.read_bytes())
        packets=[state['cells'][0]['conditions'][str(i)] for i in range(300)]
        proof={'schema_version':1,'status':'complete_fresh300_packet_archive_verification',
            'consumer':{'source_sha256':m.CONSUMER_SOURCE,'qualification':qbinding},
            'request':{'path':str(request_path),'sha256':digest(request_path)},'private_helper_sha256':m.VERIFIER_SHA,
            'state_file_sha256':request['state']['sha256'],'state_sha256':state['state_sha256'],
            'campaign_plan_sha256':plan['plan_sha256'],'archive_registry':reglocator,
            'cells':[{'cell_index':0,'condition_packets':300,'packet_roster_sha256':canonical_hash(packets),
                'index_sha256':descriptor['index_sha256'],'archive_sha256':descriptor['archive_sha256'],
                'assembly_job_id':'300','all_current_archived_bytes_reverified':True}],
            'raw_condition_packets_changed':False,'endpoint_values_recomputed':False,
            'scientific_result':False,'admission_authority':False}
        write(Path(command[command.index('--output')+1]),proof)
        return subprocess.CompletedProcess(command,0)
    return c,plan,state,registry,runner


def test_fresh_full_packet_view_preserves_all300_original_packets(fixture):
    c,plan,state,registry,runner=fixture
    m.prepare_view(None,c,runner=runner,clock=lambda:0)
    result=m.snapshot_map(c,plan,state)
    assert result[registry['cells']['0']['evaluation_case']]==[state['cells'][0]['conditions'][str(i)] for i in range(300)]
    assert len(result[registry['cells']['0']['evaluation_case']])==300


@pytest.mark.parametrize('change',['state_file','state_seal','plan_file','binding_file','registry','condition_packet','complete',
    'assembly','tar','checkpoint','assembled'])
def test_changed_live_state_or_canonical_archive_cannot_reuse_view(fixture,change):
    c,plan,state,registry,runner=fixture;m.prepare_view(None,c,runner=runner,clock=lambda:0)
    descriptor=registry['cells']['0']
    if change=='state_file':(c['case']/'state.json.gz').write_bytes(b'changed')
    if change=='state_seal':state['state_sha256']='0'*64
    if change=='plan_file':write(c['case']/'campaign-plan.json',{'changed':True})
    if change=='binding_file':write(c['case']/'state-binding.json',{'changed':True})
    if change=='registry':write(Path(c['descriptor']['archive_overlay']['registry']['path']),{'changed':True})
    if change=='condition_packet':state['cells'][0]['conditions']['0']['sha256']='changed'
    if change=='complete':state['cells'][0]['complete']=False
    if change=='assembly':state['jobs']['assembly0']['job_id']='301'
    if change=='tar':Path(descriptor['archive_directory'],'evidence.tar').write_bytes(b'changed')
    if change=='checkpoint':Path(m.strict(m.read_bound(descriptor['policy_inputs']['plan']))['checkpoint']).write_bytes(b'changed')
    if change=='assembled':Path(descriptor['evaluation_case'],'assembled-fixture.parquet').write_bytes(b'changed')
    with pytest.raises(ValueError):m.snapshot_map(c,plan,state)


@pytest.mark.parametrize('failure',['nonzero','bool_zero','timeout','wrong_source','wrong_request','partial_roster'])
def test_child_failure_never_uses_original_file_fallback(fixture,failure):
    c,plan,state,registry,real=fixture
    def runner(command,**kwargs):
        if failure=='timeout':raise subprocess.TimeoutExpired(command,1)
        response=real(command,**kwargs)
        if failure=='nonzero':return subprocess.CompletedProcess(command,1)
        if failure=='bool_zero':return subprocess.CompletedProcess(command,False)
        path=Path(command[command.index('--output')+1]);proof=m.strict(path.read_bytes())
        if failure=='wrong_source':proof['consumer']['source_sha256']='b'*64
        if failure=='wrong_request':proof['request']['sha256']='b'*64
        if failure=='partial_roster':proof['cells']=[]
        path.write_text(json.dumps(proof))
        return response
    expected=subprocess.TimeoutExpired if failure=='timeout' else ValueError
    if failure=='partial_roster':
        m.prepare_view(None,c,runner=runner,clock=lambda:0)
        with pytest.raises(ValueError):m.snapshot_map(c,plan,state)
    else:
        with pytest.raises(expected):m.prepare_view(None,c,runner=runner,clock=lambda:0)
    assert not c.get('prepared_archive_view') or failure=='partial_roster'


def test_vendor_dependencies_retained_user_site_and_foreign_horsehair_removed(fixture,monkeypatch,tmp_path):
    c,plan,state,registry,real=fixture;vendor=tmp_path/'vendor';vendor.mkdir()
    foreign=tmp_path/'foreign';(foreign/'horsehair').mkdir(parents=True)
    monkeypatch.setenv('PYTHONPATH',os.pathsep.join([str(vendor),str(foreign),str(c['framework']/'src')]))
    monkeypatch.setenv('PYTHONHOME','invalid');monkeypatch.setenv('PYTHONSTARTUP','invalid');monkeypatch.setenv('PYTHONINSPECT','1')
    def runner(command,**kwargs):
        env=kwargs['env'];assert str(vendor.resolve()) in env['PYTHONPATH'].split(os.pathsep)
        assert str(foreign.resolve()) not in env['PYTHONPATH'].split(os.pathsep)
        assert str((c['framework']/'src').resolve()) not in env['PYTHONPATH'].split(os.pathsep)
        assert env['PYTHONNOUSERSITE']=='1' and env['PYTHONDONTWRITEBYTECODE']=='1' and env['TZ']=='UTC'
        assert not any(key in env for key in ('PYTHONHOME','PYTHONSTARTUP','PYTHONINSPECT'))
        assert command[1]=='-s' and '-I' not in command and kwargs['timeout']<=900
        assert kwargs['stdin']==subprocess.DEVNULL and kwargs['cwd']==m.CONSUMER_FRAMEWORK
        return real(command,**kwargs)
    m.prepare_view(None,c,runner=runner,clock=lambda:0)


def test_no_child_launch_without_native_margin_and_attempts_are_fresh(fixture):
    c,plan,state,registry,runner=fixture
    c['deadline']=600
    with pytest.raises(ValueError,match='margin'):m.prepare_view(None,c,runner=lambda *a,**k:pytest.fail('No launch'),clock=lambda:0)
    c['deadline']=10000;m.prepare_view(None,c,runner=runner,clock=lambda:0);m.prepare_view(None,c,runner=runner,clock=lambda:0)
    assert len(list(c['area'].glob('archive-read-*')))==3


def test_overlay_only_replaces_assembled_listed_snapshot_and_restores_on_failure(fixture,monkeypatch):
    c,plan,state,registry,runner=fixture;m.prepare_view(None,c,runner=runner,clock=lambda:0);events=[]
    original=lambda case,ep:events.append(('original',str(case))) or ['original']
    def original_collect(context,p,s,n):
        first=base.condition_snapshot(Path(registry['cells']['0']['evaluation_case']),{})
        second=base.condition_snapshot(c['case']/'unlisted',{})
        assert len(first)==300 and second==['original'];raise RuntimeError('later strict historical receipt failure')
    base=SimpleNamespace(__file__=str(FROZEN),source_guard=lambda ctx:events.append('guard'),
        prepare_census=lambda ctx:events.append('census'),collect_packets=original_collect,condition_snapshot=original,
        startup_admission=lambda ctx,**kwargs:None)
    m.install(base)
    with pytest.raises(RuntimeError,match='historical'):base.collect_packets(c,plan,state,{})
    assert base.condition_snapshot is original and events==[('original',str(c['case']/'unlisted'))]


def test_slow_child_runs_after_census_and_before_registration_mutex(fixture,monkeypatch):
    c,plan,state,registry,runner=fixture;events=[]
    base=SimpleNamespace(__file__=str(FROZEN),source_guard=lambda ctx:None,
        prepare_census=lambda ctx:events.append('census-outside-lock'),collect_packets=lambda *args:{},condition_snapshot=lambda *args:[],
        startup_admission=lambda ctx,**kwargs:None)
    monkeypatch.setattr(m,'prepare_view',lambda *args,**kwargs:events.append('wholef047-child-outside-lock'))
    monkeypatch.setattr(m,'refresh_qualification',lambda *args:events.append('fresh-owner-outside-lock'))
    m.install(base);base.prepare_census(c)
    assert events==['census-outside-lock','fresh-owner-outside-lock','wholef047-child-outside-lock']


@pytest.fixture
def qualification(tmp_path,monkeypatch):
    # Synthetic source/command fixtures, with the real cost and13-field native
    # parsers. Never supply ownership fields inside a fabricated cost row.
    root=tmp_path/'study';area=root/'maintenance/q';area.mkdir(parents=True)
    package=Path(agents.__file__).resolve().parents[2]
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'FRAMEWORK',package);monkeypatch.setattr(m,'PRIMARY',package)
    overlay=area/Path(m.__file__).name;shutil.copyfile(Path(m.__file__),overlay);monkeypatch.setattr(m,'__file__',str(overlay))
    alternate=root/'releases/consumer';(alternate/'src/horsehair').mkdir(parents=True)
    for name in ('core','slurm','repair'):shutil.copyfile(package/('src/horsehair/'+name+'.py'),alternate/('src/horsehair/'+name+'.py'))
    public_source=PUBLIC_FIXTURE_ROOT/'src/horsehair/agent_analysis_v2.py'
    assert digest(public_source)==m.VALIDATION_MODULE_SHA
    shutil.copyfile(public_source,alternate/'src/horsehair/agent_analysis_v2.py')
    monkeypatch.setattr(m,'CONSUMER_FRAMEWORK',alternate);monkeypatch.setattr(m,'VALIDATION_FRAMEWORK',alternate)
    monkeypatch.setattr(m,'source_identity',lambda path:{'sha256':m.SOURCE if Path(path)==package else m.CONSUMER_SOURCE})
    verifier=area/'verify-f2-condition-archives-v2-20261022.py';verifier.write_text('# disposable child transport source')
    monkeypatch.setattr(m,'VERIFIER_SHA',digest(verifier))
    frozen=area/FROZEN.name;shutil.copyfile(FROZEN,frozen);monkeypatch.setattr(m,'BASE_PATH',frozen)
    suites={};expected={}
    for filename in (m.OVERLAY_TEST,m.VERIFIER_TEST):
        source=area/filename;source.write_text('# disposable source contract only: '+filename)
        names=['synthetic_complete_one','synthetic_complete_two'];roster=sorted((source.stem,name) for name in names)
        tree=ET.Element('testsuite',tests='2',failures='0',errors='0',skipped='0')
        for name in names:ET.SubElement(tree,'testcase',classname=source.stem,name=name)
        junit=area/(source.stem+'.xml');junit.write_bytes(ET.tostring(tree))
        expected[filename]={'sha256':digest(source),'tests':2,'roster_sha256':canonical_hash(roster)}
        suites[filename]={'tests':{'tests':2,'failures':0,'errors':0,'skipped':0},'test_source':m.locator(source),'junit':m.locator(junit)}
    monkeypatch.setattr(m,'TEST_SUITES',expected)
    producer=area/'qualify-overlay.py';producer.write_text('# disposable prospective producer source')
    producer_locator=m.locator(producer)
    plan_path=root/'plans/q/plan.json';script=plan_path.with_name('q.sbatch');ledger_path=root/'state/q-submission.json'
    inputs={'overlay':m.locator(overlay),'verifier':m.locator(verifier),'base_controller':m.locator(frozen),
        'overlay_tests':suites[m.OVERLAY_TEST]['test_source'],'verifier_tests':suites[m.VERIFIER_TEST]['test_source'],
        'fixture_root':str(alternate)}
    pairs={'--overlay':str(overlay),'--overlay-sha256':digest(overlay),'--verifier':str(verifier),'--verifier-sha256':digest(verifier),
        '--base-controller':str(frozen),'--base-controller-sha256':m.BASE_SHA,'--overlay-tests':inputs['overlay_tests']['path'],
        '--verifier-tests':inputs['verifier_tests']['path'],'--qualification-case':str(area),'--native-plan':str(plan_path),
        '--native-ledger':str(ledger_path),'--native-script':str(script),'--native-task':'q-overlay'}
    command=shlex.join(['python','-u',str(producer),*[x for pair in pairs.items() for x in pair]])
    script.parent.mkdir(parents=True);script.write_text('#!/bin/bash\n'+command+'\n')
    task={'name':'q-overlay','script':str(script),'command':command,'script_sha256':canonical_hash(script.read_text()),
        'resources':{'cpus':4,'gpu':False,'hours':1,'memory':'16G','array':None}}
    plan={'release_identity':{'sha256':m.SOURCE},'tasks':[task]}
    ledger={'tasks':{'q-overlay':{'state':'submitted','job_id':'500','script_sha256':task['script_sha256']}}}
    registration={'plan':write(plan_path,plan),'ledger':write(ledger_path,ledger),'script':{'path':str(script),'sha256':digest(script)},
        'task_name':'q-overlay','native_job_id':'500'}
    driver=area/'registered-pair.py';driver.write_text('# mocked transport boundary only')
    monkeypatch.setattr(m,'DRIVER',driver);monkeypatch.setattr(m,'DRIVER_SHA',digest(driver))
    original_run=m.runpy.run_path
    def run(path,**kwargs):
        if Path(path)==driver:return {'checked_pair':lambda p,l,*args:(read_json(p),read_json(l))}
        return original_run(path,**kwargs)
    monkeypatch.setattr(m.runpy,'run_path',run)
    allocation={'JobId':'500','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING','NumCPUs':'4',
        'NumNodes':'1-1','MinMemoryNode':'16G','TimeLimit':'01:00:00','Command':str(script),'JobName':'hhw-q-overlay',
        'AllocTRES':'cpu=4,mem=16G,node=1','ReqTRES':'cpu=4,mem=16G,node=1'}
    q={'status':'complete_f2_archive_overlay_qualification','returncode':0,'source_sha256':m.SOURCE,
        'config_sha256':m.CONFIG,'source_verified_unchanged':True,'slurm_job_id':'500',
        'wrapper_sha256':digest(overlay),'base_controller_sha256':m.BASE_SHA,'verifier_sha256':m.VERIFIER_SHA,
        'consumer_qualification':m.CONSUMER_Q,'scientific_result':False,'admission_authority':False,'suites':suites,
        'qualification_driver':producer_locator,'native_registration':registration,'qualification_inputs':inputs,
        'producer_allocation':allocation,'resource_policy':{'path':str(m.POLICY),'sha256':m.POLICY_SHA}}
    native_ledger=root/'state/resource-budget.json'
    write(native_ledger,{'native_jobs':{},'leases':{},'spent_cpu_core_hours':0.,'spent_gpu_hours':0.})
    budget.native_accounting(root,['500'],runner=lambda command,**kwargs:subprocess.CompletedProcess(command,0,
        '500|500|COMPLETED|60|4|cpu=4,mem=16G,node=1|0:0|1024K\n',''))
    native={'user':'pwa209','account':'def-ptewarie_cpu','job_id_raw':'500','unallocated_metadata':False,
        'cpus':4,'elapsed_seconds':60,'alloc_tres':'cpu=4,mem=16G,node=1'}
    calls=[]
    def transport(command,**kwargs):
        calls.append(command)
        assert command==['sacct','--array','-j','500','--format='+repair.REPAIR_NATIVE_FORMAT,'--units=K','--noheader','--parsable2']
        assert 0<kwargs['timeout']<=60
        values=['500',native['job_id_raw'],'COMPLETED','0:0',str(native['elapsed_seconds']),str(native['cpus']),native['alloc_tres'],
            native.get('node_list','nc10000'),'2026-10-21T10:00:00','2026-10-21T10:01:00',native['user'],native['account'],'1024K']
        return subprocess.CompletedProcess(command,0,'|'.join(values)+'\n','')
    monkeypatch.setattr(m.subprocess,'run',transport)
    base=SimpleNamespace(slurm=slurm)
    c={'area':area/'controller','deadline':time.time()+3600};c['area'].mkdir()
    def bind():
        binding={**write(area/'qualification.json',q),'job_id':'500','producer':producer_locator}
        c['descriptor']={'archive_overlay':{'qualification':binding}}
        return binding
    def refresh():
        binding=bind();observation=m.refresh_qualification(base,c)
        return binding,observation
    return SimpleNamespace(area=area,q=q,native=native,base=base,context=c,bind=bind,refresh=refresh,calls=calls,
        script=script,plan=plan_path,ledger=ledger_path,producer=producer_locator)


def test_actual_source_and_full_unique_passing_junit_required(qualification):
    f=qualification;binding,observation=f.refresh()
    assert m.private_qualification(f.base,binding,observation=observation)==f.q
    assert len(f.calls)==1
    assert not {'user','account','unallocated_metadata','alloc_tres'} & set(read_json(f.area.parents[1]/'state/resource-budget.json')['native_jobs']['500'])


@pytest.mark.parametrize('failure',['bool_return','source','config','source_changed','base','verifier','public_q','count','skip',
    'foreign_owner','gpu','bool_cpus','zero_elapsed','unallocated','duplicate_case','test_source'])
def test_unqualified_overlay_cannot_replace_original_files(qualification,failure):
    f=qualification;area,q,native,base=f.area,f.q,f.native,f.base;suite=q['suites'][Path(__file__).name]
    if failure=='bool_return':q['returncode']=False
    if failure=='source':q['source_sha256']='0'*64
    if failure=='config':q['config_sha256']='0'*64
    if failure=='source_changed':q['source_verified_unchanged']=False
    if failure=='base':q['base_controller_sha256']='0'*64
    if failure=='verifier':q['verifier_sha256']='0'*64
    if failure=='public_q':q['consumer_qualification']={**q['consumer_qualification'],'job_id':'1'}
    if failure=='count':suite['tests']['tests']=1
    if failure=='skip':suite['tests']['skipped']=1
    if failure=='foreign_owner':native['user']='other'
    if failure=='gpu':native['alloc_tres']='cpu=4,gres/gpu=1'
    if failure=='bool_cpus':native['cpus']=True
    if failure=='zero_elapsed':native['elapsed_seconds']=0
    if failure=='unallocated':native['node_list']='Unknown'
    if failure=='duplicate_case':
        p=Path(suite['junit']['path']);tree=ET.fromstring(p.read_bytes());tree[1].set('name',tree[0].get('name'))
        p.write_bytes(ET.tostring(tree));suite['junit']=m.locator(p)
    if failure=='test_source':Path(suite['test_source']['path']).write_text('# changed')
    with pytest.raises(ValueError):
        binding,observation=f.refresh()
        m.private_qualification(base,binding,observation=observation)


@pytest.mark.parametrize('failure',['producer','input_verifier','missing_suite','command_prefix','extra_argument','allocation_command',
    'allocation_name','allocation_owner','allocation_gpu','allocation_empty_tres','array','registration_job','script_bytes','policy'])
def test_private_qualification_producer_is_exact_source_owned_registered_command(qualification,failure):
    f=qualification;q=f.q
    if failure=='producer':q['qualification_driver']={**f.producer,'sha256':'0'*64}
    if failure=='input_verifier':q['qualification_inputs']['verifier']['sha256']='0'*64
    if failure=='missing_suite':q['suites'].pop(m.VERIFIER_TEST)
    if failure in {'command_prefix','extra_argument','array'}:
        plan=read_json(f.plan);task=plan['tasks'][0]
        if failure=='command_prefix':task['command']='unrelated '+task['command']
        if failure=='extra_argument':task['command']+=' --unexpected ignored'
        if failure=='array':task['resources']['array']='0-1'
        q['native_registration']['plan']=write(f.plan,plan)
    if failure=='allocation_command':q['producer_allocation']['Command']=str(f.script.with_name('other.sbatch'))
    if failure=='allocation_name':q['producer_allocation']['JobName']='other'
    if failure=='allocation_owner':q['producer_allocation']['UserId']='other(1)'
    if failure=='allocation_gpu':q['producer_allocation']['AllocTRES']='cpu=4,gres/gpu=1'
    if failure=='allocation_empty_tres':q['producer_allocation']['ReqTRES']=''
    if failure=='registration_job':q['native_registration']['native_job_id']='501'
    if failure=='script_bytes':f.script.write_text('# changed script')
    if failure=='policy':q['resource_policy']['sha256']='0'*64
    with pytest.raises(ValueError):f.refresh()
    assert not f.calls and not list(f.context['area'].glob('archive-native-qualification-*'))


def test_owner_is_read_once_per_context_and_never_by_immutable_inside_lock_checks(qualification,monkeypatch):
    f=qualification;binding,first=f.refresh()
    assert len(f.calls)==1 and type(first) is m.QualificationObservation
    with pytest.raises(AttributeError):first.job_id='other'
    original=m.subprocess.run
    monkeypatch.setattr(m.subprocess,'run',lambda *a,**k:pytest.fail('No scheduler query during inside-lock checks'))
    assert m.private_qualification(f.base,binding,observation=first)==f.q
    assert m.private_qualification(f.base,binding,observation=first)==f.q
    monkeypatch.setattr(m.subprocess,'run',original)
    binding,second=f.refresh();assert len(f.calls)==2 and second is not first
    assert len(list(f.context['area'].glob('archive-native-qualification-*')))==2


@pytest.mark.parametrize('change',['cost','public_source','dependency','unrelated_cost'])
def test_current_selected_cost_and_source_rechecked_without_remote_read(qualification,monkeypatch,change):
    f=qualification;binding,observation=f.refresh()
    monkeypatch.setattr(m.subprocess,'run',lambda *a,**k:pytest.fail('No scheduler read under the kernel mutex'))
    if change in {'cost','unrelated_cost'}:
        path=m.ROOT/'state/resource-budget.json';ledger=read_json(path)
        if change=='cost':ledger['native_jobs']['500']['elapsed_seconds']=61
        else:ledger['native_jobs']['999']={'unrelated':'metadata'}
        write(path,ledger)
    else:
        source=m.strict(observation.source_json)
        if change=='dependency':
            # Emulate one changed file-hash result without writing the actual
            # package. The canonical source files remain untouched.
            path=Path(source['dependencies']['core']['path']);original=m.digest
            monkeypatch.setattr(m,'digest',lambda p:'0'*64 if Path(p)==path else original(p))
        else:
            path=Path(source['public_module']['path'])
            path.write_bytes(path.read_bytes()+b'\n# disposable changed source\n')
    if change=='unrelated_cost':assert m.private_qualification(f.base,binding,observation=observation)==f.q
    else:
        with pytest.raises(ValueError):m.private_qualification(f.base,binding,observation=observation)


def test_startup_and_tick_owner_reads_are_before_the_registration_mutex(qualification,monkeypatch):
    f=qualification;f.bind();events=[];c=f.context
    transport=m.subprocess.run
    def checked_transport(*args,**kwargs):
        assert not c.get('locked');events.append('owner-outside');return transport(*args,**kwargs)
    monkeypatch.setattr(m.subprocess,'run',checked_transport)
    monkeypatch.setattr(m,'overlay_binding',lambda base,descriptor,*,observation:
        m.private_qualification(base,descriptor['archive_overlay']['qualification'],observation=observation))
    monkeypatch.setattr(m,'prepare_view',lambda base,ctx:events.append('child-outside') if not ctx.get('locked') else pytest.fail('Locked child'))
    def original_startup(ctx,**kwargs):
        ctx['locked']=True
        try:base.source_guard(ctx);events.append('startup-inside')
        finally:ctx['locked']=False
    base=SimpleNamespace(__file__=str(FROZEN),slurm=slurm,source_guard=lambda ctx:None,
        startup_admission=original_startup,prepare_census=lambda ctx:events.append('census-outside'),
        collect_packets=lambda *args:{},condition_snapshot=lambda *args:[])
    m.install(base);base.startup_admission(c);base.prepare_census(c)
    c['locked']=True
    try:base.source_guard(c);events.append('tick-inside')
    finally:c['locked']=False
    assert events==['owner-outside','startup-inside','census-outside','owner-outside','child-outside','tick-inside']
    assert len(f.calls)==2


@pytest.mark.parametrize('observation',[None,{},('untyped','proof')])
def test_missing_or_untyped_observation_never_triggers_a_read_inside(qualification,monkeypatch,observation):
    f=qualification;binding=f.bind()
    monkeypatch.setattr(m.subprocess,'run',lambda *a,**k:pytest.fail('No fallback scheduler read'))
    with pytest.raises(ValueError):m.private_qualification(f.base,binding,observation=observation)


@pytest.mark.parametrize('raw',[b'{"a":1,"a":2}',b'{"a":NaN}',b'{"a":1e999}'])
def test_overlay_metadata_rejects_duplicate_or_nonfinite_values(raw):
    with pytest.raises(ValueError):m.strict(raw)


@pytest.mark.parametrize('maximum',[4,5])
def test_locator_has_exact_finite_byte_limit_without_eof_digest(tmp_path,monkeypatch,maximum):
    root=tmp_path/'study';path=root/'maintenance/bytes';path.parent.mkdir(parents=True);path.write_bytes(b'12345')
    monkeypatch.setattr(m,'ROOT',root)
    monkeypatch.setattr(m,'digest',lambda *args:pytest.fail('Unbounded digest is forbidden for locators'))
    if maximum==4:
        original=Path.open
        def unopened(target,*args,**kwargs):
            if target==path:pytest.fail('Oversized source must be rejected before open')
            return original(target,*args,**kwargs)
        monkeypatch.setattr(Path,'open',unopened)
        with pytest.raises(ValueError,match='Finite'):m.locator(path,maximum=maximum)
    else:
        assert m.locator(path,maximum=maximum)=={'path':str(path),'sha256':hashlib.sha256(b'12345').hexdigest()}


@pytest.mark.parametrize('phase',['before-child','locked-snapshot'])
def test_oversized_state_is_refused_before_child_or_locked_packet_use(fixture,monkeypatch,phase):
    c,plan,state,registry,runner=fixture
    if phase=='locked-snapshot':m.prepare_view(None,c,runner=runner,clock=lambda:0)
    path=c['case']/'state.json.gz';monkeypatch.setattr(m,'MAX_STATE',path.stat().st_size-1)
    original=Path.open
    def unopened(target,*args,**kwargs):
        if target==path:pytest.fail('Oversized state must not be opened or hashed')
        return original(target,*args,**kwargs)
    monkeypatch.setattr(Path,'open',unopened)
    with pytest.raises(ValueError,match='Finite'):
        if phase=='before-child':
            m.prepare_view(None,c,runner=lambda *a,**k:pytest.fail('No child launch for oversized state'),clock=lambda:0)
        else:m.snapshot_map(c,plan,state)


@pytest.mark.parametrize('kind',['plan','proof','state-binding'])
def test_default_metadata_locator_limit_precedes_locked_file_read(fixture,monkeypatch,kind):
    c,plan,state,registry,runner=fixture;m.prepare_view(None,c,runner=runner,clock=lambda:0)
    prepared=c['prepared_archive_view']
    path={'plan':c['case']/'campaign-plan.json','proof':Path(prepared['proof_locator']['path']),
        'state-binding':c['case']/'state-binding.json'}[kind]
    with path.open('wb') as stream:stream.seek(m.MAX_METADATA);stream.write(b'x')
    original=Path.open
    def unopened(target,*args,**kwargs):
        if target==path:pytest.fail('Oversized metadata must not be opened or hashed')
        return original(target,*args,**kwargs)
    monkeypatch.setattr(Path,'open',unopened)
    with pytest.raises(ValueError,match='Finite'):m.snapshot_map(c,plan,state)


def test_locator_refuses_growth_between_stat_and_stream_open(tmp_path,monkeypatch):
    root=tmp_path/'study';path=root/'maintenance/bytes';path.parent.mkdir(parents=True);path.write_bytes(b'1234')
    monkeypatch.setattr(m,'ROOT',root);original=Path.open
    def grow(target,*args,**kwargs):
        if target==path:
            with original(target,'ab') as stream:stream.write(b'5')
        return original(target,*args,**kwargs)
    monkeypatch.setattr(Path,'open',grow)
    with pytest.raises(ValueError,match='changed before bounded hash'):m.locator(path,maximum=4)


def test_locator_refuses_same_size_mutation_during_streamed_hash(tmp_path,monkeypatch):
    root=tmp_path/'study';path=root/'maintenance/bytes';path.parent.mkdir(parents=True);path.write_bytes(b'1234')
    monkeypatch.setattr(m,'ROOT',root);original=Path.open
    class Changed:
        def __init__(self,stream):self.stream=stream;self.changed=False
        def __enter__(self):return self
        def __exit__(self,*args):self.stream.close()
        def fileno(self):return self.stream.fileno()
        def read(self,size):
            raw=self.stream.read(size)
            if not self.changed:
                self.changed=True
                with original(path,'r+b') as stream:stream.write(b'4321')
                os.utime(path,ns=(path.stat().st_atime_ns,path.stat().st_mtime_ns+1_000_000_000))
            return raw
    def opened(target,*args,**kwargs):
        stream=original(target,*args,**kwargs)
        return Changed(stream) if target==path else stream
    monkeypatch.setattr(Path,'open',opened)
    with pytest.raises(ValueError,match='changed during bounded hash'):m.locator(path,maximum=4)


@pytest.mark.parametrize('job',['300','301','302','6684484','1889'])
def test_every_child_selected_cost_row_is_rechecked_inside_lock(fixture,monkeypatch,job):
    c,plan,state,registry,runner=fixture;m.prepare_view(None,c,runner=runner,clock=lambda:0)
    path=m.ROOT/'state/resource-budget.json';value=read_json(path)
    value['native_jobs'][job]['elapsed_seconds']+=1;write(path,value)
    monkeypatch.setattr(m.subprocess,'run',lambda *a,**k:pytest.fail('No native read inside locked packet substitution'))
    with pytest.raises(ValueError,match='Selected archive native authority changed'):m.snapshot_map(c,plan,state)


def test_unrelated_budget_updates_do_not_invalidate_exact_authority_rows(fixture,monkeypatch):
    c,plan,state,registry,runner=fixture;m.prepare_view(None,c,runner=runner,clock=lambda:0)
    path=m.ROOT/'state/resource-budget.json';value=read_json(path)
    value['native_jobs']['999']={**value['native_jobs']['300'],'job_id':'999','job_id_raw':'999'}
    value['unrelated_current_reservation']=123.;write(path,value)
    monkeypatch.setattr(m.subprocess,'run',lambda *a,**k:pytest.fail('No native read inside locked packet substitution'))
    original_locator=m.locator
    def selected_only(target,**kwargs):
        if Path(target)==path:pytest.fail('Unrelated mutable ledger bytes must not require a whole-file SHA')
        return original_locator(target,**kwargs)
    monkeypatch.setattr(m,'locator',selected_only)
    packets=m.snapshot_map(c,plan,state)[registry['cells']['0']['evaluation_case']]
    assert packets==[state['cells'][0]['conditions'][str(i)] for i in range(300)]


def test_selected_cost_change_while_child_runs_is_refused_before_publication(fixture):
    c,plan,state,registry,original=fixture
    def runner(*args,**kwargs):
        result=original(*args,**kwargs);path=m.ROOT/'state/resource-budget.json';value=read_json(path)
        value['native_jobs']['301']['elapsed_seconds']+=1;write(path,value);return result
    with pytest.raises(ValueError,match='Selected archive native authority changed'):
        m.prepare_view(None,c,runner=runner,clock=lambda:0)
    assert 'prepared_archive_view' not in c


def test_conflicting_same_job_authority_proofs_are_refused_before_child(fixture):
    c,plan,state,registry,runner=fixture;descriptor=registry['cells']['0']
    index=read_json(Path(descriptor['archive_directory'])/'index.json')
    proof=read_json(index['authority']['native']['completion']['path']);proof['native_records']['300']['elapsed_seconds']+=1
    proof['native_records_sha256']=canonical_hash(proof['native_records'])
    proof['completion_proof_sha256']=canonical_hash({k:v for k,v in proof.items() if k!='completion_proof_sha256'})
    descriptor['pack_producer']['native']={'completion':write(c['area']/'conflicting-completion.json',proof),
        'owner':index['authority']['native']['owner']}
    with pytest.raises(ValueError,match='Conflicting selected native authority rows'):
        m.prepare_view(None,c,runner=lambda *a,**k:pytest.fail('No conflicting child authority'),clock=lambda:0)


@pytest.mark.parametrize('change',['completion-source','authority-file','projection-file'])
def test_native_authority_proof_source_and_current_bytes_are_bound(fixture,change):
    c,plan,state,registry,runner=fixture;index=read_json(Path(registry['cells']['0']['archive_directory'])/'index.json')
    if change=='completion-source':
        binding=index['authority']['native']['completion'];proof=read_json(binding['path'])
        proof['source_path']=str(m.ROOT/'state/other-budget.json')
        proof['completion_proof_sha256']=canonical_hash({k:v for k,v in proof.items() if k!='completion_proof_sha256'})
        index['authority']['native']['completion']=write(Path(binding['path']),proof)
        descriptor=registry['cells']['0'];descriptor['index_sha256']=write(Path(descriptor['archive_directory'])/'index.json',index)['sha256']
        with pytest.raises(ValueError,match='source-bound selected native completion'):
            m.prepare_view(None,c,runner=lambda *a,**k:pytest.fail('No foreign ledger authority'),clock=lambda:0)
    else:
        m.prepare_view(None,c,runner=runner,clock=lambda:0)
        path=Path(index['authority']['native']['completion']['path']) if change=='authority-file' \
            else Path(c['prepared_archive_view']['native_projection_locator']['path'])
        path.write_bytes(b'{"changed":true}')
        with pytest.raises(ValueError,match='source bytes changed'):m.snapshot_map(c,plan,state)


def test_minimal_three_file_stage_has_no_old_fixture_dependencies(tmp_path):
    stage=tmp_path/'s';stage.mkdir()
    for source in (Path(__file__),Path(m.__file__),FROZEN):shutil.copyfile(source,stage/source.name)
    public=tmp_path/'q/src/horsehair/agent_analysis_v2.py';public.parent.mkdir(parents=True)
    shutil.copyfile(PUBLIC_FIXTURE_ROOT/'src/horsehair/agent_analysis_v2.py',public)
    assert digest(public)==m.VALIDATION_MODULE_SHA
    env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1',
        'HHW_OVERLAY_PUBLIC_FIXTURE_ROOT':str(tmp_path/'q')}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
    command=[sys.executable,'-s','-m','pytest',str(stage/Path(__file__).name),'-q','-p','no:cacheprovider','--rootdir',str(stage),
        '--deselect',Path(__file__).name+'::test_minimal_three_file_stage_has_no_old_fixture_dependencies','--basetemp',str(tmp_path/'f')]
    result=subprocess.run(command,cwd=stage,capture_output=True,text=True,timeout=180,env=env)
    assert result.returncode==0,result.stdout+result.stderr
    assert str(m.TEST_SUITES[Path(__file__).name]['tests']-1)+' passed' in result.stdout



def load_frozen_base():
    spec=importlib.util.spec_from_file_location('private_frozen_registry_v6_for_archive',FROZEN)
    base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)
    return base


def bind_retry(base,c,path,value):
    value['amendment_sha256']=canonical_hash({k:v for k,v in value.items() if k!='amendment_sha256'})
    binding={**write(path,value),'job_id':'99001'}
    c['descriptor']['prestart_retry_amendment']=binding
    c['registry_overlay']['prestart_retry_binding']=copy.deepcopy(binding)
    c['registry_overlay']['prestart_retry_value']=copy.deepcopy(value)
    return value


def retry_context(base,root,case,monkeypatch):
    monkeypatch.setattr(base,'ROOT',root)
    original=write(root/'maintenance/original-training0000-recovery.json',{'kind':'original0000-fixture'})
    c={'case':case,'descriptor':{'registry_overlay':{'recovery':original}},'registry_overlay':{
        'recovery_value':{'native_namespace_overrides':{'training-0000':'af-f2-r1-training-0000'},
            'submission_journal_overrides':{'training-0000':'submission-r1-training-0000.json'}}}}
    directory=root/'maintenance/study-af-prestart-three-recovery-v1-20261022';directory.mkdir(parents=True)
    path=directory/'amendment.json'
    value={'schema_version':1,'kind':'explicit-prestart-three-operational-recovery-v1',
        'status':'validated_target_only_state_derivative_not_activated','case':str(directory),
        'original_revision':405,'new_revision':406,'original_state_sha256':base.PRESTART_RETRY_ORIGINAL_STATE_SHA256,
        'new_state_sha256':'a'*64,'campaign_plan_sha256':base.PRESTART_RETRY_CAMPAIGN_PLAN_SHA256,
        'source_sha256':base.FRAMEWORK_SHA,'config_sha256':base.BASE_CONFIG_SHA256,
        'dispatcher_sha256':base.PRESTART_RETRY_DISPATCHER_SHA256,'targets':copy.deepcopy(base.PRESTART_RETRY_TARGETS),
        'native_namespace_overrides':{t:'af-f2-r2-'+t for t in base.PRESTART_RETRY_TARGETS},
        'submission_journal_overrides':{t:'submission-r2-'+t+'.json' for t in base.PRESTART_RETRY_TARGETS},
        'old_recovery':original,'activation_producer_job_id':'99001','scientific_result':False,'admission_authority':False}
    bind_retry(base,c,path,value)
    budget.native_accounting(root,['99001'],runner=lambda command,**kwargs:subprocess.CompletedProcess(command,0,
        '99001|99001|COMPLETED|60|4|cpu=4,mem=16G,node=1|0:0|1024K\n',''))
    return c,value,path


@pytest.fixture
def registry_v6(tmp_path,monkeypatch):
    root=tmp_path/'r';(root/'state').mkdir(parents=True);case=root/'maintenance/campaign';case.mkdir(parents=True)
    monkeypatch.setattr(m,'ROOT',root)
    base=load_frozen_base();c,value,path=retry_context(base,root,case,monkeypatch)
    return base,c,value,path


def test_frozen_registry_v6_ast_and_all_untouched_function_objects_survive_install(registry_v6):
    base,c,value,path=registry_v6
    assert m.BASE_SHA=='e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
    assert m.BASE_PATH.name=='run-af-agent-controller-registry-v6-20261022.py' and digest(FROZEN)==m.BASE_SHA
    functions={name:function for name,function in vars(base).items() if inspect.isfunction(function)}
    ast_before={name:ast.dump(ast.parse(inspect.getsource(function)),include_attributes=False) for name,function in functions.items()}
    m.install(base)
    hooks={'source_guard','startup_admission','prepare_census','collect_packets'}
    for name,function in functions.items():
        if name not in hooks:
            assert getattr(base,name) is function
            assert ast.dump(ast.parse(inspect.getsource(getattr(base,name))),include_attributes=False)==ast_before[name]
    for hook,closure_name in [('source_guard','original_guard'),('startup_admission','original_startup'),
        ('prepare_census','original_prepare'),('collect_packets','original_collect')]:
        assert inspect.getclosurevars(getattr(base,hook)).nonlocals[closure_name] is functions[hook]
    assert base.load_context is functions['load_context'] and base.refresh_retry_producer_accounting is functions['refresh_retry_producer_accounting']


def test_base_load_context_keeps_actual_producer_refresh_before_retry_binding_and_startup(registry_v6):
    base,c,value,path=registry_v6;m.install(base)
    calls=[node.func.id for node in ast.walk(ast.parse(inspect.getsource(base.load_context)))
        if isinstance(node,ast.Call) and isinstance(node.func,ast.Name)]
    assert calls.index('refresh_retry_producer_accounting')<calls.index('retry_binding')<calls.index('startup_admission')


def test_archive_overlay_preserves_original0000_and_only_three_bound_r2_maps(registry_v6):
    base,c,value,path=registry_v6
    original=copy.deepcopy(c['registry_overlay']['recovery_value']);m.install(base)
    assert base.retry_binding(c['descriptor'])==value
    for target in base.PRESTART_RETRY_TARGETS:
        assert base.native_namespace(c,target)=='af-f2-r2-'+target
        assert base.submission_journal(c,target)==c['case']/('submission-r2-'+target+'.json')
    assert base.native_namespace(c,'training-0000')=='af-f2-r1-training-0000'
    assert base.submission_journal(c,'training-0000').name=='submission-r1-training-0000.json'
    for other in ('training-0001','training-0078','evaluation-0073-02','assembly-0073'):
        assert base.native_namespace(c,other)=='af-f2-'+other
        assert base.submission_journal(c,other).name=='submission-'+other+'.json'
    assert c['registry_overlay']['recovery_value']==original


def test_archive_overlay_keeps_optional_retry_absent_and_lookup_free_of_native_queries(registry_v6,monkeypatch):
    base,c,value,path=registry_v6;m.install(base)
    def unexpected(*args,**kwargs):pytest.fail('Namespace lookup must use the validated in-memory retry cache')
    monkeypatch.setattr(base,'retry_binding',unexpected);monkeypatch.setattr(base.slurm,'completed_native_dependency',unexpected)
    for _ in range(10):
        assert base.native_namespace(c,'training-0076')=='af-f2-r2-training-0076'
        assert base.submission_journal(c,'evaluation-0073-01').name=='submission-r2-evaluation-0073-01.json'
    c['descriptor'].pop('prestart_retry_amendment');c['registry_overlay'].pop('prestart_retry_binding')
    c['registry_overlay']['prestart_retry_value']=None
    assert base.native_namespace(c,'training-0076')=='af-f2-training-0076'
    assert base.native_namespace(c,'training-0000')=='af-f2-r1-training-0000'


@pytest.mark.parametrize('change',['source','config','target','namespace_extra','journal_extra','old_recovery','producer','case','cleared'])
def test_archive_candidate_refuses_resealed_retry_scope_or_source_expansion(registry_v6,change):
    base,c,value,path=registry_v6;m.install(base)
    if change=='source':value['source_sha256']='b'*64
    if change=='config':value['config_sha256']='b'*64
    if change=='target':value['targets']['training-0076']='1'
    if change=='namespace_extra':value['native_namespace_overrides']['training-0000']='forbidden'
    if change=='journal_extra':value['submission_journal_overrides']['training-0078']='forbidden.json'
    if change=='old_recovery':value['old_recovery']={'path':'/another/amendment.json','sha256':'b'*64}
    if change=='producer':value['activation_producer_job_id']='99002'
    if change=='case':value['case']=str(path.parent.parent/'other-case')
    if change=='cleared':value['scientific_result']=True
    bind_retry(base,c,path,value)
    with pytest.raises(ValueError):base.retry_binding(c['descriptor'])
    with pytest.raises(ValueError):base.native_namespace(c,'training-0076')
    with pytest.raises(ValueError):base.submission_journal(c,'training-0076')


@pytest.mark.parametrize('change',['stale_bytes','foreign_path','failed_native','foreign_ledger','missing_native'])
def test_archive_candidate_requires_actual_bound_retry_bytes_and_positive_final_producer(registry_v6,change):
    base,c,value,path=registry_v6;m.install(base)
    ledger=m.ROOT/'state/resource-budget.json'
    if change=='stale_bytes':path.write_bytes(path.read_bytes()+b'\n')
    if change=='foreign_path':
        other=path.parent.parent/'foreign-amendment.json';binding=write(other,value)
        c['descriptor']['prestart_retry_amendment'].update(binding)
    if change=='failed_native':
        record=read_json(ledger);record['native_jobs']['99001']['state']='FAILED';write(ledger,record)
    if change=='foreign_ledger':
        foreign=ledger.with_name('other-budget.json');foreign.write_bytes(ledger.read_bytes());write(ledger,{'native_jobs':{}})
    if change=='missing_native':write(ledger,{'native_jobs':{}})
    with pytest.raises(ValueError):base.retry_binding(c['descriptor'])


def test_archive_install_retains_actual_retry_refresh_lock_admission_and_error_semantics(registry_v6,monkeypatch):
    from contextlib import contextmanager
    base,c,value,path=registry_v6;events=[];locked=[False]
    @contextmanager
    def registration(context,role):
        assert role=='controller-start' and not locked[0];locked[0]=True;events.append('lock')
        try:yield
        finally:locked[0]=False;events.append('unlock')
    def admit(context,label,**resources):
        assert locked[0] and label=='retry-producer-accounting' and resources=={'cpus':0,'gpus':0,'hours':1}
        events.append('original-admission')
    monkeypatch.setattr(base,'registration_guard',registration);monkeypatch.setattr(base,'admission',admit)
    original=base.refresh_retry_producer_accounting;m.install(base)
    assert base.refresh_retry_producer_accounting is original
    base.refresh_retry_producer_accounting(c)
    assert events==['lock','original-admission','unlock'] and not locked[0]
    def refused(*args,**kwargs):raise ValueError('actual original admission refusal')
    monkeypatch.setattr(base,'admission',refused)
    with pytest.raises(ValueError,match='original admission refusal'):base.refresh_retry_producer_accounting(c)
    assert not locked[0]


def test_bound_r2_map_and_archive_full300_packets_coexist_without_reader_changes(fixture,monkeypatch):
    c,plan,state,registry,runner=fixture;base=load_frozen_base()
    retry,value,path=retry_context(base,m.ROOT,c['case'],monkeypatch)
    c['descriptor'].update(retry['descriptor']);c['registry_overlay']=retry['registry_overlay']
    m.prepare_view(base,c,runner=runner,clock=lambda:0)
    original_snapshot=base.condition_snapshot;seen=[]
    def collect(context,plan,state,native):
        seen.append(base.condition_snapshot(Path(registry['cells']['0']['evaluation_case']),{}))
        assert base.native_namespace(context,'training-0076')=='af-f2-r2-training-0076'
        assert base.submission_journal(context,'training-0000').name=='submission-r1-training-0000.json'
        return {'unchanged-original-reader-result':True}
    base.collect_packets=collect;m.install(base)
    assert base.collect_packets(c,plan,state,{})=={'unchanged-original-reader-result':True}
    assert seen==[[state['cells'][0]['conditions'][str(i)] for i in range(300)]]
    assert base.condition_snapshot is original_snapshot and base.retry_binding(c['descriptor'])==value


@pytest.mark.parametrize('change',['producer_running','producer_failed','producer_zero','resealed_wrong_source','resealed_wrong_job'])
def test_archive_packet_substitution_requires_positive_finalized_pack_native_proof(fixture,change):
    c,plan,state,registry,runner=fixture;descriptor=registry['cells']['0']
    if change.startswith('producer_'):
        ledger=m.ROOT/'state/resource-budget.json';value=read_json(ledger)
        if change=='producer_running':value['native_jobs']['301']['state']='RUNNING'
        if change=='producer_failed':value['native_jobs']['301']['state']='FAILED'
        if change=='producer_zero':value['native_jobs']['301']['elapsed_seconds']=0
        write(ledger,value)
    else:
        binding=descriptor['pack_producer']['native']['completion'];proof=read_json(binding['path'])
        if change=='resealed_wrong_source':proof['source_path']=str(m.ROOT/'state/wrong-budget.json')
        else:proof['job_id']='999'
        proof['completion_proof_sha256']=canonical_hash({k:v for k,v in proof.items() if k!='completion_proof_sha256'})
        descriptor['pack_producer']['native']['completion']=write(Path(binding['path']),proof)
    with pytest.raises(ValueError):m.prepare_view(None,c,runner=lambda *a,**k:pytest.fail('No child with unfinalized or foreign producer'),clock=lambda:0)
    assert 'prepared_archive_view' not in c
