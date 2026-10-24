"""Generic scheduler contracts only; no remote or scientific work is executed."""
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
import ast
import hashlib
import importlib.util
import json
import math
import re
import time

import pytest

HERE=Path(__file__).resolve().parent
REPO=HERE.parents[2]
BASE=HERE/'run-af-agent-controller-backfill-base-v1-20261024.py'
OUTER=HERE/'run-af-agent-controller-backfill-policy-v1-20261024.py'
OLD_BASE=REPO/'operations/20261023/prestart-three-continuation/run-af-agent-controller-registry-v6-20261022.py'
OLD_OUTER=REPO/'operations/20261023/census-and-controller-resumption-v1/census-v5/run-af-agent-controller-census-policy-v1-20261023.py'
if not OLD_BASE.is_file():
    OLD_BASE=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-registry-repair-support-v6-20261022/run-af-agent-controller-registry-v6-20261022.py')
    OLD_OUTER=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v4-20261023/run-af-agent-controller-census-policy-v1-20261023.py')
OLD_BASE_SHA='e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
OLD_OUTER_SHA='120f02d098b6c9afa0ea0df54f590ae381b4227d4892ba0f9ee4acc93e6fec0f'


def functions(path):
    return {node.name:node for node in ast.parse(path.read_text()).body if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef))}


def pure_base():
    nodes=functions(BASE)
    namespace={'math':math,'re':re,'json':json,'time':time,'datetime':datetime,'timezone':timezone,
        'BACKFILL_MIN_SECONDS':8*3600,'BACKFILL_MAX_SECONDS':96*3600,
        'BACKFILL_SHUTDOWN_RESERVE_SECONDS':4*3600,'CONTROLLER_TICK_SECONDS':180}
    module=ast.Module(body=[nodes[name] for name in ('require','seconds','native_backfill_window','run')],type_ignores=[])
    exec(compile(module,str(BASE),'exec'),namespace)
    return namespace


def allocation(hours=8):
    start=1_800_000_000
    stamp=lambda value:datetime.fromtimestamp(value,timezone.utc).replace(tzinfo=None).isoformat()
    return {'JobId':'123','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
        'Command':str(Path('/scratch/controller.sbatch')),'JobName':'hhw-test-controller','NumCPUs':'1','NumNodes':'1',
        'TimeLimit':str(hours)+':00:00','TimeMin':'08:00:00','MinMemoryNode':'64G',
        'StartTime':stamp(start),'EndTime':stamp(start+hours*3600),'AllocTRES':'cpu=1,mem=64G,node=1',
        'ReqTRES':'cpu=1,mem=64G,node=1'},start,start+hours*3600


@pytest.mark.parametrize('hours',[8,24,96])
def test_true_native_effective_window_retains_four_hour_shutdown_reserve(hours):
    module=pure_base();fields,start,end=allocation(hours)
    value=module['native_backfill_window'](fields,float(end),now=float(start+2))
    assert value=={'allocation_end_unix':float(end),'effective_seconds':hours*3600,
        'work_deadline_unix':float(end-4*3600),'shutdown_reserve_seconds':14400}


@pytest.mark.parametrize('change',['too_short','too_long','wrong_min','missing_min','end_mismatch','duration_mismatch',
    'future_start','no_shutdown_reserve','nonfinite_clock','boolean_end','nonfinite_end'])
def test_false_or_insufficient_effective_allocation_is_rejected(change):
    module=pure_base();fields,start,end=allocation();now=float(start+2);environment_end=float(end)
    if change=='too_short':fields['TimeLimit']='07:59:00'
    if change=='too_long':fields['TimeLimit']='96:01:00'
    if change=='wrong_min':fields['TimeMin']='04:00:00'
    if change=='missing_min':fields.pop('TimeMin')
    if change=='end_mismatch':environment_end+=3
    if change=='duration_mismatch':fields['TimeLimit']='09:00:00'
    if change=='future_start':now=start-3
    if change=='no_shutdown_reserve':now=end-4*3600
    if change=='nonfinite_clock':now=float('nan')
    if change=='boolean_end':environment_end=True
    if change=='nonfinite_end':environment_end=float('inf')
    with pytest.raises(ValueError):module['native_backfill_window'](fields,environment_end,now=now)


@pytest.fixture
def outer():
    spec=importlib.util.spec_from_file_location('candidate_f2_backfill_outer',OUTER)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_outer_accepts_shortened_native_allocation_without_changing_registered_maximum(outer):
    module=pure_base();fields,_,_=allocation()
    base=SimpleNamespace(**module)
    outer.outer_native(base,fields,'123','test-controller',Path('/scratch/controller.sbatch'))
    assert outer.BACKFILL_RUNTIME=={'requested_hours':96,'minimum_hours':8,'shutdown_reserve_seconds':14400,'memory':'64G','cpus':1}


@pytest.mark.parametrize('change',['cpu','memory','owner','array','gpu','script','effective_time','time_min'])
def test_outer_preserves_owned_scalar_resource_and_command_contract(outer,change):
    module=pure_base();fields,_,_=allocation()
    if change=='cpu':fields['NumCPUs']='2'
    if change=='memory':fields['MinMemoryNode']='16G'
    if change=='owner':fields['UserId']='other(1)'
    if change=='array':fields['ArrayTaskId']='0'
    if change=='gpu':fields['AllocTRES']+=',gres/gpu=1'
    if change=='script':fields['Command']='/other/script'
    if change=='effective_time':fields['TimeLimit']='04:00:00'
    if change=='time_min':fields['TimeMin']='04:00:00'
    with pytest.raises(ValueError):outer.outer_native(SimpleNamespace(**module),fields,'123','test-controller',Path('/scratch/controller.sbatch'))


def test_runtime_uses_effective_deadline_and_keeps_conservative_lease(tmp_path):
    module=pure_base();fields,start,end=allocation();window=module['native_backfill_window'](fields,float(end),now=float(start+2))
    clock=[float(start+2)];ticks=[];writes=[];leases=[]
    @contextmanager
    def resource_lease(*args,**kwargs):
        leases.append(kwargs);yield
    def tick(context):
        ticks.append(clock[0]);clock[0]+=5
        return {'status':'still_running'}
    module.update(ROOT=tmp_path,resource_lease=resource_lease,tick=tick,
        PendingStorage=type('PendingStorage',(ValueError,),{}),PendingRegistration=type('PendingRegistration',(ValueError,),{}),
        atomic_json=lambda path,value:writes.append((path,value)))
    context={'config':{},'job_id':'123','policy':{},'segment':0,'case':tmp_path,'area':tmp_path,
        'deadline':window['work_deadline_unix'],'backfill_window':window}
    result=module['run'](context,clock=lambda:clock[0],sleeper=lambda delay:clock.__setitem__(0,clock[0]+delay))
    assert 0<len(ticks)<1920
    assert all(value+180<context['deadline'] for value in ticks)
    assert clock[0]<=end-4*3600
    assert leases==[{'cpus':1,'gpus':0,'max_hours':96,'job_id':'123','resource_policy':{}}]
    assert result['status']=='finite_segment_complete' and result['backfill_window']==window
    assert result['new_tick_cutoff_unix']==context['deadline']-180


def test_no_tick_begins_at_or_after_cutoff(tmp_path):
    module=pure_base();calls=[]
    @contextmanager
    def resource_lease(*args,**kwargs):yield
    module.update(ROOT=tmp_path,resource_lease=resource_lease,tick=lambda context:pytest.fail('Late tick started'),
        atomic_json=lambda *args:None)
    context={'config':{},'job_id':'123','policy':{},'segment':0,'area':tmp_path,'deadline':1000,'backfill_window':{}}
    result=module['run'](context,clock=lambda:820)
    assert result['status']=='finite_segment_complete'


def test_new_base_is_pinned_and_original_source_bytes_are_preserved(outer):
    assert hashlib.sha256(BASE.read_bytes()).hexdigest()==outer.BACKFILL_BASE_SHA
    assert hashlib.sha256(OLD_BASE.read_bytes()).hexdigest()==OLD_BASE_SHA
    assert hashlib.sha256(OLD_OUTER.read_bytes()).hexdigest()==OLD_OUTER_SHA


def test_unchanged_controller_functions_retain_original_ast():
    old=functions(OLD_BASE);new=functions(BASE)
    changed={'registry_binding','load_context','run'}
    assert set(new)==set(old)|{'native_backfill_window'}
    for name in set(old)-changed:
        assert ast.dump(new[name],include_attributes=False)==ast.dump(old[name],include_attributes=False),name


def test_unchanged_provider_and_submission_functions_retain_original_ast():
    old=functions(OLD_OUTER);new=functions(OUTER)
    changed={'load_base','outer_native','main','policy_guard'}
    for name in set(old)-changed:
        assert ast.dump(new[name],include_attributes=False)==ast.dump(old[name],include_attributes=False),name


def test_old_provider_qualification_cannot_stand_in_for_new_backfill_qualification(outer,monkeypatch):
    monkeypatch.setattr(outer,'json_packet',lambda *args,**kwargs:{'status':'complete_rolling_census_operational_qualification'})
    with pytest.raises(ValueError,match='new source-bound backfill'):
        outer.backfill_qualification({'backfill_qualification':{'path':'/scratch/new.json','sha256':'0'*64,'job_id':'123'}})
