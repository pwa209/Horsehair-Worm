import pytest
from horsehair.budget import resource_lease
from horsehair.budget import native_accounting
from types import SimpleNamespace
from horsehair.core import atomic_json
from horsehair.core import read_json


def test_parallel_gpu_and_resource_budget_enforced(tmp_path):
    c={'resources':{'cpu_ceiling':16,'cpu_core_hours_cap':100,'gpu_hours_cap':10}}
    with resource_lease(tmp_path,c,cpus=4,gpus=1,max_hours=2,job_id='1'):
        with pytest.raises(ValueError,match='concurrency'):
            with resource_lease(tmp_path,c,cpus=4,gpus=1,max_hours=2,job_id='2'):pass
    r=read_json(tmp_path/'state/resource-budget.json')
    assert r['leases']['1']['state']=='finished'
    assert r['spent_gpu_hours']>=0


def test_hours_are_reserved_before_running(tmp_path):
    c={'resources':{'cpu_ceiling':16,'cpu_core_hours_cap':1,'gpu_hours_cap':10}}
    with pytest.raises(ValueError,match='CPU-hour'):
        with resource_lease(tmp_path,c,cpus=4,gpus=0,max_hours=2,job_id='1'):pass


def test_native_accounting_excludes_steps_and_is_idempotent(tmp_path):
    runner=lambda *a,**k:SimpleNamespace(returncode=0,stdout='10|FAILED|60|4|cpu=4,gres/gpu=1,gres/gpu:a100=1|1:0\n10.batch|FAILED|60|4|cpu=4|1:0\n11|RUNNING|40|8|cpu=8|0:0\n')
    first=native_accounting(tmp_path,['10','11'],runner)
    second=native_accounting(tmp_path,['10','11'],runner)
    assert first==second
    assert first['spent_cpu_core_hours']==pytest.approx(4/60)
    assert first['spent_gpu_hours']==pytest.approx(1/60)
    assert first['pending_native_records']==['11']


def test_only_terminal_native_record_releases_crashed_lease(tmp_path):
    atomic_json(tmp_path/'state/resource-budget.json',{'spent_cpu_core_hours':0,'spent_gpu_hours':0,'leases':{'10':{'state':'running','cpus':4,'gpus':0,'max_hours':2}}})
    running=lambda *a,**k:SimpleNamespace(returncode=0,stdout='10|RUNNING|60|4|cpu=4|0:0\n')
    native_accounting(tmp_path,['10'],running)
    assert read_json(tmp_path/'state/resource-budget.json')['leases']['10']['state']=='running'
    final=lambda *a,**k:SimpleNamespace(returncode=0,stdout='10|TIMEOUT|7200|4|cpu=4|0:0\n')
    native_accounting(tmp_path,['10'],final)
    result=read_json(tmp_path/'state/resource-budget.json')
    assert result['leases']['10']['state']=='native_reconciled'
    assert result['spent_cpu_core_hours']==8


def test_native_accounting_keeps_maximum_step_memory(tmp_path):
    runner=lambda *a,**k:SimpleNamespace(returncode=0,stdout='10|COMPLETED|60|8|cpu=8|0:0|\n10.batch|COMPLETED|60|8|cpu=8|0:0|12G\n10.extern|COMPLETED|60|8|cpu=8|0:0|1M\n')
    native_accounting(tmp_path,['10'],runner)
    assert '|12G|' in (tmp_path/'state/native-accounting.tsv').read_text()

