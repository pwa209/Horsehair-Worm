import pytest
from horsehair.budget import resource_lease
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

