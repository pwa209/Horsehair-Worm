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
    runner=lambda *a,**k:SimpleNamespace(returncode=0,stdout='10|10|FAILED|60|4|cpu=4,gres/gpu=1,gres/gpu:a100=1|1:0|\n10.batch|10.batch|FAILED|60|4|cpu=4|1:0|\n11|11|RUNNING|40|8|cpu=8|0:0|\n')
    first=native_accounting(tmp_path,['10','11'],runner)
    second=native_accounting(tmp_path,['10','11'],runner)
    assert first==second
    assert first['spent_cpu_core_hours']==pytest.approx(4/60)
    assert first['spent_gpu_hours']==pytest.approx(1/60)
    assert first['pending_native_records']==['11']


def test_only_terminal_native_record_releases_crashed_lease(tmp_path):
    atomic_json(tmp_path/'state/resource-budget.json',{'spent_cpu_core_hours':0,'spent_gpu_hours':0,'leases':{'10':{'state':'running','cpus':4,'gpus':0,'max_hours':2}}})
    running=lambda *a,**k:SimpleNamespace(returncode=0,stdout='10|10|RUNNING|60|4|cpu=4|0:0|\n')
    native_accounting(tmp_path,['10'],running)
    assert read_json(tmp_path/'state/resource-budget.json')['leases']['10']['state']=='running'
    final=lambda *a,**k:SimpleNamespace(returncode=0,stdout='10|10|TIMEOUT|7200|4|cpu=4|0:0|\n')
    native_accounting(tmp_path,['10'],final)
    result=read_json(tmp_path/'state/resource-budget.json')
    assert result['leases']['10']['state']=='native_reconciled'
    assert result['spent_cpu_core_hours']==8


def test_native_accounting_keeps_maximum_step_memory(tmp_path):
    runner=lambda *a,**k:SimpleNamespace(returncode=0,stdout='10|10|COMPLETED|60|8|cpu=8|0:0|\n10.batch|10.batch|COMPLETED|60|8|cpu=8|0:0|12G\n10.extern|10.extern|COMPLETED|60|8|cpu=8|0:0|1M\n')
    native_accounting(tmp_path,['10'],runner)
    assert '|12G|' in (tmp_path/'state/native-accounting.tsv').read_text()


def test_unallocated_cancelled_jobs_cost_zero_without_invalid_benchmark_rows(tmp_path):
    runner=lambda *a,**k:SimpleNamespace(returncode=0,stdout='10|10|CANCELLED|0|0||0:0|\n')
    result=native_accounting(tmp_path,['10'],runner)
    assert result['spent_cpu_core_hours']==0
    assert (tmp_path/'state/native-accounting.tsv').read_text().strip()=='JobID|JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES|ExitCode'


def test_array_raw_identity_migration_reconciles_once_without_double_charge(tmp_path):
    old={'state':'COMPLETED','exit_code':'0:0','elapsed_seconds':3600,'cpus':4,'gpus':1,
         'cpu_core_hours':4.,'gpu_hours':1.,'basis':'sacct final allocated wall time'}
    path=tmp_path/'state/resource-budget.json'
    atomic_json(path,{'spent_cpu_core_hours':8.,'spent_gpu_hours':2.,'native_jobs':{'901':old},
                     'leases':{'800_0':{'state':'finished','cpus':4,'gpus':1,'max_hours':2,'elapsed_hours':1.}}})
    (tmp_path/'state/native-accounting.tsv').write_text('JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES\n901|COMPLETED|3600|4|2G|cpu=4,gres/gpu=1\n')
    calls=[]
    def runner(command,**kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0,stdout='800_0|901|COMPLETED|3600|4|cpu=4,gres/gpu=1|0:0|\n800_0.batch|901.batch|COMPLETED|3600|4|cpu=4|0:0|3G\n')
    result=native_accounting(tmp_path,['800'],runner)
    assert not result['pending_native_records']
    first=read_json(path)
    native_accounting(tmp_path,['800'],runner)
    assert read_json(path)==first
    assert first['spent_cpu_core_hours']==4 and first['spent_gpu_hours']==1
    assert set(first['native_jobs'])=={'800_0'} and first['leases']['800_0']['state']=='native_reconciled'
    assert first['native_raw_aliases']=={'901':'800_0'}
    lines=(tmp_path/'state/native-accounting.tsv').read_text().splitlines()
    assert len(lines)==2 and lines[1].startswith('800_0|901|') and '|3G|' in lines[1]
    assert '--format=JobID%64,JobIDRaw%64,State,ElapsedRaw,AllocCPUS,AllocTRES,ExitCode,MaxRSS' in calls[0]


def test_conflicting_array_alias_cannot_mutate_budget(tmp_path):
    path=tmp_path/'state/resource-budget.json'
    atomic_json(path,{'spent_cpu_core_hours':0.,'spent_gpu_hours':0.,'leases':{}})
    before=path.read_bytes()
    runner=lambda *a,**k:SimpleNamespace(returncode=0,stdout='800_0|901|COMPLETED|60|4|cpu=4|0:0|\n800_1|901|COMPLETED|60|4|cpu=4|0:0|\n')
    with pytest.raises(ValueError,match='Conflicting'):
        native_accounting(tmp_path,['800'],runner)
    assert path.read_bytes()==before


def test_running_raw_lease_preserves_live_owner_key_until_terminal_native_proof(tmp_path):
    config={'resources':{'cpu_ceiling':8,'cpu_core_hours_cap':100,'gpu_hours_cap':10}}
    running=lambda *a,**k:SimpleNamespace(returncode=0,stdout='800_0|901|RUNNING|60|4|cpu=4|0:0|\n')
    final=lambda *a,**k:SimpleNamespace(returncode=0,stdout='800_0|901|COMPLETED|3600|4|cpu=4|0:0|\n')
    with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='901'):
        result=native_accounting(tmp_path,['800'],running)
        record=read_json(tmp_path/'state/resource-budget.json')
        assert record['native_raw_aliases']=={'901':'800_0'}
        assert record['leases']['901']['state']=='running' and '800_0' not in record['leases']
        assert result['pending_native_records']==['800']
    assert read_json(tmp_path/'state/resource-budget.json')['leases']['901']['state']=='finished'
    native_accounting(tmp_path,['800'],final)
    record=read_json(tmp_path/'state/resource-budget.json')
    assert set(record['leases'])=={'800_0'} and record['leases']['800_0']['state']=='native_reconciled'
    assert record['spent_cpu_core_hours']==4

