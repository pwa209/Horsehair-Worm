import pytest
from horsehair.budget import resource_lease
from horsehair.budget import native_accounting
from types import SimpleNamespace
from horsehair.core import atomic_json
from horsehair.core import read_json
from horsehair.core import ReceiptLockTimeout,canonical_hash
from horsehair import budget
from contextlib import contextmanager


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


def finish_contention_fixture(monkeypatch,*,failures=None,native_remaining=None):
    """Controlled acquisition waits, retaining real kernel/atomic writes on success."""
    clock=[1000.];calls=[];original=budget.exclusive_file_lock
    monkeypatch.setattr(budget.time,'time',lambda:clock[0])
    monkeypatch.setattr(budget.time,'monotonic',lambda:clock[0])
    monkeypatch.delenv('SLURM_JOB_END_TIME',raising=False)
    if native_remaining is not None:monkeypatch.setenv('SLURM_JOB_END_TIME',str(clock[0]+native_remaining))
    @contextmanager
    def guard(path,timeout=30):
        calls.append(timeout)
        if len(calls)>1 and (failures is None or len(calls)<=failures+1):
            clock[0]+=timeout
            raise ReceiptLockTimeout(path,timeout,{'pid':42,'state':'held','authority':'informational fixture'})
        with original(path,timeout=timeout):yield
    monkeypatch.setattr(budget,'exclusive_file_lock',guard)
    config={'resources':{'cpu_ceiling':8,'cpu_core_hours_cap':100,'gpu_hours_cap':10}}
    return clock,calls,original,config


def test_finish_retries_after_thirty_second_lustre_wait_without_losing_science_error(tmp_path,monkeypatch):
    clock,calls,_,config=finish_contention_fixture(monkeypatch,failures=1)
    error=ValueError('Nonestimable scientific design fixture')
    with pytest.raises(ValueError) as caught:
        with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='200_3'):
            clock[0]+=10
            raise error
    assert caught.value is error and calls==[30,30.,30.]
    record=read_json(tmp_path/'state/resource-budget.json')
    assert record['leases']['200_3']['state']=='finished'
    assert record['spent_cpu_core_hours']==pytest.approx(4*40/3600)
    journal=read_json(tmp_path/'state/resource-lease-finalizations/200_3.json')
    assert journal['command_exception']['type']=='ValueError' and journal['command_elapsed_seconds']==10
    assert journal['local_finalization']=='recorded' and journal['finish_attempts']==2
    value=journal.pop('self_sha256');assert canonical_hash(journal)==value


@pytest.mark.parametrize('science_raises',[False,True])
def test_exhausted_finish_contention_keeps_reservation_and_durable_evidence_until_native_terminal(tmp_path,monkeypatch,science_raises):
    clock,calls,original,config=finish_contention_fixture(monkeypatch)
    error=ValueError('Independent science failure fixture')
    def command():
        with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='200_3'):
            clock[0]+=10
            if science_raises:raise error
        return 'actual command returned'
    if science_raises:
        with pytest.raises(ValueError) as caught:command()
        assert caught.value is error
    else:
        assert command()=='actual command returned'
    record=read_json(tmp_path/'state/resource-budget.json')
    assert record['leases']['200_3']['state']=='running' and record['spent_cpu_core_hours']==0
    journal=read_json(tmp_path/'state/resource-lease-finalizations/200_3.json')
    assert journal['status']=='pending_native_reconciliation' and journal['finish_attempts']==4
    assert journal['finalization_elapsed_seconds']==120 and journal['command_elapsed_seconds']==10
    assert clock[0]==1130 and (tmp_path/'state/resource-budget.mutex').is_file()
    # The finalization intent cannot release a live/missing native allocation.
    monkeypatch.setattr(budget,'exclusive_file_lock',original)
    running=lambda *a,**k:SimpleNamespace(returncode=0,stdout='200_3|301|RUNNING|130|4|cpu=4|0:0|\n')
    native_accounting(tmp_path,['200'],running)
    assert read_json(tmp_path/'state/resource-budget.json')['leases']['200_3']['state']=='running'
    final=lambda *a,**k:SimpleNamespace(returncode=0,stdout='200_3|301|FAILED|135|4|cpu=4|1:0|\n')
    native_accounting(tmp_path,['200'],final)
    reconciled=read_json(tmp_path/'state/resource-budget.json')
    assert reconciled['leases']['200_3']['state']=='native_reconciled'
    assert reconciled['spent_cpu_core_hours']==pytest.approx(4*135/3600)
    native_accounting(tmp_path,['200'],final)
    assert read_json(tmp_path/'state/resource-budget.json')==reconciled


def test_finish_wait_cannot_extend_native_allocation_deadline(tmp_path,monkeypatch):
    clock,calls,_,config=finish_contention_fixture(monkeypatch,native_remaining=45)
    with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='200_3') as lease:
        assert lease['deadline_unix']==1045
        clock[0]+=10
    assert calls==[30,30.,5.] and clock[0]==1045
    journal=read_json(tmp_path/'state/resource-lease-finalizations/200_3.json')
    assert journal['status']=='pending_native_reconciliation' and journal['finalization_elapsed_seconds']==35


def test_reservation_contention_fails_before_command_and_never_creates_finish_intent(tmp_path,monkeypatch):
    @contextmanager
    def blocked(path,timeout=30):
        raise ReceiptLockTimeout(path,timeout,None)
        yield
    monkeypatch.setattr(budget,'exclusive_file_lock',blocked)
    config={'resources':{'cpu_ceiling':8,'cpu_core_hours_cap':100,'gpu_hours_cap':10}}
    entered=False
    with pytest.raises(ReceiptLockTimeout):
        with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='200_3'):
            entered=True
    assert not entered and not (tmp_path/'state/resource-lease-finalizations/200_3.json').exists()


def test_unexpected_finish_write_error_cannot_replace_primary_scientific_exception(tmp_path,monkeypatch):
    original=budget.atomic_json;writes=[0]
    def writer(path,payload):
        if path.name=='resource-budget.json':
            writes[0]+=1
            if writes[0]>1:raise OSError('Controlled final receipt I/O failure')
        original(path,payload)
    monkeypatch.setattr(budget,'atomic_json',writer)
    config={'resources':{'cpu_ceiling':8,'cpu_core_hours_cap':100,'gpu_hours_cap':10}}
    error=ValueError('Real original scientific exception fixture')
    with pytest.raises(ValueError) as caught:
        with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='200_3'):
            raise error
    assert caught.value is error
    journal=read_json(tmp_path/'state/resource-lease-finalizations/200_3.json')
    assert journal['command_exception']['type']=='ValueError' and journal['finalization_error']['type']=='OSError'
    assert read_json(tmp_path/'state/resource-budget.json')['leases']['200_3']['state']=='running'

