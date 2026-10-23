import pytest
from horsehair.budget import resource_lease
from horsehair.budget import native_accounting
from types import SimpleNamespace
from horsehair.core import atomic_json
from horsehair.core import read_json
from horsehair.core import ReceiptLockTimeout,canonical_hash
from horsehair import budget
from contextlib import contextmanager
import errno


def quota_text(*,used='1024',soft='20971520',hard='20971520',used_files='999440',soft_files='1000000',hard_files='1000000'):
    return ('Disk quotas for usr pwa209 (uid 3164768):\n'
            'Filesystem kbytes quota limit grace files quota limit grace\n'
            f'/scratch/pwa209 {used} {soft} {hard} - {used_files} {soft_files} {hard_files} -\n')


def test_personal_quota_uses_native_kib_and_both_limits_without_human_unit_guessing():
    row=budget.parse_personal_quota(quota_text(used='1024*',soft='2048',hard='4096',used_files='900*',soft_files='1000',hard_files='2000'))
    assert row['personal_used_bytes']==1024**2
    assert row['personal_hard_limit_bytes']==4096*1024
    assert row['personal_effective_limit_bytes']==2048*1024
    assert row['personal_available_bytes']==1024**2
    assert row['personal_available_files']==100
    assert row['personal_hard_limit_files']==2000


@pytest.mark.parametrize('soft,hard,expected', [('0','1000',1000),('1000','0',1000),('2000','1000',1000),('0','0',None)])
def test_personal_quota_zero_limit_is_unlimited_and_positive_hard_limit_remains_enforced(soft,hard,expected):
    row=budget.parse_personal_quota(quota_text(used_files='100',soft_files=soft,hard_files=hard))
    assert row['personal_effective_limit_files']==expected
    assert row['personal_available_files']==(expected-100 if expected is not None else None)


@pytest.mark.parametrize('text',[
    '/scratch/pwa209 1G 20T 20T - 1 1000000 1000000 -\n',
    '/scratch/pwa209 1024 2048 2048 - -1 1000000 1000000 -\n',
    '/scratch/pwa209 1024 2048 2048 - 1.5 1000000 1000000 -\n',
    '/scratch/shared 1024 2048 2048 - 1 1000000 1000000 -\n',
    quota_text()+quota_text(),
])
def test_personal_quota_rejects_wrong_namespace_units_and_ambiguous_rows(text):
    with pytest.raises(ValueError,match='personal|Personal'):
        budget.parse_personal_quota(text)


def test_near_full_personal_inodes_block_forecast_with_ample_byte_capacity():
    row=budget.parse_personal_quota(quota_text())
    result=budget.storage_readiness(row,required_bytes=1024,required_files=500,reserve_files=100)
    assert row['personal_available_bytes']>20_000_000_000
    assert result['status']=='pending' and not result['ready']
    assert result['personal_available_files']==560
    assert result['reasons']==['Personal scratch inode headroom is insufficient: 560 available, 600 required including reserve']
    assert not result['is_capacity_reservation']


def test_headroom_enforces_exact_required_plus_reserve_and_overquota_remains_negative():
    row=budget.parse_personal_quota(quota_text())
    assert budget.storage_readiness(row,required_files=500,reserve_files=60)['ready']
    exhausted=budget.parse_personal_quota(quota_text(used_files='1000001'))
    assert exhausted['personal_available_files']==-1
    assert not budget.storage_readiness(exhausted,required_files=0)['ready']


def test_unlimited_native_limits_do_not_invent_measured_capacity():
    row=budget.parse_personal_quota(quota_text(soft='0',hard='0',soft_files='0',hard_files='0'))
    result=budget.storage_readiness(row,required_bytes=1,required_files=1)
    assert not result['ready'] and len(result['reasons'])==2
    assert result['personal_available_bytes'] is None and result['personal_available_files'] is None


@pytest.mark.parametrize('kwargs',[{'required_files':True},{'required_files':0.5},{'reserve_files':-1},{'required_bytes':float('nan')}])
def test_storage_demands_require_explicit_nonnegative_integer_counts(kwargs):
    with pytest.raises(ValueError,match='nonnegative integer'):
        budget.storage_readiness(budget.parse_personal_quota(quota_text()),**kwargs)


def personal_guard_fixture(tmp_path,monkeypatch,*,native_text=None):
    monkeypatch.setattr(budget,'PERSONAL_STUDY_ROOT',tmp_path)
    monkeypatch.setattr(budget.os,'getuid',lambda:3164768,raising=False)
    calls=[]
    def runner(command,**kwargs):
        calls.append((command,kwargs))
        return SimpleNamespace(returncode=0,stdout=native_text or quota_text())
    return calls,runner


def test_fresh_personal_headroom_guard_persists_refusal_and_does_not_accumulate_receipt_files(tmp_path,monkeypatch):
    calls,runner=personal_guard_fixture(tmp_path,monkeypatch)
    for _ in range(2):
        with pytest.raises(budget.StorageHeadroomError,match='inode headroom') as caught:
            budget.require_storage_headroom(tmp_path,required_files=500,reserve_files=100,runner=runner)
        record=read_json(tmp_path/'state/storage-headroom.json')
        assert caught.value.record==record and record['status']=='pending'
        assert record['required_files']==505 and record['planned_required_files']==500 and record['receipt_file_overhead']==5
        assert record['native_stdout']==quota_text() and record['personal_uid']==3164768
        receipt_hash=record.pop('self_sha256');assert canonical_hash(record)==receipt_hash
    assert {p.name for p in (tmp_path/'state').iterdir()}=={'storage-headroom.json','storage-headroom.mutex','storage-headroom.mutex.owner.json'}
    assert len(calls)==2
    assert calls[0]==(['lfs','quota','-u','3164768','/scratch/pwa209'],{'text':True,'capture_output':True,'timeout':30})


def test_fresh_guard_accounts_for_atomic_receipt_inodes_and_refreshes_each_check(tmp_path,monkeypatch):
    calls,runner=personal_guard_fixture(tmp_path,monkeypatch)
    record=budget.require_storage_headroom(tmp_path,required_files=555,runner=runner)
    assert record['ready'] and record['required_files']==560
    with pytest.raises(budget.StorageHeadroomError,match='561 required'):
        budget.require_storage_headroom(tmp_path,required_files=556,runner=runner)
    assert len(calls)==2


def test_headroom_never_queries_or_writes_outside_personal_study_root(tmp_path,monkeypatch):
    calls,runner=personal_guard_fixture(tmp_path,monkeypatch)
    with pytest.raises(ValueError,match='authorized personal'):
        budget.require_storage_headroom(tmp_path/'another-project',required_files=100,runner=runner)
    assert not calls and not (tmp_path/'another-project').exists()


def test_concurrent_controller_and_rna_headroom_checks_preserve_both_callers_and_one_receipt(tmp_path,monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    import time
    _,runner=personal_guard_fixture(tmp_path,monkeypatch)
    original=budget.atomic_json;counter=threading.Lock();active=[0];peak=[0]
    def writer(path,record):
        with counter:active[0]+=1;peak[0]=max(peak[0],active[0])
        try:
            time.sleep(.01);original(path,record)
        finally:
            with counter:active[0]-=1
    monkeypatch.setattr(budget,'atomic_json',writer)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda n:budget.require_storage_headroom(tmp_path,required_files=n,runner=runner),(10,20)))
    assert peak[0]==1 and all(item['ready'] for item in results)
    assert {item['planned_required_files'] for item in results}=={10,20}
    assert read_json(tmp_path/'state/storage-headroom.json') in results
    assert len(list((tmp_path/'state').iterdir()))==3


@pytest.mark.parametrize('error_number',[errno.EDQUOT,errno.ENOSPC])
def test_quota_receipt_write_failure_is_never_retried_or_reported_ready(tmp_path,monkeypatch,capsys,error_number):
    calls,runner=personal_guard_fixture(tmp_path,monkeypatch)
    writes=[]
    original=OSError(error_number,'Native quota or free-inode limit reached')
    def fail(path,record):writes.append(path);raise original
    monkeypatch.setattr(budget,'atomic_json',fail)
    with pytest.raises(OSError) as caught:
        budget.require_storage_headroom(tmp_path,required_files=1000,runner=runner)
    assert caught.value is original and len(calls)==len(writes)==1
    message=__import__('json').loads(capsys.readouterr().err)
    assert message['retryable'] is False and message['errno']==error_number
    assert message['measured_storage']['status']=='pending'


def test_storage_snapshot_keeps_native_evidence_and_uses_conservative_inode_limit(tmp_path,monkeypatch):
    calls,runner=personal_guard_fixture(tmp_path,monkeypatch,native_text=quota_text(soft_files='1000000',hard_files='2000000'))
    (tmp_path/'state').mkdir()
    (tmp_path/'data.txt').write_text('measured study file')
    record=budget.storage_snapshot(tmp_path,runner=runner)
    assert record['status']=='measured' and record['personal_available_files']==560
    assert record['personal_hard_limit_files']==2000000
    assert len(calls)==1 and record['personal_uid']==3164768
    assert read_json(tmp_path/'state/storage-budget.json')==record
    source=__import__('pathlib').Path(record['native_source_paths'][0])
    assert source.read_text()==quota_text(soft_files='1000000',hard_files='2000000')


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
    assert calls==[0.,30.,5.] and clock[0]==1045
    journal=read_json(tmp_path/'state/resource-lease-finalizations/200_3.json')
    assert journal['status']=='pending_native_reconciliation' and journal['finalization_elapsed_seconds']==35


def test_reservation_contention_fails_before_command_and_never_creates_finish_intent(tmp_path,monkeypatch):
    attempts=[]
    @contextmanager
    def blocked(path,timeout=30):
        attempts.append(timeout)
        raise ReceiptLockTimeout(path,timeout,None)
        yield
    monkeypatch.setattr(budget,'exclusive_file_lock',blocked)
    config={'resources':{'cpu_ceiling':8,'cpu_core_hours_cap':100,'gpu_hours_cap':10}}
    entered=False
    with pytest.raises(ReceiptLockTimeout):
        with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='200_3'):
            entered=True
    assert not entered and not (tmp_path/'state/resource-lease-finalizations/200_3.json').exists()
    assert attempts==[30.]*10


def reservation_contention_fixture(monkeypatch,*,failures,native_remaining=None):
    clock=[1000.];calls=[];original=budget.exclusive_file_lock
    monkeypatch.setattr(budget.time,'time',lambda:clock[0])
    monkeypatch.setattr(budget.time,'monotonic',lambda:clock[0])
    monkeypatch.delenv('SLURM_JOB_END_TIME',raising=False)
    if native_remaining is not None:monkeypatch.setenv('SLURM_JOB_END_TIME',str(clock[0]+native_remaining))
    @contextmanager
    def guard(path,timeout=30):
        calls.append(timeout)
        if len(calls)<=failures:
            clock[0]+=timeout
            raise ReceiptLockTimeout(path,timeout,{'pid':42,'state':'released','authority':'informational only'})
        with original(path,timeout=timeout):yield
    monkeypatch.setattr(budget,'exclusive_file_lock',guard)
    config={'resources':{'cpu_ceiling':8,'cpu_core_hours_cap':100,'gpu_hours_cap':10}}
    return clock,calls,config


def test_reservation_recovers_after_simultaneous_startup_wait_and_charges_wait_walltime(tmp_path,monkeypatch):
    clock,calls,config=reservation_contention_fixture(monkeypatch,failures=2)
    with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='201') as lease:
        assert clock[0]==1060 and lease['reservation_attempts']==3 and lease['reservation_wait_seconds']==60
        record=read_json(tmp_path/'state/resource-budget.json')
        assert record['leases']['201']['state']=='running' and record['leases']['201']['reservation_attempts']==3
        assert record['leases']['201']['reservation_acquisition_wait_seconds']==60
        clock[0]+=10
    final=read_json(tmp_path/'state/resource-budget.json')
    assert calls==[30.]*4 and final['leases']['201']['state']=='finished'
    assert final['spent_cpu_core_hours']==pytest.approx(4*70/3600)
    assert read_json(tmp_path/'state/resource-lease-finalizations/201.json')['reservation_wait_seconds']==60


@pytest.mark.parametrize('native_remaining,expected',[(None,[30.]*10),(165,[30.,15.]),(110,[0.])])
def test_reservation_wait_is_bounded_before_command_and_preserves_completion_margin(tmp_path,monkeypatch,native_remaining,expected):
    clock,calls,config=reservation_contention_fixture(monkeypatch,failures=100,native_remaining=native_remaining)
    entered=False
    with pytest.raises(ReceiptLockTimeout):
        with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='201'):entered=True
    assert not entered and calls==expected
    assert clock[0]==1000+sum(expected)
    assert not (tmp_path/'state/resource-budget.json').exists()
    assert not (tmp_path/'state/resource-lease-finalizations/201.json').exists()


def test_cap_refusal_after_recovered_guard_is_not_retried_or_bypassed(tmp_path,monkeypatch):
    clock,calls,config=reservation_contention_fixture(monkeypatch,failures=1)
    path=tmp_path/'state/resource-budget.json'
    record={'spent_cpu_core_hours':0.,'spent_gpu_hours':0.,'leases':{'200':{'state':'running','cpus':8,'gpus':0,'max_hours':1}}}
    atomic_json(path,record);entered=False
    with pytest.raises(ValueError,match='concurrency ceiling'):
        with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='201'):entered=True
    assert not entered and calls==[30.,30.] and read_json(path)==record


def test_reservation_io_failure_is_not_treated_as_safe_lock_contention(tmp_path,monkeypatch):
    calls=[]
    @contextmanager
    def failure(path,timeout=30):
        calls.append(timeout);raise OSError('Unknown receipt I/O state');yield
    monkeypatch.setattr(budget,'exclusive_file_lock',failure)
    config={'resources':{'cpu_ceiling':8,'cpu_core_hours_cap':100,'gpu_hours_cap':10}}
    with pytest.raises(OSError,match='Unknown receipt'):
        with resource_lease(tmp_path,config,cpus=4,gpus=0,max_hours=1,job_id='201'):pytest.fail('No reservation was committed')
    assert calls==[30.]


def test_identical_native_accounting_does_not_rewrite_durable_ledger_or_tsv(tmp_path,monkeypatch):
    from pathlib import Path
    writes=[];replacements=[];original_json=budget.atomic_json;original_replace=Path.replace
    def writer(path,record):writes.append(path);return original_json(path,record)
    def replace(path,target):replacements.append((path,target));return original_replace(path,target)
    monkeypatch.setattr(budget,'atomic_json',writer);monkeypatch.setattr(Path,'replace',replace)
    runner=lambda *a,**k:SimpleNamespace(returncode=0,stdout='201|201|COMPLETED|90|4|cpu=4|0:0|2G\n')
    first=native_accounting(tmp_path,['201'],runner)
    assert len(writes)==len(replacements)==1
    second=native_accounting(tmp_path,['201'],runner)
    assert first==second and len(writes)==len(replacements)==1


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

