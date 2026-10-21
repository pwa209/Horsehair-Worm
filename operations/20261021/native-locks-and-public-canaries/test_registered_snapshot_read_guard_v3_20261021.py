"""Real pinned registry reader with synthetic publication races; no scheduler."""
from pathlib import Path
import copy
import importlib.util
import json
import runpy
import time
from contextlib import contextmanager

import pytest

from horsehair.core import atomic_json,canonical_hash,read_json

spec=importlib.util.spec_from_file_location('registry_guard_v3',Path(__file__).with_name('registered-snapshot-read-guard-v3-20261021.py'))
g=importlib.util.module_from_spec(spec);spec.loader.exec_module(g)
_NATIVE_DRIVER=Path('/scratch/pwa209/horsehair-worm/maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py')
DRIVER=_NATIVE_DRIVER if _NATIVE_DRIVER.exists() else Path(__file__).with_name('controller-primary-repair-sixth-20261016.py')


@pytest.fixture
def registry(tmp_path,monkeypatch):
    root=tmp_path/'study';(root/'plans').mkdir(parents=True);(root/'state').mkdir()
    evidence=root/'maintenance/evidence';evidence.mkdir(parents=True)
    config={'paths':{'root':str(root)},'synthetic_fixture':True}
    monkeypatch.setattr(g,'ROOT',root);monkeypatch.setattr(g,'CONFIG',canonical_hash(config))
    module=runpy.run_path(str(DRIVER),run_name='pinned_registry_fixture_only')
    original=module['all_registered_work'];namespace=original.__globals__;reader=namespace['read_json']
    def publish(index):
        plan={'root':str(root),'config_sha256':canonical_hash(config),'release_identity':{'sha256':'a'*64},
            'tasks':[{'name':'task-'+str(index),'resources':{'cpus':6,'gpu':True,'hours':12,'array':None},'depends':[]}]}
        path=root/'plans'/('phase-'+str(index))/'plan.json';atomic_json(path,plan)
        ledger={'config_sha256':canonical_hash(config),'plan_sha256':canonical_hash(plan),'source_sha256':'a'*64,
            'tasks':{'task-'+str(index):{'state':'submitted','job_id':str(100+index)}}}
        target=root/'state'/('phase-'+str(index)+'-submission.json');atomic_json(target,ledger)
        return path,target
    old_plan,old_ledger=publish(0)
    sleeps=[]
    def invoke():
        return g.guarded_reader(original,module['checked_pair'],root=root,config=config,rid='synthetic',
            evidence=evidence,deadline=time.time()+3600,sleeper=lambda delay:sleeps.append(delay))
    return root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke


def test_exact_strict_snapshot_unchanged_without_race(registry):
    root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke=registry
    result=invoke()
    assert result[2]=={'100'} and not sleeps and namespace['read_json'] is reader
    record=read_json(evidence/'attempt-1-result.json')
    assert record['status']=='complete_original_strict_snapshot'
    assert record['actual_reads'] and record['records_merged_or_selected'] is False


@pytest.mark.parametrize('when',['after_plan_list','after_ledger_list'])
def test_new_plan_and_ledger_require_fresh_complete_query(registry,when):
    root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke=registry
    created=[]
    def reading(path):
        value=reader(path)
        trigger=old_plan if when=='after_plan_list' else old_ledger
        if Path(path)==trigger and not created:created.append(publish(1))
        return value
    namespace['read_json']=reading
    result=invoke()
    assert result[2]=={'100','101'} and sleeps==[1] and namespace['read_json'] is reading
    assert read_json(evidence/'attempt-1-result.json')['retry_read_only'] is True
    assert read_json(evidence/'attempt-2-result.json')['status']=='complete_original_strict_snapshot'


def test_stable_missing_plan_is_not_retried(registry):
    root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke=registry
    _,path=publish(1);value=read_json(path);value['plan_sha256']='b'*64;atomic_json(path,value)
    with pytest.raises(ValueError,match='no unchanged study plan'):invoke()
    assert not sleeps and len(list(evidence.glob('*-result.json')))==1


def test_wrong_source_or_changed_plan_does_not_become_retryable_publication(registry):
    root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke=registry
    made=[]
    def reading(path):
        value=reader(path)
        if Path(path)==old_plan and not made:
            p,l=publish(1);made.append(p);bad=read_json(l);bad['source_sha256']='foreign';atomic_json(l,bad)
        return value
    namespace['read_json']=reading
    with pytest.raises(ValueError,match='not bound'):invoke()
    assert not sleeps


def test_changed_captured_plan_is_not_misclassified_as_a_new_plan(registry):
    root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke=registry
    changed=[]
    def reading(path):
        value=reader(path)
        if Path(path)==old_plan and not changed:
            changed.append(True);plan=copy.deepcopy(value);plan['changed']=True;atomic_json(path,plan)
            ledger=read_json(old_ledger);ledger['plan_sha256']=canonical_hash(plan);atomic_json(old_ledger,ledger)
        return value
    namespace['read_json']=reading
    with pytest.raises(ValueError,match='no unchanged study plan'):invoke()
    assert not sleeps


def test_persistent_publications_stop_after_three_fresh_reads(registry):
    root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke=registry
    index=[0]
    def reading(path):
        value=reader(path)
        if Path(path)==old_plan:
            index[0]+=1;publish(index[0])
        return value
    namespace['read_json']=reading
    with pytest.raises(g.PublicationReadRace):invoke()
    assert sleeps==[1,2] and len(list(evidence.glob('*-result.json')))==3
    assert namespace['read_json'] is reading


def test_malformed_json_and_arbitrary_exceptions_are_not_retried(registry):
    root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke=registry
    old_plan.write_text('{malformed')
    with pytest.raises(json.JSONDecodeError):invoke()
    assert not sleeps and namespace['read_json'] is reader


def test_native_deadline_refuses_first_read(registry):
    root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke=registry
    with pytest.raises(ValueError,match='deadline'):
        g.guarded_reader(original,original.__globals__['checked_pair'],root=root,config=config,rid='synthetic',
            evidence=evidence,deadline=time.time()+119)
    assert not list(evidence.iterdir()) and not sleeps


def test_runpy_proxy_is_scoped_and_does_not_modify_global_module(registry,monkeypatch):
    root,config,evidence,original,namespace,reader,publish,old_plan,old_ledger,sleeps,invoke=registry
    monkeypatch.setattr(g,'DRIVER',DRIVER)
    unchanged=runpy.run_path
    proxy=g.ScopedRunpy(runpy,evidence,time.time()+3600)
    namespace=proxy.run_path(DRIVER,run_name='af_registered_accounting_only')
    assert namespace['all_registered_work'](root,config,'synthetic')[2]=={'100'}
    assert runpy.run_path is unchanged and g.runpy.run_path is unchanged
    # A different run_name remains a byte-for-byte ordinary namespace load.
    other=proxy.run_path(DRIVER,run_name='different_reader')
    assert other['all_registered_work'].__name__=='all_registered_work'


@pytest.fixture
def install_context(tmp_path,monkeypatch):
    from horsehair import campaign,repair
    root=tmp_path/'study';root.mkdir();framework=root/'release'
    monkeypatch.setattr(g,'ROOT',root);monkeypatch.setattr(g,'FRAMEWORK',framework)
    monkeypatch.setattr(campaign,'__file__',str(framework/'src/horsehair/campaign.py'))
    monkeypatch.setattr(g,'source_identity',lambda path:{'sha256':g.SOURCE})
    def native_reader(*args,**kwargs):return {}
    native_reader.__hhw_native_accounting_read_guard__={'path':str(root/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'),'sha256':g.NATIVE_GUARD_SHA}
    monkeypatch.setattr(campaign,'_repair_native_states',native_reader);monkeypatch.setattr(repair,'_repair_native_states',native_reader)
    monkeypatch.setattr(campaign,'runpy',runpy);monkeypatch.delattr(campaign,g.MARKER,raising=False)
    calls=[];held=[False]
    @contextmanager
    def guard(path,**kwargs):
        assert not held[0];held[0]=True;calls.append(('lock',path,kwargs));yield;held[0]=False;calls.append(('unlock',))
    monkeypatch.setattr(g,'exclusive_file_lock',guard)
    def admission(*args,**kwargs):calls.append(('admission',held[0],args,kwargs));return {'fresh':'result'}
    monkeypatch.setattr(campaign,'combined_admission',admission)
    return root,framework,campaign,repair,calls,held,admission


def test_worker_locks_only_admission_without_global_runpy_mutation(install_context):
    root,framework,campaign,repair,calls,held,original=install_context
    run_path=runpy.run_path;deadline=time.time()+3600
    identity=g.install(root/'maintenance/worker-evidence',registration_mode='worker-lock',allocation_deadline_unix=deadline)
    assert runpy.run_path is run_path and campaign.runpy is not runpy
    assert not held[0] and not calls
    assert campaign.combined_admission(root,{'frozen':True})=={'fresh':'result'}
    assert [row[0] for row in calls]==['lock','admission','unlock'] and calls[1][1] is True and not held[0]
    assert g.install(root/'maintenance/worker-evidence',registration_mode='worker-lock',allocation_deadline_unix=deadline)==identity


def test_controller_declares_existing_lock_and_does_not_acquire_again(install_context):
    root,framework,campaign,repair,calls,held,original=install_context
    g.install(root/'maintenance/controller-evidence',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+3600)
    assert campaign.combined_admission is original
    campaign.combined_admission(root,{})
    assert calls[0][0]=='admission' and len(calls)==1


@pytest.mark.parametrize('change',['marker','reader_binding','source','role','deadline','nested'])
def test_install_fails_closed_on_unqualified_or_conflicting_scope(install_context,monkeypatch,change):
    root,framework,campaign,repair,calls,held,original=install_context
    mode='worker-lock';deadline=time.time()+3600
    if change=='marker':repair._repair_native_states.__hhw_native_accounting_read_guard__['sha256']='b'*64
    elif change=='reader_binding':monkeypatch.setattr(campaign,'_repair_native_states',object())
    elif change=='source':monkeypatch.setattr(g,'source_identity',lambda path:{'sha256':'b'*64})
    elif change=='role':mode='automatic'
    elif change=='deadline':deadline=float('nan')
    else:
        g.install(root/'maintenance/evidence',registration_mode=mode,allocation_deadline_unix=deadline)
    with pytest.raises(ValueError):
        g.install(root/'maintenance/different-evidence',registration_mode=mode,allocation_deadline_unix=deadline)


@pytest.fixture
def waiting(tmp_path,monkeypatch):
    root=tmp_path/'study';root.mkdir();monkeypatch.setattr(g,'ROOT',root)
    clock=[0.];calls=[];held=[False]
    def factory(failures=2,error=None):
        @contextmanager
        def lock(path,timeout):
            calls.append((path,timeout));clock[0]+=timeout if len(calls)<=failures else 0
            if len(calls)<=failures:
                if error is not None:raise error
                raise g.ReceiptLockTimeout(path,timeout,{'pid':123,'authority':'informational only'})
            held[0]=True
            try:yield
            finally:held[0]=False
        return lock
    return root,clock,calls,held,factory


def test_worker_waits_through_two_long_publications_then_runs_operation_once(waiting):
    root,clock,calls,held,factory=waiting;operations=[]
    with g.registration_lock(root/'maintenance/wait',20000.,role='worker-admission',lock_factory=factory(),
        clock=lambda:clock[0],wall=lambda:10000.+clock[0]):
        assert held[0];operations.append('once')
    assert len(calls)==3 and clock[0]==120 and operations==['once'] and not held[0]
    assert len(list((root/'maintenance/wait').glob('*-result.json')))==3


@pytest.mark.parametrize('role,count',[('worker-admission',60),('controller-tick',15)])
def test_wait_exhaustion_is_bounded_pending_without_any_operation_or_guard_removal(waiting,role,count):
    root,clock,calls,held,factory=waiting;guard=root/'state/af-campaign-registration.mutex';guard.parent.mkdir();guard.write_text('permanent')
    with pytest.raises(g.RegistrationWaitBudgetExceeded):
        with g.registration_lock(root/'maintenance/wait',50000.,role=role,lock_factory=factory(999),
            clock=lambda:clock[0],wall=lambda:10000.+clock[0]):raise AssertionError('operation cannot start')
    assert len(calls)==count and guard.read_text()=='permanent' and not held[0]
    pending=read_json(root/'maintenance/wait/pending.json');assert pending['operation_started'] is False


@pytest.mark.parametrize('kind',['foreign_lock','ordinary_timeout','os_error'])
def test_no_retry_of_foreign_guard_or_arbitrary_failures(waiting,kind):
    root,clock,calls,held,factory=waiting
    error=g.ReceiptLockTimeout(root/'state/foreign.mutex',60,None) if kind=='foreign_lock' else TimeoutError('ordinary') if kind=='ordinary_timeout' else OSError('disk')
    with pytest.raises(type(error)):
        with g.registration_lock(root/'maintenance/wait',20000.,role='worker-admission',lock_factory=factory(999,error),
            clock=lambda:clock[0],wall=lambda:10000.+clock[0]):raise AssertionError('cannot enter')
    assert len(calls)==1 and not held[0]


def test_native_margin_stops_before_another_lock_attempt(waiting):
    root,clock,calls,held,factory=waiting
    with pytest.raises(g.RegistrationWaitBudgetExceeded):
        with g.registration_lock(root/'maintenance/wait',10200.,role='worker-admission',lock_factory=factory(),
            clock=lambda:clock[0],wall=lambda:10000.):raise AssertionError('cannot enter')
    assert not calls and not held[0]


def test_operation_failure_unlocks_and_never_retries_the_operation(waiting):
    root,clock,calls,held,factory=waiting;error=ValueError('original admission failure')
    with pytest.raises(ValueError) as observed:
        with g.registration_lock(root/'maintenance/wait',20000.,role='worker-admission',lock_factory=factory(0),
            clock=lambda:clock[0],wall=lambda:10000.):raise error
    assert observed.value is error and len(calls)==1 and not held[0]


def test_evidence_failure_after_acquisition_still_releases_kernel_guard(waiting,monkeypatch):
    root,clock,calls,held,factory=waiting;original=g.exclusive
    def write(path,value):
        if value.get('status')=='kernel_registration_guard_acquired':raise OSError('synthetic disk error')
        original(path,value)
    monkeypatch.setattr(g,'exclusive',write)
    with pytest.raises(OSError):
        with g.registration_lock(root/'maintenance/wait',20000.,role='worker-admission',lock_factory=factory(0),
            clock=lambda:clock[0],wall=lambda:10000.):raise AssertionError('cannot enter')
    assert len(calls)==1 and not held[0]
