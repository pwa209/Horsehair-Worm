"""Private read-only registry publication overlay for the exact qualified4f caller.

Only a fresh full original read can return. No record is omitted or merged and
no scheduler mutation is retried. Workers additionally serialize only admission
with the existing F2 registration mutex; controller admission is already locked.
The time bound is cooperative: blocking filesystem operations can overrun it,
and a late operation fails at the next check rather than returning a snapshot.
"""
from pathlib import Path
from contextlib import contextmanager
import hashlib
import json
import math
import os
import re
import runpy
import time
import uuid

from horsehair.core import canonical_hash,digest,exclusive_file_lock,source_identity,ReceiptLockTimeout

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
DRIVER=ROOT/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'
DRIVER_SHA='49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'
NATIVE_GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
PREFIX='A source/configuration-bound submission ledger has no unchanged study plan: '
MARKER='__hhw_registered_snapshot_guard__'
MAX_ATTEMPTS=3
BACKOFF=(1,2)
MAX_SECONDS=900
MARGIN=120
MAX_READS=20000
MAX_METADATA_BYTES=8_000_000
MAX_LOCK_SECONDS=7200
CONTROLLER_LOCK_SECONDS=900
LOCK_ATTEMPT_SECONDS=60
LOCK_MARGIN=300


def require(value,message):
    if not value:raise ValueError(message)


def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    require(len(raw)<=MAX_METADATA_BYTES,'Registry read evidence exceeds its finite bound')
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def ordinary(path,root,*,exists=True):
    path=Path(path)
    require(path.is_absolute() and path==path.resolve(strict=exists) and path.is_relative_to(root)
        and path!=root and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(root)),
        'Exact ordinary registry evidence namespace required')
    return path


class PublicationReadRace(ValueError):
    pass


class RegistrationWaitBudgetExceeded(TimeoutError):pass


@contextmanager
def registration_lock(evidence,deadline,*,role,lock_factory=None,clock=time.monotonic,wall=time.time):
    """Wait only for this exact permanent kernel guard, before any operation."""
    lock_factory=exclusive_file_lock if lock_factory is None else lock_factory
    require(role in {'worker-admission','controller-start','controller-tick'},'Explicit lock role required')
    require(type(deadline) in (int,float) and math.isfinite(deadline),'Finite native lock deadline required')
    path=(ROOT/'state/af-campaign-registration.mutex').resolve();start=clock();last=None
    evidence=ordinary(evidence,ROOT,exists=False);evidence.mkdir(parents=True,exist_ok=False)
    limit=MAX_LOCK_SECONDS if role=='worker-admission' else CONTROLLER_LOCK_SECONDS
    for number in range(1,121 if role=='worker-admission' else 16):
        allowance=min(LOCK_ATTEMPT_SECONDS,limit-(clock()-start),deadline-wall()-LOCK_MARGIN)
        if allowance<=0:break
        prefix=evidence/('lock-attempt-'+str(number).zfill(2))
        exclusive(str(prefix)+'-intent.json',{'role':role,'guard_path':str(path),'maximum_wait_seconds':allowance,
            'operation_started':False,'guard_removed_or_replaced':False})
        guard=lock_factory(path,timeout=allowance)
        try:guard.__enter__()
        except ReceiptLockTimeout as error:
            if Path(error.path).resolve()!=path:
                exclusive(str(prefix)+'-result.json',{'status':'nonretryable_foreign_lock','diagnostics':error.diagnostics})
                raise
            last=error
            exclusive(str(prefix)+'-result.json',{'status':'waiting_exact_registration_guard','diagnostics':error.diagnostics,
                'operation_started':False,'guard_removed_or_replaced':False})
            continue
        except BaseException as error:
            exclusive(str(prefix)+'-result.json',{'status':'nonretryable_lock_failure','error_type':type(error).__name__,'error':str(error)})
            raise
        try:
            exclusive(str(prefix)+'-result.json',{'status':'kernel_registration_guard_acquired','role':role,
                'waited_seconds':clock()-start,'owner_metadata_is_authoritative':False})
            require(wall()+LOCK_MARGIN<deadline,'Native margin exhausted after registration lock acquisition')
            yield
        finally:guard.__exit__(*__import__('sys').exc_info())
        return
    exclusive(evidence/'pending.json',{'status':'resource_pending_registration','role':role,'waited_seconds':clock()-start,
        'last_diagnostics':getattr(last,'diagnostics',None),'operation_started':False,'guard_removed_or_replaced':False,
        'native_deadline_unix':deadline,'scientific_result':False,'admission_authority':False})
    raise RegistrationWaitBudgetExceeded('Exact registration guard remains busy within finite native wait budget') from last


def guarded_reader(original,checked_pair,*,root,config,rid,evidence,deadline,
                   clock=time.monotonic,wall=time.time,sleeper=time.sleep):
    """Execute only the pinned original reader, with bounded observed-growth retries."""
    require(Path(root)==ROOT and canonical_hash(config)==CONFIG,'Exact personal full-config registry scope required')
    start=clock();end=min(start+MAX_SECONDS,start+deadline-wall()-MARGIN)
    def remaining():
        require(clock()<end and wall()+MARGIN<deadline,'Native registry-read deadline/margin exhausted')
    original_read=original.__globals__['read_json']
    def paths(folder,pattern):
        result=[]
        for path in folder.rglob(pattern) if folder.name=='plans' else folder.glob(pattern):
            remaining();result.append(ordinary(path,root))
            require(len(result)<=MAX_READS,'Finite registry directory roster exceeded')
        return sorted(result)
    for attempt in range(1,MAX_ATTEMPTS+1):
        remaining();reads=[];initial_plans=set();initial_ledgers={}
        attempt_start=clock();phase=['attempt-intent-evidence'];phase_start=[attempt_start]
        phases=('attempt-intent-evidence','original','publication-classification',
                'post-read-roster-check','result-evidence','failure-evidence')
        phase_seconds={name:0. for name in phases};phase_read_calls={name:0 for name in phases}
        read_calls=[0];read_seconds=[0.]
        def change_phase(name):
            now=clock();phase_seconds[phase[0]]+=max(0.,now-phase_start[0])
            phase[0]=name;phase_start[0]=now
        def telemetry():
            now=clock();elapsed=dict(phase_seconds)
            elapsed[phase[0]]+=max(0.,now-phase_start[0])
            return {'elapsed_monotonic_seconds':max(0.,now-start),
                'attempt_elapsed_monotonic_seconds':max(0.,now-attempt_start),
                'read_calls':read_calls[0],'captured_read_count':len(reads),
                'read_elapsed_monotonic_seconds':read_seconds[0],
                'phase_elapsed_monotonic_seconds':elapsed,'phase_read_calls':dict(phase_read_calls),
                'current_phase':phase[0],'includes_current_evidence_write':False,
                'time_bound_is_cooperative':True}
        def captured(path,*args,**kwargs):
            remaining();path=Path(path);read_start=clock();read_calls[0]+=1;phase_read_calls[phase[0]]+=1
            try:
                value=original_read(path,*args,**kwargs)
                remaining()
            finally:read_seconds[0]+=max(0.,clock()-read_start)
            require(len(reads)<MAX_READS,'Finite exact registry read count exceeded')
            fingerprint=canonical_hash(value)
            reads.append({'path':str(path),'canonical_sha256':fingerprint,'phase':phase[0]})
            if phase[0]=='original':
                if path.name=='plan.json' and path.is_relative_to(root/'plans'):initial_plans.add(path)
                elif path.name.endswith('-submission.json') and path.parent==root/'state':
                    initial_ledgers[path]={'sha256':fingerprint,'header':{k:value.get(k) for k in
                        ('config_sha256','source_sha256','plan_sha256')}}
            return value
        namespace=original.__globals__
        require(checked_pair.__globals__ is namespace,'Pinned reader/checker globals must remain shared')
        prefix=evidence/('attempt-'+str(attempt))
        result_write_started=False;result_written=False
        namespace['read_json']=captured
        try:
            exclusive(str(prefix)+'-intent.json',{'attempt':attempt,'operation':'fresh complete unchanged all_registered_work',
                'scheduler_mutation_retried':False,'records_merged_or_selected':False,'telemetry':telemetry()})
            remaining();change_phase('original')
            try:
                result=original(root,config,rid)
            except ValueError as error:
                if type(error) is not ValueError or not str(error).startswith(PREFIX):raise
                ledger=ordinary(Path(str(error)[len(PREFIX):]),root)
                require(ledger.parent==root/'state' and ledger.name.endswith('-submission.json')
                    and ledger in initial_ledgers,'Missing-plan error lacks actual captured source-bound ledger')
                header=initial_ledgers[ledger]['header'];change_phase('publication-classification')
                latest=captured(ledger)
                require(canonical_hash(latest)==initial_ledgers[ledger]['sha256'],
                    'Fail closed: offending ledger changed during publication classification')
                candidates=[]
                for path in paths(root/'plans','plan.json'):
                    if path in initial_plans:continue
                    value=captured(path)
                    if canonical_hash(value)==header['plan_sha256']:
                        checked_pair(path,ledger,root,config);candidates.append(str(path))
                if not candidates:raise error
                raise PublicationReadRace('New unchanged plan became visible after the original plan snapshot: '+str(ledger)) from error
            change_phase('post-read-roster-check')
            # Detect a bound ledger published after the original ledger listing.
            growth=[]
            for ledger in paths(root/'state','*-submission.json'):
                current=captured(ledger)
                if ledger in initial_ledgers:
                    require(canonical_hash(current)==initial_ledgers[ledger]['sha256'],
                        'Fail closed: a captured submission ledger changed during the registry read')
                    continue
                if current.get('config_sha256')==CONFIG and current.get('source_sha256') and current.get('plan_sha256'):
                    candidates=[]
                    for path in paths(root/'plans','plan.json'):
                        value=captured(path)
                        if canonical_hash(value)==current['plan_sha256']:
                            checked_pair(path,ledger,root,config);candidates.append(str(path))
                    require(candidates,'A newly observed source-bound ledger has no unchanged valid plan: '+str(ledger))
                    require(canonical_hash(captured(ledger))==canonical_hash(current),
                        'A new source-bound ledger changed during strict publication validation')
                    growth.append(str(ledger))
            if growth:raise PublicationReadRace('New source-bound submission ledgers appeared after the original ledger snapshot: '+','.join(growth))
            remaining()
            change_phase('result-evidence');result_write_started=True
            exclusive(str(prefix)+'-result.json',{'status':'complete_original_strict_snapshot',
                'actual_reads':reads,'unique_allocation_count':len(result[2]),'records_merged_or_selected':False,
                'post_write_deadline_check_required_before_return':True,'telemetry':telemetry()})
            result_written=True
            remaining()
            return result
        except BaseException as error:
            retry=type(error) is PublicationReadRace and attempt<MAX_ATTEMPTS
            failure_stage='postwrite_deadline_check' if result_written else 'result_evidence_write' if result_write_started else phase[0]
            suffix='-postwrite-failure.json' if result_written else '-evidence-write-failure.json' if result_write_started else '-result.json'
            change_phase('failure-evidence')
            exclusive(str(prefix)+suffix,{'status':'fresh_read_retry' if retry else 'strict_read_failed',
                'error_type':type(error).__name__,'error':str(error),'actual_reads':reads,
                'records_merged_or_selected':False,'retry_read_only':retry,
                'failure_stage':failure_stage,'original_result_returned':False,
                'supersedes_result_evidence':str(prefix)+'-result.json' if result_write_started else None,
                'telemetry':telemetry()})
            if not retry:raise
        finally:
            namespace['read_json']=original_read
        delay=BACKOFF[attempt-1]
        require(clock()+delay<end and wall()+delay+MARGIN<deadline,'No native margin for another complete registry read')
        sleeper(delay)
    raise AssertionError('Finite registry loop exhausted')


class ScopedRunpy:
    """A campaign-module proxy, never a change to the global runpy module."""
    def __init__(self,original,evidence,deadline):self.original,self.evidence,self.deadline=original,evidence,deadline
    def __getattr__(self,name):return getattr(self.original,name)
    def run_path(self,path,*args,**kwargs):
        if Path(path)!=DRIVER or kwargs.get('run_name')!='af_registered_accounting_only':
            return self.original.run_path(path,*args,**kwargs)
        require(digest(DRIVER)==DRIVER_SHA,'Exact pinned registry driver changed')
        namespace=self.original.run_path(path,*args,**kwargs)
        original,checker=namespace['all_registered_work'],namespace['checked_pair']
        def read(root,config,rid):
            call=self.evidence/('read-'+str(time.time_ns())+'-'+uuid.uuid4().hex);call.mkdir(mode=0o700,exist_ok=False)
            return guarded_reader(original,checker,root=root,config=config,rid=rid,evidence=call,deadline=self.deadline)
        result=dict(namespace);result['all_registered_work']=read
        require(digest(DRIVER)==DRIVER_SHA,'Registry driver changed while loading')
        return result


def install(evidence_dir,*,registration_mode,allocation_deadline_unix):
    """Install after358 and before importing/binding campaign admission at call sites.

    Worker mode locks only combined_admission; controller mode declares its
    existing outer registration lock and must not acquire it recursively.
    """
    require(registration_mode in {'worker-lock','controller-already-locked'},'Explicit process registration-lock role required')
    require(not isinstance(allocation_deadline_unix,bool) and isinstance(allocation_deadline_unix,(int,float))
        and math.isfinite(allocation_deadline_unix) and allocation_deadline_unix>time.time()+MARGIN,
        'Finite verified native allocation deadline required')
    from horsehair import campaign,repair
    require(Path(campaign.__file__).resolve()==FRAMEWORK/'src/horsehair/campaign.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE and campaign._repair_native_states is repair._repair_native_states,
        'Exact qualified4f campaign and prior installed native reader binding required')
    native_guard=getattr(repair._repair_native_states,'__hhw_native_accounting_read_guard__',{})
    require(isinstance(native_guard,dict) and native_guard.get('sha256')==NATIVE_GUARD_SHA
        and native_guard.get('path')==str(ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'),
        'Pinned358 native read guard must be installed first')
    evidence=ordinary(evidence_dir,ROOT,exists=False)
    require(evidence.is_relative_to(ROOT/'maintenance'),'Personal immutable maintenance evidence required')
    identity={'helper_sha256':digest(Path(__file__)),'evidence_dir':str(evidence),
        'registration_mode':registration_mode,'allocation_deadline_unix':allocation_deadline_unix,
        'driver_sha256':DRIVER_SHA,'max_complete_reads':MAX_ATTEMPTS,'maximum_read_seconds':MAX_SECONDS,
        'registration_lock_maximum_seconds':MAX_LOCK_SECONDS,'registration_lock_native_margin_seconds':LOCK_MARGIN}
    previous=getattr(campaign,MARKER,None)
    if previous is not None:
        require(previous==identity,'Different/nested registry guard installation refused');return identity
    evidence.mkdir(mode=0o700,parents=True,exist_ok=False)
    require(campaign.runpy is runpy,'Another campaign runpy overlay is already installed')
    campaign.runpy=ScopedRunpy(runpy,evidence,allocation_deadline_unix)
    if registration_mode=='worker-lock':
        original=campaign.combined_admission
        def admitted(*args,**kwargs):
            require(Path(args[0]).resolve()==ROOT,'Foreign worker admission root refused')
            require(time.time()+MARGIN<allocation_deadline_unix,'Native admission margin exhausted')
            with registration_lock(evidence/('admission-lock-'+uuid.uuid4().hex),allocation_deadline_unix,role='worker-admission'):
                return original(*args,**kwargs)
        campaign.combined_admission=admitted
    setattr(campaign,MARKER,identity)
    exclusive(evidence/'installation.json',identity)
    return identity
