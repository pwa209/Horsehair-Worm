"""Private read-only registry publication overlay for the exact qualified4f caller.

Only a fresh full original read can return. No record is omitted or merged and
no scheduler mutation is retried. Workers additionally serialize only admission
with the existing F2 registration mutex; controller admission is already locked.
"""
from pathlib import Path
import hashlib
import json
import math
import os
import re
import runpy
import time
import uuid

from horsehair.core import canonical_hash,digest,exclusive_file_lock,source_identity

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
MAX_SECONDS=180
MARGIN=120
MAX_READS=20000
MAX_METADATA_BYTES=8_000_000


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
        remaining();reads=[];initial_plans=set();initial_ledgers={};phase=['original']
        def captured(path,*args,**kwargs):
            remaining();path=Path(path);value=original_read(path,*args,**kwargs)
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
        namespace['read_json']=captured
        prefix=evidence/('attempt-'+str(attempt))
        exclusive(str(prefix)+'-intent.json',{'attempt':attempt,'operation':'fresh complete unchanged all_registered_work',
            'scheduler_mutation_retried':False,'records_merged_or_selected':False})
        try:
            try:
                result=original(root,config,rid)
            except ValueError as error:
                if type(error) is not ValueError or not str(error).startswith(PREFIX):raise
                ledger=ordinary(Path(str(error)[len(PREFIX):]),root)
                require(ledger.parent==root/'state' and ledger.name.endswith('-submission.json')
                    and ledger in initial_ledgers,'Missing-plan error lacks actual captured source-bound ledger')
                header=initial_ledgers[ledger]['header'];phase[0]='publication-classification'
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
            phase[0]='post-read-roster-check'
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
            exclusive(str(prefix)+'-result.json',{'status':'complete_original_strict_snapshot',
                'actual_reads':reads,'unique_allocation_count':len(result[2]),'records_merged_or_selected':False})
            return result
        except BaseException as error:
            retry=type(error) is PublicationReadRace and attempt<MAX_ATTEMPTS
            exclusive(str(prefix)+'-result.json',{'status':'fresh_read_retry' if retry else 'strict_read_failed',
                'error_type':type(error).__name__,'error':str(error),'actual_reads':reads,
                'records_merged_or_selected':False,'retry_read_only':retry})
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
        'driver_sha256':DRIVER_SHA,'max_complete_reads':MAX_ATTEMPTS,'maximum_read_seconds':MAX_SECONDS}
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
            with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=60):
                return original(*args,**kwargs)
        campaign.combined_admission=admitted
    setattr(campaign,MARKER,identity)
    exclusive(evidence/'installation.json',identity)
    return identity
