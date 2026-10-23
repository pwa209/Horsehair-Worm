"""Local static payload and real native-parent contract checks; no submissions."""
from pathlib import Path
from datetime import datetime,timezone
from types import SimpleNamespace
import ast
import base64
import hashlib
import importlib.util
import json
import math
import os
import time
import zlib
from contextlib import contextmanager

import pytest
from horsehair.core import canonical_hash,digest,read_json

W=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('cancelled_build',W/'build-rolling-dependents-430-434.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)

def payload(ident):
    source=(W/('hhw-recovery-stage-20261021-'+ident+'.sh')).read_text()
    line=next(x for x in source.splitlines() if x.startswith('payload='))
    blob=next(n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100)
    return source,json.loads(zlib.decompress(base64.b64decode(blob)))

@pytest.mark.parametrize('ident',['430','431','432','433','434'])
def test_requests_are_fresh_lf_valid_python_and_exact_predecessor_set(ident):
    source,p=payload(ident)
    assert '\r' not in source and p['id']==ident and p['area'].endswith('resubmission-'+ident+'-20261023')
    assert p['old_jobs']==b.OLD_JOBS and p['after']==[] and p['after_specs']==[]
    body=source.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
    compile(body,'<request>','exec')
    assert body.index('observed=repair._repair_native_states')<body.index('admission=combined_admission')<body.index('submitted=submit')
    assert body.index("with api['registration_lock']")<body.index('observed=repair._repair_native_states')
    assert "value['state']=='CANCELLED'" in body and "value['elapsed_seconds']==0" in body and "value['cpus']==0" in body
    assert "value['start'] in {'None','Unknown'}" in body and "value['alloc_tres']==''" in body

@pytest.mark.parametrize('ident',['430','431','432'])
def test_canonical_frozen_helpers_are_never_written(ident):
    _,p=payload(ident)
    assert 'derived_helper_body' not in p
    assert p['canonical_sources'][p['canonical_helper']]==p['helper_sha256']
    if ident in {'431','432'}:assert '/study-af-registry-repair-support-v6-20261022/' in p['canonical_helper']

def test_prefix_changes_four_operational_arguments_and_preserves_entire_embedded_bundle():
    _,old,original=b.unpack('399');_,p=payload('433')
    derived=zlib.decompress(base64.b64decode(p['derived_helper_body']))
    a,c=ast.parse(original),ast.parse(derived)
    fa=next(n for n in a.body if isinstance(n,ast.For));fc=next(n for n in c.body if isinstance(n,ast.For))
    assert ast.dump(fa,include_attributes=False)==ast.dump(fc,include_attributes=False)
    before=[n.value for n in ast.walk(a) if isinstance(n,ast.Constant) and isinstance(n.value,str)]
    after=[n.value for n in ast.walk(c) if isinstance(n,ast.Constant) and isinstance(n.value,str)]
    changes=[(a,c) for a,c in zip(before,after) if a!=c]
    assert len(changes)==4 and all('376' not in new and 'stage-v5-r1-20261022/' not in new for _,new in changes)
    assert any(new.endswith('/recovery-433.sbatch') for _,new in changes)
    assert hashlib.sha256(derived).hexdigest()==p['helper_sha256']

def test_archive_derivative_changes_only_support_and_preserves_all_functions():
    _,_,original=b.unpack('406');_,p=payload('434')
    derived=zlib.decompress(base64.b64decode(p['derived_helper_body']))
    a,c=ast.parse(original),ast.parse(derived)
    changes=[]
    for old,new in zip(a.body,c.body):
        if ast.dump(old,include_attributes=False)!=ast.dump(new,include_attributes=False):changes.append((old,new))
    assert len(changes)==1
    assert isinstance(changes[0][0],ast.Assign) and changes[0][0].targets[0].id=='SUPPORT'
    assert hashlib.sha256(derived).hexdigest()==p['helper_sha256']
    assert all('/study-af-resubmission-434-20261023/' in x for x in p['args'] if x.endswith('.py') and 'base-controller' not in x and 'registry-v6' not in x)

def test_census429_exact_source_bound_registration_contract():
    _,p=payload('430')
    assert p['census_args']['--source-manifest-sha256']=='43660c97e50c5a24d42abb321514405107b9f1cade649a492f1c91355486ecb8'
    assert p['census_helper_sha256']=='61001913e746537da3583c2844c760f6589b48be763a44f83be446d5d204a1da'
    assert p['census_args']['--native-task']=='rolling-storage-census-01'
    assert p['administration_guard'].endswith('/administration-registry-read-guard-v4-20261022.py')

@pytest.mark.parametrize('ident',['430','431','432','433','434'])
def test_only_own_future_mutations_must_be_absent_so_completed_new_q_is_allowed(ident):
    _,p=payload(ident)
    assert p['absent_outputs']==b.ABSENT[ident]
    assert len(p['absent_outputs'])=={'430':2,'431':2,'432':4,'433':2,'434':1}[ident]
    if ident in {'432','434'}:
        assert not any(path.endswith('/private-qualification.json') or path.endswith('/native-qualification') for path in p['absent_outputs'])

def test_derived_archive_uses_unchanged_canonical_guard_not_a_missing_stage_alias():
    _,p=payload('434');tree=ast.parse(zlib.decompress(base64.b64decode(p['derived_helper_body'])))
    guard=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='REGISTRY_GUARD' for t in n.targets))
    assert ast.unparse(guard.value)=="BASE_PATH.with_name('registered-snapshot-read-guard-v5-20261022.py')"

@pytest.mark.parametrize('ident',['430','431','432','433','434'])
def test_future_native_ids_are_resolved_only_from_actual_ledgers(ident):
    source,p=payload(ident)
    assert 'dependencies=[census_job]' in source and "registered('429'" in source
    assert "registered('431'" in source and "q_job if x=='__Q_JOB__'" in source
    if ident=='432':assert '__Q_JOB__' in p['args']
    assert "payload['id'] in {'432','434'}" in source

@pytest.fixture
def prefix_native(tmp_path,monkeypatch):
    _,_,raw=b.unpack('399');tree=ast.parse(raw);bundle=ast.literal_eval(next(n for n in tree.body if isinstance(n,ast.For)).iter.func.value)
    context=zlib.decompress(base64.b64decode(bundle['run-full-cohort-prefix-context-v5-20261022.py']['data'])).decode()
    nodes=ast.parse(context).body
    selected=[n for n in nodes if isinstance(n,ast.FunctionDef) and n.name in {'require','memory','native_allocation'}]
    source=next(ast.literal_eval(n.value) for n in nodes if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='SOURCE' for t in n.targets))
    config=next(ast.literal_eval(n.value) for n in nodes if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='CONFIG' for t in n.targets))
    root=tmp_path/'study';directory=root/'plans/recovery-support-20261021-433';directory.mkdir(parents=True);(root/'state').mkdir()
    script=directory/'recovery-433.sbatch';script.write_text('#!/bin/bash\npython -u /fresh/parent.py\n')
    task={'name':'recovery-433','script':str(script),'script_sha256':canonical_hash(script.read_text()),'resources':{'cpus':1,'memory':'64G','hours':8,'gpu':False,'array':None}}
    plan={'config_sha256':config,'release_identity':{'sha256':source},'tasks':[task]}
    (directory/'plan.json').write_text(json.dumps(plan));(root/'state'/('recovery-support-20261021-433-submission.json')).write_text(json.dumps({'source_sha256':source,'plan_sha256':canonical_hash(plan),'tasks':{'recovery-433':{'job_id':'123','state':'submitted','script_sha256':task['script_sha256']}}}))
    end=time.time()+18000;monkeypatch.setenv('SLURM_JOB_END_TIME',str(int(end)))
    fields={'JobId':'123','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING','NumCPUs':'1','NumNodes':'1','TimeLimit':'08:00:00','MinMemoryNode':'64G','Command':str(script),'JobName':'hhw-recovery-433','EndTime':datetime.fromtimestamp(int(end),timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}
    # Native paths contain no spaces. Windows fixture paths do; supply the same
    # already parsed scheduler fields while exercising the unchanged binding.
    regex=SimpleNamespace(fullmatch=__import__('re').fullmatch,findall=lambda *a:list(fields.items()))
    namespace={'Path':Path,'math':math,'re':regex,'time':time,'datetime':datetime,'timezone':timezone,'os':os,'ROOT':root,'SUPPORT':root,'CONFIG':config,'SOURCE':source,'read_json':read_json,'canonical_hash':canonical_hash,'digest':digest}
    exec(compile(ast.Module(body=selected,type_ignores=[]),'<unchanged-context-native-allocation>','exec'),namespace)
    def reader(job,evidence):return SimpleNamespace(stdout=' '.join(k+'='+v for k,v in fields.items()),check_returncode=lambda:None)
    return namespace,script,fields,reader

def test_actual_unchanged_native_parent_contract_accepts_fresh433_plan(prefix_native):
    n,script,fields,reader=prefix_native
    value=n['native_allocation']('123',script,reader)
    assert value['plan_path']==str(script.parent/'plan.json')

@pytest.mark.parametrize('change',['stale_script','wrong_job','wrong_resources','wrong_script_hash'])
def test_actual_unchanged_native_parent_refuses_invalid_registration(prefix_native,change):
    n,script,fields,reader=prefix_native
    if change=='stale_script':fields['Command']=str(script.parent/'recovery-376.sbatch')
    elif change=='wrong_job':fields['JobId']='124'
    else:
        plan=read_json(script.parent/'plan.json')
        if change=='wrong_resources':plan['tasks'][0]['resources']['cpus']=2
        else:plan['tasks'][0]['script_sha256']='0'*64
        (script.parent/'plan.json').write_text(json.dumps(plan))
    with pytest.raises(ValueError):n['native_allocation']('123',script,reader)

def test_activation_bootstrap_precedes_exact_frozen_helper_command_in_native_script():
    from horsehair.slurm import _script
    import shlex
    _,p=payload('432');qjob='actual431job'
    args=[qjob if x=='__Q_JOB__' else x for x in p['args']]
    command=shlex.join(['python','-u',p['canonical_helper'],*args])
    config={'paths':{'root':'/scratch/pwa209/horsehair-worm','cpu_account':'def-ptewarie_cpu','environment':'/home/pwa209/hhw-environment'}}
    namespace={'_script':_script,'config':config,'framework':Path(p['framework']),'name':'recovery-432','command':command,'payload':p,'shlex':shlex}
    tree=ast.parse(b.REGISTRATION);transaction=next(n for n in tree.body if isinstance(n,ast.With))
    start=next(i for i,n in enumerate(transaction.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='text' for t in n.targets) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='_script')
    exec(compile(ast.Module(body=transaction.body[start:start+3],type_ignores=[]),'<native-envelope>','exec'),namespace)
    script=namespace['text'];lines=script.splitlines();actual=next(i for i,line in enumerate(lines) if line==command)
    bootstrap_command=shlex.join(['python','-u','-c',p['native_accounting_bootstrap']])
    assert actual>0 and bootstrap_command+'\n'+command+'\n' in script
    assert shlex.split(bootstrap_command)==['python','-u','-c',p['native_accounting_bootstrap']]
    assert shlex.split(lines[actual])==['python','-u',p['canonical_helper'],*args]
    assert p['helper_sha256']=='586bbf301ef69621cc1132b8b25a9c243f29a2e918b54d564e84be816af2d2eb'

def test_actual_accounting_bootstrap_installs_guards_then_refreshes_under_mutex(monkeypatch):
    from horsehair import core,campaign,repair
    import runpy
    import time
    calls=[];end=time.time()+10000;monkeypatch.setenv('SLURM_JOB_END_TIME',str(end))
    pins={'native-accounting-read-guard.py':'358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91','administration-registry-read-guard-v4-20261022.py':'5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a','policy.json':'f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'}
    monkeypatch.setattr(core,'digest',lambda path:pins[Path(path).name])
    monkeypatch.setattr(core,'read_json',lambda path:{'release_id':'science'} if Path(path).name=='release.json' else {'frozen':'config'})
    monkeypatch.setattr(core,'canonical_hash',lambda value:'446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35')
    monkeypatch.setattr(core,'source_identity',lambda path:{'sha256':'4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1' if '4f3903' in str(path) else '0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'})
    @contextmanager
    def lock(path,deadline,*,role):calls.append('lock');assert deadline==end and role=='controller-start';yield;calls.append('unlock')
    def load(path,**kwargs):
        if Path(path).name=='native-accounting-read-guard.py':return {'install':lambda *a,**k:calls.append('358')}
        return {'install':lambda *a,**k:calls.append('900'),'registration_lock':lock}
    monkeypatch.setattr(runpy,'run_path',load)
    reader=lambda *a,**k:{};monkeypatch.setattr(repair,'_repair_native_states',reader);monkeypatch.setattr(campaign,'_repair_native_states',reader)
    def admission(*a,**k):assert calls[-1]=='lock' and k['cpus']==0 and k['hours']==4;calls.append('actual-accounting-admission')
    monkeypatch.setattr(campaign,'combined_admission',admission)
    exec(compile(b.BOOTSTRAP,'<reviewed-native-bootstrap>','exec'),{})
    assert calls==['358','900','lock','actual-accounting-admission','unlock']

def test_all64_provider_source_contract_and_actual_q2_bindings_are_required():
    source,p=payload('430')
    assert "task_name='rolling-storage-census-01'" in source and "provider['MAX_SLOTS']==64" in source
    assert "provider['provider_registration']" in source and "['slot_count']==64" in source
    assert p['census_args']['--case-id']=='rolling-01'
    assert 'study-af-rolling-census-qualification-v4-20261023' in source
