"""Read-only emitted-descriptor routing; no real controller allocation or run."""
from pathlib import Path
from contextlib import contextmanager,ExitStack
from datetime import datetime,timezone
from types import SimpleNamespace
from unittest.mock import patch
import builtins
import hashlib
import importlib.util
import io
import json
import os
import shlex
import subprocess

BASE_SHA='cbf01cffbbaa9a983ec1e8e6394b38ac04623483e768398c18f48b5e30e31445'
OUTER_SHA='381fbde43ba0798cb207388a402afcc702b569129c81afa6d8226e2cb5258c63'
OLD_DESCRIPTOR_SHA='9a0706642ac1660fb7f39fe68279ca34bdc10f89812a0c2397608d2a382cf464'
OLD_DESCRIPTOR=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-registry-repair-support-v6-20261022/native-activation-prestart-three-v1/descriptor-prestart-three-v1.json')
SUPPORT=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-backfill-controller-support-v1-20261024')
FIXTURE_JOB='900000001'


def require(value,message):
    if not value:raise ValueError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def read(path):
    raw=Path(path).read_bytes()
    require(len(raw)<=8_000_000,'Bounded emitted metadata required')
    def unique(pairs):
        value={}
        for key,item in pairs:
            require(key not in value,'Duplicate metadata key');value[key]=item
        return value
    return json.loads(raw,object_pairs_hook=unique,parse_constant=lambda value:(_ for _ in ()).throw(ValueError('Nonfinite metadata refused')))


def load(path,pin,name):
    require(digest(path)==pin,'Frozen canary input module changed')
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    require(digest(path)==pin,'Frozen module changed during import')
    return module


def descriptor_clone(original,emitted,qualification,runtime):
    expected={**original,'controller_sha256':BASE_SHA,'backfill_qualification':qualification,
        'backfill_runtime_contract':runtime}
    require(emitted==expected,'Emitted base descriptor must be the exact original with only the declared operational additions')
    return {'original_controller_sha256':original['controller_sha256'],'emitted_controller_sha256':emitted['controller_sha256'],
        'changed_or_added_fields':sorted(k for k in expected if original.get(k)!=expected[k])}


def fixture_registration(plan,job):
    require(plan.get('tasks') and len({x['name'] for x in plan['tasks']})==len(plan['tasks']),
        'Unique actual emitted task roster required')
    return {'source_sha256':plan['release_identity']['sha256'],'plan_sha256':canonical(plan),
        'tasks':{task['name']:{'job_id':job,'state':'submitted','script_sha256':task['script_sha256']}
            for task in plan['tasks']}}


@contextmanager
def readonly(attempts):
    """Prevent writes or subprocesses in the current canary process only."""
    original_builtin=builtins.open;original_io=io.open;original_os_open=os.open
    def deny(operation,*args,**kwargs):
        attempts.append({'operation':operation,'target':str(args[0]) if args else None})
        raise RuntimeError('Read-only canary refused '+operation)
    def guarded(original,file,mode='r',*args,**kwargs):
        require(isinstance(mode,str),'Explicit ordinary file mode required')
        if any(flag in mode for flag in ('w','a','x','+')):return deny('file-write',file,mode)
        return original(file,mode,*args,**kwargs)
    def guarded_os_open(path,flags,*args,**kwargs):
        if flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND):return deny('os.open-write',path)
        return original_os_open(path,flags,*args,**kwargs)
    with ExitStack() as stack:
        stack.enter_context(patch('builtins.open',lambda file,mode='r',*a,**k:guarded(original_builtin,file,mode,*a,**k)))
        stack.enter_context(patch('io.open',lambda file,mode='r',*a,**k:guarded(original_io,file,mode,*a,**k)))
        stack.enter_context(patch.object(os,'open',guarded_os_open))
        for name in ('mkdir','makedirs','remove','unlink','rename','replace','rmdir'):
            stack.enter_context(patch.object(os,name,lambda *a,_name=name,**k:deny('os.'+_name,*a,**k)))
        for name in ('Popen','run','call','check_call','check_output'):
            stack.enter_context(patch.object(subprocess,name,lambda *a,_name=name,**k:deny('subprocess.'+_name,*a,**k)))
        yield


class StopBeforeControllerRun(Exception):pass


def canary(pack,policy_path):
    """Route actual emitted files through real outer startup and read-only gates.

    The not-yet-submitted controller ledger and allocation are explicit in-memory
    fixtures. Real base.load_context, admission, guard installation, run and
    dispatch never execute. Qualification/source/worker/registry checks use real
    immutable files and persisted native completion proofs.
    """
    plan_path=Path(pack['plan']);ledger_path=Path(pack['ledger']);policy_path=Path(policy_path)
    plan=read(plan_path)
    require(canonical(plan)==pack['plan_sha256'] and digest(plan_path)==pack['plan_file_sha256']
        and plan['tasks']==pack['tasks'] and len(plan['tasks'])==4,'Actual emitted four-task plan differs from builder pack')
    require(not ledger_path.exists(),'Canary requires the real controller ledger to remain unsubmitted')
    require(policy_path==Path(plan['descriptor_path']) and digest(policy_path)==plan['descriptor_sha256'],
        'Exact actual emitted policy locator required')
    outer=load(SUPPORT/'run-af-agent-controller-backfill-policy-v1-20261024.py',OUTER_SHA,'emitted_canary_outer')
    base=outer.load_base()
    require(digest(Path(base.__file__))==BASE_SHA,'Exact actual new base required')
    policy=read(policy_path)
    original=read(OLD_DESCRIPTOR);require(digest(OLD_DESCRIPTOR)==OLD_DESCRIPTOR_SHA,'Exact original descriptor required')
    descriptor_path=Path(policy['base_descriptor']['path'])
    emitted=read(descriptor_path)
    clone=descriptor_clone(original,emitted,policy['backfill_qualification'],outer.BACKFILL_RUNTIME)
    require(digest(descriptor_path)==policy['base_descriptor']['sha256'],'Actual emitted descriptor hash differs')
    fixture=fixture_registration(plan,FIXTURE_JOB);fixture_sha=canonical(fixture)
    inputs={str(plan_path):digest(plan_path),str(policy_path):digest(policy_path),str(descriptor_path):digest(descriptor_path),
        str(OLD_DESCRIPTOR):digest(OLD_DESCRIPTOR),str(Path(outer.__file__)):OUTER_SHA,str(Path(base.__file__)):BASE_SHA}
    attempts=[];routes=[];actual_gates=[];stop=[]
    real_packet=outer.json_packet;real_digest=outer.digest
    def json_packet(binding,namespace,*args,**kwargs):
        if Path(binding['path'])==ledger_path:
            require(binding['sha256']==fixture_sha,'In-memory registration fixture binding changed')
            return fixture
        return real_packet(binding,namespace,*args,**kwargs)
    def route_digest(path):
        return fixture_sha if Path(path)==ledger_path else real_digest(path)
    proxy=SimpleNamespace(**vars(base))
    def stop_before_run(_context):
        stop.append(True);raise StopBeforeControllerRun()
    def metadata_context(path,pin,segment):
        d=base.frozen_file(path,pin)
        worker_path,worker_sha,worker=base.worker_binding(d);actual_gates.append('worker_binding')
        registry={**base.registry_binding(d),'prestart_retry_value':base.retry_binding(d),
            'prestart_retry_binding':dict(d['prestart_retry_amendment']) if 'prestart_retry_amendment' in d else None}
        actual_gates.extend(['registry_binding','retry_binding'])
        config=base.read_json(base.PRIMARY/'configs/study.json')
        framework=base.contained(d['framework_release'],base.ROOT/'releases',exists=False)
        require(base.canonical_hash(config)==base.BASE_CONFIG_SHA256
            and base.read_json(framework/'configs/study.json')==config,'Actual original full configuration changed')
        protocol_path=base.agents._directory(base.ROOT,config['agents'])/'frozen_protocol.json'
        context={'descriptor':d,'descriptor_path':Path(path).resolve(),'descriptor_sha256':pin,
            'framework':framework,'protocol_path':protocol_path,'worker_descriptor_path':worker_path,
            'worker_descriptor_sha256':worker_sha,'worker_descriptor':worker,'registry_overlay':registry}
        base.source_guard(context);actual_gates.append('source_guard')
        task=plan['tasks'][segment]
        context['allocation']={'JobId':FIXTURE_JOB,'UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu',
            'JobState':'RUNNING','Command':task['script'],'JobName':'hhw-'+task['name'],'NumCPUs':'1','NumNodes':'1',
            'TimeLimit':'08:00:00','TimeMin':'08:00:00','MinMemoryNode':'64G','AllocTRES':'cpu=1,mem=64G,node=1'}
        return context
    proxy.load_context=metadata_context;proxy.run=stop_before_run
    # Imports above define modules only; every routed startup gate below runs
    # under the write/subprocess prohibition. Only this new module is patched.
    with patch.object(outer,'json_packet',json_packet),patch.object(outer,'digest',route_digest),\
            patch.object(outer,'native_job_id',lambda:FIXTURE_JOB),patch.object(outer,'load_base',lambda:proxy):
        with readonly(attempts):
            for segment,task in enumerate(plan['tasks']):
                argv=shlex.split(task['command']);helper=str(Path(outer.__file__))
                require(argv.count(helper)==1,'Exact emitted outer helper invocation required')
                tail=argv[argv.index(helper)+1:]
                require(tail[tail.index('--segment')+1]==str(segment),'Emitted task/segment order changed')
                try:outer.main(tail)
                except StopBeforeControllerRun:pass
                else:raise ValueError('Canary did not stop before the original controller run')
                routes.append({'segment':segment,'task':task['name'],'script':task['script'],
                    'script_sha256':digest(task['script']),'actual_outer_main_routed':True})
    require(len(stop)==4 and not attempts,'Every segment must stop before original run without mutations')
    require(all(digest(path)==pin for path,pin in inputs.items()),'Canary input bytes changed during read-only routing')
    return {'status':'actual_emitted_descriptor_readonly_canary_passed','routes':routes,'descriptor_clone':clone,
        'source_inputs':inputs,'actual_generic_gate_calls':actual_gates,
        'registration_and_allocation':'in_memory_fixtures_for_unsubmitted_controllers_only',
        'fixture_job_id':FIXTURE_JOB,'actual_controller_native_registration_verified':False,
        'full_base_load_context_executed':False,'original_base_run_executed':False,'worker_dispatch_executed':False,
        'native_or_filesystem_mutation_attempts':attempts,'canonical_state_changed':False,
        'scientific_result':False,'admission_authority':False,'scientific_gates_changed':False,
        'checked_utc':datetime.now(timezone.utc).isoformat()}
