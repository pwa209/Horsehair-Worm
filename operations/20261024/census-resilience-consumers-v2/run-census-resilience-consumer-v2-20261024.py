"""Source-bound provider routing for new finite AF and primary allocations.

Only a private copy of the provider-proof function's global namespace changes.
The original controller, primary package and census clocks remain authoritative.
"""
from pathlib import Path
from types import FunctionType
from datetime import datetime, timezone
import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import runpy
import shlex
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-census-resilience-consumers-v2-20261024'
SELF_NAME='run-census-resilience-consumer-v2-20261024.py'
TEST_NAME='test_census_resilience_consumers_v2_20261024.py'
QUALIFIER_NAME='qualify-census-resilience-consumers-v2-20261024.py'
FILES={SELF_NAME,TEST_NAME,QUALIFIER_NAME,'build-consumer-plan.py'}
PROVIDER_SUPPORT=ROOT/'maintenance/study-census-resilience-support-v1-20261024'
PROVIDER_HELPER=PROVIDER_SUPPORT/'run-fresh-storage-census-resilience-v1-20261024.py'
PROVIDER_HELPER_SHA='94ac29d11da50412ba3c1f9269ed5762224f93635cd1fae505bfabc5f9b63a93'
PROVIDER_MANIFEST_SHA='dbe6c589a7dda4e8d54cc041ddee1dfa77f7804d67e290efcd396c7108b6f152'
PROVIDER_CASE=ROOT/'maintenance/study-census-resilience-v1-20261024'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
OLD_SOURCE='d89b31180d4c4ffe9c4023dc5534b96f2d157a3a8e3aecaff30849283d00a705'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
AF=ROOT/'maintenance/study-af-backfill-controller-support-v2-20261024/run-af-agent-controller-backfill-policy-v2-20261024.py'
AF_SHA='c878954615c56d750e963a7d2b81b8ad3752b74b798b32a7ff64037318e652bf'
PRIMARY=ROOT/'maintenance/study-primary-external-census-policy-v1-20261023/run-primary-external-census-v1-20261023.py'
PRIMARY_SHA='dd5f8d2aec90896fdcfe190971ea8b6c32a94158a3684566c2b8405748e7c794'
SCHEDULER=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
FALSE_FLAGS={'scientific_result':False,'admission_authority':False,'scientific_gates_cleared':False,
    'freshness_timestamp_rewritten':False,'canonical_science_changed':False,'parallel_controller_authorized':False}


def require(value,message):
    if not value:raise ValueError(message)


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def strict(raw):
    def pairs(items):
        value={}
        for key,item in items:
            require(key not in value,'Duplicate JSON key refused');value[key]=item
        return value
    def constant(value):raise ValueError('Nonfinite JSON refused')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)


def packet(path,sha=None,*,namespace=None,limit=8_000_000):
    p=Path(path);namespace=ROOT if namespace is None else Path(namespace)
    require(p.is_absolute() and p==p.resolve() and p!=namespace and p.is_relative_to(namespace)
        and p.is_file() and all(not q.is_symlink() for q in (p,*p.parents) if q.is_relative_to(namespace)),
        'Ordinary confined immutable packet required')
    with p.open('rb') as stream:raw=stream.read(limit+1)
    require(len(raw)<=limit and (sha is None or hashlib.sha256(raw).hexdigest()==sha),'Bounded source/evidence bytes changed')
    return raw


def locator(path):return {'path':str(path),'sha256':digest(path)}


def json_packet(binding,*,namespace=None,limit=8_000_000):
    require(type(binding) is dict and set(binding)=={'path','sha256'}
        and re.fullmatch('[0-9a-f]{64}',str(binding['sha256'])),'Exact immutable locator required')
    return strict(packet(binding['path'],binding['sha256'],namespace=namespace,limit=limit))


def source_guard(pins):
    for path,sha in pins.items():packet(path,sha)


def module(path,sha,name):
    packet(path,sha,limit=200_000)
    spec=importlib.util.spec_from_file_location(name,path)
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    require(digest(path)==sha,'Imported exact source changed')
    return value


def source_manifest(binding):
    require(Path(binding['path'])==SUPPORT/'source-manifest.json','Exact new consumer manifest namespace required')
    value=json_packet(binding,namespace=SUPPORT)
    require(set(value)=={'kind','files','suites'} and value['kind']=='census-resilience-consumer-source-manifest-v2'
        and set(value['files'])==FILES and set(value['suites'])=={'consumers'},'Complete new consumer source closure required')
    pins={binding['path']:binding['sha256']}
    for name,sha in value['files'].items():
        packet(SUPPORT/name,sha,namespace=SUPPORT,limit=200_000);pins[str(SUPPORT/name)]=sha
    contract=value['suites']['consumers']
    require(set(contract)=={'source','source_sha256','tests','roster_sha256'} and contract['source']==TEST_NAME
        and contract['source_sha256']==value['files'][TEST_NAME] and type(contract['tests']) is int and contract['tests']>0
        and re.fullmatch('[0-9a-f]{64}',str(contract['roster_sha256'])),'Complete exact consumer test contract required')
    require(Path(__file__)==SUPPORT/SELF_NAME and digest(__file__)==value['files'][SELF_NAME],
        'Only the exact qualified consumer entry point may execute')
    return value,pins


def passing_junit(binding,contract):
    raw=packet(binding['path'],binding['sha256'],namespace=SUPPORT)
    require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Unsafe JUnit refused')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'))
    names=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
    leaves=[n for n in tree.iter('testsuite') if not n.findall('testsuite')]
    require(len(cases)==len(set(names))==contract['tests'] and canonical(names)==contract['roster_sha256']
        and all(c.get('classname') and c.get('name') for c in cases)
        and not any(list(c.iter(k)) for c in cases for k in ('failure','error','skipped'))
        and leaves and sum(int(n.get('tests','-1')) for n in leaves)==contract['tests']
        and all(int(n.get(k,'0'))==0 for n in tree.iter() if n.tag in {'testsuite','testsuites'} for k in ('failures','errors','skipped')),
        'Actual complete passing source-bound native consumer roster required')


def registration(plan_binding,ledger_binding,script_binding,task_name,job,*,entry,argv,source,cpus,hours,memory):
    plan=json_packet(plan_binding,namespace=ROOT/'plans');ledger=json_packet(ledger_binding,namespace=ROOT/'state')
    text=packet(script_binding['path'],script_binding['sha256'],namespace=ROOT/'plans').decode()
    selected=[t for t in plan.get('tasks',[]) if t.get('name')==task_name]
    require(len(selected)==1 and plan.get('root')==str(ROOT) and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG
        and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256')==source
        and ledger.get('plan_sha256')==canonical(plan),'Exact registered source/config/plan pair required')
    task=selected[0];row=ledger.get('tasks',{}).get(task_name,{})
    command=shlex.split(task.get('command',''))
    require(command.count(str(entry))==1 and command[command.index(str(entry))+1:]==list(argv)
        and task.get('script')==script_binding['path'] and task['command'] in text
        and task.get('script_sha256')==row.get('script_sha256')==canonical(text)
        and row.get('state')=='submitted' and row.get('job_id')==job
        and task.get('resources')=={'cpus':cpus,'memory':memory,'hours':hours,'gpu':False,'array':None},
        'Exact complete entry/argv/script/native/resource registration required')
    return task


def native_fields(response):
    response.check_returncode();require(not response.stderr,'Native scheduler stderr refused')
    text=response.stdout.decode() if isinstance(response.stdout,bytes) else response.stdout
    return dict(part.split('=',1) for part in shlex.split(text) if '=' in part)


def seconds(value):
    days,clock=(value.split('-',1) if '-' in value else ('0',value));parts=[int(x) for x in clock.split(':')]
    require(len(parts)==3,'Exact native hour:minute:second duration required')
    return int(days)*86400+parts[0]*3600+parts[1]*60+parts[2]


def allocation(fields,job,task,kind,*,wall=time.time):
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING'
        and fields.get('JobName')=='hhw-'+task['name'] and fields.get('Command')==task['script']
        and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'}
        and fields.get('MinMemoryNode') in {'64G','65536M'} and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields
        and all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Exact owned scalar1CPU64G/noGPU consumer allocation required')
    limit=seconds(fields.get('TimeLimit',''))
    require((kind=='af' and 28800<=limit<=345600 and seconds(fields.get('TimeMin',''))==28800)
        or (kind=='primary' and limit==259200),'Original finite native time contract required')
    end=datetime.fromisoformat(fields.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(end-env)<=2 and wall()+14400+300<min(end,env),'Actual native end and original shutdown margin required')
    return min(end,env)


def chain_guard(plan,ledger,binding):
    kind=binding['consumer_kind'];tasks=plan.get('tasks',[])
    phase='study-'+kind+'-census-resilience-consumers-v2-20261024'
    require(plan.get('phase')==phase and len(tasks)==4,'One exact new four-controller phase required')
    jobs=[]
    for index,task in enumerate(tasks):
        name=kind+'-resilient-census-controller-'+str(index)
        require(task.get('name')==name and task.get('depends')==([] if index==0 else [tasks[index-1]['name']])
            and task.get('external_dependencies')==([binding['afterany_parent']] if index==0 else [])
            and task.get('dependency_kind')=='afterany','Exact sequential afterany retirement/controller edges required')
        row=ledger.get('tasks',{}).get(name,{})
        require(row.get('state')=='submitted' and re.fullmatch('[1-9][0-9]*',str(row.get('job_id','')))
            and row.get('script_sha256')==task.get('script_sha256'),'Exact distinct registered replacement jobs required')
        jobs.append(row['job_id'])
    require(len(set(jobs))==4 and binding['afterany_parent'] not in jobs,'Replacement allocations must be distinct from their old parent')
    return jobs


def predecessor_guard(fields,task,ledger,binding,completion):
    """A new allocation may never execute beside its inherited old parent."""
    parent=binding['afterany_parent']
    require(completion(parent,dependency_kind='afterany') is not None,'Old parent has no final terminal accounting proof')
    row=ledger['tasks'][task['name']];issued=row.get('issued_native_dependencies');done=row.get('completed_native_dependencies',[])
    require(type(issued) is list and len(issued)==len(set(issued)) and row.get('dependency_kind')=='afterany'
        and row.get('dependency_resolution_sha256')==canonical({'kind':'afterany','issued':issued,'completed':done}),
        'Exact persisted dependency resolution required')
    expected=set(task['external_dependencies'])|{ledger['tasks'][name]['job_id'] for name in task['depends']}
    require(set(issued)|{p.get('job_id') for p in done}==expected,'Registered afterany predecessor set differs')
    for proof in done:
        require(proof.get('dependency_kind')=='afterany' and completion(proof['job_id'],dependency_kind='afterany') is not None,
            'Removed native dependency requires positive terminal proof')
    # Slurm may remove a fulfilled afterany edge from a running allocation.
    # Every absent edge therefore needs the same owned native final proof.
    observed={}
    for part in fields.get('Dependency','').split(','):
        if part in {'','(null)','None'}:continue
        match=re.fullmatch(r'afterany:([1-9][0-9]*)(?:\((?:fulfilled|unfulfilled)\))?',part)
        require(match is not None,'Only exact afterany native dependencies are permitted');observed[match[1]]=True
    require(set(observed)<=expected,'Unexpected native controller dependency')
    for job in expected:require(completion(job,dependency_kind='afterany') is not None,'Registered predecessor is not positively terminal')


def validate_qualification(binding,manifest_binding,native_reader):
    from horsehair import slurm
    require(set(binding)=={'path','sha256','job_id'} and Path(binding['path'])==SUPPORT/'qualification.json'
        and re.fullmatch('[1-9][0-9]*',str(binding['job_id'])),'Exact separately completed consumer qualification required')
    manifest,pins=source_manifest(manifest_binding)
    proof=slurm.completed_native_dependency(ROOT,binding['job_id'])
    require(proof is not None,'Consumer qualification native completion is pending')
    row=proof['native_records'].get(binding['job_id'],{})
    require(row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0' and row.get('cpus')==4 and row.get('gpus')==0
        and type(row.get('elapsed_seconds')) is int and 0<row['elapsed_seconds']<=3600,'Positive scalar4CPU1h consumer qualification required')
    q=json_packet({k:binding[k] for k in ('path','sha256')},namespace=SUPPORT)
    require(q.get('status')=='complete_census_resilience_consumer_qualification' and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('native_job_id')==binding['job_id'] and q.get('source_manifest')==manifest_binding
        and q.get('source_pins')==pins and all(q.get(flag) is False for flag in FALSE_FLAGS),
        'Actual new consumer source-bound qualification receipt required')
    contract=manifest['suites']['consumers'];suite=q['suite']
    require(suite.get('source')=={'path':str(SUPPORT/TEST_NAME),'sha256':contract['source_sha256']}
        and suite.get('test_counts')=={'tests':contract['tests'],'failures':0,'errors':0,'skipped':0}
        and suite.get('test_roster_sha256')==contract['roster_sha256'],'Exact native consumer suite source/counts required')
    passing_junit(suite['junit'],contract);pins[suite['junit']['path']]=suite['junit']['sha256']
    reg=q['native_registration'];helper=SUPPORT/QUALIFIER_NAME
    expected={'--native-plan':reg['plan']['path'],'--native-ledger':reg['ledger']['path'],'--native-task':reg['task_name'],
        '--native-script':reg['script']['path'],'--source-manifest':manifest_binding['path'],'--source-manifest-sha256':manifest_binding['sha256']}
    plan=json_packet(reg['plan'],namespace=ROOT/'plans');tasks=[t for t in plan['tasks'] if t['name']==reg['task_name']]
    require(len(tasks)==1,'One actual consumer qualification task required')
    tokens=shlex.split(tasks[0]['command']);require(tokens.count(str(helper))==1,'Exact consumer qualifier invocation required')
    tail=tokens[tokens.index(str(helper))+1:]
    require(len(tail)==12 and len(set(tail[::2]))==6 and dict(zip(tail[::2],tail[1::2]))==expected,'Complete exact qualifier arguments required')
    task=registration(reg['plan'],reg['ledger'],reg['script'],reg['task_name'],binding['job_id'],entry=helper,argv=tail,
        source=SOURCE,cpus=4,hours=1,memory='16G')
    require(reg['native_job_id']==binding['job_id'] and task.get('census_resilience_consumer_qualification')=={
        'helper':{'path':str(helper),'sha256':manifest['files'][QUALIFIER_NAME]},'source_manifest':manifest_binding,'suite':contract},
        'Actual registered qualifier source/manifest/suite metadata required')
    fields=q['allocation']
    require(fields.get('JobId')==binding['job_id'] and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING'
        and fields.get('NumCPUs')=='4' and fields.get('NumNodes') in {'1','1-1'}
        and fields.get('Command')==task['script'] and fields.get('JobName')=='hhw-'+task['name']
        and fields.get('MinMemoryNode') in {'16G','16384M'} and seconds(fields.get('TimeLimit',''))==3600
        and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields
        and all('gpu' not in fields.get(k,'').lower() for k in ('AllocTRES','ReqTRES','Gres')),'Actual qualifier native allocation differs')
    pins[binding['path']]=binding['sha256']
    for key in ('plan','ledger','script'):pins[reg[key]['path']]=reg[key]['sha256']
    source_guard(pins)
    return {'checked_paths':pins,'completion':proof,'qualification':binding}


def check_binding(binding,sha):
    require(Path(binding).parent==SUPPORT and Path(binding).name in {'af-consumer-binding.json','primary-consumer-binding.json'},
        'Exact new separate consumer binding path required')
    value=json_packet({'path':str(binding),'sha256':sha},namespace=SUPPORT)
    require(set(value)=={'kind','consumer_kind','source_manifest','qualification','provider_schedule','legacy_entry','legacy_policy','afterany_parent','flags'}
        and value['kind']=='census-resilience-consumer-binding-v2' and value['consumer_kind'] in {'af','primary'}
        and value['flags']==FALSE_FLAGS,'Exact new consumer routing descriptor required')
    require(Path(binding).name==value['consumer_kind']+'-consumer-binding.json','Consumer identity and descriptor name must agree')
    expected=(AF,AF_SHA,'6950429') if value['consumer_kind']=='af' else (PRIMARY,PRIMARY_SHA,'6873101')
    require(value['legacy_entry']=={'path':str(expected[0]),'sha256':expected[1]} and value['afterany_parent']==expected[2],
        'Only exact frozen legacy controller and actual old parent may be routed')
    schedule=value['provider_schedule']
    require(schedule.get('helper')=={'path':str(PROVIDER_HELPER),'sha256':PROVIDER_HELPER_SHA}
        and schedule.get('source_manifest')=={'path':str(PROVIDER_SUPPORT/'source-manifest.json'),'sha256':PROVIDER_MANIFEST_SHA}
        and schedule.get('case_root')==str(PROVIDER_CASE),'Exact separately sealed new provider source required')
    manifest,pins=source_manifest(value['source_manifest']);pins[str(binding)]=sha
    packet(expected[0],expected[1],limit=200_000);pins[str(expected[0])]=expected[1]
    json_packet(value['legacy_policy'],namespace=ROOT/'maintenance');pins[value['legacy_policy']['path']]=value['legacy_policy']['sha256']
    return value,manifest,pins


def routed_provider(outer,binding,pins,native_reader):
    """Retain the exact old proof code and change its private case-root constant."""
    original=outer.provider_proof
    namespace=dict(original.__globals__);namespace['PROVIDER_CASE']=PROVIDER_CASE
    private=FunctionType(original.__code__,namespace,original.__name__,original.__defaults__,original.__closure__)
    require(private.__code__ is original.__code__ and all(namespace[k] is v for k,v in original.__globals__.items() if k!='PROVIDER_CASE'),
        'Only the private provider case-root lookup may differ')
    def proof(base,context,pair):
        source_guard(pins)
        # Original guarded accounting/admission installs the existing exact
        # reader before the new metadata-only provider validation uses it.
        with base.registration_guard(context['base_context'],'controller-start'):
            base.admission(context['base_context'],'resilient-provider-qualification',cpus=0,gpus=0,hours=1)
        provider=module(PROVIDER_HELPER,binding['provider_schedule']['helper']['sha256'],'new_qualified_resilient_provider')
        require(binding['provider_schedule']['helper']['path']==str(PROVIDER_HELPER)
            and binding['provider_schedule']['case_root']==str(PROVIDER_CASE),'Exact new qualified provider namespace required')
        checked=provider.validate_schedule_binding(binding['provider_schedule'],binding['provider_schedule']['qualification'],native_reader)
        source_guard(checked['checked_paths'])
        routed={**context,'policy':{**context['policy'],'provider_schedule':binding['provider_schedule']}}
        result=private(base,routed,pair)
        receipt=json_packet(result['receipt'],namespace=PROVIDER_CASE)
        provider.validate_predecessor_receipt(receipt,native_reader)
        source_guard({**pins,**checked['checked_paths']})
        return {**result,'consumer_source_manifest':binding['source_manifest'],'new_provider_schedule':binding['provider_schedule'],
            'legacy_provider_proof_code_preserved':True,**FALSE_FLAGS}
    return proof


def prefix(binding_path,binding_sha,legacy_entry):
    return [str(SUPPORT/SELF_NAME),'--consumer-binding',str(binding_path),'--consumer-binding-sha256',binding_sha,'--legacy-entry',str(legacy_entry)]


def install_primary(primary,binding,pins,native_reader,binding_path,binding_sha):
    original_module=primary.module;original_make=primary.make_consumer;original_child=primary.bounded_child
    def load(path,sha,name):
        loaded=original_module(path,sha,name)
        if Path(path)==primary.CENSUS_SUPPORT/primary.OUTER_NAME:
            loaded.provider_proof=routed_provider(loaded,binding,pins,native_reader)
        return loaded
    def make(context):
        def child(command,**kwargs):
            require(command.count(str(PRIMARY))==1 and command[command.index(str(PRIMARY))+1:][:2]==['--mode','verify'],
                'Only the exact isolated original primary verifier may be routed')
            source_guard(pins)
            position=command.index(str(PRIMARY))
            routed=command[:position]+prefix(binding_path,binding_sha,PRIMARY)+command[position+1:]
            return original_child(routed,**kwargs)
        return original_make(context,runner=child)
    primary.module=load;primary.make_consumer=make
    return {'original_module':original_module,'original_make_consumer':original_make,'original_bounded_child':original_child}


def parse(argv):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--consumer-binding',type=Path,required=True);p.add_argument('--consumer-binding-sha256',required=True)
    p.add_argument('--legacy-entry',type=Path,required=True)
    require(argv.count('--legacy-entry')==1,'One explicit legacy entry boundary required')
    boundary=argv.index('--legacy-entry')+2
    require(boundary<=len(argv),'Actual legacy entry token required')
    args=p.parse_args(argv[:boundary]);args.legacy_argv=argv[boundary:]
    return args


def bounded_native_reader(end):
    """Use the currently installed original guard and a finite real transport."""
    from horsehair import repair
    def read(ids,rosters,**kwargs):
        def bounded(command,**options):
            remaining=end-time.time()-600
            require(remaining>0,'Native reader cleanup margin exhausted')
            require(command and command[0]=='sacct','Only the original read-only accounting command is allowed')
            options['timeout']=min(45,remaining)
            return subprocess.run(command,**options)
        require('runner' not in kwargs,'Only the bounded actual native transport is allowed')
        return repair._repair_native_states(ids,rosters,runner=bounded,**kwargs)
    return read


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv);args=parse(argv)
    binding,manifest,pins=check_binding(args.consumer_binding,args.consumer_binding_sha256)
    require(str(args.legacy_entry)==binding['legacy_entry']['path'],'Actual legacy entry token differs from frozen binding')
    legacy=module(args.legacy_entry,binding['legacy_entry']['sha256'],'consumer_exact_legacy_entry')
    kind=binding['consumer_kind'];job=os.environ.get('SLURM_JOB_ID','')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209',
        'Owned scalar native consumer allocation required')
    parent_argv=args.legacy_argv
    verify=kind=='primary' and parent_argv[:2]==['--mode','verify']
    if verify:
        request=legacy.request_context(legacy.parser().parse_args(parent_argv));parent_argv=request['argv']
    flags=dict(zip(parent_argv[::2],parent_argv[1::2]))
    require(len(parent_argv)%2==0 and len(flags)==len(parent_argv)//2
        and flags.get('--policy-descriptor')==binding['legacy_policy']['path']
        and flags.get('--policy-descriptor-sha256')==binding['legacy_policy']['sha256']
        and (kind!='primary' or flags.get('--mode')=='run'),'Complete exact legacy run arguments required')
    full=prefix(args.consumer_binding,args.consumer_binding_sha256,args.legacy_entry)[1:]+parent_argv
    task=registration(locator(flags['--native-plan']),locator(flags['--native-ledger']),locator(flags['--native-script']),
        flags['--native-task'],job,entry=SUPPORT/SELF_NAME,argv=full,source=SOURCE if kind=='af' else OLD_SOURCE,
        cpus=1,hours=96 if kind=='af' else 72,memory='64G')
    require(task.get('census_resilience_consumer')=={'entry':{'path':str(SUPPORT/SELF_NAME),'sha256':manifest['files'][SELF_NAME]},
        'binding':{'path':str(args.consumer_binding),'sha256':args.consumer_binding_sha256},'consumer_kind':kind},
        'Actual source-bound new envelope registration metadata required')
    packet(SCHEDULER,SCHEDULER_SHA);scheduler=runpy.run_path(str(SCHEDULER))['run_scontrol_job']
    # Verify children have their original allocation-local request; only the
    # parent creates this separate, immutable envelope allocation evidence.
    area=SUPPORT/('native-'+job)
    if not verify:
        require(not area.exists(),'New envelope allocation cannot replay evidence');area.mkdir(mode=0o700)
    fields=native_fields(scheduler(job,area/('verify-native-'+str(request['sequence']).zfill(6) if verify else 'native-read')))
    end=allocation(fields,job,task,kind)
    from horsehair import slurm
    plan=json_packet(locator(flags['--native-plan']),namespace=ROOT/'plans')
    ledger=json_packet(locator(flags['--native-ledger']),namespace=ROOT/'state')
    chain_guard(plan,ledger,binding)
    predecessor_guard(fields,task,ledger,binding,lambda job,**kw:slurm.completed_native_dependency(ROOT,job,**kw))
    # The new provider API takes the qualified accounting reader used by all
    # existing proof validators; it never submits or refreshes a census.
    native_reader=bounded_native_reader(end)
    qualification=validate_qualification(binding['qualification'],binding['source_manifest'],native_reader)
    pins.update(qualification['checked_paths'])
    for flag in ('--native-plan','--native-ledger','--native-script'):pins[flags[flag]]=digest(flags[flag])
    source_guard(pins)
    if kind=='af':
        legacy.provider_proof=routed_provider(legacy,binding,pins,native_reader)
    else:
        install_primary(legacy,binding,pins,native_reader,args.consumer_binding,args.consumer_binding_sha256)
    result=legacy.main(args.legacy_argv)
    source_guard(pins)
    return result


if __name__=='__main__':raise SystemExit(main())
