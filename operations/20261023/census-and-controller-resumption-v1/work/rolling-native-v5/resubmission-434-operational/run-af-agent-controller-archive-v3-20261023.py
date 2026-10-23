"""Prospective private archive representation overlay for frozen F2 controller.

The base controller and numerical worker remain separate unchanged files.
Whole-f047 archive reads run in a child before the registration mutex is taken.
Only its exact current state packets can replace completed-policy file reads.
This helper never packs or unlinks anything and never discounts future savings.
"""
from pathlib import Path
import argparse
import ast
import gzip
import hashlib
import importlib.util
import json
import math
import os
import re
import runpy
import shlex
import stat
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from typing import NamedTuple

from horsehair import core,slurm
from horsehair.core import canonical_hash, digest, source_identity,read_json
from horsehair.rna_recovery import _primary_environment

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
BASE_SHA='e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
BASE_PATH=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022/run-af-agent-controller-registry-v6-20261022.py'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
CONSUMER_SOURCE='f047ce12b0a4a31312f2b4f5b6cff144342a12950f2e84fe1d444c8ea3a9b68c'
CONSUMER_FRAMEWORK=ROOT/'releases/20261021-e104ce63fa53-f047ce12b0a4'
CONSUMER_Q={'path':str(ROOT/'maintenance/study-af-execution-support-v4-20261021/qualification.json'),
    'sha256':'8c3e1910deabea6c1061ddb353c3a6881fb70fb9fe86948007557a03ca809176','job_id':'6703313'}
# Frozen only after the independent full suites and explicit closure are ready.
VERIFIER_SHA='b02afa747f8e37a654dc0eda6f3a370808cdd381cf92a55a9b56ea9a8b6bfcae'
TEST_SUITES={
    'test_af_agent_controller_archive_v3_20261023.py':{
        'sha256':'de914a3b8a3ad99586a9082912dcbf36004fcd9391501cbe5f44cfee79f8e461',
        'tests':111,'roster_sha256':'ecd310d80640df1e95c408c1f2e2b3ceaba7fd168a8705066c69b19ca83835b7'},
    'test_verify_f2_condition_archives_v2_20261022.py':{
        'sha256':'88e79c8fcb4fe7b80882c176d78006f301c50a0c0f6cb8266d4c5bfbb867c565',
        'tests':25,'roster_sha256':'3efab27035c49b116b8ef8583d15b531fb9f081c1e49d77e119629e42dcee7d2'}}
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
VALIDATION_FRAMEWORK=CONSUMER_FRAMEWORK
VALIDATION_SOURCE=CONSUMER_SOURCE
VALIDATION_MODULE_SHA='507fbe01700e4310a4e44b16f178d924fe291585d20646830e97b7a28e13fd6c'
DRIVER=ROOT/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'
DRIVER_SHA='49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
OVERLAY_TEST='test_af_agent_controller_archive_v3_20261023.py'
VERIFIER_TEST='test_verify_f2_condition_archives_v2_20261022.py'
MAX_METADATA=10_000_000
MAX_STATE=512_000_000


def require(value,message):
    if not value:raise ValueError(message)


def strict(raw):
    def pairs(values):
        result={}
        for key,value in values:
            require(key not in result,'Duplicate archive overlay JSON key');result[key]=value
        return result
    def invalid(value):raise ValueError('Nonfinite archive overlay JSON refused')
    def floating(text):
        value=float(text);require(math.isfinite(value),'Nonfinite archive overlay JSON refused');return value
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid,parse_float=floating)


def ordinary(path,*,fresh=False,namespace=None):
    namespace=ROOT/'maintenance' if namespace is None else Path(namespace)
    path=Path(path)
    require(path.is_absolute() and path==path.resolve() and path.is_relative_to(namespace)
        and path!=namespace and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(ROOT)),
        'Ordinary canonical personal maintenance path required')
    require(not path.exists() if fresh else path.is_file(),'Fresh evidence required' if fresh else 'Ordinary evidence file required')
    return path


def locator(path,*,namespace=None,maximum=MAX_METADATA):
    """Hash a finite unchanged file; state opts into its separate gzip cap."""
    require(type(maximum) is int and maximum>0,'Finite positive archive overlay byte bound required')
    path=ordinary(path,namespace=namespace);before=path.stat()
    require(before.st_size<=maximum,'Finite archive overlay source exceeded')
    def identity(row):
        # Windows path.stat and descriptor fstat expose different ctime
        # conventions. Compare each ctime only within its own observation API.
        return (row.st_dev,row.st_ino,row.st_mode,row.st_size,row.st_mtime_ns)
    sha=hashlib.sha256();total=0
    with path.open('rb') as stream:
        opened=os.fstat(stream.fileno())
        require(identity(opened)==identity(before),'Archive overlay source changed before bounded hash')
        while True:
            chunk=stream.read(min(1_048_576,maximum-total+1))
            if not chunk:break
            total+=len(chunk);require(total<=maximum,'Finite archive overlay source exceeded');sha.update(chunk)
        after_stream=os.fstat(stream.fileno())
        require(total==before.st_size and identity(after_stream)==identity(opened)
            and after_stream.st_ctime_ns==opened.st_ctime_ns,
            'Archive overlay source changed during bounded hash')
    after=ordinary(path,namespace=namespace).stat()
    require(identity(after)==identity(before) and after.st_ctime_ns==before.st_ctime_ns,
        'Archive overlay source changed after bounded hash')
    return {'path':str(path),'sha256':sha.hexdigest()}


def read_bound(binding,maximum=MAX_METADATA,*,namespace=None):
    require(type(binding) is dict and set(binding)=={'path','sha256'}
        and type(binding['sha256']) is str and re.fullmatch('[0-9a-f]{64}',binding['sha256']),
        'Exact archive overlay path/SHA required')
    path=ordinary(binding['path'],namespace=namespace);require(path.stat().st_size<=maximum,'Finite archive overlay source exceeded')
    with path.open('rb') as stream:raw=stream.read(maximum+1)
    require(len(raw)<=maximum and hashlib.sha256(raw).hexdigest()==binding['sha256'],'Archive overlay source bytes changed')
    return raw


def exclusive(path,value):
    path=ordinary(path,fresh=True);raw=(json.dumps(value,sort_keys=True,allow_nan=False)+'\n').encode()
    require(len(raw)<=MAX_METADATA,'Finite archive overlay evidence exceeded')
    with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def public_native_join():
    """Compile only the exact qualified operational function, no mixed graph."""
    import horsehair
    from horsehair import repair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE
        and source_identity(VALIDATION_FRAMEWORK)['sha256']==VALIDATION_SOURCE,
        'Separate unchanged qualified4f and new validation releases required')
    path=VALIDATION_FRAMEWORK/'src/horsehair/agent_analysis_v2.py';raw=path.read_bytes()
    require(hashlib.sha256(raw).hexdigest()==VALIDATION_MODULE_SHA,'Qualified native-join public source changed')
    dependencies={}
    for name,module in [('core',core),('slurm',slurm),('repair',repair)]:
        actual=FRAMEWORK/('src/horsehair/'+name+'.py');other=VALIDATION_FRAMEWORK/('src/horsehair/'+name+'.py')
        require(Path(module.__file__).resolve()==actual and digest(actual)==digest(other),
            'Executed native-join dependency differs from the newly qualified source: '+name)
        dependencies[name]={'path':str(actual),'sha256':digest(actual)}
    tree=ast.parse(raw);names={'_require','_owned_completed_cpu_qualification'}
    definitions=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
    require(len(definitions)==2 and {n.name for n in definitions}==names and all(not n.decorator_list for n in definitions),
        'Exact undecorated public native-join functions required')
    constants=[n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name)
        and t.id=='ARCHIVE_NATIVE_REPAIR_SHA256' for t in n.targets)]
    require(len(constants)==1 and ast.literal_eval(constants[0].value)==dependencies['repair']['sha256'],
        'Exact public strict-native parser dependency binding required')
    namespace={'__name__':'horsehair.private_source_bound_native_join','__package__':'horsehair',
        'Path':Path,'re':re,'math':math,'time':time,'subprocess':subprocess,'digest':digest,
        'completed_native_dependency':slurm.completed_native_dependency,
        'ARCHIVE_NATIVE_REPAIR_SHA256':dependencies['repair']['sha256']}
    exec(compile(ast.Module(body=definitions,type_ignores=[]),str(path),'exec'),namespace)
    function=namespace['_owned_completed_cpu_qualification']
    proof={'public_module':{'path':str(path),'sha256':VALIDATION_MODULE_SHA},
        'function_ast_sha256':canonical_hash([ast.dump(n,include_attributes=False) for n in definitions]),
        'dependencies':dependencies,'execution_package':str(FRAMEWORK/'src/horsehair'),
        'mixed_package_import':False,'qualification_source_sha256':VALIDATION_SOURCE}
    def joined(job,*,deadline=None):
        for item in dependencies.values():require(digest(Path(item['path']))==item['sha256'],'Native-join dependency changed')
        require(digest(path)==VALIDATION_MODULE_SHA,'Native-join source changed before query')
        value=function(ROOT,job,deadline=deadline)
        for item in dependencies.values():require(digest(Path(item['path']))==item['sha256'],'Native-join dependency changed during query')
        require(digest(path)==VALIDATION_MODULE_SHA,'Native-join source changed during query');return value
    return joined,proof

def exact_resources(value):
    if type(value) is not dict:return False
    expected={'cpus':4,'gpu':False,'hours':1,'memory':'16G'}
    return (value==expected or value=={**expected,'array':None}) and type(value.get('cpus')) is int \
        and type(value.get('gpu')) is bool and type(value.get('hours')) in (int,float)

def exact_command(command,helper,expected):
    """One reviewed Python invocation, no shell prefix or unknown arguments."""
    if type(command) is not str:return False
    try:tokens=shlex.split(command)
    except ValueError:return False
    if not tokens or tokens[0] not in {'python',sys.executable}:return False
    position=1
    if len(tokens)>position and tokens[position] in {'-u','-s'}:position+=1
    if len(tokens)<=position or tokens[position]!=str(helper):return False
    tail=tokens[position+1:]
    if len(tail)!=2*len(expected):return False
    keys=tail[::2]
    return len(set(keys))==len(keys) and dict(zip(keys,tail[1::2]))==expected

def _qualified_metadata(base,binding):
    require(type(binding) is dict and set(binding)=={'path','sha256','job_id','producer'}
        and type(binding['job_id']) is str and re.fullmatch('[1-9][0-9]*',binding['job_id']),
        'Explicit completed private overlay qualification required')
    q=strict(read_bound({k:binding[k] for k in ('path','sha256')}))
    require(TEST_SUITES and q.get('status')=='complete_f2_archive_overlay_qualification'
        and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('source_sha256')==SOURCE and q.get('slurm_job_id')==binding['job_id']
        and q.get('config_sha256')==CONFIG and q.get('source_verified_unchanged') is True
        and q.get('wrapper_sha256')==digest(Path(__file__)) and q.get('base_controller_sha256')==BASE_SHA
        and q.get('verifier_sha256')==VERIFIER_SHA and q.get('consumer_qualification')==CONSUMER_Q
        and q.get('qualification_driver')==binding['producer']
        and q.get('resource_policy',{}).get('path')==str(POLICY) and q['resource_policy'].get('sha256')==POLICY_SHA
        and q.get('scientific_result') is False and q.get('admission_authority') is False,
        'Full exact private archive overlay qualification is missing')
    require(type(q.get('suites')) is dict and set(q['suites'])==set(TEST_SUITES),'Complete private overlay suite set required')
    from horsehair.cli import _qualification_junit
    for name,expected in TEST_SUITES.items():
        suite=q['suites'][name];require(set(suite)=={'tests','test_source','junit'},'Exact private suite proof required')
        source=suite['test_source'];require(Path(source['path']).name==name and source['sha256']==expected['sha256'],
            'Private overlay test source identity differs');read_bound(source)
        counts={'tests':expected['tests'],'failures':0,'errors':0,'skipped':0}
        require(suite['tests']==counts and all(type(v) is int for v in suite['tests'].values()),'Full private overlay tests must pass')
        raw=read_bound(suite['junit'],8_000_000)
        require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Plain bounded private JUnit required')
        cases=list(ET.fromstring(raw).iter('testcase'))
        roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
        require(len(cases)==expected['tests'] and len(set(roster))==len(roster) and all(a and b for a,b in roster)
            and canonical_hash(roster)==expected['roster_sha256']
            and not any(list(c.iter(k)) for c in cases for k in ('failure','error','skipped'))
            and _qualification_junit(Path(suite['junit']['path']))==counts,'Actual full private overlay JUnit roster differs')
    private_producer_registration(q,binding)
    return q


def private_producer_registration(q,binding):
    """Recheck immutable producer inputs and the exact registered invocation."""
    read_bound(binding['producer'])
    registration=q['native_registration'];inputs=q['qualification_inputs'];job=binding['job_id']
    require(type(registration) is dict and set(registration)=={'plan','ledger','script','task_name','native_job_id'}
        and registration['native_job_id']==job and type(inputs) is dict
        and set(inputs)=={'overlay','verifier','base_controller','overlay_tests','verifier_tests','fixture_root'}
        and inputs['overlay']==locator(Path(__file__)) and inputs['verifier']['sha256']==VERIFIER_SHA
        and inputs['base_controller']=={'path':str(BASE_PATH),'sha256':BASE_SHA}
        and inputs['overlay_tests']==q['suites'][OVERLAY_TEST]['test_source']
        and inputs['verifier_tests']==q['suites'][VERIFIER_TEST]['test_source']
        and inputs['fixture_root']==str(CONSUMER_FRAMEWORK),
        'Exact full private overlay qualification inputs/registered producer required')
    for key in ('overlay','verifier','base_controller','overlay_tests','verifier_tests'):read_bound(inputs[key])
    for key,namespace in [('plan',ROOT/'plans'),('ledger',ROOT/'state'),('script',ROOT/'plans')]:
        read_bound(registration[key],namespace=namespace)
    require(digest(DRIVER)==DRIVER_SHA,'Frozen qualification registration validator changed')
    pair=runpy.run_path(str(DRIVER),run_name='private_archive_qualification_registration_only')
    config=read_json(PRIMARY/'configs/study.json')
    plan,ledger=pair['checked_pair'](Path(registration['plan']['path']),Path(registration['ledger']['path']),ROOT,config)
    tasks=[t for t in plan['tasks'] if t['name']==registration['task_name']]
    require(len(tasks)==1,'One exact private archive qualification task required')
    task=tasks[0];record=ledger.get('tasks',{}).get(registration['task_name'],{})
    expected={'--overlay':inputs['overlay']['path'],'--overlay-sha256':inputs['overlay']['sha256'],
        '--verifier':inputs['verifier']['path'],'--verifier-sha256':inputs['verifier']['sha256'],
        '--base-controller':inputs['base_controller']['path'],'--base-controller-sha256':BASE_SHA,
        '--overlay-tests':inputs['overlay_tests']['path'],'--verifier-tests':inputs['verifier_tests']['path'],
        '--qualification-case':str(Path(binding['path']).parent),
        '--native-plan':registration['plan']['path'],'--native-ledger':registration['ledger']['path'],
        '--native-script':registration['script']['path'],'--native-task':registration['task_name']}
    script=Path(registration['script']['path']);text=script.read_text()
    require(exact_command(task['command'],binding['producer']['path'],expected)
        and plan.get('release_identity',{}).get('sha256')==SOURCE and task.get('script')==str(script)
        and canonical_hash(text)==task.get('script_sha256')==record.get('script_sha256')
        and task['command'] in text and record.get('job_id')==job and record.get('state')=='submitted'
        and exact_resources(task.get('resources')),'Actual private qualification command/script/source/allocation differs')
    row=q['producer_allocation'];match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',row.get('TimeLimit',''))
    require(match is not None,'Finite original qualification native allocation required')
    day,hour,minute,second=(int(v or 0) for v in match.groups())
    require(row.get('JobId')==job and row.get('UserId')=='pwa209(3164768)' and row.get('Account')=='def-ptewarie_cpu'
        and row.get('JobState')=='RUNNING' and row.get('NumCPUs')=='4' and row.get('NumNodes') in {'1','1-1'}
        and row.get('MinMemoryNode') in {'16G','16384M'} and day*86400+hour*3600+minute*60+second==3600
        and row.get('Command')==str(script) and row.get('JobName')=='hhw-'+registration['task_name'],
        'Producer original owner/command/job name does not bind its completed native job')
    for name in ('AllocTRES','ReqTRES'):
        fields=row.get(name,'').split(',');resources={}
        for field in fields:
            key,separator,value=field.partition('=')
            require(separator and key and key not in resources,'Explicit unique original producer TRES required')
            resources[key]=value
        require(resources.get('cpu')=='4' and not any('gpu' in key.lower() for key in resources),
            'Producer original TRES does not prove its scalar CPU allocation')
    require(all('gpu' not in row.get(key,'').lower() for key in ('TresPerNode','TresPerJob','TresPerTask','Gres')),
        'Original CPU qualification may not advertise a GPU')
    return registration


class QualificationObservation(NamedTuple):
    root:str
    job_id:str
    qualification_json:bytes
    joined_json:bytes
    source_json:bytes


def private_qualification(base,binding,*,observation):
    """Immutable/source/selected-cost rechecks only; no scheduler read here."""
    q=_qualified_metadata(base,binding)
    require(type(observation) is QualificationObservation and observation.root==str(ROOT)
        and observation.job_id==binding['job_id'] and strict(observation.qualification_json)==binding,
        'Fresh outside-lock qualification observation for this exact descriptor required')
    joined=strict(observation.joined_json);source=strict(observation.source_json)
    require(source['qualification_source_sha256']==CONSUMER_SOURCE and source['mixed_package_import'] is False
        and source['public_module']=={'path':str(CONSUMER_FRAMEWORK/'src/horsehair/agent_analysis_v2.py'),
            'sha256':VALIDATION_MODULE_SHA},'Exact fresh source-bound native owner observation required')
    for item in [source['public_module'],*source['dependencies'].values()]:
        require(digest(Path(item['path']))==item['sha256'],'Native owner-join source dependency changed')
    proof=base.slurm.completed_native_dependency(ROOT,binding['job_id'])
    require(proof is not None and proof['native_records_sha256']==joined['completion']['native_records_sha256'],
        'Selected positive qualification allocation changed after its outside-lock owner read')
    return q


def refresh_qualification(base,c):
    """Capture one fresh completed owner/cost join before a complete context."""
    binding=c['descriptor']['archive_overlay']['qualification'];_qualified_metadata(base,binding)
    join,source=public_native_join()
    remaining=c['deadline']-time.time()-300.
    require(math.isfinite(remaining) and remaining>0,'Native owner observation margin exhausted')
    joined=join(binding['job_id'],deadline=time.monotonic()+min(60.,remaining))
    encode=lambda value:json.dumps(value,sort_keys=True,allow_nan=False).encode()
    observation=QualificationObservation(str(ROOT),binding['job_id'],encode(binding),encode(joined),encode(source))
    private_qualification(base,binding,observation=observation)
    c['archive_qualification_sequence']=c.get('archive_qualification_sequence',0)+1
    path=c['area']/('archive-native-qualification-'+str(c['archive_qualification_sequence']).zfill(7)+'.json')
    exclusive(path,{'qualification':binding,'joined':joined,'source':source,'registration_mutex_held':False,
        'scientific_result':False,'admission_authority':False})
    c['archive_qualification_observation']=observation
    return observation


def overlay_binding(base,descriptor,*,observation):
    value=descriptor.get('archive_overlay')
    require(type(value) is dict and set(value)=={'wrapper','verifier','registry','qualification','consumer_qualification'},
        'Exact explicit archive overlay descriptor required')
    require(value['wrapper']==locator(Path(__file__)) and value['verifier']['sha256']==VERIFIER_SHA,
        'Pinned separate archive controller/verifier required')
    read_bound(value['wrapper']);read_bound(value['verifier'])
    require(value['consumer_qualification']==CONSUMER_Q,'Exact wholef047 consumer qualification required')
    read_bound({k:CONSUMER_Q[k] for k in ('path','sha256')})
    registry=strict(read_bound(value['registry']))
    require(type(registry) is dict and registry.get('kind')=='final_policy_condition_archive_registry_v1'
        and registry.get('root')==str(ROOT) and type(registry.get('cells')) is dict and registry['cells']
        and all(type(k) is str and re.fullmatch('0|[1-9][0-9]*',k) and 0<=int(k)<240 for k in registry['cells']),
        'Nonempty immutable complete-policy archive registry required')
    private_qualification(base,value['qualification'],observation=observation)
    return {'binding':value,'registry':registry}


def observations(registry):
    """Retain ordinary-file identity around the child's complete hash reads."""
    paths=set()
    def walk(value):
        if type(value) is dict:
            if set(value)=={'path','sha256'}:paths.add(value['path'])
            else:
                for item in value.values():walk(item)
        elif type(value) is list:
            for item in value:walk(item)
    walk(registry)
    for descriptor in registry['cells'].values():
        directory=Path(descriptor['archive_directory'])
        paths.update(map(str,(directory/'evidence.tar',directory/'index.json')))
        plan=strict(read_bound(descriptor['policy_inputs']['plan']))
        summary=strict(read_bound(descriptor['policy_inputs']['summary']))
        paths.add(plan['checkpoint']);paths.add(str(Path(descriptor['evaluation_case'])/summary['artifact_name']))
    result={}
    for value in sorted(paths):
        path=Path(value)
        require(path.is_absolute() and path==path.resolve() and path.is_relative_to(ROOT) and path!=ROOT
            and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(ROOT)),
            'Archive input observation must remain confined and ordinary')
        row=path.stat();require(stat.S_ISREG(row.st_mode),'Only regular archive/canonical evidence may be observed')
        result[value]=[row.st_dev,row.st_ino,row.st_mode,row.st_nlink,row.st_size,row.st_mtime_ns,row.st_ctime_ns]
    return result


def selected_native_rows(jobs):
    """Read only final cost rows, never scheduler metadata or a whole-ledger SHA."""
    require(type(jobs) in (set,list,tuple) and 0<len(jobs)<=500
        and all(type(job) is str and re.fullmatch('[1-9][0-9]*',job) for job in jobs),
        'Finite exact selected native authority roster required')
    path=ordinary(ROOT/'state/resource-budget.json',namespace=ROOT/'state')
    require(path.stat().st_size<=MAX_METADATA,'Finite current native cost ledger exceeded')
    # This file is atomically republished by unrelated accounting. Read one
    # bounded open-file snapshot; only the selected immutable rows are bound.
    with path.open('rb') as stream:raw=stream.read(MAX_METADATA+1)
    require(len(raw)<=MAX_METADATA,'Finite current native cost ledger exceeded')
    ledger=strict(raw)
    require(type(ledger) is dict and type(ledger.get('native_jobs')) is dict,'Current native cost ledger required')
    result={}
    for job in sorted(jobs):
        row=ledger['native_jobs'].get(job)
        require(type(row) is dict and row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0'
            and row.get('job_id_raw')==job and row.get('job_id')==job
            and row.get('basis')=='sacct final allocated wall time'
            and type(row.get('cpus')) is int and row['cpus']>0
            and type(row.get('elapsed_seconds')) is int and row['elapsed_seconds']>0
            and type(row.get('gpus')) is int and 0<=row['gpus']<=1,
            'Positive exact scalar selected native allocation required')
        result[job]=row
    return result


def selected_native_projection(registry):
    """Bind every selected child authority before its complete outside-lock read."""
    sources={};expected={};jobs=set()
    def bound(binding,*,decode=True):
        raw=read_bound(binding,namespace=ROOT);path=binding['path']
        require(path not in sources or sources[path]==binding,'Conflicting native authority source locators')
        sources[path]=binding
        require(len(sources)<=2000,'Finite native authority source roster exceeded')
        return strict(raw) if decode else raw
    def native(binding):
        require(type(binding) is dict and set(binding)=={'completion','owner'},'Exact selected native authority binding required')
        proof=bound(binding['completion']);bound(binding['owner'])
        job=proof.get('job_id');records=proof.get('native_records')
        require(type(job) is str and re.fullmatch('[1-9][0-9]*',job) and type(records) is dict and set(records)=={job}
            and proof.get('array') is None and proof.get('source_path')==str(ROOT/'state/resource-budget.json')
            and proof.get('native_records_sha256')==canonical_hash(records)
            and proof.get('completion_proof_sha256')==canonical_hash({k:v for k,v in proof.items() if k!='completion_proof_sha256'}),
            'Exact source-bound selected native completion proof required')
        require(job not in expected or expected[job]==records[job],'Conflicting selected native authority rows')
        expected[job]=records[job];jobs.add(job)
    def qualification(binding,job):
        value=bound(binding)
        require(value.get('slurm_job_id')==job,'Selected native framework qualification identity changed')
        artifact=value.get('artifacts',{}).get('junit')
        require(type(artifact) is dict and 'path' in artifact and 'sha256' in artifact,'Selected native framework JUnit source required')
        bound({k:artifact[k] for k in ('path','sha256')},decode=False);jobs.add(job)
    canary=bound(registry['archive_canary']);native(canary['native'])
    for key in ('test_source','junit'):bound(canary[key],decode=False)
    for descriptor in registry['cells'].values():
        index=bound({'path':str(Path(descriptor['archive_directory'])/'index.json'),'sha256':descriptor['index_sha256']})
        native(index['authority']['native']);native(descriptor['pack_producer']['native'])
        qualification(descriptor['pack_producer']['qualification'],'6684484')
    qualification({k:CONSUMER_Q[k] for k in ('path','sha256')},CONSUMER_Q['job_id'])
    rows=selected_native_rows(jobs)
    require(all(rows[job]==row for job,row in expected.items()),'Selected archive authority differs from current native cost row')
    for job in ('6684484',CONSUMER_Q['job_id']):
        require(rows[job]['cpus']==4 and rows[job]['gpus']==0,'CPU-only framework qualification allocation changed')
    return {'ledger_path':str(ROOT/'state/resource-budget.json'),'rows':rows,'rows_sha256':canonical_hash(rows),
        'sources':[sources[path] for path in sorted(sources)]}


def recheck_selected_native_projection(projection):
    """Local bounded metadata/selected-row recheck, valid inside the mutex."""
    require(type(projection) is dict and set(projection)=={'ledger_path','rows','rows_sha256','sources'}
        and projection['ledger_path']==str(ROOT/'state/resource-budget.json') and type(projection['rows']) is dict
        and canonical_hash(projection['rows'])==projection['rows_sha256']
        and type(projection['sources']) is list and 0<len(projection['sources'])<=2000,
        'Exact complete selected native authority projection required')
    for binding in projection['sources']:read_bound(binding,namespace=ROOT)
    require(selected_native_rows(set(projection['rows']))==projection['rows'],
        'Selected archive native authority changed after the outside-lock child read')


def prepare_view(base,c,*,runner=subprocess.run,clock=time.time):
    """This slow child is called before base.tick enters registration_guard."""
    extra=overlay_binding(base,c['descriptor'],observation=c.get('archive_qualification_observation'));binding=extra['binding']
    before=observations(extra['registry'])
    state_path=c['case']/'state.json.gz';plan_path=c['case']/'campaign-plan.json'
    state_binding_path=c['case']/'state-binding.json';state_binding_raw=read_bound(locator(state_binding_path))
    state_binding=strict(state_binding_raw);state_locator=locator(state_path,maximum=MAX_STATE)
    require(state_locator['sha256']==state_binding.get('state_file_sha256'),'State gzip/binding differs before archive read')
    plan_locator=locator(plan_path);plan=strict(read_bound(plan_locator))
    require(plan.get('plan_sha256')==state_binding.get('campaign_plan_sha256'),'Original campaign plan/state binding differs')
    c['archive_sequence']=c.get('archive_sequence',0)+1
    area=c['area']/('archive-read-'+str(c['archive_sequence']).zfill(7));ordinary(area,fresh=True);area.mkdir(mode=0o700)
    request={'schema_version':1,'kind':'fresh_complete_archive_controller_verification_v1','root':str(ROOT),
        'consumer_qualification':binding['consumer_qualification'],'campaign_plan':plan_locator,'state':state_locator,
        'state_sha256':state_binding['state_sha256'],'archive_registry':binding['registry']}
    request_path=area/'request.json';exclusive(request_path,request);request_locator=locator(request_path)
    native_projection=selected_native_projection(extra['registry'])
    projection_path=area/'selected-native-authorities.json';exclusive(projection_path,native_projection)
    projection_locator=locator(projection_path)
    maximum=min(900.,c['deadline']-clock()-600.)
    require(math.isfinite(maximum) and maximum>=1,'Insufficient native margin for a fresh archive read')
    command=[sys.executable,'-s',binding['verifier']['path'],'--request',str(request_path),
        '--request-sha256',request_locator['sha256'],'--output',str(area/'proof.json'),'--max-seconds',str(int(maximum))]
    environment=_primary_environment({'primary_release':str(CONSUMER_FRAMEWORK),
        'maintenance_release':str(c['framework']),'root':str(ROOT)})
    for key in ('PYTHONHOME','PYTHONSTARTUP','PYTHONINSPECT'):environment.pop(key,None)
    environment['TZ']='UTC'
    exclusive(area/'intent.json',{'command':command,'request':request_locator,'verifier':binding['verifier'],
        'consumer_qualification':CONSUMER_Q,'maximum_child_seconds':maximum,
        'registration_mutex_held':False,'scientific_result':False,'admission_authority':False})
    with (area/'stdout.txt').open('xb') as stdout,(area/'stderr.txt').open('xb') as stderr:
        response=runner(command,cwd=CONSUMER_FRAMEWORK,stdout=stdout,stderr=stderr,stdin=subprocess.DEVNULL,
            timeout=maximum,check=False,env=environment)
    require(type(response.returncode) is int and response.returncode==0,'Wholef047 archive child failed; original-file fallback is forbidden')
    proof_locator=locator(area/'proof.json');proof=strict(read_bound(proof_locator))
    require(proof.get('status')=='complete_fresh300_packet_archive_verification'
        and proof.get('private_helper_sha256')==VERIFIER_SHA and proof.get('request')==request_locator
        and proof.get('state_file_sha256')==state_locator['sha256'] and proof.get('state_sha256')==state_binding['state_sha256']
        and proof.get('campaign_plan_sha256')==plan['plan_sha256'] and proof.get('archive_registry')==binding['registry']
        and proof.get('consumer',{}).get('source_sha256')==CONSUMER_SOURCE
        and proof['consumer'].get('qualification')==CONSUMER_Q
        and proof.get('raw_condition_packets_changed') is False and proof.get('endpoint_values_recomputed') is False
        and proof.get('scientific_result') is False and proof.get('admission_authority') is False,
        'Archive child proof does not bind the exact live state/request/source')
    require(read_bound(locator(state_binding_path))==state_binding_raw and locator(state_path,maximum=MAX_STATE)==state_locator
        and locator(plan_path)==plan_locator,'State changed while outside-lock archive verification ran')
    overlay_binding(base,c['descriptor'],observation=c.get('archive_qualification_observation'))
    require(observations(extra['registry'])==before,'Archive/canonical input changed around the complete child read')
    recheck_selected_native_projection(native_projection)
    c['prepared_archive_view']={'proof':proof,'proof_locator':proof_locator,'state_locator':state_locator,
        'state_binding_sha256':hashlib.sha256(state_binding_raw).hexdigest(),'plan_locator':plan_locator,
        'registry':extra['registry'],'binding':binding,'observations':before,'native_projection':native_projection,
        'native_projection_locator':projection_locator}
    return c['prepared_archive_view']


def snapshot_map(c,plan,state):
    prepared=c.get('prepared_archive_view');require(prepared is not None,'Fresh outside-lock archive proof required')
    proof=prepared['proof'];require(strict(read_bound(prepared['proof_locator']))==proof,'Fresh archive proof bytes changed')
    require(state['state_sha256']==proof['state_sha256'] and plan['plan_sha256']==proof['campaign_plan_sha256']
        and locator(c['case']/'state.json.gz',maximum=MAX_STATE)==prepared['state_locator']
        and locator(c['case']/'campaign-plan.json')==prepared['plan_locator']
        and locator(c['case']/'state-binding.json')['sha256']==prepared['state_binding_sha256'],
        'State changed before archived packets entered the locked tick')
    require(c['descriptor']['archive_overlay']==prepared['binding'],'Archive descriptor changed before packet collection')
    registry=strict(read_bound(prepared['binding']['registry']))
    require(registry==prepared['registry'],'Immutable archive registry changed')
    require(observations(registry)==prepared['observations'],'Archive/canonical inputs changed before the locked packet view')
    require(strict(read_bound(prepared['native_projection_locator']))==prepared['native_projection'],
        'Selected archive authority projection evidence changed')
    recheck_selected_native_projection(prepared['native_projection'])
    rows=proof.get('cells');require(type(rows) is list and len(rows)==len(registry['cells']),
        'Every listed archive requires one complete fresh proof row')
    by_index={row['cell_index']:row for row in rows}
    require(len(by_index)==len(rows) and set(by_index)=={int(k) for k in registry['cells']},'Archive proof roster differs')
    result={}
    for key,descriptor in registry['cells'].items():
        index=int(key);cell=state['cells'][index];row=by_index[index]
        require(type(row['cell_index']) is int and cell['index']==index and cell['complete'] is True
            and cell['assembly_job'] is not None and set(cell['conditions'])=={str(i) for i in range(300)},
            'Only full300 positively assembled state cells may use an archive')
        packets=[cell['conditions'][str(i)] for i in range(300)]
        require(row.get('condition_packets')==300 and type(row['condition_packets']) is int
            and row.get('packet_roster_sha256')==canonical_hash(packets)
            and row.get('index_sha256')==descriptor['index_sha256'] and row.get('archive_sha256')==descriptor['archive_sha256']
            and row.get('assembly_job_id')==state['jobs'][cell['assembly_job']]['job_id']
            and row.get('all_current_archived_bytes_reverified') is True
            and cell['evaluation_plan']['case']==descriptor['evaluation_case'],
            'Archived packets or original assembly differ from the fresh proof')
        require(descriptor['evaluation_case'] not in result,'Duplicate archived evaluation case refused')
        result[descriptor['evaluation_case']]=packets
    return result


def install(base):
    require(digest(Path(base.__file__))==BASE_SHA and Path(base.__file__).name==BASE_PATH.name,
        'Exact separate frozen registry-v6 controller library required')
    original_guard=base.source_guard;original_prepare=base.prepare_census;original_collect=base.collect_packets
    original_startup=base.startup_admission
    def source_guard(c):
        original_guard(c);overlay_binding(base,c['descriptor'],observation=c.get('archive_qualification_observation'))
    def startup_admission(c,**kwargs):
        refresh_qualification(base,c)
        return original_startup(c,**kwargs)
    def prepare_census(c):
        value=original_prepare(c);refresh_qualification(base,c);prepare_view(base,c);return value
    def collect_packets(c,plan,state,native):
        snapshots=snapshot_map(c,plan,state);original=base.condition_snapshot
        def condition_snapshot(case,ep):
            return snapshots[str(case)] if str(case) in snapshots else original(case,ep)
        # This is only this private library instance, under its registration
        # mutex. No public module, runpy API or global scheduler is replaced.
        base.condition_snapshot=condition_snapshot
        try:
            result=original_collect(c,plan,state,native)
            require(observations(c['prepared_archive_view']['registry'])==c['prepared_archive_view']['observations'],
                'Archive/canonical evidence changed during historical packet collection')
            recheck_selected_native_projection(c['prepared_archive_view']['native_projection'])
            return result
        finally:base.condition_snapshot=original
    base.source_guard=source_guard;base.prepare_census=prepare_census;base.collect_packets=collect_packets
    base.startup_admission=startup_admission
    return base


def main():
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--descriptor',type=Path,required=True);parser.add_argument('--descriptor-sha256',required=True)
    parser.add_argument('--segment',type=int,required=True);a=parser.parse_args()
    require(digest(BASE_PATH)==BASE_SHA,'Frozen base controller bytes changed')
    spec=importlib.util.spec_from_file_location('hhw_frozen_registry_v6_archive_controller',BASE_PATH)
    base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base);install(base)
    c=base.load_context(a.descriptor,a.descriptor_sha256,a.segment)
    return base.run(c)


if __name__=='__main__':main()
