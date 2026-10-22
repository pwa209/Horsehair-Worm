"""Originals-retaining full-policy packs and positive-producer finalization.

Prospective private operations only. No scheduler mutation, source-file unlink,
controller-descriptor publication, science calculation or future inode credit.
The pack framework remains qualified4f; the exact corrected native ownership
function is compiled alone from the separately qualified new public source.
"""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import ast
import gzip
import hashlib
import io
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

from horsehair import core,slurm,agent_campaign as dispatch
from horsehair.core import canonical_hash,digest,source_identity,read_json,native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
VALIDATION_FRAMEWORK=ROOT/'releases/20261021-e104ce63fa53-f047ce12b0a4'
VALIDATION_SOURCE='f047ce12b0a4a31312f2b4f5b6cff144342a12950f2e84fe1d444c8ea3a9b68c'
VALIDATION_MODULE_SHA='507fbe01700e4310a4e44b16f178d924fe291585d20646830e97b7a28e13fd6c'
VALIDATION_Q={'path':str(ROOT/'maintenance/study-af-execution-support-v4-20261021/qualification.json'),
    'sha256':'8c3e1910deabea6c1061ddb353c3a6881fb70fb9fe86948007557a03ca809176','job_id':'6703313'}
FRAMEWORK_Q={'path':str(ROOT/'maintenance/study-af-execution-support-v1-20261021/qualification.json'),
    'sha256':'29ed530f3783f3c35c3cf20343bdb663732ec2bf4bf5c3218f8b029510becc2f','job_id':'6684484'}
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
ARCHIVE_SHA='045d8650db62c0fa52d7f56bb7bdb2669729509c23ecfbbc847e73cbbff8d42a'
CANARY={'path':str(ROOT/'maintenance/study-af-evidence-archive-canary-v1-20261021/finalized-native-proof/canary.json'),
    'sha256':'921635cbeac2b954c7946c7a6ddd3c375a8ecd862630492d1aa0ee26b58ac14b'}
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'
QUOTA_SHA='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045'
REGISTRY_GUARD=ROOT/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'
REGISTRY_GUARD_SHA='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'
DRIVER=ROOT/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'
DRIVER_SHA='49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'
PRIVATE_TESTS={'test_f2_policy_archive_pack_v1_20261021.py':{
    'sha256':'25a4c507d4f2b23a70ef496519fcfafd643f986277d11224cb38413edbe93225','tests':103,
    'roster_sha256':'dff8b217135ec1d0957baebf26e1ee345abc168cee164c10a6d6f93aea49e87d'}}
MAX_METADATA=10_000_000
MAX_STATE=512_000_000
MAX_BATCH=8
FLAGS={'scientific_result':False,'admission_authority':False,'original_removal_authorized':False}


def require(value,message):
    if not value:raise ValueError(message)


def strict(raw):
    def pairs(values):
        result={}
        for key,value in values:
            require(key not in result,'Duplicate pack metadata key refused');result[key]=value
        return result
    def invalid(value):raise ValueError('Nonfinite pack metadata refused')
    def floating(text):
        value=float(text);require(math.isfinite(value),'Nonfinite pack metadata refused');return value
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid,parse_float=floating)


def ordinary(path,*,fresh=False,namespace=None):
    path=Path(path);namespace=ROOT if namespace is None else Path(namespace)
    require(path.is_absolute() and path==path.resolve() and path.is_relative_to(namespace) and path!=namespace
        and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(ROOT)),
        'Ordinary canonical personal path required')
    require(not path.exists() if fresh else path.is_file(),'Fresh immutable output required' if fresh else 'Required ordinary file absent')
    return path


def locator(path):
    path=ordinary(path);return {'path':str(path),'sha256':digest(path)}


def bound(binding,*,maximum=MAX_METADATA):
    require(type(binding) is dict and set(binding)=={'path','sha256'} and type(binding['sha256']) is str
        and re.fullmatch('[0-9a-f]{64}',binding['sha256']),'Exact source path/SHA required')
    path=ordinary(binding['path']);require(path.stat().st_size<=maximum,'Finite pack metadata exceeded')
    with path.open('rb') as stream:raw=stream.read(maximum+1)
    require(len(raw)<=maximum and hashlib.sha256(raw).hexdigest()==binding['sha256'],'Immutable pack source bytes changed')
    return raw


def exclusive(path,value):
    path=ordinary(path,fresh=True,namespace=ROOT/'maintenance')
    raw=(json.dumps(value,sort_keys=True,allow_nan=False)+'\n').encode();require(len(raw)<=MAX_METADATA,'Finite pack output exceeded')
    with path.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def load_archive(binding):
    require(binding.get('sha256')==ARCHIVE_SHA,'Only reviewed045d archive kernel is supported')
    raw=bound(binding);module=type(sys)('private_exact045d_pack_library');module.__file__=binding['path']
    exec(compile(raw,binding['path'],'exec'),module.__dict__)
    require(bound(binding)==raw,'Archive kernel changed while loading');return module


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


def qualification(binding,source,count,join,*,end):
    require(type(binding) is dict and set(binding)=={'path','sha256','job_id'} and type(binding['job_id']) is str
        and re.fullmatch('[1-9][0-9]*',binding['job_id']),'Exact immutable completed qualification locator required')
    q=strict(bound({k:binding[k] for k in ('path','sha256')}));counts={'tests':count,'failures':0,'errors':0,'skipped':0}
    require(q.get('status')=='complete' and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('source_sha256')==source and q.get('config_sha256')==CONFIG and q.get('source_verified_unchanged') is True
        and q.get('slurm_job_id')==binding['job_id'] and q.get('test_counts')==counts
        and all(type(v) is int for v in q['test_counts'].values()),'Exact complete unchanged native source qualification required')
    artifact=q.get('artifacts',{}).get('junit',{});raw=bound({k:artifact[k] for k in ('path','sha256')},maximum=8_000_000)
    require(len(raw)==artifact.get('bytes') and Path(artifact['path']).is_relative_to(ROOT/'state/qualification-evidence'),
        'Actual complete qualification JUnit bytes required')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(Path(artifact['path']))==counts,'Actual full qualification JUnit counts differ')
    return join(binding['job_id'],deadline=time.monotonic()+min(60.,end-time.time()-300.))


def private_qualification(binding,join,*,end):
    require(PRIVATE_TESTS and type(binding) is dict and set(binding)=={'path','sha256','job_id','producer'},
        'Separately qualified exact private pack wrapper required')
    q=strict(bound({k:binding[k] for k in ('path','sha256')}))
    require(q.get('status')=='complete_f2_policy_pack_operational_qualification_v1' and type(q.get('returncode')) is int
        and q['returncode']==0 and q.get('source_sha256')==SOURCE and q.get('config_sha256')==CONFIG
        and q.get('slurm_job_id')==binding['job_id'] and q.get('wrapper_sha256')==digest(Path(__file__))
        and q.get('archive_helper_sha256')==ARCHIVE_SHA and q.get('validation_source_sha256')==VALIDATION_SOURCE
        and q.get('validation_qualification')==VALIDATION_Q and q.get('scientific_result') is False
        and q.get('admission_authority') is False and set(q.get('suites',{}))==set(PRIVATE_TESTS)
        and q.get('qualification_driver')==binding['producer'],
        'Positive separate exact pack operational qualification required')
    bound(binding['producer'])
    from horsehair.cli import _qualification_junit
    for name,expected in PRIVATE_TESTS.items():
        item=q['suites'][name];require(set(item)=={'tests','test_source','junit'},'Exact private pack suite fields required')
        require(Path(item['test_source']['path']).name==name and item['test_source']['sha256']==expected['sha256'],
            'Private pack test source binding differs');bound(item['test_source'])
        counts={'tests':expected['tests'],'failures':0,'errors':0,'skipped':0}
        require(item['tests']==counts and all(type(v) is int for v in item['tests'].values()),'Every private pack test must pass')
        raw=bound(item['junit'],maximum=8_000_000)
        require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Plain private pack JUnit required')
        cases=list(ET.fromstring(raw).iter('testcase'));roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
        require(len(cases)==expected['tests'] and len(set(roster))==len(roster) and canonical_hash(roster)==expected['roster_sha256']
            and not any(list(c.iter(k)) for c in cases for k in ('failure','error','skipped'))
            and _qualification_junit(Path(item['junit']['path']))==counts,'Actual full private pack testcase roster differs')
    private_producer_registration(q,binding)
    return join(binding['job_id'],deadline=time.monotonic()+min(60.,end-time.time()-300.))


def private_producer_registration(q,binding):
    """Independently bind the qualified producer to its immutable native command."""
    registration=q['native_registration'];job=binding['job_id']
    require(set(registration)=={'plan','ledger','script','task_name','native_job_id'}
        and registration['native_job_id']==job and q.get('pack_driver')==locator(Path(__file__))
        and q.get('fixture_root')==str(VALIDATION_FRAMEWORK)
        and q.get('framework_qualification')==FRAMEWORK_Q,
        'Exact private qualification producer/source/native inputs required')
    for key in ('plan','ledger','script'):bound(registration[key])
    require(digest(DRIVER)==DRIVER_SHA,'Registered private qualification validator changed')
    pair=runpy.run_path(str(DRIVER),run_name='pack_qualification_registration_only')
    config=read_json(PRIMARY/'configs/study.json')
    plan,ledger=pair['checked_pair'](Path(registration['plan']['path']),Path(registration['ledger']['path']),ROOT,config)
    tasks=[t for t in plan['tasks'] if t['name']==registration['task_name']]
    require(len(tasks)==1,'One registered private qualification task required')
    task=tasks[0];record=ledger.get('tasks',{}).get(registration['task_name'],{})
    tests=list(q['suites'])
    require(len(tests)==1,'Exactly one complete private pack contract suite required')
    expected={'--pack-driver':q['pack_driver']['path'],'--pack-driver-sha256':q['pack_driver']['sha256'],
        '--test-source':q['suites'][tests[0]]['test_source']['path'],'--qualification-case':str(Path(binding['path']).parent),
        '--native-plan':registration['plan']['path'],'--native-ledger':registration['ledger']['path'],
        '--native-script':registration['script']['path'],'--native-task':registration['task_name']}
    require(exact_command(task['command'],binding['producer']['path'],expected),
        'Actual registered command differs from this exact private qualification producer')
    script=Path(registration['script']['path']);script_text=script.read_text()
    require(plan.get('release_identity',{}).get('sha256')==SOURCE and task.get('script')==str(script)
        and canonical_hash(script_text)==task.get('script_sha256')==record.get('script_sha256')
        and record.get('job_id')==job and record.get('state')=='submitted'
        and exact_resources(task.get('resources')) and task['command'] in script_text,
        'Private qualification must retain its exact source-bound native plan/script/allocation')
    return registration


def validate_request(request,mode):
    common={'schema_version','kind','root','case','archive_helper','archive_canary','operational_qualification',
        'campaign_plan','campaign_snapshot'}
    extra={'policies'} if mode=='pack' else {'pack_outputs','previous_registry'}
    require(mode in {'pack','finalize'} and type(request) is dict and set(request)==common|extra
        and type(request['schema_version']) is int and request['schema_version']==1
        and request['kind']=='originals_retaining_policy_archive_'+mode+'_v1' and request['root']==str(ROOT),
        'Exact prospective bounded policy-pack request required')
    case=ordinary(request['case'],fresh=True,namespace=ROOT/'maintenance')
    require(request['archive_canary']==CANARY and request['archive_helper'].get('sha256')==ARCHIVE_SHA,
        'Fixed positive39 native archive canary and reviewed kernel required')
    rows=request['policies'] if mode=='pack' else request['pack_outputs']
    require(type(rows) is list and 0<len(rows)<=MAX_BATCH,'One through eight complete policies per native job required')
    indexes=[]
    plan,state=campaign_inputs(request)
    for row in rows:
        require(type(row) is dict and set(row)==({'cell_index','evaluation_case','authority'} if mode=='pack' else {'cell_index','result'}),
            'Exact explicit policy case/authority or producer-result fields required')
        index=row['cell_index'];require(type(index) is int and 0<=index<240,'Exact original policy index required');indexes.append(index)
        if mode=='pack':
            source=Path(row['evaluation_case']);ordinary(source/'plan.json')
            require(source.name=='cell-'+str(index).zfill(4) and not case.is_relative_to(source),
                'Policy pack output must be separate from its exact original cell')
            authority=row['authority'];require(authority.get('primary_source_sha256')==PRIMARY_SHA
                and authority.get('full_config_sha256')==CONFIG and authority.get('execution_source_sha256')==SOURCE,
                'Original positive4f assembly authority must retain frozen science/config')
            eligible_policy(plan,state,index,row['evaluation_case'],authority)
        else:bound(row['result'])
    require(indexes==sorted(indexes) and len(set(indexes))==len(indexes),'Unique sorted fixed policy roster required')
    return case


def campaign_inputs(request):
    plan=strict(bound(request['campaign_plan']));raw=bound(request['campaign_snapshot'],maximum=MAX_STATE)
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:decoded=stream.read(MAX_STATE+1)
    require(len(decoded)<=MAX_STATE,'Finite immutable campaign state snapshot exceeded')
    state=strict(decoded)
    require(request['campaign_plan']['path']==str(Path(plan['case'])/'campaign-plan.json')
        and Path(request['campaign_snapshot']['path'])!=Path(plan['case'])/'state.json.gz'
        and canonical_hash(plan['config'])==CONFIG and plan['source_binding']['primary_source_sha256']==PRIMARY_SHA
        and plan['source_binding']['execution_source_sha256']==SOURCE
        and len(plan['cells'])==240 and [c['index'] for c in plan['cells']]==list(range(240)),
        'Exact preserved original240-cell campaign and separate immutable state copy required')
    dispatch._state(plan,state)
    return plan,state


def eligible_policy(plan,state,index,case,authority):
    cell=state['cells'][index]
    require(plan['cells'][index]['evaluation_case']==case and cell['index']==index and cell['complete'] is True
        and cell['assembly_job'] is not None and set(cell['conditions'])=={str(i) for i in range(300)}
        and state['jobs'][cell['assembly_job']]['status']=='complete',
        'Only an already assembled exact300-condition original campaign policy may be packed')
    job=state['jobs'][cell['assembly_job']]['job_id'];proof=strict(bound(authority['native']['completion']))
    require(proof.get('job_id')==job and cell['evaluation_plan']['case']==case
        and authority['policy_inputs']['plan']['sha256']==cell['evaluation_plan']['plan_sha256']
        and authority['policy_inputs']['binding']['sha256']==cell['evaluation_plan']['plan_binding_sha256'],
        'Original assembly job/evaluation activation differs from the captured complete campaign cell')
    return [cell['conditions'][str(i)] for i in range(300)]


def plan_packs(helper,request,*,checkpoint=lambda:None):
    plans=[]
    for item in request['policies']:
        checkpoint();plan=helper.plan_policy_pack(item['evaluation_case'],authority=item['authority'],root=ROOT,checkpoint=checkpoint)
        require(plan.get('status')=='planned_not_admitted' and len(plan.get('members',[]))==600
            and plan.get('helper_sha256')==ARCHIVE_SHA,'Only full600-member original rosters may be packed')
        plans.append({'cell_index':item['cell_index'],'plan':plan})
    return {'status':'planned_not_admitted','policies':plans,
        'additional_peak_files':2000+sum(x['plan']['maximum_additional_peak_regular_files'] for x in plans),
        'additional_peak_bytes':10_000_000_000+sum(x['plan']['maximum_additional_peak_bytes'] for x in plans),
        'future_storage_savings_credited':0,**FLAGS}


def execute_packs(helper,request,forecast,context,*,checkpoint=lambda:None):
    require(forecast.get('future_storage_savings_credited')==0 and len(forecast['policies'])==len(request['policies']),
        'No prospective savings or partial pack plan may authorize execution')
    rows=[];case=Path(request['case']);campaign,state=campaign_inputs(request)
    for item,planned in zip(request['policies'],forecast['policies']):
        require(item['cell_index']==planned['cell_index'],'Frozen pack roster order changed');checkpoint()
        directory=case/('policy-'+str(item['cell_index']).zfill(4))
        result=helper.pack_policy(item['evaluation_case'],directory,authority=item['authority'],root=ROOT,
            expected_roster_sha256=planned['plan']['roster_sha256'],checkpoint=checkpoint)
        require(result.get('status')=='verified_archive_originals_retained' and result.get('files')==600
            and result.get('original_removal_authorized') is False,'Full independent archive roundtrip with originals retained required')
        helper.verify_policy_archive(directory,result['index_sha256'],result['archive_sha256'],root=ROOT,checkpoint=checkpoint)
        expected=eligible_policy(campaign,state,item['cell_index'],item['evaluation_case'],item['authority'])
        actual=helper.committed_conditions(directory,result['index_sha256'],result['archive_sha256'],root=ROOT,checkpoint=checkpoint)
        require(actual==expected,'All300 archived raw SHA/value packets must equal the captured completed campaign state')
        paths={name:locator(directory/filename) for name,filename in [('pack_intent','pack-intent.json'),
            ('roundtrip','roundtrip.json'),('pack_receipt','pack-receipt.json')]}
        producer={'schema_version':1,'status':'complete_verified_policy_archive_pack','native_job_id':context['job_id'],
            'archive_helper_sha256':ARCHIVE_SHA,'evaluation_case':item['evaluation_case'],'archive_directory':str(directory),
            'index_sha256':result['index_sha256'],'archive_sha256':result['archive_sha256'],
            **{name+'_sha256':binding['sha256'] for name,binding in paths.items()},
            'execution_source_sha256':SOURCE,'full_config_sha256':CONFIG,'primary_scientific_source_sha256':PRIMARY_SHA,
            'scientific_result':False,'admission_authority':False,'original_removal_authorized':False,
            'cell_index':item['cell_index'],'request':context['request'],'native_registration':context['registration'],
            'campaign_plan':request['campaign_plan'],'campaign_snapshot':request['campaign_snapshot'],
            'condition_packet_roster_sha256':canonical_hash(actual),
            'private_pack_driver_sha256':digest(Path(__file__)),'private_pack_driver':locator(Path(__file__)),
            'resource_policy':context['policy'].receipt(),
            'validation_qualification':VALIDATION_Q,'operational_qualification':request['operational_qualification']}
        path=case/('pack-result-'+str(item['cell_index']).zfill(4)+'.json');exclusive(path,producer)
        rows.append({'cell_index':item['cell_index'],'result':locator(path),'native_producer_completion':'pending',
            'original_regular_files_removed':0,'future_storage_savings_credited':0})
    return rows


def finalized_registry(helper,request,context,join,*,checkpoint=lambda:None):
    """Create one immutable registry only after positive actual pack completion."""
    campaign,state=campaign_inputs(request)
    if request['previous_registry'] is None:
        registry={'schema_version':1,'kind':'final_policy_condition_archive_registry_v1','root':str(ROOT),
            'full_config_sha256':CONFIG,'primary_scientific_source_sha256':PRIMARY_SHA,
            'archive_helper':request['archive_helper'],'archive_canary':CANARY,'cells':{},
            'scientific_result':False,'admission_authority':False}
    else:
        previous=request['previous_registry']
        require(type(previous) is dict and set(previous)=={'registry','finalization_result','native'},
            'Previous registry needs its separate completed finalizer provenance')
        old_receipt=strict(bound(previous['finalization_result']));old_job,_=helper.verify_native(previous['native'],ROOT,completed=True)
        require(old_receipt.get('status')=='complete_positive_producer_registry_originals_retained'
            and old_receipt.get('mode')=='finalize' and old_receipt.get('native_job_id')==old_job
            and old_receipt.get('registry')==previous['registry'] and old_receipt.get('execution_source_sha256')==SOURCE
            and old_receipt.get('full_config_sha256')==CONFIG and old_receipt.get('primary_scientific_source_sha256')==PRIMARY_SHA
            and old_receipt.get('private_pack_driver_sha256')==digest(Path(__file__))
            and old_receipt.get('validation_qualification')==VALIDATION_Q
            and old_receipt.get('operational_qualification')==request['operational_qualification']
            and old_receipt.get('original_regular_files_removed')==0 and old_receipt.get('controller_descriptor_published') is False
            and old_receipt.get('scientific_result') is False and old_receipt.get('admission_authority') is False,
            'Previous registry must retain positive separate native finalization lineage')
        registry=strict(bound(previous['registry']))
        require(set(registry)=={'schema_version','kind','root','full_config_sha256','primary_scientific_source_sha256',
            'archive_helper','archive_canary','cells','scientific_result','admission_authority'} and registry['schema_version']==1
            and registry['kind']=='final_policy_condition_archive_registry_v1' and registry['root']==str(ROOT)
            and registry['full_config_sha256']==CONFIG and registry['primary_scientific_source_sha256']==PRIMARY_SHA
            and registry['archive_helper']==request['archive_helper'] and registry['archive_canary']==CANARY
            and registry['scientific_result'] is False and registry['admission_authority'] is False
            and type(registry['cells']) is dict and all(k in {str(i) for i in range(240)} for k in registry['cells']),
            'Exact preserved prior immutable archive registry required')
    producers={};case=Path(request['case']);rows=[]
    for item in request['pack_outputs']:
        checkpoint();key=str(item['cell_index']);require(key not in registry['cells'],'Existing archived policy cannot be overwritten')
        result=strict(bound(item['result']));job=result.get('native_job_id')
        require(type(job) is str and re.fullmatch('[1-9][0-9]*',job) and job!=context['job_id'],
            'Only a separate positively completed scalar pack producer may be finalized')
        if job not in producers:
            joined=join(job,deadline=time.monotonic()+min(60.,context['end']-time.time()-300.))
            proof=joined['completion'];owner=joined['native'];cost=proof['native_records'][job]
            owner_value={'job_id':job,'user':owner['user'],'account':owner['account'],
                **{k:cost[k] for k in ('state','exit_code','cpus','gpus','elapsed_seconds')}}
            completion_path=case/('pack-'+job+'-completion.json');owner_path=case/('pack-'+job+'-owner.json')
            exclusive(completion_path,proof);exclusive(owner_path,owner_value)
            producers[job]={'completion':locator(completion_path),'owner':locator(owner_path)}
        require(result.get('schema_version')==1 and result.get('status')=='complete_verified_policy_archive_pack'
            and result.get('private_pack_driver_sha256')==digest(Path(__file__)) and result.get('cell_index')==item['cell_index']
            and result.get('archive_helper_sha256')==ARCHIVE_SHA and result.get('execution_source_sha256')==SOURCE
            and result.get('full_config_sha256')==CONFIG and result.get('primary_scientific_source_sha256')==PRIMARY_SHA
            and result.get('scientific_result') is False and result.get('admission_authority') is False
            and result.get('original_removal_authorized') is False and result.get('validation_qualification')==VALIDATION_Q
            and result.get('operational_qualification')==request['operational_qualification'],
            'Exact current private pack source/request/qualification producer required')
        registration=result['native_registration']
        for k in ('plan','ledger','script'):bound(registration[k])
        require(registration['native_job_id']==job,'Pack producer native registration differs')
        pack_request=strict(bound(result['request']));validate_pack_source_request(pack_request,item,result)
        old_campaign,old_state=campaign_inputs(pack_request)
        require(pack_request['campaign_plan']==request['campaign_plan'] and old_campaign==campaign
            and result.get('campaign_plan')==pack_request['campaign_plan']
            and result.get('campaign_snapshot')==pack_request['campaign_snapshot'],
            'Pack producer and finalizer must preserve the identical original240-cell campaign')
        bound(result['private_pack_driver'])
        require(result['private_pack_driver']['sha256']==digest(Path(__file__)),'Pack producer wrapper source changed')
        registration_args=argparse.Namespace(native_plan=Path(registration['plan']['path']),
            native_ledger=Path(registration['ledger']['path']),native_script=Path(registration['script']['path']),
            native_task=registration['task_name'],mode='pack',request=Path(result['request']['path']),request_sha256=result['request']['sha256'])
        require(registered_task(registration_args,job,context['config'],helper_path=result['private_pack_driver']['path'])==registration,
            'Completed pack producer plan/script/task/request identity changed')
        directory=Path(result['archive_directory']);helper.verify_policy_archive(directory,result['index_sha256'],
            result['archive_sha256'],root=ROOT,checkpoint=checkpoint)
        index=helper.bound_json({'path':str(directory/'index.json'),'sha256':result['index_sha256']},ROOT)
        require(index['source']==result['evaluation_case'] and index['authority']==
            next(row['authority'] for row in pack_request['policies'] if row['cell_index']==item['cell_index']),
            'Full archive authority differs from immutable pack request')
        expected=eligible_policy(campaign,state,item['cell_index'],result['evaluation_case'],index['authority'])
        old_expected=eligible_policy(old_campaign,old_state,item['cell_index'],result['evaluation_case'],index['authority'])
        actual=helper.committed_conditions(directory,result['index_sha256'],result['archive_sha256'],root=ROOT,checkpoint=checkpoint)
        require(actual==expected==old_expected and result.get('condition_packet_roster_sha256')==canonical_hash(actual),
            'Every original300 condition packet must agree across pack and finalization snapshots')
        paths={name:locator(directory/filename) for name,filename in [('pack_intent','pack-intent.json'),
            ('roundtrip','roundtrip.json'),('pack_receipt','pack-receipt.json')]}
        require(all(result.get(name+'_sha256')==binding['sha256'] for name,binding in paths.items()),
            'Published pack intent/roundtrip/receipt changed')
        registry['cells'][key]={'evaluation_case':result['evaluation_case'],'policy_inputs':index['authority']['policy_inputs'],
            'archive_directory':str(directory),'index_sha256':result['index_sha256'],'archive_sha256':result['archive_sha256'],
            **paths,'pack_producer':{'native':producers[job],'result':item['result'],
                'qualification':{k:FRAMEWORK_Q[k] for k in ('path','sha256')}}}
        helper.verify_native(producers[job],ROOT,completed=True)
        rows.append({'cell_index':item['cell_index'],'pack_job_id':job,'original_regular_files_removed':0,
            'future_storage_savings_credited':0})
    path=case/'condition-archive-registry.json';exclusive(path,registry)
    return {'registry':locator(path),'new_policies':rows,'previous_registry':request['previous_registry'],
        'previous_policy_archives_reverified':False,'consumer_full_revalidation_required':True,
        'controller_descriptor_published':False,'original_regular_files_removed':0,'future_storage_savings_credited':0,**FLAGS}


def validate_pack_source_request(pack_request,item,result):
    require(pack_request.get('kind')=='originals_retaining_policy_archive_pack_v1'
        and pack_request.get('root')==str(ROOT) and result['archive_directory']==
        str(Path(pack_request['case'])/('policy-'+str(item['cell_index']).zfill(4)))
        and result['evaluation_case']==next(row['evaluation_case'] for row in pack_request['policies']
            if row['cell_index']==item['cell_index']),'Producer result belongs to another exact pack request/cell')


def fresh_census(record):
    now=time.time();measured=record.get('measured_unix');captured=datetime.fromisoformat(record.get('captured_utc','').replace('Z','+00:00'))
    require(type(measured) in (int,float) and math.isfinite(measured) and captured.tzinfo is not None
        and 0<=now-measured<21600 and 0<=now-captured.timestamp()<21600,'Exact study census must be fresh within six hours')


def storage_gate(context,files,bytes_,sequence):
    require(type(files) is int and files>=2000 and type(bytes_) is int and bytes_>=10_000_000_000,
        'Finite conservative additional peak storage required')
    path=ROOT/'state/storage-budget.json';sha=digest(path);fresh_census(read_json(path))
    quota=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=files,required_bytes=bytes_,
        reserve_files=max(0,250000-files),reserve_bytes=2_000_000_000_000,
        evidence_dir=context['case']/('quota-reads-'+str(sequence)),allocation_deadline_unix=context['end'])
    from horsehair.admission import _storage
    future_bytes,future_files,paths=_storage(ROOT,context['config'],bytes_,files,personal_headroom=quota)
    require(digest(path)==sha,'Exact census changed during positive pack peak admission')
    proof={'census':{'path':str(path),'sha256':sha},'future_study_bytes':future_bytes,'future_study_files':future_files,
        'source_paths':list(map(str,paths)),'personal_quota':quota,'additional_peak_files':files,'additional_peak_bytes':bytes_,
        'future_storage_savings_credited':0,**FLAGS}
    exclusive(context['case']/('storage-proof-'+str(sequence)+'.json'),proof);return proof


def registered_task(args,job,config,*,helper_path=None):
    for path,namespace in [(args.native_plan,ROOT/'plans'),(args.native_ledger,ROOT/'state'),(args.native_script,ROOT/'plans')]:
        ordinary(path,namespace=namespace)
    require(digest(DRIVER)==DRIVER_SHA,'Registered-plan validator changed')
    pair=runpy.run_path(str(DRIVER),run_name='pack_registered_identity_only')
    plan,ledger=pair['checked_pair'](args.native_plan,args.native_ledger,ROOT,config)
    tasks=[t for t in plan['tasks'] if t['name']==args.native_task];require(len(tasks)==1,'Unique registered pack task required')
    task=tasks[0];record=ledger.get('tasks',{}).get(args.native_task,{})
    helper_path=str(Path(__file__)) if helper_path is None else str(helper_path)
    expected={'--mode':args.mode,'--request':str(args.request),'--request-sha256':args.request_sha256,
        '--native-plan':str(args.native_plan),'--native-ledger':str(args.native_ledger),
        '--native-script':str(args.native_script),'--native-task':args.native_task}
    require(exact_command(task['command'],helper_path,expected),
        'Registered pack command must bind every actual mode/request/native argument')
    require(plan.get('release_identity',{}).get('sha256')==SOURCE and task.get('script')==str(args.native_script)
        and canonical_hash(args.native_script.read_text())==task.get('script_sha256')==record.get('script_sha256')
        and record.get('job_id')==job and record.get('state')=='submitted'
        and exact_resources(task.get('resources'))
        and task['command'] in args.native_script.read_text(),
        'Actual registered plan/script/native allocation does not bind this exact pack operation')
    return {'plan':locator(args.native_plan),'ledger':locator(args.native_ledger),'script':locator(args.native_script),
        'task_name':args.native_task,'native_job_id':job}


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


def native_registration_owner(allocation,registration):
    require(allocation.get('Command')==registration['script']['path']
        and allocation.get('JobName')=='hhw-'+registration['task_name'],
        'Actual native scheduler command/name differs from the registered immutable script/task')


def native_owner(job,case):
    response=runpy.run_path(str(SCHEDULER))['run_scontrol_job'](job,case/'scheduler-reads');response.check_returncode()
    lines=[line for line in response.stdout.splitlines() if line.strip()];require(len(lines)==1,'Exactly one native owner allocation required')
    pairs=re.findall(r'(?<!\S)(\w+)=([^\s]*)',lines[0]);row=dict(pairs)
    match=re.fullmatch(r'(?:(\d+)-)?(\d+):(\d{2}):(\d{2})',row.get('TimeLimit',''))
    require(match is not None,'Finite native time limit required');d,h,m,s=map(lambda x:int(x or 0),match.groups())
    for name in ('AllocTRES','ReqTRES'):
        fields=row.get(name,'').split(',');values={}
        for field in fields:
            key,separator,value=field.partition('=')
            require(separator and key and key not in values,'Exact unique nonempty native CPU TRES required')
            values[key]=value
        require(values.get('cpu')=='4' and not any('gpu' in key.lower() for key in values),
            'Native TRES must positively prove four CPUs and no GPU')
    require(len(row)==len(pairs) and row.get('JobId')==job and row.get('UserId')=='pwa209(3164768)'
        and row.get('Account')=='def-ptewarie_cpu' and row.get('JobState')=='RUNNING' and row.get('NumCPUs')=='4'
        and row.get('NumNodes') in {'1','1-1'} and row.get('MinMemoryNode') in {'16G','16384M'}
        and d*86400+h*3600+m*60+s==3600 and all('gpu' not in row.get(k,'').lower()
            for k in ('TresPerNode','TresPerJob','TresPerTask','AllocTRES','ReqTRES','Gres')),
        'Exact owned4CPU16GB1h CPU-only pack allocation required')
    end=datetime.fromisoformat(row['EndTime']).replace(tzinfo=timezone.utc).timestamp();env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(env-end)<=2 and end>time.time()+900,'Verified native completion margin required')
    return row,end


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--mode',choices=['pack','finalize'],required=True)
    parser.add_argument('--request',type=Path,required=True);parser.add_argument('--request-sha256',required=True)
    for name in ('native-plan','native-ledger','native-script'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--native-task',required=True);a=parser.parse_args(argv)
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    job=str(native_job_id() or '');require(job.isdigit() and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='4','Owned finite native pack job required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    config=read_json(PRIMARY/'configs/study.json')
    require(Path(core.__file__).resolve()==FRAMEWORK/'src/horsehair/core.py'
        and source_identity(FRAMEWORK)['sha256']==SOURCE
        and source_identity(PRIMARY)['sha256']==PRIMARY_SHA and canonical_hash(config)==CONFIG,
        'Original science/configuration must remain frozen')
    for path,sha in [(GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(QUOTA,QUOTA_SHA),(REGISTRY_GUARD,REGISTRY_GUARD_SHA)]:
        require(digest(ordinary(path))==sha,'Pinned native reader changed')
    request_locator={'path':str(a.request),'sha256':a.request_sha256};request=strict(bound(request_locator))
    case=validate_request(request,a.mode);case.mkdir(mode=0o700)
    allocation,end=native_owner(job,case)
    runpy.run_path(str(GUARD))['install'](case/'accounting-reads')
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'358 must precede campaign function binding')
    runpy.run_path(str(REGISTRY_GUARD))['install'](case/'registered-snapshot-reads',registration_mode='worker-lock',allocation_deadline_unix=end)
    registration=registered_task(a,job,config);native_registration_owner(allocation,registration)
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],case,'native-start-admission',
        resource_policy=policy,cpus=0,gpus=0,hours=1)
    join,join_binding=public_native_join()
    require(type(VALIDATION_Q) is dict,'New complete1889 native qualification is not yet frozen')
    qualification(FRAMEWORK_Q,SOURCE,1694,join,end=end);qualification(VALIDATION_Q,VALIDATION_SOURCE,1889,join,end=end)
    private_qualification(request['operational_qualification'],join,end=end)
    helper=load_archive(request['archive_helper']);helper._verify_canary(CANARY,ROOT)
    context={'case':case,'job_id':job,'end':end,'config':config,'policy':policy,'request':request_locator,'registration':registration,
        'wrapper_sha256':digest(Path(__file__))}
    def checkpoint():require(time.time()+300<end,'Finite pack native finalization margin exhausted')
    exclusive(case/'intent.json',{'mode':a.mode,'request':request_locator,'native_registration':registration,
        'allocation':allocation,'native_join_binding':join_binding,'validation_qualification':VALIDATION_Q,
        'resource_policy':policy.receipt(),'private_pack_driver_sha256':digest(Path(__file__)),**FLAGS})
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        if a.mode=='pack':
            forecast=plan_packs(helper,request,checkpoint=checkpoint);exclusive(case/'pack-plan.json',forecast)
            storage_gate(context,forecast['additional_peak_files'],forecast['additional_peak_bytes'],1)
            value={'status':'complete_originals_retaining_policy_packs_pending_native_completion',
                'policies':execute_packs(helper,request,forecast,context,checkpoint=checkpoint)}
        else:
            storage_gate(context,2000,10_000_000_000,1)
            value={'status':'complete_positive_producer_registry_originals_retained',
                **finalized_registry(helper,request,context,join,checkpoint=checkpoint)}
        checkpoint();require(bound(request_locator)==a.request.read_bytes(),'Immutable operation request changed')
        helper._verify_canary(CANARY,ROOT);public_native_join();bound(request['archive_helper'])
        require(digest(Path(__file__))==context['wrapper_sha256'],'Private pack wrapper changed during execution')
        exclusive(case/'receipt.json',{'schema_version':1,'mode':a.mode,'native_job_id':job,'request':request_locator,
            'execution_source_sha256':SOURCE,'full_config_sha256':CONFIG,'primary_scientific_source_sha256':PRIMARY_SHA,
            'validation_qualification':VALIDATION_Q,'operational_qualification':request['operational_qualification'],
            'private_pack_driver_sha256':digest(Path(__file__)),'original_regular_files_removed':0,
            'controller_descriptor_published':False,'future_storage_savings_credited':0,**value,**FLAGS})
    return value


if __name__=='__main__':main()
