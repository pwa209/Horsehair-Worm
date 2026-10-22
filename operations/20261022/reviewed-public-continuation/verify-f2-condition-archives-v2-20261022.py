"""Isolated qualifiedf047 read-only archive-to-controller packet proof.

No statistics, endpoints, scheduler mutation, deletion or numerical execution.
Every requested complete policy is read again from its verified whole archive.
"""
from pathlib import Path
import argparse
import gzip
import hashlib
import io
import json
import math
import os
import re
import sys
import time

from horsehair import agent_analysis_v2 as consumer,agent_campaign as dispatch
from horsehair.core import source_identity,digest,canonical_hash

ROOT=Path('/scratch/pwa209/horsehair-worm')
SOURCE='f047ce12b0a4a31312f2b4f5b6cff144342a12950f2e84fe1d444c8ea3a9b68c'
MODULE='507fbe01700e4310a4e44b16f178d924fe291585d20646830e97b7a28e13fd6c'
FRAMEWORK=ROOT/'releases/20261021-e104ce63fa53-f047ce12b0a4'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PRIMARY='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
QPATH=ROOT/'maintenance/study-af-execution-support-v4-20261021/qualification.json'
QSHA='8c3e1910deabea6c1061ddb353c3a6881fb70fb9fe86948007557a03ca809176'
QJOB='6703313'
MAX_METADATA=10_000_000
MAX_STATE=512_000_000
MAX_SECONDS=900


def require(value,message):
    if not value:raise ValueError(message)


def strict(raw):
    def pairs(values):
        result={}
        for key,value in values:
            require(key not in result,'Duplicate JSON metadata key refused');result[key]=value
        return result
    def nonfinite(value):raise ValueError('Nonfinite JSON metadata refused')
    def floating(text):
        value=float(text);require(math.isfinite(value),'Nonfinite JSON metadata refused');return value
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=nonfinite,parse_float=floating)


def bound(locator,*,maximum=MAX_METADATA):
    require(isinstance(locator,dict) and set(locator)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',locator['sha256']),
        'Exact source path/SHA locator required')
    p=Path(locator['path'])
    require(p.is_absolute() and p==p.resolve() and p.is_relative_to(ROOT) and p!=ROOT and p.is_file()
        and all(not a.is_symlink() for a in [p,*p.parents] if a.is_relative_to(ROOT)),'Ordinary confined archive evidence required')
    require(p.stat().st_size<=maximum,'Finite metadata source size exceeded')
    with p.open('rb') as stream:raw=stream.read(maximum+1)
    require(len(raw)<=maximum and hashlib.sha256(raw).hexdigest()==locator['sha256'],'Source metadata bytes changed')
    return raw


def qualified_consumer(binding,*,deadline=None):
    """This process must use the complete qualified package, never a mixed graph."""
    require(isinstance(binding,dict) and set(binding)=={'path','sha256','job_id'} and Path(binding['path'])==QPATH
        and binding['sha256']==QSHA and binding['job_id']==QJOB,'Exact explicit full1889 source qualification required')
    import horsehair
    identity=source_identity(FRAMEWORK)
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py'
        and identity['sha256']==SOURCE and digest(Path(consumer.__file__))==MODULE
        and Path(consumer.__file__).resolve()==FRAMEWORK/'src/horsehair/agent_analysis_v2.py',
        'Archive verifier must execute the whole qualifiedf047 package in isolation')
    q=strict(bound({k:binding[k] for k in ('path','sha256')}));expected={'tests':1889,'failures':0,'errors':0,'skipped':0}
    require(q.get('status')=='complete' and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('source_sha256')==SOURCE and q.get('config_sha256')==CONFIG
        and q.get('source_verified_unchanged') is True and q.get('source_identity')==identity
        and q.get('slurm_job_id')==binding['job_id'] and q.get('test_counts')==expected
        and all(type(v) is int for v in q['test_counts'].values()),'Complete exact unchanged1889 native source qualification required')
    command=q.get('command',[]);workspace=Path(q.get('workspace',''))
    require(workspace.is_absolute() and isinstance(command,list) and command and q.get('test_selection')==consumer.FULL_TEST_SELECTION
        and command==[command[0],'-u','-m','pytest',str(FRAMEWORK/'tests'),'-v','--color=no','--basetemp',str(workspace/'pytest'),
            '--junitxml',str(workspace/'junit.xml'),'-o','cache_dir='+str(workspace/'cache'),'-o','faulthandler_timeout=120'],
        'Complete immutable source/tests directory qualification command required')
    artifact=q.get('artifacts',{}).get('junit',{});raw=bound({k:artifact[k] for k in ('path','sha256')},maximum=8_000_000)
    require(len(raw)==artifact.get('bytes') and Path(artifact['path']).is_relative_to(ROOT/'state/qualification-evidence'),
        'Actual complete1889 native JUnit bytes required')
    from horsehair.cli import _qualification_junit
    require(_qualification_junit(Path(artifact['path']))==expected,'Actual1889 JUnit counts differ')
    joined=consumer._owned_completed_cpu_qualification(ROOT,binding['job_id'],deadline=deadline)
    proof=joined['completion']
    record=joined['native']
    require(record.get('user')=='pwa209' and record.get('account')=='def-ptewarie_cpu' and record.get('job_id_raw')==binding['job_id']
        and record.get('unallocated_metadata') is False and type(record.get('cpus')) is int and record['cpus']==4
        and type(record.get('elapsed_seconds')) is int and 0<record['elapsed_seconds']<=3600
        and 'gpu' not in record.get('alloc_tres','').lower(),'Owned scalar4CPU consumer fullqualification proof required')
    return {'source_sha256':SOURCE,'module_sha256':MODULE,'qualification':binding,
        'completion_proof_sha256':proof['completion_proof_sha256'],'owned_native_record_sha256':canonical_hash(record)}


def verify_cells(root,plan,state,archives,*,checkpoint=lambda:None):
    """Freshly read every listed archive and compare all original state packets."""
    dispatch._state(plan,state)
    require(archives is not None and archives['registry']['full_config_sha256']==CONFIG,'Explicit full archive registry required')
    rows=[];cells={cell['index']:cell for cell in state['cells']}
    for key,descriptor in archives['registry']['cells'].items():
        checkpoint();index=int(key);cell=cells[index];original=plan['cells'][index]
        require(cell.get('complete') is True and cell.get('assembly_job') is not None
            and set(cell.get('conditions',{}))=={str(i) for i in range(300)},'Only positively assembled full300 policies may use an archive')
        assembly=state['jobs'][cell['assembly_job']]
        require(assembly.get('status')=='complete' and assembly.get('job_id') and assembly['intent']['kind']=='assembly',
            'Positive original assembly job/receipt required')
        helper=archives['helper']
        native_index=consumer._archive_descriptor(root,helper,descriptor,original,pack_qualification=archives['pack_qualification'])
        assembly_job,_=helper.verify_native(native_index['authority']['native'],root,completed=True)
        require(assembly_job==assembly['job_id'],'Archive assembly authority differs from completed controller job')
        actual=helper.committed_conditions(descriptor['archive_directory'],descriptor['index_sha256'],descriptor['archive_sha256'],
            root=root,checkpoint=checkpoint)
        require(len(actual)==300 and [p['value']['condition_index'] for p in actual]==list(range(300)),
            'Every ordered original committed receipt required')
        expected=[cell['conditions'][str(i)] for i in range(300)]
        require(actual==expected,'Archived raw SHA/value packets differ from original frozen controller state')
        ep=cell['evaluation_plan']['value']
        require(descriptor['evaluation_case']==cell['evaluation_plan']['case']==original['evaluation_case']
            and descriptor['policy_inputs']['plan']['sha256']==cell['evaluation_plan']['plan_sha256']
            and descriptor['policy_inputs']['binding']['sha256']==cell['evaluation_plan']['plan_binding_sha256'],
            'Archive controller plan/binding/case changed')
        for index,packet in enumerate(actual):
            consumer.evaluation._validate_restoration(packet['value']['restoration'],ep['conditions'][index])
        rows.append({'cell_index':cell['index'],'condition_packets':300,'packet_roster_sha256':canonical_hash(actual),
            'index_sha256':descriptor['index_sha256'],'archive_sha256':descriptor['archive_sha256'],
            'assembly_job_id':assembly_job,'all_current_archived_bytes_reverified':True})
    checkpoint();return rows


def validate_request(request):
    require(type(request) is dict and set(request)=={'schema_version','kind','root','consumer_qualification','campaign_plan','state','state_sha256','archive_registry'}
        and type(request['schema_version']) is int and request['schema_version']==1
        and request['kind']=='fresh_complete_archive_controller_verification_v1'
        and request['root']==str(ROOT),'Exact bounded controller archive request required')
    require(type(request['state_sha256']) is str and re.fullmatch('[0-9a-f]{64}',request['state_sha256']),
        'Exact complete controller state seal required')


def execute(request,*,deadline):
    validate_request(request)
    def checkpoint():require(time.monotonic()<deadline,'Bounded archive verification deadline exceeded')
    checkpoint();qualified=qualified_consumer(request['consumer_qualification'],deadline=deadline)
    plan=strict(bound(request['campaign_plan']));state_raw=bound(request['state'],maximum=MAX_STATE)
    with gzip.GzipFile(fileobj=io.BytesIO(state_raw)) as stream:decoded=stream.read(MAX_STATE+1)
    require(len(decoded)<=MAX_STATE,'Finite uncompressed controller state exceeded')
    state=strict(decoded)
    require(state.get('state_sha256')==request['state_sha256'] and plan.get('case')==str(Path(request['state']['path']).parent)
        and canonical_hash(plan['config'])==CONFIG and plan['source_binding']['primary_source_sha256']==PRIMARY,
        'Exact live original campaign/state source identity required')
    context=consumer._archive_context(ROOT,request['archive_registry'],plan['cells'],native_deadline=deadline)
    require(context is not None and context['registry']['cells'],'Nonempty explicit archive policy roster required')
    rows=verify_cells(ROOT,plan,state,context,checkpoint=checkpoint)
    # Recheck the entire source/registry after traversal; no persistent cache.
    require(bound(request['state'],maximum=MAX_STATE)==state_raw,'Controller state changed during archive verification')
    bound(request['campaign_plan']);consumer._archive_context(ROOT,request['archive_registry'],plan['cells'],native_deadline=deadline)
    qualified_consumer(request['consumer_qualification'],deadline=deadline);checkpoint()
    return {'schema_version':1,'status':'complete_fresh300_packet_archive_verification','consumer':qualified,
        'state_sha256':state['state_sha256'],'state_file_sha256':request['state']['sha256'],
        'campaign_plan_sha256':plan['plan_sha256'],'archive_registry':request['archive_registry'],
        'cells':rows,'raw_condition_packets_changed':False,'endpoint_values_recomputed':False,
        'scientific_result':False,'admission_authority':False}


def main():
    os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    p=argparse.ArgumentParser();p.add_argument('--request',type=Path,required=True);p.add_argument('--request-sha256',required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--max-seconds',type=int,default=MAX_SECONDS);a=p.parse_args()
    require(type(a.max_seconds) is int and 0<a.max_seconds<=MAX_SECONDS,'Finite read-only native verification deadline required')
    output=a.output
    require(output.is_absolute() and output==output.resolve() and output.is_relative_to(ROOT/'maintenance')
        and not output.exists() and not output.is_symlink(),'Fresh confined verification output required')
    request_locator={'path':str(a.request),'sha256':a.request_sha256};request=strict(bound(request_locator))
    result=execute(request,deadline=time.monotonic()+a.max_seconds)
    result.update(request=request_locator,private_helper_sha256=digest(Path(__file__)))
    raw=(json.dumps(result,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    require(len(raw)<=MAX_METADATA,'Finite archive verification proof exceeded')
    with output.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    print(json.dumps({'status':result['status'],'policies':len(result['cells']),'output_sha256':digest(output)},sort_keys=True))


if __name__=='__main__':main()
