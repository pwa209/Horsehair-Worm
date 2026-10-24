"""New finite backfill policy; original provider qualification remains separate."""
from pathlib import Path
from datetime import datetime,timezone
import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import shlex
import sys
import time
import xml.etree.ElementTree as ET
from types import SimpleNamespace

from horsehair import slurm
from horsehair.core import canonical_hash,digest,read_json,native_job_id

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-af-rolling-census-support-v4-20261023'
BASE=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022/run-af-agent-controller-registry-v6-20261022.py'
BASE_SHA='e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
BACKFILL_SUPPORT=ROOT/'maintenance/study-af-backfill-controller-support-v2-20261024'
BACKFILL_BASE=BACKFILL_SUPPORT/'run-af-agent-controller-backfill-base-v2-20261024.py'
BACKFILL_BASE_SHA='cbf01cffbbaa9a983ec1e8e6394b38ac04623483e768398c18f48b5e30e31445'
ORIGINAL_POLICY_SOURCE=SUPPORT/'run-af-agent-controller-census-policy-v1-20261023.py'
ORIGINAL_POLICY_SHA='120f02d098b6c9afa0ea0df54f590ae381b4227d4892ba0f9ee4acc93e6fec0f'
ORIGINAL_PROVIDER_POLICY=ROOT/'maintenance/study-af-external-census-handoff-v1-20261023/native-6865706/external-census-policy.json'
ORIGINAL_PROVIDER_POLICY_SHA='2b0e6ebaa0f1b511bca4343d4722d43702cf051f956cce59494ddccae656c517'
BACKFILL_TEST_NAME='test_f2_backfill_controller_v2_20261024.py'
BACKFILL_TEST_SHA='e75be24c69362e9fe471695c532cb5704cf7990f26c8aa37ed9e31c826d970c6'
BACKFILL_QUALIFIER_NAME='qualify-f2-backfill-controller-v2-20261024.py'
BACKFILL_ASSET_MANIFEST_SHA='afddbb5f5e0833dee421dc8a8bdcbb2b7b34edbd05e254eac736bd8f24e7b250'
BACKFILL_ARTIFACT_MAX_BYTES=8_000_000
BACKFILL_RUNTIME={'requested_hours':96,'minimum_hours':8,'shutdown_reserve_seconds':14400,'memory':'64G','cpus':1}
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
QUALIFICATION=ROOT/'maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json'
PROVIDER_CASE=ROOT/'maintenance/study-af-rolling-storage-census-v4-20261023'
PROVIDER_NAME='run-fresh-storage-census-v6-20261023.py'
QUALIFIER_NAME='qualify-streaming-census-v2-20261023.py'
SELF_NAME='run-af-agent-controller-census-policy-v1-20261023.py'
TEST_NAME='test_af_agent_controller_census_policy_v1_20261023.py'
SUITE_SOURCES={**{'baseline_'+name:'tests/test_'+name+'.py' for name in
    ('budget','storage_artifacts','storage_artifact_cache','storage_boundary_cache','storage_snapshot_artifacts','storage_snapshot_boundary')},
    'kernel':'test_streaming_storage_census_v1_20261023.py','wrapper':'test_run_fresh_storage_census_v5_20261023.py',
    'registry':'test_registered_snapshot_read_guard_v4_20261022.py','qualifier':'test_qualify_streaming_census_v1_20261023.py',
    'repeatable_wrapper':'test_run_fresh_storage_census_v6_20261023.py','rolling_qualifier':'test_qualify_streaming_census_v2_20261023.py',
    'external_census_policy':TEST_NAME}
PRIVATE_FILES={SELF_NAME,TEST_NAME,PROVIDER_NAME,QUALIFIER_NAME,'test_run_fresh_storage_census_v6_20261023.py',
    'test_qualify_streaming_census_v2_20261023.py','run-fresh-storage-census-v5-20261023.py',
    'test_run_fresh_storage_census_v5_20261023.py','streaming-storage-census-v1-20261023.py',
    'test_streaming_storage_census_v1_20261023.py','qualify-streaming-census-v1-20261023.py',
    'test_qualify_streaming_census_v1_20261023.py','registered-snapshot-read-guard-v4-20261022.py',
    'test_registered_snapshot_read_guard_v4_20261022.py'}
FALSE_FLAGS=('scientific_result','admission_authority','scientific_gates_cleared','freshness_timestamp_rewritten','end_time_extends_admission_freshness')


def require(value,message):
    if not value:raise ValueError(message)


def ordinary(path,namespace):
    p=Path(path)
    require(p.is_absolute() and p==p.resolve() and p!=namespace and p.is_relative_to(namespace)
        and p.is_file() and all(not q.is_symlink() for q in [p,*p.parents] if q.is_relative_to(namespace)),
        'Exact ordinary confined source/evidence file required')
    return p


def packet(path,namespace,sha=None,limit=1_000_000):
    p=ordinary(path,namespace)
    with p.open('rb') as stream:raw=stream.read(limit+1)
    actual=hashlib.sha256(raw).hexdigest()
    require(len(raw)<=limit and (sha is None or actual==sha),'Bounded immutable packet bytes changed')
    return raw,actual


def strict(raw):
    def pairs(items):
        value={}
        for key,item in items:
            require(key not in value,'Duplicate JSON field refused');value[key]=item
        return value
    def constant(value):raise ValueError('Nonfinite JSON refused')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)


def json_packet(locator,namespace,limit=1_000_000):
    require(type(locator) is dict and set(locator)=={'path','sha256'}
        and re.fullmatch('[0-9a-f]{64}',str(locator['sha256'])),'Exact file locator required')
    return strict(packet(locator['path'],namespace,locator['sha256'],limit)[0])


def load_base():
    packet(BACKFILL_BASE,ROOT/'maintenance',BACKFILL_BASE_SHA,200_000)
    spec=importlib.util.spec_from_file_location('qualified_backfill_base1',BACKFILL_BASE)
    base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)
    require(digest(BACKFILL_BASE)==BACKFILL_BASE_SHA,'New frozen backfill controller changed during library import')
    return base


def registered(plan_binding,ledger_binding,task_name,script_binding,job,*,helper,hours,cpus,memory,argv=None):
    plan=json_packet(plan_binding,ROOT/'plans',8_000_000);ledger=json_packet(ledger_binding,ROOT/'state',8_000_000)
    raw,_=packet(script_binding['path'],ROOT/'plans',script_binding['sha256'],1_000_000)
    selected=[task for task in plan.get('tasks',[]) if task.get('name')==task_name]
    require(len(selected)==1 and plan.get('config_sha256')==CONFIG and plan.get('release_identity',{}).get('sha256')==SOURCE
        and ledger.get('source_sha256')==SOURCE and ledger.get('plan_sha256')==canonical_hash(plan),
        'Exact registered source/config/plan/ledger binding required')
    task=selected[0];row=ledger.get('tasks',{}).get(task_name,{})
    command=shlex.split(task.get('command',''));helper=str(helper)
    require(command.count(helper)==1 and task.get('script')==script_binding['path'] and task['command'] in raw.decode()
        and task.get('script_sha256')==row.get('script_sha256')==canonical_hash(raw.decode())
        and row.get('job_id')==job and row.get('state')=='submitted'
        and task.get('resources')=={'cpus':cpus,'memory':memory,'hours':hours,'gpu':False,'array':None},
        'Exact registered helper/script/native/resources required')
    tail=command[command.index(helper)+1:]
    if argv is not None:require(tail==list(argv),'Every actual outer policy argument must equal its registered invocation')
    return task,tail


def locator(path):return {'path':str(path),'sha256':digest(path)}


def passing_suite(value,contract):
    require(value.get('test_counts')=={'tests':contract['tests'],'failures':0,'errors':0,'skipped':0}
        and all(type(n) is int for n in value['test_counts'].values())
        and value.get('test_roster_sha256')==contract['roster_sha256'],'Exact source-bound passing native suite required')
    require(type(value.get('junit')) is dict and set(value['junit'])=={'path','sha256'},'Exact native JUnit locator required')
    raw,_=packet(value['junit']['path'],QUALIFICATION.parent,value['junit']['sha256'],8_000_000)
    require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Unsafe JUnit declarations refused')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'))
    names=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
    require(len(cases)==len(set(names))==contract['tests'] and all(c.get('classname') and c.get('name') for c in cases)
        and canonical_hash(names)==contract['roster_sha256']
        and not any(list(c.iter(name)) for c in cases for name in ('failure','error','skipped'))
        and all(all(int(n.get(key,'0'))==0 for key in ('failures','errors','skipped'))
            for n in tree.iter() if n.tag in {'testsuite','testsuites'}),'Actual full native passing JUnit roster required')
    leaves=[n for n in tree.iter('testsuite') if not n.findall('testsuite')]
    require(leaves and sum(int(n.get('tests','-1')) for n in leaves)==contract['tests'],'Actual JUnit counts disagree')


def qualification(policy):
    binding=policy['qualification'];require(set(binding)=={'path','sha256','job_id'} and Path(binding['path'])==QUALIFICATION
        and re.fullmatch('[1-9][0-9]*',str(binding['job_id'])),
        'Exact separately completed new policy qualification required')
    q=json_packet({k:binding[k] for k in ('path','sha256')},ROOT/'maintenance')
    schedule=policy['provider_schedule'];manifest_binding=schedule['source_manifest']
    require(Path(manifest_binding['path'])==SUPPORT/'source-manifest.json','Exact shared qualified manifest required')
    manifest=json_packet(manifest_binding,ROOT/'maintenance',100_000)
    require(set(manifest)=={'kind','files','suites'} and manifest['kind']=='streaming-census-operational-source-manifest-v2'
        and set(manifest['files'])==PRIVATE_FILES and set(manifest['suites'])==set(SUITE_SOURCES),
        'Complete new policy/provider source closure required')
    paths={}
    for name,sha in manifest['files'].items():
        require(Path(name).name==name and re.fullmatch('[0-9a-f]{64}',str(sha)),'Exact private basename/SHA required')
        p=ordinary(SUPPORT/name,ROOT/'maintenance');require(digest(p)==sha,'Qualified policy/provider source changed');paths[str(p)]=sha
    for scope,contract in manifest['suites'].items():
        require(set(contract)=={'source','source_sha256','tests','roster_sha256'}
            and contract['source']==SUITE_SOURCES[scope]
            and type(contract['tests']) is int and contract['tests']>0
            and re.fullmatch('[0-9a-f]{64}',str(contract['roster_sha256'])),'Exact positive new suite contract required')
        p=FRAMEWORK/contract['source'] if scope.startswith('baseline_') else SUPPORT/contract['source']
        ordinary(p,FRAMEWORK if scope.startswith('baseline_') else SUPPORT)
        require(digest(p)==contract['source_sha256'],'Actual qualified suite source changed');paths[str(p)]=contract['source_sha256']
        require(scope.startswith('baseline_') or contract['source_sha256']==manifest['files'][contract['source']],
            'Private suite must match the complete private file closure')
    for relative in ('tests/conftest.py','src/horsehair/budget.py','src/horsehair/storage_artifacts.py'):
        p=ordinary(FRAMEWORK/relative,FRAMEWORK);paths[str(p)]=digest(p)
    require(q.get('status')=='complete_rolling_census_operational_qualification' and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('slurm_job_id')==binding['job_id'] and q.get('source_sha256')==SOURCE and q.get('config_sha256')==CONFIG
        and q.get('source_verified_unchanged') is True and q.get('source_manifest')==manifest_binding and q.get('source_pins')==paths
        and q.get('qualification_helper')=={'path':str(SUPPORT/QUALIFIER_NAME),'sha256':manifest['files'][QUALIFIER_NAME]}
        and q.get('base_controller')=={'path':str(BASE),'sha256':BASE_SHA}
        and q.get('baseline_iterator_policy')=='original_canonical_iterator'
        and q.get('private_iterator_policy')=='scoped_private_kernel_only'
        and q.get('native_completion_required_separately') is True and all(q.get(flag) is False for flag in FALSE_FLAGS),
        'Positive new source-bound policy/provider native qualification required')
    suites=q.get('suites',[]);require(len(suites)==13 and {s['scope'] for s in suites}==set(manifest['suites']),
        'Every new native qualification suite required')
    require(manifest['suites']['external_census_policy']['source']==TEST_NAME,'New outer policy suite cannot use old coverage')
    for suite in suites:
        contract=manifest['suites'][suite['scope']];expected_path=FRAMEWORK/contract['source'] if suite['scope'].startswith('baseline_') else SUPPORT/contract['source']
        require(suite.get('source')=={'path':str(expected_path),'sha256':contract['source_sha256']},'Native suite source locator differs')
        passing_suite(suite,contract);paths[suite['junit']['path']]=suite['junit']['sha256']
    registration=q['native_registration'];helper=SUPPORT/QUALIFIER_NAME
    task,tail=registered(registration['plan'],registration['ledger'],registration['task_name'],registration['script'],binding['job_id'],
        helper=helper,hours=1,cpus=8,memory='64G')
    expected={'--native-plan':registration['plan']['path'],'--native-ledger':registration['ledger']['path'],
        '--native-task':registration['task_name'],'--native-script':registration['script']['path'],
        '--source-manifest':manifest_binding['path'],'--source-manifest-sha256':manifest_binding['sha256']}
    require(registration['native_job_id']==binding['job_id'] and len(tail)==12 and len(set(tail[::2]))==6
        and dict(zip(tail[::2],tail[1::2]))==expected
        and task.get('rolling_census_qualification')=={'helper':{'path':str(helper),'sha256':manifest['files'][QUALIFIER_NAME]},
            'source_manifest':manifest_binding},'Actual registered Q2 helper/manifest invocation required')
    proof=slurm.completed_native_dependency(ROOT,binding['job_id'])
    require(proof is not None,'New policy qualification native completion is pending')
    row=proof['native_records'].get(binding['job_id'],{})
    require(row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0' and type(row.get('cpus')) is int and row['cpus']==8
        and type(row.get('gpus')) is int and row['gpus']==0 and type(row.get('elapsed_seconds')) is int and 0<row['elapsed_seconds']<=3600,
        'Exact positive finalized8CPU Q2 allocation required')
    require(policy['policy_source']=={'path':str(SUPPORT/SELF_NAME),'sha256':manifest['files'][SELF_NAME]}
        and schedule['helper']=={'path':str(SUPPORT/PROVIDER_NAME),'sha256':manifest['files'][PROVIDER_NAME]},
        'Outer/provider source locators must equal their qualified manifest bytes')
    paths.update({binding['path']:binding['sha256'],manifest_binding['path']:manifest_binding['sha256'],str(BASE):BASE_SHA})
    return paths


def provider_policy_guard(context):
    policy=json_packet(context['policy_binding'],ROOT/'maintenance')
    require(policy==context['policy'] and digest(ORIGINAL_POLICY_SOURCE)==ORIGINAL_POLICY_SHA
        and policy['policy_source']=={'path':str(ORIGINAL_POLICY_SOURCE),'sha256':ORIGINAL_POLICY_SHA},
        'Original provider policy source/descriptor changed')
    for path,sha in context['checked_paths'].items():require(digest(ordinary(path,ROOT))==sha,'Qualified policy source/evidence changed')
    proof=slurm.completed_native_dependency(ROOT,policy['qualification']['job_id'])
    require(proof is not None and proof['native_records']==context['qualification_completion']['native_records'],
        'New policy qualification selected final native proof changed')



def backfill_artifact_packet(binding):
    require(type(binding) is dict and set(binding)=={'path','sha256'}
        and re.fullmatch('[0-9a-f]{64}',str(binding['sha256'])),'Exact persisted qualification artifact locator required')
    return packet(binding['path'],BACKFILL_SUPPORT,binding['sha256'],BACKFILL_ARTIFACT_MAX_BYTES)[0]


def qualification_artifact_bounds(q):
    locators={'backfill_junit':q['suite']['junit'],
        **{item['scope']+'_junit':item['junit'] for item in q['regression_suites']},
        **{name:q[name] for name in ('native_accounting_admission','local_fixture_source_proof')}}
    require(set(locators)=={'backfill_junit','new_base_operational_junit','original_provider_junit',
        'native_accounting_admission','local_fixture_source_proof'},'Exact five persisted qualification artifacts required')
    return {'maximum_bytes':BACKFILL_ARTIFACT_MAX_BYTES,'artifacts':{
        name:{**binding,'bytes':len(backfill_artifact_packet(binding))} for name,binding in sorted(locators.items())}}


def backfill_evidence(q):
    bounds=qualification_artifact_bounds(q)
    require(q.get('persisted_artifact_bounds')==bounds,'Actual persisted qualification byte sizes/limits changed')
    fixture=strict(backfill_artifact_packet(q['local_fixture_source_proof']))
    require(fixture.get('fixtures_allocation_local') is True and fixture.get('fixture_quota_is_admission_authority') is False,
        'Regressions must use real allocation-local fixtures without simulated admission authority')
    return {item['path']:item['sha256'] for item in bounds['artifacts'].values()},fixture


def passing_backfill_suite(value,contract):
    require(value.get('test_counts')=={'tests':contract['tests'],'failures':0,'errors':0,'skipped':0}
        and all(type(n) is int for n in value['test_counts'].values())
        and value.get('test_roster_sha256')==contract['roster_sha256'],'Exact source-bound passing new native suite required')
    require(type(value.get('junit')) is dict and set(value['junit'])=={'path','sha256'},'Exact new native JUnit locator required')
    raw=backfill_artifact_packet(value['junit'])
    require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Unsafe new JUnit declarations refused')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'))
    names=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
    require(len(cases)==len(set(names))==contract['tests'] and all(c.get('classname') and c.get('name') for c in cases)
        and canonical_hash(names)==contract['roster_sha256']
        and not any(list(c.iter(name)) for c in cases for name in ('failure','error','skipped'))
        and all(all(int(n.get(key,'0'))==0 for key in ('failures','errors','skipped'))
            for n in tree.iter() if n.tag in {'testsuite','testsuites'}),'Actual full new native passing JUnit roster required')
    leaves=[n for n in tree.iter('testsuite') if not n.findall('testsuite')]
    require(leaves and sum(int(n.get('tests','-1')) for n in leaves)==contract['tests'],'Actual new JUnit counts disagree')


def backfill_qualification(policy,*,provider_paths=None):
    """Old provider coverage cannot qualify the newly changed controller."""
    binding=policy['backfill_qualification']
    require(set(binding)=={'path','sha256','job_id'} and re.fullmatch('[1-9][0-9]*',str(binding['job_id'])),
        'Exact new native backfill qualification binding required')
    q=json_packet({k:binding[k] for k in ('path','sha256')},ROOT/'maintenance')
    expected_sources={'base':{'path':str(BACKFILL_BASE),'sha256':BACKFILL_BASE_SHA},
        'outer':{'path':str(Path(__file__)),'sha256':digest(Path(__file__))}}
    require(q.get('status')=='complete_f2_backfill_controller_qualification'
        and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('slurm_job_id')==binding['job_id'] and q.get('source_sha256')==SOURCE and q.get('config_sha256')==CONFIG
        and q.get('source_verified_unchanged') is True and q.get('runtime_contract')==BACKFILL_RUNTIME
        and q.get('original_base')=={'path':str(BASE),'sha256':BASE_SHA}
        and q.get('original_policy')=={'path':str(ORIGINAL_POLICY_SOURCE),'sha256':ORIGINAL_POLICY_SHA}
        and q.get('provider_policy')=={'path':str(ORIGINAL_PROVIDER_POLICY),'sha256':ORIGINAL_PROVIDER_POLICY_SHA}
        and q.get('native_completion_required_separately') is True
        and q.get('sources')==expected_sources and all(q.get(flag) is False for flag in FALSE_FLAGS),
        'Positive new source-bound backfill native qualification required')
    suite=q.get('suite',{});contract=q.get('suite_contract',{})
    require(set(contract)=={'source','source_sha256','tests','roster_sha256'}
        and contract['source']==BACKFILL_TEST_NAME and contract['source_sha256']==BACKFILL_TEST_SHA
        and type(contract['tests']) is int and contract['tests']==40
        and re.fullmatch('[0-9a-f]{64}',str(contract['roster_sha256']))
        and suite.get('source')=={'path':str(BACKFILL_SUPPORT/BACKFILL_TEST_NAME),'sha256':contract['source_sha256']},
        'Actual new backfill suite source/count/roster contract required')
    packet(BACKFILL_SUPPORT/BACKFILL_TEST_NAME,ROOT/'maintenance',contract['source_sha256'],200_000)
    passing_backfill_suite(suite,contract)
    asset_path=BACKFILL_SUPPORT/'qualification-asset-pins.json'
    assets=json_packet({'path':str(asset_path),'sha256':BACKFILL_ASSET_MANIFEST_SHA},ROOT/'maintenance')
    regression=q.get('regression_suites',[])
    require(len(regression)==2 and {item.get('scope') for item in regression}=={'new_base_operational','original_provider'},
        'New base operational and preserved provider regressions required')
    for item in regression:
        expected_count=172 if item['scope']=='new_base_operational' else 71
        expected_names={'test_registered_snapshot_read_guard_v5_20261022.py','test_f00d_worker_registry_overlay_20261021.py',
            'test_af_agent_controller_registry_v6_20261022.py'} if item['scope']=='new_base_operational' else {'test_af_agent_controller_census_policy_v1_20261023.py'}
        require(item.get('test_sources')=={name:assets[name] for name in expected_names}
            and item.get('tested_controller_sha256')==(BACKFILL_BASE_SHA if item['scope']=='new_base_operational' else ORIGINAL_POLICY_SHA)
            and item.get('contract',{}).get('tests')==expected_count,
            'Exact regression source bytes, tested new base and suite counts required')
        if item['scope']=='new_base_operational':
            require(item.get('original_operational_roster_sha256')=='cae9f3349cf709af939c6953b626cf54e9d7ef1a4a0a711874960d89e939195b',
                'Exact original full 172-test operational roster required')
        else:
            require(item['contract'].get('roster_sha256')=='26f42367f6b53579e6ed923fc075c68466871e9296e4beb1a7103a4166384c21',
                'Exact preserved original full 71-test provider roster required')
        passing_backfill_suite(item,item['contract'])
    registration=q['native_registration'];helper=BACKFILL_SUPPORT/BACKFILL_QUALIFIER_NAME
    task,tail=registered(registration['plan'],registration['ledger'],registration['task_name'],registration['script'],binding['job_id'],
        helper=helper,hours=1,cpus=1,memory='64G')
    expected={'--native-plan':registration['plan']['path'],'--native-ledger':registration['ledger']['path'],
        '--native-task':registration['task_name'],'--native-script':registration['script']['path']}
    require(registration['native_job_id']==binding['job_id'] and len(tail)==8 and len(set(tail[::2]))==4
        and dict(zip(tail[::2],tail[1::2]))==expected,'Exact new native qualifier invocation required')
    closure=task.get('backfill_qualification_sources',{})
    expected_names={BACKFILL_BASE.name,Path(__file__).name,BACKFILL_QUALIFIER_NAME,BACKFILL_TEST_NAME,
        'qualification-asset-pins.json','build-backfill-native-plans-v2-20261024.py'}|{'qualification-assets/'+name for name in assets}
    require(closure.get('kind')=='f2-backfill-controller-transport-source-manifest-v2'
        and closure.get('native_support')==str(BACKFILL_SUPPORT) and closure.get('native_framework')==str(FRAMEWORK)
        and set(closure.get('files',{}))==expected_names
        and all(digest(BACKFILL_SUPPORT/name)==sha for name,sha in closure['files'].items()),
        'New qualifier source closure must match its immutable registered task')
    helper_binding=q.get('qualification_helper',{})
    require(set(helper_binding)=={'path','sha256'} and helper_binding['path']==str(helper),
        'Exact new qualification helper locator required')
    packet(helper,ROOT/'maintenance',helper_binding['sha256'],200_000)
    proof=slurm.completed_native_dependency(ROOT,binding['job_id'])
    row=(proof or {}).get('native_records',{}).get(binding['job_id'],{})
    require(row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0'
        and type(row.get('cpus')) is int and row['cpus']==1 and type(row.get('gpus')) is int and row['gpus']==0
        and type(row.get('elapsed_seconds')) is int and 0<row['elapsed_seconds']<=3600,
        'Positive finalized new one-CPU native qualification required')
    paths={value['path']:value['sha256'] for value in expected_sources.values()}
    paths.update({binding['path']:binding['sha256'],suite['source']['path']:suite['source']['sha256'],
        suite['junit']['path']:suite['junit']['sha256'],helper_binding['path']:helper_binding['sha256']})
    paths[str(asset_path)]=BACKFILL_ASSET_MANIFEST_SHA
    for name,sha in assets.items():
        asset=BACKFILL_SUPPORT/'qualification-assets'/name;packet(asset,ROOT/'maintenance',sha,200_000);paths[str(asset)]=sha
    for item in regression:paths[item['junit']['path']]=item['junit']['sha256']
    expected_pins={str(BACKFILL_SUPPORT/name):sha for name,sha in closure['files'].items()}
    expected_pins.update({str(ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'):assets['native-accounting-read-guard-20261021.py'],
        str(ROOT/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'):assets['controller-primary-repair-sixth-20261016.py']})
    require(type(provider_paths) is dict and q.get('source_pins')=={**expected_pins,**provider_paths},
        'Exact full new and preserved provider qualification source closure required')
    paths.update(q['source_pins'])
    evidence_paths,fixture=backfill_evidence(q);paths.update(evidence_paths)
    return paths,proof


def policy_guard(context):
    provider_policy_guard(context)
    top=json_packet(context['backfill_policy_binding'],ROOT/'maintenance')
    require(top==context['backfill_policy'] and top['policy_source']=={'path':str(Path(__file__)),'sha256':digest(Path(__file__))},
        'New backfill policy source/descriptor changed')
    proof=slurm.completed_native_dependency(ROOT,top['backfill_qualification']['job_id'])
    require(proof is not None and proof['native_records']==context['backfill_qualification_completion']['native_records'],
        'New backfill qualification selected final native proof changed')


def fresh_pair(base,*,clock=time.time):
    try:
        budget_path=ROOT/'state/storage-budget.json';detail_path=ROOT/'state/storage-entry-sizes.json'
        raw,budget_sha=packet(budget_path,ROOT/'state');value=strict(raw);now=clock()
        measured=value.get('measured_unix');captured=datetime.fromisoformat(value.get('captured_utc','').replace('Z','+00:00'))
        require(type(measured) in (int,float) and math.isfinite(measured) and captured.tzinfo is not None
            and 0<=now-measured<21600 and 0<=now-captured.timestamp()<21600,'Original global quota clocks are stale or invalid')
        entry=value.get('exact_entry_measurements',{})
        require(value.get('status')=='measured' and value.get('root')==str(ROOT) and value.get('personal_uid')==3164768
            and type(value['personal_uid']) is int and entry.get('receipt_path')==str(detail_path)
            and entry.get('method')=='owned_regular_length','Exact original owned global census required')
        raw_detail,detail_sha=packet(detail_path,ROOT/'state',entry['receipt_sha256'],512*1024**2);detail=strict(raw_detail)
        require(detail.get('status')=='measured' and detail.get('root')==str(ROOT) and detail.get('method')=='owned_regular_length'
            and type(detail.get('count')) is int and detail['count']==entry.get('count')==value.get('study_files')==len(detail.get('items',[]))
            and type(detail.get('bytes')) is int and detail['bytes']==entry.get('bytes')==value.get('study_bytes')
            and sum(item['bytes'] for item in detail['items'])==detail['bytes'],'Complete global census totals required')
        names=[item.get('relative_path') for item in detail['items']]
        require(all(type(name) is str for name in names) and names==sorted(set(names)),'Unique sorted complete global inventory required')
        from horsehair.storage_artifacts import validate_regular_length_measurement
        for item in detail['items']:
            validate_regular_length_measurement(item,expected_uid=3164768,max_bytes=6_000_000_000_000,root=ROOT,path=ROOT/item['relative_path'])
        sources=value.get('native_source_hashes',{})
        require(sources and set(value.get('native_source_paths',[]))==set(sources),'Original native quota evidence required')
        for path,sha in sources.items():packet(path,ROOT/'state',sha,1_000_000)
        require(digest(budget_path)==budget_sha and digest(detail_path)==detail_sha,'Global pair changed during outside-lock preparation')
        require(0<=clock()-measured<21600 and 0<=clock()-captured.timestamp()<21600,'Original quota clocks expired during validation')
        return value,{'budget':budget_sha,'entry_sizes':detail_sha}
    except (OSError,ValueError,KeyError,TypeError,OverflowError) as error:
        raise base.PendingStorage('External complete fresh census pending: '+str(error)) from error


def provider_proof(base,context,pair):
    schedule=context['policy']['provider_schedule'];plan=json_packet(schedule['plan'],ROOT/'plans',8_000_000)
    ledger=json_packet(schedule['ledger'],ROOT/'state',8_000_000)
    tasks=plan.get('tasks',[]);require(1<=len(tasks)<=64 and len({t['name'] for t in tasks})==len(tasks),'Finite explicit provider roster required')
    refreshed=False
    for task in reversed(tasks):
        job=ledger.get('tasks',{}).get(task['name'],{}).get('job_id','')
        require(re.fullmatch('[1-9][0-9]*',str(job)),'Exact registered provider native identity required')
        case=PROVIDER_CASE/('native-'+job);path=case/'receipt.json'
        if not path.exists():continue
        # The producer creates this file before its final native completion.
        # Do not parse any possibly partial bytes until genuine final proof.
        proof=slurm.completed_native_dependency(ROOT,job)
        if proof is None and not refreshed:
            with base.registration_guard(context['base_context'],'controller-start'):
                base.admission(context['base_context'],'external-census-producer-'+job,cpus=0,gpus=0,hours=1)
            refreshed=True;proof=slurm.completed_native_dependency(ROOT,job)
        if proof is None:continue
        try:
            raw,receipt_sha=packet(path,ROOT/'maintenance');receipt=strict(raw)
            sweep=json_packet(receipt['sweep'],case)
            if sweep['budget']['sha256']!=pair['budget'] or sweep['entry_sizes']['sha256']!=pair['entry_sizes']:continue
            require(receipt.get('status')=='complete_exact_size_sweep_with_original_conservative_freshness'
                and receipt.get('native_job_id')==job and receipt.get('wrapper_sha256')==schedule['helper']['sha256']
                and receipt.get('source_manifest')==schedule['source_manifest'] and receipt.get('qualification')==schedule['qualification']
                and receipt.get('source_sha256')==SOURCE and receipt.get('config_sha256')==CONFIG
                and all(receipt.get(flag) is False for flag in FALSE_FLAGS),'Positive qualified provider receipt required')
            require(receipt['sweep']['path']==str(case/'sweep-result.json')
                and sweep['budget']['path']==str(case/'original-storage-budget.json')
                and sweep['entry_sizes']['path']==str(case/'original-storage-entry-sizes.json'),'Exact immutable provider pair paths required')
            for key in ('budget','entry_sizes'):packet(sweep[key]['path'],case,pair[key],512*1024**2)
            registration=receipt['allocation']['native_registration']
            require(registration['plan']==schedule['plan'] and registration['ledger']==schedule['ledger']
                and registration['task_name']==task['name'] and registration['native_job_id']==job,'Provider registration differs from qualified schedule')
            _,tail=registered(schedule['plan'],schedule['ledger'],task['name'],registration['script'],job,
                helper=schedule['helper']['path'],hours=6,cpus=8,memory='64G')
            require(len(tail)==26 and len(set(tail[::2]))==13,'Exact complete provider invocation required')
            args=SimpleNamespace(**{flag.removeprefix('--').replace('-','_'):value for flag,value in zip(tail[::2],tail[1::2])})
            packet(schedule['helper']['path'],ROOT/'maintenance',schedule['helper']['sha256'],100_000)
            spec=importlib.util.spec_from_file_location('qualified_external_census_provider',schedule['helper']['path'])
            provider=importlib.util.module_from_spec(spec);spec.loader.exec_module(provider)
            selected=provider.provider_registration(args,job,context['base_context']['config'])
            require(selected==receipt.get('provider_registration') and selected['case_id']==receipt.get('case_id'),
                'Actual finite provider slot/afterok/source/argument bindings differ')
            fields=receipt['allocation']['fields']
            require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
                and fields.get('JobState')=='RUNNING' and fields.get('NumCPUs')=='8' and fields.get('NumNodes') in {'1','1-1'}
                and fields.get('Command')==registration['script']['path'] and fields.get('JobName')=='hhw-'+task['name']
                and provider.memory(fields.get('MinMemoryNode',''))==64*1024**3
                and fields.get('TimeLimit') in {'06:00:00','6:00:00','0-06:00:00'}
                and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields,'Source-bound actual native provider allocation required')
            provider.cpu_only_tres(fields.get('ReqTRES',''));provider.cpu_only_tres(fields.get('AllocTRES',''))
            require(digest(schedule['helper']['path'])==schedule['helper']['sha256'],'Qualified provider changed during proof validation')
            rows=proof['native_records'];row=rows.get(job,{})
            require(row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0' and type(row.get('cpus')) is int and row['cpus']==8
                and type(row.get('gpus')) is int and row['gpus']==0
                and type(row.get('elapsed_seconds')) is int and 0<row['elapsed_seconds']<=21600,'Positive finalized scalar8CPU provider completion required')
            return {'receipt':{'path':str(path),'sha256':receipt_sha},'job_id':job,'completion':proof}
        except FileNotFoundError:continue
    raise base.PendingStorage('Current global census pair has no finalized qualified external producer; native completion is not finalized')


def install(base,context):
    original=base.fresh_census;objects={name:value for name,value in vars(base).items() if callable(value)}
    def fresh_census(c,*,clock=time.time,sleeper=None,popen=None):
        require(c is context['base_context'],'Outer census policy belongs to one verified controller context')
        policy_guard(context)
        record,pair=fresh_pair(base,clock=clock)
        proof=provider_proof(base,context,pair)
        current,current_pair=fresh_pair(base,clock=clock)
        if pair!=current_pair:raise base.PendingStorage('Provider publication changed while validating finalized producer')
        require(all(getattr(base,name) is value for name,value in objects.items() if name!='fresh_census'),
            'Outer policy changed a frozen controller function')
        c['external_census_proof']=proof
        return current
    base.fresh_census=fresh_census
    require(all(getattr(base,name) is value for name,value in objects.items() if name!='fresh_census'),
        'Only the census preparation entry point may be replaced')
    return {'original_fresh_census':original,'installed_fresh_census':fresh_census,'base_functions':objects}


def outer_native(base,fields,job,task_name,script):
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
        and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING'
        and fields.get('Command')==str(script) and fields.get('JobName')=='hhw-'+task_name
        and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'}
        and base.BACKFILL_MIN_SECONDS<=base.seconds(fields.get('TimeLimit',''))<=base.BACKFILL_MAX_SECONDS
        and base.seconds(fields.get('TimeMin',''))==base.BACKFILL_MIN_SECONDS
        and fields.get('MinMemoryNode') in {'64G','65536M'} and 'ArrayJobId' not in fields and 'ArrayTaskId' not in fields
        and all('gpu' not in fields.get(key,'').lower() for key in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerJob','TresPerTask')),
        'Actual outer allocation must be owned scalar1CPU64G8to96h with exact8h TimeMin and registered command/name')


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv);parser=argparse.ArgumentParser(description=__doc__)
    for name in ('policy-descriptor','native-plan','native-ledger','native-script'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('policy-descriptor-sha256','native-task'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--segment',type=int,required=True);args=parser.parse_args(argv)
    binding={'path':str(args.policy_descriptor),'sha256':args.policy_descriptor_sha256};policy=json_packet(binding,ROOT/'maintenance')
    require(set(policy)=={'kind','base_controller','base_descriptor','policy_source','provider_policy','backfill_qualification','runtime_contract'}
        and policy['kind']=='af-controller-backfill-external-census-policy-v1'
        and policy['base_controller']=={'path':str(BACKFILL_BASE),'sha256':BACKFILL_BASE_SHA}
        and policy['policy_source']=={'path':str(Path(__file__)),'sha256':digest(Path(__file__))}
        and policy['runtime_contract']==BACKFILL_RUNTIME,'Exact separate new backfill policy descriptor required')
    json_packet(policy['base_descriptor'],ROOT/'maintenance')
    base=load_base();job=str(native_job_id() or '')
    registered(locator(args.native_plan),locator(args.native_ledger),args.native_task,locator(args.native_script),job,
        helper=Path(__file__),hours=96,cpus=1,memory='64G',argv=argv)
    script_raw,_=packet(args.native_script,ROOT/'plans')
    require(script_raw.decode().splitlines().count('#SBATCH --time-min=08:00:00')==1,'Exact registered 8-hour native backfill directive required')
    require(json_packet(policy['base_descriptor'],ROOT/'maintenance').get('backfill_qualification')==policy['backfill_qualification'],
        'New base descriptor and outer must bind the same separately qualified backfill producer')
    old_binding=policy['provider_policy']
    require(old_binding=={'path':str(ORIGINAL_PROVIDER_POLICY),'sha256':ORIGINAL_PROVIDER_POLICY_SHA},
        'Exact preserved original provider policy locator required')
    provider_policy=json_packet(old_binding,ROOT/'maintenance')
    require(set(provider_policy)=={'kind','base_controller','base_descriptor','policy_source','qualification','provider_schedule'}
        and provider_policy.get('kind')=='af-controller-external-census-policy-v1'
        and provider_policy.get('base_controller')=={'path':str(BASE),'sha256':BASE_SHA}
        and provider_policy.get('policy_source')=={'path':str(ORIGINAL_POLICY_SOURCE),'sha256':ORIGINAL_POLICY_SHA},
        'Preserved original qualified provider policy required')
    schedule=provider_policy['provider_schedule']
    require(set(schedule)=={'plan','ledger','helper','source_manifest','qualification','case_root'}
        and schedule['case_root']==str(PROVIDER_CASE) and schedule['helper']['path']==str(SUPPORT/PROVIDER_NAME)
        and schedule['qualification']==provider_policy['qualification'],'Original finite external provider schedule contract required')
    checked=qualification(provider_policy);new_checked,new_proof=backfill_qualification(policy,provider_paths=checked)
    c=base.load_context(Path(policy['base_descriptor']['path']),policy['base_descriptor']['sha256'],args.segment)
    outer_native(base,c['allocation'],job,args.native_task,args.native_script)
    context={'policy_binding':old_binding,'policy':provider_policy,'base_context':c,'checked_paths':{**checked,**new_checked},
        'backfill_policy_binding':binding,'backfill_policy':policy,'backfill_qualification_completion':new_proof}
    context['qualification_completion']=slurm.completed_native_dependency(ROOT,provider_policy['qualification']['job_id'])
    context['checked_paths'].update({binding['path']:binding['sha256'],old_binding['path']:old_binding['sha256'],
        policy['base_descriptor']['path']:policy['base_descriptor']['sha256']})
    install(base,context);policy_guard(context)
    return base.run(c)


if __name__=='__main__':main()
