"""Read-only census qualification bridge in the exact separate 4f interpreter."""
from pathlib import Path
import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import runpy
import time

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
CAPSULE=ROOT/'maintenance/study-behaviour-full-cohort-continuation-v2-20261024'
SUPPORT=ROOT/'maintenance/behaviour-full-cohort-continuation-support-v2-20261024'
SELF='verify-census-provider-v1-20261024.py'
CONTEXT=CAPSULE/'run-full-cohort-continuation-context-v2-20261024.py'
CONTEXT_SHA='b90861f983c8318d7968b8e56ab0a368932f611369880a9f05e41f7556dfeb24'
PROVIDER=ROOT/'maintenance/study-census-resilience-support-v1-20261024/run-fresh-storage-census-resilience-v1-20261024.py'
PROVIDER_SHA='94ac29d11da50412ba3c1f9269ed5762224f93635cd1fae505bfabc5f9b63a93'
PROVIDER_MANIFEST={'path':str(PROVIDER.with_name('source-manifest.json')),
    'sha256':'dbe6c589a7dda4e8d54cc041ddee1dfa77f7804d67e290efcd396c7108b6f152'}
FALSE_FLAGS={'numerical_work_executed':False,'canonical_state_changed':False,'artifacts_deserialized':False,
    'scientific_result':False,'scientific_fit':False,'complete_declared_workload':False,'admission_authority':False,
    'original_H1_H2_H3_gates_cleared':False,'native_gpu_qualified':False,'second_derivatives_supported':False,
    'whole_cohort_objective_qualified':False,'optimizer_convergence_qualified':False,'MALA_acceptance_qualified':False}


def require(value,message):
    if not value:raise ValueError(message)


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def ordinary(path):
    path=Path(path)
    require(path.is_absolute() and path==path.resolve() and path.is_relative_to(ROOT)
        and path!=ROOT and all(not p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(ROOT)),
        'Ordinary exact personal metadata path required')
    require(path.is_file(),'Existing regular metadata file required')
    return path


def strict(path,pin,maximum=2_000_000):
    path=ordinary(path);require(re.fullmatch('[0-9a-f]{64}',str(pin)) and path.stat().st_size<=maximum,
        'Pinned bounded JSON required')
    with path.open('rb') as stream:raw=stream.read(maximum+1)
    require(len(raw)<=maximum and hashlib.sha256(raw).hexdigest()==pin,'Metadata bytes changed')
    def unique(pairs):
        value={}
        for key,item in pairs:
            require(key not in value,'Duplicate metadata key');value[key]=item
        return value
    value=json.loads(raw,object_pairs_hook=unique,parse_constant=lambda value:(_ for _ in ()).throw(ValueError('Nonfinite metadata refused')))
    require(digest(path)==pin,'Metadata changed during read');return value


def validate_request(value,self_sha):
    require(set(value)=={'schema_version','schedule','qualification','source_manifest','context_helper','provider_helper'}
        and value['schema_version']==1 and type(value['schema_version']) is int,'Exact bridge request schema required')
    require(value['context_helper']=={'path':str(CONTEXT),'sha256':CONTEXT_SHA}
        and value['provider_helper']=={'path':str(PROVIDER),'sha256':PROVIDER_SHA},'Exact pinned bridge helpers required')
    schedule=value['schedule']
    require(set(schedule)=={'plan','ledger','helper','source_manifest','qualification','case_root'}
        and schedule['source_manifest']==PROVIDER_MANIFEST and schedule['helper']==value['provider_helper']
        and schedule['qualification']==value['qualification'],'Exact provider schedule and qualification required')
    source=value['source_manifest'];require(source['path']==str(CAPSULE/'source-manifest.json'),'Exact parent source manifest required')
    manifest=strict(source['path'],source['sha256'],200_000)
    require(manifest['files'].get(SELF)==self_sha and manifest['files'].get(CONTEXT.name)==CONTEXT_SHA,
        'Bridge must belong to the exact reviewed parent closure')
    for name,pin in manifest['files'].items():
        require(Path(name).name==name and digest(ordinary(CAPSULE/name))==pin,'Parent source closure changed')
    return manifest


def package_identity(package,source_identity,read_json):
    require(Path(package.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py',
        'Actual separate 4f package origin required')
    framework=source_identity(FRAMEWORK);primary=source_identity(PRIMARY)
    require(framework.get('sha256')==SOURCE and primary.get('sha256')==PRIMARY_SHA,
        'Exact original framework and primary source identities required')
    config=read_json(FRAMEWORK/'configs/study.json')
    canonical=hashlib.sha256(json.dumps(config,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
    require(canonical==CONFIG,'Unchanged original full configuration required')
    return {'package_path':str(Path(package.__file__).resolve()),'framework':framework,'primary':primary,'config_sha256':canonical}


def load(name,path,pin):
    require(digest(ordinary(path))==pin,'Pinned bridge module changed')
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    require(digest(path)==pin,'Bridge module changed during import');return module


def verify(request,self_sha,end,evidence):
    require(type(end) in (int,float) and math.isfinite(end) and time.time()+600<end,'Finite original native cleanup reserve required')
    manifest=validate_request(request,self_sha)
    import horsehair
    from horsehair.core import source_identity,read_json
    origin=package_identity(horsehair,source_identity,read_json)
    context=load('separate_census_context',CONTEXT,CONTEXT_SHA)
    require(digest(ordinary(context.GUARD))==context.GUARD_SHA,'Original accounting guard changed')
    runpy.run_path(str(context.GUARD))['install'](evidence/'accounting-reads')
    from horsehair import repair
    reader=context.bounded_native_reader(repair._repair_native_states,evidence/'bounded-native-reads',end)
    producer=load('separate_census_provider',PROVIDER,PROVIDER_SHA)
    proof=producer.validate_schedule_binding(request['schedule'],request['qualification'],reader)
    require(proof.get('numerical_work_executed') is False and proof.get('canonical_state_changed') is False
        and proof.get('scientific_result') is False and proof.get('admission_authority') is False,
        'Metadata-only original validator proof required')
    for path,pin in proof['checked_paths'].items():require(digest(ordinary(path))==pin,'Provider proof changed')
    require(package_identity(horsehair,source_identity,read_json)==origin,'Separate package/source identity changed')
    require(validate_request(request,self_sha)==manifest,'Parent source closure changed during bridge')
    return {'schema_version':1,'status':'complete_exact_4f_census_qualification_metadata_bridge',
        'source_identity':origin,'provider_helper':request['provider_helper'],'source_manifest':request['source_manifest'],
        'checked_paths':proof['checked_paths'],'qualification':proof,'bridge_sha256':self_sha,**FALSE_FLAGS}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request',type=Path,required=True);parser.add_argument('--request-sha256',required=True)
    parser.add_argument('--response',type=Path,required=True);parser.add_argument('--self-sha256',required=True)
    parser.add_argument('--allocation-end',type=float,required=True);args=parser.parse_args()
    require(Path(__file__).resolve()==CAPSULE/SELF and digest(Path(__file__))==args.self_sha256,
        'Exact reviewed separate bridge source required')
    request=strict(args.request,args.request_sha256);evidence=args.request.parent
    require(evidence.is_relative_to(SUPPORT) and args.response==evidence/'response.json'
        and not args.response.exists() and not args.response.is_symlink(),'Fresh owned bridge response required')
    result=verify(request,args.self_sha256,args.allocation_end,evidence)
    result['request_sha256']=args.request_sha256
    raw=(json.dumps(result,sort_keys=True,indent=2,allow_nan=False)+'\n').encode();require(len(raw)<=2_000_000,'Bounded bridge response required')
    with args.response.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


if __name__=='__main__':main()
