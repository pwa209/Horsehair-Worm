"""Create an immutable one-job plan; never submit or execute the continuation."""
from pathlib import Path
import argparse
import hashlib
import importlib.util
import json
import re
import shlex

ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-caebc9c1dc77-8495a3d98551'
SOURCE='8495a3d985515bf71f476295da87e663a1e764864ddce47a06794e7e11d88f94'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
CAPSULE=ROOT/'maintenance/study-behaviour-full-cohort-continuation-v2-20261024'
PLAN_DIR=ROOT/'plans/behaviour-full-cohort-continuation-v2-20261024'
TASK='behaviour-full-cohort-continuation-v2'
CONTEXT=CAPSULE/'run-full-cohort-continuation-context-v2-20261024.py'
CONTEXT_SHA='b90861f983c8318d7968b8e56ab0a368932f611369880a9f05e41f7556dfeb24'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v2/policy.json'
POLICY_SHA='5dac85400e709525458cc34a2edc2bb40b029eb743078672cd721ab444c17015'
QUALIFICATION=ROOT/'maintenance/study-af-execution-support-v2-20261021/qualification.json'
QUALIFICATION_SHA='f4f7684e3b04fde1a61c2ce20baec430a1dfe56b9e055b75cfaabc78cbdc7c13'
RECORD_PROBE_JOB='6943757'
RECORD_PROBE_RECEIPT=ROOT/'maintenance/behaviour-full-record-adjoint-v5prefix-support-v1-20261024/receipt.json'
RECORD_PROBE_RECEIPT_SHA='76126379b9f292d2df77779223af3e947d40e81222ed7bb24c18d4230d1315c7'
RECORD_PROBE_SOURCE=ROOT/'maintenance/study-behaviour-adjoint-continuation-v1-20261024'
RECORD_PROBE_SOURCE_MANIFEST_SHA='8a409623d93ede64b5f980dab2160bda0745ebd240a3255e70498d26cc34d794'
RECORD_PROBE_HELPER_SHA='459e0aa542eabd824dfe6f9deacf5e0e702b0108e5816760024c155823e1bb2f'
RECORD_PROBE_BINDING={'job_id':RECORD_PROBE_JOB,'receipt':str(RECORD_PROBE_RECEIPT),'receipt_sha256':RECORD_PROBE_RECEIPT_SHA,
    'source_manifest':{'path':str(RECORD_PROBE_SOURCE/'source-manifest.json'),'sha256':RECORD_PROBE_SOURCE_MANIFEST_SHA},
    'helper':{'path':str(RECORD_PROBE_SOURCE/'run-full-record-adjoint-v5prefix-v1-20261024.py'),'sha256':RECORD_PROBE_HELPER_SHA}}
OPERATIONAL_LIMITS={'requested_hours':24,'maximum_child_seconds':85800,
    'minimum_cleanup_seconds':600,'child_memory_ceiling_bytes':48*1024**3}


def require(value,message):
    if not value:raise ValueError(message)


def canonical(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def render(config,release_identity,bundle_sha,verifier_job,receipt_sha,verifier_bundle_sha,census_provider,proofs,script_renderer):
    require(canonical(config)==CONFIG and release_identity.get('sha256')==SOURCE,
        'Exact qualified framework source and unchanged full configuration required')
    require(re.fullmatch('[1-9][0-9]*',verifier_job) and verifier_job not in {'6865674','6689965',RECORD_PROBE_JOB},
        'Independent positive verifier job required')
    require(all(re.fullmatch('[0-9a-f]{64}',pin) for pin in (bundle_sha,receipt_sha,verifier_bundle_sha)),
        'Explicit source and receipt hashes required')
    provider_job=census_provider.get('job_id') if isinstance(census_provider,dict) else None
    require(isinstance(provider_job,str) and re.fullmatch('[1-9][0-9]*',provider_job)
        and provider_job not in {verifier_job,'6865674','6689965',RECORD_PROBE_JOB,'6975379'},
        'Explicit distinct actual first new census provider required')
    require(all(re.fullmatch('[0-9a-f]{64}',str(census_provider.get(key,{}).get('sha256')))
        for key in ('schedule','receipt')),'Pinned actual provider schedule and completed receipt required')
    require(isinstance(proofs,list) and len(proofs)==4 and {p.get('job_id') for p in proofs}=={verifier_job,'6689965',RECORD_PROBE_JOB,provider_job}
        and all(p.get('dependency_kind')=='afterok' for p in proofs),
        'Positive persisted verifier, framework, record-probe and first new census dependencies required')
    helper=CAPSULE/'run-full-cohort-continuation-v2-20261024.py'
    script_path=PLAN_DIR/(TASK+'.sbatch')
    test=CAPSULE/'test_full_record_adjoint_v3_20261021.py'
    arguments=[str(helper),'--qualification',str(QUALIFICATION),'--qualification-sha256',QUALIFICATION_SHA,
        '--qualification-job-id','6689965','--resource-policy',str(POLICY),'--resource-policy-sha256',POLICY_SHA,
        '--native-script',str(script_path),'--context-helper',str(CONTEXT),'--context-helper-sha256',CONTEXT_SHA,
        '--tests',str(test),'--tests-sha256','6e15c7c00f4ab421cc76e269ad5f1524bc301b9770d0b8efd073b51906e30978',
        '--prefix-receipt',str(ROOT/'maintenance/study-behaviour-supervisor-reconciliation-v1-20261024'/('native-'+verifier_job)/'receipt.json'),
        '--prefix-receipt-sha256',receipt_sha,'--prefix-job-id',verifier_job,
        '--source-manifest-sha256',bundle_sha,'--verifier-source-manifest-sha256',verifier_bundle_sha,
        '--census-provider-job-id',provider_job,'--census-provider-schedule',census_provider['schedule']['path'],
        '--census-provider-schedule-sha256',census_provider['schedule']['sha256'],
        '--census-provider-receipt-sha256',census_provider['receipt']['sha256']]
    command='python -s -u '+shlex.join(arguments)
    script=script_renderer(config,FRAMEWORK,TASK,command,cpus=1,memory='64G',hours=24,gpu=False,array=None)
    task={'name':TASK,'command':command,'script':str(script_path),'script_sha256':canonical(script),
        'depends':[],'dependency_kind':'afterok','external_dependencies':[],
        'completed_external_dependencies':proofs,
        'resources':{'cpus':1,'memory':'64G','hours':24,'gpu':False,'array':None},
        'operational_limits':dict(OPERATIONAL_LIMITS),
        'completed_record_probe':dict(RECORD_PROBE_BINDING),
        'completed_new_census_provider':census_provider,
        'scientific_result':False,'complete_declared_workload':False,'admission_authority':False}
    plan={'schema_version':2,'phase':'behaviour-full-cohort-continuation-v2-20261024','root':str(ROOT),
        'release':str(FRAMEWORK),'release_identity':release_identity,'config_sha256':CONFIG,
        'source_manifest_sha256':bundle_sha,'tasks':[task],
        'operational_limits':dict(OPERATIONAL_LIMITS),
        'completed_record_probe':dict(RECORD_PROBE_BINDING),
        'completed_new_census_provider':census_provider,
        'original_no_probe_attempt_preserved':'6975379 pending_storage_no_probe_started',
        'original_native_failure_preserved':'6865674 FAILED/1:0',
        'scientific_result':False,'complete_declared_workload':False,'admission_authority':False,
        'registration_and_resource_admission_required':True}
    return plan,script


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-manifest-sha256',required=True)
    parser.add_argument('--verifier-job-id',required=True)
    parser.add_argument('--verifier-receipt-sha256',required=True)
    parser.add_argument('--verifier-source-manifest-sha256',required=True)
    parser.add_argument('--census-provider-job-id',required=True)
    parser.add_argument('--census-provider-schedule',type=Path,required=True)
    parser.add_argument('--census-provider-schedule-sha256',required=True)
    args=parser.parse_args()
    require(re.fullmatch('[1-9][0-9]*',args.verifier_job_id) and args.verifier_job_id not in {'6865674','6689965',RECORD_PROBE_JOB},
        'Independent scalar verifier job required')
    require(Path(__file__).resolve()==CAPSULE/'build-native-plan.py','Exact reviewed native source namespace required')
    helper=CAPSULE/'run-full-cohort-continuation-v2-20261024.py'
    # Loading this module defines functions only; the guarded main never runs.
    spec=importlib.util.spec_from_file_location('reviewed_operational_probe',helper)
    candidate=importlib.util.module_from_spec(spec);spec.loader.exec_module(candidate)
    candidate.verify_capsule(args.source_manifest_sha256)
    require(digest(CONTEXT)==CONTEXT_SHA and digest(POLICY)==POLICY_SHA and digest(QUALIFICATION)==QUALIFICATION_SHA,
        'Existing immutable context/policy/qualification prerequisites required')
    require(digest(RECORD_PROBE_RECEIPT)==RECORD_PROBE_RECEIPT_SHA,'Exact completed record-probe receipt required')
    require(digest(RECORD_PROBE_SOURCE/'source-manifest.json')==RECORD_PROBE_SOURCE_MANIFEST_SHA
        and digest(RECORD_PROBE_SOURCE/'run-full-record-adjoint-v5prefix-v1-20261024.py')==RECORD_PROBE_HELPER_SHA,
        'Exact independently completed record-probe source capsule required')
    receipt=ROOT/'maintenance/study-behaviour-supervisor-reconciliation-v1-20261024'/('native-'+args.verifier_job_id)/'receipt.json'
    require(digest(receipt)==args.verifier_receipt_sha256,'Exact reviewed independent verification receipt required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py',
        'Plan rendering must use the exact qualified maintenance framework')
    from horsehair.core import read_json,source_identity
    from horsehair.slurm import _script,completed_native_dependency
    identity=source_identity(FRAMEWORK)
    config=read_json(FRAMEWORK/'configs/study.json')
    census_provider=candidate.census_provider_evidence(args.census_provider_schedule,args.census_provider_schedule_sha256,
        args.census_provider_job_id)
    proofs=[completed_native_dependency(ROOT,job) for job in (args.verifier_job_id,'6689965',RECORD_PROBE_JOB,args.census_provider_job_id)]
    require(all(proofs),'All four final native afterok proofs are required before any plan write')
    plan,script=render(config,identity,args.source_manifest_sha256,args.verifier_job_id,
        args.verifier_receipt_sha256,args.verifier_source_manifest_sha256,census_provider,proofs,_script)
    require(not PLAN_DIR.exists() and not PLAN_DIR.is_symlink(),'Fresh one-use continuation plan required')
    PLAN_DIR.mkdir(mode=0o700)
    with (PLAN_DIR/(TASK+'.sbatch')).open('x',encoding='utf8',newline='\n') as stream:stream.write(script)
    with (PLAN_DIR/'plan.json').open('x',encoding='utf8',newline='\n') as stream:
        json.dump(plan,stream,sort_keys=True,indent=2,allow_nan=False);stream.write('\n')
    print(json.dumps({'status':'immutable_plan_written_not_submitted','plan':str(PLAN_DIR/'plan.json'),
        'plan_sha256':canonical(plan),'scientific_result':False,'complete_declared_workload':False},sort_keys=True))


if __name__=='__main__':main()
