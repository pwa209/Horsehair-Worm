"""Freeze local/native files and finite plans; this module never submits jobs."""
from pathlib import Path
import argparse
import importlib.util
import json
import re
import shlex
import sys

from horsehair import slurm
from horsehair.core import canonical_hash,digest,read_json,source_identity

HERE=Path(__file__).resolve().parent
ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-af-backfill-controller-support-v1-20261024'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
OLD_PLAN=ROOT/'plans/study-af-f2-external-census-controllers-v1-20261023/plan.json'
OLD_DESCRIPTOR=ROOT/'maintenance/study-af-registry-repair-support-v6-20261022/native-activation-prestart-three-v1/descriptor-prestart-three-v1.json'
OLD_DESCRIPTOR_SHA='9a0706642ac1660fb7f39fe68279ca34bdc10f89812a0c2397608d2a382cf464'
OLD_POLICY=ROOT/'maintenance/study-af-external-census-handoff-v1-20261023/native-6865706/external-census-policy.json'
OLD_POLICY_SHA='2b0e6ebaa0f1b511bca4343d4722d43702cf051f956cce59494ddccae656c517'
BASE_NAME='run-af-agent-controller-backfill-base-v1-20261024.py'
OUTER_NAME='run-af-agent-controller-backfill-policy-v1-20261024.py'
QUALIFIER_NAME='qualify-f2-backfill-controller-v1-20261024.py'


def require(value,message):
    if not value:raise ValueError(message)


def exclusive(path,value):
    with Path(path).open('x',encoding='utf8',newline='\n') as stream:
        json.dump(value,stream,indent=2,sort_keys=True);stream.write('\n')


def exclusive_text(path,value):
    with Path(path).open('x',encoding='utf8',newline='\n') as stream:stream.write(value)


def outer_module():
    spec=importlib.util.spec_from_file_location('backfill_plan_outer',HERE/OUTER_NAME)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def source_manifest():
    """Return exact transport bytes, excluding local test artifacts and caches."""
    names=['build-backfill-native-plans-v1-20261024.py',BASE_NAME,OUTER_NAME,QUALIFIER_NAME,
        'test_f2_backfill_controller_v1_20261024.py','qualification-asset-pins.json']
    assets=read_json(HERE/'qualification-asset-pins.json')
    names += ['qualification-assets/'+name for name in sorted(assets)]
    result={name:digest(HERE/name) for name in names}
    require(all(result['qualification-assets/'+name]==sha for name,sha in assets.items()),'Transport source assets changed')
    return {'kind':'f2-backfill-controller-transport-source-manifest-v1','files':result,
        'native_support':SUPPORT.as_posix(),'native_framework':FRAMEWORK.as_posix()}


def native_sources():
    outer=outer_module()
    require(HERE==SUPPORT and digest(SUPPORT/BASE_NAME)==outer.BACKFILL_BASE_SHA,'Use exact frozen native backfill source directory')
    config=read_json(FRAMEWORK/'configs/study.json');identity=source_identity(FRAMEWORK)
    require(identity['sha256']==outer.SOURCE and canonical_hash(config)==outer.CONFIG,'Frozen qualified native framework/configuration differs')
    original=read_json(OLD_PLAN)
    require(original['release_identity']['sha256']==outer.SOURCE and original['config_sha256']==outer.CONFIG,
        'Original source-bound resource policy plan required')
    return outer,config,identity,original['resource_policy']


def freeze_plan(config,identity,resource_policy,phase,tasks,**metadata):
    require(re.fullmatch('[a-z0-9][a-z0-9-]{0,119}',phase),'Finite ordinary phase name required')
    directory=ROOT/'plans'/phase;ledger=ROOT/'state'/(phase+'-submission.json')
    require(not directory.exists() and not ledger.exists(),'Fresh immutable plan and ledger only; no automatic replay')
    directory.mkdir(mode=0o700)
    for task in tasks:
        text=task.pop('_script_text');exclusive_text(task['script'],text)
    plan={'schema_version':2,'phase':phase,'root':str(ROOT),'config_sha256':canonical_hash(config),
        'release_identity':identity,'tasks':tasks,'maximum_simultaneous_resources':{'cpus':1,'gpus':0},
        'cpu_ceiling':256,'gpu_concurrency':4,'resource_policy':resource_policy,'scientific_gates_changed':False,**metadata}
    path=directory/'plan.json';exclusive(path,plan)
    return {'plan':str(path),'ledger':str(ledger),'plan_sha256':canonical_hash(plan),
        'plan_file_sha256':digest(path),'tasks':tasks}


def script(config,name,command,*,hours,backfill=False):
    text=slurm._script(config,FRAMEWORK,name,command,cpus=1,memory='64G',hours=hours,gpu=False)
    text=text.replace('set -euo pipefail','set -euo pipefail\nexport TZ=UTC\nexport PYTHONDONTWRITEBYTECODE=1',1)
    if backfill:
        needle='#SBATCH --time=96:00:00\n'
        require(text.count(needle)==1,'Exact conservative 96-hour script required')
        text=text.replace(needle,needle+'#SBATCH --time-min=08:00:00\n',1)
    return text


def qualification_plan(phase='study-af-f2-backfill-qualification-v1-20261024'):
    outer,config,identity,policy=native_sources();name='af-f2-backfill-qualification-v1'
    directory=ROOT/'plans'/phase;ledger=ROOT/'state'/(phase+'-submission.json');path=directory/(name+'.sbatch')
    command=shlex.join([sys.executable,'-s','-u',str(SUPPORT/QUALIFIER_NAME),
        '--native-plan',str(directory/'plan.json'),'--native-ledger',str(ledger),'--native-script',str(path),'--native-task',name])
    text=script(config,name,command,hours=1)
    task={'name':name,'command':command,'script':str(path),'script_sha256':canonical_hash(text),'_script_text':text,
        'depends':[],'external_dependencies':[],'dependency_kind':'afterok',
        'resources':{'cpus':1,'memory':'64G','hours':1,'gpu':False,'array':None},
        'backfill_qualification_sources':source_manifest()}
    return freeze_plan(config,identity,policy,phase,[task],runtime_contract=outer.BACKFILL_RUNTIME,
        qualification_helper={'path':str(SUPPORT/QUALIFIER_NAME),'sha256':digest(SUPPORT/QUALIFIER_NAME)})


def controller_plan(report,*,handoff_job_id,area,phase='study-af-f2-backfill-controllers-v1-20261024'):
    """Caller owns permanent mutex, admission, pending retirement, and release."""
    require(re.fullmatch('[1-9][0-9]*',str(handoff_job_id)),'Actual finite native handoff identity required')
    outer,config,identity,resource_policy=native_sources()
    report=Path(report);qualification=read_json(report)
    qbinding={'path':str(report),'sha256':digest(report),'job_id':qualification['slurm_job_id']}
    outer.backfill_qualification({'backfill_qualification':qbinding},provider_paths=outer.qualification(read_json(OLD_POLICY)))
    require(digest(OLD_DESCRIPTOR)==OLD_DESCRIPTOR_SHA and digest(OLD_POLICY)==OLD_POLICY_SHA,'Preserved original descriptors changed')
    area=Path(area).resolve()
    require(area.is_relative_to(SUPPORT) and area!=SUPPORT and area.is_dir(),'Ordinary new support evidence area required')
    descriptor=read_json(OLD_DESCRIPTOR)
    descriptor['controller_sha256']=outer.BACKFILL_BASE_SHA
    descriptor['backfill_qualification']=qbinding
    descriptor['backfill_runtime_contract']=outer.BACKFILL_RUNTIME
    descriptor_path=area/'backfill-base-descriptor.json';exclusive(descriptor_path,descriptor)
    policy={'kind':'af-controller-backfill-external-census-policy-v1',
        'base_controller':{'path':str(SUPPORT/BASE_NAME),'sha256':outer.BACKFILL_BASE_SHA},
        'base_descriptor':{'path':str(descriptor_path),'sha256':digest(descriptor_path)},
        'policy_source':{'path':str(SUPPORT/OUTER_NAME),'sha256':digest(SUPPORT/OUTER_NAME)},
        'provider_policy':{'path':str(OLD_POLICY),'sha256':OLD_POLICY_SHA},'backfill_qualification':qbinding,
        'runtime_contract':outer.BACKFILL_RUNTIME}
    policy_path=area/'backfill-policy.json';exclusive(policy_path,policy)
    tasks=[];previous=None;directory=ROOT/'plans'/phase;ledger=ROOT/'state'/(phase+'-submission.json')
    for index in range(4):
        name='af-f2-backfill-v1-controller-'+str(index);path=directory/(name+'.sbatch')
        command=shlex.join([sys.executable,'-s','-u',str(SUPPORT/OUTER_NAME),
            '--policy-descriptor',str(policy_path),'--policy-descriptor-sha256',digest(policy_path),'--segment',str(index),
            '--native-plan',str(directory/'plan.json'),'--native-ledger',str(ledger),'--native-task',name,'--native-script',str(path)])
        text=script(config,name,command,hours=96,backfill=True)
        tasks.append({'name':name,'command':command,'script':str(path),'script_sha256':canonical_hash(text),'_script_text':text,
            'depends':[previous] if previous else [],'external_dependencies':[] if previous else [str(handoff_job_id)],
            'dependency_kind':'afterany' if previous else 'afterok',
            'resources':{'cpus':1,'memory':'64G','hours':96,'gpu':False,'array':None},
            'backfill_runtime_contract':outer.BACKFILL_RUNTIME,'backfill_qualification':qbinding});previous=name
    return freeze_plan(config,identity,resource_policy,phase,tasks,handoff_native_job_id=str(handoff_job_id),
        descriptor_path=str(policy_path),descriptor_sha256=digest(policy_path),backfill_qualification=qbinding,
        runtime_contract=outer.BACKFILL_RUNTIME,original_plan=str(OLD_PLAN),
        controller_submission_must_be_held=True,old_pending_terminal_proof_required_before_release=True)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=('source-manifest','qualification-plan','controller-plan'),required=True)
    parser.add_argument('--report',type=Path);parser.add_argument('--handoff-job-id');parser.add_argument('--area',type=Path)
    args=parser.parse_args(argv)
    if args.mode=='source-manifest':value=source_manifest()
    elif args.mode=='qualification-plan':value=qualification_plan()
    else:
        require(args.report is not None and args.handoff_job_id is not None and args.area is not None,'Report, native handoff identity and fresh evidence area required')
        value=controller_plan(args.report,handoff_job_id=args.handoff_job_id,area=args.area)
    print(json.dumps(value,indent=2,sort_keys=True))


if __name__=='__main__':main()
