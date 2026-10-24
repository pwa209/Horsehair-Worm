"""Freeze the separate pending-only handoff task; never submit a native job."""
from pathlib import Path
import argparse
import json
import re
import shlex
import sys
import xml.etree.ElementTree as ET

from horsehair import slurm
from horsehair.core import canonical_hash,digest,read_json,source_identity

HERE=Path(__file__).resolve().parent
ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-af-pending-handoff-support-v1-20261024'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SELF='handoff-f2-pending-backfill-v1-20261024.py'
TEST='test_f2_pending_handoff_v1_20261024.py'
CANARY='run_emitted_descriptor_canary.py'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PHASE='study-af-f2-pending-handoff-v1-20261024'


def require(value,message):
    if not value:raise ValueError(message)


def exclusive(path,value):
    with Path(path).open('x',encoding='utf8',newline='\n') as stream:
        json.dump(value,stream,indent=2,sort_keys=True);stream.write('\n')


def source_manifest(junit,backfill_manifest):
    raw=Path(junit).read_bytes();require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Ordinary local contract JUnit required')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'));names=sorted(node.get('classname','').split('.')[-1]+'::'+node.get('name','') for node in cases)
    require(len(cases)==len(set(names))==25 and not any(list(node.iter(key)) for node in cases for key in ('failure','error','skipped')),
        'All exact 25 local contract cases must pass before source freeze')
    package=read_json(backfill_manifest)
    require(package.get('kind')=='f2-backfill-controller-transport-source-manifest-v1' and len(package.get('files',{}))==16,
        'Exact frozen backfill package source manifest required')
    return {'kind':'f2-pending-handoff-source-manifest-v1',
        'files':{name:digest(HERE/name) for name in (SELF,TEST,CANARY,Path(__file__).name)},
        'tests':{'count':25,'roster_sha256':canonical_hash(names)},'backfill_package':package}


def freeze_plan(manifest_path,report):
    require(HERE==SUPPORT,'Freeze only in the exact native pending-handoff support directory')
    manifest_path=Path(manifest_path);report=Path(report);manifest=read_json(manifest_path);q=read_json(report)
    for name,sha in manifest['files'].items():require(digest(SUPPORT/name)==sha,'Actual frozen handoff source changed')
    qid=q.get('slurm_job_id');require(re.fullmatch('[1-9][0-9]*',str(qid)) and q.get('status')=='complete_f2_backfill_controller_qualification'
        and q.get('returncode')==0,'Positive separately completed native backfill qualification report required')
    proof=slurm.completed_native_dependency(ROOT,qid);require(proof is not None,'Positive persisted COMPLETED/0:0 backfill qualifier accounting required before registration')
    config=read_json(FRAMEWORK/'configs/study.json');identity=source_identity(FRAMEWORK)
    require(identity['sha256']==SOURCE and canonical_hash(config)=='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35',
        'Exact frozen administration framework and original configuration required')
    directory=ROOT/'plans'/PHASE;ledger=ROOT/'state'/(PHASE+'-submission.json')
    require(not directory.exists() and not ledger.exists(),'Fresh one-shot handoff plan and ledger required; no replay')
    directory.mkdir(mode=0o700);name='af-f2-pending-handoff-v1';script=directory/(name+'.sbatch')
    command=shlex.join([sys.executable,'-s','-u',str(SUPPORT/SELF),
        '--native-plan',str(directory/'plan.json'),'--native-ledger',str(ledger),'--native-task',name,'--native-script',str(script),
        '--source-manifest',str(manifest_path),'--source-manifest-sha256',digest(manifest_path),
        '--qualification-report',str(report),'--qualification-report-sha256',digest(report)])
    text=slurm._script(config,FRAMEWORK,name,command,cpus=1,memory='16G',hours=1,gpu=False)
    text=text.replace('#SBATCH --time=1:00:00\n','#SBATCH --time=1:00:00\n#SBATCH --chdir='+str(FRAMEWORK.resolve())+'\n',1)
    text=text.replace('set -euo pipefail','set -euo pipefail\nexport TZ=UTC\nexport PYTHONDONTWRITEBYTECODE=1',1)
    with script.open('x',encoding='utf8',newline='\n') as stream:stream.write(text)
    task={'name':name,'command':command,'script':str(script),'script_sha256':canonical_hash(text),'depends':[],
        'external_dependencies':[qid],'dependency_kind':'afterok','resources':{'cpus':1,'memory':'16G','hours':1,'gpu':False,'array':None},
        'pending_f2_handoff':{'helper':{'path':str(SUPPORT/SELF),'sha256':digest(SUPPORT/SELF)},
            'source_manifest':{'path':str(manifest_path),'sha256':digest(manifest_path)},'qualification_report':{'path':str(report),'sha256':digest(report)}}}
    old_plan=read_json(ROOT/'plans/study-af-f2-external-census-controllers-v1-20261023/plan.json')
    value={'schema_version':2,'phase':PHASE,'root':str(ROOT),'release_identity':identity,'config_sha256':canonical_hash(config),
        'tasks':[task],'maximum_simultaneous_resources':{'cpus':1,'gpus':0},'cpu_ceiling':256,'gpu_concurrency':4,
        'resource_policy':old_plan['resource_policy'],'scientific_gates_changed':False}
    path=directory/'plan.json';exclusive(path,value)
    return {'plan':str(path),'plan_sha256':canonical_hash(value),'plan_file_sha256':digest(path),'ledger':str(ledger),
        'qualification_job_id':qid,'native_source_manifest':{'path':str(manifest_path),'sha256':digest(manifest_path)}}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--mode',choices=('source-manifest','freeze'),required=True)
    parser.add_argument('--junit',type=Path);parser.add_argument('--backfill-manifest',type=Path)
    parser.add_argument('--source-manifest',type=Path);parser.add_argument('--report',type=Path);args=parser.parse_args()
    if args.mode=='source-manifest':
        require(args.junit is not None and args.backfill_manifest is not None,'Local JUnit and frozen backfill manifest required')
        value=source_manifest(args.junit,args.backfill_manifest)
    else:
        require(args.source_manifest is not None and args.report is not None,'Exact frozen source manifest and native qualification report required')
        value=freeze_plan(args.source_manifest,args.report)
    print(json.dumps(value,sort_keys=True,indent=2))


if __name__=='__main__':main()
