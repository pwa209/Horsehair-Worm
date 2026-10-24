"""Freeze new finite provider/qualification plans; never submit jobs."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import argparse
import hashlib
import importlib.util
import json
import re
import shlex

ROOT=Path('/scratch/pwa209/horsehair-worm')
AREA=ROOT/'maintenance/study-census-resilience-support-v1-20261024'
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
PHASE='study-census-resilience-v1-20261024'
QPHASE='study-census-resilience-qualification-v1-20261024'


def require(value,message):
    if not value:raise ValueError(message)


def canonical(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def render(config,identity,manifest_sha,renderer,*,mode,qualification=None,proof=None,anchor=None):
    require(canonical(config)==CONFIG and identity.get('sha256')==SOURCE and re.fullmatch('[0-9a-f]{64}',manifest_sha),
        'Exact unchanged configuration/source and explicit manifest required')
    require(mode in {'qualification','provider'},'Explicit finite plan kind required')
    phase=QPHASE if mode=='qualification' else PHASE;directory=ROOT/'plans'/phase;ledger=ROOT/'state'/(phase+'-submission.json')
    helper=AREA/('qualify-census-resilience-v1-20261024.py' if mode=='qualification' else 'run-fresh-storage-census-resilience-v1-20261024.py')
    if mode=='provider':
        require(set(qualification)=={'path','sha256','job_id'} and qualification['path']==str(ROOT/'maintenance'/QPHASE/'qualification.json')
            and re.fullmatch('[1-9][0-9]*',qualification['job_id']) and re.fullmatch('[0-9a-f]{64}',qualification['sha256'])
            and proof.get('job_id')==qualification['job_id'] and proof.get('dependency_kind')=='afterok',
            'First provider slot requires completed positive actual new qualification')
        require(isinstance(anchor,datetime) and anchor.tzinfo is not None,'Fresh explicit UTC schedule anchor required')
    tasks=[];scripts={}
    for index in range(1,2 if mode=='qualification' else 65):
        name='census-resilience-qualification-v1' if mode=='qualification' else 'rolling-storage-census-'+str(index).zfill(2)
        script=directory/(name+'.sbatch')
        flags={'--source-manifest':str(AREA/'source-manifest.json'),'--source-manifest-sha256':manifest_sha,
            '--native-plan':str(directory/'plan.json'),'--native-ledger':str(ledger),'--native-task':name,'--native-script':str(script)}
        if mode=='provider':flags.update({'--case-id':'rolling-'+str(index).zfill(2),'--kernel-qualification':qualification['path'],
            '--kernel-qualification-sha256':qualification['sha256'],'--kernel-qualification-job-id':qualification['job_id'],
            '--registry-qualification':str(ROOT/'maintenance/study-af-registry-repair-support-v4-20261021/private-qualification.json'),
            '--registry-qualification-sha256':'1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca','--registry-qualification-job-id':'6699561'})
        command='python -s -u '+shlex.join([str(helper),*[v for pair in flags.items() for v in pair]])
        resources={'cpus':4 if mode=='qualification' else 8,'memory':'16G' if mode=='qualification' else '64G',
            'hours':1 if mode=='qualification' else 6,'gpu':False,'array':None}
        text=renderer(config,FRAMEWORK,name,command,**resources)
        if mode=='provider' and index>1:
            begin=(anchor+timedelta(hours=4*(index-1))).astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
            text=text.replace('set -euo pipefail','#SBATCH --begin='+begin+'\nset -euo pipefail',1)
        require(mode=='qualification' or index>1 or '#SBATCH --begin=' not in text,'First recovery provider must be immediate')
        task={'name':name,'command':command,'script':str(script),'script_sha256':canonical(text),'resources':resources,
            'depends':[] if index==1 else ['rolling-storage-census-'+str(index-1).zfill(2)],
            'dependency_kind':'afterok' if index==1 else 'afterany','external_dependencies':[],
            'completed_external_dependencies':[proof] if mode=='provider' and index==1 else [],
            'scientific_result':False,'admission_authority':False}
        if mode=='provider':
            task['external_dependencies']=[qualification['job_id']] if index==1 else []
            task['rolling_census_provider']={'helper':{'path':str(helper),'sha256':PROVIDER_SHA},
                'source_manifest':{'path':str(AREA/'source-manifest.json'),'sha256':manifest_sha},'case_id':flags['--case-id']}
        else:task['census_resilience_qualification']={'helper':{'path':str(helper),'sha256':QUALIFIER_SHA},
            'source_manifest':{'path':str(AREA/'source-manifest.json'),'sha256':manifest_sha}}
        tasks.append(task);scripts[script.name]=text
    plan={'schema_version':2,'root':str(ROOT),'phase':phase,'release':str(FRAMEWORK),'release_identity':identity,'config_sha256':CONFIG,
        'tasks':tasks,'scientific_result':False,'admission_authority':False,'complete_declared_workload':False}
    if mode=='provider':plan['finite_schedule']={'slots':64,'first_slot_immediate':True,'cadence_seconds':14400,
        'anchor_utc':anchor.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),'last_slot_earliest_offset_hours':252,
        'requested_coverage_is_not_a_freshness_guarantee':True,'maximum_cpu_core_hours':3072,'maximum_concurrent_cpus':8}
    return plan,scripts


PROVIDER_SHA=None
QUALIFIER_SHA=None


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--mode',choices=['qualification','provider'],required=True)
    p.add_argument('--source-manifest-sha256',required=True);p.add_argument('--qualification',type=Path)
    p.add_argument('--qualification-sha256');p.add_argument('--qualification-job-id');a=p.parse_args()
    require(Path(__file__).resolve()==AREA/'build-native-plans.py','Exact reviewed source builder required')
    import horsehair
    from horsehair.core import read_json,source_identity
    from horsehair.slurm import _script,completed_native_dependency
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py','Exact qualified4f interpreter required')
    spec=importlib.util.spec_from_file_location('census_resilience',AREA/'run-fresh-storage-census-resilience-v1-20261024.py')
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);manifest,pins=m.source_manifest(AREA/'source-manifest.json',a.source_manifest_sha256)
    global PROVIDER_SHA,QUALIFIER_SHA;PROVIDER_SHA=pins[str(AREA/m.WRAPPER_NAME)];QUALIFIER_SHA=pins[str(AREA/m.QUALIFIER_NAME)]
    qualification=proof=None
    if a.mode=='provider':
        require(a.qualification is not None and a.qualification_sha256 is not None and a.qualification_job_id is not None,'Actual finalized new qualification required')
        qualification={'path':str(a.qualification),'sha256':a.qualification_sha256,'job_id':a.qualification_job_id}
        require(m.digest(a.qualification)==a.qualification_sha256,'Actual new qualification report changed')
        proof=completed_native_dependency(ROOT,a.qualification_job_id);require(proof is not None,'Qualification afterok proof required')
    plan,scripts=render(read_json(FRAMEWORK/'configs/study.json'),source_identity(FRAMEWORK),a.source_manifest_sha256,_script,
        mode=a.mode,qualification=qualification,proof=proof,anchor=datetime.now(timezone.utc))
    directory=ROOT/'plans'/plan['phase'];require(not directory.exists(),'Fresh one-use finite plan required');directory.mkdir(mode=0o700)
    for name,text in scripts.items():
        with (directory/name).open('x',encoding='utf8',newline='\n') as out:out.write(text)
    with (directory/'plan.json').open('x',encoding='utf8',newline='\n') as out:json.dump(plan,out,sort_keys=True,indent=2);out.write('\n')
    print(json.dumps({'status':'finite_plan_written_not_submitted','plan':str(directory/'plan.json'),'plan_sha256':canonical(plan)},sort_keys=True))


if __name__=='__main__':main()
