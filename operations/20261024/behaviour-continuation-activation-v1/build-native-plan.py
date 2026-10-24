"""Freeze the one allocated activation job; never submit it."""
from pathlib import Path
import argparse
import importlib.util
import json
import shlex

AREA=Path(__file__).resolve().parent
RUNTIME='run-behaviour-continuation-activation-v1-20261024.py'


def runtime():
    spec=importlib.util.spec_from_file_location('activation_plan_runtime',AREA/RUNTIME)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def render(config,identity,manifest_binding,manifest,renderer):
    m=runtime();m.require(m.canonical(config)==m.CONFIG and identity.get('sha256')==m.SOURCE,'Exact4f source/config required')
    directory=m.ROOT/'plans'/m.PHASE;plan_path=directory/'plan.json';ledger=m.ROOT/'state'/(m.PHASE+'-submission.json')
    script=directory/(m.TASK+'.sbatch')
    flags={'--native-plan':str(plan_path),'--native-ledger':str(ledger),'--native-task':m.TASK,'--native-script':str(script),
        '--source-manifest':manifest_binding['path'],'--source-manifest-sha256':manifest_binding['sha256']}
    command='python -s -u '+shlex.join([str(m.SUPPORT/m.SELF),*[item for pair in flags.items() for item in pair]])
    resources={'cpus':1,'memory':'16G','hours':4,'gpu':False,'array':None}
    text=renderer(config,m.FRAMEWORK,m.TASK,command,**resources)
    task={'name':m.TASK,'command':command,'script':str(script),'script_sha256':m.canonical(text),'resources':resources,
        'depends':[],'dependency_kind':'afterok','external_dependencies':[m.FIRST],
        'behaviour_continuation_activation':{'helper':{'path':str(m.SUPPORT/m.SELF),'sha256':manifest['files'][m.SELF]},
            'source_manifest':manifest_binding,'first_provider_job_id':m.FIRST,'provider_schedule':m.SCHEDULE,'suite':manifest['suite']},
        **m.FALSE_FLAGS}
    plan={'schema_version':2,'phase':m.PHASE,'root':str(m.ROOT),'release':str(m.FRAMEWORK),'release_identity':identity,
        'config_sha256':m.CONFIG,'tasks':[task],'source_manifest':manifest_binding,**m.FALSE_FLAGS}
    return plan,text


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--source-manifest-sha256',required=True);args=parser.parse_args()
    m=runtime();m.require(AREA==m.SUPPORT,'Exact deployed activation namespace required')
    binding={'path':str(m.SUPPORT/'source-manifest.json'),'sha256':args.source_manifest_sha256}
    manifest,pins=m.source_manifest(binding)
    import horsehair
    from horsehair.core import read_json,source_identity
    from horsehair.slurm import _script
    m.require(Path(horsehair.__file__).resolve()==m.FRAMEWORK/'src/horsehair/__init__.py','Exact4f plan interpreter required')
    plan,script=render(read_json(m.FRAMEWORK/'configs/study.json'),source_identity(m.FRAMEWORK),binding,manifest,_script)
    directory=m.ROOT/'plans'/m.PHASE
    m.require(not directory.exists() and not (m.ROOT/'state'/(m.PHASE+'-submission.json')).exists(),'Fresh activation plan and ledger namespace required')
    m.source_guard(pins);directory.mkdir(mode=0o700)
    with (directory/(m.TASK+'.sbatch')).open('x',encoding='utf8',newline='\n') as stream:stream.write(script)
    m.save(directory/'plan.json',plan);m.source_guard(pins)
    print(json.dumps({'status':'activation_plan_written_not_submitted','plan':str(directory/'plan.json'),'plan_sha256':m.canonical(plan),**m.FALSE_FLAGS},sort_keys=True))


if __name__=='__main__':main()
