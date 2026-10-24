"""Emit a new held consumer chain; this builder never submits or retires jobs."""
from pathlib import Path
import argparse
import copy
import hashlib
import importlib.util
import json
import shlex

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/study-census-resilience-consumers-v1-20261024'
SELF_NAME='run-census-resilience-consumer-v1-20261024.py'
PARENTS={'af':'6950429','primary':'6873101'}
PHASES={kind:'study-'+kind+'-census-resilience-consumers-v1-20261024' for kind in PARENTS}


def require(value,message):
    if not value:raise ValueError(message)


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def runtime():
    path=Path(__file__).with_name(SELF_NAME)
    spec=importlib.util.spec_from_file_location('new_consumer_plan_runtime',path)
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value


def exclusive(path,value):
    with Path(path).open('x',encoding='utf8',newline='\n') as out:json.dump(value,out,sort_keys=True,indent=2,allow_nan=False);out.write('\n')


def binding(kind,*,manifest,qualification,provider_schedule,legacy_policy):
    r=runtime();require(kind in PARENTS,'Exact finite consumer kind required')
    entry,sha=(r.AF,r.AF_SHA) if kind=='af' else (r.PRIMARY,r.PRIMARY_SHA)
    return {'kind':'census-resilience-consumer-binding-v1','consumer_kind':kind,'source_manifest':manifest,
        'qualification':qualification,'provider_schedule':provider_schedule,'legacy_entry':{'path':str(entry),'sha256':sha},
        'legacy_policy':legacy_policy,'afterany_parent':PARENTS[kind],'flags':r.FALSE_FLAGS}


def render(old_plan,kind,binding_locator,binding_value,manifest,*,script_reader):
    """Preserve old invocation and task metadata except new registration paths.

    The old parent is a dependency, never a cancellation target for this module.
    Root separately establishes quiescent retirement and submits/release proofs.
    """
    r=runtime();require(kind in PARENTS and binding_value['consumer_kind']==kind and binding_value['afterany_parent']==PARENTS[kind],
        'Exact old active parent and finite consumer kind required')
    require(binding_locator['path']==str(SUPPORT/(kind+'-consumer-binding.json')),'Exact new binding path required')
    tasks=old_plan.get('tasks');require(type(tasks) is list and len(tasks)==4,'Exactly four inherited finite controller segments required')
    phase=PHASES[kind];directory=ROOT/'plans'/phase;plan_path=directory/'plan.json';ledger_path=ROOT/'state'/(phase+'-submission.json')
    own=str(SUPPORT/SELF_NAME);entry=binding_value['legacy_entry']['path'];new_tasks=[];scripts={}
    old_names=[t['name'] for t in tasks];require(len(set(old_names))==4,'Distinct old finite controller names required')
    for index,old in enumerate(tasks):
        text=script_reader(old['script']);require(canonical(text)==old['script_sha256'] and text.count(old['command'])==1,
            'Original script and exact single source-bound invocation required')
        command=shlex.split(old['command']);require(command.count(entry)==1 and own not in command,'Exact unchanged old helper invocation required')
        boundary=command.index(entry);tail=command[boundary+1:]
        require(len(tail)%2==0 and len(set(tail[::2]))==len(tail)//2,'Unique complete inherited native arguments required')
        flags=dict(zip(tail[::2],tail[1::2]));require(flags.get('--segment')==str(index)
            and flags.get('--policy-descriptor')==binding_value['legacy_policy']['path']
            and flags.get('--policy-descriptor-sha256')==binding_value['legacy_policy']['sha256'],
            'Exact old segment and unmodified original policy locator required')
        require(old.get('resources')=={'cpus':1,'memory':'64G','hours':96 if kind=='af' else 72,'gpu':False,'array':None},
            'Original finite conservative controller reservation required')
        require(old.get('depends')==([] if index==0 else [old_names[index-1]]),'Original ordered four-controller chain required')
        name=kind+'-resilient-census-controller-'+str(index);script=directory/(name+'.sbatch')
        replacements={'--native-plan':str(plan_path),'--native-ledger':str(ledger_path),'--native-task':name,'--native-script':str(script)}
        require(all(flag in flags for flag in replacements),'Every inherited native registration argument is required')
        for flag,value in replacements.items():tail[tail.index(flag)+1]=value
        envelope=[own,'--consumer-binding',binding_locator['path'],'--consumer-binding-sha256',binding_locator['sha256'],'--legacy-entry',entry]
        new_command=shlex.join(command[:boundary]+envelope+tail)
        new_text=text.replace(old['command'],new_command)
        # These are scheduler output/name paths only; the exact old command was
        # replaced above before any script path/name changes are considered.
        require(text.splitlines().count('#SBATCH --job-name=hhw-'+old['name'])==1,
            'Exact inherited scheduler job name required')
        new_text=new_text.replace('#SBATCH --job-name=hhw-'+old['name'],'#SBATCH --job-name=hhw-'+name)
        for key in ('--output=','--error='):
            lines=new_text.splitlines(keepends=True)
            new_text=''.join(('#SBATCH '+key+str(directory/(name+('.out' if key=='--output=' else '.err')))+'\n')
                if line.startswith('#SBATCH '+key) else line for line in lines)
        require(not any(line.startswith('#SBATCH --dependency') for line in new_text.splitlines()),
            'Dependencies must be registered from the new explicit plan, never stale script directives')
        if '#SBATCH --hold' not in new_text.splitlines():
            require(new_text.startswith('#!'),'Original batch interpreter line required')
            first,rest=new_text.split('\n',1);new_text=first+'\n#SBATCH --hold\n'+rest
        require(new_text.splitlines().count('#SBATCH --hold')==1,'Every new controller must initially be held')
        if kind=='af':require(new_text.splitlines().count('#SBATCH --time-min=08:00:00')==1,'Exact original eight-hour TimeMin required')
        task=copy.deepcopy(old);task.update(name=name,command=new_command,script=str(script),script_sha256=canonical(new_text),
            depends=[] if index==0 else [new_tasks[-1]['name']],dependency_kind='afterany',
            external_dependencies=[PARENTS[kind]] if index==0 else [])
        task['census_resilience_consumer']={'entry':{'path':own,'sha256':manifest['files'][SELF_NAME]},
            'binding':binding_locator,'consumer_kind':kind}
        new_tasks.append(task);scripts[str(script)]=new_text
    plan=copy.deepcopy(old_plan);plan.update(phase=phase,tasks=new_tasks)
    plan['census_resilience_replacement']={'binding':binding_locator,'source_manifest':binding_value['source_manifest'],
        'afterany_old_parent':PARENTS[kind],'all_new_jobs_initially_held':True,'parallel_controller_authorized':False,
        'old_plan_sha256':canonical(old_plan),'retirement_is_external_and_requires_positive_proof':True}
    return {'plan':plan,'plan_path':str(plan_path),'ledger_path':str(ledger_path),'scripts':scripts}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--kind',choices=tuple(PARENTS),required=True)
    for name in ('old-plan','binding','source-manifest'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--binding-sha256',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args(argv)
    r=runtime();value,manifest,pins=r.check_binding(a.binding,a.binding_sha256)
    require(r.locator(a.source_manifest)==value['source_manifest'],'Exact qualified consumer manifest required')
    old=json.loads(a.old_plan.read_text());result=render(old,a.kind,r.locator(a.binding),value,manifest,script_reader=lambda x:Path(x).read_text())
    # Only emit into the new native phase after read-only source verification.
    require(a.output==Path(result['plan_path']).parent and not a.output.exists(),'Fresh exact new plan directory required')
    r.source_guard(pins);a.output.mkdir(mode=0o700)
    for path,text in result['scripts'].items():
        with Path(path).open('x',encoding='utf8',newline='\n') as out:out.write(text)
    exclusive(result['plan_path'],result['plan']);r.source_guard(pins)


if __name__=='__main__':main()
