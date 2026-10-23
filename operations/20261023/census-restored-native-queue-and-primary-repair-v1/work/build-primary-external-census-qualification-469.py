"""Build a local source-bound primary qualification registration after freeze.

Rendering requires the author's exact frozen manifest SHA. This builder never
executes a registration, scheduler command, primary controller or census.
"""
from pathlib import Path
import argparse
import ast
import base64
import hashlib
import json
import re
import zlib

R='/scratch/pwa209/horsehair-worm'
AREA=R+'/maintenance/study-primary-external-census-policy-v1-20261023'
CENSUS=R+'/maintenance/study-af-rolling-census-support-v4-20261023'
CENSUS_SHA='1b21742f6ad5b7b48899a4890988da7ddba439798ba8b12a2bad57c7f1898e6e'
QPATH=R+'/maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json'
QSHA='8b0dc612cee64461b991a9dfb9a975cf5afe1389a037b1357244d829f5aca76c'
QJOB='6863966'
PLAN_SHA='a848dfdd01b148b0bf68c59b0fe6a881d8284ce30d49282e157ca88fd17c36cc'
LEDGER_SHA='7eac4140a4a31405360ad001b115517203ce57bcb57de0ed8018e3157cffb192'
BASE_SHA='e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
HELPER='run-primary-external-census-v1-20261023.py'
TEST='test_primary_external_census_v1_20261023.py'
OUTER='run-af-agent-controller-census-policy-v1-20261023.py'
PROVIDER='run-fresh-storage-census-v6-20261023.py'
TEMPLATE=Path('work/hhw-recovery-stage-20261021-394.sh')
TEMPLATE_SHA='8254e96c402b4d8aa43c98d43880a607f2d222408d0f1eb2c351a60277f29e9e'
OUTPUTS=('hhw-recovery-stage-20261021-469.sh','source-manifest.json','qualification-policy.json','registration-local-evidence.json')


def require(value,message):
    if not value:raise ValueError(message)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def encoded(raw):return base64.b64encode(zlib.compress(raw,9)).decode()


def json_bytes(value):return (json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()


def checked(path,expected):
    require(type(expected) is str and re.fullmatch('[0-9a-f]{64}',expected),'Explicit frozen SHA required')
    raw=Path(path).read_bytes();require(sha(raw)==expected,'Frozen source changed: '+str(path));return raw


def source_inputs(directory,manifest_path,expected_sha,baseline_directory,census_directory):
    directory=Path(directory);baseline_directory=Path(baseline_directory);census_directory=Path(census_directory)
    raw=checked(manifest_path,expected_sha);manifest=json.loads(raw)
    require(set(manifest)=={'kind','files','suites'} and manifest['kind']=='primary-external-census-source-manifest-v1'
        and set(manifest['files'])=={HELPER,TEST} and set(manifest['suites'])=={'baseline_pipeline','baseline_budget','primary_policy'},
        'Exact separately frozen primary source/test closure required')
    sources={name:checked(directory/name,h) for name,h in manifest['files'].items()}
    for name,body in sources.items():ast.parse(body,feature_version=(3,11));compile(body,name,'exec')
    expected={'baseline_pipeline':'tests/test_pipeline.py','baseline_budget':'tests/test_budget.py','primary_policy':TEST}
    for scope,name in expected.items():
        item=manifest['suites'][scope]
        require(set(item)=={'source','source_sha256','tests','roster_sha256'} and item['source']==name
            and type(item['tests']) is int and item['tests']>0 and re.fullmatch('[0-9a-f]{64}',str(item['roster_sha256'])),
            'Exact positive primary suite source/count/roster required')
        checked((baseline_directory if scope.startswith('baseline_') else directory)/name,item['source_sha256'])
    require(manifest['suites']['primary_policy']['source_sha256']==manifest['files'][TEST],'Private suite and test source must agree')
    cmanifest=json.loads(checked(census_directory/'source-manifest.json',CENSUS_SHA))
    require(cmanifest.get('kind')=='streaming-census-operational-source-manifest-v2' and len(cmanifest.get('files',{}))==14
        and len(cmanifest.get('suites',{}))==13 and sum(x['tests'] for x in cmanifest['suites'].values())==757,
        'Actual separately qualified fourteen-source/757-test census closure required')
    for name,h in cmanifest['files'].items():
        require(Path(name).name==name,'Confined private basename required');checked(census_directory/name,h)
    checked(census_directory/'run-af-agent-controller-registry-v6-20261022.py',BASE_SHA)
    return raw,manifest,sources,cmanifest


def render(source_directory,manifest_path,expected_manifest_sha,*,baseline_directory=Path('work/primary-framework-d89-source'),census_directory=Path('work/rolling-native-v5')):
    mraw,manifest,sources,census=source_inputs(source_directory,manifest_path,expected_manifest_sha,baseline_directory,census_directory)
    qualification={'path':QPATH,'sha256':QSHA,'job_id':QJOB}
    policy={'kind':'primary-external-census-policy-v1','source_manifest':{'path':AREA+'/source-manifest.json','sha256':sha(mraw)},
        'qualification':None,'census':{'qualification':qualification,
            'policy_source':{'path':CENSUS+'/'+OUTER,'sha256':census['files'][OUTER]},
            'provider_schedule':{'plan':{'path':R+'/plans/recovery-support-20261021-429/plan.json','sha256':PLAN_SHA},
                'ledger':{'path':R+'/state/recovery-support-20261021-429-submission.json','sha256':LEDGER_SHA},
                'helper':{'path':CENSUS+'/'+PROVIDER,'sha256':census['files'][PROVIDER]},
                'source_manifest':{'path':CENSUS+'/source-manifest.json','sha256':CENSUS_SHA},
                'qualification':qualification,'case_root':R+'/maintenance/study-af-rolling-storage-census-v4-20261023'}}}
    praw=json_bytes(policy);policy_sha=sha(praw)
    template=checked(TEMPLATE,TEMPLATE_SHA).decode();line=next(l for l in template.splitlines() if l.startswith('payload='))
    blobs=[n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100]
    require(len(blobs)==1,'One frozen original template payload required')
    payload=json.loads(zlib.decompress(base64.b64decode(blobs[0])))
    require(not payload['after'] and not payload['after_specs'] and payload['native_minutes'] is None,'Original template must have no hidden prerequisites/time override')
    plan=R+'/plans/recovery-support-20261021-469';ledger=R+'/state/recovery-support-20261021-469-submission.json'
    argv=['--mode','qualify','--policy-descriptor',AREA+'/qualification-policy.json','--policy-descriptor-sha256',policy_sha,
        '--native-plan',plan+'/plan.json','--native-ledger',ledger,'--native-task','recovery-469','--native-script',plan+'/recovery-469.sbatch','--segment','0']
    payload.update(id='469',area=AREA,helper=HELPER,helper_sha256=sha(sources[HELPER]),helper_body=encoded(sources[HELPER]),
        cpus=4,memory='64G',hours=1,args=argv,dependency_kind='afterok')
    text=template.replace(line,'payload=json.loads(zlib.decompress(base64.b64decode('+repr(encoded(json.dumps(payload).encode()))+')))',1)
    old=next(l for l in text.splitlines() if l.startswith('for name,item in {'))
    text=text.replace(old,'for name,item in '+repr({TEST:{'data':encoded(sources[TEST]),'sha256':sha(sources[TEST])}})+'.items():',1)
    marker=" with (area/name).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())\n"
    require(text.count(marker)==1,'Exact immutable source staging point required')
    staging=''
    for name,raw in (('source-manifest.json',mraw),('qualification-policy.json',praw)):
        staging+='raw=base64.b64decode('+repr(base64.b64encode(raw).decode())+');assert hashlib.sha256(raw).hexdigest()=='+repr(sha(raw))+'\n'
        staging+="with (area/"+repr(name)+").open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
    text=text.replace(marker,marker+staging,1)
    text=text.replace("area/'administration-registry-read-guard-v4-20261022.py'","root/'maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py'")
    begin=text.index('    # Both previous registrations ended before plan/sbatch publication.');end=text.index('    admission=combined_admission',begin)
    text=text[:begin]+"    assert not (root/'plans/recovery-support-20261021-469').exists() and not (root/'state/recovery-support-20261021-469-submission.json').exists(),'Fresh primary qualification registration required'\n"+text[end:]
    begin=text.index("    census_directory=root/'plans/recovery-support-20261021-375'");end=text.index("    if payload['after_specs']",begin)
    block="""    task['resources']['array']=None
    task['primary_external_census']={'helper':{'path':str(helper),'sha256':payload['helper_sha256']},'policy_descriptor':{'path':str(area/'qualification-policy.json'),'sha256':__POLICY_SHA__},'mode':'qualify'}
    task['external_dependencies']=[__QJOB__]
    from horsehair.slurm import completed_native_dependency
    assert completed_native_dependency(root,__QJOB__) is not None,'Positive completed Q456 is required'
    assert digest(Path(__QPATH__))==__QSHA__ and digest(Path(__CENSUS_MANIFEST__))==__CENSUS_SHA__
    assert digest(Path(__PLAN_PATH__))==__PLAN_SHA__ and digest(Path(__LEDGER_PATH__))==__LEDGER_SHA__,'Released provider graph changed'
    import importlib.util
    op=Path(__OUTER__);assert digest(op)==__OUTER_SHA__
    spec=importlib.util.spec_from_file_location('primary_registration_qualified_census',op);outer=importlib.util.module_from_spec(spec);spec.loader.exec_module(outer)
    outer.qualification(read_json(area/'qualification-policy.json')['census'])
"""
    replacements={'__POLICY_SHA__':policy_sha,'__QJOB__':QJOB,'__QPATH__':QPATH,'__QSHA__':QSHA,
        '__CENSUS_MANIFEST__':CENSUS+'/source-manifest.json','__CENSUS_SHA__':CENSUS_SHA,
        '__PLAN_PATH__':policy['census']['provider_schedule']['plan']['path'],'__PLAN_SHA__':PLAN_SHA,
        '__LEDGER_PATH__':policy['census']['provider_schedule']['ledger']['path'],'__LEDGER_SHA__':LEDGER_SHA,
        '__OUTER__':CENSUS+'/'+OUTER,'__OUTER_SHA__':census['files'][OUTER]}
    for name,value in replacements.items():block=block.replace(name,repr(value))
    text=text[:begin]+block+text[end:]
    body=text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];ast.parse(body,feature_version=(3,11));compile(body,'<primary469-registration>','exec')
    raw=text.encode();require(b'\r' not in raw,'Exact LF registration required')
    evidence={'kind':'primary-external-census-qualification-local-registration-v1','request_id':'469','source_manifest_sha256':sha(mraw),
        'policy_descriptor_sha256':policy_sha,'helper_sha256':sha(sources[HELPER]),'test_sha256':sha(sources[TEST]),'shell_sha256':sha(raw),
        'resources':{'cpus':4,'memory':'64G','hours':1,'gpu':False,'array':None},'external_dependencies':[QJOB],
        'provider_dependency_required':False,'qualification_claimed':False,'scheduler_submitted':False}
    return raw,mraw,praw,json_bytes(evidence)


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-directory',type=Path,default=Path('work'));p.add_argument('--source-manifest',type=Path,required=True)
    p.add_argument('--source-manifest-sha256',required=True);p.add_argument('--output-directory',type=Path,default=Path('work/primary-native-469'))
    p.add_argument('--baseline-directory',type=Path,default=Path('work/primary-framework-d89-source'));p.add_argument('--census-directory',type=Path,default=Path('work/rolling-native-v5'))
    args=p.parse_args(argv);require(not args.output_directory.exists(),'Preserve existing immutable registration output')
    packets=render(args.source_directory,args.source_manifest,args.source_manifest_sha256,baseline_directory=args.baseline_directory,census_directory=args.census_directory)
    args.output_directory.mkdir(mode=0o700)
    for name,raw in zip(OUTPUTS,packets):
        with (args.output_directory/name).open('xb') as stream:stream.write(raw)
    print(packets[-1].decode(),end='')


if __name__=='__main__':main()
