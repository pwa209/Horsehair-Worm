"""Build one local immutable coordinator envelope from a complete reviewed graph.

The seven real shell inputs and the complete qualified source manifest must
already exist. Missing inputs fail before any output is written. No remote or
scheduler command is executed by this builder.
"""
from pathlib import Path
import argparse
import ast
import base64
import hashlib
import json
import re
import zlib

ROOT='/scratch/pwa209/horsehair-worm'
TEMPLATE_SHA='8254e96c402b4d8aa43c98d43880a607f2d222408d0f1eb2c351a60277f29e9e'
CHILD_IDS=('429','430','431','432','433','434','451')
KIND='finite-reviewed-registration-coordinator-source-manifest-v1'
CLOSURE={'qualify-streaming-census-v1-20261023.py','qualify-streaming-census-v2-20261023.py',
    'registered-snapshot-read-guard-v4-20261022.py','run-af-agent-controller-census-policy-v1-20261023.py',
    'run-fresh-storage-census-v5-20261023.py','run-fresh-storage-census-v6-20261023.py','streaming-storage-census-v1-20261023.py',
    'test_af_agent_controller_census_policy_v1_20261023.py','test_qualify_streaming_census_v1_20261023.py',
    'test_qualify_streaming_census_v2_20261023.py','test_registered_snapshot_read_guard_v4_20261022.py',
    'test_run_fresh_storage_census_v5_20261023.py','test_run_fresh_storage_census_v6_20261023.py','test_streaming_storage_census_v1_20261023.py'}
SCOPES={'baseline_budget','baseline_storage_artifact_cache','baseline_storage_artifacts','baseline_storage_boundary_cache',
    'baseline_storage_snapshot_artifacts','baseline_storage_snapshot_boundary','external_census_policy','kernel','qualifier',
    'registry','repeatable_wrapper','rolling_qualifier','wrapper'}

def sha(raw):return hashlib.sha256(raw).hexdigest()
def require(value,message):
    if not value:raise ValueError(message)

def source_constants(raw):
    tree=ast.parse(raw,feature_version=(3,11));values={}
    for node in tree.body:
        if not isinstance(node,ast.Assign) or len(node.targets)!=1 or not isinstance(node.targets[0],ast.Name):continue
        name=node.targets[0].id;value=node.value
        if isinstance(value,ast.Constant):values[name]=value.value
        elif isinstance(value,ast.Call) and isinstance(value.func,ast.Name) and value.func.id=='Path' and len(value.args)==1 and isinstance(value.args[0],ast.Constant):values[name]=value.args[0].value
        elif isinstance(value,ast.BinOp) and isinstance(value.op,ast.Div) and isinstance(value.left,ast.Name) and value.left.id in values and isinstance(value.right,ast.Constant):values[name]=values[value.left.id]+'/'+value.right.value
    require(all(name in values for name in ('ROOT','AREA','HELPER','QPATH','QSUPPORT','SOURCE','CONFIG')),'Explicit source-bound coordinator namespace constants required')
    require(values['ROOT']==ROOT and values['AREA'].startswith(ROOT+'/maintenance/study-af-reviewed-registration-coordinator-')
        and values['QSUPPORT'].startswith(ROOT+'/maintenance/study-af-rolling-census-support-')
        and values['QPATH'].startswith(ROOT+'/maintenance/study-af-rolling-census-qualification-'),'Reviewed ordinary coordinator/source namespaces required')
    return values

def checked_shell(path,ident):
    raw=Path(path).read_bytes();require(len(raw)<=2_000_000 and b'\r' not in raw,'Finite LF reviewed shell required')
    source=raw.decode();body=source.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];ast.parse(body,feature_version=(3,11));compile(body,str(path),'exec')
    lines=[line for line in source.splitlines() if line.startswith('payload=')];require(len(lines)==1,'One static child payload required')
    values=[n.value for n in ast.walk(ast.parse(lines[0])) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100]
    require(len(values)==1,'One bounded encoded child payload required');payload=json.loads(zlib.decompress(base64.b64decode(values[0])))
    require(payload.get('id')==ident,'Reviewed child logical ID mismatch')
    return raw

def collect(directory,helper,qualification_id):
    directory=Path(directory);helper=Path(helper);raw=helper.read_bytes();constants=source_constants(raw)
    require(helper.name==constants['HELPER'] and re.fullmatch('[1-9][0-9]*',qualification_id),'Exact coordinator helper and Q logical ID required')
    text=raw.decode();require('recovery-support-20261021-'+qualification_id in text and 'recovery-'+qualification_id in text,'Coordinator source must require the selected qualification lineage')
    # Gather every input first. In particular, never manufacture a missing451.
    children={ident:checked_shell(directory/('hhw-recovery-stage-20261021-'+ident+'.sh'),ident) for ident in CHILD_IDS}
    manifest_raw=(directory/'source-manifest.json').read_bytes();qualified=json.loads(manifest_raw)
    require(qualified.get('kind')=='streaming-census-operational-source-manifest-v2' and set(qualified.get('files',{}))==CLOSURE and set(qualified.get('suites',{}))==SCOPES,'Exact complete qualified14-source/13-suite manifest required')
    for name,expected in qualified['files'].items():require(sha((directory/name).read_bytes())==expected,'Qualified frozen source changed: '+name)
    require('run-fresh-storage-census-v6-20261023.py' in qualified['files'] and 'qualify-streaming-census-v2-20261023.py' in qualified['files'],'Qualified provider and producer source required')
    files={constants['HELPER']:sha(raw),**{'hhw-recovery-stage-20261021-'+ident+'.sh':sha(body) for ident,body in children.items()}}
    qualification={'path':constants['QPATH'],'plan':ROOT+'/plans/recovery-support-20261021-'+qualification_id+'/plan.json',
        'ledger':ROOT+'/state/recovery-support-20261021-'+qualification_id+'-submission.json','task':'recovery-'+qualification_id,
        'validator':{'path':constants['QSUPPORT']+'/run-fresh-storage-census-v6-20261023.py','sha256':qualified['files']['run-fresh-storage-census-v6-20261023.py'],'function':'kernel_qualification'},
        'source_manifest':{'path':constants['QSUPPORT']+'/source-manifest.json','sha256':sha(manifest_raw)}}
    own_manifest={'kind':KIND,'files':files,'qualification':qualification}
    return raw,children,qualified,constants,own_manifest

BODY=r'''
from pathlib import Path
import ast,base64,hashlib,json,os,re,runpy,shlex,time,zlib
from horsehair.core import read_json,digest,source_identity,canonical_hash,atomic_json
from horsehair.slurm import _script,submit
root=Path('/scratch/pwa209/horsehair-worm')
primary=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
old=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261016-5c3ae6df532d-91c8824f2adb')
__PAYLOAD__
__MANIFEST__
__SOURCES__
framework=Path(payload['framework']);config=read_json(primary/'configs/study.json');identity=read_json(framework/'release.json');science=read_json(primary/'release.json')
assert os.geteuid()==3164768 and source_identity(primary)==science['source_identity'] and source_identity(old)==read_json(old/'release.json')['source_identity']
assert source_identity(framework)==identity['source_identity'] and identity['source_identity']['sha256']==payload['source_sha256']
assert read_json(framework/'configs/study.json')==config and canonical_hash(config)==science['config_sha256']==identity['config_sha256']==payload['config_sha256']
area=Path(payload['area']);assert area==area.resolve() and area.is_relative_to(root/'maintenance') and not area.exists() and not area.is_symlink(),'Immutable coordinator stage cannot be replayed'
area.mkdir(mode=0o700,parents=True,exist_ok=False)
raw=zlib.decompress(base64.b64decode(payload['helper_body']));assert hashlib.sha256(raw).hexdigest()==payload['helper_sha256'];ast.parse(raw)
helper=area/payload['helper']
with helper.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
for name,item in sources.items():
    raw=zlib.decompress(base64.b64decode(item['data']));assert hashlib.sha256(raw).hexdigest()==item['sha256']
    assert name.endswith('.sh') and name in payload['child_names']
    with (area/name).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
assert hashlib.sha256(manifest_raw).hexdigest()==payload['coordinator_manifest_sha256']
with (area/'coordinator-source-manifest.json').open('xb') as stream:stream.write(manifest_raw);stream.flush();os.fsync(stream.fileno())
own_manifest=json.loads(manifest_raw);contract=own_manifest['qualification'];qm=Path(contract['source_manifest']['path'])
assert digest(qm)==contract['source_manifest']['sha256'];qualified=read_json(qm)
for name,expected in qualified['files'].items():assert digest(qm.parent/name)==expected,'Qualified source closure changed'
assert digest(Path(contract['validator']['path']))==contract['validator']['sha256']
gp=root/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py';assert digest(gp)=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
runpy.run_path(str(gp))['install'](area/'submission-native-accounting-reads')
admin=root/'maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py';assert digest(admin)=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
api=runpy.run_path(str(admin));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+1500)
from horsehair import campaign,repair
assert campaign._repair_native_states is repair._repair_native_states
policy=(root/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5')
with api['registration_lock'](area/'submission-registration-lock',time.time()+1500,role='controller-start'):
    directory=root/'plans/recovery-support-20261021-425';ledger_path=root/'state/recovery-support-20261021-425-submission.json'
    assert not directory.exists() and not directory.is_symlink() and not ledger_path.exists() and not ledger_path.is_symlink(),'Fresh coordinator registration required'
    qplan=read_json(contract['plan']);qledger=read_json(contract['ledger']);qname=contract['task']
    assert canonical_hash(qplan)==qledger['plan_sha256'] and qplan['root']==str(root)
    assert qplan['config_sha256']==qledger['config_sha256']==canonical_hash(config) and qplan['release_identity']['sha256']==qledger['source_sha256']==payload['source_sha256']
    qtasks=[task for task in qplan['tasks'] if task['name']==qname];assert len(qtasks)==len(qplan['tasks'])==1
    qt=qtasks[0];qr=qledger['tasks'][qname];qs=Path(qt['script'])
    assert qs==Path(contract['plan']).parent/(qname+'.sbatch') and qs==qs.resolve() and not qs.is_symlink()
    assert qt['resources']=={'cpus':8,'memory':'64G','hours':1,'gpu':False,'array':None} and qr['state']=='submitted'
    assert qt['command'] in qs.read_text() and canonical_hash(qs.read_text())==qt['script_sha256']==qr['script_sha256']
    qhelper=qm.parent/'qualify-streaming-census-v2-20261023.py';qhelper_binding={'path':str(qhelper),'sha256':qualified['files'][qhelper.name]}
    assert qt['rolling_census_qualification']=={'helper':qhelper_binding,'source_manifest':contract['source_manifest']}
    qc=shlex.split(qt['command']);assert qc[:3]==['python','-u',str(qhelper)]
    tail=qc[3:];flags={'--native-plan':contract['plan'],'--native-ledger':contract['ledger'],'--native-task':qname,'--native-script':str(qs),'--source-manifest':str(qm),'--source-manifest-sha256':contract['source_manifest']['sha256']}
    assert len(tail)==2*len(flags) and len(set(tail[::2]))==len(flags) and dict(zip(tail[::2],tail[1::2]))==flags
    qjob=str(qr['job_id']);assert re.fullmatch('[1-9][0-9]*',qjob)
    observed=repair._repair_native_states([qjob],{},owner='pwa209',account='def-ptewarie_cpu');native=observed[qjob]
    assert native['job_id_raw']==qjob and native['user']=='pwa209' and native['account']=='def-ptewarie_cpu' and native['state'] in {'PENDING','CONFIGURING','RUNNING','COMPLETING','COMPLETED'},'Failed or foreign qualification cannot authorize a coordinator'
    if native['state']=='COMPLETED':assert native['exit_code']=='0:0' and native['cpus']==8 and native['elapsed_seconds']>0
    atomic_json(area/'qualification-registration-native.json',{'registration':contract,'native_job_id':qjob,'native_record':native,'positive_final_completion_required_by_coordinator':True,'scientific_result':False})
    admission=campaign.combined_admission(root,config,science['release_id'],area,'submit-expanded-combined-admission',resource_policy=policy,cpus=1,gpus=0,hours=4)
    quota=__import__('subprocess').run(['lfs','quota','-u','pwa209','/scratch/pwa209'],capture_output=True,text=True,check=True,timeout=30)
    rows=[row.split() for row in quota.stdout.splitlines() if row.startswith('/scratch/pwa209 ')];assert len(rows)==1 and int(rows[0][7])-int(rows[0][5].rstrip('*'))>=config['resources']['scratch_budget_files']
    for name,expected in own_manifest['files'].items():assert digest(area/name)==expected,'Reviewed coordinator graph changed before registration'
    args=payload['args'];command=shlex.join(['python','-u',str(helper),*args]);name='recovery-425'
    text=_script(config,framework,name,command,cpus=1,memory='16G',hours=4)
    text=text.replace('set -euo pipefail','set -euo pipefail\nexport PYTHONDONTWRITEBYTECODE=1',1)
    directory.mkdir(mode=0o700);script=directory/(name+'.sbatch')
    with script.open('x') as stream:stream.write(text);stream.flush();os.fsync(stream.fileno())
    task={'name':name,'script':str(script),'script_sha256':canonical_hash(text),'command':command,'depends':[],'resources':{'cpus':1,'memory':'16G','hours':4,'gpu':False,'array':None},'dependency_kind':'afterok','external_dependencies':[qjob],
        'reviewed_registration_coordinator':{'helper':{'path':str(helper),'sha256':payload['helper_sha256']},'source_manifest':{'path':str(area/'coordinator-source-manifest.json'),'sha256':payload['coordinator_manifest_sha256']}}}
    plan={'schema_version':2,'phase':'recovery-support','root':str(root),'config_sha256':canonical_hash(config),'release_identity':identity['source_identity'],'tasks':[task],'cpu_ceiling':256,'gpu_concurrency':4,'resource_policy':admission['resource_policy'],'maximum_simultaneous_resources':{'cpus':1,'gpus':0}}
    atomic_json(directory/'plan.json',plan)
    intent={key:value for key,value in payload.items() if key!='helper_body'};intent.update(automatic_replay_permitted=False,plan_path=str(directory/'plan.json'),ledger_path=str(ledger_path),qualification_job_id=qjob,quota_stdout=quota.stdout)
    atomic_json(area/'submission-intent.json',intent)
    submitted=submit(directory/'plan.json',ledger_path)
print('RECOVERY_STAGE_SUBMITTED '+json.dumps({'request_id':'425','area':str(area),'helper_sha256':payload['helper_sha256'],'tasks':{key:{name:value.get(name) for name in ['job_id','state']} for key,value in submitted['tasks'].items()}},sort_keys=True),flush=True)
'''

def render(directory,helper,qualification_id,*,template=Path('work/hhw-recovery-stage-20261021-394.sh')):
    helper_raw,children,qualified,constants,manifest=collect(directory,helper,qualification_id)
    template_raw=Path(template).read_bytes();require(sha(template_raw)==TEMPLATE_SHA,'Reviewed394 template bytes changed')
    header=template_raw.decode().split("python -u - <<'PY'\n",1)[0]+"python -u - <<'PY'\n"
    manifest_raw=(json.dumps(manifest,indent=2,sort_keys=True)+'\n').encode();mh=sha(manifest_raw);area=constants['AREA']
    payload={'id':'425','helper':constants['HELPER'],'helper_sha256':sha(helper_raw),'helper_body':base64.b64encode(zlib.compress(helper_raw,9)).decode(),
        'area':area,'framework':constants['FRAMEWORK'],'source_sha256':constants['SOURCE'],'config_sha256':constants['CONFIG'],'cpus':1,'memory':'16G','hours':4,
        'child_names':list(manifest['files'].keys())[1:],'coordinator_manifest_sha256':mh,'coordinator_native_qualification_claimed':False,
        'args':['--native-plan',ROOT+'/plans/recovery-support-20261021-425/plan.json','--native-ledger',ROOT+'/state/recovery-support-20261021-425-submission.json','--native-task','recovery-425','--native-script',ROOT+'/plans/recovery-support-20261021-425/recovery-425.sbatch','--source-manifest',area+'/coordinator-source-manifest.json','--source-manifest-sha256',mh]}
    sources={'hhw-recovery-stage-20261021-'+ident+'.sh':{'data':base64.b64encode(zlib.compress(raw,9)).decode(),'sha256':sha(raw)} for ident,raw in children.items()}
    body=BODY.replace('__PAYLOAD__','payload=json.loads(zlib.decompress(base64.b64decode('+repr(base64.b64encode(zlib.compress(json.dumps(payload).encode(),9)).decode())+')))')
    body=body.replace('__MANIFEST__','manifest_raw=base64.b64decode('+repr(base64.b64encode(manifest_raw).decode())+')').replace('__SOURCES__','sources='+repr(sources))
    ast.parse(body,feature_version=(3,11));compile(body,'<coordinator-registration425>','exec');raw=(header+body+'\nPY\n').encode();require(b'\r' not in raw,'LF registration source required')
    return raw,manifest_raw,{'request_id':'425','helper_sha256':sha(helper_raw),'source_manifest_sha256':mh,'child_ids':list(CHILD_IDS),'qualification_logical_id':qualification_id,'scheduler_submitted':False}

def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--directory',type=Path,default=Path('work/rolling-native-v4'))
    p.add_argument('--helper',type=Path,default=Path('work/coordinate-reviewed-registrations-v3-20261023.py'));p.add_argument('--qualification-id',default='452');args=p.parse_args(argv)
    target=args.directory/'hhw-recovery-stage-20261021-425.sh';manifest_path=args.directory/'coordinator-source-manifest.json'
    summary_path=args.directory/'coordinator425-registration-local-summary.json'
    require(all(not path.exists() for path in (target,manifest_path,summary_path)),'Local registration candidates are immutable')
    raw,manifest,summary=render(args.directory,args.helper,args.qualification_id)
    with target.open('xb') as stream:stream.write(raw)
    with manifest_path.open('xb') as stream:stream.write(manifest)
    summary.update(path=str(target),sha256=sha(raw),source_manifest_path=str(manifest_path))
    with summary_path.open('x') as stream:stream.write(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2))

if __name__=='__main__':main()
