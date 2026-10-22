"""Build fresh, explicit registration requests; this local file submits nothing."""
from pathlib import Path
import ast
import base64
import hashlib
import json
import sys
import zlib

WORK=Path('work')
ROOT='/scratch/pwa209/horsehair-worm'
SUPPORT=ROOT+'/maintenance/study-af-registry-repair-support-v6-20261022'
ADMIN_NAME='administration-registry-read-guard-v4-20261022.py'
ADMIN_SHA='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'

def sha(raw):return hashlib.sha256(raw).hexdigest()
def encode(raw):return base64.b64encode(zlib.compress(raw,9)).decode()
def literals(source,name):
    node=next(n for n in ast.parse(source).body if isinstance(n,ast.Assign)
        and any(isinstance(t,ast.Name) and t.id==name for t in n.targets))
    return ast.literal_eval(node.value)

def main(number):
    assert number in ('384','385')
    qualifier='qualify-f2-registry-repair-v6-20261022.py'
    pins=literals((WORK/qualifier).read_text(),'PINS')
    assert len(pins)>=12 and all(len(v)==64 and sha((WORK/k).read_bytes())==v for k,v in pins.items())
    closure={k:(WORK/k).read_bytes() for k in pins}
    closure[qualifier]=(WORK/qualifier).read_bytes()
    qtest='test_qualify_f2_registry_repair_v6_20261022.py'
    closure[qtest]=(WORK/qtest).read_bytes()
    manifest={'kind':'f2-prestart-three-source-freeze-v1','files':{k:sha(v) for k,v in sorted(closure.items())}}
    manifest_raw=(json.dumps(manifest,indent=2,sort_keys=True)+'\n').encode()
    manifest_file=WORK/'prestart-three-source-manifest-20261022.json'
    if manifest_file.exists():assert manifest_file.read_bytes()==manifest_raw
    else:manifest_file.write_bytes(manifest_raw)
    closure['source-manifest.json']=manifest_raw
    admin=(WORK/'registered-snapshot-read-guard-v4-20261022.py').read_bytes();assert sha(admin)==ADMIN_SHA
    closure[ADMIN_NAME]=admin

    template=(WORK/'hhw-recovery-stage-20261021-377.sh').read_text()
    payload_line=next(line for line in template.splitlines() if line.startswith('payload='))
    blob=next(n.value for n in ast.walk(ast.parse(payload_line)) if isinstance(n,ast.Constant)
        and isinstance(n.value,str) and len(n.value)>100)
    payload=json.loads(zlib.decompress(base64.b64decode(blob)))
    helper=qualifier if number=='384' else 'activate-f2-prestart-three-v1-20261022.py'
    payload.update(id=number,helper=helper,helper_sha256=sha(closure[helper]),helper_body=encode(closure[helper]),
        area=SUPPORT if number=='384' else ROOT+'/maintenance/study-af-prestart-three-activation-stage-v1-20261022',
        cpus=4,memory='16G',hours=1 if number=='384' else 4)
    payload['args']=['--native-plan',ROOT+'/plans/recovery-support-20261021-'+number+'/plan.json',
        '--native-ledger',ROOT+'/state/recovery-support-20261021-'+number+'-submission.json',
        '--native-task','recovery-'+number,'--native-script',ROOT+'/plans/recovery-support-20261021-'+number+'/recovery-'+number+'.sbatch']
    if number=='385':
        payload['args']+=['--source-manifest',SUPPORT+'/source-manifest.json',
            '--source-manifest-sha256',sha(manifest_raw),'--qualification-job-id','__Q_JOB__',
            '--approval-reference','Existing user authorization to repair and continue the computational study, 22 October 2026.']
    template=template.replace(payload_line,"payload=json.loads(zlib.decompress(base64.b64decode('"+encode(json.dumps(payload).encode())+"')))",1)
    start=template.index('for name,item in ')
    stop=template.index('.items():',start)+len('.items():')
    staged={k:v for k,v in closure.items() if k!=helper} if number=='384' else {ADMIN_NAME:admin}
    embedded={k:{'data':encode(v),'sha256':sha(v)} for k,v in staged.items()}
    template=template[:start]+'for name,item in '+repr(embedded)+'.items():'+template[stop:]
    template=template.replace("area/'registered-snapshot-read-guard-v4-20261022.py'","area/"+repr(ADMIN_NAME))
    if number=='385':
        old="    args=[str(driver) if x=='__DRIVER__' else digest(driver) if x=='__DRIVER_SHA__' else x for x in payload['args']]"
        new="""    qdirectory=root/'plans/recovery-support-20261021-384'
    qplan=read_json(qdirectory/'plan.json');qledger=read_json(root/'state/recovery-support-20261021-384-submission.json')
    assert canonical_hash(qplan)==qledger['plan_sha256'] and qplan['release_identity']['sha256']==qledger['source_sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
    qt=[t for t in qplan['tasks'] if t['name']=='recovery-384'];assert len(qt)==1
    assert qt[0]['resources']=={'cpus':4,'memory':'16G','hours':1,'gpu':False} and canonical_hash(Path(qt[0]['script']).read_text())==qt[0]['script_sha256']
    qintent=read_json(root/'maintenance/study-af-registry-repair-support-v6-20261022/submission-intent.json')
    assert qintent['helper_sha256']==QUALIFIER_SHA
    qjob=str(qledger['tasks']['recovery-384']['job_id']);assert qjob.isdigit()
    args=[qjob if x=='__Q_JOB__' else x for x in payload['args']]""".replace('QUALIFIER_SHA',repr(sha(closure[qualifier])))
        assert template.count(old)==1;template=template.replace(old,new,1)
        old="command=shlex.join(['python','-u',str(helper),*args])"
        new="command=shlex.join(['python','-u',"+repr(SUPPORT+'/'+helper)+",*args])"
        assert template.count(old)==1;template=template.replace(old,new,1)
        start=template.index("    census_directory=root/'plans/recovery-support-20261021-375'")
        stop=template.index("    task['external_dependencies']=[census_job]",start)+len("    task['external_dependencies']=[census_job]")
        template=template[:start]+"    task['external_dependencies']=[qjob]"+template[stop:]
    target=WORK/('hhw-recovery-stage-20261021-'+number+'.sh');assert not target.exists()
    body=template.encode();assert b'\r' not in body
    try:ast.parse(template.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0])
    except SyntaxError as error:raise AssertionError('Invalid registration body at line '+str(error.lineno)+': '+error.msg) from None
    target.write_bytes(body)
    print(json.dumps({'request':str(target),'bytes':len(body),'sha256':sha(body),'manifest_sha256':sha(manifest_raw)}))

if __name__=='__main__':main(sys.argv[1])
