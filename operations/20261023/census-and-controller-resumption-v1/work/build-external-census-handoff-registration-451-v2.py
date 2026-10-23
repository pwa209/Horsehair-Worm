"""Freeze a reviewed, self-qualifying handoff registration; no remote action."""
from pathlib import Path
import argparse
import ast
import base64
import hashlib
import json
import zlib
import xml.etree.ElementTree as ET

R='/scratch/pwa209/horsehair-worm'
AREA=R+'/maintenance/study-af-external-census-handoff-v1-20261023'
SUPPORT=R+'/maintenance/study-af-rolling-census-support-v4-20261023'
HELPER_DIR=Path('work/rolling-native-v5')
HELPER='handoff-f2-external-census-v1-20261023.py'
TEST='test_handoff_f2_external_census_v1_20261023.py'
ACT=R+'/maintenance/study-af-registry-repair-support-v6-20261022/activate-f2-prestart-three-v1-20261022.py'
ACT_SHA='586bbf301ef69621cc1132b8b25a9c243f29a2e918b54d564e84be816af2d2eb'
TEMPLATE=Path('work/hhw-recovery-stage-20261021-394.sh')
TEMPLATE_SHA='8254e96c402b4d8aa43c98d43880a607f2d222408d0f1eb2c351a60277f29e9e'

def sha(raw):return hashlib.sha256(raw).hexdigest()
def canonical(value):return sha(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
def registration(ident):
    plan=R+'/plans/recovery-support-20261021-'+ident
    return {'plan_path':plan+'/plan.json','ledger_path':R+'/state/recovery-support-20261021-'+ident+'-submission.json',
        'task_name':'recovery-'+ident,'script_path':plan+'/recovery-'+ident+'.sbatch'}

def render(directory,junit):
    directory=Path(directory);jr=Path(junit).read_bytes();tree=ET.fromstring(jr);cases=list(tree.iter('testcase'))
    names=sorted(c.get('classname','').rsplit('.',1)[-1]+'::'+c.get('name','') for c in cases)
    assert cases and len(cases)==len(set(names)) and all(c.get('classname','').rsplit('.',1)[-1]==TEST[:-3] and c.get('name') for c in cases)
    assert not any(list(c.iter(k)) for c in cases for k in ('failure','error','skipped'))
    assert all(all(int(n.get(k,'0'))==0 for k in ('failures','errors','skipped')) for n in tree.iter() if n.tag in {'testsuite','testsuites'})
    helper=HELPER_DIR/HELPER;test=HELPER_DIR/TEST
    hraw=helper.read_bytes();traw=test.read_bytes();ast.parse(hraw,feature_version=(3,11));ast.parse(traw,feature_version=(3,11))
    qmraw=(directory/'source-manifest.json').read_bytes();qm=json.loads(qmraw)
    assert qm['kind']=='streaming-census-operational-source-manifest-v2' and len(qm['files'])==14 and len(qm['suites'])==13
    for n,h in qm['files'].items():assert sha((directory/n).read_bytes())==h,n
    assert b"Q2_SUPPORT=ROOT/'maintenance/study-af-rolling-census-support-v4-20261023'" in hraw
    assert b"Q2_PATH=ROOT/'maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json'" in hraw
    manifest={'kind':'f2-external-census-handoff-source-manifest-v1','files':{HELPER:sha(hraw),TEST:sha(traw)},
        'tests':{'count':len(cases),'roster_sha256':canonical(names)},
        'q2_source_manifest':{'path':SUPPORT+'/source-manifest.json','sha256':sha(qmraw)},
        'q2_registration':registration('456'),'activation_source':{'path':ACT,'sha256':ACT_SHA},
        'activation_registration':registration('432'),
        'provider_registration':{k:v for k,v in registration('429').items() if k in {'plan_path','ledger_path'}}}
    mraw=(json.dumps(manifest,sort_keys=True,indent=2)+'\n').encode();mh=sha(mraw)
    raw=TEMPLATE.read_bytes();assert sha(raw)==TEMPLATE_SHA;s=raw.decode()
    line=next(l for l in s.splitlines() if l.startswith('payload='));blob=next(n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100)
    p=json.loads(zlib.decompress(base64.b64decode(blob)));binding=registration('451')
    p.update(id='451',area=AREA,helper=HELPER,helper_sha256=sha(hraw),helper_body=base64.b64encode(zlib.compress(hraw,9)).decode(),cpus=4,memory='16G',hours=2,
        args=['--native-plan',binding['plan_path'],'--native-ledger',binding['ledger_path'],'--native-task',binding['task_name'],
            '--native-script',binding['script_path'],'--source-manifest',AREA+'/source-manifest.json','--source-manifest-sha256',mh])
    assert not p['after'] and not p['after_specs'] and p['native_minutes'] is None
    s=s.replace(line,"payload=json.loads(zlib.decompress(base64.b64decode("+repr(base64.b64encode(zlib.compress(json.dumps(p).encode(),9)).decode())+")))",1)
    old=next(l for l in s.splitlines() if l.startswith('for name,item in {'))
    sources={TEST:{'data':base64.b64encode(zlib.compress(traw,9)).decode(),'sha256':sha(traw)}}
    s=s.replace(old,'for name,item in '+repr(sources)+'.items():',1)
    marker=" with (area/name).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())\n"
    assert s.count(marker)==1
    s=s.replace(marker,marker+'manifest_raw=base64.b64decode('+repr(base64.b64encode(mraw).decode())+');assert hashlib.sha256(manifest_raw).hexdigest()=='+repr(mh)+"\nwith (area/'source-manifest.json').open('xb') as f:f.write(manifest_raw);f.flush();os.fsync(f.fileno())\n",1)
    s=s.replace("area/'administration-registry-read-guard-v4-20261022.py'","root/'maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py'")
    begin=s.index('    # Both previous registrations ended before plan/sbatch publication.');end=s.index('    admission=combined_admission',begin)
    s=s[:begin]+"    assert not (root/'plans/recovery-support-20261021-451').exists() and not (root/'state/recovery-support-20261021-451-submission.json').exists(),'Fresh handoff registration required'\n"+s[end:]
    begin=s.index("    census_directory=root/'plans/recovery-support-20261021-375'");end=s.index("    if payload['after_specs']",begin)
    block="""    task['resources']['array']=None
    task['external_census_handoff']={'helper':{'path':str(helper),'sha256':payload['helper_sha256']},'source_manifest':{'path':str(area/'source-manifest.json'),'sha256':__MH__}}
    activation_plan=read_json(root/'plans/recovery-support-20261021-432/plan.json');activation_ledger=read_json(root/'state/recovery-support-20261021-432-submission.json')
    assert canonical_hash(activation_plan)==activation_ledger['plan_sha256'] and activation_ledger['source_sha256']==activation_plan['release_identity']['sha256']==identity['source_identity']['sha256']
    assert activation_plan['config_sha256']==activation_ledger['config_sha256']==canonical_hash(config)
    activation_tasks=[t for t in activation_plan['tasks'] if t['name']=='recovery-432'];assert len(activation_tasks)==len(activation_plan['tasks'])==1
    at=activation_tasks[0];ar=activation_ledger['tasks']['recovery-432'];ascript=Path(at['script'])
    assert at['resources']=={'cpus':4,'memory':'16G','hours':4,'gpu':False} and ar['state']=='submitted'
    assert str(ascript)==__ASCRIPT__ and not ascript.is_symlink() and canonical_hash(ascript.read_text())==at['script_sha256']==ar['script_sha256'] and at['command'] in ascript.read_text()
    assert shlex.split(at['command'])[:3]==['python','-u',__ACT__] and digest(Path(__ACT__))==__ACT_SHA__
    activation_job=str(ar['job_id']);assert __import__('re').fullmatch('[1-9][0-9]*',activation_job)
    task['external_dependencies']=[activation_job]
""".replace('__MH__',repr(mh)).replace('__ASCRIPT__',repr(registration('432')['script_path'])).replace('__ACT__',repr(ACT)).replace('__ACT_SHA__',repr(ACT_SHA))
    s=s[:begin]+block+s[end:]
    body=s.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];ast.parse(body,feature_version=(3,11));compile(body,'<handoff451-registration>','exec')
    assert b'\r' not in s.encode()
    return s.encode(),mraw,{'kind':'f2-external-census-handoff-local-registration-evidence-v1','tests':manifest['tests'],
        'junit':{'path':str(junit),'sha256':sha(jr)},'source_manifest_sha256':mh,'helper_sha256':sha(hraw),'test_source_sha256':sha(traw),
        'qualification_claimed':False,'scheduler_submitted':False}

def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--directory',type=Path,default=Path('work/rolling-native-v5'));p.add_argument('--junit',type=Path,required=True);a=p.parse_args(argv)
    names=['hhw-recovery-stage-20261021-451.sh','handoff-source-manifest.json','handoff-registration-local-evidence.json']
    assert all(not (a.directory/n).exists() for n in names),'Preserve immutable handoff candidates'
    raw,mraw,evidence=render(a.directory,a.junit);evidence['shell_sha256']=sha(raw)
    packets=[raw,mraw,(json.dumps(evidence,sort_keys=True,indent=2)+'\n').encode()]
    for name,body in zip(names,packets):
        with (a.directory/name).open('xb') as f:f.write(body)
    print(json.dumps(evidence,sort_keys=True))

if __name__=='__main__':main()
