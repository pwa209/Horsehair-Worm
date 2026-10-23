"""Freeze the reviewed primary handoff registration; never execute it locally."""
from pathlib import Path
import argparse,ast,base64,hashlib,importlib.util,json,re,zlib,xml.etree.ElementTree as ET

R='/scratch/pwa209/horsehair-worm'
AREA=R+'/maintenance/study-primary-external-census-handoff-v4-20261023'
PRIMARY_AREA=R+'/maintenance/study-primary-external-census-policy-v1-20261023'
HELPER='handoff-primary-external-census-v1-20261023.py'
TEST='test_handoff_primary_external_census_v1_20261023.py'
QPLAN=R+'/plans/recovery-support-20261021-469/plan.json'
QLEDGER=R+'/state/recovery-support-20261021-469-submission.json'
QP_SHA='eb100ade4309fe3656ed7b3e34f9d7ce63a0deca7fea4c6cd9de92d3c8d784fc'
QL_SHA='0a248a14e1eb3a29e4a656abfef05e1450651f48df746db81c9757c8cea9ebb8'
QM_SHA='fd5d7b951b2df0094e82c7c670d6e578a30efa913b4dcc6923cf335627d1dde9'
QPOLICY_SHA='b597cd76d14a668e35a36a8a9745e0f034fc47981e29e3ddab779e6e4f011d18'
QHELPER_SHA='dd5f8d2aec90896fdcfe190971ea8b6c32a94158a3684566c2b8405748e7c794'
LIB=Path('work/build-primary-external-census-qualification-469.py')
LIB_SHA='d7e73467a2a5a2fe590f3bf540130fba4feb090c9277c6677e2837104c15c608'

def sha(raw):return hashlib.sha256(raw).hexdigest()
def require(value,message):
    if not value:raise ValueError(message)
def canonical(value):return sha(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
def raw_json(value):return (json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()

def render(junit,helper_sha,test_sha,*,source_directory=Path('work/primary-handoff-v4')):
    require(sha(LIB.read_bytes())==LIB_SHA,'Reviewed registration utility changed')
    spec=importlib.util.spec_from_file_location('primary469_frozen_registration_utilities',LIB)
    lib=importlib.util.module_from_spec(spec);spec.loader.exec_module(lib)
    hraw=lib.checked(Path(source_directory)/HELPER,helper_sha);traw=lib.checked(Path(source_directory)/TEST,test_sha)
    ast.parse(hraw,feature_version=(3,11));ast.parse(traw,feature_version=(3,11))
    jr=Path(junit).read_bytes();require(b'<!DOCTYPE' not in jr.upper() and b'<!ENTITY' not in jr.upper(),'Unsafe JUnit refused')
    tree=ET.fromstring(jr);cases=list(tree.iter('testcase'))
    names=sorted(c.get('classname','').rsplit('.',1)[-1]+'::'+c.get('name','') for c in cases)
    require(cases and len(cases)==len(set(names)) and all(c.get('classname','').rsplit('.',1)[-1]==TEST[:-3] and c.get('name') for c in cases),'Complete unique own handoff suite required')
    require(not any(list(c.iter(k)) for c in cases for k in ('failure','error','skipped')),'All actual handoff cases must pass')
    require(all(all(int(n.get(k,'0'))==0 for k in ('failures','errors','skipped')) for n in tree.iter() if n.tag in {'testsuite','testsuites'}),'Positive JUnit totals required')
    manifest={'kind':'primary-external-census-handoff-source-manifest-v1','files':{HELPER:helper_sha,TEST:test_sha},
        'tests':{'count':len(cases),'roster_sha256':canonical(names)},
        'primary_source_manifest':{'path':PRIMARY_AREA+'/source-manifest.json','sha256':QM_SHA},
        'qualification_policy_descriptor':{'path':PRIMARY_AREA+'/qualification-policy.json','sha256':QPOLICY_SHA}}
    mraw=raw_json(manifest);mh=sha(mraw)
    template=lib.checked(lib.TEMPLATE,lib.TEMPLATE_SHA).decode()
    line=next(l for l in template.splitlines() if l.startswith('payload='))
    blobs=[n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100]
    require(len(blobs)==1,'One reviewed payload required');payload=json.loads(zlib.decompress(base64.b64decode(blobs[0])))
    require(not payload['after'] and not payload['after_specs'] and payload['native_minutes'] is None,'No hidden template prerequisites or time override')
    plan=R+'/plans/recovery-support-20261021-500';ledger=R+'/state/recovery-support-20261021-500-submission.json'
    payload.update(id='500',area=AREA,helper=HELPER,helper_sha256=helper_sha,helper_body=lib.encoded(hraw),cpus=4,memory='16G',hours=1,
        args=['--native-plan',plan+'/plan.json','--native-ledger',ledger,'--native-script',plan+'/recovery-500.sbatch',
            '--native-task','recovery-500','--source-manifest',AREA+'/source-manifest.json','--source-manifest-sha256',mh],dependency_kind='afterok')
    text=template.replace(line,'payload=json.loads(zlib.decompress(base64.b64decode('+repr(lib.encoded(json.dumps(payload).encode()))+')))',1)
    old=next(l for l in text.splitlines() if l.startswith('for name,item in {'))
    text=text.replace(old,'for name,item in '+repr({TEST:{'data':lib.encoded(traw),'sha256':test_sha}})+'.items():',1)
    marker=" with (area/name).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())\n"
    require(text.count(marker)==1,'One immutable source staging point required')
    staging='raw=base64.b64decode('+repr(base64.b64encode(mraw).decode())+');assert hashlib.sha256(raw).hexdigest()=='+repr(mh)+'\n'
    staging+="with (area/'source-manifest.json').open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
    text=text.replace(marker,marker+staging,1)
    text=text.replace("area/'administration-registry-read-guard-v4-20261022.py'","root/'maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py'")
    begin=text.index('    # Both previous registrations ended before plan/sbatch publication.');end=text.index('    admission=combined_admission',begin)
    text=text[:begin]+"    assert not (root/'plans/recovery-support-20261021-500').exists() and not (root/'state/recovery-support-20261021-500-submission.json').exists(),'Fresh primary handoff registration required'\n"+text[end:]
    begin=text.index("    census_directory=root/'plans/recovery-support-20261021-375'");end=text.index("    if payload['after_specs']",begin)
    block="""    task['resources']['array']=None
    assert not (root/'plans/primary-external-census-controllers-v3').exists() and not (root/'state/primary-external-census-controllers-v3-submission.json').exists(),'Fresh namespace-corrected controller phase required'
    assert digest(root/'plans/recovery-support-20261021-490/plan.json')=='05139b295b85e6af6c889f82c369e1a29a3c88af8ced08f1ab427a0efa1f522c'
    assert digest(root/'state/recovery-support-20261021-490-submission.json')=='acf7442e51e1a0de34bb8784e6aa63019f3608e38e8cc7d341c28ee26f22aaf8'
    assert digest(root/'plans/primary-external-census-controllers-v2/plan.json')=='2b5b77e7230b5365de91caf64f72484c456856b0526549a53400320ee7c32cb3'
    assert digest(root/'state/primary-external-census-controllers-v2-submission.json')=='0bd363318e2c4195de7f1a4bc5dc424555e26e8ff8251d4333c5c902649ce55c'
    assert digest(root/'maintenance/study-primary-external-census-handoff-v3-20261023/handoff-primary-external-census-v1-20261023.py')=='0d37f96bd93dd96266eb3f46eb66bf7bd1ecb929b1e8474f3153b1624fda50f3'
    assert digest(root/'maintenance/study-primary-external-census-handoff-v3-20261023/native-6870245/receipt.json')=='9e525425ba063c824eed9fe40e15272d33be5ae88bc208850167b0e2d07523a1'
    assert digest(root/'maintenance/study-primary-descriptor-prestart-reconciliation-v1-20261023/receipt.json')=='567ff41003556ffaf316db0d9d5499a20287ffb3fbe304537efac1bb745326ed'
    from horsehair import repair
    previous=repair._repair_native_states(['6870245','6870608','6870610','6870628','6870632'],{},owner='pwa209',account='def-ptewarie_cpu')
    assert previous['6870245']['state']=='COMPLETED' and previous['6870245']['exit_code']=='0:0' and previous['6870245']['elapsed_seconds']==654 and previous['6870245']['cpus']==4
    assert previous['6870608']['state']=='FAILED' and previous['6870608']['exit_code']=='1:0' and previous['6870608']['elapsed_seconds']==4 and previous['6870608']['cpus']==1
    for job in ['6870610','6870628','6870632']:
        row=previous[job]
        assert row['state']=='CANCELLED' and row['exit_code']=='0:0' and row['cpus']==0 and row['elapsed_seconds']==0 and row['alloc_tres']=='' and row['start'] in {'None','Unknown'} and row['node_list']=='None assigned' and row['unallocated_metadata'] is False and row['identity_basis']=='Unique scalar native job; zero-allocation queue record'
    atomic_json(area/'completed-v3-and-prestart-phase-preserved.json',{'native_records':previous,'historical_scheduler_mutations':False,'physical_failed_allocation_cost_preserved':True,'scientific_result':False})
    task['primary_external_census_handoff']={'helper':{'path':str(helper),'sha256':payload['helper_sha256']},'source_manifest':{'path':str(area/'source-manifest.json'),'sha256':__MH__}}
    qp=Path(__QP__);ql=Path(__QL__);assert digest(qp)==__QPS__ and digest(ql)==__QLS__,'Exact registered qualification pair changed'
    qplan=read_json(qp);qledger=read_json(ql)
    assert canonical_hash(qplan)==qledger['plan_sha256'] and qplan['config_sha256']==qledger['config_sha256']==canonical_hash(config)
    assert qplan['release_identity']['sha256']==qledger['source_sha256']==identity['source_identity']['sha256']
    assert len(qplan['tasks'])==1 and set(qledger['tasks'])=={'recovery-469'}
    qt=qplan['tasks'][0];qr=qledger['tasks']['recovery-469'];qs=Path(qt['script'])
    assert qt['name']=='recovery-469' and qt['resources']=={'cpus':4,'memory':'64G','hours':1,'gpu':False,'array':None}
    assert qr['job_id']=='6866457' and qr['state']=='submitted'
    assert str(qs)==__QS__ and not qs.is_symlink() and digest(qs)=='50734d94eab535f315088581dc16d147757109f6922df249343a57cea0e964b1'
    assert canonical_hash(qs.read_text())==qt['script_sha256']==qr['script_sha256'] and qt['command'] in qs.read_text()
    assert qt['primary_external_census']=={'helper':{'path':__QH__,'sha256':__QHS__},'policy_descriptor':{'path':__QD__,'sha256':__QDS__},'mode':'qualify'}
    assert digest(Path(__QH__))==__QHS__ and digest(Path(__QD__))==__QDS__ and digest(Path(__QM__))==__QMS__
    from horsehair import repair
    observed=repair._repair_native_states(['6866457'],{},owner='pwa209',account='def-ptewarie_cpu')
    assert observed.get('6866457',{}).get('state') in {'PENDING','RUNNING','COMPLETED'},'Positive owned live or completed qualification required'
    task['external_dependencies']=['6866457']
"""
    values={'__MH__':mh,'__QP__':QPLAN,'__QL__':QLEDGER,'__QPS__':QP_SHA,'__QLS__':QL_SHA,
        '__QS__':R+'/plans/recovery-support-20261021-469/recovery-469.sbatch',
        '__QH__':PRIMARY_AREA+'/run-primary-external-census-v1-20261023.py','__QHS__':QHELPER_SHA,
        '__QD__':PRIMARY_AREA+'/qualification-policy.json','__QDS__':QPOLICY_SHA,'__QM__':PRIMARY_AREA+'/source-manifest.json','__QMS__':QM_SHA}
    for key,value in values.items():block=block.replace(key,repr(value))
    text=text[:begin]+block+text[end:]
    body=text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];ast.parse(body,feature_version=(3,11));compile(body,'<primary500-registration>','exec')
    raw=text.encode();require(b'\r' not in raw,'Exact LF registration required')
    evidence={'kind':'primary-external-census-handoff-local-registration-v1','source_manifest_sha256':mh,'helper_sha256':helper_sha,
        'test_sha256':test_sha,'shell_sha256':sha(raw),'local_tests':manifest['tests'],'junit':{'path':str(junit),'sha256':sha(jr)},
        'own_resources':{'cpus':4,'memory':'16G','hours':1,'gpu':False,'array':None},'external_dependencies':['6866457'],
        'qualification_claimed':False,'scheduler_submitted':False}
    return raw,mraw,raw_json(evidence)

def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--junit',type=Path,required=True)
    p.add_argument('--helper-sha256',required=True);p.add_argument('--test-sha256',required=True)
    p.add_argument('--output-directory',type=Path,default=Path('work/primary-native-500'))
    a=p.parse_args(argv);require(not a.output_directory.exists(),'Preserve immutable handoff registration candidates')
    packets=render(a.junit,a.helper_sha256,a.test_sha256);a.output_directory.mkdir(mode=0o700)
    for name,raw in zip(['hhw-recovery-stage-20261021-500.sh','source-manifest.json','registration-local-evidence.json'],packets):
        with (a.output_directory/name).open('xb') as f:f.write(raw)
    print(packets[-1].decode(),end='')

if __name__=='__main__':main()
