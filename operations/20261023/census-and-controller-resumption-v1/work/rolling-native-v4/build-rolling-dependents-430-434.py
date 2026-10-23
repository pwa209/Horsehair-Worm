"""Build fresh dependent registrations for the qualified rolling429 provider.

Only new local files are created. Existing frozen sources and old request
drafts are read-only. No remote operation or scheduler call occurs here.
"""
from pathlib import Path
import ast
import base64
import hashlib
import importlib.util
import json
import zlib

WORK_ROOT=Path('work');W=WORK_ROOT/'rolling-native-v4';R='/scratch/pwa209/horsehair-worm'
spec=importlib.util.spec_from_file_location('reviewed_old_builder',WORK_ROOT/'build-cancelled-dependents-418-422.py')
old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
PAIRS=(('377','430'),('394','431'),('395','432'),('399','433'),('406','434'))
OLD_JOBS=old.OLD_JOBS
ABSENT={new:old.ABSENT[previous] for previous,new in zip(('418','419','420','421','422'),('430','431','432','433','434'))}
unpack=old.unpack
SUPPORT=R+'/maintenance/study-af-rolling-census-support-v3-20261023'
MANIFEST_SHA='43660c97e50c5a24d42abb321514405107b9f1cade649a492f1c91355486ecb8'
V6_SHA='61001913e746537da3583c2844c760f6589b48be763a44f83be446d5d204a1da'

BOOTSTRAP=r'''from pathlib import Path
import math,os,runpy,time
from horsehair.core import digest,read_json,source_identity,canonical_hash
root=Path('/scratch/pwa209/horsehair-worm')
framework=root/'releases/20261021-6ed201653dfa-4f3903d7c26e'
primary=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
area=root/'maintenance/study-af-resubmission-432-20261023'
end=float(os.environ['SLURM_JOB_END_TIME']);assert math.isfinite(end) and time.time()+300<end
assert source_identity(framework)['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
assert source_identity(primary)['sha256']=='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
config=read_json(primary/'configs/study.json');assert canonical_hash(config)=='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35' and read_json(framework/'configs/study.json')==config
gp=root/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py';assert digest(gp)=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
runpy.run_path(str(gp))['install'](area/'native-start-accounting-reads')
guard=root/'maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py';assert digest(guard)=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
api=runpy.run_path(str(guard));api['install'](area/'native-start-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=end)
from horsehair import campaign,repair
assert campaign._repair_native_states is repair._repair_native_states
policy=(root/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5')
assert digest(policy[0])==policy[1]
with api['registration_lock'](area/'native-start-registration-lock',end,role='controller-start'):
    campaign.combined_admission(root,config,read_json(primary/'release.json')['release_id'],area,'native-start-before-frozen-activation',resource_policy=policy,cpus=0,gpus=0,hours=4)
'''

def replace_once(text,before,after):
    assert text.count(before)==1,before[:120]
    return text.replace(before,after,1)

REGISTRATION=old.REGISTRATION
for before,after in [('417','429'),('419','431'),('420','432'),('422','434'),('421','433'),('416','452')]:
    REGISTRATION=REGISTRATION.replace(before,after)
REGISTRATION=replace_once(REGISTRATION,'def registered(ident,*,resources,helper_sha=None):','def registered(ident,*,resources,helper_sha=None,task_name=None):')
REGISTRATION=replace_once(REGISTRATION,"name='recovery-'+ident","name=task_name or 'recovery-'+ident")
REGISTRATION=replace_once(REGISTRATION,"registered('429',resources={'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None},helper_sha=payload['census_helper_sha256'])","registered('429',resources={'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None},helper_sha=payload['census_helper_sha256'],task_name='rolling-storage-census-01')")
REGISTRATION=REGISTRATION.replace('study-af-streaming-census-qualification-v1-20261023','study-af-rolling-census-qualification-v4-20261023')
begin=REGISTRATION.index("    assert census_task['streaming_census']")
end=REGISTRATION.index("    census_job=str(census_record['job_id'])",begin)
REGISTRATION=REGISTRATION[:begin]+'''    expected_manifest={'path':payload['census_args']['--source-manifest'],'sha256':payload['census_args']['--source-manifest-sha256']}
    expected_q={'path':str(qpath),'sha256':digest(qpath),'job_id':str(q452)}
    assert census_task['rolling_census_provider']=={'helper':{'path':payload['census_helper'],'sha256':payload['census_helper_sha256']},'source_manifest':expected_manifest,'case_id':'rolling-01'}
    census_intent=read_json(root/'maintenance/study-af-rolling-census-registration-v3-20261023/submission-intent.json')
    assert census_intent['id']=='429' and census_intent['helper_sha256']==payload['census_helper_sha256'] and census_intent['helper']==Path(payload['census_helper']).name and census_intent['source_manifest']==expected_manifest and census_intent['qualification']==expected_q and census_intent['finite_slots']==64 and census_intent['automatic_replay_permitted'] is False
    import argparse
    provider=runpy.run_path(payload['census_helper'],run_name='dependent_qualified_rolling_provider_contract')
    provider_args=argparse.Namespace(**{k[2:].replace('-','_'):v for k,v in census_args.items()})
    for field in ('native_plan','native_ledger','native_script','source_manifest','kernel_qualification','registry_qualification'):setattr(provider_args,field,Path(getattr(provider_args,field)))
    assert provider['MAX_SLOTS']==64 and provider['provider_registration'](provider_args,str(census_record['job_id']),config)['slot_count']==64
'''+REGISTRATION[end:]
needle="    text=text.replace('set -euo pipefail','set -euo pipefail\\nexport PYTHONDONTWRITEBYTECODE=1',1)"
REGISTRATION=replace_once(REGISTRATION,needle,"""    if payload.get('native_accounting_bootstrap'):
        bootstrap_command=shlex.join(['python','-u','-c',payload['native_accounting_bootstrap']])
        assert text.count(command+'\\n')==1
        text=text.replace(command+'\\n',bootstrap_command+'\\n'+command+'\\n',1)
"""+needle)

def main():
    assert hashlib.sha256((W/'source-manifest.json').read_bytes()).hexdigest()==MANIFEST_SHA
    frozen=json.loads((W/'source-manifest.json').read_text());assert frozen['files']['run-fresh-storage-census-v6-20261023.py']==V6_SHA
    for _,ident in PAIRS:assert not (W/('hhw-recovery-stage-20261021-'+ident+'.sh')).exists()
    report=[]
    for previous,ident in PAIRS:
        source,p,raw=old.unpack(previous);old_area=p['area'];canonical=old_area+'/'+p['helper'];old_hash=p['helper_sha256'];p.pop('helper_body')
        census_helper=SUPPORT+'/run-fresh-storage-census-v6-20261023.py';manifest_path=SUPPORT+'/source-manifest.json'
        p.update(id=ident,area=R+'/maintenance/study-af-resubmission-'+ident+'-20261023',canonical_helper=canonical,canonical_sources={canonical:old_hash,census_helper:V6_SHA,manifest_path:MANIFEST_SHA},old_jobs=old.OLD_JOBS,absent_outputs=old.ABSENT[{'430':'418','431':'419','432':'420','433':'421','434':'422'}[ident]],census_helper=census_helper,census_helper_sha256=V6_SHA,administration_guard=R+'/maintenance/study-af-registry-repair-support-v6-20261022/administration-registry-read-guard-v4-20261022.py')
        p['census_args']={'--native-plan':R+'/plans/recovery-support-20261021-429/plan.json','--native-ledger':R+'/state/recovery-support-20261021-429-submission.json','--native-task':'rolling-storage-census-01','--native-script':R+'/plans/recovery-support-20261021-429/rolling-storage-census-01.sbatch','--source-manifest':manifest_path,'--source-manifest-sha256':MANIFEST_SHA,'--case-id':'rolling-01'}
        p['args']=[x.replace('recovery-support-20261021-'+previous,'recovery-support-20261021-'+ident).replace('recovery-'+previous,'recovery-'+ident) for x in p['args']]
        if previous=='395':
            p['canonical_helper']=R+'/maintenance/study-af-registry-repair-support-v6-20261022/'+p['helper'];p['canonical_sources'][p['canonical_helper']]=old_hash;p['native_accounting_bootstrap']=BOOTSTRAP
        line=next((line for line in source.splitlines() if line.startswith('for name,item in {')),None)
        if line:
            for name,item in ast.literal_eval(ast.parse(line+'\n pass').body[0].iter.func.value).items():p['canonical_sources'][old_area+'/'+name]=item['sha256']
        differences=[];derived=None
        if previous=='399':
            text=raw.decode();text=text.replace(R+'/plans/recovery-support-20261021-376/recovery-376.sbatch',R+'/plans/recovery-support-20261021-433/recovery-433.sbatch').replace(R+'/maintenance/study-af-cohort-prefix-stage-v5-r1-20261022/',p['area']+'/');derived=text.encode()
            before=ast.parse(raw);after=ast.parse(derived)
            assert ast.dump(next(n for n in before.body if isinstance(n,ast.For)),include_attributes=False)==ast.dump(next(n for n in after.body if isinstance(n,ast.For)),include_attributes=False)
            a=[n.value for n in ast.walk(before) if isinstance(n,ast.Constant) and isinstance(n.value,str)];b=[n.value for n in ast.walk(after) if isinstance(n,ast.Constant) and isinstance(n.value,str)]
            differences=[{'before':x,'after':y} for x,y in zip(a,b) if x!=y];assert len(differences)==4
        elif previous=='406':
            before="SUPPORT=ROOT/'maintenance/study-af-archive-consumer-qualification-stage-v3-20261023'";after="SUPPORT=ROOT/'maintenance/study-af-resubmission-434-20261023'"
            derived=replace_once(raw.decode(),before,after).encode();differences=[{'before':before,'after':after}]
            p['args']=[x.replace(old_area,p['area']) for x in p['args']]
            p['copy_sources']={Path(path).name:path for path in p['canonical_sources'] if path.startswith(old_area+'/') and path!=canonical and not Path(path).name.startswith('administration-')}
        if derived is not None:
            ast.parse(derived);p['derived_helper_body']=base64.b64encode(zlib.compress(derived,9)).decode();p['helper_sha256']=hashlib.sha256(derived).hexdigest()
            local=W/('resubmission-'+ident+'-operational');assert not local.exists();local.mkdir();(local/p['helper']).write_bytes(derived)
            for name,path in p.get('copy_sources',{}).items():
                candidate=WORK_ROOT/name;assert hashlib.sha256(candidate.read_bytes()).hexdigest()==p['canonical_sources'][path];(local/name).write_bytes(candidate.read_bytes())
            (local/'operational-differences.json').write_text(json.dumps({'original_sha256':old_hash,'derived_sha256':p['helper_sha256'],'differences':differences,'embedded_payload_bytes_changed':False},indent=2)+'\n')
        blob=base64.b64encode(zlib.compress(json.dumps(p).encode(),9)).decode()
        text=old.HEADER+old.COMMON+"payload=json.loads(zlib.decompress(base64.b64decode("+repr(blob)+")))\n"+old.INITIAL+REGISTRATION+'\nPY\n'
        body=text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];ast.parse(body,feature_version=(3,11));compile(body,'<new-dependent>','exec')
        target=W/('hhw-recovery-stage-20261021-'+ident+'.sh');target.write_bytes(text.encode());assert b'\r' not in target.read_bytes()
        report.append({'request_id':ident,'predecessor_logical_id':previous,'path':str(target),'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'helper_sha256':p['helper_sha256'],'canonical_helper':p['canonical_helper'],'operational_differences':differences,'native_accounting_bootstrap':ident=='432','scheduler_submitted':False})
    (W/'rolling-dependents-430-434-local-manifest.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))

if __name__=='__main__':main()
