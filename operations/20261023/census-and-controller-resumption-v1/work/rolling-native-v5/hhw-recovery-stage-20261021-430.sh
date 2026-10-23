#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e/src:${PYTHONPATH:-}"
cd /project/def-ptewarie/pwa209/horsehair-worm/releases/20261016-5c3ae6df532d-91c8824f2adb
python -u - <<'PY'

from pathlib import Path
import ast,base64,hashlib,json,os,runpy,shlex,time,zlib
from horsehair.core import read_json,digest,source_identity,canonical_hash,atomic_json
from horsehair.slurm import _script,submit
root=Path('/scratch/pwa209/horsehair-worm')
primary=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
old=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261016-5c3ae6df532d-91c8824f2adb')
payload=json.loads(zlib.decompress(base64.b64decode('eNrNWNuS2zYM/ZWMn6uY90t+oB+R6WhAEtxVVpZcUd50J5N/Lyj5snEu2ybrTt9kCgRwwIMj0J82Xdq8e7NRkm1+e7O5x36PU1348wB9l5+aLJoJ77oyT0/0sIduah51I5gwnAnxdv902daWexDa1N1OixhsNCHyCDnbxAQP2YJjyToWsvAJjI70SyrU6ExkUmOwKhmHWdrqFCaE6mtb4gRzvN/uP4Jgfns/TgXvayIfx2m33UE3zDjAEHFb5kN6aiBTpuUQdl0p3Tg0hO2UsKyO8wQ7pL0P/8D7hD1CwbI9OuCNwSQYN1qmDI3K0jOZbBQGq+u4PxTyqmpNxsNUnzk973A3Tk81HDe/r9ju6rv3m6YZYO4esdn3MNQ3L+RTzQplFcdHpBMph/1+nObmnB1hXWzefijj4u8coMd0R0f7cogyw4w/DtFcqvt1oBnKQ104ezhy62xA4bv9/Epgn0d5W0L1tvmjVjjPC5Hf1x8J9zgkHOJT+9ANC+GX9+OS6PLYlj3GctqAf8X+kLAaVosL3PbqzTAm7Kk7zr8XiO2uGw4zVnfDoe/rOs5HxmXoC1aqwDAOXYS+vTTdz1H9y+48VelZl27/VTNfEitE4big+PQ/SO2VZOWngIx93w13TcShHMoFhzrrynY6DE0m2blvyjxOcIcn40dzNjqiUM5SqglBZB2dF1Ear51BzqNyTqNSqKIOIaWQgKNHGZzI3GljTPZZ3hDFeuLNDoYuY5nX5q6yFQS3SmQDSQcbKE3vQTnPvHMJLGUKSnrrXQAXuAARyNBGS1l7h6s23opBqw1OmJoywL7cjzOZQ2ruDjClZ/BOLNKZmZyM5EJILyBHY4XXAqU3kAOa4FmGwDXV3ASjSVi4YITFJS+NcnBTMJVGjKXq5gGny54qcj08ncXviCUl6T3TkUiTnc+UZOWNFDpxYJknjdF6zVJyPjEiUhDGZkKf0Vpg/uZYyJR6YZibOA5zJeBzTF83uBDOKC9ENFzwnGPEmER2jLEsXE4+SOoE+gCjD7qemXQyGi8caYIynttbwiE9n9sL2doT2dpKtnYhW/uo2itEDFVmwjuuMocI3viMwokEkTrIcGmJhg5MCgEVMGJiAG2ZASuJeTr4myOqbGtXtrWnTe2Rbe0V20ROMTsJHALRymQnSGOTTNQhKpMEE+u8JkGOoBKdiLYkcyYlnQKdnDfu5mAgtwvd2gvdLqAe9fXhKCq+4hqYtipEGTKpcGQOABPJGRBKUrRomAKa+kiJlWDBeRWtIAt74+5ZB5v6YZwnckLKTW1Ao1cdz65lQDOFQVijmJWgg+EqeWecsDES6ZIkGUZQATXpteSSCKssBMUd00RHA/9B3yxo2iza52jaBc01zQx95IlpUetMMmBUcrx++izD2hIGSbw5Ay68D4DZGFTESWF5pibixsrNZ4Iz9qn9MIZ1epHWLo4d96SXFa30al0RQkmxrujTil6pWk2PK3adYRUzxxWlnVriQCiVcONh3h/mdai/VSWP8/M6MtGENnfjcMuT20/dYyXbFwHXmaAOyesc8avj6+uNVtcpPbuPvsbIBYmm+lqzpQyr4L/60G4u1f8y3vMt35ttLgU43i8/Xd0v3/36lUv4H98v3/36/VL4l++XFObEnCtSMP6ty+brIP9uyNPNc4l8NUTfsDO+Oa5/K4fm0gqvMco3TYSCzfrX0SlPKvznz38DzWPzNQ==')))
framework=Path(payload['framework']);config=read_json(primary/'configs/study.json');identity=read_json(framework/'release.json');science=read_json(primary/'release.json')
assert os.geteuid()==3164768 and source_identity(primary)==science['source_identity'] and source_identity(old)==read_json(old/'release.json')['source_identity']
assert source_identity(framework)==identity['source_identity'] and read_json(framework/'configs/study.json')==config and canonical_hash(config)==science['config_sha256']==identity['config_sha256']
q=read_json(root/'maintenance/sixth-repair-support-20261016/qualification.json')
assert q['status']=='complete' and q['slurm_job_id']=='5865804' and q['source_identity']==read_json(old/'release.json')['source_identity'] and q['test_counts']=={'tests':809,'failures':0,'errors':0,'skipped':0}
driver=root/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'
assert digest(driver)=='49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'

area=Path(payload['area'])
assert area.is_relative_to(root/'maintenance') and area==area.resolve() and not area.exists() and not area.is_symlink(),'Fresh immutable registration area required'
area.mkdir(mode=0o700,parents=True,exist_ok=False)
for path,expected in payload['canonical_sources'].items():
    p=Path(path);assert p==p.resolve() and p.is_file() and not p.is_symlink() and digest(p)==expected,'Frozen canonical source changed'
helper=Path(payload['canonical_helper'])
if payload.get('derived_helper_body'):
    raw=zlib.decompress(base64.b64decode(payload['derived_helper_body']));assert hashlib.sha256(raw).hexdigest()==payload['helper_sha256'];ast.parse(raw)
    helper=area/payload['helper']
    with helper.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    for name,source in payload.get('copy_sources',{}).items():
        raw=Path(source).read_bytes();assert hashlib.sha256(raw).hexdigest()==payload['canonical_sources'][source]
        with (area/name).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
else:assert digest(helper)==payload['helper_sha256']
admin=Path(payload['administration_guard']);assert digest(admin)=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
gp=root/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
assert digest(gp)=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
runpy.run_path(str(gp))['install'](area/'submission-native-accounting-reads')
api=runpy.run_path(str(admin));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+1500)
from horsehair import repair
from horsehair.campaign import combined_admission
policy_pair=(root/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json','f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5')
def registered(ident,*,resources,helper_sha=None,task_name=None):
    directory=root/('plans/recovery-support-20261021-'+ident);path=directory/'plan.json';ledger_path=root/('state/recovery-support-20261021-'+ident+'-submission.json')
    plan=read_json(path);ledger=read_json(ledger_path);name=task_name or 'recovery-'+ident
    assert plan['config_sha256']==ledger['config_sha256']==canonical_hash(config) and canonical_hash(plan)==ledger['plan_sha256']
    assert plan['release_identity']['sha256']==ledger['source_sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
    tasks=[t for t in plan['tasks'] if t['name']==name];assert len(tasks)==1
    task=tasks[0];record=ledger['tasks'][name];script=directory/(name+'.sbatch')
    assert task['script']==str(script) and script==script.resolve() and not script.is_symlink()
    assert canonical_hash(script.read_text())==task['script_sha256']==record['script_sha256'] and task['command'] in script.read_text()
    assert task['resources']==resources and record['state']=='submitted' and str(record['job_id']).isdigit()
    if helper_sha is not None:
        command=shlex.split(task['command']);assert len(command)>=3 and command[0]=='python' and command[1]=='-u' and digest(Path(command[2]))==helper_sha
    return task,record,plan,ledger
with api['registration_lock'](area/'submission-registration-lock',time.time()+1500,role='controller-start'):
    observed=repair._repair_native_states(sorted(payload['old_jobs'].values()),{},owner='pwa209',account='def-ptewarie_cpu')
    assert set(observed)==set(payload['old_jobs'].values()),'All five actual cancelled records required'
    for ident,job in payload['old_jobs'].items():
        value=observed[job]
        assert value['state']=='CANCELLED' and value['exit_code']=='0:0' and value['job_id_raw']==job and value['user']=='pwa209' and value['account']=='def-ptewarie_cpu'
        assert type(value['elapsed_seconds']) is int and value['elapsed_seconds']==0 and type(value['cpus']) is int and value['cpus']==0 and value['alloc_tres']=='' and value['node_list']=='None assigned' and value['start'] in {'None','Unknown'},'Cancelled predecessor allocated or started; refuse replay'
        old_plan=read_json(root/('plans/recovery-support-20261021-'+ident)/'plan.json');old_ledger=read_json(root/('state/recovery-support-20261021-'+ident+'-submission.json'))
        assert canonical_hash(old_plan)==old_ledger['plan_sha256'] and old_ledger['tasks']['recovery-'+ident]['job_id']==job
    absent=list(map(Path,payload['absent_outputs']))
    assert all(not p.exists() and not p.is_symlink() for p in absent),'Native output namespace exists; explicit reconciliation required'
    atomic_json(area/'cancelled-unstarted-predecessors.json',{'native_records':observed,'old_jobs':payload['old_jobs'],'absent_outputs':list(map(str,absent)),'registration_mutex':str(root/'state/af-campaign-registration.mutex'),'automatic_replay_permitted':False,'scientific_result':False,'admission_authority':False})
    census_task,census_record,_,_=registered('429',resources={'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None},helper_sha=payload['census_helper_sha256'],task_name='rolling-storage-census-01')
    census_command=shlex.split(census_task['command']);assert census_command[:3]==['python','-u',payload['census_helper']]
    census_tail=census_command[3:];assert len(census_tail)%2==0 and len(set(census_tail[::2]))==len(census_tail[::2])
    census_args=dict(zip(census_tail[::2],census_tail[1::2]));assert all(census_args.get(k)==v for k,v in payload['census_args'].items())
    assert set(census_args)==set(payload['census_args'])|{'--kernel-qualification','--kernel-qualification-sha256','--kernel-qualification-job-id','--registry-qualification','--registry-qualification-sha256','--registry-qualification-job-id'}
    qpath=root/'maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json'
    q456=read_json(root/'state/recovery-support-20261021-456-submission.json')['tasks']['recovery-456']['job_id']
    assert census_args['--kernel-qualification']==str(qpath) and census_args['--kernel-qualification-sha256']==digest(qpath) and census_args['--kernel-qualification-job-id']==str(q456)
    expected_manifest={'path':payload['census_args']['--source-manifest'],'sha256':payload['census_args']['--source-manifest-sha256']}
    expected_q={'path':str(qpath),'sha256':digest(qpath),'job_id':str(q456)}
    assert census_task['rolling_census_provider']=={'helper':{'path':payload['census_helper'],'sha256':payload['census_helper_sha256']},'source_manifest':expected_manifest,'case_id':'rolling-01'}
    census_intent=read_json(root/'maintenance/study-af-rolling-census-registration-v4-20261023/submission-intent.json')
    assert census_intent['id']=='429' and census_intent['helper_sha256']==payload['census_helper_sha256'] and census_intent['helper']==Path(payload['census_helper']).name and census_intent['source_manifest']==expected_manifest and census_intent['qualification']==expected_q and census_intent['finite_slots']==64 and census_intent['automatic_replay_permitted'] is False
    import argparse
    provider=runpy.run_path(payload['census_helper'],run_name='dependent_qualified_rolling_provider_contract')
    provider_args=argparse.Namespace(**{k[2:].replace('-','_'):v for k,v in census_args.items()})
    for field in ('native_plan','native_ledger','native_script','source_manifest','kernel_qualification','registry_qualification'):setattr(provider_args,field,Path(getattr(provider_args,field)))
    assert provider['MAX_SLOTS']==64 and provider['provider_registration'](provider_args,str(census_record['job_id']),config)['slot_count']==64
    census_job=str(census_record['job_id']);dependencies=[census_job];q_job=None
    if payload['id'] in {'432','434'}:
        qt,qr,_,_=registered('431',resources={'cpus':4,'memory':'16G','hours':1,'gpu':False},helper_sha='875ec697466351b21718eded61a6e81ac23d74ce0902ce481ebc970012261020')
        qcommand=shlex.split(qt['command']);assert qcommand[2]==str(root/'maintenance/study-af-registry-repair-support-v6-20261022/qualify-f2-registry-repair-v6-20261022.py')
        assert qcommand[3:]==['--native-plan',str(root/'plans/recovery-support-20261021-431/plan.json'),'--native-ledger',str(root/'state/recovery-support-20261021-431-submission.json'),'--native-task','recovery-431','--native-script',str(root/'plans/recovery-support-20261021-431/recovery-431.sbatch')]
        q_job=str(qr['job_id']);assert q_job!=census_job;dependencies.append(q_job)
    admission=combined_admission(root,config,science['release_id'],area,'submit-expanded-combined-admission',resource_policy=policy_pair,cpus=payload['cpus'],gpus=0,hours=payload['hours'])
    quota=__import__('subprocess').run(['lfs','quota','-u','pwa209','/scratch/pwa209'],capture_output=True,text=True,check=True,timeout=30)
    rows=[x.split() for x in quota.stdout.splitlines() if x.startswith('/scratch/pwa209 ')];assert len(rows)==1 and int(rows[0][7])-int(rows[0][5].rstrip('*'))>=config['resources']['scratch_budget_files']
    args=[q_job if x=='__Q_JOB__' else x for x in payload['args']]
    command=shlex.join(['python','-u',str(helper),*args]);name='recovery-'+payload['id']
    text=_script(config,framework,name,command,cpus=payload['cpus'],memory=payload['memory'],hours=payload['hours'],network=False)
    if payload.get('native_accounting_bootstrap'):
        bootstrap_command=shlex.join(['python','-u','-c',payload['native_accounting_bootstrap']])
        assert text.count(command+'\n')==1
        text=text.replace(command+'\n',bootstrap_command+'\n'+command+'\n',1)
    text=text.replace('set -euo pipefail','set -euo pipefail\nexport PYTHONDONTWRITEBYTECODE=1',1)
    directory=root/('plans/recovery-support-20261021-'+payload['id']);ledger=root/('state/recovery-support-20261021-'+payload['id']+'-submission.json')
    assert not directory.exists() and not directory.is_symlink() and not ledger.exists() and not ledger.is_symlink();directory.mkdir(mode=0o700)
    script=directory/(name+'.sbatch')
    with script.open('x') as f:f.write(text);f.flush();os.fsync(f.fileno())
    task={'name':name,'script':str(script),'script_sha256':canonical_hash(text),'command':command,'depends':[],'resources':{'cpus':payload['cpus'],'memory':payload['memory'],'hours':payload['hours'],'gpu':False},'dependency_kind':'afterok','external_dependencies':dependencies}
    if payload['id']=='433':task['resources']['array']=None
    plan={'schema_version':2,'phase':'recovery-support','root':str(root),'config_sha256':canonical_hash(config),'release_identity':identity['source_identity'],'tasks':[task],'cpu_ceiling':256,'gpu_concurrency':4,'resource_policy':admission['resource_policy'],'maximum_simultaneous_resources':{'cpus':payload['cpus'],'gpus':0}}
    atomic_json(directory/'plan.json',plan)
    intent={k:v for k,v in payload.items() if k!='derived_helper_body'}
    intent.update(args=args,automatic_replay_permitted=False,plan_path=str(directory/'plan.json'),ledger_path=str(ledger),quota_stdout=quota.stdout)
    atomic_json(area/'submission-intent.json',intent)
    for path,expected in payload['canonical_sources'].items():assert digest(Path(path))==expected,'Frozen source changed before submission'
    submitted=submit(directory/'plan.json',ledger)
print('RECOVERY_STAGE_SUBMITTED '+json.dumps({'request_id':payload['id'],'area':str(area),'helper_sha256':payload['helper_sha256'],'tasks':{k:{x:v.get(x) for x in ['job_id','state']} for k,v in submitted['tasks'].items()}},sort_keys=True),flush=True)

PY
