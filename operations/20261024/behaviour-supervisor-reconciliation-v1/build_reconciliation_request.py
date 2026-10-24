"""Package committed verifier sources and build one non-replayable registration."""
from pathlib import Path
import argparse
import base64
import hashlib
import json
import subprocess
import zlib

AREA = Path(__file__).resolve().parent
REPO = AREA.parents[2]


def build(request):
    names = ['expected-original-manifest.json', 'reconcile_completed_prefix.py',
             'test_reconcile_completed_prefix.py', 'run_native_reconciliation.py',
             'test_native_reconciliation_runner.py',
             'lifecycle/run-full-cohort-prefix-v5-exit-race-repaired-20261024.py',
             'lifecycle/test_lifecycle_exit_race_20261024.py', 'lifecycle/manifest.json']
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    files = {}
    for name in names:
        path = AREA/name
        raw = path.read_bytes()
        committed = subprocess.check_output(['git', 'show', commit+':'+path.relative_to(REPO).as_posix()], cwd=REPO)
        if raw != committed:
            raise ValueError('Commit reviewed sources before packaging: '+name)
        files[name] = raw
    expected = json.loads(files['expected-original-manifest.json'])
    lifecycle = json.loads((AREA/'lifecycle/local-test-result.json').read_text())
    reconcile = json.loads((AREA/'reconcile-local-test-result.json').read_text())
    native = json.loads((AREA/'native-runner-local-test-result.json').read_text())
    source = {'schema_version': 1, 'git_commit': commit,
              'files': {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()},
              'original_helper_path': expected['helper_path'],
              'original_helper_sha256': expected['helper_sha256'],
              'original_context_path': 'maintenance/study-af-resubmission-433-20261023/run-full-cohort-prefix-context-v5-20261022.py',
              'original_support_path': expected['support'],
              'test_suites': [
                  {'name': 'lifecycle', 'path': names[6], 'tests': lifecycle['tests']},
                  {'name': 'reconciliation', 'path': names[2], 'tests': reconcile['tests']},
                  {'name': 'native-runner', 'path': names[4], 'tests': native['tests']}],
              'scientific_gates_changed': False}
    for result in [lifecycle, reconcile, native]:
        if not (result['successful'] is True and all(result[k] == 0 for k in ['failures', 'errors', 'skipped'])):
            raise ValueError('Complete passing local tests required')
    manifest = (json.dumps(source, sort_keys=True, indent=2)+'\n').encode()
    files['source-manifest.json'] = manifest
    payload = {name: {'data': base64.b64encode(zlib.compress(raw, 9)).decode(),
                      'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
               for name, raw in files.items()}
    body = r'''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
source /home/pwa209/hhw-environment/bin/activate
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=/scratch/pwa209/horsehair-worm/releases/20261021-caebc9c1dc77-8495a3d98551/src
python -B -u - <<'PY'
from pathlib import Path,PurePosixPath
from datetime import datetime,timezone
import base64,hashlib,json,os,re,subprocess,zlib
from horsehair.core import canonical_hash,read_json,exclusive_file_lock
from horsehair import slurm
assert os.geteuid()==3164768
root=Path('/scratch/pwa209/horsehair-worm')
framework=root/'releases/20261021-caebc9c1dc77-8495a3d98551'
capsule=root/'maintenance/study-behaviour-supervisor-reconciliation-source-v1-20261024'
phase='behaviour-reconciliation-v1-20261024'
plan_dir=root/'plans'/phase
ledger_path=root/'state'/(phase+'-submission.json')
registration=root/'maintenance/study-behaviour-supervisor-reconciliation-registration-v1-20261024'
payload=PAYLOAD
assert not capsule.exists() and not capsule.is_symlink()
assert not plan_dir.exists() and not ledger_path.exists() and not registration.exists()
for p in [capsule.parent,plan_dir.parent,registration.parent]:assert p.is_dir() and not p.is_symlink()
capsule.mkdir(mode=0o700);registration.mkdir(mode=0o700);plan_dir.mkdir(mode=0o700)
def save(p,v):
 data=(json.dumps(v,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
 with p.open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
for name,entry in payload.items():
 rel=PurePosixPath(name);assert not rel.is_absolute() and '..' not in rel.parts
 data=zlib.decompress(base64.b64decode(entry['data']))
 assert len(data)==entry['bytes'] and hashlib.sha256(data).hexdigest()==entry['sha256']
 target=capsule/name;target.parent.mkdir(exist_ok=True,parents=True)
 with target.open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
 target.chmod(0o444)
source_sha=hashlib.sha256((capsule/'source-manifest.json').read_bytes()).hexdigest()
assert source_sha==SOURCE_SHA
config=read_json(framework/'configs/study.json');release=read_json(framework/'release.json')
name='behaviour-reconciliation-v1'
command='/home/pwa209/hhw-environment/bin/python -B -u '+str(capsule/'run_native_reconciliation.py')+' --source-manifest-sha256 '+source_sha
text=slurm._script(config,framework,name,command,cpus=4,memory='16G',hours=2)
script=plan_dir/(name+'.sbatch')
with script.open('x',encoding='utf-8',newline='\n') as f:f.write(text);f.flush();os.fsync(f.fileno())
task={'name':name,'command':command,'script':str(script),'script_sha256':canonical_hash(text),
      'depends':[],'external_dependencies':[],'dependency_kind':'afterok',
      'resources':{'cpus':4,'memory':'16G','hours':2,'gpu':False,'array':None}}
plan={'schema_version':2,'phase':phase,'root':str(root),'release':str(framework),
      'release_identity':release['source_identity'],'config_sha256':release['config_sha256'],
      'tasks':[task],'scope':'Independent verification only; original failed allocation and scientific gates preserved'}
plan_path=plan_dir/'plan.json';save(plan_path,plan)
def held_runner(args,**kwargs):
 actual=[args[0],'--hold',*args[1:]]
 save(registration/'submit-intent.json',{'command':actual,'plan_sha256':canonical_hash(plan),'automatic_replay_permitted':False})
 p=subprocess.run(actual,**kwargs)
 save(registration/'submit-response.json',{'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr})
 return p
with exclusive_file_lock(root/'state/af-campaign-registration.mutex',timeout=60):
 ledger=slurm.submit(plan_path,ledger_path,runner=held_runner)
 job=str(ledger['tasks'][name]['job_id']);assert re.fullmatch('[1-9][0-9]*',job)
 p=subprocess.run(['scontrol','show','job',job,'-o'],capture_output=True,text=True,timeout=45);p.check_returncode()
 import shlex
 fields=dict(t.split('=',1) for t in shlex.split(p.stdout) if '=' in t)
 assert fields.get('UserId')=='pwa209(3164768)' and fields.get('Account')=='def-ptewarie_cpu'
 assert fields.get('JobState')=='PENDING' and fields.get('Reason')=='JobHeldUser'
 assert fields.get('Command')==str(script) and fields.get('NumCPUs')=='4' and fields.get('MinMemoryNode') in {'16G','16384M'}
 assert fields.get('TimeLimit') in {'02:00:00','2:00:00'} and fields.get('RunTime')=='00:00:00'
 assert fields.get('Dependency')=='(null)' and fields.get('Restarts')=='0'
 save(registration/'held-native.json',{'native_stdout':p.stdout,'fields':fields,'source_manifest_sha256':source_sha})
 save(registration/'release-intent.json',{'job_id':job,'command':['scontrol','release',job],'automatic_replay_permitted':False})
 p=subprocess.run(['scontrol','release',job],capture_output=True,text=True,timeout=45)
 save(registration/'release-response.json',{'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr});p.check_returncode()
 save(registration/'receipt.json',{'status':'independent_reconciliation_queued','native_job_id':job,'plan':str(plan_path),'ledger':str(ledger_path),'source_manifest_sha256':source_sha,'git_commit':GIT_COMMIT,'scientific_gates_changed':False,'checked_utc':datetime.now(timezone.utc).isoformat()})
print('BEHAVIOUR_RECONCILIATION_REGISTERED '+json.dumps(read_json(registration/'receipt.json'),sort_keys=True),flush=True)
PY
'''
    body = body.replace('PAYLOAD', repr(payload)).replace('SOURCE_SHA', repr(hashlib.sha256(manifest).hexdigest())).replace('GIT_COMMIT', repr(commit))
    request = Path(request)
    with request.open('x', encoding='utf-8', newline='\n') as f:
        f.write(body)
    print(json.dumps({'request': str(request), 'source_manifest_sha256': hashlib.sha256(manifest).hexdigest(),
                      'git_commit': commit, 'files': len(files), 'payload_bytes': sum(len(v) for v in files.values())}, sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--request', type=Path, required=True)
    build(parser.parse_args().request)
