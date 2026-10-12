"""Build the reviewed, immutable environment-bootstrap request, without credentials."""
from pathlib import Path
import base64
import hashlib
import json
import argparse

parser=argparse.ArgumentParser()
parser.add_argument('--version',default='v1')
args=parser.parse_args()
if not args.version.replace('-','').isalnum():raise ValueError('Invalid version')

root=Path(__file__).resolve().parents[1]
files={p.name:p.read_bytes() for p in [root/'scripts/bootstrap_narval.sh',root/'requirements-narval.txt']}
payload={name:{'base64':base64.b64encode(data).decode(),'sha256':hashlib.sha256(data).hexdigest()} for name,data in files.items()}
header='''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
module load StdEnv/2023 python/3.11.5
python - <<'PY'
import pathlib,json,base64,hashlib,subprocess,time
root=pathlib.Path('/scratch/pwa209/horsehair-worm')
project=pathlib.Path('/project/def-ptewarie/pwa209/horsehair-worm')
bootstrap=project/'BOOTSTRAP_NAME'
for p in [root/'state',root/'logs',root/'raw',root/'data',root/'manifests',project/'releases',bootstrap]:p.mkdir(parents=True,exist_ok=True)
payload=PAYLOAD_JSON
for name,record in payload.items():
 data=base64.b64decode(record['base64']);assert hashlib.sha256(data).hexdigest()==record['sha256']
 path=bootstrap/name
 if path.exists():
  assert path.read_bytes()==data,'Bootstrap differs; use a new versioned namespace'
 else:path.write_bytes(data)
script=''' + "'''" + '''#!/bin/bash
#SBATCH --account=def-ptewarie_cpu
#SBATCH --job-name=hhw-environment
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=02:00:00
#SBATCH --output=/scratch/pwa209/horsehair-worm/logs/environment-%j.log
export HHW_BOOTSTRAP_DIR=/project/def-ptewarie/pwa209/horsehair-worm/BOOTSTRAP_NAME
bash "$HHW_BOOTSTRAP_DIR/bootstrap_narval.sh"
''' + "'''" + '''
jobscript=bootstrap/'environment.sbatch'
if jobscript.exists():assert jobscript.read_text()==script
else:jobscript.write_text(script)
ledger=root/'state'/'BOOTSTRAP_LEDGER.json'
if ledger.exists():
 print('EXISTING_BOOTSTRAP',ledger.read_text())
else:
 ledger.write_text(json.dumps({'state':'submitting','created_unix':time.time(),'source_sha256':payload['bootstrap_narval.sh']['sha256']},indent=2))
 p=subprocess.run(['sbatch','--parsable',str(jobscript)],capture_output=True,text=True)
 job=p.stdout.strip().split(';')[0]
 if p.returncode!=0 or not job.isdigit():
  print(p.stdout,p.stderr);raise RuntimeError('Ambiguous bootstrap submission: never replay')
 ledger.write_text(json.dumps({'state':'submitted','job_id':job,'source_sha256':payload['bootstrap_narval.sh']['sha256']},indent=2))
 print('BOOTSTRAP_SUBMITTED',job)
PY
'''
request=root/f'work/hhw-bootstrap-20261012-{args.version}.sh'
bootstrap_name='bootstrap' if args.version=='v1' else 'bootstrap-'+args.version
ledger_name='bootstrap-submission' if args.version=='v1' else 'bootstrap-submission-'+args.version
request.write_text(header.replace('PAYLOAD_JSON',repr(payload)).replace('BOOTSTRAP_NAME',bootstrap_name).replace('BOOTSTRAP_LEDGER',ledger_name),encoding='utf8',newline='\n')
print(request)

