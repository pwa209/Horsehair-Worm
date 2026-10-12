#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
test -n "${SLURM_JOB_ID:-}" || { echo 'Environment bootstrap must run under Slurm'; exit 2; }
module load StdEnv/2023 python/3.11.5 cuda/12.6 arrow/19.0.1
STUDY=/scratch/pwa209/horsehair-worm
ENV=/home/pwa209/hhw-environment
PROJECT=/project/def-ptewarie/pwa209/horsehair-worm
mkdir -p "$STUDY/state" "$PROJECT/environment"
if ! test -x "$ENV/bin/python"; then python -m venv "$ENV"; fi
source "$ENV/bin/activate"
BOOTSTRAP_DIR=${HHW_BOOTSTRAP_DIR:-$PROJECT/bootstrap}
python -m pip install --no-index -r "$BOOTSTRAP_DIR/requirements-narval.txt"
python -m pip freeze > "$PROJECT/environment/python-$(date -u +%Y%m%dT%H%M%SZ).txt"
module list 2> "$PROJECT/environment/modules-bootstrap.txt"
python - <<'PY'
import importlib.metadata as m,json,sys,pathlib,torch,time,subprocess,os,urllib.request
root=pathlib.Path('/scratch/pwa209/horsehair-worm/state')
packages={x:m.version(x) for x in ['numpy','scipy','pandas','torch','scikit-learn','pyarrow','h5py','statsmodels','pytest']}
record={'status':'complete','job_id':os.environ['SLURM_JOB_ID'],'python':sys.version,'packages':packages,'completed_unix':time.time(),'cuda_available':torch.cuda.is_available()}
(root/'environment.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record))
PY
# Network diagnosis is measured on a compute node; no proxy credentials are logged.
module load httpproxy/1.0
python - <<'PY'
import urllib.request,json,pathlib
urls={'ena':'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=PRJNA918668&result=read_run&fields=run_accession&format=json&limit=1','zenodo':'https://zenodo.org/api/records/7464925','dryad':'https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.jh9w0vtkb','ncbi':'https://ftp.ncbi.nlm.nih.gov/geo/series/GSE237nnn/GSE237182/suppl/'}
result={}
for name,url in urls.items():
 result[name]={}
 for route,proxies in [('proxy',None),('direct',{})]:
  try:
   opener=urllib.request.build_opener(urllib.request.ProxyHandler(proxies))
   with opener.open(url,timeout=10) as r:result[name][route]={'http_status':r.status,'read_bytes':len(r.read(256))}
  except Exception as e:result[name][route]={'error':type(e).__name__,'message':str(e)}
pathlib.Path('/scratch/pwa209/horsehair-worm/state/network.json').write_text(json.dumps(result,indent=2)+'\n')
print('COMPUTE_NETWORK',json.dumps(result))
PY

