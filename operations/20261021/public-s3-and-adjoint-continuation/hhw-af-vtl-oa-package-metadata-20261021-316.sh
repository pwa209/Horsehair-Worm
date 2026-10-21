#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import subprocess,json,hashlib,os,xml.etree.ElementTree as ET
r=Path('/scratch/pwa209/horsehair-worm');area=r/'maintenance/study-af-vtl-oa-package-metadata-v1-20261021';area.mkdir(mode=0o700,exist_ok=False)
url='https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id=PMC11063160';target=area/'oa-metadata-response.txt'
args=['curl','--silent','--show-error','--fail','--location','--proto','=https','--proto-redir','=https','--max-redirs','3','--max-time','25','--max-filesize','1000000','--dump-header',str(area/'http-headers.txt'),'--output',str(target),'--write-out','%{http_code}|%{url_effective}|%{size_download}',url]
with (area/'intent.json').open('x') as f:json.dump({'url':url,'command':args,'metadata_only':True,'package_not_downloaded':True},f,sort_keys=True)
try:
 p=subprocess.run(args,capture_output=True,text=True,timeout=30);out={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
except subprocess.TimeoutExpired as error:out={'status':'parent_timeout','stderr':str(error.stderr)}
value={'url':url,'checked_utc':datetime.now(timezone.utc).isoformat(),'transfer':out,'metadata_only':True,'scientific_result':False,'status':'public_oa_metadata_unavailable','links':[]}
if out.get('returncode')==0 and out.get('stdout','').split('|')[0]=='200' and target.is_file() and target.stat().st_size<=1000000:
 raw=target.read_bytes();value.update(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
 if b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper():
  try:
   tree=ET.fromstring(raw)
   if tree.tag=='OA':
    records=[x for x in tree.iter('record') if x.get('id')=='PMC11063160']
    if len(records)==1:value.update(status='complete_public_oa_record_metadata',record=records[0].attrib,links=[x.attrib for x in records[0].iter('link')])
  except ET.ParseError:pass
with (area/'receipt.json').open('xb') as f:f.write(json.dumps(value,sort_keys=True,indent=2,allow_nan=False).encode()+bytes([10]));f.flush();os.fsync(f.fileno())
print('AF_VTL_OA_METADATA '+json.dumps(value,sort_keys=True),flush=True)
PY
