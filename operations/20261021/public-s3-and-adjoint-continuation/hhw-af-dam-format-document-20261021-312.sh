#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
test "$(id -u)" = 3164768
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import subprocess,json,hashlib,os
r=Path('/scratch/pwa209/horsehair-worm');area=r/'maintenance/study-af-dam-manufacturer-format-v2-20261021';area.mkdir(mode=0o700,exist_ok=False)
url='https://trikinetics.com/Downloads/DAMSystem3%20Software%20Data%20Sheet.pdf';target=area/'DAMSystem3-Software-Data-Sheet.pdf.partial'
args=['curl','--silent','--show-error','--fail','--location','--proto','=https','--proto-redir','=https','--max-redirs','3','--max-time','45','--max-filesize','25000000','--dump-header',str(area/'http-headers.txt'),'--output',str(target),'--write-out','%{http_code}|%{url_effective}|%{size_download}',url]
with (area/'intent.json').open('x') as f:json.dump({'url':url,'command':args,'maximum_bytes':25000000,'documentation_only':True,'scientific_data_acquisition':False},f,sort_keys=True)
try:
 p=subprocess.run(args,capture_output=True,text=True,timeout=50);out={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
except subprocess.TimeoutExpired as error:out={'status':'parent_timeout','stderr':str(error.stderr)}
value={'url':url,'checked_utc':datetime.now(timezone.utc).isoformat(),'transfer':out,'documentation_only':True,'deposited_code_executed':False,'scientific_result':False,'status':'not_acquired'}
if out.get('returncode')==0 and out.get('stdout','').split('|')[0]=='200' and target.is_file() and 0<target.stat().st_size<=25000000:
 raw=target.read_bytes()
 if raw.startswith(b'%PDF-') and b'%%EOF' in raw[-2048:]:
  final=area/'DAMSystem3-Software-Data-Sheet.pdf';os.link(target,final);target.unlink()
  value.update(status='complete_manufacturer_document_acquisition',path=str(final),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),sha256_basis='first_complete_acquisition_not_provider_checksum',provider_checksum_verified=False)
with (area/'receipt.json').open('xb') as f:f.write(json.dumps(value,sort_keys=True,indent=2,allow_nan=False).encode()+bytes([10]));f.flush();os.fsync(f.fileno())
print('AF_DAM_MANUFACTURER_FORMAT '+json.dumps(value,sort_keys=True),flush=True)
PY
