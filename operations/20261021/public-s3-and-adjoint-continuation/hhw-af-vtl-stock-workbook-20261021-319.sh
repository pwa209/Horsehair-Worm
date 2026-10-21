#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
from urllib.parse import urlsplit,parse_qs
import subprocess,json,hashlib,os,zipfile
r=Path('/scratch/pwa209/horsehair-worm');source=r/'maintenance/study-af-vtl-s3-metadata-v1-20261021/PMC11063160.1.json';raw=source.read_bytes()
assert len(raw)==3335 and hashlib.sha256(raw).hexdigest()=='91bde60e247081cd5afef1a1a121bda8577308bb9a78ab81d13f8f03625dfee1'
meta=json.loads(raw);assert meta['pmcid']=='PMC11063160' and meta['doi']=='10.1038/s41467-024-48005-w' and meta['is_manuscript'] is False and meta['license_code']=='CC BY'
name='41467_2024_48005_MOESM8_ESM.xlsx';links=[x for x in meta['media_urls'] if urlsplit(x).path=='/PMC11063160.1/'+name];assert len(links)==1
parsed=urlsplit(links[0]);assert parsed.scheme=='s3' and parsed.netloc=='pmc-oa-opendata' and not parsed.fragment
assert parse_qs(parsed.query)=={'md5':['38192352700c09694811a8e8e43e8097']}
url='https://pmc-oa-opendata.s3.amazonaws.com'+parsed.path;area=r/'maintenance/study-af-vtl-stock-workbook-s3-v1-20261021';area.mkdir(mode=0o700,exist_ok=False);target=area/(name+'.partial')
args=['curl','--silent','--show-error','--fail','--location','--proto','=https','--proto-redir','=https','--max-redirs','2','--max-time','60','--max-filesize','50000000','--dump-header',str(area/'http-headers.txt'),'--output',str(target),'--write-out','%{http_code}|%{url_effective}|%{size_download}',url]
with (area/'intent.json').open('x') as f:json.dump({'url':url,'provider_metadata':{'path':str(source),'sha256':hashlib.sha256(raw).hexdigest()},'provider_md5':'38192352700c09694811a8e8e43e8097','metadata_supplement_only':True,'command':args,'maximum_bytes':50000000},f,sort_keys=True)
try:
 p=subprocess.run(args,capture_output=True,text=True,timeout=65);out={'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
except subprocess.TimeoutExpired as error:out={'status':'parent_timeout','stderr':str(error.stderr)}
value={'url':url,'checked_utc':datetime.now(timezone.utc).isoformat(),'transfer':out,'status':'not_acquired','workbook_values_inspected':False,'scientific_result':False,'author_contact':False}
if out.get('returncode')==0 and out.get('stdout','').split('|')[0]=='200' and target.is_file() and 0<target.stat().st_size<=50000000:
 raw=target.read_bytes();md5=hashlib.md5(raw).hexdigest();value.update(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),actual_md5=md5)
 if md5=='38192352700c09694811a8e8e43e8097' and zipfile.is_zipfile(target):
  with zipfile.ZipFile(target) as z:
   names=z.namelist();assert len(names)<=10000 and '[Content_Types].xml' in names and 'xl/workbook.xml' in names and sum(x.file_size for x in z.infolist())<=100000000
  final=area/name;os.link(target,final);target.unlink();value.update(status='provider_md5_verified_official_stock_workbook_acquired',path=str(final),provider_md5_verified=True,provider_metadata={'path':str(source),'sha256':'91bde60e247081cd5afef1a1a121bda8577308bb9a78ab81d13f8f03625dfee1'},license_code='CC BY',publisher_version_verified=True)
with (area/'receipt.json').open('xb') as f:f.write(json.dumps(value,sort_keys=True,indent=2,allow_nan=False).encode()+bytes([10]));f.flush();os.fsync(f.fileno())
print('AF_VTL_STOCK_WORKBOOK '+json.dumps(value,sort_keys=True),flush=True)
PY
