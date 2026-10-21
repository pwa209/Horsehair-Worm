#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import subprocess,json,hashlib,os,re,xml.etree.ElementTree as ET
r=Path('/scratch/pwa209/horsehair-worm');area=r/'maintenance/study-af-vtl-s3-metadata-v1-20261021';area.mkdir(mode=0o700,exist_ok=False)
host='https://pmc-oa-opendata.s3.amazonaws.com';attempts=[]
def fetch(url,name):
 target=area/name
 args=['curl','--silent','--show-error','--fail','--location','--proto','=https','--proto-redir','=https','--max-redirs','2','--max-time','25','--max-filesize','1000000','--dump-header',str(area/(name+'.headers')),'--output',str(target),'--write-out','%{http_code}|%{url_effective}|%{size_download}',url]
 try:
  p=subprocess.run(args,capture_output=True,text=True,timeout=30);result={'url':url,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
 except subprocess.TimeoutExpired as error:result={'url':url,'status':'parent_timeout','stderr':str(error.stderr)}
 attempts.append(result)
 if result.get('returncode')!=0 or result.get('stdout','').split('|')[0]!='200' or not target.is_file() or target.stat().st_size>1000000:return None
 raw=target.read_bytes();result.update(path=str(target),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest());return raw
listing=fetch(host+'/?list-type=2&prefix=PMC11063160.&delimiter=/&max-keys=20','versions.xml');records=[]
if listing and b'<!DOCTYPE' not in listing.upper() and b'<!ENTITY' not in listing.upper():
 try:
  tree=ET.fromstring(listing);ns={'s':'http://s3.amazonaws.com/doc/2006-03-01/'}
  assert tree.findtext('s:IsTruncated',namespaces=ns)=='false'
  prefixes=[x.text for x in tree.findall('s:CommonPrefixes/s:Prefix',ns)];assert len(prefixes)<=10
  for prefix in prefixes:
   assert re.fullmatch(r'PMC11063160\.[1-9][0-9]*/',prefix)
   key=prefix+prefix[:-1]+'.json';raw=fetch(host+'/'+key,prefix[:-1]+'.json')
   if raw:
    try:
     data=json.loads(raw)
     if data.get('pmcid')=='PMC11063160' and data.get('doi')=='10.1038/s41467-024-48005-w':records.append(data)
    except (ValueError,AttributeError):pass
 except (ValueError,AssertionError,ET.ParseError):pass
value={'status':'complete_public_s3_article_metadata' if records else 'public_s3_article_metadata_unavailable','checked_utc':datetime.now(timezone.utc).isoformat(),'provider':'NIH NLM NCBI PubMed Central Article Datasets on AWS','official_distribution_documentation':'https://pmc.ncbi.nlm.nih.gov/tools/pmcaws/','metadata_only':True,'scientific_data_downloaded':False,'scientific_result':False,'records':records,'attempts':attempts}
with (area/'receipt.json').open('xb') as f:f.write(json.dumps(value,sort_keys=True,indent=2,allow_nan=False).encode()+bytes([10]));f.flush();os.fsync(f.fileno())
print('AF_VTL_S3_METADATA '+json.dumps(value,sort_keys=True),flush=True)
PY
