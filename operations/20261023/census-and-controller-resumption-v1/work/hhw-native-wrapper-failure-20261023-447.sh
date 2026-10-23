#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import os,json,hashlib,xml.etree.ElementTree as ET
assert os.geteuid()==3164768
q=Path('/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-qualification-v3-20261023')
o={'checked_utc':datetime.now(timezone.utc).isoformat(),'read_only':True,'files':{},'suites':{}}
for n in ['wrapper-stdout.txt','wrapper-stderr.txt','wrapper-intent.json','native-python-dependencies.json']:
 p=q/n;raw=p.read_bytes();assert len(raw)<1000000;o['files'][n]={'sha256':hashlib.sha256(raw).hexdigest(),'text':raw.decode(errors='replace')}
for p in q.glob('*.xml'):
 raw=p.read_bytes();assert len(raw)<3000000;t=ET.fromstring(raw);cases=list(t.iter('testcase'));o['suites'][p.name]={'sha256':hashlib.sha256(raw).hexdigest(),'tests':len(cases),'failures':[{'name':c.get('name'),'classname':c.get('classname'),'details':ET.tostring(c,encoding='unicode')} for c in cases if list(c)]}
print('NATIVE_WRAPPER_FAILURE '+json.dumps(o,sort_keys=True),flush=True)
PY
