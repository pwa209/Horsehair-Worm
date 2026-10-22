"""Fresh versioned retry for the source-specific opaque printer metadata."""
from pathlib import Path
import ast,base64,hashlib,json,runpy,subprocess,sys,xml.etree.ElementTree as ET,zlib

root='/scratch/pwa209/horsehair-worm'
area=root+'/maintenance/study-af-stock-metadata-stage-v2-20261022'
pins={'audit-vtl-stock-workbook-schema-v2-20261022.py':'77af55c3cd2f1ef0b0ca4ed38252e86b39591d4104007812771da994fb92ab4f',
      'test_audit_vtl_stock_workbook_schema_v2_20261022.py':'b5c33f212d7d256a27418ff19de657346af501bc1a60cfb73889a295dd414d42'}
contents={}
for name,sha in pins.items():
 raw=(Path('work')/name).read_bytes();assert hashlib.sha256(raw).hexdigest()==sha;ast.parse(raw);contents[name]=raw
cases=list(ET.fromstring(Path('work/stock-v2-root-final-20261022.xml').read_bytes()).iter('testcase'))
rows=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
assert len(cases)==len(set(rows))==76 and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases)
roster=hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).hexdigest()
assert roster=='e220c9312f6a795366854116c9b706f8ba8e06e019910469607631ef90f365cb'
template=Path('work/run-native-metadata-gate-v1-20261021.py').read_bytes()
assert hashlib.sha256(template).hexdigest()=='00f743f2943fa3d58b8b4dc8cd4c1f6b26f38375d3504977cea4da61455edd2d'
binding={'stock':{'helper':'audit-vtl-stock-workbook-schema-v2-20261022.py','helper_sha256':pins['audit-vtl-stock-workbook-schema-v2-20261022.py'],
 'tests':'test_audit_vtl_stock_workbook_schema_v2_20261022.py','tests_sha256':pins['test_audit_vtl_stock_workbook_schema_v2_20261022.py'],
 'count':76,'worker_seconds':900,'arguments':[],'roster_sha256':roster}}
text=template.decode();assert text.count('FROZEN_SUITES = None')==1
text=text.replace('FROZEN_SUITES = None','FROZEN_SUITES = '+repr(binding),1)
# Only this attempt's three generated metadata namespaces change.
assert text.count("+'-metadata-stage-v1-20261021'")==1 and text.count("+'-metadata-audit-v1-20261021'")==1 and text.count("+'-metadata-support-v1-20261021'")==1
for kind in ['stage','audit','support']:text=text.replace("+'-metadata-"+kind+"-v1-20261021'","+'-metadata-"+kind+"-v2-20261022'",1)
ast.parse(text);parent='run-native-stock-metadata-gate-v2-20261022.py'
with (Path('work')/parent).open('xb') as f:f.write(text.encode())
contents[parent]=text.encode()
sources={name:{'sha256':hashlib.sha256(raw).hexdigest(),'data':base64.b64encode(zlib.compress(raw)).decode()} for name,raw in contents.items()}
code="from pathlib import Path\nimport ast,base64,zlib,hashlib,os,sys,runpy\narea=Path(__file__).resolve().parent\nfor name,item in "+repr(sources)+".items():\n raw=zlib.decompress(base64.b64decode(item['data']));assert hashlib.sha256(raw).hexdigest()==item['sha256'];ast.parse(raw)\n with (area/name).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\nhelper=area/"+repr(parent)+"\nsys.argv=[str(helper),'--mode','stock','--native-script',"+repr(root+'/plans/recovery-support-20261021-342/recovery-342.sbatch')+"]\nrunpy.run_path(str(helper),run_name='__main__')\n"
ast.parse(code);helper=Path('work/run-native-stock-metadata-stage-v2-20261022.py')
with helper.open('xb') as f:f.write(code.encode())
subprocess.run([sys.executable,'work/build-native-stage-20261021.py','--id','342','--helper',str(helper),'--area',area,
 '--framework',root+'/releases/20261021-6ed201653dfa-4f3903d7c26e','--cpus','1','--memory','16G','--hours','1'],check=True)
p=Path('work/hhw-recovery-stage-20261021-342.sh');text=runpy.run_path('work/patch-af-stage-expanded-20261021.py')['patch'](p.read_text())
needle='from horsehair.campaign import combined_admission\n'
insert="import time\nrgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'\nassert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'\napi=runpy.run_path(str(rgp));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)\n"
assert text.count(needle)==1;text=text.replace(needle,insert+needle,1)
start=text.index('admission=combined_admission');end=text.index("print('RECOVERY_STAGE_SUBMITTED",start)
text=text[:start]+"with api['registration_lock'](area/'submission-registration-lock',time.time()+900,role='controller-start'):\n"+''.join('    '+line if line.strip() else line for line in text[start:end].splitlines(keepends=True))+text[end:]
needle="task['dependency_kind']=payload['dependency_kind']";assert text.count(needle)==1
text=text.replace(needle,needle+"\n    task['external_dependencies']=['6699739']\n    task['external_dependency_specs']=[{'job_id':'6699739','array':None,'purpose':'Positive exact-size census before versioned read-only metadata retry; priorfailed6704402evidencepreserved'}]",1)
ast.parse(text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]);p.write_bytes(text.encode())
print(json.dumps({'request':'342','wrapper_sha256':sources[parent]['sha256'],'stage_sha256':hashlib.sha256(text.encode()).hexdigest(),'actual_native_prerequisite_tests':76,'source_stage':area},sort_keys=True))
