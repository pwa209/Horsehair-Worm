from pathlib import Path
import ast,base64,zlib,hashlib,json,subprocess,sys,runpy,re,xml.etree.ElementTree as ET
r='/scratch/pwa209/horsehair-worm';area=r+'/maintenance/study-af-fresh-storage-census-stage-v2-20261021'
pins={'run-fresh-storage-census-v1-20261021.py':'084651bd0accb1127f1e41ce94bb763bbe308755819642d365c9eff9f78d02ce','test_run_fresh_storage_census_v1_20261021.py':'808d0521d586d6523bbcc5f184b96e406c5c681a3f052e9af56924917f18f2d4'}
pins['run-ethoscope-sqlite-audit-v3-20261021.py']='81f4dcc425c9e7b242ba96223c20ecd3b3711360fc389827e81eaf13df8af274'
sources={}
for name,sha in pins.items():
 raw=(Path('work')/name).read_bytes();assert hashlib.sha256(raw).hexdigest()==sha;ast.parse(raw)
 sources[name]={'data':base64.b64encode(zlib.compress(raw)).decode(),'sha256':sha}
xml=ET.fromstring(Path('work/census-root-review-20261021.xml').read_bytes());roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in xml.iter('testcase'));assert len(roster)==35
rostersha=hashlib.sha256(json.dumps(roster,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
args=['--native-script',r+'/plans/recovery-support-20261021-295/recovery-295.sbatch','--registry-qualification',r+'/maintenance/study-af-registry-repair-support-v1-20261021/private-qualification.json','--registry-qualification-sha256','30184b679dbedce8aa6bc0e6a328e3d5d077de38d1897434e19cb5cb4b4a0f6b','--registry-qualification-job-id','6696233']
code="from pathlib import Path\nimport ast,base64,zlib,hashlib,os,sys,runpy,subprocess,xml.etree.ElementTree as ET,json\nfrom horsehair.core import canonical_hash\narea=Path(__file__).resolve().parent\nfor name,item in "+repr(sources)+".items():\n raw=zlib.decompress(base64.b64decode(item['data']));assert hashlib.sha256(raw).hexdigest()==item['sha256'];ast.parse(raw)\n with (area/name).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
code+="""temp=Path(os.environ['SLURM_TMPDIR']);assert temp.is_absolute() and temp.is_dir() and temp.stat().st_uid==3164768
workspace=temp/('hhw-fresh-census-tests-'+os.environ['SLURM_JOB_ID']);workspace.mkdir(mode=0o700,exist_ok=False)
env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','TZ':'UTC'}
for k in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(k,None)
xml=area/'census35.xml'
subprocess.run([sys.executable,'-s','-m','pytest',str(area/'test_run_fresh_storage_census_v1_20261021.py'),'-q','-p','no:cacheprovider','--basetemp',str(workspace/'fixtures'),'--junitxml',str(xml)],env=env,timeout=300,check=True)
cases=list(ET.fromstring(xml.read_bytes()).iter('testcase'));rows=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
"""
code+="assert len(cases)==35 and canonical_hash(rows)=="+repr(rostersha)+" and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases)\n"
code+="helper=area/'run-fresh-storage-census-v1-20261021.py'\nsys.argv=[str(helper)]+"+repr(args)+"\nrunpy.run_path(str(helper),run_name='__main__')\n"
ast.parse(code);helper=Path('work/run-fresh-census-stage-v2-20261021.py');helper.write_bytes(code.encode())
subprocess.run([sys.executable,'work/build-native-stage-20261021.py','--id','295','--helper',str(helper),'--area',area,'--framework',r+'/releases/20261021-6ed201653dfa-4f3903d7c26e','--cpus','4','--memory','16G','--hours','2'],check=True)
p=Path('work/hhw-recovery-stage-20261021-295.sh');text=runpy.run_path('work/patch-af-stage-expanded-20261021.py')['patch'](p.read_text())
# The frozen builder permits at most two hours; this explicit maintenance-only
# amendment reserves exactly the independently reviewed four-hour census bound.
pattern=r"payload=json.loads\(zlib.decompress\(base64.b64decode\('([^']+)'\)\)\)"
m=re.search(pattern,text);payload=json.loads(zlib.decompress(base64.b64decode(m.group(1))));assert payload['hours']==2 and payload['cpus']==4 and payload['memory']=='16G';payload['hours']=4
text=text[:m.start()]+"payload=json.loads(zlib.decompress(base64.b64decode("+repr(base64.b64encode(zlib.compress(json.dumps(payload).encode())).decode())+")))"+text[m.end():]
needle='from horsehair.campaign import combined_admission\n'
insert="import time\nfrom horsehair.core import exclusive_file_lock\nrgp=root/'maintenance/study-af-registry-repair-support-v1-20261021/registered-snapshot-read-guard-20261021.py'\nassert digest(rgp)=='42fd488d27d1bf9aa75b46c113257525e296c526250e75ba12319d6139854db4'\nrunpy.run_path(str(rgp))['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)\n"
assert text.count(needle)==1;text=text.replace(needle,insert+needle,1)
start=text.index('admission=combined_admission');end=text.index("print('RECOVERY_STAGE_SUBMITTED",start)
text=text[:start]+"with exclusive_file_lock(root/'state/af-campaign-registration.mutex',timeout=60):\n"+''.join('    '+s if s.strip() else s for s in text[start:end].splitlines(keepends=True))+text[end:]
ast.parse(text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]);p.write_bytes(text.encode());print('built295',rostersha)
