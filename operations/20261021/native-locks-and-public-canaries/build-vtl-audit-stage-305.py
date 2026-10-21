from pathlib import Path
import ast,base64,zlib,hashlib,json,subprocess,sys,runpy,xml.etree.ElementTree as ET
r='/scratch/pwa209/horsehair-worm';area=r+'/maintenance/study-af-vtl-structure-stage-v2-20261021'
pins={'run-vtl-track-schema-audit-v2-20261021.py':'284b082a04eccebe707378a6e8f26836a4467846e3b213c3e9ff9b4325a86ff1','test_run_vtl_track_schema_audit_v2_20261021.py':'b03824ab1135d5dbd45a1d52623461822a15b278cf2bfd66da8a5286298d84b3','audit-vtl-track-schema-20261021.py':'3da5b6a05436aa82d9ed94e9fcecdc9032ed04ecf890944313f384db3b572c1c','test_audit_vtl_track_schema_20261021.py':'63471f8706ccacd801de27314c37c30072d9806a8302df3f9cc7772807d0e930','acquire-vtl-track-canary-20261021.py':'2a866bb7842111b7fdcb7a56e05cf77e8ce6a866277f4b808d7f761a6f7a8ae1','test_acquire_vtl_track_canary_20261021.py':'a1c38e14262a243ad5c90fe217e8a44a413c01f0b2cfdc73a6ba067a95cc1ca4'}
sources={}
for name,sha in pins.items():
 raw=(Path('work')/name).read_bytes();assert hashlib.sha256(raw).hexdigest()==sha;ast.parse(raw);sources[name]={'data':base64.b64encode(zlib.compress(raw)).decode(),'sha256':sha}
for name,sha in {'VTL-1.vtl.py':'d83e7431c258977ba07e78b9f7430fe3e8a5dfb5b7569a5cfb9740e643f8d114','VTL-2.dataorder.py':'ff011406d399640da05b82c37256ea9d4a6f69f8b48e8c1ef93927ce43a3fe64'}.items():
 raw=(Path('work/public-cohort-metadata-20261021')/name).read_bytes();assert hashlib.sha256(raw).hexdigest()==sha;sources[name]={'data':base64.b64encode(zlib.compress(raw)).decode(),'sha256':sha}
xml=ET.fromstring(Path('work/vtl-wrapper-v2-root-20261021.xml').read_bytes());roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in xml.iter('testcase'));assert len(roster)==45
rostersha=hashlib.sha256(json.dumps(roster,sort_keys=True,separators=(',',':')).encode()).hexdigest()
args=['--auditor',area+'/audit-vtl-track-schema-20261021.py','--tests',area+'/test_audit_vtl_track_schema_20261021.py','--acquisition-helper',area+'/acquire-vtl-track-canary-20261021.py','--acquisition-tests',area+'/test_acquire_vtl_track_canary_20261021.py','--writer',area+'/VTL-1.vtl.py','--order',area+'/VTL-2.dataorder.py','--native-script',r+'/plans/recovery-support-20261021-305/recovery-305.sbatch','--registry-qualification',r+'/maintenance/study-af-registry-repair-support-v4-20261021/private-qualification.json','--registry-qualification-sha256','1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca','--registry-qualification-job-id','6699561']
code="from pathlib import Path\nimport ast,base64,zlib,hashlib,os,sys,runpy,subprocess,xml.etree.ElementTree as ET\nfrom horsehair.core import canonical_hash\narea=Path(__file__).resolve().parent\nfor name,item in "+repr(sources)+".items():\n raw=zlib.decompress(base64.b64decode(item['data']));assert hashlib.sha256(raw).hexdigest()==item['sha256']\n with (area/name).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
code+="""temp=Path(os.environ['SLURM_TMPDIR']);assert temp.is_absolute() and temp.is_dir() and temp.stat().st_uid==3164768
workspace=temp/('hhw-vtl-wrapper-tests-'+os.environ['SLURM_JOB_ID']);workspace.mkdir(mode=0o700,exist_ok=False)
env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','TZ':'UTC'}
for k in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(k,None)
xml=area/'wrapper45.xml'
subprocess.run([sys.executable,'-s','-m','pytest',str(area/'test_run_vtl_track_schema_audit_v2_20261021.py'),'-q','-p','no:cacheprovider','--basetemp',str(workspace/'fixtures'),'--junitxml',str(xml)],env=env,timeout=300,check=True)
cases=list(ET.fromstring(xml.read_bytes()).iter('testcase'));rows=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
"""
code+="assert len(cases)==45 and canonical_hash(rows)=="+repr(rostersha)+" and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases)\nhelper=area/'run-vtl-track-schema-audit-v2-20261021.py'\nsys.argv=[str(helper)]+"+repr(args)+"\nrunpy.run_path(str(helper),run_name='__main__')\n"
ast.parse(code);helper=Path('work/run-vtl-audit-stage-v2-20261021.py');helper.write_bytes(code.encode())
subprocess.run([sys.executable,'work/build-native-stage-20261021.py','--id','305','--helper',str(helper),'--area',area,'--framework',r+'/releases/20261021-6ed201653dfa-4f3903d7c26e','--cpus','1','--memory','16G','--hours','1'],check=True)
p=Path('work/hhw-recovery-stage-20261021-305.sh');text=runpy.run_path('work/patch-af-stage-expanded-20261021.py')['patch'](p.read_text())
needle='from horsehair.campaign import combined_admission\n';insert="import time\nrgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'\nassert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'\napi=runpy.run_path(str(rgp));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)\n"
assert text.count(needle)==1;text=text.replace(needle,insert+needle,1)
start=text.index('admission=combined_admission');end=text.index("print('RECOVERY_STAGE_SUBMITTED",start)
text=text[:start]+"with api['registration_lock'](area/'submission-registration-lock',time.time()+900,role='controller-start'):\n"+''.join('    '+s if s.strip() else s for s in text[start:end].splitlines(keepends=True))+text[end:]
needle="task['dependency_kind']=payload['dependency_kind']";assert text.count(needle)==1;text=text.replace(needle,needle+"\n    task['external_dependencies']=['6699739']\n    task['external_dependency_specs']=[{'job_id':'6699739','array':None,'purpose':'Positive exact-size census before read-only VTL structure audit; unchanged runtime freshness checks retained'}]",1)
ast.parse(text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]);p.write_bytes(text.encode());print('built305',rostersha)
