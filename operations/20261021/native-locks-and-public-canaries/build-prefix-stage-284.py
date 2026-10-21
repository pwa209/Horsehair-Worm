from pathlib import Path
import ast,base64,zlib,hashlib,json,subprocess,sys,runpy
r='/scratch/pwa209/horsehair-worm';area=r+'/maintenance/study-af-cohort-prefix-stage-v3-20261021'
pins={'run-full-cohort-prefix-v3-20261021.py':'b131d6317858eae3c43c2ae7337f46e3173ab64355693980af2e7e489af46d3d','test_full_cohort_prefix_v3_20261021.py':'01b50175e001a44db682befdfd2728602d4ad467b5b8345092e5d3fa6d9ed80c','run-full-cohort-profile-v2-20261021.py':'396e0ec6f9cf5936884fdd08d23e5e3d6e92f9bb06c654d747d04b2b27338787'}
sources={}
for name,sha in pins.items():
 raw=(Path('work')/name).read_bytes();assert hashlib.sha256(raw).hexdigest()==sha;ast.parse(raw)
 sources[name]={'data':base64.b64encode(zlib.compress(raw)).decode(),'sha256':sha}
args=['--qualification',r+'/maintenance/study-af-execution-support-v2-20261021/qualification.json','--qualification-sha256','f4f7684e3b04fde1a61c2ce20baec430a1dfe56b9e055b75cfaabc78cbdc7c13','--qualification-job-id','6689965','--resource-policy',r+'/maintenance/execution-resource-policies/af-expanded-20261021-v2/policy.json','--resource-policy-sha256','5dac85400e709525458cc34a2edc2bb40b029eb743078672cd721ab444c17015','--native-script',r+'/plans/recovery-support-20261021-284/recovery-284.sbatch','--context-helper',area+'/run-full-cohort-profile-v2-20261021.py','--context-helper-sha256',pins['run-full-cohort-profile-v2-20261021.py'],'--tests',area+'/test_full_cohort_prefix_v3_20261021.py','--tests-sha256',pins['test_full_cohort_prefix_v3_20261021.py']]
code="from pathlib import Path\nimport ast,base64,zlib,hashlib,os,sys,runpy\narea=Path(__file__).resolve().parent\nfor name,item in "+repr(sources)+".items():\n raw=zlib.decompress(base64.b64decode(item['data']));assert hashlib.sha256(raw).hexdigest()==item['sha256'];ast.parse(raw)\n with (area/name).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\nhelper=area/'run-full-cohort-prefix-v3-20261021.py'\nsys.argv=[str(helper)]+"+repr(args)+"\nrunpy.run_path(str(helper),run_name='__main__')\n"
ast.parse(code);helper=Path('work/run-cohort-prefix-stage-v3-20261021.py');helper.write_bytes(code.encode())
subprocess.run([sys.executable,'work/build-native-stage-20261021.py','--id','284','--helper',str(helper),'--area',area,'--framework',r+'/releases/20261021-caebc9c1dc77-8495a3d98551','--cpus','1','--memory','64G','--hours','2'],check=True)
p=Path('work/hhw-recovery-stage-20261021-284.sh');text=runpy.run_path('work/patch-af-stage-expanded-20261021.py')['patch'](p.read_text())
needle='from horsehair.campaign import combined_admission\n'
insert="import time\nfrom horsehair.core import exclusive_file_lock\nrgp=root/'maintenance/study-af-registry-repair-support-v1-20261021/registered-snapshot-read-guard-20261021.py'\nassert digest(rgp)=='42fd488d27d1bf9aa75b46c113257525e296c526250e75ba12319d6139854db4'\nrunpy.run_path(str(rgp))['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)\n"
assert text.count(needle)==1;text=text.replace(needle,insert+needle,1)
start=text.index('admission=combined_admission');end=text.index("print('RECOVERY_STAGE_SUBMITTED",start)
text=text[:start]+"with exclusive_file_lock(root/'state/af-campaign-registration.mutex',timeout=60):\n"+''.join('    '+s if s.strip() else s for s in text[start:end].splitlines(keepends=True))+text[end:]
ast.parse(text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]);p.write_bytes(text.encode());print('built284')
