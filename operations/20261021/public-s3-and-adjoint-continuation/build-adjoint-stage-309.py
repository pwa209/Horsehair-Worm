from pathlib import Path
import ast,base64,zlib,hashlib,subprocess,sys,runpy
r='/scratch/pwa209/horsehair-worm';area=r+'/maintenance/study-af-whole-record-adjoint-stage-v3-20261021'
pins={'run-full-record-adjoint-v3-20261021.py':'120d3ec88267748ad0f36d03527702f7b5f163ce2f4c5b0aa0e455e6a1b3abca','test_full_record_adjoint_v3_20261021.py':'fce4ba75f80dc25d52a301719531eb742080f3f38df632cd654c38c486d8fdd5','run-full-cohort-prefix-v3-20261021.py':'b131d6317858eae3c43c2ae7337f46e3173ab64355693980af2e7e489af46d3d'}
sources={}
for name,sha in pins.items():
 raw=(Path('work')/name).read_bytes();assert hashlib.sha256(raw).hexdigest()==sha;ast.parse(raw)
 sources[name]={'data':base64.b64encode(zlib.compress(raw)).decode(),'sha256':sha}
args=['--qualification',r+'/maintenance/study-af-execution-support-v2-20261021/qualification.json','--qualification-sha256','f4f7684e3b04fde1a61c2ce20baec430a1dfe56b9e055b75cfaabc78cbdc7c13','--qualification-job-id','6689965','--resource-policy',r+'/maintenance/execution-resource-policies/af-expanded-20261021-v2/policy.json','--resource-policy-sha256','5dac85400e709525458cc34a2edc2bb40b029eb743078672cd721ab444c17015','--native-script',r+'/plans/recovery-support-20261021-309/recovery-309.sbatch','--context-helper',r+'/maintenance/study-af-cohort-prefix-stage-v3-20261021/run-full-cohort-profile-v2-20261021.py','--context-helper-sha256','396e0ec6f9cf5936884fdd08d23e5e3d6e92f9bb06c654d747d04b2b27338787','--tests',area+'/test_full_record_adjoint_v3_20261021.py','--tests-sha256',pins['test_full_record_adjoint_v3_20261021.py'],'--prefix-receipt',r+'/maintenance/behaviour-full-cohort-prefix-v3-support-20261021/receipt.json','--prefix-job-id','6699432']
code="from pathlib import Path\nimport ast,base64,zlib,hashlib,os,sys,runpy,json,stat\narea=Path(__file__).resolve().parent\nfor name,item in "+repr(sources)+".items():\n raw=zlib.decompress(base64.b64decode(item['data']));assert hashlib.sha256(raw).hexdigest()==item['sha256'];ast.parse(raw)\n with (area/name).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
code+="prefix=Path("+repr(r+'/maintenance/behaviour-full-cohort-prefix-v3-support-20261021/receipt.json')+")\n"
code+="""assert prefix.is_absolute() and prefix==prefix.resolve() and not prefix.is_symlink()
status=prefix.stat();assert stat.S_ISREG(status.st_mode) and status.st_uid==3164768 and 0<status.st_size<=20_000_000
raw=prefix.read_bytes();assert len(raw)==status.st_size
prefix_sha=hashlib.sha256(raw).hexdigest()
locator={'prefix_receipt':str(prefix),'prefix_receipt_sha256':prefix_sha,'prefix_job_id':'6699432','digest_observed_only_at_native_runtime':True,'requires_helper_positive_native_producer_and_full32_tests_and_stage1_proof':True,'scientific_result':False,'scientific_fit':False,'admission_authority':False}
with (area/'prefix-receipt-ingress.json').open('xb') as f:f.write(json.dumps(locator,sort_keys=True,indent=2,allow_nan=False).encode()+bytes([10]));f.flush();os.fsync(f.fileno())
helper=area/'run-full-record-adjoint-v3-20261021.py'
"""
code+="sys.argv=[str(helper)]+"+repr(args)+"+['--prefix-receipt-sha256',prefix_sha]\nrunpy.run_path(str(helper),run_name='__main__')\n"
ast.parse(code);helper=Path('work/run-adjoint-stage-v3-20261021.py');helper.write_bytes(code.encode())
subprocess.run([sys.executable,'work/build-native-stage-20261021.py','--id','309','--helper',str(helper),'--area',area,'--framework',r+'/releases/20261021-caebc9c1dc77-8495a3d98551','--cpus','1','--memory','64G','--hours','2'],check=True)
p=Path('work/hhw-recovery-stage-20261021-309.sh');text=runpy.run_path('work/patch-af-stage-expanded-20261021.py')['patch'](p.read_text())
needle='from horsehair.campaign import combined_admission\n'
insert="import time\nrgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'\nassert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'\napi=runpy.run_path(str(rgp));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)\n"
assert text.count(needle)==1;text=text.replace(needle,insert+needle,1)
start=text.index('admission=combined_admission');end=text.index("print('RECOVERY_STAGE_SUBMITTED",start)
text=text[:start]+"with api['registration_lock'](area/'submission-registration-lock',time.time()+900,role='controller-start'):\n"+''.join('    '+s if s.strip() else s for s in text[start:end].splitlines(keepends=True))+text[end:]
needle="task['dependency_kind']=payload['dependency_kind']";assert text.count(needle)==1;text=text.replace(needle,needle+"\n    task['external_dependencies']=['6699432']\n    task['external_dependency_specs']=[{'job_id':'6699432','array':None,'purpose':'Positive completed full-cohort preprocessing prefix with source-bound32 native tests and exact committed stage1 evidence before first-order operator comparison'}]",1)
ast.parse(text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]);p.write_bytes(text.encode());print('built309')
