from pathlib import Path
import ast,base64,zlib,hashlib,json,subprocess,sys,runpy
qpath=Path('work/qualify-f2-registry-repair-v4-20261021.py')
q=runpy.run_path(str(qpath),run_name='root_frozen_q_pins_only')
assert hashlib.sha256(qpath.read_bytes()).hexdigest()=='7fd2348c3370811cb5ca2933357a007447b18c2ea57be1cef502fdd035fdda51'
r='/scratch/pwa209/horsehair-worm';area=r+'/maintenance/study-af-registry-repair-support-v4-20261021'
args=['--native-plan',r+'/plans/recovery-support-20261021-290/plan.json','--native-ledger',r+'/state/recovery-support-20261021-290-submission.json','--native-task','recovery-290','--native-script',r+'/plans/recovery-support-20261021-290/recovery-290.sbatch']
subprocess.run([sys.executable,'work/build-native-stage-20261021.py','--id','290','--helper',str(qpath),'--area',area,'--framework',q['FRAMEWORK'].as_posix(),'--args-json',json.dumps(args)],check=True)
p=Path('work/hhw-recovery-stage-20261021-290.sh')
text=runpy.run_path('work/patch-af-stage-expanded-20261021.py')['patch'](p.read_text())
extra={}
for name,sha in q['PINS'].items():
 raw=(Path('work')/name).read_bytes();assert hashlib.sha256(raw).hexdigest()==sha;ast.parse(raw)
 extra[name]={'data':base64.b64encode(zlib.compress(raw)).decode(),'sha256':sha}
needle="assert not payload['after'] and not payload['after_specs']"
insert="for name,item in "+repr(extra)+".items():\n raw=zlib.decompress(base64.b64decode(item['data']));assert hashlib.sha256(raw).hexdigest()==item['sha256'];ast.parse(raw)\n with (area/name).open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())\n"
assert text.count(needle)==1;text=text.replace(needle,insert+needle,1)
needle='from horsehair.campaign import combined_admission\n'
insert="import time\napi=runpy.run_path(str(area/'registered-snapshot-read-guard-v3-20261021.py'))\nassert digest(area/'registered-snapshot-read-guard-v3-20261021.py')=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'\napi['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)\n"
assert text.count(needle)==1;text=text.replace(needle,insert+needle,1)
start=text.index('admission=combined_admission');end=text.index("print('RECOVERY_STAGE_SUBMITTED",start)
text=text[:start]+"with api['registration_lock'](area/'submission-registration-lock',time.time()+900,role='controller-start'):\n"+''.join('    '+s if s.strip() else s for s in text[start:end].splitlines(keepends=True))+text[end:]
assert text.count(",'array':None")==1;text=text.replace(",'array':None",'',1)
body=text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0];ast.parse(body)
p.write_bytes(text.encode());print('built290')
