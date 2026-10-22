"""Publish the reviewed private qualifier directly as its registered command."""
from pathlib import Path
import ast
import base64
import hashlib
import json
import runpy
import shlex
import subprocess
import sys
import xml.etree.ElementTree as ET
import zlib

ROOT = '/scratch/pwa209/horsehair-worm'
AREA = ROOT+'/maintenance/study-af-policy-pack-qualification-stage-v1-20261022'
RESULT = ROOT+'/maintenance/study-af-policy-pack-qualification-support-v1-20261022'
FRAMEWORK = ROOT+'/releases/20261021-6ed201653dfa-4f3903d7c26e'
QUALIFIER = 'qualify-f2-policy-pack-v1-20261022.py'
PINS = {
    'run-f2-policy-archive-pack-v1-20261021.py': '0490b796efcbd6b7063ef3fc1e454f8b38cd6692d139d06626829e0db04a018d',
    'test_f2_policy_archive_pack_v1_20261021.py': '25a4c507d4f2b23a70ef496519fcfafd643f986277d11224cb38413edbe93225',
    QUALIFIER: '63dbc330c9e8b21e643e1337ff44c07cb31ef05ca123fc07721eab1fd34989d9',
    'test_qualify_f2_policy_pack_v1_20261022.py': '05799d2828d8dcd571de8051adf9dc62471ae86826ddf73715873dc7b28d7649',
}
sources = {}
for name, expected in PINS.items():
    raw = (Path('work')/name).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == expected, name
    ast.parse(raw)
    if name != QUALIFIER:
        sources[name] = {'sha256': expected, 'data': base64.b64encode(zlib.compress(raw)).decode()}

cases = list(ET.fromstring(Path('work/pack-qualifier19-root-tests-20261022.xml').read_bytes()).iter('testcase'))
rows = sorted((c.get('classname', '').split('.')[-1], c.get('name', '')) for c in cases)
assert len(rows) == len(set(rows)) == 19
assert all(not any(c.find(k) is not None for k in ('failure', 'error', 'skipped')) for c in cases)
roster_sha = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

args = [
    '--pack-driver', AREA+'/run-f2-policy-archive-pack-v1-20261021.py',
    '--pack-driver-sha256', PINS['run-f2-policy-archive-pack-v1-20261021.py'],
    '--test-source', AREA+'/test_f2_policy_archive_pack_v1_20261021.py',
    '--qualification-case', RESULT,
    '--native-plan', ROOT+'/plans/recovery-support-20261021-336/plan.json',
    '--native-ledger', ROOT+'/state/recovery-support-20261021-336-submission.json',
    '--native-script', ROOT+'/plans/recovery-support-20261021-336/recovery-336.sbatch',
    '--native-task', 'recovery-336',
]
subprocess.run([sys.executable, 'work/build-native-stage-20261021.py', '--id', '336',
                '--helper', 'work/'+QUALIFIER, '--area', AREA, '--framework', FRAMEWORK,
                '--cpus', '4', '--memory', '16G', '--hours', '1', '--args-json', json.dumps(args)], check=True)
path = Path('work/hhw-recovery-stage-20261021-336.sh')
text = runpy.run_path('work/patch-af-stage-expanded-20261021.py')['patch'](path.read_text())

needle = "admitter=runpy.run_path"
assert needle not in text
needle = "from horsehair.campaign import combined_admission\n"
insert = (
    "import time\n"
    "for name,item in "+repr(sources)+".items():\n"
    " extra=zlib.decompress(base64.b64decode(item['data']));assert hashlib.sha256(extra).hexdigest()==item['sha256'];ast.parse(extra)\n"
    " with (area/name).open('xb') as stream:stream.write(extra);stream.flush();os.fsync(stream.fileno())\n"
    "rgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'\n"
    "assert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'\n"
    "api=runpy.run_path(str(rgp));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)\n"
)
assert text.count(needle) == 1
text = text.replace(needle, insert+needle, 1)

# A small native bootstrap exercises the qualifier's own source/provenance
# contracts. The registered task command remains the exact direct qualifier.
bootstrap = """from pathlib import Path
import hashlib,os,subprocess,sys,xml.etree.ElementTree as ET
from horsehair.core import canonical_hash
from horsehair.cli import _qualification_junit
area=Path(__AREA__)
pins=__PINS__
for name,sha in pins.items():
 p=area/name;assert p.is_file() and not p.is_symlink() and hashlib.sha256(p.read_bytes()).hexdigest()==sha
temporary=Path(os.environ['SLURM_TMPDIR']);assert temporary.is_absolute() and temporary==temporary.resolve() and temporary.is_dir() and temporary.stat().st_uid==3164768
fixtures=temporary/('hhw-policy-pack-qualifier-bootstrap-'+os.environ['SLURM_JOB_ID']);fixtures.mkdir(mode=0o700,exist_ok=False)
environment={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','PYTHONNOUSERSITE':'1','TZ':'UTC'}
for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):environment.pop(key,None)
xml=area/'qualifier19.xml'
subprocess.run([sys.executable,'-s','-m','pytest',str(area/'test_qualify_f2_policy_pack_v1_20261022.py'),'-q','-p','no:cacheprovider','--rootdir',str(area),'--basetemp',str(fixtures/'tests'),'--junitxml',str(xml)],env=environment,check=True,timeout=180)
cases=list(ET.fromstring(xml.read_bytes()).iter('testcase'));rows=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
assert len(cases)==len(set(rows))==19 and canonical_hash(rows)==__ROSTER__ and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases)
assert _qualification_junit(xml)=={'tests':19,'failures':0,'errors':0,'skipped':0}
for name,sha in pins.items():assert hashlib.sha256((area/name).read_bytes()).hexdigest()==sha
""".replace('__AREA__', repr(AREA)).replace('__PINS__', repr(PINS)).replace('__ROSTER__', repr(roster_sha))
ast.parse(bootstrap)
needle = "text=text.replace('set -euo pipefail'"
position = text.index(needle)
text = text[:position]+"bootstrap="+repr(bootstrap)+"\ntext=text.replace(command+'\\n',shlex.join(['python','-u','-c',bootstrap])+'\\n'+command+'\\n',1)\n"+text[position:]

start = text.index('admission=combined_admission')
end = text.index("print('RECOVERY_STAGE_SUBMITTED", start)
text = text[:start]+"with api['registration_lock'](area/'submission-registration-lock',time.time()+900,role='controller-start'):\n"+''.join('    '+line if line.strip() else line for line in text[start:end].splitlines(keepends=True))+text[end:]
needle = "task['dependency_kind']=payload['dependency_kind']"
assert text.count(needle) == 1
text = text.replace(needle, needle+"\n    task['external_dependencies']=['6699739']\n    task['external_dependency_specs']=[{'job_id':'6699739','array':None,'purpose':'Positive exact-size census before disposable private pack qualification; runtime source/freshness checks retained'}]", 1)
ast.parse(text.split("python -u - <<'PY'\n", 1)[1].rsplit('\nPY\n', 1)[0])
path.write_bytes(text.encode())
print(json.dumps({'request': '336', 'stage_sha256': hashlib.sha256(text.encode()).hexdigest(),
                  'qualifier_bootstrap_tests': 19, 'bootstrap_roster_sha256': roster_sha,
                  'native_target_tests': 103, 'source_stage': AREA, 'fresh_result_case': RESULT}, sort_keys=True))
