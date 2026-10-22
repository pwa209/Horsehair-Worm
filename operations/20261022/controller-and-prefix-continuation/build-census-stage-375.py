"""Build one fresh, finite census envelope; local source preparation only."""
from pathlib import Path
import ast
import base64
import hashlib
import json
import re
import runpy
import subprocess
import sys
import xml.etree.ElementTree as ET
import zlib

ROOT = '/scratch/pwa209/horsehair-worm'
AREA = ROOT + '/maintenance/study-af-fresh-storage-census-stage-v4-20261022'
PINS = {'run-fresh-storage-census-v4-20261022.py': '05627118529a153e38eb45f2cf778fb4fbc09289bb890cb7962cdb8c2b137c18', 'test_run_fresh_storage_census_v4_20261022.py': 'f28bc638824e5f5dc5d56e923c184a8451f667412c1c545b10d9b30c9b31da4e', 'run-vtl-track-schema-audit-v2-20261021.py': '284b082a04eccebe707378a6e8f26836a4467846e3b213c3e9ff9b4325a86ff1', 'registered-snapshot-read-guard-v4-20261022.py': '5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a', 'test_registered_snapshot_read_guard_v4_20261022.py': '0e4f029814f1aca969fe282dac99a61373128a6dbbe4a0672ba5706a736545b9'}
BUILD = Path('work/build-native-stage-20261021.py')
PATCH = Path('work/patch-af-stage-expanded-20261021.py')
assert hashlib.sha256(BUILD.read_bytes()).hexdigest() == 'bfc456e17a0e1a2f1c1d4639d2e05b478b988be7e4010cc56265504082644d8b'
assert hashlib.sha256(PATCH.read_bytes()).hexdigest() == 'c12faedc1ae6dbdba1220d8721a900b34bf06995d731169d37740fbaed5b5980'
sources = {}
for name, sha in PINS.items():
    raw = (Path('work') / name).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == sha
    ast.parse(raw)
    sources[name] = {'data': base64.b64encode(zlib.compress(raw)).decode(), 'sha256': sha}
xml = ET.fromstring(Path('work/census-v4-root-final-20261022.xml').read_bytes())
cases = list(xml.iter('testcase'))
rows = sorted((case.get('classname', '').split('.')[-1], case.get('name', '')) for case in cases)
assert len(cases) == len(set(rows)) == 44
assert all(not any(case.find(k) is not None for k in ('failure', 'error', 'skipped')) for case in cases)
roster = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
args = ['--native-script', ROOT + '/plans/recovery-support-20261021-375/recovery-375.sbatch',
    '--registry-qualification', ROOT + '/maintenance/study-af-registry-repair-support-v4-20261021/private-qualification.json',
    '--registry-qualification-sha256', '1f391caccd4629eb47150862f7bc2575e991e4ce5cb5aae99e955cdeae4510ca',
    '--registry-qualification-job-id', '6699561']
code = '''from pathlib import Path
import ast,base64,zlib,hashlib,os,sys,runpy,subprocess,xml.etree.ElementTree as ET,json
from horsehair.core import canonical_hash
from horsehair.cli import _qualification_junit
area=Path(__file__).resolve().parent
'''
code += 'sources=' + repr(sources) + '\n'
code += '''for name,item in sources.items():
 raw=zlib.decompress(base64.b64decode(item['data']));assert hashlib.sha256(raw).hexdigest()==item['sha256'];ast.parse(raw)
 with (area/name).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
temporary=Path(os.environ['SLURM_TMPDIR'])
assert temporary.is_absolute() and temporary==temporary.resolve() and temporary.is_dir() and temporary.stat().st_uid==3164768
fixtures=temporary/('hhw-fresh-census-tests-'+os.environ['SLURM_JOB_ID']);fixtures.mkdir(mode=0o700,exist_ok=False)
environment={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','PYTHONNOUSERSITE':'1','TZ':'UTC'}
for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):environment.pop(key,None)
xml=area/'census44.xml'
subprocess.run([sys.executable,'-s','-m','pytest',str(area/'test_run_fresh_storage_census_v4_20261022.py'),'-q','-p','no:cacheprovider','--rootdir',str(area),'--basetemp',str(fixtures/'tests'),'--junitxml',str(xml)],env=environment,check=True,timeout=300)
cases=list(ET.fromstring(xml.read_bytes()).iter('testcase'))
rows=sorted((case.get('classname','').split('.')[-1],case.get('name','')) for case in cases)
'''
code += 'assert len(cases)==len(set(rows))==44 and canonical_hash(rows)==' + repr(roster) + " and all(not any(case.find(k) is not None for k in ('failure','error','skipped')) for case in cases)\n"
code += "assert _qualification_junit(xml)=={'tests':44,'failures':0,'errors':0,'skipped':0}\n"
code += '''for name,item in sources.items():assert hashlib.sha256((area/name).read_bytes()).hexdigest()==item['sha256']
helper=area/'run-fresh-storage-census-v4-20261022.py'
'''
code += 'sys.argv=[str(helper)]+' + repr(args) + '\n'
code += "runpy.run_path(str(helper),run_name='__main__')\n"
code=code.replace("xml=area/'census44.xml'\n", "driver=Path('/scratch/pwa209/horsehair-worm/maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py')\nassert hashlib.sha256(driver.read_bytes()).hexdigest()=='49815eeb509909d5eac2ebde2e4f4a3918cf40cefdc5f643f2bcaffead629eb1'\nregistry_xml=area/'registry30.xml'\nsubprocess.run([sys.executable,'-s','-m','pytest',str(area/'test_registered_snapshot_read_guard_v4_20261022.py'),'-q','--noconftest','-p','no:cacheprovider','--rootdir',str(area),'--basetemp',str(fixtures/'registry-tests'),'--junitxml',str(registry_xml)],env=environment,check=True,timeout=300)\nregistry_cases=list(ET.fromstring(registry_xml.read_bytes()).iter('testcase'))\nregistry_rows=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in registry_cases)\nassert len(registry_cases)==len(set(registry_rows))==30 and canonical_hash(registry_rows)=='a0a7edd720fd68fb30a511f10d935a042481a168b173c24ebe190b185ee2d0d5' and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in registry_cases)\nassert _qualification_junit(registry_xml)=={'tests':30,'failures':0,'errors':0,'skipped':0}\nregistry_proof={'source_sha256':sources['registered-snapshot-read-guard-v4-20261022.py']['sha256'],'test_source_sha256':sources['test_registered_snapshot_read_guard_v4_20261022.py']['sha256'],'test_counts':{'tests':30,'failures':0,'errors':0,'skipped':0},'test_roster_sha256':'a0a7edd720fd68fb30a511f10d935a042481a168b173c24ebe190b185ee2d0d5','junit_sha256':hashlib.sha256(registry_xml.read_bytes()).hexdigest(),'native_job_id':os.environ['SLURM_JOB_ID'],'scientific_controller_qualification_authorized':False}\nwith (area/'registry30-proof.json').open('x') as stream:json.dump(registry_proof,stream,sort_keys=True)\n"+"xml=area/'census44.xml'\n",1)
ast.parse(code)
helper = Path('work/run-fresh-census-stage-v4-20261022.py')
with helper.open('x', encoding='utf-8', newline='\n') as stream:
    stream.write(code)
subprocess.run([sys.executable, str(BUILD), '--id', '375', '--helper', str(helper), '--area', AREA,
    '--framework', ROOT + '/releases/20261021-6ed201653dfa-4f3903d7c26e',
    '--cpus', '4', '--memory', '16G', '--hours', '2'], check=True)
stage = Path('work/hhw-recovery-stage-20261021-375.sh')
text = runpy.run_path(str(PATCH))['patch'](stage.read_text())
pattern = r"payload=json.loads\(zlib.decompress\(base64.b64decode\('([^']+)'\)\)\)"
match = re.search(pattern, text)
assert match is not None
payload = json.loads(zlib.decompress(base64.b64decode(match.group(1))))
assert payload['hours'] == 2 and payload['cpus'] == 4 and payload['memory'] == '16G'
# Explicitly preserve the four-hour native census allocation. The general
# historical builder permits only two hours; no numerical job is altered.
payload['hours'] = 4
encoded = base64.b64encode(zlib.compress(json.dumps(payload).encode())).decode()
text = text[:match.start()] + 'payload=json.loads(zlib.decompress(base64.b64decode(' + repr(encoded) + ')))' + text[match.end():]
needle = 'from horsehair.campaign import combined_admission\n'
replacement = """import time
rgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'
assert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'
api=runpy.run_path(str(rgp));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+1500)
"""
assert text.count(needle) == 1
text = text.replace(needle, replacement + needle, 1)
start = text.index('admission=combined_admission')
end = text.index("print('RECOVERY_STAGE_SUBMITTED", start)
text = text[:start] + "with api['registration_lock'](area/'submission-registration-lock',time.time()+1500,role='controller-start'):\n" + ''.join('    ' + line if line.strip() else line for line in text[start:end].splitlines(keepends=True)) + text[end:]
ast.parse(text.split("python -u - <<'PY'\n", 1)[1].rsplit('\nPY\n', 1)[0])
stage.write_bytes(text.encode())
print(json.dumps({'logical_id': '375', 'native_tests': 44, 'roster_sha256': roster,
    'sources': PINS, 'parent_sha256': hashlib.sha256(helper.read_bytes()).hexdigest(),
    'stage_sha256': hashlib.sha256(stage.read_bytes()).hexdigest(),
    'native_resources': {'cpus': 4, 'memory': '16G', 'hours': 4, 'gpus': 0},
    'fresh_census_prerequisite': False, 'scheduler_submitted': False}, sort_keys=True))
