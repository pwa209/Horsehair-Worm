"""Freeze reviewed metadata-only suites and create two fresh native envelopes."""
from pathlib import Path
import ast
import base64
import hashlib
import json
import runpy
import subprocess
import sys
import xml.etree.ElementTree as ET
import zlib

ROOT = '/scratch/pwa209/horsehair-worm'
FRAMEWORK = ROOT + '/releases/20261021-6ed201653dfa-4f3903d7c26e'
WORK = Path('work')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def pinned(name, expected):
    raw = (WORK/name).read_bytes()
    assert sha(raw) == expected, name
    ast.parse(raw)
    return raw


def roster(path, count):
    raw = path.read_bytes()
    assert b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper()
    document = ET.fromstring(raw)
    cases = list(document.iter('testcase'))
    rows = sorted((c.get('classname', '').split('.')[-1], c.get('name', '')) for c in cases)
    assert len(rows) == len(set(rows)) == count
    assert all(not any(c.find(k) is not None for k in ('failure', 'error', 'skipped')) for c in cases)
    suites = list(document.iter('testsuite'))
    assert sum(int(s.get('tests', '0')) for s in suites) == count
    assert all(int(s.get(k, '0')) == 0 for s in suites for k in ('failures', 'errors', 'skipped'))
    return sha(json.dumps(rows, sort_keys=True, separators=(',', ':')).encode())


definitions = {
    'census': {
        'request': '324',
        'helper': 'project-terminal-reader-census-v1-20261021.py',
        'helper_sha256': 'f976b5da226573ed5d64aa428fcab2e1470de6c74e24a360c8eaf5305bc3478e',
        'tests': 'test_project_terminal_reader_census_v1_20261021.py',
        'tests_sha256': '1b5a6c2eb2447e4dc712c83953abe26114e11e5e8d2011e12a62fe754dd86993',
        'count': 38,
        'junit': 'projector-root-final-20261021.xml',
        'worker_seconds': 1800,
        'arguments': [
            '--budget', ROOT+'/maintenance/study-af-fresh-storage-census-v1-20261021/original-storage-budget.json',
            '--budget-sha256', '06fcae248ec48ad65a2db122919f1a29bd2aa82d11cd01927712e9aaa18b59f2',
            '--entries', ROOT+'/maintenance/study-af-fresh-storage-census-v1-20261021/original-storage-entry-sizes.json',
            '--entries-sha256', '95cc190cc2fcfc0c9cf3458ca0dd00d508c16cdd7c9f4a87a413d9a44b8fc6d3',
        ],
    },
    'stock': {
        'request': '325',
        'helper': 'audit-vtl-stock-workbook-schema-20261021.py',
        'helper_sha256': '83ac33c41fc42c35f78add06458ad99bcbd2402425b5dd49afd17d964fc2c8d7',
        'tests': 'test_audit_vtl_stock_workbook_schema_20261021.py',
        'tests_sha256': 'ec4e3791cc3da26219a882a0c93b3328b50356e4595e46c80647e98b96540021',
        'count': 47,
        'junit': 'stock-auditor47-root-final-20261021.xml',
        'worker_seconds': 900,
        'arguments': [],
    },
}

suites = {}
for mode, definition in definitions.items():
    for kind in ('helper', 'tests'):
        pinned(definition[kind], definition[kind+'_sha256'])
    suites[mode] = {k: v for k, v in definition.items() if k not in {'request', 'junit'}}
    suites[mode]['roster_sha256'] = roster(WORK/definition['junit'], definition['count'])
assert suites['stock']['roster_sha256'] == '794bcc7fda7eed3f0f701936e3c21f02235e0717ccd687913417531fc8638c7f'

template = pinned('run-native-metadata-gate-v1-20261021.py', '00f743f2943fa3d58b8b4dc8cd4c1f6b26f38375d3504977cea4da61455edd2d').decode()
assert template.count('FROZEN_SUITES = None') == 1
frozen = template.replace('FROZEN_SUITES = None', 'FROZEN_SUITES = '+repr(suites), 1).encode()
ast.parse(frozen)
parent_name = 'run-native-metadata-gate-frozen-v1-20261021.py'
with (WORK/parent_name).open('xb') as stream:
    stream.write(frozen)

for mode, definition in definitions.items():
    request = definition['request']
    area = ROOT+'/maintenance/study-af-'+mode+'-metadata-stage-v1-20261021'
    contents = {parent_name: frozen}
    for kind in ('helper', 'tests'):
        contents[definition[kind]] = pinned(definition[kind], definition[kind+'_sha256'])
    sources = {name: {'sha256': sha(raw), 'data': base64.b64encode(zlib.compress(raw)).decode()} for name, raw in contents.items()}
    args = ['--mode', mode, '--native-script', ROOT+'/plans/recovery-support-20261021-'+request+'/recovery-'+request+'.sbatch']
    code = ('from pathlib import Path\nimport ast,base64,zlib,hashlib,os,sys,runpy\n'
            'area=Path(__file__).resolve().parent\n'
            'for name,item in '+repr(sources)+'.items():\n'
            ' raw=zlib.decompress(base64.b64decode(item["data"]));assert hashlib.sha256(raw).hexdigest()==item["sha256"];ast.parse(raw)\n'
            ' with (area/name).open("xb") as f:f.write(raw);f.flush();os.fsync(f.fileno())\n'
            'helper=area/'+repr(parent_name)+'\nsys.argv=[str(helper)]+'+repr(args)+'\n'
            'runpy.run_path(str(helper),run_name="__main__")\n')
    ast.parse(code)
    helper = WORK/('run-native-'+mode+'-metadata-stage-v1-20261021.py')
    with helper.open('xb') as stream:
        stream.write(code.encode())
    subprocess.run([sys.executable, 'work/build-native-stage-20261021.py', '--id', request,
                    '--helper', str(helper), '--area', area, '--framework', FRAMEWORK,
                    '--cpus', '1', '--memory', '16G', '--hours', '1'], check=True)
    path = WORK/('hhw-recovery-stage-20261021-'+request+'.sh')
    text = runpy.run_path('work/patch-af-stage-expanded-20261021.py')['patch'](path.read_text())
    needle = 'from horsehair.campaign import combined_admission\n'
    inserted = ("import time\nrgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'\n"
                "assert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'\n"
                "api=runpy.run_path(str(rgp));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)\n")
    assert text.count(needle) == 1
    text = text.replace(needle, inserted+needle, 1)
    start = text.index('admission=combined_admission')
    end = text.index("print('RECOVERY_STAGE_SUBMITTED", start)
    text = text[:start]+"with api['registration_lock'](area/'submission-registration-lock',time.time()+900,role='controller-start'):\n"+''.join('    '+line if line.strip() else line for line in text[start:end].splitlines(keepends=True))+text[end:]
    needle = "task['dependency_kind']=payload['dependency_kind']"
    assert text.count(needle) == 1
    text = text.replace(needle, needle+"\n    task['external_dependencies']=['6699739']\n    task['external_dependency_specs']=[{'job_id':'6699739','array':None,'purpose':'Positive exact-size census before bounded read-only metadata audit; runtime six-hour freshness unchanged'}]", 1)
    ast.parse(text.split("python -u - <<'PY'\n", 1)[1].rsplit('\nPY\n', 1)[0])
    path.write_bytes(text.encode())
    print(json.dumps({'mode': mode, 'request': request, 'stage_sha256': sha(text.encode()),
                      'wrapper_sha256': sha(frozen), 'suite': suites[mode]}, sort_keys=True))
