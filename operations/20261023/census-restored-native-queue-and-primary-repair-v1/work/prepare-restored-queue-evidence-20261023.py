"""Copy observed bytes into a new, not-yet-sealed operational evidence capsule."""
from pathlib import Path
import base64
import hashlib
import json

DEST = Path('operations/20261023/census-restored-native-queue-and-primary-repair-v1')
NAMES = [
    'work/hhw-census-restoration-live-20261023-471.sh',
    'work/hhw-restored-native-proof-20261023-472.sh',
    'work/narval-deployment/hhw-census-restoration-live-20261023-471.txt',
    'work/narval-deployment/hhw-census-restoration-live-20261023-471.json',
    'work/narval-deployment/hhw-restored-native-proof-20261023-472.txt',
    'work/narval-deployment/hhw-restored-native-proof-20261023-472.json',
    'work/hhw-census-and-budget-live-20261023-473.sh',
    'work/narval-deployment/hhw-census-and-budget-live-20261023-473.txt',
    'work/narval-deployment/hhw-census-and-budget-live-20261023-473.json',
    'work/hhw-census-and-budget-live-20261023-474.sh',
    'work/narval-deployment/hhw-census-and-budget-live-20261023-474.txt',
    'work/narval-deployment/hhw-census-and-budget-live-20261023-474.json',
    'work/hhw-primary-qualification-live-20261023-475.sh',
    'work/narval-deployment/hhw-primary-qualification-live-20261023-475.txt',
    'work/narval-deployment/hhw-primary-qualification-live-20261023-475.json',
    'work/hhw-primary-qualification-live-20261023-476.sh',
    'work/narval-deployment/hhw-primary-qualification-live-20261023-476.txt',
    'work/narval-deployment/hhw-primary-qualification-live-20261023-476.json',
    'work/hhw-primary-qualification-live-20261023-477.sh',
    'work/narval-deployment/hhw-primary-qualification-live-20261023-477.txt',
    'work/narval-deployment/hhw-primary-qualification-live-20261023-477.json',
    'work/narval-deployment/hhw-recovery-stage-20261021-470-request-transport.json',
    'work/narval-deployment/hhw-recovery-stage-20261021-470-bridge-completion.json',
    'work/narval-deployment/hhw-recovery-stage-20261021-470-bridge-output.txt',
    'work/hhw-primary-handoff-live-20261023-478.sh',
    'work/narval-deployment/hhw-primary-handoff-live-20261023-478.json',
    'work/narval-deployment/hhw-primary-handoff-live-20261023-478.txt',
    'work/hhw-primary-handoff-live-20261023-479.sh',
    'work/narval-deployment/hhw-primary-handoff-live-20261023-479.json',
    'work/narval-deployment/hhw-primary-handoff-live-20261023-479.txt',
    'work/hhw-primary-handoff-live-20261023-480.sh',
    'work/narval-deployment/hhw-primary-handoff-live-20261023-480.json',
    'work/narval-deployment/hhw-primary-handoff-live-20261023-480.txt',
    'work/hhw-primary-handoff-live-20261023-481.sh',
    'work/narval-deployment/hhw-primary-handoff-live-20261023-481.json',
    'work/narval-deployment/hhw-primary-handoff-live-20261023-481.txt',
    'work/narval-deployment/hhw-recovery-stage-20261021-483-request-transport.json',
    'work/narval-deployment/hhw-recovery-stage-20261021-483-bridge-completion.json',
    'work/narval-deployment/hhw-recovery-stage-20261021-483-bridge-output.txt',
    'work/hhw-controller-resumption-live-20261023-484.sh',
    'work/narval-deployment/hhw-controller-resumption-live-20261023-484.json',
    'work/narval-deployment/hhw-controller-resumption-live-20261023-484.txt',
    'work/hhw-controller-resumption-live-20261023-485.sh',
    'work/narval-deployment/hhw-controller-resumption-live-20261023-485.json',
    'work/narval-deployment/hhw-controller-resumption-live-20261023-485.txt',
    'work/narval-deployment/hhw-recovery-stage-20261021-469-request-transport.json',
    'work/narval-deployment/hhw-recovery-stage-20261021-469-bridge-completion.json',
    'work/narval-deployment/hhw-recovery-stage-20261021-469-bridge-output.txt',
    'work/narval-deployment/hhw-af-reviewed-source-mirror-20261023-463-bridge-completion.json',
    'work/narval-deployment/hhw-af-reviewed-source-mirror-20261023-463-bridge-output.txt',
]

def put(relative, raw):
    target = DEST/relative
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        assert target.read_bytes() == raw, 'Preserve differing evidence: '+str(target)
    else:
        with target.open('xb') as stream:
            stream.write(raw)

assert not (DEST/'source-manifest.json').exists(), 'A sealed evidence capsule cannot change'
for name in NAMES:
    source = Path(name)
    assert source.is_file() and not source.is_symlink(), str(source)
    put(name, source.read_bytes())
put('.gitattributes', b'* -text\n*.xml -whitespace\n')
source = Path('work/narval-deployment/hhw-restored-native-proof-20261023-472.txt')
proof = json.loads(source.read_text().split('RESTORED_NATIVE_PROOF ', 1)[1])
primary = json.loads(Path('work/narval-deployment/hhw-primary-qualification-live-20261023-475.txt').read_text().split('PRIMARY_QUALIFICATION_LIVE ', 1)[1])
primary_final = json.loads(Path('work/narval-deployment/hhw-primary-qualification-live-20261023-477.txt').read_text().split('PRIMARY_QUALIFICATION_LIVE ', 1)[1])
handoff = json.loads(Path('work/narval-deployment/hhw-primary-handoff-live-20261023-479.txt').read_text().split('PRIMARY_HANDOFF_LIVE ', 1)[1])
for name, packet in {**proof['files'], **primary['files'], **primary_final['files'], **handoff['files']}.items():
    if packet.get('missing'):
        continue
    raw = base64.b64decode(packet['base64'], validate=True)
    assert hashlib.sha256(raw).hexdigest() == packet['sha256']
    prefix = '/scratch/pwa209/horsehair-worm/'
    assert name.startswith(prefix)
    put('narval/'+name[len(prefix):], raw)
for name, packet in primary_final['suites'].items():
    raw=base64.b64decode(packet['base64'],validate=True)
    assert hashlib.sha256(raw).hexdigest()==packet['sha256'] and not packet['failed_cases']
    put('narval/maintenance/study-primary-external-census-policy-v1-20261023/native-6866457/'+name,raw)
for number,stem in [('480','hhw-primary-handoff-live-20261023-480'),('481','hhw-primary-handoff-live-20261023-481'),('484','hhw-controller-resumption-live-20261023-484'),('485','hhw-controller-resumption-live-20261023-485')]:
    latest = json.loads((Path('work/narval-deployment')/(stem+'.txt')).read_text().split('PRIMARY_HANDOFF_LIVE ', 1)[1])
    for name, packet in {**latest['files'], **latest['suites']}.items():
        if packet.get('missing'):
            continue
        raw=base64.b64decode(packet['base64'],validate=True)
        assert hashlib.sha256(raw).hexdigest()==packet['sha256'] and name.startswith('/scratch/pwa209/horsehair-worm/')
        put('narval-observations/'+number+'/'+name[len('/scratch/pwa209/horsehair-worm/'):],raw)
live = json.loads(Path(NAMES[2]).read_text().split('RESTORATION_LIVE ', 1)[1])
summary = {'kind': 'restored-native-queue-observation-v1', 'checked_utc': live['checked_utc'],
    'census_native_job_id': '6864491', 'coordinator_native_job_id': '6864084',
    'coordinator_completed': True, 'native_census_status': 'RUNNING',
    'partial_progress_is_admission_authority': False,
    'cancelled_replacements': {old: live['registrations'][new]['tasks']['recovery-'+new]['job_id']
        for old, new in [('6819399','430'),('6822432','431'),('6822568','432'),('6822730','433'),('6824584','434')]},
    'external_census_controller_handoff_job_id': '6865706',
    'campaign': live['campaign'], 'scientific_result': False,
    'replacement_controllers_healthy_claimed': False, 'full_study_complete': False}
assert '6864491|hhw-rolling-storage-census-01|RUNNING|' in live['queue']['stdout']
assert '6864084|6864084|COMPLETED|0:0|' in live['native']['stdout']
assert live['files']['/scratch/pwa209/horsehair-worm/maintenance/study-af-reviewed-registration-coordinator-v4-20261023/coordinator-execution/receipt.json']['value']['status'] == 'complete_seven_reviewed_registrations'
put('restored-queue-observation.json', (json.dumps(summary, sort_keys=True, indent=2)+'\n').encode())
print(json.dumps({'capsule': str(DEST), 'sealed': False, 'observed': summary}, sort_keys=True))
