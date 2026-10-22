"""Fresh registration after proven pre-submission failure376; submits nothing."""
from pathlib import Path
import ast,base64,zlib,json,hashlib

source=Path('work/hhw-recovery-stage-20261021-376.sh').read_text(encoding='utf8')
line=next(v for v in source.splitlines() if v.startswith('payload='))
blob=next(n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100)
payload=json.loads(zlib.decompress(base64.b64decode(blob)))
assert payload['id']=='376' and payload['cpus']==1 and payload['memory']=='64G' and payload['hours']==8
original_helper_sha=payload['helper_sha256']
payload['id']='390';payload['area']='/scratch/pwa209/horsehair-worm/maintenance/study-af-cohort-prefix-stage-v5-r2-20261023'
payload['args']=[x.replace('recovery-support-20261021-376','recovery-support-20261021-390').replace('recovery-376','recovery-390') for x in payload['args']]
encoded=base64.b64encode(zlib.compress(json.dumps(payload).encode(),9)).decode()
source=source.replace(line,"payload=json.loads(zlib.decompress(base64.b64decode('"+encoded+"')))",1)
needle="    admission=combined_admission(root,config,science['release_id'],area,'submit-expanded-combined-admission'"
i=source.index(needle)
extra="""    # The old attempt failed during admission, before plan publication/sbatch.
    # Recheck its complete namespace and both shared numerical namespaces under
    # the permanent transaction lock. No old request, journal or file is replayed.
    absent=[root/'plans/recovery-support-20261021-376',root/'state/recovery-support-20261021-376-submission.json',
        root/'maintenance/study-af-cohort-prefix-stage-v5-r1-20261022/submission-intent.json',
        root/'maintenance/behaviour-full-cohort-prefix-v5-20261022',
        root/'maintenance/behaviour-full-cohort-prefix-v5-support-20261022']
    assert all(not p.exists() and not p.is_symlink() for p in absent),'Prior prefix publication/output exists; explicit reconciliation required'
    atomic_json(area/'predecessor-376-absence-under-lock.json',{'failed_before_plan_publication':True,
        'original_request_replayed':False,'registration_mutex':str(root/'state/af-campaign-registration.mutex'),
        'entries':[{'path':str(p),'exists':False,'is_symlink':False} for p in absent],
        'unchanged_parent_sha256':payload['helper_sha256'],'scientific_result':False,'admission_authority':False})
"""
source=source[:i]+extra+source[i:]
assert payload['helper_sha256']==original_helper_sha
body=source.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
try:ast.parse(body)
except SyntaxError as error:raise AssertionError('Invalid registration body: '+str(error.msg)) from None
target=Path('work/hhw-recovery-stage-20261021-390.sh');assert not target.exists();raw=source.encode();assert b'\r' not in raw;target.write_bytes(raw)
print(json.dumps({'request':str(target),'sha256':hashlib.sha256(raw).hexdigest(),'unchanged_helper_sha256':original_helper_sha}))
