"""Fresh registration IDs following verified pre-publication filesystem failures."""
from pathlib import Path
import ast,base64,zlib,json,hashlib

for old,new in [('384','394'),('385','395')]:
 source=Path('work/hhw-recovery-stage-20261021-'+old+'.sh').read_text(encoding='utf8')
 line=next(v for v in source.splitlines() if v.startswith('payload='))
 blob=next(n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100)
 payload=json.loads(zlib.decompress(base64.b64decode(blob)));assert payload['id']==old
 payload['id']=new
 payload['args']=[x.replace('recovery-support-20261021-'+old,'recovery-support-20261021-'+new).replace('recovery-'+old,'recovery-'+new) for x in payload['args']]
 if old=='385':payload['area']='/scratch/pwa209/horsehair-worm/maintenance/study-af-prestart-three-activation-stage-v2-20261023'
 encoded=base64.b64encode(zlib.compress(json.dumps(payload).encode(),9)).decode()
 source=source.replace(line,"payload=json.loads(zlib.decompress(base64.b64decode('"+encoded+"')))",1)
 if old=='385':
  source=source.replace('recovery-support-20261021-384','recovery-support-20261021-394').replace("'recovery-384'","'recovery-394'")
 needle="    admission=combined_admission(root,config,science['release_id'],area,'submit-expanded-combined-admission'"
 i=source.index(needle)
 extra="""    # Both previous registrations ended before plan/sbatch publication. Their
    # filesystem-error evidence remains untouched; fresh IDs never replay them.
    previous=[root/'plans/recovery-support-20261021-384',root/'state/recovery-support-20261021-384-submission.json',
        root/'plans/recovery-support-20261021-385',root/'state/recovery-support-20261021-385-submission.json']
    assert all(not p.exists() and not p.is_symlink() for p in previous),'Prior registration published; explicit reconciliation required'
    atomic_json(area/'failed-384-385-publication-absence.json',{'entries':[{'path':str(p),'exists':False,'is_symlink':False} for p in previous],
        'registration_mutex':str(root/'state/af-campaign-registration.mutex'),'old_requests_replayed':False,
        'native_work_not_started':True,'scientific_result':False,'admission_authority':False})
"""
 source=source[:i]+extra+source[i:]
 body=source.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
 try:ast.parse(body)
 except SyntaxError as error:raise AssertionError('Invalid registration body: '+str(error.msg)) from None
 target=Path('work/hhw-recovery-stage-20261021-'+new+'.sh');assert not target.exists();raw=source.encode();assert b'\r' not in raw;target.write_bytes(raw)
 print(json.dumps({'request':str(target),'sha256':hashlib.sha256(raw).hexdigest(),'unchanged_helper_sha256':payload['helper_sha256']}))
