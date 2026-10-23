"""Freeze the independently audited native Python AST portability qualification registration."""
from pathlib import Path
import ast,base64,hashlib,json,zlib
W=Path('work/rolling-native-v5');R='/scratch/pwa209/horsehair-worm'
AREA=R+'/maintenance/study-af-rolling-census-support-v4-20261023'
ID='456';HELPER='qualify-streaming-census-v2-20261023.py'
TEMPLATE=Path('work/hhw-recovery-stage-20261021-394.sh')
assert hashlib.sha256(TEMPLATE.read_bytes()).hexdigest()=='8254e96c402b4d8aa43c98d43880a607f2d222408d0f1eb2c351a60277f29e9e'
raw=(W/'source-manifest.json').read_bytes();manifest=json.loads(raw);mh=hashlib.sha256(raw).hexdigest()
assert manifest['kind']=='streaming-census-operational-source-manifest-v2' and len(manifest['files'])==14 and len(manifest['suites'])==13
files=manifest['files'];suites=manifest['suites'];private=list(files)
assert suites['baseline_budget']['tests']==46 and suites['baseline_budget']['roster_sha256']=='a29d0c2854dafc41da5b6bcb7e31a03545bcad6134612b52a5f64c166bb52e7e'
for name,sha in files.items():
 body=(W/name).read_bytes();assert hashlib.sha256(body).hexdigest()==sha,name;ast.parse(body,feature_version=(3,11))
text=TEMPLATE.read_text(encoding='utf8')
line=next(l for l in text.splitlines() if l.startswith('payload='))
blob=next(n.value for n in ast.walk(ast.parse(line)) if isinstance(n,ast.Constant) and isinstance(n.value,str) and len(n.value)>100)
p=json.loads(zlib.decompress(base64.b64decode(blob)))
helper=(W/HELPER).read_bytes();plan=R+'/plans/recovery-support-20261021-'+ID;ledger=R+'/state/recovery-support-20261021-'+ID+'-submission.json'
p.update(id=ID,area=AREA,helper=HELPER,helper_sha256=files[HELPER],helper_body=base64.b64encode(zlib.compress(helper,9)).decode(),cpus=8,memory='64G',hours=1)
p['args']=['--native-plan',plan+'/plan.json','--native-ledger',ledger,'--native-task','recovery-'+ID,
 '--native-script',plan+'/recovery-'+ID+'.sbatch','--source-manifest',AREA+'/source-manifest.json','--source-manifest-sha256',mh]
assert not p['after'] and not p['after_specs']
encoded=base64.b64encode(zlib.compress(json.dumps(p).encode(),9)).decode()
text=text.replace(line,"payload=json.loads(zlib.decompress(base64.b64decode('"+encoded+"')))",1)
sources={}
# Additional exact sibling fixture required by the external policy tests.
fixture_name='run-af-agent-controller-registry-v6-20261022.py';fixture_sha='e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
assert fixture_name not in files
fixture_body=(W/fixture_name).read_bytes();assert hashlib.sha256(fixture_body).hexdigest()==fixture_sha
ast.parse(fixture_body,feature_version=(3,11))
sources[fixture_name]={'data':base64.b64encode(zlib.compress(fixture_body,9)).decode(),'sha256':fixture_sha}
for name in private:
 if name==HELPER:continue
 body=(W/name).read_bytes();ast.parse(body)
 sources[name]={'data':base64.b64encode(zlib.compress(body,9)).decode(),'sha256':files[name]}
old=next(l for l in text.splitlines() if l.startswith('for name,item in {'))
text=text.replace(old,'for name,item in '+repr(sources)+'.items():',1)
needle=" with (area/name).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())\n"
assert text.count(needle)==1
text=text.replace(needle,needle+"manifest_raw=base64.b64decode("+repr(base64.b64encode(raw).decode())+");assert hashlib.sha256(manifest_raw).hexdigest()=="+repr(mh)+"\nwith (area/'source-manifest.json').open('xb') as f:f.write(manifest_raw);f.flush();os.fsync(f.fileno())\n",1)
text=text.replace("area/'administration-registry-read-guard-v4-20261022.py'","area/'registered-snapshot-read-guard-v4-20261022.py'")
begin=text.index('    # Both previous registrations ended before plan/sbatch publication.')
end=text.index('    admission=combined_admission',begin)
text=text[:begin]+"    assert not (root/'plans/recovery-support-20261021-456').exists() and not (root/'state/recovery-support-20261021-456-submission.json').exists(),'Fresh registration required'\n"+text[end:]
begin=text.index("    census_directory=root/'plans/recovery-support-20261021-375'")
end=text.index("    if payload['after_specs']",begin)
text=text[:begin]+"    task['resources']['array']=None\n    task['rolling_census_qualification']={'helper':{'path':str(helper),'sha256':payload['helper_sha256']},'source_manifest':{'path':str(area/'source-manifest.json'),'sha256':"+repr(mh)+"}}\n"+text[end:]
body=text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];ast.parse(body)
target=W/'hhw-recovery-stage-20261021-456.sh';assert not target.exists();target.write_bytes(text.encode())
print(json.dumps({'request':str(target),'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'manifest_sha256':mh,'suite_tests':{k:v['tests'] for k,v in suites.items()},'scheduler_submitted':False}))
