"""Freeze and register one native census qualification; never replay a request."""
from pathlib import Path
import ast,base64,hashlib,json,zlib,xml.etree.ElementTree as ET

W=Path('work');R='/scratch/pwa209/horsehair-worm'
AREA=R+'/maintenance/study-af-rolling-census-support-v1-20261023'
ID='428';HELPER='qualify-streaming-census-v2-20261023.py'
TEMPLATE=W/'hhw-recovery-stage-20261021-394.sh'
assert hashlib.sha256(TEMPLATE.read_bytes()).hexdigest()=='8254e96c402b4d8aa43c98d43880a607f2d222408d0f1eb2c351a60277f29e9e'
private=[HELPER,'test_qualify_streaming_census_v2_20261023.py',
 'qualify-streaming-census-v1-20261023.py','test_qualify_streaming_census_v1_20261023.py',
 'run-fresh-storage-census-v6-20261023.py','test_run_fresh_storage_census_v6_20261023.py',
 'run-af-agent-controller-census-policy-v1-20261023.py','test_af_agent_controller_census_policy_v1_20261023.py',
 'streaming-storage-census-v1-20261023.py','test_streaming_storage_census_v1_20261023.py',
 'run-fresh-storage-census-v5-20261023.py','test_run_fresh_storage_census_v5_20261023.py',
 'registered-snapshot-read-guard-v4-20261022.py','test_registered_snapshot_read_guard_v4_20261022.py']
files={name:hashlib.sha256((W/name).read_bytes()).hexdigest() for name in private}
def canonical(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def cases(xml,module):
 all_cases=list(ET.parse(W/xml).iter('testcase'))
 selected=[c for c in all_cases if c.get('classname','').split('.')[-1]==module]
 assert selected and all(not any(c.find(k) is not None for k in ('error','failure','skipped')) for c in selected)
 roster=sorted(module+'::'+c.get('name','') for c in selected)
 assert len(roster)==len(set(roster))
 return len(roster),canonical(roster)
suites={}
for name in ['budget','storage_artifacts','storage_artifact_cache','storage_boundary_cache','storage_snapshot_artifacts','storage_snapshot_boundary']:
 source='tests/test_'+name+'.py';n,h=cases('streaming-census-public-baseline-local-20261023.xml','test_'+name)
 suites['baseline_'+name]={'source':source,'source_sha256':hashlib.sha256(Path(source).read_bytes()).hexdigest(),'tests':n,'roster_sha256':h}
for scope,module,xml in [('kernel','test_streaming_storage_census_v1_20261023','streaming-census-v1-local-20261023.xml'),
 ('wrapper','test_run_fresh_storage_census_v5_20261023','census-v5-wrapper-final-20261023.xml'),
 ('registry','test_registered_snapshot_read_guard_v4_20261022','streaming-census-kernel-registry-qualifier-local-20261023.xml'),
 ('qualifier','test_qualify_streaming_census_v1_20261023','qualify-streaming-census-v1-local-20261023.xml')]:
 source=module+'.py';n,h=cases(xml,module)
 suites[scope]={'source':source,'source_sha256':files[source],'tests':n,'roster_sha256':h}
for scope,module,xml in [('repeatable_wrapper','test_run_fresh_storage_census_v6_20261023','rolling-census-complete-local-20261023.xml'),('rolling_qualifier','test_qualify_streaming_census_v2_20261023','rolling-census-complete-local-20261023.xml'),('external_census_policy','test_af_agent_controller_census_policy_v1_20261023','external-census-policy-final-20261023.xml')]:
 source=module+'.py';n,h=cases(xml,module);suites[scope]={'source':source,'source_sha256':files[source],'tests':n,'roster_sha256':h}
manifest={'kind':'streaming-census-operational-source-manifest-v2','files':files,'suites':suites}
raw=(json.dumps(manifest,indent=2,sort_keys=True)+'\n').encode();mh=hashlib.sha256(raw).hexdigest()
mp=W/'rolling-census-source-manifest-20261023.json'
if mp.exists():assert mp.read_bytes()==raw,'An existing freeze must match exactly'
else:mp.write_bytes(raw)
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
text=text[:begin]+"    assert not (root/'plans/recovery-support-20261021-428').exists() and not (root/'state/recovery-support-20261021-428-submission.json').exists(),'Fresh registration required'\n"+text[end:]
begin=text.index("    census_directory=root/'plans/recovery-support-20261021-375'")
end=text.index("    if payload['after_specs']",begin)
text=text[:begin]+"    task['resources']['array']=None\n    task['rolling_census_qualification']={'helper':{'path':str(helper),'sha256':payload['helper_sha256']},'source_manifest':{'path':str(area/'source-manifest.json'),'sha256':"+repr(mh)+"}}\n"+text[end:]
body=text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];ast.parse(body)
target=W/'hhw-recovery-stage-20261021-428.sh';assert not target.exists();target.write_bytes(text.encode())
print(json.dumps({'request':str(target),'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'manifest_sha256':mh,'suite_tests':{k:v['tests'] for k,v in suites.items()},'scheduler_submitted':False}))
