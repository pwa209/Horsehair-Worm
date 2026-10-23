from pathlib import Path
import subprocess,sys,shutil,ast,json
name='hhw-af-reviewed-source-mirror-20261023-499'
subprocess.run([sys.executable,'scripts/build_release.py','--git',shutil.which('git'),'--request-id',name,'--output','work'],check=True)
p=Path('work')/(name+'.sh');text=p.read_text(encoding='utf-8')
needle="release=project/'releases'/record['release_id']";assert text.count(needle)==1
text=text.replace(needle,"release=root/'releases'/record['release_id']",1)
start=text.index("budget=root/'state/resource-budget.json'");end=text.index("deployment={'status':'deployed'",start)
text=text[:start]+"# Immutable source-only scratch placement; full qualification is scheduled separately.\n"+text[end:]
text=text.replace("deployment={'status':'deployed'","deployment={'source_only':True,'placement_reason':'Personal scratch immutable execution source','status':'deployed'",1)
needle="(project/('deployment-'+record['release_id']+'.json')).write_text(json.dumps(deployment,indent=2)+'\\n')"
assert text.count(needle)==1;text=text.replace(needle,'# No duplicate personal-project receipt; scratch receipt is authoritative.',1)
needle="(root/'state'/('deployment-'+record['release_id']+'.json')).write_text(json.dumps(deployment,indent=2)+'\\n')"
assert text.count(needle)==1;text=text.replace(needle,"with (root/'state'/('deployment-'+record['release_id']+'.json')).open('xb') as output:output.write(json.dumps(deployment,indent=2,sort_keys=True).encode()+bytes([10]));output.flush();os.fsync(output.fileno())",1)
ast.parse(text.split("python - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]);p.write_bytes(text.encode())
record=json.loads((Path('work')/(name+'.release.json')).read_bytes());print(json.dumps({'request':str(p),'release_id':record['release_id'],'source_sha256':record['source_identity']['sha256']}))
