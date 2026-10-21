from pathlib import Path
import argparse,ast,base64,zlib,hashlib,json,subprocess,sys,runpy,re,xml.etree.ElementTree as ET
p=argparse.ArgumentParser();p.add_argument('--qualification-job',required=True);p.add_argument('--qualification-sha256',required=True);p.add_argument('--after-census-job',required=True);a=p.parse_args()
assert re.fullmatch('[1-9][0-9]*',a.qualification_job) and re.fullmatch('[1-9][0-9]*',a.after_census_job) and re.fullmatch('[0-9a-f]{64}',a.qualification_sha256)
r='/scratch/pwa209/horsehair-worm';area=r+'/maintenance/study-af-registry-repair-activation-stage-v3-20261021';support=r+'/maintenance/study-af-registry-repair-support-v4-20261021'
helper=Path('work/activate-f2-registry-repair-v3-20261021.py');tests=Path('work/test_activate_f2_registry_repair_v3_20261021.py')
hsha='04b9b43f1aebf2bb700597bbec2f42cb9c1a0b85239ff84d1dd6fa6dc08c448c';tsha='937e40e09b70e8797b245c0105401f853bd214abcdbc75ead854dc4a246fe62c'
assert hashlib.sha256(helper.read_bytes()).hexdigest()==hsha
tsha_actual=hashlib.sha256(tests.read_bytes()).hexdigest()
assert tsha_actual==tsha
xml=ET.fromstring(Path('work/activation-v3-root-review-20261021.xml').read_bytes());roster=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in xml.iter('testcase'));assert len(roster)==31
rostersha=hashlib.sha256(json.dumps(roster,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
args=['--qualification-helper',support+'/qualify-f2-registry-repair-v4-20261021.py','--qualification',support+'/private-qualification.json','--qualification-sha256',a.qualification_sha256,'--qualification-job-id',a.qualification_job,'--evidence-packet',r+'/maintenance/study-af-f2-race-repair-evidence-v1-20261021/evidence-packet.json','--evidence-packet-sha256','b505d32ecb47f4365764acdfb6c2241312a9f122c09fa00464f2eed2c2849d37','--postactivation-evidence',r+'/maintenance/study-af-postactivation-evidence-v1-20261021/evidence.json','--postactivation-evidence-sha256','dc931704d39cea5e7962a5a3b430312e572c85b1edde3fb7aa0759e57d123d3e','--original-descriptor',r+'/maintenance/study-af-campaign-v1-20261021/precision-and-f2-continuation-v2/descriptor-v3-controller.json','--original-descriptor-sha256','ebe5ae3ce3c58aa509b4f7fafe29691f7818c04c2576e8eb276b08470fcdefde','--native-plan',r+'/plans/recovery-support-20261021-291/plan.json','--native-ledger',r+'/state/recovery-support-20261021-291-submission.json','--native-task','recovery-291','--native-script',r+'/plans/recovery-support-20261021-291/recovery-291.sbatch','--approval-reference','Current user 21 October 2026: complete ABCDEF, repair repeated failures and resume execution; CPU/GPU restrictions lifted']
subprocess.run([sys.executable,'work/build-native-stage-20261021.py','--id','291','--helper',str(helper),'--area',area,'--framework',r+'/releases/20261021-6ed201653dfa-4f3903d7c26e','--args-json',json.dumps(args)],check=True)
p=Path('work/hhw-recovery-stage-20261021-291.sh');text=runpy.run_path('work/patch-af-stage-expanded-20261021.py')['patch'](p.read_text())
raw=tests.read_bytes();insert="traw=zlib.decompress(base64.b64decode("+repr(base64.b64encode(zlib.compress(raw)).decode())+"));assert hashlib.sha256(traw).hexdigest()=="+repr(tsha)+";ast.parse(traw)\nwith (area/"+repr(tests.name)+").open('xb') as f:f.write(traw);f.flush();os.fsync(f.fileno())\n"
needle="assert not payload['after'] and not payload['after_specs']";assert text.count(needle)==1;text=text.replace(needle,insert+needle,1)
needle='from horsehair.campaign import combined_admission\n'
insert="import time\nrgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'\nassert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'\napi=runpy.run_path(str(rgp));api['install'](area/'submission-registry-reads',registration_mode='controller-already-locked',allocation_deadline_unix=time.time()+900)\n"
assert text.count(needle)==1;text=text.replace(needle,insert+needle,1)
bootstrap="from pathlib import Path\nimport os,sys,subprocess,hashlib,xml.etree.ElementTree as ET\nfrom horsehair.core import canonical_hash\narea=Path("+repr(area)+");helper=area/"+repr(helper.name)+";tests=area/"+repr(tests.name)+"\nassert hashlib.sha256(helper.read_bytes()).hexdigest()=="+repr(hsha)+" and hashlib.sha256(tests.read_bytes()).hexdigest()=="+repr(tsha)+"\n"
bootstrap+="from horsehair.core import read_json,digest\nroot=Path("+repr(r)+")\ncensus=read_json(root/'maintenance/study-af-fresh-storage-census-v1-20261021/receipt.json')\nassert census['native_job_id']=="+repr(a.after_census_job)+" and census['status']=='complete_exact_size_sweep_with_original_conservative_freshness' and census['source_sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'\nassert census['resource_policy']['sha256']=='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'\nassert census['sweep']['path']==str(root/'maintenance/study-af-fresh-storage-census-v1-20261021/sweep-result.json') and digest(census['sweep']['path'])==census['sweep']['sha256']\n"
bootstrap+="""temp=Path(os.environ['SLURM_TMPDIR']);assert temp.is_absolute() and temp.is_dir() and temp.stat().st_uid==3164768
workspace=temp/('hhw-f2-activation-contract-'+os.environ['SLURM_JOB_ID']);workspace.mkdir(mode=0o700,exist_ok=False)
env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','TZ':'UTC'}
for k in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(k,None)
xml=area/'activation31.xml';subprocess.run([sys.executable,'-s','-m','pytest',str(tests),'-q','-p','no:cacheprovider','--basetemp',str(workspace/'fixtures'),'--junitxml',str(xml)],env=env,timeout=300,check=True)
cases=list(ET.fromstring(xml.read_bytes()).iter('testcase'));rows=sorted((c.get('classname','').split('.')[-1],c.get('name','')) for c in cases)
"""
bootstrap+="assert len(cases)==31 and canonical_hash(rows)=="+repr(rostersha)+" and all(not any(c.find(k) is not None for k in ('failure','error','skipped')) for c in cases)\nassert hashlib.sha256(helper.read_bytes()).hexdigest()=="+repr(hsha)+" and hashlib.sha256(tests.read_bytes()).hexdigest()=="+repr(tsha)+"\nprint('AF_NATIVE_ACTIVATION_CONTRACT31_PASSED',flush=True)\n"
ast.parse(bootstrap)
needle="text=text.replace('set -euo pipefail','set -euo pipefail\\nexport PYTHONDONTWRITEBYTECODE=1',1)"
assert text.count(needle)==1;text=text.replace(needle,needle+"\ntext=text.replace(command+'\\n',shlex.join(['python','-s','-u','-c',"+repr(bootstrap)+"])+ '\\n'+command+'\\n',1)",1)
start=text.index('admission=combined_admission');end=text.index("print('RECOVERY_STAGE_SUBMITTED",start)
text=text[:start]+"with api['registration_lock'](area/'submission-registration-lock',time.time()+900,role='controller-start'):\n"+''.join('    '+s if s.strip() else s for s in text[start:end].splitlines(keepends=True))+text[end:]
assert text.count(",'array':None")==1;text=text.replace(",'array':None",'',1)
needle="task['dependency_kind']=payload['dependency_kind']"
assert text.count(needle)==1;text=text.replace(needle,needle+"\n    task['external_dependencies']=["+repr(a.after_census_job)+"]\n    task['external_dependency_specs']=[{'job_id':"+repr(a.after_census_job)+",'array':None,'source_sha256':'4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1','purpose':'Positive exact-size census before activation; unchanged six-hour runtime guard retained'}]",1)
ast.parse(text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]);p.write_bytes(text.encode());print('built291',rostersha,tsha)
