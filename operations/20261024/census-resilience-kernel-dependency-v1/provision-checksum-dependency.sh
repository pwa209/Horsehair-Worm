#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
export TZ=UTC
python3 -B -u - <<'PY'
from pathlib import Path
from datetime import datetime,timezone
import ast,os,json,hashlib,stat,subprocess
assert os.geteuid()==3164768
r=Path('/scratch/pwa209/horsehair-worm');s=r/'maintenance/study-census-resilience-support-v1-20261024'
a=r/'maintenance/study-census-resilience-kernel-checksum-dependency-v1-20261024';assert not a.exists();a.mkdir(mode=0o700)
def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def regular(p,maximum=2000000):
 p=Path(p);q=p.lstat();assert p==p.resolve() and stat.S_ISREG(q.st_mode) and q.st_uid==3164768 and q.st_size<maximum
 return p
manifest_path=regular(s/'source-manifest.json');manifest_sha=digest(manifest_path);assert manifest_sha=='dbe6c589a7dda4e8d54cc041ddee1dfa77f7804d67e290efcd396c7108b6f152'
m=json.loads(manifest_path.read_bytes());before={str(regular(s/n)):sha for n,sha in m['files'].items()}
assert all(digest(p)==sha for p,sha in before.items())
helper=s/'run-fresh-storage-census-resilience-v1-20261024.py';assert digest(helper)=='94ac29d11da50412ba3c1f9269ed5762224f93635cd1fae505bfabc5f9b63a93'
legacy_manifest=regular(m['legacy']['source_manifest']['path']);assert digest(legacy_manifest)==m['legacy']['source_manifest']['sha256']
legacy=json.loads(legacy_manifest.read_bytes());name='streaming-storage-census-v1-20261023.py';expected=legacy['files'][name]
source=regular(legacy_manifest.parent/name);raw=source.read_bytes();assert len(raw)<1000000 and hashlib.sha256(raw).hexdigest()==expected
q=r/'maintenance/study-census-resilience-qualification-v1-20261024/qualification.json';assert digest(regular(q))=='7291b858df276bcc8dc6439bbe384b970c3f6a9fa47bfe20f4a9baa0b9e6b04b'
tree=ast.parse(helper.read_bytes());calls=[ast.unparse(n.args[0]) for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='run_path' and n.args and 'KERNEL_NAME' in ast.unparse(n.args[0])]
assert calls==['str(LEGACY_SUPPORT / KERNEL_NAME)'];assert helper.read_text().count('digest(SOURCE_SUPPORT/KERNEL_NAME)')==1
witness=a/'checksum-witness.bytes'
with witness.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
witness.chmod(0o444);target=s/name;assert not target.exists() and not target.is_symlink()
# Atomic exclusive publication of a separate byte-copy. The legacy file and all
# sealed source files remain unchanged. The new path is a checksum witness only.
os.link(witness,target);regular(target)
assert digest(target)==digest(source)==expected and target.read_bytes()==raw and digest(manifest_path)==manifest_sha
assert all(digest(p)==sha for p,sha in before.items()) and digest(q)=='7291b858df276bcc8dc6439bbe384b970c3f6a9fa47bfe20f4a9baa0b9e6b04b'
receipt={'status':'exact_already_qualified_kernel_checksum_dependency_provisioned','source':{'path':str(source),'sha256':expected},'target':{'path':str(target),'sha256':expected},'witness':{'path':str(witness),'sha256':expected},'bytes':len(raw),'source_manifest':{'path':str(manifest_path),'sha256':manifest_sha},'unchanged_source_pins':before,'actual_kernel_execution_path':str(source),'qualified_executable_added':False,'existing_source_or_evidence_modified':False,'automatic_replay_permitted':False,'scientific_result':False,'admission_authority':False,'freshness_timestamp_rewritten':False,'checked_utc':datetime.now(timezone.utc).isoformat()}
with (a/'receipt.json').open('xb') as f:f.write((json.dumps(receipt,sort_keys=True,indent=2,allow_nan=False)+'\n').encode());f.flush();os.fsync(f.fileno())
ledger=r/'state/study-census-resilience-v1-20261024-submission.json'
if ledger.exists():
 l=json.loads(ledger.read_bytes());first=str(l.get('tasks',{}).get('rolling-storage-census-01',{}).get('job_id',''))
 if first.isdigit():
  p=subprocess.run(['sacct','-j',first,'--format=JobID%64,State,ExitCode,ElapsedRaw,AllocCPUS,Start,End,User','--noheader','--parsable2'],capture_output=True,text=True,timeout=45)
  receipt['first_job_observed']={'job_id':first,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
print('RESILIENCE_KERNEL_CHECKSUM_DEPENDENCY_574 '+json.dumps(receipt,sort_keys=True),flush=True)
PY
