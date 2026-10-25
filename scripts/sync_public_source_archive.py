"""Build an integrity-verified public-source sync using three scratch inodes.

The generated administrative request stores the committed repository as an
archive and manifest. It never extracts another historical source tree or
changes a scientific allocation, resource budget, or active release.
"""
from pathlib import Path, PurePosixPath
import argparse
import base64
import hashlib
import inspect
import json
import re
import shutil
import subprocess
import tarfile


def verify_archive(path, manifest):
    if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=manifest['archive_sha256']:
        raise ValueError('Public source archive bytes changed')
    observed={}
    with tarfile.open(path,'r:gz') as stream:
        for member in stream:
            name=PurePosixPath(member.name)
            if name.is_absolute() or '..' in name.parts or not (member.isfile() or member.isdir()):
                raise ValueError('Only ordinary repository files and directories are permitted')
            if member.isdir():continue
            if member.name in observed:raise ValueError('Duplicate source archive member')
            observed[member.name]=hashlib.sha256(stream.extractfile(member).read()).hexdigest()
    if observed!=manifest['files']:raise ValueError('The complete committed source manifest must match')
    return len(observed)


def build(request_id):
    if not re.fullmatch('[A-Za-z0-9_-]+',request_id):raise ValueError('Plain request identifier required')
    repo=Path(__file__).resolve().parents[1];git=shutil.which('git')
    if git is None:raise ValueError('Git is required')
    def command(*args):return subprocess.check_output([git,'-C',str(repo),*args])
    if command('status','--porcelain','--untracked-files=no').strip():raise ValueError('Commit reviewed tracked changes before source sync')
    commit=command('rev-parse','HEAD').decode().strip()
    if command('show','HEAD:scripts/sync_public_source_archive.py').decode('utf8').replace('\r\n','\n')!=Path(__file__).read_text(encoding='utf8'):
        raise ValueError('The source sync script must itself be committed and unchanged')
    work=repo/'work';work.mkdir(exist_ok=True)
    archive=work/(request_id+'.tar.gz')
    if archive.exists():raise ValueError('Use a fresh request identifier; never replace a sent source archive')
    subprocess.run([git,'-C',str(repo),'archive','--format=tar.gz','--output',str(archive),'HEAD'],check=True)
    files={}
    with tarfile.open(archive,'r:gz') as stream:
        for member in stream:
            if member.isfile():files[member.name]=hashlib.sha256(stream.extractfile(member).read()).hexdigest()
    tracked=set(command('ls-files','-z').decode().rstrip('\0').split('\0'))
    if set(files)!=tracked:raise ValueError('Git archive does not contain every tracked public file')
    manifest={'kind':'complete_committed_public_source_archive','git_commit':commit,'files':files,
              'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'source_only':True}
    verify_archive(archive,manifest)
    metadata=(json.dumps(manifest,indent=2,sort_keys=True)+'\n').encode()
    (work/(request_id+'-manifest.json')).write_bytes(metadata)
    payload={'archive':base64.b64encode(archive.read_bytes()).decode(),
             'manifest':base64.b64encode(metadata).decode()}
    remote='''#!/bin/bash
set -euo pipefail
test "$(id -un)" = pwa209
case "$(hostname -f)" in *.narval.calcul.quebec) ;; *) exit 2 ;; esac
python3 -B -u - <<'PY'
from pathlib import Path,PurePosixPath
import os,json,hashlib,tarfile,base64
assert os.geteuid()==3164768
'''+inspect.getsource(verify_archive)+'''
payload=PAYLOAD
metadata=base64.b64decode(payload['manifest']);manifest=json.loads(metadata)
assert manifest['git_commit']==COMMIT
root=Path('/scratch/pwa209/horsehair-worm');assert root.resolve()==root and not root.is_symlink()
directory=root/'source-archives';directory.mkdir(exist_ok=True)
assert directory.resolve()==directory and not directory.is_symlink() and directory.stat().st_uid==3164768
commit=manifest['git_commit'];archive=directory/(commit+'.tar.gz');mp=directory/(commit+'-manifest.json')
for target,raw in [(archive,base64.b64decode(payload['archive'])),(mp,metadata)]:
 if target.exists():assert not target.is_symlink() and target.read_bytes()==raw
 else:
  with target.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
count=verify_archive(archive,manifest)
receipt={'status':'verified_complete_public_source_archive','git_commit':commit,'tracked_public_files':count,
 'archive':str(archive),'archive_sha256':manifest['archive_sha256'],
 'manifest':str(mp),'manifest_sha256':hashlib.sha256(metadata).hexdigest(),
 'source_only':True,'archive_extracted':False,'scientific_jobs_changed':False}
rp=directory/(commit+'-receipt.json');raw=(json.dumps(receipt,indent=2,sort_keys=True)+'\\n').encode()
if rp.exists():assert rp.read_bytes()==raw
else:
 with rp.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
print('PUBLIC_SOURCE_ARCHIVE_SYNC '+json.dumps(receipt),flush=True)
PY
'''
    remote=remote.replace('COMMIT',repr(commit)).replace('PAYLOAD',repr(payload))
    request=work/(request_id+'.sh');request.write_text(remote,encoding='utf8',newline='\n')
    return {'request':str(request),'git_commit':commit,'tracked_public_files':len(files),
            'archive_bytes':archive.stat().st_size,'archive_extracted':False}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--request-id',required=True)
    print(json.dumps(build(parser.parse_args().request_id)),flush=True)
