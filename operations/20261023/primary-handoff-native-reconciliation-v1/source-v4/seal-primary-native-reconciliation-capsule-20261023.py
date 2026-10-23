"""Seal the finished operational source and native evidence without Git actions."""
from pathlib import Path
import hashlib,json

root=Path('operations/20261023/primary-handoff-native-reconciliation-v1')
target=root/'source-manifest.json'
assert not target.exists(), 'Preserve an existing capsule seal'
files={}
for path in sorted(root.rglob('*')):
    assert not path.is_symlink(), 'No symbolic links in the source capsule'
    if not path.is_file() or path.name=='.gitattributes':continue
    raw=path.read_bytes()
    files[path.relative_to(root).as_posix()]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
assert len(files)>400 and 'README.md' in files
assert files['source-v4/handoff-primary-external-census-v1-20261023.py']['sha256']=='2a28f1b476a42959febbb445824f043521e74713b9f25dee3e9e13db92699381'
value={'kind':'primary_native_handoff_reconciliation_source_capsule_v1','scientific_source_sha256':'f047ce12b0a4a31312f2b4f5b6cff144342a12950f2e84fe1d444c8ea3a9b68c','files':files}
raw=(json.dumps(value,sort_keys=True,indent=2)+'\n').encode()
with target.open('xb') as stream:stream.write(raw)
print(json.dumps({'capsule':str(root),'files':len(files),'bytes':sum(item['bytes'] for item in files.values()),'manifest_sha256':hashlib.sha256(raw).hexdigest()}))
