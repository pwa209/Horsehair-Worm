"""Seal only the completed local operational evidence capsule; no Git action."""
from pathlib import Path
import hashlib,json

root=Path('operations/20261023/census-restored-native-queue-and-primary-repair-v1')
manifest=root/'source-manifest.json'
assert not manifest.exists(), 'Preserve an existing immutable capsule seal'
files={}
for path in sorted(root.rglob('*')):
    assert not path.is_symlink(), 'No symbolic links in the source capsule'
    if not path.is_file() or path.name=='.gitattributes':
        continue
    raw=path.read_bytes()
    files[path.relative_to(root).as_posix()]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
assert len(files)>100 and 'README.md' in files
value={'kind':'restored_census_native_queue_and_primary_repair_source_capsule_v1','files':files}
raw=(json.dumps(value,sort_keys=True,indent=2)+'\n').encode()
with manifest.open('xb') as stream:stream.write(raw)
print(json.dumps({'capsule':str(root),'files':len(files),'bytes':sum(x['bytes'] for x in files.values()),'manifest_sha256':hashlib.sha256(raw).hexdigest()}))
