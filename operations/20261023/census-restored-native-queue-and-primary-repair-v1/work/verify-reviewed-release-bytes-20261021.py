from pathlib import Path
import subprocess,sys,io,json,hashlib,argparse
sys.path.insert(0,str(Path('src').resolve()))
from horsehair.core import source_identity
p=argparse.ArgumentParser();p.add_argument('--tree',default='');a=p.parse_args();root=Path.cwd();identity=source_identity(root)
expected={name:{'sha256':sha,'bytes':(root/name).stat().st_size} for name,sha in identity['files'].items()}
folders=['operations/20261021/public-s3-and-adjoint-continuation','operations/20261021/native-locks-and-public-canaries','operations/20261021/public-data-and-repair','operations/20261022/reviewed-public-continuation','operations/20261022/controller-and-prefix-continuation','operations/20261023/prestart-three-continuation','operations/20261023/healthy-narval-route','operations/20261023/queued-prestart-repair','operations/20261023/archive-reader-v3-qualification','operations/20261023/native-archive-test-registration','operations/20261023/census-deadline-status-check','operations/20261023/census-streaming-repair-v1','operations/20261023/census-and-controller-resumption-v1','operations/20261023/census-restored-native-queue-and-primary-repair-v1']
for manifest_path in [root/folder/'source-manifest.json' for folder in folders]:
 folder=manifest_path.parent.relative_to(root);raw=manifest_path.read_bytes();manifest=json.loads(raw)
 expected[manifest_path.relative_to(root).as_posix()]={'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
 for name,item in manifest['files'].items():expected[(folder/name).as_posix()]={k:item[k] for k in ('sha256','bytes')}
 attributes=manifest_path.parent/'.gitattributes';raw=attributes.read_bytes()
 expected[attributes.relative_to(root).as_posix()]={'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
names=list(expected);refs=[a.tree+':'+name for name in names]
r=subprocess.run(['git','cat-file','--batch'],input=('\n'.join(refs)+'\n').encode(),capture_output=True,check=True);stream=io.BytesIO(r.stdout)
for name in names:
 header=stream.readline().split();assert len(header)==3 and header[1]==b'blob',(name,header)
 raw=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert len(raw)==expected[name]['bytes'] and hashlib.sha256(raw).hexdigest()==expected[name]['sha256'],name
assert stream.read()==b''
print(json.dumps({'tree':a.tree or 'index','exact_byte_files_verified':len(names),'source_sha256':identity['sha256']}))
