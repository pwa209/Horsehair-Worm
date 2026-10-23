"""Copy exact read-only native evidence into the unsealed operational capsule."""
from pathlib import Path
import argparse,base64,hashlib,json,shutil

p=argparse.ArgumentParser();p.add_argument('number',type=int);a=p.parse_args()
assert 492<=a.number<=520
stem='hhw-controller-resumption-live-20261023-'+str(a.number)
root=Path('operations/20261023/primary-handoff-native-reconciliation-v1')
assert root.is_dir() and not (root/'source-manifest.json').exists()
dest=root/'narval-observations'/str(a.number);dest.mkdir(parents=True,exist_ok=False)
for src in [Path('work')/(stem+'.sh'),Path('work/narval-deployment')/(stem+'.json'),Path('work/narval-deployment')/(stem+'.txt')]:
    shutil.copyfile(src,dest/src.name)
v=json.loads(next(line.split(' ',1)[1] for line in (dest/(stem+'.txt')).read_text().splitlines() if line.startswith('PRIMARY_HANDOFF_LIVE ')))
for itemset in ['files','suites']:
    for remote,item in v.get(itemset,{}).items():
        if 'base64' not in item:continue
        prefix='/scratch/pwa209/horsehair-worm/';assert remote.startswith(prefix)
        rel=Path(remote[len(prefix):]);assert not rel.is_absolute() and '..' not in rel.parts
        target=dest/rel;assert not target.exists()
        raw=base64.b64decode(item['base64'],validate=True);assert hashlib.sha256(raw).hexdigest()==item['sha256']
        target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
print(json.dumps({'checked_utc':v['checked_utc'],'capsule_observation':str(dest),'native_returncode':v['native']['returncode'],'queue_returncode':v['queue']['returncode']}))
