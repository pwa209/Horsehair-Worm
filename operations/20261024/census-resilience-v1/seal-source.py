"""Seal explicit provider files only; never recurse into separate consumers/."""
from pathlib import Path
import ast
import hashlib
import json

AREA=Path(__file__).resolve().parent
FILES=['README.md','build-candidate.py','resilience-functions.py.txt','original-run-fresh-storage-census-v6-20261023.py',
    'original-registered-snapshot-read-guard-v4-20261022.py','run-fresh-storage-census-resilience-v1-20261024.py',
    'registered-snapshot-read-guard-resilience-v1-20261024.py','qualify-census-resilience-v1-20261024.py',
    'build-native-plans.py','test_census_resilience_v1_20261024.py','preservation.json','seal-source.py']


def main():
    pins={}
    for name in FILES:
        raw=(AREA/name).read_bytes()
        if b'\r' in raw:raise ValueError('LF bytes required: '+name)
        if name.endswith('.py'):ast.parse(raw,filename=name)
        pins[name]=hashlib.sha256(raw).hexdigest()
    provider=ast.parse((AREA/'run-fresh-storage-census-resilience-v1-20261024.py').read_bytes())
    legacy=ast.literal_eval(next(n.value for n in provider.body if isinstance(n,ast.Assign)
        and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='LEGACY_BINDING'))
    test='test_census_resilience_v1_20261024.py';tree=ast.parse((AREA/test).read_bytes())
    names=sorted(c.name+'::'+f.name for c in tree.body if isinstance(c,ast.ClassDef)
        for f in c.body if isinstance(f,ast.FunctionDef) and f.name.startswith('test_'))
    roster=hashlib.sha256(json.dumps(names,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    value={'kind':'census-resilience-source-manifest-v1','files':pins,'legacy':legacy,
        'suites':{'resilience':{'source':test,'source_sha256':pins[test],'tests':len(names),'roster_sha256':roster}}}
    output=AREA/'source-manifest.json'
    if output.exists():raise ValueError('Existing sealed manifest must be preserved')
    output.write_bytes((json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode())
    print(json.dumps({'source_manifest_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'files':len(pins),'generic_tests':len(names)},sort_keys=True))


if __name__=='__main__':main()
