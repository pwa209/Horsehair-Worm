"""Seal exact LF source and test rosters; never execute or submit a worker."""
from pathlib import Path
import ast
import hashlib
import json

AREA=Path(__file__).resolve().parent
FILES=['README.md','build-candidate.py','build-native-plan.py','seal-source.py',
    'full-cohort-functions.py.txt','expected-original-manifest.json','preservation.json',
    'original-one-record-helper.py','original-eight-hour-context.py',
    'run-full-record-adjoint-v3-20261021.py','run-full-cohort-prefix-v3-20261021.py',
    'run-full-cohort-continuation-v1-20261024.py','run-full-cohort-continuation-context-v1-20261024.py',
    'test_full_record_adjoint_v3_20261021.py','test_adjoint_lifecycle_20261024.py',
    'test_adjoint_operational_ingress_20261024.py','test_full_cohort_routing_20261024.py',
    'test_full_cohort_plan_20261024.py']
SUITES=[('test_adjoint_lifecycle_20261024.py','lifecycle'),
    ('test_adjoint_operational_ingress_20261024.py','operational-ingress'),
    ('test_full_cohort_routing_20261024.py','full-cohort-routing'),
    ('test_full_cohort_plan_20261024.py','full-cohort-plan')]


def source_manifest():
    pins={}
    for name in FILES:
        raw=(AREA/name).read_bytes()
        if b'\r' in raw:raise ValueError('LF transport required: '+name)
        if name.endswith('.py'):ast.parse(raw,filename=name)
        pins[name]=hashlib.sha256(raw).hexdigest()
    suites=[]
    for name,label in SUITES:
        tree=ast.parse((AREA/name).read_bytes())
        names=sorted(node.name+'.'+method.name for node in tree.body if isinstance(node,ast.ClassDef)
            for method in node.body if isinstance(method,ast.FunctionDef) and method.name.startswith('test_'))
        if not names or len(names)!=len(set(names)):raise ValueError('Nonempty exact operational suite roster required')
        suites.append({'path':name,'name':label,'tests':len(names),'test_names':names})
    return {'schema_version':1,'files':pins,'operational_test_suites':suites,
        'legacy_numerical_tests':72,'complete_native_qualification_tests':72+sum(x['tests'] for x in suites),
        'scientific_result':False,'complete_declared_workload':False,'admission_authority':False,
        'native_measurement_already_completed':False}


def main():
    result=source_manifest();output=AREA/'source-manifest.json'
    if output.exists():raise FileExistsError('Existing sealed source closure cannot be replaced automatically')
    raw=(json.dumps(result,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with output.open('xb') as stream:stream.write(raw)
    print(json.dumps({'source_manifest_sha256':hashlib.sha256(raw).hexdigest(),'files':len(result['files']),
        'native_cases_required':result['complete_native_qualification_tests']},sort_keys=True))


if __name__=='__main__':main()
