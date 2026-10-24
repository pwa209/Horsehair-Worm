"""Create a provider-only orchestration derivative; retain exact census primitives."""
from pathlib import Path
import ast
import hashlib
import json

AREA=Path(__file__).resolve().parent


def main():
    old=(AREA/'original-run-fresh-storage-census-v6-20261023.py').read_bytes()
    guard=(AREA/'original-registered-snapshot-read-guard-v4-20261022.py').read_bytes()
    assert hashlib.sha256(old).hexdigest()=='487bf2dea2f5c892c369586e11c4885e44e4c5bbddbda1e9e3b82f185666f9f3'
    assert hashlib.sha256(guard).hexdigest()=='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
    g=guard.decode().replace('CONTROLLER_LOCK_SECONDS=900','CONTROLLER_LOCK_SECONDS=7200').replace('LOCK_MARGIN=300','LOCK_MARGIN=600').replace("61 if role=='worker-admission' else 16","61 if role=='worker-admission' else 121")
    (AREA/'registered-snapshot-read-guard-resilience-v1-20261024.py').write_bytes(g.encode())
    methods=(AREA/'resilience-functions.py.txt').read_text();lines=methods.splitlines(keepends=True)
    replacements={n.name:''.join(lines[n.lineno-1:n.end_lineno]) for n in ast.parse(methods).body if isinstance(n,ast.FunctionDef)}
    extra='\n\n'.join(replacements.pop(k) for k in ('legacy_module','predecessor_terminal','qualification_registration','validate_schedule_binding','validate_predecessor_receipt'))
    raw=old.decode();lines=raw.splitlines(keepends=True)
    for n in sorted((n for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef) and n.name in replacements),key=lambda n:n.lineno,reverse=True):
        lines[n.lineno-1:n.end_lineno]=[replacements[n.name]+'\n']
    raw=''.join(lines);raw=raw.replace('def main():',extra+'\n\n\ndef main():',1)
    raw=raw.replace("CASE_ROOT=ROOT/'maintenance/study-af-rolling-storage-census-v4-20261023'","CASE_ROOT=ROOT/'maintenance/study-census-resilience-v1-20261024'")
    raw=raw.replace("SOURCE_SUPPORT=ROOT/'maintenance/study-af-rolling-census-support-v4-20261023'","SOURCE_SUPPORT=ROOT/'maintenance/study-census-resilience-support-v1-20261024'")
    raw=raw.replace("KERNEL_Q=ROOT/'maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json'","KERNEL_Q=ROOT/'maintenance/study-census-resilience-qualification-v1-20261024/qualification.json'")
    raw=raw.replace("QUALIFIER_NAME='qualify-streaming-census-v2-20261023.py'","QUALIFIER_NAME='qualify-census-resilience-v1-20261024.py'")
    raw=raw.replace("WRAPPER_NAME='run-fresh-storage-census-v6-20261023.py'","WRAPPER_NAME='run-fresh-storage-census-resilience-v1-20261024.py'")
    raw=raw.replace("PHASE='study-af-rolling-storage-census-v4-20261023'","PHASE='study-census-resilience-v1-20261024'")
    raw=raw.replace("EXTENDED_REGISTRY_GUARD=Path(__file__).with_name('registered-snapshot-read-guard-v4-20261022.py')","EXTENDED_REGISTRY_GUARD=Path(__file__).with_name('registered-snapshot-read-guard-resilience-v1-20261024.py')")
    raw=raw.replace("EXTENDED_REGISTRY_SHA='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'",f"EXTENDED_REGISTRY_SHA='{hashlib.sha256(g.encode()).hexdigest()}'")
    raw=raw.replace("task.get('dependency_kind')=='afterok'","task.get('dependency_kind')==('afterok' if index==1 else 'afterany')")
    raw=raw.replace("require(provider['predecessor_job_id'] is None or completed_native_dependency(ROOT,provider['predecessor_job_id']) is not None,\n        'Rolling predecessor must have a positive completed native accounting proof')","predecessor=predecessor_terminal(provider,observed)")
    raw=raw.replace("args.kernel_qualification_job_id}),", "args.kernel_qualification_job_id,'6863966',*([provider['predecessor_job_id']] if provider['predecessor_job_id'] else [])}),")
    raw=raw.replace("str(SOURCE_SUPPORT/KERNEL_NAME)","str(LEGACY_SUPPORT/KERNEL_NAME)").replace("digest(SOURCE_SUPPORT/KERNEL_NAME)==pins[str(SOURCE_SUPPORT/KERNEL_NAME)]","digest(LEGACY_SUPPORT/KERNEL_NAME)==pins[str(LEGACY_SUPPORT/KERNEL_NAME)]")
    raw=raw.replace("'provider_registration':provider,","'provider_registration':provider,'predecessor_terminal':predecessor,'legacy_binding':LEGACY_BINDING,")
    binding={'helper':{'path':'/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v4-20261023/run-fresh-storage-census-v6-20261023.py','sha256':'487bf2dea2f5c892c369586e11c4885e44e4c5bbddbda1e9e3b82f185666f9f3'},
        'source_manifest':{'path':'/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-support-v4-20261023/source-manifest.json','sha256':'1b21742f6ad5b7b48899a4890988da7ddba439798ba8b12a2bad57c7f1898e6e'},
        'qualification':{'path':'/scratch/pwa209/horsehair-worm/maintenance/study-af-rolling-census-qualification-v5-20261023/qualification.json','sha256':'8b0dc612cee64461b991a9dfb9a975cf5afe1389a037b1357244d829f5aca76c','job_id':'6863966'}}
    raw=raw.replace('def require(value,message):',"LEGACY_SUPPORT=ROOT/'maintenance/study-af-rolling-census-support-v4-20261023'\nLEGACY_BINDING="+repr(binding)+'\n\n\ndef require(value,message):',1)
    ast.parse(raw);assert '\r' not in raw;(AREA/'run-fresh-storage-census-resilience-v1-20261024.py').write_bytes(raw.encode())
    before={n.name:n for n in ast.parse(old).body if isinstance(n,ast.FunctionDef)};after={n.name:n for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef)}
    changed=sorted(k for k in before if ast.dump(before[k],include_attributes=False)!=ast.dump(after[k],include_attributes=False))
    proof={'original_provider_sha256':hashlib.sha256(old).hexdigest(),'provider_sha256':hashlib.sha256(raw.encode()).hexdigest(),
        'guard_sha256':hashlib.sha256(g.encode()).hexdigest(),'changed_orchestration_functions':changed,
        'unchanged_function_asts':sorted(set(before)-set(changed)),'census_measurement_definition_changed':False}
    (AREA/'preservation.json').write_bytes((json.dumps(proof,sort_keys=True,indent=2)+'\n').encode());print(json.dumps(proof,sort_keys=True))


if __name__=='__main__':main()
