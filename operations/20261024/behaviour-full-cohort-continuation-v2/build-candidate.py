"""Reproduce a bounded orchestration derivative; numerical API files are untouched."""
from pathlib import Path
import ast
import hashlib
import json

AREA=Path(__file__).resolve().parent


def sha(raw):return hashlib.sha256(raw).hexdigest()


def replace_functions(raw,replacements):
    lines=raw.splitlines(keepends=True)
    tree=ast.parse(raw)
    for node in sorted((n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in replacements),
                       key=lambda n:n.lineno,reverse=True):
        lines[node.lineno-1:node.end_lineno]=[replacements.pop(node.name).rstrip()+'\n']
    text=''.join(lines)
    return text+'\n\n'+'\n\n'.join(replacements.values())+'\n' if replacements else text


def main():
    original=(AREA/'original-one-record-helper.py').read_bytes()
    context=(AREA/'original-eight-hour-context.py').read_bytes()
    assert sha(original)=='459e0aa542eabd824dfe6f9deacf5e0e702b0108e5816760024c155823e1bb2f'
    assert sha(context)=='e055352de535d029c658723b69707c371ab5d89e451cace506d305c344a3fd0f'
    ctx=context.decode().replace('Operational-only eight-hour prefix context','Operational-only24h full-training-fold comparison context')
    ctx=ctx.replace("{'08:00:00','8:00:00','0-08:00:00'}","{'24:00:00','1-00:00:00','0-24:00:00'}")
    ctx=ctx.replace('eight-hour prefix allocation','24-hour training-fold allocation').replace("'hours':8","'hours':24")
    ctx=ctx.replace("CASE=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-20261022'","CASE=ROOT/'maintenance/behaviour-full-cohort-continuation-v2-20261024'")
    ctx=ctx.replace("SUPPORT=ROOT/'maintenance/behaviour-full-cohort-prefix-v5-support-20261022'","SUPPORT=ROOT/'maintenance/behaviour-full-cohort-continuation-support-v2-20261024'")
    (AREA/'run-full-cohort-continuation-context-v2-20261024.py').write_bytes(ctx.encode())
    methods=(AREA/'full-cohort-functions.py.txt').read_text(encoding='utf-8')
    nodes=ast.parse(methods).body;lines=methods.splitlines(keepends=True)
    replacements={n.name:''.join(lines[n.lineno-1:n.end_lineno]) for n in nodes if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
    extra='\n\n'.join(replacements.pop(name) for name in ('ResidentDeadlineGuard','full_training_comparison','record_probe_evidence','census_qualification_bridge','census_provider_evidence'))
    raw=replace_functions(original.decode(),replacements)
    # Insert the new routing function before _worker and the guarded main call.
    raw=raw.replace('def _worker(',extra+'\n\n\ndef _worker(',1)
    raw=raw.replace("CONTEXT_SHA='e055352de535d029c658723b69707c371ab5d89e451cace506d305c344a3fd0f'",f"CONTEXT_SHA='{sha(ctx.encode())}'")
    raw=raw.replace("CASE=ROOT/'maintenance/behaviour-full-record-adjoint-v5prefix-v1-20261024'","CASE=ROOT/'maintenance/behaviour-full-cohort-continuation-v2-20261024'")
    raw=raw.replace("SUPPORT=ROOT/'maintenance/behaviour-full-record-adjoint-v5prefix-support-v1-20261024'","SUPPORT=ROOT/'maintenance/behaviour-full-cohort-continuation-support-v2-20261024'")
    old_stages="STAGES=('training_full_clock_scores','validation_full_quadrature_scores',\n        'training_component_all_gradients_and_one_Adam_update','validation_full_quadrature_all_gradients')"
    new_stages="STAGES=('full_fold_preprocessing_and_eligibility_census','primary_full_training_objective_and_all_gradients',\n        'adjoint_full_training_objective_and_all_gradients','full_training_objective_and_gradient_comparison')"
    assert old_stages in raw;raw=raw.replace(old_stages,new_stages)
    raw=raw.replace("os.environ.get('HHW_MAX_HOURS')=='8'","os.environ.get('HHW_MAX_HOURS')=='24'")
    raw=raw.replace('Owned finite1CPU/eight-hour native adjoint context required','Owned finite1CPU/24-hour native full-fold context required')
    raw=raw.replace('hours=8','hours=24').replace('max_hours=8','max_hours=24')
    raw=raw.replace("SUPPORT,'adjoint-baseline'","SUPPORT,'full-fold-first-order-technical'")
    raw=raw.replace("selection_amendment='full-clock-observation-eligible-validation-canary-v2',preprocessing_recomputed_not_restored=True,", "preprocessing_recomputed_not_restored=True,numerical_scope='One complete frozen training fold at initialization',")
    raw=raw.replace("pending=['Whole-cohort objective/gradient/performance','Other models/folds/starts/rates/bin widths'", "pending=['Native full-design timing/admission remains unqualified','Other models/folds/starts/rates/bin widths'")
    raw=raw.replace('args.prefix_job_id,ORIGINAL_JOB_ID}',"args.prefix_job_id,ORIGINAL_JOB_ID,'6943757'}")
    raw=raw.replace('    prefix_info=prefix_evidence(args.prefix_receipt', '    previous_probe=record_probe_evidence(observed)\n    prefix_info=prefix_evidence(args.prefix_receipt',1)
    raw=raw.replace("'prefix_evidence':prefix_info,\n            'private_qualification'", "'prefix_evidence':prefix_info,'previous_one_record_probe':previous_probe,\n            'private_qualification'")
    raw=raw.replace('            prefix_evidence=prefix_info,\n            private_qualification', '            prefix_evidence=prefix_info,previous_one_record_probe=previous_probe,\n            private_qualification')
    raw=raw.replace("'context-helper','prefix-receipt','tests')", "'context-helper','prefix-receipt','tests','census-provider-schedule')")
    raw=raw.replace("'prefix-job-id','tests-sha256')", "'prefix-job-id','tests-sha256','census-provider-job-id','census-provider-schedule-sha256','census-provider-receipt-sha256')")
    raw=raw.replace("ORIGINAL_JOB_ID,'6943757'}", "ORIGINAL_JOB_ID,'6943757',args.census_provider_job_id}")
    raw=raw.replace('    with context.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=24',
        "    provider_info=census_provider_evidence(args.census_provider_schedule,args.census_provider_schedule_sha256,\n"
        "        args.census_provider_job_id,repair._repair_native_states,receipt_sha=args.census_provider_receipt_sha256,\n"
        "        qualification_verifier=lambda schedule,qualification:census_qualification_bridge(schedule,qualification,allocation['allocation_end_unix']))\n"
        "    with context.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=24",1)
    raw=raw.replace("'previous_one_record_probe':previous_probe,", "'previous_one_record_probe':previous_probe,'new_census_provider':provider_info,")
    raw=raw.replace('prefix_evidence=prefix_info,previous_one_record_probe=previous_probe,',
        'prefix_evidence=prefix_info,previous_one_record_probe=previous_probe,new_census_provider=provider_info,')
    raw=raw.replace('no context numerical\n    # function, qualification requirement, resource bound or kernel is changed.', 'the derivative context changes only owned24h allocation validation;\n    # qualification, accounting, storage and numerical kernels retain their pins.')
    ast.parse(raw);assert '\r' not in raw
    target=AREA/'run-full-cohort-continuation-v2-20261024.py';target.write_bytes(raw.encode())
    before={n.name:n for n in ast.parse(original).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
    after={n.name:n for n in ast.parse(raw).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
    changed=sorted(k for k in before if ast.dump(before[k],include_attributes=False)!=ast.dump(after[k],include_attributes=False))
    preserved=sorted(k for k in before if k not in changed)
    proof={'schema_version':1,'original_helper_sha256':sha(original),'original_context_sha256':sha(context),
        'new_context_sha256':sha(ctx.encode()),'changed_existing_orchestration_functions':changed,
        'preserved_existing_function_asts':preserved,'new_orchestration_functions':['ResidentDeadlineGuard','full_training_comparison','record_probe_evidence','census_qualification_bridge','census_provider_evidence'],
        'numerical_api_source_files_changed':False,'scientific_result':False,'admission_authority':False,'complete_declared_workload':False}
    (AREA/'preservation.json').write_bytes((json.dumps(proof,sort_keys=True,indent=2)+'\n').encode())
    print(json.dumps({'runner_sha256':sha(raw.encode()),'context_sha256':sha(ctx.encode()),'changed_functions':changed},sort_keys=True))


if __name__=='__main__':main()
