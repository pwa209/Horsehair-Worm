"""Generic immutable allocation/dependency plan tests; no scheduler or worker."""
from pathlib import Path
import ast
import hashlib
import importlib.util
import io
import json
import os
import shlex
import sys
import unittest

AREA=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('bounded_full_cohort_plan',AREA/'build-native-plan.py')
builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)


class FullCohortPlanTests(unittest.TestCase):
    def setUp(self):
        self.config={'fixture':'generic plan binding'}
        self.previous=builder.CONFIG;builder.CONFIG=builder.canonical(self.config)
        self.addCleanup(setattr,builder,'CONFIG',self.previous)
        self.proofs=[{'job_id':job,'dependency_kind':'afterok'} for job in ('6942117','6689965','6943757')]
        self.calls=[]

    def render(self,**changes):
        args={'config':self.config,'release_identity':{'sha256':builder.SOURCE},'bundle_sha':'a'*64,
            'verifier_job':'6942117','receipt_sha':'b'*64,'verifier_bundle_sha':'c'*64,'proofs':self.proofs,
            'script_renderer':lambda *args,**kwargs:self.calls.append((args,kwargs)) or '# inert source-bound script\n'}
        args.update(changes);return builder.render(**args)

    def test_allocation_and_child_limits_leave_ten_minute_cleanup_without_new_submission(self):
        plan,script=self.render();task=plan['tasks'][0]
        self.assertEqual(task['resources'],{'cpus':1,'memory':'64G','hours':24,'gpu':False,'array':None})
        self.assertEqual(self.calls[0][1],{'cpus':1,'memory':'64G','hours':24,'gpu':False,'array':None})
        limits=plan['operational_limits']
        self.assertEqual(limits,task['operational_limits'])
        self.assertEqual(limits['maximum_child_seconds']+limits['minimum_cleanup_seconds'],24*3600)
        self.assertEqual(limits['child_memory_ceiling_bytes'],48*1024**3)
        self.assertEqual(task['script_sha256'],builder.canonical(script))
        self.assertEqual(task['completed_external_dependencies'],self.proofs)

    def test_source_qualification_receipt_and_new_context_are_bound_in_exact_argv(self):
        plan,_=self.render();argv=shlex.split(plan['tasks'][0]['command']);options=dict(zip(argv[4::2],argv[5::2]))
        self.assertEqual(argv[:4],['python','-s','-u',str(builder.CAPSULE/'run-full-cohort-continuation-v1-20261024.py')])
        self.assertEqual(options['--context-helper'],str(builder.CONTEXT))
        self.assertEqual(options['--context-helper-sha256'],builder.CONTEXT_SHA)
        self.assertEqual(options['--source-manifest-sha256'],'a'*64)
        self.assertEqual(options['--verifier-source-manifest-sha256'],'c'*64)
        self.assertEqual(options['--prefix-job-id'],'6942117')
        self.assertEqual(options['--qualification-job-id'],'6689965')
        self.assertEqual(plan['completed_record_probe'],builder.RECORD_PROBE_BINDING)
        for flag in ('scientific_result','complete_declared_workload','admission_authority'):
            self.assertIs(plan[flag],False);self.assertIs(plan['tasks'][0][flag],False)

    def test_missing_probe_wrong_kind_duplicate_or_reused_identity_refuses(self):
        for proofs in (self.proofs[:-1],self.proofs+[self.proofs[-1]],
                [self.proofs[0],self.proofs[1],dict(self.proofs[2],dependency_kind='afterany')],
                [self.proofs[0],self.proofs[1],dict(self.proofs[2],job_id='6943758')]):
            with self.subTest(proofs=proofs):
                with self.assertRaises(ValueError):self.render(proofs=proofs)
        for job in ('6943757','6689965','6865674','0','6942117.batch'):
            with self.subTest(job=job):
                with self.assertRaises(ValueError):self.render(verifier_job=job)
        self.assertEqual(self.calls,[])

    def test_changed_source_configuration_or_unpinned_bundle_refuses_before_render(self):
        for changes in ({'config':{}},{'release_identity':{'sha256':'f'*64}},{'bundle_sha':'unknown'},
                {'receipt_sha':'unknown'},{'verifier_bundle_sha':'unknown'}):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):self.render(**changes)
        self.assertEqual(self.calls,[])

    def test_native_preflight_all_three_final_dependencies_precede_fresh_plan_write(self):
        tree=ast.parse((AREA/'build-native-plan.py').read_bytes())
        main=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='main')
        proof=next(node for node in ast.walk(main) if isinstance(node,ast.ListComp)
            and 'completed_native_dependency' in ast.unparse(node))
        mkdir=next(node for node in ast.walk(main) if isinstance(node,ast.Call)
            and isinstance(node.func,ast.Attribute) and node.func.attr=='mkdir')
        self.assertLess(proof.lineno,mkdir.lineno)
        self.assertIn("(args.verifier_job_id, '6689965', RECORD_PROBE_JOB)",ast.unparse(proof))
        self.assertNotIn('slurm.submit',ast.unparse(main))

    def test_current_context_file_matches_the_frozen_builder_pin(self):
        context=AREA/'run-full-cohort-continuation-context-v1-20261024.py'
        self.assertEqual(hashlib.sha256(context.read_bytes()).hexdigest(),builder.CONTEXT_SHA)


if __name__=='__main__':
    output=io.StringIO()
    result=unittest.TextTestRunner(stream=output,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    destination=os.environ.get('HHW_TEST_RESULT_DIR')
    if destination:
        directory=Path(destination).resolve(strict=True)
        with (directory/'test-output.txt').open('x',encoding='utf8',newline='\n') as stream:stream.write(output.getvalue())
        with (directory/'test-result.json').open('x',encoding='utf8',newline='\n') as stream:
            json.dump(dict(tests=result.testsRun,failures=len(result.failures),errors=len(result.errors),skipped=len(result.skipped),
                successful=result.wasSuccessful(),tests_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                derivative_sha256=hashlib.sha256((AREA/'run-full-cohort-continuation-v1-20261024.py').read_bytes()).hexdigest()),
                stream,sort_keys=True,indent=2);stream.write('\n')
    sys.stdout.write(output.getvalue());raise SystemExit(0 if result.wasSuccessful() else 1)
