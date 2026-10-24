"""Generic emitted metadata and mutation prohibition; no numerical functions."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import ast
import copy
import hashlib
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest

AREA=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('canary_generic_tests',AREA/'run_emitted_descriptor_canary.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class MetadataTests(unittest.TestCase):
    def setUp(self):
        self.original=m.read(AREA/'captured-original-descriptor.json')
        self.q=m.read(AREA/'captured-native-qualification.json')
        self.binding={'path':'/scratch/qualification.json','sha256':m.digest(AREA/'captured-native-qualification.json'),
            'job_id':self.q['slurm_job_id']}
        self.runtime=self.q['runtime_contract']

    def test_actual_captured_original_and_qualified_new_base_are_exact(self):
        self.assertEqual(m.digest(AREA/'captured-original-descriptor.json'),m.OLD_DESCRIPTOR_SHA)
        self.assertEqual(self.q['sources']['base']['sha256'],m.BASE_SHA)
        self.assertEqual(self.q['sources']['outer']['sha256'],m.OUTER_SHA)
        self.assertEqual(self.q['slurm_job_id'],'6942315')
        self.assertEqual(self.q['suite_contract']['tests']+sum(x['contract']['tests'] for x in self.q['regression_suites']),272)

    def test_real_builder_emits_only_compatible_operational_descriptor_changes(self):
        # Execute the real frozen emitter with intercepted writes and plan freeze.
        source=AREA.parent/'f2-backfill-controller-v1/build-backfill-native-plans-v1-20261024.py'
        function=next(n for n in ast.parse(source.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='controller_plan')
        with tempfile.TemporaryDirectory(dir=AREA) as temporary:
            support=Path(temporary);area=support/'new-handoff';area.mkdir()
            original=Path('/original-descriptor.json');policy=Path('/original-policy.json');report=Path('/new-qualification.json')
            values={original:copy.deepcopy(self.original),policy:{},report:copy.deepcopy(self.q)}
            rawpins={original:'9a0706642ac1660fb7f39fe68279ca34bdc10f89812a0c2397608d2a382cf464',policy:'old-policy-pin',report:self.binding['sha256']}
            outer=SimpleNamespace(BACKFILL_BASE_SHA=m.BASE_SHA,BACKFILL_RUNTIME=self.runtime,
                qualification=lambda value:{'original_provider_checked':True},
                backfill_qualification=lambda value,**kwargs:None)
            def exclusive(path,value):values[Path(path)]=copy.deepcopy(value)
            def digest(path):return rawpins.get(Path(path),m.canonical(values.get(Path(path),str(path))))
            scope=dict(Path=Path,re=__import__('re'),read_json=lambda path:copy.deepcopy(values[Path(path)]),digest=digest,
                require=m.require,exclusive=exclusive,canonical_hash=m.canonical,shlex=__import__('shlex'),sys=__import__('sys'),ROOT=support,SUPPORT=support,
                OLD_DESCRIPTOR=original,OLD_DESCRIPTOR_SHA=rawpins[original],OLD_POLICY=policy,OLD_POLICY_SHA=rawpins[policy],
                OLD_PLAN=support/'old-plan.json',
                BASE_NAME='base.py',OUTER_NAME='outer.py',native_sources=lambda:(outer,{}, {},{}),
                script=lambda *args,**kwargs:'inert registered script',
                freeze_plan=lambda config,identity,resource_policy,phase,tasks,**metadata:{'tasks':tasks,**metadata})
            exec(compile(ast.Module(body=[function],type_ignores=[]),str(source),'exec'),scope)
            result=scope['controller_plan'](report,handoff_job_id='123',area=area)
            emitted=values[area/'backfill-base-descriptor.json']
            proof=m.descriptor_clone(self.original,emitted,dict(self.binding,path=str(report)),self.runtime)
            self.assertEqual(proof['changed_or_added_fields'],['backfill_qualification','backfill_runtime_contract','controller_sha256'])
            self.assertEqual(emitted['registry_overlay'],self.original['registry_overlay'])
            self.assertEqual(emitted['worker_descriptor'],self.original['worker_descriptor'])
            self.assertEqual(len(result['tasks']),4)

    def test_wrong_worker_registry_or_source_mutation_is_rejected(self):
        expected={**self.original,'controller_sha256':m.BASE_SHA,'backfill_qualification':self.binding,
            'backfill_runtime_contract':self.runtime}
        for field in ['worker_descriptor','registry_overlay','framework_release','full_config_sha256','case']:
            with self.subTest(field=field):
                changed=copy.deepcopy(expected);changed[field]='changed'
                with self.assertRaises(ValueError):m.descriptor_clone(self.original,changed,self.binding,self.runtime)

    def test_in_memory_registration_models_actual_tasks_without_writing_a_ledger(self):
        plan={'release_identity':{'sha256':'source'},'tasks':[{'name':'controller-'+str(i),'script_sha256':str(i)} for i in range(4)]}
        proof=m.fixture_registration(plan,'fixture-job')
        self.assertEqual(proof['plan_sha256'],m.canonical(plan))
        self.assertEqual(len(proof['tasks']),4)
        self.assertTrue(all(x['job_id']=='fixture-job' for x in proof['tasks'].values()))

    def test_duplicate_emitted_task_roster_is_rejected(self):
        with self.assertRaises(ValueError):m.fixture_registration({'tasks':[{'name':'same'},{'name':'same'}]},'fixture')

    def test_module_has_no_controller_dispatch_or_full_load_context_call(self):
        function=next(n for n in ast.parse((AREA/'run_emitted_descriptor_canary.py').read_bytes()).body
            if isinstance(n,ast.FunctionDef) and n.name=='canary')
        calls=[ast.unparse(n.func) for n in ast.walk(function) if isinstance(n,ast.Call)]
        self.assertNotIn('base.run',calls)
        self.assertNotIn('base.load_context',calls)
        self.assertNotIn('base.tick',calls)
        self.assertIn('outer.main',calls)
        for name in ['base.worker_binding','base.registry_binding','base.retry_binding','base.source_guard']:
            self.assertIn(name,calls)


class ReadonlyTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(dir=AREA);self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name);self.source=self.root/'source.json';self.source.write_text('{}')

    def test_real_reads_remain_available_and_guard_restores_after_exit(self):
        attempts=[]
        with m.readonly(attempts):self.assertEqual(m.read(self.source),{})
        self.assertEqual(attempts,[])
        (self.root/'after.txt').write_text('guard restored')

    def test_file_write_is_blocked_before_mutation(self):
        attempts=[]
        with m.readonly(attempts):
            with self.assertRaises(RuntimeError):self.source.write_text('changed')
        self.assertEqual(self.source.read_text(),'{}');self.assertEqual(attempts[0]['operation'],'file-write')

    def test_directory_creation_and_removal_are_blocked(self):
        attempts=[]
        with m.readonly(attempts):
            with self.assertRaises(RuntimeError):(self.root/'new').mkdir()
            with self.assertRaises(RuntimeError):self.source.unlink()
        self.assertFalse((self.root/'new').exists());self.assertTrue(self.source.exists())

    def test_subprocess_and_scheduler_invocations_are_blocked(self):
        attempts=[]
        with m.readonly(attempts):
            with self.assertRaises(RuntimeError):subprocess.run(['sbatch','inert'])
            with self.assertRaises(RuntimeError):subprocess.check_output(['scontrol','inert'])
        self.assertEqual(len(attempts),2)

    def test_raw_os_write_is_blocked(self):
        attempts=[]
        with m.readonly(attempts):
            with self.assertRaises(RuntimeError):os.open(self.source,os.O_WRONLY|os.O_TRUNC)
        self.assertEqual(self.source.read_text(),'{}')


if __name__=='__main__':unittest.main(verbosity=2)
