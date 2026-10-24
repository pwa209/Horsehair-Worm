"""Generic separate-interpreter metadata protocol and deadline regression cases."""
from pathlib import Path
from types import ModuleType,SimpleNamespace
from unittest.mock import patch
import ast
import copy
import hashlib
import importlib.util
import io
import json
import math
import os
import runpy
import signal
import subprocess
import sys
import tempfile
import time
import unittest

AREA=Path(__file__).resolve().parent
CANDIDATE=AREA/'run-full-cohort-continuation-v2-20261024.py'
CHILD=AREA/'verify-census-provider-v1-20261024.py'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def selected(names,**scope):
    nodes=[n for n in ast.parse(CANDIDATE.read_bytes()).body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(CANDIDATE),'exec'),scope);return scope


def child_module():
    spec=importlib.util.spec_from_file_location('fixture_census_metadata_bridge',CHILD)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


class BridgeTests(unittest.TestCase):
    def setUp(self):
        workspace=Path(os.environ.get('HHW_TEST_WORKSPACE',str(AREA/'w')));workspace.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(prefix='p',dir=workspace.resolve());self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.capsule=self.root/'maintenance/capsule'
        self.capsule.mkdir(parents=True);self.support=self.root/'maintenance/support';self.support.mkdir()
        self.runner=self.capsule/CANDIDATE.name;self.runner.write_text('# inert parent fixture\n')
        self.bridge=self.capsule/CHILD.name;self.bridge.write_text('# inert child fixture\n')
        self.manifest={'files':{CHILD.name:sha(self.bridge)}};self.source=self.capsule/'source-manifest.json'
        self.source.write_text(json.dumps(self.manifest));self.bound=self.support/'pinned.json';self.bound.write_text('{}')
        self.flags=next(ast.literal_eval(node.value) for node in ast.parse(CANDIDATE.read_bytes()).body
            if isinstance(node,ast.Assign) and isinstance(node.targets[0],ast.Name) and node.targets[0].id=='FALSE_FLAGS')
        self.scope=selected({'require','ordinary','exclusive','digest','bound_json','stop','census_qualification_bridge'},
            Path=Path,hashlib=hashlib,json=json,re=__import__('re'),math=math,time=time,os=os,signal=signal,
            subprocess=subprocess,sys=sys,__file__=str(self.runner),ROOT=self.root,SUPPORT=self.support,
            CAPSULE_MANIFEST=self.manifest,CONTEXT_SHA='c'*64,PRIMARY_SHA='p'*64,CONFIG='config',FALSE_FLAGS=self.flags)
        self.schedule={'helper':{'path':'/fixture/pinned-provider.py','sha256':'d'*64}}
        self.qualification={'path':'/fixture/qualification.json','sha256':'e'*64,'job_id':'900000002'}
        self.calls=[];self.terminated=[];self.waits=[];self.mode='positive'

    def factory(self,command,**options):
        self.calls.append((command,options));request_path=Path(command[command.index('--request')+1]);request=json.loads(request_path.read_bytes())
        response=Path(command[command.index('--response')+1]);framework=self.root/'releases/20261021-6ed201653dfa-4f3903d7c26e'
        result={'schema_version':1,'status':'complete_exact_4f_census_qualification_metadata_bridge',
            'source_identity':{'package_path':str(framework/'src/horsehair/__init__.py'),
                'framework':{'sha256':SOURCE},'primary':{'sha256':'p'*64},'config_sha256':'config'},
            'request_sha256':sha(request_path),'bridge_sha256':sha(self.bridge),'provider_helper':self.schedule['helper'],
            'source_manifest':request['source_manifest'],'checked_paths':{str(self.bound):sha(self.bound)},
            'qualification':{'checked_paths':{str(self.bound):sha(self.bound)}},
            **self.flags,'numerical_work_executed':False,'canonical_state_changed':False,'artifacts_deserialized':False}
        if self.mode=='wrong_package':result['source_identity']['package_path']=str(self.root/'releases/8495/src/horsehair/__init__.py')
        raw=json.dumps(result)
        if self.mode=='duplicate':raw=raw[:-1]+',"status":"complete_exact_4f_census_qualification_metadata_bridge"}'
        if self.mode=='nonfinite':raw=raw[:-1]+',"unexpected":NaN}'
        response.write_text(raw)
        if self.mode=='request_changed':request_path.write_text(request_path.read_text()+' ')
        if self.mode=='source_changed':self.bridge.write_text('# changed bytes\n')
        calls=[0]
        def wait(timeout):
            self.waits.append(timeout)
            calls[0]+=1
            if self.mode=='timeout' and calls[0]==1:raise subprocess.TimeoutExpired(command,timeout)
            return 1 if self.mode=='nonzero' else 0
        return SimpleNamespace(pid=999999999,poll=lambda:None,wait=wait,
            terminate=lambda:self.terminated.append('terminate'),kill=lambda:self.terminated.append('kill'))

    def invoke(self):
        return self.scope['census_qualification_bridge'](self.schedule,self.qualification,5000,process_factory=self.factory,wall=lambda:1000)

    def test_success_routes_only_metadata_to_separate_4f_environment(self):
        result=self.invoke();self.assertEqual(len(self.calls),1);command,options=self.calls[0]
        self.assertEqual(command[:3],[sys.executable,'-s','-u']);self.assertEqual(Path(command[3]).name,CHILD.name)
        self.assertEqual(options['env']['PYTHONPATH'],str(self.root/'releases/20261021-6ed201653dfa-4f3903d7c26e/src'))
        self.assertTrue(options['start_new_session']);self.assertTrue(options['close_fds'])
        self.assertEqual(result['checked_paths'],{str(self.bound):sha(self.bound)})
        self.assertIs(result['separate_4f_bridge']['artifacts_deserialized'],False)

    def test_parent_rejects_successful_child_reporting_8495_package(self):
        self.mode='wrong_package'
        with self.assertRaisesRegex(ValueError,'origin'):self.invoke()

    def test_nonzero_child_never_promotes_a_valid_response(self):
        self.mode='nonzero'
        with self.assertRaisesRegex(ValueError,'validator failed'):self.invoke()

    def test_timeout_uses_finite_teardown_and_never_retries(self):
        self.mode='timeout'
        original_stop=self.scope['stop'];signals=[]
        with patch.dict(self.scope,stop=lambda process:original_stop(process,group_signal=lambda pid,sig:signals.append(sig))):
            with self.assertRaises(subprocess.TimeoutExpired):self.invoke()
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.waits,[300.,10.]);self.assertEqual(signals,[signal.SIGTERM])
        record=json.loads((self.support/'census-provider-verification/process-result.json').read_bytes())
        self.assertEqual(record['status'],'metadata_bridge_failed_no_retry');self.assertTrue(record['termination']['termination_confirmed'])

    def test_duplicate_response_keys_are_rejected(self):
        self.mode='duplicate'
        with self.assertRaisesRegex(ValueError,'Duplicate'):self.invoke()

    def test_nonfinite_response_is_rejected(self):
        self.mode='nonfinite'
        with self.assertRaises(ValueError):self.invoke()

    def test_changed_request_after_child_is_rejected(self):
        self.mode='request_changed'
        with self.assertRaisesRegex(ValueError,'request changed'):self.invoke()

    def test_changed_child_source_after_completion_is_rejected(self):
        self.mode='source_changed'
        with self.assertRaisesRegex(ValueError,'source or bridge'):self.invoke()

    def test_exhausted_cleanup_margin_never_starts_child(self):
        with self.assertRaisesRegex(ValueError,'margin exhausted'):
            self.scope['census_qualification_bridge'](self.schedule,self.qualification,1600,process_factory=self.factory,wall=lambda:1000)
        self.assertEqual(self.calls,[])

    def test_actual_package_file_origin_must_be_4f(self):
        module=child_module();package=ModuleType('horsehair');framework=self.root/'4f';primary=self.root/'primary'
        package.__file__=str(framework/'src/horsehair/__init__.py')
        Path(package.__file__).parent.mkdir(parents=True);Path(package.__file__).write_text('# inert package-origin fixture\n')
        identities=lambda path:{'sha256':module.SOURCE if path==framework else module.PRIMARY_SHA}
        with patch.multiple(module,FRAMEWORK=framework,PRIMARY=primary,CONFIG=hashlib.sha256(b'{}').hexdigest()):
            proof=module.package_identity(package,identities,lambda path:{})
            self.assertEqual(proof['package_path'],package.__file__)
            package.__file__=str(module.ROOT/'releases/20261021-caebc9c1dc77-8495a3d98551/src/horsehair/__init__.py')
            with self.assertRaisesRegex(ValueError,'package origin'):module.package_identity(package,identities,lambda path:{})

    def test_real_source_and_configuration_identity_mismatches_refuse(self):
        module=child_module();package=ModuleType('horsehair');framework=self.root/'4f';primary=self.root/'primary'
        package.__file__=str(framework/'src/horsehair/__init__.py')
        with patch.multiple(module,FRAMEWORK=framework,PRIMARY=primary):
            for identities,config in [(lambda path:{'sha256':'f'*64},{}),
                    (lambda path:{'sha256':module.SOURCE if path==framework else module.PRIMARY_SHA},{'changed':True})]:
                with self.subTest(config=config):
                    with self.assertRaises(ValueError):module.package_identity(package,identities,lambda path:config)

    def test_child_request_binds_exact_helpers_and_parent_closure(self):
        module=child_module();context=self.capsule/'context.py';context.write_text('# fixture\n')
        manifest={'files':{CHILD.name:sha(self.bridge),context.name:sha(context)}};self.source.write_text(json.dumps(manifest))
        schedule={'plan':{},'ledger':{},'helper':{'path':str(self.bound),'sha256':sha(self.bound)},
            'source_manifest':{'fixture':'provider-source'},'qualification':self.qualification,'case_root':'fixture'}
        value={'schema_version':1,'schedule':schedule,'qualification':self.qualification,
            'source_manifest':{'path':str(self.source),'sha256':sha(self.source)},
            'context_helper':{'path':str(context),'sha256':sha(context)},'provider_helper':schedule['helper']}
        with patch.multiple(module,ROOT=self.root,CAPSULE=self.capsule,CONTEXT=context,CONTEXT_SHA=sha(context),
                PROVIDER=self.bound,PROVIDER_SHA=sha(self.bound),PROVIDER_MANIFEST=schedule['source_manifest']):
            self.assertEqual(module.validate_request(value,sha(self.bridge)),manifest)
            for change in ('schema','helper','closure'):
                changed=copy.deepcopy(value)
                if change=='schema':changed['extra']=False
                elif change=='helper':changed['context_helper']['sha256']='f'*64
                else:changed['source_manifest']['sha256']='f'*64
                with self.subTest(change=change):
                    with self.assertRaises(ValueError):module.validate_request(changed,sha(self.bridge))

    def test_child_installs_bound_reader_before_original_validator(self):
        module=child_module();events=[];provider=self.bound;context=self.source
        proof={'checked_paths':{str(self.bound):sha(self.bound)},'numerical_work_executed':False,
            'canonical_state_changed':False,'scientific_result':False,'admission_authority':False}
        repair=ModuleType('horsehair.repair');repair._repair_native_states=object()
        package=ModuleType('horsehair');package.repair=repair
        core=ModuleType('horsehair.core');core.source_identity=lambda path:None;core.read_json=lambda path:None
        reader=object();ctx=SimpleNamespace(GUARD=self.bound,GUARD_SHA=sha(self.bound),
            bounded_native_reader=lambda *args:events.append('bounded_reader') or reader)
        def validate(schedule,qualification,native_reader):
            self.assertIs(native_reader,reader);events.append('original_validator');return proof
        producer=SimpleNamespace(validate_schedule_binding=validate)
        def loader(name,path,pin):return ctx if 'context' in name else producer
        with (patch.multiple(module,ROOT=self.root,CONTEXT=context,PROVIDER=provider,
                validate_request=lambda *args:{'unchanged':True},load=loader,
                package_identity=lambda *args:events.append('actual_package_identity') or {'origin':'4f'}),
                patch.dict(sys.modules,{'horsehair':package,'horsehair.core':core,'horsehair.repair':repair}),
                patch.object(module.runpy,'run_path',return_value={'install':lambda path:events.append('qualified_guard')}),
                patch.object(module.time,'time',return_value=1000)):
            result=module.verify({'schedule':self.schedule,'qualification':self.qualification,
                'provider_helper':self.schedule['helper'],'source_manifest':{'fixture':True}},'f'*64,5000,self.support)
        self.assertEqual(events,['actual_package_identity','qualified_guard','bounded_reader','original_validator','actual_package_identity'])
        self.assertIs(result['numerical_work_executed'],False)
        for flag in self.flags:self.assertIs(result.get(flag),False,flag)

    def test_child_does_not_import_numerics_or_swap_parent_modules(self):
        tree=ast.parse(CHILD.read_bytes());text=ast.unparse(tree)
        self.assertNotIn('import torch',text);self.assertNotIn('sys.path',text);self.assertNotIn('sys.modules',text)
        self.assertNotIn('resource_lease(',text);self.assertNotIn('storage_snapshot(',text)
        self.assertIn('producer.validate_schedule_binding',text);self.assertIn('source_identity(FRAMEWORK)',text)

    def test_native_capsule_declares_new_bridge_and_exact_test_roster(self):
        manifest=runpy.run_path(str(AREA/'seal-source.py'))['source_manifest']()
        self.assertIn(CHILD.name,manifest['files']);self.assertIn(Path(__file__).name,manifest['files'])
        suite=next(item for item in manifest['operational_test_suites'] if item['path']==Path(__file__).name)
        self.assertEqual(suite['tests'],15);self.assertEqual(manifest['complete_native_qualification_tests'],179)


if __name__=='__main__':
    output=io.StringIO();result=unittest.TextTestRunner(stream=output,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    destination=os.environ.get('HHW_TEST_RESULT_DIR')
    if destination:
        directory=Path(destination).resolve(strict=True)
        with (directory/'test-output.txt').open('x',encoding='utf8',newline='\n') as stream:stream.write(output.getvalue())
        with (directory/'test-result.json').open('x',encoding='utf8',newline='\n') as stream:
            json.dump(dict(tests=result.testsRun,failures=len(result.failures),errors=len(result.errors),skipped=len(result.skipped),
                successful=result.wasSuccessful(),tests_sha256=sha(Path(__file__)),derivative_sha256=sha(CANDIDATE)),stream,sort_keys=True,indent=2);stream.write('\n')
    sys.stdout.write(output.getvalue());raise SystemExit(0 if result.wasSuccessful() else 1)
