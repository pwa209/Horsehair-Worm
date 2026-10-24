"""Additional local review tests; separate from the unchanged 25 native contracts."""
from pathlib import Path
from types import SimpleNamespace
import ast
import hashlib
import json
import tempfile
import unittest

AREA=Path(__file__).resolve().parent
SOURCE=AREA/'build-pending-f2-handoff-plan-v2-20261024.py'


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def require(value,message):
    if not value:raise ValueError(message)


class PreflightReview(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(dir=AREA);self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name).resolve();self.support=self.root/'backfill';self.support.mkdir()
        self.outer_name='run-af-agent-controller-backfill-policy-v2-20261024.py'
        self.files={}
        for name in [self.outer_name]+['source-'+str(i)+'.py' for i in range(15)]:
            path=self.support/name;path.write_bytes(b'# inert pinned fixture\n');self.files[name]=digest(path)
        self.policy=self.root/'provider-policy.json';self.policy.write_text('{}')
        self.report=self.root/'complete-report.json';self.report.write_text('{"slurm_job_id":"new-v2-job","scope":"complete real-report locator fixture"}')
        self.package={'kind':'f2-backfill-controller-transport-source-manifest-v2','native_support':str(self.support),
            'native_framework':str(self.root/'framework'),'files':self.files}
        self.calls=[];self.refuse=False
        def provider(policy):
            self.calls.append(('provider',policy));return {'provider-path':'provider-pin'}
        def consumer(policy,*,provider_paths):
            self.calls.append(('actual_complete_consumer',policy,provider_paths))
            if self.refuse:raise ValueError('Actual producer evidence refused')
            return {str(self.report):digest(self.report)},{'completion_proof_sha256':'native-completion-pin'}
        module=SimpleNamespace(qualification=provider,backfill_qualification=consumer)
        importer=SimpleNamespace(spec_from_file_location=lambda *args:SimpleNamespace(loader=SimpleNamespace(exec_module=lambda module:None)),
            module_from_spec=lambda spec:module)
        scope=dict(Path=Path,digest=digest,canonical_hash=canonical,require=require,
            read_json=lambda path:json.loads(Path(path).read_text()),importlib=SimpleNamespace(util=importer),
            BACKFILL_SUPPORT=self.support,FRAMEWORK=self.root/'framework',OUTER=self.outer_name,
            OLD_POLICY=self.policy,OLD_POLICY_SHA=digest(self.policy))
        function=next(n for n in ast.parse(SOURCE.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='qualification_preflight')
        exec(compile(ast.Module(body=[function],type_ignores=[]),str(SOURCE),'exec'),scope)
        self.preflight=scope['qualification_preflight']

    def test_full_actual_consumer_receives_exact_complete_report_and_provider_paths(self):
        result=self.preflight(self.report,self.package)
        self.assertEqual([x[0] for x in self.calls],['provider','actual_complete_consumer'])
        call=self.calls[1]
        self.assertEqual(call[1],{'backfill_qualification':{'path':str(self.report),'sha256':digest(self.report),'job_id':'new-v2-job'}})
        self.assertEqual(call[2],{'provider-path':'provider-pin'})
        self.assertEqual(result['native_completion_proof_sha256'],'native-completion-pin')
        self.assertIs(result['own_handoff_plan_created'],False)

    def test_actual_consumer_failure_propagates_without_creating_any_handoff_plan(self):
        self.refuse=True
        before=sorted(str(p.relative_to(self.root)) for p in self.root.rglob('*'))
        with self.assertRaises(ValueError):self.preflight(self.report,self.package)
        self.assertEqual(before,sorted(str(p.relative_to(self.root)) for p in self.root.rglob('*')))

    def test_freeze_calls_production_preflight_before_directory_creation(self):
        function=next(n for n in ast.parse(SOURCE.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='freeze_plan')
        calls=[n for n in ast.walk(function) if isinstance(n,ast.Call)]
        preflight=next(n for n in calls if isinstance(n.func,ast.Name) and n.func.id=='qualification_preflight')
        mutations=[n for n in calls if isinstance(n.func,ast.Attribute) and n.func.attr in {'mkdir','open'}]
        self.assertTrue(mutations)
        self.assertLess(preflight.lineno,min(n.lineno for n in mutations))

    def test_changed_v2_capsule_refuses_before_any_consumer_import_or_call(self):
        (self.support/'source-0.py').write_bytes(b'# changed\n')
        with self.assertRaises(ValueError):self.preflight(self.report,self.package)
        self.assertEqual(self.calls,[])


if __name__=='__main__':unittest.main(verbosity=2)
