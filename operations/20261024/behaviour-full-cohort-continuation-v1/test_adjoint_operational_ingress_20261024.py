"""Synthetic metadata and inert artifacts only; no numerical or remote work."""
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch
import ast
import copy
import hashlib
import io
import json
import math
import os
import re
import sys
import tempfile
import time
import unittest

AREA=Path(__file__).resolve().parent
CANDIDATE=AREA/'run-full-cohort-continuation-v1-20261024.py'
PREFIX_STAGES=('full_fold_preprocessing_and_eligibility_census','full_training_objective_and_all_gradients',
    'single_original_clip_and_Adam_update','all_original_validation_calls_with_explicit_nulls')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n')


def load_ingress():
    """Compile only generic JSON/file ingress, never import the study package."""
    constants={'ROOT','SOURCE','PRIMARY_SHA','CONFIG','PREFIX_HELPER_SHA','PREFIX_CASE','PREFIX_SUPPORT',
        'PREFIX_STAGE','FALSE_FLAGS','ORIGINAL_JOB_ID','EXPECTED_MANIFEST_SHA','EXPECTED_CANONICAL_SHA',
        'VERIFIER_SOURCE','VERIFIER_OUTPUT','VERIFIER_SOURCE_MANIFEST_SHA'}
    functions={'require','digest','ordinary','bound_json','registered_json','original_file_bindings','verifier_registration','prefix_evidence','verify_capsule'}
    nodes=[]
    for node in ast.parse(CANDIDATE.read_bytes()).body:
        if isinstance(node,ast.Assign) and isinstance(node.targets[0],ast.Name) and node.targets[0].id in constants:
            nodes.append(node)
        elif isinstance(node,ast.FunctionDef) and node.name in functions:
            nodes.append(node)
    scope=dict(Path=Path,json=json,hashlib=hashlib,re=re)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(CANDIDATE),'exec'),scope)
    return scope


class IngressTests(unittest.TestCase):
    def setUp(self):
        workspace=Path(os.environ.get('HHW_TEST_WORKSPACE',str(AREA/'ingress-test-work')))
        workspace.mkdir(parents=True,exist_ok=True)
        self.temporary=tempfile.TemporaryDirectory(prefix='case-',dir=workspace.resolve())
        self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name).resolve()
        self.scope=load_ingress()
        self.case=self.root/'maintenance/original-case'
        self.support=self.root/'maintenance/original-support'
        self.source=self.root/'maintenance/continuation-source'
        self.verifier_source=self.root/'maintenance/verifier-source'
        self.verifier_output=self.root/'maintenance/verifier-output'
        self.case.mkdir(parents=True);self.support.mkdir();self.source.mkdir();self.verifier_source.mkdir()
        self.scope.update(ROOT=self.root,PREFIX_CASE=self.case,PREFIX_SUPPORT=self.support,
            VERIFIER_SOURCE=self.verifier_source,VERIFIER_OUTPUT=self.verifier_output,
            __file__=str(self.source/'candidate.py'))
        self.flags=self.scope['FALSE_FLAGS']
        self.producer={'job_id':'6865674','state':'FAILED','exit_code':'1:0','elapsed_seconds':18500,'cpus':1,
            'owner':'pwa209','account':'def-ptewarie_cpu','start':'2026-10-23T11:07:41','end':'2026-10-23T16:16:01'}
        self.helper=self.root/'maintenance/original-source/prefix.py'
        self.helper.parent.mkdir();self.helper.write_text('# Inert fixture. Never imported.\n')
        self.scope['PREFIX_HELPER_SHA']=sha(self.helper)
        self.census=self.case/'clock-eligibility-census.json'
        write(self.census,{'all_original_roster_rows_retained':True,'rows':[]})
        self.plan=self.case/'plan.json'
        write(self.plan,{'census_sha256':sha(self.census),'private_helper_sha256':sha(self.helper),
            'primary_source_sha256':self.scope['PRIMARY_SHA'],'framework_source_sha256':self.scope['SOURCE'],
            'full_config_sha256':self.scope['CONFIG']})
        stages=[]
        for name in PREFIX_STAGES:
            artifact=self.case/(name+'.pt');artifact.write_bytes(('opaque fixture:'+name).encode())
            stage=dict(stage=name,status='operator_prefix_stage_complete',artifacts_path=str(artifact),
                artifacts_sha256=sha(artifact),plan_sha256=sha(self.plan),census_sha256=sha(self.census),**self.flags)
            write(self.case/(name+'.json'),stage);stages.append(stage)
        self.worker=self.case/'worker-result.json'
        write(self.worker,dict(status='complete_full_cohort_first_update_technical_prefix',torch_version='synthetic-runtime',
            completed_stages=stages,primary_source_sha256=self.scope['PRIMARY_SHA'],framework_source_sha256=self.scope['SOURCE'],
            full_config_sha256=self.scope['CONFIG'],**self.flags))
        write(self.case/'receipt.json',dict(status='worker_failed',**self.flags))
        for name in ['private-tests.xml','context-private-tests.xml']:
            (self.support/name).write_text('<testsuite/>\n')
        paths=[self.helper,self.plan,self.worker,self.case/'receipt.json',*sorted(self.case.glob('*.json')),
               self.support/'private-tests.xml',self.support/'context-private-tests.xml']
        self.expected={'original_job_id':'6865674','original_native':self.producer,
            'helper_path':str(self.helper.relative_to(self.root)),
            'pinned_files':{str(p.relative_to(self.root)):sha(p) for p in paths},
            'artifact_pins':{str(p):sha(p) for p in sorted(self.case.glob('*.pt'))}}
        self.expected_path=self.source/'expected-original-manifest.json';write(self.expected_path,self.expected)
        self.scope.update(EXPECTED_MANIFEST_SHA=sha(self.expected_path),EXPECTED_CANONICAL_SHA=canonical(self.expected))
        write(self.verifier_source/'expected-original-manifest.json',self.expected)
        write(self.verifier_source/'source-manifest.json',{'files':{'expected-original-manifest.json':sha(self.expected_path)}})
        self.scope['VERIFIER_SOURCE_MANIFEST_SHA']=sha(self.verifier_source/'source-manifest.json')
        all_files=dict(self.expected['pinned_files'])
        all_files.update({str(Path(name).relative_to(self.root)):pin for name,pin in self.expected['artifact_pins'].items()})
        self.certificate=dict(status='technical_prefix_independently_verified',native_job_id='999',original_job_id='6865674',
            original_native=copy.deepcopy(self.producer),expected_manifest_sha256=canonical(self.expected),
            original_helper_sha256=sha(self.helper),primary_source_sha256=self.scope['PRIMARY_SHA'],
            framework_source_sha256=self.scope['SOURCE'],full_config_sha256=self.scope['CONFIG'],
            original_receipt_status='worker_failed',worker_result_status='complete_full_cohort_first_update_technical_prefix',
            consumer_requires_verifier_native_completed_zero=True,canonical_state_changed=False,numerical_worker_replayed=False,
            artifacts_deserialized=False,original_native_history_rewritten=False,completed_stages=list(PREFIX_STAGES),
            source_manifest_sha256=self.scope['VERIFIER_SOURCE_MANIFEST_SHA'],
            evidence_files={name:{'sha256':pin,'snapshot':{'size':(self.root/name).stat().st_size}} for name,pin in all_files.items()},
            **self.flags)
        self.receipt=self.verifier_output/'native-999/receipt.json';write(self.receipt,self.certificate)
        self.native_plan=self.root/'plans/behaviour-reconciliation-v1-20261024/plan.json'
        self.native_script=self.native_plan.with_name('verifier.sbatch')
        self.native_script.parent.mkdir(parents=True)
        self.native_script.write_text('python '+str(self.verifier_source/'run_native_reconciliation.py')+
            ' --source-manifest-sha256 '+self.scope['VERIFIER_SOURCE_MANIFEST_SHA']+'\n')
        self.plan_value={'config_sha256':self.scope['CONFIG'],'release_identity':{'sha256':self.scope['SOURCE']},
            'tasks':[{'name':'behaviour-reconciliation-v1','script':str(self.native_script),
                      'script_sha256':canonical(self.native_script.read_text())}]}
        write(self.native_plan,self.plan_value)
        self.native_ledger=self.root/'state/behaviour-reconciliation-v1-20261024-submission.json'
        self.ledger_value={'source_sha256':self.scope['SOURCE'],'config_sha256':self.scope['CONFIG'],
            'plan_sha256':canonical(self.plan_value),'tasks':{'behaviour-reconciliation-v1':{'job_id':'999',
                'state':'submitted','script_sha256':self.plan_value['tasks'][0]['script_sha256']}}}
        write(self.native_ledger,self.ledger_value)
        self.started=self.receipt.with_name('started.json')
        self.started_value={'native_job_id':'999','source_manifest_sha256':self.scope['VERIFIER_SOURCE_MANIFEST_SHA'],
            'allocation':{'plan_sha256':sha(self.native_plan),'ledger_sha256':sha(self.native_ledger),
                'fields':{'JobId':'999','JobState':'RUNNING','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu',
                    'NumCPUs':'4','MinMemoryNode':'16G','TimeLimit':'02:00:00','Command':str(self.native_script)}}}
        write(self.started,self.started_value)
        self.observed={
            '999':{'state':'COMPLETED','exit_code':'0:0','user':'pwa209','account':'def-ptewarie_cpu','job_id_raw':'999',
                'unallocated_metadata':False,'cpus':4,'elapsed_seconds':100,'alloc_tres':'cpu=4,mem=16G,node=1'},
            '6865674':{'state':'FAILED','exit_code':'1:0','user':'pwa209','account':'def-ptewarie_cpu','job_id_raw':'6865674',
                'unallocated_metadata':False,'cpus':1,'elapsed_seconds':18500,'alloc_tres':'cpu=1,mem=64G,node=1'}}
        self.loaded=[]
        self.junit=[]
        def validate(worker,case):
            if worker['status']!='complete_full_cohort_first_update_technical_prefix' or [x['stage'] for x in worker['completed_stages']]!=list(PREFIX_STAGES):
                raise ValueError('Synthetic strict generic stage validator')
        fake=SimpleNamespace(verify_private_junit=lambda path,**kwargs:self.junit.append((path,kwargs['suite'])),validate_worker_result=validate)
        def load(*args):
            self.loaded.append(args)
            return fake
        self.scope['load_exact']=load
        package=ModuleType('horsehair');package.__path__=[]
        slurm=ModuleType('horsehair.slurm');slurm.completed_native_dependency=lambda root,job:{'job_id':job,'persisted':True}
        self.slurm=slurm
        module_patch=patch.dict(sys.modules,{'horsehair':package,'horsehair.slurm':slurm})
        module_patch.start();self.addCleanup(module_patch.stop)

    def proof(self, *, save=True):
        if save:write(self.receipt,self.certificate)
        return self.scope['prefix_evidence'](self.receipt,sha(self.receipt),'999',self.observed)

    def test_positive_independent_completion_preserves_failure_and_all_false_gates(self):
        result=self.proof()
        self.assertEqual(result['original_native_outcome_preserved']['state'],'FAILED')
        self.assertEqual(result['original_job_id'],'6865674')
        self.assertEqual(result['helper']['sha256'],sha(self.helper))
        self.assertIs(result['original_stage1_artifact']['loaded'],False)
        self.assertEqual(len(self.junit),2)
        self.assertTrue(all(result[k] is False for k in self.flags))
        self.assertTrue(result['all_original_v5_stages_independently_verified'])

    def test_missing_persisted_verifier_completion_refuses(self):
        self.slurm.completed_native_dependency=lambda *args:None
        with self.assertRaises(ValueError):self.proof()

    def test_each_false_authority_and_preservation_flag_is_required(self):
        for name in [*self.flags,'canonical_state_changed','numerical_worker_replayed','artifacts_deserialized','original_native_history_rewritten']:
            with self.subTest(name=name):
                self.certificate[name]=True
                with self.assertRaises(ValueError):self.proof()
                self.certificate[name]=False

    def test_verifier_not_positive_native_record_refuses(self):
        for field,value in [('state','RUNNING'),('exit_code','1:0'),('user','other'),('account','other'),('job_id_raw','999.batch'),
                ('unallocated_metadata',True),('cpus',1),('cpus',True),('elapsed_seconds',0),('elapsed_seconds',7201),('alloc_tres','cpu=4,gres/gpu=1')]:
            with self.subTest(field=field,value=value):
                old=self.observed['999'][field];self.observed['999'][field]=value
                with self.assertRaises(ValueError):self.proof()
                self.observed['999'][field]=old

    def test_original_failure_cannot_be_relabelled(self):
        for field,value in [('state','COMPLETED'),('exit_code','0:0'),('job_id_raw','1'),('user','other'),('account','other'),
                ('cpus',2),('elapsed_seconds',18000),('unallocated_metadata',True),('alloc_tres','gres/gpu=1')]:
            with self.subTest(field=field):
                old=self.observed['6865674'][field];self.observed['6865674'][field]=value
                with self.assertRaises(ValueError):self.proof()
                self.observed['6865674'][field]=old

    def test_certificate_identity_status_and_source_drift_refuse(self):
        for field,value in [('status','complete'),('native_job_id','1'),('original_job_id','1'),('expected_manifest_sha256','f'*64),
                ('original_helper_sha256','f'*64),('primary_source_sha256','f'*64),('framework_source_sha256','f'*64),
                ('full_config_sha256','f'*64),('original_receipt_status','complete'),('worker_result_status','pending'),
                ('consumer_requires_verifier_native_completed_zero',False),('source_manifest_sha256','f'*64)]:
            with self.subTest(field=field):
                old=self.certificate[field];self.certificate[field]=value
                with self.assertRaises(ValueError):self.proof()
                self.certificate[field]=old

    def test_independent_job_and_exact_output_namespace_required(self):
        for path,job in [(self.receipt,'6865674'),(self.receipt,'999.batch'),(self.receipt.parent/'other.json','999')]:
            with self.subTest(job=job,path=str(path)):
                with self.assertRaises(ValueError):self.scope['prefix_evidence'](path,sha(self.receipt),job,self.observed)

    def test_changed_original_pinned_source_refuses_before_module_load(self):
        self.helper.write_text('# changed\n')
        with self.assertRaises(ValueError):self.proof()
        self.assertEqual(self.loaded,[])

    def test_changed_original_opaque_artifact_refuses_without_deserialization(self):
        (self.case/(PREFIX_STAGES[0]+'.pt')).write_bytes(b'changed')
        with self.assertRaises(ValueError):self.proof()

    def test_missing_or_changed_independent_artifact_hash_refuses(self):
        name=str((self.case/(PREFIX_STAGES[0]+'.pt')).relative_to(self.root))
        old=self.certificate['evidence_files'].pop(name)
        with self.assertRaises(ValueError):self.proof()
        self.certificate['evidence_files'][name]=dict(old,sha256='f'*64)
        with self.assertRaises(ValueError):self.proof()

    def test_changed_manifest_refuses(self):
        self.expected_path.write_text('{}')
        with self.assertRaises(ValueError):self.proof()

    def test_changed_verifier_bundle_bytes_refuse(self):
        (self.verifier_source/'expected-original-manifest.json').write_text('{}')
        with self.assertRaises(ValueError):self.proof()

    def test_changed_verifier_manifest_refuses(self):
        (self.verifier_source/'source-manifest.json').write_text('{}')
        with self.assertRaises(ValueError):self.proof()

    def test_certificate_stages_cannot_be_partial_or_reordered(self):
        for stages in [list(PREFIX_STAGES[:-1]),list(reversed(PREFIX_STAGES))]:
            self.certificate['completed_stages']=stages
            with self.assertRaises(ValueError):self.proof()

    def test_changed_census_refuses(self):
        write(self.census,{'all_original_roster_rows_retained':False})
        with self.assertRaises(ValueError):self.proof()

    def test_changed_original_private_junit_refuses(self):
        (self.support/'private-tests.xml').write_text('<testsuite tests="1"/>')
        with self.assertRaises(ValueError):self.proof()

    def test_certificate_duplicate_keys_and_nonfinite_values_refuse(self):
        for raw in ['{"status":"one","status":"two"}','{"status":NaN}']:
            self.receipt.write_text(raw)
            with self.assertRaises(ValueError):self.proof(save=False)

    def test_receipt_digest_is_mandatory(self):
        with self.assertRaises(ValueError):self.scope['prefix_evidence'](self.receipt,'f'*64,'999',self.observed)

    def test_native_verifier_source_pin_is_mandatory(self):
        self.scope['VERIFIER_SOURCE_MANIFEST_SHA']=None
        with self.assertRaises(ValueError):self.proof()

    def test_positive_native_job_must_bind_actual_verifier_registration_and_source_script(self):
        original=self.native_script.read_text()
        self.native_script.write_text(original.replace(self.scope['VERIFIER_SOURCE_MANIFEST_SHA'],'f'*64))
        with self.assertRaises(ValueError):self.proof()
        self.native_script.write_text(original)
        for field,value in [('JobId','1000'),('UserId','other(1)'),('Command','/other/script'),('NumCPUs','1'),
                ('MinMemoryNode','64G'),('TimeLimit','08:00:00')]:
            with self.subTest(field=field):
                old=self.started_value['allocation']['fields'][field]
                self.started_value['allocation']['fields'][field]=value;write(self.started,self.started_value)
                with self.assertRaises(ValueError):self.proof()
                self.started_value['allocation']['fields'][field]=old
        write(self.started,self.started_value)
        self.ledger_value['tasks']['behaviour-reconciliation-v1']['job_id']='1000';write(self.native_ledger,self.ledger_value)
        with self.assertRaises(ValueError):self.proof()

    def test_preserved_v3_v5_preprocessing_ast_qualification(self):
        # Read and compare source only. No module or numerical function runs.
        old=AREA/'run-full-cohort-prefix-v3-20261021.py'
        real_manifest=json.loads((AREA/'expected-original-manifest.json').read_text())
        original_v5=Path(os.environ.get('HHW_ADJOINT_ORIGINAL_PREFIX',str(
            AREA.parents[2]/'work/repair-20261024-509-evidence'/real_manifest['helper_path'])))
        # Native qualification points to the pinned existing helper explicitly.
        if os.environ.get('SLURM_JOB_ID'):
            original_v5=Path(real_manifest['root'])/real_manifest['helper_path']
        self.assertEqual(sha(original_v5),real_manifest['helper_sha256'])
        def defs(path):
            return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_bytes()).body if isinstance(n,ast.FunctionDef)}
        before,after=defs(old),defs(original_v5)
        for name in ['prepare','clock_census','prospective_validation_canary_v2','source_bindings',
                'validation_call_or_null','validation_summary','run_prefix']:
            self.assertEqual(before[name],after[name],name)


class PlanTests(unittest.TestCase):
    def setUp(self):
        source=AREA/'build-native-plan.py'
        tree=ast.parse(source.read_bytes())
        nodes=[n for n in tree.body if isinstance(n,ast.Assign) or isinstance(n,ast.FunctionDef) and n.name in {'require','canonical','render'}]
        import shlex
        self.scope=dict(Path=Path,json=json,hashlib=hashlib,re=re,shlex=shlex)
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(source),'exec'),self.scope)
        self.config={'synthetic':'configuration binding only'}
        self.scope['CONFIG']=canonical(self.config)
        self.proofs=[{'job_id':'999','dependency_kind':'afterok'},{'job_id':'6689965','dependency_kind':'afterok'},{'job_id':'6943757','dependency_kind':'afterok'}]
        self.calls=[]

    def render(self,**changed):
        args=dict(config=self.config,release_identity={'sha256':self.scope['SOURCE']},bundle_sha='a'*64,
            verifier_job='999',receipt_sha='b'*64,verifier_bundle_sha='c'*64,proofs=self.proofs,
            script_renderer=lambda *args,**kwargs:self.calls.append((args,kwargs)) or '# synthetic inert script\n')
        args.update(changed)
        return self.scope['render'](**args)

    def test_exact_single_finite_cpu_plan_and_dependency_closure(self):
        plan,script=self.render()
        self.assertEqual(script,'# synthetic inert script\n')
        self.assertEqual(len(plan['tasks']),1)
        task=plan['tasks'][0]
        self.assertEqual(task['resources'],{'cpus':1,'memory':'64G','hours':24,'gpu':False,'array':None})
        self.assertEqual(task['completed_external_dependencies'],self.proofs)
        self.assertEqual(task['script_sha256'],canonical(script))
        self.assertEqual(self.calls[0][1],{'cpus':1,'memory':'64G','hours':24,'gpu':False,'array':None})

    def test_plan_binds_both_source_bundles_receipt_and_exact_job(self):
        plan,_=self.render();command=plan['tasks'][0]['command'].replace('\\','/')
        for value in ['--source-manifest-sha256 '+'a'*64,'--verifier-source-manifest-sha256 '+'c'*64,
                '--prefix-receipt-sha256 '+'b'*64,'--prefix-job-id 999','native-999/receipt.json']:
            self.assertIn(value,command)
        self.assertEqual(plan['original_native_failure_preserved'],'6865674 FAILED/1:0')
        for name in ['scientific_result','complete_declared_workload','admission_authority']:
            self.assertIs(plan[name],False);self.assertIs(plan['tasks'][0][name],False)

    def test_wrong_source_or_configuration_refuses_before_rendering(self):
        for change in [dict(config={}),dict(release_identity={'sha256':'f'*64})]:
            with self.assertRaises(ValueError):self.render(**change)
        self.assertEqual(self.calls,[])

    def test_original_or_invalid_verifier_identity_refuses(self):
        for job in ['6865674','0','999.batch','999;other','../999']:
            with self.subTest(job=job):
                with self.assertRaises(ValueError):self.render(verifier_job=job)
        self.assertEqual(self.calls,[])

    def test_missing_nonpositive_duplicate_or_wrong_dependency_refuses(self):
        for proofs in [[],self.proofs[:1],self.proofs+self.proofs[:1],
                [dict(self.proofs[0],dependency_kind='afterany'),self.proofs[1]],
                [dict(self.proofs[0],job_id='6865674'),self.proofs[1]]]:
            with self.assertRaises(ValueError):self.render(proofs=proofs)
        self.assertEqual(self.calls,[])

    def test_unpinned_bundle_or_receipt_refuses(self):
        for field in ['bundle_sha','receipt_sha','verifier_bundle_sha']:
            with self.assertRaises(ValueError):self.render(**{field:'not a SHA256'})
        self.assertEqual(self.calls,[])


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        nodes=[n for n in ast.parse(CANDIDATE.read_bytes()).body if isinstance(n,ast.FunctionDef)
            and n.name in {'require','adjoint_runtime_seconds'}]
        scope=dict(time=time,math=math)
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(CANDIDATE),'exec'),scope)
        self.bound=scope['adjoint_runtime_seconds']

    def test_full_twenty_four_hour_allocation_retains_85800_second_worker_ceiling(self):
        self.assertEqual(self.bound(86400,now=0),85800)

    def test_finalization_reserve_is_retained_after_startup(self):
        self.assertEqual(self.bound(86400,now=1000),84800)
        self.assertEqual(self.bound(630,now=0),30)

    def test_invalid_unbounded_short_or_changed_native_clock_refuses(self):
        for end,now in [(float('inf'),0),(float('nan'),0),(True,0),(86400,True),(86403,0),(629,0)]:
            with self.subTest(end=end,now=now):
                with self.assertRaises(ValueError):self.bound(end,now=now)

    def test_main_uses_twenty_four_hour_lease_context_and_own_bound(self):
        tree=ast.parse(CANDIDATE.read_bytes())
        main=ast.unparse(next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main'))
        self.assertIn('adjoint_runtime_seconds(',main)
        self.assertNotIn('runtime_limit',main)
        self.assertIn('max_hours=24',main)
        self.assertIn('hours=24',main)
        self.assertIn("HHW_MAX_HOURS') == '24'",main)

    def test_bounded_native_reader_is_installed_before_campaign_binding(self):
        tree=ast.parse(CANDIDATE.read_bytes())
        main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
        statements=[(n.lineno,ast.unparse(n),n) for n in main.body]
        install=next(line for line,text,node in statements if 'accounting-reads' in text)
        allocation=next(line for line,text,node in statements if isinstance(node,ast.Assign)
            and any(isinstance(t,ast.Name) and t.id=='allocation' for t in node.targets))
        repair_import=next(line for line,text,node in statements if isinstance(node,ast.ImportFrom)
            and node.module=='horsehair' and any(alias.name=='repair' for alias in node.names))
        bound=next(line for line,text,node in statements if isinstance(node,ast.Assign)
            and isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Attribute)
            and node.value.func.attr=='bounded_native_reader')
        campaign_import=next(line for line,text,node in statements if isinstance(node,ast.ImportFrom)
            and node.module=='horsehair' and any(alias.name=='campaign' for alias in node.names))
        identity=next(line for line,text,node in statements if 'Bounded accounting must precede campaign import' in text)
        self.assertEqual([install,allocation,repair_import,bound,campaign_import,identity],
            sorted([install,allocation,repair_import,bound,campaign_import,identity]))
        self.assertEqual(len({install,allocation,repair_import,bound,campaign_import,identity}),6)


class QualificationTests(unittest.TestCase):
    def setUp(self):
        workspace=Path(os.environ.get('HHW_TEST_WORKSPACE',str(AREA/'qualification-test-work')))
        workspace.mkdir(parents=True,exist_ok=True)
        self.temporary=tempfile.TemporaryDirectory(prefix='qualification-',dir=workspace.resolve())
        self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name).resolve()
        self.area=self.root/'maintenance/source';self.area.mkdir(parents=True)
        self.output=self.root/'maintenance/results';self.output.mkdir()
        self.local=self.root/'local';self.local.mkdir()
        self.helper=self.area/'candidate.py';self.helper.write_text('# inert candidate\n')
        self.baseline=self.area/'run-full-record-adjoint-v3-20261021.py';self.baseline.write_text('# inert baseline\n')
        self.prefix=self.area/'run-full-cohort-prefix-v3-20261021.py';self.prefix.write_text('# inert fixture\n')
        self.tests=self.area/'test_operational.py';self.tests.write_text('# inert suite; synthetic runner is injected\n')
        self.manifest={'files':{p.name:sha(p) for p in [self.helper,self.baseline,self.prefix,self.tests]},
            'operational_test_suites':[{'path':self.tests.name,'name':'synthetic','tests':1}]}
        scope=dict(Path=Path,os=os,json=json,hashlib=hashlib,re=re,time=time,sys=sys,subprocess=__import__('subprocess'))
        wanted={'require','digest','ordinary','exclusive','bound_json','qualify_private'}
        nodes=[n for n in ast.parse(CANDIDATE.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name in wanted]
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(CANDIDATE),'exec'),scope)
        scope.update(ROOT=self.root,SUPPORT=self.output,FRAMEWORK=self.area,BASELINE_HELPER_SHA=sha(self.baseline),
            BASELINE_PREFIX_SHA=sha(self.prefix),CAPSULE_MANIFEST=self.manifest,__file__=str(self.helper),FALSE_FLAGS={})
        scope['load_exact']=lambda *args:SimpleNamespace(qualify_private=lambda *args,**kwargs:{'test_counts':{'tests':72}})
        self.scope=scope
        self.override={};self.returncode=0
        env_patch=patch.dict(os.environ,{'SLURM_TMPDIR':str(self.local),'SLURM_JOB_ID':'synthetic-qualification'})
        env_patch.start();self.addCleanup(env_patch.stop)

    def runner(self,command,**kwargs):
        value=dict(tests=1,failures=0,errors=0,skipped=0,successful=True,
            tests_sha256=sha(self.tests),derivative_sha256=sha(self.helper))
        value.update(self.override)
        write(Path(kwargs['env']['HHW_TEST_RESULT_DIR'])/'test-result.json',value)
        return SimpleNamespace(returncode=self.returncode)

    def qualify(self):
        return self.scope['qualify_private'](self.tests,sha(self.tests),time.time()+1800,runner=self.runner)

    def test_pinned_complete_generic_suites_add_to_unchanged_legacy_qualification(self):
        result=self.qualify()
        self.assertEqual(result['total_tests'],73)
        self.assertEqual(result['legacy_numerical_baseline']['test_counts']['tests'],72)
        self.assertTrue(result['all_numerical_definition_bytes_preserved'])

    def test_failed_skipped_partial_or_falsely_bound_suite_refuses(self):
        # Each case needs its own fresh append-only output/workspace.
        for change in [{'tests':0},{'failures':1},{'errors':1},{'skipped':1},{'successful':False},
                {'tests_sha256':'f'*64},{'derivative_sha256':'f'*64}]:
            with self.subTest(change=change):
                self.override=change
                with self.assertRaises(ValueError):self.qualify()
                # Recreate the fixture under a new temporary namespace; no source data exist.
                self.setUp()

    def test_nonzero_actual_native_suite_returncode_refuses(self):
        self.returncode=1
        with self.assertRaises(ValueError):self.qualify()

    def test_changed_capsule_source_refuses_qualification(self):
        self.tests.write_text('# changed suite\n')
        with self.assertRaises(ValueError):self.qualify()


if __name__=='__main__':
    destination=os.environ.get('HHW_TEST_RESULT_DIR')
    if destination:
        output=io.StringIO()
        result=unittest.TextTestRunner(stream=output,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
        directory=Path(destination).resolve(strict=True)
        with (directory/'test-output.txt').open('x') as stream:stream.write(output.getvalue())
        with (directory/'test-result.json').open('x') as stream:
            json.dump(dict(tests=result.testsRun,failures=len(result.failures),errors=len(result.errors),skipped=len(result.skipped),
                successful=result.wasSuccessful(),tests_sha256=sha(Path(__file__)),derivative_sha256=sha(CANDIDATE)),stream,sort_keys=True,indent=2)
            stream.write('\n')
        sys.stdout.write(output.getvalue())
        raise SystemExit(0 if result.wasSuccessful() else 1)
    unittest.main(verbosity=2)
