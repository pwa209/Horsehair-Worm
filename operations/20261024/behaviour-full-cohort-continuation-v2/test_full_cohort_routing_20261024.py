"""Generic routing, native-allocation and metadata fixtures; no numerical execution."""
from pathlib import Path
from types import SimpleNamespace,ModuleType
from unittest.mock import patch
from datetime import datetime,timezone
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
CANDIDATE=AREA/'run-full-cohort-continuation-v2-20261024.py'
CONTEXT=AREA/'run-full-cohort-continuation-context-v2-20261024.py'


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def selected(names,path=CANDIDATE,**scope):
    nodes=[n for n in ast.parse(path.read_bytes()).body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),scope)
    return scope


class Value:
    def __init__(self,value):self.value=value
    def detach(self):return self
    def clone(self):return Value(self.value)


class Model:
    def __init__(self):self.parameter=SimpleNamespace(grad=None)
    def state_dict(self):return {'original_parameter':Value(7)}
    def named_parameters(self):return [('original_parameter',self.parameter)]


class Guard:
    def __init__(self,deadline):self.deadline=deadline
    def check(self):pass
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def evidence(self):return {'unique_saved_storage_bytes':0,'observed_current_rss_peak_bytes':0}


class Timer:
    def __init__(self,emit):self.totals={};self.emit=emit
    def call(self,phase,callback,**unit):
        self.totals[phase]={'calls':1};return callback()


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.scope=selected({'require','full_training_comparison'},copy=copy,time=time,MemoryGuard=Guard,ResidentDeadlineGuard=Guard)
        self.scope['STAGES']=('preprocessing','primary_gradient','adjoint_gradient','comparison')
        self.initial=Model();self.events=[];self.commits=[];self.calls=[]
        # Only scalar roster/clock metadata are fabricated; neither API computes.
        counts=[2409]*1106+[sum([2666463])-2409*1106]
        self.training=[SimpleNamespace(animal_id=str(k),times=range(7072 if k<1106 else 6551),observed=SimpleNamespace(sum=lambda n=n:n)) for k,n in enumerate(counts)]
        self.scope['compare_tensor']=lambda a,b,**kw:dict(max_absolute_difference=0.,max_tolerance_ratio=0.)
        self.scope['compare_gradients']=lambda a,b:dict(all_named=True)
        def primary(model,roster,n,timer):
            self.calls.append(('primary',model,roster,n,{}))
            model.parameter.grad=Value(11)
            return timer.call('likelihood_forward',lambda:Value(13))
        def adjoint(model,roster,n,**kwargs):
            self.calls.append(('adjoint',model,roster,n,kwargs));model.parameter.grad=Value(11);return Value(13)
        self.profile=SimpleNamespace(Timings=Timer,objective_gradient=primary)
        self.adjoint=SimpleNamespace(objective_gradient=adjoint)

    def run_route(self,**changed):
        args=dict(profile=self.profile,adjoint=self.adjoint,initial=self.initial,training=self.training,
            normalizer=2666463,deadline=100,emit=lambda *args:self.events.append(args),
            commit=lambda *args:self.commits.append(args),guard_factory=Guard,candidate_guard_factory=Guard)
        args.update(changed);return self.scope['full_training_comparison'](**args)

    def test_exact_apis_receive_same_complete_ordered_roster_and_normalizer(self):
        self.run_route();self.assertEqual([q[0] for q in self.calls],['primary','adjoint'])
        self.assertTrue(all(q[2] is self.training and q[3]==2666463 for q in self.calls))
        self.assertEqual(self.calls[1][4],{'batch_size':1,'block_size':None})

    def test_separate_copies_share_identical_initial_state_without_mutating_initial(self):
        self.run_route();left,right=[q[1] for q in self.calls]
        self.assertIsNot(left,right);self.assertIsNot(left,self.initial);self.assertIsNot(right,self.initial)
        self.assertIsNone(self.initial.parameter.grad)
        self.assertEqual(left.state_dict()['original_parameter'].value,right.state_dict()['original_parameter'].value)

    def test_complete_api_returns_are_committed_in_order_before_comparison(self):
        self.run_route();self.assertEqual([q[0] for q in self.commits],['primary_gradient','adjoint_gradient','comparison'])
        self.assertTrue(all(q[1]['training_clocks']==1107 for q in self.commits))

    def test_route_never_calls_prior_backward_optimizer_or_validation(self):
        tree=ast.parse(CANDIDATE.read_bytes());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='full_training_comparison')
        attrs=[n.func.attr for n in ast.walk(node) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)]
        self.assertFalse(set(attrs)&{'log_prior','backward','step','score','adam_epoch','profile_fit','forward_sequence'})
        result=self.run_route();self.assertEqual(result['optimizer_updates'],0);self.assertEqual(result['validation_calls'],0)

    def test_whole_fold_measurements_make_no_mid_operation_resume_claim(self):
        result=self.run_route();self.assertIs(result['mid_operation_resume_supported'],False)
        self.assertEqual(result['prior_applications_per_implementation'],1)
        self.assertEqual(result['global_observed_bin_normalizer'],2666463)

    def test_partial_roster_refuses_before_either_api(self):
        with self.assertRaises(ValueError):self.run_route(training=self.training[:-1])
        self.assertEqual(self.calls,[])

    def test_duplicate_recording_refuses_before_either_api(self):
        self.training[-1].animal_id=self.training[0].animal_id
        with self.assertRaises(ValueError):self.run_route()
        self.assertEqual(self.calls,[])

    def test_changed_global_normalizer_refuses_before_either_api(self):
        with self.assertRaises(ValueError):self.run_route(normalizer=2666462)
        self.assertEqual(self.calls,[])

    def test_wrong_observed_bin_sum_refuses_before_either_api(self):
        self.training[-1].observed.sum=lambda:0
        with self.assertRaises(ValueError):self.run_route()
        self.assertEqual(self.calls,[])

    def test_initial_state_mismatch_refuses_before_either_api(self):
        self.scope['compare_tensor']=lambda *args,**kwargs:{'max_absolute_difference':1.}
        with self.assertRaises(ValueError):self.run_route()
        self.assertEqual(self.calls,[])

    def test_primary_failure_does_not_start_adjoint_or_commit(self):
        self.profile.objective_gradient=lambda *args:(_ for _ in ()).throw(ValueError('fixture primary refusal'))
        with self.assertRaises(ValueError):self.run_route()
        self.assertEqual(self.calls,[]);self.assertEqual(self.commits,[])

    def test_adjoint_failure_retains_only_completed_primary_boundary(self):
        self.adjoint.objective_gradient=lambda *args,**kwargs:(_ for _ in ()).throw(ValueError('fixture candidate refusal'))
        with self.assertRaises(ValueError):self.run_route()
        self.assertEqual([q[0] for q in self.commits],['primary_gradient'])

    def test_gradient_comparison_failure_never_commits_comparison(self):
        self.scope['compare_gradients']=lambda *args:(_ for _ in ()).throw(ValueError('fixture gradient mismatch'))
        with self.assertRaises(ValueError):self.run_route()
        self.assertEqual([q[0] for q in self.commits],['primary_gradient','adjoint_gradient'])

    def test_candidate_has_explicit_rss_only_cohort_guard(self):
        tree=ast.parse(CANDIDATE.read_bytes());node=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='ResidentDeadlineGuard')
        text=ast.unparse(node)
        self.assertIn('self.check()',text);self.assertNotIn('self.saved_bytes',text)
        self.assertIn("'cumulative_saved_storage_rejection_applied': False",text)


class AllocationTests(unittest.TestCase):
    def setUp(self):
        workspace=Path(os.environ.get('HHW_TEST_WORKSPACE',str(AREA/'test-work-routing')));workspace.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(prefix='allocation-',dir=workspace.resolve());self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.directory=self.root/'plans/full-cohort-v1';self.directory.mkdir(parents=True)
        (self.root/'state').mkdir();self.script=self.directory/'full-cohort.sbatch';self.script.write_text('# inert native fixture\n')
        self.end=datetime.now(timezone.utc).timestamp()+86400
        self.fields={'JobId':'900001','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING',
            'NumCPUs':'1','NumNodes':'1','TimeLimit':'1-00:00:00','MinMemoryNode':'64G','Command':str(self.script),
            'EndTime':datetime.fromtimestamp(self.end,timezone.utc).replace(microsecond=0).isoformat().replace('+00:00',''),
            'JobName':'hhw-full-cohort-v1','AllocTRES':'cpu=1,mem=64G,node=1','ReqTRES':'cpu=1,mem=64G,node=1'}
        self.end=datetime.fromisoformat(self.fields['EndTime']).replace(tzinfo=timezone.utc).timestamp()
        self.plan={'config_sha256':'config','release_identity':{'sha256':'source'},'tasks':[{'name':'full-cohort-v1',
            'script':str(self.script),'script_sha256':canonical(self.script.read_text()),
            'resources':{'cpus':1,'memory':'64G','hours':24,'gpu':False,'array':None}}]}
        self.ledger={'source_sha256':'source','plan_sha256':canonical(self.plan),'tasks':{'full-cohort-v1':{'job_id':'900001',
            'state':'submitted','script_sha256':canonical(self.script.read_text())}}}
        def fixture_fields(pattern,raw):
            # Native /scratch paths have no spaces. Preserve the real parser on
            # a Windows fixture path by replacing only this known literal value.
            token='/fixture-command-without-spaces'
            pairs=re.findall(pattern,raw.replace(str(self.script),token))
            return [(k,str(self.script) if k=='Command' and v==token else v) for k,v in pairs]
        parser=SimpleNamespace(findall=fixture_fields,fullmatch=re.fullmatch)
        self.scope=selected({'require','memory','native_allocation'},CONTEXT,Path=Path,re=parser,datetime=datetime,timezone=timezone,
            os=os,time=time,math=math,ROOT=self.root,SUPPORT=self.root/'maintenance/evidence',CONFIG='config',SOURCE='source',
            read_json=lambda p:json.loads(Path(p).read_text()),canonical_hash=canonical,digest=sha)
        self.patch=patch.dict(os.environ,{'SLURM_JOB_END_TIME':str(self.end)});self.patch.start();self.addCleanup(self.patch.stop)
        self.persist()

    def persist(self):
        (self.directory/'plan.json').write_text(json.dumps(self.plan))
        self.ledger['plan_sha256']=canonical(self.plan)
        (self.root/'state/full-cohort-v1-submission.json').write_text(json.dumps(self.ledger))

    def proof(self):
        response=SimpleNamespace(stdout=' '.join(k+'='+v for k,v in self.fields.items())+'\n',check_returncode=lambda:None)
        return self.scope['native_allocation']('900001',self.script,lambda *args:response)

    def test_exact_registered_owned_twenty_four_hour_cpu_allocation_passes(self):
        result=self.proof();self.assertEqual(result['allocation_end_unix'],self.end)

    def test_old_eight_hour_allocation_refuses(self):
        self.fields['TimeLimit']='08:00:00'
        with self.assertRaises(ValueError):self.proof()

    def test_gpu_owner_cpu_memory_and_script_mismatches_refuse(self):
        for field,value in [('AllocTRES','cpu=1,gres/gpu=1'),('UserId','other(1)'),('NumCPUs','2'),('MinMemoryNode','32G'),('Command','/unrelated')]:
            old=self.fields[field];self.fields[field]=value
            with self.subTest(field=field):
                with self.assertRaises(ValueError):self.proof()
            self.fields[field]=old

    def test_old_eight_hour_plan_resources_refuse(self):
        self.plan['tasks'][0]['resources']['hours']=8;self.persist()
        with self.assertRaises(ValueError):self.proof()

    def test_wrong_native_registration_refuses(self):
        self.ledger['tasks']['full-cohort-v1']['job_id']='900002';self.persist()
        with self.assertRaises(ValueError):self.proof()

    def test_changed_script_bytes_refuse(self):
        self.script.write_text('# changed fixture\n')
        with self.assertRaises(ValueError):self.proof()

    def test_native_end_environment_disagreement_refuses(self):
        with patch.dict(os.environ,{'SLURM_JOB_END_TIME':str(self.end+5)}):
            with self.assertRaises(ValueError):self.proof()

    def test_duplicate_native_fields_refuse(self):
        raw=' '.join(k+'='+v for k,v in self.fields.items())+' JobId=900001\n'
        with self.assertRaises(ValueError):self.scope['native_allocation']('900001',self.script,
            lambda *args:SimpleNamespace(stdout=raw,check_returncode=lambda:None))


class ClosureTests(unittest.TestCase):
    def test_context_derivative_changes_only_allocation_validation_and_namespaces(self):
        old=ast.parse((AREA/'original-eight-hour-context.py').read_bytes());new=ast.parse(CONTEXT.read_bytes())
        defs=lambda t:{n.name:ast.dump(n,include_attributes=False) for n in t.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
        a,b=defs(old),defs(new);self.assertEqual([k for k in a if a[k]!=b[k]],['native_allocation'])

    def test_immutable_numerical_api_pins_are_retained(self):
        tree=ast.parse(CANDIDATE.read_bytes());values={}
        nodes=[n for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in {'ADJOINT_SHA','PROFILE_SHA','MEMORY_LIMIT'}]
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(CANDIDATE),'exec'),values)
        values={k:v for k,v in values.items() if k!='__builtins__'}
        self.assertEqual(values,{'ADJOINT_SHA':'5ff03ba506d4ad8dab66dd51def1c728aade96a8af77661be03de20e40a205f9',
            'PROFILE_SHA':'e60561b425d500d08690675d98e4f86b2617044e446183433cdf3e4465602e59','MEMORY_LIMIT':48*1024**3})
        previous=AREA.parent/'behaviour-full-cohort-continuation-v1'/'run-full-cohort-continuation-v1-20261024.py'
        if not previous.is_file():
            previous=Path('/scratch/pwa209/horsehair-worm/maintenance/study-behaviour-full-cohort-continuation-v1-20261024/run-full-cohort-continuation-v1-20261024.py')
        self.assertEqual(sha(previous),'ef9096e9ed244b3f4390cf5c283a6edc87ad24263b3b3e8c367347b10445613d')
        oldraw=previous.read_bytes();newraw=CANDIDATE.read_bytes()
        old={n.name:n for n in ast.parse(oldraw).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
        new={n.name:n for n in ast.parse(newraw).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
        part=lambda raw,node:b''.join(raw.splitlines(keepends=True)[node.lineno-1:node.end_lineno])
        for name in set(old)-{'main'}:self.assertEqual(part(oldraw,old[name]),part(newraw,new[name]),name)

    def test_prepare_uses_unchanged_pinned_api_and_complete_roster_checks(self):
        node=next(n for n in ast.parse(CANDIDATE.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='prepare')
        text=ast.unparse(node)
        self.assertIn('prefix.prepare(s, b, profile, config, root, case, deadline, emit)',text)
        self.assertIn('== 1107',text);self.assertIn('== 7828183',text);self.assertIn('== 2666463',text)
        self.assertNotIn('select_clock(',text);self.assertNotIn('prospective_validation_canary',text)

    def test_worker_writes_only_own_completed_stages_without_canonical_benchmark(self):
        node=next(n for n in ast.parse(CANDIDATE.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='_worker')
        text=ast.unparse(node)
        self.assertNotIn('behaviour-workload-benchmark',text);self.assertNotIn('optimizer',text)
        self.assertIn('exact_mid_operation_resume_supported=False',text)

    def test_completed_record_and_actual_first_census_prerequisites_precede_worker(self):
        node=next(n for n in ast.parse(CANDIDATE.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='main')
        text=ast.unparse(node);self.assertIn("'6943757'",text)
        self.assertLess(text.index('record_probe_evidence(observed)'),text.index('with context.resource_lease('))
        self.assertLess(text.index('census_provider_evidence('),text.index('with context.resource_lease('))
        self.assertIn('receipt_sha=args.census_provider_receipt_sha256',text)
        for change in ('positive','builder_metadata_only','old_identity','wrong_first_job','bad_source_binding',
                'wrong_status','scientific_flag','wrong_registration','receipt_tamper','native_failed',
                'fresh_native_missing','qualification_failed'):
            with self.subTest(change=change):self._provider_gate_case(change)

    def _provider_gate_case(self,change):
        def require(value,message):
            if not value:raise ValueError(message)
        workspace=Path(os.environ.get('HHW_TEST_WORKSPACE',str(AREA/'w')))
        workspace.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='p',dir=workspace.resolve()) as temporary:
            root=Path(temporary).resolve();job='900000001';source=root/'maintenance/study-census-resilience-support-v1-20261024'
            source.mkdir(parents=True);helper=source/'run-fresh-storage-census-resilience-v1-20261024.py'
            manifest=source/'source-manifest.json';helper.write_bytes(b'# inert source fixture\n');manifest.write_text('{}')
            fixed={str(helper):'94ac29d11da50412ba3c1f9269ed5762224f93635cd1fae505bfabc5f9b63a93',
                str(manifest):'dbe6c589a7dda4e8d54cc041ddee1dfa77f7804d67e290efcd396c7108b6f152'}
            digest=lambda path:fixed.get(str(Path(path)),sha(path))
            def write(path,value):
                path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value));return {'path':str(path),'sha256':digest(path)}
            plan_path=root/'plans/study-census-resilience-v1-20261024/plan.json'
            ledger_path=root/'state/study-census-resilience-v1-20261024-submission.json'
            script=plan_path.parent/'provider-0.sbatch';script.parent.mkdir(parents=True);script.write_text('# inert registered fixture\n')
            plan={'config_sha256':'config','tasks':[{'name':'provider-'+str(k),'script':str(script)} for k in range(64)]}
            ledger={'source_sha256':'provider-source','plan_sha256':canonical(plan),'tasks':{'provider-0':{'job_id':job}}}
            plan_binding=write(plan_path,plan);ledger_binding=write(ledger_path,ledger)
            q=write(source/'qualification.json',{'slurm_job_id':'fixture-qualification'})
            schedule={'plan':plan_binding,'ledger':ledger_binding,'helper':{'path':str(helper),'sha256':fixed[str(helper)]},
                'source_manifest':{'path':str(manifest),'sha256':fixed[str(manifest)]},'qualification':q,
                'case_root':str(root/'maintenance/study-census-resilience-v1-20261024')}
            registration={'plan':plan_binding,'ledger':ledger_binding,'script':{'path':str(script),'sha256':digest(script)},
                'task_name':'provider-0','native_job_id':job}
            case=Path(schedule['case_root'])/('native-'+job);sweep=write(case/'sweep-result.json',{'fixture':'complete sweep metadata'})
            receipt={'status':'complete_exact_size_sweep_with_original_conservative_freshness','native_job_id':job,
                'source_sha256':'provider-source','config_sha256':'config','source_manifest':schedule['source_manifest'],
                'qualification':q,'provider_helper':schedule['helper'],'wrapper_sha256':fixed[str(helper)],
                'allocation':{'native_registration':copy.deepcopy(registration)},'sweep':sweep,'scientific_result':False,'admission_authority':False}
            if change=='wrong_first_job':ledger['tasks']['provider-0']['job_id']='900000002';schedule['ledger']=write(ledger_path,ledger)
            if change=='bad_source_binding':schedule['helper']['sha256']='f'*64
            if change=='wrong_status':receipt['status']='resource_pending_registration'
            if change=='scientific_flag':receipt['scientific_result']=True
            if change=='wrong_registration':receipt['allocation']['native_registration']['native_job_id']='900000002'
            receipt_binding=write(case/'receipt.json',receipt)
            if change=='receipt_tamper':(case/'receipt.json').write_text(json.dumps(receipt)+' ')
            schedule_path=root/'maintenance/actual-schedule.json';schedule_binding=write(schedule_path,schedule)
            row={'state':'COMPLETED','exit_code':'0:0','cpus':8,'gpus':0,'elapsed_seconds':100}
            if change=='native_failed':row['state']='FAILED'
            completion={'job_id':job,'native_records':{job:row}}
            observed={**row,'job_id_raw':job,'user':'pwa209','account':'def-ptewarie_cpu','unallocated_metadata':False,'alloc_tres':'cpu=8,mem=64G,node=1'}
            calls=[]
            def qualification(*args):
                calls.append('full_qualification')
                if change=='qualification_failed':raise ValueError('fixture qualification refused')
                return {'checked_paths':{}}
            producer=SimpleNamespace(SOURCE='provider-source',FALSE_FLAGS=('scientific_result','admission_authority'),
                source_manifest=lambda *args:({'kind':'census-resilience-source-manifest-v1'},dict(fixed)),
                registered_task=lambda *args,**kwargs:(registration,[]),validate_schedule_binding=qualification)
            def load(name,path,pin):
                require(Path(path)==helper and pin==fixed[str(helper)],'unreviewed helper refused');return producer
            scope=selected({'require','ordinary','registered_json','bound_json','census_provider_evidence'},
                Path=Path,re=re,hashlib=hashlib,json=json,ROOT=root,CONFIG='config',
                FALSE_FLAGS={'scientific_result':False,'admission_authority':False},digest=digest,load_exact=load)
            fake_slurm=ModuleType('horsehair.slurm');fake_slurm.completed_native_dependency=lambda *args:completion
            fake_core=ModuleType('horsehair.core');fake_core.canonical_hash=canonical
            reader=None if change=='builder_metadata_only' else lambda *args,**kwargs:{} if change=='fresh_native_missing' else {job:observed}
            invoke=lambda:scope['census_provider_evidence'](schedule_path,schedule_binding['sha256'],
                '6975379' if change=='old_identity' else job,reader,receipt_sha=receipt_binding['sha256'],qualification_verifier=qualification)
            with patch.dict(sys.modules,{'horsehair':ModuleType('horsehair'),'horsehair.slurm':fake_slurm,'horsehair.core':fake_core}):
                if change in {'positive','builder_metadata_only'}:
                    result=invoke();self.assertEqual(result['job_id'],job);self.assertIs(result['artifacts_deserialized'],False)
                    self.assertEqual(result['fresh_native_and_full_qualification_verified'],change=='positive')
                    self.assertEqual(calls,['full_qualification'] if change=='positive' else [])
                else:
                    with self.assertRaises((ValueError,FileNotFoundError)):invoke()


if __name__=='__main__':
    output=io.StringIO();result=unittest.TextTestRunner(stream=output,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    destination=os.environ.get('HHW_TEST_RESULT_DIR')
    if destination:
        directory=Path(destination).resolve(strict=True)
        with (directory/'test-output.txt').open('x') as stream:stream.write(output.getvalue())
        with (directory/'test-result.json').open('x') as stream:
            json.dump(dict(tests=result.testsRun,failures=len(result.failures),errors=len(result.errors),skipped=len(result.skipped),
                successful=result.wasSuccessful(),tests_sha256=sha(Path(__file__)),derivative_sha256=sha(CANDIDATE)),stream,sort_keys=True,indent=2);stream.write('\n')
    sys.stdout.write(output.getvalue());raise SystemExit(0 if result.wasSuccessful() else 1)
