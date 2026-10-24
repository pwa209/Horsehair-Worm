"""Generic metadata/lock/chain/closure fixtures only; never a real census or dispatch."""
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from datetime import datetime,timezone,timedelta
import ast
import copy
import hashlib
import io
import json
import os
import re
import shlex
import sys
import tempfile
import time
import unittest
import xml.etree.ElementTree as ET

AREA=Path(__file__).resolve().parent
PROVIDER=AREA/'run-fresh-storage-census-resilience-v1-20261024.py'
GUARD=AREA/'registered-snapshot-read-guard-resilience-v1-20261024.py'


def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def canonical(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def selected(path,names,scope):
    nodes=[n for n in ast.parse(path.read_bytes()).body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),scope);return scope


class MetadataTests(unittest.TestCase):
    def setUp(self):
        workspace=Path(os.environ.get('HHW_TEST_WORKSPACE',str(AREA/'test-work')));workspace.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(prefix='meta-',dir=workspace.resolve());self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();(self.root/'maintenance').mkdir();(self.root/'state').mkdir();self.directory=self.root/'plans/chain';self.directory.mkdir(parents=True)
        self.driver=self.root/'maintenance/driver.py';self.driver.write_text('# inert metadata fixture\n')
        self.scope=selected(PROVIDER,{'require','ordinary','provider_registration','predecessor_terminal'},dict(
            Path=Path,json=json,re=re,shlex=shlex,digest=digest,canonical_hash=canonical,__file__=str(PROVIDER),
            ROOT=self.root,DRIVER=self.driver,DRIVER_SHA=digest(self.driver),PHASE='fixture-phase',MAX_SLOTS=64))
        self.scope['runpy']=SimpleNamespace(run_path=lambda *a,**k:{'checked_pair':lambda p,l,*unused:(json.loads(p.read_text()),json.loads(l.read_text()))})
        self.manifest=self.root/'maintenance/source-manifest.json';self.manifest.write_text('{}')
        self.plan_path=self.directory/'plan.json';self.ledger_path=self.root/'state/chain-submission.json'
        self.args=SimpleNamespace(native_plan=self.plan_path,native_ledger=self.ledger_path,native_task='rolling-storage-census-01',
            native_script=self.directory/'rolling-storage-census-01.sbatch',case_id='rolling-01',source_manifest=self.manifest,
            source_manifest_sha256=digest(self.manifest),kernel_qualification=self.root/'maintenance/q.json',
            kernel_qualification_sha256='a'*64,kernel_qualification_job_id='99',registry_qualification='/original/registry.json',
            registry_qualification_sha256='b'*64,registry_qualification_job_id='98')
        self.plan={'phase':'fixture-phase','tasks':[]};self.ledger={'tasks':{}}
        for index in range(1,65):
            name='rolling-storage-census-'+str(index).zfill(2);slot='rolling-'+str(index).zfill(2);script=self.directory/(name+'.sbatch')
            flags={'--case-id':slot,'--source-manifest':str(self.manifest),'--source-manifest-sha256':digest(self.manifest),
                '--kernel-qualification':str(self.args.kernel_qualification),'--kernel-qualification-sha256':'a'*64,
                '--kernel-qualification-job-id':'99','--native-plan':str(self.plan_path),'--native-ledger':str(self.ledger_path),
                '--native-task':name,'--native-script':str(script),'--registry-qualification':'/original/registry.json',
                '--registry-qualification-sha256':'b'*64,'--registry-qualification-job-id':'98'}
            command=shlex.join(['python','-s',str(PROVIDER),*[v for pair in flags.items() for v in pair]])
            script.write_text('# inert script\n'+command+'\n')
            task={'name':name,'script':str(script),'command':command,'script_sha256':canonical(script.read_text()),
                'resources':{'cpus':8,'gpu':False,'hours':6,'memory':'64G','array':None},
                'rolling_census_provider':{'helper':{'path':str(PROVIDER),'sha256':digest(PROVIDER)},
                    'source_manifest':{'path':str(self.manifest),'sha256':digest(self.manifest)},'case_id':slot},
                'depends':[] if index==1 else ['rolling-storage-census-'+str(index-1).zfill(2)],
                'dependency_kind':'afterok' if index==1 else 'afterany','external_dependencies':['99'] if index==1 else []}
            self.plan['tasks'].append(task);self.ledger['tasks'][name]={'state':'submitted','job_id':str(100+index),'script_sha256':task['script_sha256']}
        self.persist()

    def persist(self):
        self.plan_path.write_text(json.dumps(self.plan));self.ledger_path.write_text(json.dumps(self.ledger))

    def registration(self):return self.scope['provider_registration'](self.args,'101',{})

    def test_all_sixty_four_source_bound_sequential_slots_pass(self):
        value=self.registration();self.assertEqual(value['slot_count'],64);self.assertEqual(value['maximum_cpu_core_hours'],3072)

    def test_first_slot_remains_afterok_qualification_gate(self):
        self.plan['tasks'][0]['dependency_kind']='afterany';self.persist()
        with self.assertRaises(ValueError):self.registration()

    def test_later_afterok_dependency_is_refused(self):
        self.plan['tasks'][4]['dependency_kind']='afterok';self.persist()
        with self.assertRaises(ValueError):self.registration()

    def test_parallel_or_missing_predecessor_is_refused(self):
        self.plan['tasks'][4]['depends']=[];self.persist()
        with self.assertRaises(ValueError):self.registration()

    def test_changed_helper_manifest_resources_or_arguments_refuse(self):
        original=copy.deepcopy(self.plan)
        for field in ('helper','manifest','resource','argv'):
            self.plan=copy.deepcopy(original);t=self.plan['tasks'][3]
            if field=='helper':t['rolling_census_provider']['helper']['sha256']='f'*64
            if field=='manifest':t['rolling_census_provider']['source_manifest']['sha256']='f'*64
            if field=='resource':t['resources']['cpus']=64
            if field=='argv':t['command']+=' --unreviewed value'
            self.persist()
            with self.subTest(field=field):
                with self.assertRaises(ValueError):self.registration()

    def test_aliased_jobs_or_changed_script_refuse(self):
        self.ledger['tasks']['rolling-storage-census-02']['job_id']='101';self.persist()
        with self.assertRaises(ValueError):self.registration()

    def test_wrong_selected_actual_job_refuses(self):
        with self.assertRaises(ValueError):self.scope['provider_registration'](self.args,'999',{})

    def emitted(self,mode):
        path=AREA/'build-native-plans.py';tree=ast.parse(path.read_bytes())
        nodes=[n for n in tree.body if isinstance(n,ast.Assign) or isinstance(n,ast.FunctionDef) and n.name in {'require','canonical','render'}]
        b=dict(Path=Path,datetime=datetime,timezone=timezone,timedelta=timedelta,hashlib=hashlib,json=json,re=re,shlex=shlex)
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),b)
        b.update(ROOT=self.root,AREA=AREA,PHASE='emitted-provider',QPHASE='emitted-qualification',CONFIG=canonical({}),
            PROVIDER_SHA=digest(PROVIDER),QUALIFIER_SHA=digest(AREA/'qualify-census-resilience-v1-20261024.py'))
        q={'path':str(self.root/'maintenance/emitted-qualification/qualification.json'),'sha256':'a'*64,'job_id':'99'}
        plan,scripts=b['render']({}, {'sha256':b['SOURCE']},'c'*64,lambda *a,**k:'#!/bin/bash\nset -euo pipefail\n'+a[3]+'\n',
            mode=mode,qualification=q,proof={'job_id':'99','dependency_kind':'afterok'},anchor=datetime(2026,10,24,12,tzinfo=timezone.utc))
        directory=self.root/'plans'/plan['phase'];directory.mkdir();ledger_path=self.root/'state'/(plan['phase']+'-submission.json')
        ledger={'source_sha256':b['SOURCE'],'plan_sha256':canonical(plan),'tasks':{}}
        for index,task in enumerate(plan['tasks'],1):
            (directory/Path(task['script']).name).write_text(scripts[Path(task['script']).name])
            ledger['tasks'][task['name']]={'state':'submitted','job_id':str(100+index),'script_sha256':task['script_sha256']}
        plan_path=directory/'plan.json';plan_path.write_text(json.dumps(plan));ledger_path.write_text(json.dumps(ledger))
        task=plan['tasks'][0];parts=shlex.split(task['command']);tail=parts[parts.index(str(AREA/('qualify-census-resilience-v1-20261024.py' if mode=='qualification' else PROVIDER.name)))+1:]
        args=SimpleNamespace(**{k[2:].replace('-','_'):v for k,v in zip(tail[::2],tail[1::2])})
        return b,plan,ledger,plan_path,ledger_path,args

    def test_actual_emitted_sixty_four_scripts_pass_real_provider_validator(self):
        b,plan,ledger,p,l,args=self.emitted('provider');self.scope['PHASE']=plan['phase']
        for key in ('native_plan','native_ledger','native_script','source_manifest','kernel_qualification','registry_qualification'):
            setattr(args,key,Path(getattr(args,key)))
        result=self.scope['provider_registration'](args,'101',{})
        self.assertEqual(result['slot_count'],64);self.assertEqual(result['maximum_cpu_core_hours'],3072)

    def test_actual_emitted_qualification_passes_full_command_tail_and_metadata_validator(self):
        b,plan,ledger,p,l,args=self.emitted('qualification');task=plan['tasks'][0]
        scope=selected(PROVIDER,{'require','ordinary','qualification_registration'},dict(Path=Path,re=re,shlex=shlex,
            ROOT=self.root,SOURCE_SUPPORT=AREA,QUALIFIER_NAME='qualify-census-resilience-v1-20261024.py',SOURCE=b['SOURCE'],CONFIG=canonical({}),
            read_json=lambda p:json.loads(Path(p).read_text()),digest=digest,canonical_hash=canonical))
        registration={'plan':{'path':str(p),'sha256':digest(p)},'ledger':{'path':str(l),'sha256':digest(l)},
            'script':{'path':task['script'],'sha256':digest(task['script'])},'task_name':task['name'],'native_job_id':'101'}
        scope['qualification_registration'](registration,'101',manifest_path=Path(args.source_manifest),manifest_sha=args.source_manifest_sha256)
        task['command']+=' --extra forbidden';p.write_text(json.dumps(plan));ledger['plan_sha256']=canonical(plan);l.write_text(json.dumps(ledger))
        with self.assertRaises(ValueError):scope['qualification_registration'](registration,'101',manifest_path=Path(args.source_manifest),manifest_sha=args.source_manifest_sha256)

    def terminal(self,state='FAILED',**changed):
        value={'job_id_raw':'123','state':state,'exit_code':'1:0','user':'pwa209','account':'def-ptewarie_cpu',
            'cpus':8,'elapsed_seconds':925,'alloc_tres':'cpu=8,mem=64G,node=1','unallocated_metadata':False}
        value.update(changed);calls=[]
        self.scope['completed_native_dependency']=lambda *a,**k:calls.append((a,k)) or {'actual_terminal':True}
        proof=self.scope['predecessor_terminal']({'predecessor_job_id':'123'},{'123':value})
        return proof,calls

    def test_failed_owned_predecessor_allows_sequence_without_claiming_success(self):
        proof,calls=self.terminal();self.assertIs(proof['successful_census_claimed'],False)
        self.assertEqual(calls[0][1],{'dependency_kind':'afterany'})

    def test_cancelled_zero_allocation_is_explicit_terminal_metadata(self):
        proof,_=self.terminal('CANCELLED',cpus=0,elapsed_seconds=0,unallocated_metadata=True,alloc_tres='')
        self.assertEqual(proof['native_observed']['cpus'],0)

    def test_running_missing_foreign_gpu_and_malformed_predecessors_refuse(self):
        for change in ({'state':'RUNNING'},{'user':'other'},{'alloc_tres':'cpu=8,gres/gpu=1'},{'exit_code':'missing'},
                {'cpus':0,'elapsed_seconds':925},{'job_id_raw':'999'}):
            with self.subTest(change=change):
                with self.assertRaises(ValueError):self.terminal(**change)


class LockTimeout(TimeoutError):
    def __init__(self,path):self.path=str(path);self.diagnostics={'guard_path':str(path)}


class LockTests(unittest.TestCase):
    def setUp(self):
        workspace=Path(os.environ.get('HHW_TEST_WORKSPACE',str(AREA/'test-work')));workspace.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(prefix='lock-',dir=workspace.resolve());self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.now=0.;self.calls=[];self.success_after=0;self.releases=0
        self.scope=selected(GUARD,{'require','exclusive','ordinary','RegistrationWaitBudgetExceeded','registration_lock'},dict(
            Path=Path,json=json,os=os,math=__import__('math'),time=time,contextmanager=contextmanager,ROOT=self.root,
            ReceiptLockTimeout=LockTimeout,MAX_METADATA_BYTES=8_000_000,MAX_LOCK_SECONDS=3600,CONTROLLER_LOCK_SECONDS=7200,
            LOCK_ATTEMPT_SECONDS=60,LOCK_MARGIN=600,exclusive_file_lock=None))

    def factory(self,path,timeout):
        self.calls.append(timeout);test=self
        class Lock:
            def __enter__(self):
                if len(test.calls)<=test.success_after:test.now+=timeout;raise LockTimeout(path)
            def __exit__(self,*args):test.releases+=1
        return Lock()

    def lock(self,deadline=30000):return self.scope['registration_lock'](self.root/'evidence',deadline,
        role='controller-start',lock_factory=self.factory,clock=lambda:self.now,wall=lambda:self.now)

    def test_wait_longer_than_old_fifteen_minutes_can_acquire(self):
        self.success_after=20
        with self.lock():self.assertEqual(self.now,1200)
        self.assertEqual(self.releases,1);self.assertEqual(len(self.calls),21)

    def test_two_hour_wait_is_finite_and_callback_never_starts(self):
        self.success_after=1000;started=False
        with self.assertRaises(self.scope['RegistrationWaitBudgetExceeded']):
            with self.lock():started=True
        self.assertFalse(started);self.assertEqual(self.now,7200);self.assertEqual(len(self.calls),120)

    def test_real_native_end_reserves_six_hundred_seconds(self):
        self.success_after=1000
        with self.assertRaises(self.scope['RegistrationWaitBudgetExceeded']):
            with self.lock(deadline=1000):pass
        self.assertEqual(self.now,400);self.assertLessEqual(max(self.calls),60)

    def test_callback_error_is_never_retried_and_lock_releases(self):
        with self.assertRaises(ValueError):
            with self.lock():raise ValueError('single fixture operation failed')
        self.assertEqual(len(self.calls),1);self.assertEqual(self.releases,1)

    def test_foreign_lock_error_is_never_retried(self):
        class Foreign:
            def __enter__(self):raise LockTimeout('/unrelated/guard')
        with self.assertRaises(LockTimeout):
            with self.scope['registration_lock'](self.root/'evidence',1000,role='controller-start',
                lock_factory=lambda *a,**k:Foreign(),clock=lambda:0,wall=lambda:0):pass

    def test_invalid_native_deadline_refuses_before_any_callback(self):
        with self.assertRaises(ValueError):
            with self.lock(float('inf')):pass
        self.assertEqual(self.calls,[])


class PlanAndClosureTests(unittest.TestCase):
    def builder(self):
        p=AREA/'build-native-plans.py';scope=dict(Path=Path,datetime=datetime,timezone=timezone,timedelta=timedelta,
            hashlib=hashlib,json=json,re=re,shlex=shlex)
        tree=ast.parse(p.read_bytes());nodes=[n for n in tree.body if isinstance(n,ast.Assign) or isinstance(n,ast.FunctionDef) and n.name in {'require','canonical','render'}]
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(p),'exec'),scope);scope['CONFIG']=canonical({});scope['PROVIDER_SHA']='a'*64
        return scope

    def render(self,**changed):
        b=self.builder();q={'path':str(b['ROOT']/'maintenance'/b['QPHASE']/'qualification.json'),'sha256':'b'*64,'job_id':'99'}
        kw=dict(mode='provider',qualification=q,proof={'job_id':'99','dependency_kind':'afterok'},anchor=datetime(2026,10,24,12,tzinfo=timezone.utc))
        kw.update(changed)
        return b['render']({}, {'sha256':b['SOURCE']},'c'*64,lambda *a,**k:'#!/bin/bash\nset -euo pipefail\n'+a[3]+'\n',**kw)

    def test_immediate_first_then_sixty_three_four_hour_begin_slots(self):
        p,s=self.render();self.assertEqual(len(p['tasks']),64)
        self.assertNotIn('--begin=',s['rolling-storage-census-01.sbatch'])
        self.assertIn('--begin=2026-10-24T16:00:00',s['rolling-storage-census-02.sbatch'])
        self.assertIn('--begin=2026-11-04T00:00:00',s['rolling-storage-census-64.sbatch'])

    def test_afterany_chain_is_finite_serial_and_resources_are_unchanged(self):
        p,_=self.render();self.assertEqual(p['tasks'][0]['dependency_kind'],'afterok')
        self.assertTrue(all(t['dependency_kind']=='afterany' for t in p['tasks'][1:]))
        self.assertTrue(all(t['resources']=={'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None} for t in p['tasks']))
        self.assertEqual(p['finite_schedule']['maximum_cpu_core_hours'],3072)

    def test_nonpositive_new_qualification_dependency_refuses(self):
        with self.assertRaises(ValueError):self.render(proof={'job_id':'99','dependency_kind':'afterany'})

    def test_generic_qualification_is_small_and_has_no_fresh_census_dependency(self):
        p,_=self.render(mode='qualification');self.assertEqual(len(p['tasks']),1)
        self.assertEqual(p['tasks'][0]['resources'],{'cpus':4,'memory':'16G','hours':1,'gpu':False,'array':None})
        self.assertEqual(p['tasks'][0]['external_dependencies'],[])
        text=(AREA/'qualify-census-resilience-v1-20261024.py').read_text()
        self.assertNotIn('combined_admission(',text);self.assertNotIn('storage_snapshot(',text)

    def test_census_measurement_and_freshness_functions_are_byte_preserved(self):
        old=(AREA/'original-run-fresh-storage-census-v6-20261023.py').read_bytes();new=PROVIDER.read_bytes()
        a={n.name:n for n in ast.parse(old).body if isinstance(n,ast.FunctionDef)};b={n.name:n for n in ast.parse(new).body if isinstance(n,ast.FunctionDef)}
        oldlines=old.splitlines(keepends=True);newlines=new.splitlines(keepends=True)
        changed={'main','source_manifest','kernel_qualification','provider_registration','registry_read_extension'}
        for name,n in a.items():
            if name not in changed:self.assertEqual(b''.join(oldlines[n.lineno-1:n.end_lineno]),b''.join(newlines[b[name].lineno-1:b[name].end_lineno]),name)

    def test_only_registration_guard_budget_and_attempt_count_change(self):
        old=ast.parse((AREA/'original-registered-snapshot-read-guard-v4-20261022.py').read_bytes());new=ast.parse(GUARD.read_bytes())
        defs=lambda t:{n.name:ast.dump(n,include_attributes=False) for n in t.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
        a,b=defs(old),defs(new);self.assertEqual([k for k in a if a[k]!=b[k]],['registration_lock'])
        self.assertNotIn('unlink',ast.unparse(b if False else new))

    def test_no_new_scheduler_mutations_or_actual_census_calls_in_generic_consumer(self):
        node=next(n for n in ast.parse(PROVIDER.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='validate_schedule_binding')
        text=ast.unparse(node)
        self.assertNotIn('storage_snapshot',text);self.assertNotIn('subprocess',text);self.assertNotIn('submit(',text)
        self.assertIn('provider_registration(',text);self.assertIn('kernel_qualification(',text)


class Records(unittest.TextTestResult):
    def startTest(self,test):super().startTest(test);self.seen.append(test)


if __name__=='__main__':
    output=io.StringIO();Records.seen=[];result=unittest.TextTestRunner(stream=output,verbosity=2,resultclass=Records).run(
        unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    directory=os.environ.get('HHW_TEST_RESULT_DIR')
    if directory:
        target=Path(directory).resolve(strict=True);summary={'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
            'skipped':len(result.skipped),'successful':result.wasSuccessful(),'tests_sha256':digest(Path(__file__)),'provider_sha256':digest(PROVIDER)}
        (target/'test-result.json').write_bytes((json.dumps(summary,sort_keys=True,indent=2)+'\n').encode())
        suite=ET.Element('testsuite',tests=str(result.testsRun),failures=str(len(result.failures)),errors=str(len(result.errors)),skipped=str(len(result.skipped)))
        failed={test.id() for test,_ in [*result.failures,*result.errors]}
        skipped={test.id() for test,_ in result.skipped}
        for test in result.seen:
            case=ET.SubElement(suite,'testcase',classname=type(test).__name__,name=test._testMethodName)
            if test.id() in failed:ET.SubElement(case,'failure')
            if test.id() in skipped:ET.SubElement(case,'skipped')
        (target/'resilience.xml').write_bytes(ET.tostring(suite,encoding='utf-8',xml_declaration=True))
        (target/'test-output.txt').write_text(output.getvalue())
    sys.stdout.write(output.getvalue());raise SystemExit(0 if result.wasSuccessful() else 1)
