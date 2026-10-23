from pathlib import Path
from types import SimpleNamespace
import pytest
from horsehair.core import atomic_json, canonical_hash
from horsehair.slurm import submit,_script,plan,completed_native_dependency


def test_submission_does_not_duplicate_jobs(tmp_path):
    script=tmp_path/'job.sbatch';script.write_text('#!/bin/bash\ntrue\n')
    plan={'tasks':[{'name':'a','script':str(script),'script_sha256':canonical_hash(script.read_text()),'depends':[]}]}
    atomic_json(tmp_path/'plan.json',plan)
    calls=[]
    def runner(command,**kwargs):
        calls.append(command);return SimpleNamespace(returncode=0,stdout='12345\n',stderr='')
    first=submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)
    second=submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)
    assert first==second and len(calls)==1


def test_ambiguous_sbatch_is_never_replayed(tmp_path):
    script=tmp_path/'job.sbatch';script.write_text('true\n')
    atomic_json(tmp_path/'plan.json',{'tasks':[{'name':'a','script':str(script),'script_sha256':canonical_hash(script.read_text()),'depends':[]}]})
    runner=lambda *a,**k:SimpleNamespace(returncode=1,stdout='',stderr='connection lost')
    with pytest.raises(RuntimeError):submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)
    with pytest.raises(RuntimeError,match='prior'):submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)


def test_alliance_module_python_paths_survive_source_overlay():
    # Alliance's scipy-stack/Arrow packages and some wheel dependencies are
    # supplied through module PYTHONPATH, rather than inside the virtualenv.
    config={'paths':{'root':'/scratch/pwa209/horsehair-worm','cpu_account':'def-ptewarie_cpu','environment':'/home/pwa209/hhw-environment'}}
    script=_script(config,Path('/project/release'),'qualify','python -m horsehair.cli qualify')
    overlay=next(line for line in script.splitlines() if line.startswith('export PYTHONPATH='))
    assert overlay.endswith(':"${PYTHONPATH:-}"')


def _config(tmp_path):
    from horsehair.core import read_json
    config=read_json(Path(__file__).parents[1]/'configs/study.json')
    config['paths']['root']=str(tmp_path/'study')
    return config


def _release(tmp_path):
    release=tmp_path/'release';release.mkdir()
    atomic_json(release/'release.json',{'release_id':'fixture'})
    return release


def _native_record(state='COMPLETED',exit_code='0:0',seconds=12,cpus=2):
    return {'state':state,'exit_code':exit_code,'elapsed_seconds':seconds,'cpus':cpus,'gpus':0,'basis':'sacct final allocated wall time'}


def _register_prior(config,phase,name,job_id,array=None):
    root=Path(config['paths']['root'])
    record={'tasks':[{'name':name,'resources':{'array':array}}]}
    atomic_json(root/'plans'/('fixture-'+phase)/'plan.json',record)
    atomic_json(root/'state'/('fixture-'+phase+'-submission.json'),{'plan_sha256':canonical_hash(record),'tasks':{name:{'state':'submitted','job_id':job_id}}})


def test_managed_initial_has_full_coverage_arrays_and_finite_afterany_controllers(tmp_path):
    config=_config(tmp_path)
    record=plan(config,_release(tmp_path),tmp_path/'plan','managed-initial')
    tasks={task['name']:task for task in record['tasks']}
    assert 'fly-pilot' not in tasks
    assert tasks['agent-benchmark-training']['resources']['array']=='0-11%1'
    assert tasks['agent-benchmark-evaluation']['resources']['array']=='0-11%2'
    assert tasks['agent-benchmark-aggregate']['depends']==['agent-benchmark-evaluation']
    assert tasks['molecular-runtime']['resources']['cpus']==2
    assert tasks['molecular-runtime']['resources']['memory']=='8G'
    assert tasks['molecular-pilot']['resources']['hours']==24
    assert tasks['molecular-pilot']['depends']==['rna-reference','molecular-runtime']
    assert record['maximum_simultaneous_resources']['cpus']<=64
    assert record['maximum_simultaneous_resources']['gpus']==1
    for index in range(4):
        task=tasks['pipeline-controller-'+str(index)]
        assert task['resources']=={'cpus':1,'memory':'8G','hours':144,'gpu':False}
        assert task['dependency_kind']==('afterany' if index else 'afterok')
        assert task['depends']==(['pipeline-controller-'+str(index-1)] if index else ['qualification'])
    rna=Path(tasks['rna-pilot-0']['script']).read_text()
    assert all(module in rna for module in ['star/2.7.11b','subread/2.0.6','samtools/1.20'])
    # Dependency jobs do not transfer their loaded R environment. TransDecoder
    # needs Rscript in its own runtime-qualification and ORF allocations.
    assert 'module load r/4.4.0' in Path(tasks['molecular-runtime']['script']).read_text()


def test_afterany_is_preserved_in_native_command_and_hashed_ledger(tmp_path):
    script=tmp_path/'job.sbatch';script.write_text('true\n')
    tasks=[{'name':name,'script':str(script),'script_sha256':canonical_hash(script.read_text()),'depends':[] if name=='a' else ['a'],'dependency_kind':'afterok' if name=='a' else 'afterany'} for name in ['a','b']]
    atomic_json(tmp_path/'plan.json',{'tasks':tasks})
    calls=[]
    def runner(command,**kwargs):
        calls.append(command);return SimpleNamespace(returncode=0,stdout=str(100+len(calls)),stderr='')
    ledger=submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)
    assert '--dependency=afterany:101' in calls[1]
    assert ledger['tasks']['b']['dependency_kind']=='afterany'
    assert ledger['tasks']['b']['dependency_resolution_sha256']==canonical_hash({'kind':'afterany','issued':['101'],'completed':[]})


def test_expired_completed_array_dependency_requires_all_declared_native_tasks(tmp_path):
    atomic_json(tmp_path/'state/resource-budget.json',{'native_jobs':{'100_0':_native_record()}})
    assert completed_native_dependency(tmp_path,'100','0-1%1') is None
    atomic_json(tmp_path/'state/resource-budget.json',{'native_jobs':{'100_0':_native_record(),'100_1':_native_record()}})
    proof=completed_native_dependency(tmp_path,'100','0-1%1')
    assert set(proof['native_records'])=={'100_0','100_1'}
    # A failed second task must never be hidden behind one completed task.
    atomic_json(tmp_path/'state/resource-budget.json',{'native_jobs':{'100_0':_native_record(),'100_1':_native_record('FAILED','1:0')}})
    with pytest.raises(ValueError,match='failed'):
        completed_native_dependency(tmp_path,'100','0-1%1')


def test_late_submit_omits_expired_success_with_immutable_selected_native_proof(tmp_path):
    script=tmp_path/'job.sbatch';script.write_text('true\n')
    atomic_json(tmp_path/'plan.json',{'root':str(tmp_path),'tasks':[{'name':'a','script':str(script),'script_sha256':canonical_hash(script.read_text()),'depends':[],'external_dependencies':['100'],'external_dependency_specs':[{'job_id':'100','array':'0-1%1'}]}]})
    atomic_json(tmp_path/'state/resource-budget.json',{'native_jobs':{'100_0':_native_record(),'100_1':_native_record()}})
    calls=[]
    def runner(command,**kwargs):
        calls.append(command);return SimpleNamespace(returncode=0,stdout='200',stderr='')
    ledger=submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)
    assert not any(value.startswith('--dependency=') for value in calls[0])
    assert ledger['tasks']['a']['completed_native_dependencies'][0]['native_records_sha256']


def test_afterany_can_use_proven_cancelled_job_without_allocated_compute(tmp_path):
    atomic_json(tmp_path/'state/resource-budget.json',{'native_jobs':{'100':_native_record('CANCELLED','0:0',0,0)}})
    proof=completed_native_dependency(tmp_path,'100',dependency_kind='afterany')
    assert proof['native_records']['100']['state']=='CANCELLED'
    with pytest.raises(ValueError,match='failed'):
        completed_native_dependency(tmp_path,'100')


def test_parallel_submitters_do_not_duplicate_native_submission(tmp_path):
    import time
    from concurrent.futures import ThreadPoolExecutor
    script=tmp_path/'job.sbatch';script.write_text('true\n')
    atomic_json(tmp_path/'plan.json',{'tasks':[{'name':'a','script':str(script),'script_sha256':canonical_hash(script.read_text()),'depends':[]}]})
    calls=[]
    def runner(command,**kwargs):
        calls.append(command);time.sleep(.1);return SimpleNamespace(returncode=0,stdout='12345',stderr='')
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner),range(2)))
    assert results[0]==results[1] and len(calls)==1


def test_all_managed_pipeline_phases_have_executable_finite_plans(tmp_path,monkeypatch):
    import horsehair.slurm as slurm
    import horsehair.molecular as molecular
    from horsehair.pipeline import NODES
    config=_config(tmp_path);release=_release(tmp_path)
    monkeypatch.setattr(slurm,'require_admission',lambda config,root,phase:{'phase':phase,'allocation_parameters':{}})
    monkeypatch.setattr(molecular,'validate_freeze',lambda path:({'test_fixture':True},path))
    # The primary reference and molecular pilot share their registered initial plan.
    root=Path(config['paths']['root'])
    initial={'tasks':[{'name':name,'resources':{}} for name in ['rna-reference','molecular-pilot']]}
    atomic_json(root/'plans/fixture-managed-initial/plan.json',initial)
    atomic_json(root/'state/fixture-managed-initial-submission.json',{'plan_sha256':canonical_hash(initial),'tasks':{name:{'state':'submitted','job_id':str(100+i)} for i,name in enumerate(['rna-reference','molecular-pilot'])}})
    _register_prior(config,'behaviour','wp1-freeze','102')
    _register_prior(config,'rna-full','ant-score-freeze','103')
    _register_prior(config,'molecular-benchmark','molecular-benchmark','104')
    for phase,_,_ in NODES:
        record=plan(config,release,tmp_path/('plan-'+phase),phase)
        assert record['tasks'] and record['maximum_simultaneous_resources']['cpus']<=64
        assert all(Path(task['script']).is_file() for task in record['tasks'])
        if phase=='molecular-transfer':
            assert len(record['tasks'])==1 # Screen targets must not be regenerated after their benchmark.
        if phase=='agents-final':
            assert record['tasks'][0]['resources']['array']=='0-239%1'
        if phase=='molecular-corroboration':
            by_name={task['name']:task for task in record['tasks']}
            assert 'module load r/4.4.0' in Path(by_name['mantis-orfs']['script']).read_text()


def test_failed_preflight_does_not_create_an_ambiguous_plan_directory(tmp_path,monkeypatch):
    import horsehair.slurm as slurm
    monkeypatch.setattr(slurm,'require_admission',lambda *_: (_ for _ in ()).throw(ValueError('Missing measured admission')))
    output=tmp_path/'plan'
    with pytest.raises(ValueError,match='admission'):
        plan(_config(tmp_path),_release(tmp_path),output,'behaviour')
    assert not output.exists()

