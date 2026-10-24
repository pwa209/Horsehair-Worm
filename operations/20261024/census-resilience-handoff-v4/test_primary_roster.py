"""Generic immutable registration/array/terminal-state fixtures only."""
from pathlib import Path
import copy
import functools
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import pytest

AREA=Path(__file__).resolve().parent
REPOSITORY=AREA.parents[2]


@functools.lru_cache(maxsize=1)
def original_planners():
    primary=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf/src/horsehair/slurm.py')
    framework=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261021-57e0eb8c4c6c-d89b31180d4c/src/horsehair/slurm.py')
    if primary.exists() and framework.exists():
        return {'primary':primary.read_bytes(),'framework':framework.read_bytes()}
    git=shutil.which('git') or str(Path(os.environ['USERPROFILE'])/'.cache/codex-runtimes/codex-primary-runtime/dependencies/native/git/cmd/git.exe')
    primary_raw=subprocess.run([git,'show','829e3817bee1:src/horsehair/slurm.py'],cwd=REPOSITORY,
        check=True,capture_output=True).stdout
    return {'primary':primary_raw,'framework':(REPOSITORY/'src/horsehair/slurm.py').read_bytes()}


@pytest.fixture
def m():
    spec=importlib.util.spec_from_file_location('primary_roster_fixture',AREA/'primary-roster.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value),encoding='utf8');return path


@pytest.fixture
def fixture(m,tmp_path):
    root=tmp_path/'study';root.mkdir();helper=root/m.HELPER;helper.parent.mkdir(parents=True)
    original=REPOSITORY/'operations/20261023/census-restored-native-queue-and-primary-repair-v1/work/run-primary-external-census-v1-20261023.py'
    if not original.exists():
        original=Path('/scratch/pwa209/horsehair-worm')/m.HELPER
    helper.write_bytes(original.read_bytes())
    pipeline=REPOSITORY/'src/horsehair/pipeline.py'
    if not pipeline.exists():pipeline=m.OLD_FRAMEWORK/'src/horsehair/pipeline.py'
    planners={}
    for label,raw in original_planners().items():
        path=tmp_path/'planners'/(label+'.py');path.parent.mkdir(exist_ok=True);path.write_bytes(raw);planners[label]=path
    paths={'_planners':planners};jobs=iter(['100','101','102','200','201'])
    def phase(name,task_arrays):
        directory=root/'plans'/(m.PRIMARY_RELEASE_ID+'-'+name);tasks=[];rows={}
        for task_name,array in task_arrays:
            job=next(jobs);script=directory/(task_name+'.sbatch');script.parent.mkdir(parents=True,exist_ok=True)
            command='printf generic-metadata-fixture';text='#!/bin/bash\n'+command+'\n';script.write_text(text,encoding='utf8',newline='\n')
            task={'name':task_name,'script':str(script),'script_sha256':m.canonical(text),'command':command,
                'resources':{'cpus':1,'memory':'1G','hours':1,'gpu':False,'array':array}}
            tasks.append(task);rows[task_name]={'state':'submitted','job_id':job,'script_sha256':task['script_sha256']}
        plan={'root':str(root),'phase':name,'config_sha256':m.CONFIG_SHA,'release_identity':{'sha256':m.PRIMARY_SHA},'tasks':tasks}
        pp=write(directory/'plan.json',plan);lp=write(root/'state'/(m.PRIMARY_RELEASE_ID+'-'+name+'-submission.json'),
            {'source_sha256':m.PRIMARY_SHA,'config_sha256':m.CONFIG_SHA,'plan_sha256':m.canonical(plan),'tasks':rows})
        jp=None
        if name!='managed-initial':
            jp=write(root/'state/pipeline'/m.PRIMARY_RELEASE_ID/(name+'-plan-journal.json'),
                {'state':'submitted','phase':name,'source_sha256':m.PRIMARY_SHA,'plan_sha256':m.canonical(plan),'ledger':str(lp)})
        paths[name]={'plan':pp,'ledger':lp,'journal':jp};return paths[name]
    phase('managed-initial',[('worker-scalar',None),('worker-array','0-4:2%2'),('pipeline-controller-0',None)])
    phase('rna-full',[('phase-worker',None)])
    return root,pipeline,paths,phase


def discover(m,fixture):
    root,pipeline,paths,_=fixture
    return m.primary_worker_roster(root,m.PRIMARY_RELEASE_ID,pipeline_path=pipeline,planner_paths=paths['_planners'])


def update_plan(m,fixture,phase,mutate):
    paths=fixture[2][phase];plan=json.loads(paths['plan'].read_bytes());mutate(plan)
    ledger=json.loads(paths['ledger'].read_bytes());ledger['plan_sha256']=m.canonical(plan)
    write(paths['plan'],plan);write(paths['ledger'],ledger)
    if paths['journal'] is not None:
        journal=json.loads(paths['journal'].read_bytes());journal['plan_sha256']=m.canonical(plan);write(paths['journal'],journal)


@pytest.fixture
def historical_fixture(m,fixture,tmp_path,monkeypatch):
    root=fixture[0];paths=fixture[2]['rna-full']
    plan=json.loads(paths['plan'].read_bytes());task=plan['tasks'][0]
    original=Path(task['script']);script=root/'plans/preserved-repair'/original.name
    script.parent.mkdir();original.rename(script)
    update_plan(m,fixture,'rna-full',lambda value:value['tasks'][0].update(script=str(script)))
    plan=json.loads(paths['plan'].read_bytes());ledger=json.loads(paths['ledger'].read_bytes())
    task=plan['tasks'][0];row=ledger['tasks'][task['name']]
    record={'plan_sha256':hashlib.sha256(paths['plan'].read_bytes()).hexdigest(),
        'ledger_sha256':hashlib.sha256(paths['ledger'].read_bytes()).hexdigest(),
        'script_relative_path':script.relative_to(root).as_posix(),
        'script_sha256':hashlib.sha256(script.read_bytes()).hexdigest(),
        'script_text_sha256':task['script_sha256'],'job_id':row['job_id'],
        'task_execution_release_sha256':m.canonical(task.get('execution_release')),
        'ledger_execution_release_sha256':m.canonical(row.get('execution_release'))}
    history=write(tmp_path/'history.json',{'kind':'source-bound-primary-historical-script-bindings-v1',
        'primary_release_id':m.PRIMARY_RELEASE_ID,'tasks':{'rna-full/'+task['name']:record}})
    monkeypatch.setattr(m,'HISTORY',history)
    return history,script,record


def test_exact_historical_repair_keeps_all_registered_worker_bindings(m,fixture,historical_fixture):
    history,script,record=historical_fixture;result=discover(m,fixture)
    assert result['workers']['200']['script']==str(script)
    assert result['checked_paths'][str(script)]==record['script_sha256']
    assert str(history) in result['checked_paths']


@pytest.mark.parametrize('key',['plan_sha256','ledger_sha256','script_text_sha256','job_id',
    'task_execution_release_sha256','ledger_execution_release_sha256'])
def test_historical_exception_requires_every_frozen_registration_binding(m,fixture,historical_fixture,key):
    history,_,_=historical_fixture;value=json.loads(history.read_bytes())
    value['tasks']['rna-full/phase-worker'][key]='999' if key=='job_id' else 'f'*64;write(history,value)
    with pytest.raises(ValueError,match='historical plan/ledger/script/job/source'):discover(m,fixture)


def test_historical_exception_requires_original_executable_bytes(m,fixture,historical_fixture):
    history,_,_=historical_fixture;value=json.loads(history.read_bytes())
    value['tasks']['rna-full/phase-worker']['script_sha256']='f'*64;write(history,value)
    with pytest.raises(ValueError,match='historical executable bytes'):discover(m,fixture)


@pytest.mark.parametrize('path',['../escape/phase-worker.sbatch','other/phase-worker.sbatch',
    'plans/preserved-repair/other.sbatch','/plans/preserved-repair/phase-worker.sbatch'])
def test_historical_exception_cannot_authorize_another_path(m,fixture,historical_fixture,path):
    history,_,_=historical_fixture;value=json.loads(history.read_bytes())
    value['tasks']['rna-full/phase-worker']['script_relative_path']=path;write(history,value)
    with pytest.raises(ValueError,match='historical plan/ledger/script/job/source'):discover(m,fixture)


@pytest.mark.parametrize('phase',['managed-initial','rna-full'])
def test_original_shared_factory_command_omission_is_source_bound(m,fixture,phase):
    update_plan(m,fixture,phase,lambda plan:[task.pop('command') for task in plan['tasks']])
    roster=discover(m,fixture)
    assert roster['legacy_command_omission_source_verified'] is True
    assert {task['phase'] for task in roster['legacy_tasks_without_command']}=={phase}
    assert len(roster['legacy_tasks_without_command'])==(3 if phase=='managed-initial' else 1)
    for label,binding in roster['source_planners'].items():
        assert binding['sha256']==m.PLANNER_PINS[label]
        assert binding['plan_ast_sha256']==m.PLAN_AST_SHA and binding['add_ast_sha256']==m.ADD_AST_SHA
        assert roster['checked_paths'][binding['path']]==binding['sha256']


@pytest.mark.parametrize('command',[None,7,'','absent-from-bound-script'])
def test_present_invalid_command_never_uses_legacy_omission(m,fixture,command):
    update_plan(m,fixture,'rna-full',lambda plan:plan['tasks'][0].update(command=command))
    with pytest.raises(ValueError,match='Present registered primary command'):discover(m,fixture)


@pytest.mark.parametrize('label',['primary','framework'])
def test_changed_original_planner_cannot_authorize_omitted_command(m,fixture,label):
    update_plan(m,fixture,'rna-full',lambda plan:plan['tasks'][0].pop('command'))
    planner=fixture[2]['_planners'][label];planner.write_bytes(planner.read_bytes()+b'\n')
    with pytest.raises(ValueError,match='original shared planner bytes'):discover(m,fixture)


def test_omitted_command_cannot_authorize_nonoriginal_plan_source(m,fixture):
    def change(plan):
        plan['tasks'][0].pop('command');plan['release_identity']['sha256']='f'*64
    update_plan(m,fixture,'rna-full',change)
    with pytest.raises(ValueError,match='Exact primary source'):discover(m,fixture)


def test_partial_regression_planner_override_is_refused(m,fixture):
    with pytest.raises(ValueError,match='Both explicit regression planner routes'):
        m.primary_worker_roster(fixture[0],m.PRIMARY_RELEASE_ID,pipeline_path=fixture[1],
            planner_paths={'primary':fixture[2]['_planners']['primary']})


def test_source_ast_hash_normalizes_only_empty_cross_version_type_parameters(m):
    first=m.ast.parse('def sample():\n    return 1\n').body[0]
    expected=m.hashlib.sha256(m.ast.dump(first,include_attributes=False).replace(', type_params=[]','').encode()).hexdigest()
    assert m._ast_sha(first)==expected
    second=m.ast.parse('def sample():\n    return 1\n').body[0];second.type_params=[m.ast.Name(id='NewSyntax')]
    with pytest.raises(ValueError,match='generic source syntax'):m._ast_sha(second)


def test_complete_discovery_binds_all_sources_arrays_and_absences(m,fixture):
    roster=discover(m,fixture)
    assert roster['expected_worker_ids']==['100','101_0','101_2','101_4','200']
    assert roster['query_ids']==['100','101','200'] and roster['array_rosters']=={'101':[0,2,4]}
    assert set(roster['excluded_controllers'])=={'102'}
    assert len(roster['phases'])==2 and len(roster['absent_paths'])==40
    assert roster['source_helper']['sha256']==m.HELPER_SHA and roster['source_pipeline']['sha256']==m.PIPELINE_SHA
    assert all(roster[key] is False for key in m.FALSE_FLAGS)


@pytest.mark.parametrize('change',['source','config','plan_hash','task_missing','unsubmitted','duplicate_parent','script','array_ledger'])
def test_registration_mismatch_refuses(m,fixture,change):
    _,_,paths,_=fixture;pp=paths['managed-initial']['plan'];lp=paths['managed-initial']['ledger']
    plan=json.loads(pp.read_bytes());ledger=json.loads(lp.read_bytes())
    if change=='source':ledger['source_sha256']='f'*64
    elif change=='config':plan['config_sha256']='f'*64
    elif change=='plan_hash':ledger['plan_sha256']='f'*64
    elif change=='task_missing':ledger['tasks'].pop('worker-scalar')
    elif change=='unsubmitted':ledger['tasks']['worker-scalar']['state']='submitting'
    elif change=='duplicate_parent':ledger['tasks']['worker-array']['job_id']='100'
    elif change=='script':Path(plan['tasks'][0]['script']).write_text('# changed\n')
    else:ledger['tasks']['worker-array']['resources']={'array':'0-2'}
    if change not in {'plan_hash','script'}:ledger['plan_sha256']=m.canonical(plan)
    write(pp,plan);write(lp,ledger)
    with pytest.raises(ValueError):discover(m,fixture)


@pytest.mark.parametrize('change',['missing','planning','submitting','source','plan_hash','ledger_path'])
def test_incomplete_phase_journal_refuses(m,fixture,change):
    jp=fixture[2]['rna-full']['journal'];record=json.loads(jp.read_bytes())
    if change=='missing':jp.unlink()
    else:
        if change in {'planning','submitting'}:record['state']=change
        elif change=='source':record['source_sha256']='f'*64
        elif change=='plan_hash':record['plan_sha256']='f'*64
        else:record['ledger']='foreign-ledger.json'
        write(jp,record)
    with pytest.raises(ValueError):discover(m,fixture)


def test_unregistered_phase_directory_or_journal_is_ambiguous(m,fixture):
    root=fixture[0];directory=root/'plans'/(m.PRIMARY_RELEASE_ID+'-behaviour');directory.mkdir()
    with pytest.raises(ValueError,match='Unfinished'):discover(m,fixture)
    directory.rmdir();write(root/'state/pipeline'/m.PRIMARY_RELEASE_ID/'behaviour-plan-journal.json',{'state':'planning'})
    with pytest.raises(ValueError,match='Incomplete'):discover(m,fixture)


def test_unknown_primary_phase_journal_cannot_hide_an_unbound_submission(m,fixture):
    root=fixture[0]
    write(root/'state/pipeline'/m.PRIMARY_RELEASE_ID/'unknown-plan-journal.json',{'state':'submitted'})
    with pytest.raises(ValueError,match='Unknown primary phase'):discover(m,fixture)


@pytest.mark.parametrize('expression',['0-4%2','0,2-6:2','4'])
def test_finite_original_pipeline_array_grammar(m,expression):
    expected={'0-4%2':[0,1,2,3,4],'0,2-6:2':[0,2,4,6],'4':[4]}
    assert m.array_indices(expression)==expected[expression]


@pytest.mark.parametrize('expression',['','4-0','0-4:0','0-10001','0-2,2','0-3%bad','0-3%0'])
def test_ambiguous_or_unbounded_array_refuses(m,expression):
    with pytest.raises(ValueError):m.array_indices(expression)


def rows(roster):
    return {ident:{'job_id_raw':ident if '_' not in ident else str(1000+index),'user':'pwa209','account':'def-ptewarie_cpu',
        'state':'COMPLETED','exit_code':'0:0','cpus':1,'elapsed_seconds':10,'unallocated_metadata':False,
        'alloc_tres':'cpu=1,mem=1G,node=1','node_list':'native-node','start':'2026-10-24T10:00:00',
        'end':'2026-10-24T10:00:10','max_rss':'1024K'} for index,ident in enumerate(roster['expected_worker_ids'])}


def gpu_fixture(m,fixture):
    update_plan(m,fixture,'managed-initial',lambda plan:plan['tasks'][1]['resources'].update(gpu=True))
    roster=discover(m,fixture);native=rows(roster)
    for ident in roster['expected_worker_ids']:
        if ident.startswith('101_'):native[ident].update(account='def-ptewarie_gpu',alloc_tres='cpu=1,mem=1G,node=1,gres/gpu=1')
    return roster,native


def test_mixed_frozen_cpu_gpu_roster_requires_each_actual_account(m,fixture):
    roster,native=gpu_fixture(m,fixture);proof=m.verify_primary_terminal(roster,native)
    assert proof['workers']==5 and proof['allocated_terminal_workers']==5
    assert proof['expected_accounts']=={ident:('def-ptewarie_gpu' if ident.startswith('101_') else 'def-ptewarie_cpu') for ident in native}
    with pytest.raises(ValueError,match='Explicit account conflicts'):
        m.verify_primary_terminal(roster,native,account='def-ptewarie_cpu')


@pytest.mark.parametrize('ident,account',[('100','def-ptewarie_gpu'),('101_0','def-ptewarie_cpu')])
def test_terminal_wrong_account_cannot_cross_frozen_resource_identity(m,fixture,ident,account):
    roster,native=gpu_fixture(m,fixture);native[ident]['account']=account
    with pytest.raises(ValueError,match='Owned exact terminal'):m.verify_primary_terminal(roster,native)


def test_gpu_never_allocated_cancelled_members_keep_exact_zero_cost(m,fixture):
    roster,native=gpu_fixture(m,fixture)
    for ident in ['101_0','101_2','101_4']:
        native[ident].update(job_id_raw='101',state='CANCELLED',exit_code='0:0',cpus=0,elapsed_seconds=0,
            unallocated_metadata=True,alloc_tres='',node_list='None assigned',start='Unknown',max_rss='0')
    proof=m.verify_primary_terminal(roster,native)
    assert proof['allocated_terminal_workers']==2 and proof['explicit_unallocated_cancelled_workers']==3
    assert proof['expected_accounts']['101_0']=='def-ptewarie_gpu'


def test_array_summary_account_is_bound_to_same_frozen_gpu_parent(m,fixture):
    roster,native=gpu_fixture(m,fixture);native['101']=copy.deepcopy(native['101_0'])
    m.verify_primary_terminal(roster,native);native['101']['account']='def-ptewarie_cpu'
    with pytest.raises(ValueError,match='Foreign array summary'):m.verify_primary_terminal(roster,native)


@pytest.mark.parametrize('value',[None,'false',1])
def test_ambiguous_frozen_gpu_resource_identity_refuses(m,fixture,value):
    update_plan(m,fixture,'rna-full',lambda plan:plan['tasks'][0]['resources'].update(gpu=value))
    with pytest.raises(ValueError,match='CPU/GPU resource identity'):discover(m,fixture)


def test_every_member_positive_terminal_preserves_failed_native_cost(m,fixture):
    roster=discover(m,fixture);native=rows(roster);native['100'].update(state='FAILED',exit_code='1:0')
    proof=m.verify_primary_terminal(roster,native)
    assert proof['workers']==5 and proof['allocated_terminal_workers']==5
    assert proof['native_records']['100']['state']=='FAILED' and proof['native_records']['100']['exit_code']=='1:0'
    assert native['100']['elapsed_seconds']==10 and proof['native_records_sha256']==m.canonical(native)


@pytest.mark.parametrize('change',['missing_member','parent_only','running','owner','account','duplicate_physical','zero_completed','bad_zero','scalar_physical','foreign'])
def test_terminal_proof_rejects_missing_foreign_active_or_ambiguous_allocation(m,fixture,change):
    roster=discover(m,fixture);native=rows(roster)
    if change=='missing_member':native.pop('101_2')
    elif change=='parent_only':native={k:v for k,v in native.items() if '_' not in k};native['101']=copy.deepcopy(native['100'])
    elif change=='running':native['100']['state']='RUNNING'
    elif change=='owner':native['100']['user']='foreign'
    elif change=='account':native['100']['account']='foreign'
    elif change=='duplicate_physical':native['101_2']['job_id_raw']=native['101_0']['job_id_raw']
    elif change=='zero_completed':native['100'].update(cpus=0,elapsed_seconds=0)
    elif change=='bad_zero':native['100'].update(state='CANCELLED',cpus=0,elapsed_seconds=0)
    elif change=='scalar_physical':native['100']['job_id_raw']='999'
    else:native['999']=copy.deepcopy(native['100'])
    with pytest.raises(ValueError):m.verify_primary_terminal(roster,native)


def test_explicit_never_started_cancelled_array_templates_preserve_zero_cost(m,fixture):
    roster=discover(m,fixture);native=rows(roster)
    for ident in ['101_0','101_2','101_4']:
        native[ident].update(job_id_raw='101',state='CANCELLED',exit_code='0:0',cpus=0,elapsed_seconds=0,
            unallocated_metadata=True,alloc_tres='',node_list='None assigned',start='Unknown',max_rss='0')
    proof=m.verify_primary_terminal(roster,native)
    assert proof['allocated_terminal_workers']==2 and proof['explicit_unallocated_cancelled_workers']==3


@pytest.mark.parametrize('ident,marker',[('100',True),('101_0',False)])
def test_zero_allocation_scalar_and_array_template_classifications_must_match(m,fixture,ident,marker):
    roster=discover(m,fixture);native=rows(roster);parent=roster['workers'][ident]['parent_job_id']
    native[ident].update(job_id_raw=parent,state='CANCELLED',exit_code='0:0',cpus=0,elapsed_seconds=0,
        unallocated_metadata=marker,alloc_tres='',node_list='None assigned',start='Unknown',max_rss='0')
    with pytest.raises(ValueError,match='never-started'):m.verify_primary_terminal(roster,native)


def test_source_pin_duplicate_json_and_symlink_refuse(m,fixture):
    root,pipeline,paths,_=fixture;helper=root/m.HELPER;raw=helper.read_bytes();helper.write_bytes(raw+b'\n')
    with pytest.raises(ValueError,match='helper'):discover(m,fixture)
    helper.write_bytes(raw);pp=paths['managed-initial']['plan'];raw=pp.read_bytes();pp.write_bytes(b'{"phase":1,"phase":2}')
    with pytest.raises(ValueError,match='Duplicate'):discover(m,fixture)
    pp.write_bytes(raw)
    if hasattr(os,'symlink'):
        target=pp.with_name('copy.json');target.write_bytes(raw);pp.unlink()
        try:pp.symlink_to(target)
        except OSError:
            pp.write_bytes(raw);return
        with pytest.raises(ValueError,match='symlink'):discover(m,fixture)


def test_stable_regular_read_requires_actual_owner(m,tmp_path):
    path=tmp_path/'metadata.json';path.write_text('{}');uid=tmp_path.stat().st_uid
    checked={};assert m._read(path,tmp_path,uid,checked)==b'{}'
    assert checked[str(path)]==m.hashlib.sha256(b'{}').hexdigest()
    with pytest.raises(ValueError,match='Owned'):m._read(path,tmp_path,uid+1,{})


def test_stable_read_detects_post_read_length_change(m,tmp_path,monkeypatch):
    path=tmp_path/'metadata.json';path.write_text('{}');original=m.os.fstat;calls=[0]
    def changing(fd):
        calls[0]+=1
        if calls[0]==2:
            with path.open('ab') as stream:stream.write(b' ')
        return original(fd)
    monkeypatch.setattr(m.os,'fstat',changing)
    with pytest.raises(ValueError,match='during read'):m._read(path,tmp_path,tmp_path.stat().st_uid,{})
