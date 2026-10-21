"""Synthetic integrity fixtures; none are pretrained or scientific evidence."""
import copy
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from horsehair import agents, agent_analysis as old, agent_analysis_v2 as bridge, agent_execution_v2 as execution
from horsehair.core import atomic_json, canonical_hash, digest, read_json


@pytest.fixture
def context(tmp_path, monkeypatch):
    config=read_json(Path(__file__).resolve().parents[1]/'configs/study.json')
    root=tmp_path.resolve();config['paths']['root']=str(root)
    monkeypatch.setattr(bridge,'PERSONAL_ROOT',root)
    monkeypatch.setattr(bridge,'BASE_CONFIG_SHA256',canonical_hash(config))
    settings=config['agents']
    protocol={'config_sha256':agents._digest(settings),'full_config_sha256':canonical_hash(config),
        'implementation_sha256':bridge.AGENTS_SHA256,'H4_inference_specification':agents.h4_specification(config),
        'final_seeds':settings['final_seeds'],'learning_rate':.001,'biological_directions':None,
        'heldout_architecture':'modular','heldout_task':'delayed_choice',
        'directions':{f'{m}:{rank}':(np.ones(rank)/np.sqrt(rank)).tolist() for m in agents.MECHANISMS for rank in settings['ranks'] if m!='motor_gain' or rank==1}}
    campaign_case=root/'maintenance/campaign'
    cells=execution.final_cells(config)
    for cell in cells:
        name=agents._cell_name(cell['task'],cell['architecture'],cell['seed'])
        cell.update(training_case=str(agents._directory(root,settings)/'final'/name),evaluation_case=str(campaign_case/f"cell-{cell['index']:04d}"))
    campaign={'config':config,'protocol':protocol,'protocol_sha256':agents._digest(protocol),'case':str(campaign_case),
        'source_binding':{'primary_source_sha256':bridge.PRIMARY_SCIENTIFIC_SOURCE_SHA256,
            'primary_agents_sha256':bridge.AGENTS_SHA256,'evaluation_module_sha256':bridge.EVALUATION_SHA256,'execution_source_sha256':'a'*64},
        'cells':cells,'conditions':execution._conditions(settings,protocol),'scientific_result':False,'admission_authority':False}
    campaign['plan_sha256']=canonical_hash(campaign)
    plan={'root':str(root),'config':config,'campaign':campaign,'cells':cells,'original_protocol':protocol,
          'inference':protocol['H4_inference_specification'],'source_bindings':bridge._sources()}
    return root,config,protocol,campaign,plan


def seal(campaign):
    campaign.pop('plan_sha256',None);campaign['plan_sha256']=canonical_hash(campaign)


def test_frozen_modules_and_exact_original_rosters(context):
    root,config,protocol,campaign,plan=context
    assert bridge._campaign(config,protocol,campaign,root)==campaign['cells']
    assert len(campaign['cells'])==240 and len(campaign['conditions'])==300
    assert {c['seed'] for c in campaign['cells']}==set(range(1000,1020))
    assert sum(c['mechanism'] in old.PRIMARY for c in campaign['conditions'])*12==2304
    assert bridge._sources()['statistics_module_sha256']==digest(Path(old.__file__))


@pytest.mark.parametrize('change',['missing_policy','duplicate_policy','reduced_grid','source','protocol','flags','seal'])
def test_entire_original_grid_and_source_cannot_be_waived(context,change):
    root,config,protocol,campaign,_=context;campaign=copy.deepcopy(campaign)
    if change=='missing_policy':campaign['cells'].pop()
    elif change=='duplicate_policy':campaign['cells'][1]=campaign['cells'][0]
    elif change=='reduced_grid':campaign['conditions'].pop()
    elif change=='source':campaign['source_binding']['evaluation_module_sha256']='f'*64
    elif change=='protocol':campaign['protocol']['learning_rate']=.003
    elif change=='flags':campaign['scientific_result']=True
    else:campaign['plan_sha256']='f'*64
    if change!='seal':seal(campaign)
    with pytest.raises(ValueError):bridge._campaign(config,protocol,campaign,root)


def final_plan(plan,cell):
    config,protocol=plan['config'],plan['original_protocol']
    return {'config':config,'full_config_sha256':bridge.BASE_CONFIG_SHA256,'original_protocol':protocol,
        'original_protocol_sha256':agents._digest(protocol),'evaluation_settings':execution._settings(config),
        'canary':False,'canary_overrides':None,'benchmark_directions':False,'evaluation_dtype':'float64',
        'amendment':execution.AMENDMENT,'numerical_difference':execution.DIFFERENCE,
        'execution_module_sha256':bridge.EVALUATION_SHA256,'primary_agents_sha256':bridge.AGENTS_SHA256,
        'conditions':plan['campaign']['conditions'],'original_training_steps':config['agents']['total_steps'],
        **{k:cell[k] for k in ['task','architecture','seed']},'checkpoint':str(Path(cell['training_case'])/'checkpoint.pt')}


@pytest.mark.parametrize('change',['dtype','source','protocol','checkpoint','steps','conditions','canary','benchmark'])
def test_final_precision_manifest_cannot_hide_a_different_experiment(context,change):
    _,_,_,_,plan=context;cell=plan['cells'][0];value=copy.deepcopy(final_plan(plan,cell))
    bridge._final_plan(plan,cell,value)
    if change=='dtype':value['evaluation_dtype']='float32'
    elif change=='source':value['execution_module_sha256']='f'*64
    elif change=='protocol':value['original_protocol']['learning_rate']=.003
    elif change=='checkpoint':value['checkpoint']=str(Path(cell['training_case']).parent/'other.pt')
    elif change=='steps':value['original_training_steps']=32768
    elif change=='conditions':value['conditions'].pop()
    elif change=='canary':value['canary']=True
    else:value['benchmark_directions']=True
    with pytest.raises(ValueError,match='Final amended plan'):bridge._final_plan(plan,cell,value)


def trial_frame(seed=1000,n=3,protocol=None):
    protocol=protocol or {'biological_directions':None,'evaluation_amendment':execution.AMENDMENT}
    settings={'ranks':[1],'strengths':[.2],'evaluation_episodes':n}
    rows=[]
    for rule,mechanism,rank,arm,strength in old._conditions(settings,protocol):
        for probe,control in old._assays():
            for episode in range(n):
                rows.append({'architecture':'rnn','task':'foraging','training_seed':seed,'rule_type':rule,
                    'perturbation':mechanism,'rank':rank,'matching_arm':arm,'strength':strength,'probe':probe,
                    'restoration_control':control,'episode_seed':40_000_000+seed*1000+episode,
                    'success':1,'redirection':int(control=='none' and mechanism in old.PRIMARY),
                    'motor_accuracy':1.,'baseline_success':1.,'baseline_redirection':0.,'baseline_motor_accuracy':1.,
                    'achieved_parameter_norm':strength,'clean_policy_kl':strength**2/2,'target_policy_kl':strength**2/2,
                    'protocol_sha256':agents._digest(protocol),'heldout_condition':True,'effective_coefficients':rank,
                    'numerical_status':'available','calibration_numerical_status':'available','numerical_failure_reason':None})
    return pd.DataFrame(rows),settings,protocol


def test_original_endpoint_validation_and_summary_are_used_exactly():
    frame,settings,protocol=trial_frame()
    summary,controls=bridge._trial_summary(frame,settings,protocol,architecture='rnn',task='foraging',seed=1000)
    pd.testing.assert_frame_equal(summary,old.summarize_trials(frame),check_exact=True)
    assert len(summary)==14 and len(controls)==56
    assert set(frame.protocol_sha256)=={agents._digest(protocol)}
    assert agents._digest(protocol)!=agents._digest({'biological_directions':None})


@pytest.mark.parametrize('change',['missing','duplicate','nonfinite','null','calibration','restoration','old_protocol','seed'])
def test_missing_or_numerically_unavailable_trials_are_never_filled(context,change):
    frame,settings,protocol=trial_frame()
    if change=='missing':frame=frame.iloc[1:]
    elif change=='duplicate':frame=pd.concat([frame,frame.iloc[:1]])
    elif change=='nonfinite':
        frame['success']=frame.success.astype(float);frame.loc[0,'success']=np.inf
    elif change=='null':
        frame.loc[0,['success','redirection','motor_accuracy']]=np.nan
        frame.loc[0,['numerical_status','numerical_failure_reason']]=['unavailable','nonfinite_logits']
    elif change=='calibration':frame.loc[0,'calibration_numerical_status']='unavailable'
    elif change=='restoration':frame.loc[frame.restoration_control.eq('exact'),'redirection']=1
    elif change=='old_protocol':frame['protocol_sha256']=agents._digest({'biological_directions':None})
    else:frame.loc[0,'episode_seed']+=7
    with pytest.raises(ValueError):bridge._trial_summary(frame,settings,protocol,architecture='rnn',task='foraging',seed=1000)


def seed_summaries():
    frames=[]
    for seed in range(1000,1020):
        frame,_,_=trial_frame(seed)
        frames.append(old.summarize_trials(frame))
    return pd.concat(frames,ignore_index=True)


def test_statistics_are_identical_including_seed_pairs_and_unqualified_points():
    data=seed_summaries();settings={'final_seeds':list(range(1000,1020)),'architectures':['rnn']}
    data.loc[data.training_seed.eq(1000)&data.perturbation.eq('random_subspace'),'strength_matched']=False
    inference={'bootstrap_replicates':31}
    actual,paired=bridge.statistics(data,settings,inference)
    assert actual==old.analyze_seed_summaries(data,settings,inference)
    assert paired==old._paired_architecture_contrasts(data,settings,inference)
    primary=[r for r in actual if r['primary_grid_point']]
    assert all(r['holm_pvalue']==1 and not r['operating_region_supported'] for r in primary)
    assert all(r['comparators']['random_subspace']['independent_seed_pairs']==20 for r in actual)


@pytest.mark.parametrize('qualified,supported,expected',[(True,False,'not_supported'),(False,True,'pending'),(True,True,'supported')])
def test_actual_negative_and_unqualified_science_are_preserved(context,qualified,supported,expected):
    _,config,_,_,plan=context
    records=[]
    reference=plan['inference']['agent_reference_point']
    for cell in plan['cells']:
        records.append({**reference,'task':cell['task'],'architecture':cell['architecture'],'training_seed':cell['seed'],
            'baseline_main_success':1. if qualified else .5,**{f'baseline_{p}':1. for p in agents.PROBES},
            **{outcome:0. for outcome in old.OUTCOMES}})
    summaries=pd.DataFrame(records)
    curves=[{'primary_grid_point':i<2304,'independent_seeds':20,'operating_region_supported':bool(supported and i==0),
             'available_for_H4':qualified,'holm_pvalue':.01 if supported else 1.,'task':'foraging','architecture':'rnn'} for i in range(3600)]
    result=bridge._result(plan,summaries,curves)
    assert result['claim_status']==expected and result['computational_claim_accepted']==(expected=='supported')
    assert result['biological_transfer_status']=='pending' and not result['biological_constraints_available']
    assert result['correction']['tests']==2304 and result['independent_seed_count_per_cell']==20
    assert result['estimate']==0 and result['reference_point']==reference


def test_immutable_bindings_confinement_and_exclusive_output(context,tmp_path):
    root,*_=context
    file=root/'maintenance/evidence.json';atomic_json(file,{'status':'fixture'})
    binding=bridge._locator(root,file);assert bridge._bound(root,binding)=={'status':'fixture'}
    atomic_json(file,{'status':'changed'})
    with pytest.raises(ValueError,match='Immutable evidence'):bridge._bound(root,binding)
    with pytest.raises(ValueError):bridge._confined(root,root.parent/'outside.json')
    target=root/'new.json';bridge._exclusive_json(target,{'a':1})
    with pytest.raises(FileExistsError):bridge._exclusive_json(target,{'a':2})
    assert read_json(target)=={'a':1}


def test_cli_requires_independently_pinned_analysis_plan(tmp_path):
    with pytest.raises(ValueError,match='independent immutable analysis-plan SHA'):
        bridge.main(['freeze-inputs','--case',str(tmp_path/'new')])
    assert not (tmp_path/'new').exists()


@pytest.fixture
def qualified_evidence(context,monkeypatch):
    root,config,protocol,campaign,plan=context
    area=root/'maintenance/precision';area.mkdir(parents=True)
    precision={'config':config,'original_protocol':protocol,'canary':True,'benchmark_directions':True,
        'canary_overrides':{'ranks':[1,4],'strengths':[0,.4],'evaluation_episodes':4,'calibration_episodes':32,'kl_bisections':10},
        'task':'delayed_choice','architecture':'gru','seed':900,'conditions':list(range(52))}
    atomic_json(area/'plan.json',precision);atomic_json(area/'plan-binding.json',{'plan_sha256':digest(area/'plan.json')})
    monkeypatch.setattr(execution,'_load_plan',lambda *a,**kw:copy.deepcopy(precision))
    qualification={'status':'complete','returncode':0,'source_sha256':plan['source_bindings']['execution_source_sha256'],
        'config_sha256':bridge.BASE_CONFIG_SHA256,'source_verified_unchanged':True,'slurm_job_id':'100',
        'test_counts':{'tests':1737,'failures':0,'errors':0,'skipped':0},'test_selection':bridge.FULL_TEST_SELECTION,
        'workspace':str(area/'qualification-workspace')}
    workspace=Path(qualification['workspace'])
    qualification['command']=['python','-u','-m','pytest',str(Path(bridge.__file__).resolve().parents[2]/'tests'),'-v',
        '--color=no','--basetemp',str(workspace/'pytest'),'--junitxml',str(workspace/'junit.xml'),
        '-o','cache_dir='+str(workspace/'cache'),'-o','faulthandler_timeout=120']
    junit=area/'junit.xml'
    junit.write_text('<testsuites><testsuite tests="1737" failures="0" errors="0" skipped="0">'+
        ''.join(f'<testcase classname="synthetic_fixture" name="case_{i}"/>' for i in range(1737))+'</testsuite></testsuites>',encoding='utf-8')
    qualification['artifacts']={'junit':{'path':str(junit),'sha256':digest(junit),'bytes':junit.stat().st_size}}
    reports=[{'dtype':dtype,'condition_index':i,'status':'unavailable' if dtype=='torch.float32' and i==0 else 'available',
              'unavailable_rows':1 if dtype=='torch.float32' and i==0 else 0} for dtype in ['torch.float32','torch.float64'] for i in range(52)]
    base={'status':'complete_native_work','source_sha256':campaign['source_binding']['execution_source_sha256'],
          'full_config_sha256':bridge.BASE_CONFIG_SHA256,'scientific_result':False,'admission_authority':False}
    diagnostics={**base,'mode':'precision-diagnostics','native_job_id':'101','float32_failure_reproduced':True,
        'float64_all_canary_conditions_available':True,'result':{'plan_sha256':digest(area/'plan.json'),'reports':reports}}
    assembly={**base,'mode':'precision-assemble','native_job_id':'102','result':{'status':'complete_precision_canary',
        'plan_sha256':digest(area/'plan.json'),'canary':True,'conditions':52,'trial_rows':1664,
        'scientific_result':False,'admission_authority':False}}
    values={'qualification':qualification,'precision_plan':precision,'precision_binding':{'plan_sha256':digest(area/'plan.json')},
            'precision_diagnostics':diagnostics,'precision_assembly':assembly}
    paths={'precision_plan':area/'plan.json','precision_binding':area/'plan-binding.json',
        **{key:area/(key+'.json') for key in ['qualification','precision_diagnostics','precision_assembly']}}
    records={job:{'state':'COMPLETED','exit_code':'0:0','basis':'sacct final allocated wall time','elapsed_seconds':1,'cpus':4} for job in ['100','101','102']}
    def save():
        for key,value in values.items():atomic_json(paths[key],value)
        atomic_json(root/'state/resource-budget.json',{'native_jobs':records})
        return {key:bridge._locator(root,path) for key,path in paths.items()}
    return values,records,save


@pytest.mark.parametrize('change',['none','qualification_source','qualification_skip','qualification_native','qualification_single_test',
    'qualification_selector','qualification_concatenated_selector','qualification_ignore','qualification_positional',
    'qualification_short_roster','canary_plan',
    'f32_not_reproduced','f64_unavailable','duplicate_report','wrong_assembly','native_failed'])
def test_independent_native_qualification_and_paired_precision_gates(context,qualified_evidence,change):
    root,config,_,campaign,plan=context;values,records,save=qualified_evidence
    if change=='qualification_source':values['qualification']['source_sha256']='f'*64
    elif change=='qualification_skip':values['qualification']['test_counts']['skipped']=1
    elif change=='qualification_native':records.pop('100')
    elif change=='qualification_single_test':values['qualification']['test_counts']['tests']=1
    elif change=='qualification_selector':values['qualification']['command'].append('--lf')
    elif change=='qualification_concatenated_selector':values['qualification']['command'].append('-kcase')
    elif change=='qualification_ignore':values['qualification']['command'].append('--ignore=tests/test_agent_analysis_v2.py')
    elif change=='qualification_positional':values['qualification']['command'].append('tests/test_agent_analysis_v2.py')
    elif change=='qualification_short_roster':
        artifact=values['qualification']['artifacts']['junit'];path=Path(artifact['path'])
        path.write_text('<testsuites><testsuite tests="1737"><testcase name="only_one"/></testsuite></testsuites>',encoding='utf-8')
        artifact['sha256']=digest(path)
    elif change=='canary_plan':values['precision_plan']['canary']=False
    elif change=='f32_not_reproduced':
        for row in values['precision_diagnostics']['result']['reports']:
            if row['dtype']=='torch.float32':row.update(status='available',unavailable_rows=0)
    elif change=='f64_unavailable':values['precision_diagnostics']['result']['reports'][-1].update(status='unavailable',unavailable_rows=1)
    elif change=='duplicate_report':values['precision_diagnostics']['result']['reports'][-1]['condition_index']=0
    elif change=='wrong_assembly':values['precision_assembly']['result']['trial_rows']=1663
    elif change=='native_failed':records['102'].update(state='FAILED',exit_code='1:0')
    evidence=save()
    if change=='none':bridge._evidence(config,root,campaign,plan['source_bindings'],evidence)
    else:
        with pytest.raises(ValueError):bridge._evidence(config,root,campaign,plan['source_bindings'],evidence)


def test_analysis_plan_is_separate_immutable_and_missing_inputs_cannot_overwrite_legacy(context,qualified_evidence):
    root,config,protocol,campaign,_=context;_,_,save=qualified_evidence;evidence=save()
    protocol_path=agents._directory(root,config['agents'])/'frozen_protocol.json';atomic_json(protocol_path,protocol)
    campaign_path=root/'maintenance/campaign-plan.json';atomic_json(campaign_path,campaign)
    legacy=root/'results/inference/H4.json';atomic_json(legacy,{'historical':'preserved'});legacy_bytes=legacy.read_bytes()
    case=root/'results/amended-h4/new-analysis'
    plan=bridge.freeze_plan(config,root,campaign_path,case,campaign_plan_sha256=digest(campaign_path),
        original_protocol=protocol_path,original_protocol_sha256=digest(protocol_path),evidence=evidence)
    pinned=digest(case/'analysis-plan.json')
    assert bridge._load(case,pinned)==plan
    with pytest.raises(ValueError,match='Missing source-bound'):bridge.freeze_inputs(case,expected_plan_sha256=pinned)
    assert not (case/'inputs.json').exists() and legacy.read_bytes()==legacy_bytes
    changed=copy.deepcopy(plan);changed['inference']['alpha']=.5;atomic_json(case/'analysis-plan.json',changed)
    with pytest.raises(ValueError,match='Immutable analysis plan'):bridge._load(case,pinned)


def test_missing_policy_files_record_full_incomplete_audit_and_no_result(context,monkeypatch):
    root,_,_,_,plan=context;case=root/'results/amended-h4/missing';case.mkdir(parents=True)
    atomic_json(case/'analysis-plan.json',{'unit_test':'isolated validator fan-in'})
    manifest={'analysis_plan_sha256':digest(case/'analysis-plan.json'),'cells':[{'index':i} for i in range(240)]}
    atomic_json(case/'inputs.json',manifest);atomic_json(case/'inputs-binding.json',{'sha256':digest(case/'inputs.json')})
    monkeypatch.setattr(bridge,'_load',lambda *a,**kw:plan)
    with pytest.raises(ValueError,match='remains incomplete'):bridge.run(case,inputs_sha256=digest(case/'inputs.json'))
    audit=read_json(case/'validation-audit.json')
    assert audit['verified_policy_cells']==0 and len(audit['failures'])==240
    assert not (case/'analysis').exists()
    assert audit['scientific_result'] is False and audit['admission_authority'] is False


def test_real_full300_condition_cell_revalidates_parts_checkpoint_and_assembly(context):
    torch=pytest.importorskip('torch')
    root,config,protocol,_,plan=context;cell=plan['cells'][0]
    training=Path(cell['training_case']);training.mkdir(parents=True)
    policy=agents.make_policy(cell['architecture'],config['agents']['capacity'],config['agents']['observation_window'])
    checkpoint=training/'checkpoint.pt';settings=config['agents']
    torch.save({'config':settings,'config_sha256':agents._digest(settings),'implementation_sha256':bridge.AGENTS_SHA256,
        'state_dict':policy.state_dict(),'weight_sha256':agents.state_hash(policy),'steps':1000000,'training_budget':1000000,
        'seed':cell['seed'],'architecture':cell['architecture'],'task':cell['task'],'learning_rate':.001},checkpoint)
    summary={'seed':cell['seed'],'architecture':cell['architecture'],'task':cell['task'],'training_steps':1000000,
        'complete_training_budget':True,'config_sha256':agents._digest(settings),'implementation_sha256':bridge.AGENTS_SHA256,
        'learning_rate':.001,'checkpoint_sha256':digest(checkpoint),'weight_sha256':agents.state_hash(policy)}
    atomic_json(training/'summary.json',summary)
    case=Path(cell['evaluation_case'])
    value=execution.prepare_plan(config,protocol,checkpoint,case,checkpoint_sha256=digest(checkpoint),
        primary_agents_sha256=bridge.AGENTS_SHA256,task=cell['task'],architecture=cell['architecture'],seed=cell['seed'])
    promoted=agents.state_hash(execution._precision_policy(policy,torch.float64))
    for i,condition in enumerate(value['conditions']):
        rows=[]
        for probe,control in old._assays():
            for episode in range(64):
                rows.append({'architecture':cell['architecture'],'task':cell['task'],'training_seed':cell['seed'],
                    'rule_type':condition['rule_type'],'perturbation':condition['mechanism'],'rank':condition['rank'],
                    'matching_arm':condition['matching_arm'],'strength':condition['strength'],'probe':probe,'restoration_control':control,
                    'episode_seed':40_000_000+cell['seed']*1000+episode,'success':1,'redirection':0,'motor_accuracy':1.,
                    'baseline_success':1.,'baseline_redirection':0.,'baseline_motor_accuracy':1.,
                    'achieved_parameter_norm':condition['strength'],'clean_policy_kl':condition['strength']**2/2,
                    'target_policy_kl':condition['strength']**2/2,'protocol_sha256':value['evaluation_protocol_sha256'],
                    'heldout_condition':True,'effective_coefficients':condition['rank'],'numerical_status':'available',
                    'calibration_numerical_status':'available','numerical_failure_reason':None})
        artifact=case/f'condition-{i:05d}-fixture.parquet';pd.DataFrame(rows).to_parquet(artifact,index=False)
        restoration={**{k:condition[k] for k in ['mechanism','rank','strength','matching_arm']},'base_weights_unchanged':True,
            'intervened_probability_status':'available','restoration_max_probability_error':0.,'altered_max_probability_change':0.}
        atomic_json(case/f'condition-{i:05d}-fixture.json',{'status':'complete','plan_sha256':digest(case/'plan.json'),
            'condition_index':i,'condition':condition,'artifact_name':artifact.name,'artifact_sha256':digest(artifact),
            'rows':512,'restoration':restoration,'calibration_numerical_diagnostics':[],
            'original_weight_sha256':summary['weight_sha256'],'evaluation_weight_sha256':promoted,'slurm_job_id':'1'})
    assembly=execution.assemble(case,expected_plan_sha256=digest(case/'plan.json'))
    entry=bridge._entry(root,cell)
    summaries,controls=bridge._validate_cell(plan,cell,entry)
    assert len(summaries)==300 and len(controls)==1200
    assert (summaries.redirection_difference==0).all()
    part=case/'condition-00000-fixture.parquet';previous=part.read_bytes();part.write_bytes(b'corrupted')
    with pytest.raises(ValueError,match='Committed condition file changed'):bridge._validate_cell(plan,cell,entry)
    part.write_bytes(previous)
    receipt=case/'condition-00000-fixture.json';old_receipt=receipt.read_bytes();receipt.rename(case/'missing-receipt.saved')
    with pytest.raises(ValueError,match='Every original condition'):bridge._validate_cell(plan,cell,entry)
    (case/'missing-receipt.saved').rename(receipt);assert receipt.read_bytes()==old_receipt
    before=checkpoint.read_bytes();checkpoint.write_bytes(b'corrupted')
    with pytest.raises(ValueError,match='manifest changed'):bridge._validate_cell(plan,cell,entry)
    checkpoint.write_bytes(before)
    # The assembled file is independently checked against the committed parts,
    # even if a caller pins its changed bytes and changes the summary accordingly.
    assembled=case/assembly['artifact_name'];frame=pd.read_parquet(assembled);frame.loc[0,'redirection']=1
    frame.to_parquet(assembled,index=False);assembly['trial_file_sha256']=digest(assembled)
    atomic_json(case/'evaluation_summary.json',assembly);changed=bridge._entry(root,cell)
    with pytest.raises(AssertionError):bridge._validate_cell(plan,cell,changed)
