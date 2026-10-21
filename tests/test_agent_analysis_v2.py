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


@pytest.fixture
def complete_policy(context):
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
        stem=f'condition-{i:05d}-{i+1:032x}'
        artifact=case/(stem+'.parquet');pd.DataFrame(rows).to_parquet(artifact,index=False)
        restoration={**{k:condition[k] for k in ['mechanism','rank','strength','matching_arm']},'base_weights_unchanged':True,
            'intervened_probability_status':'available','restoration_max_probability_error':0.,'altered_max_probability_change':0.}
        atomic_json(case/(stem+'.json'),{'status':'complete','plan_sha256':digest(case/'plan.json'),
            'condition_index':i,'condition':condition,'artifact_name':artifact.name,'artifact_sha256':digest(artifact),
            'rows':512,'restoration':restoration,'calibration_numerical_diagnostics':[],
            'original_weight_sha256':summary['weight_sha256'],'evaluation_weight_sha256':promoted,'slurm_job_id':'1'})
    assembly=execution.assemble(case,expected_plan_sha256=digest(case/'plan.json'))
    return plan,cell,checkpoint,value,assembly


def test_real_full300_condition_cell_revalidates_parts_checkpoint_and_assembly(context,complete_policy):
    root,*_=context;plan,cell,checkpoint,value,assembly=complete_policy;case=Path(cell['evaluation_case'])
    entry=bridge._entry(root,cell)
    summaries,controls=bridge._validate_cell(plan,cell,entry)
    assert len(summaries)==300 and len(controls)==1200
    assert (summaries.redirection_difference==0).all()
    part=case/f'condition-00000-{1:032x}.parquet';previous=part.read_bytes();part.write_bytes(b'corrupted')
    with pytest.raises(ValueError,match='Committed condition file changed'):bridge._validate_cell(plan,cell,entry)
    part.write_bytes(previous)
    receipt=case/f'condition-00000-{1:032x}.json';old_receipt=receipt.read_bytes();receipt.rename(case/'missing-receipt.saved')
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


def archive_native(root, job):
    """Synthetic persisted sacct/owner proof; no scheduler calls."""
    record={'state':'COMPLETED','exit_code':'0:0','elapsed_seconds':12,'cpus':1,'gpus':0,
        'job_id':job,'job_id_raw':job,'cpu_core_hours':12/3600,'gpu_hours':0.,'basis':'sacct final allocated wall time'}
    ledger=root/'state/resource-budget.json'
    value=read_json(ledger) if ledger.exists() else {'native_jobs':{},'leases':{},
        'spent_cpu_core_hours':0.,'spent_gpu_hours':0.}
    value['native_jobs'][job]=record;atomic_json(ledger,value)
    proof={'job_id':job,'array':None,'dependency_kind':'afterok','source_path':str(ledger),
        'source_file_sha256_at_capture':digest(ledger),'native_records':{job:record},
        'native_records_sha256':canonical_hash({job:record}),
        'basis':'Positive persisted final sacct records for every declared task; require COMPLETED/0:0 for afterok'}
    proof['completion_proof_sha256']=canonical_hash(proof)
    owner={'job_id':job,'state':'COMPLETED','exit_code':'0:0','elapsed_seconds':12,'cpus':1,'gpus':0,
        'user':'pwa209','account':'def-ptewarie_cpu'}
    result={}
    for key,data in [('completion',proof),('owner',owner)]:
        path=root/f'maintenance/archive-proofs/{job}-{key}.json';atomic_json(path,data)
        result[key]=bridge._locator(root,path)
    return result


def archive_frame_qualification(root, monkeypatch):
    import xml.etree.ElementTree as ET
    import subprocess
    from horsehair import budget,repair
    qjob=bridge.ARCHIVE_PACK_QUALIFICATION_JOB
    # Run the real accounting parser. Its persisted cost rows intentionally
    # have no owner/account/template/TRES fields; source ownership comes from
    # an independent strict native allocation read, never invented enrichment.
    def accounting_runner(command,**kwargs):
        assert command[0]=='sacct' and command[command.index('-j')+1]==qjob
        return subprocess.CompletedProcess(command,0,
            qjob+'|'+qjob+'|COMPLETED|12|4|billing=4,cpu=4,mem=16G,node=1|0:0|1024K\n','')
    budget.native_accounting(root,[qjob],runner=accounting_runner)
    native_path=root/'maintenance/archive-frame-native.txt';native_path.parent.mkdir(parents=True,exist_ok=True)
    native_path.write_text(qjob+'|'+qjob+'|COMPLETED|0:0|12|4|billing=4,cpu=4,mem=16G,node=1|nc10000|'
        '2026-10-21T10:00:00|2026-10-21T10:00:12|pwa209|def-ptewarie_cpu|1024K\n')
    def native_query(command,**kwargs):
        assert command==['sacct','--array','-j',qjob,'--format='+repair.REPAIR_NATIVE_FORMAT,
            '--units=K','--noheader','--parsable2']
        assert kwargs['text'] is True and kwargs['capture_output'] is True and 0<kwargs['timeout']<=60
        return subprocess.CompletedProcess(command,0,native_path.read_text(),'')
    monkeypatch.setattr(bridge.subprocess,'run',native_query)
    suite=ET.Element('testsuite',tests='1694',failures='0',errors='0',skipped='0')
    for i in range(1694):ET.SubElement(suite,'testcase',classname='synthetic_complete_framework',name=f'case_{i}')
    junit=root/'state/qualification-evidence/archive-frame.xml';junit.parent.mkdir(parents=True,exist_ok=True);junit.write_bytes(ET.tostring(suite))
    qualification={'status':'complete','returncode':0,'source_sha256':bridge.ARCHIVE_PACK_FRAMEWORK_SHA256,
        'config_sha256':bridge.BASE_CONFIG_SHA256,'source_verified_unchanged':True,'slurm_job_id':qjob,
        'test_counts':{'tests':1694,'failures':0,'errors':0,'skipped':0},
        'artifacts':{'junit':{'path':str(junit),'sha256':digest(junit),'bytes':junit.stat().st_size}}}
    qpath=root/bridge.ARCHIVE_PACK_QUALIFICATION_PATH;atomic_json(qpath,qualification)
    monkeypatch.setattr(bridge,'ARCHIVE_PACK_QUALIFICATION_SHA256',digest(qpath))
    return bridge._locator(root,qpath)


@pytest.fixture
def archive_support(context, monkeypatch):
    import xml.etree.ElementTree as ET
    root,config,*_=context
    monkeypatch.delenv('SLURM_JOB_ID',raising=False)
    fixtures=Path(__file__).parent/'fixtures/evidence_archive'
    area=root/'maintenance/archive-support';area.mkdir(parents=True)
    helper_path=area/'verified-evidence-archive-20261021.py'
    helper_path.write_bytes((fixtures/helper_path.name).read_bytes())
    assert digest(helper_path)==bridge.ARCHIVE_HELPER_SHA256
    original_loader=bridge._load_archive_helper
    def load(path):
        helper=original_loader(path)
        # Only the test's temporary personal-root configuration is substituted.
        # The production reader is the exact frozen fixture source bytes.
        helper.CONFIG=bridge.BASE_CONFIG_SHA256
        return helper
    monkeypatch.setattr(bridge,'_load_archive_helper',load)
    helper=load(helper_path)
    test_path=area/'test_verified_evidence_archive_20261021.py'
    test_path.write_bytes((fixtures/(test_path.name+'.txt')).read_bytes())
    assert digest(test_path)==helper.EXPECTED_CANARY_TEST_SHA256
    suite=ET.Element('testsuite',tests='39',failures='0',errors='0',skipped='0')
    for name in helper.EXPECTED_CANARY_TEST_NAMES:
        ET.SubElement(suite,'testcase',classname='test_verified_evidence_archive_20261021',name=name)
    junit=area/'junit.xml';junit.write_bytes(ET.tostring(suite))
    canary={'status':'complete_verified_native_archive_canary','helper_sha256':bridge.ARCHIVE_HELPER_SHA256,
        'roundtrip_verified':True,'unchanged_source_removal_guards_verified':True,
        'scientific_result':False,'admission_authority':False,
        'test_counts':{'tests':39,'failures':0,'errors':0,'skipped':0},'test_source':bridge._locator(root,test_path),
        'junit':bridge._locator(root,junit),'test_roster_sha256':canonical_hash(sorted(helper.EXPECTED_CANARY_TEST_NAMES)),
        'native':archive_native(root,'200'),'native_job_id':'200'}
    canary_path=area/'canary.json';atomic_json(canary_path,canary)
    registry={'schema_version':1,'kind':bridge.ARCHIVE_REGISTRY_KIND,'root':str(root),
        'full_config_sha256':bridge.BASE_CONFIG_SHA256,'primary_scientific_source_sha256':bridge.PRIMARY_SCIENTIFIC_SOURCE_SHA256,
        'archive_helper':bridge._locator(root,helper_path),'archive_canary':bridge._locator(root,canary_path),
        'cells':{},'scientific_result':False,'admission_authority':False}
    registry_path=area/'registry.json'
    def save():
        atomic_json(registry_path,registry)
        return bridge._locator(root,registry_path)
    return helper,registry,save


@pytest.mark.parametrize('change',['none','helper','cell_key','foreign_root','flags','canary_boolean','canary_one_test',
    'canary_failed_case','canary_missing_native','duplicate_json'])
def test_archive_registry_requires_exact_helper_and_complete_positive_native_canary(context,archive_support,change):
    root,_,_,_,plan=context;helper,registry,save=archive_support
    if change=='helper':registry['archive_helper']['sha256']='f'*64
    elif change=='cell_key':registry['cells']['00']={}
    elif change=='foreign_root':registry['root']=str(root.parent)
    elif change=='flags':registry['admission_authority']=True
    elif change.startswith('canary_'):
        path=Path(registry['archive_canary']['path']);value=read_json(path)
        if change=='canary_boolean':value=True
        elif change=='canary_one_test':value['test_counts']['tests']=1
        elif change=='canary_failed_case':
            junit=Path(value['junit']['path']);raw=junit.read_text();junit.write_text(raw.replace(' />','><failure /></testcase>',1))
            value['junit']['sha256']=digest(junit)
        else:
            ledger=root/'state/resource-budget.json';atomic_json(ledger,{'native_jobs':{}})
        atomic_json(path,value);registry['archive_canary']['sha256']=digest(path)
    binding=save()
    if change=='duplicate_json':
        path=Path(binding['path']);raw=path.read_text();path.write_text(raw.replace('{','{"schema_version":1,',1))
        binding['sha256']=digest(path)
    if change=='none':assert bridge._archive_context(root,binding,plan['cells'])['registry']==registry
    else:
        with pytest.raises((ValueError,AttributeError)):bridge._archive_context(root,binding,plan['cells'])


@pytest.mark.parametrize('change',['none','source','single_test','short_junit','failed_native','foreign_owner','zero_seconds','wrong_cpus','wrong_locator'])
def test_pack_framework_qualification_is_independent_full_and_positive(context,monkeypatch,change):
    root,*_=context;locator=archive_frame_qualification(root,monkeypatch);path=Path(locator['path']);q=read_json(path)
    if change=='source':q['source_sha256']='f'*64
    elif change=='single_test':q['test_counts']['tests']=1
    elif change=='short_junit':
        artifact=q['artifacts']['junit'];junit=Path(artifact['path'])
        junit.write_text('<testsuite tests="1694"><testcase name="one"/></testsuite>')
        artifact.update(sha256=digest(junit),bytes=junit.stat().st_size)
    elif change=='foreign_owner':
        p=root/'maintenance/archive-frame-native.txt';p.write_text(p.read_text().replace('|pwa209|','|other|'))
    elif change in {'failed_native','zero_seconds','wrong_cpus'}:
        ledger_path=root/'state/resource-budget.json';ledger=read_json(ledger_path);record=ledger['native_jobs'][bridge.ARCHIVE_PACK_QUALIFICATION_JOB]
        if change=='failed_native':record.update(state='FAILED',exit_code='1:0')
        elif change=='zero_seconds':record['elapsed_seconds']=0
        else:record['cpus']=1
        atomic_json(ledger_path,ledger)
    atomic_json(path,q);locator=bridge._locator(root,path)
    # Keep the test receipt byte-bound while separately exercising semantic
    # gates. Production's constant pins the actual immutable native receipt.
    monkeypatch.setattr(bridge,'ARCHIVE_PACK_QUALIFICATION_SHA256',locator['sha256'])
    if change=='wrong_locator':locator['path']=str(root/'maintenance/another-qualification.json')
    if change=='none':bridge._pack_qualification(root,locator)
    else:
        with pytest.raises(ValueError):bridge._pack_qualification(root,locator)


def test_native_owner_join_uses_actual_cost_only_accounting_and_strict_parser(context,monkeypatch):
    root,*_=context;archive_frame_qualification(root,monkeypatch)
    path=root/'state/resource-budget.json';before=path.read_bytes()
    row=read_json(path)['native_jobs'][bridge.ARCHIVE_PACK_QUALIFICATION_JOB]
    assert not {'user','account','unallocated_metadata','alloc_tres'} & set(row)
    joined=bridge._owned_completed_cpu_qualification(root,bridge.ARCHIVE_PACK_QUALIFICATION_JOB)
    assert joined['completion']['native_records'][bridge.ARCHIVE_PACK_QUALIFICATION_JOB]==row
    assert joined['native']['user']=='pwa209' and joined['native']['account']=='def-ptewarie_cpu'
    assert joined['native']['unallocated_metadata'] is False and path.read_bytes()==before


@pytest.mark.parametrize('change',['native_missing','native_running','native_nonzero','native_cpus','native_elapsed',
    'native_rawid','native_gpu','native_no_node','native_no_start','native_no_end','native_account','native_duplicate',
    'native_empty_tres','native_tres_cpus','native_tres_duplicate','native_tres_missing_cpu',
    'cost_bool_cpus','cost_bool_gpus','cost_gpu','cost_missing'])
def test_native_owner_and_cost_projections_must_independently_agree(context,monkeypatch,change):
    root,*_=context;archive_frame_qualification(root,monkeypatch);job=bridge.ARCHIVE_PACK_QUALIFICATION_JOB
    native_path=root/'maintenance/archive-frame-native.txt';fields=native_path.read_text().strip().split('|')
    if change.startswith('native_'):
        position={'native_running':2,'native_nonzero':3,'native_cpus':5,'native_elapsed':4,'native_rawid':1,
            'native_gpu':6,'native_no_node':7,'native_no_start':8,'native_no_end':9,'native_account':11,
            'native_empty_tres':6,'native_tres_cpus':6,'native_tres_duplicate':6,'native_tres_missing_cpu':6}
        value={'native_running':'RUNNING','native_nonzero':'1:0','native_cpus':'1','native_elapsed':'13','native_rawid':'999',
            'native_gpu':'cpu=4,gres/gpu=1','native_no_node':'None assigned','native_no_start':'Unknown',
            'native_no_end':'Unknown','native_account':'def-other_cpu','native_empty_tres':'',
            'native_tres_cpus':'cpu=1,node=1','native_tres_duplicate':'cpu=4,cpu=4,node=1','native_tres_missing_cpu':'node=1,mem=16G'}
        if change=='native_missing':native_path.write_text('')
        elif change=='native_duplicate':
            duplicate=fields.copy();duplicate[4]='13';native_path.write_text('|'.join(fields)+'\n'+'|'.join(duplicate)+'\n')
        else:
            fields[position[change]]=value[change];native_path.write_text('|'.join(fields)+'\n')
    else:
        ledger_path=root/'state/resource-budget.json';ledger=read_json(ledger_path);cost=ledger['native_jobs'][job]
        if change=='cost_bool_cpus':cost['cpus']=True
        elif change=='cost_bool_gpus':cost['gpus']=False
        elif change=='cost_gpu':cost['gpus']=1
        else:del ledger['native_jobs'][job]
        atomic_json(ledger_path,ledger)
    with pytest.raises(ValueError):bridge._owned_completed_cpu_qualification(root,job)


def test_native_owner_read_cannot_hide_selected_cost_row_change(context,monkeypatch):
    from horsehair import repair
    root,*_=context;archive_frame_qualification(root,monkeypatch);job=bridge.ARCHIVE_PACK_QUALIFICATION_JOB
    query=repair._repair_native_states
    def changing_query(*args,**kwargs):
        records=query(*args,**kwargs);path=root/'state/resource-budget.json';ledger=read_json(path)
        ledger['native_jobs'][job]['elapsed_seconds']=13;atomic_json(path,ledger);return records
    monkeypatch.setattr(repair,'_repair_native_states',changing_query)
    with pytest.raises(ValueError,match='changed during'):bridge._owned_completed_cpu_qualification(root,job)


def test_owner_observation_is_scoped_to_one_context_with_cost_and_junit_rechecks(context,monkeypatch):
    root,*_=context;locator=archive_frame_qualification(root,monkeypatch);calls=[];query=bridge.subprocess.run
    def counted(*args,**kwargs):calls.append(args[0]);return query(*args,**kwargs)
    monkeypatch.setattr(bridge.subprocess,'run',counted)
    observation=bridge._pack_qualification(root,locator)
    assert len(calls)==1
    assert bridge._pack_qualification(root,locator,observation=observation) is observation and len(calls)==1
    bridge._pack_qualification(root,locator)
    assert len(calls)==2  # A new context always performs a fresh strict read.
    with pytest.raises(ValueError,match='context-scoped'):
        bridge._pack_qualification(root,locator,observation={'allocation_json':observation.allocation_json})
    assert len(calls)==2
    path=root/'state/resource-budget.json';ledger=read_json(path)
    ledger['native_jobs'][bridge.ARCHIVE_PACK_QUALIFICATION_JOB]['elapsed_seconds']=13;atomic_json(path,ledger)
    with pytest.raises(ValueError,match='within this archive context'):
        bridge._pack_qualification(root,locator,observation=observation)
    assert len(calls)==2


def test_context_scoped_owner_observation_never_skips_q_or_junit_bindings(context,monkeypatch):
    root,*_=context;locator=archive_frame_qualification(root,monkeypatch)
    observation=bridge._pack_qualification(root,locator)
    q=read_json(Path(locator['path']));junit=Path(q['artifacts']['junit']['path']);junit.write_text('<testsuite/>')
    with pytest.raises(ValueError):bridge._pack_qualification(root,locator,observation=observation)


@pytest.mark.parametrize('change',['expired','nan','bool','timeout','nonzero','bool_return','oversized','over_deadline','parser_source'])
def test_native_owner_query_has_one_exact_bounded_source_checked_read(context,monkeypatch,change):
    import subprocess
    root,*_=context;archive_frame_qualification(root,monkeypatch);job=bridge.ARCHIVE_PACK_QUALIFICATION_JOB
    original=bridge.subprocess.run;calls=[];clock=[100.]
    monkeypatch.setattr(bridge.time,'monotonic',lambda:clock[0])
    def query(command,**kwargs):
        calls.append(command)
        if change=='timeout':raise subprocess.TimeoutExpired(command,kwargs['timeout'])
        response=original(command,**kwargs)
        if change=='nonzero':response.returncode=1
        if change=='bool_return':response.returncode=False
        if change=='oversized':response.stdout='x'*1_000_001
        if change=='over_deadline':clock[0]+=61
        return response
    monkeypatch.setattr(bridge.subprocess,'run',query)
    if change=='parser_source':monkeypatch.setattr(bridge,'ARCHIVE_NATIVE_REPAIR_SHA256','f'*64)
    deadline={'expired':99.,'nan':float('nan'),'bool':True}.get(change,160.)
    with pytest.raises((ValueError,RuntimeError,subprocess.TimeoutExpired)):
        bridge._owned_completed_cpu_qualification(root,job,deadline=deadline)
    assert len(calls)==(0 if change in {'expired','nan','bool','parser_source'} else 1)


@pytest.mark.parametrize('outcome',['complete','timeout'])
def test_bounded_owner_query_composes_with_exact358_native_read_guard(context,monkeypatch,outcome):
    import runpy
    import subprocess
    from horsehair import repair
    root,*_=context;archive_frame_qualification(root,monkeypatch);job=bridge.ARCHIVE_PACK_QUALIFICATION_JOB
    source=Path(__file__).parent/'fixtures/evidence_archive/native-accounting-read-guard-20261021.py.txt'
    assert digest(source)=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
    original=repair._repair_native_states;transport=bridge.subprocess.run;calls=[]
    def query(command,**kwargs):
        calls.append(command);assert 0<kwargs['timeout']<=60
        if outcome=='timeout':raise subprocess.TimeoutExpired(command,kwargs['timeout'])
        return transport(command,**kwargs)
    monkeypatch.setattr(bridge.subprocess,'run',query)
    evidence=root/'maintenance/guard-composition';before=(root/'state/resource-budget.json').read_bytes()
    try:
        runpy.run_path(str(source),run_name='synthetic358_composition_only')['install'](evidence)
        if outcome=='complete':
            joined=bridge._owned_completed_cpu_qualification(root,job);assert joined['native']['user']=='pwa209'
        else:
            with pytest.raises(subprocess.TimeoutExpired):bridge._owned_completed_cpu_qualification(root,job)
    finally:repair._repair_native_states=original
    assert len(calls)==1 and (root/'state/resource-budget.json').read_bytes()==before
    reads=list(evidence.glob('read-*'));assert len(reads)==1
    result=read_json(reads[0]/'result.json')
    assert result['status']==('strict_fresh_query_passed' if outcome=='complete' else 'strict_read_failed')
    assert result['complete_reads_attempted']==1


def test_real600_member_archive_is_identical_after_original_parts_are_absent(context,complete_policy,archive_support,monkeypatch):
    root,_,_,_,_=context;plan,cell,checkpoint,value,assembly=complete_policy;helper,registry,save=archive_support
    case=Path(cell['evaluation_case']);raw_entry=bridge._entry(root,cell)
    expected=bridge._validate_cell(plan,cell,raw_entry)
    inputs={key:bridge._locator(root,case/name) for key,name in [('plan','plan.json'),('binding','plan-binding.json'),('summary','evaluation_summary.json')]}
    result={'native_job_id':'100','source_sha256':plan['campaign']['source_binding']['execution_source_sha256'],
        'full_config_sha256':bridge.BASE_CONFIG_SHA256,'scientific_result':False,'admission_authority':False,
        'status':'complete_native_work','mode':'agent-assembly','evaluation_case':str(case),
        'evaluation_plan_sha256':inputs['plan']['sha256'],'evaluation_binding_sha256':inputs['binding']['sha256'],'result':assembly}
    result_path=root/'maintenance/archive-proofs/assembly-result.json';atomic_json(result_path,result)
    authority={'primary_source_sha256':bridge.PRIMARY_SCIENTIFIC_SOURCE_SHA256,'full_config_sha256':bridge.BASE_CONFIG_SHA256,
        'execution_source_sha256':result['source_sha256'],'native':archive_native(root,'100'),
        'result_receipt':bridge._locator(root,result_path),'policy_inputs':inputs}
    # Windows sandbox disallows hard links. Publication is fixture construction,
    # not the consumer under test; the real tar/index/hash/parser remain intact.
    monkeypatch.setattr(helper,'_publish_file',lambda source,destination:Path(source).rename(destination))
    directory=root/'maintenance/policy-archive'
    pack=helper.pack_policy(case,directory,authority=authority,root=root)
    descriptor={'evaluation_case':str(case),'policy_inputs':inputs,'archive_directory':str(directory),
        'index_sha256':pack['index_sha256'],'archive_sha256':pack['archive_sha256'],
        **{key:bridge._locator(root,directory/name) for key,name in [('pack_intent','pack-intent.json'),('roundtrip','roundtrip.json'),('pack_receipt','pack-receipt.json')]}}
    producer={'schema_version':1,'status':'complete_verified_policy_archive_pack','native_job_id':'300',
        'archive_helper_sha256':bridge.ARCHIVE_HELPER_SHA256,'evaluation_case':str(case),'archive_directory':str(directory),
        'index_sha256':pack['index_sha256'],'archive_sha256':pack['archive_sha256'],
        'pack_intent_sha256':descriptor['pack_intent']['sha256'],'roundtrip_sha256':descriptor['roundtrip']['sha256'],
        'pack_receipt_sha256':descriptor['pack_receipt']['sha256'],'execution_source_sha256':'e'*64,
        'full_config_sha256':bridge.BASE_CONFIG_SHA256,'primary_scientific_source_sha256':bridge.PRIMARY_SCIENTIFIC_SOURCE_SHA256,
        'scientific_result':False,'admission_authority':False}
    producer_path=root/'maintenance/archive-proofs/pack-result.json';atomic_json(producer_path,producer)
    descriptor['pack_producer']={'native':archive_native(root,'300'),'result':bridge._locator(root,producer_path),
        'qualification':archive_frame_qualification(root,monkeypatch)}
    producer['execution_source_sha256']=bridge.ARCHIVE_PACK_FRAMEWORK_SHA256;atomic_json(producer_path,producer)
    descriptor['pack_producer']['result']=bridge._locator(root,producer_path)
    registry['cells']['0']=descriptor;binding=save()
    archives=bridge._archive_context(root,binding,plan['cells'])
    entry={**raw_entry,'condition_archive':copy.deepcopy(descriptor)}
    # Remove only disposable synthetic originals, exactly their known600 paths.
    for condition in assembly['condition_bindings']:
        (case/condition['receipt_name']).unlink();(case/condition['artifact_name']).unlink()
    actual=bridge._validate_cell(plan,cell,entry,archives=archives)
    for a,b in zip(actual,expected):pd.testing.assert_frame_equal(a,b,check_exact=True)
    assert checkpoint.exists() and (case/assembly['artifact_name']).exists()
    assert len(actual[0])==300 and len(actual[1])==1200
    # A listed archive never silently falls back to originals, even with only a
    # subset absent/corrupt. Unlisted cells still require all original parts.
    with pytest.raises(ValueError,match='Every original condition'):bridge._validate_cell(plan,cell,raw_entry)
    with pytest.raises(ValueError,match='manifest changed'):bridge._validate_cell(plan,cell,raw_entry,archives=archives)
    for change in ['pack_job','pack_result','wrong_cell','pack_receipt','index']:
        altered=copy.deepcopy(descriptor)
        if change=='pack_job':altered['pack_producer']['native']=authority['native']
        elif change=='pack_result':altered['pack_producer']['result']=authority['result_receipt']
        elif change=='wrong_cell':altered['evaluation_case']=str(case.parent/'cell-0001')
        elif change=='pack_receipt':altered['pack_receipt']['sha256']='f'*64
        else:altered['index_sha256']='f'*64
        with pytest.raises(ValueError):bridge._archive_descriptor(root,helper,altered,cell)
    # Path replacement/corruption is detected by the real full tar reader.
    archive=directory/'evidence.tar';before=archive.read_bytes();archive.write_bytes(b'corrupted')
    with pytest.raises(ValueError,match='Archive bytes changed'):bridge._validate_cell(plan,cell,entry,archives=archives)
    archive.write_bytes(before)
    # Re-pinning a changed canonical assembly cannot replace comparison against
    # all exact archived condition rows or the original native result authority.
    assembled=case/assembly['artifact_name'];frame=pd.read_parquet(assembled);frame.loc[0,'redirection']=1
    frame.to_parquet(assembled,index=False);assembly['trial_file_sha256']=digest(assembled)
    atomic_json(case/'evaluation_summary.json',assembly)
    with pytest.raises(ValueError):bridge._validate_cell(plan,cell,entry,archives=archives)
