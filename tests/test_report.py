"""Generated contract fixtures only; none are observed study findings."""
import copy
from pathlib import Path

import pytest

from horsehair import agents, agent_analysis, report
from horsehair.core import atomic_json, canonical_hash, digest,read_json
from horsehair.inference import holm


@pytest.fixture(autouse=True)
def immutable_test_release(monkeypatch):
    # Collaborating agents edit other source files during local tests. Capture one
    # release identity for this contract fixture, as an immutable Slurm release
    # would, rather than making the test depend on unrelated concurrent writes.
    identity = report._source_sha()
    monkeypatch.setattr(report,"_source_sha",lambda:identity)


def config_fixture():
    return {"protocol_id":"unit-test-contract-fixture","inference":{"primary_family":["H1","H2","H3"],"alpha":.05},
            "molecular":{"incremental_rank_effect_threshold":.2},
            "agents":{"ranks":[1],"strengths":[.2],"feedforward_baseline":True}}


def biological_result(root,config,claim,estimate=.3,pvalue=.001):
    manifest = root/"manifests/source_manifest.json"
    if not manifest.exists():atomic_json(manifest,{"fixture":"generated unit-test manifest"})
    source = root/f"fixture_{claim}_input.tsv"
    source.write_text("GENERATED CONTRACT FIXTURE; NOT A STUDY OBSERVATION\n",encoding="utf-8")
    unit = next(unit for name,_,unit in report.CLAIMS if name==claim)
    result = {"claim_id":claim,"independent_unit":unit,"evidence_class":"biological_reanalysis",
              "config_sha256":canonical_hash(config),"source_sha256":report._source_sha(),"data_manifest_sha256":digest(manifest),
              "estimate":estimate,"pvalue":pvalue,"ci95":[estimate-.05,estimate+.05],
              "source_result_paths":[str(source)],"source_result_hashes":{str(source):digest(source)},
              "parameter_recovery_passed":True,"parameter_identifiability_passed":True,"multivariate_diagnostic_improvement":.1,
              "family_generalization_passed":True,"shared_control_covariance_preserved":True,"intervention_families_evaluated":3}
    if claim == "H2":
        result.update(maximum_derivative_relative_stability_error=.02,
                      bootstrap_outside_validated_response_radius_fraction=.01,
                      unrestricted_comparison={"estimate":estimate,"ci95":[estimate-.05,estimate+.05],
                        "maximum_derivative_relative_stability_error":.02,
                        "bootstrap_outside_validated_response_radius_fraction":.01})
    atomic_json(root/f"results/inference/{claim}.json",result)
    return result


def computational_result(root,config,positive=False):
    settings = config["agents"]
    protocol = {"full_config_sha256":canonical_hash(config),"config_sha256":agents._digest(settings),
                "implementation_sha256":agents._implementation_sha256(),"H4_inference_specification":agents.h4_specification(config),
                "biological_directions":None,"biological_transfer_status":"pending"}
    protocol_path = root/"results/agents/frozen_protocol.json"
    atomic_json(protocol_path,protocol)
    cells = agents._cells(settings)
    curves = []
    for architecture,task in sorted({(a,t) for t,a,_,_ in cells}):
        for condition in agent_analysis._conditions(settings,protocol):
            rule,mechanism,rank,arm,strength = condition
            primary = mechanism in agent_analysis.PRIMARY
            raw = .000001 if positive and mechanism=="recurrent_gate" else 1.
            curves.append({"architecture":architecture,"task":task,"rule_type":rule,"perturbation":mechanism,"rank":rank,
                           "matching_arm":arm,"strength":strength,"independent_seeds":20,"available_for_H4":True,
                           "baseline_qualified":True,"strength_matched":True,
                           "primary_grid_point":primary,"raw_joint_pvalue":raw,
                           "component_pvalues":{o:raw for o in agent_analysis.OUTCOMES},
                           "comparators":{k:{"independent_seed_pairs":20,"strength_matched":True,"pvalue":raw} for k in ("random_subspace","output_gain")},
                           "outcomes":{o:{"estimate":.3 if o=="redirection_difference" else .0,"ci95":[.25,.35] if o=="redirection_difference" else [-.01,.01]} for o in agent_analysis.OUTCOMES}})
    primary = [r for r in curves if r["primary_grid_point"]]
    adjusted = holm([r["raw_joint_pvalue"] for r in primary])
    for row,value in zip(primary,adjusted):
        row["holm_pvalue"] = float(value);row["operating_region_supported"] = bool(value<.05)
    for row in curves:
        if not row["primary_grid_point"]:row.update(holm_pvalue=None,operating_region_supported=False)
    curve_path = root/"results/agents/analysis/response_curves.json"
    atomic_json(curve_path,curves)
    supported = sum(r["operating_region_supported"] for r in primary)
    result = {"claim_id":"H4","independent_unit":agent_analysis.UNIT,"evidence_class":"computational_experiment",
              "primary_evidence_class":"computational_experiment","biological_transfer_status":"pending",
              "config_sha256":canonical_hash(config),"source_sha256":report._source_sha(),"frozen_protocol_sha256":agents._digest(protocol),
              "status":"complete","computational_analysis_complete":True,"planned_policy_cells":len(cells),"independent_seed_count_per_cell":20,
              "estimate":.3,"ci95":[.25,.35],"pvalue":min(adjusted),"all_baseline_competence_qualified":True,
              "biological_constraints_available":False,"claim_status":"supported" if positive else "not_supported",
              "headline_claim_accepted":positive,"computational_claim_accepted":positive,"supported_grid_points":supported,
              "biologically_constrained_supported_grid_points":0,"response_curves":str(curve_path),
              "correction":{"method":"Holm","family":"H4 prespecified computational grid","tests":len(primary),"alpha":.05},
              "source_result_hashes":{str(protocol_path):digest(protocol_path),str(curve_path):digest(curve_path)}}
    atomic_json(root/"results/inference/H4.json",result)
    return result


def auxiliary_fixture(root,config,*,ready=True):
    """Generated readiness contract only, never deposited scientific evidence."""
    directory=root/'results/behaviour';directory.mkdir(parents=True,exist_ok=True)
    def fit(terms):
        return {'terms':terms,'coefficient':[0.]*len(terms),'covariance_laplace':[[float(i==j) for j in range(len(terms))] for i in range(len(terms))],
                'design_audit':{'protected_primary_estimable':True,'full_column_space_preserved':True}}
    choice_fit=fit(['Intercept','infection','dop','infection:dop'])
    mating_fit=fit(['Intercept','male_infection','female_infection'])
    source={'choice':{'trials':20,'mutually_consistent_trials':20 if ready else 19,'ambiguous_trials':0 if ready else 1,'source_coding_reconciled':ready},
            'mating':{'independence_verified':ready,'pair_records':20}}
    validation={'status':'complete' if ready else 'pending_source_reconciliation','confirmatory_ready':ready,
                'source_coding_reconciled':ready,'future_outcomes_used_for_preprocessing':False,
                'scores':[{'scheme':'individual','individual_id':'QA-only'}],'training_contracts':[{'fixture':'generated contract'}]}
    sensitivity={'status':'not_required' if ready else 'pending','confirmatory_ready':ready}
    choice={'status':'complete','engagement':choice_fit,'conditional_preference':choice_fit,
            'design_audit':{name:choice_fit['design_audit'] for name in ('engagement','conditional_preference')},
            'source_coding_reconciled':ready,'confirmatory_ready':ready,'deposited_source_trials':20,
            'included_source_trials':source['choice']['mutually_consistent_trials'],'ambiguous_source_trials':source['choice']['ambiguous_trials']}
    mating={endpoint:{**copy.deepcopy(mating_fit),'status':'complete'} for endpoint in ('male_escape','male_mounting')}
    mating['specificity']={'status':'complete','escape':mating_fit,'mounting':mating_fit,
                          'male_infection_coefficient_difference':0.,'cluster_bootstrap_interval':[-.1,.1]}
    mating['statistical_models_complete']=True
    models={'choice':choice,'choice_validation':validation,'choice_sensitivity':sensitivity,'mating':mating}
    atomic_json(directory/'mantis_models.json',models);atomic_json(directory/'mantis_source_audit.json',source)
    for name in ('mantis_choice_canonical.parquet','mantis_mating_canonical.parquet'):
        (directory/name).write_text('GENERATED HASH-BOUND CONTRACT INPUT; NO SCIENTIFIC OBSERVATIONS\n')
    model_path=str((directory/'mantis_models.json').resolve())
    for name,record in [('mantis_choice_validation.json',validation),('mantis_choice_sensitivity.json',sensitivity)]:
        atomic_json(directory/name,{**record,'config_sha256':canonical_hash(config),'source_sha256':report._source_sha(),
                                   'source_result_paths':[model_path],'source_result_hashes':{model_path:digest(model_path)}})
    paths=[directory/name for name in ('mantis_models.json','mantis_source_audit.json','mantis_choice_canonical.parquet',
           'mantis_mating_canonical.parquet','mantis_choice_validation.json','mantis_choice_sensitivity.json')]
    audit={'config_sha256':canonical_hash(config),'source_sha256':report._source_sha(),'choice_status':'complete',
           'choice_source_coding_reconciled':ready,'choice_source_trials':20,'choice_mutually_consistent_trials':source['choice']['mutually_consistent_trials'],
           'choice_ambiguous_trials':source['choice']['ambiguous_trials'],'choice_confirmatory_ready':ready,
           'choice_validation_status':validation['status'],'choice_sensitivity_status':sensitivity['status'],
           'mating_models_complete':True,'mating_independence_verified':ready,
           'mating_models':{name:{'status':'complete'} for name in ('male_escape','male_mounting','specificity')},
           'source_result_paths':[str(p.resolve()) for p in paths],'source_result_hashes':{str(p.resolve()):digest(p) for p in paths}}
    atomic_json(directory/'mantis_statistical_design_audit.json',audit)
    return directory,audit


def test_declared_auxiliary_science_is_required_even_after_all_primary_and_visual_results(tmp_path,monkeypatch):
    config=config_fixture();config['sources']={'mantis_polarization_doi':'QA-only-choice','mantis_mating_doi':'QA-only-mating'}
    for claim in ('H1','H2','H3'):biological_result(tmp_path,config,claim,estimate=-.2,pvalue=.8)
    computational_result(tmp_path,config)
    for number,_,_ in report.FIGURES:atomic_json(tmp_path/f'results/figure_inputs/{number}.json',{'fixture':'render boundary mocked'})
    monkeypatch.setattr(report,'render_figure',lambda path,out,title,number,**kwargs:{'id':number,'paths':['generated-qa.png']})
    monkeypatch.setattr(report,'_visual_review',lambda *a:True)
    missing=report.build_report(config,tmp_path)
    assert missing['primary_analyses_complete'] and missing['figures_complete'] and missing['visual_inspection_complete']
    assert not missing['analysis_complete'] and not missing['complete'] and not missing['publication_ready']
    auxiliary_fixture(tmp_path,config,ready=False)
    pending=report.build_report(config,tmp_path)
    assert pending['primary_analyses_complete'] and not pending['auxiliary_analyses_complete']
    assert any('ambiguity' in p for p in pending['auxiliary_readiness']['pending'])
    auxiliary_fixture(tmp_path,config,ready=True)
    complete=report.build_report(config,tmp_path)
    assert complete['primary_analyses_complete'] and complete['auxiliary_analyses_complete']
    assert complete['analysis_complete'] and complete['complete'] and complete['publication_ready']


@pytest.mark.parametrize('corruption',['stale_source','stale_config','mutated_model','pending_fit','missing_endpoint'])
def test_auxiliary_readiness_refuses_stale_missing_and_pending_hash_bound_models(tmp_path,corruption):
    config=config_fixture();config['sources']={'mantis_polarization_doi':'QA-only-choice','mantis_mating_doi':'QA-only-mating'}
    directory,audit=auxiliary_fixture(tmp_path,config)
    assert report.auxiliary_readiness(config,tmp_path)['ready']
    if corruption in ('stale_source','stale_config'):
        audit['source_sha256' if corruption=='stale_source' else 'config_sha256']='0'*64
    else:
        path=directory/'mantis_models.json';models=read_json(path)
        if corruption=='pending_fit':
            models['choice']={'status':'pending_nonestimable_design','numeric_claim_available':False,'design_audit':None}
        elif corruption=='missing_endpoint':models['mating'].pop('male_escape')
        else:models['choice']['engagement']['coefficient'][0]=999.
        atomic_json(path,models)
        if corruption!='mutated_model':
            audit['source_result_hashes'][str(path.resolve())]=digest(path)
            for name in ('mantis_choice_validation.json','mantis_choice_sensitivity.json'):
                bound=read_json(directory/name);bound['source_result_hashes'][str(path.resolve())]=digest(path)
                atomic_json(directory/name,bound);audit['source_result_hashes'][str((directory/name).resolve())]=digest(directory/name)
    atomic_json(directory/'mantis_statistical_design_audit.json',audit)
    result=report.auxiliary_readiness(config,tmp_path)
    assert not result['ready'] and result['pending']


def test_missing_primary_member_prevents_joint_positive_claim(tmp_path):
    config = config_fixture()
    biological_result(tmp_path,config,"H1",pvalue=.000001)
    result = report.build_report(config,tmp_path)
    assert result["claims"][0]["status"]=="joint_primary_family_pending"
    assert not result["analysis_complete"] and not result["publication_ready"]
    assert not (tmp_path/"results/figure_inputs/06.json").exists()


def test_mixed_negative_positive_and_missing_diagnostic_decisions(tmp_path):
    config = config_fixture()
    first = biological_result(tmp_path,config,"H1",pvalue=.001)
    first.pop("parameter_identifiability_passed")
    atomic_json(tmp_path/"results/inference/H1.json",first)
    biological_result(tmp_path,config,"H2",estimate=-.2,pvalue=.5)
    biological_result(tmp_path,config,"H3",estimate=.4,pvalue=.01)
    result = report.build_report(config,tmp_path)
    assert [c["status"] for c in result["claims"][:3]]==["required_diagnostics_pending","not_supported","supported"]
    assert [c["primary_family_holm_pvalue"] for c in result["claims"][:3]]==pytest.approx([.003,.5,.02])


def test_four_valid_negative_analyses_do_not_mean_publication_ready(tmp_path):
    config = config_fixture()
    for claim in ("H1","H2","H3"):biological_result(tmp_path,config,claim,estimate=-.2,pvalue=.8)
    computational_result(tmp_path,config)
    result = report.build_report(config,tmp_path)
    assert result["analysis_complete"] and result["analyses_available"]
    assert not result["figures_complete"] and not result["publication_ready"]
    assert all(c["status"]=="not_supported" for c in result["claims"])
    synthesis = (tmp_path/"results/figure_inputs/06.json").read_text()
    assert "mixed_study_evidence" in synthesis and "without pooling" in synthesis
    assert result["figures"][-1]["pages"]==1


def test_missing_identifiability_is_pending_even_when_primary_effect_is_negative(tmp_path):
    config = config_fixture()
    result = biological_result(tmp_path,config,"H1",estimate=-.2,pvalue=.8)
    result.update(parameter_identifiability_passed=False, parameter_identifiability_status="pending: recovery coverage unavailable")
    atomic_json(tmp_path/"results/inference/H1.json",result)
    biological_result(tmp_path,config,"H2",estimate=-.2,pvalue=.8)
    biological_result(tmp_path,config,"H3",estimate=-.2,pvalue=.8)
    observed = report.build_report(config,tmp_path)
    assert observed["claims"][0]["status"] == "required_diagnostics_pending"
    assert not observed["analysis_complete"]


def test_h2_cannot_pass_without_unrestricted_or_valid_calibration_uncertainty(tmp_path):
    config = config_fixture()
    value = biological_result(tmp_path,config,"H2",estimate=.3,pvalue=.001)
    assert report._operational_gates(value,"H2",config)["passed"]
    value["unrestricted_comparison"]["estimate"] = .05
    assert not report._operational_gates(value,"H2",config)["passed"]
    value["unrestricted_comparison"]["estimate"] = .3
    value["bootstrap_outside_validated_response_radius_fraction"] = .5
    gate = report._operational_gates(value,"H2",config)
    assert not gate["passed"] and "gain_nonlocal_calibration_uncertainty" in gate["missing"]


def test_supported_H4_is_computational_with_a_separate_pending_bridge(tmp_path):
    config = config_fixture()
    result = computational_result(tmp_path,config,positive=True)
    report.validate_result(result,"H4",agent_analysis.UNIT,config,tmp_path)
    assert result["computational_claim_accepted"] and result["biological_transfer_status"]=="pending"
    result["biological_constraints_available"] = True
    with pytest.raises(ValueError,match="qualification"):
        report.validate_result(result,"H4",agent_analysis.UNIT,config,tmp_path)


def test_tampered_source_or_unhashed_path_is_rejected(tmp_path):
    config = config_fixture()
    result = biological_result(tmp_path,config,"H1")
    Path(result["source_result_paths"][0]).write_text("changed input",encoding="utf-8")
    with pytest.raises(ValueError,match="digest mismatch"):
        report.validate_result(result,"H1",report.CLAIMS[0][2],config,tmp_path)
    result = biological_result(tmp_path,config,"H1")
    result["source_result_hashes"] = {}
    with pytest.raises(ValueError,match="recorded SHA"):
        report.validate_result(result,"H1",report.CLAIMS[0][2],config,tmp_path)
    result = biological_result(tmp_path,config,"H1")
    result["source_sha256"] = "0"*64
    with pytest.raises(ValueError,match="implementation changed"):
        report.validate_result(result,"H1",report.CLAIMS[0][2],config,tmp_path)


def test_H4_cannot_reduce_grid_or_claim_support_by_boolean_alone(tmp_path):
    config = config_fixture()
    result = computational_result(tmp_path,config)
    result["claim_status"]="supported";result["headline_claim_accepted"]=True;result["computational_claim_accepted"]=True
    with pytest.raises(ValueError,match="unsupported"):
        report.validate_result(result,"H4",agent_analysis.UNIT,config,tmp_path)
    result = computational_result(tmp_path,config)
    curve = Path(result["response_curves"])
    from horsehair.core import read_json
    rows = read_json(curve);rows.pop();atomic_json(curve,rows)
    result["source_result_hashes"][str(curve)] = digest(curve)
    with pytest.raises(ValueError,match="omit or duplicate"):
        report.validate_result(result,"H4",agent_analysis.UNIT,config,tmp_path)


def qa_specification(root,count=13):
    source = root/"generated_qa_data.json"
    atomic_json(source,{"fixture":"generated curves, not observed data"})
    series = [{"label":"Generated controller","x":[0,.2,.4],"y":[0,.2,.3],"lower":[0,.1,.2],"upper":[.02,.3,.4],
               "baseline_qualified":[True,False,True],"strength_matched":[True,True,False]},
              {"label":"Generated control","x":[0,.2,.4],"y":[0,.03,.05],"lower":[-.02,.01,.02],"upper":[.02,.05,.08]}]
    specification = {"evidence_status":"synthetic_fixture","evidence_class":"computational_experiment",
        "source_result_paths":[str(source)],"source_result_hashes":{str(source):digest(source)},
        "layout":{"max_panels_per_page":12,"columns":3},"caption":"SYNTHETIC QA FIXTURE ONLY. No observed study result.",
        "panels":[{"kind":"interval","title":f"Generated fixture {i+1}","xlabel":"Frozen strength","ylabel":"Probability difference",
                   "series":copy.deepcopy(series)} for i in range(count)]}
    path = root/"qa_figure.json";atomic_json(path,specification)
    return path,specification


def test_paginated_series_rendering_preserves_every_panel_and_fixture_label(tmp_path):
    path,specification = qa_specification(tmp_path)
    result = report.render_figure(path,tmp_path/"rendered","Generated QA only","fixture",allow_fixture=True)
    assert result["pages"]==2 and len(result["paths"])==6
    assert all(Path(p).is_file() for p in result["paths"])
    assert result["evidence_status"]=="synthetic_fixture"
    from horsehair.core import read_json
    copied = read_json(tmp_path/"rendered/figure_fixture.plotted_data.json")
    assert len(copied["panels"])==13 and copied["panels"][0]["series"]==specification["panels"][0]["series"]
    with pytest.raises(ValueError,match="Synthetic QA"):
        report.render_figure(path,tmp_path/"prohibited","Forbidden study result","05")


def test_figures_reject_mutated_inputs_nonfinite_x_and_false_intervals(tmp_path):
    path,specification = qa_specification(tmp_path,1)
    source = Path(specification["source_result_paths"][0]);source.write_text("tampered",encoding="utf-8")
    with pytest.raises(ValueError,match="digest mismatch"):
        report.render_figure(path,tmp_path/"rendered","QA","fixture",allow_fixture=True)
    specification["source_result_hashes"][str(source)] = digest(source)
    specification["panels"][0]["series"][0]["lower"] = [0,.4,.2]
    atomic_json(path,specification)
    with pytest.raises(ValueError,match="uncertainty interval"):
        report.render_figure(path,tmp_path/"rendered","QA","fixture",allow_fixture=True)
