import json

import numpy as np
import pandas as pd
import pytest

from horsehair import agent_analysis as analysis, agents
from horsehair.inference import holm
from horsehair.core import canonical_hash


def seed_fixture(episodes=8):
    rows = []
    for seed in range(1000, 1020):
        for mechanism in agents.MECHANISMS:
            for probe in ("main", *agents.PROBES):
                for episode in range(episodes):
                    pattern = episode % 8
                    redirected = int(pattern < 4) if mechanism in ("input_gain", "recurrent_gate", "hidden_displacement") else 0
                    rows.append({"training_seed":seed, "architecture":"rnn", "task":"foraging", "rule_type":"synthetic",
                        "perturbation":mechanism, "rank":1, "matching_arm":"parameter_norm", "strength":.2,
                        "restoration_control":"none", "probe":probe, "episode_seed":40_000_000+seed*1000+episode,
                        "success":1, "redirection":redirected, "motor_accuracy":1., "baseline_success":1.,
                        "baseline_redirection":0., "baseline_motor_accuracy":1., "achieved_parameter_norm":.2,
                        "clean_policy_kl":.02, "target_policy_kl":.02})
    return pd.DataFrame(rows)


def settings():
    return {"final_seeds":list(range(1000,1020)), "baseline_probe_accuracy_min":.8}


def test_episode_replication_does_not_increase_independent_sample_size_or_significance():
    first = analysis.analyze_seed_summaries(analysis.summarize_trials(seed_fixture(8)), settings(), {"bootstrap_replicates":100})
    second = analysis.analyze_seed_summaries(analysis.summarize_trials(seed_fixture(80)), settings(), {"bootstrap_replicates":100})
    for a,b in zip(first,second):
        assert a["independent_seeds"] == b["independent_seeds"] == 20
        assert a["outcomes"] == b["outcomes"]
        assert a["raw_joint_pvalue"] == b["raw_joint_pvalue"]
        assert a["holm_pvalue"] == b["holm_pvalue"]
    assert first[0]["nested_episodes_per_seed"] == [8]
    assert second[0]["nested_episodes_per_seed"] == [80]


def test_unqualified_policy_is_preserved_and_cannot_support_H4():
    data = analysis.summarize_trials(seed_fixture())
    selected = data.perturbation.eq("recurrent_gate") & data.training_seed.eq(1000)
    data.loc[selected,"baseline_delayed_cue"] = .5
    records = analysis.analyze_seed_summaries(data, settings(), {"bootstrap_replicates":100})
    point = next(r for r in records if r["perturbation"] == "recurrent_gate")
    assert point["independent_seeds"] == 20
    assert not point["baseline_qualified"]
    assert point["outcomes"]["redirection_difference"]["estimate"] == .5
    assert not point["available_for_H4"] and point["holm_pvalue"] == 1
    assert not point["operating_region_supported"]


def test_calibration_mismatch_is_unavailable_and_never_dropped_from_family():
    data = analysis.summarize_trials(seed_fixture())
    data.loc[data.perturbation.eq("random_subspace") & data.training_seed.eq(1000),"strength_matched"] = False
    records = analysis.analyze_seed_summaries(data, settings(), {"bootstrap_replicates":100})
    points = [r for r in records if r["primary_grid_point"]]
    assert len(points) == 4
    assert all(r["holm_pvalue"] == 1 for r in points)
    assert all(not r["available_for_H4"] for r in points)


def test_joint_one_sided_test_and_Holm_include_every_declared_primary_point():
    records = analysis.analyze_seed_summaries(analysis.summarize_trials(seed_fixture()), settings(), {"bootstrap_replicates":100})
    points = [r for r in records if r["primary_grid_point"]]
    assert [r["holm_pvalue"] for r in points] == holm([r["raw_joint_pvalue"] for r in points]).tolist()
    assert next(r for r in points if r["perturbation"] == "recurrent_gate")["operating_region_supported"]
    assert not next(r for r in points if r["perturbation"] == "output_gain")["operating_region_supported"]
    assert analysis.one_sided(np.full(20,.2),.2) == 1
    assert analysis.one_sided(np.full(20,.05),.05,"less") == 1
    assert analysis.one_sided(np.full(20,.3),.2) == 0


def test_missing_independent_seed_is_not_imputed_or_excluded():
    data = analysis.summarize_trials(seed_fixture())
    data = data[~(data.perturbation.eq("hidden_displacement") & data.training_seed.eq(1019))]
    with pytest.raises(ValueError,match="Missing independently trained seed"):
        analysis.analyze_seed_summaries(data,settings())


def test_missing_policy_grid_writes_incomplete_audit_not_H4_result(tmp_path):
    config = {"agents":settings()}
    protocol = {"config_sha256":agents._digest(config["agents"]), "implementation_sha256":agents._implementation_sha256(),
                "full_config_sha256":canonical_hash(config), "H4_inference_specification":agents.h4_specification(config),
                "learning_rate":.001,"biological_directions":None}
    agents._json(tmp_path/"results/agents/frozen_protocol.json",protocol)
    with pytest.raises(ValueError,match="incomplete"):
        analysis.run(config,tmp_path)
    audit = json.loads((tmp_path/"results/agents/analysis_audit.json").read_text())
    assert audit["status"] == "incomplete"
    assert audit["planned_policy_cells"] == 180 and audit["verified_policy_cells"] == 0
    assert len(audit["failed_or_missing_cells"]) == 180
    assert not (tmp_path/"results/inference/H4.json").exists()


def test_seed_pair_architecture_contrasts_preserve_pairing():
    first = analysis.summarize_trials(seed_fixture())
    second = first.copy()
    second["architecture"] = "gru"
    second["redirection_difference"] += .1
    records = analysis._paired_architecture_contrasts(pd.concat([first,second]),{"architectures":["rnn","gru"]},{"bootstrap_replicates":100})
    redirection = [r for r in records if r["outcome"] == "redirection_difference"]
    assert all(r["seed_pairs"] == 20 and r["estimate"] == pytest.approx(-.1) for r in redirection)
    assert all(np.allclose(r["ci95"],[-.1,-.1]) for r in redirection)


def test_figure_contains_all_strengths_mechanisms_and_separate_probe_panels(tmp_path):
    records = analysis.analyze_seed_summaries(analysis.summarize_trials(seed_fixture()),settings(),{"bootstrap_replicates":100})
    for name in ("seeds.parquet","controls.parquet","curves.json"):
        (tmp_path/name).write_text("unit-test source fixture",encoding="utf-8")
    analysis._figure_inputs(records,[tmp_path/"seeds.parquet",tmp_path/"controls.parquet",tmp_path/"curves.json"],tmp_path/"05.json")
    figure = json.loads((tmp_path/"05.json").read_text())
    assert len(figure["panels"]) == 4
    assert len(figure["panels"][0]["series"]) == len(agents.MECHANISMS)
    assert all(p["x"] == [.2] for p in figure["panels"])
    assert figure["main_figure_rule"] == "synthetic"


def complete_trial_fixture():
    frame = seed_fixture()
    frame = frame[frame.training_seed.eq(1000)].copy()
    second = frame.copy()
    second["matching_arm"] = "policy_kl"
    frame = pd.concat([frame,second],ignore_index=True)
    controls = []
    for control in ("exact","random_subspace","cue_removal","lower_action_noise"):
        copy = frame[frame.probe.eq("main")].copy()
        copy["restoration_control"] = control
        copy["redirection"] = 0
        controls.append(copy)
    frame = pd.concat([frame,*controls],ignore_index=True)
    protocol = {"biological_directions":None}
    frame["protocol_sha256"] = agents._digest(protocol)
    frame["heldout_condition"] = True
    frame["effective_coefficients"] = frame["rank"]
    config = {"ranks":[1],"strengths":[.2],"evaluation_episodes":8}
    return frame,config,protocol


def test_full_frozen_assay_roster_and_restored_episode_outcomes():
    frame,config,protocol = complete_trial_fixture()
    analysis.validate_trial_roster(frame,config,protocol,architecture="rnn",task="foraging",seed=1000)
    missing = frame.drop(index=frame.index[0])
    with pytest.raises(ValueError,match="roster"):
        analysis.validate_trial_roster(missing,config,protocol,architecture="rnn",task="foraging",seed=1000)
    duplicate = pd.concat([frame,frame.iloc[:1]])
    with pytest.raises(ValueError,match="Duplicate"):
        analysis.validate_trial_roster(duplicate,config,protocol,architecture="rnn",task="foraging",seed=1000)
    bad_restore = frame.copy()
    bad_restore.loc[bad_restore.restoration_control.eq("exact"),"redirection"] = 1
    with pytest.raises(ValueError,match="restoration"):
        analysis.validate_trial_roster(bad_restore,config,protocol,architecture="rnn",task="foraging",seed=1000)


def test_explicit_numerical_failure_cannot_be_imputed_dropped_or_treated_as_complete_H4():
    frame,config,protocol=complete_trial_fixture()
    frame['numerical_status']='available';frame['numerical_failure_reason']=None
    frame.loc[0,['success','redirection','motor_accuracy']]=np.nan
    frame.loc[0,'numerical_status']='unavailable';frame.loc[0,'numerical_failure_reason']='nonfinite_logits'
    with pytest.raises(ValueError,match='Explicit unavailable numerical'):
        analysis.validate_trial_roster(frame,config,protocol,architecture='rnn',task='foraging',seed=1000)
    frame.loc[0,'success']=0
    with pytest.raises(ValueError,match='substituted'):
        analysis.validate_trial_roster(frame,config,protocol,architecture='rnn',task='foraging',seed=1000)


@pytest.mark.parametrize("biological_ready,qualified,expected",[(False,True,"supported"),(False,False,"pending")])
def test_full_analysis_never_accepts_headline_without_biology_and_baseline_competence(tmp_path,monkeypatch,biological_ready,qualified,expected):
    # This generated-seed fixture isolates inference/claim gating; source integrity
    # and assay rosters are exercised separately, not represented as real study data.
    config = {"agents":{**settings(),"feedforward_baseline":True},"inference":{"bootstrap_replicates":100}}
    protocol = {"config_sha256":agents._digest(config["agents"]),"implementation_sha256":agents._implementation_sha256(),
                "full_config_sha256":canonical_hash(config), "H4_inference_specification":agents.h4_specification(config),
                "learning_rate":.001,"biological_directions":{"evidence_status":"validated","source_sha256":"1"*64,
                    "training_splits":["generated fixture"],"classes":{k:1. for k in analysis.PRIMARY}} if biological_ready else None,
                "biological_direction_status":"test fixture only"}
    agents._json(tmp_path/"results/agents/frozen_protocol.json",protocol)
    template = analysis.summarize_trials(seed_fixture())
    def generated_cell(directory,settings,protocol,task,architecture,seed):
        frame = template[template.training_seed.eq(seed)].copy()
        frame["task"],frame["architecture"] = task,architecture
        if not qualified:
            frame["baseline_delayed_cue"] = .5
        if protocol.get("biological_directions"):
            biological = frame.copy()
            biological["rule_type"] = "biological"
            frame = pd.concat([frame,biological],ignore_index=True)
        controls = pd.DataFrame([{"training_seed":seed,"task":task,"architecture":architecture,"restoration_control":"exact","success":1.}])
        return frame,controls,qualified,{}
    monkeypatch.setattr(analysis,"_validate_cell",generated_cell)
    result = analysis.run(config,tmp_path)
    assert result["planned_policy_cells"] == 240
    assert result["claim_status"] == expected and result["headline_claim_accepted"] is (expected=="supported")
    assert result["primary_evidence_class"]=="computational_experiment" and result["biological_transfer_status"]=="pending"
    assert result["computational_analysis_complete"]
    assert (tmp_path/"results/inference/H4.json").exists()
    assert (tmp_path/"results/figure_inputs/05.json").exists()


def test_H4_analysis_refuses_new_thresholds_or_tampered_specification(tmp_path):
    config = {"agents":{},"inference":{"agent_redirection_difference":.2}}
    protocol = {"config_sha256":agents._digest(config["agents"]),"implementation_sha256":agents._implementation_sha256(),
                "full_config_sha256":canonical_hash(config),"H4_inference_specification":agents.h4_specification(config)}
    path = tmp_path/"results/agents/frozen_protocol.json"
    agents._json(path,protocol)
    amended = {"agents":{},"inference":{"agent_redirection_difference":.1}}
    with pytest.raises(ValueError,match="H4 inference specification changed"):
        analysis.run(amended,tmp_path)
    protocol["H4_inference_specification"]["alpha"] = .2
    agents._json(path,protocol)
    with pytest.raises(ValueError,match="H4 inference specification changed"):
        analysis.run(config,tmp_path)
    assert not (tmp_path/"results/inference/H4.json").exists()
