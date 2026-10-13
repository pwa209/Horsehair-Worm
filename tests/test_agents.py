"""Numerical protections for computational causality and frozen-policy testing."""
import copy

import numpy as np
import pytest

from horsehair import agents
from horsehair.core import canonical_hash


def test_episode_cap_and_seed_reproducibility():
    for task in agents.TASKS:
        first = agents.Environment(task, 44, "heldout")
        second = agents.Environment(task, 44, "heldout")
        assert np.array_equal(first.observe(), second.observe())
        for _ in range(200):
            a = first.step(4)
            b = second.step(4)
            assert np.array_equal(a[0], b[0])
            assert a[1:] == b[1:]
            if a[2]:
                break
        assert first.done and first.t <= 200


def test_delayed_cue_is_absent_during_choice():
    env = agents.Environment("delayed_choice", 8, "heldout")
    assert env.observe()[10:12].sum() == 1
    for _ in range(env.delay+2):
        env.step(4)
    assert env.observe()[10:12].sum() == 0
    assert env.observe()[12] == 1
    _, reward, done, result = env.step(3 if env.target else 2)
    assert done and reward == 1 and result["success"]


def test_food_and_gravity_are_independent():
    pairs = {(agents.Environment("corridor", s).food_endpoint, agents.Environment("corridor", s).gravity) for s in range(100)}
    assert len(pairs) == 4


def test_heldout_goal_positions_and_delay_lengths():
    train = [agents.Environment("foraging", s) for s in range(50)]
    heldout = [agents.Environment("foraging", s, "heldout") for s in range(50)]
    assert {tuple(e.goal) for e in train}.isdisjoint({tuple(e.goal) for e in heldout})
    assert min(agents.Environment("delayed_choice", s, "heldout").delay for s in range(20)) > max(agents.Environment("delayed_choice", s).delay for s in range(20))


def test_architecture_budgets_are_actually_matched():
    pytest.importorskip("torch")
    for target in (25000, 100000):
        counts = [agents.make_policy(a, target).parameter_count for a in (*agents.ARCHITECTURES, "feedforward")]
        assert max(counts)/min(counts) < 1.05
        assert all(abs(count-target)/target < .05 for count in counts)


@pytest.mark.parametrize("architecture", (*agents.ARCHITECTURES, "feedforward"))
@pytest.mark.parametrize("mechanism", agents.MECHANISMS)
def test_restore_original_action_probabilities_and_frozen_weights(architecture, mechanism):
    torch = pytest.importorskip("torch")
    torch.set_num_threads(1)
    agents.seed_everything(66)
    policy = agents.make_policy(architecture, 25000)
    policy.requires_grad_(False)
    before = agents.state_hash(policy)
    perturbation = agents.Perturbation(mechanism, 2, [.2, -.1])
    result = agents._restoration_check(policy, "foraging", [7, 8], perturbation)
    assert result["base_weights_unchanged"]
    assert result["restoration_max_probability_error"] <= 1e-7
    assert before == agents.state_hash(policy)
    assert all(p.grad is None for p in policy.parameters())


def test_low_rank_matrix_rank_and_norm():
    torch = pytest.importorskip("torch")
    for rank in (1, 2, 4, 8):
        theta = np.arange(1, rank+1, dtype=float)*.1
        perturbation = agents.Perturbation("recurrent_gate", rank, theta)
        matrix = perturbation.recurrent(torch.eye(20))
        assert torch.linalg.matrix_rank(matrix, atol=1e-6) <= rank
        assert float(matrix.norm()) == pytest.approx(np.linalg.norm(theta), abs=1e-6)


def test_evaluation_no_optimisation_and_reproducible_restoration():
    torch = pytest.importorskip("torch")
    torch.set_num_threads(1)
    agents.seed_everything(3)
    policy = agents.make_policy("gru", 25000)
    policy.requires_grad_(False)
    original = copy.deepcopy(policy.state_dict())
    baseline, _ = agents.evaluate(policy, "delayed_choice", [17, 18, 19])
    agents.evaluate(policy, "delayed_choice", [17, 18, 19], perturbation=agents.Perturbation("unstructured", 2, [.4, .2]))
    restored, _ = agents.evaluate(policy, "delayed_choice", [17, 18, 19])
    assert baseline == restored
    assert all(torch.equal(original[k], policy.state_dict()[k]) for k in original)
    assert all(p.grad is None for p in policy.parameters())


@pytest.mark.parametrize('deterministic',[False,True])
def test_nonfinite_actions_are_unavailable_in_stochastic_and_deterministic_controls(deterministic):
    torch=pytest.importorskip('torch')
    logits=torch.tensor([[0.,0.,float('inf'),0.,0.]])
    with pytest.raises(agents.NumericalPolicyError,match='Nonfinite'):
        agents._sample_action(logits,np.random.default_rng(1),deterministic)


def numerical_fixture_policy(fail_at=14, intervention_only=False):
    torch=pytest.importorskip('torch')
    class Policy(torch.nn.Module):
        def __init__(self):
            super().__init__();self.weight=torch.nn.Parameter(torch.zeros(1));self.architecture='gru';self.calls=[]
        def initial_state(self,n,device=None):
            state=torch.zeros(n,2);state[0,1]=1.;return state
        def forward(self,obs,state,perturbation=None):
            self.calls.append(len(obs))
            logits=torch.zeros(len(obs),agents.ACTION_DIM);logits[:,2]=100.
            if perturbation is not None or not intervention_only:
                logits[(state[:,1]>0)&(state[:,0]>=fail_at),2]=float('inf')
            new_state=state.clone();new_state[:,0]+=1
            return logits,torch.zeros(len(obs)),new_state
    return Policy()


def delayed_roster():
    return [next(s for s in range(40_000_000,40_000_100) if agents.Environment('delayed_choice',s,'heldout').delay==delay) for delay in (10,20)]


def test_evaluation_never_propagates_or_samples_completed_batch_members():
    policy=numerical_fixture_policy()
    before=agents.state_hash(policy)
    rows,meta=agents.evaluate(policy,'delayed_choice',delayed_roster())
    assert len(rows)==2 and meta['numerically_available']
    assert sorted(r['steps'] for r in rows)==[12,22]
    assert policy.calls[:12]==[2]*12 and policy.calls[12:]==[1]*10
    assert agents.state_hash(policy)==before


def test_active_numerical_failure_retains_null_outcomes_diagnostics_and_other_episode():
    policy=numerical_fixture_policy(fail_at=5)
    rows,meta=agents.evaluate(policy,'delayed_choice',delayed_roster(),on_numerical_failure='record')
    failed=next(r for r in rows if r['numerical_status']=='unavailable')
    valid=next(r for r in rows if r['numerical_status']=='available')
    assert all(failed[k] is None for k in ('success','redirection','motor_accuracy'))
    assert failed['steps']==5 and failed['numerical_failure_step']==5 and failed['numerical_failure_reason']=='nonfinite_logits'
    assert valid['steps']==22 and meta['environment_steps']==27 and not meta['numerically_available']
    assert meta['unavailable_episodes'][0]['episode_seed']==failed['episode_seed']
    with pytest.raises(agents.NumericalPolicyError,match='step'):
        agents.evaluate(numerical_fixture_policy(fail_at=5),'delayed_choice',delayed_roster())


def test_kl_calibration_ignores_terminal_members_and_rejects_active_nonfinite_state():
    policy=numerical_fixture_policy(intervention_only=True)
    _,meta=agents.evaluate(policy,'delayed_choice',delayed_roster(),capture=True)
    assert agents._clean_kl(policy,agents.Perturbation('recurrent_gate',1,[.1]),meta['sequence'])==pytest.approx(0.)
    policy=numerical_fixture_policy(fail_at=5,intervention_only=True)
    _,meta=agents.evaluate(policy,'delayed_choice',delayed_roster(),capture=True)
    with pytest.raises(agents.NumericalPolicyError,match='calibration'):
        agents._clean_kl(policy,agents.Perturbation('recurrent_gate',1,[.1]),meta['sequence'])


def test_final_fails_before_development_protocol_is_frozen(tmp_path):
    config = {"agents": {"n_envs": 2, "total_steps": 16}}
    with pytest.raises(RuntimeError, match="frozen development protocol"):
        agents.run(config, tmp_path, mode="final", task_index=0)


def test_biological_constraints_are_pending_without_a_validated_fit(tmp_path):
    value, status = agents._load_biological_directions({}, tmp_path)
    assert value is None and status.startswith("pending")


def test_real_ppo_smoke_produces_optimizer_and_seed_checkpoint(tmp_path):
    torch = pytest.importorskip("torch")
    config = {"capacity": 25000, "n_envs": 4, "rollout_steps": 8, "ppo_epochs": 1,
              "total_steps": 32, "baseline_episodes": 2, "device": "cpu"}
    policy, result = agents.train(config, "delayed_choice", "rnn", 10, tmp_path, .001)
    checkpoint = torch.load(tmp_path/"checkpoint.pt", weights_only=False)
    assert checkpoint["steps"] == 32
    assert checkpoint["optimizer"]["state"]
    assert result["steps_per_second"] > 0
    assert set(result["competence"]) == set(agents.PROBES)
    assert agents.state_hash(policy) == result["weight_sha256"]
    assert checkpoint["seed"] == 10


def test_resumed_ppo_exact_steps_weights_optimizer_and_hidden_state(tmp_path):
    torch = pytest.importorskip("torch")
    config = {"capacity": 25000, "n_envs": 4, "rollout_steps": 8, "ppo_epochs": 1,
              "total_steps": 64, "baseline_episodes": 2, "device": "cpu", "checkpoint_steps": 32}
    uninterrupted, _ = agents.train(config, "delayed_choice", "gru", 11, tmp_path/"uninterrupted", .001)
    agents.train(config, "delayed_choice", "gru", 11, tmp_path/"resumed", .001, steps=32)
    resumed, result = agents.train(config, "delayed_choice", "gru", 11, tmp_path/"resumed", .001, steps=64)
    first = torch.load(tmp_path/"uninterrupted/checkpoint.pt", weights_only=False)
    second = torch.load(tmp_path/"resumed/checkpoint.pt", weights_only=False)
    assert result["training_steps"] == first["steps"] == second["steps"] == 64
    assert agents.state_hash(uninterrupted) == agents.state_hash(resumed)
    assert torch.equal(first["recurrent_state"], second["recurrent_state"])
    assert np.array_equal(first["observation"], second["observation"])
    assert first["rollout_rng"] == second["rollout_rng"]
    for key, state in first["optimizer"]["state"].items():
        assert all(torch.equal(value, second["optimizer"]["state"][key][name]) for name, value in state.items())
    # Terminal boundaries are reset during the post-update ordered replay.
    for index, environment in enumerate(second["environments"]):
        if environment.t == 0:
            assert torch.count_nonzero(second["recurrent_state"][index]) == 0


def test_checkpoint_rejects_changed_config_lr_or_weight_bytes(tmp_path):
    torch = pytest.importorskip("torch")
    config = {"capacity": 25000, "n_envs": 4, "rollout_steps": 8, "ppo_epochs": 1,
              "total_steps": 32, "baseline_episodes": 1, "device": "cpu"}
    agents.train(config, "delayed_choice", "rnn", 12, tmp_path, .001)
    with pytest.raises(RuntimeError, match="configuration"):
        agents.train(dict(config, entropy_coefficient=.02), "delayed_choice", "rnn", 12, tmp_path, .001)
    with pytest.raises(RuntimeError, match="learning_rate"):
        agents.train(config, "delayed_choice", "rnn", 12, tmp_path, .003)
    checkpoint = torch.load(tmp_path/"checkpoint.pt", weights_only=False)
    next(iter(checkpoint["state_dict"].values())).flatten()[0] += .01
    torch.save(checkpoint, tmp_path/"checkpoint.pt")
    with pytest.raises(RuntimeError, match="weight hash"):
        agents.train(config, "delayed_choice", "rnn", 12, tmp_path, .001)


@pytest.mark.parametrize("field,value", [("final_seeds", [1000, 1000]), ("architectures", ["rnn", "rnn"]),
                                         ("lr_grid", [.001, .001]), ("envs", ["foraging", "foraging"]),
                                         ("pilot_seeds", [1000, 1001])])
def test_duplicate_cells_or_seed_overlap_are_rejected(field, value):
    with pytest.raises(ValueError):
        agents._cells({field: value})


def test_freeze_rejects_incomplete_or_mismatched_development_summary(tmp_path):
    config = {"development_seeds": [100], "final_seeds": [1000], "lr_grid": [.001], "total_steps": 32}
    path = tmp_path/"results/agents/development/foraging_rnn_100_lr0.001/summary.json"
    agents._json(path, {"config_sha256": "wrong", "complete_training_budget": False})
    with pytest.raises(RuntimeError, match="Incomplete or configuration/source-mismatched"):
        agents.freeze_protocol(config, tmp_path)


@pytest.mark.parametrize("changed",[{"agent_redirection_difference":.3},{"agent_competence_loss_max":.1},
                                  {"alpha":.1},{"bootstrap_replicates":500},
                                  {"agent_reference_point":{"strength":.4}}])
def test_final_refuses_changed_H4_analysis_after_freeze(tmp_path,changed):
    original = {"agents":{},"inference":{"alpha":.05}}
    protocol = {"config_sha256":agents._digest(original["agents"]),"implementation_sha256":agents._implementation_sha256(),
                "full_config_sha256":canonical_hash(original),"H4_inference_specification":agents.h4_specification(original)}
    agents._json(tmp_path/"results/agents/frozen_protocol.json",protocol)
    amended = {"agents":{},"inference":{**original["inference"],**changed}}
    with pytest.raises(RuntimeError,match="H4 inference specification changed"):
        agents.run(amended,tmp_path,mode="final",task_index=0)


def test_cpu_evaluation_pilot_measures_actual_cpu_without_training(tmp_path):
    torch = pytest.importorskip("torch")
    config = {"agents":{"capacity":25000,"n_envs":4,"rollout_steps":4,"ppo_epochs":1,"total_steps":16,
              "pilot_steps":16,"baseline_episodes":1,"pilot_evaluation_episodes":2,"ranks":[1],"strengths":[0.],"device":"cpu"}}
    pilot = agents.run(config,tmp_path,"pilot",0)["reports"][0]
    checkpoint = tmp_path/"results/agents/pilot/foraging_rnn_900/checkpoint.pt"
    original_bytes = checkpoint.read_bytes()
    result = agents.run(config,tmp_path,"cpu-evaluation-pilot",0)["reports"][0]
    assert result["training_steps"] == 0 and result["evaluation_device"] == "cpu"
    assert result["evaluation_episodes"] == 4 and result["evaluation_environment_steps"]>0
    assert result["evaluation_seconds"]>0 and result["full_config_sha256"] == canonical_hash(config)
    assert result["base_weight_sha256"] == pilot["weight_sha256"]
    assert checkpoint.read_bytes() == original_bytes
    assert result["working_bytes"]>0 and len(result["source_sha256"]) == 64
    assert not result["baseline_competence_qualified"]


def test_required_biological_export_prevents_freeze_without_writing_a_protocol(tmp_path):
    config = {"agents":{"require_biological_directions_for_freeze":True,
                         "biological_direction_file":"results/behaviour/biological_directions.json"}}
    with pytest.raises(RuntimeError,match="Biological directions are required"):
        agents.run(config,tmp_path,mode="freeze")
    assert not (tmp_path/"results/agents/frozen_protocol.json").exists()


def test_native_benchmark_design_covers_every_configured_cell_and_both_seeds():
    config = {"agents":{"feedforward_baseline":True}}
    cells = agents.benchmark_cells(config)
    assert len(cells)==12
    assert {(task,architecture) for task,architecture,_ in cells}=={
        (task,architecture) for task in agents.TASKS for architecture in (*agents.ARCHITECTURES,"feedforward")}
    assert all(seeds==(900,901) for _,_,seeds in cells)
    spec = agents.benchmark_specification(config)
    assert spec["training_steps"]==32768 and spec["calibration_episodes"]==32 and spec["kl_bisections"]==10
    assert set(spec["mechanisms"])==set(agents.MECHANISMS)
    with pytest.raises(ValueError,match="complete PPO rollouts"):
        agents.benchmark_specification({"agents":{"benchmark_training_steps":17}})


@pytest.fixture
def measured_technical_agent_fixture(tmp_path):
    """Real tiny CPU algorithms; temporary fixture is never a native GPU claim."""
    pytest.importorskip("torch")
    config = {"agents":{"architectures":["gru"],"tasks":["delayed_choice"],"pilot_seeds":[900,901],
              "capacity":25000,"n_envs":4,"rollout_steps":4,"ppo_epochs":1,"total_steps":16,
              "lr_grid":[.0003,.001,.003],
              "benchmark_training_steps":16,"benchmark_evaluation_episodes":1,"baseline_episodes":1,
              "evaluation_episodes":2,"calibration_episodes":8,"kl_bisections":3,"ranks":[1],"strengths":[0.],"device":"cpu"},
              "resources":{"cpu_ceiling":64}}
    training = agents.run(config,tmp_path,"benchmark-training",0)
    cpu = agents.run(config,tmp_path,"benchmark-cpu-evaluation",0)
    aggregate = agents.run(config,tmp_path,"benchmark-aggregate")
    return config,training,cpu,aggregate


def test_actual_benchmark_aggregation_full_grid_separate_competence_and_hashes(tmp_path,measured_technical_agent_fixture):
    config,training,cpu,aggregate = measured_technical_agent_fixture
    assert len(training["reports"])==len(cpu["reports"])==2
    for record in training["reports"]:
        assert record["training_steps"]==16 and record["software_qualified"]
        assert record["seconds"]>0 and record["direction_selection_seconds"]>0 and record["working_files"]>=5
        assert record["policy_competence_qualified"] is False
    for record in cpu["reports"]:
        assert record["training_steps"]==0 and record["evaluation_device"]=="cpu"
        assert record["complete_mechanism_grid"] and record["grid_points"]==14 and record["condition_groups"]==112
        assert record["trial_rows"]==112 and record["calibration_episodes"]==8 and record["kl_bisections"]==3
        assert record["restoration_qualified"] and record["base_weights_unchanged"]
    assert aggregate["measured_policies"]==2 and aggregate["architecture_task_cells"]==1
    assert not aggregate["scientific_result"] and aggregate["complete_architecture_task_coverage"]
    for path,value in aggregate["source_result_hashes"].items():
        assert agents.digest(agents.Path(path))==value
    changed = copy.deepcopy(config)
    changed["agents"]["calibration_episodes"]=12
    with pytest.raises(RuntimeError,match="configuration/source/specification"):
        agents.aggregate_benchmarks(changed,tmp_path)


def test_cpu_benchmark_cannot_claim_complete_after_missing_trial_condition(tmp_path,measured_technical_agent_fixture):
    import pandas as pd
    _,_,cpu,_ = measured_technical_agent_fixture
    destination = tmp_path/"results/agents/benchmark-cpu-evaluation/delayed_choice_gru_900"
    path = destination/"agent_trials.parquet"
    trials = pd.read_parquet(path)
    trials = trials.iloc[1:]
    trials.to_parquet(path,index=False)
    record = copy.deepcopy(cpu["reports"][0])
    record["trial_rows"] = len(trials)
    record["trial_file_sha256"] = agents.digest(path)
    with pytest.raises(RuntimeError,match="complete configured"):
        agents._validate_benchmark_grid(measured_technical_agent_fixture[0],destination,record)
    with pytest.raises(RuntimeError,match="source evidence changed"):
        agents.aggregate_benchmarks(measured_technical_agent_fixture[0],tmp_path)


def test_cpu_benchmark_retains_attempted_grid_but_not_finite_qualification_for_unavailable_episode(tmp_path,measured_technical_agent_fixture):
    import pandas as pd
    config,_,cpu,_=measured_technical_agent_fixture
    destination=tmp_path/'results/agents/benchmark-cpu-evaluation/delayed_choice_gru_900'
    path=destination/'agent_trials.parquet';trials=pd.read_parquet(path)
    trials.loc[0,['success','redirection','motor_accuracy']]=np.nan
    trials.loc[0,'numerical_status']='unavailable';trials.loc[0,'numerical_failure_reason']='nonfinite_hidden_state'
    trials.loc[0,'numerical_failure_step']=5
    trials.to_parquet(path,index=False)
    record=copy.deepcopy(cpu['reports'][0]);record['trial_file_sha256']=agents.digest(path)
    grid=agents._validate_benchmark_grid(config,destination,record)
    assert grid['complete_mechanism_grid'] and not grid['numerically_valid_grid'] and grid['unavailable_episode_rows']==1
    trials.loc[0,'success']=0  # assigning an outcome to a failed episode is forbidden
    trials.to_parquet(path,index=False);record['trial_file_sha256']=agents.digest(path)
    with pytest.raises(RuntimeError,match='substituted'):
        agents._validate_benchmark_grid(config,destination,record)


def test_available_tag_cannot_hide_invalid_measured_calibration_or_endpoints(tmp_path,measured_technical_agent_fixture):
    import pandas as pd
    config,_,cpu,_=measured_technical_agent_fixture
    destination=tmp_path/'results/agents/benchmark-cpu-evaluation/delayed_choice_gru_900'
    path=destination/'agent_trials.parquet';original=pd.read_parquet(path)
    record=copy.deepcopy(cpu['reports'][0])
    for column,value in [('clean_policy_kl',np.nan),('clean_policy_kl',-.1),('achieved_parameter_norm',np.inf),
                         ('target_policy_kl',-.1),('success',.5),('redirection',2),('motor_accuracy',1.1)]:
        trials=original.copy();trials[column]=trials[column].astype(float)
        trials.loc[0,column]=value;trials.to_parquet(path,index=False)
        record['trial_file_sha256']=agents.digest(path)
        with pytest.raises(RuntimeError,match='calibration|ranges'):
            agents._validate_benchmark_grid(config,destination,record)
    trials=original.copy();trials.loc[0,'clean_policy_kl']=np.nan
    trials.loc[0,'calibration_numerical_status']='unavailable';trials.to_parquet(path,index=False)
    record['trial_file_sha256']=agents.digest(path)
    grid=agents._validate_benchmark_grid(config,destination,record)
    assert grid['complete_mechanism_grid'] and not grid['numerically_valid_grid'] and grid['unavailable_calibration_rows']==1


def test_admission_requires_native_cuda_and_does_not_use_competence_to_choose_timing(tmp_path,measured_technical_agent_fixture,monkeypatch):
    from horsehair import admission
    config,training,_,_ = measured_technical_agent_fixture
    identity = agents.source_identity(agents.Path(agents.__file__).resolve().parents[2])["sha256"]
    native = {"fixture":{"job_id":"fixture","state":"COMPLETED","elapsed_seconds":100.,"cpus":1,"gpus":1,"peak_rss_bytes":1024**3}}
    # The actual local CPU fixture cannot qualify a native GPU allocation.
    with pytest.raises(admission.AdmissionError):
        admission._agents(config,tmp_path,native,identity,1.5,"agents-development")
    # Hardware-only boundary is mocked explicitly; all numerical receipts,
    # cohorts, file hashes and configured work units remain actual fixture data.
    monkeypatch.setattr(admission,"_proof",lambda record,*args,**kwargs:native["fixture"])
    original_checked = agents._benchmark_checked
    def fixture_checked(*args,**kwargs):
        record = original_checked(*args,**kwargs)
        record["slurm_job_id"] = "fixture"
        record["allocated_cpus"] = 1
        return record
    monkeypatch.setattr(agents,"_benchmark_checked",fixture_checked)
    forecast = admission._agents(config,tmp_path,native,identity,1.5,"agents-development")
    assert forecast["workload"]["timing_policies"]==2
    assert forecast["workload"]["benchmark_competence_qualified_policies"]==0
    assert forecast["workload"]["development_training_fits"]==15 and forecast["workload"]["final_training_fits"]==20
    assert forecast["cpu_hours"]>0 and forecast["gpu_hours"]>0 and forecast["additional_files"]>0
    assert forecast["all_remaining_training_gpu_hours"]>forecast["gpu_hours"]
    missing = tmp_path/"results/agents/benchmark-training/delayed_choice_gru_901/training_benchmark.json"
    missing.rename(missing.with_suffix(".fixture-removed"))
    with pytest.raises(admission.AdmissionError,match="timing coverage is pending"):
        admission._agents(config,tmp_path,native,identity,1.5,"agents-development")


def test_benchmark_provenance_matches_native_array_task_identity(tmp_path,monkeypatch):
    pytest.importorskip("torch")
    (tmp_path/"actual-file.txt").write_text("generated software test fixture")
    monkeypatch.setenv("SLURM_JOB_ID","34567")
    monkeypatch.setenv("SLURM_ARRAY_JOB_ID","34500")
    monkeypatch.setenv("SLURM_ARRAY_TASK_ID","11")
    record = agents._benchmark_provenance({"agents":{}},tmp_path,"cpu")
    assert record["slurm_job_id"]=="34500_11" and record["working_files"]==1


def test_final_admission_consumes_actual_cpu_roster_and_counts_evaluation_storage(tmp_path,measured_technical_agent_fixture,monkeypatch):
    from horsehair import admission
    config,_,_,aggregate = measured_technical_agent_fixture
    settings = config["agents"]
    directory = tmp_path/"results/agents"
    selected_weights = []
    # Genuine tiny development PPO checkpoints; this reduced design is a
    # software fixture, not the proposal's180 development fits or H4 evidence.
    for task,architecture,seed,rate in agents._cells(settings,development=True):
        destination = directory/"development"/agents._cell_name(task,architecture,seed,rate)
        _,summary = agents.train(settings,task,architecture,seed,destination,rate)
        if rate==settings["lr_grid"][0]:
            selected_weights.append(summary["weight_sha256"])
    protocol = {"config_sha256":agents._digest(settings),"full_config_sha256":canonical_hash(config),
                "implementation_sha256":agents._implementation_sha256(),"H4_inference_specification":agents.h4_specification(config),
                "learning_rate":settings["lr_grid"][0],"directions":{f"{mechanism}:1":[1.] for mechanism in agents.MECHANISMS},
                "development_seeds":list(range(100,105)),"final_seeds":list(range(1000,1020)),
                "source_development_weights":selected_weights}
    agents._json(directory/"frozen_protocol.json",protocol)
    identity = aggregate["source_sha256"]
    native = {"gpu-fixture":{"job_id":"gpu-fixture","state":"COMPLETED","elapsed_seconds":100.,"cpus":1,"gpus":1,"peak_rss_bytes":1024**3},
              "cpu-fixture":{"job_id":"cpu-fixture","state":"COMPLETED","elapsed_seconds":200.,"cpus":2,"gpus":0,"peak_rss_bytes":1024**3}}
    original_checked = agents._benchmark_checked
    def fixture_checked(*args,**kwargs):
        record = original_checked(*args,**kwargs)
        record["slurm_job_id"] = "gpu-fixture" if record["benchmark_kind"]=="training" else "cpu-fixture"
        record["allocated_cpus"] = native[record["slurm_job_id"]]["cpus"]
        return record
    monkeypatch.setattr(agents,"_benchmark_checked",fixture_checked)
    monkeypatch.setattr(admission,"_proof",lambda record,*args,**kwargs:native[record["slurm_job_id"]])
    forecast = admission._agents(config,tmp_path,native,identity,1.5,"agents-final")
    assert forecast["workload"]["final_training_fits"]==20
    assert forecast["parameters"]["agent-evaluation"]["cpus"]==2 and not forecast["parameters"]["agent-evaluation"]["gpu"]
    assert forecast["cpu_hours"]>forecast["gpu_hours"]
    assert forecast["all_remaining_training_gpu_hours"]==pytest.approx(forecast["gpu_hours"])
    assert forecast["additional_files"]>=20*9
    assert tmp_path/"state/agent-cpu-evaluation-benchmark.json" in forecast["inputs"]
    original_grid=agents._validate_benchmark_grid
    def unavailable_grid(*args):
        return {**original_grid(*args),"numerically_valid_grid":False}
    monkeypatch.setattr(agents,'_validate_benchmark_grid',unavailable_grid)
    with pytest.raises(admission.AdmissionError,match='unavailable numerical evidence'):
        admission._agents(config,tmp_path,native,identity,1.5,'agents-final')
    monkeypatch.setattr(agents,'_validate_benchmark_grid',original_grid)
    entry = aggregate["records"][0]
    entry["sha256"] = "0"*64
    agents._json(tmp_path/"state/agent-cpu-evaluation-benchmark.json",aggregate)
    with pytest.raises(admission.AdmissionError,match="changed native evaluation evidence"):
        admission._agents(config,tmp_path,native,identity,1.5,"agents-final")
