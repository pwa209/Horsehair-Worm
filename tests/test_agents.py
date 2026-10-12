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
