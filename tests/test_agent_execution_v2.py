"""Small numerical fixtures, never pretrained or final scientific evidence."""
import copy
import json
from pathlib import Path

import numpy as np
import pytest

from horsehair import agents, agent_execution_v2 as execution
from horsehair.core import atomic_json, canonical_hash, digest, read_json

torch = pytest.importorskip("torch")


def fixture_plan(tmp_path, *, canary=True, benchmark_directions=False, omit_default_kl=False):
    config = json.loads((Path(__file__).resolve().parents[1] / "configs/study.json").read_text())
    settings = config["agents"]
    settings.update(capacity=2000, observation_window=2, total_steps=16,
        ranks=[1], strengths=[0.], evaluation_episodes=1, calibration_episodes=1, kl_bisections=1)
    if omit_default_kl: settings.pop("kl_bisections")
    seed = 900 if canary else 1000
    torch.manual_seed(15)
    policy = agents.make_policy("gru", settings["capacity"], settings["observation_window"])
    checkpoint = tmp_path / "fixture-checkpoint.pt"
    torch.save({"config": settings, "config_sha256": agents._digest(settings),
        "implementation_sha256": agents._implementation_sha256(), "state_dict": policy.state_dict(),
        "weight_sha256": agents.state_hash(policy), "steps": 16, "training_budget": 16,
        "seed": seed, "architecture": "gru", "task": "foraging", "learning_rate": .0003}, checkpoint)
    protocol = {"config_sha256": agents._digest(settings), "full_config_sha256": canonical_hash(config),
        "implementation_sha256": agents._implementation_sha256(), "H4_inference_specification": agents.h4_specification(config),
        "learning_rate": .0003, "biological_directions": None,
        "directions": {f"{m}:1": [1.] for m in agents.MECHANISMS},
        "final_seeds": list(settings["final_seeds"])}
    case = tmp_path / "case"
    plan = execution.prepare_plan(config, protocol, checkpoint, case, checkpoint_sha256=digest(checkpoint),
        primary_agents_sha256=agents._implementation_sha256(), task="foraging", architecture="gru", seed=seed,
        canary={"evaluation_episodes": 1} if canary else None, benchmark_directions=benchmark_directions)
    return case, plan, policy


@pytest.fixture
def short_fixture_environments(monkeypatch):
    original = agents.Environment
    # Bound test runtime while exercising the actual primary evaluation code.
    # Native canaries retain the original literal 200-step environment.
    monkeypatch.setattr(agents, "Environment", lambda *args, **kwargs: original(*args, **dict(kwargs, max_steps=2)))


@pytest.mark.parametrize("mechanism", agents.MECHANISMS)
@pytest.mark.parametrize("random_restoration", [False, True])
@pytest.mark.parametrize("rank", [1, 4])
def test_float32_adapter_exactly_reproduces_original_perturbation_arithmetic(mechanism, random_restoration, rank):
    torch.manual_seed(42)
    policy = agents.make_policy("gru", 2000, 2).eval()
    theta = np.linspace(.1, .2, rank)
    original = agents.Perturbation(mechanism, rank, theta, random_restoration=random_restoration)
    revised = execution._perturbation(mechanism, rank, theta, torch.float32, random_restoration=random_restoration)
    obs, state = torch.randn(2, agents.OBS_DIM), torch.randn(2, policy.state_dim)
    first, p1 = agents._perturbed_policy(policy, original)
    second, p2 = agents._perturbed_policy(execution._precision_policy(policy, torch.float32), revised)
    with torch.no_grad():
        reference, actual = first(obs, state, p1), second(obs, state, p2)
    for a, b in zip(reference, actual):
        torch.testing.assert_close(a, b, rtol=0, atol=0)


def test_float64_copy_preserves_original_weight_values_and_uses_double_inputs_states():
    policy = agents.make_policy("gru", 2000, 2)
    before = agents.state_hash(policy)
    revised = execution._precision_policy(policy, torch.float64)
    state = revised.initial_state(2)
    assert state.dtype == torch.float64
    outputs = revised(torch.zeros(2, agents.OBS_DIM), state,
        execution._perturbation("recurrent_gate", 1, [.2], torch.float64))
    assert all(value.dtype == torch.float64 and torch.isfinite(value).all() for value in outputs)
    for p, q in zip(policy.parameters(), revised.parameters()):
        torch.testing.assert_close(p.double(), q, rtol=0, atol=0)
    assert agents.state_hash(policy) == before


def test_plan_binds_original_and_amended_protocols_without_mutating_checkpoint(tmp_path):
    case, plan, _ = fixture_plan(tmp_path)
    assert execution._load_plan(case) == plan
    assert plan["original_protocol_sha256"] != plan["evaluation_protocol_sha256"]
    assert plan["evaluation_protocol_sha256"] == agents._digest(plan["evaluation_protocol"])
    assert plan["evaluation_dtype"] == "float64"
    assert plan["scientific_result"] is False and plan["admission_authority"] is False
    assert digest(Path(plan["checkpoint"])) == plan["checkpoint_sha256"]
    assert read_json(case / "plan-binding.json")["plan_sha256"] == digest(case / "plan.json")
    with pytest.raises(ValueError, match="binding"):
        execution._load_plan(case, "0"*64)


def test_original_implicit_ten_bisection_default_is_preserved(tmp_path):
    case, plan, _ = fixture_plan(tmp_path, omit_default_kl=True)
    assert plan["evaluation_settings"].get("kl_bisections", 10) == 10
    assert execution._load_plan(case) == plan


@pytest.mark.parametrize("field", ["condition", "source", "checkpoint", "settings", "protocol"])
def test_plan_refuses_changed_source_checkpoint_settings_or_condition_roster(tmp_path, field):
    case, plan, _ = fixture_plan(tmp_path)
    if field == "condition": plan["conditions"] = plan["conditions"][:-1]
    elif field == "source": plan["execution_module_sha256"] = "changed"
    elif field == "checkpoint": Path(plan["checkpoint"]).write_bytes(b"corrupt")
    elif field == "settings": plan["evaluation_settings"]["evaluation_episodes"] = 2
    else: plan["evaluation_protocol"]["evaluation_dtype"] = "float32"
    atomic_json(case / "plan.json", plan)
    with pytest.raises(ValueError): execution._load_plan(case)


def test_benchmark_directions_are_explicitly_technical_and_cannot_be_final(tmp_path):
    case, plan, _ = fixture_plan(tmp_path, benchmark_directions=True)
    assert plan["evaluation_protocol"]["direction_source"] == "original_predeclared_technical_benchmark"
    expected = agents._normalize(np.random.default_rng(420).normal(size=1)).tolist()
    assert plan["evaluation_protocol"]["directions"]["recurrent_gate:1"] == expected
    assert execution._load_plan(case)["canary"] is True
    plan["canary"] = False
    atomic_json(case / "plan.json", plan)
    with pytest.raises(ValueError, match="binding|Final evaluation"): execution._load_plan(case)


def test_complete_canary_resumes_conditions_exactly_once_and_assembles_original_endpoints(tmp_path, short_fixture_environments):
    case, plan, _ = fixture_plan(tmp_path)
    first = execution.run_chunk(case, max_conditions=2)
    assert first["completed_conditions"] == 2
    with pytest.raises(ValueError, match="incomplete"): execution.assemble(case)
    second = execution.run_chunk(case, max_conditions=len(plan["conditions"]))
    assert second["completed_conditions"] == len(plan["conditions"]) == 14
    assert second["newly_completed_conditions"] == 12
    again = execution.run_chunk(case, max_conditions=1)
    assert again["newly_completed_conditions"] == 0
    summary = execution.assemble(case)
    assert summary["trial_rows"] == 14*8
    assert summary["status"] == "complete_precision_canary"
    assert execution.assemble(case) == summary
    assert len(list(case.glob("condition-*.json"))) == 14
    assert summary["scientific_result"] is False and summary["admission_authority"] is False


def test_failed_numerical_rows_are_preserved_but_never_committed_or_filled(tmp_path, monkeypatch, short_fixture_environments):
    case, _, _ = fixture_plan(tmp_path)
    original = execution._condition
    def invalid(*args):
        rows, restoration, failures = original(*args)
        rows[0].update(success=None, redirection=None, motor_accuracy=None, numerical_status="unavailable",
            numerical_failure_reason="fixture_nonfinite", numerical_failure_step=1)
        return rows, restoration, failures
    monkeypatch.setattr(execution, "_condition", invalid)
    with pytest.raises(ValueError, match="unavailable numerical evidence"):
        execution.run_chunk(case, max_conditions=1)
    assert not list(case.glob("condition-*.json"))
    raw = list(case.glob("*-uncommitted.parquet"))
    assert len(raw) == 1
    import pandas as pd
    rows = pd.read_parquet(raw[0])
    assert rows.iloc[0].numerical_status == "unavailable"
    assert rows.iloc[0][["success", "redirection", "motor_accuracy"]].isna().all()
    assert list(case.glob("failure-*.json"))
    with pytest.raises(ValueError, match="incomplete"): execution.assemble(case)


@pytest.mark.parametrize("corrupt", ["duplicate", "artifact", "restoration"])
def test_resume_refuses_duplicate_or_corrupt_committed_conditions(tmp_path, corrupt, short_fixture_environments):
    case, _, _ = fixture_plan(tmp_path)
    execution.run_chunk(case, max_conditions=1)
    path = next(case.glob("condition-*.json")); receipt = read_json(path)
    if corrupt == "duplicate": atomic_json(case / "condition-duplicate.json", receipt)
    elif corrupt == "artifact": (case / receipt["artifact_name"]).write_bytes(b"corrupt")
    else:
        # A finite but invalid restoration is also refused.
        receipt["restoration"]["restoration_max_probability_error"] = .5
        atomic_json(path, receipt)
    with pytest.raises(ValueError): execution.run_chunk(case, max_conditions=1)


def test_diagnostics_cover_both_precisions_and_keep_original_checkpoint_hash(tmp_path, short_fixture_environments):
    case, plan, _ = fixture_plan(tmp_path)
    result = execution.diagnostics(case)
    assert {row["dtype"] for row in result["reports"]} == {"torch.float32", "torch.float64"}
    assert len(result["reports"]) == 28
    assert all(row["unavailable_rows"] == 0 for row in result["reports"])
    assert digest(Path(plan["checkpoint"])) == plan["checkpoint_sha256"]
    assert not list(case.glob("condition-*.json"))


def test_final_roster_retains_all_240_policies_and_twenty_original_seeds():
    config = json.loads((Path(__file__).resolve().parents[1] / "configs/study.json").read_text())
    cells = execution.final_cells(config)
    assert len(cells) == 240
    assert {r["seed"] for r in cells} == set(range(1000, 1020))
    assert len({(r["task"], r["architecture"], r["seed"]) for r in cells}) == 240
