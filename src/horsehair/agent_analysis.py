"""Seed-level H4 inference from complete, integrity-checked frozen experiments.

Episodes are nested measurements. No condition, strength or training seed is
selected after evaluation; every declared grid point remains in the family.
"""
from __future__ import annotations

import hashlib
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from . import agents
from .core import atomic_json, canonical_hash, read_json, source_identity
from .inference import holm

UNIT = "independent training seed with nested evaluation episodes"
KEYS = ["architecture", "task", "rule_type", "perturbation", "rank", "matching_arm", "strength"]
OUTCOMES = ["redirection_difference", "loss_delayed_cue", "loss_goal_detection", "loss_motor_execution"]
PRIMARY = agents.MECHANISMS[:4]


def _file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _settings(config):
    result = dict(config.get("agents", config))
    if "output_dir" in config and "output_dir" not in result:
        result["output_dir"] = config["output_dir"]
    return result


def _conditions(settings, protocol):
    rules = ["synthetic", "biological"] if protocol.get("biological_directions") else ["synthetic"]
    return [(rule, mechanism, int(rank), arm, float(strength)) for rule in rules for mechanism in agents.MECHANISMS
            for rank in settings.get("ranks", [1, 2, 4, 8]) if mechanism != "motor_gain" or rank == 1
            for arm in ("parameter_norm", "policy_kl") for strength in settings.get("strengths", [0, .05, .1, .2, .4, .8])]


def _assays():
    return [(probe, "none") for probe in ("main", *agents.PROBES)]+[("main", control) for control in ("exact", "random_subspace", "cue_removal", "lower_action_noise")]


def validate_trial_roster(frame, settings, protocol, *, architecture, task, seed):
    columns = KEYS+["training_seed", "probe", "restoration_control", "episode_seed", "success", "redirection", "motor_accuracy",
                    "baseline_success", "baseline_redirection", "baseline_motor_accuracy", "achieved_parameter_norm", "clean_policy_kl",
                    "target_policy_kl", "protocol_sha256", "heldout_condition", "effective_coefficients"]
    missing = set(columns)-set(frame)
    if missing:
        raise ValueError(f"Missing agent-trial columns: {sorted(missing)}")
    for column, expected in (("architecture", architecture), ("task", task), ("training_seed", seed), ("protocol_sha256", agents._digest(protocol))):
        if not frame[column].eq(expected).all():
            raise ValueError(f"Agent trial {column} disagrees with its frozen policy receipt")
    if not frame.heldout_condition.eq(True).all():
        raise ValueError("H4 requires designated held-out evaluation episodes")
    if not frame.effective_coefficients.eq(frame["rank"]).all():
        raise ValueError("Perturbation coefficient count differs from its declared matched rank")
    unique = KEYS+["probe", "restoration_control", "episode_seed"]
    if frame[unique].isna().any().any() or frame.duplicated(unique).any():
        raise ValueError("Duplicate or missing nested-episode keys")
    n = int(settings.get("evaluation_episodes", 64))
    conditions = _conditions(settings, protocol)
    group_columns = ["rule_type", "perturbation", "rank", "matching_arm", "strength", "probe", "restoration_control"]
    counts = frame.groupby(group_columns, dropna=False).size().to_dict()
    expected = {(*condition, probe, control): n for condition in conditions for probe, control in _assays()}
    if counts != expected:
        absent = set(expected)-set(counts)
        raise ValueError(f"Agent trial roster is incomplete or unexpected: {len(absent)} missing assay cells")
    seeds = set(range(40_000_000+seed*1000, 40_000_000+seed*1000+n))
    for _, group in frame.groupby(group_columns, dropna=False):
        if set(group.episode_seed.astype(int)) != seeds:
            raise ValueError("Held-out environment seed roster differs across conditions")
    for column in ("success", "redirection", "motor_accuracy", "achieved_parameter_norm", "clean_policy_kl", "target_policy_kl"):
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy()
        if not np.isfinite(values).all():
            raise ValueError(f"Nonfinite outcome/calibration metadata: {column}")
    if not frame.success.isin([0, 1]).all() or not frame.redirection.isin([0, 1]).all() or not frame.motor_accuracy.between(0, 1).all():
        raise ValueError("Invalid endpoint probability/accuracy")
    if (frame.achieved_parameter_norm<0).any() or (frame.clean_policy_kl<0).any():
        raise ValueError("Negative achieved intervention norm or divergence")
    if not np.allclose(frame.target_policy_kl,frame.strength.astype(float)**2/2,rtol=1e-10,atol=1e-12):
        raise ValueError("Functional matching target differs from the frozen strength rule")
    primary = frame[frame.restoration_control.eq("none")]
    for column in ("baseline_success", "baseline_redirection", "baseline_motor_accuracy"):
        if not np.isfinite(primary[column].to_numpy(dtype=float)).all():
            raise ValueError("Missing paired clean-policy baseline")
        if not primary[column].between(0,1).all():
            raise ValueError("Invalid clean-policy baseline probability/accuracy")
        if (primary.groupby(["probe", "episode_seed"])[column].nunique()>1).any():
            raise ValueError("The clean baseline changed across perturbation conditions")
    # Exact restoration must recover the clean policy's complete terminal outcome,
    # not only one isolated action-probability comparison.
    baseline = primary[primary.probe.eq("main")].groupby("episode_seed")[["baseline_success", "baseline_redirection"]].first()
    restored = frame[frame.restoration_control.eq("exact")]
    if not np.array_equal(restored.success.to_numpy(), baseline.loc[restored.episode_seed, "baseline_success"].to_numpy()):
        raise ValueError("Exact restoration did not recover clean episode competence")
    if not np.array_equal(restored.redirection.to_numpy(), baseline.loc[restored.episode_seed, "baseline_redirection"].to_numpy()):
        raise ValueError("Exact restoration did not recover clean episode redirection")


def summarize_trials(frame):
    """One estimate per independently trained policy and frozen condition."""
    data = frame[frame.restoration_control.eq("none")]
    records = []
    for key, group in data.groupby(["training_seed", *KEYS], sort=True):
        record = dict(zip(["training_seed", *KEYS], key))
        main = group[group.probe.eq("main")]
        record["redirection_difference"] = float((main.redirection-main.baseline_redirection).mean())
        record["main_accuracy_difference"] = float((main.success-main.baseline_success).mean())
        for probe in agents.PROBES:
            rows = group[group.probe.eq(probe)]
            if rows.empty:
                raise ValueError(f"Missing separate competence probe: {probe}")
            altered = rows.motor_accuracy if probe == "motor_execution" else rows.success
            clean = rows.baseline_motor_accuracy if probe == "motor_execution" else rows.baseline_success
            record[f"loss_{probe}"] = float((clean-altered).mean())
            record[f"baseline_{probe}"] = float(clean.mean())
        if main.empty:
            raise ValueError("Missing main redirection endpoint")
        record["nested_main_episodes"] = len(main)
        record["achieved_parameter_norm"] = float(main.achieved_parameter_norm.mean())
        record["clean_policy_kl"] = float(main.clean_policy_kl.mean())
        record["target_policy_kl"] = float(main.target_policy_kl.mean())
        # All episode rows in a cell must share the calibration result.
        for column in ("achieved_parameter_norm", "clean_policy_kl", "target_policy_kl"):
            if group[column].nunique() != 1:
                raise ValueError("Calibration metadata changes across the same intervention cell")
        target = record["strength"] if record["matching_arm"] == "parameter_norm" else record["target_policy_kl"]
        achieved = record["achieved_parameter_norm"] if record["matching_arm"] == "parameter_norm" else record["clean_policy_kl"]
        record["strength_matched"] = abs(achieved-target) <= max(1e-6, abs(target)*.1)
        records.append(record)
    return pd.DataFrame(records)


def one_sided(values, bound=0.0, alternative="greater"):
    values = np.asarray(values, dtype=float)
    if values.ndim != 1 or len(values)<2 or not np.isfinite(values).all():
        raise ValueError("Inference requires at least two finite independent-seed values")
    delta = values-float(bound)
    if alternative == "less":
        delta = -delta
    elif alternative != "greater":
        raise ValueError("Unknown one-sided alternative")
    if float(delta.std(ddof=1)) < 1e-14:
        return 0.0 if float(delta.mean())>0 else 1.0
    statistic = float(delta.mean()/(delta.std(ddof=1)/math.sqrt(len(delta))))
    return float(stats.t.sf(statistic, len(delta)-1))


def bootstrap_ci(values, *, repetitions=10000, seed=20261012):
    values = np.asarray(values, dtype=float)
    if values.ndim != 1 or len(values)<2 or not np.isfinite(values).all():
        raise ValueError("Seed-cluster bootstrap requires at least two finite seed estimates")
    rng = np.random.default_rng(seed)
    draws = values[rng.integers(len(values), size=(int(repetitions), len(values)))].mean(axis=1)
    return np.quantile(draws, [.025, .975]).tolist()


def _margin(group, redirection_threshold, loss_threshold):
    return np.minimum.reduce([group.redirection_difference.to_numpy()-redirection_threshold,
                              *[loss_threshold-group[f"loss_{probe}"].to_numpy() for probe in agents.PROBES]])


def analyze_seed_summaries(summaries, settings, inference=None):
    inference = inference or {}
    data = summaries.copy()
    redirection = float(inference.get("agent_redirection_difference", .2))
    loss = float(inference.get("agent_competence_loss_max", .05))
    repetitions = int(inference.get("bootstrap_replicates", 10000))
    alpha = float(inference.get("alpha", .05))
    expected_seeds = set(settings.get("final_seeds", range(1000, 1020)))
    if data.duplicated(["training_seed", *KEYS]).any():
        raise ValueError("A policy seed is duplicated within a frozen intervention condition")
    records = []
    for key, group in data.groupby(KEYS, sort=True):
        if set(group.training_seed) != expected_seeds:
            raise ValueError("Missing independently trained seed in an intervention condition")
        group = group.sort_values("training_seed")
        record = dict(zip(KEYS, key))
        record["independent_seeds"] = len(group)
        record["nested_episodes_per_seed"] = sorted(set(group.nested_main_episodes.astype(int)))
        record["baseline_qualified"] = bool(all(group[f"baseline_{p}"].ge(float(settings.get("baseline_probe_accuracy_min", .8))).all() for p in agents.PROBES))
        record["strength_matched"] = bool(group.strength_matched.all())
        record["outcomes"] = {column: {"estimate": float(group[column].mean()), "ci95": bootstrap_ci(group[column], repetitions=repetitions)} for column in OUTCOMES}
        for probe in agents.PROBES:
            record[f"baseline_{probe}"] = float(group[f"baseline_{probe}"].mean())
        record["achieved_parameter_norm"] = float(group.achieved_parameter_norm.mean())
        record["clean_policy_kl"] = float(group.clean_policy_kl.mean())
        probabilities = [one_sided(group.redirection_difference, redirection),
                         *[one_sided(group[f"loss_{p}"], loss, "less") for p in agents.PROBES]]
        record["component_pvalues"] = dict(zip(OUTCOMES, probabilities))
        record["raw_operating_region_pvalue"] = max(probabilities)
        record["comparators"] = {}
        unavailable = []
        for control in ("random_subspace", "output_gain"):
            selector = np.ones(len(data), dtype=bool)
            for column, value in record.items():
                if column in KEYS and column != "perturbation":
                    selector &= data[column].eq(value).to_numpy()
            comparator = data[selector & data.perturbation.eq(control)].sort_values("training_seed")
            if len(comparator) != len(group) or set(comparator.training_seed) != expected_seeds:
                raise ValueError("A declared matched comparator is missing independent seeds")
            difference = _margin(group, redirection, loss)-_margin(comparator, redirection, loss)
            matched = bool(group.strength_matched.all() and comparator.strength_matched.all())
            record["comparators"][control] = {"estimand": "paired seed difference in worst operating margin",
                "estimate": float(difference.mean()), "ci95": bootstrap_ci(difference, repetitions=repetitions),
                "pvalue": one_sided(difference), "strength_matched": matched, "independent_seed_pairs": len(difference)}
            probabilities.append(record["comparators"][control]["pvalue"])
            if not matched:
                unavailable.append(f"unmatched {control} calibration")
        if not record["baseline_qualified"]:
            unavailable.append("one or more baseline competence probes are unqualified")
        if not record["strength_matched"]:
            unavailable.append("intervention strength target was not achieved")
        record["raw_joint_pvalue"] = max(probabilities)
        record["available_for_H4"] = not unavailable
        record["unavailable_reasons"] = unavailable
        record["primary_grid_point"] = record["perturbation"] in PRIMARY
        records.append(record)
    primary = [r for r in records if r["primary_grid_point"]]
    # Unavailable conditions retain a conservative value of one in the predeclared
    # family; their raw outcomes/p values remain in the exported table.
    adjusted = holm([r["raw_joint_pvalue"] if r["available_for_H4"] else 1.0 for r in primary])
    for record, adjusted_p in zip(primary, adjusted):
        record["holm_pvalue"] = float(adjusted_p)
        record["operating_region_supported"] = bool(record["available_for_H4"] and adjusted_p<alpha)
    for record in records:
        if not record["primary_grid_point"]:
            record["holm_pvalue"] = None
            record["operating_region_supported"] = False
    return records


def _validate_cell(directory, settings, protocol, task, architecture, seed):
    summary = read_json(directory/"summary.json")
    expected = {"seed": seed, "task": task, "architecture": architecture, "config_sha256": agents._digest(settings),
                "implementation_sha256": agents._implementation_sha256(), "training_steps": int(settings.get("total_steps", 1000000)),
                "complete_training_budget": True, "learning_rate": float(protocol["learning_rate"])}
    if any(summary.get(k) != v for k, v in expected.items()):
        raise ValueError("Final training receipt is incomplete or has different source/configuration/cell")
    checkpoint_path = directory/"checkpoint.pt"
    if summary.get("checkpoint_sha256") != _file_hash(checkpoint_path):
        raise ValueError("Final checkpoint bytes differ from its summary")
    checkpoint = agents._torch().load(checkpoint_path, map_location="cpu", weights_only=False)
    agents._verify_checkpoint(checkpoint, settings, seed=seed, task=task, architecture=architecture,
                              learning_rate=float(protocol["learning_rate"]), complete=True)
    if checkpoint["weight_sha256"] != summary.get("weight_sha256"):
        raise ValueError("Final model hash differs between checkpoint and summary")
    receipt = read_json(directory/"evaluation_summary.json")
    expected = {"training_seed": seed, "architecture": architecture, "task": task, "base_weights_unchanged": True,
                "base_weight_sha256": checkpoint["weight_sha256"], "protocol_sha256": agents._digest(protocol)}
    if any(receipt.get(k) != v for k, v in expected.items()):
        raise ValueError("Evaluation receipt does not identify the frozen policy/protocol")
    trial_path = directory/"agent_trials.parquet"
    if receipt.get("trial_file_sha256") != _file_hash(trial_path):
        raise ValueError("Evaluation trial bytes differ from the receipt")
    frame = pd.read_parquet(trial_path)
    if receipt.get("trial_rows") != len(frame):
        raise ValueError("Trial row count differs from its receipt")
    validate_trial_roster(frame, settings, protocol, architecture=architecture, task=task, seed=seed)
    restoration = read_json(directory/"restoration_checks.json")
    conditions = _conditions(settings, protocol)
    expected_restoration = {}
    for _, mechanism, rank, arm, strength in conditions:
        key = (mechanism, rank, arm, strength)
        expected_restoration[key] = expected_restoration.get(key, 0)+1
    observed = {}
    for row in restoration:
        key = (row["mechanism"], row["rank"], row["matching_arm"], row["strength"])
        observed[key] = observed.get(key, 0)+1
        error = float(row["restoration_max_probability_error"])
        if not row.get("base_weights_unchanged") or not math.isfinite(error) or error>1e-7:
            raise ValueError("Numerical restoration failed for a frozen condition")
    if observed != expected_restoration:
        raise ValueError("Incomplete numerical restoration roster")
    summaries = summarize_trials(frame)
    baseline = {probe: float(summaries[f"baseline_{probe}"].iloc[0]) for probe in agents.PROBES}
    if any(abs(baseline[p]-float(receipt["baseline_competence"][p]))>1e-10 for p in agents.PROBES):
        raise ValueError("Baseline competence receipt disagrees with actual paired episodes")
    qualified = all(v>=float(settings.get("baseline_probe_accuracy_min", .8)) for v in baseline.values())
    if bool(receipt.get("baseline_competence_qualified")) != qualified:
        raise ValueError("Baseline qualification flag disagrees with actual probe scores")
    controls = frame[~frame.restoration_control.eq("none")].groupby([*KEYS, "restoration_control"]).agg(
        success=("success", "mean"), redirection=("redirection", "mean"), nested_episodes=("episode_seed", "size")).reset_index()
    controls["training_seed"] = seed
    paths = [directory/name for name in ("summary.json", "checkpoint.pt", "evaluation_summary.json", "agent_trials.parquet", "restoration_checks.json")]
    return summaries, controls, qualified, {str(p): _file_hash(p) for p in paths}


def _paired_architecture_contrasts(summaries, settings, inference):
    rows = []
    architectures = list(dict.fromkeys(cell[1] for cell in agents._cells(settings)))
    group_keys = [key for key in KEYS if key != "architecture"]
    for key, group in summaries.groupby(group_keys, sort=True):
        for index, first in enumerate(architectures):
            for second in architectures[index+1:]:
                a = group[group.architecture.eq(first)].set_index("training_seed")
                b = group[group.architecture.eq(second)].set_index("training_seed")
                if set(a.index) != set(b.index) or len(a)<2:
                    raise ValueError("Paired architecture contrast has incomplete seed blocks")
                b = b.loc[a.index]
                for outcome in OUTCOMES:
                    difference = a[outcome]-b[outcome]
                    rows.append({**dict(zip(group_keys, key)), "first_architecture": first, "second_architecture": second,
                                 "outcome": outcome, "estimate": float(difference.mean()), "ci95": bootstrap_ci(difference,
                                 repetitions=int(inference.get("bootstrap_replicates", 10000))), "seed_pairs": len(difference),
                                 "status": "descriptive paired contrast; no additional primary test"})
    return rows


def _figure_inputs(curves, source_paths, output, restoration=None, repetitions=10000, config=None):
    panels = []
    # Main figure: prespecified rank-one norm arm, all architectures/tasks,
    # mechanisms and strengths. All remaining ranks/arms are complete in the
    # linked machine-readable response curves, rather than a thousands-wide plot.
    rule = "biological" if any(r["rule_type"] == "biological" for r in curves) else "synthetic"
    frame = pd.DataFrame([{**{k:r[k] for k in KEYS}, "record":r} for r in curves
                          if r["rank"] == 1 and r["matching_arm"] == "parameter_norm" and r["rule_type"] == rule])
    for key, group in frame.groupby(["architecture", "task"], sort=True):
        title = "/".join(str(v) for v in key)
        for outcome in OUTCOMES:
            series = []
            for mechanism, subset in group.groupby("perturbation", sort=True):
                ordered = subset.sort_values("strength")
                values = [r["outcomes"][outcome] for r in ordered.record]
                series.append({"label":mechanism, "x":ordered.strength.tolist(), "y":[v["estimate"] for v in values],
                               "lower":[v["ci95"][0] for v in values], "upper":[v["ci95"][1] for v in values],
                               "baseline_qualified":[r["baseline_qualified"] for r in ordered.record],
                               "strength_matched":[r["strength_matched"] for r in ordered.record]})
            fallback = next(s for s in series if s["label"] == "recurrent_gate")
            panels.append({"kind":"interval", "title":title+" "+outcome, **{k:fallback[k] for k in ("x","y","lower","upper")},
                "series":series, "xlabel":"Frozen perturbation strength", "ylabel":"Probability difference"})
    if restoration is not None and set([*KEYS,"restoration_control","training_seed","redirection"]) <= set(restoration):
        selected = restoration[restoration["rank"].eq(1) & restoration.matching_arm.eq("parameter_norm") &
                               restoration.rule_type.eq(rule) & restoration.perturbation.eq("recurrent_gate")]
        for key, group in selected.groupby(["architecture","task"],sort=True):
            series = []
            for control, rows in group.groupby("restoration_control",sort=True):
                points = []
                for strength, point in rows.groupby("strength",sort=True):
                    if point.training_seed.duplicated().any():
                        raise ValueError("Restoration plot duplicates an independent seed")
                    ci = bootstrap_ci(point.redirection,repetitions=repetitions)
                    points.append((float(strength),float(point.redirection.mean()),*ci))
                series.append({"label":control,"x":[p[0] for p in points],"y":[p[1] for p in points],
                               "lower":[p[2] for p in points],"upper":[p[3] for p in points]})
            fallback = next(s for s in series if s["label"] == "exact")
            panels.append({"kind":"interval","title":"/".join(key)+" restoration",**{k:fallback[k] for k in ("x","y","lower","upper")},
                           "series":series,"xlabel":"Frozen perturbation strength","ylabel":"Redirection probability"})
    specification = {"evidence_class":"computational_experiment", "source_result_paths":[str(p) for p in source_paths],
        "source_result_hashes":{str(p):_file_hash(p) for p in source_paths},
        "config_sha256":canonical_hash(config or {}), "source_sha256":source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "layout":{"columns":3,"max_panels_per_page":12}, "main_figure_rule":rule, "main_figure_rank":1,
        "main_figure_matching_arm":"parameter_norm", "full_response_curves_path":str(source_paths[-1]),
        "panels":panels, "caption":"Prespecified rank-one parameter-norm curves for every architecture/task and mechanism; all strengths are shown. Complete other ranks, matching arms and rule types are provided in response_curves.json. Each point averages paired episode differences within each independently trained seed and then equally across seeds. Intervals resample seed clusters. Redirection and the three competence losses are separate. Restoration panels show absolute endpoint probabilities for exact intervention removal, equal-norm random correction, cue removal and lower action noise. Unqualified baselines and unmatched calibration cells remain visible. These are computational experiments, not biological observations; neither a positive point nor restoration establishes an insect mechanism."}
    atomic_json(output, specification)


def run(config, root):
    root = Path(root)
    settings = _settings(config)
    directory = agents._directory(root, settings)
    protocol_path = directory/"frozen_protocol.json"
    protocol = read_json(protocol_path)
    if protocol.get("config_sha256") != agents._digest(settings) or protocol.get("implementation_sha256") != agents._implementation_sha256():
        raise ValueError("H4 protocol has different frozen source/configuration")
    if protocol.get("full_config_sha256") != canonical_hash(config) or protocol.get("H4_inference_specification") != agents.h4_specification(config):
        raise ValueError("Full study configuration or H4 inference specification changed after protocol freeze")
    biological = protocol.get("biological_directions")
    if biological:
        raise ValueError("The synthetic H4 primary cannot activate biological directions; a separately frozen bridge protocol is required")
    cells = agents._cells(settings)
    summaries, restoration, failures, provenance, qualifications = [], [], [], {}, []
    for task, architecture, seed, _ in cells:
        path = directory/"final"/agents._cell_name(task, architecture, seed)
        try:
            summary, controls, qualified, source_hashes = _validate_cell(path, settings, protocol, task, architecture, seed)
        except (FileNotFoundError, ValueError, RuntimeError, KeyError) as exc:
            failures.append({"task":task, "architecture":architecture, "training_seed":seed, "error_type":type(exc).__name__, "error":str(exc)})
            continue
        summaries.append(summary)
        restoration.append(controls)
        qualifications.append(qualified)
        provenance.update(source_hashes)
    audit = {"status":"incomplete" if failures else "verified_complete_grid", "planned_policy_cells":len(cells),
             "verified_policy_cells":len(summaries), "failed_or_missing_cells":failures, "config_sha256":canonical_hash(config),
             "frozen_protocol_sha256":agents._digest(protocol), "all_baseline_competence_qualified":all(qualifications) and not failures}
    atomic_json(directory/"analysis_audit.json", audit)
    if failures:
        raise ValueError(f"H4 inference is incomplete: {len(failures)} missing, failed or integrity-mismatched policy cells; inspect analysis_audit.json")
    seeds = pd.concat(summaries, ignore_index=True)
    controls = pd.concat(restoration, ignore_index=True)
    inference = protocol["H4_inference_specification"]
    curves = analyze_seed_summaries(seeds, settings, inference)
    primary = [r for r in curves if r["primary_grid_point"]]
    paired = _paired_architecture_contrasts(seeds, settings, inference)
    output = directory/"analysis"
    output.mkdir(parents=True, exist_ok=True)
    seed_path, control_path, curve_path = output/"seed_summaries.parquet", output/"restoration_controls.parquet", output/"response_curves.json"
    seeds.to_parquet(seed_path, index=False)
    controls.to_parquet(control_path, index=False)
    atomic_json(curve_path, curves)
    atomic_json(output/"paired_architecture_contrasts.json", paired)
    provenance.update({str(p):_file_hash(p) for p in [protocol_path,seed_path,control_path,curve_path,
                                                    output/"paired_architecture_contrasts.json",directory/"analysis_audit.json"]})
    # Prespecified reference point summarizes an effect without selecting the
    # best-looking evaluated strength. Omnibus evidence concerns the entire grid.
    reference = inference.get("agent_reference_point", {"rule_type":"synthetic", "perturbation":"recurrent_gate", "rank":1,
                                                       "matching_arm":"parameter_norm", "strength":.2})
    selector = np.ones(len(seeds), dtype=bool)
    for key, value in reference.items():
        selector &= seeds[key].eq(value).to_numpy()
    recurrent = seeds[selector & seeds.architecture.isin(settings.get("architectures", agents.ARCHITECTURES))]
    if recurrent.empty:
        raise ValueError("Declared H4 reference point is absent from the frozen grid")
    blocked = recurrent.groupby("training_seed")[OUTCOMES].mean()
    biological_ready = False
    all_qualified = all(qualifications)
    supported = [r for r in primary if r["operating_region_supported"]]
    synthetic_supported = [r for r in supported if r["rule_type"] == "synthetic"]
    heldout_supported = [r for r in synthetic_supported if r["architecture"] == protocol.get("heldout_architecture") and r["task"] == protocol.get("heldout_task")]
    claim_status = "supported" if all_qualified and synthetic_supported else "not_supported" if all_qualified else "pending"
    result = {"claim_id":"H4", "independent_unit":UNIT, "evidence_class":"computational_experiment",
        "config_sha256":canonical_hash(config), "source_sha256":source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "frozen_protocol_sha256":agents._digest(protocol), "status":"complete", "claim_status":claim_status,
        "estimate":float(blocked.redirection_difference.mean()), "ci95":bootstrap_ci(blocked.redirection_difference,
            repetitions=int(inference.get("bootstrap_replicates",10000))),
        "pvalue":min(r["holm_pvalue"] for r in primary if r["rule_type"] == ("biological" if biological_ready else "synthetic")),
        "pvalue_estimand":"omnibus existence of a multiplicity-adjusted joint operating point, including paired comparator advantages",
        "computational_analysis_complete":True, "headline_claim_accepted":claim_status == "supported",
        "primary_evidence_class":"computational_experiment","computational_claim_accepted":claim_status == "supported",
        "biological_transfer_status":"pending",
        "reference_point":reference, "reference_estimand":"equal-task/architecture mean redirection change, blocked by training seed; not selected on evaluation",
        "reference_competence_losses":{p:float(blocked[f"loss_{p}"].mean()) for p in agents.PROBES},
        "correction":{"method":"Holm", "family":"H4 prespecified computational grid", "tests":len(primary),
            "alpha":float(inference.get("alpha",.05)), "point_test":"intersection-union maximum of redirection, three probe losses and two paired comparator advantages",
            "unavailable_points_retained_as_pvalue_one":sum(not r["available_for_H4"] for r in primary)},
        "seed_inference":"one-sided Student-t mean contrasts across independent seeds; pointwise percentile bootstrap intervals resample seed clusters",
        "planned_policy_cells":len(cells), "independent_seed_count_per_cell":len(settings.get("final_seeds",range(1000,1020))),
        "supported_grid_points":len(supported), "all_baseline_competence_qualified":all_qualified,
        "biologically_constrained_supported_grid_points":0,
        "computational_dual_heldout_supported_grid_points":len(heldout_supported),
        "computational_dual_heldout_transfer_status":"supported" if heldout_supported else "not_supported" if all_qualified else "pending",
        "biological_direction_status":protocol.get("biological_direction_status"), "biological_constraints_available":biological_ready,
        "restoration":"original weights and exact terminal outcomes verified; every numerical action-probability error <=1e-7",
        "limitations":["Computational causality does not validate an insect mechanism.",
            *([] if biological_ready else ["Biologically constrained transfer remains pending; computed curves are explicitly synthetic."]),
            *([] if all_qualified else ["At least one baseline policy lacks qualified competence; no headline H4 claim is accepted."])],
        "source_result_hashes":provenance, "response_curves":str(curve_path)}
    atomic_json(root/"results/inference/H4.json", result)
    _figure_inputs(curves, [seed_path,control_path,curve_path], root/"results/figure_inputs/05.json",controls,
                   repetitions=int(inference.get("bootstrap_replicates",10000)),config=config)
    return result
