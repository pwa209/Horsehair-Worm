"""Independent intervention calibration and shared-control inference.

Intervention amplitudes are estimated from calibration activity only. Evaluation
bout durations, occupancy and conditional speed do not enter amplitude fitting.
"""
from __future__ import annotations

from pathlib import Path
import copy
import hashlib
import json
from typing import Any

import numpy as np
import pandas as pd
from scipy import optimize

from .behaviour import bin_trajectories, read_table, to_sequences


FAMILIES = {"neuronal_silencing", "neuronal_activation", "endocrine_manipulation", "physiological_transfer"}


def calibration_half(animal_id: str, salt: str = "horsehair-intervention-v1") -> int:
    """An identical animal, including a reused control, always gets one half."""
    digest = hashlib.sha256(f"{salt}\0{animal_id}".encode()).digest()
    return digest[0] % 2


def restricted_mean_duration(durations: list[float], censored: list[bool], horizon: float) -> float:
    if not durations:
        return float("nan")
    times = np.asarray(durations, float)
    events = ~np.asarray(censored, bool)
    survival, integral, previous = 1., 0., 0.
    for time in np.unique(times[times <= horizon]):
        integral += survival * (time - previous)
        at_risk = int((times >= time).sum())
        deaths = int(((times == time) & events).sum())
        survival *= 1 - deaths / at_risk
        previous = float(time)
    integral += survival * max(horizon - previous, 0)
    return float(integral)


def summarize(sequence: Any, movement_threshold: float, duration_horizon: float = 600) -> dict[str, float]:
    valid = sequence.observed & np.isfinite(sequence.observations).all(1)
    speed = np.linalg.norm(np.nan_to_num(sequence.observations), axis=1) / sequence.bin_seconds
    moving = (speed > movement_threshold) & valid
    eligible_entries = valid[1:] & valid[:-1] & ~moving[:-1]
    entries = eligible_entries & moving[1:]
    durations, censored = [], []
    start = None
    left_censored = False
    for t in range(len(valid)):
        if moving[t] and start is None:
            start = t
            left_censored = t == 0 or not valid[t - 1]
        if start is not None and (not moving[t] or t == len(valid) - 1):
            end = t + 1 if moving[t] else t
            if not left_censored:
                durations.append((end - start) * sequence.bin_seconds)
                censored.append(not valid[t] or t == len(valid) - 1)
            start = None
    return {"integrated_activity": float(np.sum(speed[valid]) * sequence.bin_seconds),
            "movement_entry": float(entries.sum() / eligible_entries.sum()) if eligible_entries.any() else float("nan"),
            "occupancy": float(moving[valid].mean()) if valid.any() else float("nan"),
            "conditional_speed": float(speed[moving].mean()) if moving.any() else float("nan"),
            "bout_rmst": restricted_mean_duration(durations, censored, duration_horizon),
            "observed_seconds": float(valid.sum() * sequence.bin_seconds)}


def shared_control_effects(animals: pd.DataFrame, contrasts: pd.DataFrame,
                           metric: str, repetitions: int = 10000, seed: int = 0
                           ) -> tuple[pd.DataFrame, np.ndarray]:
    """Resample each biological animal once per bootstrap, including reused controls.

    Required animal table: animal_id, experiment_id, condition_id, metric.
    Required contrast map: contrast_id, treatment_condition, control_condition.
    A condition ID must identify a declared, compatible source experimental group.
    """
    needed = {"animal_id", "experiment_id", "condition_id", metric}
    if needed - set(animals):
        raise ValueError(f"Animal-level screen input needs {sorted(needed - set(animals))}")
    if animals.animal_id.duplicated().any():
        raise ValueError("Each shared control animal must appear only once in the animal table")
    if not np.isfinite(animals[metric]).all():
        raise ValueError("Nonfinite screen metric")
    columns = {"contrast_id", "treatment_condition", "control_condition"}
    if columns - set(contrasts) or contrasts.contrast_id.duplicated().any():
        raise ValueError("Screen contrasts require unique identities and designated controls")
    groups = {name: group for name, group in animals.groupby("condition_id", sort=False)}
    for row in contrasts.itertuples():
        if row.treatment_condition == row.control_condition:
            raise ValueError("A treatment condition cannot be its own designated control")
        if row.treatment_condition not in groups or row.control_condition not in groups:
            raise ValueError(f"Unmatched designated control for {row.contrast_id}")
        if len(groups[row.treatment_condition]) < 2 or len(groups[row.control_condition]) < 2:
            raise ValueError("An uncertainty estimate needs at least two animals per group")
    def effects(sampled: dict[str, pd.DataFrame]) -> np.ndarray:
        values = []
        for row in contrasts.itertuples():
            t = sampled[row.treatment_condition][metric].to_numpy(float)
            c = sampled[row.control_condition][metric].to_numpy(float)
            pooled = np.sqrt(((len(t) - 1) * np.var(t, ddof=1) + (len(c) - 1) * np.var(c, ddof=1))
                             / (len(t) + len(c) - 2))
            if pooled < 1e-12:
                values.append(np.nan)
                continue
            correction = 1 - 3 / (4 * (len(t) + len(c) - 2) - 1)
            values.append(correction * (np.mean(t) - np.mean(c)) / pooled)
        return np.asarray(values)
    rng = np.random.default_rng(seed)
    draws = []
    for _ in range(repetitions):
        sampled = {}
        # Resample experiments first when a condition spans multiple independent
        # experiments, then animals inside them. Shared conditions reuse this draw.
        for name, group in groups.items():
            experiments = list(group.experiment_id.unique())
            parts = []
            for experiment in rng.choice(experiments, len(experiments), replace=True):
                units = group[group.experiment_id == experiment]
                parts.append(units.iloc[rng.integers(len(units), size=len(units))])
            sampled[name] = pd.concat(parts, ignore_index=True)
        draw = effects(sampled)
        if np.isfinite(draw).all():
            draws.append(draw)
    if len(draws) < max(10, int(.8 * repetitions)):
        raise ValueError("Too many degenerate shared-control bootstrap samples")
    array = np.asarray(draws)
    covariance = np.atleast_2d(np.cov(array, rowvar=False, ddof=1))
    result = contrasts.copy()
    result["effect"] = effects(groups)
    result["effect_se"] = np.sqrt(np.diag(covariance))
    result["ci_low"], result["ci_high"] = np.quantile(array, [.025, .975], axis=0)
    result["n_treatment"] = [len(groups[c]) for c in contrasts.treatment_condition]
    result["n_control"] = [len(groups[c]) for c in contrasts.control_condition]
    result["conditional_on_source_inclusion"] = contrasts.get("conditional_on_source_inclusion", True)
    return result, covariance


def aggregate_gene_effects(effects: pd.DataFrame, covariance: np.ndarray) -> pd.DataFrame:
    """GLS gene estimates retain reagent heterogeneity and shared-control covariance."""
    if "gene_id" not in effects or effects.gene_id.isna().any():
        raise ValueError("A curated, unique source gene identifier is required; line names are not gene IDs")
    if covariance.shape != (len(effects), len(effects)) or not np.isfinite(covariance).all():
        raise ValueError("Shared-control effect covariance has the wrong shape")
    rows, aggregation_weights = [], []
    for gene, group in effects.groupby("gene_id", sort=False):
        indices = effects.index.get_indexer(group.index)
        values = group.effect.to_numpy(float)
        matrix = covariance[np.ix_(indices, indices)]
        diagonal = np.maximum(np.diag(matrix), 1e-12)
        inverse = np.linalg.pinv(matrix + np.eye(len(matrix)) * 1e-12)
        one = np.ones(len(values))
        fixed = (one @ inverse @ values) / (one @ inverse @ one)
        # Paule-Mandel-like method-of-moments excess reagent variance; the final
        # GLS fit keeps the off-diagonal covariance from shared controls.
        heterogeneity = max(float(np.mean((values - fixed) ** 2 - diagonal)), 0.)
        inverse = np.linalg.pinv(matrix + np.eye(len(matrix)) * (heterogeneity + 1e-12))
        precision = float(one @ inverse @ one)
        weight = np.zeros(len(effects))
        weight[indices] = one @ inverse / precision
        aggregation_weights.append(weight)
        rows.append({"gene_id": gene, "effect": float(one @ inverse @ values / precision),
                     "effect_se": float(np.sqrt(1 / precision)), "n_reagents": len(values),
                     "reagent_variance": heterogeneity,
                     "reagents": json.dumps(group.contrast_id.tolist()),
                     "conditional_on_source_inclusion": bool(group.conditional_on_source_inclusion.any())})
    result = pd.DataFrame(rows)
    weights = np.vstack(aggregation_weights)
    gene_covariance = weights @ covariance @ weights.T
    # Heterogeneity augments within-gene uncertainty, while every off-diagonal
    # contribution from a shared control survives the gene-level linear map.
    np.fill_diagonal(gene_covariance, result.effect_se.to_numpy() ** 2)
    result.attrs["shared_control_covariance"] = gene_covariance
    result.attrs["reagent_aggregation_weights"] = weights
    return result


def frozen_direction(fit: Any, modulation_column: int = 0) -> dict[str, np.ndarray]:
    if hasattr(fit, "constructor"):
        return {"modulation_shift": np.ones(1), "modulation_column": np.array([modulation_column])}
    model = fit.model
    parts = {"transition_shift": model.transition_mod[:, modulation_column].detach().cpu().numpy(),
             "gain_shift": model.gain_mod[:, modulation_column].detach().cpu().numpy(),
             "direction_shift": model.direction_mod[:, modulation_column].detach().cpu().numpy()}
    norm = np.sqrt(sum(float(np.sum(value ** 2)) for value in parts.values()))
    if norm < 1e-8:
        raise ValueError("WP1 did not identify a perturbation direction for this model")
    return {key: value / norm for key, value in parts.items()}


def predicted_summary(fit: Any, sequences: list[Any], amplitude: float,
                      direction: dict[str, np.ndarray], threshold: float,
                      repetitions: int, seed: int, horizon: float = 600) -> dict[str, float]:
    from .semimarkov import simulate
    rows = []
    for sequence in sequences:
        transformed = fit.scaler.transform(sequence)
        identity_seed = int.from_bytes(hashlib.sha256(sequence.animal_id.encode()).digest()[:4], "little")
        for replicate in range(repetitions):
            random_seed = (seed + identity_seed + replicate * 104729) % 2**32
            if hasattr(fit, "constructor"):
                from .behaviour import simulate_comparator
                generated = simulate_comparator(fit, sequence, random_seed,
                    modulation_shift=amplitude * float(direction["modulation_shift"][0]),
                    modulation_column=int(direction["modulation_column"][0]))
                rows.append(summarize(generated, threshold, horizon))
                continue
            overrides = {key: fit.model._tensor(value * amplitude) for key, value in direction.items()}
            # New animals/experiments are generated from their group-effect prior.
            rng = np.random.default_rng(random_seed + 1)
            variance = np.zeros(fit.model.states)
            if sequence.animal_id not in fit.model.animal_ids:
                variance += fit.model.log_animal_sd.detach().cpu().exp().numpy() ** 2
            if sequence.experiment_id not in fit.model.experiment_ids:
                variance += fit.model.log_experiment_sd.detach().cpu().exp().numpy() ** 2
            overrides["transition_shift"] = overrides["transition_shift"] + fit.model._tensor(rng.normal(0, np.sqrt(variance)))
            generated, _ = simulate(fit.model, transformed, random_seed, overrides)
            rows.append(summarize(generated, threshold, horizon))
    return {column: float(np.nanmean([row[column] for row in rows])) for column in rows[0]}


def calibrate_amplitude(fit: Any, treatment: list[Any], control: list[Any],
                        direction: dict[str, np.ndarray], threshold: float,
                        repetitions: int = 32, seed: int = 0,
                        bounds: tuple[float, float] = (-4., 4.), horizon: float = 600
                        ) -> tuple[float, dict[str, Any]]:
    if {s.animal_id for s in treatment} & {s.animal_id for s in control}:
        raise ValueError("The same animal cannot be its own unrelated treatment/control")
    if not treatment or not control:
        raise ValueError("Calibration halves need both treatment and designated control animals")
    observed = (np.mean([summarize(s, threshold, horizon)["integrated_activity"] for s in treatment])
                - np.mean([summarize(s, threshold, horizon)["integrated_activity"] for s in control]))
    control_prediction = predicted_summary(fit, control, 0., direction, threshold, repetitions, seed, horizon)
    def objective(amplitude: float) -> float:
        target = predicted_summary(fit, treatment, amplitude, direction, threshold, repetitions, seed, horizon)
        prediction = target["integrated_activity"] - control_prediction["integrated_activity"]
        return float((prediction - observed) ** 2)
    result = optimize.minimize_scalar(objective, bounds=bounds, method="bounded",
                                      options={"xatol": 1e-3, "maxiter": 100})
    if not result.success or not np.isfinite(result.fun):
        raise RuntimeError("Calibration amplitude optimizer failed")
    return float(result.x), {"integrated_activity_difference": float(observed),
                            "calibration_squared_error": float(result.fun),
                            "n_calibration_treatment": len(treatment), "n_calibration_control": len(control),
                            "amplitude_at_bound": bool(min(result.x - bounds[0], bounds[1] - result.x) < .01)}


def challenge(contrasts: pd.DataFrame, sequences: list[Any], animal_metadata: pd.DataFrame,
              fits: dict[str, Any], config: dict[str, Any], output: Path) -> pd.DataFrame:
    output.mkdir(parents=True, exist_ok=True)
    threshold = config.get("movement_threshold")
    if threshold is None:
        raise ValueError("Freeze movement_threshold on WP1 development data before opening challenge outcomes")
    required = {"contrast_id", "family", "treatment_condition", "control_condition"}
    if required - set(contrasts):
        raise ValueError(f"Intervention map needs {sorted(required - set(contrasts))}")
    if not set(contrasts.family).issubset(FAMILIES):
        raise ValueError("Unrecognized preassigned intervention family")
    if animal_metadata.animal_id.duplicated().any() or "condition_id" not in animal_metadata:
        raise ValueError("Animal-to-condition crosswalk must be unique")
    sequence_by_id = {s.animal_id: s for s in sequences}
    if len(sequence_by_id) != len(sequences):
        raise ValueError("Repeated representations of challenge animals")
    condition = {name: [sequence_by_id[a] for a in group.animal_id if a in sequence_by_id]
                 for name, group in animal_metadata.groupby("condition_id")}
    eligible, exclusions = [], []
    salt = config.get("split_salt", "horsehair-intervention-v1")
    for row in contrasts.to_dict("records"):
        treatment, control = condition.get(row["treatment_condition"], []), condition.get(row["control_condition"], [])
        if min(len(treatment), len(control)) < 4:
            exclusions.append(dict(row, reason="Fewer than four identified animals in treatment/control"))
            continue
        if any(sum(calibration_half(s.animal_id, salt) == half for s in units) < 2
               for half in (0, 1) for units in (treatment, control)):
            exclusions.append(dict(row, reason="Frozen hash split has fewer than two independent animals in a half"))
            continue
        eligible.append(row)
    pd.DataFrame(exclusions).to_csv(output / "excluded_interventions.tsv", sep="\t", index=False)
    minimum = int(config.get("minimum_conditions", 8))
    families = len({r["family"] for r in eligible})
    if len(eligible) < minimum or families < int(config.get("minimum_families", 2)):
        raise ValueError(f"G3 failed: {len(eligible)} usable intervention contrasts across {families} families")
    animal_components = []
    for name, units in condition.items():
        for s in units:
            animal_components.append({"animal_id": s.animal_id, "experiment_id": s.experiment_id,
                "condition_id": name, "half": calibration_half(s.animal_id, salt),
                **summarize(s, threshold, float(config.get("duration_horizon", 600)))})
    component_table = pd.DataFrame(animal_components)
    if component_table.animal_id.duplicated().any():
        raise ValueError("Shared controls must use one condition identity in the crosswalk")
    component_table.to_csv(output / "animal_components.tsv", sep="\t", index=False)
    records = []
    components = config.get("components", ["movement_entry", "bout_rmst", "occupancy", "conditional_speed"])
    horizon = float(config.get("duration_horizon", 600))
    for row in eligible:
        treatment, control = condition[row["treatment_condition"]], condition[row["control_condition"]]
        all_ids = {s.animal_id for s in treatment + control}
        for name, fit in fits.items():
            if fit.diagnostics.get("benchmark_only"):
                raise ValueError("A resource benchmark is not a WP1-frozen scientific fit")
            if all_ids & set(fit.scaler.training_animals):
                raise ValueError("Targeted intervention animals entered WP1 training")
            direction = frozen_direction(fit, int(config.get("modulation_column", 0)))
            for half in (0, 1):
                calibration_t = [s for s in treatment if calibration_half(s.animal_id, salt) == half]
                calibration_c = [s for s in control if calibration_half(s.animal_id, salt) == half]
                evaluation_t = [s for s in treatment if calibration_half(s.animal_id, salt) != half]
                evaluation_c = [s for s in control if calibration_half(s.animal_id, salt) != half]
                amplitude, diagnostics = calibrate_amplitude(fit, calibration_t, calibration_c, direction,
                    threshold, int(config.get("calibration_repetitions", 32)), int(config.get("seed", 0)),
                    tuple(config.get("amplitude_bounds", [-4, 4])), horizon)
                predicted_t = predicted_summary(fit, evaluation_t, amplitude, direction, threshold,
                    int(config.get("evaluation_repetitions", 128)), int(config.get("evaluation_seed", 9000)), horizon)
                predicted_c = predicted_summary(fit, evaluation_c, 0., direction, threshold,
                    int(config.get("evaluation_repetitions", 128)), int(config.get("evaluation_seed", 9000)), horizon)
                observed_t = [summarize(s, threshold, horizon) for s in evaluation_t]
                observed_c = [summarize(s, threshold, horizon) for s in evaluation_c]
                epsilon = float(config.get("calibration_derivative_step", .1))
                if epsilon <= 0:
                    raise ValueError("A calibration derivative step must be positive")
                derivatives = {}
                for sign in (-1, 1):
                    cal = predicted_summary(fit, calibration_t, amplitude + sign * epsilon, direction, threshold,
                        int(config.get("calibration_repetitions", 32)), int(config.get("seed", 0)), horizon)
                    ev = predicted_summary(fit, evaluation_t, amplitude + sign * epsilon, direction, threshold,
                        int(config.get("evaluation_repetitions", 128)), int(config.get("evaluation_seed", 9000)), horizon)
                    derivatives[sign] = {"calibration": cal, "evaluation": ev}
                fine_derivatives = {}
                for sign in (-1, 1):
                    cal = predicted_summary(fit, calibration_t, amplitude + sign * epsilon / 2, direction, threshold,
                        int(config.get("calibration_repetitions", 32)), int(config.get("seed", 0)), horizon)
                    ev = predicted_summary(fit, evaluation_t, amplitude + sign * epsilon / 2, direction, threshold,
                        int(config.get("evaluation_repetitions", 128)), int(config.get("evaluation_seed", 9000)), horizon)
                    fine_derivatives[sign] = {"calibration": cal, "evaluation": ev}
                calibration_derivative = (derivatives[1]["calibration"]["integrated_activity"]
                    - derivatives[-1]["calibration"]["integrated_activity"]) / (2 * epsilon)
                fine_calibration_derivative = (fine_derivatives[1]["calibration"]["integrated_activity"]
                    - fine_derivatives[-1]["calibration"]["integrated_activity"]) / epsilon
                for component in components:
                    observed = float(np.nanmean([r[component] for r in observed_t])
                                     - np.nanmean([r[component] for r in observed_c]))
                    predicted = predicted_t[component] - predicted_c[component]
                    if not np.isfinite(observed) or not np.isfinite(predicted):
                        raise ValueError(f"Nonidentifiable withheld component {component} for {row['contrast_id']}")
                    derivative = (derivatives[1]["evaluation"][component] - derivatives[-1]["evaluation"][component]) / (2 * epsilon)
                    fine_derivative = (fine_derivatives[1]["evaluation"][component] - fine_derivatives[-1]["evaluation"][component]) / epsilon
                    stability = max(abs(calibration_derivative - fine_calibration_derivative) /
                        max(abs(fine_calibration_derivative), 1e-8),
                        abs(derivative - fine_derivative) / max(abs(fine_derivative), 1e-8))
                    records.append(dict(row, model=name, calibration_half=half, component=component,
                        amplitude=amplitude, observed=observed, predicted=predicted,
                        evaluation_treatment=json.dumps([s.animal_id for s in evaluation_t]),
                        evaluation_controls=json.dumps([s.animal_id for s in evaluation_c]),
                        calibration_treatment=json.dumps([s.animal_id for s in calibration_t]),
                        calibration_controls=json.dumps([s.animal_id for s in calibration_c]),
                        calibration_activity_derivative=calibration_derivative,
                        prediction_component_derivative=derivative,
                        derivative_relative_stability_error=stability,
                        derivative_step=epsilon,
                        **diagnostics))
    result = pd.DataFrame(records)
    result.to_csv(output / "intervention_predictions.tsv", sep="\t", index=False)
    return result


def _condition_draw(animals: pd.DataFrame, rng: Any) -> pd.DataFrame:
    # Each condition/experiment/hash-half is sampled once. Reused controls and
    # calibration/evaluation reversal therefore share exactly the same draw.
    return pd.concat([group.iloc[rng.integers(len(group), size=len(group))]
        for _, group in animals.groupby(["condition_id", "experiment_id", "half"], sort=False)], ignore_index=True)


def h2_shared_control_bootstrap(predictions: pd.DataFrame, animals: pd.DataFrame,
                                candidate: str, reference: str, scales: dict[str, float],
                                repetitions: int = 10000, seed: int = 0) -> dict[str, Any]:
    required = {"contrast_id", "model", "component", "calibration_half", "observed", "predicted",
        "treatment_condition", "control_condition", "calibration_activity_derivative",
        "prediction_component_derivative", "integrated_activity_difference", "family", "amplitude_at_bound"}
    if required - set(predictions):
        raise ValueError("H2 uncertainty requires saved calibration-response derivatives and control identities")
    if animals.animal_id.duplicated().any():
        raise ValueError("A shared control appears more than once in the H2 animal table")
    table = predictions[predictions.model.isin([candidate, reference])].copy()
    if set(table.model) != {candidate, reference}:
        raise ValueError("Missing complexity-matched H2 candidate/reference predictions")
    keys = ["contrast_id", "component", "calibration_half"]
    if table.duplicated(keys + ["model"]).any():
        raise ValueError("Repeated H2 evaluation row")
    groups = table.groupby(keys).model.agg(set)
    if any(models != {candidate, reference} for models in groups):
        raise ValueError("H2 reference must predict precisely the same withheld components/animals")
    for column in ("treatment_condition", "control_condition", "family", "calibration_treatment",
                   "calibration_controls", "evaluation_treatment", "evaluation_controls"):
        if column in table and (table.groupby(keys)[column].nunique(dropna=False) != 1).any():
            raise ValueError("H2 candidate/reference used different biological contrasts or withheld animals")
    for component in table.component.unique():
        if component not in scales or not np.isfinite(scales[component]) or scales[component] <= 0:
            raise ValueError("H2 component scales must be frozen from WP1 development data")
    if not np.isfinite(table[["observed", "predicted", "calibration_activity_derivative",
                              "prediction_component_derivative"]]).all().all():
        raise ValueError("A calibrated H2 response derivative is nonfinite")
    if (table.calibration_activity_derivative.abs() < 1e-8).any() or table.amplitude_at_bound.any():
        raise ValueError("Calibration is locally nonidentifiable or at an amplitude bound; no H2 confidence interval")
    identity_columns = ("calibration_treatment", "calibration_controls", "evaluation_treatment", "evaluation_controls")
    if any(column not in table for column in identity_columns):
        raise ValueError("H2 needs saved calibration/evaluation animal identities to verify separation")
    for row in table.itertuples():
        groups = {column: set(json.loads(getattr(row, column))) for column in identity_columns}
        calibration_ids = groups["calibration_treatment"] | groups["calibration_controls"]
        evaluation_ids = groups["evaluation_treatment"] | groups["evaluation_controls"]
        if calibration_ids & evaluation_ids or not calibration_ids or not evaluation_ids:
            raise ValueError("H2 calibration/evaluation biological identities overlap or are absent")
        for column, condition, half in (("calibration_treatment", row.treatment_condition, row.calibration_half),
            ("calibration_controls", row.control_condition, row.calibration_half),
            ("evaluation_treatment", row.treatment_condition, 1 - row.calibration_half),
            ("evaluation_controls", row.control_condition, 1 - row.calibration_half)):
            expected = set(animals.loc[(animals.condition_id == condition) & (animals.half == half), "animal_id"])
            if groups[column] != expected:
                raise ValueError("H2 prediction identities do not match the shared-control animal/half crosswalk")
    names = sorted(table.contrast_id.unique())
    excursions = []
    def errors(sample: pd.DataFrame | None = None) -> np.ndarray:
        rows = []
        for row in table.itertuples():
            observed, predicted = row.observed, row.predicted
            if sample is not None:
                treatment = sample[sample.condition_id == row.treatment_condition]
                control = sample[sample.condition_id == row.control_condition]
                cal_t = treatment[treatment.half == row.calibration_half]
                cal_c = control[control.half == row.calibration_half]
                eval_t = treatment[treatment.half != row.calibration_half]
                eval_c = control[control.half != row.calibration_half]
                if min(len(cal_t), len(cal_c), len(eval_t), len(eval_c)) < 2:
                    raise ValueError("H2 bootstrap half lacks independent units")
                activity = cal_t.integrated_activity.mean() - cal_c.integrated_activity.mean()
                amplitude_change = (activity - row.integrated_activity_difference) / row.calibration_activity_derivative
                if "derivative_step" in table:
                    excursions.append(abs(amplitude_change) > row.derivative_step)
                predicted += row.prediction_component_derivative * amplitude_change
                observed = eval_t[row.component].mean() - eval_c[row.component].mean()
            error = ((observed - predicted) / scales[row.component]) ** 2
            rows.append({"contrast_id": row.contrast_id, "model": row.model, "squared_error": error})
        wide = pd.DataFrame(rows).groupby(["contrast_id", "model"]).squared_error.mean().unstack("model").loc[names]
        return wide[[candidate, reference]].to_numpy()
    point_errors = errors()
    reference_error = point_errors[:, 1].mean()
    if reference_error <= 0:
        raise ValueError("A zero-error reference cannot support a relative H2 improvement estimate")
    estimate = float(1 - point_errors[:, 0].mean() / reference_error)
    rng = np.random.default_rng(seed)
    draws, contrast_differences = [], []
    for _ in range(repetitions):
        values = errors(_condition_draw(animals, rng))
        contrast_differences.append(values[:, 1] - values[:, 0])
        indices = rng.integers(len(names), size=len(names))
        denominator = values[indices, 1].mean()
        if denominator > 0:
            draws.append(1 - values[indices, 0].mean() / denominator)
    if len(draws) < .8 * repetitions:
        raise ValueError("H2 resampling produced insufficient finite contrast-unit inference")
    draws = np.asarray(draws)
    centered = draws - estimate
    pvalue = float((1 + np.sum(np.abs(centered) >= abs(estimate))) / (len(draws) + 1))
    excursion_fraction = float(np.mean(excursions)) if excursions else None
    stability_error = (float(table.derivative_relative_stability_error.max())
                       if "derivative_relative_stability_error" in table else None)
    response_validated = bool(stability_error is not None and stability_error <= .1
                              and excursion_fraction is not None and excursion_fraction <= .05)
    return {"estimate": estimate, "ci95": np.quantile(draws, [.025, .975]).tolist(), "pvalue": pvalue,
        "candidate": candidate, "reference": reference, "conditions": len(names),
        "families": int(table.family.nunique()), "contrast_ids": names,
        "shared_control_error_covariance": np.cov(np.asarray(contrast_differences), rowvar=False).tolist(),
        "pvalue_method": "centered coupled animal/contrast bootstrap; first-order calibration-response propagation",
        "uncertainty_scope": "WP1 fits frozen; calibration uncertainty propagated by local response derivative",
        "calibration_response_validated": response_validated,
        "maximum_derivative_relative_stability_error": stability_error,
        "bootstrap_outside_validated_response_radius_fraction": excursion_fraction,
        "calibration_validation_rule": "Coarse/fine derivative relative discrepancy <=10% and >=95% bootstrap amplitude changes within the measured response radius",
        "bootstrap_repetitions": repetitions}


def write_h2(root: Path, config: dict[str, Any], output: Path | None = None) -> dict[str, Any]:
    from .core import atomic_json, canonical_hash, digest, source_identity
    root = Path(root)
    output = Path(output) if output else root / "results" / "interventions"
    settings = resolve_intervention_settings(config, root)
    prediction_path, animal_path = output / "intervention_predictions.tsv", output / "animal_components.tsv"
    predictions, animals = read_table(prediction_path), read_table(animal_path)
    candidate = settings.get("primary_candidate")
    unrestricted = settings.get("unrestricted_reference", "GRU")
    if candidate not in ("MT", "MX"):
        raise ValueError("Freeze MT/MX primary selection on WP1 development before H2 outcomes")
    scales = settings.get("component_scales", {})
    repetitions = int(config.get("inference", {}).get("bootstrap_replicates", 10000))
    gain = h2_shared_control_bootstrap(predictions, animals, candidate, "MG", scales, repetitions,
                                     int(settings.get("seed", 0)))
    flexible = h2_shared_control_bootstrap(predictions, animals, candidate, unrestricted, scales, repetitions,
                                         int(settings.get("seed", 0)))
    if gain["conditions"] < int(settings.get("minimum_conditions", 8)) or gain["families"] < 2:
        raise ValueError("H2 requires at least eight conditions across two independent intervention families")
    # Show every family; a benefit confined to one family cannot pass the gate.
    family_results = {}
    for family in sorted(predictions.family.unique()):
        subset = predictions[predictions.family == family]
        errors = subset.copy()
        errors["squared_error"] = [((r.observed - r.predicted) / scales[r.component]) ** 2 for r in errors.itertuples()]
        means = errors.groupby(["contrast_id", "model"]).squared_error.mean().unstack("model").mean()
        family_results[family] = float(1 - means[candidate] / means.MG) if means.MG > 0 else None
    generalizes = sum(value is not None and value > 0 for value in family_results.values()) >= 2
    ten_percent = gain["estimate"] >= .1 and flexible["estimate"] >= .1
    # A delta-method interval is provisional unless both its finite-difference
    # response approximation and its bootstrap range were actually checked.
    calibration_validated = bool(gain.get("calibration_response_validated", False)
                                and flexible.get("calibration_response_validated", False))
    manifest = root / "manifests" / "source_manifest.json"
    if not manifest.exists():
        raise FileNotFoundError("Actual frozen source manifest is required for H2 provenance")
    result = dict(gain, claim_id="H2", evidence_class="biological_reanalysis",
        independent_unit="intervention-control contrast with shared-control covariance",
        pvalue=max(gain["pvalue"], flexible["pvalue"]), unrestricted_comparison=flexible,
        config_sha256=canonical_hash(config), source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"],
        data_manifest_sha256=digest(manifest), relative_prediction_improvement=gain["estimate"],
        intervention_families_evaluated=gain["families"],
        family_results=family_results, family_generalization_passed=bool(generalizes),
        shared_control_covariance_preserved=True, calibration_response_validated=calibration_validated,
        operational_gates_passed=bool(ten_percent and generalizes and calibration_validated),
        required_checks={"ten_percent_gain_and_unrestricted_improvement": ten_percent,
                         "at_least_two_families": gain["families"] >= 2,
                         "positive_benefit_in_two_families": generalizes,
                         "calibration_and_evaluation_separate": True,
                         "calibration_uncertainty_propagated": True,
                         "calibration_response_validated": calibration_validated,
                         "shared_controls_resampled_once": True},
        claim_status=("pending_joint_Holm" if ten_percent and generalizes and calibration_validated
                      else "unsupported_or_calibration_validation_pending"),
        multiplicity_status="H1/H2/H3 joint Holm adjustment pending",
        source_result_paths=[str(prediction_path.resolve()), str(animal_path.resolve())])
    result["source_result_hashes"] = {path: digest(Path(path)) for path in result["source_result_paths"]}
    atomic_json(root / "results" / "inference" / "H2.json", result)
    plotted = predictions[predictions.model == candidate]
    panels = []
    for component, rows in plotted.groupby("component", sort=True):
        average = rows.groupby("contrast_id")[["observed", "predicted"]].mean()
        panels.append({"kind": "scatter", "x": average.observed.tolist(), "y": average.predicted.tolist(),
            "title": component, "xlabel": "Observed withheld effect", "ylabel": "Predicted withheld effect"})
    atomic_json(root / "results" / "figure_inputs" / "03.json", {
        "evidence_class": "biological_reanalysis", "config_sha256": result["config_sha256"],
        "source_sha256": result["source_sha256"], "source_result_paths": result["source_result_paths"],
        "source_result_hashes": result["source_result_hashes"], "panels": panels,
        "caption": "Existing source interventions challenge a development-frozen model. One intervention amplitude is calibrated from integrated activity only, then predicts withheld movement components in other animals; halves are reversed. Comparators receive the same amplitude budget. Coupled resampling preserves reused controls and propagates calibration uncertainty through the local response. These are calibrated shape predictions, not zero-shot signed intervention forecasts."})
    return result


def family_holdout_folds(mapping: pd.DataFrame) -> list[tuple[str, list[str], list[str]]]:
    if "family" not in mapping or "contrast_id" not in mapping:
        raise ValueError("Preassigned family metadata missing")
    return [(family, mapping.loc[mapping.family != family, "contrast_id"].tolist(),
             mapping.loc[mapping.family == family, "contrast_id"].tolist())
            for family in sorted(mapping.family.unique())]


def attach_frozen_gene_families(genes: pd.DataFrame, families: pd.DataFrame) -> pd.DataFrame:
    """Keep transfer HOGs and verified original MCL families distinct."""
    columns = ["gene_id", "orthogroup", "family_id"]
    if not set(columns).issubset(families):
        raise ValueError("Verified gene_id/orthogroup/family_id map required; older HOG-only family maps remain pending")
    mapped = families[columns].copy()
    if mapped.isna().any().any() or mapped.gene_id.duplicated().any() or genes.gene_id.duplicated().any():
        raise ValueError("Each gene requires a unique nonmissing verified HOG and original MCL family")
    if any(mapped[column].astype(str).str.strip().eq("").any() for column in columns):
        raise ValueError("Empty source gene/HOG/MCL family identity")
    if not set(genes.gene_id).issubset(mapped.gene_id):
        raise ValueError("Target genes are absent from the independently frozen HOG/family map")
    result = genes.merge(mapped, on="gene_id", how="left", validate="one_to_one", suffixes=("_target", ""), sort=False)
    for name in ["orthogroup", "family_id"]:
        existing = name + "_target"
        if existing in result:
            if not result[existing].eq(result[name]).all():
                raise ValueError("Target HOG or original MCL family disagrees with its frozen source map")
            result = result.drop(columns=existing)
    if result.gene_id.tolist() != genes.gene_id.tolist():
        raise ValueError("Source family attachment changed the shared-covariance gene order")
    return result


def run(config: dict[str, Any], root: Path, stage: str = "challenge", **kwargs: Any) -> dict[str, Any]:
    from .semimarkov import Fit
    root = Path(root)
    settings = resolve_intervention_settings(config, root)
    output = root / "results" / "interventions"
    output.mkdir(parents=True, exist_ok=True)
    if stage == "inference":
        return write_h2(root, config, output)
    if stage == "readiness":
        return intervention_readiness(config, root)
    if stage == "screen":
        readiness = intervention_readiness(config, root)
        if not readiness["screen_ready"]:
            raise ValueError("Screen inputs unresolved: " + "; ".join(readiness["screen_pending"]))
        animals = read_table(root / settings["screen_animals"])
        mapping = read_table(root / settings["screen_mapping"])
        effects, covariance = shared_control_effects(animals, mapping, settings.get("screen_metric", "summit_metric"),
                                                     int(settings.get("bootstrap_repetitions", 10000)), int(settings.get("seed", 0)))
        effects.to_csv(output / "reagent_effects.tsv", sep="\t", index=False)
        np.save(output / "shared_control_covariance.npy", covariance)
        if "gene_id" in effects:
            from .core import atomic_json, digest
            eligible = effects.gene_id.notna() & effects.gene_id.astype(str).str.len().gt(0)
            selected = effects.loc[eligible].copy()
            indices = np.flatnonzero(eligible.to_numpy())
            effects.loc[~eligible].to_csv(output / "unmapped_gene_reagents.tsv", sep="\t", index=False)
            if selected.empty:
                raise ValueError("No curated source gene IDs available for molecular transfer")
            selected_covariance = covariance[np.ix_(indices, indices)]
            selected = selected.reset_index(drop=True)
            genes = aggregate_gene_effects(selected, selected_covariance)
            gene_covariance = genes.attrs["shared_control_covariance"]
            table_path = output / "gene_effects.tsv"
            family_map = root / settings.get("target_family_map", "data/molecular/orthology/fly_target_families.tsv")
            catalog_path = root / settings.get("gene_catalog", "data/molecular/orthology/fly_gene_catalog.tsv")
            families, catalog = read_table(family_map), read_table(catalog_path)
            if "gene_id" not in catalog or not {"gene_id", "orthogroup", "family_id"}.issubset(families):
                raise ValueError("An independent catalog and verified HOG/original-MCL family map are required; older HOG-only maps remain pending")
            if catalog.gene_id.duplicated().any() or families.gene_id.duplicated().any():
                raise ValueError("Gene catalog and orthogroup map must uniquely identify each source gene")
            missing_ids = set(genes.gene_id) - set(catalog.gene_id)
            missing_families = set(genes.gene_id) - set(families.gene_id)
            if missing_ids or missing_families:
                raise ValueError(f"Unvalidated gene IDs {sorted(missing_ids)} or absent orthogroups {sorted(missing_families)}")
            freeze_path = root / settings.get("molecular_freeze", "data/molecular/freeze/freeze.json")
            freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
            provenance = freeze.get("provenance", {})
            if (freeze.get("status") != "frozen_before_external_outcomes" or not freeze.get("freeze_hash")
                or provenance.get("fly_outcomes_inspected") is not False
                or provenance.get("fly_gene_catalog_sha256") != digest(catalog_path)
                or provenance.get("target_family_map_sha256") != digest(family_map)):
                raise ValueError("The outcome-blind molecular/catalog/family freeze is absent or changed")
            from .molecular import validate_family_provenance
            family_source = Path(provenance.get("target_family_source_provenance_path", ""))
            if (not family_source.is_file() or provenance.get("target_family_source_provenance_sha256") != digest(family_source)):
                raise ValueError("The prior freeze lacks unchanged verified original-MCL family source provenance")
            validate_family_provenance(json.loads(family_source.read_text(encoding="utf-8")))
            genes = attach_frozen_gene_families(genes, families)
            genes.to_csv(table_path, sep="\t", index=False)
            gene_covariance_path = output / "gene_shared_control_covariance.npy"
            np.save(gene_covariance_path, gene_covariance)
            gene_order_path = output / "gene_covariance_order.json"
            atomic_json(gene_order_path, {"gene_id": genes.gene_id.tolist()})
            input_paths = [root / settings["screen_animals"], root / settings["screen_mapping"],
                catalog_path, family_map, family_source, freeze_path, output / "reagent_effects.tsv", output / "shared_control_covariance.npy",
                gene_covariance_path, gene_order_path]
            atomic_json(output / "target_provenance.json", {
                "validated_gene_ids": True, "shared_control_covariance_preserved": True,
                "reagents_clustered_by_gene": True, "target_family_map_path": str(family_map.resolve()),
                "target_family_map_sha256": digest(family_map), "target_table_sha256": digest(table_path),
                "target_family_source_provenance_path": str(family_source.resolve()),
                "target_family_source_provenance_sha256": digest(family_source),
                "family_definition": "Verified original MCL OG for family dependence; HOG orthogroup retained separately for transfer",
                "gene_catalog_path": str(catalog_path.resolve()), "gene_catalog_sha256": digest(catalog_path),
                "molecular_freeze_path": str(freeze_path.resolve()), "molecular_freeze_hash": freeze["freeze_hash"],
                "gene_covariance_path": str(gene_covariance_path.resolve()),
                "gene_covariance_sha256": digest(gene_covariance_path),
                "gene_covariance_order_path": str(gene_order_path.resolve()),
                "gene_covariance_order_sha256": digest(gene_order_path),
                "source_result_paths": [str(path.resolve()) for path in input_paths],
                "source_result_hashes": {str(path.resolve()): digest(path) for path in input_paths},
                "unmapped_reagents": int((~eligible).sum()),
                "conditional_source_inclusion_flags_retained": True,
                "gene_aggregation": "GLS with retained shared-control covariance and estimated excess reagent variance"})
        return {"reagent_contrasts": len(effects), "output": str(output / "reagent_effects.tsv")}
    if stage == "challenge":
        readiness = intervention_readiness(config, root)
        if not readiness["challenge_ready"]:
            raise ValueError("Challenge inputs unresolved: " + "; ".join(readiness["challenge_pending"]))
        mapping = read_table(root / settings["mapping"])
        animal_metadata = read_table(root / settings["animal_crosswalk"])
        bin_seconds = float(config.get("behaviour", {}).get("primary_bin_seconds", 10))
        bin_path = root / "results" / "behaviour" / f"fly_bins_{bin_seconds:g}s.parquet"
        if bin_path.exists():
            bins = pd.read_parquet(bin_path, filters=[("animal_id", "in", animal_metadata.animal_id.tolist())])
        else:
            data = pd.read_parquet(root / "data" / "fly_observations.parquet", filters=[("animal_id", "in", animal_metadata.animal_id.tolist())])
            bins = bin_trajectories(data, bin_seconds)
        sequences = to_sequences(bins, config.get("behaviour", {}))
        import torch
        from .behaviour import ComparatorFit
        fits = {}
        for name, path in settings.get("frozen_fits", {}).items():
            checkpoint = torch.load(root / path, map_location="cpu", weights_only=False)
            fits[name] = (ComparatorFit.load(root / path) if "kind" in checkpoint.get("constructor", {})
                          else Fit.load(root / path))
        if not fits:
            raise ValueError("WP1 development-frozen fits are required before challenge analysis")
        results = challenge(mapping, sequences, animal_metadata, fits, settings, output)
        folds = family_holdout_folds(mapping)
        (output / "family_holdout_folds.json").write_text(json.dumps(folds, indent=2), encoding="utf-8")
        return {"prediction_rows": len(results), "family_holdout_folds": len(folds)}
    raise ValueError(f"Unsupported intervention stage {stage}")


def intervention_readiness(config: dict[str, Any], root: Path) -> dict[str, Any]:
    """Inventory actual source contracts without assigning biological identities."""
    from .core import atomic_json, canonical_hash, digest, source_identity
    root = Path(root)
    settings = resolve_intervention_settings(config, root)
    screen_pending, challenge_pending, paths = [], [], []
    for key in ("screen_animals", "screen_mapping"):
        path = root / settings[key] if settings.get(key) else None
        if path is None or not path.is_file():
            screen_pending.append(f"{key}: audited individual/experiment/reagent/shared-control source crosswalk absent")
        else:
            paths.append(path)
    for key, default in (("gene_catalog", "data/molecular/orthology/fly_gene_catalog.tsv"),
                         ("target_family_map", "data/molecular/orthology/fly_target_families.tsv"),
                         ("molecular_freeze", "data/molecular/freeze/freeze.json")):
        path = root / settings.get(key, default)
        if not path.is_file():
            screen_pending.append(f"{key}: independent frozen reference absent ({path})")
        else:
            paths.append(path)
            if key == "target_family_map":
                family = read_table(path)
                if not {"gene_id", "orthogroup", "family_id"}.issubset(family):
                    screen_pending.append("target_family_map: older HOG-only map lacks verified original MCL family_id")
            if key == "molecular_freeze":
                freeze = json.loads(path.read_text(encoding="utf-8"))
                frozen_proof = freeze.get("provenance", {})
                source = Path(frozen_proof.get("target_family_source_provenance_path", ""))
                if not source.is_file() or frozen_proof.get("target_family_source_provenance_sha256") != digest(source):
                    screen_pending.append("molecular_freeze: verified original-MCL family source proof absent or changed")
                else:
                    paths.append(source)
    for key in ("mapping", "animal_crosswalk"):
        path = root / settings[key] if settings.get(key) else None
        if path is None or not path.is_file():
            challenge_pending.append(f"{key}: original animal/condition/control/family annotations absent")
        else:
            paths.append(path)
    if not settings.get("frozen_fits"):
        challenge_pending.append("frozen_fits: WP1 development-only model checkpoints absent")
    else:
        for model, specified in settings["frozen_fits"].items():
            path = root / specified
            if not path.is_file():
                challenge_pending.append(f"frozen_fits.{model}: checkpoint absent")
            else:
                paths.append(path)
    if not isinstance(settings.get("movement_threshold"), (float, int)) or settings.get("movement_threshold", -1) <= 0:
        challenge_pending.append("movement_threshold: development-frozen movement criterion absent")
    if not settings.get("component_scales"):
        challenge_pending.append("component_scales: development-only uncertainty scales absent")
    result = {"screen_ready": not screen_pending, "challenge_ready": not challenge_pending,
        "screen_pending": screen_pending, "challenge_pending": challenge_pending,
        "source_constraint": "Pooled cadaver summit matrices cannot establish independent animals, intervention families or shared controls; original crosswalks are mandatory",
        "config_sha256": canonical_hash(config), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "source_result_paths": [str(path.resolve()) for path in paths],
        "source_result_hashes": {str(path.resolve()): digest(path) for path in paths}}
    atomic_json(root / "results" / "interventions" / "source_readiness.json", result)
    return result


def resolve_intervention_settings(config: dict[str, Any], root: Path) -> dict[str, Any]:
    """Resolve reviewed source crosswalks and a validated development freeze."""
    from .core import canonical_hash, digest
    root = Path(root)
    settings = dict(config.get("interventions", {}))
    adapters = root / "manifests" / "intervention_adapters.json"
    if adapters.exists():
        manifest = json.loads(adapters.read_text(encoding="utf-8"))
        if manifest.get("audit_status") != "validated_source_crosswalk":
            raise ValueError("Intervention adapters require a validated original-source crosswalk audit")
        for path, expected in manifest.get("source_result_hashes", {}).items():
            candidate = Path(path) if Path(path).is_absolute() else root / path
            if digest(candidate) != expected:
                raise ValueError("Audited intervention source crosswalk changed")
        if not manifest.get("source_result_hashes"):
            raise ValueError("An intervention adapter without underlying source hashes is not audited")
        operational = manifest.get("settings", {})
        for key in ("mapping", "animal_crosswalk", "screen_animals", "screen_mapping", "screen_metric"):
            if key in operational:
                settings.setdefault(key, operational[key])
    development = root / "manifests" / "wp1_development_freeze.json"
    if development.exists():
        manifest = json.loads(development.read_text(encoding="utf-8"))
        if manifest.get("status") != "development_only_frozen" or manifest.get("config_sha256") != canonical_hash(config):
            raise ValueError("WP1 development freeze does not match the frozen scientific config")
        for path, expected in manifest.get("source_result_hashes", {}).items():
            if digest(Path(path)) != expected:
                raise ValueError("WP1 development model/selection evidence changed after freezing")
        for key in ("primary_candidate", "unrestricted_reference", "frozen_fits", "movement_threshold", "component_scales"):
            if key in manifest:
                if key in settings and settings[key] != manifest[key]:
                    raise ValueError(f"Conflicting config and development-frozen {key}")
                settings[key] = manifest[key]
    return settings
