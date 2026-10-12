"""Duration-dependent movement models with grouped, past-only prediction.

The forward recursion integrates states and dwell ages; it never treats decoded
states as observed neural states. Units, splits and scaling are saved with fits.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from itertools import product
from pathlib import Path
import copy
import json
import math
from typing import Any, Iterable

import numpy as np
import torch
from torch import nn
from scipy.special import logsumexp
from scipy.stats import t as student_t_distribution


MODELS = ("M0", "MG", "MT", "MD", "MX")


@dataclass
class Sequence:
    animal_id: str
    experiment_id: str
    observations: np.ndarray
    covariates: np.ndarray
    modulation: np.ndarray
    previous: np.ndarray
    food_vector: np.ndarray
    arena_vector: np.ndarray
    observed: np.ndarray
    times: np.ndarray
    bin_seconds: float = 10.0
    diagnostics: dict[str, Any] | None = None

    def validate(self) -> None:
        n = len(self.times)
        if n < 2 or self.observations.ndim != 2:
            raise ValueError("A sequence needs at least two bins and vector observations")
        d = self.observations.shape[1]
        for name in ("covariates", "modulation", "previous", "food_vector", "arena_vector"):
            value = np.asarray(getattr(self, name))
            if value.ndim != 2 or len(value) != n or not np.isfinite(value).all():
                raise ValueError(f"Nonfinite/misaligned past-only {name} for {self.animal_id}")
        if any(np.asarray(getattr(self, name)).shape != (n, d)
               for name in ("previous", "food_vector", "arena_vector")):
            raise ValueError("Displacement and drift dimensions disagree")
        if self.observed.shape != (n,) or self.observations.shape[0] != n:
            raise ValueError("Observation mask has the wrong shape")
        if not np.isfinite(self.observations[self.observed]).all():
            raise ValueError("An observed displacement is nonfinite")
        if not np.allclose(np.diff(self.times), self.bin_seconds, atol=1e-6):
            raise ValueError("State sequences require a regular clock, with missing bins masked")


@dataclass
class TrainingScaler:
    mean: list[float]
    scale: list[float]
    modulation_mean: list[float]
    modulation_scale: list[float]
    training_animals: list[str]

    @classmethod
    def fit(cls, sequences: list[Sequence]) -> "TrainingScaler":
        # One animal's long recording does not define everybody else's scale.
        def moments(name: str) -> tuple[list[float], list[float]]:
            arrays = [np.asarray(getattr(s, name), float) for s in sequences]
            mean = np.mean([a.mean(0) for a in arrays], axis=0)
            second = np.mean([(a * a).mean(0) for a in arrays], axis=0)
            scale = np.sqrt(np.maximum(second - mean * mean, 0))
            scale[scale < 1e-10] = 1.0
            return mean.tolist(), scale.tolist()
        mean, scale = moments("covariates")
        mmean, mscale = moments("modulation")
        return cls(mean, scale, mmean, mscale, sorted(s.animal_id for s in sequences))

    def transform(self, sequence: Sequence) -> Sequence:
        result = copy.copy(sequence)
        result.covariates = (sequence.covariates - self.mean) / self.scale
        result.modulation = (sequence.modulation - self.modulation_mean) / self.modulation_scale
        result.diagnostics = dict(sequence.diagnostics or {}, covariate_mean=self.mean,
                                  covariate_scale=self.scale)
        return result


def assert_disjoint(train: list[Sequence], test: list[Sequence], experiment: bool = False) -> None:
    overlap = {s.animal_id for s in train} & {s.animal_id for s in test}
    if overlap:
        raise ValueError(f"Animal leakage across split: {sorted(overlap)[:5]}")
    if experiment:
        overlap = {s.experiment_id for s in train} & {s.experiment_id for s in test}
        if overlap:
            raise ValueError(f"Experiment leakage across split: {sorted(overlap)}")


def training_duration_knots(sequences: list[Sequence]) -> np.ndarray:
    """Freeze duration knots from development-only provisional movement bouts.

    Provisional labels set the spline's coordinate system, not the latent states.
    The threshold is learned only from the supplied training observations.
    """
    speeds = np.concatenate([np.linalg.norm(s.observations[s.observed], axis=1)
                             for s in sequences])
    if not len(speeds):
        raise ValueError("No observed training displacement")
    threshold = np.median(speeds)
    elapsed: list[float] = []
    for sequence in sequences:
        states = np.linalg.norm(np.nan_to_num(sequence.observations), axis=1) > threshold
        age, previous_state = 0, None
        for state, valid in zip(states, sequence.observed):
            if not valid:
                age, previous_state = 0, None
                continue
            age = age + 1 if previous_state == state else 1
            elapsed.append(math.log(age * sequence.bin_seconds))
            previous_state = state
    knots = np.quantile(elapsed, [0.25, 0.5, 0.75])
    # Repeated quartiles retain their observed locations; the basis uses a stable
    # zero contribution for a collapsed natural-spline interval.
    return knots


def duration_basis(ages: torch.Tensor, knots: torch.Tensor) -> torch.Tensor:
    """Linear term plus a natural cubic term with three training-quartile knots."""
    x = torch.log(ages.clamp_min(1e-10))
    a, b, c = knots.unbind()
    width = (c - a).clamp_min(1e-8)
    def term(k: torch.Tensor) -> torch.Tensor:
        return ((x - k).clamp_min(0).pow(3)
                - (x - c).clamp_min(0).pow(3)) / (c - k).clamp_min(1e-8)
    cubic = (term(a) - term(b)) / width.pow(2)
    cubic = torch.where((c - b) > 1e-8, cubic, torch.zeros_like(cubic))
    return torch.stack([x - b, cubic], dim=-1)


def student_t_logpdf(observation: torch.Tensor, mean: torch.Tensor,
                     chol: torch.Tensor, df: float = 5.0) -> torch.Tensor:
    """Multivariate Student-t log density; chol describes its scale matrix."""
    residual = observation - mean
    whitened = torch.linalg.solve_triangular(chol, residual.unsqueeze(-1), upper=False).squeeze(-1)
    d = residual.shape[-1]
    constant = (torch.lgamma(torch.as_tensor((df + d) / 2, dtype=residual.dtype,
                                             device=residual.device))
                - torch.lgamma(torch.as_tensor(df / 2, dtype=residual.dtype,
                                              device=residual.device))
                - 0.5 * d * math.log(df * math.pi))
    return (constant - torch.diagonal(chol, dim1=-2, dim2=-1).log().sum(-1)
            - 0.5 * (df + d) * torch.log1p(whitened.square().sum(-1) / df))


class MovementModel(nn.Module):
    """An explicit-duration, two/three-state movement model.

    All alternatives share covariates, duration basis, drift and priors. Only the
    permitted modulation coefficients change. A finite age tail is optional and
    its probability is returned; exact recursion is the default.
    """
    def __init__(self, model: str, n_covariates: int, n_modulation: int,
                 dimension: int = 2, knots: Iterable[float] = (1, 2, 3),
                 animal_ids: Iterable[str] = (), experiment_ids: Iterable[str] = (),
                 states: int = 2, max_duration_bins: int | None = None,
                 bin_seconds: float = 10.0):
        super().__init__()
        model = model.replace("_", "")
        if model not in MODELS or states not in (2, 3):
            raise ValueError("Unknown movement model or state count")
        if dimension not in (1, 2) or (dimension == 1 and model in ("MD", "MX")):
            raise ValueError("Directional modulation requires actual two-dimensional positions")
        self.model, self.dimension, self.states = model, dimension, states
        self.max_duration_bins, self.bin_seconds = max_duration_bins, bin_seconds
        self.animal_ids = sorted(set(animal_ids))
        self.experiment_ids = sorted(set(experiment_ids))
        self.register_buffer("knots", torch.tensor(list(knots), dtype=torch.float64))
        self.exit_intercept = nn.Parameter(torch.full((states,), -2.5, dtype=torch.float64))
        self.context_beta = nn.Parameter(torch.zeros(states, n_covariates, dtype=torch.float64))
        self.duration_beta = nn.Parameter(torch.zeros(states, 2, dtype=torch.float64))
        self.destination_logits = nn.Parameter(torch.zeros(states, states, dtype=torch.float64))
        self.animal_z = nn.Parameter(torch.zeros(len(self.animal_ids), states, dtype=torch.float64))
        self.experiment_z = nn.Parameter(torch.zeros(len(self.experiment_ids), states, dtype=torch.float64))
        self.log_animal_sd = nn.Parameter(torch.full((states,), -2., dtype=torch.float64))
        self.log_experiment_sd = nn.Parameter(torch.full((states,), -2., dtype=torch.float64))
        self.log_rest_scale = nn.Parameter(torch.zeros(dimension, dtype=torch.float64))
        self.scale_separation = nn.Parameter(torch.ones(states - 1, dimension, dtype=torch.float64))
        self.move_correlation = nn.Parameter(torch.zeros(states - 1, dtype=torch.float64))
        self.ar_raw = nn.Parameter(torch.zeros(states - 1, dtype=torch.float64))
        self.food_beta = nn.Parameter(torch.zeros(states - 1, dtype=torch.float64))
        self.axis_beta = nn.Parameter(torch.zeros(states - 1, dtype=torch.float64))
        self.baseline_scale = nn.Parameter(torch.zeros(states - 1, n_covariates, dtype=torch.float64))
        def modulation_parameter(name: str, shape: tuple[int, ...], active: bool) -> None:
            value = torch.zeros(shape, dtype=torch.float64)
            if active:
                setattr(self, name, nn.Parameter(value))
            else:
                self.register_buffer(name, value)
        modulation_parameter("transition_mod", (states, n_modulation), model in ("MT", "MX"))
        modulation_parameter("gain_mod", (states - 1, n_modulation), model in ("MG", "MX"))
        modulation_parameter("direction_mod", (states - 1, n_modulation), model in ("MD", "MX"))

    def _tensor(self, value: Any) -> torch.Tensor:
        return torch.as_tensor(value, dtype=self.knots.dtype, device=self.knots.device)

    def _random_effect(self, sequence: Sequence) -> torch.Tensor:
        result = torch.zeros(self.states, dtype=self.knots.dtype, device=self.knots.device)
        if sequence.animal_id in self.animal_ids:
            result = result + self.animal_z[self.animal_ids.index(sequence.animal_id)] * self.log_animal_sd.exp()
        if sequence.experiment_id in self.experiment_ids:
            result = result + self.experiment_z[self.experiment_ids.index(sequence.experiment_id)] * self.log_experiment_sd.exp()
        return result

    def predictive_random_effects(self, sequence: Sequence, quadrature: int = 3
                                  ) -> tuple[torch.Tensor, torch.Tensor]:
        """Integrate unseen animal/experiment effects, conditioned through history."""
        variance = torch.zeros(self.states, dtype=self.knots.dtype, device=self.knots.device)
        if sequence.animal_id not in self.animal_ids:
            variance = variance + self.log_animal_sd.exp().square()
        if sequence.experiment_id not in self.experiment_ids:
            variance = variance + self.log_experiment_sd.exp().square()
        mean = self._random_effect(sequence)
        if not bool(torch.any(variance > 0)):
            return mean[None], self._tensor([0.0])
        nodes, weights = np.polynomial.hermite.hermgauss(quadrature)
        combinations = np.asarray(list(product(range(quadrature), repeat=self.states)))
        offsets = self._tensor(nodes[combinations] * math.sqrt(2)) * variance.sqrt()
        log_weights = self._tensor(np.log(weights[combinations] / math.sqrt(math.pi)).sum(1))
        return mean[None] + offsets, log_weights

    def emissions(self, sequence: Sequence, overrides: dict[str, torch.Tensor] | None = None
                  ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        x, m = self._tensor(sequence.covariates), self._tensor(sequence.modulation)
        prev, food, axis = (self._tensor(getattr(sequence, n))
                            for n in ("previous", "food_vector", "arena_vector"))
        override = overrides or {}
        gain_coef = override.get("gain_mod", self.gain_mod)
        direction_coef = override.get("direction_mod", self.direction_mod)
        # Ordered noise scales remove a free permutation of latent state labels.
        log_scales = torch.cat([self.log_rest_scale[None],
                               self.log_rest_scale[None] +
                               torch.cumsum(torch.nn.functional.softplus(self.scale_separation), 0)], 0)
        means, chols = [], []
        for state in range(self.states):
            if state == 0:
                means.append(torch.zeros_like(prev))
                chols.append(torch.diag(log_scales[0].exp()).expand(len(x), -1, -1))
                continue
            j = state - 1
            mean = self.ar_raw[j].tanh() * prev + self.food_beta[j] * food + self.axis_beta[j] * axis
            if self.dimension == 2:
                angle = m @ direction_coef[j] + override.get("direction_shift", self._tensor(np.zeros(self.states - 1)))[j]
                cosine, sine = torch.cos(angle), torch.sin(angle)
                mean = torch.stack([cosine * mean[:, 0] - sine * mean[:, 1],
                                    sine * mean[:, 0] + cosine * mean[:, 1]], -1)
            gain = torch.exp((x @ self.baseline_scale[j] + m @ gain_coef[j]
                              + override.get("gain_shift", self._tensor(np.zeros(self.states - 1)))[j]).clamp(-12, 12))
            means.append(mean * gain[:, None])
            sd = log_scales[state].exp()
            if self.dimension == 2:
                rho = 0.95 * self.move_correlation[j].tanh()
                zero = sd[0] * 0
                chol = torch.stack([torch.stack([sd[0], zero]),
                                    torch.stack([rho * sd[1], torch.sqrt(1 - rho.square()) * sd[1]])])
            else:
                chol = torch.diag(sd)
            chols.append(chol[None] * gain[:, None, None])
        means_t, chols_t = torch.stack(means, 1), torch.stack(chols, 1)
        y = self._tensor(np.nan_to_num(sequence.observations))[:, None, :]
        density = student_t_logpdf(y, means_t, chols_t)
        mask = torch.as_tensor(sequence.observed, device=x.device)
        return torch.where(mask[:, None], density, torch.zeros_like(density)), means_t, chols_t

    def forward(self, sequence: Sequence, random_effects: torch.Tensor | None = None,
                overrides: dict[str, torch.Tensor] | None = None,
                return_filter: bool = False) -> dict[str, torch.Tensor]:
        sequence.validate()
        x, m = self._tensor(sequence.covariates), self._tensor(sequence.modulation)
        effects = self._random_effect(sequence)[None] if random_effects is None else random_effects
        b, k, n = len(effects), self.states, len(x)
        cap = n if self.max_duration_bins is None else int(self.max_duration_bins)
        if cap < 2:
            raise ValueError("Duration cap must allow at least two bins")
        ages = torch.arange(1, cap + 1, dtype=x.dtype, device=x.device) * sequence.bin_seconds
        basis = duration_basis(ages, self.knots)
        tr = (overrides or {}).get("transition_mod", self.transition_mod)
        hazards = (x @ self.context_beta.T + m @ tr.T + self.exit_intercept
                   + (overrides or {}).get("transition_shift", self._tensor(np.zeros(k))))
        duration_term = basis @ self.duration_beta.T
        emissions, means, chols = self.emissions(sequence, overrides)
        diagonal = torch.eye(k, dtype=torch.bool, device=x.device)
        destinations = torch.softmax(self.destination_logits.masked_fill(diagonal, -torch.inf), -1)
        alpha = torch.ones(b, k, 1, dtype=x.dtype, device=x.device) / k
        scores, occupancies, priors, tails = [], [], [], []
        for t in range(n):
            if t:
                age_count = alpha.shape[-1]
                hazard = torch.sigmoid(hazards[t][None, :, None] + effects[:, :, None]
                                       + duration_term[:age_count].T[None])
                exits = (alpha * hazard).sum(-1)
                entrants = exits @ destinations
                stays = alpha * (1 - hazard)
                if age_count < cap:
                    alpha = torch.cat([entrants[:, :, None], stays], -1)
                else:
                    # The optional last-age bucket is a geometric tail, never a
                    # forced state exit. Its occupied probability is audited.
                    tail = stays[:, :, -1:] + stays[:, :, -2:-1]
                    alpha = torch.cat([entrants[:, :, None], stays[:, :, :-2], tail], -1)
            log_emission = emissions[t]
            if return_filter:
                priors.append(alpha.sum(-1))
            shift = torch.max(log_emission)
            weighted = alpha * torch.exp(log_emission[None, :, None] - shift)
            normalizer = weighted.sum((1, 2)).clamp_min(torch.finfo(x.dtype).tiny)
            alpha = weighted / normalizer[:, None, None]
            scores.append(torch.log(normalizer) + shift)
            if return_filter:
                occupancies.append(alpha.sum(-1))
            tails.append(alpha[:, :, -1].sum(-1) if t + 1 >= cap and cap < n
                         else torch.zeros(b, dtype=x.dtype, device=x.device))
        result = {"log_scores": torch.stack(scores, 1),
                  "log_likelihood": torch.stack(scores, 1).sum(1),
                  "max_tail_probability": torch.stack(tails, 1).max(1).values,
                  "log_prior": self.log_prior()}
        if return_filter:
            result.update(occupancy=torch.stack(occupancies, 1),
                          prior_occupancy=torch.stack(priors, 1), mean=means, chol=chols)
        return result

    def log_prior(self) -> torch.Tensor:
        # Noncentred Gaussian group effects and half-Normal(0,.5) SD priors.
        # +log(sd) is the Jacobian for the optimized log-SD coordinates.
        prior = torch.zeros((), dtype=self.knots.dtype, device=self.knots.device)
        group = {"animal_z", "experiment_z", "log_animal_sd", "log_experiment_sd"}
        for name, parameter in self.named_parameters():
            if name in ("animal_z", "experiment_z"):
                prior = prior - 0.5 * parameter.square().sum()
            elif name in ("log_animal_sd", "log_experiment_sd"):
                prior = prior - 0.5 * (parameter.exp() / 0.5).square().sum() + parameter.sum()
            elif name not in group:
                prior = prior - 0.5 * (parameter / 1.5).square().sum()
        return prior


@dataclass
class Fit:
    model: MovementModel
    scaler: TrainingScaler
    diagnostics: dict[str, Any]

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({"state_dict": self.model.state_dict(), "scaler": asdict(self.scaler),
                    "diagnostics": self.diagnostics,
                    "constructor": {"model": self.model.model,
                        "n_covariates": len(self.scaler.mean),
                        "n_modulation": len(self.scaler.modulation_mean),
                        "dimension": self.model.dimension, "states": self.model.states,
                        "knots": self.model.knots.cpu().tolist(),
                        "animal_ids": self.model.animal_ids,
                        "experiment_ids": self.model.experiment_ids,
                        "max_duration_bins": self.model.max_duration_bins,
                        "bin_seconds": self.model.bin_seconds}}, path)

    @classmethod
    def load(cls, path: Path, device: str = "cpu") -> "Fit":
        value = torch.load(path, map_location=device, weights_only=False)
        model = MovementModel(**value["constructor"]).to(device)
        model.load_state_dict(value["state_dict"])
        return cls(model, TrainingScaler(**value["scaler"]), value["diagnostics"])

    def score(self, sequence: Sequence, warmup_seconds: float = 600,
              quadrature: int = 3) -> dict[str, Any]:
        seq = self.scaler.transform(sequence)
        with torch.no_grad():
            effects, weights = self.model.predictive_random_effects(seq, quadrature)
            out = self.model(seq, effects, return_filter=True)
            cumulative = torch.cumsum(out["log_scores"], 1)
            marginal = torch.logsumexp(cumulative + weights[:, None], 0)
            scores = torch.cat([marginal[:1], marginal[1:] - marginal[:-1]])
            eligible = (sequence.observed &
                        (sequence.times - sequence.times[0] >= warmup_seconds))
            if not eligible.any():
                raise ValueError(f"No observed prediction window after warm-up: {sequence.animal_id}")
            tail = float(out["max_tail_probability"].max().cpu())
            tolerance = self.diagnostics.get("tail_tolerance", 1e-8)
            if tail > tolerance:
                raise ValueError(f"Duration tail probability {tail:g} exceeds {tolerance:g}; increase cap")
            previous_cumulative = torch.cat([torch.zeros_like(cumulative[:, :1]), cumulative[:, :-1]], 1)
            effect_weights = torch.softmax(previous_cumulative + weights[:, None], 0)
            state_weights = (effect_weights[:, :, None] * out["prior_occupancy"]).sum(0).cpu().numpy()
            mean = out["mean"].cpu().numpy()
            coordinate_scale = np.sqrt(np.sum(out["chol"].cpu().numpy() ** 2, axis=-1))
            z = (np.nan_to_num(sequence.observations)[:, None, :] - mean) / coordinate_scale
            pit = np.sum(state_weights[:, :, None] * student_t_distribution.cdf(z, df=5), axis=1)
            coverage = np.mean((pit[eligible] >= .05) & (pit[eligible] <= .95), axis=0)
            return {"animal_id": sequence.animal_id, "experiment_id": sequence.experiment_id,
                    "model": self.diagnostics.get("display_model", self.model.model), "n_scored": int(eligible.sum()),
                    "log_score": float(scores.cpu().numpy()[eligible].mean()),
                    "max_tail_probability": tail,
                    "marginal_90pct_coverage": coverage.tolist(),
                    "calibration_error": float(np.mean(np.abs(coverage - .9)))}


def fit_model(train: list[Sequence], model: str, config: dict[str, Any] | None = None,
              validation: list[Sequence] | None = None) -> Fit:
    config = config or {}
    if not train:
        raise ValueError("Cannot fit without independent training animals")
    for s in train:
        s.validate()
    if len({s.animal_id for s in train}) != len(train):
        raise ValueError("Repeated animal representation in training")
    if validation:
        assert_disjoint(train, validation, experiment=True)
    scaler = TrainingScaler.fit(train)
    sequences = [scaler.transform(s) for s in train]
    n_observed = sum(int(s.observed.sum()) for s in sequences)
    if not n_observed:
        raise ValueError("No observed training data")
    knots = training_duration_knots(train)
    starts = int(config.get("starts", 5))
    epochs = int(config.get("max_epochs", 500))
    learning_rate = float(config.get("learning_rate", 0.001))
    patience = int(config.get("patience", 30))
    device = config.get("device", "cpu")
    if str(device) == "cpu":
        torch.set_num_threads(int(config.get("threads", 1)))
    candidates, records = [], []
    for start in range(starts):
        torch.manual_seed(int(config.get("seed", 0)) + start)
        m = MovementModel(model, len(scaler.mean), len(scaler.modulation_mean),
                          train[0].observations.shape[1], knots,
                          [s.animal_id for s in train], [s.experiment_id for s in train],
                          states=int(config.get("states", 2)),
                          max_duration_bins=config.get("max_duration_bins"),
                          bin_seconds=train[0].bin_seconds).to(device)
        if config.get("hmm_baseline"):
            with torch.no_grad():
                m.duration_beta.zero_()
            m.duration_beta.requires_grad_(False)
        observed = np.concatenate([s.observations[s.observed] for s in train])
        scale = np.maximum(np.std(observed, axis=0), 1e-4)
        with torch.no_grad():
            m.log_rest_scale.copy_(m._tensor(np.log(scale / 3)))
            for name, parameter in m.named_parameters():
                if parameter.requires_grad and name not in ("log_rest_scale", "log_animal_sd", "log_experiment_sd"):
                    parameter.add_(torch.randn_like(parameter) * 0.05)
        optimizer = torch.optim.Adam(m.parameters(), lr=learning_rate)
        best, stale, best_state = math.inf, 0, None
        history = []
        for epoch in range(epochs):
            optimizer.zero_grad()
            objective = -m.log_prior() / n_observed
            objective.backward()
            objective_value = float(objective.detach().cpu())
            for sequence in sequences:
                loss = -m(sequence)["log_likelihood"].sum() / n_observed
                if not torch.isfinite(loss):
                    raise FloatingPointError("Nonfinite movement likelihood")
                loss.backward()
                objective_value += float(loss.detach().cpu())
            nn.utils.clip_grad_norm_(m.parameters(), float(config.get("gradient_clip", 5)))
            optimizer.step()
            current_fit = Fit(m, scaler, {"tail_tolerance": config.get("tail_tolerance", 1e-8)})
            if validation:
                # Equal weighting by held-out experiment, then animal.
                grouped: dict[str, list[float]] = {}
                for seq in validation:
                    value = current_fit.score(seq, config.get("warmup_seconds", 600))["log_score"]
                    grouped.setdefault(seq.experiment_id, []).append(value)
                criterion = -float(np.mean([np.mean(v) for v in grouped.values()]))
            else:
                criterion = objective_value
            history.append({"epoch": epoch, "objective": objective_value, "criterion": criterion})
            if criterion < best - float(config.get("convergence_tolerance", 1e-7)):
                best, stale = criterion, 0
                best_state = copy.deepcopy(m.state_dict())
            else:
                stale += 1
            if stale >= patience:
                break
        if best_state is None:
            raise RuntimeError("No finite optimizer solution")
        m.load_state_dict(best_state)
        # L-BFGS refines the selected Adam solution on the same training set.
        iterations = int(config.get("lbfgs_iterations", 200))
        if iterations:
            optimizer = torch.optim.LBFGS(m.parameters(), max_iter=iterations,
                                         line_search_fn="strong_wolfe")
            def closure() -> torch.Tensor:
                optimizer.zero_grad()
                total = -m.log_prior() / n_observed
                total.backward()
                value = total.detach().clone()
                for seq in sequences:
                    loss = -m(seq)["log_likelihood"].sum() / n_observed
                    loss.backward()
                    value = value + loss.detach()
                return value
            optimizer.step(closure)
        with torch.no_grad():
            objective = float((-m.log_prior() - sum(m(s)["log_likelihood"].sum()
                                                   for s in sequences)).cpu()) / n_observed
            tails = [float(m(s)["max_tail_probability"].max().cpu()) for s in sequences]
        tolerance = float(config.get("tail_tolerance", 1e-8))
        if max(tails) > tolerance:
            raise ValueError("Nonnegligible duration overflow; increase max_duration_bins or use exact recursion")
        record = {"start": start, "objective": objective, "epochs": len(history),
                  "criterion": best, "history": history, "max_tail_probability": max(tails)}
        records.append(record)
        candidates.append((objective, copy.deepcopy(m)))
    candidates.sort(key=lambda c: c[0])
    spread = [v - candidates[0][0] for v, _ in candidates]
    diagnostics = {"starts": records, "objective_spread": spread,
                   "tail_tolerance": config.get("tail_tolerance", 1e-8),
                   "training_animals": scaler.training_animals,
                   "training_experiments": sorted({s.experiment_id for s in train}),
                   "duration_knots": knots.tolist(), "config": config,
                   "converged_starts": sum(v < config.get("start_agreement_tolerance", 1e-3)
                                            for v in spread)}
    return Fit(candidates[0][1], scaler, diagnostics)


def simulate(model: MovementModel, template: Sequence, seed: int,
             overrides: dict[str, torch.Tensor] | None = None) -> tuple[Sequence, np.ndarray]:
    """Parametric recovery at the exact clock, covariates and missingness pattern."""
    template.validate()
    rng = np.random.default_rng(seed)
    result = copy.deepcopy(template)
    states = np.zeros(len(template.times), dtype=int)
    state, age = int(rng.integers(model.states)), 1
    y = np.zeros_like(template.observations)
    information = template.diagnostics or {}
    feature_names = information.get("covariate_names", [])
    feature_means = np.asarray(information.get("covariate_mean", np.zeros(template.covariates.shape[1])))
    feature_scales = np.asarray(information.get("covariate_scale", np.ones(template.covariates.shape[1])))
    initial_position = np.asarray(information.get("initial_position", np.zeros(model.dimension)), float)
    position = initial_position.copy()
    food_target = initial_position + template.food_vector[0]
    with torch.no_grad():
        for t in range(len(y)):
            if t == 0 and not template.observed[0] and information.get("initial_position_known"):
                states[t] = state
                continue
            for j, coordinate in enumerate(("previous_x", "previous_y") if model.dimension == 2 else ("previous_y",)):
                if coordinate in feature_names:
                    index = feature_names.index(coordinate)
                    result.covariates[t, index] = (position[j] - feature_means[index]) / feature_scales[index]
            result.food_vector[t] = food_target - position
            if t:
                cap = model.max_duration_bins
                dwell = min(age, cap) if cap else age
                basis = duration_basis(model._tensor([dwell * template.bin_seconds]), model.knots)[0]
                tr = (overrides or {}).get("transition_mod", model.transition_mod)
                logit = (model.exit_intercept[state]
                         + model._tensor(result.covariates[t]) @ model.context_beta[state]
                         + model._tensor(result.modulation[t]) @ tr[state]
                         + basis @ model.duration_beta[state]
                         + model._random_effect(result)[state]
                         + (overrides or {}).get("transition_shift", model._tensor(np.zeros(model.states)))[state])
                if rng.random() < float(torch.sigmoid(logit).cpu()):
                    logits = model.destination_logits[state].cpu().numpy().copy()
                    logits[state] = -np.inf
                    probabilities = np.exp(logits - logsumexp(logits))
                    state, age = int(rng.choice(model.states, p=probabilities)), 1
                else:
                    age += 1
            result.previous[t] = y[t - 1] if t and result.observed[t - 1] else 0
            # Slice keeps the autoregressive mean consistent with generated past.
            one = copy.copy(result)
            for name in ("observations", "covariates", "modulation", "previous",
                         "food_vector", "arena_vector", "observed", "times"):
                setattr(one, name, getattr(result, name)[t:t + 1])
            _, means, chols = model.emissions(one, overrides)
            mean = means[0, state].cpu().numpy()
            chol = chols[0, state].cpu().numpy()
            y[t] = mean + chol @ rng.standard_normal(model.dimension) / math.sqrt(rng.chisquare(5) / 5)
            position = position + y[t]
            states[t] = state
    result.observations = y
    result.observations[~result.observed] = np.nan
    return result, states


def bayesian_check(fit: Fit, sequences: list[Sequence], config: dict[str, Any] | None = None
                   ) -> dict[str, Any]:
    """Exact-density MALA check on a declared representative subset.

    This checks the MAP approximation using posterior samples; it is not a claim
    that a short or poorly mixed chain establishes credible parameter recovery.
    Acceptance, split R-hat and conservative autocorrelation ESS are returned.
    """
    from torch.func import functional_call
    config = config or {}
    m = fit.model
    transformed = [fit.scaler.transform(s) for s in sequences]
    if set(s.animal_id for s in sequences) != set(m.animal_ids):
        raise ValueError("Refit the declared Bayesian subset; do not leave unused animal effects")
    names, shapes, sizes = [], [], []
    pieces = []
    for name, value in m.named_parameters():
        if value.requires_grad:
            names.append(name)
            shapes.append(value.shape)
            sizes.append(value.numel())
            pieces.append(value.detach().flatten())
    initial = torch.cat(pieces)
    if len(initial) > int(config.get("maximum_parameters", 256)):
        raise ValueError("Choose a smaller declared Bayesian subset before dense posterior checking")
    def parameters(theta: torch.Tensor) -> dict[str, torch.Tensor]:
        result, offset = {}, 0
        for name, shape, size in zip(names, shapes, sizes):
            result[name] = theta[offset:offset + size].reshape(shape)
            offset += size
        return result
    def density(theta: torch.Tensor) -> torch.Tensor:
        params = parameters(theta)
        values = [functional_call(m, params, (s,)) for s in transformed]
        return sum(value["log_likelihood"].sum() for value in values) + values[0]["log_prior"]
    def value_gradient(theta: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        theta = theta.detach().requires_grad_(True)
        value = density(theta)
        grad = torch.autograd.grad(value, theta)[0]
        return value.detach(), grad.detach()
    curvature = -torch.autograd.functional.hessian(density, initial).diagonal().detach()
    preconditioner = 1 / curvature.abs().clamp_min(.1)
    samples, rates = [], []
    chains = int(config.get("chains", 4))
    burn = int(config.get("warmup", 1000))
    draws = int(config.get("draws", 2000))
    if chains < 2 or draws < 20:
        raise ValueError("Posterior diagnostics need at least two chains and twenty retained samples")
    for chain in range(chains):
        torch.manual_seed(int(config.get("seed", 0)) + chain)
        theta = initial + torch.randn_like(initial) * .01 * preconditioner.sqrt()
        value, gradient = value_gradient(theta)
        log_step = math.log(float(config.get("step_size", .05)))
        saved, accepted = [], 0
        for iteration in range(burn + draws):
            step = math.exp(log_step)
            mean = theta + .5 * step * step * preconditioner * gradient
            proposal = mean + step * preconditioner.sqrt() * torch.randn_like(theta)
            try:
                next_value, next_gradient = value_gradient(proposal)
                reverse_mean = proposal + .5 * step * step * preconditioner * next_gradient
                log_forward = -.5 * ((proposal - mean).square() / (step * step * preconditioner)).sum()
                log_reverse = -.5 * ((theta - reverse_mean).square() / (step * step * preconditioner)).sum()
                log_accept = next_value - value + log_reverse - log_forward
                accept = bool(torch.isfinite(log_accept) and torch.log(torch.rand((), device=theta.device)) < log_accept)
            except (RuntimeError, FloatingPointError):
                accept = False
            if accept:
                theta, value, gradient = proposal.detach(), next_value, next_gradient
            if iteration < burn:
                log_step = float(np.clip(log_step + (iteration + 10) ** -.6 * (float(accept) - .574), -12, 1))
            else:
                accepted += int(accept)
                saved.append(theta.detach().cpu().numpy().copy())
        samples.append(np.asarray(saved))
        rates.append(accepted / draws)
    array = np.asarray(samples)
    half = draws // 2
    split = np.concatenate([array[:, :half], array[:, -half:]], axis=0)
    within = split.var(axis=1, ddof=1).mean(0)
    between = half * split.mean(axis=1).var(axis=0, ddof=1)
    variance = (half - 1) / half * within + between / half
    rhat = np.sqrt(variance / np.maximum(within, 1e-30))
    ess = np.empty(len(initial))
    for parameter in range(len(initial)):
        autocorrelation = []
        for lag in range(1, min(half // 2, 200)):
            centered = split[:, :, parameter] - split[:, :, parameter].mean(1)[:, None]
            covariance = np.mean(np.sum(centered[:, :-lag] * centered[:, lag:], axis=1) / (half - lag))
            rho = covariance / max(within[parameter], 1e-30)
            if rho <= 0:
                break
            autocorrelation.append(rho)
        ess[parameter] = len(split) * half / (1 + 2 * sum(autocorrelation))
    flat = array.reshape(-1, array.shape[-1])
    return {"parameter_names": names, "parameter_shapes": [list(s) for s in shapes],
            "posterior_mean": flat.mean(0).tolist(),
            "posterior_interval": np.quantile(flat, [.025, .975], axis=0).tolist(),
            "acceptance_rate": rates, "split_rhat": rhat.tolist(), "effective_sample_size": ess.tolist(),
            "adequate_mixing": bool(np.max(rhat) < 1.05 and np.min(ess) >= 100),
            "configuration": config, "samples": array}
