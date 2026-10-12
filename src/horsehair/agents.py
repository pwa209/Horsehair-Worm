"""Reproducible recurrent PPO and frozen-policy perturbation experiments.

These environments are computational interventions, never biological observations.
PyTorch is imported lazily so dataset commands do not require a GPU runtime.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import random
import time
from pathlib import Path
from typing import Any

import numpy as np

from .core import canonical_hash, source_identity

OBS_DIM = 32
ACTION_DIM = 5
TASKS = ("foraging", "corridor", "delayed_choice")
ARCHITECTURES = ("rnn", "gru", "modular")
PROBES = ("delayed_cue", "goal_detection", "motor_execution")
MECHANISMS = ("input_gain", "recurrent_gate", "hidden_displacement", "output_gain", "random_subspace", "unstructured", "motor_gain")
ACTIONS = np.asarray(((0, -1), (0, 1), (-1, 0), (1, 0), (0, 0)), dtype=int)


def _torch():
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError("Agent experiments require PyTorch; install the pinned research environment.") from exc
    return torch


def _json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _implementation_sha256():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def h4_specification(config):
    """Freeze every H4 analysis choice before final-policy evaluation."""
    inference = config.get("inference", {})
    return {"alpha": float(inference.get("alpha", .05)),
            "agent_redirection_difference": float(inference.get("agent_redirection_difference", .2)),
            "agent_competence_loss_max": float(inference.get("agent_competence_loss_max", .05)),
            "bootstrap_replicates": int(inference.get("bootstrap_replicates", 10000)),
            "agent_reference_point": dict(inference.get("agent_reference_point", {
                "rule_type":"synthetic", "perturbation":"recurrent_gate", "rank":1,
                "matching_arm":"parameter_norm", "strength":.2})),
            "point_test":"intersection-union of redirection, three probe losses and two paired comparator advantages",
            "comparator_estimand":"paired seed difference in worst operating margin",
            "family":"all frozen primary mechanism/rank/strength/matching-arm/rule/architecture/task grid points",
            "correction":"Holm", "unavailable_points":"retain with conservative pvalue one",
            "baseline_probe_accuracy_min":float(config.get("agents",config).get("baseline_probe_accuracy_min",.8))}


def _benchmark_provenance(config, destination, evaluation_device):
    peak = None
    try:
        import resource
        peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)*1024
    except ImportError:
        pass
    return {"full_config_sha256":canonical_hash(config),
            "source_sha256":source_identity(Path(__file__).resolve().parents[2])["sha256"],
            "slurm_job_id":os.environ.get("SLURM_JOB_ID"),
            "allocated_cpus":int(os.environ.get("SLURM_CPUS_PER_TASK", str(_torch().get_num_threads()))),
            "working_bytes":sum(p.stat().st_size for p in Path(destination).rglob("*") if p.is_file()),
            "peak_process_ram_bytes":peak, "evaluation_device":evaluation_device,
            "torch_threads":_torch().get_num_threads()}


def seed_everything(seed: int, deterministic: bool = True) -> None:
    # Set before importing/initializing CUDA. Required by deterministic CUDA
    # matrix multiplication on CUDA >=10.2, including the linear policy heads.
    if deterministic:
        os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")
    torch = _torch()
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    if deterministic:
        torch.use_deterministic_algorithms(True)
        torch.backends.cudnn.benchmark = False


class Environment:
    """32-dimensional partially observed tasks with five shared actuator actions.

    Training and evaluation generate disjoint seeds. Every environment reaches a
    terminal or the 200-step cap. A separate endpoint tracks distractor selection.
    """

    def __init__(self, task: str, seed: int, split: str = "train", probe: str | None = None, max_steps: int = 200):
        if task not in TASKS or probe not in (None, *PROBES):
            raise ValueError(f"Unknown task/probe: {task}/{probe}")
        if split not in ("train", "calibration", "validation", "heldout"):
            raise ValueError(f"Unknown environment split: {split}")
        if not 1 <= max_steps <= 200:
            raise ValueError("Episode cap must be between 1 and 200 steps.")
        self.task, self.seed, self.split, self.probe, self.max_steps = task, int(seed), split, probe, max_steps
        self.rng = np.random.default_rng(seed)
        self.t = 0
        self.reset()

    def reset(self):
        self.t = 0
        self.position = np.asarray([5, 5], dtype=int)
        self.goal = np.asarray([9, 5], dtype=int)
        self.distractor = np.asarray([1, 5], dtype=int)
        self.target = int(self.rng.integers(2))
        self.orientation = 0
        self.gravity = int(self.rng.choice((-1, 1)))
        self.delay = int(self.rng.integers(3, 9))
        if self.split == "heldout":
            self.delay = int(self.rng.choice((10, 14, 20)))
        self.motor_instructions = self.rng.integers(0, 4, size=8)
        self.motor_correct = 0
        self.food_endpoint = int(self.rng.choice((-1, 1)))
        self.reliabilities = np.asarray([0.9, 0.6])
        if self.task == "foraging" or self.probe == "goal_detection":
            # Training goal positions exclude both corners and the central row;
            # held-out positions and reliability combinations are outside training.
            if self.split == "heldout":
                positions = [(1, 1), (1, 9), (9, 1), (9, 9), (2, 5), (8, 5)]
                self.reliabilities = np.asarray(self.rng.choice(((1.0, 0.5), (0.75, 0.55))))
            else:
                positions = [(x, y) for x in (2, 4, 6, 8) for y in (2, 4, 6, 8)]
                self.reliabilities = np.asarray([self.rng.uniform(0.8, 0.95), self.rng.uniform(0.55, 0.7)])
            chosen = self.rng.choice(len(positions), size=2, replace=False)
            self.goal, self.distractor = (np.asarray(positions[int(i)], dtype=int) for i in chosen)
            # Each cue has its declared probability of locating the true goal.
            self.cue_positions = np.stack([self.goal if self.rng.random() < r else self.distractor for r in self.reliabilities])
            # Cue combinations swapped at holdout; reliability remains explicitly visible.
            permutation = [1, 0] if self.split == "heldout" else self.rng.permutation(2)
            self.cue_positions = self.cue_positions[permutation]
            self.reliabilities = self.reliabilities[permutation]
            yy, xx = np.mgrid[0:11, 0:11]
            self.cue_fields = np.stack([np.exp(-((xx-p[0])**2+(yy-p[1])**2)/8) for p in self.cue_positions])
            if self.probe == "goal_detection":
                # Independent, noiseless visible-target probe; not a movement-rate proxy.
                self.cue_positions[0] = self.goal
                self.reliabilities[0] = 1.0
        elif self.task == "corridor":
            # Food and gravity are independent. Heldout swaps the arena axis.
            self.orientation = 1 if self.split == "heldout" else 0
            axis = self.orientation
            self.goal[axis] = 10 if self.food_endpoint > 0 else 0
            self.goal[1-axis] = 5
            self.distractor = self.goal.copy()
            self.distractor[axis] = 10-self.goal[axis]
        self.done = False
        return self.observe()

    def observe(self):
        obs = np.zeros(OBS_DIM, dtype=np.float32)
        obs[TASKS.index(self.task)] = 1
        if self.probe:
            obs[3+PROBES.index(self.probe)] = 1
        obs[6:8] = (self.position-5)/5
        obs[8] = self.t/self.max_steps
        obs[9] = 1
        if self.probe == "motor_execution":
            obs[24+int(self.motor_instructions[min(self.t, 7)])] = 1
            return obs
        if self.probe == "delayed_cue" or (self.task == "delayed_choice" and self.probe is None):
            if self.t == 0:
                obs[10+self.target] = 1
            if self.t > self.delay:
                obs[12] = 1  # choice is available; the old cue is absent
            obs[13] = min(self.t, self.delay+1)/21
            return obs
        if self.task == "foraging" or self.probe == "goal_detection":
            obs[14:18] = ((self.cue_positions-self.position)/10).flatten()
            obs[18:20] = self.reliabilities
            obs[20:22] = self.cue_fields[:, self.position[1], self.position[0]]
        else:
            obs[14:16] = (self.goal-self.position)/10
            obs[22] = float(self.orientation)
            obs[23] = self.gravity
        return obs

    def step(self, action: int):
        if self.done:
            raise RuntimeError("Reset a terminal environment before stepping.")
        if action not in range(ACTION_DIM):
            raise ValueError("Action must be an integer in [0, 4].")
        reward, success, redirected = -0.005, False, False
        if self.probe == "motor_execution":
            correct = action == int(self.motor_instructions[self.t])
            self.motor_correct += int(correct)
            reward = float(correct)
            self.done = self.t == 7
            success = self.motor_correct == 8
        elif self.probe == "delayed_cue" or (self.task == "delayed_choice" and self.probe is None):
            if self.t > self.delay and action in (2, 3):
                choice = int(action == 3)
                success = choice == self.target
                redirected = choice != self.target
                reward = 1.0 if success else -1.0
                self.done = True
            elif self.t <= self.delay:
                reward = 0.0
        else:
            old_distance = np.abs(self.goal-self.position).sum()
            if self.task == "corridor" and self.probe is None:
                axis = self.orientation
                delta = ACTIONS[action, axis]
                # Stochastic environmental drift acts independently of the policy.
                if self.rng.random() < 0.12:
                    delta += self.gravity
                self.position[axis] = np.clip(self.position[axis]+delta, 0, 10)
                spatial_endpoint = self.position[axis] == (10 if self.gravity > 0 else 0)
                redirected = bool(spatial_endpoint)
            else:
                self.position = np.clip(self.position+ACTIONS[action], 0, 10)
            new_distance = np.abs(self.goal-self.position).sum()
            # Potential shaping provides a learnable small environment; terminal
            # reward still depends on the actual goal, never the redirection label.
            reward += 0.025*(old_distance-new_distance)
            success = bool(np.array_equal(self.position, self.goal))
            distractor = bool(np.array_equal(self.position, self.distractor))
            if self.task != "corridor" or self.probe:
                redirected = distractor
            if success or distractor:
                reward += 1.0 if success else -0.5
                self.done = True
        self.t += 1
        self.done = self.done or self.t >= self.max_steps
        info = {"success": bool(success), "redirection": bool(redirected), "steps": self.t,
                "motor_accuracy": self.motor_correct/max(self.t, 1), "truncated": self.t >= self.max_steps}
        return self.observe(), float(reward), self.done, info


def make_policy(architecture: str, capacity: int = 100_000, window: int = 8):
    torch = _torch()
    nn = torch.nn
    if architecture not in (*ARCHITECTURES, "feedforward"):
        raise ValueError(f"Unknown architecture {architecture}")

    class Policy(nn.Module):
        def __init__(self, width):
            super().__init__()
            self.architecture, self.width, self.window = architecture, width, window
            if architecture == "gru":
                self.core = nn.GRUCell(OBS_DIM, width)
                self.state_dim = width
            elif architecture == "rnn":
                self.core = nn.RNNCell(OBS_DIM, width, nonlinearity="tanh")
                self.state_dim = width
            elif architecture == "modular":
                self.sensory = nn.Linear(OBS_DIM+width, width)
                self.memory = nn.Linear(OBS_DIM+width, width)
                self.cross_sm = nn.Linear(width, width, bias=False)
                self.cross_ms = nn.Linear(width, width, bias=False)
                self.state_dim = 2*width
            else:
                self.core = nn.Sequential(nn.Linear(OBS_DIM*window, width), nn.Tanh(), nn.Linear(width, width), nn.Tanh())
                self.state_dim = OBS_DIM*window
            hidden = self.state_dim if architecture != "feedforward" else width
            self.actor, self.critic = nn.Linear(hidden, ACTION_DIM), nn.Linear(hidden, 1)

        def initial_state(self, batch_size, device=None):
            return torch.zeros(batch_size, self.state_dim, device=device or next(self.parameters()).device)

        def forward(self, obs, state, perturbation=None):
            incoming = obs
            if perturbation is not None and perturbation.mechanism == "input_gain":
                incoming = obs*(1+perturbation.vector(OBS_DIM, obs.device))
            if architecture in ("rnn", "gru"):
                latent = self.core(incoming, state)
            elif architecture == "modular":
                sensory, memory = state.chunk(2, -1)
                latent = torch.cat((torch.tanh(self.sensory(torch.cat((incoming, sensory), -1))+self.cross_ms(memory)),
                                    torch.tanh(self.memory(torch.cat((incoming, memory), -1))+self.cross_sm(sensory))), -1)
            else:
                new_state = torch.cat((state[:, OBS_DIM:], incoming), -1)
                latent = self.core(new_state)
            if perturbation is not None and perturbation.mechanism in ("recurrent_gate", "random_subspace"):
                recurrent_state = state if architecture != "feedforward" else self.core(state)
                latent = latent+perturbation.recurrent(recurrent_state)
            if perturbation is not None and perturbation.mechanism == "hidden_displacement":
                latent = latent+perturbation.vector(latent.shape[-1], obs.device)
            logits, value = self.actor(latent), self.critic(latent).squeeze(-1)
            if perturbation is not None and perturbation.mechanism == "output_gain":
                # Gain of each action channel; the stay channel is available, so
                # this includes movement amplification rather than only turning.
                logits = logits*(1+perturbation.output_gain(latent, ACTION_DIM))
            if perturbation is not None and perturbation.mechanism == "motor_gain":
                # One scalar globally amplifies movement relative to staying.
                movement = torch.as_tensor([1., 1., 1., 1., 0.], device=obs.device)/2
                if perturbation.random_restoration:
                    rng = np.random.default_rng(perturbation.seed+104729)
                    correction = rng.normal(size=ACTION_DIM)
                    correction /= np.linalg.norm(correction)
                    movement = movement-torch.as_tensor(correction, dtype=torch.float32, device=obs.device)
                logits = logits+float(perturbation.theta[0])*movement
            return logits, value, latent if architecture != "feedforward" else new_state

    # Count, rather than guess, parameters. Search chooses the nearest attainable
    # budget for every architecture; all compared networks must be within 5%.
    best_width, best_difference = None, math.inf
    for width in range(8, 513):
        if architecture == "rnn":
            count = width*width+(OBS_DIM+2+ACTION_DIM+1)*width+ACTION_DIM+1
        elif architecture == "gru":
            count = 3*width*width+(3*OBS_DIM+6+ACTION_DIM+1)*width+ACTION_DIM+1
        elif architecture == "modular":
            count = 4*width*width+(2*OBS_DIM+2+2*(ACTION_DIM+1))*width+ACTION_DIM+1
        else:
            count = width*width+(OBS_DIM*window+2+ACTION_DIM+1)*width+ACTION_DIM+1
        difference = abs(count-capacity)
        if difference < best_difference:
            best_width, best_difference = width, difference
    policy = Policy(best_width)
    count = sum(p.numel() for p in policy.parameters())
    if abs(count-capacity)/capacity > 0.05:
        raise ValueError(f"Cannot match architecture {architecture} to capacity {capacity}: {count}")
    policy.parameter_count = count
    return policy


class Perturbation:
    """Exactly rank adjustable coefficients in a fixed, normalized subspace.

    Low-rank transition matrices have rank at most r. The other mechanisms have
    r adjustable coefficients too, with immutable bases and an equal episode cap.
    Norms are the norm of the actual intervention, not the coefficient count.
    """

    def __init__(self, mechanism: str, rank: int, theta, seed: int = 419, random_restoration: bool = False):
        if mechanism not in MECHANISMS or rank not in (1, 2, 4, 8):
            raise ValueError("Unknown mechanism or primary rank")
        self.mechanism, self.rank = mechanism, rank
        self.theta = np.asarray(theta, dtype=np.float64)
        if self.theta.shape != (rank,) or not np.isfinite(self.theta).all():
            raise ValueError("Perturbation coefficients must be finite and rank-sized")
        self.seed = seed+7919 if mechanism == "random_subspace" else seed
        self.random_restoration, self.cache = random_restoration, {}

    def _control(self, offset):
        key = ("control", offset)
        if key not in self.cache:
            self.cache[key] = Perturbation(self.mechanism, self.rank, self.theta, self.seed+offset)
        return self.cache[key]

    def _basis(self, dim, device):
        torch = _torch()
        key = (dim, str(device))
        if key not in self.cache:
            rng = np.random.default_rng(self.seed+dim*31)
            # QR creates orthonormal directions where r<=dim; otherwise retain
            # unit vectors and record the attainable rank in metadata.
            raw = rng.normal(size=(dim, self.rank))
            basis = np.linalg.qr(raw, mode="reduced")[0] if dim >= self.rank else raw/np.linalg.norm(raw, axis=0)
            self.cache[key] = torch.as_tensor(basis, dtype=torch.float32, device=device)
        return self.cache[key]

    def vector(self, dim, device):
        torch = _torch()
        value = self._basis(dim, device)@torch.as_tensor(self.theta, dtype=torch.float32, device=device)
        if self.random_restoration:
            other = self._control(104729)
            correction = other.vector(dim, device)
            value = value-correction*(value.norm()/correction.norm().clamp_min(1e-12))
        return value

    def recurrent(self, state):
        torch = _torch()
        dim = state.shape[-1]
        left = self._basis(dim, state.device)
        right = self._control(271)._basis(dim, state.device)
        coefficient = torch.as_tensor(self.theta, dtype=torch.float32, device=state.device)
        value = ((state@right)*coefficient)@left.T
        if self.random_restoration:
            control = self._control(104729)
            value = value-control.recurrent(state)
        return value

    def output_gain(self, latent, action_dim):
        torch = _torch()
        right = self._basis(latent.shape[-1], latent.device)
        key = ("output_left", action_dim, str(latent.device))
        if key not in self.cache:
            rng = np.random.default_rng(self.seed+action_dim*997)
            left_np = rng.normal(size=(action_dim, self.rank))
            left_np /= np.linalg.norm(left_np, axis=0)
            self.cache[key] = torch.as_tensor(left_np, dtype=torch.float32, device=latent.device)
        left = self.cache[key]
        coefficient = torch.as_tensor(self.theta, dtype=torch.float32, device=latent.device)
        value = ((latent@right)*coefficient)@left.T
        if self.random_restoration:
            control = self._control(104729)
            value = value-control.output_gain(latent, action_dim)
        return value


def _perturbed_policy(policy, perturbation):
    """Unstructured controls change a copy; the original state is never altered."""
    if perturbation is None or perturbation.mechanism != "unstructured":
        return policy, perturbation
    torch = _torch()
    alternative = copy.deepcopy(policy)
    with torch.no_grad():
        flat = torch.cat([p.detach().flatten() for p in policy.parameters()])
        delta = perturbation.vector(flat.numel(), flat.device)
        offset = 0
        for parameter in alternative.parameters():
            parameter.add_(delta[offset:offset+parameter.numel()].view_as(parameter))
            offset += parameter.numel()
    alternative.requires_grad_(False)
    return alternative, None


def _state_dict_hash(state_dict):
    h = hashlib.sha256()
    for name, value in state_dict.items():
        h.update(name.encode())
        h.update(value.detach().cpu().contiguous().numpy().tobytes())
    return h.hexdigest()


def state_hash(policy):
    return _state_dict_hash(policy.state_dict())


def _sample_action(logits, rng, deterministic=False):
    torch = _torch()
    probabilities = torch.softmax(logits, -1).detach().cpu().numpy()
    return np.argmax(probabilities, axis=-1) if deterministic else np.asarray([rng.choice(ACTION_DIM, p=p/p.sum()) for p in probabilities])


def evaluate(policy, task, seeds, split="heldout", perturbation=None, probe=None, cue_removal=False, deterministic=False, capture=False):
    torch = _torch()
    before = state_hash(policy)
    active_policy, active_perturbation = _perturbed_policy(policy, perturbation)
    active_policy.eval()
    device = next(policy.parameters()).device
    environments = [Environment(task, int(s), split, probe) for s in seeds]
    rng = np.random.default_rng(int(seeds[0])+9349)
    state = active_policy.initial_state(len(environments))
    obs = np.stack([e.observe() for e in environments])
    finished = np.zeros(len(environments), dtype=bool)
    rows, sequence = [], []
    steps = 0
    with torch.no_grad():
        for t in range(200):
            tensor = torch.as_tensor(obs, device=device)
            if cue_removal:
                tensor = tensor.clone()
                tensor[:, 10:12] = 0
                tensor[:, 14:22] = 0
            if capture:
                sequence.append((tensor.detach().cpu().clone(), state.detach().cpu().clone(), (~finished).copy()))
            logits, _, state = active_policy(tensor, state, active_perturbation)
            actions = _sample_action(logits, rng, deterministic)
            for i, environment in enumerate(environments):
                if finished[i]:
                    continue
                obs[i], _, done, info = environment.step(int(actions[i]))
                steps += 1
                if done:
                    finished[i] = True
                    rows.append({"episode_seed": int(seeds[i]), "success": int(info["success"]), "redirection": int(info["redirection"]),
                                 "steps": info["steps"], "motor_accuracy": info["motor_accuracy"], "truncated": info["truncated"]})
            if finished.all():
                break
    if before != state_hash(policy):
        raise AssertionError("Base-policy weights changed during evaluation")
    return rows, {"environment_steps": steps, "weight_sha256": before, "sequence": sequence}


def _checkpoint(path, policy, optimizer, steps, config, seed, task, learning_rate, budget, observation, states, environments, serials, rollout_rng, metrics):
    torch = _torch()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    torch.save({"state_dict": policy.state_dict(), "optimizer": optimizer.state_dict(), "steps": steps, "seed": seed,
                "architecture": policy.architecture, "parameter_count": policy.parameter_count, "config": config,
                "config_sha256": _digest(config), "implementation_sha256": _implementation_sha256(),
                "weight_sha256": state_hash(policy), "task": task, "learning_rate": float(learning_rate), "training_budget": budget,
                "rng_python": random.getstate(), "rng_numpy": np.random.get_state(), "rng_torch": torch.get_rng_state(),
                "rng_cuda": torch.cuda.get_rng_state_all() if torch.cuda.is_available() else None,
                "observation": observation, "recurrent_state": states.detach().cpu(), "environments": environments,
                "environment_serials": serials, "training_metrics": metrics,
                "rollout_rng": rollout_rng.bit_generator.state}, temporary)
    temporary.replace(path)


def _verify_checkpoint(checkpoint, config, *, seed=None, architecture=None, task=None, learning_rate=None, complete=False):
    if checkpoint.get("config_sha256") != _digest(config) or checkpoint.get("config") != config:
        raise RuntimeError("Checkpoint configuration provenance differs from this request")
    if checkpoint.get("implementation_sha256") != _implementation_sha256():
        raise RuntimeError("Checkpoint implementation changed; use a new prospective output namespace")
    if checkpoint.get("weight_sha256") != _state_dict_hash(checkpoint["state_dict"]):
        raise RuntimeError("Checkpoint model-weight hash mismatch")
    expected = {"seed": seed, "architecture": architecture, "task": task, "learning_rate": learning_rate}
    for key, value in expected.items():
        if value is not None and checkpoint.get(key) != value:
            raise RuntimeError(f"Checkpoint {key} provenance differs from this request")
    steps = checkpoint.get("steps", -1)
    if not isinstance(steps, int) or steps < 0:
        raise RuntimeError("Invalid checkpoint step count")
    if complete and (steps != int(config.get("total_steps", 1000000)) or checkpoint.get("training_budget") != steps):
        raise RuntimeError("Final/development checkpoint has not completed the declared training budget")


def train(config, task, architecture, seed, destination, learning_rate, steps=None):
    torch = _torch()
    seed_everything(seed)
    torch.set_num_threads(int(config.get("torch_threads", 1)))
    device = torch.device(config.get("device", "cuda" if torch.cuda.is_available() else "cpu"))
    policy = make_policy(architecture, int(config.get("capacity", 100000)), int(config.get("observation_window", 8))).to(device)
    optimizer = torch.optim.Adam(policy.parameters(), lr=float(learning_rate), eps=1e-5)
    budget = int(steps if steps is not None else config.get("total_steps", 1000000))
    n_envs = int(config.get("n_envs", 16))
    if budget <= 0 or budget % n_envs:
        raise ValueError("Training budget must be a positive multiple of vector environment count")
    if budget > int(config.get("total_steps", 1000000)):
        raise ValueError("Requested training exceeds the declared total-step budget")
    rollout_length = int(config.get("rollout_steps", 128))
    rng = np.random.default_rng(seed+9181)
    # Common independent competence-probe curriculum (25%) for every architecture
    # and main task. Held-out probe episodes never enter training.
    def new_environment(index, serial):
        probe = PROBES[index % 3] if index % 4 == 0 else None
        return Environment(task, seed*1000003+index*1009+serial*9176, "train", probe)
    serials = np.zeros(n_envs, dtype=int)
    environments = [new_environment(i, 0) for i in range(n_envs)]
    observation = np.stack([env.observe() for env in environments])
    states = policy.initial_state(n_envs)
    metrics, episode_returns = [], np.zeros(n_envs)
    completed_episodes, completed_success = 0, 0
    total, started, checkpoint_at = 0, time.perf_counter(), int(config.get("checkpoint_steps", 50000))
    previous_elapsed = 0.0
    saved_path = destination/"checkpoint.pt"
    if saved_path.exists():
        saved = torch.load(saved_path, map_location=device, weights_only=False)
        _verify_checkpoint(saved, config, seed=seed, architecture=architecture, task=task, learning_rate=float(learning_rate))
        policy.load_state_dict(saved["state_dict"])
        optimizer.load_state_dict(saved["optimizer"])
        total = saved["steps"]
        if total > budget:
            raise RuntimeError("Checkpoint has already exceeded the requested training budget")
        random.setstate(saved["rng_python"])
        np.random.set_state(saved["rng_numpy"])
        torch.set_rng_state(saved["rng_torch"].cpu())
        if saved["rng_cuda"] is not None and torch.cuda.is_available():
            torch.cuda.set_rng_state_all(saved["rng_cuda"])
        observation, states = saved["observation"], saved["recurrent_state"].to(device)
        environments, serials = saved["environments"], saved["environment_serials"]
        metrics = saved["training_metrics"]
        rng.bit_generator.state = saved["rollout_rng"]
        previous_elapsed = metrics[-1]["elapsed_seconds"] if metrics else 0.0
        checkpoint_at = total+int(config.get("checkpoint_steps", 50000))
        completed_episodes = metrics[-1]["completed_episodes"] if metrics else 0
        completed_success = round(metrics[-1]["training_success"]*completed_episodes) if metrics else 0
    gamma, gae_lambda = float(config.get("gamma", .99)), float(config.get("gae_lambda", .95))
    while total < budget:
        initial_state = states.detach().clone()
        buffers = {k: [] for k in ("obs", "action", "reward", "done", "value", "logprob")}
        length = min(rollout_length, (budget-total)//n_envs)
        with torch.no_grad():
            for _ in range(length):
                obs_tensor = torch.as_tensor(observation, device=device).clone()
                logits, value, states = policy(obs_tensor, states)
                actions = _sample_action(logits, rng)
                action_tensor = torch.as_tensor(actions, dtype=torch.long, device=device)
                logprob = torch.distributions.Categorical(logits=logits).log_prob(action_tensor)
                reward, done = np.zeros(n_envs, dtype=np.float32), np.zeros(n_envs, dtype=np.float32)
                for i, env in enumerate(environments):
                    observation[i], reward[i], finished, info = env.step(int(actions[i]))
                    episode_returns[i] += reward[i]
                    if finished:
                        completed_episodes += 1
                        completed_success += int(info["success"])
                        done[i] = 1
                        serials[i] += 1
                        environments[i] = new_environment(i, int(serials[i]))
                        observation[i] = environments[i].observe()
                        episode_returns[i] = 0
                buffers["obs"].append(obs_tensor)
                buffers["action"].append(action_tensor)
                buffers["reward"].append(torch.as_tensor(reward, device=device))
                buffers["done"].append(torch.as_tensor(done, device=device))
                buffers["value"].append(value)
                buffers["logprob"].append(logprob)
                states *= (1-torch.as_tensor(done, device=device))[:, None]
                total += n_envs
            _, bootstrap, _ = policy(torch.as_tensor(observation, device=device), states)
        batch = {k: torch.stack(v) for k, v in buffers.items()}
        advantages = torch.zeros_like(batch["reward"])
        carry = torch.zeros(n_envs, device=device)
        for t in reversed(range(length)):
            next_value = bootstrap if t == length-1 else batch["value"][t+1]
            live = 1-batch["done"][t]
            delta = batch["reward"][t]+gamma*next_value*live-batch["value"][t]
            carry = delta+gamma*gae_lambda*live*carry
            advantages[t] = carry
        returns = advantages+batch["value"]
        advantages = (advantages-advantages.mean())/(advantages.std(unbiased=False)+1e-8)
        loss_terms = None
        # Recompute entire ordered segments for recurrent PPO. Hidden states are
        # never shuffled across time; episode boundaries reset the recurrence.
        for _ in range(int(config.get("ppo_epochs", 4))):
            state = initial_state
            new_logprobs, values, entropies = [], [], []
            for t in range(length):
                logits, value, state = policy(batch["obs"][t], state)
                distribution = torch.distributions.Categorical(logits=logits)
                new_logprobs.append(distribution.log_prob(batch["action"][t]))
                values.append(value)
                entropies.append(distribution.entropy())
                state = state*(1-batch["done"][t])[:, None]
            new_logprobs, values, entropy = torch.stack(new_logprobs), torch.stack(values), torch.stack(entropies).mean()
            ratio = torch.exp(new_logprobs-batch["logprob"])
            clip = float(config.get("clip_epsilon", .2))
            policy_loss = -torch.minimum(ratio*advantages, ratio.clamp(1-clip, 1+clip)*advantages).mean()
            value_loss = (values-returns).square().mean()/2
            loss = policy_loss+float(config.get("value_coefficient", .5))*value_loss-float(config.get("entropy_coefficient", .01))*entropy
            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(policy.parameters(), float(config.get("gradient_clip", .5)))
            optimizer.step()
            loss_terms = {"policy_loss": float(policy_loss.detach()), "value_loss": float(value_loss.detach()), "entropy": float(entropy.detach())}
        # Update carry state under the *new* weights. Carrying the old rollout's
        # state into a newly updated policy would introduce unnecessary hidden-
        # state mismatch. The original segment start remains the truncated-BPTT
        # boundary; every terminal is reset again during this ordered replay.
        with torch.no_grad():
            states = initial_state
            for t in range(length):
                _, _, states = policy(batch["obs"][t], states)
                states = states*(1-batch["done"][t])[:, None]
            states = states.detach()
        elapsed = previous_elapsed+time.perf_counter()-started
        metrics.append({"steps": total, "elapsed_seconds": elapsed, "steps_per_second": total/elapsed,
                        "completed_episodes": completed_episodes, "training_success": completed_success/max(1, completed_episodes), **loss_terms})
        if total >= checkpoint_at:
            _checkpoint(destination/"checkpoint.pt", policy, optimizer, total, config, seed, task, learning_rate, budget, observation, states, environments, serials, rng, metrics)
            checkpoint_at += int(config.get("checkpoint_steps", 50000))
    _checkpoint(destination/"checkpoint.pt", policy, optimizer, total, config, seed, task, learning_rate, budget, observation, states, environments, serials, rng, metrics)
    _json(destination/"training_metrics.json", metrics)
    policy.requires_grad_(False)
    policy.eval()
    seeds = list(range(20_000_000+seed*1000, 20_000_000+seed*1000+int(config.get("baseline_episodes", 32))))
    baseline, meta = evaluate(policy, task, seeds, split="validation")
    competence = {}
    for probe in PROBES:
        rows, probe_meta = evaluate(policy, task, seeds, split="validation", probe=probe)
        competence[probe] = float(np.mean([r["motor_accuracy"] if probe == "motor_execution" else r["success"] for r in rows]))
        meta["environment_steps"] += probe_meta["environment_steps"]
    summary = {"seed": seed, "architecture": architecture, "task": task, "learning_rate": learning_rate,
               "parameter_count": policy.parameter_count, "training_steps": total, "seconds": time.perf_counter()-started,
               "training_seconds": metrics[-1]["elapsed_seconds"], "steps_per_second": metrics[-1]["steps_per_second"],
               "baseline_success": float(np.mean([r["success"] for r in baseline])), "competence": competence,
               "baseline_evaluation_steps": meta["environment_steps"], "weight_sha256": state_hash(policy),
               "config_sha256": _digest(config), "torch_version": torch.__version__, "device": str(device),
               "implementation_sha256": _implementation_sha256(),
               "checkpoint_sha256": hashlib.sha256((destination/"checkpoint.pt").read_bytes()).hexdigest(),
               "gpu": torch.cuda.get_device_name(device) if device.type == "cuda" else None,
               "peak_gpu_bytes": torch.cuda.max_memory_allocated(device) if device.type == "cuda" else None,
               "complete_training_budget": total >= int(config.get("total_steps", 1000000))}
    _json(destination/"summary.json", summary)
    return policy, summary


def _clean_kl(policy, perturbation, sequences):
    torch = _torch()
    active, p = _perturbed_policy(policy, perturbation)
    device = next(policy.parameters()).device
    values = []
    # Replay clean observations using independently propagated intervention states;
    # this is policy divergence, not redirection calibration on heldout endpoints.
    state = active.initial_state(sequences[0][0].shape[0])
    with torch.no_grad():
        for obs, clean_state, mask in sequences:
            obs = obs.to(device)
            clean_logits, _, _ = policy(obs, clean_state.to(device))
            changed_logits, _, state = active(obs, state, p)
            log_p, log_q = torch.log_softmax(clean_logits, -1), torch.log_softmax(changed_logits, -1)
            divergence = (log_p.exp()*(log_p-log_q)).sum(-1)
            if mask.any():
                values.extend(divergence[torch.as_tensor(mask, device=device)].cpu().tolist())
    return max(float(np.mean(values)), 0.0)


def _normalize(theta):
    value = np.asarray(theta, dtype=np.float64)
    return value/(np.linalg.norm(value)+1e-12)


def calibrate_rule(policy, task, config, seed):
    """Synthetic development-only derivative-free rule; base weights stay frozen."""
    budget = int(config.get("calibration_episodes", 32))
    if budget < 8:
        raise ValueError("At least eight calibration episodes are needed for directional selection")
    directions = {}
    rng = np.random.default_rng(seed+1729)
    for mechanism in MECHANISMS:
        for rank in config.get("ranks", [1, 2, 4, 8]):
            if mechanism == "motor_gain" and rank != 1:
                continue
            # Every cell gets exactly the same number of episodes. Four proposals
            # use common-random-number environments, and no validation/test seed.
            candidates = [_normalize(rng.normal(size=rank)) for _ in range(4)]
            per_candidate = budget//4
            if budget % 4:
                raise ValueError("Calibration episode cap must be divisible by four")
            seeds = list(range(30_000_000+seed*100, 30_000_000+seed*100+per_candidate))
            scored = []
            for candidate in candidates:
                rows, _ = evaluate(policy, task, seeds, split="calibration", perturbation=Perturbation(mechanism, rank, candidate*.4))
                score = np.mean([r["redirection"] for r in rows])
                scored.append(float(score))
            # Random and unstructured controls have equal selection opportunity.
            selected = int(np.argmax(scored))
            directions[f"{mechanism}:{rank}"] = {"theta": candidates[selected].tolist(), "selection_redirection": scored[selected],
                                                     "calibration_episodes": budget, "base_weight_sha256": state_hash(policy)}
    return directions


def _load_biological_directions(config, root):
    specified = config.get("biological_direction_file")
    if not specified:
        return None, "pending: a validated behavioural-fit export has not been configured"
    path = Path(specified)
    if not path.is_absolute():
        path = root/path
    if not path.exists():
        return None, f"pending: biological parameter export does not exist: {path}"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("evidence_status") != "validated" or not payload.get("source_sha256") or not payload.get("training_splits"):
        raise ValueError("Biological directions require validated source hashes and training-only split provenance")
    classes = payload.get("classes", {})
    if not all(k in classes and math.isfinite(float(classes[k])) for k in MECHANISMS[:4]):
        raise ValueError("Biological export must specify finite, signed values for the four primary mechanism classes")
    return payload, "validated fit export; computational mapping remains a model assumption"


def _restoration_check(policy, task, seeds, perturbation):
    torch = _torch()
    device = next(policy.parameters()).device
    environments = [Environment(task, s, "calibration") for s in seeds]
    obs = torch.as_tensor(np.stack([e.observe() for e in environments]), device=device)
    state = policy.initial_state(len(seeds))
    state[:] = torch.sin(torch.arange(state.numel(), device=device).reshape_as(state))*.1
    snapshot = {k: v.detach().clone() for k, v in policy.state_dict().items()}
    with torch.no_grad():
        original = torch.softmax(policy(obs, state)[0], -1)
        active, p = _perturbed_policy(policy, perturbation)
        altered = torch.softmax(active(obs, state, p)[0], -1)
        # Restore the intervention to exactly zero, holding observation, state,
        # RNG-independent probabilities, and all original weights constant.
        restored = torch.softmax(policy(obs, state, None)[0], -1)
    maximum = float((original-restored).abs().max())
    unchanged = all(torch.equal(v, policy.state_dict()[k]) for k, v in snapshot.items())
    if maximum > 1e-7 or not unchanged:
        raise AssertionError("Agent restoration did not recover original action probabilities and weights")
    return {"restoration_max_probability_error": maximum, "altered_max_probability_change": float((original-altered).abs().max()),
            "base_weights_unchanged": unchanged}


def perturbation_experiment(policy, task, seed, config, protocol, destination):
    torch = _torch()
    started = time.perf_counter()
    policy.requires_grad_(False)
    before = state_hash(policy)
    n = int(config.get("evaluation_episodes", 64))
    evaluation_seeds = list(range(40_000_000+seed*1000, 40_000_000+seed*1000+n))
    calibration_seeds = list(range(50_000_000+seed*1000, 50_000_000+seed*1000+int(config.get("calibration_episodes", 32))))
    _, clean_meta = evaluate(policy, task, calibration_seeds, split="calibration", capture=True)
    baseline = {}
    all_rows, evaluation_steps = [], clean_meta["environment_steps"]
    for probe in (None, *PROBES):
        rows, meta = evaluate(policy, task, evaluation_seeds, probe=probe)
        evaluation_steps += meta["environment_steps"]
        baseline[probe] = rows
    baseline_competence = {probe: float(np.mean([r["motor_accuracy"] if probe == "motor_execution" else r["success"] for r in baseline[probe]])) for probe in PROBES}
    competence_qualified = all(value >= float(config.get("baseline_probe_accuracy_min", .8)) for value in baseline_competence.values())
    biological = protocol.get("biological_directions")
    rule_types = ("synthetic", "biological") if biological else ("synthetic",)
    strengths = config.get("strengths", [0, .05, .1, .2, .4, .8])
    protocol_hash = _digest(protocol)
    restoration = []
    for rule_type in rule_types:
        for mechanism in MECHANISMS:
            for rank in config.get("ranks", [1, 2, 4, 8]):
                if mechanism == "motor_gain" and rank != 1:
                    continue
                theta = np.asarray(protocol["directions"][f"{mechanism}:{rank}"])
                if rule_type == "biological" and mechanism in MECHANISMS[:4]:
                    theta = _normalize(np.ones(rank))*float(biological["classes"][mechanism])
                for matching_arm in ("parameter_norm", "policy_kl"):
                    for strength in strengths:
                        multiplier = float(strength)
                        target_kl = float(strength)**2/2
                        if matching_arm == "policy_kl" and strength > 0:
                            lo, hi = 0.0, 8.0
                            for _ in range(int(config.get("kl_bisections", 10))):
                                mid = (lo+hi)/2
                                achieved = _clean_kl(policy, Perturbation(mechanism, rank, theta*mid), clean_meta["sequence"])
                                if achieved < target_kl:
                                    lo = mid
                                else:
                                    hi = mid
                            multiplier = (lo+hi)/2
                        perturbation = Perturbation(mechanism, rank, theta*multiplier)
                        achieved_kl = _clean_kl(policy, perturbation, clean_meta["sequence"])
                        restoration.append({"mechanism": mechanism, "rank": rank, "strength": strength, "matching_arm": matching_arm,
                                            **_restoration_check(policy, task, calibration_seeds[:4], perturbation)})
                        # Every primary strength includes full competence probes,
                        # not merely a benign rate of emitting actions.
                        for probe in (None, *PROBES):
                            rows, meta = evaluate(policy, task, evaluation_seeds, perturbation=perturbation, probe=probe)
                            evaluation_steps += meta["environment_steps"]
                            for row, clean in zip(sorted(rows, key=lambda r: r["episode_seed"]), sorted(baseline[probe], key=lambda r: r["episode_seed"])):
                                all_rows.append({"training_seed": seed, "architecture": policy.architecture, "task": task,
                                                 "perturbation": mechanism, "rank": rank, "effective_coefficients": rank,
                                                 "rule_type": rule_type, "strength": strength, "matching_arm": matching_arm,
                                                 "achieved_parameter_norm": float(np.linalg.norm(theta*multiplier)), "clean_policy_kl": achieved_kl,
                                                 "norm_match_within_10pct": abs(float(np.linalg.norm(theta*multiplier))-float(strength)) <= max(1e-6, float(strength)*.1),
                                                 "target_policy_kl": target_kl, "functional_match_within_10pct": abs(achieved_kl-target_kl) <= max(1e-6, target_kl*.1),
                                                 "probe": probe or "main", "heldout_condition": True, "restoration_control": "none",
                                                 "baseline_success": clean["success"], "baseline_redirection": clean["redirection"],
                                                 "baseline_motor_accuracy": clean["motor_accuracy"], "protocol_sha256": protocol_hash, **row})
                        # Restoration controls at every strength: exactly restored,
                        # equal-norm random correction, cue removal, and lower noise.
                        for control in ("exact", "random_subspace", "cue_removal", "lower_action_noise"):
                            active = None if control == "exact" else perturbation
                            if control == "random_subspace":
                                active = Perturbation(mechanism, rank, theta*multiplier, random_restoration=True)
                            rows, meta = evaluate(policy, task, evaluation_seeds, perturbation=active,
                                                  cue_removal=control == "cue_removal", deterministic=control == "lower_action_noise")
                            evaluation_steps += meta["environment_steps"]
                            for row in rows:
                                all_rows.append({"training_seed": seed, "architecture": policy.architecture, "task": task,
                                                 "perturbation": mechanism, "rank": rank, "effective_coefficients": rank,
                                                 "rule_type": rule_type, "strength": strength, "matching_arm": matching_arm,
                                                 "achieved_parameter_norm": float(np.linalg.norm(theta*multiplier)), "clean_policy_kl": achieved_kl,
                                                 "target_policy_kl": target_kl, "functional_match_within_10pct": abs(achieved_kl-target_kl) <= max(1e-6, target_kl*.1),
                                                 "probe": "main", "heldout_condition": True, "restoration_control": control,
                                                 "protocol_sha256": protocol_hash, **row})
    if before != state_hash(policy):
        raise AssertionError("Base policy changed during the perturbation experiment")
    import pandas as pd
    destination.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(all_rows).to_parquet(destination/"agent_trials.parquet", index=False)
    _json(destination/"restoration_checks.json", restoration)
    summary = {"training_seed": seed, "task": task, "architecture": policy.architecture, "trial_rows": len(all_rows),
               "evaluation_environment_steps": evaluation_steps, "evaluation_seconds": time.perf_counter()-started,
               "base_weight_sha256": before, "base_weights_unchanged": True, "protocol_sha256": protocol_hash,
               "baseline_competence": baseline_competence, "baseline_competence_qualified": competence_qualified,
               "baseline_probe_accuracy_min": float(config.get("baseline_probe_accuracy_min", .8)),
               "trial_file_sha256": hashlib.sha256((destination/"agent_trials.parquet").read_bytes()).hexdigest(),
               "biological_direction_status": protocol.get("biological_direction_status"),
               "heldout_architecture": policy.architecture == protocol["heldout_architecture"],
               "heldout_task": task == protocol["heldout_task"]}
    _json(destination/"evaluation_summary.json", summary)
    return summary


def _directory(root, config):
    output = Path(config.get("output_dir", "results"))
    return (output if output.is_absolute() else root/output)/"agents"


def _cells(config, development=False, pilot=False):
    architectures = list(config.get("architectures", ARCHITECTURES))
    if config.get("include_feedforward", config.get("feedforward_baseline", False)) and "feedforward" not in architectures:
        architectures.append("feedforward")
    tasks = list(config.get("tasks", config.get("envs", TASKS)))
    if len(set(architectures)) != len(architectures) or any(a not in (*ARCHITECTURES, "feedforward") for a in architectures):
        raise ValueError("Architecture cells must be unique and use declared policy classes")
    if len(set(tasks)) != len(tasks) or any(t not in TASKS for t in tasks):
        raise ValueError("Task cells must be unique and use declared environments")
    all_seed_groups = {name: list(config.get(name, default)) for name, default in
                       (("development_seeds", range(100, 105)), ("final_seeds", range(1000, 1020)), ("pilot_seeds", [900, 901]))}
    for name, group in all_seed_groups.items():
        if not group or any(not isinstance(s, int) or isinstance(s, bool) or s < 0 for s in group) or len(set(group)) != len(group):
            raise ValueError(f"{name} must contain unique, nonnegative integer replicates")
    names = list(all_seed_groups)
    if any(set(all_seed_groups[names[i]]) & set(all_seed_groups[names[j]]) for i in range(3) for j in range(i+1, 3)):
        raise ValueError("Pilot, development and final seed groups must be disjoint")
    seeds = list(config.get("development_seeds" if development else "final_seeds", range(100, 105) if development else range(1000, 1020)))
    grid = list(config.get("lr_grid", [3e-4, 1e-3, 3e-3]))
    if not grid or len(set(grid)) != len(grid) or any(not math.isfinite(float(r)) or float(r) <= 0 for r in grid):
        raise ValueError("Learning-rate grid must contain unique positive finite values")
    if pilot:
        pilot_seeds = list(config.get("pilot_seeds", [900, 901]))
        if len(pilot_seeds) != 2:
            raise ValueError("The benchmark requires exactly two separate pilot seeds")
        return [(tasks[0], architectures[0], int(s), float(config.get("lr_grid", [3e-4])[0])) for s in pilot_seeds]
    rates = grid if development else [None]
    return [(task, arch, int(seed), rate) for arch in architectures for task in tasks for seed in seeds for rate in rates]


def _cell_name(task, arch, seed, rate=None):
    return f"{task}_{arch}_{seed}"+(f"_lr{float(rate):.6g}" if rate is not None else "")


def freeze_protocol(config, root, full_config=None):
    full_config = config if full_config is None else full_config
    # Behavioural hazards/gains do not identify arbitrary neural-subspace signs.
    # The computational primary is synthetic. Activating a biological map needs
    # its own development-signature calibration and separately frozen protocol.
    biological = None
    status = "pending: model-informed biological transfer requires a separate bridge protocol and validated development-only signature calibration"
    if config.get("require_biological_directions_for_freeze", False):
        raise RuntimeError(f"Biological directions are required before this requested freeze: {status}")
    directory = _directory(root, config)
    # All predeclared development hyperparameters are mandatory; choose on
    # validation competence, with first two tasks/architectures for transfer.
    rows = []
    missing = []
    for task, architecture, seed, rate in _cells(config, development=True):
        path = directory/"development"/_cell_name(task, architecture, seed, rate)/"summary.json"
        if not path.exists():
            missing.append(str(path))
            continue
        record = json.loads(path.read_text(encoding="utf-8"))
        expected = {"config_sha256": _digest(config), "implementation_sha256": _implementation_sha256(),
                    "seed": seed, "architecture": architecture, "task": task, "learning_rate": float(rate),
                    "training_steps": int(config.get("total_steps", 1000000)), "complete_training_budget": True}
        if any(record.get(key) != value for key, value in expected.items()):
            raise RuntimeError(f"Incomplete or configuration/source-mismatched development summary: {path}")
        checkpoint_path = path.parent/"checkpoint.pt"
        if not checkpoint_path.exists() or record.get("checkpoint_sha256") != hashlib.sha256(checkpoint_path.read_bytes()).hexdigest():
            raise RuntimeError(f"Development checkpoint file hash mismatch: {checkpoint_path}")
        checkpoint = _torch().load(checkpoint_path, map_location="cpu", weights_only=False)
        _verify_checkpoint(checkpoint, config, seed=seed, architecture=architecture, task=task, learning_rate=float(rate), complete=True)
        if record.get("weight_sha256") != checkpoint["weight_sha256"]:
            raise RuntimeError(f"Development summary model hash mismatch: {path}")
        rows.append(record)
    if missing:
        raise RuntimeError(f"Cannot freeze the agent protocol: {len(missing)} development runs are missing")
    permitted_arch = list(config.get("transfer_training_architectures", list(config.get("architectures", ARCHITECTURES))[:2]))
    permitted_tasks = list(config.get("transfer_training_tasks", list(config.get("tasks", config.get("envs", TASKS)))[:2]))
    all_arch = list(config.get("architectures", ARCHITECTURES))
    all_tasks = list(config.get("tasks", config.get("envs", TASKS)))
    if len(set(permitted_arch)) != 2 or len(permitted_arch) != 2 or not set(permitted_arch) <= set(all_arch):
        raise ValueError("Transfer selection requires two unique declared recurrent architectures")
    if len(set(permitted_tasks)) != 2 or len(permitted_tasks) != 2 or not set(permitted_tasks) <= set(all_tasks):
        raise ValueError("Transfer selection requires two unique declared tasks")
    heldout_arch = set(all_arch)-set(permitted_arch)
    heldout_tasks = set(all_tasks)-set(permitted_tasks)
    if len(heldout_arch) != 1 or len(heldout_tasks) != 1:
        raise ValueError("Transfer selection must leave exactly one architecture and one task held out")
    eligible = [r for r in rows if r["architecture"] in permitted_arch and r["task"] in permitted_tasks]
    lr_scores = {}
    for rate in config.get("lr_grid", [3e-4, 1e-3, 3e-3]):
        selected = [r for r in eligible if r["learning_rate"] == rate]
        lr_scores[str(rate)] = float(np.mean([np.mean([r["baseline_success"], *r["competence"].values()]) for r in selected]))
    selected_rate = float(max(lr_scores, key=lr_scores.get))
    direction_records = []
    for row in eligible:
        if row["learning_rate"] != selected_rate:
            continue
        path = directory/"development"/_cell_name(row["task"], row["architecture"], row["seed"], selected_rate)/"calibrated_directions.json"
        if not path.exists():
            raise RuntimeError(f"Missing development-only perturbation calibration: {path}")
        direction_records.append(json.loads(path.read_text(encoding="utf-8")))
        latest = direction_records[-1]
        expected_keys = {f"{mechanism}:{rank}" for mechanism in MECHANISMS for rank in config.get("ranks", [1, 2, 4, 8])
                         if mechanism != "motor_gain" or rank == 1}
        if set(latest) != expected_keys:
            raise RuntimeError("Development directions omit a primary mechanism/rank cell")
        for key, value in latest.items():
            rank = int(key.split(":")[1])
            theta = np.asarray(value.get("theta"), dtype=float)
            if value.get("base_weight_sha256") != row["weight_sha256"] or theta.shape != (rank,) or not np.isfinite(theta).all():
                raise RuntimeError("Development direction has mismatched base weights or invalid coefficients")
            if value.get("calibration_episodes") != int(config.get("calibration_episodes", 32)):
                raise RuntimeError("Development directions have mismatched calibration episode budgets")
    directions = {}
    for key in direction_records[0]:
        average = np.mean([d[key]["theta"] for d in direction_records], axis=0)
        # Development ties use the predeclared first seed, never heldout models.
        if np.linalg.norm(average) < 1e-8:
            average = direction_records[0][key]["theta"]
        directions[key] = _normalize(average).tolist()
    protocol = {"schema": 1, "config_sha256": _digest(config), "learning_rate": selected_rate, "learning_rate_scores": lr_scores,
                "implementation_sha256": _implementation_sha256(),
                "full_config_sha256":canonical_hash(full_config), "H4_inference_specification":h4_specification(full_config),
                "selection_architectures": permitted_arch, "selection_tasks": permitted_tasks,
                "heldout_architecture": next(iter(heldout_arch)),
                "heldout_task": next(iter(heldout_tasks)),
                "development_seeds": list(config.get("development_seeds", range(100, 105))),
                "final_seeds": list(config.get("final_seeds", range(1000, 1020))), "directions": directions,
                "strengths": config.get("strengths", [0, .05, .1, .2, .4, .8]),
                "ranks": config.get("ranks", [1, 2, 4, 8]), "biological_directions": biological,
                "primary_evidence_class":"computational_experiment","biological_transfer_status":"pending",
                "biological_direction_status": status, "source_development_weights": [r["weight_sha256"] for r in eligible if r["learning_rate"] == selected_rate]}
    target = directory/"frozen_protocol.json"
    if target.exists() and json.loads(target.read_text(encoding="utf-8")) != protocol:
        raise RuntimeError("Frozen protocol already exists and differs; make a dated protocol amendment in a new output directory")
    _json(target, protocol)
    return {"protocol": str(target), "protocol_sha256": _digest(protocol), "learning_rate": selected_rate, "biological_direction_status": status}


def _load_policy(path, device, config=None, **expected):
    torch = _torch()
    checkpoint = torch.load(path, map_location=device, weights_only=False)
    _verify_checkpoint(checkpoint, checkpoint["config"] if config is None else config, **expected)
    policy = make_policy(checkpoint["architecture"], int(checkpoint["config"].get("capacity", 100000)), int(checkpoint["config"].get("observation_window", 8))).to(device)
    policy.load_state_dict(checkpoint["state_dict"])
    policy.requires_grad_(False)
    return policy, checkpoint


def cpu_evaluation_pilot(config, root, task_index=None):
    """Finite CPU benchmark of the two existing pilot policies; never retrains."""
    settings = dict(config.get("agents",config))
    if "output_dir" in config and "output_dir" not in settings:
        settings["output_dir"] = config["output_dir"]
    directory = _directory(Path(root),settings)
    torch = _torch()
    torch.set_num_threads(int(os.environ.get("SLURM_CPUS_PER_TASK",settings.get("torch_threads",1))))
    cells = _cells(settings,pilot=True)
    if task_index is not None:
        if task_index not in range(2):
            raise ValueError("CPU evaluation pilot index must be zero or one")
        cells = [cells[task_index]]
    reports = []
    for task,architecture,seed,rate in cells:
        original = directory/"pilot"/_cell_name(task,architecture,seed)
        summary = json.loads((original/"summary.json").read_text(encoding="utf-8"))
        if summary.get("full_config_sha256") != canonical_hash(config):
            raise RuntimeError("CPU evaluation pilot requires the matching full-configuration pilot receipt")
        if summary.get("checkpoint_sha256") != hashlib.sha256((original/"checkpoint.pt").read_bytes()).hexdigest():
            raise RuntimeError("Pilot checkpoint bytes changed")
        policy, checkpoint = _load_policy(original/"checkpoint.pt","cpu",settings,seed=seed,architecture=architecture,task=task,learning_rate=rate)
        if summary.get("training_steps") != checkpoint["steps"] or checkpoint["steps"] != int(settings.get("pilot_steps",4096)):
            raise RuntimeError("CPU evaluation pilot does not match the declared pilot training budget")
        if state_hash(policy) != summary.get("weight_sha256"):
            raise RuntimeError("CPU evaluation pilot does not match the pilot policy")
        destination = directory/"cpu-evaluation-pilot"/_cell_name(task,architecture,seed)
        probe_settings = dict(settings,evaluation_episodes=4,calibration_episodes=8,kl_bisections=3)
        directions = {f"{mechanism}:{rank}":_normalize(np.random.default_rng(419+rank).normal(size=rank)).tolist()
                      for mechanism in MECHANISMS for rank in settings.get("ranks",[1,2,4,8]) if mechanism!="motor_gain" or rank==1}
        protocol = {"directions":directions,"biological_directions":None,"biological_direction_status":"pending; pilot synthetic directions",
                    "heldout_architecture":ARCHITECTURES[2],"heldout_task":TASKS[2],"pilot":True}
        evaluation = perturbation_experiment(policy,task,seed,probe_settings,protocol,destination)
        record = {**evaluation,**_benchmark_provenance(config,destination,"cpu"),"evaluation_episodes":4,
                  "calibration_episodes":8,"kl_bisections":3,"seed":seed,"training_steps":0,
                  "pilot_checkpoint_sha256":summary["checkpoint_sha256"],"pilot_checkpoint":str(original/"checkpoint.pt"),
                  "training_checkpoint_steps":summary["training_steps"],
                  "forecast_coverage":"two foraging/RNN pilot policies only; other architecture/task and full calibration coverage pending",
                  "evidence_class":"technical CPU evaluation benchmark; not final H4 evidence"}
        _json(destination/"cpu_evaluation_benchmark.json",record)
        reports.append(record)
    return {"mode":"cpu-evaluation-pilot","reports":reports,"complete_architecture_task_forecast":False}


def run(config: dict, root: Path | str, mode: str = "pilot", task_index: int | None = None):
    """Run one Slurm-array cell, all cells, or freeze development selection.

    No final model is trained until every development run and its frozen selection
    exists. Pilot explicitly reports incomplete competence and projected training.
    """
    root = Path(root)
    if mode == "cpu-evaluation-pilot":
        return cpu_evaluation_pilot(config,root,task_index)
    settings = dict(config.get("agents", config))
    if "output_dir" in config and "output_dir" not in settings:
        settings["output_dir"] = config["output_dir"]
    if mode == "freeze":
        return freeze_protocol(settings, root, full_config=config)
    if mode not in ("pilot", "development", "final", "evaluate"):
        raise ValueError(f"Unknown agent mode {mode}")
    development, pilot = mode == "development", mode == "pilot"
    cells = _cells(settings, development, pilot)
    array_cells = len(cells)
    if development:
        # One array task is one architecture/task/seed, fitting all three learning
        # rates with matched RNG streams. There are 45 primary seed cells, not
        # 135 independent replicates.
        rates_per_cell = len(settings.get("lr_grid", [3e-4, 1e-3, 3e-3]))
        array_cells //= rates_per_cell
    if task_index is not None:
        if task_index not in range(array_cells):
            raise ValueError(f"Array index must be in [0, {array_cells-1}]")
        cells = cells[task_index*rates_per_cell:(task_index+1)*rates_per_cell] if development else [cells[task_index]]
    directory = _directory(root, settings)
    protocol = None
    if mode in ("final", "evaluate"):
        protocol_path = directory/"frozen_protocol.json"
        if not protocol_path.exists():
            raise RuntimeError("Final agent work requires the complete, frozen development protocol")
        protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
        if protocol["config_sha256"] != _digest(settings):
            raise RuntimeError("Configuration changed after the agent protocol was frozen")
        if protocol.get("full_config_sha256") != canonical_hash(config) or protocol.get("H4_inference_specification") != h4_specification(config):
            raise RuntimeError("Full study configuration or H4 inference specification changed after protocol freeze")
        if protocol.get("implementation_sha256") != _implementation_sha256():
            raise RuntimeError("Agent implementation changed after the protocol freeze")
    reports = []
    for task, architecture, seed, rate in cells:
        destination = directory/("final" if mode == "evaluate" else mode)/_cell_name(task, architecture, seed, rate if development else None)
        summary_path = destination/"summary.json"
        if summary_path.exists():
            saved = json.loads(summary_path.read_text(encoding="utf-8"))
            if saved["config_sha256"] != _digest(settings):
                raise RuntimeError(f"Existing output has a different configuration: {summary_path}")
            expected_steps = int(settings.get("pilot_steps", 4096) if pilot else settings.get("total_steps", 1000000))
            expected_summary = {"seed": seed, "architecture": architecture, "task": task, "training_steps": expected_steps}
            if any(saved.get(key) != value for key, value in expected_summary.items()):
                raise RuntimeError("Existing summary does not identify this completed policy cell")
            checkpoint_path = destination/"checkpoint.pt"
            if saved.get("checkpoint_sha256") != hashlib.sha256(checkpoint_path.read_bytes()).hexdigest():
                raise RuntimeError("Existing checkpoint file hash differs from its summary")
            expected_rate = float(rate if development else protocol["learning_rate"] if protocol else settings.get("lr_grid", [3e-4])[0])
            policy, checkpoint = _load_policy(checkpoint_path, settings.get("device", "cuda" if _torch().cuda.is_available() else "cpu"),
                                              settings, seed=seed, architecture=architecture, task=task,
                                              learning_rate=expected_rate, complete=not pilot)
            if saved.get("weight_sha256") != state_hash(policy) or saved.get("implementation_sha256") != _implementation_sha256():
                raise RuntimeError("Existing policy/source hash differs from its summary")
            summary = saved
        elif mode == "evaluate":
            raise RuntimeError(f"Evaluate requires an existing final checkpoint: {destination}")
        else:
            rate = rate or (protocol["learning_rate"] if protocol else float(settings.get("lr_grid", [3e-4])[0]))
            policy, summary = train(settings, task, architecture, seed, destination, rate,
                                    steps=int(settings.get("pilot_steps", 4096)) if pilot else None)
        if development and architecture in list(settings.get("transfer_training_architectures", list(settings.get("architectures", ARCHITECTURES))[:2])) and task in list(settings.get("transfer_training_tasks", list(settings.get("tasks", settings.get("envs", TASKS)))[:2])):
            target = destination/"calibrated_directions.json"
            if not target.exists():
                _json(target, calibrate_rule(policy, task, settings, seed))
        if mode == "evaluate":
            target = destination/"evaluation_summary.json"
            if target.exists():
                evaluation = json.loads(target.read_text(encoding="utf-8"))
                expected_evaluation = {"training_seed": seed, "architecture": architecture, "task": task,
                                       "base_weight_sha256": state_hash(policy), "protocol_sha256": _digest(protocol), "base_weights_unchanged": True}
                if any(evaluation.get(key) != value for key, value in expected_evaluation.items()):
                    raise RuntimeError("Evaluation receipt has a different frozen policy or protocol")
                trial_path = destination/"agent_trials.parquet"
                if not trial_path.exists() or evaluation.get("trial_file_sha256") != hashlib.sha256(trial_path.read_bytes()).hexdigest():
                    raise RuntimeError("Evaluation trial file changed or is missing")
                summary["evaluation"] = evaluation
            else:
                summary["evaluation"] = perturbation_experiment(policy, task, seed, settings, protocol, destination)
        if pilot:
            primary_count = 3*3*len(settings.get("final_seeds", range(1000, 1020)))
            summary["projected_primary_training_gpu_hours"] = primary_count*int(settings.get("total_steps", 1000000))/summary["steps_per_second"]/3600
            final_count = len(_cells(settings))
            development_fit_count = len(_cells(settings, development=True))
            summary["projected_configured_final_training_gpu_hours"] = final_count*int(settings.get("total_steps", 1000000))/summary["steps_per_second"]/3600
            summary["projected_development_training_gpu_hours"] = development_fit_count*int(settings.get("total_steps", 1000000))/summary["steps_per_second"]/3600
            summary["admission_status"] = "benchmark only; full-budget competence and complete evaluation throughput remain unverified"
            # A genuine shortened evaluation of every mechanism/rank plus all
            # probes measures costs without pretending to be final evidence.
            probe_settings = dict(settings, evaluation_episodes=int(settings.get("pilot_evaluation_episodes", 4)),
                                  calibration_episodes=8, kl_bisections=3)
            random_directions = {f"{mechanism}:{rank}": _normalize(np.random.default_rng(419+rank).normal(size=rank)).tolist()
                                 for mechanism in MECHANISMS for rank in settings.get("ranks", [1, 2, 4, 8])
                                 if mechanism != "motor_gain" or rank == 1}
            pilot_protocol = {"directions": random_directions, "biological_directions": None, "biological_direction_status": "pending; pilot synthetic directions",
                              "heldout_architecture": ARCHITECTURES[2], "heldout_task": TASKS[2], "pilot": True}
            summary["pilot_evaluation"] = perturbation_experiment(policy, task, seed, probe_settings, pilot_protocol, destination)
            summary.update(_benchmark_provenance(config,destination,str(next(policy.parameters()).device)))
            summary["forecast_coverage"] = "two foraging/RNN pilot policies only; other architecture/task and full calibration coverage pending"
            measured = summary["pilot_evaluation"]["evaluation_seconds"]
            summary["projected_primary_evaluation_gpu_hours"] = primary_count*measured*int(settings.get("evaluation_episodes", 64))/int(probe_settings["evaluation_episodes"])/3600
            _json(destination/"pilot_benchmark.json", summary)
            _json(destination/"summary.json", summary)
        reports.append(summary)
    if development and task_index is None:
        frozen = freeze_protocol(settings, root, full_config=config)
    else:
        frozen = None
    return {"mode": mode, "array_cells": array_cells, "reports": reports, "frozen": frozen}
