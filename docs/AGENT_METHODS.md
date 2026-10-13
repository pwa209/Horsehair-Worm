# Computational intervention protocol

This implements the supplied proposal §§4.19–4.21, with all new outcomes generated
by locally trained computational systems. It does not generate biological data or
claim that artificial networks are anatomical insect models. Implementation and
configuration hashes accompany checkpoints and outputs. Results remain prospective
until the corresponding training and held-out evaluations actually finish.

## Replicates and training

The primary design has three tasks, RNN/GRU/modular recurrent policies, and 20
independent training seeds (1000–1019): 180 primary policies. The configured
feedforward baseline adds 60 policies with a 32-observation rolling history.
Each policy has approximately 100,000 parameters. Width is chosen using actual
parameter counts, rather than equal hidden widths. At this target the counts are
100,095 (RNN), 99,501 (GRU), 100,432 (modular), and 99,775 (feedforward), all within
1% of one another. The optional 25,000-parameter, ten-seed design is not enabled by
the primary configuration and must use a distinct output directory/protocol.

Development seeds 100–104 are disjoint from final and pilot seeds. Every development
architecture/task/seed fits the entire learning-rate grid (0.0003, 0.001, 0.003),
with identical initial random streams across learning rates. This yields 135
primary development fits and 45 baseline development fits; these are 60 independent
seed cells, not 180 independent replicates. Slurm array indices 0–59 each fit all
three rates. Final indices 0–239 each train one independent policy.

The optimizer is Adam, with clipped PPO, epsilon 0.2, gamma 0.99, GAE lambda 0.95,
four optimization epochs per rollout, value coefficient 0.5, entropy coefficient
0.01, and gradient clipping at 0.5. Sixteen environments produce ordered
128-step rollouts. Recurrent PPO recomputes each ordered rollout with truncated
backpropagation, its original initial state, and explicit resets at terminal
boundaries. Time steps are never shuffled into unrelated recurrent histories.
After each optimizer update, the ordered segment is replayed without gradients
under the updated weights to obtain its carry state; terminal boundaries are reset
again. CUDA determinism configures `CUBLAS_WORKSPACE_CONFIG=:4096:8` before CUDA
initialization.
The training budget is exactly 1,000,000 environment steps, not a significance
stopping rule. Checkpoints include weights, optimizer, consumed steps, Python,
NumPy, Torch and CUDA random states, environment random states and serial numbers,
current observations, recurrent states and training metrics. Interrupted training
resumes from these records after checking seed/configuration provenance. The
checkpoint records its source/configuration and actual model-weight hashes;
summaries also checksum the complete checkpoint file. Resume rejects changes to
seed, task, architecture, learning rate, source code or configuration. A numerical
test confirms exact equality of step count, weights, optimizer state, environment
observations, random stream and recurrent carry state between uninterrupted and
resumed PPO on a small deterministic case.

## Tasks and observations

Every policy receives the same 32-dimensional observation and five actions: up,
down, left, right, stay. The observation encodes task/probe identity, normalized
position, time, task-relevant cues, cue reliabilities, and current visible motor
instruction when applicable. The hidden rewarded target is never exposed through
an outcome label. Each episode terminates on its task endpoint or at 200 steps.

* **Foraging:** an 11×11 grid, start (5,5), two independently generated Gaussian
  cue fields, randomized true goal and distractor. Cue reliability is visible;
  each cue center locates the true goal with its declared reliability, otherwise
  the distractor. Training reliabilities are 0.8–0.95 and 0.55–0.7. Training goals
  use coordinates {2,4,6,8} on both axes. Held-out goals are corners (1,1), (1,9),
  (9,1), (9,9), or (2,5)/(8,5), with reliability pairs (1,0.5) or (0.75,0.55)
  and reversed cue ordering. Reward is +1 at the actual goal, −0.5 at the
  distractor, with −0.005 step cost and 0.025 times Manhattan distance reduction
  as potential shaping. Redirection is distractor arrival, never training reward.
* **Corridor:** positions 0–10 on the active axis, start 5, randomized food
  endpoint and independently randomized gravity sign. Twelve percent of steps
  receive one unit of environmental drift in the gravity direction. The
  training arena is horizontal; held-out orientation is vertical. Food reward,
  distractor penalty and distance shaping follow the foraging task. Redirection
  is arrival at the gravity-favored spatial endpoint, independently of whether
  it contains food. This makes movement amplification an explicit alternative.
* **Delayed conditional choice:** an initial binary cue identifies the rewarded
  terminal. The cue disappears after the first observation. Training delays are
  3–8 steps; held-out delays are 10,14,20. After the delay a choice-available
  signal appears; left/right then selects a terminal, with +1 for correct and
  −1 for incorrect choice. Wrong terminal selection is the redirection endpoint.
  Up/down/stay cannot manufacture an incorrect choice; timeout is separately
  recorded as truncation.

Training, calibration, validation and held-out episodes have separately reserved
seed ranges. Episode seed, independent training seed, task, architecture and probe
are retained in the trial table. A held-out axis or reliability can cause baseline
failure; that failure remains visible and cannot be converted into evidence of
selective redirection.

## Independent competence assays

Every architecture/main task receives the same probe curriculum: one quarter of
vector-environment slots train the following probe tasks, with three probes
distributed across those slots. Held-out episodes remain independently generated.
This gives each policy a defined competence to test; a policy is never declared
competent merely because it emits actions in an unfamiliar task.

* **Delayed cue:** the delayed conditional-choice task, with cue-free choice
  periods and held-out longer delays; endpoint is correct terminal selection.
* **Goal detection:** the foraging task with a separate noiseless visible-target
  cue. Endpoint is reaching the indicated true goal, not movement frequency.
* **Motor execution:** eight independently generated visible direction
  instructions; endpoint is per-instruction actuator accuracy. The stricter
  all-eight-correct episode flag is also retained.

All three probes run with the perturbation enabled. Probe performance is reported
separately; no average can conceal failure of one component. The implemented
qualification requires at least 0.8 clean held-out success in the policy's own
trained main task **and** at least 0.8 clean held-out accuracy on each probe.
The explicit configuration fields are `baseline_main_success_min` and
`baseline_probe_accuracy_min`; both are frozen in the H4 inference specification.
The existing training benchmark already used the joint 0.8 rule on validation
episodes. H4 eligibility applies that rule to actual, separately seeded held-out
episodes; training summaries or clean calibration trajectories cannot substitute
for these observations. The supplied proposal does not prescribe this numerical
cutoff, so it is declared prospectively before final fitting and evaluation.
All planned clean episodes enter the score. A numerical failure leaves
qualification pending, without dropping the failed episode or imputing its
endpoint. Each of the 20 seeds must qualify; a pooled mean cannot conceal a
failed policy. Main-task, probe and joint qualification flags remain separate.
Failed baseline qualifications remain in outputs. This is baseline eligibility,
not an additional main-task loss test under a redirection perturbation.
The proposal's primary operating
region is a ≥0.20 redirection increase and ≤0.05 accuracy loss on every probe.
Episodes are nested within independently trained seeds for inference.

## Frozen interventions, calibration and transfer

Base policy weights are frozen before perturbation selection and remain frozen
through calibration, evaluation and restoration. Unstructured controls perturb a
copy of the policy; the original state hash is verified after every evaluation.
No optimizer is created or invoked during these stages.

The four primary perturbation classes are sensory input gain, additive low-rank
recurrent transition modulation, additive hidden-state displacement, and
state-dependent action-output gain. All have exactly r adjustable coefficients
for r∈{1,2,4,8}; their bases are fixed, seed-generated and normalized. The recurrent
matrix rank is at most r. The output-gain matrix has at most five output dimensions,
but eight independent coefficients remain possible because their hidden factors
are orthogonal. Matched-rank random recurrent subspaces and unstructured weight
subspaces have the same coefficient and calibration budgets. An additional
one-coefficient motor-gain baseline amplifies all four movement logits relative
to staying; it is explicitly simpler and is evaluated only at rank one.

Each development cell receives 32 calibration episodes per mechanism/rank,
distributed equally across four derivative-free directional proposals, using
common environment/action random numbers. Selection maximizes redirection on
these development-only episodes. It does not optimize held-out competence to
obtain a desirable tradeoff. Global learning-rate and perturbation-rule selection
uses only RNN/GRU and foraging/corridor development agents. Modular architecture
and delayed choice are held out from this selection. The chosen direction is the
normalized development mean, with a predeclared first-development-seed tie break.

The complete declared development grid must exist before `--mode freeze` can
write `results/agents/frozen_protocol.json`. Final training/evaluation refuses
missing or configuration-mismatched protocols. Every development summary must
match its expected cell, source/configuration hashes and complete training budget;
the checkpoint bytes and model hashes are verified before selection. Duplicate
seeds, tasks, architectures or learning-rate grid entries are rejected. The frozen file records selection
tasks/architectures, seed lists, learning rate, parameter directions, strength
grid, source weight hashes and biological export status. Final policies receive
no redirection optimization. Their clean calibration observations may determine
functional strength normalization only.

The freeze also binds the SHA-256 of the **full study configuration** and an
explicit H4 inference specification: main-task and probe baseline qualification,
redirection and competence-loss thresholds,
alpha, bootstrap count, fixed reference point, matched comparator estimand,
multiplicity family and treatment of unavailable points. Final training,
evaluation and H4 analysis reject changes to these choices after the freeze.
An agent-subconfiguration match alone is insufficient.

Parameter norm alone and policy KL divergence alone do not guarantee equivalent
interventions. Two prespecified arms therefore evaluate the complete strength
grid: common parameter norm, and common clean-policy KL target s²/2. KL matching
uses bounded scalar bisection on clean calibration observation sequences, with
intervened hidden states propagated separately. The achieved norm, achieved KL,
target KL and tolerance flags are saved. Both quantities are always reported;
inability to reach a target is flagged instead of mislabeled as an exact match.

Behavioural transition hazards, locomotor gain and directional drift do not
identify input-gain, hidden-state or arbitrary neural-subspace signs. The current
primary computational experiment therefore freezes explicitly synthetic rules.
Biologically informed transfer is a separate **pending bridge**, requiring a
prospective mapping assumption, identifiable training-only behavioural bounds,
development-only signature calibration and its own frozen protocol before
evaluation. A readiness artifact is not mislabeled as a neural direction. The
prospective export path is bound in the full configuration; a later file cannot
silently activate a biological comparison in an already frozen experiment.
Requiring that unavailable bridge at primary freeze raises an explicit error.

## Restoration and persisted outcomes

At every strength the numerical test compares original and restored action
probabilities on identical observations and nonzero recurrent states, requiring
maximum error ≤1e−7 and bitwise unchanged base weights. Held-out restoration
controls compare exact removal of the intervention, equal-norm random correction,
cue removal, and deterministic rather than stochastic action selection (generic
reduction of action noise). Exact removal uses the original policy and recurrent
conditions without retraining.

`agent_trials.parquet` stores training/episode seeds, architecture, task, mechanism,
rank, matching arm, strength, achieved norms/divergences, probe, endpoint,
baseline endpoint, restoration control and frozen-protocol hash. Checkpoints,
training curves, validation/competence summaries, restoration checks and evaluation
throughput summaries are separate files. Timeouts are reported, not imputed as
successful or redirected episodes.

Pilot seeds 900/901 consume 4,096 training steps and run a genuine shortened
intervention/competence/restoration battery. The pilot's four evaluation episodes
per condition are a throughput measurement. They are not final H4 evidence and
cannot establish full-budget competence. Pilot extrapolations include primary,
configured baseline and development training costs separately. Full evaluation
projection remains approximate until calibration cost and all architectures are
measured. Neither a passing software test nor successful GPU qualification
authorizes a claim that selective control has been demonstrated.

## Execution interface

`horsehair.agents.run(config, root, mode, task_index=None)` supports `pilot`,
`cpu-evaluation-pilot`, `development`, `freeze`, `final`, `evaluate`. Final trains only; evaluate reads
the existing final checkpoint and runs the full frozen experiment. Existing
receipts are checked against configuration and reused. Evaluation reuse additionally
verifies its exact frozen policy/protocol and checksums the trial file. Final evaluation can be
an array with one policy per cell; running without an index processes all cells
and skips complete evaluation receipts. The API makes no remote changes itself.

The dedicated tests cover seed reproducibility, valid episode caps, hidden delayed
cues, independent food/gravity, disjoint held-out goals/delays, actual parameter
budgets, low-rank matrix rank/norm, exact restoration for every architecture and
mechanism, frozen evaluation weights, mandatory protocol freeze, pending biological
directions, and a real PPO optimizer/checkpoint smoke execution.

## H4 analysis and Figure 5

`horsehair.agent_analysis.run(config, root)` requires the entire planned final
grid, including all 60 feedforward baseline policies. It validates final
checkpoint/model hashes, configuration/source hashes, trial bytes, every frozen
mechanism/rank/strength/matching-arm/probe/restoration cell and every held-out
episode seed. A missing or failed policy writes an incomplete audit and prevents
an H4 result. Exact restoration must recover the clean terminal outcomes as well
as pass the numerical action-probability check.

Each perturbation's episode differences are first averaged within an independent
training seed. Twenty seed estimates then enter the one-sided mean contrasts;
confidence intervals resample seed clusters. Additional episodes cannot increase
the number of independent policy replicates. The pointwise intersection-union
test takes the maximum P value across redirection increase >0.20, each of the
three competence losses <0.05, and paired advantages over matched random-subspace
and output-gain controls. Comparator advantage is the paired difference in the
worst operating margin: the minimum of redirection-minus-0.20 and each
0.05-minus-competence-loss component. Every raw component remains separately
reported. Holm adjustment spans the full prespecified primary grid. Unqualified
or unmatched cells retain their actual curves and receive a conservative value
of one in this family; no favorable seed or strength is selected.

`results/inference/H4.json` distinguishes a complete computational analysis from
an accepted computational claim. The computational claim requires qualified
clean held-out main-task and separate-probe baseline competence for every planned
seed and the joint operating-region/comparator tests. The report checks these
scores against the complete hash-bound seed-summary roster, rather than trusting
an asserted qualification flag. Missing
biological constraints do not invalidate this explicitly computational primary;
the separate biological bridge remains pending and cannot be inferred from a
synthetic success. Computational and biological transfer have separate statuses.
The reference effect and interval use a prespecified point rather than the largest
evaluated effect. The omnibus P value concerns existence of a corrected joint
operating point and is labeled separately from that reference estimate.

Figure 5 inputs show every architecture/task, separate redirection and competence
components, and restoration controls. The main display uses prespecified rank-one
parameter-norm curves with all mechanisms and strengths; complete remaining
ranks, arms and rule types are preserved in `response_curves.json`. Architecture
contrasts pair identical seed blocks and are explicitly descriptive. These
outputs do not convert computational causality into an insect-neural mechanism.
