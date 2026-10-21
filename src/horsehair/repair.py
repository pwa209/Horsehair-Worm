"""Explicit, forensic-preserving DAG repair; never changes an execution receipt.

The primary immutable release remains the numerical execution origin. A named
preprocessing task may have a separately verified execution release. Registration
replaces only confirmed terminal attempts, after controllers have stopped.
"""
from __future__ import annotations

import ast
import copy
import json
import math
import os
import re
import shlex
import subprocess
from types import SimpleNamespace
from pathlib import Path
from contextlib import nullcontext

from .core import atomic_json, canonical_hash, digest, exclusive_file_lock, read_json, source_identity
from .pipeline import TERMINAL_FAILURE, _array_indices, _native_states, _task_status, _task_terminal

INITIAL_TARGETS = {"source-audit", "source-replication", *[f"pipeline-controller-{i}" for i in range(4)]}
ADDED_FILES = {"src/horsehair/repair.py", "scripts/amend_pipeline.py", "tests/test_repair.py",
               "scripts/transfer_rna_ftp.py", "tests/test_transfer_rna_ftp.py"}
INGEST_HELPERS = {"_trajectory_parquet_schemas", "_trajectory_arrow_table", "_ingestion_bin_trajectories"}


def _inside(path, directory):
    path, directory = Path(path).resolve(), Path(directory).resolve()
    if not path.is_relative_to(directory):
        raise ValueError("Repair descriptor path escapes its declared study namespace")
    return path


def _release(path, config):
    path = Path(path).resolve()
    record = read_json(path / "release.json")
    identity = source_identity(path)
    if record.get("source_identity") != identity or record.get("config_sha256") != canonical_hash(config):
        raise ValueError("Repair execution release/configuration is not immutable and matching")
    if read_json(path / "configs/study.json") != config:
        raise ValueError("Repair requires the unchanged complete primary configuration")
    return path, record, identity


def _ingestion_projection(path):
    tree = ast.parse(path.read_text(encoding="utf8"))
    projected = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in INGEST_HELPERS:
                continue
            if node.name == "run":
                node.body = [statement for statement in node.body if not (
                    isinstance(statement, ast.If) and isinstance(statement.test, ast.Compare)
                    and isinstance(statement.test.left, ast.Name) and statement.test.left.id == "stage"
                    and len(statement.test.ops) == 1 and isinstance(statement.test.ops[0], ast.In)
                    and isinstance(statement.test.comparators[0], (ast.Tuple, ast.List))
                    and [item.value for item in statement.test.comparators[0].elts if isinstance(item, ast.Constant)] == ["ingest", "validate"])]
        projected.append(node)
    tree.body = projected
    return ast.dump(tree, include_attributes=False)


def _preprocessing_release(primary, other, old_identity, new_identity):
    changed = {name for name in set(old_identity["files"]) | set(new_identity["files"])
               if old_identity["files"].get(name) != new_identity["files"].get(name)}
    allowed = ADDED_FILES | {"src/horsehair/behaviour.py", "tests/test_behaviour.py"}
    if changed - allowed:
        raise ValueError("Preprocessing repair changed an undeclared numerical dependency: " + ", ".join(sorted(changed - allowed)))
    if _ingestion_projection(primary / "src/horsehair/behaviour.py") != _ingestion_projection(other / "src/horsehair/behaviour.py"):
        raise ValueError("Behaviour repair changed methods outside chronological ingestion")


def _controller_maintenance(descriptor, targets, controllers, primary, identity, config, root):
    """A qualified framework may control only the unchanged primary science DAG."""
    change = descriptor.get('controller_maintenance')
    if change is None:
        return None
    required = {'framework_release', 'primary_release', 'qualification_path',
                'qualification_sha256', 'qualification_job_id'}
    if (not isinstance(change, dict) or set(change) != required
            or descriptor['phase'] != 'managed-initial' or targets != set(controllers)
            or Path(change['primary_release']).resolve() != primary):
        raise ValueError('Controller maintenance must replace exactly all four primary controller segments')
    framework, framework_record, framework_identity = _release(change['framework_release'], config)
    if framework == primary or framework_identity == identity:
        raise ValueError('Controller maintenance requires a distinct verified framework release')
    if any(Path(descriptor['execution_releases'][name]).resolve() != framework for name in controllers):
        raise ValueError('All maintenance controllers must execute the same verified framework')
    qualification_path = _inside(change['qualification_path'], root / 'maintenance')
    if (qualification_path.is_symlink() or not qualification_path.is_file()
            or digest(qualification_path) != change['qualification_sha256']):
        raise ValueError('Controller framework qualification receipt is missing or changed')
    qualification = read_json(qualification_path)
    job = str(change['qualification_job_id'])
    if (not re.fullmatch(r'[0-9]+', job) or qualification.get('slurm_job_id') != job
            or qualification.get('status') != 'complete' or qualification.get('returncode') != 0
            or qualification.get('source_identity') != framework_identity
            or qualification.get('source_sha256') != framework_identity['sha256']
            or qualification.get('config_sha256') != canonical_hash(config)
            or qualification.get('primary_source_sha256') != identity['sha256']
            or qualification.get('source_verified_unchanged') is not True
            or qualification.get('test_selection') != 'complete source/tests directory; no environment selectors or third-party plugin autoload'):
        raise ValueError('Controller qualification does not positively bind the framework and unchanged primary configuration/source')
    counts = qualification.get('test_counts')
    if (not isinstance(counts, dict) or type(counts.get('tests')) is not int or counts['tests'] <= 0
            or any(type(counts.get(key)) is not int or counts[key] != 0 for key in ('failures', 'errors', 'skipped'))):
        raise ValueError('Controller framework requires complete positive qualification with no failures/errors/skips')
    artifacts = qualification.get('artifacts', {})
    for label in ('log', 'junit'):
        item = artifacts.get(label, {})
        artifact = _inside(item.get('path', ''), root)
        if (artifact.is_symlink() or not artifact.is_file()
                or digest(artifact) != item.get('sha256')):
            raise ValueError('Controller qualification log/JUnit evidence is absent or changed')
    from .cli import _qualification_junit
    if _qualification_junit(Path(artifacts['junit']['path'])) != counts:
        raise ValueError('Controller qualification counts differ from its pinned JUnit evidence')
    return {**change, 'framework_release': str(framework), 'framework_release_id': framework_record['release_id'],
            'framework_source_identity': framework_identity, 'primary_source_identity': identity,
            'config_sha256': canonical_hash(config), 'test_counts': counts}


def _normalized_script(text):
    return "\n".join(line for line in text.splitlines() if not (
        line.startswith("cd ") or line.startswith("export PYTHONPATH=")
        or line == "#SBATCH --hold"
        or line.startswith("#SBATCH --job-name=") or line.startswith("#SBATCH --output=")
        or line.startswith("#SBATCH --error=")))


def _rna_recovery_binding(descriptor, targets, primary, identity, config, root, original, old_ledger):
    """Bind a qualified count reuse guard without changing numerical execution."""
    if descriptor.get('rna_recovery') is not None and descriptor.get('rna_followup') is not None:
        raise ValueError('A repair cannot declare two RNA guard kinds')
    guard_module = 'rna_followup' if descriptor.get('rna_followup') is not None else 'rna_recovery'
    change = descriptor.get(guard_module)
    if change is None:
        return None
    required = {'framework_release', 'qualification_path', 'qualification_sha256',
                'qualification_job_id', 'manifest_path', 'manifest_sha256',
                'preparation_path', 'preparation_sha256'}
    if (not isinstance(change, dict) or set(change) != required
            or descriptor['phase'] != 'rna-full' or 'rna-counts' not in targets
            or not targets <= {'rna-counts', 'ant-phase', 'ant-score-freeze'}
            or any(Path(descriptor['execution_releases'][name]).resolve() != primary for name in targets)):
        raise ValueError('RNA recovery must preserve the primary count and downstream execution releases')
    controllers = {f'pipeline-controller-{index}' for index in range(4)}
    framework_change = {key: change[key] for key in ('framework_release', 'qualification_path',
                                                   'qualification_sha256', 'qualification_job_id')}
    framework_change['primary_release'] = str(primary)
    # Reuse the complete source/JUnit qualification contract, without treating
    # the guard as a new numerical execution release or a controller migration.
    qualified = _controller_maintenance({'phase': 'managed-initial',
        'controller_maintenance': framework_change,
        'execution_releases': {name: change['framework_release'] for name in controllers}},
        controllers, controllers, primary, identity, config, root)
    for label in ('manifest', 'preparation'):
        declared = Path(change[label + '_path'])
        path = _inside(declared, root / 'maintenance')
        if (declared.is_symlink() or not path.is_file()
                or not re.fullmatch('[0-9a-f]{64}', str(change[label + '_sha256']))
                or digest(path) != change[label + '_sha256']):
            raise ValueError('RNA recovery ' + label + ' is missing, relocated or changed')
    if guard_module == 'rna_followup':
        from .rna_followup import validate_registration_manifest
    else:
        from .rna_recovery import validate_registration_manifest
    context = validate_registration_manifest(change['manifest_path'], change['manifest_sha256'],
        change['preparation_path'], change['preparation_sha256'])
    manifest, preparation = context['manifest'], context['preparation']
    if (Path(manifest['root']).resolve() != root or Path(manifest['primary_release']).resolve() != primary
            or manifest['primary_source_identity'] != identity
            or manifest['full_config_sha256'] != canonical_hash(config)
            or Path(manifest['maintenance_release']).resolve() != Path(qualified['framework_release'])
            or manifest['maintenance_source_identity'] != qualified['framework_source_identity']
            or str(manifest['previous_array_job_id']) != str(old_ledger['tasks']['rna-counts']['job_id'])
            or original['rna-counts']['resources'].get('array') != '0-59%2'):
        raise ValueError('RNA recovery evidence differs from the frozen count cohort/source/configuration')
    jobs = {'qualification_job_id': str(change['qualification_job_id']),
            'capture_job_id': str(manifest['capture_slurm_job_id']),
            'preparation_job_id': str(preparation['slurm_job_id'])}
    if any(not re.fullmatch('[0-9]+', job) for job in jobs.values()):
        raise ValueError('RNA recovery requires exact scalar native maintenance identities')
    previous_guard = None
    if guard_module == 'rna_followup':
        # A second guard must explain the exact first guard it supersedes. The
        # canonical amendment is retained; no old raw CLI command is invented.
        previous = [item.get('rna_recovery') for item in old_ledger.get('dag_amendments', [])
                    if item.get('rna_recovery') is not None and 'rna-counts' in item.get('replaced_tasks', [])]
        if not previous:
            raise ValueError('RNA follow-up lacks its canonical first-recovery amendment')
        previous_guard = previous[-1]
        original_manifest, original_preparation = manifest['original_manifest'], manifest['original_preparation']
        if (previous_guard.get('manifest_path') != original_manifest['path']
                or previous_guard.get('manifest_sha256') != original_manifest['sha256']
                or previous_guard.get('preparation_path') != original_preparation['path']
                or previous_guard.get('preparation_sha256') != original_preparation['sha256']):
            raise ValueError('RNA follow-up does not extend the unchanged canonical first-recovery evidence')
    result = {**change, **jobs, 'root': str(root),
            'framework_release': qualified['framework_release'],
            'framework_source_identity': qualified['framework_source_identity'],
            'primary_source_identity': identity, 'config_sha256': canonical_hash(config),
            'manifest_plan_sha256': manifest['plan_sha256'],
            'reuse_indices': manifest['reuse_indices'], 'rerun_indices': manifest['rerun_indices']}
    if guard_module == 'rna_followup':
        result.update(guard_module=guard_module, previous_guard=previous_guard)
    return result


def _mantis_recovery_binding(descriptor, targets, primary, identity, config, root, original, old_ledger):
    """Bind one explicit reference-map adapter to unchanged primary ORF kernels.

    This exception belongs only to the two auxiliary Mantis nodes. The parent
    phase and orthology execution retain the primary identity; the independently
    qualified adapter has its own truthful source identity in the amendment.
    """
    change = descriptor.get('mantis_reference_recovery')
    if change is None:
        return None
    required = {'framework_release', 'qualification_path', 'qualification_sha256',
                'qualification_job_id', 'manifest_path', 'manifest_sha256',
                'preparation_path', 'preparation_sha256'}
    names = {'mantis-orfs', 'mantis-orthology'}
    if (not isinstance(change, dict) or set(change) != required
            or descriptor['phase'] != 'molecular-corroboration' or targets != names
            or any(descriptor.get(key) is not None for key in ('rna_recovery', 'rna_followup', 'controller_maintenance'))
            or any(Path(descriptor['execution_releases'][name]).resolve() != primary for name in names)):
        raise ValueError('Mantis reference recovery must replace exactly both auxiliary nodes in the unchanged primary context')
    controllers = {f'pipeline-controller-{index}' for index in range(4)}
    framework_change = {key: change[key] for key in ('framework_release', 'qualification_path',
                                                   'qualification_sha256', 'qualification_job_id')}
    framework_change['primary_release'] = str(primary)
    qualified = _controller_maintenance({'phase': 'managed-initial',
        'controller_maintenance': framework_change,
        'execution_releases': {name: change['framework_release'] for name in controllers}},
        controllers, controllers, primary, identity, config, root)
    for label in ('manifest', 'preparation'):
        declared = Path(change[label + '_path'])
        path = _inside(declared, root / 'maintenance')
        if (declared.is_symlink() or not path.is_file()
                or not re.fullmatch('[0-9a-f]{64}', str(change[label + '_sha256']))
                or digest(path) != change[label + '_sha256']):
            raise ValueError('Mantis reference recovery ' + label + ' is missing, relocated or changed')
    from .mantis_recovery import ORF_RECIPE, validate_registration_manifest
    context = validate_registration_manifest(change['manifest_path'], change['manifest_sha256'],
        change['preparation_path'], change['preparation_sha256'])
    manifest, preparation = context['manifest'], context['preparation']
    previous = {name: str(old_ledger['tasks'][name]['job_id']) for name in names}
    if (Path(manifest['root']).resolve() != root or Path(manifest['primary_release']).resolve() != primary
            or manifest['primary_source_identity'] != identity
            or manifest['full_config_sha256'] != canonical_hash(config)
            or Path(manifest['maintenance_release']).resolve() != Path(qualified['framework_release'])
            or manifest['maintenance_source_identity'] != qualified['framework_source_identity']
            or manifest.get('previous_job_ids') != previous or manifest.get('orf_recipe') != ORF_RECIPE
            or any(original[name].get('resources', {}).get('array') is not None for name in names)):
        raise ValueError('Mantis reference recovery differs from the frozen primary source, configuration, recipe or prior nodes')
    jobs = {'qualification_job_id': str(change['qualification_job_id']),
            'capture_job_id': str(manifest['capture_slurm_job_id']),
            'preparation_job_id': str(preparation['slurm_job_id'])}
    if any(not re.fullmatch('[0-9]+', job) for job in jobs.values()):
        raise ValueError('Mantis reference recovery requires exact scalar native maintenance identities')
    return {**change, **jobs, 'root': str(root), 'primary_release': str(primary),
            'framework_release': qualified['framework_release'],
            'framework_source_identity': qualified['framework_source_identity'],
            'primary_source_identity': identity, 'config_sha256': canonical_hash(config),
            'previous_job_ids': previous, 'orf_recipe': copy.deepcopy(ORF_RECIPE)}


def _runtime_adjustments(descriptor, original, candidates, targets):
    adjustments = descriptor.get("runtime_adjustments", {})
    if not isinstance(adjustments, dict) or set(adjustments) - {"source-audit"} or not set(adjustments) <= targets:
        raise ValueError("Runtime amendment may declare only a replaced source-audit")
    for name, change in adjustments.items():
        if descriptor["phase"] != "managed-initial" or not isinstance(change, dict):
            raise ValueError("Runtime amendment is limited to the initial source audit")
        if set(change) != {"old_hours", "new_hours", "failed_job_id", "justification"}:
            raise ValueError("Runtime amendment requires explicit bounded hours, failed job and justification")
        old, new = change["old_hours"], change["new_hours"]
        before, after = original[name]["resources"], candidates[name]["resources"]
        if (type(old) is not int or type(new) is not int or old != 8 or not old < new <= 168
                or not isinstance(change["justification"], str) or not change["justification"].strip()
                or not re.fullmatch(r"[0-9]+", str(change["failed_job_id"]))
                or before.get("hours") != old or after.get("hours") != new
                or before.get("gpu") is not False or before.get("array")
                or {key:value for key,value in before.items() if key != "hours"}
                   != {key:value for key,value in after.items() if key != "hours"}):
            raise ValueError("Audit runtime amendment must preserve CPU, memory, GPU and array contracts within bounded hours")
    return adjustments


def _runtime_script_projection(text, hours):
    lines = text.splitlines()
    time_lines = [line for line in lines if line.startswith("#SBATCH --time")]
    lease_lines = [line for line in lines if line.startswith("export HHW_MAX_HOURS")]
    if time_lines != [f"#SBATCH --time={hours}:00:00"] or lease_lines != [f"export HHW_MAX_HOURS={hours}"]:
        raise ValueError("Audit script Slurm and lease time bounds must exactly match its declared hours")
    return "\n".join(line for line in lines if line not in {*time_lines, *lease_lines})


def _rna_recovery_script_projection(text, old_text, recovery):
    """Project one qualified operational guard onto its exact primary command."""
    old_commands = [line for line in old_text.splitlines()
                    if line.startswith('python -m horsehair.cli ')
                    and 'molecular' in shlex.split(line)[4:]]
    if len(old_commands) != 1:
        raise ValueError('RNA recovery requires one original numerical CLI command')
    original = old_commands[0]
    argv = shlex.split(original)
    expected = ['python', '-m', 'horsehair.cli', '--config', 'configs/study.json',
                '--root', argv[6] if len(argv) == 12 else '', 'molecular', '--stage', 'rna-run',
                '--task-index', '$SLURM_ARRAY_TASK_ID']
    if argv != expected or Path(argv[6]).resolve() != Path(recovery['root']).resolve():
        raise ValueError('RNA recovery changes the frozen primary numerical CLI command')
    guard_module = recovery.get('guard_module', 'rna_recovery')
    if guard_module not in {'rna_recovery', 'rna_followup'}:
        raise ValueError('Unknown qualified RNA operational guard')
    command = ('python ' + shlex.quote(str(Path(recovery['framework_release']) / ('src/horsehair/' + guard_module + '.py')))
               + ' run --manifest ' + shlex.quote(recovery['manifest_path'])
               + ' --index "$SLURM_ARRAY_TASK_ID" --manifest-sha256 ' + recovery['manifest_sha256']
               + ' --preparation ' + shlex.quote(recovery['preparation_path'])
               + ' --preparation-sha256 ' + recovery['preparation_sha256'])
    if text.splitlines().count(command) != 1:
        raise ValueError('RNA recovery wrapper does not match its exact pinned invocation')
    return text.replace(command, original, 1)


def _rna_followup_prior_projection(text, recovery):
    """Explain a bound prior wrapper using its preserved first amendment."""
    prior = recovery.get('previous_guard')
    if not isinstance(prior, dict):
        raise ValueError('RNA follow-up requires the canonical prior guard')
    command = ('python ' + shlex.quote(str(Path(prior['framework_release']) / 'src/horsehair/rna_recovery.py'))
               + ' run --manifest ' + shlex.quote(prior['manifest_path'])
               + ' --index "$SLURM_ARRAY_TASK_ID" --manifest-sha256 ' + prior['manifest_sha256']
               + ' --preparation ' + shlex.quote(prior['preparation_path'])
               + ' --preparation-sha256 ' + prior['preparation_sha256'])
    if text.splitlines().count(command) != 1:
        raise ValueError('Canonical prior RNA script differs from its preserved qualified wrapper')
    original = ('python -m horsehair.cli --config configs/study.json --root ' + shlex.quote(recovery['root'])
                + ' molecular --stage rna-run --task-index "$SLURM_ARRAY_TASK_ID"')
    return text.replace(command, original, 1)


def _mantis_recovery_script_projection(text, old_text, recovery):
    """Permit one qualified input adapter, preserving the frozen primary shell."""
    commands = [line for line in old_text.splitlines()
                if line.startswith('python -m horsehair.cli ')
                and 'molecular-pipeline' in shlex.split(line)[4:]]
    if len(commands) != 1:
        raise ValueError('Mantis reference recovery requires one exact original ORF CLI command')
    original = commands[0]
    argv = shlex.split(original)
    expected = ['python', '-m', 'horsehair.cli', '--config', 'configs/study.json',
                '--root', argv[6] if len(argv) == 10 else '', 'molecular-pipeline', '--stage', 'mantis-orfs']
    if argv != expected or Path(argv[6]).resolve() != Path(recovery['root']).resolve():
        raise ValueError('Mantis reference recovery changes the frozen primary ORF CLI command')
    command = ('python ' + shlex.quote(str(Path(recovery['framework_release']) / 'src/horsehair/mantis_recovery.py'))
               + ' run --manifest ' + shlex.quote(recovery['manifest_path'])
               + ' --manifest-sha256 ' + recovery['manifest_sha256']
               + ' --preparation ' + shlex.quote(recovery['preparation_path'])
               + ' --preparation-sha256 ' + recovery['preparation_sha256'])
    if text.splitlines().count(command) != 1:
        raise ValueError('Mantis reference adapter does not match its exact qualified invocation')
    return text.replace(command, original, 1)


def _check_script(task, original, execution, root, runtime=None, controller_primary=None, rna_recovery=None, mantis_recovery=None):
    path = _inside(task["script"], root / "plans")
    text = path.read_text(encoding="utf8")
    if canonical_hash(text) != task.get("script_sha256"):
        raise ValueError("Replacement script bytes differ from their frozen plan")
    old_text = Path(original["script"]).read_text(encoding="utf8")
    if canonical_hash(old_text) != original.get("script_sha256"):
        raise ValueError("Original script bytes changed after their plan freeze")
    working = [shlex.split(line)[1] for line in text.splitlines() if line.startswith("cd ")]
    if len(working) != 1 or Path(working[0]).resolve() != execution:
        raise ValueError("Replacement script does not execute its declared immutable release")
    exports = [shlex.split(line)[1].split("=", 1)[1] for line in text.splitlines() if line.startswith("export PYTHONPATH=")]
    suffix = ':${PYTHONPATH:-}'
    if (len(exports) != 1 or not exports[0].endswith(suffix)
            or Path(exports[0][:-len(suffix)]).resolve() != execution / "src"):
        raise ValueError("Replacement Python import path differs from its execution release")
    if runtime:
        text = _runtime_script_projection(text, runtime["new_hours"])
        old_text = _runtime_script_projection(old_text, runtime["old_hours"])
    if rna_recovery is not None:
        if task.get('name') != 'rna-counts' or runtime or controller_primary is not None:
            raise ValueError('RNA recovery projection is limited to the primary count array')
        if rna_recovery.get('guard_module') == 'rna_followup':
            old_text = _rna_followup_prior_projection(old_text, rna_recovery)
        text = _rna_recovery_script_projection(text, old_text, rna_recovery)
    if mantis_recovery is not None:
        if task.get('name') != 'mantis-orfs' or runtime or controller_primary is not None or rna_recovery is not None:
            raise ValueError('Mantis reference adapter projection is limited to the primary auxiliary ORF node')
        text = _mantis_recovery_script_projection(text, old_text, mantis_recovery)
    if controller_primary is not None:
        old_commands = [line for line in old_text.splitlines() if line.startswith('python -m horsehair.cli ')
                        and 'pipeline' in shlex.split(line)[4:]]
        new_commands = [line for line in text.splitlines() if line.startswith('python -m horsehair.cli ')
                        and 'pipeline' in shlex.split(line)[4:]]
        if len(old_commands) != 1 or len(new_commands) != 1:
            raise ValueError('Maintenance replacement must retain the original CLI pipeline command')
        before, after = shlex.split(old_commands[0]), shlex.split(new_commands[0])
        old_flags = [i for i, value in enumerate(before) if value == '--primary-release' or value.startswith('--primary-release=')]
        new_flags = [i for i, value in enumerate(after) if value == '--primary-release' or value.startswith('--primary-release=')]
        if old_flags:
            index = old_flags[0]
            valid = (len(old_flags) == len(new_flags) == 1 and before == after
                     and before[index] == '--primary-release' and index + 1 < len(before)
                     and Path(before[index + 1]).resolve() == controller_primary)
        else:
            valid = (len(new_flags) == 1 and len(after) == len(before) + 2 and after[:-2] == before
                     and after[-2] == '--primary-release' and Path(after[-1]).resolve() == controller_primary)
        if not valid:
            raise ValueError('Maintenance controller must preserve its single exact verified --primary-release argument or append it once')
        text = text.replace(new_commands[0], old_commands[0], 1)
    if _normalized_script(text) != _normalized_script(old_text):
        raise ValueError("Replacement script changes the original command/environment contract")


def _archive_bytes(path, body):
    with path.open("xb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())


def _expanded_accounting_runner(command, **kwargs):
    # Repair alone expands cancelled/pending arrays. The immutable primary
    # numerical release and its ordinary accounting implementation are unchanged.
    command = list(command)
    if command and command[0] == "sacct" and "--array" not in command:
        command.insert(1, "--array")
    return subprocess.run(command, **kwargs)


REPAIR_NATIVE_FORMAT = "JobID%64,JobIDRaw%64,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES,NodeList%128,Start,End,User,Account,MaxRSS"


def _parse_repair_native(stdout, array_rosters, *, owner=None, account=None):
    """Keep queue/template metadata separate from actual physical allocations."""
    records, physical, step_rss = {}, {}, {}
    for line in stdout.splitlines():
        fields = line.strip().split("|")
        if len(fields) != 13:
            raise ValueError("Repair accounting lacks the complete native allocation metadata")
        ident, raw, state, exit_code, seconds, cpus, tres, nodes, start, end, user, charge_account, rss = fields
        if "." in ident:
            if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?K?", rss):
                parent = ident.split(".",1)[0]
                if parent not in step_rss or float(rss.rstrip("K")) > float(step_rss[parent].rstrip("K")):
                    step_rss[parent] = rss
            continue  # Allocation identity/cost belongs to its parent, not a step.
        if not re.fullmatch(r"[0-9]+(?:_[0-9]+)?", ident) or not raw.isdigit() or not seconds.isdigit() or not cpus.isdigit():
            raise ValueError("Unexpected repair native identity or resource measurement")
        state = state.split()[0].split("+")[0]
        if owner and user != owner or account and charge_account != account:
            raise ValueError("Repair native metadata belongs to another owner/account")
        match = re.fullmatch(r"([0-9]+)_([0-9]+)", ident)
        base = match.group(1) if match else ident
        if match and (base not in array_rosters or int(match.group(2)) not in array_rosters[base]):
            raise ValueError("Native array metadata lies outside the frozen task roster")
        never_started = (state in {"PENDING", "CANCELLED"} and exit_code == "0:0" and seconds == "0" and cpus == "0"
            and tres == "" and nodes == "None assigned" and start in {"None", "Unknown"}
            and rss in {"", "0", "0K"}
            and bool(user) and bool(charge_account)
            and (end in {"None", "Unknown"} if state == "PENDING" else bool(re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]+", end))))
        metadata = bool(match) and never_started and raw == base
        if match and raw == base and not metadata:
            # A real member may share the parent's numeric ID with its pending
            # siblings. It must have actual allocation markers, rather than one
            # changed field in an otherwise unallocated template.
            if state == "PENDING" or int(cpus) <= 0 or not tres or nodes in {"", "None assigned"} or start in {"", "None", "Unknown"}:
                raise ValueError("Array template has neither proven unused metadata nor an actual allocation")
        if not metadata:
            if raw in physical and physical[raw] != ident:
                raise ValueError("Actual physical native allocation has conflicting logical identities")
            physical[raw] = ident
        record = {"state":state,"exit_code":exit_code,"job_id_raw":raw,
            "elapsed_seconds":int(seconds),"cpus":int(cpus),"alloc_tres":tres,"node_list":nodes,
            "start":start,"end":end,"user":user,"account":charge_account,"max_rss":rss,
            "unallocated_metadata":metadata,
            "identity_basis":("Explicit never-started queue/template metadata, not a physical allocation" if metadata else
                "Unique scalar native job; zero-allocation queue record" if never_started and not match else "Actual native allocation identity")}
        if ident in records and records[ident] != record:
            raise ValueError("Conflicting repair native records")
        records[ident] = record
    for ident,rss in step_rss.items():
        if ident in records and records[ident]["unallocated_metadata"] and float(rss.rstrip("K")) > 0:
            raise ValueError("Unallocated array metadata contradicts positive native step memory")
        if ident in records and not records[ident]["unallocated_metadata"]:
            current = records[ident]["max_rss"]
            if not current or float(rss.rstrip("K")) > float(current.rstrip("K")):
                records[ident]["max_rss"] = rss
    return records


def _repair_native_states(ids, array_rosters, *, runner=subprocess.run, owner=None, account=None):
    ids = list(dict.fromkeys(map(str, ids)))
    if any(not re.fullmatch(r"[0-9]+(?:_[0-9]+)?", ident) for ident in ids):
        raise ValueError("Invalid repair native query identity")
    response = runner(["sacct","--array","-j",",".join(ids),"--format="+REPAIR_NATIVE_FORMAT,
                       "--units=K","--noheader","--parsable2"],text=True,capture_output=True)
    if response.returncode:
        raise RuntimeError("Repair native proof query failed")
    return _parse_repair_native(response.stdout,array_rosters,owner=owner,account=account)


def _repair_allocation_accounting_runner(array_rosters, *, owner=None, account=None):
    def run(command, **kwargs):
        if not command or command[0] != "sacct":
            return subprocess.run(command, **kwargs)
        ids = command[command.index("-j")+1].split(",")
        records = _repair_native_states(ids,array_rosters,owner=owner,account=account)
        rows = ["|".join([ident,record["job_id_raw"],record["state"],str(record["elapsed_seconds"]),
            str(record["cpus"]),record["alloc_tres"],record["exit_code"],record["max_rss"]])
            for ident,record in records.items() if not record["unallocated_metadata"]]
        return SimpleNamespace(returncode=0,stdout="\n".join(rows),stderr="")
    return run


def _unused_terminal_arrays(root, checks, observed):
    budget_path = root / "state/resource-budget.json"
    budget = read_json(budget_path) if budget_path.exists() else {}
    unused = {}
    for _,task,record in checks:
        if not task.get("resources",{}).get("array"):
            continue
        base = str(record["job_id"])
        indices = _array_indices(task["resources"]["array"])
        identities = {base+"_"+str(index) for index in indices}
        if identities <= set(observed) and all(observed[key].get("unallocated_metadata") and observed[key]["state"] in TERMINAL_FAILURE for key in identities):
            keys = {base,*identities}
            if keys & set(budget.get("leases",{})) or keys & set(budget.get("native_jobs",{})):
                raise ValueError("Unused array metadata conflicts with an existing lease/allocation receipt")
            facts = {key:observed[key] for key in sorted(identities)}
            unused[base] = {"status":"terminal_never_allocated_array_template", "expected_indices":sorted(indices),
                "native_records":facts,"native_records_sha256":canonical_hash(facts),"cpu_core_hours":0.,"gpu_hours":0.,
                "cost_basis":"Explicit complete zero-elapsed/zero-CPU/no-node/no-start native metadata; excluded from physical allocation accounting"}
    return unused


def _runtime_native_proofs(adjustments, original, ledger, observed):
    proofs = {}
    for name, change in adjustments.items():
        ident = str(ledger["tasks"][name]["job_id"])
        facts = observed.get(ident, {})
        tres = dict(pair.split("=", 1) for pair in facts.get("alloc_tres", "").split(",") if "=" in pair)
        if (str(change["failed_job_id"]) != ident or facts.get("state") != "TIMEOUT"
                or type(facts.get("elapsed_seconds")) is not int
                or facts["elapsed_seconds"] < change["old_hours"] * 3600
                or facts.get("cpus") != original[name]["resources"]["cpus"]
                or tres.get("cpu") != str(facts.get("cpus"))
                or any(key.startswith("gres/gpu") and value != "0" for key,value in tres.items())
                or not re.fullmatch(r"[0-9]+", str(facts.get("job_id_raw", "")))
                or facts.get("node_list") in {None, "", "None assigned"}
                or facts.get("start") in {None, "", "None", "Unknown"}
                or facts.get("end") in {None, "", "None", "Unknown"}
                or facts.get("unallocated_metadata")):
            raise ValueError("Audit runtime increase requires matching native allocated eight-hour TIMEOUT evidence")
        proofs[name] = {"job_id":ident, "native_record":copy.deepcopy(facts),
                        "native_record_sha256":canonical_hash(facts)}
    return proofs


def _runtime_readmission(root, rid, phase, plan, ledger, config, identity, observed, accounted):
    """Called under the budget guard; reserve all registered unfinished bounds.

    Missing final native proofs are conservatively treated as unfinished. Active
    leases outside the registered DAG are also reserved; registered leases are
    already represented by their full task bounds and are not added twice.
    """
    from .pipeline import NODES, prospective_registered_budget
    budget_path = root / "state/resource-budget.json"
    if not budget_path.is_file():
        raise ValueError("Audit runtime re-admission requires the durable resource budget")
    budget = read_json(budget_path)
    plans, ledgers, hashes, members = {}, {}, {}, set()
    # Only canonical controller phases are registered workload. Archived and
    # subordinate replacement ledgers must not be counted as additional jobs.
    for registered_phase in ["managed-initial", *[name for name, _, _ in NODES]]:
        path = root / "state" / (rid + "-" + registered_phase + "-submission.json")
        if not path.is_file():
            continue
        plan_path = root / "plans" / (rid + "-" + registered_phase) / "plan.json"
        current_plan = plan if registered_phase == phase else read_json(plan_path)
        current_ledger = ledger if registered_phase == phase else read_json(path)
        if (current_plan.get("release_identity") != identity
                or current_plan.get("phase") != registered_phase or Path(current_plan["root"]).resolve() != root
                or current_plan.get("config_sha256") != canonical_hash(config)
                or current_ledger.get("plan_sha256") != canonical_hash(current_plan)
                or current_ledger.get("source_sha256") != identity["sha256"]
                or current_ledger.get("config_sha256") != canonical_hash(config)):
            raise ValueError("Audit budget re-admission found an unbound registered phase")
        plans[registered_phase], ledgers[registered_phase] = current_plan, current_ledger
        hashes[registered_phase] = {"plan_sha256":canonical_hash(current_plan), "ledger_sha256":canonical_hash(current_ledger)}
        for task in current_plan["tasks"]:
            resources = task["resources"]
            if (type(resources.get("hours")) is not int or not 1 <= resources["hours"] <= 168
                    or type(resources.get("cpus")) is not int or resources["cpus"] < 1):
                raise ValueError("Registered workload has an unbounded allocation")
            record = current_ledger.get("tasks", {}).get(task["name"], {})
            ident = str(record.get("job_id", ""))
            members.add(ident)
            if resources.get("array"):
                members.update(ident+"_"+str(index) for index in _array_indices(resources["array"]))
    if phase not in plans:
        raise ValueError("Amended phase is absent from the canonical registered workload")
    states = dict(observed)
    for ident, facts in budget.get("native_jobs", {}).items():
        if facts.get("basis") == "sacct final allocated wall time" and facts.get("state") in TERMINAL_FAILURE | {"COMPLETED"}:
            if ident in states and (states[ident]["state"], states[ident]["exit_code"]) != (facts["state"], facts["exit_code"]):
                raise ValueError("Audit re-admission found conflicting final native state evidence")
            states.setdefault(ident, {"state":facts["state"], "exit_code":facts["exit_code"]})
    spent = {key:float(budget.get(key, accounted.get(key, float("nan"))))
             for key in ("spent_cpu_core_hours", "spent_gpu_hours")}
    if any(not math.isfinite(value) or value < 0 for value in spent.values()):
        raise ValueError("Audit re-admission requires finite nonnegative native spent costs")
    totals = prospective_registered_budget(plans, ledgers, states, spent)
    outside = {}
    for ident, lease in budget.get("leases", {}).items():
        logical = budget.get("native_raw_aliases", {}).get(ident, ident)
        if lease.get("state") != "running" or logical in members:
            continue
        if logical in budget.get("native_jobs", {}):
            raise ValueError("Active external lease contradicts a final native allocation")
        hours, cpus, gpus = lease.get("max_hours"), lease.get("cpus"), lease.get("gpus")
        if (not isinstance(hours, (int, float)) or isinstance(hours, bool) or not math.isfinite(hours) or not 0 < hours <= 168
                or type(cpus) is not int or cpus < 1 or type(gpus) is not int or gpus not in {0,1}):
            raise ValueError("Active external lease is not a bounded resource reservation")
        outside[ident] = copy.deepcopy(lease)
        totals["cpu_core_hours"] += cpus * hours
        totals["gpu_hours"] += gpus * hours
    for total, cap in (("cpu_core_hours", "cpu_core_hours_cap"), ("gpu_hours", "gpu_hours_cap")):
        ceiling = config["resources"].get(cap)
        if (not isinstance(ceiling, (int, float)) or isinstance(ceiling, bool) or not math.isfinite(ceiling) or ceiling <= 0
                or totals[total] > ceiling):
            raise ValueError("Raised audit runtime exceeds the registered workload resource budget")
    return {"status":"admitted", "config_sha256":canonical_hash(config), "source_sha256":identity["sha256"],
            "resource_budget_sha256":canonical_hash(budget), "registered_workload_hashes":hashes,
            "prospective_registered_budget":totals, "external_active_leases":outside,
            "rule":"Native spent plus full unfinished registered allocation bounds and nonduplicated external active leases; checked under resource-budget guard"}


def register_amendment(descriptor_path, *, native=None, accountant=None):
    """Register already-confirmed replacement jobs, with recoverable pair writes.

    Production queries native state and reconciles old allocation costs. Test
    injection supplies explicit synthetic evidence. This function never submits,
    cancels, copies numerical results, changes source pins or edits a release.
    """
    descriptor_path = Path(descriptor_path).resolve()
    descriptor = read_json(descriptor_path)
    root = Path(descriptor["root"]).resolve()
    _inside(descriptor_path, root / "plans")
    ident, phase = descriptor["amendment_id"], descriptor["phase"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", ident) or phase not in {"managed-initial", "rna-full", "molecular-corroboration"}:
        raise ValueError("Unsupported scoped DAG amendment identity/phase")
    if not descriptor.get("reason"):
        raise ValueError("An explicit repair reason is required")
    config = read_json(Path(descriptor["primary_release"]) / "configs/study.json")
    if Path(config["paths"]["root"]).resolve() != root:
        raise ValueError("Repair descriptor changes the frozen study root")
    primary, release, identity = _release(descriptor["primary_release"], config)
    rid = release["release_id"]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", rid):
        raise ValueError("Unsafe primary release identity")
    base = root / "state/pipeline" / rid
    archive = base / "dag-amendments" / ident
    plan_path = root / "plans" / (rid + "-" + phase) / "plan.json"
    ledger_path = root / "state" / (rid + "-" + phase + "-submission.json")
    new_plan_path = _inside(descriptor["replacement_plan"], root / "plans")
    new_ledger_path = _inside(descriptor["replacement_ledger"], root / "state")
    descriptor_hash = canonical_hash(descriptor)
    with exclusive_file_lock(root / "state/pipeline/dag-amendment-registration.mutex"):
        journal_path = archive / "registration.json"
        journal = read_json(journal_path) if journal_path.exists() else None
        if journal:
            if journal.get("descriptor_sha256") != descriptor_hash:
                raise ValueError("Existing amendment differs; never replay another registration")
            old_plan = read_json(archive / "original-plan.json")
            old_ledger = read_json(archive / "original-ledger.json")
            for name, expected in journal["archived_file_sha256"].items():
                if digest(archive / name) != expected:
                    raise ValueError("Archived amendment evidence changed")
            if journal["status"] == "complete":
                if canonical_hash(read_json(plan_path)) != journal["amended_plan_sha256"] or canonical_hash(read_json(ledger_path)) != journal["amended_ledger_sha256"]:
                    raise ValueError("Completed amendment's canonical registration changed")
                return journal
        else:
            old_plan, old_ledger = read_json(plan_path), read_json(ledger_path)
        if old_plan.get("release_identity") != identity or old_plan.get("config_sha256") != canonical_hash(config):
            raise ValueError("Original primary plan belongs to another release/configuration")
        if old_plan.get("phase") != phase or Path(old_plan["root"]).resolve() != root:
            raise ValueError("Original plan phase/study namespace mismatch")
        if old_ledger.get("plan_sha256") != canonical_hash(old_plan) or old_ledger.get("source_sha256") != identity["sha256"]:
            raise ValueError("Original ledger is not bound to its immutable primary plan")
        replacement, submitted = read_json(new_plan_path), read_json(new_ledger_path)
        default_path, _, default_identity = _release(descriptor.get("replacement_plan_release", str(primary)), config)
        if replacement.get("release_identity") != default_identity or replacement.get("config_sha256") != canonical_hash(config):
            raise ValueError("Replacement plan release/configuration mismatch")
        if Path(replacement["root"]).resolve() != root:
            raise ValueError("Replacement plan changes the frozen study namespace")
        if submitted.get("plan_sha256") != canonical_hash(replacement) or submitted.get("config_sha256") != canonical_hash(config) or submitted.get("source_sha256") != default_identity["sha256"]:
            raise ValueError("Replacement submission ledger differs from its frozen plan")
        targets = set(descriptor["execution_releases"])
        original = {task["name"]: task for task in old_plan["tasks"]}
        candidates = {task["name"]: task for task in replacement["tasks"]}
        if len(original) != len(old_plan["tasks"]) or len(candidates) != len(replacement["tasks"]) or set(submitted.get("tasks", {})) != set(candidates):
            raise ValueError("Duplicate or unregistered replacement task/ledger roster")
        if not targets or not targets <= set(original) or not targets <= set(candidates):
            raise ValueError("Replacement tasks do not match original named nodes")
        if phase == "managed-initial" and not targets <= INITIAL_TARGETS:
            raise ValueError("Initial repair may replace only failed preprocessing and controller nodes")
        if phase == 'molecular-corroboration' and (targets != {'mantis-orfs', 'mantis-orthology'}
                or descriptor.get('mantis_reference_recovery') is None):
            raise ValueError('Mantis corroboration repair requires its explicit qualified reference adapter and both auxiliary nodes')
        runtime_adjustments = _runtime_adjustments(descriptor, original, candidates, targets)
        for name in set(candidates) - targets:
            if candidates[name] != original.get(name) or submitted.get("tasks", {}).get(name) != old_ledger.get("tasks", {}).get(name):
                raise ValueError("An unaffected planned task or job was altered")
        controllers_plan = read_json(root / "plans" / (rid + "-managed-initial") / "plan.json")
        controllers_ledger = read_json(root / "state" / (rid + "-managed-initial-submission.json"))
        if phase == "managed-initial":
            controllers_plan, controllers_ledger = old_plan, old_ledger
        controllers = {task["name"]: task for task in controllers_plan["tasks"] if task["name"].startswith("pipeline-controller-")}
        if len(controllers) != 4:
            raise ValueError("The complete four-segment primary controller roster is required")
        controller_maintenance = _controller_maintenance(descriptor, targets, controllers, primary, identity, config, root)
        rna_recovery = _rna_recovery_binding(descriptor, targets, primary, identity, config, root, original, old_ledger)
        mantis_recovery = _mantis_recovery_binding(descriptor, targets, primary, identity, config, root, original, old_ledger)
        checks = [(name, original[name], old_ledger["tasks"][name]) for name in targets]
        checks += [(name, task, old_ledger["tasks"][name]) for name, task in original.items()] if phase == "rna-full" else []
        checks += [(name, task, controllers_ledger["tasks"][name]) for name, task in controllers.items()]
        jobs = [str(record.get("job_id", "")) for _, _, record in checks]
        new_ids = []
        releases = {}
        for name in targets:
            candidate = candidates[name]
            if rna_recovery is not None and candidate.get('depends', []) != original[name].get('depends', []):
                raise ValueError('RNA recovery must retain every original named scientific dependency')
            if mantis_recovery is not None and candidate.get('depends', []) != original[name].get('depends', []):
                raise ValueError('Mantis reference recovery must retain every original named scientific dependency')
            if mantis_recovery is not None and any(candidate.get(key, []) != original[name].get(key, [])
                    for key in ('external_dependencies', 'external_dependency_specs')):
                raise ValueError('Mantis reference recovery must retain the original external scientific dependencies')
            if candidate.get("resources") != original[name].get("resources") and name not in runtime_adjustments:
                raise ValueError("Replacement changed a node's frozen resource/array contract")
            execution, execution_record, execution_identity = _release(descriptor["execution_releases"][name], config)
            if execution_identity != identity:
                if controller_maintenance is not None and name in controllers:
                    if execution_identity != controller_maintenance['framework_source_identity']:
                        raise ValueError('Controller execution does not match its qualified framework')
                elif name == 'source-audit':
                    _preprocessing_release(primary, execution, identity, execution_identity)
                else:
                    raise ValueError("Only source-audit or explicitly qualified primary controllers may use another execution release")
            _check_script(candidate, original[name], execution, root, runtime_adjustments.get(name),
                          primary if controller_maintenance is not None else None,
                          rna_recovery if name == 'rna-counts' else None,
                          mantis_recovery if name == 'mantis-orfs' else None)
            record = submitted.get("tasks", {}).get(name, {})
            if (record.get("state") != "submitted" or not re.fullmatch(r"[0-9]+", str(record.get("job_id", "")))
                    or record.get("script_sha256") != candidate["script_sha256"]):
                raise ValueError("Replacement submission is absent, ambiguous or script-mismatched")
            required = {str(value) for value in descriptor.get("required_external_dependencies", {}).get(name, [])}
            if name == "source-audit" and execution_identity != identity and len(required) < 2:
                raise ValueError("Preprocessing repair requires qualification and preservation dependencies")
            if (candidate.get("dependency_kind", "afterok") != original[name].get("dependency_kind", "afterok")
                    or record.get("dependency_kind", "afterok") != candidate.get("dependency_kind", "afterok")):
                raise ValueError("Replacement changed the original native dependency policy")
            confirmed = set(map(str, record.get("issued_native_dependencies", []))) | {str(proof["job_id"]) for proof in record.get("completed_native_dependencies", [])}
            if not required <= confirmed:
                raise ValueError("Replacement omitted a required qualification/preservation dependency")
            if controller_maintenance is not None:
                from .slurm import _verify_completed_dependency
                qualifier = str(controller_maintenance['qualification_job_id'])
                proofs = [proof for proof in record.get('completed_native_dependencies', [])
                          if str(proof.get('job_id')) == qualifier
                          and proof.get('dependency_kind') == candidate.get('dependency_kind', 'afterok')
                          and proof.get('array') is None]
                planned_proofs = candidate.get('completed_external_dependencies', [])
                if qualifier not in required or len(proofs) != 1 or proofs[0] not in planned_proofs:
                    raise ValueError('Every maintenance controller requires its unchanged positive completed qualification proof')
                _verify_completed_dependency(root, proofs[0])
            if rna_recovery is not None and name == 'rna-counts':
                from .slurm import _verify_completed_dependency
                for key in ('qualification_job_id', 'preparation_job_id'):
                    ident = rna_recovery[key]
                    proofs = [proof for proof in record.get('completed_native_dependencies', [])
                              if str(proof.get('job_id')) == ident and proof.get('array') is None
                              and proof.get('dependency_kind', 'afterok') == 'afterok']
                    if (candidate.get('dependency_kind', 'afterok') != 'afterok' or ident not in required
                            or len(proofs) != 1 or proofs[0] not in candidate.get('completed_external_dependencies', [])):
                        raise ValueError('RNA count recovery requires unchanged positive qualification and preservation dependencies')
                    _verify_completed_dependency(root, proofs[0])
            if mantis_recovery is not None and name == 'mantis-orfs':
                from .slurm import _verify_completed_dependency
                for key in ('qualification_job_id', 'preparation_job_id'):
                    dependency_id = mantis_recovery[key]
                    proofs = [proof for proof in record.get('completed_native_dependencies', [])
                              if str(proof.get('job_id')) == dependency_id and proof.get('array') is None
                              and proof.get('dependency_kind', 'afterok') == 'afterok']
                    if (candidate.get('dependency_kind', 'afterok') != 'afterok' or dependency_id not in required
                            or len(proofs) != 1 or proofs[0] not in candidate.get('completed_external_dependencies', [])):
                        raise ValueError('Mantis ORF recovery requires unchanged positive qualification and preservation dependencies')
                    _verify_completed_dependency(root, proofs[0])
            if mantis_recovery is not None and name == 'mantis-orthology':
                parent = str(submitted['tasks']['mantis-orfs'].get('job_id', ''))
                if (candidate.get('depends', []) != ['mantis-orfs']
                        or candidate.get('dependency_kind', 'afterok') != 'afterok'
                        or parent not in set(map(str, record.get('issued_native_dependencies', [])))):
                    raise ValueError('Mantis orthology requires its unchanged afterok dependency on the new held ORF adapter')
            new_ids.append(str(record["job_id"]))
            releases[name] = {"path": str(execution), "release_id": execution_record["release_id"],
                              "source_identity": execution_identity, "config_sha256": canonical_hash(config)}
        if len(new_ids) != len(set(new_ids)) or set(new_ids) & set(jobs):
            raise ValueError("Replacement aliases an old or another replacement allocation")
        rosters = {}
        for _,task,record in checks:
            if task.get("resources",{}).get("array"):rosters[str(record["job_id"])] = _array_indices(task["resources"]["array"])
        for name in targets:
            if candidates[name]["resources"].get("array"):rosters[submitted["tasks"][name]["job_id"]] = _array_indices(candidates[name]["resources"]["array"])
        owner, account = os.environ.get("USER"), config.get("paths",{}).get("cpu_account")
        qualification_ids = [str(controller_maintenance['qualification_job_id'])] if controller_maintenance else []
        if rna_recovery:
            qualification_ids.extend(rna_recovery[key] for key in
                                     ('qualification_job_id', 'capture_job_id', 'preparation_job_id'))
        if mantis_recovery:
            qualification_ids.extend(mantis_recovery[key] for key in
                                     ('qualification_job_id', 'capture_job_id', 'preparation_job_id'))
        qualification_ids = list(dict.fromkeys(qualification_ids))
        observed = _repair_native_states(list(dict.fromkeys(jobs + new_ids + qualification_ids)), rosters, owner=owner, account=account) if native is None else native
        for qualification_id in qualification_ids:
            proof = observed.get(qualification_id, {})
            if (proof.get('state') != 'COMPLETED' or proof.get('exit_code') != '0:0'
                    or proof.get('unallocated_metadata') is True
                    or not math.isfinite(float(proof.get('elapsed_seconds', 0)))
                    or not math.isfinite(float(proof.get('cpus', 0)))
                    or float(proof.get('elapsed_seconds', 0)) <= 0 or float(proof.get('cpus', 0)) <= 0):
                raise ValueError('Repair framework qualification/preservation lacks positive allocated native completion')
        for name, task, record in checks:
            if not _task_terminal(record, observed, task):
                raise ValueError("A replaced allocation or primary controller is not positively terminal: " + name)
        for name in targets:
            if not name.startswith("pipeline-controller-") and _task_status(old_ledger["tasks"][name], observed, original[name]) != "failed":
                raise ValueError("Completed valid work must be retained rather than replaced: " + name)
            candidate, record = candidates[name], submitted["tasks"][name]
            expected = ({record["job_id"] + "_" + str(i) for i in _array_indices(candidate["resources"]["array"])}
                        if candidate["resources"].get("array") else {record["job_id"]})
            if not expected <= set(observed):
                raise ValueError("Replacement native acknowledgement/roster is incomplete")
            if name.startswith("pipeline-controller-"):
                if "#SBATCH --hold" not in Path(candidate["script"]).read_text(encoding="utf8").splitlines() or observed[record["job_id"]]["state"] != "PENDING":
                    raise ValueError("Replacement controllers must remain explicitly held until registration completes")
            if name in runtime_adjustments:
                if "#SBATCH --hold" not in Path(candidate["script"]).read_text(encoding="utf8").splitlines() or observed[record["job_id"]]["state"] != "PENDING":
                    raise ValueError("Runtime-adjusted source audit must remain explicitly held and pending until budget re-admission and registration complete")
            if rna_recovery is not None and name == 'rna-counts':
                if ('#SBATCH --hold' not in Path(candidate['script']).read_text(encoding='utf8').splitlines()
                        or any(observed[ident]['state'] != 'PENDING' for ident in expected)):
                    raise ValueError('RNA recovery count array must remain explicitly held and pending until registration and re-admission complete')
            if mantis_recovery is not None and name == 'mantis-orfs':
                if ('#SBATCH --hold' not in Path(candidate['script']).read_text(encoding='utf8').splitlines()
                        or candidate.get('resources', {}).get('array') is not None
                        or any(observed[ident]['state'] != 'PENDING' for ident in expected)):
                    raise ValueError('Mantis ORF adapter must remain explicitly held and pending until canonical registration completes')
        runtime_native_proofs = _runtime_native_proofs(runtime_adjustments, original, old_ledger, observed)
        unused_arrays = _unused_terminal_arrays(root,checks,observed)
        amended_plan, amended_ledger = copy.deepcopy(old_plan), copy.deepcopy(old_ledger)
        preserved_dependencies = {}
        from .slurm import _verify_completed_dependency
        for task in amended_plan["tasks"]:
            name = task["name"]
            if name in targets:
                original_completed = copy.deepcopy(original[name].get("completed_external_dependencies", []))
                for proof in original_completed:
                    # Original logical preconditions remain requirements even
                    # when the replacement submitter omitted their already-final
                    # proof. Revalidate facts; never pretend it issued a native
                    # dependency or checked this proof at acknowledgement time.
                    _verify_completed_dependency(root, proof)
                task.update(copy.deepcopy(candidates[name]))
                task["depends"] = copy.deepcopy(original[name].get("depends", []))
                task["execution_release"] = releases[name]
                amended_ledger["tasks"][name] = copy.deepcopy(submitted["tasks"][name])
                amended_ledger["tasks"][name]["execution_release"] = releases[name]
                if original_completed:
                    union = {canonical_hash(proof):proof for proof in [
                        *task.get("completed_external_dependencies", []), *original_completed]}
                    task["completed_external_dependencies"] = list(union.values())
                    amended_ledger["tasks"][name]["preserved_original_completed_dependencies"] = original_completed
                    preserved_dependencies[name] = original_completed
        amendment = {"amendment_id": ident, "reason": descriptor["reason"], "descriptor_sha256": descriptor_hash,
            "original_plan_sha256": canonical_hash(old_plan), "original_ledger_sha256": canonical_hash(old_ledger),
            "replacement_plan_sha256": canonical_hash(replacement), "replacement_ledger_sha256": canonical_hash(submitted),
            "replaced_tasks": sorted(targets), "execution_releases": releases, "archive": str(archive),
            "preserved_original_completed_dependencies": preserved_dependencies,
            "dependency_preservation_rule": "Original positive completed preconditions revalidated at registration; actual replacement acknowledgement/command/dependency-resolution records remain unchanged"}
        if runtime_adjustments:
            amendment.update(runtime_adjustments=copy.deepcopy(runtime_adjustments), runtime_native_proofs=runtime_native_proofs)
        if controller_maintenance:
            amendment['controller_maintenance'] = controller_maintenance
        if rna_recovery:
            amendment['rna_followup' if rna_recovery.get('guard_module') == 'rna_followup' else 'rna_recovery'] = rna_recovery
        if mantis_recovery:
            amendment['mantis_reference_recovery'] = mantis_recovery
        amended_plan.setdefault("dag_amendments", []).append(amendment)
        amended_ledger.setdefault("dag_amendments", []).append(amendment)
        amended_ledger["plan_sha256"] = canonical_hash(amended_plan)
        prepared = {"amended_plan_sha256": canonical_hash(amended_plan), "amended_ledger_sha256": canonical_hash(amended_ledger)}
        if journal is None:
            archive.mkdir(parents=True, exist_ok=False)
            originals = {"original-plan.json": plan_path.read_bytes(), "original-ledger.json": ledger_path.read_bytes(),
                "descriptor.json": descriptor_path.read_bytes(), "replacement-plan.json": new_plan_path.read_bytes(),
                "replacement-ledger.json": new_ledger_path.read_bytes()}
            phase_journal = base / (phase + "-plan-journal.json")
            if phase_journal.exists():originals["original-phase-journal.json"] = phase_journal.read_bytes()
            for name, body in originals.items():_archive_bytes(archive / name, body)
            journal = dict(amendment, **prepared, status="prepared", archived_file_sha256={name:digest(archive/name) for name in originals},
                           terminal_native_proofs={key: observed[key] for key in observed if key in set(jobs) or any(key.startswith(job+"_") for job in jobs)},
                           unused_old_array_templates=unused_arrays)
            atomic_json(journal_path, journal)
        elif any(journal.get(key) != value for key, value in prepared.items()):
            raise ValueError("Prepared amendment changed during recovery")
        for current, original_document, amended_document in [(plan_path, old_plan, amended_plan), (ledger_path, old_ledger, amended_ledger)]:
            if canonical_hash(read_json(current)) not in {canonical_hash(original_document), canonical_hash(amended_document)}:
                raise ValueError("Canonical registration changed during repair; preserve ambiguity")
        if accountant is None:
            from .budget import native_accounting
            accountant = lambda study, ids: native_accounting(study, ids,
                runner=_repair_allocation_accounting_runner(rosters,owner=owner,account=account))
        accounting_ids = [ident for ident in dict.fromkeys(jobs) if ident not in unused_arrays]
        accounted = accountant(root, accounting_ids)
        if accounted.get("pending_native_records"):
            raise ValueError("Old allocation cost proof remains pending; no registration committed")
        if rna_recovery is not None:
            if _rna_recovery_binding(descriptor, targets, primary, identity, config, root, original, old_ledger) != rna_recovery:
                raise ValueError('RNA recovery binding changed during native registration accounting')
        if mantis_recovery is not None:
            if _mantis_recovery_binding(descriptor, targets, primary, identity, config, root, original, old_ledger) != mantis_recovery:
                raise ValueError('Mantis reference recovery binding changed during native registration accounting')
        budget_guard = exclusive_file_lock(root / "state/resource-budget.mutex") if runtime_adjustments else nullcontext()
        with budget_guard:
            if runtime_adjustments:
                journal["runtime_readmission"] = _runtime_readmission(root, rid, phase, amended_plan, amended_ledger,
                    config, identity, observed, accounted)
            journal.update(status="committing", old_allocation_accounting=accounted)
            atomic_json(journal_path, journal)
            atomic_json(plan_path, amended_plan)
            atomic_json(ledger_path, amended_ledger)
            phase_journal = base / (phase + "-plan-journal.json")
            if phase_journal.exists():
                previous = read_json(phase_journal)
                if not (archive / "original-phase-journal.json").exists():_archive_bytes(archive / "original-phase-journal.json", phase_journal.read_bytes())
                atomic_json(phase_journal, {**previous, "state":"submitted", "source_sha256":identity["sha256"],
                    "plan_sha256":canonical_hash(amended_plan), "ledger":str(ledger_path), "dag_amendment":str(journal_path)})
            journal["status"] = "complete"
            atomic_json(journal_path, journal)
        return journal
