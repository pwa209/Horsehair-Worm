"""Immutable scientific inputs, atomic receipts, and independent-unit contracts."""
from __future__ import annotations
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.partial')
    with temp.open('w', encoding='utf8') as f:
        json.dump(value, f, indent=2, sort_keys=True, allow_nan=False)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    deadline = time.monotonic() + 2
    while True:
        try:
            os.replace(temp, path)
            break
        except PermissionError:
            # Windows readers/scanners can temporarily hold a file without
            # delete sharing. Retry this same prepared receipt, never the action
            # it records. Genuine access failures preserve both files and fail.
            if sys.platform != 'win32' or time.monotonic() >= deadline:
                raise
            time.sleep(.05)


def read_json(path: Path) -> Any:
    return json.loads(Path(path).read_text(encoding='utf8'))


def native_job_id() -> str | None:
    """Use the same array-task identity as native Slurm accounting receipts."""
    parent, index = os.environ.get('SLURM_ARRAY_JOB_ID'), os.environ.get('SLURM_ARRAY_TASK_ID')
    if parent is not None and index is not None:
        if not parent.isdigit() or not index.isdigit():
            raise ValueError('Invalid native Slurm array identity')
        return parent + '_' + index
    value = os.environ.get('SLURM_JOB_ID')
    if value is not None and not value.isdigit():
        raise ValueError('Invalid native Slurm job identity')
    return value


@contextmanager
def exclusive_file_lock(path: Path, timeout: float = 30):
    """Lock a permanent inode; process death releases the kernel lock.

    The guard file must never be unlinked or replaced. The scientific receipt
    remains a separate atomically replaced file protected by this guard.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as handle:
        if os.name == 'nt':
            import msvcrt
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b'0');handle.flush()
            def acquire():
                handle.seek(0);msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            def release():
                handle.seek(0);msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            def acquire():
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            def release():
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        deadline = time.monotonic() + timeout
        while True:
            try:
                acquire();break
            except (BlockingIOError, PermissionError):
                if time.monotonic() >= deadline:
                    raise TimeoutError('Live process holds the receipt guard; preserve its owner')
                time.sleep(min(.2, max(0, deadline-time.monotonic())))
        try:
            yield
        finally:
            release()


def source_identity(root: Path) -> dict:
    root = Path(root)
    included = {}
    for directory in ['src', 'scripts', 'configs', 'tests']:
        for p in sorted((root / directory).rglob('*')):
            if p.is_file() and '__pycache__' not in p.parts and p.suffix not in ['.pyc', '.partial']:
                included[p.relative_to(root).as_posix()] = digest(p)
    for name in ['pyproject.toml', 'requirements-narval.txt']:
        if (root / name).is_file():
            included[name] = digest(root / name)
    return {'files': included, 'sha256': canonical_hash(included)}


def validate_config(config: dict) -> None:
    if config.get('schema_version') != 1:
        raise ValueError('Unsupported config schema')
    r = config['resources']
    if not 1 <= r['cpu_ceiling'] <= 100 or r['gpu_concurrency'] != 1:
        raise ValueError('Study resource ceiling must be bounded')
    if r['cpu_core_hours_cap'] > 20000 or r['gpu_hours_cap'] > 400:
        raise ValueError('Proposal budget exceeded; prospective resource amendment required')
    if r['scratch_budget_bytes'] > 6_000_000_000_000:
        raise ValueError('Scratch reservation exceeds initial admitted budget')
    a = config['agents']
    for field in ['development_seeds','final_seeds','pilot_seeds','architectures','envs','lr_grid','strengths','ranks']:
        if len(a[field])!=len(set(a[field])):
            raise ValueError(f'Duplicate agent {field} would change replication or matching')
    if set(a['architectures']) != {'rnn','gru','modular'} or set(a['envs']) != {'foraging','corridor','delayed_choice'}:
        raise ValueError('Primary architecture or task set changed')
    if set(a['development_seeds']) & set(a['final_seeds']):
        raise ValueError('Development and final agent seeds overlap')
    if set(a['pilot_seeds']) & (set(a['development_seeds']) | set(a['final_seeds'])):
        raise ValueError('Pilot seeds overlap scientific development or final replication')
    if len(a['final_seeds']) != 20 or len(a['development_seeds']) != 5:
        raise ValueError('Primary design requires twenty final and five development seeds per cell')
    if len(a['architectures']) != 3 or len(a['envs']) != 3:
        raise ValueError('Primary recurrent design requires three architectures and three tasks')
    if config['inference']['primary_family'] != ['H1', 'H2', 'H3']:
        raise ValueError('Primary multiplicity family changed')


def verify_unique_units(frame, columns: list[str]) -> None:
    if frame[columns].isna().any().any() or frame.duplicated(columns).any():
        raise ValueError(f'Missing or duplicate independent-unit keys: {columns}')


def assert_disjoint(train, test, key: str) -> None:
    overlap = set(train[key].astype(str)) & set(test[key].astype(str))
    if overlap:
        raise ValueError(f'Holdout leakage in {key}: {sorted(overlap)[:10]}')


@contextmanager
def stage_receipt(output: Path, config: dict, stage: str, inputs: list[Path] | None = None):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    signatures = {str(p): digest(p) for p in inputs or []}
    identity = {'stage': stage, 'config_sha256': canonical_hash(config), 'inputs': signatures}
    completed = output / 'completed.json'
    if completed.exists():
        old = read_json(completed)
        if any(old.get(k) != v for k, v in identity.items()):
            raise ValueError('Completed output has different source/configuration; use a fresh namespace')
        raise FileExistsError(f'Already completed: {output}')
    lock = output / '.stage.lock'
    with lock.open('x', encoding='utf8') as f:
        json.dump({'pid': os.getpid(), 'host': platform.node(), 'started_unix': time.time()}, f)
    started = time.time()
    record = {**identity, 'started_unix': started, 'evidence_class': 'computational_execution'}
    atomic_json(output / 'started.json', record)
    try:
        yield record
    except BaseException as exc:
        atomic_json(output / 'failed.json', {**record, 'elapsed_seconds': time.time() - started, 'error_type': type(exc).__name__, 'error': str(exc)})
        raise
    else:
        atomic_json(completed, {**record, 'elapsed_seconds': time.time() - started, 'status': 'complete'})
    finally:
        lock.unlink(missing_ok=True)


def environment_record() -> dict:
    import importlib.metadata as metadata
    packages = {}
    for name in ['numpy', 'scipy', 'pandas', 'torch', 'scikit-learn', 'pyarrow', 'h5py', 'openpyxl', 'statsmodels', 'pytest']:
        try:
            packages[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            packages[name] = None
    return {'python': sys.version, 'platform': platform.platform(), 'packages': packages}

