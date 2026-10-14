"""Finite, explicit ENA FTP transport using unchanged primary source checksums.

Run --launch only on the failed worker's narval1 transfer endpoint after native
preservation. No extraction or scientific analysis runs here. FTP is a transport
route: the frozen canonical HTTPS URL and provider checksum remain unchanged.
"""
from __future__ import annotations
import argparse
import ftplib
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import signal
import socket
import subprocess
import sys
import time
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit

SOURCES = ['ant_rna', 'ant_reference', 'mantis_rna', 'fly_reference']
MAX_SECONDS = 120 * 3600
BANDWIDTH = 64 * 1024**2


class TransferError(RuntimeError):
    pass


class TransferDeadline(BaseException):
    """The global cutoff cannot be swallowed by ordinary network retries."""


def ftp_route(item):
    url = item['url']
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise TransferError('Invalid canonical ENA source URL') from error
    path = unquote(parsed.path)
    if (item.get('kind') != 'fastq' or parsed.scheme != 'https'
        or parsed.hostname != 'ftp.sra.ebi.ac.uk' or port is not None
        or parsed.username or parsed.password or parsed.query or parsed.fragment
        or any(c in url or c in path for c in ('\r', '\n', '\x00'))
        or not re.fullmatch(r'/vol1/fastq/[A-Za-z0-9_./-]+\.fastq\.gz', path)
        or '..' in PurePosixPath(path).parts):
        raise TransferError('Only canonical credential-free ENA /vol1/fastq HTTPS identities may route through FTP')
    if (not isinstance(item.get('bytes'), int) or isinstance(item['bytes'], bool) or item['bytes'] <= 0
        or not re.fullmatch(r'md5:[a-fA-F0-9]{32}', item.get('provider_checksum', ''))):
        raise TransferError('FTP FASTQs require an exact pinned byte count and provider MD5')
    return path, 'ftp://ftp.sra.ebi.ac.uk' + path


def _snapshot(path):
    s = path.stat()
    return {'device': s.st_dev, 'inode': s.st_ino, 'bytes': s.st_size, 'mtime_ns': s.st_mtime_ns}


def _ftp_snapshot(ftp, path, expected):
    ftp.voidcmd('TYPE I')
    size = ftp.size(path)
    stamp = ftp.sendcmd('MDTM ' + path)
    if size != expected or not re.fullmatch(r'213 [0-9]{14}(?:\.[0-9]+)?', stamp):
        raise TransferError('FTP binary SIZE/MDTM does not establish the pinned representation')
    return {'ftp_size': size, 'ftp_mdtm': stamp[4:]}


def download_ftp_asset(item, root, helpers, *, ftp_factory=ftplib.FTP, deadline=None,
                       bandwidth=BANDWIDTH, sleeper=time.sleep):
    """One transfer attempt; only self-bound FTP prefixes may resume.

    Stream digests are independently checked against file bytes before a
    no-overwrite promotion. Rejected bytes and their diagnostics remain intact.
    """
    if not math.isfinite(bandwidth) or not 0 < bandwidth <= BANDWIDTH:
        raise ValueError('FTP bandwidth must be positive and at most 64MiB/s')
    core, acquire = helpers.core, helpers.acquire
    path, route = ftp_route(item)
    root = Path(root).resolve()
    lexical = root / item['relative_path']
    if lexical.is_symlink():
        raise TransferError('A source target symlink cannot be adopted')
    target = acquire.safe_destination(root, item['relative_path'])
    if target.exists():
        result = acquire.verify_file(target, item)
        return {**result, 'transfer_route': 'existing verified file; no download', 'canonical_url': item['url']}
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + '.part')
    sidecar = partial.with_name(partial.name + '.json')
    diagnostic = partial.with_name(partial.name + '.integrity.json')
    promoted_sidecar = target.with_name(target.name + '.ftp.json')
    if promoted_sidecar.exists() or promoted_sidecar.is_symlink():
        raise TransferError('Preserve an existing FTP promotion history when its final source is absent')
    signature = acquire.canonical_hash(item)
    contexts = {'md5': hashlib.md5(), 'sha256': hashlib.sha256()}
    started = time.monotonic()
    ftp = None
    receipt, written = None, 0

    def check_time():
        if deadline is not None and time.monotonic() >= deadline:
            raise TransferDeadline('Finite FTP worker deadline reached; partial retained')

    def persist_prefix(handle=None):
        if handle is not None:
            handle.flush(); os.fsync(handle.fileno())
        receipt.update(prefix_bytes=written, prefix_sha256=contexts['sha256'].hexdigest(),
                       prefix_md5=contexts['md5'].hexdigest(), status='partial')
        core.atomic_json(sidecar, receipt)

    try:
        check_time()
        ftp = ftp_factory()
        ftp.connect('ftp.sra.ebi.ac.uk', 21, timeout=60)
        ftp.login(); ftp.set_pasv(True)
        remote = _ftp_snapshot(ftp, path, item['bytes'])
        existing = partial.exists() or sidecar.exists() or partial.is_symlink() or sidecar.is_symlink()
        if existing:
            if partial.is_symlink() or sidecar.is_symlink() or not partial.is_file() or not sidecar.is_file():
                raise TransferError('An existing foreign or incomplete sidecar binding must be preserved')
            old = core.read_json(sidecar)
            binding = {'asset_hash': signature, 'protocol': 'ftp', 'canonical_url': item['url'],
                       'route_url': route, **remote}
            if any(old.get(key) != value for key, value in binding.items()):
                raise TransferError('FTP partial asset/protocol/SIZE/MDTM changed; preserve it')
            before = _snapshot(partial)
            if before['bytes'] > item['bytes'] or old.get('prefix_bytes') != before['bytes']:
                raise TransferError('FTP partial length differs from its recorded prefix; preserve it')
            with partial.open('rb') as prefix:
                for chunk in iter(lambda: prefix.read(8 * 1024**2), b''):
                    check_time()
                    for context in contexts.values(): context.update(chunk)
            if (_snapshot(partial) != before or contexts['sha256'].hexdigest() != old.get('prefix_sha256')
                or contexts['md5'].hexdigest() != old.get('prefix_md5')):
                raise TransferError('FTP prefix bytes changed; never append to an unverified prefix')
            written = before['bytes']
        else:
            binding = {'asset_hash': signature, 'protocol': 'ftp', 'canonical_url': item['url'],
                       'route_url': route, **remote}
        receipt = {**binding, 'resumed_from_bytes': written, 'status': 'partial'}
        mode = 'ab' if existing else 'xb'
        if written == item['bytes']:
            # A complete prefix is never fetched again or silently accepted.
            # The same independent disk/pinned-checksum gate below decides it.
            persist_prefix()
        else:
            with partial.open(mode) as output:
                persist_prefix(output)
                def receive(chunk):
                    nonlocal written
                    check_time()
                    if written + len(chunk) > item['bytes']:
                        raise TransferError('FTP body exceeds the pinned byte count; rejected chunk not written')
                    output.write(chunk)
                    for context in contexts.values(): context.update(chunk)
                    written += len(chunk)
                    delay = (written - receipt['resumed_from_bytes']) / bandwidth - (time.monotonic() - started)
                    if delay > 0: sleeper(delay)
                try:
                    ftp.retrbinary('RETR ' + path, receive, blocksize=8 * 1024**2,
                                   rest=written if written else None)
                except BaseException:
                    # A caught transport/deadline failure remains safely resumable.
                    # An uncatchable kill leaves a stale prefix binding and refuses
                    # reuse until independently inspected; no bytes are truncated.
                    try:
                        persist_prefix(output)
                    except BaseException as checkpoint_error:
                        print('Prefix checkpoint failed; preserve original transfer failure: '
                              + repr(checkpoint_error), file=sys.stderr, flush=True)
                    raise
                else:
                    persist_prefix(output)
        if written != item['bytes']:
            raise TransferError('FTP body is shorter than the pinned byte count; prefix retained')
        if _ftp_snapshot(ftp, path, item['bytes']) != remote:
            raise TransferError('FTP SIZE/MDTM changed during transfer; do not promote')
        stream = {key: value.hexdigest() for key, value in contexts.items()}
        disk = acquire.file_digests(partial, {'md5', 'sha256'})
        core.atomic_json(diagnostic, {'asset_id': item['asset_id'], 'asset_hash': signature,
            'canonical_url': item['url'], 'transfer_route': route, 'bytes': written,
            'stream_digests': stream, 'independent_disk_digests': disk, 'provider_checksum': item['provider_checksum'],
            'stream_equals_disk': stream == disk, 'status': 'integrity_checked_before_promotion'})
        if stream != disk:
            raise TransferError('FTP stream/disk digests differ; failed bytes and diagnostics retained')
        result = acquire.verify_file(partial, item, computed_digests=disk)
        # Hard-link creation is atomic and fails if another actor created target.
        os.link(partial, target)
        partial.unlink()
        receipt.update(status='verified_promoted', stream_digests=stream, independent_disk_digests=disk)
        core.atomic_json(sidecar, receipt)
        # The data-worker guard serializes promotion; a history destination is
        # nevertheless created without overwriting any existing evidence.
        os.link(sidecar, promoted_sidecar)
        sidecar.unlink()
        return {**result, 'path': str(target), 'canonical_url': item['url'], 'transfer_route': route,
            'protocol': 'ftp', **remote, 'stream_digests': stream, 'independent_disk_digests': disk,
            'independent_disk_verified': True, 'resumed_from_bytes': receipt['resumed_from_bytes']}
    except BaseException as error:
        if receipt is not None:
            try:
                core.atomic_json(diagnostic, {'asset_id': item['asset_id'], 'asset_hash': signature,
                    'canonical_url': item['url'], 'transfer_route': route, 'bytes_written': written,
                    'provider_checksum': item['provider_checksum'], 'error_type': type(error).__name__, 'error': str(error),
                    'partial_retained': partial.exists(), 'source_definition_unchanged': True,
                    **({'stream_digests': stream, 'independent_disk_digests': disk} if 'disk' in locals() else {})})
            except BaseException as diagnostic_error:
                print('Integrity diagnostic failed; preserve original transfer failure: '
                      + repr(diagnostic_error), file=sys.stderr, flush=True)
        raise
    finally:
        if ftp is not None:
            try: ftp.close()
            except OSError: pass


def _module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module; spec.loader.exec_module(module)
    return module


def bootstrap(primary):
    """Import scientific input helpers from the verified primary release only."""
    primary = Path(primary).resolve()
    maintenance = Path(__file__).resolve().parents[1]
    core = _module('_hhw_ftp_primary_core', primary / 'src/horsehair/core.py')
    acquire = _module('_hhw_ftp_primary_acquire', primary / 'src/horsehair/acquire.py')
    config = core.read_json(primary / 'configs/study.json'); core.validate_config(config)
    identity = core.read_json(primary / 'release.json')
    other = core.read_json(maintenance / 'release.json')
    if (core.source_identity(primary) != identity['source_identity']
        or core.source_identity(maintenance) != other['source_identity']
        or identity['config_sha256'] != core.canonical_hash(config)
        or other['config_sha256'] != identity['config_sha256']
        or core.read_json(maintenance / 'configs/study.json') != config
        or other['source_identity']['files'].get('scripts/transfer_rna_ftp.py') != core.digest(Path(__file__))):
        raise TransferError('Primary/maintenance release or full scientific configuration changed')
    return SimpleNamespace(core=core, acquire=acquire, config=config, primary=primary,
        maintenance=maintenance, identity=identity, maintenance_identity=other,
        root=Path(config['paths']['root']).resolve())


def ordinary(path, root):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(Path(root).resolve()):
        raise TransferError('Ordinary contained source evidence required: ' + str(path))
    return path


def native_completed(job, runner=subprocess.run):
    if not re.fullmatch(r'[0-9]+', str(job)): raise TransferError('Invalid native preservation identity')
    result = runner(['sacct', '-j', str(job), '--noheader', '--parsable2',
                     '--format=JobIDRaw%64,State%32,ExitCode'], text=True, capture_output=True, check=True, timeout=30)
    rows = [line.split('|') for line in result.stdout.splitlines() if line.split('|')[0].strip() == str(job)]
    if len(rows) != 1 or rows[0][1].strip() != 'COMPLETED' or rows[0][2].strip() != '0:0':
        raise TransferError('Native preservation must be positively COMPLETED0:0')
    return result.stdout


def preflight(context, case_dir, *, runner=subprocess.run):
    core, acquire, root = context.core, context.acquire, context.root
    if socket.gethostname().split('.')[0] != 'narval1': raise TransferError('One worker belongs on actual narval1')
    case_dir = Path(case_dir).resolve()
    if not case_dir.is_relative_to(root / 'maintenance'): raise TransferError('Case escaped personal maintenance namespace')
    ready_path = ordinary(case_dir / 'ready.json', root); ready = core.read_json(ready_path)
    if (ready.get('status') != 'ready_for_transfer' or ready.get('transfer_launched') is not False
        or ready.get('source_definition_unchanged') is not True
        or ready.get('source_sha256') != context.identity['source_identity']['sha256']
        or ready.get('config_sha256') != context.identity['config_sha256'] or ready.get('old_worker_host') != 'narval1'):
        raise TransferError('Preservation is not bound to unchanged primary source/configuration')
    native = native_completed(ready['slurm_job_id'], runner)
    old_pid = ready['old_worker_pid']
    if not isinstance(old_pid, int) or old_pid <= 1: raise TransferError('Invalid old worker PID')
    try: os.kill(old_pid, 0)
    except ProcessLookupError: pass
    else: raise TransferError('Old worker PID exists/reused; do not kill it or replay recovery')
    guard = root / 'state/.transfer-rna.lock'
    if guard.exists() or guard.is_symlink(): raise TransferError('Preserve an existing transfer guard')
    failed_path = ordinary(root / 'state/transfer-rna.json', root)
    if core.digest(failed_path) != ready['failed_transfer_sha256']: raise TransferError('Failed receipt changed after preservation')
    failed = core.read_json(failed_path)
    proof_path = ordinary(ready['provider_worker_proof_path'], root); proof = core.read_json(proof_path)
    if (core.digest(proof_path) != ready['provider_proof_sha256'] or proof.get('host') != 'narval1'
        or proof.get('worker_pid') != old_pid or proof.get('worker_absent') is not True
        or proof.get('transfer_guard_absent') is not True or proof.get('failed_transfer') != failed
        or proof.get('failed_transfer_sha256') != ready['failed_transfer_sha256']
        or proof.get('provider_metadata', {}).get('matches_pinned_asset') is not True
        or proof.get('provider_metadata', {}).get('manifest_modified') is not False):
        raise TransferError('Actual worker/provider proof changed or is ambiguous')
    manifest_path = ordinary(root / 'manifests/source_manifest.json', root)
    manifest = core.read_json(manifest_path); acquire.validate_manifest(manifest)
    if manifest['manifest_hash'] != ready['manifest_hash']: raise TransferError('Frozen manifest changed')
    selected = [a for a in manifest['assets'] if a['source_id'] in SOURCES and (a.get('kind') != 'fastq' or a.get('eligible'))]
    if (not selected or len(selected) > min(256, context.config['resources']['scratch_budget_files'])
        or any(a.get('bytes') is None for a in selected)
        or sum(a['bytes'] for a in selected) > context.config['resources']['scratch_budget_bytes']):
        raise TransferError('Finite fixed RNA roster exceeds its resource ceilings')
    first = [a for a in selected if a['asset_id'] == ready['asset_id']]
    if len(first) != 1 or first[0] != proof['asset']: raise TransferError('Preserved asset does not match frozen source')
    asset = first[0]; ftp_route(asset)
    if (proof.get('asset_sha256') != acquire.canonical_hash(asset)
        or proof.get('manifest_hash') != manifest['manifest_hash']):
        raise TransferError('Provider proof is not bound to the frozen asset/manifest')
    target = acquire.safe_destination(root, asset['relative_path'])
    if any(p.exists() or p.is_symlink() for p in [target, target.with_name(target.name+'.part'), target.with_name(target.name+'.part.json')]):
        raise TransferError('Case asset requires a fresh original destination after preservation')
    q = ready['quarantine']
    if (q.get('status') != 'quarantined_invalid_partial' or q.get('asset_hash') != acquire.canonical_hash(asset)
        or q.get('asset_id') != asset['asset_id'] or q.get('source_version') != asset['version']
        or q.get('provider_checksum') != asset['provider_checksum'] or q.get('bytes') != asset['bytes']
        or q.get('source_definition_unchanged') is not True
        or not re.fullmatch('[0-9a-f]{32}', q.get('observed_digests', {}).get('md5', ''))
        or not re.fullmatch('[0-9a-f]{64}', q.get('observed_digests', {}).get('sha256', ''))
        or q['observed_digests']['md5'] == asset['provider_checksum'].split(':', 1)[1].lower()):
        raise TransferError('Quarantine identity differs or does not prove a bad pinned MD5')
    preserved = ordinary(q['preserved_partial'], root); sidecar = ordinary(q['preserved_sidecar'], root)
    journal = ordinary(preserved.parent/'recovery.json', root)
    if (Path(q['original_partial']).resolve() != target.with_name(target.name+'.part')
        or Path(q['original_sidecar']).resolve() != target.with_name(target.name+'.part.json')
        or preserved.parent.resolve().parent != (target.parent/'.quarantine').resolve()
        or not re.fullmatch('[a-z0-9][a-z0-9-]{0,63}', preserved.parent.name)
        or preserved.name != target.name+'.part' or sidecar != preserved.with_name(preserved.name+'.json')
        or _snapshot(preserved) != proof.get('partial_snapshot')
        or preserved.stat().st_size != asset['bytes'] or core.digest(sidecar) != q['sidecar_sha256']
        or core.digest(sidecar) != proof.get('sidecar_sha256')
        or core.read_json(sidecar).get('asset_hash') != acquire.canonical_hash(asset)
        or core.read_json(journal) != q or preserved.stat().st_mtime_ns > journal.stat().st_mtime_ns):
        raise TransferError('Preserved failed bytes/sidecar/journal changed')
    return SimpleNamespace(case_dir=case_dir, ready=ready, ready_path=ready_path,
        proof_path=proof_path, failed_path=failed_path, manifest=manifest, selected=selected, asset=asset, native=native)


def _owned_guard(path, owner, action):
    with path.open('xb') as output:
        output.write(owner); output.flush(); os.fsync(output.fileno())
    try: return action()
    finally:
        if path.is_file() and not path.is_symlink() and path.read_bytes() == owner: path.unlink()
        else: print('Transfer guard owner changed; preserve it', file=sys.stderr, flush=True)


def launch(context, case_dir, restart_id, *, runner=subprocess.run, popen=subprocess.Popen):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', restart_id): raise TransferError('Invalid unique restart identity')
    checked = preflight(context, case_dir, runner=runner)
    core, root = context.core, context.root
    restart = checked.case_dir / ('restart-' + restart_id)
    if restart.exists(): raise TransferError('Restart outcome already exists; never replay ambiguity')
    ps = runner(['ps', '-u', str(root.parent.name), '-o', 'pid=,args='], text=True, capture_output=True, check=True, timeout=20)
    conflicts = [line for line in ps.stdout.splitlines() if ('scripts/transfer_sources.py' in line and '--group rna' in line)
        or 'scripts/start_transfers.py' in line or 'rna301-transfer-worker' in line and '--worker' in line
        or 'transfer_rna_ftp.py' in line and '--worker' in line]
    if conflicts: raise TransferError('Another RNA data worker or launcher exists')
    restart.mkdir(exist_ok=False)
    for name, source in [('old-failed-transfer.json', checked.failed_path), ('provider-worker-proof.json', checked.proof_path),
                         ('native-ready.json', checked.ready_path)]:
        with (restart/name).open('xb') as output:
            output.write(source.read_bytes()); output.flush(); os.fsync(output.fileno())
    pilot = root/'state/transfer-rna-pilot.json'
    if pilot.exists():
        with (restart/'old-pilot-transfer.json').open('xb') as output:
            output.write(pilot.read_bytes()); output.flush(); os.fsync(output.fileno())
    journal = restart/'launch.json'
    record = {'status':'starting','restart_id':restart_id,'primary_release':str(context.primary),
        'primary_source_sha256':context.identity['source_identity']['sha256'],
        'maintenance_release':str(context.maintenance),'maintenance_source_sha256':context.maintenance_identity['source_identity']['sha256'],
        'config_sha256':context.identity['config_sha256'],'script_sha256':core.digest(Path(__file__)),
        'ready_sha256':core.digest(checked.ready_path),'preservation_job_id':checked.ready['slurm_job_id'],
        'preservation_native_proof':checked.native,'created_unix':time.time(),'maximum_seconds':MAX_SECONDS,
        'maximum_data_workers':1,'bandwidth_bytes_per_second':BANDWIDTH,'automatic_quarantine_retries':0}
    with journal.open('x') as output:
        json.dump(record, output, indent=2, sort_keys=True); output.flush(); os.fsync(output.fileno())
    log = root/'logs'/('transfer-rna-ftp-'+restart_id+'.log')
    env = os.environ.copy()
    for key in list(env):
        if key.lower() in {'http_proxy','https_proxy','all_proxy'}: env.pop(key)
    env['PYTHONUNBUFFERED'] = '1'
    command = ['nohup',sys.executable,str(Path(__file__).resolve()),'--primary-release',str(context.primary),
               '--case-dir',str(checked.case_dir),'--restart-id',restart_id,'--worker']
    try:
        with log.open('x') as output:
            child = popen(command,cwd=context.primary,env=env,stdin=subprocess.DEVNULL,
                          stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
        record.update(status='started',pid=child.pid,command=command,log=str(log),started_unix=time.time())
        core.atomic_json(journal, record)
    except BaseException as error:
        record.update(status='launch_ambiguous' if 'child' in locals() else 'launch_failed',error_type=type(error).__name__,error=str(error))
        if 'child' in locals(): record['pid']=child.pid
        core.atomic_json(journal,record); raise
    return record


def worker(context, case_dir, restart_id):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', restart_id): raise TransferError('Invalid unique restart identity')
    checked = preflight(context, case_dir)
    core, acquire, root = context.core, context.acquire, context.root
    restart = checked.case_dir / ('restart-' + restart_id)
    journal = core.read_json(ordinary(restart/'launch.json',root))
    if (journal.get('status') not in {'starting','started'} or journal.get('script_sha256') != core.digest(Path(__file__))
        or journal.get('ready_sha256') != core.digest(checked.ready_path)
        or journal.get('primary_source_sha256') != context.identity['source_identity']['sha256']
        or journal.get('maintenance_source_sha256') != context.maintenance_identity['source_identity']['sha256']):
        raise TransferError('Worker launch source/receipt identity changed')
    owner = json.dumps({'pid':os.getpid(),'host':'narval1','restart_id':restart_id,'created_unix':time.time()},sort_keys=True).encode()
    guard = root/'state/.transfer-rna.lock'
    def transfer():
        started=time.monotonic(); deadline=started+MAX_SECONDS
        record={'status':'running','group':'rna','sources':SOURCES,'pid':os.getpid(),'host':'narval1','started_unix':time.time(),
            'config_sha256':context.identity['config_sha256'],'source_identity':context.identity['source_identity']['sha256'],
            'maintenance_source_sha256':context.maintenance_identity['source_identity']['sha256'],
            'transfer_script_sha256':core.digest(Path(__file__)),'preservation_job_id':checked.ready['slurm_job_id'],
            'restart_id':restart_id,'maximum_seconds':MAX_SECONDS,'bandwidth_bytes_per_second':BANDWIDTH,
            'automatic_quarantine_retries':0,'execution_class':'finite public-data file transfer; canonical pins unchanged',
            'verified_files':0}
        state=root/'state/transfer-rna.json'
        report={'manifest_hash':checked.manifest['manifest_hash'],'selection_hash':core.canonical_hash([a['asset_id'] for a in checked.selected]),
            'sources':SOURCES,'files':[],'receipt_path':str(restart/'download-receipts.json')}
        def expire(*_): raise TransferDeadline('Finite 120h data worker deadline reached; preserve partial')
        old=signal.signal(signal.SIGALRM,expire);signal.setitimer(signal.ITIMER_REAL,MAX_SECONDS)
        session=acquire.http_session();session.trust_env=False
        session.headers.update({'Cache-Control':'no-cache','Pragma':'no-cache'})
        try:
            if core.digest(state)!=checked.ready['failed_transfer_sha256']:raise TransferError('Failure changed before owned guard claim')
            core.atomic_json(state,record)
            ordered=[checked.asset,*[a for a in checked.selected if a['asset_id']!=checked.asset['asset_id']]]
            pilots=acquire.rna_pilot_asset_ids(checked.manifest);pilot_written=False
            if not pilots or not set(pilots)<={a['asset_id'] for a in checked.selected}:
                raise TransferError('Fixed pilot IDs are not all in the finite frozen RNA roster')
            for item in ordered:
                if time.monotonic()>=deadline:raise TransferDeadline('Finite data deadline reached before next file')
                if item.get('kind')=='fastq':
                    result=download_ftp_asset(item,root,context,deadline=deadline)
                else:
                    result=acquire.download_asset(item,root,session=session,attempts=1,bandwidth_bytes_per_second=BANDWIDTH,
                        max_seconds=deadline-time.monotonic())
                report['files'].append(result);core.atomic_json(restart/'download-receipts.json',report)
                record.update(verified_files=len(report['files']),elapsed_seconds=time.monotonic()-started)
                core.atomic_json(state,record)
                if not pilot_written and set(pilots)<={row['asset_id'] for row in report['files']}:
                    core.atomic_json(root/'state/transfer-rna-pilot.json',{**record,'group':'rna-pilot','status':'complete',
                        'asset_ids':pilots,'verified_files':len(pilots),'source_manifest_sha256':core.canonical_hash(checked.manifest)})
                    pilot_written=True
            if time.monotonic()>=deadline:raise TransferDeadline('Finite deadline elapsed before full verification')
            record.update(status='complete',elapsed_seconds=time.monotonic()-started,
                source_manifest_sha256=core.canonical_hash(checked.manifest),download_receipt_path=report['receipt_path'])
            core.atomic_json(state,record)
        except BaseException as error:
            record.update(status='failed',error_type=type(error).__name__,error=str(error),elapsed_seconds=time.monotonic()-started,
                partial_preserved=True,repair_repeated=False)
            core.atomic_json(state,record);raise
        finally:
            signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,old);session.close()
        return record
    return _owned_guard(guard,owner,transfer)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--primary-release',required=True,type=Path)
    p.add_argument('--case-dir',required=True,type=Path)
    p.add_argument('--restart-id',required=True)
    modes=p.add_mutually_exclusive_group(required=True);modes.add_argument('--launch',action='store_true');modes.add_argument('--worker',action='store_true')
    args=p.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}',args.restart_id):raise TransferError('Invalid restart identity')
    context=bootstrap(args.primary_release)
    result=launch(context,args.case_dir,args.restart_id) if args.launch else worker(context,args.case_dir,args.restart_id)
    print(json.dumps(result,sort_keys=True),flush=True)


if __name__=='__main__':main()
