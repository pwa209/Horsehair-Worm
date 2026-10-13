"""Explicit finite RNA transfer recovery, preserving rejected provider bytes.

Run in a network-enabled Slurm allocation. The pinned source manifest remains
unchanged; verified finals are reverified, and the rejected complete partial is
preserved with independently measured digests before downloading a fresh copy.
"""
import argparse
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import time

from horsehair.acquire import quarantine_invalid_complete_partial, validate_manifest
from horsehair.budget import resource_lease
from horsehair.core import atomic_json, canonical_hash, native_job_id, read_json, source_identity, validate_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--identity', type=Path, required=True)
    parser.add_argument('--asset-id', required=True)
    parser.add_argument('--recovery-id', required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', args.recovery_id):
        raise ValueError('Invalid finite recovery identity')
    if not os.environ.get('SLURM_JOB_ID'):
        raise RuntimeError('Large-file recovery must run in a scheduled allocation')
    config = read_json(args.config)
    validate_config(config)
    release = args.identity.resolve().parent
    identity = read_json(args.identity)
    if source_identity(release) != identity['source_identity']:
        raise ValueError('Immutable transfer release changed')
    if args.root.resolve() != Path(config['paths']['root']).resolve():
        raise ValueError('Transfer recovery must use the configured personal study root')
    manifest = read_json(args.root / 'manifests/source_manifest.json')
    validate_manifest(manifest)
    matches = [item for item in manifest['assets'] if item['asset_id'] == args.asset_id]
    if len(matches) != 1 or matches[0]['source_id'] != 'ant_rna' or not matches[0].get('eligible'):
        raise ValueError('Recovery requires one eligible manifest-defined ant RNA asset')
    item = matches[0]
    lock = args.root / 'state/.transfer-rna.lock'
    if lock.exists():
        raise RuntimeError('An existing transfer guard requires inspection; never remove it')
    previous = read_json(args.root / 'state/transfer-rna.json')
    expected_error = 'Provider checksum mismatch for ' + Path(item['relative_path']).name + '.part'
    if previous.get('status') != 'failed' or previous.get('error') != expected_error:
        raise RuntimeError('Recovery does not match the preserved failed transfer receipt')
    journal = args.root / 'state' / ('transfer-recovery-' + args.recovery_id + '.json')
    journal.parent.mkdir(parents=True, exist_ok=True)
    record = {'status': 'starting', 'asset_id': args.asset_id,
              'previous_transfer': previous, 'manifest_hash': manifest['manifest_hash'],
              'source_sha256': identity['source_identity']['sha256'],
              'config_sha256': canonical_hash(config), 'slurm_job_id': native_job_id(),
              'created_unix': time.time()}
    # Immutable request journal prevents silent replay after a lost receipt.
    with journal.open('x', encoding='utf8') as handle:
        json.dump(record, handle, indent=2)
    with resource_lease(args.root, config, cpus=int(os.environ.get('SLURM_CPUS_PER_TASK', '1')),
                        gpus=0, max_hours=float(os.environ.get('HHW_MAX_HOURS', '120')),
                        job_id=native_job_id()):
        try:
            record['quarantine'] = quarantine_invalid_complete_partial(item, args.root, args.recovery_id)
            record['status'] = 'fresh_transfer'
            atomic_json(journal, record)
            subprocess.run([sys.executable, str(release / 'scripts/transfer_sources.py'),
                            '--root', str(args.root), '--config', str(args.config),
                            '--identity', str(args.identity), '--group', 'rna'], cwd=release, check=True)
            record.update(status='complete', transfer=read_json(args.root / 'state/transfer-rna.json'))
        except BaseException as error:
            record.update(status='failed', error_type=type(error).__name__, error=str(error))
            atomic_json(journal, record)
            raise
        atomic_json(journal, record)
    print(json.dumps({'status': record['status'], 'journal': str(journal)}))


if __name__ == '__main__':
    main()
