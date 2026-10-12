"""Submit a single scoped request through an existing authenticated SSH bridge.

Credentials are never accessed. Request IDs are immutable and ambiguous submissions
are never automatically replayed. The bridge is supplied explicitly by the operator.
"""
from pathlib import Path
import argparse
import json
import shutil
import time


def submit(script, bridge, local, timeout=45, quiet=False):
    script,bridge,local=map(Path,(script,bridge,local))
    ident=script.stem
    if not ident.replace('-','').replace('_','').isalnum():
        raise ValueError('Unsafe request identifier')
    if (bridge/'DISCONNECTED.json').exists():
        raise RuntimeError('Authenticate in the user console; session disconnected')
    for area,suffixes in [('requests',['.sh']),('sent',['.sh','.transport.json']),('completed',['.json']),('outputs',['.txt'])]:
        if any((bridge/area/(ident+s)).exists() for s in suffixes):
            raise RuntimeError('Request ID already exists; inspect receipt without replaying')
    payload=script.read_bytes()
    if not payload.strip():
        raise ValueError('Empty request')
    local.mkdir(parents=True,exist_ok=True)
    with (bridge/'requests'/(ident+'.sh')).open('xb') as f:
        f.write(payload)
    print(json.dumps({'request':ident,'state':'queued'}),flush=True)
    deadline=time.monotonic()+timeout
    receipt=bridge/'completed'/(ident+'.json')
    while time.monotonic()<deadline:
        if receipt.exists():
            record=json.loads(receipt.read_text())
            shutil.copyfile(receipt,local/receipt.name)
            source=bridge/'outputs'/(ident+'.txt')
            if source.exists():
                shutil.copyfile(source,local/source.name)
            print(json.dumps(record),flush=True)
            if not quiet and source.exists():
                print(source.read_text(encoding='utf8',errors='replace'))
            return record['exit_code']
        if (bridge/'DISCONNECTED.json').exists():
            raise RuntimeError('Disconnected; preserve pending request and inspect existing receipts')
        time.sleep(.5)
    print(json.dumps({'request':ident,'state':'pending_or_ambiguous','action':'inspect existing receipt; never replay'}))
    return 124


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--script',type=Path,required=True)
    p.add_argument('--bridge',type=Path,required=True)
    p.add_argument('--local',type=Path,required=True)
    p.add_argument('--timeout',type=int,default=45)
    p.add_argument('--quiet',action='store_true')
    a=p.parse_args()
    raise SystemExit(submit(**vars(a)))

