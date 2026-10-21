"""Explicit expanded-policy admission for a freshly built maintenance envelope.

The existing builder is preserved. This patch requires its exact pre-patch
snippets, installs the pinned read guard before function binding, and runs the
outer accounting from the already qualified 4f release. A dependent native
bootstrap is deliberately refused: its executing source needs its own pinned
policy, which must be handled by the native helper.
"""
from pathlib import Path
import argparse
import ast

QUALIFIED = '/scratch/pwa209/horsehair-worm/releases/20261021-6ed201653dfa-4f3903d7c26e'
OLD = '/project/def-ptewarie/pwa209/horsehair-worm/releases/20261016-5c3ae6df532d-91c8824f2adb'
POLICY = '/scratch/pwa209/horsehair-worm/maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA = 'f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
GUARD_SHA = '358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'


def patch(text):
    needle = 'export PYTHONPATH="' + OLD + '/src:'
    assert text.count(needle) == 1, 'Fresh, unpatched builder envelope required'
    text = text.replace(needle, 'export PYTHONPATH="' + QUALIFIED + '/src:', 1)
    needle = "admitter=runpy.run_path(str(driver),run_name='postdownload_admission_only')\nadmission=admitter['combined_admission'](root,config,science['release_id'],area,'submit-combined-admission')\nassert admission['totals']['cpu_core_hours']+payload['cpus']*payload['hours']<=config['resources']['cpu_core_hours_cap'] and admission['capacity']['cpus']+payload['cpus']<=config['resources']['cpu_ceiling']"
    replacement = (
        "assert not payload['after'] and not payload['after_specs'],'Expanded maintenance requires helper-owned source-bound native prerequisites'\n"
        "gp=root/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py';"
        f"assert digest(gp)=={GUARD_SHA!r};runpy.run_path(str(gp))['install'](area/'submission-native-accounting-reads')\n"
        "from horsehair.campaign import combined_admission\n"
        f"policy_pair=(Path({POLICY!r}),{POLICY_SHA!r})\n"
        "admission=combined_admission(root,config,science['release_id'],area,'submit-expanded-combined-admission',"
        "resource_policy=policy_pair,cpus=payload['cpus'],gpus=0,hours=payload['hours'])"
    )
    assert text.count(needle) == 1, 'Exact original admission body required'
    text = text.replace(needle, replacement, 1)
    needle = "'cpu_ceiling':64,'gpu_concurrency':1,'maximum_simultaneous_resources'"
    assert text.count(needle) == 1
    text = text.replace(needle, "'cpu_ceiling':256,'gpu_concurrency':4,'resource_policy':admission['resource_policy'],'maximum_simultaneous_resources'", 1)
    # Compile the generated Python body without executing any remote action.
    body = text.split("python -u - <<'PY'\n", 1)[1].rsplit('\nPY\n', 1)[0]
    ast.parse(body)
    return text


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path', type=Path)
    args = parser.parse_args()
    original = args.path.read_text(encoding='utf-8')
    updated = patch(original)
    args.path.write_bytes(updated.encode('utf-8'))
