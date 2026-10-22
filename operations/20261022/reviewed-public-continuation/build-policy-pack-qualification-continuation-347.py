"""Continue a staged, unsubmitted qualification using a new transport identity.

The interrupted336 request is retained. Existing immutable source is verified,
never replaced. The logical336 plan may be published once under the original
registration mutex; every existing publication or result refuses continuation.
"""
from pathlib import Path
import ast
import hashlib
import json

SOURCE = Path('work/hhw-recovery-stage-20261021-336.sh')
TARGET = Path('work/hhw-af-pack-qualification-continuation-20261022-347.sh')
EXPECTED = '2257fce420e3467d50634e1fc12ebc52402ac13c5fcd3da5bea22e1826606704'
PINS = {
    'run-f2-policy-archive-pack-v1-20261021.py': '0490b796efcbd6b7063ef3fc1e454f8b38cd6692d139d06626829e0db04a018d',
    'test_f2_policy_archive_pack_v1_20261021.py': '25a4c507d4f2b23a70ef496519fcfafd643f986277d11224cb38413edbe93225',
    'qualify-f2-policy-pack-v1-20261022.py': '63dbc330c9e8b21e643e1337ff44c07cb31ef05ca123fc07721eab1fd34989d9',
    'test_qualify_f2_policy_pack_v1_20261022.py': '05799d2828d8dcd571de8051adf9dc62471ae86826ddf73715873dc7b28d7649',
}


def build(raw):
    assert hashlib.sha256(raw).hexdigest() == EXPECTED
    text = raw.decode('utf-8')
    start = text.index("area=Path(payload['area']);assert ")
    end = text.index("assert not payload['after']", start)
    replacement = """area=Path(payload['area'])
assert area==root/'maintenance/study-af-policy-pack-qualification-stage-v1-20261022'
assert area.is_dir() and area==area.resolve() and not area.is_symlink() and area.stat().st_uid==3164768
staged_pins=__PINS__
def verify_staged_sources():
 for source_name,source_sha in staged_pins.items():
  source=area/source_name
  assert source.is_file() and not source.is_symlink() and source.stat().st_uid==3164768 and digest(source)==source_sha,source_name
verify_staged_sources()
helper=area/payload['helper']
assert helper.name=='qualify-f2-policy-pack-v1-20261022.py' and digest(helper)==payload['helper_sha256']==staged_pins[helper.name]
def require_unpublished():
 verify_staged_sources()
 for path in [root/'plans/recovery-support-20261021-336',root/'state/recovery-support-20261021-336-submission.json',area/'submission-intent.json',root/'maintenance/study-af-policy-pack-qualification-support-v1-20261022']:
  assert not path.exists() and not path.is_symlink(),'Logical336 publication/result exists; reconcile without replay: '+str(path)
require_unpublished()
""".replace('__PINS__', repr(PINS))
    text = text[:start] + replacement + text[end:]
    start = text.index('for name,item in ')
    end = text.index("rgp=root/", start)
    text = text[:start] + 'verify_staged_sources()\n' + text[end:]
    for old, new in [
        ('submission-native-accounting-reads', 'resumption-347-native-accounting-reads'),
        ('submission-registry-reads', 'resumption-347-registry-reads'),
        ('submission-registration-lock', 'resumption-347-registration-lock'),
        ('submit-expanded-combined-admission', 'resumption-347-expanded-combined-admission'),
    ]:
        assert text.count(old) == 1
        text = text.replace(old, new, 1)
    needle = "    admission=combined_admission"
    assert text.count(needle) == 1
    text = text.replace(needle, "    require_unpublished()\n"+needle, 1)
    # Recheck immediately before exclusive plan publication, after slow admission.
    needle = "    assert not directory.exists() and not ledger.exists();directory.mkdir(mode=0o700)"
    assert text.count(needle) == 1
    text = text.replace(needle, '    require_unpublished()\n'+needle, 1)
    needle = "print('RECOVERY_STAGE_SUBMITTED "
    assert text.count(needle) == 1
    text = text.replace(needle, "print('RECOVERY_STAGE_CONTINUED_TRANSPORT_347 ", 1)
    body = text.split("python -u - <<'PY'\n", 1)[1].rsplit('\nPY\n', 1)[0]
    ast.parse(body)
    assert text.count("open('xb')") == 0, 'Continuation must never rewrite staged source'
    return text.encode('utf-8')


if __name__ == '__main__':
    raw = build(SOURCE.read_bytes())
    with TARGET.open('xb') as stream:
        stream.write(raw)
    print(json.dumps({'request': '347', 'logical_plan': '336', 'sha256': hashlib.sha256(raw).hexdigest(),
        'bytes': len(raw), 'existing_sources_replaced': False, 'scheduler_submitted': False}, sort_keys=True))
