"""Derive unsubmitted reviewed registration sources for the new frozen namespace.

No remote actions; old drafts and their evidence remain unchanged.
"""
from pathlib import Path
import ast,hashlib,json
OLD=Path('work/rolling-native-v3');NEW=Path('work/rolling-native-v4')
PINS={'build-rolling-census-registration-429.py': '088cd82e4b85f6cc8a988f35172d13f3932ebd53bd1721fad3968b39bfc2486b', 'test_rolling_census_registration_429.py': '18ecc59f300f8dde0bde9a3f83fe06189ac6634a7774cbaa02e04c4069a51586', 'build-rolling-dependents-430-434.py': 'cfd5a02bc8b6fb1369ffffedefd1f2049832a0669a77bdd4438abe4134d3cc85', 'test_rolling_dependents_430_434.py': '7af6b0a80f842d0abd74306a52ce91c9b306450472ff34dc58789a3120fab175'}
def sha(raw):return hashlib.sha256(raw).hexdigest()
def main():
 raw=(NEW/'source-manifest.json').read_bytes();m=json.loads(raw);mh=sha(raw)
 assert m['kind']=='streaming-census-operational-source-manifest-v2' and len(m['files'])==14 and len(m['suites'])==13 and sum(c['tests'] for c in m['suites'].values())==757
 for name,h in m['files'].items():assert sha((NEW/name).read_bytes())==h,name
 changes={
 'work/rolling-native-v3':'work/rolling-native-v4',"WORK_ROOT/'rolling-native-v3'":"WORK_ROOT/'rolling-native-v4'",
 'study-af-rolling-census-support-v2-20261023':'study-af-rolling-census-support-v3-20261023',
 'study-af-rolling-census-qualification-v3-20261023':'study-af-rolling-census-qualification-v4-20261023',
 'study-af-rolling-storage-census-v2-20261023':'study-af-rolling-storage-census-v3-20261023',
 'study-af-rolling-census-registration-v2-20261023':'study-af-rolling-census-registration-v3-20261023',
 'recovery-support-20261021-444':'recovery-support-20261021-452','recovery-444':'recovery-452',
 "('416','444')":"('416','452')",'q444':'q452',
 'fefcb2cd1e5f7b70ebab7e959955d96c9ea4072dd133370adf2f6a8e6cb03ff0':mh,
 '6cdbd5507b065ed2b23bc7b60c5fce498f43794c417d7ce1643aaa8f925c4cb8':m['files']['run-fresh-storage-census-v6-20261023.py']}
 report=[]
 for name,h in PINS.items():
  before=(OLD/name).read_bytes();assert sha(before)==h,name;s=before.decode();used={}
  for a,b in changes.items():
   if a in s:used[a]={'after':b,'occurrences':s.count(a)};s=s.replace(a,b)
  ast.parse(s,feature_version=(3,11));target=NEW/name;assert not target.exists();target.write_bytes(s.encode())
  report.append({'name':name,'before_sha256':h,'after_sha256':sha(target.read_bytes()),'explicit_replacements':used})
 p=NEW/'registration-namespace-derivation.json';assert not p.exists();p.write_text(json.dumps({'kind':'unsubmitted-registration-namespace-derivation-v1','sources':report,'new_source_manifest_sha256':mh,'scheduler_submitted':False},indent=2,sort_keys=True)+'\n')
 print(json.dumps({'manifest_sha256':mh,'sources':report,'scheduler_submitted':False},sort_keys=True))
if __name__=='__main__':main()
