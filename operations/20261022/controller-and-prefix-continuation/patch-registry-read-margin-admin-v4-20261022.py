"""Fresh administrative registry read extension; no deployed source overwrite.

All old module AST nodes are unchanged except the 180-to-900s total read bound.
The old qualified source remains immutable. This does not qualify a scientific
controller; its separate full native qualification is still required.
"""
from pathlib import Path
import ast,base64,hashlib,json,zlib,xml.etree.ElementTree as ET

def patch(text):
 old=Path('work/registered-snapshot-read-guard-v3-20261021.py').read_bytes()
 new=Path('work/registered-snapshot-read-guard-v4-20261022.py').read_bytes()
 assert hashlib.sha256(old).hexdigest()=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'
 def strip(raw):
  tree=ast.parse(raw);assign=[n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='MAX_SECONDS' for t in n.targets)]
  assert len(assign)==1;value=ast.literal_eval(assign[0].value);tree.body.remove(assign[0]);return ast.dump(tree,include_attributes=False),value
 a,av=strip(old);b,bv=strip(new);assert a==b and (av,bv)==(180,900)
 cases=list(ET.parse('work/registry-v4-root-20261022.xml').getroot().iter('testcase'));assert len(cases)==30 and all(not any(c.find(k) is not None for k in ['failure','error','skipped']) for c in cases)
 sha=hashlib.sha256(new).hexdigest();data=base64.b64encode(zlib.compress(new)).decode()
 needle="rgp=root/'maintenance/study-af-registry-repair-support-v4-20261021/registered-snapshot-read-guard-v3-20261021.py'\nassert digest(rgp)=='2583776f764f4738dd0285f7885709bc9dc45044cfea52c44909c4723f0d64b6'\n"
 assert text.count(needle)==1
 replacement="rgp=area/'administration-registry-read-guard-v4-20261022.py'\nraw=zlib.decompress(base64.b64decode("+repr(data)+"));assert hashlib.sha256(raw).hexdigest()=="+repr(sha)+"\nwith rgp.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())\nassert digest(rgp)=="+repr(sha)+"\n"
 text=text.replace(needle,replacement,1);ast.parse(text.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]);return text
