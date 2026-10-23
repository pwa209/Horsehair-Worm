"""Exercise actual deployment bootstrap imports in an isolated interpreter."""
import ast
import os
from pathlib import Path
import shutil
import subprocess
import sys


def test_managed_orchestrator_imports_release_and_preserves_module_environment(tmp_path):
    repository=Path(__file__).resolve().parents[1]
    tree=ast.parse((repository/'scripts/build_release.py').read_text())
    body=next(node.value.value for node in tree.body if isinstance(node,ast.Assign)
              and any(isinstance(target,ast.Name) and target.id=='body' for target in node.targets))
    native_imports=next(line for line in body.splitlines() if line.startswith('import base64,'))
    bootstrap=body[body.index("os.environ['PYTHONPATH']="):body.index('prefix=')]
    release=tmp_path/'isolated-release'
    package=release/'src/horsehair';package.mkdir(parents=True)
    for name in ['__init__.py','core.py','pipeline.py']:
        shutil.copyfile(repository/'src/horsehair'/name,package/name)
    code=(native_imports+'\nrelease=pathlib.Path('+repr(str(release))+')\n'+bootstrap+
          '\nfrom horsehair.pipeline import prospective_capacity\n'
          'assert os.environ["PYTHONPATH"].endswith("module-sentinel")\n'
          'print(prospective_capacity({"initial":{"tasks":[{"name":"fixture","resources":{"cpus":2}}]}},{},{}))\n')
    env=dict(os.environ,PYTHONPATH='module-sentinel')
    result=subprocess.run([sys.executable,'-I','-S','-c',code],text=True,capture_output=True,env=env,timeout=30)
    assert result.returncode==0,result.stderr
    assert "'cpus': 2, 'gpus': 0" in result.stdout
