"""A deployment must carry the complete sealed closure, including non-code files."""
from pathlib import Path
import ast,hashlib,importlib.util,json
import pytest


@pytest.fixture
def builder(tmp_path,monkeypatch):
    source=Path(__file__).with_name('build-native-deployment.py')
    spec=importlib.util.spec_from_file_location('native_deployment',source)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    directory=tmp_path/'repo/operations/20261106/storage-reserve-200k'
    directory.mkdir(parents=True);(tmp_path/'repo/work').mkdir()
    monkeypatch.setattr(module,'__file__',str(directory/'build-native-deployment.py'))
    for name in ['storage_policy.py','policy.json','test_storage_policy.py']:
        (directory/name).write_bytes(b'{}\n' if name.endswith('.json') else b'# source\n')
    component=directory/'census';component.mkdir()
    members={'README.md':b'necessary source provenance\n','fixture.txt':b'original preservation proof\n','runtime.py':b'pass\n'}
    for name,raw in members.items():(component/name).write_bytes(raw)
    (component/'source-manifest.json').write_text(json.dumps({'files':{n:hashlib.sha256(b).hexdigest() for n,b in members.items()}}),encoding='utf-8')
    return module,directory


def test_complete_manifest_carries_non_python_members_and_lf_capsule(builder):
    module,directory=builder
    result=module.build('fixture',('census',));text=Path(result['request']).read_text()
    assert '\r' not in text
    script=text.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0]
    tree=ast.parse(script)
    payload=next(ast.literal_eval(node.value) for node in tree.body if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='payload' for t in node.targets))
    assert {'census/README.md','census/fixture.txt','census/runtime.py','census/source-manifest.json'}<=set(payload)
    compile(script,'<native-registration>','exec')


def test_missing_provenance_member_refuses_capsule_before_submission(builder):
    module,directory=builder;(directory/'census/README.md').unlink()
    with pytest.raises(ValueError,match='manifest member is missing'):
        module.build('missing',('census',))
    assert not (directory.parents[2]/'work/missing.sh').exists()
