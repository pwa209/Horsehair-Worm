"""Synthetic original-worker/guard fixtures: no native or numerical work."""
from pathlib import Path
import importlib.util
import json
import sys
import time

import pytest

spec=importlib.util.spec_from_file_location('worker_overlay',Path(__file__).with_name('run-f00d-worker-registry-overlay-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    root=tmp_path/'study';directory=root/'maintenance/stable';directory.mkdir(parents=True)
    area=root/'maintenance/native-123';worker=directory/'native-worker.py';guard=directory/'guard.py'
    code="""from pathlib import Path
import json,sys
ROOT=Path(ROOT_LITERAL)
AREA=Path(AREA_LITERAL)
def combined_admission(*args,**kwargs):
    assert (AREA/'358-ready').exists()
    assert (AREA/'registered-snapshot-reads/installed.json').exists()
    assert (AREA/'registered-snapshot-overlay.json').exists()
    (AREA/'original-admission.json').write_text(json.dumps({'argv':sys.argv,'file':__file__}))
    return {'fresh':'unchanged original admission'}
def main():
    AREA.mkdir()
    (AREA/'358-ready').write_text('original358 installed here, not by shim')
    result=combined_admission(ROOT,{},'primary',AREA,'native-start-admission')
    (AREA/'original-main-finished').write_text(result['fresh'])
    return result
if __name__=='__main__':raise AssertionError('Shim must not execute main during initial module load')
""".replace('ROOT_LITERAL',repr(str(root))).replace('AREA_LITERAL',repr(str(area)))
    worker.write_text(code)
    guard.write_text("""import json
def install(evidence,**kwargs):
    assert (evidence.parent/'358-ready').exists()
    evidence.mkdir()
    (evidence/'installed.json').write_text(json.dumps(kwargs))
    return {'mode':kwargs['registration_mode'],'scientific_result':False}
""")
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'WORKER',worker);monkeypatch.setattr(m,'WORKER_SHA',m.sha(worker))
    monkeypatch.setenv('SLURM_JOB_END_TIME',str(time.time()+3600))
    return root,worker,guard,area


def test_original_area_and358_precede_overlay_and_descriptor_argv_remain_original(fixture):
    root,worker,guard,area=fixture;before=list(sys.argv)
    arguments=['--descriptor','stable-descriptor.json','--descriptor-sha256','a'*64,'--mode','agent-train','--index','0']
    result=m.run(guard,m.sha(guard),arguments)
    assert result=={'fresh':'unchanged original admission'} and sys.argv==before
    receipt=json.loads((area/'original-admission.json').read_text())
    assert receipt['argv']==[str(worker),*arguments] and receipt['file']==str(worker)
    overlay=json.loads((area/'registered-snapshot-overlay.json').read_text())
    assert overlay['stable_worker_descriptor_changed'] is overlay['numeric_functions_replaced'] is False
    assert overlay['original_worker_arguments']==arguments and (area/'original-main-finished').exists()


@pytest.mark.parametrize('change',['guard_sha','worker_bytes','foreign_guard','deadline'])
def test_wrong_bindings_cannot_start_original_worker(fixture,monkeypatch,change):
    root,worker,guard,area=fixture;sha=m.sha(guard)
    if change=='guard_sha':sha='b'*64
    elif change=='worker_bytes':worker.write_text('changed')
    elif change=='foreign_guard':guard=root.parent/'outside.py';guard.write_text('changed');sha=m.sha(guard)
    else:monkeypatch.setenv('SLURM_JOB_END_TIME','nan')
    with pytest.raises(ValueError):m.run(guard,sha,['--index','0'])
    assert not area.exists()


def test_original_failure_is_preserved_and_never_replayed(fixture):
    root,worker,guard,area=fixture
    worker.write_text(worker.read_text().replace("return {'fresh':'unchanged original admission'}","raise ValueError('strict original failure')"))
    m.WORKER_SHA=m.sha(worker)
    with pytest.raises(ValueError,match='strict original failure'):m.run(guard,m.sha(guard),[])
    assert (area/'registered-snapshot-overlay.json').exists() and not (area/'original-main-finished').exists()
    with pytest.raises(FileExistsError):m.run(guard,m.sha(guard),[])
