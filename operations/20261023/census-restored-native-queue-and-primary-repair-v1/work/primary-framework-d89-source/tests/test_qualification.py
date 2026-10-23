"""Real tiny subprocess fixtures for the complete native qualification harness."""
import copy
from pathlib import Path
import tarfile
import time

import pytest

from horsehair import cli
from horsehair.core import atomic_json,canonical_hash,digest,read_json,source_identity


@pytest.fixture
def harness(tmp_path,monkeypatch):
    release=tmp_path/'generated-release';tests=release/'tests';tests.mkdir(parents=True)
    (release/'src/horsehair').mkdir(parents=True)
    (release/'src/horsehair/cli.py').write_text('# Generated immutable source fixture\n')
    test=tests/'test_fixture.py'
    test.write_text('def test_generated_contract(tmp_path):\n (tmp_path/"observed.txt").write_text("generated fixture")\n assert 2+2==4\n')
    root=tmp_path/'durable';root.mkdir()
    local=tmp_path/'node-local';local.mkdir()
    monkeypatch.delenv('SLURM_JOB_ID',raising=False)
    monkeypatch.delenv('SLURM_ARRAY_JOB_ID',raising=False)
    monkeypatch.delenv('SLURM_ARRAY_TASK_ID',raising=False)
    monkeypatch.delenv('SLURM_JOB_END_TIME',raising=False)
    monkeypatch.delenv('HHW_MAX_HOURS',raising=False)
    monkeypatch.delenv('SLURM_TMPDIR',raising=False)
    config={'protocol_id':'generated harness fixture; not study evidence'}
    return release,root,local,test,config


def check_receipt(record,root,config,release):
    assert record['config_sha256']==canonical_hash(config) and record['source_sha256']==source_identity(release)['sha256']
    payload=copy.deepcopy(record);assert payload.pop('receipt_sha256')==canonical_hash(payload)
    assert read_json(root/'state/cpu-qualification.json')==record
    assert read_json(Path(record['evidence_directory'])/'receipt.json')==record
    assert record['source_verified_unchanged'] and not record['workspace_deleted']
    for item in record['artifacts'].values():assert digest(Path(item['path']))==item['sha256']


def test_native_full_suite_is_node_local_current_running_and_durably_bound(harness,monkeypatch,capsys):
    release,root,local,test,config=harness
    test.write_text(test.read_text()+'\ndef test_second_generated_contract():\n assert True\n')
    previous={'status':'complete','source_sha256':'0'*64,'returncode':0}
    atomic_json(root/'state/cpu-qualification.json',previous)
    monkeypatch.setenv('SLURM_JOB_ID','12345');monkeypatch.setenv('SLURM_TMPDIR',str(local))
    monkeypatch.setenv('PYTEST_ADDOPTS','-k nonexistent_selector')
    original=cli.subprocess.Popen;starts=[]
    def spawn(*args,**kwargs):
        if not isinstance(args[0],list) or 'pytest' not in args[0]:return original(*args,**kwargs)
        current=read_json(root/'state/cpu-qualification.json')
        assert current['status']=='running' and current['slurm_job_id']=='12345'
        assert current['source_sha256']==source_identity(release)['sha256']
        assert 'PYTEST_ADDOPTS' not in kwargs['env']
        starts.append(current)
        return original(*args,**kwargs)
    monkeypatch.setattr(cli.subprocess,'Popen',spawn)
    record=cli._cpu_qualification(config,root,release)
    assert record['status']=='complete' and record['test_counts']=={'tests':2,'failures':0,'errors':0,'skipped':0}
    workspace=Path(record['workspace'])
    assert workspace.is_relative_to(local) and workspace!=local and record['workspace_kind']=='SLURM_TMPDIR'
    assert (workspace/'pytest').is_dir() and (workspace/'cache').is_dir()
    assert not (release/'.pytest_cache').exists()
    assert 'test_second_generated_contract' in capsys.readouterr().out
    assert read_json(Path(record['evidence_directory'])/'previous-cpu-qualification.json')==previous
    assert len(starts)==1
    check_receipt(record,root,config,release)


def test_native_does_not_fall_back_to_lustre_when_local_workspace_is_missing(harness,monkeypatch):
    release,root,_,_,config=harness
    monkeypatch.setenv('SLURM_JOB_ID','12345')
    monkeypatch.setattr(cli.subprocess,'Popen',lambda *a,**k:pytest.fail('Must not launch tests without node-local workspace'))
    with pytest.raises(RuntimeError,match='requires SLURM_TMPDIR'):cli._cpu_qualification(config,root,release)
    assert not (root/'tests').exists()


def test_local_runs_keep_unique_workspaces_and_previous_receipts(harness):
    release,root,_,_,config=harness
    first=cli._cpu_qualification(config,root,release)
    second=cli._cpu_qualification(config,root,release)
    assert first['status']==second['status']=='complete' and first['slurm_job_id'] is None
    assert first['workspace']!=second['workspace'] and first['evidence_directory']!=second['evidence_directory']
    assert Path(first['workspace']).is_dir() and Path(second['workspace']).is_relative_to(root/'tests')
    assert read_json(Path(second['evidence_directory'])/'previous-cpu-qualification.json')==first
    check_receipt(second,root,config,release)


def test_failing_test_keeps_full_log_junit_and_actual_fixture_bytes(harness):
    release,root,_,test,config=harness
    test.write_text('def test_generated_failure(tmp_path):\n (tmp_path/"actual-context.txt").write_text("failure context")\n assert False,"actual generated failure"\n')
    with pytest.raises(RuntimeError,match='qualification failed'):cli._cpu_qualification(config,root,release)
    record=read_json(root/'state/cpu-qualification.json')
    assert record['status']=='failed' and record['returncode']==1 and record['test_counts']['failures']==1
    assert 'actual generated failure' in Path(record['artifacts']['log']['path']).read_text()
    failure=record['failed_fixture_evidence'];assert failure['status']=='preserved'
    with tarfile.open(failure['path']) as archive:
        context=next(item for item in archive.getmembers() if item.name.endswith('actual-context.txt'))
        assert archive.extractfile(context).read()==b'failure context'
    check_receipt(record,root,config,release)


def test_skipped_tests_cannot_be_mislabeled_successful_qualification(harness):
    release,root,_,test,config=harness
    test.write_text('import pytest\n@pytest.mark.skip(reason="explicit fixture gap")\ndef test_required_fixture():\n assert True\n')
    with pytest.raises(ValueError,match='skipped tests'):cli._cpu_qualification(config,root,release)
    record=read_json(root/'state/cpu-qualification.json')
    assert record['status']=='failed' and record['returncode']==0 and record['test_counts']['skipped']==1
    check_receipt(record,root,config,release)


def test_allocation_deadline_includes_cleanup_margin(monkeypatch):
    monkeypatch.setenv('HHW_MAX_HOURS','1');monkeypatch.setenv('SLURM_JOB_END_TIME','1800')
    monkeypatch.setattr(cli.time,'time',lambda:1000.)
    assert cli._qualification_timeout()==(500.,300.)
    monkeypatch.setenv('SLURM_JOB_END_TIME','1299')
    with pytest.raises(RuntimeError,match='cleanup margin'):cli._qualification_timeout()


def test_timeout_stops_owned_process_and_preserves_incomplete_evidence(harness,monkeypatch):
    release,root,_,test,config=harness
    test.write_text('import time\ndef test_generated_stall(tmp_path):\n (tmp_path/"started.txt").write_text("started")\n time.sleep(60)\n')
    monkeypatch.setattr(cli,'_qualification_timeout',lambda:(1.5,300.))
    original=cli.subprocess.Popen;processes=[]
    def spawn(*args,**kwargs):
        process=original(*args,**kwargs)
        if isinstance(args[0],list) and 'pytest' in args[0]:processes.append(process)
        return process
    monkeypatch.setattr(cli.subprocess,'Popen',spawn)
    started=time.monotonic()
    with pytest.raises(RuntimeError,match='qualification timed_out'):cli._cpu_qualification(config,root,release)
    record=read_json(root/'state/cpu-qualification.json')
    assert time.monotonic()-started<20 and processes[0].poll() is not None
    assert record['status']=='timed_out' and record['test_counts'] is None and record['returncode']!=0
    assert Path(record['artifacts']['log']['path']).is_file()
    check_receipt(record,root,config,release)


def test_source_change_during_child_run_refuses_success(harness):
    release,root,_,test,config=harness
    test.write_text('from pathlib import Path\ndef test_generated_mutation():\n Path("src/horsehair/cli.py").write_text("modified fixture source")\n assert True\n')
    with pytest.raises(ValueError,match='source/configuration changed'):cli._cpu_qualification(config,root,release)
    record=read_json(root/'state/cpu-qualification.json')
    assert record['status']=='failed' and record['returncode']==0 and not record['source_verified_unchanged']
    assert record['failed_fixture_evidence']['status']=='preserved'


def test_failure_archive_omits_symlinks_and_never_copies_their_targets(tmp_path,monkeypatch):
    workspace=tmp_path/'workspace';evidence=tmp_path/'evidence';workspace.mkdir();evidence.mkdir()
    (workspace/'fixture.txt').write_text('preserved fixture')
    outside=tmp_path/'outside-private.txt';outside.write_text('must not copy')
    link=workspace/'link.txt'
    try:link.symlink_to(outside)
    except OSError:
        link.write_text('simulated symlink fixture')
        original=Path.is_symlink
        monkeypatch.setattr(Path,'is_symlink',lambda path:path==link or original(path))
        monkeypatch.setattr(cli.os,'readlink',lambda path:str(outside))
    result=cli._preserve_qualification_failure(workspace,evidence,time.monotonic()+30)
    assert result['status']=='preserved' and len(result['omitted_symlinks'])==1
    with tarfile.open(result['path']) as archive:assert archive.getnames()==['fixture.txt']
