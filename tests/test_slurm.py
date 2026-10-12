from pathlib import Path
from types import SimpleNamespace
import pytest
from horsehair.core import atomic_json, canonical_hash
from horsehair.slurm import submit,_script


def test_submission_does_not_duplicate_jobs(tmp_path):
    script=tmp_path/'job.sbatch';script.write_text('#!/bin/bash\ntrue\n')
    plan={'tasks':[{'name':'a','script':str(script),'script_sha256':canonical_hash(script.read_text()),'depends':[]}]}
    atomic_json(tmp_path/'plan.json',plan)
    calls=[]
    def runner(command,**kwargs):
        calls.append(command);return SimpleNamespace(returncode=0,stdout='12345\n',stderr='')
    first=submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)
    second=submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)
    assert first==second and len(calls)==1


def test_ambiguous_sbatch_is_never_replayed(tmp_path):
    script=tmp_path/'job.sbatch';script.write_text('true\n')
    atomic_json(tmp_path/'plan.json',{'tasks':[{'name':'a','script':str(script),'script_sha256':canonical_hash(script.read_text()),'depends':[]}]})
    runner=lambda *a,**k:SimpleNamespace(returncode=1,stdout='',stderr='connection lost')
    with pytest.raises(RuntimeError):submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)
    with pytest.raises(RuntimeError,match='prior'):submit(tmp_path/'plan.json',tmp_path/'ledger.json',runner=runner)


def test_alliance_module_python_paths_survive_source_overlay():
    # Alliance's scipy-stack/Arrow packages and some wheel dependencies are
    # supplied through module PYTHONPATH, rather than inside the virtualenv.
    config={'paths':{'root':'/scratch/pwa209/horsehair-worm','cpu_account':'def-ptewarie_cpu','environment':'/home/pwa209/hhw-environment'}}
    script=_script(config,Path('/project/release'),'qualify','python -m horsehair.cli qualify')
    overlay=next(line for line in script.splitlines() if line.startswith('export PYTHONPATH='))
    assert overlay.endswith(':"${PYTHONPATH:-}"')

