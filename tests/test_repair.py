import copy
import shlex
from pathlib import Path

import pytest

from horsehair import repair
from horsehair.core import atomic_json, canonical_hash, read_json, source_identity


def fixture(tmp_path, phase="managed-initial", replace_controllers=False):
    root, primary, preprocessing = tmp_path / "study", tmp_path / "v7", tmp_path / "v8"
    config = {"paths":{"root":str(root)}, "resources":{"cpu_ceiling":16}}
    code = "def run(stage):\n    if stage in ('ingest', 'validate'):\n        return 0\n    return 1\n"
    for directory, changed in [(primary, False), (preprocessing, True)]:
        atomic_json(directory / "configs/study.json", config)
        path = directory / "src/horsehair/behaviour.py"
        path.parent.mkdir(parents=True)
        path.write_text(code.replace("return 0", "return 2") if changed else code)
        identity = source_identity(directory)
        atomic_json(directory / "release.json", {"release_id":"v8" if changed else "v7", "config_sha256":canonical_hash(config), "source_identity":identity})
    identity = source_identity(primary)
    def task(name, directory, script_directory, hold=False, array=None):
        script = script_directory / (name + ".sbatch")
        script.parent.mkdir(parents=True, exist_ok=True)
        text = ("#!/bin/bash\n" + ("#SBATCH --hold\n" if hold else "")
            + "cd " + shlex.quote(directory.as_posix()) + "\n"
            + "export PYTHONPATH=" + shlex.quote((directory / "src").as_posix()) + ':"${PYTHONPATH:-}"\n'
            + "python -m horsehair.cli " + name + "\n")
        script.write_text(text)
        resources = {"cpus":8,"memory":"64G","hours":8,"gpu":False}
        if array:resources["array"] = array
        return {"name":name,"script":str(script),"script_sha256":canonical_hash(text),"resources":resources,"depends":[]}
    initial_names = ["source-audit", "source-replication", "agent-benchmark-training", *[f"pipeline-controller-{i}" for i in range(4)]]
    initial_path = root / "plans/v7-managed-initial/plan.json"
    initial = {"phase":"managed-initial","root":str(root),"config_sha256":canonical_hash(config),"release_identity":identity,
        "tasks":[task(name, primary, initial_path.parent) for name in initial_names]}
    initial_ledger = {"plan_sha256":canonical_hash(initial),"source_sha256":identity["sha256"],"config_sha256":canonical_hash(config),
        "tasks":{name:{"state":"submitted","job_id":str(10+i),"script_sha256":initial["tasks"][i]["script_sha256"]} for i,name in enumerate(initial_names)}}
    atomic_json(initial_path, initial)
    atomic_json(root / "state/v7-managed-initial-submission.json", initial_ledger)
    if phase == "rna-full":
        names = ["rna-full-plan", "rna-counts", "ant-phase"]
        plan_path = root / "plans/v7-rna-full/plan.json"
        old = {**initial, "phase":phase,"tasks":[task(name, primary, plan_path.parent, array="0-2%2" if name=="rna-counts" else None) for name in names]}
        ledger = {**initial_ledger,"plan_sha256":canonical_hash(old),"tasks":{name:{"state":"submitted","job_id":str(30+i),"script_sha256":old["tasks"][i]["script_sha256"]} for i,name in enumerate(names)}}
        atomic_json(plan_path,old)
        atomic_json(root / "state/v7-rna-full-submission.json",ledger)
        targets = {"rna-counts", "ant-phase"}
    else:
        plan_path, old, ledger = initial_path, initial, initial_ledger
        targets = {"source-audit", "source-replication"}
        if replace_controllers:targets |= {f"pipeline-controller-{i}" for i in range(4)}
    executions = {name:preprocessing if name=="source-audit" else primary for name in targets}
    replacement_path = root / "plans/repair-plan/plan.json"
    candidates = [task(name,executions[name],replacement_path.parent,hold=name.startswith("pipeline-controller-"),
        array="0-2%2" if name=="rna-counts" else None) for name in sorted(targets)]
    replacement = {**old,"tasks":candidates}
    submitted = {**ledger,"plan_sha256":canonical_hash(replacement),"tasks":{name:{"state":"submitted","job_id":str(100+i),
        "script_sha256":candidates[i]["script_sha256"],"issued_native_dependencies":["800","801"]} for i,name in enumerate(sorted(targets))}}
    atomic_json(replacement_path,replacement)
    replacement_ledger = root / "state/repair-submission.json"
    atomic_json(replacement_ledger,submitted)
    descriptor = {"amendment_id":"scoped-repair","phase":phase,"root":str(root),"primary_release":str(primary),
        "replacement_plan":str(replacement_path),"replacement_ledger":str(replacement_ledger),"reason":"Synthetic source-scoped repair fixture",
        "execution_releases":{name:str(path) for name,path in executions.items()},
        "required_external_dependencies":{"source-audit":["800","801"]} if "source-audit" in targets else {}}
    descriptor_path = root / "plans/repair-plan/descriptor.json"
    atomic_json(descriptor_path,descriptor)
    native = {record["job_id"]:{"state":"CANCELLED","exit_code":"0:15"} for record in initial_ledger["tasks"].values()}
    native[initial_ledger["tasks"]["agent-benchmark-training"]["job_id"]] = {"state":"COMPLETED","exit_code":"0:0"}
    for item in old["tasks"]:
        record = ledger["tasks"][item["name"]]
        keys = [record["job_id"]+"_"+str(i) for i in range(3)] if item["name"]=="rna-counts" else [record["job_id"]]
        for key in keys:native[key] = {"state":"COMPLETED" if item["name"]=="rna-full-plan" else "CANCELLED","exit_code":"0:0" if item["name"]=="rna-full-plan" else "0:15"}
    for item in candidates:
        record = submitted["tasks"][item["name"]]
        keys = [record["job_id"]+"_"+str(i) for i in range(3)] if item["name"]=="rna-counts" else [record["job_id"]]
        for key in keys:native[key] = {"state":"PENDING","exit_code":"0:0"}
    calls=[]
    accountant = lambda study,ids: calls.append(ids) or {"pending_native_records":[],"spent_cpu_core_hours":10.,"spent_gpu_hours":0.}
    return locals()


def register(f):
    return repair.register_amendment(f["descriptor_path"],native=f["native"],accountant=f["accountant"])


@pytest.mark.parametrize("phase", ["managed-initial","rna-full"])
def test_repair_preserves_history_unaffected_jobs_and_actual_execution_origins(tmp_path,phase):
    f=fixture(tmp_path,phase,replace_controllers=True)
    before_plan=f["plan_path"].read_bytes()
    ledger_path=f["root"] / ("state/v7-"+phase+"-submission.json")
    before_ledger=ledger_path.read_bytes()
    result=register(f)
    assert result["status"]=="complete"
    archive=Path(result["archive"])
    assert (archive/"original-plan.json").read_bytes()==before_plan
    assert (archive/"original-ledger.json").read_bytes()==before_ledger
    plan,ledger=read_json(f["plan_path"]),read_json(ledger_path)
    assert plan["release_identity"]==f["identity"] and ledger["source_sha256"]==f["identity"]["sha256"]
    assert ledger["plan_sha256"]==canonical_hash(plan)
    retained="rna-full-plan" if phase=="rna-full" else "agent-benchmark-training"
    assert ledger["tasks"][retained]==f["ledger"]["tasks"][retained]
    if phase=="managed-initial":
        audit=next(task for task in plan["tasks"] if task["name"]=="source-audit")
        assert audit["execution_release"]["release_id"]=="v8"
        assert ledger["tasks"]["source-replication"]["execution_release"]["release_id"]=="v7"
    assert register(f)==result and len(f["calls"])==1


@pytest.mark.parametrize("fault", ["live-controller","live-old-task","valid-old-task","missing-new-ack","wrong-script","wrong-resource","missing-dependency","changed-model","escaped-path","duplicate-task","afterany-dependency"])
def test_repair_rejects_unsafe_registration_without_changing_canonical_pairs(tmp_path,fault):
    f=fixture(tmp_path)
    if fault=="live-controller":f["native"][f["initial_ledger"]["tasks"]["pipeline-controller-0"]["job_id"]]["state"]="RUNNING"
    elif fault=="live-old-task":f["native"][f["ledger"]["tasks"]["source-audit"]["job_id"]]["state"]="RUNNING"
    elif fault=="valid-old-task":f["native"][f["ledger"]["tasks"]["source-audit"]["job_id"]].update(state="COMPLETED",exit_code="0:0")
    elif fault=="missing-new-ack":f["native"].pop(f["submitted"]["tasks"]["source-audit"]["job_id"])
    elif fault=="wrong-script":Path(f["candidates"][0]["script"]).write_text("unexpected command")
    elif fault=="changed-model":
        path=f["preprocessing"] / "src/horsehair/behaviour.py"
        path.write_text(path.read_text().replace("return 1","return 3"))
        record=read_json(f["preprocessing"] / "release.json")
        record["source_identity"]=source_identity(f["preprocessing"])
        atomic_json(f["preprocessing"] / "release.json",record)
    elif fault=="escaped-path":
        f["descriptor"]["replacement_plan"]=str(tmp_path / "outside.json")
        atomic_json(f["descriptor_path"],f["descriptor"])
    else:
        replacement=copy.deepcopy(f["replacement"]);submitted=copy.deepcopy(f["submitted"])
        if fault=="wrong-resource":replacement["tasks"][0]["resources"]["hours"]+=1
        elif fault=="duplicate-task":replacement["tasks"].append(copy.deepcopy(replacement["tasks"][0]))
        elif fault=="afterany-dependency":replacement["tasks"][0]["dependency_kind"]="afterany"
        else:submitted["tasks"]["source-audit"]["issued_native_dependencies"]=[]
        submitted["plan_sha256"]=canonical_hash(replacement)
        atomic_json(f["replacement_path"],replacement);atomic_json(f["replacement_ledger"],submitted)
    plan_bytes=f["plan_path"].read_bytes();ledger_path=f["root"] / "state/v7-managed-initial-submission.json"
    ledger_bytes=ledger_path.read_bytes()
    with pytest.raises((ValueError,FileNotFoundError)):register(f)
    assert f["plan_path"].read_bytes()==plan_bytes and ledger_path.read_bytes()==ledger_bytes and not f["calls"]


def test_registration_recovers_interrupted_plan_ledger_pair_without_resubmission(tmp_path,monkeypatch):
    f=fixture(tmp_path)
    original=repair.atomic_json; failed=[False]
    ledger_path=f["root"] / "state/v7-managed-initial-submission.json"
    def interrupted(path,value):
        if Path(path)==ledger_path and not failed[0]:
            failed[0]=True
            raise OSError("Synthetic pair-write interruption")
        return original(path,value)
    monkeypatch.setattr(repair,"atomic_json",interrupted)
    with pytest.raises(OSError):register(f)
    assert read_json(f["plan_path"])["dag_amendments"]
    assert "dag_amendments" not in read_json(ledger_path)
    result=register(f)
    assert result["status"]=="complete" and read_json(ledger_path)["plan_sha256"]==canonical_hash(read_json(f["plan_path"]))
    assert len(f["calls"])==2  # Idempotent native accounting; no job submission.


def test_native_array_expansion_is_repair_specific(monkeypatch):
    calls=[]
    monkeypatch.setattr(repair.subprocess,"run",lambda command,**kwargs:calls.append(command))
    repair._expanded_accounting_runner(["sacct","-j","123"])
    repair._expanded_accounting_runner(["scontrol","show","job","123"])
    assert calls==[["sacct","--array","-j","123"],["scontrol","show","job","123"]]


@pytest.mark.parametrize("changed", [False, True])
def test_original_completed_precondition_is_revalidated_without_rewriting_actual_acknowledgement(tmp_path,changed):
    from horsehair.slurm import completed_native_dependency
    f=fixture(tmp_path,"rna-full")
    budget_path=f["root"] / "state/resource-budget.json"
    native_record={"state":"COMPLETED","exit_code":"0:0","elapsed_seconds":60,"cpus":8,"gpus":0,
        "basis":"sacct final allocated wall time"}
    atomic_json(budget_path,{"native_jobs":{"900":native_record}})
    proof=completed_native_dependency(f["root"],"900")
    old=copy.deepcopy(f["old"])
    next(task for task in old["tasks"] if task["name"]=="ant-phase")["completed_external_dependencies"]=[proof]
    old_ledger=copy.deepcopy(f["ledger"])
    old_ledger["plan_sha256"]=canonical_hash(old)
    atomic_json(f["plan_path"],old)
    ledger_path=f["root"] / "state/v7-rna-full-submission.json"
    atomic_json(ledger_path,old_ledger)
    replacement=copy.deepcopy(f["replacement"])
    submitted=copy.deepcopy(f["submitted"])
    submitted["tasks"]["ant-phase"].update(command=["sbatch","--parsable","unchanged-script"],
        completed_native_dependencies=[],dependency_resolution_sha256="actual-acknowledgement-fixture")
    atomic_json(f["replacement_ledger"],submitted)
    actual_ack=copy.deepcopy(submitted["tasks"]["ant-phase"])
    if changed:
        atomic_json(budget_path,{"native_jobs":{"900":{**native_record,"elapsed_seconds":61}}})
        before=f["plan_path"].read_bytes(),ledger_path.read_bytes()
        with pytest.raises(ValueError,match="missing or changed"):register(f)
        assert (f["plan_path"].read_bytes(),ledger_path.read_bytes())==before and not f["calls"]
    else:
        result=register(f)
        plan,ledger=read_json(f["plan_path"]),read_json(ledger_path)
        task=next(task for task in plan["tasks"] if task["name"]=="ant-phase")
        assert task["completed_external_dependencies"]==[proof]
        current=ledger["tasks"]["ant-phase"]
        assert current["preserved_original_completed_dependencies"]==[proof]
        for key in ("command","issued_native_dependencies","completed_native_dependencies","dependency_resolution_sha256"):
            assert current[key]==actual_ack[key]
        assert result["preserved_original_completed_dependencies"]=={"ant-phase":[proof]}
