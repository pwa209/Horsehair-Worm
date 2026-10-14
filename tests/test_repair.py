import copy
import shlex
from pathlib import Path

import pytest

from horsehair import repair
from horsehair.core import atomic_json, canonical_hash, digest, read_json, source_identity


def controller_maintenance_fixture(tmp_path):
    """Synthetic framework qualification and scheduler facts; never real results."""
    from horsehair.slurm import completed_native_dependency
    f = fixture(tmp_path, replace_controllers=True)
    controllers = {f'pipeline-controller-{index}' for index in range(4)}
    old = copy.deepcopy(f['old'])
    old_ledger = copy.deepcopy(f['ledger'])
    for item in old['tasks']:
        if item['name'] not in controllers:
            continue
        index = int(item['name'].rsplit('-', 1)[1])
        item['dependency_kind'] = 'afterok' if index == 0 else 'afterany'
        item['depends'] = [] if index == 0 else [f'pipeline-controller-{index - 1}']
        path = Path(item['script'])
        text = path.read_text().replace('python -m horsehair.cli ' + item['name'],
            'python -m horsehair.cli --config configs/study.json verify-source --identity release.json\n'
            + 'python -m horsehair.cli --config configs/study.json pipeline --segment ' + str(index))
        path.write_text(text)
        item['script_sha256'] = canonical_hash(text)
        old_ledger['tasks'][item['name']]['script_sha256'] = item['script_sha256']
    old_ledger['plan_sha256'] = canonical_hash(old)
    atomic_json(f['plan_path'], old)
    atomic_json(f['root']/'state/v7-managed-initial-submission.json', old_ledger)
    budget = {'spent_cpu_core_hours': 10., 'spent_gpu_hours': 0., 'leases': {}, 'native_jobs': {
        '990': {'state': 'COMPLETED', 'exit_code': '0:0', 'elapsed_seconds': 60, 'cpus': 2, 'gpus': 0,
                'basis': 'sacct final allocated wall time'}}}
    atomic_json(f['root']/'state/resource-budget.json', budget)
    framework = f['preprocessing']
    framework_identity = source_identity(framework)
    case = f['root']/'maintenance/controller-qualification-fixture'
    case.mkdir(parents=True)
    log, junit = case/'pytest.log', case/'junit.xml'
    log.write_text('Generated qualification fixture: 1 passed\n')
    junit.write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"><testcase name="generated_fixture"/></testsuite></testsuites>')
    qualification = {'status': 'complete', 'returncode': 0, 'slurm_job_id': '990',
        'source_identity': framework_identity, 'source_sha256': framework_identity['sha256'],
        'primary_source_sha256': f['identity']['sha256'], 'config_sha256': canonical_hash(f['config']),
        'source_verified_unchanged': True,
        'test_selection': 'complete source/tests directory; no environment selectors or third-party plugin autoload',
        'test_counts': {'tests': 1, 'failures': 0, 'errors': 0, 'skipped': 0},
        'artifacts': {label: {'path': str(path.resolve()), 'sha256': digest(path)}
                      for label, path in [('log', log), ('junit', junit)]}}
    qualification_path = case/'qualification.json'
    atomic_json(qualification_path, qualification)
    replacement, submitted = copy.deepcopy(old), copy.deepcopy(old_ledger)
    for item in replacement['tasks']:
        if item['name'] not in controllers:
            continue
        index = int(item['name'].rsplit('-', 1)[1])
        text = Path(item['script']).read_text()
        text = text.replace('cd ' + shlex.quote(f['primary'].as_posix()), 'cd ' + shlex.quote(framework.as_posix()))
        text = text.replace(shlex.quote((f['primary']/'src').as_posix()), shlex.quote((framework/'src').as_posix()))
        text = text.replace('#!/bin/bash\n', '#!/bin/bash\n#SBATCH --hold\n')
        text = text.replace('pipeline --segment ' + str(index),
                            'pipeline --segment ' + str(index) + ' --primary-release ' + shlex.quote(f['primary'].as_posix()))
        path = f['replacement_path'].parent/(item['name']+'.sbatch')
        path.write_text(text)
        item.update(script=str(path), script_sha256=canonical_hash(text))
        proof = completed_native_dependency(f['root'], '990', dependency_kind=item['dependency_kind'])
        item['completed_external_dependencies'] = [proof]
        submitted['tasks'][item['name']] = {'state': 'submitted', 'job_id': str(200 + index),
            'script_sha256': item['script_sha256'], 'dependency_kind': item['dependency_kind'],
            'issued_native_dependencies': [], 'completed_native_dependencies': [proof]}
    submitted['plan_sha256'] = canonical_hash(replacement)
    atomic_json(f['replacement_path'], replacement)
    atomic_json(f['replacement_ledger'], submitted)
    descriptor = {**f['descriptor'], 'execution_releases': {name: str(framework) for name in controllers},
        'required_external_dependencies': {name: ['990'] for name in controllers},
        'controller_maintenance': {'framework_release': str(framework), 'primary_release': str(f['primary']),
            'qualification_path': str(qualification_path), 'qualification_sha256': digest(qualification_path),
            'qualification_job_id': '990'}}
    atomic_json(f['descriptor_path'], descriptor)
    native = {key: value for key, value in f['native'].items() if int(key) < 100}
    native.update({str(200 + index): {'state': 'PENDING', 'exit_code': '0:0'} for index in range(4)})
    native['990'] = {'state': 'COMPLETED', 'exit_code': '0:0', 'elapsed_seconds': 60, 'cpus': 2}
    f.update(old=old, ledger=old_ledger, replacement=replacement, submitted=submitted, descriptor=descriptor,
             native=native, framework=framework, framework_identity=framework_identity,
             qualification_path=qualification_path, qualification=qualification, controllers=controllers)
    return f


def test_qualified_maintenance_replaces_only_all_held_controllers_and_keeps_primary_science(tmp_path):
    f = controller_maintenance_fixture(tmp_path)
    result = register(f)
    assert result['status'] == 'complete'
    plan = read_json(f['plan_path'])
    assert plan['release_identity'] == f['identity']
    for item in plan['tasks']:
        if item['name'] in f['controllers']:
            assert item['execution_release']['source_identity'] == f['framework_identity']
            assert '--primary-release ' in Path(item['script']).read_text()
        else:
            assert item == next(old for old in f['old']['tasks'] if old['name'] == item['name'])
    audit = plan['dag_amendments'][-1]['controller_maintenance']
    assert audit['primary_source_identity'] == f['identity']
    assert audit['framework_source_identity'] == f['framework_identity'] != f['identity']
    assert register(f) == result


@pytest.mark.parametrize('fault', ['missing-primary', 'primary-source', 'framework-source', 'framework-config',
    'missing-qualification', 'qualification-pin', 'qualification-source', 'qualification-config',
    'qualification-primary', 'qualification-skipped', 'qualification-junit', 'missing-native', 'failed-native',
    'zero-allocation', 'missing-required-dependency', 'missing-completed-proof', 'wrong-proof-kind',
    'missing-planned-proof', 'wrong-primary-command', 'extra-command', 'changed-verification',
    'changed-resources', 'unheld-controller', 'partial-roster', 'mixed-science-target', 'missing-maintenance-declaration'])
def test_maintenance_rejects_unsafe_provenance_commands_or_qualification_without_registration(tmp_path, fault):
    f = controller_maintenance_fixture(tmp_path)
    name = 'pipeline-controller-0'
    item = next(task for task in f['replacement']['tasks'] if task['name'] == name)
    proof = f['submitted']['tasks'][name]
    if fault == 'missing-primary':
        f['descriptor']['controller_maintenance']['primary_release'] = str(tmp_path/'missing-primary')
    elif fault in {'primary-source', 'framework-source'}:
        target = f['primary'] if fault == 'primary-source' else f['framework']
        (target/'src/horsehair/behaviour.py').write_text('# Changed source fixture\n')
    elif fault == 'framework-config':
        config = copy.deepcopy(f['config'])
        config['resources']['cpu_ceiling'] += 1
        atomic_json(f['framework']/'configs/study.json', config)
        record = read_json(f['framework']/'release.json')
        record.update(source_identity=source_identity(f['framework']), config_sha256=canonical_hash(config))
        atomic_json(f['framework']/'release.json', record)
    elif fault == 'missing-qualification':
        f['qualification_path'].unlink()
    elif fault == 'qualification-pin':
        f['descriptor']['controller_maintenance']['qualification_sha256'] = '0' * 64
    elif fault.startswith('qualification-'):
        if fault == 'qualification-source': f['qualification']['source_identity'] = f['identity']
        elif fault == 'qualification-config': f['qualification']['config_sha256'] = '0' * 64
        elif fault == 'qualification-primary': f['qualification']['primary_source_sha256'] = '0' * 64
        elif fault == 'qualification-skipped': f['qualification']['test_counts']['skipped'] = 1
        else: Path(f['qualification']['artifacts']['junit']['path']).write_text('<changed/>')
        atomic_json(f['qualification_path'], f['qualification'])
        f['descriptor']['controller_maintenance']['qualification_sha256'] = digest(f['qualification_path'])
    elif fault == 'missing-native':
        f['native'].pop('990')
    elif fault == 'failed-native':
        f['native']['990'].update(state='FAILED', exit_code='1:0')
    elif fault == 'zero-allocation':
        f['native']['990']['cpus'] = 0
    elif fault == 'missing-required-dependency':
        f['descriptor']['required_external_dependencies'][name] = []
    elif fault == 'missing-completed-proof':
        proof['completed_native_dependencies'] = []
        proof['issued_native_dependencies'] = ['990']
    elif fault == 'wrong-proof-kind':
        proof['completed_native_dependencies'][0]['dependency_kind'] = 'afterany'
    elif fault == 'missing-planned-proof':
        item['completed_external_dependencies'] = []
    elif fault in {'wrong-primary-command', 'extra-command', 'changed-verification', 'unheld-controller'}:
        path = Path(item['script'])
        text = path.read_text()
        if fault == 'wrong-primary-command': text = text.replace(shlex.quote(f['primary'].as_posix()), shlex.quote(f['framework'].as_posix()))
        elif fault == 'extra-command': text += 'python -m horsehair.cli behaviour --stage fit\n'
        elif fault == 'changed-verification': text = text.replace('verify-source --identity release.json', 'verify-source --identity unrelated.json')
        else: text = text.replace('#SBATCH --hold\n', '')
        path.write_text(text)
        item['script_sha256'] = canonical_hash(text)
        proof['script_sha256'] = item['script_sha256']
    elif fault == 'changed-resources':
        item['resources']['hours'] += 1
    elif fault == 'partial-roster':
        f['descriptor']['execution_releases'].pop('pipeline-controller-3')
    elif fault == 'mixed-science-target':
        f['descriptor']['execution_releases']['source-replication'] = str(f['framework'])
    else:
        f['descriptor'].pop('controller_maintenance')
    f['submitted']['plan_sha256'] = canonical_hash(f['replacement'])
    atomic_json(f['replacement_path'], f['replacement'])
    atomic_json(f['replacement_ledger'], f['submitted'])
    atomic_json(f['descriptor_path'], f['descriptor'])
    ledger = f['root']/'state/v7-managed-initial-submission.json'
    before = f['plan_path'].read_bytes(), ledger.read_bytes()
    with pytest.raises((ValueError, FileNotFoundError)):
        register(f)
    assert (f['plan_path'].read_bytes(), ledger.read_bytes()) == before
    assert not f['calls']


def fixture(tmp_path, phase="managed-initial", replace_controllers=False):
    root, primary, preprocessing = tmp_path / "study", tmp_path / "v7", tmp_path / "v8"
    config = {"paths":{"root":str(root)}, "resources":{"cpu_ceiling":16,
        "cpu_core_hours_cap":2000, "gpu_hours_cap":400}}
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
            + "#SBATCH --time=8:00:00\nexport HHW_MAX_HOURS=8\n"
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


def template_rows(base="5469496",state="CANCELLED",n=3):
    end="2026-10-13T11:38:17" if state=="CANCELLED" else "Unknown"
    return [[base+"_"+str(index),base,state,"0:0","0","0","","None assigned",
             "None" if state=="CANCELLED" else "Unknown",end,"pwa209","def-ptewarie_cpu",""] for index in range(n)]


def parse_rows(rows,rosters=None):
    return repair._parse_repair_native("\n".join("|".join(row) for row in rows),
        rosters or {"5469496":set(range(3))},owner="pwa209",account="def-ptewarie_cpu")


@pytest.mark.parametrize("state", ["CANCELLED","PENDING"])
def test_actual_never_started_array_template_rows_retain_shared_raw_as_metadata_only(state):
    records=parse_rows(template_rows(state=state))
    assert set(records)=={"5469496_0","5469496_1","5469496_2"}
    assert all(row["job_id_raw"]=="5469496" and row["unallocated_metadata"] for row in records.values())


@pytest.mark.parametrize("field,value",[(4,"1"),(5,"1"),(6,"cpu=1"),(7,"nc1000"),(8,"2026-10-13T10:00:00"),(12,"8K")])
def test_one_changed_template_marker_cannot_claim_zero_or_physical_allocation(field,value):
    rows=template_rows()
    rows[0][field]=value
    with pytest.raises(ValueError,match="neither proven unused"):parse_rows(rows)


def test_real_allocated_member_can_share_parent_raw_with_pending_metadata_without_cost_aliasing():
    rows=template_rows(state="PENDING")
    rows[0]=["5469496_0","5469496","COMPLETED","0:0","60","8","cpu=8","nc1000",
             "2026-10-13T10:00:00","2026-10-13T10:01:00","pwa209","def-ptewarie_cpu",""]
    records=parse_rows(rows)
    assert not records["5469496_0"]["unallocated_metadata"]
    assert records["5469496_1"]["unallocated_metadata"]
    rows[1]=["5469496_1","5469496","COMPLETED","0:0","60","8","cpu=8","nc1000",
             "2026-10-13T10:00:00","2026-10-13T10:01:00","pwa209","def-ptewarie_cpu",""]
    with pytest.raises(ValueError,match="physical native allocation"):parse_rows(rows)


@pytest.mark.parametrize("conflict",[None,"lease","allocation"])
def test_terminal_unused_cost_receipt_requires_complete_roster_and_no_lease_or_allocation(tmp_path,conflict):
    root=tmp_path
    task={"resources":{"array":"0-2%2"}}
    checks=[("counts",task,{"job_id":"5469496"})]
    if conflict:
        key="leases" if conflict=="lease" else "native_jobs"
        atomic_json(root/"state/resource-budget.json",{key:{"5469496_1":{"state":"running"}}})
        with pytest.raises(ValueError,match="existing lease/allocation"):repair._unused_terminal_arrays(root,checks,parse_rows(template_rows()))
    else:
        result=repair._unused_terminal_arrays(root,checks,parse_rows(template_rows()))["5469496"]
        assert result["cpu_core_hours"]==result["gpu_hours"]==0
        assert result["status"]=="terminal_never_allocated_array_template"
        partial=parse_rows(template_rows()[:2])
        assert repair._unused_terminal_arrays(root,checks,partial)=={}


def test_accounting_adapter_filters_template_rows_and_preserves_actual_physical_facts(monkeypatch):
    rows=template_rows(state="PENDING")
    rows[0]=["5469496_0","5469496","COMPLETED","0:0","60","8","cpu=8","nc1000",
             "2026-10-13T10:00:00","2026-10-13T10:01:00","pwa209","def-ptewarie_cpu",""]
    facts=parse_rows(rows)
    monkeypatch.setattr(repair,"_repair_native_states",lambda *args,**kwargs:facts)
    response=repair._repair_allocation_accounting_runner({"5469496":{0,1,2}})(["sacct","-j","5469496"])
    assert response.stdout=="5469496_0|5469496|COMPLETED|60|8|cpu=8|0:0|"


def test_registration_excludes_only_whole_proven_unused_old_array_from_cost_accounting(tmp_path):
    f=fixture(tmp_path,"rna-full")
    f["native"].update(parse_rows([*template_rows(base="31")],{"31":{0,1,2}}))
    result=register(f)
    assert "31" in result["unused_old_array_templates"]
    assert "31" not in f["calls"][0]
    assert result["unused_old_array_templates"]["31"]["cpu_core_hours"]==0


def test_unique_scalar_never_started_record_keeps_existing_zero_cost_accounting_semantics(tmp_path,monkeypatch):
    from horsehair.budget import native_accounting
    row=["200","200","CANCELLED","0:0","0","0","","None assigned","None",
         "2026-10-13T11:38:17","pwa209","def-ptewarie_cpu",""]
    facts=parse_rows([row])
    assert facts["200"]["unallocated_metadata"] is False
    assert facts["200"]["identity_basis"]=="Unique scalar native job; zero-allocation queue record"
    monkeypatch.setattr(repair,"_repair_native_states",lambda *args,**kwargs:facts)
    runner=repair._repair_allocation_accounting_runner({})
    assert runner(["sacct","-j","200"]).stdout=="200|200|CANCELLED|0|0||0:0|"
    result=native_accounting(tmp_path,["200"],runner=runner)
    assert not result["pending_native_records"] and result["spent_cpu_core_hours"]==result["spent_gpu_hours"]==0
    assert read_json(tmp_path/"state/resource-budget.json")["native_jobs"]["200"]["cpus"]==0


@pytest.mark.parametrize("rss",["0K","8K"])
def test_template_positive_step_memory_is_rejected_but_zero_step_measurement_is_not_allocation(rss):
    rows=template_rows()
    step=rows[0].copy()
    step[0]+=".batch";step[1]+=".batch";step[12]=rss
    if rss=="8K":
        with pytest.raises(ValueError,match="positive native step memory"):parse_rows([*rows,step])
    else:
        assert parse_rows([*rows,step])["5469496_0"]["unallocated_metadata"]


def runtime_fixture(tmp_path, hours=24):
    f = fixture(tmp_path)
    audit = next(task for task in f["replacement"]["tasks"] if task["name"] == "source-audit")
    audit["resources"]["hours"] = hours
    path = Path(audit["script"])
    text = path.read_text().replace("#!/bin/bash\n", "#!/bin/bash\n#SBATCH --hold\n").replace(
        "#SBATCH --time=8:00:00", f"#SBATCH --time={hours}:00:00").replace(
        "export HHW_MAX_HOURS=8", f"export HHW_MAX_HOURS={hours}")
    path.write_text(text)
    audit["script_sha256"] = canonical_hash(text)
    f["submitted"]["tasks"]["source-audit"]["script_sha256"] = audit["script_sha256"]
    f["descriptor"]["runtime_adjustments"] = {"source-audit":{"old_hours":8,"new_hours":hours,
        "failed_job_id":"10", "justification":"Measured full eight-hour CPU-bound audit timed out; increase the bounded allocation only"}}
    f["native"]["10"] = {"state":"TIMEOUT", "exit_code":"0:9", "elapsed_seconds":28801,"cpus":8,
        "alloc_tres":"billing=8,cpu=8,mem=64G,node=1", "job_id_raw":"10", "node_list":"nc1000",
        "start":"2026-10-13T12:00:00", "end":"2026-10-13T20:00:01", "unallocated_metadata":False}
    atomic_json(f["root"] / "state/resource-budget.json", {"spent_cpu_core_hours":74., "spent_gpu_hours":0.,
        "native_jobs":{"10":{**f["native"]["10"], "basis":"sacct final allocated wall time"}}, "leases":{}})
    refresh_runtime_fixture(f)
    return f


def refresh_runtime_fixture(f):
    f["submitted"]["plan_sha256"] = canonical_hash(f["replacement"])
    atomic_json(f["replacement_path"], f["replacement"])
    atomic_json(f["replacement_ledger"], f["submitted"])
    atomic_json(f["descriptor_path"], f["descriptor"])


def test_audit_runtime_amendment_is_narrow_native_bound_and_readmitted(tmp_path):
    f = runtime_fixture(tmp_path)
    original = copy.deepcopy(f["old"])
    result = register(f)
    plan = read_json(f["plan_path"])
    audit = next(task for task in plan["tasks"] if task["name"] == "source-audit")
    assert audit["resources"] == {"cpus":8,"memory":"64G","hours":24,"gpu":False}
    assert plan["config_sha256"] == original["config_sha256"] and plan["release_identity"] == original["release_identity"]
    admission = result["runtime_readmission"]
    assert admission["status"] == "admitted"
    assert admission["prospective_registered_budget"]["cpu_core_hours"] == 74 + 8 * 24 + 8 * 8
    assert result["runtime_native_proofs"]["source-audit"]["native_record"]["state"] == "TIMEOUT"
    assert result["runtime_adjustments"] == f["descriptor"]["runtime_adjustments"]
    assert register(f) == result


@pytest.mark.parametrize("fault", ["other-task","cpu","memory","gpu","array","unbounded","nan","float","bool",
    "old-hours","missing-declaration","not-timeout","short-native","wrong-native-cpu","wrong-job",
    "native-gpu","time-mismatch","lease-mismatch","duplicate-time","extra-command"])
def test_runtime_extension_rejects_contract_changes_before_canonical_mutation(tmp_path, fault):
    f = runtime_fixture(tmp_path)
    audit = next(task for task in f["replacement"]["tasks"] if task["name"] == "source-audit")
    change = f["descriptor"]["runtime_adjustments"]["source-audit"]
    if fault == "other-task":f["descriptor"]["runtime_adjustments"] = {"source-replication":change}
    elif fault in {"cpu","memory","gpu","array"}:
        key, value = {"cpu":("cpus",9),"memory":("memory","128G"),"gpu":("gpu",True),"array":("array","0-1%1")}[fault]
        audit["resources"][key] = value
    elif fault in {"unbounded","nan","float","bool"}:
        change["new_hours"] = {"unbounded":169,"nan":float("nan"),"float":24.0,"bool":True}[fault]
        if fault == "nan":
            # atomic_json itself forbids NaN; test the explicit runtime guard.
            with pytest.raises(ValueError):repair._runtime_adjustments(f["descriptor"],
                {task["name"]:task for task in f["old"]["tasks"]},
                {task["name"]:task for task in f["replacement"]["tasks"]},f["targets"])
            return
    elif fault == "old-hours":change["old_hours"] = 7
    elif fault == "missing-declaration":f["descriptor"].pop("runtime_adjustments")
    elif fault == "not-timeout":f["native"]["10"]["state"] = "FAILED"
    elif fault == "short-native":f["native"]["10"]["elapsed_seconds"] = 28799
    elif fault == "wrong-native-cpu":f["native"]["10"]["cpus"] = 4
    elif fault == "wrong-job":change["failed_job_id"] = "20"
    elif fault == "native-gpu":f["native"]["10"]["alloc_tres"] += ",gres/gpu=1"
    else:
        path = Path(audit["script"]); text = path.read_text()
        if fault == "time-mismatch":text = text.replace("--time=24:00:00", "--time=48:00:00")
        elif fault == "lease-mismatch":text = text.replace("HHW_MAX_HOURS=24", "HHW_MAX_HOURS=8")
        elif fault == "duplicate-time":text += "#SBATCH --time=24:00:00\n"
        else:text += "python -m horsehair.cli changed-model\n"
        path.write_text(text);audit["script_sha256"] = canonical_hash(text)
        f["submitted"]["tasks"]["source-audit"]["script_sha256"] = audit["script_sha256"]
    refresh_runtime_fixture(f)
    ledger = f["root"] / "state/v7-managed-initial-submission.json"
    before = f["plan_path"].read_bytes(), ledger.read_bytes()
    with pytest.raises(ValueError):register(f)
    assert before == (f["plan_path"].read_bytes(), ledger.read_bytes()) and not f["calls"]


@pytest.mark.parametrize("fault", ["running","completed","failed","missing-hold"])
def test_runtime_adjusted_audit_cannot_compute_before_prospective_readmission(tmp_path, fault):
    f = runtime_fixture(tmp_path)
    audit = next(task for task in f["replacement"]["tasks"] if task["name"] == "source-audit")
    if fault == "missing-hold":
        path = Path(audit["script"])
        text = path.read_text().replace("#SBATCH --hold\n", "")
        path.write_text(text)
        audit["script_sha256"] = canonical_hash(text)
        f["submitted"]["tasks"]["source-audit"]["script_sha256"] = audit["script_sha256"]
    else:
        ident = f["submitted"]["tasks"]["source-audit"]["job_id"]
        f["native"][ident]["state"] = fault.upper()
    refresh_runtime_fixture(f)
    ledger = f["root"] / "state/v7-managed-initial-submission.json"
    before = f["plan_path"].read_bytes(), ledger.read_bytes()
    with pytest.raises(ValueError, match="explicitly held and pending"):register(f)
    assert before == (f["plan_path"].read_bytes(), ledger.read_bytes()) and not f["calls"]


def register_extra_workload(f, array="0-59%1"):
    plan = {**f["old"], "phase":"agents-development", "tasks":[{"name":"development",
        "resources":{"cpus":2,"memory":"16G","hours":4,"gpu":True,"array":array},"depends":[]}]}
    ledger = {"plan_sha256":canonical_hash(plan),"source_sha256":f["identity"]["sha256"],
        "config_sha256":canonical_hash(f["config"]),"tasks":{"development":{"state":"submitted","job_id":"400"}}}
    atomic_json(f["root"] / "plans/v7-agents-development/plan.json", plan)
    atomic_json(f["root"] / "state/v7-agents-development-submission.json", ledger)


def test_runtime_readmission_keeps_all_unfinished_array_members_and_external_leases(tmp_path):
    f = runtime_fixture(tmp_path)
    register_extra_workload(f)
    path = f["root"] / "state/resource-budget.json"
    budget = read_json(path)
    budget["leases"] = {"400_0":{"state":"running","cpus":2,"gpus":1,"max_hours":4},
        "900":{"state":"running","cpus":2,"gpus":0,"max_hours":4}}
    atomic_json(path, budget)
    receipt = register(f)["runtime_readmission"]
    assert set(receipt["registered_workload_hashes"]) == {"managed-initial","agents-development"}
    assert receipt["prospective_registered_budget"]["gpu_hours"] == 240
    assert receipt["prospective_registered_budget"]["cpu_core_hours"] == 74 + 192 + 64 + 480 + 8
    assert set(receipt["external_active_leases"]) == {"900"}


@pytest.mark.parametrize("fault", ["gpu-cap","external-cpu-cap","absent-budget","unbound-phase"])
def test_runtime_readmission_refuses_overbudget_or_unverified_scope_without_pair_mutation(tmp_path, fault):
    f = runtime_fixture(tmp_path)
    path = f["root"] / "state/resource-budget.json"
    if fault == "gpu-cap":register_extra_workload(f,"0-100%1")
    elif fault == "external-cpu-cap":
        budget = read_json(path)
        budget["leases"] = {"900":{"state":"running","cpus":64,"gpus":0,"max_hours":48}}
        atomic_json(path,budget)
    elif fault == "absent-budget":path.unlink()
    else:
        register_extra_workload(f)
        extra = f["root"] / "state/v7-agents-development-submission.json"
        value = read_json(extra);value["plan_sha256"] = "changed";atomic_json(extra,value)
    ledger = f["root"] / "state/v7-managed-initial-submission.json"
    before = f["plan_path"].read_bytes(), ledger.read_bytes()
    with pytest.raises(ValueError):register(f)
    assert before == (f["plan_path"].read_bytes(), ledger.read_bytes())


@pytest.mark.parametrize("shared_change", [False, True])
def test_preprocessing_projection_allows_only_named_storage_helper_without_numeric_changes(tmp_path, shared_change):
    f = fixture(tmp_path)
    path = f["preprocessing"] / "src/horsehair/behaviour.py"
    text = path.read_text() + "\ndef _ingestion_bin_trajectories(frame, widths):\n    return frame\n"
    if shared_change:text += "\ndef bin_trajectories(frame):\n    return frame\n"
    path.write_text(text)
    record = read_json(f["preprocessing"] / "release.json")
    record["source_identity"] = source_identity(f["preprocessing"])
    atomic_json(f["preprocessing"] / "release.json", record)
    if shared_change:
        with pytest.raises(ValueError, match="outside chronological ingestion"):register(f)
    else:
        assert register(f)["status"] == "complete"
