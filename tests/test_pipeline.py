"""Controller safety with generated fixtures and a mocked native scheduler.

No fixture is a biological/scientific result and no command reaches Slurm.
"""
import copy
import itertools
import random
import os
import subprocess
import sys
import signal
from pathlib import Path
from types import SimpleNamespace

import pytest

from horsehair import pipeline
from horsehair.core import atomic_json, canonical_hash, read_json, source_identity
from horsehair.slurm import submit


def state(value="COMPLETED", exit_code="0:0"):
    return {"state":value,"exit_code":exit_code}


def task(name, cpus=2, gpu=False, array=None, depends=(), external=()):
    resources = {"cpus":cpus,"gpu":gpu}
    if array is not None:
        resources["array"] = array
    return {"name":name,"resources":resources,"depends":list(depends),"external_dependencies":list(external)}


def brute_antichain(weights, edges):
    reachable = {key:set() for key in weights}
    for left,right in edges:
        reachable[left].add(right)
    changed = True
    while changed:
        before = {key:set(value) for key,value in reachable.items()}
        for key in weights:
            reachable[key] |= set().union(*(before[child] for child in before[key])) if before[key] else set()
        changed = before != reachable
    best = 0
    keys = list(weights)
    for size in range(len(keys)+1):
        for chosen in itertools.combinations(keys,size):
            if all(right not in reachable[left] and left not in reachable[right] for left,right in itertools.combinations(chosen,2)):
                best = max(best,sum(weights[key] for key in chosen))
    return best


def test_weighted_antichain_matches_exhaustive_random_dags_and_detects_cycles():
    for seed in range(80):
        rng = random.Random(seed)
        keys = [str(i) for i in range(7)]
        edges = [(a,b) for i,a in enumerate(keys) for b in keys[i+1:] if rng.random()<.3]
        rng.shuffle(keys)
        weights = {key:rng.randrange(0,13) for key in keys}
        assert pipeline._weighted_antichain(weights,edges)==brute_antichain(weights,edges)
    assert pipeline._weighted_antichain({},[])==0
    with pytest.raises(ValueError,match="Cyclic"):
        pipeline._weighted_antichain({"a":2,"b":3},[("a","b"),("b","a")])


def test_prospective_capacity_uses_dependencies_arrays_and_external_native_edges():
    plans = {"one":{"tasks":[task("a",6,True),task("b",4,array="0-3%2",depends=["a"]),
                             task("c",8,True,depends=["a"]),task("d",10,True,depends=["b","c"])]}}
    assert pipeline.prospective_capacity(plans,{}, {})=={"cpus":16,"gpus":1}
    plans = {"one":{"tasks":[task("a",8,True)]},"two":{"tasks":[task("b",6,True,external=["100"])]}}
    ledgers = {"one":{"tasks":{"a":{"state":"submitted","job_id":"100"}}}}
    assert pipeline.prospective_capacity(plans,ledgers,{"100":state("RUNNING")})=={"cpus":8,"gpus":1}
    plans["two"]["tasks"][0]["external_dependencies"] = []
    assert pipeline.prospective_capacity(plans,ledgers,{"100":state("RUNNING")})=={"cpus":14,"gpus":2}


@pytest.mark.parametrize("expression,expected",[("0-5",6),("0-5%2",2),("0-0%99",1),("0-8:2,11%3",3)])
def test_array_capacity_never_assumes_unthrottled_array_is_one_task(expression,expected):
    plans = {"fixture":{"tasks":[task("array",4,True,array=expression)]}}
    assert pipeline.prospective_capacity(plans,{}, {})=={"cpus":4*expected,"gpus":expected}


def test_array_completion_requires_every_planned_child_and_successful_exit():
    planned = task("array",4,array="0-3%2")
    submitted = {"state":"submitted","job_id":"500"}
    partial = {"500":state(),"500_0":state(),"500_1":state()}
    assert pipeline._task_status(submitted,partial,planned)=="awaiting_native_accounting"
    complete = {**partial,"500_2":state(),"500_3":state()}
    assert pipeline._task_status(submitted,complete,planned)=="complete"
    complete["500_3"] = state("RUNNING")
    assert pipeline._task_status(submitted,complete,planned)=="running"
    complete["500_3"] = state("COMPLETED","1:0")
    assert pipeline._task_status(submitted,complete,planned)=="failed"


def test_partial_submission_ledger_never_completes_an_unsubmitted_planned_task():
    plan = {"tasks":[task("first"),task("never-submitted",depends=["first"])]}
    ledger = {"tasks":{"first":{"state":"submitted","job_id":"600"}}}
    assert pipeline._phase_status(ledger,{"600":state()},plan)=="ambiguous_submission"


def test_failed_array_retains_capacity_until_every_member_is_native_terminal():
    planned = task("array",4,array="0-3%2")
    plans = {"fixture":{"tasks":[planned]}}
    ledgers = {"fixture":{"tasks":{"array":{"state":"submitted","job_id":"500"}}}}
    partial = {"500_0":state("FAILED","1:0"),"500_1":state("RUNNING")}
    assert pipeline.prospective_capacity(plans,ledgers,partial)["cpus"]==8
    terminal = {"500_0":state("FAILED","1:0"),"500_1":state(),"500_2":state("CANCELLED","0:15"),"500_3":state()}
    assert pipeline.prospective_capacity(plans,ledgers,terminal)=={"cpus":0,"gpus":0}


def test_native_state_reader_keeps_array_ids_ignores_steps_and_refuses_unavailable_accounting():
    calls = []
    def runner(command,**kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0,stdout="800_0|901|COMPLETED|0:0\n800_0.batch|901.batch|COMPLETED|0:0\n800_1|800|CANCELLED+ by 1|0:15\n")
    result = pipeline._native_states(["800","800"],runner=runner)
    assert set(result)=={"800_0","800_1"} and result["800_1"]["state"]=="CANCELLED"
    assert calls[0][calls[0].index("-j")+1]=="800"
    assert '--format=JobID%64,JobIDRaw%64,State,ExitCode' in calls[0]
    assert result['800_0']['job_id_raw']=='901' and result['800_1']['job_id_raw']=='800'
    planned = task('array',4,array='0-1%1')
    assert pipeline._task_status({'state':'submitted','job_id':'800'},result,planned)=='failed'
    with pytest.raises(RuntimeError,match="unavailable"):
        pipeline._native_states(["800"],runner=lambda *a,**k:SimpleNamespace(returncode=1,stdout=""))
    with pytest.raises(ValueError,match="Invalid native"):
        pipeline._native_states(["800; sbatch arbitrary"],runner=runner)


@pytest.fixture
def controller_fixture(tmp_path,monkeypatch):
    root,release = tmp_path/"study",tmp_path/"release"
    config = {"schema_version":1,"paths":{"root":str(root)},"resources":{"cpu_ceiling":8,"gpu_concurrency":1,"cpu_core_hours_cap":20000,"gpu_hours_cap":400},"agents":{}}
    atomic_json(release/"configs/study.json",config)
    source = release/"src/horsehair/pipeline.py"
    source.parent.mkdir(parents=True)
    source.write_text("# Generated controller QA fixture; never scientific evidence\n")
    identity = source_identity(release)
    atomic_json(release/"release.json",{"release_id":"qa-release","source_identity":identity,"config_sha256":canonical_hash(config)})
    initial_plan = {"phase":"managed-initial","config_sha256":canonical_hash(config),"release_identity":identity,"tasks":[task("ready",1)]}
    atomic_json(root/"plans/qa-release-managed-initial/plan.json",initial_plan)
    initial_ledger = {"plan_sha256":canonical_hash(initial_plan),"source_sha256":identity["sha256"],
                      "tasks":{"ready":{"state":"submitted","job_id":"100"}}}
    atomic_json(root/"state/qa-release-managed-initial-submission.json",initial_ledger)
    monkeypatch.setattr(pipeline,"NODES",(("phase-a",("initial:ready",),None),("phase-b",("phase-a",),None)))
    planned,submitted = [],[]
    def planner(cfg,rel,directory,phase):
        planned.append(phase)
        directory.mkdir(parents=True)
        script = directory/"work.sbatch"
        script.write_text("#!/bin/bash\n# Generated scheduler QA fixture\ntrue\n")
        entry = task("work",2)
        entry.update(script=str(script),script_sha256=canonical_hash(script.read_text()))
        record = {"phase":phase,"config_sha256":canonical_hash(cfg),"release_identity":source_identity(rel),"tasks":[entry]}
        atomic_json(directory/"plan.json",record)
        return record
    def submitter(plan,ledger):
        def scheduler(command,**kwargs):
            submitted.append(command)
            return SimpleNamespace(returncode=0,stdout=str(199+len(submitted))+"\n",stderr="")
        return submit(plan,ledger,runner=scheduler)
    def tick(native,**kwargs):
        return pipeline.tick(config,root,release,native=native,planner=planner,submitter=submitter,
                             readiness=lambda *a:(True,[]),**kwargs)
    return SimpleNamespace(root=root,release=release,config=config,identity=identity,initial_plan=initial_plan,
                           initial_ledger=initial_ledger,planner=planner,submitter=submitter,tick=tick,
                           planned=planned,submitted=submitted)


def test_controller_submits_each_phase_once_and_waits_for_native_dependencies(controller_fixture):
    fixture = controller_fixture
    first = fixture.tick({"100":state()})
    assert first["phases"]["phase-a"]["status"]=="queued"
    assert first["phases"]["phase-b"]["status"]=="waiting_for_dependencies"
    second = fixture.tick({"100":state(),"200":state("PENDING")})
    assert len(fixture.submitted)==1 and len(fixture.planned)==1
    assert second["phases"]["phase-a"]["status"]=="queued"
    third = fixture.tick({"100":state(),"200":state()})
    assert len(fixture.submitted)==2 and third["phases"]["phase-b"]["status"]=="queued"
    final = fixture.tick({"100":state(),"200":state(),"201":state()})
    assert final["status"]=="waiting_for_primary_scientific_evidence" and final['phase_graph_complete']
    assert not final['primary_analyses_complete'] and not final["scientific_results_complete"]
    assert not final["publication_ready"] and len(fixture.submitted)==2


def test_native_graph_completion_cannot_complete_pending_auxiliary_science(controller_fixture,monkeypatch):
    from horsehair import report
    fixture=controller_fixture
    names=('behaviour','interventions','molecular-transfer','agents-final')
    monkeypatch.setattr(pipeline,'NODES',tuple((name,('initial:ready',),None) for name in names))
    # Claim-validation and auxiliary statistical contracts have their own actual
    # file/hash tests. This fixture isolates the scheduler completion boundary.
    monkeypatch.setattr(pipeline,'_phase_outputs',lambda *a,**k:(True,None))
    auxiliary={'ready':False,'status':'pending','required_assays':['mantis_choice'],'assays':{},
               'pending':['mantis_choice: original source ambiguity remains'],'source_result_hashes':{}}
    monkeypatch.setattr(report,'auxiliary_readiness',lambda *a,**k:auxiliary)
    fixture.tick({'100':state()})
    assert len(fixture.submitted)==4
    native={'100':state(),**{str(200+i):state() for i in range(4)}}
    final=fixture.tick(native)
    assert final['phase_graph_complete'] and final['primary_analyses_complete']
    assert not final['auxiliary_analyses_complete'] and not final['scientific_results_complete']
    assert final['status']=='waiting_for_auxiliary_scientific_inputs' and not final['failed_native_stages']
    assert not final['publication_ready']


def test_completed_numeric_H4_receipt_with_pending_claim_cannot_complete_scientific_phase(controller_fixture,monkeypatch):
    from horsehair import report
    fixture=controller_fixture
    path=fixture.root/'results/inference/H4.json'
    atomic_json(path,{'claim_id':'H4','status':'complete','claim_status':'pending',
                     'config_sha256':canonical_hash(fixture.config),'source_sha256':fixture.identity['sha256']})
    monkeypatch.setattr(report,'validate_result',lambda *a:None)
    valid,reason=pipeline._phase_outputs('agents-final',fixture.config,fixture.root,fixture.identity['sha256'])
    assert not valid and 'qualification remains pending' in reason


def add_capacity_guard(fixture):
    fixture.initial_plan["tasks"].append(task("guard",8))
    fixture.initial_ledger["tasks"]["guard"] = {"state":"submitted","job_id":"101"}
    fixture.initial_ledger["plan_sha256"] = canonical_hash(fixture.initial_plan)
    atomic_json(fixture.root/"plans/qa-release-managed-initial/plan.json",fixture.initial_plan)
    atomic_json(fixture.root/"state/qa-release-managed-initial-submission.json",fixture.initial_ledger)


def test_frozen_capacity_wait_plan_is_reused_once_after_capacity_releases(controller_fixture):
    fixture = controller_fixture
    add_capacity_guard(fixture)
    first = fixture.tick({"100":state(),"101":state("RUNNING")})
    assert first["phases"]["phase-a"]["status"]=="waiting_for_study_capacity"
    assert len(fixture.planned)==1 and not fixture.submitted
    journal = read_json(fixture.root/"state/pipeline/qa-release/phase-a-plan-journal.json")
    assert journal["state"]=="plan_frozen"
    fixture.tick({"100":state(),"101":state()})
    fixture.tick({"100":state(),"101":state(),"200":state("PENDING")})
    assert len(fixture.planned)==1 and len(fixture.submitted)==1


def test_ambiguous_submit_outcome_is_never_automatically_replayed(controller_fixture):
    fixture = controller_fixture
    def disconnected(plan,ledger):
        raise RuntimeError("Generated lost acknowledgement fixture")
    with pytest.raises(RuntimeError,match="lost acknowledgement"):
        pipeline.tick(fixture.config,fixture.root,fixture.release,native={"100":state()},planner=fixture.planner,
                      submitter=disconnected,readiness=lambda *a:(True,[]))
    journal = read_json(fixture.root/"state/pipeline/qa-release/phase-a-plan-journal.json")
    assert journal["state"]=="submitting"
    next_tick = fixture.tick({"100":state()})
    assert next_tick["phases"]["phase-a"]["status"]=="requires_submission_review"
    assert len(fixture.planned)==1 and not fixture.submitted


@pytest.mark.parametrize("field",["config","source","journal-source","ledger-hash"])
def test_foreign_frozen_plan_or_ledger_cannot_launch(controller_fixture,field):
    fixture = controller_fixture
    add_capacity_guard(fixture)
    fixture.tick({"100":state(),"101":state("RUNNING")})
    path = fixture.root/"plans/qa-release-phase-a/plan.json"
    journal_path = fixture.root/"state/pipeline/qa-release/phase-a-plan-journal.json"
    candidate,journal = read_json(path),read_json(journal_path)
    if field=="config":
        candidate["config_sha256"] = "0"*64
    elif field=="source":
        candidate["release_identity"] = {"files":{},"sha256":"0"*64}
    elif field=="journal-source":
        journal["source_sha256"] = "0"*64
    else:
        atomic_json(fixture.root/"state/qa-release-phase-a-submission.json",{"plan_sha256":"0"*64,"tasks":{"work":{"state":"submitted","job_id":"300"}}})
    atomic_json(path,candidate)
    journal["plan_sha256"] = canonical_hash(candidate)
    atomic_json(journal_path,journal)
    if field=="journal-source":
        outcome = fixture.tick({"100":state(),"101":state()})
        assert outcome["phases"]["phase-a"]["status"]=="requires_submission_review"
    else:
        with pytest.raises(ValueError,match="different|differs"):
            fixture.tick({"100":state(),"101":state()})
    assert not fixture.submitted


@pytest.mark.parametrize("change",["config","source"])
def test_release_change_refuses_all_controller_actions(controller_fixture,change):
    fixture = controller_fixture
    if change=="config":
        fixture.config["resources"]["cpu_ceiling"] = 7
    else:
        (fixture.release/"src/horsehair/pipeline.py").write_text("# Modified generated release fixture\n")
    with pytest.raises(ValueError,match="changed"):
        fixture.tick({"100":state()})
    assert not fixture.submitted and not fixture.planned


def configure_lock_run(fixture,monkeypatch):
    from horsehair import budget
    monkeypatch.setattr(pipeline,"__file__",str(fixture.release/"src/horsehair/pipeline.py"))
    monkeypatch.setenv("SLURM_JOB_ID","800")
    monkeypatch.delenv("SLURM_ARRAY_JOB_ID",raising=False)
    monkeypatch.delenv("SLURM_ARRAY_TASK_ID",raising=False)
    monkeypatch.setattr(budget,"native_accounting",lambda *a:None)
    monkeypatch.setattr(budget,"storage_snapshot",lambda *a:None)
    monkeypatch.setattr(pipeline,"tick",lambda *a,**k:{"status":"complete","phases":{},"initial_tasks":{},"failed_native_stages":[]})
    path = fixture.root/"state/pipeline/qa-release/controller.lock"
    atomic_json(path,{"job_id":"700","segment":0,"started_unix":1.})
    return path


def test_controller_defers_live_receipt_guard_and_recovers_without_replaying(controller_fixture,monkeypatch):
    from horsehair import budget
    from horsehair.core import ReceiptLockTimeout
    fixture=controller_fixture;path=configure_lock_run(fixture,monkeypatch)
    monkeypatch.setattr(pipeline,'_native_states',lambda *a:{'700':state()})
    now=[0.];monkeypatch.setattr(pipeline.time,'monotonic',lambda:now[0])
    monkeypatch.setattr(pipeline.time,'sleep',lambda duration:now.__setitem__(0,now[0]+duration))
    calls=[]
    def accounting(*args):
        calls.append(args)
        if len(calls)==1:
            raise ReceiptLockTimeout(fixture.root/'state/resource-budget.mutex',30.,{'pid':123,'state':'held'})
    monkeypatch.setattr(budget,'native_accounting',accounting)
    result=pipeline.run(fixture.config,fixture.root,max_seconds=90,interval=30)
    assert result['status']=='complete' and len(calls)==2 and not path.exists()
    warning=read_json(path.parent/'receipt-guard-warning.json')
    assert warning['warning_count']==1 and warning['last_known_holder']['pid']==123
    assert warning['owner_metadata_is_authoritative'] is False
    assert not fixture.submitted


def test_repeated_receipt_guard_contention_cannot_exceed_finite_controller_window(controller_fixture,monkeypatch):
    from horsehair import budget
    from horsehair.core import ReceiptLockTimeout
    fixture=controller_fixture;path=configure_lock_run(fixture,monkeypatch)
    monkeypatch.setattr(pipeline,'_native_states',lambda *a:{'700':state()})
    now=[0.];monkeypatch.setattr(pipeline.time,'monotonic',lambda:now[0])
    monkeypatch.setattr(pipeline.time,'sleep',lambda duration:now.__setitem__(0,now[0]+duration))
    calls=[]
    def accounting(*args):
        calls.append(args)
        raise ReceiptLockTimeout(fixture.root/'state/resource-budget.mutex',30.,{'pid':123,'state':'held'})
    monkeypatch.setattr(budget,'native_accounting',accounting)
    result=pipeline.run(fixture.config,fixture.root,max_seconds=60,interval=30)
    assert result['status']=='waiting_for_receipt_guard' and result['segment_status']=='finite_segment_finished'
    assert result['receipt_guard_warning_count']==2 and len(calls)==2 and now[0]==60
    assert not result['scientific_results_complete'] and not path.exists() and not fixture.submitted


@pytest.mark.parametrize("native_proof",[{}, {"700":state("RUNNING")}, {"700":state("PENDING")}])
def test_controller_never_removes_lock_without_terminal_native_proof(controller_fixture,monkeypatch,native_proof):
    fixture = controller_fixture
    path = configure_lock_run(fixture,monkeypatch)
    monkeypatch.setattr(pipeline,"_native_states",lambda *a:native_proof)
    with pytest.raises(RuntimeError,match="terminal proof"):
        pipeline.run(fixture.config,fixture.root,max_seconds=30)
    assert read_json(path)["job_id"]=="700"
    assert not list(path.parent.glob("controller-lock-700-*.json"))


@pytest.mark.parametrize("terminal",["COMPLETED","FAILED","TIMEOUT"])
def test_controller_archives_terminal_native_lock_proof_before_recovery(controller_fixture,monkeypatch,terminal):
    fixture = controller_fixture
    path = configure_lock_run(fixture,monkeypatch)
    monkeypatch.setattr(pipeline,"_native_states",lambda *a:{"700":state(terminal)})
    result = pipeline.run(fixture.config,fixture.root,max_seconds=30)
    assert result["status"]=="complete" and not path.exists()
    archived = list(path.parent.glob("controller-lock-700-*.json"))
    assert len(archived)==1
    assert read_json(archived[0])["native_terminal_proof"]["state"]==terminal


def test_controller_lock_replacement_during_native_query_is_not_deleted(controller_fixture,monkeypatch):
    fixture = controller_fixture
    path = configure_lock_run(fixture,monkeypatch)
    def scheduler_query(*args):
        atomic_json(path,{"job_id":"701","segment":1,"started_unix":2.})
        return {"700":state()}
    monkeypatch.setattr(pipeline,"_native_states",scheduler_query)
    with pytest.raises(RuntimeError):
        pipeline.run(fixture.config,fixture.root,max_seconds=30)
    assert path.exists() and read_json(path)["job_id"]=="701"


def test_controller_recovery_guard_old_file_contents_do_not_block_new_terminal_recovery(controller_fixture,monkeypatch):
    fixture = controller_fixture
    path = configure_lock_run(fixture,monkeypatch)
    guard = path.parent/"controller-recovery.lock"
    guard.write_text('{"job_id":"699","generated_fixture":"previous crash"}')
    monkeypatch.setattr(pipeline,"_native_states",lambda *a:{"700":state()})
    result = pipeline.run(fixture.config,fixture.root,max_seconds=30)
    assert result["status"]=="complete" and guard.exists() and not path.exists()


def test_os_recovery_lock_rejects_competing_process_and_releases_after_crash(tmp_path):
    """Actual process death, not a mocked finally, must release the OS mutex."""
    guard = tmp_path/"generated-os-lock-fixture.lock"
    source_root = Path(pipeline.__file__).resolve().parents[1]
    environment = dict(os.environ,PYTHONPATH=str(source_root)+os.pathsep+os.environ.get("PYTHONPATH",""))
    code = "from pathlib import Path; import sys,time,os; from horsehair.pipeline import _recovery_guard\nwith _recovery_guard(Path(sys.argv[1])):\n print('held:'+str(os.getpid()),flush=True)\n time.sleep(120)\n"
    process = subprocess.Popen([sys.executable,"-c",code,str(guard)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                               text=True,env=environment,creationflags=subprocess.CREATE_NO_WINDOW if os.name=="nt" else 0)
    child_pid = None
    try:
        acknowledgement = process.stdout.readline().strip()
        assert acknowledgement.startswith("held:")
        child_pid = int(acknowledgement.split(":")[1])
        assert child_pid>0 and child_pid!=os.getpid()
        with pytest.raises((OSError,RuntimeError)):
            with pipeline._recovery_guard(guard):
                pytest.fail("Concurrent controller recovery acquired another process's OS lock")
        # Windows venv launchers can spawn the actual interpreter; terminate
        # the PID reported by our helper, rather than leave an orphaned holder.
        os.kill(child_pid,signal.SIGTERM)
        process.wait(timeout=10)
        with pipeline._recovery_guard(guard):
            assert guard.exists()
    finally:
        if process.poll() is None:
            if child_pid is not None:
                try:
                    os.kill(child_pid,signal.SIGTERM)
                except OSError:
                    pass
            process.kill()
        process.communicate(timeout=10)


def test_controller_cleanup_preserves_replaced_main_lock_owner(controller_fixture,monkeypatch):
    fixture = controller_fixture
    path = configure_lock_run(fixture,monkeypatch)
    monkeypatch.setattr(pipeline,"_native_states",lambda *a:{"700":state()})
    def replaced(*args,**kwargs):
        atomic_json(path,{"job_id":"702","generated_fixture":"changed during tick"})
        return {"status":"complete","phases":{},"initial_tasks":{},"failed_native_stages":[]}
    monkeypatch.setattr(pipeline,"tick",replaced)
    pipeline.run(fixture.config,fixture.root,max_seconds=30)
    assert read_json(path)["job_id"]=="702"


@pytest.mark.parametrize("status",["pending","rejected"])
def test_resource_admission_refusal_cannot_create_plan_or_submit(controller_fixture,monkeypatch,status):
    fixture = controller_fixture
    monkeypatch.setattr(pipeline,"NODES",(("phase-a",("initial:ready",),"agents-development"),))
    outcome = fixture.tick({"100":state()},assessor=lambda *a,**k:{"agents-development":{"status":status,"reasons":["Generated resource fixture"]}})
    assert outcome["phases"]["phase-a"]["status"]=="resource_"+status
    assert not fixture.planned and not fixture.submitted


def test_missing_scientific_inputs_cannot_create_plan_or_submit(controller_fixture):
    fixture = controller_fixture
    outcome = pipeline.tick(fixture.config,fixture.root,fixture.release,native={"100":state()},planner=fixture.planner,
                            submitter=fixture.submitter,readiness=lambda *a:(False,["Generated missing metadata fixture"]))
    assert outcome["phases"]["phase-a"]["status"]=="waiting_for_scientific_inputs"
    assert not fixture.planned and not fixture.submitted


def test_registered_budget_reserves_every_unfinished_array_member_including_missing_records():
    entry = task("training", 6, True, array="0-5%1")
    entry["resources"]["hours"] = 12
    plans = {"qa":{"tasks":[entry]}}
    ledgers = {"qa":{"tasks":{"training":{"state":"submitted","job_id":"500"}}}}
    native = {"500_0":state(), "500_1":state("FAILED","1:0"),
              "500_2":state("RUNNING"), "500_3":state("PENDING")}
    accounting = {"spent_cpu_core_hours":22, "spent_gpu_hours":3}
    result = pipeline.prospective_registered_budget(plans, ledgers, native, accounting)
    # Four unfinished children remain reserved despite the one-GPU throttle.
    # The failed child's consumed allocation remains in native spent hours.
    assert result["cpu_core_hours"] == 22+4*6*12
    assert result["gpu_hours"] == 3+4*12
    native.update({"500_2":state(), "500_3":state("CANCELLED","0:15"),
                   "500_4":state(), "500_5":state()})
    final = pipeline.prospective_registered_budget(plans, ledgers, native, accounting)
    assert final["cpu_core_hours"] == 22 and final["gpu_hours"] == 3


def test_registered_budget_does_not_add_active_lease_bound_twice():
    gpu = task("running", 6, True); gpu["resources"]["hours"] = 12
    cpu = task("pending", 4, array="0-1%2"); cpu["resources"]["hours"] = 2
    plans = {"gpu":{"tasks":[gpu]}, "cpu":{"tasks":[cpu]}}
    ledgers = {"gpu":{"tasks":{"running":{"state":"submitted","job_id":"600"}}}}
    accounting = {"spent_cpu_core_hours":5, "spent_gpu_hours":.5,
                  "leases":{"600":{"state":"running","cpus":6,"gpus":1,"max_hours":12}}}
    result = pipeline.prospective_registered_budget(plans, ledgers, {"600":state("RUNNING")}, accounting)
    assert result["cpu_core_hours"] == 5+6*12+2*4*2
    assert result["gpu_hours"] == .5+12


def test_unsubmitted_sequential_phases_reserve_full_allocation_not_only_concurrent_weight():
    gpu = task("training", 6, True, array="0-1%1"); gpu["resources"]["hours"] = 2
    finish = task("freeze", 2, depends=["training"]); finish["resources"]["hours"] = 1
    plans = {"qa":{"tasks":[gpu,finish]}}
    result = pipeline.prospective_registered_budget(plans, {}, {},
        {"spent_cpu_core_hours":10,"spent_gpu_hours":2})
    assert pipeline.prospective_capacity(plans, {}, {}) == {"cpus":6,"gpus":1}
    assert result["cpu_core_hours"] == 10+2*6*2+2
    assert result["gpu_hours"] == 2+2*2


def test_independent_phase_budget_wait_retries_frozen_plan_after_actual_cost_releases_bound(controller_fixture,monkeypatch):
    fixture = controller_fixture
    fixture.config["resources"]["cpu_core_hours_cap"] = 6
    atomic_json(fixture.release/"configs/study.json",fixture.config)
    fixture.identity = source_identity(fixture.release)
    atomic_json(fixture.release/"release.json",{"release_id":"qa-release","source_identity":fixture.identity,
        "config_sha256":canonical_hash(fixture.config)})
    fixture.initial_plan.update(config_sha256=canonical_hash(fixture.config),release_identity=fixture.identity)
    fixture.initial_ledger.update(plan_sha256=canonical_hash(fixture.initial_plan),source_sha256=fixture.identity["sha256"])
    atomic_json(fixture.root/"plans/qa-release-managed-initial/plan.json",fixture.initial_plan)
    atomic_json(fixture.root/"state/qa-release-managed-initial-submission.json",fixture.initial_ledger)
    monkeypatch.setattr(pipeline,"NODES",(("phase-a",("initial:ready",),None),("phase-b",("initial:ready",),None)))
    accounting = fixture.root/"state/resource-budget.json"
    atomic_json(accounting,{"spent_cpu_core_hours":1,"spent_gpu_hours":0})
    first = fixture.tick({"100":state()})
    assert first["phases"]["phase-a"]["status"] == "queued"
    assert first["phases"]["phase-b"]["status"] == "waiting_for_registered_resource_budget"
    assert first["phases"]["phase-b"]["prospective_registered_budget"]["cpu_core_hours"] == 9
    assert len(fixture.planned) == 2 and len(fixture.submitted) == 1
    journal_path = fixture.root/"state/pipeline/qa-release/phase-b-plan-journal.json"
    frozen = read_json(journal_path)
    assert frozen["state"] == "plan_frozen"
    # Phase A actually cost one core-hour rather than its four-hour bound.
    # The remaining phase now exactly meets the six-hour cap.
    atomic_json(accounting,{"spent_cpu_core_hours":2,"spent_gpu_hours":0})
    second = fixture.tick({"100":state(),"200":state()})
    assert second["phases"]["phase-b"]["status"] == "queued"
    assert len(fixture.planned) == 2 and len(fixture.submitted) == 2
    assert read_json(journal_path)["plan_sha256"] == frozen["plan_sha256"]
    fixture.tick({"100":state(),"200":state(),"201":state("PENDING")})
    assert len(fixture.submitted) == 2

