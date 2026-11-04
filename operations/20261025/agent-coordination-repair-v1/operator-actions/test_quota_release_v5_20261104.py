from pathlib import Path
from datetime import datetime,timezone,timedelta
import ast,base64,hashlib,json,re,subprocess
from types import SimpleNamespace
import pytest

shell=Path(__file__).with_name('hhw-quota-release-20261104-947.sh').read_text()
body=shell.split("python3 -B -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];tree=ast.parse(body)
def assigned(nodes,name):return next(n for n in nodes if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id==name for x in n.targets))
encoded=assigned(tree.body,'code').value.args[0].value
runtime=base64.b64decode(encoded).decode();rt=ast.parse(runtime)
functions=[n for n in rt.body if isinstance(n,ast.FunctionDef) and n.name in {'epoch','text_time_proof','json_time_proof'}]
ns={'datetime':datetime,'timezone':timezone,'re':re};exec(compile(ast.Module(body=functions,type_ignores=[]),'<quota-time-proofs>','exec'),ns)

def test_generated_handoff_pins_the_exact_continuation_helper_and_tests():
    expected=ast.literal_eval(assigned(tree.body,'expected').value)
    assert expected=={name:hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() for name in [
        'compact-terminal-controller-reads-20261104.py','test_compact_terminal_controller_reads_20261104.py']}

@pytest.mark.parametrize('mutation,accepted',[(None,True),('flat_manifest',False),('wrong_native_roster',False)])
def test_stage_requires_structured_manifest_with_exact_native_test_roster(mutation,accepted):
    check=next(n for n in tree.body if isinstance(n,ast.Assert) and isinstance(n.msg,ast.Constant) and n.msg.value=='Cleanup source/test manifest differs from the reviewed v4 continuation closure')
    expected=ast.literal_eval(check.test.comparators[0]);value=json.loads(json.dumps(expected))
    assert expected['native_tests']==79 and len(expected['native_roster_sha256'])==64
    if mutation=='flat_manifest':value=value['files']
    elif mutation=='wrong_native_roster':value['native_roster_sha256']='0'*64
    boundary=compile(ast.Module(body=[check],type_ignores=[]),'<structured-cleanup-manifest>','exec')
    if accepted:exec(boundary,{'manifest':value})
    else:
        with pytest.raises(AssertionError):exec(boundary,{'manifest':value})

@pytest.mark.parametrize('remaining,expected_seconds',[(900,720),(400,220),(200,20)])
def test_accounting_wait_is_finite_and_reserves_three_minutes_for_operations(remaining,expected_seconds):
    timer=SimpleNamespace(monotonic=lambda:50.0,time=lambda:1000.0)
    env={'time':timer,'end':1000.0+remaining}
    exec(compile(ast.Module(body=[assigned(rt.body,'stop')],type_ignores=[]),'<bounded-final-accounting-wait>','exec'),env)
    assert env['stop']==50.0+expected_seconds

def accounting_loop_environment(events,stop=30):
    events=list(events);calls=[];sleeps=[];clock=[0.0]
    def read(*args,**kwargs):
        calls.append((args,kwargs));event=events.pop(0)
        if isinstance(event,BaseException):raise event
        return event
    def sleep(seconds):sleeps.append(seconds);clock[0]+=seconds
    env={'repair':SimpleNamespace(_repair_native_states=read),'cleanup':{'job_id':'8463111'},
        'subprocess':subprocess,'json':json,'stop':stop,
        'time':SimpleNamespace(monotonic=lambda:clock[0],sleep=sleep)}
    start=rt.body.index(assigned(rt.body,'accounting_attempt'))
    loop=compile(ast.Module(body=rt.body[start:start+2],type_ignores=[]),'<actual-cleanup-accounting-loop>','exec')
    return loop,env,calls,sleeps

def completed_cleanup():
    return {'8463111':{'state':'COMPLETED','exit_code':'0:0','job_id_raw':'8463111','cpus':1,'elapsed_seconds':80,'alloc_tres':'cpu=1,mem=16G'}}

def test_accounting_timeout_then_actual_completion_reuses_same_bounded_loop(capsys):
    loop,env,calls,sleeps=accounting_loop_environment([
        subprocess.TimeoutExpired(['sacct'],30,output='private diagnostic'),completed_cleanup()])
    exec(loop,env)
    assert len(calls)==2 and sleeps==[5] and env['row']['state']=='COMPLETED'
    logged=json.loads(capsys.readouterr().out.strip())
    assert logged=={'status':'cleanup_final_accounting_read_pending','attempt':1,'error_type':'TimeoutExpired'}

def test_accounting_nonzero_query_then_actual_completion_reuses_same_bounded_loop(capsys):
    loop,env,calls,sleeps=accounting_loop_environment([
        RuntimeError('Repair native proof query failed'),completed_cleanup()])
    exec(loop,env)
    assert len(calls)==2 and sleeps==[5] and env['row']['exit_code']=='0:0'
    assert json.loads(capsys.readouterr().out.strip())['error_type']=='RuntimeError'

def test_malformed_native_identity_and_unrelated_runtime_error_remain_fatal(capsys):
    for error in [ValueError('Foreign or conflicting malformed identity'),RuntimeError('Different proof failure')]:
        loop,env,calls,sleeps=accounting_loop_environment([error,completed_cleanup()])
        with pytest.raises(type(error),match=re.escape(str(error))):exec(loop,env)
        assert len(calls)==1 and sleeps==[]
    assert capsys.readouterr().out==''

def test_transient_accounting_failure_exhausts_bound_without_additional_read(capsys):
    loop,env,calls,sleeps=accounting_loop_environment([
        subprocess.TimeoutExpired(['sacct'],30),RuntimeError('Repair native proof query failed'),completed_cleanup()],stop=10)
    with pytest.raises(AssertionError,match='Final cleanup accounting is still pending'):exec(loop,env)
    assert len(calls)==2 and sleeps==[5]
    assert [json.loads(line)['attempt'] for line in capsys.readouterr().out.splitlines()]==[1,2]

def test_readiness_event_anchors_cadence_and_slot61_has_no_time_mutation():
    ready=datetime(2026,11,4,0,45,12,tzinfo=timezone.utc)
    env={'readiness_anchor':ready,'targets':[('6983056',62,4),('6983059',63,8),('6983060',64,12)],'timedelta':timedelta}
    exec(compile(ast.Module(body=[assigned(rt.body,'anchor'),assigned(rt.body,'desired')],type_ignores=[]),'<quota-cadence>','exec'),env)
    assert env['desired']=={job:{'utc':(ready+timedelta(hours=hours)).strftime('%Y-%m-%dT%H:%M:%S'),'epoch':(ready+timedelta(hours=hours)).timestamp(),'slot':slot} for job,slot,hours in env['targets']}
    assert '6983037' not in env['desired'] and env['anchor']==ready

@pytest.mark.parametrize('field,value,accepted',[('EligibleTime','2026-11-04T04:45:12',True),('StartTime','Unknown',False),('BeginTime','2026-11-04T05:45:12+01:00',True),('EligibleTime','2026-11-04T05:45:12',False)])
def test_text_proof_requires_exact_native_requested_epoch(field,value,accepted):
    wanted=datetime(2026,11,4,4,45,12,tzinfo=timezone.utc).timestamp()
    assert bool(ns['text_time_proof']({field:value},wanted))==accepted

@pytest.mark.parametrize('is_set,offset,accepted',[(True,0,True),(False,0,False),(True,100,False)])
def test_recursive_job_json_requires_real_set_epoch(is_set,offset,accepted):
    wanted=datetime(2026,11,4,4,45,12,tzinfo=timezone.utc).timestamp();value={'jobs':[{'job_id':6983056,'begin_time':{'set':is_set,'number':wanted+offset}}]}
    assert bool(ns['json_time_proof'](value,'6983056',wanted))==accepted

@pytest.mark.parametrize('native,accepted',[('8425418|CANCELLED by 3164768|0:0|0|0|pwa209|def-ptewarie_cpu\n',True),('8425418|RUNNING|0:0|0|1|pwa209|def-ptewarie_cpu\n',False),('8425418|CANCELLED by 3164768|0:0|60|1|pwa209|def-ptewarie_cpu\n',False),('8425418|CANCELLED by 3164768|0:0|0|0|other|def-ptewarie_cpu\n',False)])
def test_old_quota_cancel_before_start_is_positive_prerequisite(native,accepted):
    start=tree.body.index(assigned(tree.body,'old'));end=tree.body.index(assigned(tree.body,'manifest'))
    guard=compile(ast.Module(body=tree.body[start:end],type_ignores=[]),'<original-quota-finality-guard>','exec')
    env={'query':lambda command:native}
    if accepted:exec(guard,env)
    else:
        with pytest.raises(AssertionError):exec(guard,env)

@pytest.mark.parametrize('status,minimum_met,accepted',[
    ('terminal_controller_diagnostic_recovery_complete',True,True),
    ('terminal_controller_diagnostic_recovery_complete',False,False),
    ('terminal_controller_diagnostic_recovery_incomplete',False,False),
])
def test_incomplete_cleanup_cannot_reach_fresh_quota_or_census_updates(status,minimum_met,accepted):
    start=rt.body.index(assigned(rt.body,'result'))
    stop=rt.body.index(assigned(rt.body,'quota'))
    boundary=compile(ast.Module(body=rt.body[start:stop],type_ignores=[]),'<cleanup-result-prerequisite>','exec')
    result={'status':status,'minimum_met':minimum_met,'scientific_result':False,'admission_authority':False}
    env={'json':json,'resultpath':SimpleNamespace(read_bytes=lambda:json.dumps(result).encode())}
    if accepted:exec(boundary,env)
    else:
        with pytest.raises(AssertionError):exec(boundary,env)
