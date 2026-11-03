from pathlib import Path
from datetime import datetime,timezone,timedelta
import ast,base64,re
import pytest

shell=Path(__file__).with_name('quota-release-registration-20261103.sh').read_text()
body=shell.split("python3 -B -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];tree=ast.parse(body)
def assigned(nodes,name):return next(n for n in nodes if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id==name for x in n.targets))
encoded=assigned(tree.body,'code').value.args[0].value
runtime=base64.b64decode(encoded).decode();rt=ast.parse(runtime)
functions=[n for n in rt.body if isinstance(n,ast.FunctionDef) and n.name in {'epoch','text_time_proof','json_time_proof'}]
ns={'datetime':datetime,'timezone':timezone,'re':re};exec(compile(ast.Module(body=functions,type_ignores=[]),'<quota-time-proofs>','exec'),ns)

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

@pytest.mark.parametrize('native,accepted',[('8421133|CANCELLED by 3164768|0:0|0|0|pwa209|def-ptewarie_cpu\n',True),('8421133|RUNNING|0:0|0|1|pwa209|def-ptewarie_cpu\n',False),('8421133|CANCELLED by 3164768|0:0|60|1|pwa209|def-ptewarie_cpu\n',False),('8421133|CANCELLED by 3164768|0:0|0|0|other|def-ptewarie_cpu\n',False)])
def test_old_quota_cancel_before_start_is_positive_prerequisite(native,accepted):
    start=tree.body.index(assigned(tree.body,'old'));end=tree.body.index(assigned(tree.body,'manifest'))
    guard=compile(ast.Module(body=tree.body[start:end],type_ignores=[]),'<original-quota-finality-guard>','exec')
    env={'query':lambda command:native}
    if accepted:exec(guard,env)
    else:
        with pytest.raises(AssertionError):exec(guard,env)
