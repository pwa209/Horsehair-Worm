"""Additional deadline regressions; frozen five-file native closure stays immutable."""
from pathlib import Path
import copy
import pytest
from test_acknowledgements import m


@pytest.mark.parametrize('slow_phase',['audit','admission'])
def test_long_audit_or_admission_reaching_cutoff_never_dispatches_or_commits(monkeypatch,slow_phase):
    from contextlib import nullcontext
    from types import SimpleNamespace
    clock=[0.];events=[];state={'revision':1,'jobs':{},'cells':[]};plan={}
    c={'deadline':1000.,'descriptor':{'precision_gate':{'diagnostics_job_id':'1','assembly_job_id':'2'}},'area':Path('audit')}
    def audit(*args):
        if slow_phase=='audit':clock[0]=701.
        events.append('audited');return {}
    def admission(context,label):
        events.append(label)
        if label=='commit-1' and slow_phase=='admission':clock[0]=701.
    base=SimpleNamespace(prepare_census=lambda c:None,registration_guard=lambda *args:nullcontext(),
        initialize=lambda c:(plan,state),source_guard=lambda c:None,admission=admission,
        collect_packets=audit,atomic_json=lambda *args:None,
        dispatch=SimpleNamespace(dispatch_tick=lambda *args:pytest.fail('Late audit must not generate work')),
        save_state=lambda *args:pytest.fail('Late audit must not commit state'))
    c['native_reader']=lambda *args,**kwargs:{}
    monkeypatch.setattr(m,'pending_intents',lambda *args:[])
    monkeypatch.setattr(m.time,'time',lambda:clock[0])
    with pytest.raises(m.SubmissionWindowExhausted):m.coordinated_tick(base,c)
    expected=['snapshot-1','audited']+(['commit-1'] if slow_phase=='admission' else [])
    assert events==expected and state=={'revision':1,'jobs':{},'cells':[]}


def test_pending_batch_validation_reaching_cutoff_preserves_original_state(monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace
    clock=[0.];events=[];plan={}
    state={'revision':1,'jobs':{'training-0001':{'job_id':'1','status':'submitted',
           'intent':{'kind':'training','cell_index':1}}},'cells':[]}
    before=copy.deepcopy(state);c={'deadline':1000.,'native_reader':lambda *args,**kwargs:{'1':{}}}
    def audit(*args):clock[0]=701.;events.append('validated');return {'training-0001':{}}
    base=SimpleNamespace(registration_guard=lambda *args:nullcontext(),source_guard=lambda c:None,
        initialize=lambda c:(plan,state),admission=lambda c,label:events.append(label),
        normalized_native=lambda *args:{'state':'COMPLETED'},positive_native=lambda *args:True,
        collect_packets=audit,prepare_census=lambda c:None,
        submit_intentions=lambda *args:pytest.fail('Late resume must not submit'),
        save_state=lambda *args:pytest.fail('Late resume must not commit'))
    monkeypatch.setattr(m,'preserve_pending',lambda *args:Path('preserved-original-batch'))
    monkeypatch.setattr(m,'reconcile_only',lambda *args:pytest.fail('Late validation must defer its commit'))
    monkeypatch.setattr(m.time,'time',lambda:clock[0])
    with pytest.raises(m.SubmissionWindowExhausted):m.resume_pending_batch(base,c,plan,state)
    assert events==['resume-snapshot-1','validated'] and state==before
