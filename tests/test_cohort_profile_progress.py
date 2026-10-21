import copy
import pytest
from horsehair import cohort_profile as p


def test_many_tiny_operations_do_not_each_fsync_but_keep_full_totals(tmp_path,monkeypatch):
    now=[0.];writes=[]
    monkeypatch.setattr(p,'atomic_json',lambda path,value:writes.append(copy.deepcopy(value)))
    saver=p.ProgressWriter(tmp_path/'progress.json',clock=lambda:now[0])
    progress={}
    def emit(event):
        progress.update(last_event=event,timings=event.get('totals',{}))
        saver(progress,event)
    timer=p.Timings(emit)
    for _ in range(100):timer.call('input_read',lambda:42)
    assert len(writes)==1
    assert timer.totals['input_read']['calls']==100
    assert progress['timings']['input_read']['calls']==100
    now[0]=30.
    saver(progress,progress['last_event'])
    assert len(writes)==2 and writes[-1]['timings']['input_read']['calls']==100


@pytest.mark.parametrize('event',sorted(p.ProgressWriter.FORCE_EVENTS))
def test_important_boundary_is_immediate_even_within_interval(tmp_path,monkeypatch,event):
    writes=[]
    monkeypatch.setattr(p,'atomic_json',lambda path,value:writes.append(copy.deepcopy(value)))
    saver=p.ProgressWriter(tmp_path/'progress.json',clock=lambda:1.)
    assert saver({'marker':1},{'event':'started'})
    assert saver({'marker':2},{'event':event})
    assert writes==[{'marker':1},{'marker':2}]


@pytest.mark.parametrize('interval',[0,-1,float('nan'),float('inf')])
def test_invalid_persistence_bound_refused(tmp_path,interval):
    with pytest.raises(ValueError):p.ProgressWriter(tmp_path/'progress.json',interval_seconds=interval)


def test_failed_write_is_not_counted_as_a_persisted_heartbeat(tmp_path,monkeypatch):
    def fail(*args):raise OSError('storage error')
    monkeypatch.setattr(p,'atomic_json',fail)
    saver=p.ProgressWriter(tmp_path/'progress.json',clock=lambda:1.)
    with pytest.raises(OSError):saver({}, {'event':'started'})
    assert saver.last_saved is None
