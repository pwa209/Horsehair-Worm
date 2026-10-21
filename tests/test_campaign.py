"""The new envelope must preserve costs, failed work and queued reservations."""
import math
import pytest
from horsehair import campaign
from horsehair.execution_resources import LIMITS


@pytest.fixture
def snapshot(monkeypatch):
    monkeypatch.setattr(campaign, 'prospective_registered_budget', lambda *args: {'cpu_core_hours':25000., 'gpu_hours':500.})
    monkeypatch.setattr(campaign, 'prospective_capacity', lambda *args: {'cpus':12, 'gpus':1})
    return [{}, {}, {}, {'leases':{}}, set(), dict(LIMITS)]


def test_explicit_removed_caps_preserve_totals(snapshot):
    result=campaign.assess_registered_snapshot(*snapshot,cpus=6,gpus=1,hours=12)
    assert result['totals']=={'cpu_core_hours':25000.,'gpu_hours':500.}
    assert result['proposed_reservation']=={'cpu_core_hours':72,'gpu_hours':12}


def test_legacy_caps_still_reject(snapshot):
    snapshot[-1].update(cpu_core_hours_cap=20000,gpu_hours_cap=400)
    with pytest.raises(ValueError,match='total resource policy'):
        campaign.assess_registered_snapshot(*snapshot,cpus=1,gpus=0,hours=1)


def test_outside_leases_reserved_once_and_aliases_deduplicated(snapshot):
    snapshot[3].update(leases={'40':{'state':'running','cpus':4,'gpus':1,'max_hours':5},'50':{'state':'running','cpus':6,'gpus':1,'max_hours':3}},native_raw_aliases={'40':'41_0'})
    snapshot[4].add('41_0')
    result=campaign.assess_registered_snapshot(*snapshot,cpus=1,gpus=0,hours=1)
    assert set(result['external_active_leases'])=={'50'}
    assert result['totals']=={'cpu_core_hours':25018.,'gpu_hours':503.}
    assert result['capacity']=={'cpus':18,'gpus':2}


@pytest.mark.parametrize('cpus,gpus,hours',[(True,0,1),(1,True,1),(257,0,1),(1,2,1),(0,1,1),(1,0,math.nan),(1,0,math.inf),(1,0,169),(1,0,True)])
def test_invalid_requests(snapshot,cpus,gpus,hours):
    with pytest.raises(ValueError):campaign.assess_registered_snapshot(*snapshot,cpus=cpus,gpus=gpus,hours=hours)


def test_queue_capacity_not_only_running_leases(snapshot,monkeypatch):
    monkeypatch.setattr(campaign,'prospective_capacity',lambda *args:{'cpus':256,'gpus':4})
    with pytest.raises(ValueError,match='concurrency'):
        campaign.assess_registered_snapshot(*snapshot,cpus=1,gpus=0,hours=1)


def test_unreconciled_outside_terminal_cost_rejected(snapshot):
    snapshot[3].update(leases={'30':{'state':'running','cpus':1,'gpus':0,'max_hours':2}},native_jobs={'30':{}})
    with pytest.raises(ValueError,match='reconciliation'):
        campaign.assess_registered_snapshot(*snapshot,cpus=1,gpus=0,hours=1)


def test_real_registered_budget_retains_failed_spend_and_all_pending_array_members():
    plans={'phase':{'tasks':[{'name':'train','resources':{'cpus':6,'gpu':True,'hours':12,'array':'0-2%2'},'depends':[]}]}}
    ledgers={'phase':{'tasks':{'train':{'state':'submitted','job_id':'500'}}}}
    native={'500_0':{'state':'FAILED','exit_code':'1:0'}}
    budget={'spent_cpu_core_hours':25000.,'spent_gpu_hours':500.,'leases':{}}
    result=campaign.assess_registered_snapshot(plans,ledgers,native,budget,{'500_0','500_1','500_2'},dict(LIMITS),cpus=1,gpus=0,hours=1)
    assert result['totals']['cpu_core_hours']==25144
    assert result['totals']['gpu_hours']==524
    assert isinstance(result['totals']['basis'],str)
    assert result['capacity']=={'cpus':12,'gpus':2}


@pytest.mark.parametrize('value',[math.nan,math.inf,-1,True])
def test_nonfinite_costs_rejected(snapshot,monkeypatch,value):
    monkeypatch.setattr(campaign,'prospective_registered_budget',lambda *args:{'cpu_core_hours':value,'gpu_hours':0})
    with pytest.raises(ValueError,match='registered costs'):
        campaign.assess_registered_snapshot(*snapshot,cpus=1,gpus=0,hours=1)
