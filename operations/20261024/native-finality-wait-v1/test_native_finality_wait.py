from pathlib import Path
from types import SimpleNamespace
import importlib.util
import pytest

@pytest.fixture
def m():
    p=Path(__file__).with_name('native-finality-wait.py');spec=importlib.util.spec_from_file_location('finality_wait_test',p)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def lines(m):
    return '\n'.join('|'.join([job,row['state'],row['exit_code'],str(row['elapsed']),str(row['cpus']),
        'pwa209','def-ptewarie_cpu','cpu='+str(row['cpus'])+',mem=64G,node=1']) for job,row in m.EXPECTED.items())

def test_exact_three_final_records_are_required(m):assert m.assessed_rows(lines(m))['ready']

@pytest.mark.parametrize('text',['','6982735|RUNNING|0:0|4000|8|pwa209|def-ptewarie_cpu|cpu=8,mem=64G'])
def test_missing_or_stale_running_records_never_authorize_handoff(m,text):assert not m.assessed_rows(text)['ready']

@pytest.mark.parametrize('before,after',[('pwa209','foreign'),('def-ptewarie_cpu','foreign'),('2714','2715'),
    ('COMPLETED','CANCELLED'),('FAILED','COMPLETED'),('1:0','0:0'),('cpu=8','cpu=8,gres/gpu=1')])
def test_unexpected_identity_resources_or_outcome_refuse(m,before,after):
    with pytest.raises(ValueError):m.assessed_rows(lines(m).replace(before,after))

def test_duplicate_rows_are_not_completion(m):
    text=lines(m)
    with pytest.raises(ValueError):m.assessed_rows(text+'\n'+text.splitlines()[0])

def test_unknown_job_is_refused(m):
    with pytest.raises(ValueError):m.assessed_rows(lines(m).replace('6982735','123'))

def test_malformed_row_is_refused(m):
    with pytest.raises(ValueError):m.assessed_rows('malformed')

def test_stale_record_is_polled_until_real_finality(m):
    time=[1000.0];observations=[];calls=[]
    def run(*args,**kwargs):
        calls.append(args[0]);return SimpleNamespace(returncode=0,stderr='',stdout='' if len(calls)==1 else lines(m))
    result=m.wait_finality(1060,observations.append,runner=run,clock=lambda:time[0],sleeper=lambda seconds:time.__setitem__(0,time[0]+seconds))
    assert result['ready'] and len(calls)==len(observations)==2 and time[0]==1020
    assert all(command[0]=='sacct' for command in calls)

def test_deadline_ends_without_launch_or_mutation(m):
    time=[1000.0];observations=[]
    def run(*args,**kwargs):return SimpleNamespace(returncode=0,stderr='',stdout='')
    with pytest.raises(TimeoutError):m.wait_finality(1045,observations.append,runner=run,clock=lambda:time[0],sleeper=lambda seconds:time.__setitem__(0,time[0]+seconds))
    assert time[0]==1045 and len(observations)==3

def test_read_timeout_can_retry_but_never_replays_mutations(m):
    time=[1000.0];calls=[];observations=[]
    def run(command,**kwargs):
        calls.append(command)
        if len(calls)==1:raise m.subprocess.TimeoutExpired(command,25)
        return SimpleNamespace(returncode=0,stderr='',stdout=lines(m))
    assert m.wait_finality(1060,observations.append,runner=run,clock=lambda:time[0],sleeper=lambda seconds:time.__setitem__(0,time[0]+seconds))['ready']
    assert observations[0]['status']=='read_only_timeout' and all(c[0]=='sacct' for c in calls)

def test_invalid_rows_fail_without_retrying_or_waiting(m):
    def run(*args,**kwargs):return SimpleNamespace(returncode=0,stderr='',stdout='invalid')
    def forbidden(*args):raise AssertionError('must not sleep after invalid native identity')
    with pytest.raises(ValueError):m.wait_finality(1060,forbidden,runner=run,clock=lambda:1000,sleeper=forbidden)
