"""Exercise actual held-native admission gates, including observed Slurm keys."""
from pathlib import Path
from datetime import datetime,timezone
import ast,json,re,runpy
import pytest

W=Path(__file__).resolve().parent
SOURCE=(W/'hhw-recovery-stage-20261021-429.sh').read_text()
BODY=SOURCE.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
TREE=ast.parse(BODY)
NODE=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='native_held_fields')
VALIDATOR=runpy.run_path(str(W/'run-fresh-storage-census-v6-20261023.py'),run_name='local_429_held_validator')

@pytest.fixture
def gate():
    ns={'re':re,'datetime':datetime,'timezone':timezone,'validator':VALIDATOR,'ids':['8001','8002'],
        'root':Path('/scratch/pwa209/horsehair-worm'),'completed_native_dependency':lambda root,job:{'job_id':job}}
    exec(compile(ast.Module(body=[NODE],type_ignores=[]),'<actual-native-held-gate>','exec'),ns)
    return ns,ns['native_held_fields']

def sample(index=0):
    raw=json.loads(next(x for x in (W.parent/'narval-deployment/hhw-held-canary-diagnostics-20261023-439.txt').read_text().splitlines()
        if x.startswith('HELD_CANARY_DIAGNOSTICS ')).split(' ',1)[1])['scontrol']['stdout']
    name='rolling-storage-census-'+str(index+1).zfill(2);job=str(8001+index)
    script='/scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-429/'+name+'.sbatch'
    raw=raw.replace('6859353',job).replace('hhw-recovery-436-held-begin-canary','hhw-'+name)
    old='/scratch/pwa209/horsehair-worm/plans/recovery-support-20261021-436/recovery-436-held-begin-canary.sbatch'
    raw=raw.replace(old,script).replace('NumCPUs=1','NumCPUs=8').replace('CPUs/Task=1','CPUs/Task=8').replace('cpu=1','cpu=8').replace('mem=4G','mem=64G').replace('MinMemoryNode=4G','MinMemoryNode=64G').replace('TimeLimit=01:00:00','TimeLimit=06:00:00')
    task={'name':name,'script':script,'begin_not_before_unix':1792766108}
    record={'issued_native_dependencies':[],'completed_native_dependencies':[{'job_id':'6859130','dependency_kind':'afterok'}]}
    if index:
        raw=raw.replace('Dependency=(null)','Dependency=afterok:8001(unfulfilled)');record={'issued_native_dependencies':['8001']}
    return raw,task,job,record,index,'6859130'

@pytest.mark.parametrize('index',[0,1])
def test_observed_slash_colon_fields_null_allocation_and_earliest_time_pass(gate,index):
    ns,fn=gate;value=fn(*sample(index))
    assert value['AllocTRES']=='(null)' and value['Socks/Node']=='*' and value['NtasksPerN:B:S:C']=='0:0:*:*'
    assert value['EligibleTime']=='2026-10-23T14:35:08'

@pytest.mark.parametrize('before,after',[
    ('UserId=pwa209(3164768)','UserId=other(999)'),('Account=def-ptewarie_cpu','Account=other'),
    ('JobState=PENDING','JobState=RUNNING'),('Reason=JobHeldUser','Reason=Dependency'),
    ('NumCPUs=8','NumCPUs=16'),('NumNodes=1-1','NumNodes=2'),('TimeLimit=06:00:00','TimeLimit=12:00:00'),
    ('MinMemoryNode=64G','MinMemoryNode=16G'),('mem=64G','mem=32G'),('node=1','node=2'),
    ('AllocTRES=(null)','AllocTRES=cpu=8,mem=64G,node=1'),('TresPerTask=cpu=8','TresPerTask=gres/gpu:1'),
    ('Dependency=afterok:8001(unfulfilled)','Dependency=(null)'),('Dependency=afterok:8001(unfulfilled)','Dependency=afterany:8001(unfulfilled)'),
    ('EligibleTime=2026-10-23T14:35:08','EligibleTime=2026-10-23T14:35:07'),('EligibleTime=2026-10-23T14:35:08','EligibleTime=Unknown'),
])
def test_foreign_wrong_resource_unheld_missing_edge_or_early_future_is_refused(gate,before,after):
    ns,fn=gate;args=list(sample(1));assert before in args[0];args[0]=args[0].replace(before,after)
    with pytest.raises((AssertionError,ValueError)):fn(*args)

def test_duplicate_or_multiple_native_rows_refused(gate):
    ns,fn=gate;args=list(sample())
    for raw in [args[0].rstrip()+' JobId=8001\n',args[0]+args[0]]:
        with pytest.raises(AssertionError):fn(raw,*args[1:])

def test_completed_first_edge_requires_actual_final_proof(gate):
    ns,fn=gate;ns['completed_native_dependency']=lambda root,job:None
    with pytest.raises(AssertionError):fn(*sample())

def test_lost_or_foreign_first_source_proof_refused(gate):
    ns,fn=gate
    for proofs in [[],[{'job_id':'other','dependency_kind':'afterok'}],[{'job_id':'6859130','dependency_kind':'afterany'}]]:
        args=list(sample());args[3]['completed_native_dependencies']=proofs
        with pytest.raises(AssertionError):fn(*args)

def test_first_released_only_after_all_native_gates_and_per_job_intents():
    assert BODY.index("fields=native_held_fields")<BODY.index("for index,job in enumerate(reversed(ids),1)")
    assert "['scontrol','release',job]" in BODY and "['scontrol','release',*group]" not in BODY
    loop=next(n for n in ast.walk(TREE) if isinstance(n,ast.For) and ast.unparse(n.iter)=='enumerate(reversed(ids), 1)')
    assert ast.unparse(loop.body[0]).startswith("atomic_json(area / ('release-'")
    assert '-intent.json' in ast.unparse(loop.body[0])
    assert isinstance(loop.body[1],ast.Assign) and ast.unparse(loop.body[1].value).startswith('subprocess.run(')
    assert '-response.json' in ast.unparse(loop.body[2])
    assert ast.unparse(loop.body[3])=='response.check_returncode()'
