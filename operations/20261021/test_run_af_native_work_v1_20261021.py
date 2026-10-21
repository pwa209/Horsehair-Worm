"""Private orchestration tests. All native/scientific work is replaced by sentinels."""
from contextlib import contextmanager
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from horsehair import agents, agent_execution_v2 as evaluation, cli, cohort_profile
from horsehair.core import canonical_hash, digest, read_json
import horsehair


WORKER = Path(__file__).with_name('run-af-native-work-v1-20261021.py')
MODES = {
    'gpu-qualify': (6, 1, 2), 'agent-train': (6, 1, 12),
    'agent-evaluation': (4, 0, 12), 'agent-assembly': (1, 0, 1),
    'cohort-profile': (1, 0, 2), 'precision-plan': (4, 0, 1),
    'precision-diagnostics': (4, 0, 12), 'precision-chunk': (4, 0, 12),
    'precision-assemble': (4, 0, 1),
}


def write(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, allow_nan=False), encoding='utf-8')


@pytest.fixture
def h(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location('af_native_private', WORKER)
    w = importlib.util.module_from_spec(spec); spec.loader.exec_module(w)
    root = (tmp_path/'personal').resolve(); primary = (tmp_path/'primary').resolve()
    framework = Path(horsehair.__file__).resolve().parents[2]
    config = read_json(framework/'configs/study.json')
    write(primary/'configs/study.json', config); write(primary/'release.json', {'release_id':'original'})
    monkeypatch.setattr(w, 'ROOT', root); monkeypatch.setattr(w, 'PRIMARY', primary)
    monkeypatch.setattr(w.os, 'geteuid', lambda:3164768, raising=False)
    monkeypatch.setattr(w, 'native_job_id', lambda:'123')
    monkeypatch.setenv('USER', 'pwa209')
    monkeypatch.setattr(w.subprocess, 'check_output', lambda *a, **kw:'nc00001.narval.calcul.quebec\n')
    execution = 'a'*64; policy_sha = '4'*64
    area = root/'maintenance/activation'
    descriptor_path = area/'descriptor.json'
    qpath = area/'full-qualification.json'
    counts = {'tests':1694, 'failures':0, 'errors':0, 'skipped':0}
    q = {'status':'complete', 'returncode':0, 'config_sha256':w.BASE_CONFIG_SHA256,
         'source_sha256':execution, 'source_verified_unchanged':True,
         'test_counts':counts, 'slurm_job_id':'100'}
    write(qpath,q)
    protocol = {'config_sha256':agents._digest(config['agents']), 'full_config_sha256':canonical_hash(config),
                'implementation_sha256':w.AGENTS_SHA, 'H4_inference_specification':agents.h4_specification(config),
                'learning_rate':.001, 'final_seeds':config['agents']['final_seeds'],
                'biological_directions':None, 'directions':{}}
    protocol_path=root/'results/repair-v7-2026-10-13/agents/frozen_protocol.json';write(protocol_path,protocol)
    policy = SimpleNamespace(sha256=policy_sha, receipt=lambda:{'sha256':policy_sha})
    comparisons = {a:{'max_action_probability_difference':1e-6,'tolerance':3e-5} for a in ['rnn','gru','modular','feedforward']}
    gpu_result = {'status':'complete','iterations':20,'device':'A100','cuda':'12.6','torch':'2.6',
                  'elapsed_seconds':2.,'loss':.01,'cpu_cuda_regression':comparisons}
    gpu_path=area/'gpu-qualification.json'
    gpu={'status':'complete_native_work','mode':'gpu-qualify','native_job_id':'101',
         'descriptor_sha256':'8'*64,'source_sha256':execution,'full_config_sha256':w.BASE_CONFIG_SHA256,
         'resource_policy':{'sha256':policy_sha},'result':gpu_result};write(gpu_path,gpu)
    request_path=area/'canary-request.json';canary_case=root/'maintenance/agent-precision-canary-20261021'
    kwargs={'task':'delayed_choice','architecture':'gru','seed':900,
            'canary':{'ranks':[1,4],'strengths':[0,.4],'evaluation_episodes':4,'calibration_episodes':32,'kl_bisections':10},
            'benchmark_directions':True}
    checkpoint=agents._directory(root,config['agents'])/'benchmark-training'/agents._cell_name('delayed_choice','gru',900)/'checkpoint.pt'
    checkpoint.parent.mkdir(parents=True);checkpoint.write_bytes(b'original checkpoint sentinel')
    request={'execution_module_sha256':digest(Path(evaluation.__file__)),
             'frozen_protocol_binding':{'sha256':digest(protocol_path)}, 'checkpoint_binding':{'path':str(checkpoint),'sha256':digest(checkpoint)},
             'case':str(canary_case),'prepare_plan_kwargs':kwargs,'required_conditions':52,'required_rows':1664}
    write(request_path,request)
    final_case=root/'maintenance/final-campaign/cell-0000';cell=evaluation.final_cells(config)[0]
    final_checkpoint=agents._directory(root,config['agents'])/'final'/agents._cell_name(cell['task'],cell['architecture'],cell['seed'])/'checkpoint.pt'
    final_plan={'canary':False,'benchmark_directions':False,**{k:cell[k] for k in ['task','architecture','seed']},
                'config':copy.deepcopy(config),'full_config_sha256':w.BASE_CONFIG_SHA256,'original_protocol':copy.deepcopy(protocol),
                'original_protocol_sha256':agents._digest(protocol),'evaluation_settings':evaluation._settings(config),
                'conditions':evaluation._conditions(evaluation._settings(config),protocol),'original_training_steps':1000000,
                'checkpoint':str(final_checkpoint)}
    write(final_case/'plan.json',final_plan);write(final_case/'plan-binding.json',{'sentinel':'independent final binding'})
    canary_plan={'config':copy.deepcopy(config),'full_config_sha256':w.BASE_CONFIG_SHA256,'original_protocol':copy.deepcopy(protocol),
                 'original_protocol_sha256':agents._digest(protocol),'checkpoint':str(checkpoint.resolve()),
                 'checkpoint_sha256':digest(checkpoint),**{k:kwargs[k] for k in ['task','architecture','seed']},
                 'canary':True,'canary_overrides':copy.deepcopy(kwargs['canary']),'benchmark_directions':True,
                 'evaluation_settings':dict(evaluation._settings(config),**kwargs['canary'])}
    d={'framework_release':str(framework),'case':str(area),'execution_source_sha256':execution,'worker_sha256':digest(WORKER),
       'resource_policy':{'path':str(root/'maintenance/execution-resource-policies/policy.json'),'sha256':policy_sha},
       'qualification':{'path':str(qpath),'sha256':digest(qpath),'test_counts':copy.deepcopy(counts)},
       'gpu_qualification':{'path':str(gpu_path),'sha256':digest(gpu_path),'job_id':'101','descriptor_sha256':'8'*64},
       'frozen_protocol_sha256':digest(protocol_path),'agent_campaign_case':str(final_case.parent),
       'cohort_case':str(root/'maintenance/cohort'),'canary_request':{'path':str(request_path),'sha256':digest(request_path)}}
    state=SimpleNamespace(w=w,root=root,primary=primary,framework=framework,config=config,d=d,q=q,gpu=gpu,
        request=request,final_plan=final_plan,canary_plan=canary_plan,descriptor_path=descriptor_path,qpath=qpath,
        gpu_path=gpu_path,request_path=request_path,canary_case=canary_case,final_case=final_case,
        calls=[],leases=[],dependency_fail=set(),fields={},cuda_count=1)
    def pinned_digest(path):
        path=Path(path)
        for key in ['GUARD','SCHEDULER','QUOTA']:
            if path==getattr(w,key):return getattr(w,key+'_SHA')
        return digest(path)
    monkeypatch.setattr(w,'digest',pinned_digest)
    monkeypatch.setattr(w,'source_identity',lambda p:{'sha256':w.PRIMARY_SCIENTIFIC_SOURCE_SHA256 if Path(p)==primary else execution})
    monkeypatch.setattr(w,'load_resource_policy',lambda *a,**kw:policy)
    monkeypatch.setattr(w,'combined_admission',lambda *a,**kw:{'status':'admitted'})
    monkeypatch.setattr(w,'completed_native_dependency',lambda r,j:None if str(j) in state.dependency_fail else {'job_id':str(j)})
    @contextmanager
    def lease(*args,**kw):
        state.leases.append(kw);yield {'sentinel':'lease'}
    monkeypatch.setattr(w,'resource_lease',lease)
    def readers(path):
        path=Path(path)
        if path==w.GUARD:return {'install':lambda p:None}
        if path==w.SCHEDULER:
            def scheduler(*a):return subprocess.CompletedProcess(['scontrol'],0,' '.join(k+'='+v for k,v in state.fields.items()),'')
            return {'run_scontrol_job':scheduler}
        if path==w.QUOTA:return {'require_storage_headroom_with_wait':lambda *a,**kw:{'status':'enough_headroom'}}
        raise AssertionError('Unmocked native helper: '+str(path))
    monkeypatch.setattr(w.runpy,'run_path',readers)
    import torch
    monkeypatch.setattr(torch,'set_num_threads',lambda n:state.calls.append(('threads',n)))
    monkeypatch.setattr(torch.cuda,'is_available',lambda:True)
    monkeypatch.setattr(torch.cuda,'device_count',lambda:state.cuda_count)
    def scientific(name, result):
        def call(*a,**kw):state.calls.append((name,a,kw));return copy.deepcopy(result)
        return call
    monkeypatch.setattr(cli,'qualification',scientific('gpu',gpu_result))
    def train(*a,**kw):
        state.calls.append(('train',a,kw));write(final_checkpoint.parent/'summary.json',{'sentinel':'original training summary'});return {'status':'trained'}
    monkeypatch.setattr(agents,'run',train)
    monkeypatch.setattr(cohort_profile,'run_profile',scientific('profile',{'status':'bounded_deadline_reached'}))
    def prepare(*a,**kw):
        state.calls.append(('prepare',a,kw));write(canary_case/'plan.json',canary_plan);write(canary_case/'plan-binding.json',{'sentinel':'new binding'});return {'conditions':list(range(52))}
    monkeypatch.setattr(evaluation,'prepare_plan',prepare)
    reports=[{'dtype':dtype,'condition_index':i,'status':'unavailable' if dtype=='torch.float32' and i==0 else 'available',
              'unavailable_rows':1 if dtype=='torch.float32' and i==0 else 0} for dtype in ['torch.float32','torch.float64'] for i in range(52)]
    state.diagnostic_result={'reports':reports,'scientific_result':False,'admission_authority':False}
    monkeypatch.setattr(evaluation,'diagnostics',lambda *a,**kw:(state.calls.append(('diagnostics',a,kw)) or copy.deepcopy(state.diagnostic_result)))
    monkeypatch.setattr(evaluation,'run_chunk',scientific('chunk',{'status':'chunk_complete'}))
    monkeypatch.setattr(evaluation,'assemble',scientific('assemble',{'status':'complete_amended_evaluation'}))
    def save():
        write(qpath,state.q);d['qualification']['sha256']=digest(qpath)
        write(gpu_path,state.gpu);d['gpu_qualification']['sha256']=digest(gpu_path)
        write(request_path,state.request);d['canary_request']['sha256']=digest(request_path)
        write(final_case/'plan.json',state.final_plan)
        write(canary_case/'plan.json',state.canary_plan);write(canary_case/'plan-binding.json',{'sentinel':'independent canary binding'})
        d['canary_plan']={'plan_sha256':digest(canary_case/'plan.json'),'plan_binding_sha256':digest(canary_case/'plan-binding.json')}
        write(descriptor_path,d)
    state.save=save
    def run(mode, before=None):
        cpus,gpus,hours=MODES[mode]
        monkeypatch.setenv('SLURM_CPUS_PER_TASK',str(cpus));monkeypatch.setenv('HHW_GPU_COUNT',str(gpus));monkeypatch.setenv('HHW_MAX_HOURS',str(hours))
        monkeypatch.setenv('SLURM_JOB_END_TIME',str(w.time.time()+hours*3600-10))
        state.fields={'JobId':'123','UserId':'pwa209(3164768)','JobState':'RUNNING','NumCPUs':str(cpus),
                      'Account':config['paths']['gpu_account' if gpus else 'cpu_account'],'TimeLimit':str(hours)+':00:00',
                      'AllocTRES':'cpu='+str(cpus)+(',gres/gpu=1,gres/gpu:a100=1' if gpus else '')}
        save()
        argv=['private-worker','--descriptor',str(descriptor_path),'--descriptor-sha256',digest(descriptor_path),
              '--mode',mode,'--cpus',str(cpus),'--gpus',str(gpus),'--hours',str(hours),'--index','0',
              '--evaluation-case',str(final_case),'--evaluation-plan-sha256',digest(final_case/'plan.json'),
              '--evaluation-binding-sha256',digest(final_case/'plan-binding.json')]
        monkeypatch.setattr(sys,'argv',argv)
        if before:before()
        w.main()
        return read_json(area/'native-123/receipt.json')
    state.run=run
    return state


@pytest.mark.parametrize('mode',list(MODES))
def test_nine_mode_dispatches_remain_bounded_source_bound_and_false(h,mode):
    receipt=h.run(mode)
    assert receipt['status']=='complete_native_work' and receipt['mode']==mode
    assert receipt['source_sha256']==h.d['execution_source_sha256']
    assert receipt['full_config_sha256']==h.w.BASE_CONFIG_SHA256
    assert receipt['scientific_result'] is False and receipt['admission_authority'] is False
    cpus,gpus,hours=MODES[mode]
    assert h.leases==[dict(cpus=cpus,gpus=gpus,max_hours=hours,job_id='123',resource_policy=h.leases[0]['resource_policy'])]
    assert ('threads',cpus) in h.calls
    operations=[c for c in h.calls if c[0]!='threads']
    assert len(operations)==1
    name,args,kwargs=operations[0]
    if mode=='agent-train':assert name=='train' and kwargs=={'mode':'final','task_index':0}
    elif mode=='gpu-qualify':assert name=='gpu' and kwargs=={'gpu':True}
    elif mode=='cohort-profile':
        assert name=='profile' and kwargs['kind']=='MX' and kwargs['rate']==.001 and kwargs['device']=='cpu' and kwargs['max_seconds']<=6300
    elif mode in {'agent-evaluation','precision-chunk'}:
        assert name=='chunk' and kwargs['max_conditions']==(300 if mode=='agent-evaluation' else 52)
        assert 0<kwargs['max_wall_seconds']<hours*3600 and len(kwargs['expected_plan_sha256'])==64
    elif mode in {'agent-assembly','precision-assemble'}:assert name=='assemble' and len(kwargs['expected_plan_sha256'])==64
    elif mode=='precision-plan':assert name=='prepare' and kwargs['benchmark_directions'] is True and kwargs['canary']==h.request['prepare_plan_kwargs']['canary']
    else:assert name=='diagnostics' and receipt['float32_failure_reproduced'] and receipt['float64_all_canary_conditions_available']


def no_science(h):
    assert all(c[0]=='threads' for c in h.calls)


@pytest.mark.parametrize('change',['descriptor_bytes','source','worker','primary_source','primary_config','reader'])
def test_changed_descriptor_or_source_never_dispatches_science(h,change):
    before=None
    if change=='descriptor_bytes':before=lambda:write(h.descriptor_path,dict(h.d,unexpected='changed after binding'))
    elif change=='source':h.d['execution_source_sha256']='f'*64
    elif change=='worker':h.d['worker_sha256']='f'*64
    elif change=='primary_source':h.w.source_identity=lambda p:{'sha256':'f'*64}
    elif change=='primary_config':
        changed=copy.deepcopy(h.config);changed['agents']['total_steps']=1
        write(h.primary/'configs/study.json',changed)
    else:
        original=h.w.digest
        h.w.digest=lambda p:'f'*64 if Path(p)==h.w.SCHEDULER else original(p)
    with pytest.raises(ValueError):h.run('cohort-profile',before)
    no_science(h)


@pytest.mark.parametrize('change',['config','source','returncode','skip','native'])
def test_bad_full_qualification_cannot_dispatch_science(h,change):
    if change=='config':h.q['config_sha256']='f'*64
    elif change=='source':h.q['source_sha256']='f'*64
    elif change=='returncode':h.q['returncode']=1
    elif change=='skip':h.q['test_counts']['skipped']=1;h.d['qualification']['test_counts']['skipped']=1
    else:h.dependency_fail.add('100')
    with pytest.raises(ValueError,match='qualified-source'):h.run('cohort-profile')
    no_science(h)


@pytest.mark.parametrize('change',['owner','cpu','account','time','gpu_total','gpu_typed','gpu_visible'])
def test_native_allocation_mismatch_never_dispatches_science(h,change):
    def before():
        if change=='owner':h.fields['UserId']='other(1)'
        elif change=='cpu':h.fields['NumCPUs']='8'
        elif change=='account':h.fields['Account']='foreign'
        elif change=='time':h.fields['TimeLimit']='3:00:00'
        elif change=='gpu_total':h.fields['AllocTRES']='cpu=6,gres/gpu=2,gres/gpu:a100=2'
        elif change=='gpu_typed':h.fields['AllocTRES']='cpu=6,gres/gpu=1,gres/gpu:a100=2'
        else:h.cuda_count=2
    with pytest.raises(ValueError):h.run('gpu-qualify',before)
    no_science(h)


@pytest.mark.parametrize('change',['mode','source','policy','descriptor','native','architecture','nan_loss','bad_difference'])
def test_wrong_gpu_qualification_cannot_start_final_training(h,change):
    if change=='mode':h.gpu['mode']='cpu-qualify'
    elif change=='source':h.gpu['source_sha256']='f'*64
    elif change=='policy':h.gpu['resource_policy']['sha256']='f'*64
    elif change=='descriptor':h.gpu['descriptor_sha256']='f'*64
    elif change=='native':h.dependency_fail.add('101')
    elif change=='architecture':h.gpu['result']['cpu_cuda_regression'].pop('feedforward')
    elif change=='nan_loss':
        # A nonnumeric loss is invalid JSON evidence; write a finite sentinel then
        # inject nonfinite after the immutable file has been read for this guard.
        old=h.w.read_json
        def read(path):
            value=old(path)
            if Path(path)==h.gpu_path:value['result']['loss']=float('nan')
            return value
        h.w.read_json=read
    else:h.gpu['result']['cpu_cuda_regression']['gru']['max_action_probability_difference']=.1
    with pytest.raises(ValueError):h.run('agent-train')
    no_science(h)


@pytest.mark.parametrize('change',['config','protocol','checkpoint','conditions','budget','cell','canary','settings'])
def test_foreign_or_reduced_final_plan_never_evaluates(h,change):
    if change=='config':h.final_plan['config']['agents']['evaluation_episodes']=1
    elif change=='protocol':h.final_plan['original_protocol']['learning_rate']=.003
    elif change=='checkpoint':h.final_plan['checkpoint']=str(h.root/'other.pt')
    elif change=='conditions':h.final_plan['conditions'].pop()
    elif change=='budget':h.final_plan['original_training_steps']=32768
    elif change=='cell':h.final_plan['seed']=900
    elif change=='canary':h.final_plan['canary']=True
    else:h.final_plan['evaluation_settings']['evaluation_episodes']=4
    with pytest.raises(ValueError,match='unchanged full scientific'):h.run('agent-evaluation')
    no_science(h)


@pytest.mark.parametrize('change',['config','protocol','checkpoint','seed','overrides','benchmark','request_path'])
def test_canary_request_and_plan_must_agree(h,change):
    if change=='config':h.canary_plan['config']['agents']['evaluation_episodes']=1
    elif change=='protocol':h.canary_plan['original_protocol']['learning_rate']=.003
    elif change=='checkpoint':h.canary_plan['checkpoint_sha256']='f'*64
    elif change=='seed':h.canary_plan['seed']=901
    elif change=='overrides':h.canary_plan['canary_overrides']['evaluation_episodes']=1
    elif change=='benchmark':h.canary_plan['benchmark_directions']=False
    else:h.d['canary_request']['path']=str(h.root/'outside/request.json')
    with pytest.raises((ValueError,FileNotFoundError)):h.run('precision-diagnostics')
    no_science(h)


@pytest.mark.parametrize('change',['case','checkpoint'])
def test_precision_plan_cannot_write_a_foreign_case_or_load_a_foreign_checkpoint(h,change):
    if change=='case':h.request['case']=str(h.root/'outside')
    else:h.request['checkpoint_binding']['path']=str(h.root/'outside.pt')
    with pytest.raises(ValueError,match='Exact personal canary'):h.run('precision-plan')
    no_science(h)


@pytest.mark.parametrize('change',['no_f32_failure','f64_unavailable','missing_report'])
def test_diagnostic_gate_preserves_reports_and_refuses_bad_precision_evidence(h,change):
    if change=='no_f32_failure':
        for row in h.diagnostic_result['reports']:row.update(status='available',unavailable_rows=0)
    elif change=='f64_unavailable':h.diagnostic_result['reports'][-1].update(status='unavailable',unavailable_rows=1)
    else:h.diagnostic_result['reports'].pop()
    with pytest.raises(ValueError):h.run('precision-diagnostics')
    area=Path(h.d['case'])/'native-123'
    assert read_json(area/'paired-precision-diagnostics.json')==h.diagnostic_result
    assert read_json(area/'receipt.json')['status']=='running'
    assert [c[0] for c in h.calls]==['threads','diagnostics']


def test_existing_native_case_is_preserved_without_replay(h):
    existing=Path(h.d['case'])/'native-123';existing.mkdir(parents=True)
    (existing/'keep').write_text('previous',encoding='utf-8')
    with pytest.raises(ValueError,match='fresh native case'):h.run('gpu-qualify')
    assert (existing/'keep').read_text()=='previous' and h.calls==[]
