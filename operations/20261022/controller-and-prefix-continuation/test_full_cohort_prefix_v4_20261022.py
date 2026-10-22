"""Synthetic full-cohort prefix contracts; no scientific data or native calls."""
from pathlib import Path
import copy
import ast
import hashlib
import importlib.util
import json
import os
import subprocess
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from horsehair import semimarkov as s,behaviour as b,cohort_profile as profile
from horsehair.core import atomic_json,digest

spec=importlib.util.spec_from_file_location('private_cohort_prefix',Path(__file__).with_name('run-full-cohort-prefix-v4-20261022.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class Unreaped:
    pid=900000001
    returncode=None
    def __init__(self,waits=None):self.waits=[];self.signals=[];self.answers=iter(waits or [])
    def poll(self):return self.returncode
    def wait(self,timeout):
        self.waits.append(timeout)
        answer=next(self.answers,'timeout')
        if answer=='timeout':raise subprocess.TimeoutExpired(['synthetic-child'],timeout)
        self.returncode=answer;return answer


@pytest.fixture
def posix_group_signals(monkeypatch):
    # The injected sender exercises the allocated Linux process-group path.
    # Windows lacks SIGKILL; no real signal or process is used by these fakes.
    monkeypatch.setattr(m.signal,'SIGTERM',15,raising=False)
    monkeypatch.setattr(m.signal,'SIGKILL',9,raising=False)


def test_teardown_timeout_after_term_and_kill_retains_unknown_reap_status(posix_group_signals):
    process=Unreaped();signals=[]
    result=m.stop(process,group_signal=lambda pid,sig:signals.append((pid,sig)))
    assert len(signals)==2 and process.waits==[10.,20.]
    assert result['reaped'] is False and result['termination_confirmed'] is False and result['returncode'] is None
    assert result['additional_teardown_replay_permitted'] is False


@pytest.mark.parametrize('answers,requests',[([0],1),(['timeout',-9],2)])
def test_teardown_confirms_only_actual_reap(answers,requests,posix_group_signals):
    process=Unreaped(answers);signals=[]
    result=m.stop(process,group_signal=lambda *a:signals.append(a))
    assert result['reaped'] is True and result['returncode']==answers[-1] and len(signals)==requests


def test_teardown_signal_failure_is_recorded_and_does_not_lose_wait_diagnostics(posix_group_signals):
    process=Unreaped()
    def failure(*args):raise PermissionError('synthetic signal refusal')
    result=m.stop(process,group_signal=failure)
    assert result['reaped'] is False and len(result['errors'])==2 and len(result['waits'])==2


@pytest.mark.parametrize('bound',['deadline','memory','rss-error'])
def test_supervisor_preserves_partial_snapshot_when_child_cannot_be_reaped(tmp_path,monkeypatch,bound,posix_group_signals):
    body={'status':'whole_cohort_prefix_running','primary_source_sha256':m.PRIMARY_SHA,
        'framework_source_sha256':m.SOURCE,'full_config_sha256':m.CONFIG,'completed_stages':[],**m.FALSE_FLAGS}
    atomic_json(tmp_path/'progress.json',body);process=Unreaped();stops=[]
    def stopper(p):
        stops.append(p);return m.stop(p,group_signal=lambda *args:None)
    def rss(pid):
        if bound=='rss-error':raise OSError('synthetic inaccessible RSS')
        return m.MEMORY_LIMIT
    result=m.supervise(['synthetic-child'],{},0 if bound=='deadline' else 100,tmp_path,
        popen=lambda *a,**k:process,clock=lambda:0,sleeper=lambda x:pytest.fail('No additional child work'),rss=rss,stopper=stopper)
    assert result['status']=='worker_termination_unconfirmed' and result['worker_returncode'] is None
    assert len(stops)==1 and result['termination']['reaped'] is False
    assert result['requires_native_terminal_proof_before_consumption'] is True
    snapshot=result['preserved_partial_progress'];assert snapshot['path']!=str(tmp_path/'progress.json')
    atomic_json(tmp_path/'progress.json',{'changed':'still-running-child'})
    assert json.loads(Path(snapshot['path']).read_text())==body and digest(snapshot['path'])==snapshot['sha256']
    assert all(result[k] is False for k in m.FALSE_FLAGS)


def test_supervisor_reaped_deadline_partial_still_does_not_claim_complete(tmp_path,posix_group_signals):
    body={'status':'whole_cohort_prefix_running','primary_source_sha256':m.PRIMARY_SHA,
        'framework_source_sha256':m.SOURCE,'full_config_sha256':m.CONFIG,'completed_stages':[],**m.FALSE_FLAGS}
    atomic_json(tmp_path/'progress.json',body);process=Unreaped([-9])
    result=m.supervise(['synthetic-child'],{},0,tmp_path,popen=lambda *a,**k:process,clock=lambda:0,
        stopper=lambda p:m.stop(p,group_signal=lambda *args:None))
    assert result['status']=='bounded_deadline_reached' and result['termination']['reaped'] is True
    assert result['requires_native_terminal_proof_before_consumption'] is False


def clock(ident,group,n=9,late=True):
    rng=np.random.default_rng(sum(map(ord,ident)));y=rng.normal(size=(n,2))*.1
    observed=np.ones(n,dtype=bool) if late else np.arange(n)<2
    observed[2]=False;y[~observed]=np.nan
    return s.Sequence(ident,group,y,np.column_stack([np.arange(n),np.cos(np.arange(n))]),
        np.arange(n)[:,None]/100,np.vstack([np.zeros((1,2)),np.nan_to_num(y[:-1])]),
        np.tile([0.,1.],(n,1)),np.tile([0.,1.],(n,1)),observed,np.arange(n)*10.)


@pytest.fixture
def sample():
    torch.set_num_threads(1)
    raw=[clock('a','train1'),clock('b','train2',11,False)]
    scaler=s.TrainingScaler.fit(raw);train=[scaler.transform(q) for q in raw]
    model=s.MovementModel('MX',2,1,knots=[1.,2.,3.],animal_ids=['a','b'],experiment_ids=['train1','train2'])
    with torch.no_grad():model.animal_z.add_(.02);model.experiment_z.add_(.03)
    return model,train,scaler,[clock('valid','held'),clock('null','held',late=False)]


def test_exact_full_prior_all_likelihood_gradient_and_one_update_match_primary_prefix(sample):
    model,train,scaler,validation=sample;reference=copy.deepcopy(model)
    normalizer=sum(int(q.observed.sum()) for q in train);opt=torch.optim.Adam(reference.parameters(),lr=.001)
    opt.zero_grad();prior=-reference.log_prior()/normalizer;prior.backward();expected=prior.detach().clone()
    for q in train:
        loss=-reference(q)['log_likelihood'].sum()/normalizer;loss.backward();expected+=loss.detach()
    raw_grad={n:None if p.grad is None else p.grad.clone() for n,p in reference.named_parameters()}
    torch.nn.utils.clip_grad_norm_(reference.parameters(),5);opt.step()
    commits=[]
    def commit(stage,value,artifact):
        # Snapshot immediately; actual native commit serializes these same bytes.
        commits.append((stage,copy.deepcopy(value),copy.deepcopy(artifact)))
    result=m.run_prefix(s,profile,model,train,validation,scaler,normalizer,
        {'learning_rate':.001,'warmup_seconds':30},time.monotonic()+60,lambda e:None,commit)
    assert [r[0] for r in commits]==list(m.STAGES[1:])
    assert commits[0][1]['objective']==pytest.approx(float(expected))
    for name,value in commits[0][2]['named_gradients'].items():
        if value is None:assert raw_grad[name] is None
        else:torch.testing.assert_close(value,raw_grad[name],rtol=1e-11,atol=1e-11)
    for (name,p),(other,q) in zip(model.named_parameters(),reference.named_parameters()):
        assert name==other;torch.testing.assert_close(p,q,rtol=1e-12,atol=1e-12)
    assert commits[1][1]['clip_calls']==commits[1][1]['Adam_update_calls']==1
    calls=commits[0][1]['timings']
    assert calls['prior_forward']['calls']==calls['prior_backward']['calls']==1
    assert calls['likelihood_forward']['calls']==calls['likelihood_backward']['calls']==len(train)
    assert result['original_validation_criterion_defined'] is False
    assert result['original_validation_criterion'] is None
    assert result['experiment_denominators']=={'held':{'original_animals':2,'defined_animals':1,'undefined_animals':1,'eligible_bins':6}}
    assert result['rows'][1]['log_score'] is None and result['rows'][1]['clock_bins']==9


def test_undefined_original_score_still_executes_original_whole_clock_forward(sample,monkeypatch):
    model,train,scaler,validation=sample;fit=s.Fit(model,scaler,{'tail_tolerance':1e-8});calls=[]
    old=model.forward
    def forward(sequence,*a,**kw):calls.append((len(sequence.times),kw));return old(sequence,*a,**kw)
    monkeypatch.setattr(model,'forward',forward)
    actual=m.validation_call_or_null(fit,validation[1],30)
    assert actual['status']=='undefined_original_score' and actual['n_scored']==0 and actual['log_score'] is None
    assert calls==[(9,{'return_filter':True})]
    with pytest.raises(ValueError,match='No observed prediction window'):fit.score(validation[1],30)


@pytest.mark.parametrize('message',['Duration tail probability failed','different malformed input'])
def test_only_exact_original_zero_window_error_may_become_null(sample,message):
    *_,validation=sample
    class Fit:
        def score(self,*args):raise ValueError(message)
    with pytest.raises(ValueError,match=message):m.validation_call_or_null(Fit(),validation[1],30)


def test_positive_window_error_cannot_be_reclassified_as_null(sample):
    *_,validation=sample
    class Fit:
        def score(self,q,*args):raise ValueError('No observed prediction window after warm-up: '+q.animal_id)
    with pytest.raises(ValueError):m.validation_call_or_null(Fit(),validation[0],30)


def test_all_defined_summary_preserves_equal_experiment_then_animal_weight():
    rows=[dict(animal_id='a',experiment_id='x',status='defined_original_score',log_score=1.,n_scored=10),
          dict(animal_id='b',experiment_id='x',status='defined_original_score',log_score=3.,n_scored=100),
          dict(animal_id='c',experiment_id='y',status='defined_original_score',log_score=8.,n_scored=1)]
    result=m.validation_summary(rows)
    assert result['original_validation_criterion']==-5.
    assert result['alternative_eligible_only_criterion_executed'] is False


def test_all_null_experiment_is_retained_without_zero_or_alternative_criterion():
    rows=[dict(animal_id='a',experiment_id='x',status='undefined_original_score',log_score=None,n_scored=0),
          dict(animal_id='b',experiment_id='y',status='defined_original_score',log_score=8.,n_scored=1)]
    result=m.validation_summary(rows)
    assert result['original_validation_criterion'] is None
    assert result['experiment_denominators']['x']['original_animals']==1
    assert result['experiment_denominators']['x']['defined_animals']==0
    assert result['rows']==rows and result['null_scores_replaced_by_zero'] is False


def frame(ident,group,n=75,mode='late'):
    times=np.arange(n)*10.;observed=np.ones(n,dtype=bool)
    if mode=='early':observed=np.arange(n)<2
    if mode=='none':observed[:]=False
    return pd.DataFrame({'animal_id':ident,'experiment_id':group,'analysis_role':'behavioural_development',
        'observed':observed,'record_time_s':times,'dx':np.where(observed,.1,np.nan),'dy':np.where(observed,.2,np.nan),
        'x':np.arange(n)/10,'y':np.arange(n)/5,'state_bin_seconds':10.,'food_end':10.,'path_length':.2})


@pytest.mark.parametrize('mode,count',[('late',15),('early',0),('none',0)])
def test_census_exact_effective_mask_and_full_clocks(mode,count):
    source=frame('a','g',mode=mode);row={'animal_id':'a','experiment_id':'g','bins':75}
    value=m.clock_census(source,row,600)
    assert value['bins']==75 and value['eligible_prediction_bins']==count and value['recording_time_span_seconds']==740
    assert value['prediction_log_score'] is None


def test_nonfinite_displacement_uses_primary_effective_missingness_mask():
    source=frame('a','g');source.loc[source.index[60:],'dy']=np.nan
    value=m.clock_census(source,{'animal_id':'a','experiment_id':'g','bins':75},600)
    assert value['eligible_prediction_bins']==0 and value['observed_bins']==60


@pytest.mark.parametrize('bad',['clock','identity','length','warmup'])
def test_census_rejects_clock_or_identity_change(bad):
    source=frame('a','g');row={'animal_id':'a','experiment_id':'g','bins':75};warmup=600
    if bad=='clock':source.loc[1,'record_time_s']=0
    elif bad=='identity':row['animal_id']='other'
    elif bad=='length':row['bins']=74
    else:warmup=float('nan')
    with pytest.raises(ValueError):m.clock_census(source,row,warmup)


def test_actual_parquet_prepare_retains_whole_roster_null_clocks_and_full_nuisance(tmp_path):
    root=tmp_path/'personal';case=root/'maintenance/new';case.mkdir(parents=True)
    bins=root/'results/behaviour/fly_bins_10s.parquet';bins.parent.mkdir(parents=True)
    with pq.ParquetWriter(bins,pa.Table.from_pandas(frame('a','g')).schema) as writer:
        for group in 'abcde':
            for mode in ('late','early','none'):writer.write_table(pa.Table.from_pandas(frame(group+mode,group,mode=mode)))
    atomic_json(bins.with_name('streaming_bin_provenance.json'),{'bin_files':{str(bins):digest(bins)}})
    config={'behaviour':{'primary_bin_seconds':10,'learning_rates':[.0003,.001,.003],'inner_folds':3}}
    before=digest(bins)
    model,train,validation,scaler,n,settings,plan=m.prepare(s,b,profile,config,root,case,time.monotonic()+60,lambda e:None)
    census=json.loads((case/'clock-eligibility-census.json').read_text())['rows']
    assert len(census)==len(plan['roster'])==15 and digest(bins)==before
    assert set(q.animal_id for q in train)==set(plan['split']['training_ids'])==set(model.animal_ids)
    assert len(model.experiment_ids)==len(set(q.experiment_id for q in train))
    assert all(len(q.times)==75 for q in train+validation)
    assert n==sum(int(q.observed.sum()) for q in train) and n==plan['global_observed_bin_normalizer']
    assert len(plan['split']['excluded_all_missing_ids'])==5 and plan['original_validation_criterion_defined'] is False
    assert all(item['prediction_log_score'] is None for item in census)
    assert any(q.animal_id.endswith('early') for q in validation)
    assert plan['null_validation_clock_ids'] and all(k.endswith('early') for k in plan['null_validation_clock_ids'])
    assert all(plan[key] is False for key in m.FALSE_FLAGS)


def test_prospective_canary_v2_rejects_longest_ineligible_without_shortening_any_clock():
    rows=[dict(animal_id='longnull',bins=900,eligible_prediction_bins=0),
          dict(animal_id='eligible',bins=800,eligible_prediction_bins=1)]
    original=copy.deepcopy(rows);value=m.prospective_validation_canary_v2(rows,['longnull','eligible'])
    assert value['selected']=='eligible' and value['null_clock_ids']==['longnull'] and rows==original
    assert value['status']=='prospective_selection_only_not_executed'


def test_prospective_canary_v2_ties_use_only_source_ids_and_all_null_remains_unsupported():
    rows=[dict(animal_id=k,bins=90,eligible_prediction_bins=1,outcome='unused') for k in 'ab']
    a=m.prospective_validation_canary_v2(rows,['a','b'])
    assert m.prospective_validation_canary_v2([dict(r,outcome='changed') for r in reversed(rows)],['b','a'])==a
    assert m.prospective_validation_canary_v2([dict(r,eligible_prediction_bins=0) for r in rows],['a','b'])['selected'] is None


def test_deadline_before_first_gradient_does_not_clip_or_update(sample):
    model,train,scaler,validation=sample;original=copy.deepcopy(model.state_dict());commits=[]
    with pytest.raises(m.DeadlineBound):m.run_prefix(s,profile,model,train,validation,scaler,
        sum(int(q.observed.sum()) for q in train),{},time.monotonic()-1,lambda e:None,lambda *a:commits.append(a))
    assert not commits
    for name,value in original.items():torch.testing.assert_close(value,model.state_dict()[name],rtol=0,atol=0)


@pytest.mark.parametrize('bad',['scientific_flag','wrong_source','out_of_order','complete_without_all_stages'])
def test_supervisor_refuses_falsely_complete_or_unbound_worker_receipt(tmp_path,bad):
    value={'status':'bounded_deadline_reached','primary_source_sha256':m.PRIMARY_SHA,'framework_source_sha256':m.SOURCE,
        'full_config_sha256':m.CONFIG,'completed_stages':[],**m.FALSE_FLAGS}
    if bad=='scientific_flag':value['scientific_result']=True
    elif bad=='wrong_source':value['framework_source_sha256']='f'*64
    elif bad=='out_of_order':value['completed_stages']=[{'stage':m.STAGES[1]}]
    else:value['status']='complete_full_cohort_first_update_technical_prefix'
    with pytest.raises(ValueError):m.validate_worker_result(value,tmp_path)


def test_empty_honest_native_partial_receipt_remains_incomplete(tmp_path):
    value={'status':'bounded_deadline_reached','primary_source_sha256':m.PRIMARY_SHA,'framework_source_sha256':m.SOURCE,
        'full_config_sha256':m.CONFIG,'completed_stages':[],**m.FALSE_FLAGS}
    m.validate_worker_result(value,tmp_path)


def test_context_loader_refuses_unverified_code_before_any_execution(tmp_path):
    path=tmp_path/'foreign.py';marker=tmp_path/'should-not-exist'
    path.write_text('from pathlib import Path\nPath('+repr(str(marker))+').write_text("wrong")\n')
    with pytest.raises(ValueError,match='module bytes changed'):m.load_exact('unqualified',path,m.CONTEXT_SHA)
    assert not marker.exists()


def private_xml(path):
    import xml.etree.ElementTree as ET
    suite=ET.Element('testsuite',tests='2',failures='0',errors='0',skipped='0')
    for name in ('test_first','test_second'):ET.SubElement(suite,'testcase',classname='test_full_cohort_prefix_v4_20261022',name=name)
    path.write_bytes(ET.tostring(suite));return path


def test_private_junit_requires_actual_unique_named_passing_roster(tmp_path,monkeypatch):
    from horsehair.core import canonical_hash
    path=private_xml(tmp_path/'actual.xml')
    monkeypatch.setattr(m,'PRIVATE_TEST_COUNT',2)
    monkeypatch.setattr(m,'PRIVATE_TEST_ROSTER_SHA',canonical_hash([('test_full_cohort_prefix_v4_20261022',n) for n in ('test_first','test_second')]))
    assert m.verify_private_junit(path)['test_counts']['tests']==2
    path.write_text(path.read_text().replace('test_second','different_test'))
    with pytest.raises(ValueError):m.verify_private_junit(path)


def test_private_junit_suite_attributes_cannot_replace_real_testcases(tmp_path,monkeypatch):
    path=tmp_path/'false.xml';path.write_text('<testsuite tests="28" failures="0" errors="0" skipped="0"/>')
    with pytest.raises(ValueError):m.verify_private_junit(path)


def test_private_qualification_refuses_changed_test_source_before_fixture_or_process(tmp_path,monkeypatch):
    root=tmp_path/'root';path=root/'maintenance/stage/test_full_cohort_prefix_v4_20261022.py';path.parent.mkdir(parents=True);path.write_text('untrusted')
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'__file__',str(path.with_name('run-full-cohort-prefix-v4-20261022.py')))
    with pytest.raises(ValueError,match='test source'):m.qualify_private(path,'f'*64,time.time()+3600,runner=lambda *a,**k:pytest.fail('unverified source executed'))


@pytest.mark.parametrize('bad',['none','nonzero','skipped'])
def test_private_qualification_command_is_full_pinned_colocated_and_selector_free(tmp_path,monkeypatch,bad):
    from horsehair.core import canonical_hash
    root=tmp_path/'root';stage=root/'maintenance/stage';stage.mkdir(parents=True);support=root/'maintenance/support';support.mkdir()
    helper=stage/'run-full-cohort-prefix-v4-20261022.py';helper.write_text('synthetic helper source, never executed')
    tests=stage/'test_full_cohort_prefix_v4_20261022.py';tests.write_text('synthetic test source, mocked runner')
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'SUPPORT',support);monkeypatch.setattr(m,'__file__',str(helper))
    monkeypatch.setattr(m,'native_fixture_workspace',lambda:tmp_path/'native-fixtures')
    monkeypatch.setattr(m,'PRIVATE_TEST_COUNT',2)
    monkeypatch.setattr(m,'PRIVATE_TEST_ROSTER_SHA',canonical_hash([('test_full_cohort_prefix_v4_20261022',n) for n in ('test_first','test_second')]))
    monkeypatch.setenv('PYTEST_ADDOPTS','-k selected');monkeypatch.setenv('PYTEST_PLUGINS','foreign_plugin')
    calls=[]
    def runner(command,**kwargs):
        calls.append(command)
        assert command[:4]==[m.sys.executable,'-s','-m','pytest'] and command[4]==str(tests)
        assert command[5:8]==['-q','-p','no:cacheprovider']
        assert 'PYTEST_ADDOPTS' not in kwargs['env'] and 'PYTEST_PLUGINS' not in kwargs['env']
        assert kwargs['env']['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1' and 0<kwargs['timeout']<=600
        path=private_xml(support/'private-tests.xml')
        if bad=='skipped':path.write_text(path.read_text().replace(' />','><skipped/></testcase>',1))
        return subprocess.CompletedProcess(command,1 if bad=='nonzero' else 0)
    if bad=='none':
        result=m.qualify_private(tests,digest(tests),time.time()+3600,runner=runner)
        assert result['test_source']['sha256']==digest(tests) and result['helper_sha256']==digest(helper)
        assert (support/'private-qualification.json').is_file()
    else:
        with pytest.raises(ValueError):m.qualify_private(tests,digest(tests),time.time()+3600,runner=runner)
        assert not (support/'private-qualification.json').exists()
    assert len(calls)==1


def test_isolated_child_accepts_real_primary_directory_alias_before_source_verification(tmp_path):
    # Synthetic import-path fixture only. Scientific source verification is a
    # stop marker after the real package-path guard; no model computation runs.
    target=tmp_path/'actual-primary';package=target/'src/horsehair';package.mkdir(parents=True)
    for name in ('__init__.py','semimarkov.py','behaviour.py','core.py'):(package/name).write_text('# Synthetic primary path fixture\n')
    (package/'core.py').write_text('def atomic_json(*args): raise AssertionError("No fixture writes expected")\n')
    alias=tmp_path/'primary-alias'
    if os.name=='nt':
        # A real filesystem lexical alias needs no elevated Windows link
        # privilege; Linux native qualification exercises the directory symlink.
        alias=target/'..'/'actual-primary'
    else:alias.symlink_to(target,target_is_directory=True)
    program='''import importlib.util,pathlib,sys,time
alias,target,helper=map(pathlib.Path,sys.argv[1:])
sys.path.insert(0,str(alias/'src'))
spec=importlib.util.spec_from_file_location('synthetic_alias_prefix',helper)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
def verified_import_stop(primary,framework):
    assert primary==target.resolve() and framework==target.resolve()
    raise SystemExit(0)
m.source_bindings=verified_import_stop
m._worker(alias,target,target,target,time.monotonic()+60)
raise SystemExit(7)
'''
    env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'}
    result=subprocess.run([m.sys.executable,'-s','-c',program,str(alias),str(target),str(Path(m.__file__).resolve())],
        capture_output=True,text=True,timeout=45,env=env)
    assert result.returncode==0,result.stderr

