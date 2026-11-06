from pathlib import Path
import hashlib
import importlib.util
import json
import os
import subprocess
import tarfile
import xml.etree.ElementTree as ET
import pytest


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value


m=load('finite_archive_continuation',Path(__file__).with_name('archive_continuation.py'))
operations=Path(__file__).resolve().parents[2]
old_path=operations/'20261025/agent-coordination-repair-v1/operator-actions/archive-orthology-sequence-copies-20261104.py'
helper_path=operations/'20261025/agent-coordination-repair-v1/operator-actions/compact-terminal-controller-reads-20261104.py'
if not old_path.exists():old_path=Path('/scratch/pwa209/horsehair-worm/maintenance/orthology-sequence-archive-support-20261104/archive-orthology-sequence-copies-20261104.py')
if not helper_path.exists():helper_path=Path('/scratch/pwa209/horsehair-worm/maintenance/terminal-controller-diagnostic-support-v4-20261104/compact-terminal-controller-reads-20261104.py')
assert hashlib.sha256(old_path.read_bytes()).hexdigest()==m.ORIGINAL_WRAPPER_SHA
assert hashlib.sha256(helper_path.read_bytes()).hexdigest()==m.HELPER_SHA
old=load('exact_original_archiver',old_path);h=load('exact_original_archive_helper',helper_path)


@pytest.fixture
def archived(tmp_path,monkeypatch):
    root=tmp_path/'study';root.mkdir();specs=[(name,1) for name,count in old.SPECS]
    for name,count in specs:
        directory=root/name;directory.mkdir(parents=True)
        (directory/'OG0000001.fa').write_bytes(b'>generated_copy\nACDEFGHIK\n')
    original=root/'original-proteome.fa';original.write_bytes(b'>original\nACDEFGHIK\n')
    protected=root/'required-N0.tsv';protected.write_bytes(b'original mappings')
    local=tmp_path/'local';local.mkdir();monkeypatch.setenv('SLURM_TMPDIR',str(local))
    if os.name!='posix':
        monkeypatch.setattr(h,'sync_directory',lambda path:None)
        stamp=old.stamp;monkeypatch.setattr(old,'stamp',lambda info:stamp(info)[:4]+[0])
    bindings={str(protected):old.sha(protected)}
    rows=old.roster(specs,h,root=root);case=root/'results/storage-archives/old';case.parent.mkdir(parents=True)
    count=[0]
    class BeforeUnlink(Exception):pass
    def boundary():
        count[0]+=1
        if count[0]==2:raise BeforeUnlink('original native deadline before old removal journal')
    with pytest.raises(BeforeUnlink):old.pack_and_remove(case,rows,specs,{'domain':'offline_fixture'},h,bindings,
                                                       root=root,boundary=boundary)
    assert not (case/'removal.json').exists() and all((root/row['path']).exists() for row in rows)
    for name in ['receipt.json','index.json','intent.json']:bindings[str(case/name)]=old.sha(case/name)
    receipt=json.loads((case/'receipt.json').read_bytes());before={name:old.sha(case/name) for name in ['sequences.tar','receipt.json','index.json','intent.json']}
    return root,specs,rows,case,bindings,receipt,before


def run(case,**kwargs):
    root,specs,rows,directory,bindings,receipt,before=case
    return m.continue_removal(directory,directory/'continuation',rows,specs,receipt['archive_sha256'],
        {'domain':'offline_continuation_fixture'},old,h,bindings,root=root,**kwargs)


def test_existing_tar_roundtrip_and_removal_preserve_all_old_proofs_and_original_inputs(archived):
    root,specs,rows,case,bindings,receipt,before=archived
    proof,journal=run(archived)
    assert proof['status']=='independent_existing_tar_roundtrip_verified_originals_retained'
    assert journal['status']=='complete_verified_existing_orthology_archive_removal' and journal['removed_count']==4
    assert journal['archive_sha256']==receipt['archive_sha256']
    assert not any((root/row['path']).exists() for row in rows)
    assert all((root/name).is_dir() for name,count in specs)
    assert all(old.sha(case/name)==digest for name,digest in before.items())
    assert not (case/'removal.json').exists() and not (case/'result.json').exists()
    assert (root/'original-proteome.fa').exists() and (root/'required-N0.tsv').read_bytes()==b'original mappings'


def test_changed_or_corrupted_tar_cannot_unlink_any_original(archived):
    root,specs,rows,case,bindings,receipt,before=archived
    with (case/'sequences.tar').open('r+b') as stream:stream.write(b'corrupted existing archive')
    with pytest.raises((ValueError,tarfile.TarError)):run(archived)
    assert all((root/row['path']).exists() for row in rows)
    assert not (case/'continuation/removal.json').exists()


def test_changed_original_after_archive_verification_stops_before_first_unlink(archived,monkeypatch):
    root,specs,rows,case,bindings,receipt,before=archived;real=h.verify_tar
    def mutate(*args,**kwargs):
        real(*args,**kwargs);(root/rows[-1]['path']).write_bytes(b'recently changed original copy')
    monkeypatch.setattr(h,'verify_tar',mutate)
    with pytest.raises(ValueError,match='Original changed'):run(archived)
    assert all((root/row['path']).exists() for row in rows) and not (case/'continuation/removal.json').exists()


def test_census_boundary_failure_prevents_any_mutation(archived):
    root,specs,rows,case,bindings,receipt,before=archived
    def blocked():raise ValueError('census running')
    with pytest.raises(ValueError,match='census running'):run(archived,boundary=blocked)
    assert not (case/'continuation').exists() and all((root/row['path']).exists() for row in rows)


def test_census_boundary_rechecked_before_first_unlink(archived):
    root,specs,rows,case,bindings,receipt,before=archived;calls=[0]
    def blocked():
        calls[0]+=1
        if calls[0]==2:raise ValueError('census released')
    with pytest.raises(ValueError,match='census released'):run(archived,boundary=blocked)
    assert all((root/row['path']).exists() for row in rows) and not (case/'continuation/removal.json').exists()


def test_deadline_or_proof_failure_before_full_original_check_preserves_all(archived):
    root,specs,rows,case,bindings,receipt,before=archived;calls=[0]
    def cutoff():
        calls[0]+=1
        if calls[0]==6:raise TimeoutError('finite deadline')
    with pytest.raises(TimeoutError,match='finite deadline'):run(archived,checkpoint=cutoff)
    assert all((root/row['path']).exists() for row in rows) and not (case/'continuation/removal.json').exists()


def test_partial_unlink_is_journaled_and_never_blindly_replayed(archived):
    root,specs,rows,case,bindings,receipt,before=archived
    def fail(path):raise RuntimeError('stop after first actual unlink')
    with pytest.raises(RuntimeError):run(archived,after_unlink=fail)
    journal=json.loads((case/'continuation/removal.json').read_bytes())
    assert journal['status']=='interrupted_explicit_reconciliation_required' and journal['removed_count']==1
    assert sum((root/row['path']).exists() for row in rows)==3
    with pytest.raises(ValueError,match='never replay'):run(archived)


@pytest.mark.parametrize('name',['removal.json','result.json'])
def test_existing_original_removal_or_result_refuses_continuation(archived,name):
    root,specs,rows,case,bindings,receipt,before=archived;(case/name).write_text('{}')
    with pytest.raises(ValueError,match='Original archive removal'):run(archived)
    assert all((root/row['path']).exists() for row in rows)


def test_changed_protected_mapping_stops_every_unlink(archived):
    root,specs,rows,case,bindings,receipt,before=archived;(root/'required-N0.tsv').write_bytes(b'changed mapping')
    with pytest.raises(ValueError,match='Bound'):run(archived)
    assert all((root/row['path']).exists() for row in rows)


def test_appearing_original_journal_during_roundtrip_stops_every_unlink(archived,monkeypatch):
    root,specs,rows,case,bindings,receipt,before=archived;real=h.verify_tar
    def conflicting(*args,**kwargs):real(*args,**kwargs);(case/'removal.json').write_text('{}')
    monkeypatch.setattr(h,'verify_tar',conflicting)
    with pytest.raises(ValueError,match='Another original removal'):run(archived)
    assert all((root/row['path']).exists() for row in rows)


def native_row():
    return '8510943|pwa209|def-ptewarie_cpu|FAILED|1:0|10505|1|cpu=1,mem=8G|hhw-orthology-sequence-archive-20261104\n'


def test_actual_failed_original_native_is_required_and_not_changed_to_success():
    proof=m.failed_original_native(native_row())
    assert proof['state']=='FAILED' and proof['exit_code']=='1:0' and proof['elapsed_seconds']==10505


@pytest.mark.parametrize('old,new',[('8510943','9999999'),('pwa209','other'),('FAILED','COMPLETED'),
    ('1:0','0:0'),('10505','1'),('cpu=1,mem=8G','cpu=1,gres/gpu=1'),
    ('hhw-orthology-sequence-archive-20261104','wrong-name')])
def test_wrong_old_native_proof_refused(old,new):
    with pytest.raises(ValueError):m.failed_original_native(native_row().replace(old,new))


@pytest.fixture
def writer_gate(tmp_path):
    root=tmp_path/'study';root.mkdir();(root/'state').mkdir()
    gate={'terminal':{'6983037':{'state':'COMPLETED','exit_code':'0:0'},'6983056':{'state':'COMPLETED','exit_code':'0:0'},
                      '6983059':{'state':'FAILED','exit_code':'1:0'}},
          'held':[]}
    area=root/m.POLICY_NAMESPACE/'census';area.mkdir(parents=True)
    helper=area/'run-fresh-storage-census-resilience-v1-20261024.py';helper.write_text('# reviewed offline provider fixture\n')
    for name in ['storage_policy.py','policy.json']:
        (area.parent/name).write_bytes(Path(__file__).with_name(name).read_bytes())
    policy={'module':{'path':str(area.parent/'storage_policy.py'),'sha256':m.SHARED_MODULE_SHA},
            'policy':{'path':str(area.parent/'policy.json'),'sha256':m.SHARED_POLICY_SHA}}
    manifest=area/'source-manifest.json';manifest.write_text(json.dumps({'files':{helper.name:m.sha(helper)},'operational_storage_policy':policy}))
    queue_rows=[]
    for kind,phase in [('old_provider','study-census-resilience-v1-20261024'),('new_provider',m.NEW_PHASE)]:
        directory=root/'plans'/phase;directory.mkdir(parents=True);tasks=[];ledger_tasks={}
        for index in range(1,65):
            name='rolling-storage-census-'+(str(index).zfill(2) if kind=='new_provider' else str(index))
            script=directory/(name+'.sbatch');script.write_text('#!/bin/bash\n# '+kind+' '+name+'\n')
            ident=(str(910000+index) if kind=='new_provider' else
                   {61:'6983037',62:'6983056',63:'6983059',64:'6983060'}.get(index,str(610000+index)))
            provider=({'helper':{'path':str(helper),'sha256':m.sha(helper)},
                      'source_manifest':{'path':str(manifest),'sha256':m.sha(manifest)}} if kind=='new_provider' else {})
            task={'name':name,'script':str(script),'script_sha256':m.canonical(script.read_text()),
                  'resources':{'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None},
                  'rolling_census_provider':provider}
            tasks.append(task);ledger_tasks[name]={'state':'submitted','job_id':ident,'script_sha256':task['script_sha256'],
                                                 'command':['sbatch',str(script)]}
            if kind=='new_provider' or index==64:
                gate['held'].append({'job_id':ident,'script':{'path':str(script),'sha256':m.sha(script)},
                                     'cpus':8,'memory':'64G','hours':6,'job_name':'hhw-'+name})
                queue_rows.append(ident+'|pwa209|def-ptewarie_cpu|PENDING|JobHeldUser|hhw-'+name+'|8|64G|6:00:00|'+str(script)+'|N/A')
        plan={'root':str(root),'phase':phase,'release_identity':{'sha256':m.FRAMEWORK_SOURCE},
              'config_sha256':m.BASE_CONFIG,'tasks':tasks}
        plan_path=directory/'plan.json';plan_path.write_text(json.dumps(plan))
        ledger={'source_sha256':m.FRAMEWORK_SOURCE,'config_sha256':m.BASE_CONFIG,'plan_sha256':m.canonical(plan),'tasks':ledger_tasks}
        ledger_path=root/'state'/(phase+'-submission.json');ledger_path.write_text(json.dumps(ledger))
        gate[kind]={'plan':{'path':str(plan_path),'sha256':m.sha(plan_path)},'ledger_path':str(ledger_path)}
    queue='\n'.join(queue_rows)+'\n'
    accounting='\n'.join(f'{ident}|pwa209|def-ptewarie_cpu|{row["state"]}|{row["exit_code"]}|8|10|cpu=8,mem=64G'
                         for ident,row in gate['terminal'].items())+'\n'
    return root,gate,queue,accounting


def gate_runner(queue,accounting):
    def run(command,**kwargs):
        assert command[0] in {'squeue','sacct'}
        return subprocess.CompletedProcess(command,0,queue if command[0]=='squeue' else accounting,'')
    return run


def test_actual_writer_boundary_requires_owned_held_or_terminal_roster(writer_gate):
    root,gate,queue,accounting=writer_gate
    proof=m.census_gate(gate,h,gate_runner(queue,accounting),root=root)
    assert len(proof['held_job_ids'])==65 and '6983060' in proof['held_job_ids']
    assert proof['status']=='all_declared_census_writers_terminal_or_held'


@pytest.mark.parametrize('old,new',[('JobHeldUser','Dependency'),('PENDING','RUNNING'),('pwa209','other'),
    ('64G','32G'),('6:00:00','12:00:00'),('|N/A','|gpu:1')])
def test_released_running_wrong_owned_or_changed_writer_refused(writer_gate,old,new):
    root,gate,queue,accounting=writer_gate
    with pytest.raises(ValueError):m.census_gate(gate,h,gate_runner(queue.replace(old,new),accounting),root=root)


def test_unlisted_active_hhw_census_refused(writer_gate):
    root,gate,queue,accounting=writer_gate
    queue+='9999999|pwa209|def-ptewarie_cpu|RUNNING|None|hhw-unknown-census|8|64G|6:00:00|/script|N/A\n'
    with pytest.raises(ValueError,match='still active'):m.census_gate(gate,h,gate_runner(queue,accounting),root=root)


def test_terminal_writer_proof_cannot_be_missing_or_fabricated(writer_gate):
    root,gate,queue,accounting=writer_gate
    with pytest.raises(ValueError):m.census_gate(gate,h,gate_runner(queue,accounting.replace('6983059','123')),root=root)


@pytest.mark.parametrize('change',['missing_writer','duplicate_writer','wrong_job','wrong_plan_pin'])
def test_full_new_plan_and_every_held_writer_are_bound(writer_gate,change):
    root,gate,queue,accounting=writer_gate
    if change=='missing_writer':gate['held'].pop()
    elif change=='duplicate_writer':gate['held'][-1]=gate['held'][0]
    elif change=='wrong_job':gate['held'][-1]={**gate['held'][-1],'job_id':'9999999'}
    else:gate['new_provider']['plan']['sha256']='0'*64
    with pytest.raises(ValueError):m.census_roster_authority(gate,root=root)


@pytest.fixture
def native_test_contract(tmp_path):
    root=tmp_path/'study';parent=root/m.POLICY_NAMESPACE;parent.mkdir(parents=True)
    names=['storage_policy.py','policy.json','test_storage_policy.py','archive_continuation.py','test_archive_continuation.py']
    sources={}
    for name in names:
        path=parent/name;path.write_bytes(Path(__file__).with_name(name).read_bytes());sources[str(path)]=m.sha(path)
    area=parent/'native-12345';area.mkdir();junit=area/'tests.xml'
    suite=ET.Element('testsuite',{'tests':'67','failures':'0','errors':'0','skipped':'0'})
    roster=[]
    for index in range(67):
        name='offline_case_'+str(index)
        ET.SubElement(suite,'testcase',{'classname':'test_storage_policy','name':name})
        roster.append('test_storage_policy::'+name)
    ET.ElementTree(suite).write(junit,encoding='utf-8')
    contract={'sources':sources,'tests':67,'roster_sha256':m.canonical(sorted(roster))}
    return root,parent,junit,contract


def test_native_source_and_exact_junit_roster_are_verified(native_test_contract):
    root,parent,junit,contract=native_test_contract
    proof=m.verify_native_tests(contract,'12345',root=root,source=parent/'archive_continuation.py')
    assert proof['tests']==67 and proof['junit']['sha256']==m.sha(junit)


@pytest.mark.parametrize('change',['skip','failure','wrong_roster','wrong_source','missing_source'])
def test_native_fail_skip_wrong_roster_or_unpinned_sources_refuse_mutation(native_test_contract,change):
    root,parent,junit,contract=native_test_contract
    if change in {'skip','failure'}:
        node=ET.parse(junit);node.getroot().set('skipped' if change=='skip' else 'failures','1');node.write(junit,encoding='utf-8')
    elif change=='wrong_roster':contract['roster_sha256']='0'*64
    elif change=='wrong_source':(parent/'test_storage_policy.py').write_text('changed')
    else:contract['sources'].pop(str(parent/'test_storage_policy.py'))
    with pytest.raises(ValueError):m.verify_native_tests(contract,'12345',root=root,source=parent/'archive_continuation.py')
