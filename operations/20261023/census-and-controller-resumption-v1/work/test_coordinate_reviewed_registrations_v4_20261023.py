"""Finite coordinator contracts on disposable plans; no native submissions."""
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
import argparse
import ast
import base64
import copy
import hashlib
import importlib.util
import json
import os
import subprocess
import zlib
from contextlib import contextmanager

import pytest
from horsehair.core import canonical_hash,digest,read_json

SOURCE=Path(__file__).with_name('coordinate-reviewed-registrations-v4-20261023.py')
spec=importlib.util.spec_from_file_location('finite_coordinator',SOURCE);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def test_v2_changes_only_namespace_and_actual_q_registration_literals():
    prior=SOURCE.with_name('coordinate-reviewed-registrations-v2-20261023.py')
    original=SOURCE.with_name('coordinate-reviewed-registrations-v1-20261023.py')
    assert digest(original)=='efe0c359f8042d377b0bdad12c2785041b3d4140a781945dc97245723a34603c'
    replacements={'coordinate-reviewed-registrations-v1-20261023.py':'coordinate-reviewed-registrations-v2-20261023.py',
        'study-af-reviewed-registration-coordinator-v1-20261023':'study-af-reviewed-registration-coordinator-v2-20261023',
        'study-af-rolling-census-qualification-v2-20261023':'study-af-rolling-census-qualification-v3-20261023',
        'study-af-rolling-census-support-v1-20261023':'study-af-rolling-census-support-v2-20261023',
        'recovery-support-20261021-428':'recovery-support-20261021-444','recovery-428':'recovery-444','Q428':'Q444'}
    class Rebind(ast.NodeTransformer):
        def visit_Constant(self,node):
            if isinstance(node.value,str):
                for before,after in replacements.items():node.value=node.value.replace(before,after)
            return node
    expected=Rebind().visit(ast.parse(original.read_text()))
    assert ast.dump(expected,include_attributes=False)==ast.dump(ast.parse(prior.read_text()),include_attributes=False)

def write(path,value):path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value));return path

def test_v3_changes_only_namespace_and_q452_literals_from_frozen_v2():
    original=SOURCE.with_name('coordinate-reviewed-registrations-v2-20261023.py')
    assert digest(original)=='80dc25ad270ee0b19c0f68f5e66c0ef8f4a32db2722ea832ce6fee1dde781f13'
    replacements={'coordinate-reviewed-registrations-v2-20261023.py':'coordinate-reviewed-registrations-v3-20261023.py',
        'study-af-reviewed-registration-coordinator-v2-20261023':'study-af-reviewed-registration-coordinator-v3-20261023',
        'study-af-rolling-census-qualification-v3-20261023':'study-af-rolling-census-qualification-v4-20261023',
        'study-af-rolling-census-support-v2-20261023':'study-af-rolling-census-support-v3-20261023',
        'recovery-support-20261021-444':'recovery-support-20261021-452','recovery-444':'recovery-452','Q444':'Q452'}
    class Rebind(ast.NodeTransformer):
        def visit_Constant(self,node):
            if isinstance(node.value,str):
                for before,after in replacements.items():node.value=node.value.replace(before,after)
            return node
    assert ast.dump(Rebind().visit(ast.parse(original.read_text())),include_attributes=False)==ast.dump(ast.parse(SOURCE.with_name('coordinate-reviewed-registrations-v3-20261023.py').read_text()),include_attributes=False)

def shell(ident):
    p={'id':ident,'helper_sha256':'a'*64,'fixture_entropy':''.join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(8))}
    blob=base64.b64encode(zlib.compress(json.dumps(p).encode())).decode()
    return ("#!/bin/bash\npython -u - <<'PY'\nimport json,zlib,base64\npayload=json.loads(zlib.decompress(base64.b64decode("+repr(blob)+")))\npass\nPY\n").encode()

@pytest.fixture
def frozen(tmp_path,monkeypatch):
    root=tmp_path/'study';area=root/'maintenance/coordinator';area.mkdir(parents=True);qarea=root/'maintenance/qsource';qarea.mkdir()
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'AREA',area);monkeypatch.setattr(m,'CASE',area/'case');m.CASE.mkdir()
    monkeypatch.setattr(m,'QSUPPORT',qarea);monkeypatch.setattr(m,'QPATH',root/'maintenance/qresult/qualification.json')
    helper=area/m.HELPER;helper.write_bytes(SOURCE.read_bytes());monkeypatch.setattr(m,'__file__',str(helper))
    for ident,name in m.SHELLS.items():(area/name).write_bytes(shell(ident))
    validator=qarea/'run-fresh-storage-census-v6-20261023.py';validator.write_text('MAX_SLOTS=64\n')
    qmanifest=write(qarea/'source-manifest.json',{'frozen':'q2'})
    q={'path':str(m.QPATH),'plan':str(root/'plans/recovery-support-20261021-456/plan.json'),'ledger':str(root/'state/recovery-support-20261021-456-submission.json'),'task':'recovery-456','validator':{'path':str(validator),'sha256':digest(validator),'function':'kernel_qualification'},'source_manifest':{'path':str(qmanifest),'sha256':digest(qmanifest)}}
    value={'kind':'finite-reviewed-registration-coordinator-source-manifest-v1','files':{name:digest(area/name) for name in (m.HELPER,*m.SHELLS.values())},'qualification':q}
    path=write(area/'coordinator-source-manifest.json',value)
    return SimpleNamespace(root=root,area=area,manifest=value,path=path,helper=helper)

def test_complete_exact_eight_source_manifest_and_new_q2_contract(frozen):
    value,pins=m.source_manifest(frozen.path,digest(frozen.path))
    assert value==frozen.manifest and len(pins)==10 and len(value['files'])==8

@pytest.mark.parametrize('change',['own','child','missing','extra','old_q','old_validator','duplicate_key','bad_hash'])
def test_source_manifest_refuses_mutation_or_old_lineage(frozen,change):
    value=frozen.manifest
    if change=='own':frozen.helper.write_text('changed')
    elif change=='child':(frozen.area/m.SHELLS['429']).write_text('changed')
    elif change=='missing':value['files'].pop(m.SHELLS['429']);write(frozen.path,value)
    elif change=='extra':value['files']['unreviewed.sh']='a'*64;write(frozen.path,value)
    elif change=='old_q':value['qualification']['path']=str(m.QPATH.parent/'old-Q1.json');write(frozen.path,value)
    elif change=='old_validator':value['qualification']['validator']['path']=str(frozen.area/'v5.py');write(frozen.path,value)
    elif change=='duplicate_key':frozen.path.write_text('{"kind":1,"kind":2}')
    else:value['qualification']['validator']['sha256']='b'*64;write(frozen.path,value)
    with pytest.raises(ValueError):m.source_manifest(frozen.path,digest(frozen.path))

def own_pair(frozen,argv):
    f=frozen;directory=f.root/'plans/recovery-support-20261021-425';script=directory/'recovery-425.sbatch';script.parent.mkdir(parents=True)
    command=__import__('shlex').join(['python','-u',str(f.helper),*argv]);script.write_text('#!/bin/bash\n'+command+'\n')
    task={'name':'recovery-425','script':str(script),'command':command,'script_sha256':canonical_hash(script.read_text()),'resources':{'cpus':1,'memory':'16G','hours':4,'gpu':False,'array':None},'depends':[],'external_dependencies':['588'],'dependency_kind':'afterok','reviewed_registration_coordinator':{'helper':{'path':str(f.helper),'sha256':digest(f.helper)},'source_manifest':{'path':str(f.path),'sha256':digest(f.path)}}}
    plan={'root':str(f.root),'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]}
    ledger={'config_sha256':m.CONFIG,'source_sha256':m.SOURCE,'plan_sha256':canonical_hash(plan),'tasks':{'recovery-425':{'job_id':'599','state':'submitted','script_sha256':task['script_sha256']}}}
    args=argparse.Namespace(native_plan=directory/'plan.json',native_ledger=f.root/'state/recovery-support-20261021-425-submission.json',native_script=script,native_task='recovery-425',source_manifest=f.path)
    write(args.native_plan,plan);write(args.native_ledger,ledger)
    return args,plan,ledger

def test_actual_own_registration_binds_all_args_helper_manifest_native_and_afterok(frozen):
    argv=['--source-manifest',str(frozen.path)];args,_,_=own_pair(frozen,argv)
    assert m.own_registration(args,'599',argv,digest(frozen.path),'588')['native_job_id']=='599'

@pytest.mark.parametrize('change',['job','argv','helper_sha','manifest_sha','afterok','dependency','resources','script_hash','source'])
def test_own_registration_refuses_identity_or_source_mutation(frozen,change):
    argv=['--source-manifest',str(frozen.path)];args,plan,ledger=own_pair(frozen,argv);task=plan['tasks'][0]
    if change=='job':ledger['tasks']['recovery-425']['job_id']='600'
    elif change=='argv':argv.append('--unregistered')
    elif change=='helper_sha':task['reviewed_registration_coordinator']['helper']['sha256']='b'*64
    elif change=='manifest_sha':task['reviewed_registration_coordinator']['source_manifest']['sha256']='b'*64
    elif change=='afterok':task['dependency_kind']='afterany'
    elif change=='dependency':task['external_dependencies']=['old-failed-Q1']
    elif change=='resources':task['resources']['cpus']=2
    elif change=='script_hash':task['script_sha256']='b'*64
    else:plan['release_identity']['sha256']='b'*64;ledger['source_sha256']='b'*64
    ledger['plan_sha256']=canonical_hash(plan);write(args.native_plan,plan);write(args.native_ledger,ledger)
    with pytest.raises(ValueError):m.own_registration(args,'599',argv,digest(frozen.path),'588')

def allocation(monkeypatch):
    end=5000;monkeypatch.setenv('SLURM_JOB_END_TIME',str(end));script=Path('/immutable/recovery-425.sbatch')
    fields={'JobId':'599','UserId':'pwa209(3164768)','Account':'def-ptewarie_cpu','JobState':'RUNNING','NumCPUs':'1','NumNodes':'1','TimeLimit':'04:00:00','MinMemoryNode':'16G','Command':str(script),'JobName':'hhw-recovery-425','ReqTRES':'cpu=1,mem=16G,node=1','AllocTRES':'cpu=1,mem=16G,node=1','EndTime':datetime.fromtimestamp(end,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}
    def reader(*a):return subprocess.CompletedProcess([],0,' '.join(k+'='+v for k,v in fields.items()),'')
    return fields,script,reader

def test_actual_native_owned_scalar_cpu_context_and_finite_end(monkeypatch):
    fields,script,reader=allocation(monkeypatch)
    assert m.native_allocation('599',script,reader,wall=lambda:1000)[1]==5000

@pytest.mark.parametrize('key,value',[('Socks/Node','*'),('NtasksPerN:B:S:C','0:0:*:*')])
def test_native_slash_and_colon_keys_do_not_contaminate_adjacent_values(monkeypatch,key,value):
    fields,script,reader=allocation(monkeypatch);fields[key]=value
    result,_=m.native_allocation('599',script,reader,wall=lambda:1000)
    assert result['fields'][key]==value and result['fields']['EndTime']==fields['EndTime']

@pytest.mark.parametrize('key,value',[('JobId','600'),('UserId','foreign(1)'),('Account','gpu-account'),('JobState','PENDING'),('NumCPUs','2'),('NumNodes','2'),('TimeLimit','UNLIMITED'),('MinMemoryNode','8G'),('Command','/wrong'),('JobName','hhw-wrong'),('AllocTRES','cpu=1,mem=16G,node=1,gres/gpu=1'),('AllocTRES','(null)'),('ReqTRES','cpu=1,mem=16G,node=2'),('ArrayJobId','599'),('ArrayTaskId','1')])
def test_native_context_mutations_are_refused(monkeypatch,key,value):
    fields,script,reader=allocation(monkeypatch);fields[key]=value
    with pytest.raises(ValueError):m.native_allocation('599',script,reader,wall=lambda:1000)

@pytest.mark.parametrize('remaining,expected',[(4000,1800),(1000,820),(780,600)])
def test_child_timeout_is_finite_and_keeps_finalization_margin(remaining,expected):
    assert m.child_timeout(1000+remaining,wall=lambda:1000)==expected

@pytest.mark.parametrize('end',[1000,1700,float('nan'),float('inf')])
def test_insufficient_or_nonfinite_child_margin_refused(end):
    with pytest.raises(ValueError):m.child_timeout(end,wall=lambda:1000)

def test_frozen_child_payload_decoded_without_executing_it(frozen):
    path=frozen.area/m.SHELLS['429'];assert m.decode_child(path,digest(path),'429')['id']=='429'

@pytest.mark.parametrize('change',['bytes','id','crlf'])
def test_child_source_refuses_wrong_bytes_identity_or_line_endings(frozen,change):
    path=frozen.area/m.SHELLS['429'];expected=digest(path)
    if change=='bytes':path.write_bytes(path.read_bytes()+b'#changed')
    elif change=='crlf':path.write_bytes(path.read_bytes().replace(b'\n',b'\r\n'));expected=digest(path)
    with pytest.raises(ValueError):m.decode_child(path,expected,'430' if change=='id' else '429')

@pytest.mark.parametrize('lines',[[],['{}','{}']])
def test_missing_or_duplicate_submission_line_is_ambiguous(tmp_path,lines):
    path=tmp_path/'stdout';path.write_text('\n'.join('RECOVERY_STAGE_SUBMITTED '+x for x in lines))
    with pytest.raises(ValueError):m.submission_line(path)

def test_one_valid_submission_line_is_parsed_with_other_output(tmp_path):
    path=tmp_path/'stdout';path.write_text('progress\nRECOVERY_STAGE_SUBMITTED {"request_id":"429"}\n')
    assert m.submission_line(path)=={'request_id':'429'}

def test_child_executes_exactly_once_with_inherited_environment_and_immutable_evidence(frozen,monkeypatch):
    path=frozen.area/m.SHELLS['429'];calls=[];monkeypatch.setenv('QUALIFIED_MODULE_PATH','preserved');monkeypatch.setattr(m,'child_timeout',lambda end:600)
    def runner(command,**kwargs):
        calls.append((command,kwargs));assert kwargs['env']['QUALIFIED_MODULE_PATH']=='preserved'
        kwargs['stdout'].write(b'RECOVERY_STAGE_SUBMITTED {"request_id":"429"}\n');return SimpleNamespace(returncode=0)
    result=m.run_child('429',path,digest(path),{},9000,lambda *a:{'actual_plan':True},runner=runner)
    assert len(calls)==1 and calls[0][0]==['bash',str(path)] and result['registration']=={'actual_plan':True}
    assert (m.CASE/'child-429-intent.json').exists() and (m.CASE/'child-429-completion.json').exists()
    with pytest.raises(FileExistsError):m.run_child('429',path,digest(path),{},9000,lambda *a:{},runner=runner)
    assert len(calls)==1

@pytest.mark.parametrize('failure',['timeout','returncode','missing_line','postverification'])
def test_ambiguous_child_never_retried_and_preserves_failure(frozen,monkeypatch,failure):
    path=frozen.area/m.SHELLS['429'];calls=[];monkeypatch.setattr(m,'child_timeout',lambda end:600)
    def runner(command,**kwargs):
        calls.append(command)
        if failure=='timeout':raise subprocess.TimeoutExpired(command,600)
        if failure!='missing_line':kwargs['stdout'].write(b'RECOVERY_STAGE_SUBMITTED {"request_id":"429"}\n')
        return SimpleNamespace(returncode=1 if failure=='returncode' else 0)
    def verify(*a):
        if failure=='postverification':raise ValueError('Actual ledger is ambiguous')
        return {}
    with pytest.raises((ValueError,subprocess.TimeoutExpired)):m.run_child('429',path,digest(path),{},9000,verify,runner=runner)
    assert len(calls)==1 and not (m.CASE/'child-429-completion.json').exists()
    evidence=read_json(m.CASE/'child-429-failure.json');assert evidence['registration_may_have_occurred'] and evidence['automatic_retry_permitted'] is False

def test_main_order_requires_own_native_and_positive_q_before_any_child():
    source=SOURCE.read_text();main=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='main');text=ast.get_source_segment(source,main)
    assert text.index('own_registration(')<text.index('native_allocation(')<text.index('proof=validator[')<text.index('run_child(')
    assert text.index('Every child plan/ledger must be absent')<text.index('proof=validator[')
    assert 'retry' not in [n.func.id for n in ast.walk(main) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)]
    assert m.IDS==('429','430','431','432','433','434','451')

@pytest.fixture
def submitted_child(frozen,monkeypatch):
    monkeypatch.setattr(m,'source_identity',lambda path:{'sha256':m.SOURCE})
    root=frozen.root;helper=frozen.area/'reviewed-helper.py';helper.write_text('pass\n')
    payload={'id':'430','area':str(frozen.area),'helper':'reviewed-helper.py','helper_sha256':digest(helper),'canonical_helper':str(helper),'args':['--native-task','recovery-430'],'cpus':4,'memory':'16G','hours':1}
    directory=root/'plans/recovery-support-20261021-430';directory.mkdir(parents=True);script=directory/'recovery-430.sbatch'
    command=__import__('shlex').join(['python','-u',str(helper),*payload['args']]);script.write_text('#!/bin/bash\n'+command+'\n')
    task={'name':'recovery-430','script':str(script),'script_sha256':canonical_hash(script.read_text()),'command':command,'resources':{'cpus':4,'memory':'16G','hours':1,'gpu':False},'depends':[],'dependency_kind':'afterok','external_dependencies':['688']}
    plan={'root':str(root),'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[task]}
    ledger={'config_sha256':m.CONFIG,'source_sha256':m.SOURCE,'plan_sha256':canonical_hash(plan),'tasks':{'recovery-430':{'job_id':'699','state':'submitted','script_sha256':task['script_sha256']}}}
    write(directory/'plan.json',plan);write(root/'state/recovery-support-20261021-430-submission.json',ledger)
    cplan={'root':str(root),'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':[]}
    cledger={'config_sha256':m.CONFIG,'source_sha256':m.SOURCE,'plan_sha256':canonical_hash(cplan),'tasks':{'rolling-storage-census-01':{'job_id':'688'}}}
    write(root/'plans/recovery-support-20261021-429/plan.json',cplan);write(root/'state/recovery-support-20261021-429-submission.json',cledger)
    reported={'request_id':'430','area':payload['area'],'helper_sha256':payload['helper_sha256'],'tasks':{'recovery-430':{'job_id':'699','state':'submitted'}}}
    return SimpleNamespace(root=root,payload=payload,plan=plan,ledger=ledger,reported=reported,directory=directory,script=script,helper=helper)

def test_success_requires_actual_child_immutable_plan_ledger_script_and_dependency(submitted_child):
    f=submitted_child;proof=m.post_registration('430',f.payload,f.reported,'677','a'*64,{}, {})
    assert proof['native_jobs']==['699'] and proof['plan']['sha256']==digest(f.directory/'plan.json')

@pytest.mark.parametrize('change',['report_job','extra_task','script','helper','resources','args','dependency','afterany','source_pair','framework'])
def test_actual_child_postverification_refuses_mutation(submitted_child,change,monkeypatch):
    f=submitted_child;task=f.plan['tasks'][0]
    if change=='report_job':f.reported['tasks']['recovery-430']['job_id']='700'
    elif change=='extra_task':f.plan['tasks'].append(copy.deepcopy(task))
    elif change=='script':f.script.write_text(f.script.read_text()+'# changed\n')
    elif change=='helper':f.helper.write_text('changed')
    elif change=='resources':task['resources']['cpus']=5
    elif change=='args':f.payload['args'].append('--not-registered')
    elif change=='dependency':task['external_dependencies']=['old-cancelled-census']
    elif change=='afterany':task['dependency_kind']='afterany'
    elif change=='source_pair':f.ledger['source_sha256']='b'*64
    else:monkeypatch.setattr(m,'source_identity',lambda path:{'sha256':'b'*64})
    f.ledger['plan_sha256']=canonical_hash(f.plan);write(f.directory/'plan.json',f.plan);write(f.root/'state/recovery-support-20261021-430-submission.json',f.ledger)
    with pytest.raises(ValueError):m.post_registration('430',f.payload,f.reported,'677','a'*64,{}, {})

@pytest.fixture
def submitted_provider(frozen,monkeypatch):
    monkeypatch.setattr(m,'source_identity',lambda path:{'sha256':m.SOURCE})
    f=frozen;helper=f.area/'qualified-v6.py';helper.write_text('MAX_SLOTS=64\n');directory=f.root/'plans/recovery-support-20261021-429';directory.mkdir(parents=True)
    ledger_path=f.root/'state/recovery-support-20261021-429-submission.json';tasks=[];records={};calls=[]
    for index in range(1,65):
        name='rolling-storage-census-'+str(index).zfill(2);script=directory/(name+'.sbatch')
        args=['--case-id','rolling-'+str(index).zfill(2),'--native-plan',str(directory/'plan.json'),'--native-ledger',str(ledger_path),'--native-script',str(script),'--native-task',name,'--source-manifest',str(f.path),'--source-manifest-sha256',digest(f.path),'--kernel-qualification',str(m.QPATH),'--kernel-qualification-sha256','a'*64,'--kernel-qualification-job-id','677','--registry-qualification',str(f.root/'old-registry.json')]
        command=__import__('shlex').join(['python','-u',str(helper),*args]);script.write_text('#!/bin/bash\n#SBATCH --hold\n'+command+'\n')
        tasks.append({'name':name,'script':str(script),'script_sha256':canonical_hash(script.read_text()),'command':command,'resources':{'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None}})
        records[name]={'job_id':str(700+index),'state':'submitted','script_sha256':tasks[-1]['script_sha256']}
    plan={'root':str(f.root),'config_sha256':m.CONFIG,'release_identity':{'sha256':m.SOURCE},'tasks':tasks}
    ledger={'config_sha256':m.CONFIG,'source_sha256':m.SOURCE,'plan_sha256':canonical_hash(plan),'tasks':records}
    write(directory/'plan.json',plan);write(ledger_path,ledger)
    payload={'id':'429','area':str(f.area),'helper_sha256':digest(helper)}
    reported={'request_id':'429','area':str(f.area),'helper_sha256':digest(helper),'tasks':{k:{'job_id':v['job_id'],'state':'submitted'} for k,v in records.items()}}
    def verify(args,job,config):
        calls.append((args,job,config));assert args.native_task=='rolling-storage-census-01' and args.case_id=='rolling-01' and job=='701'
        assert isinstance(args.native_plan,Path) and args.kernel_qualification_job_id=='677';return {'slot_count':64}
    validator={'__file__':str(helper),'MAX_SLOTS':64,'provider_registration':verify}
    return SimpleNamespace(plan=plan,ledger=ledger,path=directory/'plan.json',ledger_path=ledger_path,payload=payload,reported=reported,validator=validator,calls=calls)

def test_all64_provider_registrations_delegate_to_actual_qualified_contract(submitted_provider):
    f=submitted_provider;result=m.post_registration('429',f.payload,f.reported,'677','a'*64,f.validator,{'config':'frozen'})
    assert len(result['native_jobs'])==64 and len(f.calls)==1 and result['provider_registration']=={'slot_count':64}

@pytest.mark.parametrize('change',['missing_slot','duplicate_native_job','wrong_source_contract','provider_refusal','partial_provider_proof'])
def test_incomplete_or_invalid64_provider_chain_is_refused(submitted_provider,change):
    f=submitted_provider
    if change=='missing_slot':f.plan['tasks'].pop()
    elif change=='duplicate_native_job':
        f.ledger['tasks']['rolling-storage-census-02']['job_id']='701';f.reported['tasks']['rolling-storage-census-02']['job_id']='701'
    elif change=='wrong_source_contract':f.validator['MAX_SLOTS']=16
    elif change=='provider_refusal':
        def refuse(*a):raise ValueError('Source-bound provider contract refused')
        f.validator['provider_registration']=refuse
    else:f.validator['provider_registration']=lambda *a:{'slot_count':63}
    f.ledger['plan_sha256']=canonical_hash(f.plan);write(f.path,f.plan);write(f.ledger_path,f.ledger)
    with pytest.raises(ValueError):m.post_registration('429',f.payload,f.reported,'677','a'*64,f.validator,{})

@pytest.fixture
def producer(frozen,monkeypatch):
    f=frozen;calls=[];monkeypatch.setattr(m,'CASE',f.area/'execution')
    config={'frozen':'config'};monkeypatch.setattr(m,'CONFIG',canonical_hash(config))
    primary=f.root/'primary';framework=f.root/'framework';monkeypatch.setattr(m,'PRIMARY',primary);monkeypatch.setattr(m,'FRAMEWORK',framework)
    write(primary/'configs/study.json',config);write(framework/'configs/study.json',config);write(primary/'release.json',{'release_id':'frozen-release'})
    import horsehair
    from horsehair import campaign,repair
    monkeypatch.setattr(horsehair,'__file__',str(framework/'src/horsehair/__init__.py'))
    monkeypatch.setattr(m,'verify_sources',lambda pins:calls.append('source-verification'))
    monkeypatch.setattr(m,'native_job_id',lambda:'599');monkeypatch.setattr(m.os,'geteuid',lambda:3164768,raising=False)
    for key,value in {'USER':'pwa209','SLURM_CPUS_PER_TASK':'1','HHW_GPU_COUNT':'0','HHW_MAX_HOURS':'4'}.items():monkeypatch.setenv(key,value)
    monkeypatch.setattr(m.subprocess,'check_output',lambda *a,**k:'n1.narval.calcul.quebec\n')
    for name in ('GUARD','SCHEDULER','ADMIN','POLICY'):
        path=f.root/('maintenance/'+name+'.py');path.write_text('# pinned '+name+'\n');monkeypatch.setattr(m,name,path);monkeypatch.setattr(m,name+'_SHA',digest(path))
    monkeypatch.setattr(m,'qualification_job',lambda contract:'588')
    monkeypatch.setattr(m,'own_registration',lambda *a:calls.append('own-registration') or {'native_job_id':'599'})
    monkeypatch.setattr(m,'native_allocation',lambda *a:(calls.append('own-native') or {'owner':'actual'},m.time.time()+14000))
    @contextmanager
    def lock(*a,**k):calls.append('lock');yield;calls.append('unlock')
    reader=lambda *a,**k:{'588':{'state':'COMPLETED'}}
    monkeypatch.setattr(repair,'_repair_native_states',reader);monkeypatch.setattr(campaign,'_repair_native_states',reader)
    monkeypatch.setattr(campaign,'combined_admission',lambda *a,**k:calls.append('admission') or {})
    def positive(path,sha,job,manifest_path,manifest_sha,observed):
        calls.append('positive-Q2');assert job=='588' and sha==digest(m.QPATH) and observed['588']['state']=='COMPLETED';return {'job_id':job,'sha256':sha}
    def run_path(path,**kwargs):
        if Path(path)==m.GUARD:return {'install':lambda *a,**k:calls.append('358-install')}
        if Path(path)==m.ADMIN:return {'install':lambda *a,**k:calls.append('900-install'),'registration_lock':lock}
        if Path(path)==m.SCHEDULER:return {'run_scontrol_job':lambda *a:None}
        return {'kernel_qualification':positive}
    monkeypatch.setattr(m.runpy,'run_path',run_path)
    policy=SimpleNamespace(receipt=lambda:{'qualified':'policy'});monkeypatch.setattr(m,'load_resource_policy',lambda *a,**k:policy)
    @contextmanager
    def lease(*a,**k):calls.append('lease');yield
    monkeypatch.setattr(m,'resource_lease',lease)
    def child(ident,*a,**k):calls.append('child-'+ident);return {'request_id':ident}
    monkeypatch.setattr(m,'run_child',child)
    write(m.QPATH,{'status':'actual-bound-positive-Q2-fixture'})
    argv=['--native-plan',str(f.root/'plans/recovery-support-20261021-425/plan.json'),'--native-ledger',str(f.root/'state/recovery-support-20261021-425-submission.json'),'--native-task','recovery-425','--native-script',str(f.root/'plans/recovery-support-20261021-425/recovery-425.sbatch'),'--source-manifest',str(f.path),'--source-manifest-sha256',digest(f.path)]
    return f,calls,argv

def test_complete_coordinator_orders_native_positive_q_and_seven_one_shot_children(producer):
    f,calls,argv=producer;m.main(argv)
    children=[x for x in calls if x.startswith('child-')]
    assert children==['child-'+ident for ident in m.IDS]
    assert calls.index('own-native')<calls.index('admission')<calls.index('positive-Q2')<calls.index('lease')<calls.index('child-429')
    assert read_json(m.CASE/'receipt.json')['status']=='complete_seven_reviewed_registrations'

@pytest.mark.parametrize('gate',['existing_plan','Q2_failure','child_failure'])
def test_required_gate_failure_stops_without_later_children_or_success_receipt(producer,monkeypatch,gate):
    f,calls,argv=producer
    if gate=='existing_plan':(f.root/'plans/recovery-support-20261021-429').mkdir(parents=True)
    elif gate=='Q2_failure':
        original=m.runpy.run_path
        def changed(path,**kwargs):
            value=original(path,**kwargs)
            if 'kernel_qualification' in value:
                def fail(*a):raise ValueError('Actual Q2 completion pending')
                value['kernel_qualification']=fail
            return value
        monkeypatch.setattr(m.runpy,'run_path',changed)
    else:
        def child(ident,*a,**k):
            calls.append('child-'+ident)
            if ident=='431':raise ValueError('Ambiguous registration')
            return {'request_id':ident}
        monkeypatch.setattr(m,'run_child',child)
    with pytest.raises(ValueError):m.main(argv)
    assert not (m.CASE/'receipt.json').exists()
    assert [x for x in calls if x.startswith('child-')]==(['child-429','child-430','child-431'] if gate=='child_failure' else [])

@pytest.mark.parametrize('outcomes,signals',[( [0],[]),(['timeout',-15,-15],[15,9]),(['timeout','timeout',-9],[15,9]),(['timeout','timeout','timeout'],[15,9])])
def test_child_session_timeout_terminates_owned_process_group_with_bounded_grace(monkeypatch,outcomes,signals):
    waits=[];sent=[];created=[];remaining=list(outcomes)
    class Process:
        pid=12345
        def wait(self,timeout):
            waits.append(timeout);value=remaining.pop(0)
            if value=='timeout':raise subprocess.TimeoutExpired(['bash','reviewed.sh'],timeout)
            return value
    def create(command,**kwargs):created.append((command,kwargs));return Process()
    monkeypatch.setattr(m.subprocess,'Popen',create)
    monkeypatch.setattr(m,'os',SimpleNamespace(name='posix',killpg=lambda pid,sig:sent.append((pid,int(sig)))))
    monkeypatch.setattr(m.signal,'SIGKILL',9,raising=False)
    kwargs={'cwd':'owned-area','env':{'MODULEPATH':'preserved'},'stdout':'file-out','stderr':'file-err','timeout':600,'check':False}
    if signals:
        with pytest.raises(subprocess.TimeoutExpired):m.run_shell_once(['bash','reviewed.sh'],**kwargs)
    else:assert m.run_shell_once(['bash','reviewed.sh'],**kwargs).returncode==0
    assert created[0][1]['start_new_session'] is True and created[0][1]['env']=={'MODULEPATH':'preserved'}
    assert sent==[(12345,signum) for signum in signals] and waits==[590]+([5]*(len(outcomes)-1))

def test_timeout_tolerates_an_already_disappeared_owned_group(monkeypatch):
    waits=[];signals=[]
    class Process:
        pid=12345
        def wait(self,timeout):
            waits.append(timeout)
            if len(waits)==1:raise subprocess.TimeoutExpired(['bash','reviewed.sh'],timeout)
            return -15
    monkeypatch.setattr(m.subprocess,'Popen',lambda *a,**k:Process())
    def missing(pid,sig):signals.append((pid,int(sig)));raise ProcessLookupError()
    monkeypatch.setattr(m,'os',SimpleNamespace(name='posix',killpg=missing));monkeypatch.setattr(m.signal,'SIGKILL',9,raising=False)
    with pytest.raises(subprocess.TimeoutExpired):m.run_shell_once(['bash','reviewed.sh'],cwd='owned',env={},stdout=None,stderr=None,timeout=600,check=False)
    assert signals==[(12345,15),(12345,9)] and waits==[590,5,5]


def test_v4_changes_only_namespace_and_q456_literals_from_frozen_v3():
    original=SOURCE.with_name('coordinate-reviewed-registrations-v3-20261023.py')
    assert digest(original)=='cc94ea69ca9d4d74e0cf9db5bae4548fcb4d5e6847a0d3d9466932a8eae5340d'
    replacements={'coordinate-reviewed-registrations-v3-20261023.py': 'coordinate-reviewed-registrations-v4-20261023.py', 'study-af-reviewed-registration-coordinator-v3-20261023': 'study-af-reviewed-registration-coordinator-v4-20261023', 'study-af-rolling-census-qualification-v4-20261023': 'study-af-rolling-census-qualification-v5-20261023', 'study-af-rolling-census-support-v3-20261023': 'study-af-rolling-census-support-v4-20261023', 'recovery-support-20261021-452': 'recovery-support-20261021-456', 'recovery-452': 'recovery-456', 'Q452': 'Q456'}
    class Rebind(ast.NodeTransformer):
        def visit_Constant(self,node):
            if isinstance(node.value,str):
                for before,after in replacements.items():node.value=node.value.replace(before,after)
            return node
    assert ast.dump(Rebind().visit(ast.parse(original.read_text())),include_attributes=False)==ast.dump(ast.parse(SOURCE.read_text()),include_attributes=False)
