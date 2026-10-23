"""One finite source-bound successor handoff after a terminal previous chain.

The completed earlier handoff, its failed first controller and the unstarted
cancelled successors are verified read-only. The original retired controllers
remain untouched. Every new controller is held and source-bound before release.
The actual emitted policy passes the frozen primary consumer before scheduler
mutations. No ambiguous action is retried. Original D89 budget limits remain.
"""
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
import argparse,base64,hashlib,importlib.util,json,math,os,re,runpy,shlex,signal,subprocess,sys,time,xml.etree.ElementTree as ET
from horsehair import slurm
from horsehair.core import canonical_hash,digest,read_json,source_identity,native_job_id
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
ROOT=Path('/scratch/pwa209/horsehair-worm')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
SOURCE='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
OLD_FRAMEWORK=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261021-57e0eb8c4c6c-d89b31180d4c')
OLD_SOURCE='d89b31180d4c4ffe9c4023dc5534b96f2d157a3a8e3aecaff30849283d00a705'
CONFIG='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
SUPPORT=ROOT/'maintenance/study-primary-external-census-handoff-v4-20261023'
SELF_NAME='handoff-primary-external-census-v1-20261023.py'
TEST_NAME='test_handoff_primary_external_census_v1_20261023.py'
PRIMARY_SUPPORT=ROOT/'maintenance/study-primary-external-census-policy-v1-20261023'
PRIMARY_HELPER=PRIMARY_SUPPORT/'run-primary-external-census-v1-20261023.py'
PRIMARY_HELPER_SHA='dd5f8d2aec90896fdcfe190971ea8b6c32a94158a3684566c2b8405748e7c794'
PRIMARY_MANIFEST_SHA='fd5d7b951b2df0094e82c7c670d6e578a30efa913b4dcc6923cf335627d1dde9'
Q_JOB='6866457'
Q_PLAN=ROOT/'plans/recovery-support-20261021-469/plan.json'
Q_LEDGER=ROOT/'state/recovery-support-20261021-469-submission.json'
Q_SCRIPT=Q_PLAN.parent/'recovery-469.sbatch'
Q_PLAN_SHA='eb100ade4309fe3656ed7b3e34f9d7ce63a0deca7fea4c6cd9de92d3c8d784fc'
Q_LEDGER_SHA='0a248a14e1eb3a29e4a656abfef05e1450651f48df746db81c9757c8cea9ebb8'
Q_SCRIPT_SHA='50734d94eab535f315088581dc16d147757109f6922df249343a57cea0e964b1'
Q_POLICY_SHA='b597cd76d14a668e35a36a8a9745e0f034fc47981e29e3ddab779e6e4f011d18'
Q_RECEIPT_SHA='d7966afe00498ebc4c1e23c34da8b75407fc0fb691afb488b11ed98e0c53f711'
OLD_PLAN=ROOT/'plans/mantis-reference-recovery-v14-20261021-controllers/plan.json'
OLD_LEDGER=ROOT/'state/mantis-reference-recovery-v14-20261021-controllers-submission.json'
OLD_PLAN_SHA='6346a635f1742dc87f103acda811b2242dc680d139e5a5dda63dd9e26439d0a4'
OLD_LEDGER_SHA='d35ed0c14006b5c57d2385ddcc1fb8908b4d7c89b0eb745199f08cfa7d1679a4'
OLD_JOBS=('6632164','6632209','6632225','6632239')
FIRST_PROVIDER='6864491'
PHASE='primary-external-census-controllers-v3'
PARTIAL_PHASE='primary-external-census-controllers-v1'
PARTIAL_PLAN=ROOT/'plans'/PARTIAL_PHASE/'plan.json'
PARTIAL_PLAN_SHA='6a33e4ee89f4f4ef380e712aec7d488bdd7b5acfbb39d28301cabcb7c7e7cdf3'
PARTIAL_LEDGER=ROOT/'state'/(PARTIAL_PHASE+'-submission.json')
PARTIAL_LEDGER_SHA='26efafe65339ec349a65773135d36502a3baa7d3f4523d56f704b16dba3c5be6'
PARTIAL_JOBS=('6868884','6868886','6868888','6868889')
PARTIAL_SCRIPT_SHAS=('fbe69f82abce43e936a70be0aeb27835e7bb915fadb1cef59b1c06d2d39b4938',
    '0831e0ec643aad2493f34475a62963cf6bffc8419b5ad2a538f9c33369d1539d',
    '6fc9ba93d17242e2697cfa6a52a0ba5315902be7e596a3175289de520642ce89',
    '04f55706c737a29b3ef1137c8605b74062346c2baae939bbed8061358d4fbaba')
PARTIAL_DESCRIPTOR=ROOT/'maintenance/study-primary-external-census-handoff-v2-20261023/native-6868176/run-policy.json'
PARTIAL_DESCRIPTOR_SHA='ad70a140cdd50384f860cb4adc2dc4fc38feb0a7eaa60be2403456373c9b2267'
PARTIAL_PARENT='6868176'
PARTIAL_SOURCE=PARTIAL_DESCRIPTOR.parent.parent/SELF_NAME
PARTIAL_SOURCE_SHA='de582b45b5c3f813673f6f0bcaa3aca32659c3b57a75b10511e2409fabb015c9'
PRIOR_PHASE='primary-external-census-controllers-v2'
PRIOR_PLAN=ROOT/'plans'/PRIOR_PHASE/'plan.json'
PRIOR_PLAN_SHA='2b5b77e7230b5365de91caf64f72484c456856b0526549a53400320ee7c32cb3'
PRIOR_LEDGER=ROOT/'state'/(PRIOR_PHASE+'-submission.json')
PRIOR_LEDGER_SHA='0bd363318e2c4195de7f1a4bc5dc424555e26e8ff8251d4333c5c902649ce55c'
PRIOR_JOBS=('6870608','6870610','6870628','6870632')
PRIOR_SCRIPT_SHAS=('69157d93af7578a3abbf0f9663a57d44975ab8675adc13867e5b5672d01fa1c5',
    'b5cfa87d46d747f15dcd2be6e3e5d991b222b854c6b9b211cf026af234486ff5',
    'ec21853c8c3ea8b680a7ed1b8ab9652044504255e5e89c44574634e5961d8d7a',
    'a05ab5ae921596ce703a99246dfaea0932d1f166e4f5f495549b911e2da35d3c')
PRIOR_PARENT='6870245'
PRIOR_AREA=ROOT/'maintenance/study-primary-external-census-handoff-v3-20261023'/('native-'+PRIOR_PARENT)
PRIOR_DESCRIPTOR=PRIOR_AREA/'run-policy.json'
PRIOR_DESCRIPTOR_SHA='ad70a140cdd50384f860cb4adc2dc4fc38feb0a7eaa60be2403456373c9b2267'
PRIOR_SOURCE=PRIOR_AREA.parent/SELF_NAME
PRIOR_SOURCE_SHA='0d37f96bd93dd96266eb3f46eb66bf7bd1ecb929b1e8474f3153b1624fda50f3'
PRIOR_RECEIPT=PRIOR_AREA/'receipt.json'
PRIOR_RECEIPT_SHA='9e525425ba063c824eed9fe40e15272d33be5ae88bc208850167b0e2d07523a1'
PRIOR_NATIVE_TEST_SHA='db4ced954789e1395fd42f9f2f2313727fb179f8035f13612f426a12f9231243'
PRIOR_PARTIAL_PROOF_SHA='4d1b6cb059217dff57f63f90932c30e454a1c2a21560cf95b85cf388a930d3a9'
PRIOR_ORIGINAL_PROOF_SHA='0ee8473497fad87588ce93a4a20cdf551413eeb6a4dac5ac14bccca001f3882d'
PRIOR_FAILURE_LOG=ROOT/'logs/primary-external-census-controller-0-6870608_4294967294.log'
PRIOR_FAILURE_LOG_SHA='940a51ef1252cf82122f55ec9ac24a38edf17b7841e144f710775e6bf666b811'
PRIOR_CLEANUP_RECEIPT=ROOT/'maintenance/study-primary-descriptor-prestart-reconciliation-v1-20261023/receipt.json'
PRIOR_CLEANUP_RECEIPT_SHA='567ff41003556ffaf316db0d9d5499a20287ffb3fbe304537efac1bb745326ed'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
GUARD_SHA='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91'
SCHEDULER=GUARD.with_name('native-scheduler-read.py')
SCHEDULER_SHA='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
REGISTRY=ROOT/'maintenance/study-af-rolling-census-support-v4-20261023/registered-snapshot-read-guard-v4-20261022.py'
REGISTRY_SHA='5f06fd63122392afc672952e396afbe6b90fab152f16b65430120c7f8d93648a'
POLICY=ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json'
POLICY_SHA='f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5'
TERMINAL={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
FALSE_FLAGS={'scientific_result':False,'admission_authority':False,'scientific_gates_cleared':False,'freshness_timestamp_rewritten':False,'canonical_science_changed':False}

def require(value,message):
    if not value:raise ValueError(message)

def ordinary(path,namespace):
    p=Path(path)
    require(p.is_absolute() and p==p.resolve() and p!=namespace and p.is_relative_to(namespace) and p.is_file()
        and all(not q.is_symlink() for q in [p,*p.parents] if q.is_relative_to(namespace)),'Ordinary confined file required')
    return p

def packet(path,sha=None,limit=8_000_000):
    p=Path(path)
    if not p.is_relative_to(ROOT):
        baseline=OLD_FRAMEWORK.resolve()
        allowed={baseline/'tests/test_pipeline.py':'5e0c052c596765a32ff4443d8668ba3b627dbeb1d7f7f954479048bbc5a14936',
            baseline/'tests/test_budget.py':'9ea7346e98203bc098121689f7a4ac4a45c2ef488bd2472d894cc0beee0d65de'}
        require(p in allowed and sha==allowed[p],'Only the two exact resolved original baseline sources with mandatory fixed pins may be read outside ROOT')
        p=ordinary(p,baseline)
    else:p=ordinary(p,ROOT)
    with p.open('rb') as stream:raw=stream.read(limit+1)
    h=hashlib.sha256(raw).hexdigest();require(len(raw)<=limit and (sha is None or sha==h),'Bounded exact source/evidence changed')
    return raw,h

def locator(path):return {'path':str(path),'sha256':packet(path)[1]}

def exclusive_bytes(path,raw):
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())

def exclusive(path,value):exclusive_bytes(path,(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode())

def loaded(path,sha,name):
    packet(path,sha,200_000);spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module);require(digest(path)==sha,'Library changed during import');return module

def native_fields(response):
    response.check_returncode();lines=[line for line in response.stdout.splitlines() if line.strip()]
    require(len(lines)==1,'One complete native scheduler row required')
    key=r'[A-Za-z][A-Za-z0-9_/:]*'
    pairs=re.findall(r'(?<!\S)('+key+r')=(.*?)(?=\s+'+key+r'=|$)',lines[0])
    value={name:item.strip() for name,item in pairs}
    require(len(value)==len(pairs),'Duplicate native scheduler fields refused');return value

def self_qualification(c,*,runner=subprocess.run):
    contract=c['manifest']['tests'];temp=Path(os.environ.get('SLURM_TMPDIR',''))
    require(temp.is_absolute() and temp==temp.resolve() and temp.is_dir() and not temp.is_symlink()
        and temp.stat().st_uid==3164768 and not temp.is_relative_to(ROOT),'Owned allocation-local selftest namespace required')
    workspace=temp/('hhw-external-census-handoff-'+c['job']);workspace.mkdir(mode=0o700,exist_ok=False)
    xml=c['area']/'handoff-contracts.xml';test=SUPPORT/TEST_NAME
    paths=c['primary'].environment(FRAMEWORK)['PYTHONPATH'].split(os.pathsep)
    env={**os.environ,'PYTHONPATH':os.pathsep.join(paths),'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1','TZ':'UTC'}
    for key in ('PYTEST_ADDOPTS','PYTEST_PLUGINS'):env.pop(key,None)
    command=[sys.executable,'-s','-m','pytest',str(test),'-q','-p','no:cacheprovider','--basetemp',str(workspace),'--junitxml',str(xml)]
    exclusive(c['area']/'native-selftest-intent.json',{'command':command,'source':locator(SUPPORT/SELF_NAME),'test_source':locator(test),
        'primary_q_coverage_claimed_for_handoff':False,**FALSE_FLAGS})
    with (c['area']/'native-selftest-stdout.txt').open('xb') as out,(c['area']/'native-selftest-stderr.txt').open('xb') as err:
        result=runner(command,cwd=FRAMEWORK,env=env,stdout=out,stderr=err,timeout=min(1800,c['end']-time.time()-900),check=False)
    require(result.returncode==0,'Actual native handoff contract suite failed before scheduler changes')
    raw=packet(xml,limit=8_000_000)[0];require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Unsafe native JUnit refused')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'));names=sorted(case.get('classname','').split('.')[-1]+'::'+case.get('name','') for case in cases)
    leaves=[node for node in tree.iter('testsuite') if not node.findall('testsuite')]
    require(len(cases)==len(set(names))==contract['count'] and all(case.get('classname') and case.get('name') for case in cases)
        and canonical_hash(names)==contract['roster_sha256'] and not any(list(case.iter(key)) for case in cases for key in ('failure','error','skipped'))
        and leaves and sum(int(node.get('tests','-1')) for node in leaves)==contract['count']
        and all(all(int(n.get(key,'0'))==0 for key in ('failures','errors','skipped')) for n in tree.iter() if n.tag in {'testsuite','testsuites'}),
        'Actual complete native selftest counts/roster required')
    value={'status':'complete_native_handoff_contracts_completion_pending','native_job_id':c['job'],'source':locator(SUPPORT/SELF_NAME),
        'test_source':locator(test),'junit':locator(xml),'test_counts':{'tests':contract['count'],'failures':0,'errors':0,'skipped':0},
        'test_roster_sha256':contract['roster_sha256'],'primary_q_coverage_claimed_for_handoff':False,
        'native_completion_required_before_controllers_start':True,**FALSE_FLAGS}
    exclusive(c['area']/'native-handoff-qualification.json',value);c['selftest_proof']=value;return value


def verify_sources(c):
    require(time.time()+600<c['end'],'Finite native handoff finalization margin exhausted')
    for path,pin in c['pins'].items():packet(path,pin,64_000_000)
    require(source_identity(FRAMEWORK)['sha256']==SOURCE and source_identity(PRIMARY)['sha256']==PRIMARY_SHA
        and source_identity(OLD_FRAMEWORK)['sha256']==OLD_SOURCE,'Original/verifier/science source changed')


def mutation(c,name,command,*,runner,precheck=None):
    exclusive(c['area']/(name+'-intent.json'),{'command':command,'automatic_replay_permitted':False,**FALSE_FLAGS})
    result={'command':command,'status':'stopped_ambiguous','mutation_may_have_occurred':False}
    try:
        verify_sources(c)
        if precheck is not None:result['precheck']=precheck()
        result['mutation_may_have_occurred']=True
        response=runner(command,text=True,capture_output=True,timeout=30)
        result.update(returncode=response.returncode,stdout=response.stdout,stderr=response.stderr)
        response.check_returncode();result['status']='native_command_returned_zero';return response
    except BaseException as error:
        result.update(exception=type(error).__name__,message=str(error)[:4000]);raise
    finally:exclusive(c['area']/(name+'-result.json'),result)


def old_bindings(c):
    plan=json.loads(packet(OLD_PLAN,OLD_PLAN_SHA)[0]);ledger=json.loads(packet(OLD_LEDGER,OLD_LEDGER_SHA)[0])
    require(canonical_hash(plan)==ledger['plan_sha256'] and plan['release_identity']['sha256']==ledger['source_sha256']==PRIMARY_SHA
        and plan['config_sha256']==ledger['config_sha256']==CONFIG and plan['root']==str(ROOT)
        and len(plan['tasks'])==4 and set(ledger['tasks'])=={'pipeline-controller-'+str(i) for i in range(4)},'Exact original primary registration required')
    for i,task in enumerate(plan['tasks']):
        name='pipeline-controller-'+str(i);row=ledger['tasks'][name];script=OLD_PLAN.parent/(name+'.sbatch')
        text=packet(script)[0].decode()
        require(task['name']==name and task['script']==str(script) and row['job_id']==OLD_JOBS[i] and row['state']=='submitted'
            and canonical_hash(text)==task['script_sha256']==row['script_sha256'] and task['command'] in text
            and row['command'][-1]==str(script) and task['resources']=={'cpus':1,'memory':'8G','hours':144,'gpu':False}
            and task['depends']==([] if i==0 else ['pipeline-controller-'+str(i-1)])
            and task['dependency_kind']==('afterok' if i==0 else 'afterany'),'Original controller source/script/DAG changed')
        c['pins'][str(script)]=digest(script)
    c['pins'].update({str(OLD_PLAN):OLD_PLAN_SHA,str(OLD_LEDGER):OLD_LEDGER_SHA});c['old_jobs']=list(OLD_JOBS)
    return plan


def old_pending(c,task,job):
    c['reads']=c.get('reads',0)+1
    value=native_fields(c['scheduler'](job,c['area']/('old-native-'+str(c['reads']).zfill(6))))
    require(value.get('JobId')==job and value.get('UserId')=='pwa209(3164768)' and value.get('Account')=='def-ptewarie_cpu'
        and value.get('JobName')=='hhw-'+task['name'] and value.get('Command')==task['script']
        and value.get('WorkDir')==str(OLD_FRAMEWORK.resolve()) and value.get('JobState')=='PENDING'
        and value.get('RunTime')=='00:00:00' and value.get('StartTime') in {'Unknown','None'}
        and value.get('NodeList') in {'','(null)','None'} and value.get('AllocTRES')=='(null)'
        and value.get('Restarts')=='0' and value.get('Requeue')=='0' and value.get('ExitCode')=='0:0'
        and value.get('NumCPUs')=='1' and value.get('NumNodes') in {'1','1-1'} and value.get('MinMemoryNode') in {'8G','8192M'}
        and value.get('TimeLimit') in {'5-22:00:00','142:00:00'} and 'ArrayJobId' not in value and 'ArrayTaskId' not in value,
        'Only exact owned scalar unstarted PENDING old controllers may be retired')
    tres(value.get('ReqTRES',''),1,'8G')
    return value


def owned_terminal(row,job):
    require(row.get('job_id_raw')==job and row.get('user')=='pwa209' and row.get('account')=='def-ptewarie_cpu'
        and row.get('state') in TERMINAL,'Exact owned terminal native identity required')


def unused_cancelled(row,job):
    owned_terminal(row,job)
    require(row.get('state')=='CANCELLED' and row.get('exit_code')=='0:0' and row.get('cpus')==0
        and row.get('elapsed_seconds')==0 and row.get('unallocated_metadata') is False and row.get('alloc_tres')==''
        and row.get('start') in {'None','Unknown'} and row.get('node_list')=='None assigned'
        and row.get('max_rss') in {'','0','0K'} and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]+',str(row.get('end','')))
        and row.get('identity_basis')=='Unique scalar native job; zero-allocation queue record',
        'Exact scalar CANCELLED native proof must positively establish no allocation or start')
    return row


def partial_bindings(c):
    plan=json.loads(packet(PARTIAL_PLAN,PARTIAL_PLAN_SHA)[0]);ledger=json.loads(packet(PARTIAL_LEDGER,PARTIAL_LEDGER_SHA)[0])
    packet(PARTIAL_DESCRIPTOR,PARTIAL_DESCRIPTOR_SHA);packet(PARTIAL_SOURCE,PARTIAL_SOURCE_SHA)
    require(plan.get('phase')==PARTIAL_PHASE and plan.get('root')==str(ROOT) and plan.get('handoff_native_job_id')==PARTIAL_PARENT
        and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256')==OLD_SOURCE
        and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG and canonical_hash(plan)==ledger.get('plan_sha256')
        and plan.get('descriptor_path')==str(PARTIAL_DESCRIPTOR) and plan.get('descriptor_sha256')==PARTIAL_DESCRIPTOR_SHA
        and len(plan.get('tasks',[]))==4 and set(ledger.get('tasks',{}))=={'primary-external-census-controller-'+str(i) for i in range(4)},
        'Exact immutable failed partial phase registration required')
    for index,task in enumerate(plan['tasks']):
        name='primary-external-census-controller-'+str(index);row=ledger['tasks'][name];script=PARTIAL_PLAN.parent/(name+'.sbatch')
        raw,rawsha=packet(script,PARTIAL_SCRIPT_SHAS[index]);text=raw.decode();tokens=shlex.split(task['command'])
        intent={'helper':{'path':str(PRIMARY_HELPER),'sha256':PRIMARY_HELPER_SHA},
            'policy_descriptor':{'path':str(PARTIAL_DESCRIPTOR),'sha256':PARTIAL_DESCRIPTOR_SHA},'mode':'run'}
        require(task.get('name')==name and task.get('script')==str(script) and row.get('state')=='submitted'
            and row.get('job_id')==PARTIAL_JOBS[index] and canonical_hash(text)==task.get('script_sha256')==row.get('script_sha256')
            and task['command'] in text and task.get('primary_external_census')==intent
            and task.get('resources')=={'cpus':1,'memory':'64G','hours':72,'gpu':False,'array':None}
            and task.get('depends')==([] if index==0 else ['primary-external-census-controller-'+str(index-1)])
            and task.get('external_dependencies')==([PARTIAL_PARENT,FIRST_PROVIDER] if index==0 else [])
            and task.get('dependency_kind')==row.get('dependency_kind')==('afterok' if index==0 else 'afterany')
            and row.get('issued_native_dependencies')==([PARTIAL_PARENT] if index==0 else [PARTIAL_JOBS[index-1]])
            and row.get('command')==['sbatch','--parsable','--dependency='+('afterok:'+PARTIAL_PARENT if index==0 else 'afterany:'+PARTIAL_JOBS[index-1]),str(script)]
            and tokens.count(str(PRIMARY_HELPER))==1,'Exact failed partial task/native/script/intent/DAG required')
        expected=['--mode','run','--policy-descriptor',str(PARTIAL_DESCRIPTOR),'--policy-descriptor-sha256',PARTIAL_DESCRIPTOR_SHA,
            '--native-plan',str(PARTIAL_PLAN),'--native-ledger',str(PARTIAL_LEDGER),'--native-task',name,'--native-script',str(script),'--segment',str(index)]
        require(tokens[tokens.index(str(PRIMARY_HELPER))+1:]==expected,'Exact historical partial native arguments required')
        c['pins'][str(script)]=rawsha
    parents=c['native_reader']([PARTIAL_PARENT],{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(parents)=={PARTIAL_PARENT},'Exact failed partial handoff native record required');parent=parents[PARTIAL_PARENT]
    owned_terminal(parent,PARTIAL_PARENT)
    require(parent.get('state')=='FAILED' and parent.get('exit_code')=='1:0' and parent.get('cpus')==4
        and parent.get('elapsed_seconds')==864 and parent.get('unallocated_metadata') is False,'Exact failed partial handoff allocation required')
    tres(parent.get('alloc_tres',''),4,'16G')
    c['pins'].update({str(PARTIAL_PLAN):PARTIAL_PLAN_SHA,str(PARTIAL_LEDGER):PARTIAL_LEDGER_SHA,
        str(PARTIAL_DESCRIPTOR):PARTIAL_DESCRIPTOR_SHA,str(PARTIAL_SOURCE):PARTIAL_SOURCE_SHA})
    return {'plan':plan,'ledger':ledger,'tasks':plan['tasks'],'jobs':list(PARTIAL_JOBS)}


def partial_pending(c,task,job):
    require(job in PARTIAL_JOBS[1:],'Only the three exact historical held jobs may be cancelled')
    c['partial_reads']=c.get('partial_reads',0)+1
    value=native_fields(c['scheduler'](job,c['area']/('partial-native-'+str(c['partial_reads']).zfill(6))))
    require(value.get('JobId')==job and value.get('UserId')=='pwa209(3164768)' and value.get('Account')=='def-ptewarie_cpu'
        and value.get('JobName')=='hhw-'+task['name'] and value.get('Command')==task['script']
        and value.get('WorkDir')==str(FRAMEWORK.resolve()) and value.get('JobState')=='PENDING'
        and value.get('Reason')=='JobHeldUser' and value.get('Priority')=='0' and value.get('RunTime')=='00:00:00'
        and value.get('StartTime') in {'Unknown','None'} and value.get('NodeList') in {'','(null)','None'}
        and value.get('AllocTRES')=='(null)' and value.get('Restarts')=='0' and value.get('Requeue')=='0'
        and value.get('ExitCode')=='0:0' and value.get('NumCPUs')=='1' and value.get('NumNodes') in {'1','1-1'}
        and value.get('MinMemoryNode') in {'64G','65536M'} and value.get('TimeLimit') in {'3-00:00:00','72:00:00'}
        and 'ArrayJobId' not in value and 'ArrayTaskId' not in value,'Only exact owned held unstarted historical partial controllers may be retired')
    tres(value.get('ReqTRES',''),1,'64G');return value


def partial_terminal(c,job):
    require(job in PARTIAL_JOBS,'Only exact partial native identities allowed')
    rows=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(rows)=={job},'Exact partial terminal native record required');return unused_cancelled(rows[job],job)


def reconcile_partial(c,*,runner=subprocess.run,sleeper=time.sleep):
    require(c.get('registration_locked') is True and c.get('selftest_proof',{}).get('status')=='complete_native_handoff_contracts_completion_pending',
        'Partial reconciliation requires permanent registration lock and actual native selftests')
    pack=partial_bindings(c);first=partial_terminal(c,PARTIAL_JOBS[0])
    preflight={job:partial_pending(c,task,job) for task,job in zip(pack['tasks'][1:],PARTIAL_JOBS[1:])}
    exclusive(c['area']/'partial-all-held-preflight.json',{'already_cancelled_without_mutation':first,'pending':preflight,**FALSE_FLAGS})
    for task,job in reversed(list(zip(pack['tasks'][1:],PARTIAL_JOBS[1:]))):
        command=['scancel','--ctld','--state=PENDING','--user=pwa209','--name=hhw-'+task['name'],job]
        mutation(c,'reconcile-partial-'+job,command,runner=runner,precheck=lambda task=task,job=job:partial_pending(c,task,job))
        for attempt in range(12):
            rows=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu')
            require(set(rows)=={job},'Exact partial cancellation target record required');row=rows[job]
            require(row.get('job_id_raw')==job and row.get('user')=='pwa209' and row.get('account')=='def-ptewarie_cpu'
                and row.get('state') in {'PENDING','CANCELLED'},'Partial pending-only cancellation raced or has unexpected state; further mutations forbidden')
            if row['state']=='CANCELLED':
                exclusive(c['area']/('reconcile-partial-'+job+'-terminal.json'),unused_cancelled(row,job));break
            require(attempt<11 and time.time()+600<c['end'],'Partial cancellation lacks finite positive terminal proof');sleeper(5)
    proof={job:partial_terminal(c,job) for job in PARTIAL_JOBS};verify_sources(c)
    exclusive(c['area']/'partial-reconciliation.json',{'status':'exact_failed_partial_phase_retired_without_allocation','native':proof,
        'plan':locator(PARTIAL_PLAN),'ledger':locator(PARTIAL_LEDGER),'already_cancelled_job_not_mutated':PARTIAL_JOBS[0],
        'retirement_order':list(reversed(PARTIAL_JOBS[1:])),'automatic_replay_permitted':False,'failed_phase_bytes_preserved':True,**FALSE_FLAGS})
    c['partial_reconciliation']=proof;return proof


def preflight_old(c,plan):
    rows=c['native_reader']([OLD_JOBS[0]],{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(rows)=={OLD_JOBS[0]},'Failed original controller native proof required');owned_terminal(rows[OLD_JOBS[0]],OLD_JOBS[0])
    result={job:old_pending(c,task,job) for task,job in zip(plan['tasks'][1:],OLD_JOBS[1:])}
    c['old_preflights']=c.get('old_preflights',0)+1
    suffix='' if c['old_preflights']==1 else '-'+str(c['old_preflights']).zfill(2)
    exclusive(c['area']/('all-three-old-pending-preflight'+suffix+'.json'),{'initial_controller':rows,'pending':result,**FALSE_FLAGS})
    return result


def terminal_old(c):
    rows=c['native_reader'](list(OLD_JOBS),{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(rows)==set(OLD_JOBS),'All original controller native records required')
    for job in OLD_JOBS:owned_terminal(rows[job],job)
    for job in OLD_JOBS[1:]:
        unused_cancelled(rows[job],job)
    return rows


def await_retired(c,job,*,sleeper=time.sleep):
    for attempt in range(12):
        rows=c['native_reader']([job],{},owner='pwa209',account='def-ptewarie_cpu')
        require(set(rows)=={job},'Exact retired native target required');row=rows[job]
        require(row.get('job_id_raw')==job and row.get('user')=='pwa209' and row.get('account')=='def-ptewarie_cpu',
            'Retired native owner changed')
        require(row.get('state') in {'PENDING','CANCELLED'},'Pending-only cancellation did not retire target; active or unexpected state forbids further mutations')
        if row['state']=='CANCELLED':
            unused_cancelled(row,job)
            exclusive(c['area']/('retire-old-'+job+'-terminal.json'),row);return row
        require(attempt<11 and time.time()+600<c['end'],'Pending cancellation lacks finite positive terminal proof')
        sleeper(5)


def retire_old(c,plan,*,runner=subprocess.run,sleeper=time.sleep):
    require(c.get('registration_locked') is True,'Retirement requires the permanent registration lock')
    preflight_old(c,plan)
    for task,job in reversed(list(zip(plan['tasks'][1:],OLD_JOBS[1:]))):
        command=['scancel','--ctld','--state=PENDING','--user=pwa209','--name=hhw-'+task['name'],job]
        mutation(c,'retire-old-'+job,command,runner=runner,precheck=lambda task=task,job=job:old_pending(c,task,job))
        await_retired(c,job,sleeper=sleeper)
    for attempt in range(12):
        try:
            rows=terminal_old(c);exclusive(c['area']/'old-controller-final-native.json',rows);return rows
        except ValueError:
            require(time.time()+600<c['end'],'Old terminal proof exhausted finalization margin')
            if attempt==11:raise
            sleeper(5)


def tres(value,cpus,memory):
    entries=[x.partition('=') for x in value.split(',')];items={k:v for k,sep,v in entries}
    equivalents={'8G':{'8G','8192M','8388608K'},'16G':{'16G','16384M','16777216K'},'64G':{'64G','65536M','67108864K'}}[memory]
    require(len(items)==len(entries) and all(k and sep and v for k,sep,v in entries)
        and set(items)<={'cpu','mem','node','billing'} and items.get('cpu')==str(cpus) and items.get('node')=='1'
        and items.get('mem') in equivalents,'Exact CPU-only native resources required')


def dependency_edges(value):
    result={}
    if value=='(null)':return result
    for part in value.split(','):
        kind,sep,tail=part.partition(':');require(sep and kind in {'afterok','afterany'},'Only exact native dependency kinds allowed')
        for token in tail.split(':'):
            match=re.fullmatch(r'([1-9][0-9]*)(?:\((unfulfilled|fulfilled)\))?',token)
            require(match is not None and match[1] not in result,'Malformed or duplicate native dependency')
            result[match[1]]=(kind,match[2])
    return result


def freeze_new(c,descriptor):
    directory=ROOT/'plans'/PHASE;ledger=ROOT/'state'/(PHASE+'-submission.json')
    require(not directory.exists() and not ledger.exists(),'Fresh primary controller phase required');directory.mkdir(mode=0o700)
    tasks=[];previous=None;dsha=digest(descriptor)
    for index in range(4):
        name='primary-external-census-controller-'+str(index);script=directory/(name+'.sbatch')
        args=['--mode','run','--policy-descriptor',str(descriptor),'--policy-descriptor-sha256',dsha,
            '--native-plan',str(directory/'plan.json'),'--native-ledger',str(ledger),'--native-task',name,'--native-script',str(script),'--segment',str(index)]
        command=shlex.join([sys.executable,'-s','-u',str(PRIMARY_HELPER),*args])
        text=slurm._script(c['config'],OLD_FRAMEWORK.resolve(),name,command,cpus=1,memory='64G',hours=72,gpu=False)
        require(text.count('set -euo pipefail')==1,'Exact script prologue required')
        text=text.replace('set -euo pipefail','#SBATCH --hold\n#SBATCH --chdir='+shlex.quote(str(OLD_FRAMEWORK.resolve()))+'\nset -euo pipefail\nexport TZ=UTC\nexport PYTHONDONTWRITEBYTECODE=1',1)
        exclusive_bytes(script,text.encode())
        task={'name':name,'command':command,'script':str(script),'script_sha256':canonical_hash(text),
            'depends':[] if index==0 else [previous],'dependency_kind':'afterok' if index==0 else 'afterany',
            'external_dependencies':[c['job'],FIRST_PROVIDER] if index==0 else [],
            'resources':{'cpus':1,'memory':'64G','hours':72,'gpu':False,'array':None},
            'primary_external_census':{'helper':{'path':str(PRIMARY_HELPER),'sha256':PRIMARY_HELPER_SHA},
                'policy_descriptor':{'path':str(descriptor),'sha256':dsha},'mode':'run'}}
        tasks.append(task);previous=name
    plan={'schema_version':2,'phase':PHASE,'root':str(ROOT),'config_sha256':CONFIG,'release_identity':source_identity(OLD_FRAMEWORK),
        'tasks':tasks,'maximum_simultaneous_resources':{'cpus':1,'gpus':0},'cpu_ceiling':256,'gpu_concurrency':4,
        'resource_policy':c['policy'].receipt(),'descriptor_path':str(descriptor),'descriptor_sha256':dsha,
        'handoff_native_job_id':c['job'],'original_primary_lease_limits_retained':True,'scientific_gates_changed':False}
    path=directory/'plan.json';exclusive(path,plan);return {'path':path,'ledger':ledger,'value':plan}


def new_held(c,task,job,row,index,jobs):
    require(re.fullmatch('[1-9][0-9]*',str(job)) and job not in {*OLD_JOBS,c['job'],Q_JOB,FIRST_PROVIDER},'Distinct new controller required')
    require(row.get('state')=='submitted' and row.get('job_id')==job and row.get('script_sha256')==task['script_sha256']
        and canonical_hash(packet(task['script'])[0].decode())==task['script_sha256'],'Exact new source-bound submission required')
    issued=row.get('issued_native_dependencies');completed=row.get('completed_native_dependencies',[])
    kind='afterok' if index==0 else 'afterany';expected={c['job'],FIRST_PROVIDER} if index==0 else {jobs[index-1]}
    require(type(issued) is list and len(issued)==len(set(issued)) and row.get('dependency_kind')==kind
        and row.get('dependency_resolution_sha256')==canonical_hash({'kind':kind,'issued':issued,'completed':completed})
        and set(issued)|{x.get('job_id') for x in completed}==expected,'Actual registered dependency resolution changed')
    for proof in completed:
        require(index==0 and proof.get('job_id')==FIRST_PROVIDER and proof.get('dependency_kind')=='afterok'
            and slurm.completed_native_dependency(ROOT,FIRST_PROVIDER) is not None,'Only positive completed first provider may remove its native edge')
    c['reads']=c.get('reads',0)+1
    value=native_fields(c['scheduler'](job,c['area']/('new-native-'+str(c['reads']).zfill(6))))
    require(value.get('JobId')==job and value.get('UserId')=='pwa209(3164768)' and value.get('Account')=='def-ptewarie_cpu'
        and value.get('JobName')=='hhw-'+task['name'] and value.get('Command')==task['script']
        and value.get('WorkDir')==str(OLD_FRAMEWORK.resolve()) and value.get('JobState')=='PENDING'
        and value.get('Reason')=='JobHeldUser' and value.get('Priority')=='0' and value.get('RunTime')=='00:00:00'
        and value.get('StartTime') in {'Unknown','None'} and value.get('NodeList') in {'','(null)','None'}
        and value.get('AllocTRES')=='(null)' and value.get('NumCPUs')=='1' and value.get('NumNodes') in {'1','1-1'}
        and value.get('TimeLimit') in {'3-00:00:00','72:00:00'} and value.get('MinMemoryNode') in {'64G','65536M'}
        and 'ArrayJobId' not in value and 'ArrayTaskId' not in value,'Exact held unstarted new D89 allocation required')
    tres(value.get('ReqTRES',''),1,'64G');edges=dependency_edges(value.get('Dependency',''))
    require(set(edges)<=set(issued) and all(k==kind for k,state in edges.values()),'Native dependencies differ from registered edges')
    for missing in set(issued)-set(edges):
        require(index==0 and missing==FIRST_PROVIDER and slurm.completed_native_dependency(ROOT,missing) is not None,'Unfulfilled native dependency disappeared')
    for dep,(edge,state) in edges.items():
        if state=='fulfilled':require(dep==FIRST_PROVIDER and slurm.completed_native_dependency(ROOT,dep) is not None,'Unproved fulfilled native edge')
    return value


def prior_bindings(c):
    plan=json.loads(packet(PRIOR_PLAN,PRIOR_PLAN_SHA)[0]);ledger=json.loads(packet(PRIOR_LEDGER,PRIOR_LEDGER_SHA)[0])
    packet(PRIOR_DESCRIPTOR,PRIOR_DESCRIPTOR_SHA);packet(PRIOR_SOURCE,PRIOR_SOURCE_SHA)
    require(plan.get('phase')==PRIOR_PHASE and plan.get('root')==str(ROOT) and plan.get('handoff_native_job_id')==PRIOR_PARENT
        and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256')==OLD_SOURCE
        and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG and canonical_hash(plan)==ledger.get('plan_sha256')
        and plan.get('descriptor_path')==str(PRIOR_DESCRIPTOR) and plan.get('descriptor_sha256')==PRIOR_DESCRIPTOR_SHA
        and len(plan.get('tasks',[]))==4 and set(ledger.get('tasks',{}))=={'primary-external-census-controller-'+str(i) for i in range(4)},
        'Exact immutable failed partial phase registration required')
    for index,task in enumerate(plan['tasks']):
        name='primary-external-census-controller-'+str(index);row=ledger['tasks'][name];script=PRIOR_PLAN.parent/(name+'.sbatch')
        raw,rawsha=packet(script,PRIOR_SCRIPT_SHAS[index]);text=raw.decode();tokens=shlex.split(task['command'])
        intent={'helper':{'path':str(PRIMARY_HELPER),'sha256':PRIMARY_HELPER_SHA},
            'policy_descriptor':{'path':str(PRIOR_DESCRIPTOR),'sha256':PRIOR_DESCRIPTOR_SHA},'mode':'run'}
        require(task.get('name')==name and task.get('script')==str(script) and row.get('state')=='submitted'
            and row.get('job_id')==PRIOR_JOBS[index] and canonical_hash(text)==task.get('script_sha256')==row.get('script_sha256')
            and task['command'] in text and task.get('primary_external_census')==intent
            and task.get('resources')=={'cpus':1,'memory':'64G','hours':72,'gpu':False,'array':None}
            and task.get('depends')==([] if index==0 else ['primary-external-census-controller-'+str(index-1)])
            and task.get('external_dependencies')==([PRIOR_PARENT,FIRST_PROVIDER] if index==0 else [])
            and task.get('dependency_kind')==row.get('dependency_kind')==('afterok' if index==0 else 'afterany')
            and row.get('issued_native_dependencies')==([PRIOR_PARENT] if index==0 else [PRIOR_JOBS[index-1]])
            and row.get('command')==['sbatch','--parsable','--dependency='+('afterok:'+PRIOR_PARENT if index==0 else 'afterany:'+PRIOR_JOBS[index-1]),str(script)]
            and tokens.count(str(PRIMARY_HELPER))==1,'Exact failed partial task/native/script/intent/DAG required')
        expected=['--mode','run','--policy-descriptor',str(PRIOR_DESCRIPTOR),'--policy-descriptor-sha256',PRIOR_DESCRIPTOR_SHA,
            '--native-plan',str(PRIOR_PLAN),'--native-ledger',str(PRIOR_LEDGER),'--native-task',name,'--native-script',str(script),'--segment',str(index)]
        require(tokens[tokens.index(str(PRIMARY_HELPER))+1:]==expected,'Exact historical partial native arguments required')
        c['pins'][str(script)]=rawsha
    parents=c['native_reader']([PRIOR_PARENT],{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(parents)=={PRIOR_PARENT},'Exact failed partial handoff native record required');parent=parents[PRIOR_PARENT]
    owned_terminal(parent,PRIOR_PARENT)
    require(parent.get('state')=='COMPLETED' and parent.get('exit_code')=='0:0' and parent.get('cpus')==4
        and parent.get('elapsed_seconds')==654 and parent.get('unallocated_metadata') is False,'Exact completed prior handoff allocation required')
    tres(parent.get('alloc_tres',''),4,'16G')
    require(slurm.completed_native_dependency(ROOT,PRIOR_PARENT) is not None,'Prior handoff native completion must be finalized')
    receipt=json.loads(packet(PRIOR_RECEIPT,PRIOR_RECEIPT_SHA)[0])
    require(receipt.get('status')=='three_unstarted_primary_successors_retired_and_four_source_bound_controllers_released'
        and receipt.get('native_job_id')==PRIOR_PARENT and receipt.get('old_jobs')==list(OLD_JOBS) and receipt.get('new_jobs')==list(PRIOR_JOBS)
        and receipt.get('plan')=={'path':str(PRIOR_PLAN),'sha256':PRIOR_PLAN_SHA}
        and receipt.get('ledger')=={'path':str(PRIOR_LEDGER),'sha256':PRIOR_LEDGER_SHA}
        and receipt.get('policy_descriptor')=={'path':str(PRIOR_DESCRIPTOR),'sha256':PRIOR_DESCRIPTOR_SHA}
        and receipt.get('native_selftests')=={'path':str(PRIOR_AREA/'native-handoff-qualification.json'),'sha256':PRIOR_NATIVE_TEST_SHA}
        and receipt.get('partial_reconciliation')=={'path':str(PRIOR_AREA/'partial-reconciliation.json'),'sha256':PRIOR_PARTIAL_PROOF_SHA}
        and receipt.get('qualification')=={'path':str(PRIMARY_SUPPORT/'qualification.json'),'sha256':Q_RECEIPT_SHA,'job_id':Q_JOB}
        and all(receipt.get(key) is value for key,value in FALSE_FLAGS.items()),'Exact completed prior handoff receipt and retired original lineage required')
    for path,pin in ((PRIOR_RECEIPT,PRIOR_RECEIPT_SHA),(PRIOR_AREA/'native-handoff-qualification.json',PRIOR_NATIVE_TEST_SHA),
            (PRIOR_AREA/'partial-reconciliation.json',PRIOR_PARTIAL_PROOF_SHA),(PRIOR_AREA/'old-controller-final-native.json',PRIOR_ORIGINAL_PROOF_SHA)):
        packet(path,pin);c['pins'][str(path)]=pin
    cleanup=json.loads(packet(PRIOR_CLEANUP_RECEIPT,PRIOR_CLEANUP_RECEIPT_SHA)[0])
    require(cleanup.get('status')=='three_exact_unstarted_bad_descriptor_successors_retired'
        and cleanup.get('first_failed_native_preserved')==PRIOR_JOBS[0] and cleanup.get('jobs')==list(reversed(PRIOR_JOBS[1:]))
        and cleanup.get('original_jobs_modified') is False and cleanup.get('scientific_result') is False,
        'Exact separately recorded pending-only predecessor cleanup required')
    failure=packet(PRIOR_FAILURE_LOG,PRIOR_FAILURE_LOG_SHA,4096)[0]
    require(len(failure)==1235 and b'ValueError: Exact primary policy namespace required' in failure,
        'Exact prior namespace-refusal log must remain preserved')
    c['pins'].update({str(PRIOR_CLEANUP_RECEIPT):PRIOR_CLEANUP_RECEIPT_SHA,str(PRIOR_FAILURE_LOG):PRIOR_FAILURE_LOG_SHA})
    c['pins'].update({str(PRIOR_PLAN):PRIOR_PLAN_SHA,str(PRIOR_LEDGER):PRIOR_LEDGER_SHA,
        str(PRIOR_DESCRIPTOR):PRIOR_DESCRIPTOR_SHA,str(PRIOR_SOURCE):PRIOR_SOURCE_SHA})
    return {'plan':plan,'ledger':ledger,'tasks':plan['tasks'],'jobs':list(PRIOR_JOBS)}



def prior_terminal(c):
    rows=c['native_reader'](list(PRIOR_JOBS),{},owner='pwa209',account='def-ptewarie_cpu')
    require(set(rows)==set(PRIOR_JOBS),'Every exact previous primary controller terminal record required')
    first=rows[PRIOR_JOBS[0]];owned_terminal(first,PRIOR_JOBS[0])
    require(first.get('state')=='FAILED' and first.get('exit_code')=='1:0' and first.get('cpus')==1
        and first.get('elapsed_seconds')==4 and first.get('unallocated_metadata') is False
        and first.get('node_list') not in {None,'','None','None assigned','(null)'}
        and first.get('start')=='2026-10-23T12:22:45' and first.get('end')=='2026-10-23T12:22:49',
        'Exact prior namespace-refusal allocation must remain terminal')
    tres(first.get('alloc_tres',''),1,'64G')
    for job in PRIOR_JOBS[1:]:unused_cancelled(rows[job],job)
    return rows


def verify_prior_handoff(c):
    require(c.get('registration_locked') is True and c.get('selftest_proof',{}).get('status')=='complete_native_handoff_contracts_completion_pending',
        'Prior handoff verification requires the permanent registration lock and actual native selftests')
    prior_bindings(c);native=prior_terminal(c);original=terminal_old(c);verify_sources(c)
    proof={'status':'prior_handoff_and_retired_primary_chain_verified_read_only','parent_native_job_id':PRIOR_PARENT,
        'receipt':locator(PRIOR_RECEIPT),'plan':locator(PRIOR_PLAN),'ledger':locator(PRIOR_LEDGER),
        'prior_controllers':native,'original_controllers':original,'no_prior_or_original_scheduler_mutation_performed':True,**FALSE_FLAGS}
    exclusive(c['area']/'prior-handoff-preserved.json',proof);c['prior_handoff_proof']=proof;return proof


def run_descriptor(c):
    require(c.get('registration_locked') is True and c.get('selftest_proof',{}).get('status')=='complete_native_handoff_contracts_completion_pending',
        'Run policy validation requires the permanent registration lock and actual native selftests')
    require(re.fullmatch('[1-9][0-9]*',c['job']) and PRIMARY_SUPPORT==PRIMARY_SUPPORT.resolve() and PRIMARY_SUPPORT.is_dir()
        and PRIMARY_SUPPORT.is_relative_to(ROOT) and all(not p.is_symlink() for p in [PRIMARY_SUPPORT,*PRIMARY_SUPPORT.parents] if p.is_relative_to(ROOT)),
        'Exact canonical qualified primary policy namespace required')
    parent=PRIMARY_SUPPORT/'handoffs'
    require(not parent.is_symlink(),'Primary handoff policy directory must not be a symlink')
    parent.mkdir(mode=0o700,exist_ok=True)
    require(parent==parent.resolve() and parent.is_dir(),'Canonical primary handoff policy directory required')
    directory=parent/('native-'+c['job'])
    require(not directory.exists() and not directory.is_symlink(),'Run policy publication cannot replay an existing native handoff')
    directory.mkdir(mode=0o700);path=directory/'run-policy.json';exclusive(path,c['run_policy']);pin=digest(path)
    policy,manifest,pins=c['primary'].policy_sources(path,pin)
    expected={str(path),str(PRIMARY_SUPPORT/'source-manifest.json'),str(PRIMARY_HELPER),
        str(PRIMARY_SUPPORT/'test_primary_external_census_v1_20261023.py'),
        str(OLD_FRAMEWORK.resolve()/'tests/test_pipeline.py'),str(OLD_FRAMEWORK.resolve()/'tests/test_budget.py')}
    require(policy==c['run_policy'] and manifest==c['primary_manifest'] and set(pins)==expected and pins.get(str(path))==pin,
        'Actual emitted run policy must pass the complete frozen primary policy_sources closure')
    c['pins'].update(pins);verify_sources(c)
    exclusive(c['area']/'run-policy-validation.json',{'descriptor':{'path':str(path),'sha256':pin},
        'qualified_primary_helper':locator(PRIMARY_HELPER),'validated_policy_source_pins':pins,
        'complete_policy_equal_to_finalized_qualification':True,'complete_source_manifest_equal':True,
        'validation_precedes_all_scheduler_mutations':True,**FALSE_FLAGS})
    return path


def orchestrate(c,*,runner=subprocess.run,sleeper=time.sleep):
    require(c.get('selftest_proof',{}).get('status')=='complete_native_handoff_contracts_completion_pending','Actual own native selftests must precede all scheduler changes')
    pack=None
    try:
        with c['registry']['registration_lock'](c['area']/'registration-lock',c['end'],role='controller-start'):
            c['registration_locked']=True
            try:
                verify_sources(c);descriptor=run_descriptor(c);old_bindings(c);verify_prior_handoff(c)
                c['admission']('primary-handoff-before-new-registration',cpus=1,gpus=0,hours=72)
                pack=freeze_new(c,descriptor);c['pins'][str(pack['path'])]=digest(pack['path'])
                for task in pack['value']['tasks']:c['pins'][task['script']]=digest(task['script'])
                exclusive(c['area']/'all-new-held-submission-intent.json',{'plan':str(pack['path']),'ledger':str(pack['ledger']),
                    'afterok_handoff':c['job'],'afterok_first_provider':FIRST_PROVIDER,'automatic_replay_permitted':False})
                def held(command,**kwargs):
                    verify_sources(c);require(command[0]=='sbatch' and '--hold' not in command,'One exact held submission required')
                    require(not any(token.startswith('--chdir') or token=='-D' for token in command),'Native working directory option may not be overridden')
                    return runner(['sbatch','--hold','--chdir='+str(OLD_FRAMEWORK.resolve()),*command[1:]],timeout=30,**kwargs)
                ledger=slurm.submit(pack['path'],pack['ledger'],runner=held)
                jobs=[ledger['tasks'][t['name']]['job_id'] for t in pack['value']['tasks']]
                require(len(jobs)==len(set(jobs))==4 and not set(jobs)&{*OLD_JOBS,c['job'],Q_JOB,FIRST_PROVIDER},'Four distinct new scalar controllers required')
                c['pins'][str(pack['ledger'])]=digest(pack['ledger'])
                c['admission']('primary-handoff-all-new-registered',cpus=0,gpus=0,hours=1)
                proofs={job:new_held(c,task,job,ledger['tasks'][task['name']],i,jobs) for i,(task,job) in enumerate(zip(pack['value']['tasks'],jobs))}
                exclusive(c['area']/'all-four-new-held-native-proof.json',proofs)
                provider_viable(c)
                terminal_old(c);prior_terminal(c);verify_sources(c)
                exclusive(c['area']/'release-intent.json',{'jobs':jobs,'release_order':list(reversed(jobs)),'old_terminal_required':True,'automatic_replay_permitted':False})
            finally:c['registration_locked']=False
        for i in reversed(range(4)):
            task=pack['value']['tasks'][i];job=jobs[i]
            def check(task=task,job=job,i=i):
                terminal_old(c);prior_terminal(c);return new_held(c,task,job,ledger['tasks'][task['name']],i,jobs)
            mutation(c,'release-new-'+job,['scontrol','release',job],runner=runner,precheck=check)
        verify_sources(c);terminal_old(c);prior_terminal(c)
        receipt={'status':'four_source_bound_primary_controllers_released_after_prior_terminal_handoff',
            'native_job_id':c['job'],'old_jobs':list(OLD_JOBS),'new_jobs':jobs,'plan':locator(pack['path']),'ledger':locator(pack['ledger']),
            'policy_descriptor':locator(descriptor),'qualification':c['qualification'],'native_selftests':locator(c['area']/'native-handoff-qualification.json'),
            'native_handoff_completion_required_before_new_controllers_start':True,'first_provider_afterok':FIRST_PROVIDER,
            'primary_qualification_coverage_claimed_for_handoff':False,'original_primary_lease_limits_retained':True,
            'workers_or_journals_modified':False,'canonical_plan_ledger_scripts_modified':False,**FALSE_FLAGS}
        receipt['prior_handoff_proof']=locator(c['area']/'prior-handoff-preserved.json')
        receipt['original_retirement_performed_by_this_handoff']=False
        receipt['prior_handoff']={'native_job_id':PRIOR_PARENT,'receipt':locator(PRIOR_RECEIPT)}
        exclusive(c['area']/'receipt.json',receipt);return receipt
    except BaseException as error:
        exclusive(c['area']/'handoff-failure.json',{'status':'needs_explicit_reconciliation','exception':type(error).__name__,
            'message':str(error)[:4000],'new_controllers_preserved_held_or_afterok_blocked':True,'automatic_replay_permitted':False,
            'no_automatic_cleanup_retry_release_or_cancel':True,**FALSE_FLAGS});raise


def manifest(path,pin):
    require(Path(path)==SUPPORT/'source-manifest.json','Exact handoff manifest path required')
    value=json.loads(packet(path,pin,100_000)[0])
    require(set(value)=={'kind','files','tests','primary_source_manifest','qualification_policy_descriptor'}
        and value['kind']=='primary-external-census-handoff-source-manifest-v1'
        and set(value['files'])=={SELF_NAME,TEST_NAME} and set(value['tests'])=={'count','roster_sha256'}
        and type(value['tests']['count']) is int and value['tests']['count']>0
        and re.fullmatch('[0-9a-f]{64}',str(value['tests']['roster_sha256'])),'Exact own source/test closure required')
    for name,h in value['files'].items():packet(SUPPORT/name,h,200_000)
    require(Path(__file__)==SUPPORT/SELF_NAME and digest(Path(__file__))==value['files'][SELF_NAME],'Actual frozen handoff helper required')
    require(value['primary_source_manifest']=={'path':str(PRIMARY_SUPPORT/'source-manifest.json'),'sha256':PRIMARY_MANIFEST_SHA},'Exact separately qualified primary source closure required')
    primary_manifest=json.loads(packet(PRIMARY_SUPPORT/'source-manifest.json',PRIMARY_MANIFEST_SHA,100_000)[0])
    require(primary_manifest['files'].get(PRIMARY_HELPER.name)==PRIMARY_HELPER_SHA,'Frozen primary helper changed')
    for name,h in primary_manifest['files'].items():
        require(Path(name).name==name,'Primary source basename required');packet(PRIMARY_SUPPORT/name,h,200_000)
    q=value['qualification_policy_descriptor']
    require(set(q)=={'path','sha256'} and Path(q['path']).is_relative_to(PRIMARY_SUPPORT) and q['sha256']==Q_POLICY_SHA,'Exact registered Q469 policy descriptor required')
    packet(q['path'],q['sha256'],100_000)
    return value,primary_manifest


def registration(plan_path,ledger_path,task_name,script_path,*,expected_source=SOURCE):
    plan=json.loads(packet(plan_path)[0]);ledger=json.loads(packet(ledger_path)[0])
    require(plan.get('root')==str(ROOT) and plan.get('release_identity',{}).get('sha256')==ledger.get('source_sha256')==expected_source
        and plan.get('config_sha256')==ledger.get('config_sha256')==CONFIG and canonical_hash(plan)==ledger.get('plan_sha256'),
        'Actual registered source/config pair required')
    selected=[t for t in plan.get('tasks',[]) if t.get('name')==task_name]
    require(len(selected)==len(plan['tasks'])==1,'One exact native registered task required')
    task=selected[0];row=ledger['tasks'][task_name];text=packet(script_path)[0].decode()
    require(task.get('script')==str(script_path) and task['command'] in text and canonical_hash(text)==task['script_sha256']==row['script_sha256']
        and row.get('state')=='submitted' and re.fullmatch('[1-9][0-9]*',str(row.get('job_id',''))),'Actual confirmed native script/job required')
    return task,row,{'plan':locator(plan_path),'ledger':locator(ledger_path),'script':locator(script_path),'task_name':task_name,'native_job_id':row['job_id']}


def native_allocation(job,task_name,script,reader,area,*,wall=time.time):
    value=native_fields(reader(job,area/'native-own'))
    require(value.get('JobId')==job and value.get('UserId')=='pwa209(3164768)' and value.get('Account')=='def-ptewarie_cpu'
        and value.get('JobState')=='RUNNING' and value.get('JobName')=='hhw-'+task_name and value.get('Command')==str(script)
        and value.get('NumCPUs')=='4' and value.get('NumNodes') in {'1','1-1'} and value.get('MinMemoryNode') in {'16G','16384M'}
        and value.get('TimeLimit') in {'01:00:00','1:00:00','0-01:00:00'} and 'ArrayJobId' not in value and 'ArrayTaskId' not in value,
        'Owned scalar4CPU16G1h exact handoff allocation required')
    for field in ('ReqTRES','AllocTRES'):tres(value.get(field,''),4,'16G')
    end=datetime.fromisoformat(value.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp();env=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(env) and abs(env-end)<=2 and wall()+1200<min(env,end),'Finite own native end/margin required')
    return value,min(env,end)


def qualification(c):
    for path,pin in ((Q_PLAN,Q_PLAN_SHA),(Q_LEDGER,Q_LEDGER_SHA),(Q_SCRIPT,Q_SCRIPT_SHA)):packet(path,pin)
    task,row,reg=registration(Q_PLAN,Q_LEDGER,'recovery-469',Q_SCRIPT)
    require(row['job_id']==Q_JOB and task['resources']=={'cpus':4,'memory':'64G','hours':1,'gpu':False,'array':None},'Exact actual Q469 producer required')
    tokens=shlex.split(task['command']);require(tokens.count(str(PRIMARY_HELPER))==1,'Actual Q469 helper invocation required')
    tail=tokens[tokens.index(str(PRIMARY_HELPER))+1:];args=c['primary'].parser().parse_args(tail)
    require(args.mode=='qualify' and args.segment==0 and str(args.policy_descriptor)==c['manifest']['qualification_policy_descriptor']['path']
        and args.policy_descriptor_sha256==Q_POLICY_SHA,'Exact Q469 native policy/arguments required')
    qpolicy,pmanifest,ppins=c['primary'].policy_sources(args.policy_descriptor,Q_POLICY_SHA)
    require(qpolicy['qualification'] is None and pmanifest==c['primary_manifest'],'Exact Q469 frozen policy/source closure required')
    actual,_=c['primary'].own_registration(args,Q_JOB,tail,'qualify',Q_POLICY_SHA);require(actual==reg,'Actual Q469 registration changed')
    observed=c['native_reader']([Q_JOB],{},owner='pwa209',account='def-ptewarie_cpu');native=observed.get(Q_JOB,{})
    require(native.get('state')=='COMPLETED' and native.get('exit_code')=='0:0' and native.get('job_id_raw')==Q_JOB
        and native.get('user')=='pwa209' and native.get('account')=='def-ptewarie_cpu' and native.get('unallocated_metadata') is False
        and native.get('cpus')==4 and type(native.get('elapsed_seconds')) is int and 0<native['elapsed_seconds']<=3600
        and 'gpu' not in native.get('alloc_tres','').lower() and slurm.completed_native_dependency(ROOT,Q_JOB) is not None,
        'Actual positive finalized owned Q469 completion required before scheduler mutation')
    qpath=PRIMARY_SUPPORT/'qualification.json';qraw,qsha=packet(qpath,Q_RECEIPT_SHA)
    binding={'path':str(qpath),'sha256':qsha,'job_id':Q_JOB};run_policy=dict(qpolicy);run_policy['qualification']=binding
    def refresh(label):
        with c['registry']['registration_lock'](c['area']/(label+'-lock'),c['end'],role='controller-start'):
            return c['admission'](label,cpus=0,gpus=0,hours=1)
    c['primary'].finalized_primary_qualification(run_policy,pmanifest,slurm,refresh)
    c['pins'].update(ppins);c['pins'][str(qpath)]=qsha
    for item in reg.values():
        if type(item) is dict:c['pins'][item['path']]=item['sha256']
    c.update(run_policy=run_policy,qualification=binding)
    return binding


def first_provider(c):
    census=c['run_policy']['census'];schedule=census['provider_schedule'];primary=c['primary']
    closure=json.loads(packet(primary.CENSUS_SUPPORT/'source-manifest.json',primary.CENSUS_MANIFEST_SHA,100_000)[0])
    outer=loaded(primary.CENSUS_SUPPORT/primary.OUTER_NAME,closure['files'][primary.OUTER_NAME],'primary_handoff_census_provenance')
    c['pins'].update(outer.qualification(census))
    provider_path=primary.CENSUS_SUPPORT/'run-fresh-storage-census-v6-20261023.py'
    require(schedule['helper']=={'path':str(provider_path),'sha256':closure['files'][provider_path.name]},'Exact qualified finite provider source required')
    plan=json.loads(packet(schedule['plan']['path'],schedule['plan']['sha256'])[0]);ledger=json.loads(packet(schedule['ledger']['path'],schedule['ledger']['sha256'])[0])
    task=plan['tasks'][0];row=ledger['tasks'][task['name']]
    require(task['name']=='rolling-storage-census-01' and row['job_id']==FIRST_PROVIDER,'Exact first registered provider native identity required')
    reg={'path':task['script'],'sha256':digest(task['script'])}
    _,tail=outer.registered(schedule['plan'],schedule['ledger'],task['name'],reg,FIRST_PROVIDER,
        helper=provider_path,hours=6,cpus=8,memory='64G')
    require(len(tail)==26 and len(set(tail[::2]))==13,'Complete actual provider arguments required')
    provider=loaded(provider_path,schedule['helper']['sha256'],'primary_handoff_registered_provider')
    args=SimpleNamespace(**{k[2:].replace('-','_'):v for k,v in zip(tail[::2],tail[1::2])})
    require(provider.provider_registration(args,FIRST_PROVIDER,c['config'])['slot_count']==64,'Complete actual finite provider chain required')
    provider_viable(c)
    c['pins'].update({schedule['plan']['path']:schedule['plan']['sha256'],schedule['ledger']['path']:schedule['ledger']['sha256'],reg['path']:reg['sha256']})
    return {'job_id':FIRST_PROVIDER,'registered_source_verified':True,'completion_claimed':False}


def provider_viable(c):
    native=c['native_reader']([FIRST_PROVIDER],{},owner='pwa209',account='def-ptewarie_cpu').get(FIRST_PROVIDER,{})
    require(native.get('job_id_raw')==FIRST_PROVIDER and native.get('user')=='pwa209' and native.get('account')=='def-ptewarie_cpu'
        and native.get('state') in {'PENDING','CONFIGURING','RUNNING','COMPLETING','COMPLETED'},'First provider must remain a viable owned actual native dependency')
    if native['state']=='COMPLETED':
        proof=slurm.completed_native_dependency(ROOT,FIRST_PROVIDER)
        require(native.get('exit_code')=='0:0' and native.get('cpus')==8 and type(native.get('elapsed_seconds')) is int
            and 0<native['elapsed_seconds']<=21600 and proof is not None,'Completed first provider must have positive finalized8CPU0:0 proof')
    return native


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv);parser=argparse.ArgumentParser(description=__doc__)
    for name in ('native-plan','native-ledger','native-script','source-manifest'):parser.add_argument('--'+name,type=Path,required=True)
    for name in ('native-task','source-manifest-sha256'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args(argv);os.environ['TZ']='UTC'
    if hasattr(time,'tzset'):time.tzset()
    value,pmanifest=manifest(args.source_manifest,args.source_manifest_sha256);job=str(native_job_id() or '')
    require(re.fullmatch('[1-9][0-9]*',job) and os.geteuid()==3164768 and os.environ.get('USER')=='pwa209'
        and os.environ.get('SLURM_CPUS_PER_TASK')=='4' and os.environ.get('HHW_GPU_COUNT')=='0' and os.environ.get('HHW_MAX_HOURS')=='1',
        'Owned actual native4CPU16G1h handoff context required')
    import horsehair
    require(Path(horsehair.__file__).resolve()==FRAMEWORK/'src/horsehair/__init__.py','Handoff must use the isolated qualified4f administration package')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval allocation required')
    config=read_json(PRIMARY/'configs/study.json');require(canonical_hash(config)==CONFIG and read_json(FRAMEWORK/'configs/study.json')==config,'Full original science configuration required')
    area=SUPPORT/('native-'+job);require(not area.exists() and not area.is_symlink(),'Handoff cannot replay existing evidence');area.mkdir(mode=0o700)
    task,row,own=registration(args.native_plan,args.native_ledger,args.native_task,args.native_script)
    require(row['job_id']==job and task['resources']=={'cpus':4,'memory':'16G','hours':1,'gpu':False,'array':None}
        and task.get('external_dependencies')==[Q_JOB] and task.get('dependency_kind')=='afterok'
        and task.get('primary_external_census_handoff')=={'helper':{'path':str(Path(__file__)),'sha256':value['files'][SELF_NAME]},
            'source_manifest':{'path':str(args.source_manifest),'sha256':args.source_manifest_sha256}},'Exact own afterokQ469 native resource/source intent required')
    command=shlex.split(task['command']);require(command.count(str(Path(__file__)))==1 and command[command.index(str(Path(__file__)))+1:]==argv,'Exact own registered native arguments required')
    for path,pin in ((GUARD,GUARD_SHA),(SCHEDULER,SCHEDULER_SHA),(REGISTRY,REGISTRY_SHA),(POLICY,POLICY_SHA)):packet(path,pin,200_000)
    scheduler=runpy.run_path(str(SCHEDULER))['run_scontrol_job'];allocation,end=native_allocation(job,args.native_task,args.native_script,scheduler,area)
    def expired(signum,frame):raise TimeoutError('Finite native handoff finalization margin exhausted')
    signal.signal(signal.SIGALRM,expired);signal.alarm(max(1,int(end-time.time()-180)))
    runpy.run_path(str(GUARD))['install'](area/'accounting-reads')
    registry=runpy.run_path(str(REGISTRY));registry['install'](area/'registered-reads',registration_mode='controller-already-locked',allocation_deadline_unix=end)
    from horsehair import campaign,repair
    require(campaign._repair_native_states is repair._repair_native_states,'Native accounting guard must precede campaign import binding')
    policy=load_resource_policy(POLICY,POLICY_SHA,root=ROOT,config=config)
    primary=loaded(PRIMARY_HELPER,PRIMARY_HELPER_SHA,'qualified_primary_handoff_api')
    pins={str(SUPPORT/name):pin for name,pin in value['files'].items()}
    pins.update({str(PRIMARY_SUPPORT/name):pin for name,pin in pmanifest['files'].items()})
    pins.update({str(args.source_manifest):args.source_manifest_sha256,str(PRIMARY_SUPPORT/'source-manifest.json'):PRIMARY_MANIFEST_SHA,
        value['qualification_policy_descriptor']['path']:Q_POLICY_SHA,str(GUARD):GUARD_SHA,str(SCHEDULER):SCHEDULER_SHA,str(REGISTRY):REGISTRY_SHA,str(POLICY):POLICY_SHA})
    for item in own.values():
        if type(item) is dict:pins[item['path']]=item['sha256']
    admission=lambda label,**resources:campaign.combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],area,label,resource_policy=policy,**resources)
    c={'job':job,'area':area,'end':end,'config':config,'policy':policy,'manifest':value,'primary_manifest':pmanifest,
        'manifest_binding':{'path':str(args.source_manifest),'sha256':args.source_manifest_sha256},'primary':primary,'registry':registry,
        'scheduler':scheduler,'native_reader':repair._repair_native_states,'admission':admission,'pins':pins,'allocation':allocation,'old_jobs':[]}
    with resource_lease(ROOT,config,cpus=4,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
        self_qualification(c);verify_sources(c)
        with registry['registration_lock'](area/'qualification-accounting-lock',end,role='controller-start'):
            admission('primary-handoff-finalized-qualification-accounting',cpus=0,gpus=0,hours=1)
        qualification(c);first_provider(c);verify_sources(c)
        result=orchestrate(c)
    signal.alarm(0);print(json.dumps({'status':result['status'],'old_jobs':result['old_jobs'],'new_jobs':result['new_jobs']},sort_keys=True));return result


if __name__=='__main__':main()

