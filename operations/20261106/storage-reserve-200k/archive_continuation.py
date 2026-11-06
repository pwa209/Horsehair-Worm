"""Finite continuation of the existing, verified orthology sequence-copy archive."""
from pathlib import Path, PurePosixPath
from datetime import datetime, timezone
import argparse
import hashlib
import importlib.util
import json
import os
import re
import shlex
import stat
import subprocess
import time
import xml.etree.ElementTree as ET

ROOT = Path('/scratch/pwa209/horsehair-worm')
ORIGINAL_JOB = '8510943'
ORIGINAL_WRAPPER_SHA = 'fd26774ad98b1a20703263cc0c8f468fbfe4b0877544dd6da4798e509e36a6eb'
ORIGINAL_REQUEST_SHA = '25bf39cd70a09ee4321415e80aa8858e30add550a90e5d98ab8676d5a5103564'
HELPER_SHA = 'e8a599304f4ac2561339114e7965f3cca2f06c87d565822bb32aabbdf322d979'
ARCHIVE_SHA = '1ef9c2985932b665860a5badf689a076857c3dbbfe311e74d1c2ddec1d6db558'
INDEX_SHA = '550430addad2ed020b7fc87e021a2423a09cd2641f5f9964cfccf5c1f5e03364'
INTENT_SHA = '16935a92f60fd7dd65e788a49f538d2cc374da989d995909138ae8f9fd811c12'
FILES = 43145
CONTENT_BYTES = 51476932
OLD_SUPPORT = 'maintenance/orthology-sequence-archive-support-20261104'
CASE = 'results/storage-archives/orthology-sequences-20261104'
CONTINUATION = CASE+'/continuation-20261106'
JOB_NAME = 'hhw-orthology-archive-continuation-20261106'
FRAMEWORK_SOURCE = '4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1'
BASE_CONFIG = '446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
NEW_PHASE = 'storage-reserve-200k-v2-census-20261106'
POLICY_NAMESPACE = 'maintenance/storage-reserve-200k-v2-20261106'
SHARED_MODULE_SHA = '0e8691671a32e2b55cfcd626d461c2708d11d05b00c0491721c6c3be13107f7d'
SHARED_POLICY_SHA = '2e8e2bd2c1f70aa988a9414d2b44d9ffd0f768f8bc2e329747b7839b2855f2d9'
TERMINAL = {'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for raw in iter(lambda:stream.read(1048576),b''):
            digest.update(raw)
    return digest.hexdigest()


def canonical(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def ordinary(path, root, *, directory=False):
    path = Path(path)
    require(path.is_absolute() and path.is_relative_to(root) and path.resolve()==path,
            'Ordinary confined continuation path required')
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode) and info.st_nlink==1,
            'Owned ordinary single-link continuation file required')
    if hasattr(os,'geteuid'):
        require(info.st_uid==os.geteuid(),'Foreign continuation path refused')
    return path


def pin(locator, *, root):
    require(set(locator)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',str(locator['sha256'])),
            'Explicit immutable continuation locator required')
    path = ordinary(locator['path'],root)
    require(sha(path)==locator['sha256'],'Continuation proof bytes changed: '+str(path))
    return path


def module(locator, expected, *, root):
    require(locator['sha256']==expected,'Original continuation dependency source differs')
    path = pin(locator,root=root)
    spec = importlib.util.spec_from_file_location('original_archive_'+expected[:12],path)
    value = importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value


def failed_original_native(stdout):
    rows = [line.split('|') for line in stdout.splitlines() if line.strip()]
    require(len(rows)==1 and len(rows[0])==9,'One exact original allocated archive row required')
    row = rows[0]
    require(row[:7]==[ORIGINAL_JOB,'pwa209','def-ptewarie_cpu','FAILED','1:0','10505','1']
            and 'gpu' not in row[7].lower() and row[8]=='hhw-orthology-sequence-archive-20261104',
            'Actual original archive failed allocation differs')
    return {'job_id':row[0],'user':row[1],'account':row[2],'state':row[3],
            'exit_code':row[4],'elapsed_seconds':10505,'cpus':1,'alloc_tres':row[7],'job_name':row[8]}


def _seconds(text):
    match = re.fullmatch(r'(?:(\d+)-)?(\d+):(\d+):(\d+)',text)
    require(match is not None,'Finite native time limit required')
    days,hours,minutes,seconds = [int(value or 0) for value in match.groups()]
    require(minutes<60 and seconds<60,'Malformed native time limit')
    return ((days*24+hours)*60+minutes)*60+seconds


def _memory_kib(text):
    match = re.fullmatch(r'(\d+)([KMG])',text)
    require(match is not None,'Finite native memory required')
    return int(match[1])*{'K':1,'M':1024,'G':1024*1024}[match[2]]


def census_roster_authority(gate, *, root):
    """Join every held writer to its entire original or new source-bound plan."""
    require(set(gate)=={'terminal','held','old_provider','new_provider'},'Exact census gate schema required')
    require(len(gate['held'])==65,'All64 new writers plus original final writer must be held')
    held={str(item['job_id']):item for item in gate['held']}
    require(len(held)==65,'Unique complete held census writer roster required')
    bound={};matched=set()
    for kind,phase in [('old_provider','study-census-resilience-v1-20261024'),('new_provider',NEW_PHASE)]:
        locator=gate[kind];require(set(locator)=={'plan','ledger_path'},'Exact source-bound census plan/ledger locators required')
        expected=root/'plans'/phase/'plan.json'
        require(locator['plan']['path']==str(expected)
                and locator['ledger_path']==str(root/'state'/(phase+'-submission.json')),
                'Census writer plan/ledger namespace differs')
        path=pin(locator['plan'],root=root);bound[str(path)]=locator['plan']['sha256']
        plan=json.loads(path.read_bytes());ledger=json.loads(ordinary(locator['ledger_path'],root).read_bytes())
        require(plan['root']==str(root) and plan['phase']==phase and len(plan['tasks'])==64
                and plan['release_identity']['sha256']==ledger['source_sha256']==FRAMEWORK_SOURCE
                and plan['config_sha256']==ledger['config_sha256']==BASE_CONFIG
                and ledger['plan_sha256']==canonical(plan),'Census writer source/configuration/plan binding differs')
        tasks={task['name']:task for task in plan['tasks']}
        require(len(tasks)==64 and len(ledger['tasks'])==64,'Entire finite64-slot census plan/ledger required')
        names=(['rolling-storage-census-'+str(index).zfill(2) for index in range(1,65)]
               if kind=='new_provider' else ['rolling-storage-census-'+str(index) for index in range(61,65)])
        require(all(name in tasks for name in names),'Complete declared census writer task roster required')
        for name in names:
            task=tasks[name];row=ledger['tasks'][name];ident=str(row['job_id']);resources=task['resources']
            require(row['state']=='submitted' and row['command'][-1]==task['script']
                    and task['script']==str(root/'plans'/phase/(name+'.sbatch'))
                    and task['script_sha256']==row.get('script_sha256',task['script_sha256'])
                    and task['script_sha256']==canonical(Path(task['script']).read_text())
                    and resources=={'cpus':8,'memory':'64G','hours':6,'gpu':False,'array':None},
                    'Exact submitted census writer task/script/resources required')
            provider=task['rolling_census_provider']
            if kind=='old_provider':
                require(ident=={'rolling-storage-census-61':'6983037','rolling-storage-census-62':'6983056',
                               'rolling-storage-census-63':'6983059','rolling-storage-census-64':'6983060'}[name],
                        'Original four census IDs differ')
            else:
                area=root/POLICY_NAMESPACE/'census'
                require(provider['helper']['path']==str(area/'run-fresh-storage-census-resilience-v1-20261024.py')
                        and provider['source_manifest']['path']==str(area/'source-manifest.json'),
                        'New census provider must use its exact reviewed namespace')
                manifest_path=pin(provider['source_manifest'],root=root)
                helper_path=pin(provider['helper'],root=root)
                manifest=json.loads(manifest_path.read_bytes())
                require(manifest['files'][helper_path.name]==provider['helper']['sha256']
                        and manifest['operational_storage_policy']=={
                            'module':{'path':str(root/POLICY_NAMESPACE/'storage_policy.py'),'sha256':SHARED_MODULE_SHA},
                            'policy':{'path':str(root/POLICY_NAMESPACE/'policy.json'),'sha256':SHARED_POLICY_SHA}},
                        'New census helper/manifest/storage policy binding differs')
                for item in [provider['helper'],provider['source_manifest'],*manifest['operational_storage_policy'].values()]:
                    pin(item,root=root);bound[item['path']]=item['sha256']
            if ident in gate['terminal']:
                require(kind=='old_provider' and ident in {'6983037','6983056','6983059'},
                        'Only original final allocations can be terminal at this boundary')
            else:
                require(ident in held,'Declared finite census writer is missing from held roster')
                item=held[ident]
                require(item['script']['path']==task['script'] and sha(task['script'])==item['script']['sha256']
                        and item['job_name']=='hhw-'+name and item['cpus']==8 and item['memory']=='64G' and item['hours']==6,
                        'Held census writer does not match its source-bound task')
                bound[item['script']['path']]=item['script']['sha256'];matched.add(ident)
    require(matched==set(held),'Held roster includes an unbound or duplicate finite writer')
    return bound


def census_gate(gate, helper, runner, *, root):
    """All declared writers are terminal or positively held throughout removal."""
    census_roster_authority(gate,root=root)
    require(isinstance(gate['terminal'],dict)
            and isinstance(gate['held'],list) and gate['held'],'Exact finite census writer roster required')
    held = {str(item['job_id']):item for item in gate['held']}
    require(len(held)==len(gate['held']) and set(held).isdisjoint(gate['terminal'])
            and all(re.fullmatch('[1-9][0-9]*',ident) for ident in [*held,*gate['terminal']]),
            'Unique scalar census writer identities required')
    require({'6983037','6983056','6983059'}<=set(gate['terminal']) and '6983060' in held,
            'Original three terminal censuses and held final census must be bound')
    for item in held.values():
        pin(item['script'],root=root)
        require(type(item['cpus']) is int and item['cpus']==8 and type(item['hours']) is int
                and 0<item['hours']<=24 and item['memory']=='64G'
                and isinstance(item['job_name'],str) and item['job_name'].startswith('hhw-')
                and 'census' in item['job_name'],'Exact bounded census writer resource identity required')
    response = runner(['squeue','--local','-h','-u','pwa209','-o','%i|%u|%a|%T|%r|%j|%C|%m|%l|%o|%b'],
                      capture_output=True,text=True)
    response.check_returncode();require(not response.stderr,'Actual census writer queue observation required')
    observed = {}
    for line in response.stdout.splitlines():
        if not line.strip():continue
        row=line.split('|');require(len(row)==11,'Complete native census queue row required')
        ident,user,account,state,reason,name,cpus,memory,limit,command,gpus=row
        if user=='pwa209' and name.startswith('hhw-') and 'census' in name:
            require(state not in {'RUNNING','COMPLETING','CONFIGURING'},'A census-related allocation is still active')
        if ident not in held:continue
        expected=held[ident]
        require(user=='pwa209' and account=='def-ptewarie_cpu' and state=='PENDING' and reason=='JobHeldUser'
                and name==expected['job_name'] and cpus==str(expected['cpus'])
                and _memory_kib(memory)==_memory_kib(expected['memory']) and _seconds(limit)==expected['hours']*3600
                and command==expected['script']['path'] and gpus in {'N/A','(null)',''},
                'Census writer is not the exact owned held allocation: '+ident)
        observed[ident]={'state':state,'reason':reason,'script':expected['script']}
    require(set(observed)==set(held),'A declared held census writer is not visible')
    ids=','.join(sorted(gate['terminal']))
    response=runner(['sacct','-X','-n','-P','-j',ids,
                     '--format=JobIDRaw,User,Account,State,ExitCode,AllocCPUS,ElapsedRaw,AllocTRES%256'],
                    capture_output=True,text=True)
    response.check_returncode();require(not response.stderr,'Actual terminal census accounting required')
    rows=[line.split('|') for line in response.stdout.splitlines() if line.strip()]
    require(len(rows)==len(gate['terminal']) and {row[0] for row in rows}==set(gate['terminal']),
            'All original terminal writer allocations must be observable')
    for row in rows:
        require(len(row)==8,'Complete native terminal writer row required')
        expected=gate['terminal'][row[0]]
        require(row[1:3]==['pwa209','def-ptewarie_cpu'] and row[3] in TERMINAL
                and row[3]==expected['state'] and row[4]==expected['exit_code'] and row[5]=='8'
                and row[6].isdigit() and int(row[6])>0 and 'gpu' not in row[7].lower(),
                'Original census terminal proof differs')
    return {'status':'all_declared_census_writers_terminal_or_held','held_job_ids':sorted(held),
            'terminal_job_ids':sorted(gate['terminal']),'observed_unix':time.time()}


def verify_native_tests(contract,job,*,root,source):
    require(set(contract)=={'sources','tests','roster_sha256'} and type(contract['tests']) is int
            and contract['tests']>=67 and re.fullmatch('[0-9a-f]{64}',contract['roster_sha256']),
            'Exact source-bound shared-policy and continuation native test contract required')
    parent=Path(source).parent
    required={'storage_policy.py','policy.json','test_storage_policy.py','archive_continuation.py','test_archive_continuation.py'}
    require(set(contract['sources'])=={str(parent/name) for name in required},'Complete native source/test closure required')
    for path,digest in contract['sources'].items():pin({'path':path,'sha256':digest},root=root)
    require(contract['sources'][str(parent/'storage_policy.py')]==SHARED_MODULE_SHA
            and contract['sources'][str(parent/'policy.json')]==SHARED_POLICY_SHA,
            'Native test closure uses a different operational reserve policy')
    junit=ordinary(parent/('native-'+job)/'tests.xml',root)
    require(junit.stat().st_size<=2000000,'Finite native JUnit result required')
    document=ET.fromstring(junit.read_bytes())
    suites=list(document.iter('testsuite'));cases=list(document.iter('testcase'))
    require(suites and all(int(suite.attrib.get(name,'0'))==0 for suite in suites for name in ('failures','errors','skipped'))
            and sum(int(suite.attrib['tests']) for suite in suites)==len(cases)==contract['tests']
            and not any(list(case) for case in cases),'All native policy/continuation tests must actually pass without skips')
    roster=sorted(case.attrib['classname'].rsplit('.',1)[-1]+'::'+case.attrib['name'] for case in cases)
    require(len(roster)==len(set(roster)) and canonical(roster)==contract['roster_sha256'],
            'Exact actual native test roster required')
    return {'tests':len(cases),'roster_sha256':contract['roster_sha256'],
            'junit':{'path':str(junit),'sha256':sha(junit)},'source_pins':contract['sources']}


def load_original(request, *, root):
    original=request['original'];require(set(original)=={'wrapper','request','registration','script','receipt'},
                                         'Exact original archive proof locators required')
    expected_paths={'wrapper':root/OLD_SUPPORT/'archive-orthology-sequence-copies-20261104.py',
                    'request':root/OLD_SUPPORT/'request.json','registration':root/OLD_SUPPORT/'registration.json',
                    'script':root/OLD_SUPPORT/'archive.sbatch','receipt':root/CASE/'receipt.json'}
    for name,path in expected_paths.items():require(original[name]['path']==str(path),'Original archive path differs: '+name)
    wrapper=module(original['wrapper'],ORIGINAL_WRAPPER_SHA,root=root)
    require(original['request']['sha256']==ORIGINAL_REQUEST_SHA,'Exact originally executed archive request required')
    old_request=json.loads(pin(original['request'],root=root).read_bytes())
    require(old_request['case']==str(root/CASE),'Original archive case differs')
    helper=wrapper.load_helper(old_request['helper'],root=root)
    require(old_request['helper']['sha256']==HELPER_SHA,'Original archive integrity helper differs')
    registration=json.loads(pin(original['registration'],root=root).read_bytes())
    require(registration['job_id']==ORIGINAL_JOB and registration['request_sha256']==ORIGINAL_REQUEST_SHA
            and registration['wrapper_sha256']==ORIGINAL_WRAPPER_SHA and registration['native_tests']==53
            and registration['candidate_files']==FILES,'Original executed registration differs')
    script=pin(original['script'],root=root);held=registration['actual_held']
    require(held['JobId']==ORIGINAL_JOB and held['UserId']=='pwa209(3164768)' and held['Account']=='def-ptewarie_cpu'
            and held['Command']==str(script) and held['JobName']=='hhw-orthology-sequence-archive-20261104'
            and held['JobState']=='PENDING' and held['Reason']=='JobHeldUser' and held['NumCPUs']=='1'
            and held['MinMemoryNode'] in {'8G','8192M'} and _seconds(held['TimeLimit'])==3*3600,
            'Original owned scalar archive registration differs')
    text=script.read_text();require(ORIGINAL_REQUEST_SHA in text and ORIGINAL_WRAPPER_SHA in text,
                                  'Original archive script lacks executed request/source bindings')
    bindings,audit=wrapper.metadata_authority(old_request,helper,root=root)
    tasks=wrapper.original_tasks(old_request,helper,root=root)
    for item in original.values():pin(item,root=root);bindings[item['path']]=item['sha256']
    case=ordinary(root/CASE,root,directory=True)
    require(not (case/'removal.json').exists() and not (case/'result.json').exists(),
            'Original archive already entered removal or completed; explicit reconciliation required')
    receipt=json.loads(pin(original['receipt'],root=root).read_bytes())
    require(receipt['status']=='independent_disk_roundtrip_verified_originals_retained'
            and receipt['archive_sha256']==ARCHIVE_SHA and receipt['index_sha256']==INDEX_SHA
            and receipt['intent_sha256']==INTENT_SHA and receipt['files']==FILES and receipt['bytes']==CONTENT_BYTES,
            'Original independently verified archive receipt differs')
    for name,digest in [('index.json',INDEX_SHA),('intent.json',INTENT_SHA)]:
        path=pin({'path':str(case/name),'sha256':digest},root=root);bindings[str(path)]=digest
    index=json.loads((case/'index.json').read_bytes());intent=json.loads((case/'intent.json').read_bytes())
    require(index['kind']=='completed-orthology-derived-sequence-copy-index' and index['source_root']==str(root)
            and index['archive']=={'name':'sequences.tar','sha256':ARCHIVE_SHA}
            and intent['kind']=='completed-orthology-derived-sequence-copies' and intent['source_root']==str(root)
            and index['members']==intent['members'],'Original exact archive roster/intent differs')
    rows=index['members'];wrapper.validate_rows(rows,wrapper.SPECS)
    require(len(rows)==FILES and sum(row['bytes'] for row in rows)==CONTENT_BYTES,'Original audited43145-copy byte roster differs')
    authority=intent['authority']
    require(authority['domain']=='completed_orthology_derived_sequence_copies' and authority['original_tasks']==tasks
            and authority['source_sha256']==wrapper.SOURCE and authority['config_sha256']==wrapper.CONFIG
            and authority['wrapper_sha256']==ORIGINAL_WRAPPER_SHA and authority['request_sha256']==ORIGINAL_REQUEST_SHA
            and authority['audit']==old_request['audit'],'Original completed molecular authority differs')
    for name,digest in authority['immutable_bindings'].items():
        pin({'path':name,'sha256':digest},root=root);bindings[name]=digest
    bindings[str(case/'sequences.tar')]=ARCHIVE_SHA
    return wrapper,helper,old_request,case,rows,bindings,authority


def continue_removal(case,continuation,rows,specs,archive_sha,authority,wrapper,helper,bindings,*,root,
                     checkpoint=lambda:None,boundary=lambda:None,after_unlink=lambda path:None):
    """Reverify the existing tar and all originals, then perform one finite unlink."""
    wrapper.validate_rows(rows,specs);boundary()
    case=ordinary(case,root,directory=True);continuation=Path(continuation)
    require(continuation.parent==case and continuation.resolve()==continuation and not continuation.exists(),
            'Fresh explicit continuation required; never replay a prior attempt')
    require(not (case/'removal.json').exists() and not (case/'result.json').exists(),
            'Original archive removal requires independent reconciliation')
    continuation.mkdir();helper.sync_directory(case)
    helper.atomic(continuation/'intent.json',{'status':'existing_archive_continuation_intent',
        'original_archive_sha256':archive_sha,'files':len(rows),'authority':authority,'no_repacking':True})
    archive=ordinary(case/'sequences.tar',root)
    helper.verify_tar(archive,rows,archive_sha,checkpoint=checkpoint)
    receipt={'status':'independent_existing_tar_roundtrip_verified_originals_retained',
             'original_archive_sha256':archive_sha,'archive_sha256':archive_sha,'files':len(rows),
             'content_bytes':sum(row['bytes'] for row in rows),'authority':authority,'no_repacking':True}
    helper.atomic(continuation/'continuation-receipt.json',receipt)
    bound={**bindings,str(archive):archive_sha,str(continuation/'intent.json'):sha(continuation/'intent.json'),
           str(continuation/'continuation-receipt.json'):sha(continuation/'continuation-receipt.json')}
    guard=wrapper.source_guard(specs,helper,bound,root=root)
    descriptors={relative:(os.open(root/relative,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW) if os.name=='posix' else None)
                 for relative,count in specs}
    def observed(row):
        path=PurePosixPath(row['path'])
        return wrapper.file_observation(root/path.parent.as_posix(),path.name,root=root,
                                        descriptor=descriptors[path.parent.as_posix()])
    def old_absent():
        require(not (case/'removal.json').exists() and not (case/'result.json').exists(),
                'Another original removal/result appeared; no concurrent mutation permitted')
    try:
        for row in rows:
            checkpoint();guard();old_absent()
            require(observed(row)=={key:row[key] for key in ('sha256','bytes','identity')},
                    'Original changed; no continuation unlink permitted')
        boundary();guard();old_absent()
        journal={'status':'removal_intent','removed_count':0,'last_removed':None,
                 'original_archive_sha256':archive_sha,'archive_sha256':archive_sha,
                 'scientific_result':False,'no_repacking':True}
        helper.atomic(continuation/'removal.json',journal)
        try:
            for row in rows:
                checkpoint();guard();old_absent()
                require(observed(row)=={key:row[key] for key in ('sha256','bytes','identity')},
                        'Original changed immediately before continuation unlink')
                path=PurePosixPath(row['path']);descriptor=descriptors[path.parent.as_posix()]
                guard()
                final=(os.stat(path.name,dir_fd=descriptor,follow_symlinks=False) if descriptor is not None
                       else os.lstat(root/row['path']))
                require(wrapper.stamp(final)==row['identity'] and stat.S_ISREG(final.st_mode) and final.st_nlink==1,
                        'Leaf changed at final continuation unlink guard')
                if descriptor is None:os.unlink(root/row['path'])
                else:os.unlink(path.name,dir_fd=descriptor)
                journal.update(removed_count=journal['removed_count']+1,last_removed=row['path'],
                               status='removing_verified_existing_archive_copies')
                after_unlink(row['path'])
                if journal['removed_count']%128==0:helper.atomic(continuation/'removal.json',journal)
            for relative,count in specs:helper.sync_directory(root/relative)
            journal['status']='complete_verified_existing_orthology_archive_removal'
            helper.atomic(continuation/'removal.json',journal)
        except BaseException as error:
            journal.update(status='interrupted_explicit_reconciliation_required',error_type=type(error).__name__)
            helper.atomic(continuation/'removal.json',journal);raise
        return receipt,journal
    finally:
        for descriptor in descriptors.values():
            if descriptor is not None:os.close(descriptor)


def allocation(fields,job,script,environment_end):
    require(fields.get('JobId')==job and fields.get('UserId')=='pwa209(3164768)'
            and fields.get('Account')=='def-ptewarie_cpu' and fields.get('JobState')=='RUNNING'
            and fields.get('JobName')==JOB_NAME and fields.get('Command')==str(script)
            and fields.get('NumCPUs')=='1' and fields.get('NumNodes') in {'1','1-1'}
            and fields.get('MinMemoryNode') in {'8G','8192M'} and _seconds(fields.get('TimeLimit',''))==12*3600
            and fields.get('Requeue')=='0' and 'ArrayJobId' not in fields
            and all('gpu' not in fields.get(key,'').lower() for key in ('AllocTRES','ReqTRES','Gres','TresPerNode','TresPerTask')),
            'Exact owned1CPU8GiB12h scalar continuation allocation required')
    end=datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp()
    require(abs(end-environment_end)<=2 and time.time()+600<end,'Actual finite continuation end differs')
    return min(end,environment_end)


def main(argv=None):
    parser=argparse.ArgumentParser();parser.add_argument('--request',type=Path,required=True)
    parser.add_argument('--request-sha256',required=True);parser.add_argument('--source-sha256',required=True)
    args=parser.parse_args(argv)
    require(os.geteuid()==3164768 and re.fullmatch('[1-9][0-9]*',os.environ.get('SLURM_JOB_ID','')),
            'Owned native archive continuation allocation required')
    require(sha(__file__)==args.source_sha256 and sha(args.request)==args.request_sha256,'Continuation source/request changed')
    require(subprocess.check_output(['hostname','-f'],text=True,timeout=15).strip().endswith('.narval.calcul.quebec'),
            'Narval compute allocation required')
    def run(command,**kwargs):return subprocess.run(command,**{**kwargs,'timeout':90})
    request=json.loads(ordinary(args.request,ROOT).read_bytes());job=os.environ['SLURM_JOB_ID']
    registration_path=ordinary(request['new_registration_path'],ROOT)
    registration=json.loads(registration_path.read_bytes())
    require(registration['job_id']==job and registration['request_sha256']==args.request_sha256
            and registration['source_sha256']==args.source_sha256,'Actual new continuation registration differs')
    require(registration['script_sha256']==registration['script']['sha256'],
            'New continuation script locator and registered SHA differ')
    script=pin(registration['script'],root=ROOT)
    held=registration['actual_held_native']
    require(held['JobId']==job and held['UserId']=='pwa209(3164768)' and held['Account']=='def-ptewarie_cpu'
            and held['JobState']=='PENDING' and held['Reason']=='JobHeldUser' and held['Command']==str(script)
            and held['JobName']==JOB_NAME and held['NumCPUs']=='1' and held['MinMemoryNode'] in {'8G','8192M'}
            and _seconds(held['TimeLimit'])==12*3600 and held['Requeue']=='0',
            'Original positively held new continuation registration required')
    native=run(['scontrol','show','job',job,'-o'],capture_output=True,text=True);native.check_returncode()
    require(not native.stderr,'Actual continuation allocation observation required')
    fields=dict(part.split('=',1) for part in shlex.split(native.stdout) if '=' in part)
    end=allocation(fields,job,script,float(os.environ['SLURM_JOB_END_TIME']))
    native_tests=verify_native_tests(request['native_test_contract'],job,root=ROOT,source=Path(__file__))
    scratch=Path(os.environ.get('SLURM_TMPDIR',''))
    ordinary(scratch,scratch,directory=True)
    wrapper,helper,old_request,case,rows,bindings,old_authority=load_original(request,root=ROOT)
    bindings.update({str(args.request):args.request_sha256,str(Path(__file__)):args.source_sha256,
                     str(registration_path):sha(registration_path),str(script):registration['script']['sha256']})
    bindings.update(census_roster_authority(request['census_gate'],root=ROOT))
    bindings.update(native_tests['source_pins']);bindings[native_tests['junit']['path']]=native_tests['junit']['sha256']
    raw=run(['sacct','-X','-n','-P','-j',ORIGINAL_JOB,
             '--format=JobIDRaw,User,Account,State,ExitCode,ElapsedRaw,AllocCPUS,AllocTRES%256,JobName%80'],
            capture_output=True,text=True);raw.check_returncode();require(not raw.stderr,'Original failed allocation accounting required')
    failed=failed_original_native(raw.stdout)
    raw=run(['sacct','-X','-n','-P','-j','5455844,6632058,8508393',
             '--format=JobIDRaw,User,Account,State,ExitCode,AllocCPUS,ElapsedRaw,AllocTRES%256'],
            capture_output=True,text=True);raw.check_returncode();require(not raw.stderr,'Original completed producer accounting required')
    completed=wrapper.native_proofs(raw.stdout,{'5455844':8,'6632058':8,'8508393':1})
    import horsehair
    from horsehair import core,budget
    framework=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
    require(Path(horsehair.__file__).resolve()==framework/'src/horsehair/__init__.py'
            and core.source_identity(framework)['sha256']==FRAMEWORK_SOURCE,
            'Exact unchanged execution framework required')
    original=Path(old_request['original_source_release'])
    require(core.source_identity(original)['sha256']==wrapper.SOURCE,'Exact unchanged primary scientific source required')
    budget.native_accounting(ROOT,[ORIGINAL_JOB,'5455844','6632058','8508393'],runner=run)
    from horsehair.execution_resources import load_resource_policy
    config=core.read_json(framework/'configs/study.json')
    policy=load_resource_policy(ROOT/'maintenance/execution-resource-policies/af-expanded-20261021-v1/policy.json',
        'f4445fb72d6a0fd8e62fb9a3c5ef43a6a6b1e7bf5a369254ff40d617e992b4e5',root=ROOT,config=config)
    continuation=Path(request['continuation_directory'])
    require(continuation==ROOT/CONTINUATION,'Exact new continuation transaction required')
    guard=wrapper.source_guard(wrapper.SPECS,helper,bindings,root=ROOT);last=[None];gate_proof=[None]
    def boundary():
        require(time.time()+600<end,'Ten-minute continuation finalization reserve exhausted')
        guard();gate_proof[0]=census_gate(request['census_gate'],helper,run,root=ROOT);last[0]=time.monotonic()
    def checkpoint():
        require(time.time()+600<end,'Ten-minute continuation finalization reserve exhausted')
        guard()
        if last[0] is None or time.monotonic()-last[0]>=60:boundary()
    boundary()
    authority={'native_job_id':job,'source_sha256':args.source_sha256,'request_sha256':args.request_sha256,
               'original_native_failed':failed,'original_completed_producers':completed,
               'original_request':request['original']['request'],'original_wrapper':request['original']['wrapper'],
               'original_archive_receipt':request['original']['receipt'],'census_gate':request['census_gate'],
               'initial_census_gate_observation':gate_proof[0],'native_tests':native_tests}
    with budget.resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=12,job_id=job,resource_policy=policy):
        receipt,journal=continue_removal(case,continuation,rows,wrapper.SPECS,ARCHIVE_SHA,authority,
            wrapper,helper,bindings,root=ROOT,checkpoint=checkpoint,boundary=boundary)
    for locator in old_request['binding_documents']+old_request['retained_tables']:wrapper.pin(locator,helper,root=ROOT)
    quota=budget.require_storage_headroom(ROOT,required_files=0,runner=run)
    result={'status':'complete_verified_orthology_sequence_copy_archive_continuation','files_removed':journal['removed_count'],
            'native_job_id':job,'source_sha256':args.source_sha256,'request_sha256':args.request_sha256,
            'original_archive_sha256':ARCHIVE_SHA,'original_wrapper_sha256':ORIGINAL_WRAPPER_SHA,
            'original_request_sha256':ORIGINAL_REQUEST_SHA,
            'continuation_receipt_sha256':sha(continuation/'continuation-receipt.json'),
            'removal_journal_sha256':sha(continuation/'removal.json'),
            'personal_available_files':quota['personal_available_files'],'original_inputs_preserved':True,
            'no_repacking':True,'original_archive_receipt_preserved':True,'scientific_result':False}
    require(result['files_removed']==FILES,'Complete43145-copy removal required before final success')
    helper.atomic(continuation/'result.json',result);print(json.dumps(result),flush=True)


if __name__=='__main__':main()
