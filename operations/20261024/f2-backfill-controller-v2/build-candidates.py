"""Build new local candidates; never change a preserved source or contact Slurm."""
from pathlib import Path
import hashlib
import json

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
BASE_SOURCE = REPO / 'operations/20261023/prestart-three-continuation/run-af-agent-controller-registry-v6-20261022.py'
OUTER_SOURCE = REPO / 'operations/20261023/census-and-controller-resumption-v1/census-v5/run-af-agent-controller-census-policy-v1-20261023.py'
OLD_BASE_SHA = 'e4c3d2fc8f0085900983053d4d4fe2b10cad6ffe9d176cdc8156630fc6921866'
OLD_OUTER_SHA = '120f02d098b6c9afa0ea0df54f590ae381b4227d4892ba0f9ee4acc93e6fec0f'
BASE_NAME = 'run-af-agent-controller-backfill-base-v2-20261024.py'
OUTER_NAME = 'run-af-agent-controller-backfill-policy-v2-20261024.py'
REMOTE = '/scratch/pwa209/horsehair-worm/maintenance/study-af-backfill-controller-support-v2-20261024'

ASSET_SOURCES={
    'run-af-agent-controller-registry-v6-20261022.py':BASE_SOURCE,
    'run-af-agent-controller-census-policy-v1-20261023.py':OUTER_SOURCE,
    'test_af_agent_controller_census_policy_v1_20261023.py':OUTER_SOURCE.with_name('test_af_agent_controller_census_policy_v1_20261023.py'),
    **{name:BASE_SOURCE.with_name(name) for name in (
        'test_registered_snapshot_read_guard_v5_20261022.py','test_f00d_worker_registry_overlay_20261021.py',
        'test_af_agent_controller_registry_v6_20261022.py','registered-snapshot-read-guard-v5-20261022.py',
        'run-f00d-worker-registry-overlay-20261021.py')},
    'native-accounting-read-guard-20261021.py':REPO/'operations/20261021/native-accounting-read-guard-20261021.py',
    'controller-primary-repair-sixth-20261016.py':REPO/'operations/20261022/controller-and-prefix-continuation/controller-primary-repair-sixth-20261016.py',
}
asset_dir=HERE/'qualification-assets';asset_dir.mkdir(exist_ok=True)
asset_pins={}
for name,path in ASSET_SOURCES.items():
    raw=path.read_bytes();(asset_dir/name).write_bytes(raw);asset_pins[name]=hashlib.sha256(raw).hexdigest()
(HERE/'qualification-asset-pins.json').write_text(json.dumps(asset_pins,sort_keys=True,indent=2)+'\n',encoding='utf8',newline='\n')
asset_manifest_sha=hashlib.sha256((HERE/'qualification-asset-pins.json').read_bytes()).hexdigest()


def checked(path, sha):
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == sha, str(path) + ' preserved source changed'
    return raw.decode()


def replace_once(text, before, after):
    assert text.count(before) == 1, repr(before)
    return text.replace(before, after, 1)


base = checked(BASE_SOURCE, OLD_BASE_SHA)
base = replace_once(base, '96-hour segments are registered by the external deployment, never by this file.',
    'segments request at most 96 hours and permit an 8-hour backfill allocation.\n'
    'The external deployment registers the finite chain, never this file.')
base = replace_once(base, "PRESTART_RETRY_TARGETS=", "ORIGINAL_CONTROLLER_SHA256='" + OLD_BASE_SHA + "'\n"
    "BACKFILL_MIN_SECONDS=8*3600\nBACKFILL_MAX_SECONDS=96*3600\n"
    "BACKFILL_SHUTDOWN_RESERVE_SECONDS=4*3600\nCONTROLLER_TICK_SECONDS=180\n\nPRESTART_RETRY_TARGETS=")
# The original registry qualification continues to attest the original immutable
# registry logic. The new wrapper separately requires a new native qualification
# covering the complete new base, wrapper, and scheduler-contract tests.
base = replace_once(base, "qualified.get('controller_sha256')==digest(Path(__file__))",
    "qualified.get('controller_sha256')==ORIGINAL_CONTROLLER_SHA256")
anchor = '\ndef gpu_count(tres):'
window = '''
def native_backfill_window(allocation, environment_end, *, now=None):
    """Use real native end/time fields; never synthesize a 96-hour allocation."""
    now=time.time() if now is None else now
    require(type(now) in (int,float) and math.isfinite(now),'Finite allocation clock required')
    duration=seconds(allocation.get('TimeLimit',''))
    require(BACKFILL_MIN_SECONDS<=duration<=BACKFILL_MAX_SECONDS
        and seconds(allocation.get('TimeMin',''))==BACKFILL_MIN_SECONDS,
        'Native backfill allocation must be within 8 to 96 hours with exact 8-hour TimeMin')
    require(type(environment_end) in (int,float) and math.isfinite(environment_end),
        'Finite native environment end required')
    native_end=datetime.fromisoformat(allocation.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()
    native_start=datetime.fromisoformat(allocation.get('StartTime','')).replace(tzinfo=timezone.utc).timestamp()
    require(math.isfinite(native_end) and math.isfinite(native_start)
        and abs(environment_end-native_end)<=2 and abs(native_end-native_start-duration)<=2
        and native_start<=now+2 and native_end>now+BACKFILL_SHUTDOWN_RESERVE_SECONDS+CONTROLLER_TICK_SECONDS,
        'Native start/end/effective limit/environment end must agree and retain a four-hour shutdown reserve')
    return {'allocation_end_unix':native_end,'effective_seconds':duration,
        'work_deadline_unix':native_end-BACKFILL_SHUTDOWN_RESERVE_SECONDS,
        'shutdown_reserve_seconds':BACKFILL_SHUTDOWN_RESERVE_SECONDS}

'''
base = replace_once(base, anchor, '\n'+window+anchor)
base = replace_once(base, "and allocation.get('Account')==config['paths']['cpu_account'] and seconds(allocation.get('TimeLimit',''))==96*3600,",
    "and allocation.get('Account')==config['paths']['cpu_account'],")
base = replace_once(base,
    "    native_end=datetime.fromisoformat(allocation.get('EndTime','')).replace(tzinfo=timezone.utc).timestamp()\n"
    "    require(math.isfinite(end) and abs(end-native_end)<=2 and end>time.time()+180,'Native finalization margin required')",
    "    backfill_window=native_backfill_window(allocation,end)")
base = replace_once(base, "'scheduler':scheduler,'allocation':allocation,'deadline':end-180,",
    "'scheduler':scheduler,'allocation':allocation,'deadline':backfill_window['work_deadline_unix'],\n"
    "        'backfill_window':backfill_window,")
base = replace_once(base, '        for index in range(1920):\n            if clock()+180>=c[\'deadline\']:break',
    "        # The lease reserves the registered maximum; native accounting records actual use.\n"
    "        maximum_ticks=max(0,math.ceil((c['deadline']-clock())/CONTROLLER_TICK_SECONDS))\n"
    "        require(maximum_ticks<=BACKFILL_MAX_SECONDS//CONTROLLER_TICK_SECONDS,'Bounded effective controller interval required')\n"
    "        for index in range(maximum_ticks):\n            if clock()+CONTROLLER_TICK_SECONDS>=c['deadline']:break")
base = replace_once(base, "            until=min(c['deadline']-180,clock()+180)",
    "            until=min(c['deadline']-CONTROLLER_TICK_SECONDS,clock()+CONTROLLER_TICK_SECONDS)")
base = replace_once(base, "    result={'status':'finite_segment_complete','segment':c['segment'],'native_job_id':c['job_id'],",
    "    result={'status':'finite_segment_complete','segment':c['segment'],'native_job_id':c['job_id'],\n"
    "        'backfill_window':c['backfill_window'],'new_tick_cutoff_unix':c['deadline']-CONTROLLER_TICK_SECONDS,")
base = replace_once(base, "if __name__=='__main__':main()",
    "if __name__=='__main__':raise ValueError('Backfill base requires the separately qualified outer policy entry point')")
(HERE / BASE_NAME).write_text(base, encoding='utf8', newline='\n')
base_sha = hashlib.sha256((HERE / BASE_NAME).read_bytes()).hexdigest()

outer = checked(OUTER_SOURCE, OLD_OUTER_SHA)
outer = replace_once(outer, '"""Finite F2 outer policy: consume qualified external censuses, never scan."""',
    '"""New finite backfill policy; original provider qualification remains separate."""')
outer = replace_once(outer, "BASE_SHA='" + OLD_BASE_SHA + "'",
    "BASE_SHA='" + OLD_BASE_SHA + "'\n"
    "BACKFILL_SUPPORT=ROOT/'maintenance/study-af-backfill-controller-support-v2-20261024'\n"
    "BACKFILL_BASE=BACKFILL_SUPPORT/'" + BASE_NAME + "'\n"
    "BACKFILL_BASE_SHA='" + base_sha + "'\n"
    "ORIGINAL_POLICY_SOURCE=SUPPORT/'run-af-agent-controller-census-policy-v1-20261023.py'\n"
    "ORIGINAL_POLICY_SHA='" + OLD_OUTER_SHA + "'\n"
    "ORIGINAL_PROVIDER_POLICY=ROOT/'maintenance/study-af-external-census-handoff-v1-20261023/native-6865706/external-census-policy.json'\n"
    "ORIGINAL_PROVIDER_POLICY_SHA='2b0e6ebaa0f1b511bca4343d4722d43702cf051f956cce59494ddccae656c517'\n"
    "BACKFILL_TEST_NAME='test_f2_backfill_controller_v2_20261024.py'\n"
    "BACKFILL_TEST_SHA='" + hashlib.sha256((HERE/'test_f2_backfill_controller_v2_20261024.py').read_bytes()).hexdigest() + "'\n"
    "BACKFILL_QUALIFIER_NAME='qualify-f2-backfill-controller-v2-20261024.py'\n"
    "BACKFILL_ASSET_MANIFEST_SHA='" + asset_manifest_sha + "'\n"
    "BACKFILL_ARTIFACT_MAX_BYTES=8_000_000\n"
    "BACKFILL_RUNTIME={'requested_hours':96,'minimum_hours':8,'shutdown_reserve_seconds':14400,'memory':'64G','cpus':1}")
outer = replace_once(outer,
    "    packet(BASE,ROOT/'maintenance',BASE_SHA,200_000)\n"
    "    spec=importlib.util.spec_from_file_location('qualified_external_census_base6',BASE)\n"
    "    base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)\n"
    "    require(digest(BASE)==BASE_SHA,'Frozen controller changed during library import')",
    "    packet(BACKFILL_BASE,ROOT/'maintenance',BACKFILL_BASE_SHA,200_000)\n"
    "    spec=importlib.util.spec_from_file_location('qualified_backfill_base1',BACKFILL_BASE)\n"
    "    base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)\n"
    "    require(digest(BACKFILL_BASE)==BACKFILL_BASE_SHA,'New frozen backfill controller changed during library import')")
outer = replace_once(outer, 'def policy_guard(context):', 'def provider_policy_guard(context):')
outer = replace_once(outer,
    "require(policy==context['policy'] and digest(Path(__file__))==policy['policy_source']['sha256']\n"
    "        and policy['policy_source']['path']==str(Path(__file__)),'Outer policy source/descriptor changed')",
    "require(policy==context['policy'] and digest(ORIGINAL_POLICY_SOURCE)==ORIGINAL_POLICY_SHA\n"
    "        and policy['policy_source']=={'path':str(ORIGINAL_POLICY_SOURCE),'sha256':ORIGINAL_POLICY_SHA},\n"
    "        'Original provider policy source/descriptor changed')")
additional = '''
def backfill_artifact_packet(binding):
    require(type(binding) is dict and set(binding)=={'path','sha256'}
        and re.fullmatch('[0-9a-f]{64}',str(binding['sha256'])),'Exact persisted qualification artifact locator required')
    return packet(binding['path'],BACKFILL_SUPPORT,binding['sha256'],BACKFILL_ARTIFACT_MAX_BYTES)[0]


def qualification_artifact_bounds(q):
    locators={'backfill_junit':q['suite']['junit'],
        **{item['scope']+'_junit':item['junit'] for item in q['regression_suites']},
        **{name:q[name] for name in ('native_accounting_admission','local_fixture_source_proof')}}
    require(set(locators)=={'backfill_junit','new_base_operational_junit','original_provider_junit',
        'native_accounting_admission','local_fixture_source_proof'},'Exact five persisted qualification artifacts required')
    return {'maximum_bytes':BACKFILL_ARTIFACT_MAX_BYTES,'artifacts':{
        name:{**binding,'bytes':len(backfill_artifact_packet(binding))} for name,binding in sorted(locators.items())}}


def backfill_evidence(q):
    bounds=qualification_artifact_bounds(q)
    require(q.get('persisted_artifact_bounds')==bounds,'Actual persisted qualification byte sizes/limits changed')
    fixture=strict(backfill_artifact_packet(q['local_fixture_source_proof']))
    require(fixture.get('fixtures_allocation_local') is True and fixture.get('fixture_quota_is_admission_authority') is False,
        'Regressions must use real allocation-local fixtures without simulated admission authority')
    return {item['path']:item['sha256'] for item in bounds['artifacts'].values()},fixture


def passing_backfill_suite(value,contract):
    require(value.get('test_counts')=={'tests':contract['tests'],'failures':0,'errors':0,'skipped':0}
        and all(type(n) is int for n in value['test_counts'].values())
        and value.get('test_roster_sha256')==contract['roster_sha256'],'Exact source-bound passing new native suite required')
    require(type(value.get('junit')) is dict and set(value['junit'])=={'path','sha256'},'Exact new native JUnit locator required')
    raw=backfill_artifact_packet(value['junit'])
    require(b'<!DOCTYPE' not in raw.upper() and b'<!ENTITY' not in raw.upper(),'Unsafe new JUnit declarations refused')
    tree=ET.fromstring(raw);cases=list(tree.iter('testcase'))
    names=sorted(c.get('classname','').split('.')[-1]+'::'+c.get('name','') for c in cases)
    require(len(cases)==len(set(names))==contract['tests'] and all(c.get('classname') and c.get('name') for c in cases)
        and canonical_hash(names)==contract['roster_sha256']
        and not any(list(c.iter(name)) for c in cases for name in ('failure','error','skipped'))
        and all(all(int(n.get(key,'0'))==0 for key in ('failures','errors','skipped'))
            for n in tree.iter() if n.tag in {'testsuite','testsuites'}),'Actual full new native passing JUnit roster required')
    leaves=[n for n in tree.iter('testsuite') if not n.findall('testsuite')]
    require(leaves and sum(int(n.get('tests','-1')) for n in leaves)==contract['tests'],'Actual new JUnit counts disagree')


def backfill_qualification(policy,*,provider_paths=None):
    """Old provider coverage cannot qualify the newly changed controller."""
    binding=policy['backfill_qualification']
    require(set(binding)=={'path','sha256','job_id'} and re.fullmatch('[1-9][0-9]*',str(binding['job_id'])),
        'Exact new native backfill qualification binding required')
    q=json_packet({k:binding[k] for k in ('path','sha256')},ROOT/'maintenance')
    expected_sources={'base':{'path':str(BACKFILL_BASE),'sha256':BACKFILL_BASE_SHA},
        'outer':{'path':str(Path(__file__)),'sha256':digest(Path(__file__))}}
    require(q.get('status')=='complete_f2_backfill_controller_qualification'
        and type(q.get('returncode')) is int and q['returncode']==0
        and q.get('slurm_job_id')==binding['job_id'] and q.get('source_sha256')==SOURCE and q.get('config_sha256')==CONFIG
        and q.get('source_verified_unchanged') is True and q.get('runtime_contract')==BACKFILL_RUNTIME
        and q.get('original_base')=={'path':str(BASE),'sha256':BASE_SHA}
        and q.get('original_policy')=={'path':str(ORIGINAL_POLICY_SOURCE),'sha256':ORIGINAL_POLICY_SHA}
        and q.get('provider_policy')=={'path':str(ORIGINAL_PROVIDER_POLICY),'sha256':ORIGINAL_PROVIDER_POLICY_SHA}
        and q.get('native_completion_required_separately') is True
        and q.get('sources')==expected_sources and all(q.get(flag) is False for flag in FALSE_FLAGS),
        'Positive new source-bound backfill native qualification required')
    suite=q.get('suite',{});contract=q.get('suite_contract',{})
    require(set(contract)=={'source','source_sha256','tests','roster_sha256'}
        and contract['source']==BACKFILL_TEST_NAME and contract['source_sha256']==BACKFILL_TEST_SHA
        and type(contract['tests']) is int and contract['tests']==40
        and re.fullmatch('[0-9a-f]{64}',str(contract['roster_sha256']))
        and suite.get('source')=={'path':str(BACKFILL_SUPPORT/BACKFILL_TEST_NAME),'sha256':contract['source_sha256']},
        'Actual new backfill suite source/count/roster contract required')
    packet(BACKFILL_SUPPORT/BACKFILL_TEST_NAME,ROOT/'maintenance',contract['source_sha256'],200_000)
    passing_backfill_suite(suite,contract)
    asset_path=BACKFILL_SUPPORT/'qualification-asset-pins.json'
    assets=json_packet({'path':str(asset_path),'sha256':BACKFILL_ASSET_MANIFEST_SHA},ROOT/'maintenance')
    regression=q.get('regression_suites',[])
    require(len(regression)==2 and {item.get('scope') for item in regression}=={'new_base_operational','original_provider'},
        'New base operational and preserved provider regressions required')
    for item in regression:
        expected_count=172 if item['scope']=='new_base_operational' else 71
        expected_names={'test_registered_snapshot_read_guard_v5_20261022.py','test_f00d_worker_registry_overlay_20261021.py',
            'test_af_agent_controller_registry_v6_20261022.py'} if item['scope']=='new_base_operational' else {'test_af_agent_controller_census_policy_v1_20261023.py'}
        require(item.get('test_sources')=={name:assets[name] for name in expected_names}
            and item.get('tested_controller_sha256')==(BACKFILL_BASE_SHA if item['scope']=='new_base_operational' else ORIGINAL_POLICY_SHA)
            and item.get('contract',{}).get('tests')==expected_count,
            'Exact regression source bytes, tested new base and suite counts required')
        if item['scope']=='new_base_operational':
            require(item.get('original_operational_roster_sha256')=='cae9f3349cf709af939c6953b626cf54e9d7ef1a4a0a711874960d89e939195b',
                'Exact original full 172-test operational roster required')
        else:
            require(item['contract'].get('roster_sha256')=='26f42367f6b53579e6ed923fc075c68466871e9296e4beb1a7103a4166384c21',
                'Exact preserved original full 71-test provider roster required')
        passing_backfill_suite(item,item['contract'])
    registration=q['native_registration'];helper=BACKFILL_SUPPORT/BACKFILL_QUALIFIER_NAME
    task,tail=registered(registration['plan'],registration['ledger'],registration['task_name'],registration['script'],binding['job_id'],
        helper=helper,hours=1,cpus=1,memory='64G')
    expected={'--native-plan':registration['plan']['path'],'--native-ledger':registration['ledger']['path'],
        '--native-task':registration['task_name'],'--native-script':registration['script']['path']}
    require(registration['native_job_id']==binding['job_id'] and len(tail)==8 and len(set(tail[::2]))==4
        and dict(zip(tail[::2],tail[1::2]))==expected,'Exact new native qualifier invocation required')
    closure=task.get('backfill_qualification_sources',{})
    expected_names={BACKFILL_BASE.name,Path(__file__).name,BACKFILL_QUALIFIER_NAME,BACKFILL_TEST_NAME,
        'qualification-asset-pins.json','build-backfill-native-plans-v2-20261024.py'}|{'qualification-assets/'+name for name in assets}
    require(closure.get('kind')=='f2-backfill-controller-transport-source-manifest-v2'
        and closure.get('native_support')==str(BACKFILL_SUPPORT) and closure.get('native_framework')==str(FRAMEWORK)
        and set(closure.get('files',{}))==expected_names
        and all(digest(BACKFILL_SUPPORT/name)==sha for name,sha in closure['files'].items()),
        'New qualifier source closure must match its immutable registered task')
    helper_binding=q.get('qualification_helper',{})
    require(set(helper_binding)=={'path','sha256'} and helper_binding['path']==str(helper),
        'Exact new qualification helper locator required')
    packet(helper,ROOT/'maintenance',helper_binding['sha256'],200_000)
    proof=slurm.completed_native_dependency(ROOT,binding['job_id'])
    row=(proof or {}).get('native_records',{}).get(binding['job_id'],{})
    require(row.get('state')=='COMPLETED' and row.get('exit_code')=='0:0'
        and type(row.get('cpus')) is int and row['cpus']==1 and type(row.get('gpus')) is int and row['gpus']==0
        and type(row.get('elapsed_seconds')) is int and 0<row['elapsed_seconds']<=3600,
        'Positive finalized new one-CPU native qualification required')
    paths={value['path']:value['sha256'] for value in expected_sources.values()}
    paths.update({binding['path']:binding['sha256'],suite['source']['path']:suite['source']['sha256'],
        suite['junit']['path']:suite['junit']['sha256'],helper_binding['path']:helper_binding['sha256']})
    paths[str(asset_path)]=BACKFILL_ASSET_MANIFEST_SHA
    for name,sha in assets.items():
        asset=BACKFILL_SUPPORT/'qualification-assets'/name;packet(asset,ROOT/'maintenance',sha,200_000);paths[str(asset)]=sha
    for item in regression:paths[item['junit']['path']]=item['junit']['sha256']
    expected_pins={str(BACKFILL_SUPPORT/name):sha for name,sha in closure['files'].items()}
    expected_pins.update({str(ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'):assets['native-accounting-read-guard-20261021.py'],
        str(ROOT/'maintenance/sixth-repair-support-20261016/controller-primary-repair-sixth-20261016.py'):assets['controller-primary-repair-sixth-20261016.py']})
    require(type(provider_paths) is dict and q.get('source_pins')=={**expected_pins,**provider_paths},
        'Exact full new and preserved provider qualification source closure required')
    paths.update(q['source_pins'])
    evidence_paths,fixture=backfill_evidence(q);paths.update(evidence_paths)
    return paths,proof


def policy_guard(context):
    provider_policy_guard(context)
    top=json_packet(context['backfill_policy_binding'],ROOT/'maintenance')
    require(top==context['backfill_policy'] and top['policy_source']=={'path':str(Path(__file__)),'sha256':digest(Path(__file__))},
        'New backfill policy source/descriptor changed')
    proof=slurm.completed_native_dependency(ROOT,top['backfill_qualification']['job_id'])
    require(proof is not None and proof['native_records']==context['backfill_qualification_completion']['native_records'],
        'New backfill qualification selected final native proof changed')

'''
outer = replace_once(outer, '\ndef fresh_pair(base,*,clock=time.time):', '\n'+additional+'\ndef fresh_pair(base,*,clock=time.time):')
outer = replace_once(outer,
    "and base.seconds(fields.get('TimeLimit',''))==96*3600",
    "and base.BACKFILL_MIN_SECONDS<=base.seconds(fields.get('TimeLimit',''))<=base.BACKFILL_MAX_SECONDS\n"
    "        and base.seconds(fields.get('TimeMin',''))==base.BACKFILL_MIN_SECONDS")
outer = replace_once(outer, 'Actual outer allocation must be owned scalar1CPU64G96h with exact registered command/name',
    'Actual outer allocation must be owned scalar1CPU64G8to96h with exact8h TimeMin and registered command/name')
outer = replace_once(outer, "    c=base.load_context(Path(policy['base_descriptor']['path']),policy['base_descriptor']['sha256'],args.segment)",
    "    script_raw,_=packet(args.native_script,ROOT/'plans')\n"
    "    require(script_raw.decode().splitlines().count('#SBATCH --time-min=08:00:00')==1,'Exact registered 8-hour native backfill directive required')\n"
    "    require(json_packet(policy['base_descriptor'],ROOT/'maintenance').get('backfill_qualification')==policy['backfill_qualification'],\n"
    "        'New base descriptor and outer must bind the same separately qualified backfill producer')\n"
    "    old_binding=policy['provider_policy']\n"
    "    require(old_binding=={'path':str(ORIGINAL_PROVIDER_POLICY),'sha256':ORIGINAL_PROVIDER_POLICY_SHA},\n"
    "        'Exact preserved original provider policy locator required')\n"
    "    provider_policy=json_packet(old_binding,ROOT/'maintenance')\n"
    "    require(set(provider_policy)=={'kind','base_controller','base_descriptor','policy_source','qualification','provider_schedule'}\n"
    "        and provider_policy.get('kind')=='af-controller-external-census-policy-v1'\n"
    "        and provider_policy.get('base_controller')=={'path':str(BASE),'sha256':BASE_SHA}\n"
    "        and provider_policy.get('policy_source')=={'path':str(ORIGINAL_POLICY_SOURCE),'sha256':ORIGINAL_POLICY_SHA},\n"
    "        'Preserved original qualified provider policy required')\n"
    "    schedule=provider_policy['provider_schedule']\n"
    "    require(set(schedule)=={'plan','ledger','helper','source_manifest','qualification','case_root'}\n"
    "        and schedule['case_root']==str(PROVIDER_CASE) and schedule['helper']['path']==str(SUPPORT/PROVIDER_NAME)\n"
    "        and schedule['qualification']==provider_policy['qualification'],'Original finite external provider schedule contract required')\n"
    "    checked=qualification(provider_policy);new_checked,new_proof=backfill_qualification(policy,provider_paths=checked)\n"
    "    c=base.load_context(Path(policy['base_descriptor']['path']),policy['base_descriptor']['sha256'],args.segment)")
old_start = outer.index("    require(set(policy)=={'kind','base_controller','base_descriptor','policy_source','qualification','provider_schedule'}")
old_end = outer.index("    json_packet(policy['base_descriptor'],ROOT/'maintenance')", old_start)
outer = outer[:old_start]+'''    require(set(policy)=={'kind','base_controller','base_descriptor','policy_source','provider_policy','backfill_qualification','runtime_contract'}
        and policy['kind']=='af-controller-backfill-external-census-policy-v1'
        and policy['base_controller']=={'path':str(BACKFILL_BASE),'sha256':BACKFILL_BASE_SHA}
        and policy['policy_source']=={'path':str(Path(__file__)),'sha256':digest(Path(__file__))}
        and policy['runtime_contract']==BACKFILL_RUNTIME,'Exact separate new backfill policy descriptor required')
'''+outer[old_end:]
outer = replace_once(outer,
    "    context={'policy_binding':binding,'policy':policy,'base_context':c,'checked_paths':qualification(policy)}\n"
    "    context['qualification_completion']=slurm.completed_native_dependency(ROOT,policy['qualification']['job_id'])\n"
    "    context['checked_paths'].update({binding['path']:binding['sha256'],policy['base_descriptor']['path']:policy['base_descriptor']['sha256']})",
    "    context={'policy_binding':old_binding,'policy':provider_policy,'base_context':c,'checked_paths':{**checked,**new_checked},\n"
    "        'backfill_policy_binding':binding,'backfill_policy':policy,'backfill_qualification_completion':new_proof}\n"
    "    context['qualification_completion']=slurm.completed_native_dependency(ROOT,provider_policy['qualification']['job_id'])\n"
    "    context['checked_paths'].update({binding['path']:binding['sha256'],old_binding['path']:old_binding['sha256'],\n"
    "        policy['base_descriptor']['path']:policy['base_descriptor']['sha256']})")
(HERE / OUTER_NAME).write_text(outer, encoding='utf8', newline='\n')
print(BASE_NAME, base_sha)
print(OUTER_NAME, hashlib.sha256((HERE / OUTER_NAME).read_bytes()).hexdigest())
