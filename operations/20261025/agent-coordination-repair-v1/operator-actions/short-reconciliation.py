"""One short native invocation of the already qualified recovery routine."""
from pathlib import Path
from datetime import datetime,timezone
import os,sys,json,hashlib,importlib.util,runpy,shlex,subprocess

ROOT=Path('/scratch/pwa209/horsehair-worm')
SUPPORT=ROOT/'maintenance/agent-coordination-repair-v1-20261025'
PIN='1612bdee43c7bfd51f172d1f51479a8110c2884edc3bf9aca99535718c6a0938'
assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==sys.argv[1]
assert os.geteuid()==3164768 and os.environ['USER']=='pwa209'
assert subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec')
manifest=json.loads((SUPPORT/'source-manifest.json').read_bytes())
spec=importlib.util.spec_from_file_location('qualified_coordination_recovery',SUPPORT/'coordination.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);m.verify_sources(PIN)
from horsehair.core import read_json,exclusive_file_lock,ReceiptLockTimeout
from horsehair.execution_resources import load_resource_policy
from horsehair.budget import resource_lease
from horsehair import slurm
job=os.environ['SLURM_JOB_ID'];area=SUPPORT/('short-reconciliation-'+job);area.mkdir()
scheduler=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-scheduler-read.py'
assert hashlib.sha256(scheduler.read_bytes()).hexdigest()=='100d21a32b95a621740f13d6a4afd502ea91ec6a0d129de54c7f6be99342f8b3'
response=runpy.run_path(str(scheduler))['run_scontrol_job'](job,area/'native');response.check_returncode();assert not response.stderr
fields=dict(x.split('=',1) for x in shlex.split(response.stdout) if '=' in x)
assert fields['JobId']==job and fields['UserId']=='pwa209(3164768)' and fields['JobState']=='RUNNING'
assert fields['Account']=='def-ptewarie_cpu' and fields['NumCPUs']=='2' and fields['MinMemoryNode'] in ['16G','16384M'] and fields['TimeLimit']=='01:00:00'
assert fields['Command']==str(ROOT/'plans/agent-short-reconciliation-v1-20261025/agent-short-reconciliation.sbatch')
assert all('gpu' not in fields.get(k,'').lower() for k in ['ReqTRES','AllocTRES','TresPerNode','TresPerTask'])
end=min(datetime.fromisoformat(fields['EndTime']).replace(tzinfo=timezone.utc).timestamp(),float(os.environ['SLURM_JOB_END_TIME']))
m.exclusive(area/'allocation.json',{'fields':fields,'actual_environment_end':float(os.environ['SLURM_JOB_END_TIME']),'source_manifest_sha256':PIN})
consumer=m.module(m.CONSUMER,m.CONSUMER_SHA,'old_consumer_for_targeted_recovery')
binding,_,_=consumer.check_binding(m.CONSUMER_BINDING,m.CONSUMER_BINDING_SHA)
outer=consumer.module(binding['legacy_entry']['path'],binding['legacy_entry']['sha256'],'old_policy_for_targeted_recovery')
policy=m.packet(binding['legacy_policy']);base=outer.load_base()
d=m.packet(policy['base_descriptor']);worker_path,worker_sha,worker=base.worker_binding(d)
config=read_json(base.PRIMARY/'configs/study.json');assert base.canonical_hash(config)==m.CONFIG
assert base.source_identity(m.FRAMEWORK)['sha256']==m.SOURCE
resource_policy=load_resource_policy(Path(d['resource_policy']['path']),d['resource_policy']['sha256'],root=ROOT,config=config)
registry={**base.registry_binding(d),'prestart_retry_value':base.retry_binding(d),'prestart_retry_binding':dict(d['prestart_retry_amendment'])}
campaign,native_reader=base.guarded_modules(area,registry)
protocol=base.agents._directory(ROOT,config['agents'])/'frozen_protocol.json'
c={'descriptor':d,'descriptor_path':Path(policy['base_descriptor']['path']),'descriptor_sha256':policy['base_descriptor']['sha256'],
 'framework':m.FRAMEWORK,'config':config,'job_id':job,'segment':0,'case':m.CAMPAIGN,'area':area,
 'policy':resource_policy,'protocol_path':protocol,'protocol':read_json(protocol),'deadline':end-600,
 'primary_release_id':read_json(base.PRIMARY/'release.json')['release_id'],'campaign_module':campaign,
 'native_reader':native_reader,'registry_overlay':registry,'worker_descriptor_path':worker_path,
 'worker_descriptor_sha256':worker_sha,'worker_descriptor':worker}
with resource_lease(ROOT,config,cpus=2,gpus=0,max_hours=1,job_id=job,resource_policy=resource_policy):
 try:
  with exclusive_file_lock(m.CAMPAIGN/'coordination-controller.mutex',timeout=60):
   base.source_guard(c);m.positive_old_controllers(base,c)
   with base.registration_guard(c,'controller-start'):
    base.admission(c,'short-reconciliation-native-admission',cpus=0,gpus=0,hours=1)
   q=read_json(SUPPORT/'qualification.json');proof=slurm.completed_native_dependency(ROOT,q['native_job_id'])
   assert q['tests']==72 and q['source_manifest_sha256']==PIN and proof is not None
   assert proof['native_records'][q['native_job_id']]['state']=='COMPLETED' and proof['native_records'][q['native_job_id']]['exit_code']=='0:0'
   m.recover_native(base,c,PIN);base.source_guard(c)
 except ReceiptLockTimeout:
  assert (SUPPORT/'recovery.json').is_file(),'Existing controller owns recovery; no state changes made by this helper'
  m.recover_native(base,c,PIN)
print('SHORT_RECONCILIATION_COMPLETE '+json.dumps(read_json(SUPPORT/'recovery.json')),flush=True)
