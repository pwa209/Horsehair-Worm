"""Bounded, checksum-verified public acquisition; never executes provider code."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,os,re,runpy,time,urllib.request,urllib.parse
from horsehair.core import read_json,digest,atomic_json,native_job_id,source_identity,canonical_hash
from horsehair.budget import resource_lease
from horsehair.execution_resources import load_resource_policy
ROOT=Path('/scratch/pwa209/horsehair-worm')
CASE=ROOT/'maintenance/study-af-zenodo-extension-acquisition-20261021'
METADATA=ROOT/'maintenance/study-af-next-public-metadata-20261021'
BINDINGS={'17175344':('cd32041a918ad9f5e4832a7ae59df1d28c24098a1f5429643f96edaeb7bfea00','cc-by-4.0',7),'10479262':('a459f7cf6a08db961e5943d149b9fec0fa4cfc31ea418809262ef27da0e21475','mit-license',2)}
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261013-829e3817bee1-0e2b1ebce8bf')
FRAMEWORK=ROOT/'releases/20261021-6ed201653dfa-4f3903d7c26e'
GUARD=ROOT/'maintenance/benchmark-preflight-stage-v4-20261021/native-accounting-read-guard.py'
QUOTA=ROOT/'maintenance/benchmark-quota-recovery-deployment-20261021/native-quota-read-wait.py'

def require(value,message):
 if not value:raise ValueError(message)

def checked_url(value):
 u=urllib.parse.urlsplit(value)
 require(u.scheme=='https' and u.hostname=='zenodo.org' and not u.username and not u.password and u.port in (None,443),'Only official public Zenodo HTTPS payloads allowed')
 return value

class Redirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,req,fp,code,msg,headers,newurl):
  checked_url(newurl);return super().redirect_request(req,fp,code,msg,headers,newurl)

def roster(ident,record,license_id,count):
 require(str(record.get('id'))==ident and record['metadata']['license']['id']==license_id,'Provider record/licence changed')
 entries=record.get('files');require(type(entries) is list and len(entries)==count,'Complete provider roster changed')
 names=set();total=0
 for item in entries:
  name=item['key'];size=item['size'];checksum=item['checksum']
  require(type(name) is str and re.fullmatch(r'[A-Za-z0-9_. -]+',name) and name not in names and name not in {'.','..'} and type(size) is int and 0<size<=100_000_000 and re.fullmatch(r'md5:[0-9a-f]{32}',checksum),'Unsafe/unbounded provider file binding')
  url=checked_url(item['links']['self']);u=urllib.parse.urlsplit(url)
  require(u.path=='/api/records/'+ident+'/files/'+urllib.parse.quote(name,safe='')+'/content' and not u.query,'Exact metadata-bound content route required')
  names.add(name);total+=size
 require(total<=500_000_000,'Provider record exceeds acquisition bound')
 return entries

def main():
 job=str(native_job_id());require(os.geteuid()==3164768 and os.environ.get('USER')=='pwa209' and job.isdigit() and os.environ.get('SLURM_CPUS_PER_TASK')=='1','Owned one-CPU native allocation required')
 require(source_identity(FRAMEWORK)['sha256']=='4f3903d7c26e334a75c8f7f81ad8b865fac4e5a447ff83a8bfeae8774891d9d1','Qualified operational source changed')
 dpath=ROOT/'maintenance/study-af-campaign-v1-20261021/descriptor-v1.json';require(digest(dpath)=='987c4c3bec38ca3a28d14cb1fbe22675bfcd6e5d2a465c8eb9adf01270b5b4bc','Approved operational binding changed');d=read_json(dpath)
 config=read_json(PRIMARY/'configs/study.json');require(canonical_hash(config)==d['full_config_sha256'] and source_identity(PRIMARY)['sha256']==d['primary_source_sha256'],'Primary scientific binding changed')
 q=d['qualification'];require(digest(Path(q['path']))==q['sha256'] and read_json(Path(q['path']))['status']=='complete','Native source qualification required')
 require(not CASE.exists(),'Immutable acquisition cannot be replayed');CASE.mkdir(mode=0o700)
 require(digest(GUARD)=='358145f195ee70d485fa1c7547fb8d593323bdaaaf7e7e905ce195a79dfcea91' and digest(QUOTA)=='5004a100b930691709357f4f3e7baaee57ebd11fae2c39fd26413c3bf94d9045','Pinned readers changed')
 runpy.run_path(str(GUARD))['install'](CASE/'accounting-reads')
 from horsehair.campaign import combined_admission
 from horsehair import campaign,repair
 require(campaign._repair_native_states is repair._repair_native_states,'Guard binding differs')
 policy=load_resource_policy(Path(d['resource_policy']['path']),d['resource_policy']['sha256'],root=ROOT,config=config)
 files=[]
 for ident,(sha,lic,count) in BINDINGS.items():
  path=METADATA/(ident+'.json');require(digest(path)==sha,'Exact saved provider metadata changed')
  files.extend((ident,item) for item in roster(ident,read_json(path),lic,count))
 require(sum(item['size'] for _,item in files)<=500_000_000 and len(files)==9,'Whole acquisition bound changed')
 combined_admission(ROOT,config,read_json(PRIMARY/'release.json')['release_id'],CASE,'native-start-admission',resource_policy=policy)
 with resource_lease(ROOT,config,cpus=1,gpus=0,max_hours=1,job_id=job,resource_policy=policy):
  census=read_json(ROOT/'state/storage-budget.json');now=time.time();captured=datetime.fromisoformat(census['captured_utc'].replace('Z','+00:00')).timestamp()
  require(0<=now-census['measured_unix']<6*3600 and 0<=now-captured<6*3600 and census['study_files']+2000<=config['resources']['scratch_budget_files'] and census['study_bytes']+1_000_000_000<=config['resources']['scratch_budget_bytes'],'Fresh study census and bounded acquisition space required')
  quota=runpy.run_path(str(QUOTA))['require_storage_headroom_with_wait'](ROOT,required_files=2000,required_bytes=1_000_000_000,reserve_files=248000,reserve_bytes=2_000_000_000_000,evidence_dir=CASE/'quota-reads',allocation_deadline_unix=float(os.environ['SLURM_JOB_END_TIME']))
  receipt={'status':'acquisition_running','native_job_id':job,'metadata_bindings':BINDINGS,'files':[],'effects_inspected':False,'provider_code_executed':False,'scientific_gates_cleared':False,'storage_headroom':quota}
  atomic_json(CASE/'receipt.json',receipt);opener=urllib.request.build_opener(Redirect());deadline=min(time.time()+1800,float(os.environ['SLURM_JOB_END_TIME'])-180)
  for ident,item in files:
   name=item['key'];area=CASE/ident;area.mkdir(mode=0o700,exist_ok=True);part=area/(name+'.partial');record={'record_id':ident,'name':name,'expected_bytes':item['size'],'provider_checksum':item['checksum'],'source_url':item['links']['self']}
   try:
    require(time.time()<deadline,'Acquisition deadline reached');md5=hashlib.md5();sha=hashlib.sha256();size=0
    with opener.open(urllib.request.Request(item['links']['self'],headers={'User-Agent':'Horsehair-Worm public research acquisition/1.0'}),timeout=30) as response,part.open('xb') as output:
     checked_url(response.url)
     while True:
      require(time.time()<deadline,'Finite acquisition deadline reached');raw=response.read(min(1024*1024,item['size']-size+1))
      if not raw:break
      size+=len(raw);require(size<=item['size'],'Payload exceeds exact provider byte count');output.write(raw);md5.update(raw);sha.update(raw)
     output.flush();os.fsync(output.fileno())
    require(size==item['size'] and 'md5:'+md5.hexdigest()==item['checksum'],'Independent provider size/MD5 verification failed');target=area/name;require(not target.exists(),'Existing final payload refused');part.rename(target)
    record.update(status='provider_verified',path=str(target),bytes=size,sha256=sha.hexdigest(),provider_md5_verified=True)
   except Exception as error:record.update(status='unavailable_preserved_partial',error_type=type(error).__name__,message=str(error)[:400],partial_path=str(part) if part.exists() else None)
   receipt['files'].append(record);atomic_json(CASE/'receipt.json',receipt)
  receipt.update(status='all_provider_files_verified' if all(x['status']=='provider_verified' for x in receipt['files']) else 'partial_acquisition',finished_unix=time.time());atomic_json(CASE/'receipt.json',receipt)
 print('AF_ZENODO_EXTENSION '+json.dumps(receipt,sort_keys=True),flush=True)

if __name__=='__main__':main()
