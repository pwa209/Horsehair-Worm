"""Read-only launch delay; the qualified handoff independently rechecks admission."""
from pathlib import Path
import json,math,os,re,subprocess,time

ROOT=Path('/scratch/pwa209/horsehair-worm')
EXPECTED={
    '6982735':{'state':'COMPLETED','exit_code':'0:0','cpus':8,'elapsed':2714},
    '6997524':{'state':'COMPLETED','exit_code':'0:0','cpus':1,'elapsed':25515},
    '7086253':{'state':'FAILED','exit_code':'1:0','cpus':1,'elapsed':850},
}
FALSE_FLAGS={'scientific_result':False,'admission_authority':False,'scientific_gates_cleared':False,
             'scheduler_mutations_performed':False,'automatic_replay_permitted':False}


def assessed_rows(text,expected=EXPECTED):
    rows={}
    for line in text.splitlines():
        if not line.strip():continue
        fields=[value.strip() for value in line.split('|')]
        if len(fields)!=8:raise ValueError('Malformed native accounting row')
        job,state,exit_code,elapsed,cpus,user,account,tres=fields
        if job not in expected or job in rows:raise ValueError('Unknown or duplicate native identity')
        if user!='pwa209' or account!='def-ptewarie_cpu' or 'gpu' in tres.lower():
            raise ValueError('Foreign or GPU native allocation refused')
        if not elapsed.isdigit() or not cpus.isdigit():raise ValueError('Invalid native resource integers')
        row={'state':state,'exit_code':exit_code,'elapsed':int(elapsed),'cpus':int(cpus)}
        if state in {'PENDING','RUNNING','COMPLETING'}:
            rows[job]=row;continue
        if row!=expected[job]:raise ValueError('Unexpected terminal outcome or allocation')
        rows[job]=row
    ready=set(rows)==set(expected) and all(rows[job]==expected[job] for job in expected)
    return {'ready':ready,'rows':rows,'missing':sorted(set(expected)-set(rows))}


def wait_finality(deadline,record,*,runner=subprocess.run,clock=time.time,sleeper=time.sleep,poll_seconds=20):
    if not math.isfinite(deadline) or not 0<poll_seconds<=30:raise ValueError('Finite polling contract required')
    command=['sacct','-X','-j',','.join(EXPECTED),
        '--format=JobIDRaw%64,State%64,ExitCode,ElapsedRaw,AllocCPUS,User%64,Account%64,AllocTRES%256',
        '--noheader','--parsable2']
    attempt=0
    while True:
        remaining=deadline-clock()
        if remaining<=0:raise TimeoutError('Final accounting wait ended; handoff was not started')
        attempt+=1
        try:
            result=runner(command,capture_output=True,text=True,timeout=min(25,remaining))
            evidence={'attempt':attempt,'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr}
            if result.returncode==0 and not result.stderr:
                assessment=assessed_rows(result.stdout);evidence['assessment']=assessment
            else:assessment={'ready':False}
        except subprocess.TimeoutExpired:
            assessment={'ready':False};evidence={'attempt':attempt,'status':'read_only_timeout'}
        record(evidence)
        if assessment['ready']:
            if clock()>=deadline:raise TimeoutError('Final accounting arrived after the observation deadline')
            return assessment
        remaining=deadline-clock()
        if remaining<=0:raise TimeoutError('Final accounting wait ended; handoff was not started')
        sleeper(min(poll_seconds,remaining))


def main():
    hostname=subprocess.check_output(['hostname','-f'],text=True,timeout=10).strip()
    if os.geteuid()!=3164768 or not hostname.endswith('.narval.calcul.quebec'):
        raise ValueError('Owned Narval allocation required')
    job=os.environ.get('SLURM_JOB_ID','')
    if re.fullmatch('[1-9][0-9]*',job) is None:raise ValueError('Scalar native wait allocation required')
    end=float(os.environ.get('SLURM_JOB_END_TIME','nan'));now=time.time()
    if not math.isfinite(end) or end-now<7200:raise ValueError('Native handoff finalization margin unavailable')
    deadline=min(now+5400,end-5400)
    area=ROOT/'maintenance/study-native-finality-wait-v1-20261024'/('native-'+job)
    area.mkdir(mode=0o700,parents=True,exist_ok=False)
    def save(name,value):
        with (area/name).open('x',encoding='utf8') as stream:
            stream.write(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n');stream.flush();os.fsync(stream.fileno())
    def record(value):
        save('read-'+str(value['attempt']).zfill(4)+'.json',value)
        status={'status':'native_final_accounting_ready' if value.get('assessment',{}).get('ready') else 'waiting_native_final_accounting',
                'native_job_id':job,'attempt':value['attempt'],
                'deadline_unix':deadline,**FALSE_FLAGS}
        temporary=area/'status.tmp';temporary.write_text(json.dumps(status,sort_keys=True)+'\n');temporary.replace(area/'status.json')
    result=wait_finality(deadline,record)
    save('ready.json',{'status':'native_final_accounting_ready','native_job_id':job,'assessment':result,
        'qualified_handoff_must_revalidate_all_own_preconditions':True,**FALSE_FLAGS})
    print(json.dumps({'status':'native_final_accounting_ready','native_job_id':job,**FALSE_FLAGS}),flush=True)
    return 0


if __name__=='__main__':raise SystemExit(main())
