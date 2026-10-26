"""One-off retirement of the exact deadline record that preceded any sbatch.

This is an operator action, not a controller overlay or a submission retry.
An empty scheduler lookup alone never authorizes this transition.
"""
from pathlib import Path
import copy,gzip,hashlib,json,os,subprocess,time

ROOT=Path('/scratch/pwa209/horsehair-worm')
CASE=ROOT/'maintenance/study-af-campaign-v1-20261021/agent-final-f64-v1'
OLD=ROOT/'maintenance/agent-coordination-repair-v2-20261025'
NEW=ROOT/'maintenance/agent-coordination-repair-v3-20261026'
IDENT='assembly-0089'
NAME='af-f2-'+IDENT
ACTORS=('7283317','7283326','7283327','7283328')
STATE_SHA='74342849d9e35d3815db8bf2c3157234c204e92ab64b05d380cde22a557c8a06'
STATE_FILE_SHA='1b8ec23978fadc7e01dd867ea57bf54613d20c4d13718d17c2d056f8a7a705cc'
LEDGER_SHA='b548a6616cb8889fb82a609af09e834d6fbc551fe33ab51685a710e33538a888'
SOURCE_SHA='9cf0df31a126dce2ae30b2199f058e6f4a803905ba3a2e6c5e49b2b8eed824ce'
LOG_SHA='cc2eaf00f0421900847acc3355b262d493ac64b633061bf73b62c1648e23695f'
OTHER=('assembly-0090','assembly-0091','assembly-0092','evaluation-0093-01','evaluation-0094-01',
       'evaluation-0095-01','evaluation-0096-01','training-0097','training-0098','training-0099','training-0100')


def require(value,message):
    if not value:raise ValueError(message)


def digest(raw):return hashlib.sha256(raw).hexdigest()


def canonical(value):return digest(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode())


def recovered_ledger(ledger,source,log,failure,state,window,journal,plan,script):
    require(digest(source)==SOURCE_SHA and digest(log)==LOG_SHA,'Exact frozen source and complete native traceback required')
    text=source.decode()
    require("require(time.time()+300<c['deadline'],'Submission finalization margin exhausted')\n                    return runner(" in text,
            'Pinned failing guard must precede actual subprocess invocation')
    require(log.decode().endswith('ValueError: Submission finalization margin exhausted\n')
            and 'line 183, in invoke' in log.decode() and 'result=invoke(command)' in log.decode(),
            'Actual source-bound pre-call stack required')
    require(failure=={'admission_authority':False,'error':'Submission finalization margin exhausted','error_type':'ValueError',
                     'job_id':ACTORS[0],'scientific_result':False,'segment':0,'status':'needs_repair'},'Exact native actor failure required')
    require(state['revision']==526 and state['state_sha256']==STATE_SHA,'Original state required')
    pending={k:j for k,j in state['jobs'].items() if j['status']!='complete'}
    require(set(pending)=={IDENT,*OTHER} and all(j['status']=='ambiguous' and j['job_id'] is None
                and j['native_proof'] is None and j['receipt'] is None for j in pending.values()),'Only the exact saved unacknowledged batch may resume')
    require(digest((json.dumps(ledger,sort_keys=True,indent=2)+'\n').encode())==LEDGER_SHA,'Exact original submitting ledger required')
    require(set(ledger['tasks'])=={NAME},'Only the proven first row may be removed')
    row=ledger['tasks'][NAME];task=plan['tasks'][0]
    require(set(row)=={'attempt_started_unix','command','completed_native_dependencies','dependency_kind',
                'dependency_resolution_sha256','issued_native_dependencies','script_sha256','state'}
            and row['state']=='submitting' and row['attempt_started_unix']==1792983061.834107
            and row['command']==['sbatch','--parsable',task['script']]
            and row['script_sha256']==task['script_sha256']==canonical(script.decode())
            and ledger['plan_sha256']==canonical(plan),'No accepted ID, timeout, response or altered attempt is permitted')
    require(window['work_deadline_unix']==1792983138.0
            and 0<window['work_deadline_unix']-row['attempt_started_unix']<=300,'Original conservative cutoff must explain the failure')
    require(journal['status']=='before_sbatch' and journal.get('job_id') is None
            and journal['intent']==pending[IDENT]['intent'] and journal['plan_sha256']==digest((json.dumps(plan,indent=2,sort_keys=True)+'\n').encode()),
            'Original untouched before-sbatch journal and plan required')
    result=copy.deepcopy(ledger);del result['tasks'][NAME]
    require({k:v for k,v in result.items() if k!='tasks'}=={k:v for k,v in ledger.items() if k!='tasks'},'Ledger headers changed')
    return result


def main():
    from horsehair.core import exclusive_file_lock,atomic_json
    require(os.geteuid()==3164768,'Owned personal scratch required')
    require(subprocess.check_output(['hostname','-f'],text=True).strip().endswith('.narval.calcul.quebec'),'Narval required')
    lp=ROOT/'state'/(NAME+'-submission.json');pp=ROOT/'plans'/NAME/'plan.json';jp=CASE/('submission-'+IDENT+'.json')
    area=NEW/'deadline-precall-recovery';receipt=area/'complete.json'
    def read(path):
        require(path==path.resolve() and path.is_file() and not any(p.is_symlink() for p in (path,*path.parents)),'Ordinary owned files required')
        return path.read_bytes()
    def native(command):
        p=subprocess.run(command,text=True,capture_output=True,timeout=90)
        p.check_returncode();require(not p.stderr,'Native corroborating query must succeed cleanly')
        return {'command':command,'stdout':p.stdout,'stderr':p.stderr,'returncode':p.returncode}
    with exclusive_file_lock(CASE/'coordination-controller.mutex',timeout=60):
      with exclusive_file_lock(ROOT/'state/af-campaign-registration.mutex',timeout=60):
       with exclusive_file_lock(lp.with_suffix('.mutex'),timeout=60):
        raw=read(CASE/'state.json.gz');require(digest(raw)==STATE_FILE_SHA,'Campaign changed; refuse stale repair')
        state=json.loads(gzip.decompress(raw));binding=json.loads(read(CASE/'state-binding.json'))
        require(binding['revision']==526 and binding['state_sha256']==STATE_SHA and binding['state_file_sha256']==STATE_FILE_SHA,'State binding changed')
        original=read(lp)
        if area.exists():
            before=json.loads(read(area/'before.json'))
            require(digest(original) in {LEDGER_SHA,before['after_ledger_sha256']},'Interrupted repair differs; inspect without replay')
            require(digest(read(area/'ledger.json'))==LEDGER_SHA,'Original failed row must remain preserved')
            after=before['after_ledger_sha256'];evidence=before['evidence']
            for name,pin in before['preserved'].items():
                require(digest(read(area/name))==pin['sha256'],'Interrupted archive bytes changed')
            require(digest(read(area/'state.json.gz'))==STATE_FILE_SHA,'Exact archived state required')
            result=recovered_ledger(json.loads(read(area/'ledger.json')),read(area/'source.py'),read(area/'controller.log'),
                json.loads(read(area/'failure.json')),state,json.loads(read(area/'window.json'))['window'],
                json.loads(read(area/'journal.json')),json.loads(read(area/'plan.json')),read(area/'script.sbatch'))
            require(digest((json.dumps(result,indent=2,sort_keys=True)+'\n').encode())==after,'Revalidated exact cleared ledger required')
            if receipt.exists():
                prior=json.loads(read(receipt));require(digest(original)==after==prior['after_ledger_sha256'],
                    'A new attempt exists; never clear it')
                print('DEADLINE_PRECALL_RECOVERY '+json.dumps(prior),flush=True);return
            if digest(original)==LEDGER_SHA:
                again=native(evidence['empty_matching_queue']['command'])
                history=native(evidence['empty_matching_history']['command'])
                require(not again['stdout'].strip() and not history['stdout'].strip(),'A native attempt exists; refuse replay')
                require(digest(read(lp))==LEDGER_SHA and digest(read(CASE/'state.json.gz'))==STATE_FILE_SHA,'Interrupted compare-and-swap changed')
                atomic_json(lp,result);require(digest(read(lp))==after,'Interrupted atomic retirement differs')
        else:
            require(digest(original)==LEDGER_SHA,'Exact original ledger bytes required')
            paths={'ledger.json':lp,'plan.json':pp,'journal.json':jp,'state.json.gz':CASE/'state.json.gz',
                'state-binding.json':CASE/'state-binding.json','failure.json':CASE/'controller-failure.json',
                'source.py':OLD/'coordination.py','window.json':OLD/'native-run-7283317/startup.json',
                'controller.log':ROOT/'logs/agent-coordination-v2g-controller-0-7283317_4294967294.log'}
            packet={name:read(path) for name,path in paths.items()}
            plan=json.loads(packet['plan.json']);sp=Path(plan['tasks'][0]['script']);packet['script.sbatch']=read(sp);paths['script.sbatch']=sp
            result=recovered_ledger(json.loads(original),packet['source.py'],packet['controller.log'],json.loads(packet['failure.json']),
                state,json.loads(packet['window.json'])['window'],json.loads(packet['journal.json']),plan,packet['script.sbatch'])
            for ident in OTHER:
                for p in (CASE/('submission-'+ident+'.json'),ROOT/'plans'/('af-f2-'+ident)/'plan.json',ROOT/'state'/('af-f2-'+ident+'-submission.json')):
                    require(not p.exists(),'An additional attempt exists; no reset is permitted')
            final=native(['sacct','-X','-n','-P','-j',','.join(ACTORS),'--format=JobIDRaw,State,ExitCode,User,Account'])
            rows={v[0]:v for line in final['stdout'].splitlines() if (v:=line.split('|')) and len(v)>=5}
            require(set(rows)==set(ACTORS) and rows[ACTORS[0]][1:]==['FAILED','1:0','pwa209','def-ptewarie_cpu']
                and all(rows[j][1:]==['CANCELLED','0:0','pwa209','def-ptewarie_cpu'] for j in ACTORS[1:]),'All former writers must be positively final')
            queue=native(['squeue','--local','-u','pwa209','--name='+','.join('hhw-af-f2-'+i for i in (IDENT,*OTHER)),'-h','-o','%i|%j|%T'])
            history=native(['sacct','-X','-n','-P','-u','pwa209','--starttime=2026-10-25T22:52:18','--name=hhw-'+NAME,'--format=JobIDRaw,JobName,State,Submit'])
            require(not queue['stdout'].strip() and not history['stdout'].strip(),'An actual native attempt exists; reconcile rather than clear')
            evidence={'final_actors':final,'empty_matching_queue':queue,'empty_matching_history':history}
            area.mkdir();preserved={}
            for name,data in packet.items():
                with (area/name).open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
                preserved[name]={'original':str(paths[name]),'sha256':digest(data)}
            after_raw=(json.dumps(result,indent=2,sort_keys=True)+'\n').encode();after=digest(after_raw)
            atomic_json(area/'before.json',{'before_ledger_sha256':LEDGER_SHA,'after_ledger_sha256':after,
                'state_file_sha256':STATE_FILE_SHA,'preserved':preserved,'evidence':evidence})
            require(digest(read(lp))==LEDGER_SHA and digest(read(CASE/'state.json.gz'))==STATE_FILE_SHA,'Final compare-and-swap changed')
            atomic_json(lp,result);require(digest(read(lp))==after,'Atomic retirement differs')
        require(digest(read(CASE/'state.json.gz'))==STATE_FILE_SHA,'Canonical state must remain byte-identical')
        value={'status':'proven_precall_deadline_row_preserved_and_retired','intent_id':IDENT,'failed_actor':ACTORS[0],
               'before_ledger_sha256':LEDGER_SHA,'after_ledger_sha256':after,'canonical_state_unchanged':True,
               'saved_intentions':12,'sbatch_invocations':0,'scientific_gates_changed':False,'archive':str(area),'evidence':evidence}
        atomic_json(receipt,value);print('DEADLINE_PRECALL_RECOVERY '+json.dumps(value),flush=True)


if __name__=='__main__':main()
