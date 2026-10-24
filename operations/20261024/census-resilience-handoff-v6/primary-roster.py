"""Read-only, source-bound discovery of every registered primary worker."""
from pathlib import Path
import ast
import hashlib
import json
import os
import re
import stat

PRIMARY_RELEASE_ID='20261013-829e3817bee1-0e2b1ebce8bf'
PRIMARY_SHA='0e2b1ebce8bffe10799470b3ac20828e621f1415f3a18bca8cd67d502f1e8630'
CONFIG_SHA='446b5d62fc206cf1392b90ff9cf16adafc66c57a6d38e84c029cd3b2ebbf1f35'
HELPER='maintenance/study-primary-external-census-policy-v1-20261023/run-primary-external-census-v1-20261023.py'
HELPER_SHA='dd5f8d2aec90896fdcfe190971ea8b6c32a94158a3684566c2b8405748e7c794'
PIPELINE_SHA='ef954fe249e376e0d911b64b67fae16290216b62f2efb93ecace25598fbcc575'
OLD_FRAMEWORK=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases/20261021-57e0eb8c4c6c-d89b31180d4c')
PRIMARY=Path('/project/def-ptewarie/pwa209/horsehair-worm/releases')/PRIMARY_RELEASE_ID
PLANNER_PINS={'primary':'d31e91b1fe40b5c9ada5514f1db544876df446d1f0a9e030d74bfd6e0413fb08',
    'framework':'19473528d9684bbbc9a241a621283b105beb8bfa1352f168bf21e628327e8d37'}
PLAN_AST_SHA='f2169c32fd39faca223be3a3dbbb25e28e67c335e08b166260f91100a8ec3014'
ADD_AST_SHA='5f7a6944cfd4f96d58d343c53aa2cb1276ebd780a58803f00559550ce70cf0bf'
CALLER_AST_PINS={'_primary_execution':'860ce91c2cdceffa10e976f7d010bd2216f83949633b38c0dea94302e96f8ceb',
    'tick':'12e3bceb23002d9787e8eba9fdb8ef148c3db04b5cab9f678b261ed4b8da0f54'}
PHASES=('rna-full','agents-development','agents-final','behaviour-benchmark','behaviour','interventions',
    'screen','molecular-benchmark','molecular-transfer','molecular-corroboration','synthesis')
TERMINAL={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED'}
FALSE_FLAGS={'scientific_result':False,'admission_authority':False,'canonical_state_changed':False,
    'worker_jobs_changed':False,'native_queries_performed':False}
HISTORY=Path(__file__).with_name('primary-history-pins.json')


def require(value,message):
    if not value:raise ValueError(message)


def canonical(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def _ast_sha(node):
    # The sources target Python 3.11. Python 3.12 adds empty type_params;
    # remove only that absent-in-3.11 field for a cross-version byte proof.
    for item in ast.walk(node):
        if hasattr(item,'type_params'):
            require(not item.type_params,'New generic source syntax refused');delattr(item,'type_params')
    return hashlib.sha256(ast.dump(node,include_attributes=False).encode()).hexdigest()


def _identity(s):return (s.st_dev,s.st_ino,s.st_uid,stat.S_IFMT(s.st_mode))
def _snapshot(s):return (*_identity(s),s.st_size,s.st_mtime_ns,s.st_ctime_ns)
def _open_snapshot(s):
    # Windows 3.13 lstat exposes birth time as ctime while fstat uses change
    # time. Native POSIX verification retains the full ctime equality check.
    return _snapshot(s)[:-1] if os.name=='nt' else _snapshot(s)


def _confined(path,base):
    path=Path(path);base=Path(base);physical=base.resolve(strict=True)
    require(path.is_absolute() and path.resolve().is_relative_to(physical) and path.resolve()!=physical,'Confined metadata path required')
    prefix=base if path.is_relative_to(base) else physical
    require(path.is_relative_to(prefix) and all(not p.is_symlink() for p in [path,*path.parents]
        if p!=prefix and p.is_relative_to(prefix)),'Subordinate metadata symlink refused')
    return path


def _read(path,base,uid,checked,maximum=20_000_000):
    path=_confined(path,base);directory=Path(base).stat();before=path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_uid==uid and before.st_size<=maximum,'Owned bounded regular metadata required')
    flags=os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_BINARY',0)
    fd=os.open(path,flags)
    try:
        opened=os.fstat(fd);require(_open_snapshot(opened)==_open_snapshot(before),'Metadata changed before read')
        with os.fdopen(fd,'rb',closefd=False) as stream:raw=stream.read(maximum+1)
        require(len(raw)==before.st_size and len(raw)<=maximum and _open_snapshot(os.fstat(fd))==_open_snapshot(before),'Metadata changed during read')
    finally:os.close(fd)
    require(_snapshot(path.lstat())==_snapshot(before) and _identity(Path(base).stat())==_identity(directory),'Metadata identity changed')
    pin=hashlib.sha256(raw).hexdigest();previous=checked.setdefault(str(path),pin);require(previous==pin,'Previously read metadata changed')
    return raw


def _strict(raw):
    def unique(pairs):
        value={}
        for key,item in pairs:
            require(key not in value,'Duplicate metadata key');value[key]=item
        return value
    return json.loads(raw,object_pairs_hook=unique,parse_constant=lambda value:(_ for _ in ()).throw(ValueError('Nonfinite metadata refused')))


def array_indices(expression):
    require(isinstance(expression,str) and re.fullmatch(r'[0-9,:-]+(?:%[1-9][0-9]*)?',expression),'Exact finite array grammar required')
    values=set()
    for term in expression.split('%',1)[0].split(','):
        match=re.fullmatch(r'(\d+)(?:-(\d+)(?::(\d+))?)?',term);require(match is not None,'Invalid array term')
        first,last,step=int(match[1]),int(match[2] or match[1]),int(match[3] or 1)
        require(0<=first<=last and step>0 and last-first<=10_000,'Bounded nonempty array required')
        part=set(range(first,last+1,step));require(not values&part,'Duplicate array member refused');values.update(part)
        require(len(values)<=10_001,'Bounded complete array roster required')
    require(values,'Nonempty array roster required');return sorted(values)


def registered_script(root,phase,task,entry,plan_sha,ledger_sha,history):
    """Recognize only exact, frozen historical repairs outside the phase folder."""
    name=task['name'];expected=root/'plans'/(PRIMARY_RELEASE_ID+'-'+phase)/(name+'.sbatch')
    if task.get('script')==str(expected):return expected,None
    record=history['tasks'].get(phase+'/'+name)
    require(isinstance(record,dict) and set(record)=={'plan_sha256','ledger_sha256','script_relative_path',
        'script_sha256','script_text_sha256','job_id','task_execution_release_sha256','ledger_execution_release_sha256'},
        'Exact frozen primary script namespace or reviewed historical binding required')
    relative=Path(record['script_relative_path']);script=root/relative
    require(not relative.is_absolute() and '..' not in relative.parts and relative.parts[0]=='plans'
        and relative.name==name+'.sbatch' and task.get('script')==str(script)
        and plan_sha==record['plan_sha256'] and ledger_sha==record['ledger_sha256']
        and entry.get('job_id')==record['job_id'] and task.get('script_sha256')==entry.get('script_sha256')==record['script_text_sha256']
        and canonical(task.get('execution_release'))==record['task_execution_release_sha256']
        and canonical(entry.get('execution_release'))==record['ledger_execution_release_sha256'],
        'Exact frozen primary historical plan/ledger/script/job/source binding required')
    return script,record


def primary_worker_roster(root,primary_release_id,*,pipeline_path=None,planner_paths=None):
    """Discover only; caller must hold its handoff gate and recheck all paths.

    Optional source paths are explicit same-byte local regression overrides.
    Native integration uses the exact paths parsed from the pinned old helper.
    """
    root=Path(root);require(root.is_absolute() and root.is_dir() and not root.is_symlink(),'Ordinary absolute study root required')
    require(primary_release_id==PRIMARY_RELEASE_ID,'Exact original primary release required')
    uid=root.stat().st_uid;checked={};absent=[]
    history=_strict(_read(HISTORY,HISTORY.parent,uid,checked))
    require(history.get('kind')=='source-bound-primary-historical-script-bindings-v1'
        and history.get('primary_release_id')==PRIMARY_RELEASE_ID and isinstance(history.get('tasks'),dict)
        and 0<len(history['tasks'])<=100,'Bounded reviewed original primary history required')
    helper=root/HELPER;helper_raw=_read(helper,root,uid,checked)
    require(hashlib.sha256(helper_raw).hexdigest()==HELPER_SHA,'Exact live primary helper required')
    assignments={node.targets[0].id:node.value for node in ast.parse(helper_raw).body
        if isinstance(node,ast.Assign) and isinstance(node.targets[0],ast.Name)}
    declared=assignments['OLD_FRAMEWORK'];require(isinstance(declared,ast.Call) and isinstance(declared.func,ast.Name)
        and declared.func.id=='Path' and len(declared.args)==1 and ast.literal_eval(declared.args[0])==OLD_FRAMEWORK.as_posix(),
        'Original primary framework route required')
    require(ast.literal_eval(assignments['PIPELINE_SHA'])==PIPELINE_SHA and ast.literal_eval(assignments['CONFIG'])==CONFIG_SHA,
        'Original pipeline and full configuration pins required')
    declared_primary=assignments['PRIMARY'];require(isinstance(declared_primary,ast.Call)
        and isinstance(declared_primary.func,ast.Name) and declared_primary.func.id=='Path'
        and len(declared_primary.args)==1 and ast.literal_eval(declared_primary.args[0])==PRIMARY.as_posix(),
        'Exact original isolated primary planner route required')
    expected_pipeline=OLD_FRAMEWORK/'src/horsehair/pipeline.py';pipeline=expected_pipeline if pipeline_path is None else Path(pipeline_path)
    source_raw=_read(pipeline,pipeline.parent,uid,checked)
    require(hashlib.sha256(source_raw).hexdigest()==PIPELINE_SHA,'Exact original pipeline bytes required')
    pipeline_tree=ast.parse(source_raw)
    for name,pin in CALLER_AST_PINS.items():
        function=next(node for node in pipeline_tree.body if isinstance(node,ast.FunctionDef) and node.name==name)
        require(_ast_sha(function)==pin,'Exact isolated primary planning caller AST required')
    nodes=next(node.value for node in pipeline_tree.body if isinstance(node,ast.Assign)
        and isinstance(node.targets[0],ast.Name) and node.targets[0].id=='NODES')
    require(tuple(item[0] for item in ast.literal_eval(nodes))==PHASES,'Complete original eleven-phase graph required')
    planners={'primary':PRIMARY/'src/horsehair/slurm.py','framework':OLD_FRAMEWORK/'src/horsehair/slurm.py'}
    if planner_paths is not None:
        require(isinstance(planner_paths,dict) and set(planner_paths)==set(planners),'Both explicit regression planner routes required')
        planners={key:Path(path) for key,path in planner_paths.items()}
    planner_bindings={}
    for label,path in planners.items():
        raw=_read(path,path.parent,uid,checked)
        require(hashlib.sha256(raw).hexdigest()==PLANNER_PINS[label],'Exact original shared planner bytes required')
        plan_function=next(node for node in ast.parse(raw).body if isinstance(node,ast.FunctionDef) and node.name=='plan')
        add=next(node for node in plan_function.body if isinstance(node,ast.FunctionDef) and node.name=='add')
        require(_ast_sha(plan_function)==PLAN_AST_SHA and _ast_sha(add)==ADD_AST_SHA,'Exact original shared task factory AST required')
        appends=[node for node in ast.walk(add) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)
            and isinstance(node.func.value,ast.Name) and node.func.value.id=='tasks' and node.func.attr=='append']
        require(len(appends)==1 and len(appends[0].args)==1 and isinstance(appends[0].args[0],ast.Dict),
            'One exact original task-record factory required')
        keys=[ast.literal_eval(key) for key in appends[0].args[0].keys]
        require(set(keys)=={'name','script','script_sha256','depends','dependency_kind','external_dependencies',
            'external_dependency_specs','completed_external_dependencies','resources'},'Original command-absent task schema required')
        planner_bindings[label]={'path':str(path),'sha256':PLANNER_PINS[label],'plan_ast_sha256':PLAN_AST_SHA,'add_ast_sha256':ADD_AST_SHA}
    workers={};controllers={};queries=[];rosters={};all_jobs=set();phases=[];omitted_commands=[]
    canonical_dir=root/'state/pipeline'/primary_release_id
    _confined(canonical_dir,root);require(canonical_dir.is_dir() and canonical_dir.stat().st_uid==uid,'Owned primary checkpoint directory required')
    entries=list(canonical_dir.iterdir());require(len(entries)<=5000,'Bounded primary checkpoint directory required')
    journal_names=sorted(path.name for path in entries if path.name.endswith('-plan-journal.json'))
    require(set(journal_names)<={phase+'-plan-journal.json' for phase in PHASES},'Unknown primary phase journal refused')
    def absent_path(path):
        _confined(path,root);require(not path.exists() and not path.is_symlink(),'Expected absent registration appeared');absent.append(str(path))
    for phase in ('managed-initial',*PHASES):
        directory=root/'plans'/(primary_release_id+'-'+phase);plan_path=directory/'plan.json'
        ledger_path=root/'state'/(primary_release_id+'-'+phase+'-submission.json')
        journal=canonical_dir/(phase+'-plan-journal.json')
        present=[path.exists() or path.is_symlink() for path in (plan_path,ledger_path,journal)]
        if phase!='managed-initial' and not any(present):
            require(not directory.exists() and not directory.is_symlink(),'Unfinished primary plan directory refused')
            for path in (directory,plan_path,ledger_path,journal):absent_path(path)
            continue
        require(present[0] and present[1] and (phase=='managed-initial' or present[2]),'Incomplete or ambiguous primary registration refused')
        plan_raw=_read(plan_path,root,uid,checked);ledger_raw=_read(ledger_path,root,uid,checked)
        plan=_strict(plan_raw);ledger=_strict(ledger_raw)
        require(plan.get('phase')==phase and plan.get('root')==str(root) and plan.get('config_sha256')==CONFIG_SHA
            and plan.get('release_identity',{}).get('sha256')==PRIMARY_SHA and ledger.get('source_sha256')==PRIMARY_SHA
            and ledger.get('config_sha256')==CONFIG_SHA and ledger.get('plan_sha256')==canonical(plan),
            'Exact primary source/configuration/frozen plan and ledger required')
        tasks=plan.get('tasks');require(isinstance(tasks,list) and 0<len(tasks)<=5000,'Bounded nonempty task list required')
        names=[task.get('name') for task in tasks]
        require(all(isinstance(name,str) and re.fullmatch('[a-z0-9][a-z0-9-]*',name) for name in names)
            and len(names)==len(set(names)) and isinstance(ledger.get('tasks'),dict) and set(ledger['tasks'])==set(names),
            'Complete unique plan and ledger task membership required')
        if phase!='managed-initial':
            record=_strict(_read(journal,root,uid,checked))
            require(record.get('state')=='submitted' and record.get('phase')==phase and record.get('source_sha256')==PRIMARY_SHA
                and record.get('plan_sha256')==canonical(plan) and record.get('ledger')==str(ledger_path),
                'Confirmed complete submitted phase journal required')
        for task in tasks:
            name=task['name'];entry=ledger['tasks'][name];job=entry.get('job_id')
            require(entry.get('state')=='submitted' and isinstance(job,str) and re.fullmatch('[1-9][0-9]*',job)
                and job not in all_jobs,'Distinct confirmed native parent identity required')
            all_jobs.add(job)
            script,historical=registered_script(root,phase,task,entry,hashlib.sha256(plan_raw).hexdigest(),
                hashlib.sha256(ledger_raw).hexdigest(),history)
            script_raw=_read(script,root,uid,checked);text=script_raw.decode('utf-8')
            if historical is not None:
                require(hashlib.sha256(script_raw).hexdigest()==historical['script_sha256'],
                    'Exact historical executable bytes required')
            require('\r' not in text and canonical(text)==task.get('script_sha256')==entry.get('script_sha256'),
                'Exact registered primary script bytes required')
            if 'command' in task:
                require(isinstance(task['command'],str) and task['command'].strip() and task['command'] in text,
                    'Present registered primary command must match exact script')
            else:
                # Both pinned original factories deliberately omit this field.
                # The complete executable script still has all three bindings.
                omitted_commands.append({'phase':phase,'task_name':name,'script_sha256':task['script_sha256']})
            resources=task.get('resources');require(isinstance(resources,dict) and type(resources.get('gpu')) is bool,
                'Explicit primary task CPU/GPU resource identity required')
            array=resources.get('array');require(entry.get('resources',resources).get('array')==array,'Ledger array roster changed')
            details={'parent_job_id':job,'phase':phase,'task_name':name,'array':array,'resources':resources,
                'plan':str(plan_path),'ledger':str(ledger_path),'script':str(script)}
            if phase=='managed-initial' and name.startswith('pipeline-controller-'):
                require(re.fullmatch(r'pipeline-controller-[0-3]',name) and not array,'Only original four controllers may be excluded')
                controllers[job]=details;continue
            require(not name.startswith('pipeline-controller-'),'Controller-like science task refused')
            indices=array_indices(array) if array else None;queries.append(job)
            if indices is not None:rosters[job]=indices
            for ident in ([job+'_'+str(index) for index in indices] if indices is not None else [job]):
                require(ident not in workers,'Duplicate logical worker refused');workers[ident]=details
            require(len(workers)<=50_000,'Bounded full primary worker roster required')
        phases.append({'phase':phase,'plan':str(plan_path),'ledger':str(ledger_path),'journal':None if phase=='managed-initial' else str(journal)})
    require(workers,'Nonempty original registered primary worker roster required')
    for path,pin in checked.items():
        base=root if Path(path).resolve().is_relative_to(root.resolve()) else Path(path).parent
        require(hashlib.sha256(_read(path,base,uid,{})).hexdigest()==pin,'Primary metadata changed after complete discovery')
    for path in absent:_confined(path,root);require(not Path(path).exists() and not Path(path).is_symlink(),'Absent registration changed')
    return {'schema_version':1,'status':'complete_source_bound_primary_worker_roster','root':str(root),'primary_release_id':primary_release_id,
        'source_helper':{'path':str(helper),'sha256':HELPER_SHA},'source_pipeline':{'path':str(pipeline),'sha256':PIPELINE_SHA},
        'pipeline_path_override':pipeline_path is not None or planner_paths is not None,
        'source_planners':planner_bindings,'source_planner_callers_ast_sha256':dict(CALLER_AST_PINS),
        'legacy_command_omission_source_verified':True,'legacy_tasks_without_command':omitted_commands,
        'phases':phases,'journal_names':journal_names,
        'workers':workers,'excluded_controllers':controllers,
        'query_ids':sorted(queries,key=int),'array_rosters':rosters,'expected_worker_ids':sorted(workers),
        'checked_paths':checked,'absent_paths':absent,**FALSE_FLAGS}


def verify_primary_terminal(roster,records,*,owner='pwa209',account=None):
    """Validate caller's fresh strict native rows; never query or normalize costs."""
    require(roster.get('status')=='complete_source_bound_primary_worker_roster' and isinstance(records,dict),'Complete source-bound roster and native rows required')
    expected=set(roster['expected_worker_ids']);require(expected==set(roster['workers']) and expected<=set(records),'Every frozen worker/member needs its own native row')
    require(set(records)-expected<=set(roster['array_rosters']),'Foreign or unbound native worker row refused')
    physical={};allocated=0;unallocated=0;accounts={}
    def expected_account(ident):
        gpu=roster['workers'][ident]['resources'].get('gpu');require(type(gpu) is bool,'Frozen primary CPU/GPU identity required')
        expected='def-ptewarie_gpu' if gpu else 'def-ptewarie_cpu'
        require(account is None or account==expected,'Explicit account conflicts with frozen primary CPU/GPU identity')
        return expected
    for ident in sorted(expected):
        row=records[ident];raw=row.get('job_id_raw');parent=roster['workers'][ident]['parent_job_id']
        accounts[ident]=expected_account(ident)
        require(row.get('user')==owner and row.get('account')==accounts[ident] and row.get('state') in TERMINAL
            and isinstance(raw,str) and re.fullmatch('[1-9][0-9]*',raw)
            and re.fullmatch('[0-9]+:[0-9]+',str(row.get('exit_code','')))
            and type(row.get('cpus')) is int and type(row.get('elapsed_seconds')) is int,'Owned exact terminal native row required')
        zero=(row['state']=='CANCELLED' and row['exit_code']=='0:0' and row['cpus']==0 and row['elapsed_seconds']==0
            and row.get('alloc_tres')=='' and row.get('node_list')=='None assigned' and row.get('start') in {'None','Unknown'}
            and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]+',str(row.get('end','')))
            and row.get('max_rss') in {'','0','0K'})
        if zero:
            require(type(row.get('unallocated_metadata')) is bool
                and row['unallocated_metadata']==('_' in ident and raw==parent),
                'Explicit frozen never-started metadata required');unallocated+=1
        else:
            require(row['cpus']>0 and row['elapsed_seconds']>0 and row.get('unallocated_metadata') is False
                and row.get('alloc_tres') and row.get('node_list') not in {'','None','Unknown','None assigned'}
                and row.get('start') not in {'','None','Unknown',None} and row.get('end') not in {'','None','Unknown',None},
                'Positive actual allocated terminal proof required')
            allocated+=1
        if row['unallocated_metadata'] is False:
            require(raw not in physical,'Conflicting physical native worker identities refused');physical[raw]=ident
        if '_' not in ident:require(raw==ident,'Scalar physical identity mismatch')
    for ident in set(records)-expected:
        members=[member for member in expected if roster['workers'][member]['parent_job_id']==ident]
        require(members and records[ident].get('user')==owner and records[ident].get('account')==expected_account(members[0]),
            'Foreign array summary refused')
    selected={ident:records[ident] for ident in sorted(expected)}
    return {'status':'all_registered_primary_workers_positively_terminal','workers':len(expected),
        'allocated_terminal_workers':allocated,'explicit_unallocated_cancelled_workers':unallocated,
        'expected_accounts':accounts,'native_records':selected,'native_records_sha256':canonical(selected),**FALSE_FLAGS}


verify_terminal=verify_primary_terminal
