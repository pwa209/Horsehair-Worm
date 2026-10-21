"""Prospective, read-only structural audit of the Zenodo17175344 DAM source.

The deposited R program is parsed as literal metadata, never executed. No effect,
sleep bout, causal exposure, XY trajectory or original H1-H3 gate is computed.
Native allocation, resource accounting and storage admission belong to the caller.
"""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
import argparse
import hashlib
import json
import math
import os
import re
import time

ROOT=Path('/scratch/pwa209/horsehair-worm')
RECORD_ID='17175344'
CODE_NAME='code_process_three_datasets.R'
EXPECTED_EXPERIMENTS=3
EXPECTED_FILES=6
FIELDS=['Index','Date','Time','Monitor_status','Extras','Monitor_number','Tube_number','Data_type','unused','Light_sensor']
MONTHS={name:index for index,name in enumerate(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'],1)}
MANUAL_URL='https://trikinetics.com/Downloads/DAMSystem3%20Software%20Data%20Sheet.pdf'


def require(value,message):
    if not value:raise ValueError(message)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def bounded(path,limit=16_000_000):
    with Path(path).open('rb') as stream:raw=stream.read(limit+1)
    require(len(raw)<=limit,'Bounded metadata byte limit exceeded');return raw


def ordinary(path,*,fresh=False):
    original=Path(path);root=ROOT.resolve();resolved=original.resolve()
    require(original.is_absolute() and resolved.is_relative_to(root) and resolved!=root,'Personal study path required')
    prefixes=[p for p in [ROOT,root] if original.is_relative_to(p)]
    require(prefixes,'Foreign redirected source namespace refused');prefix=max(prefixes,key=lambda p:len(p.parts))
    require(all(not p.is_symlink() for p in [original,*original.parents] if p!=prefix and p.is_relative_to(prefix)),
            'Subordinate source/output symlinks refused')
    if fresh:require(not resolved.exists(),'Fresh immutable audit output required')
    else:require(resolved.is_file(),'Required ordinary source file missing')
    return resolved


def locator(binding):
    require(set(binding)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',binding['sha256']),'Exact metadata locator required')
    path=ordinary(binding['path']);raw=bounded(path)
    require(sha(raw)==binding['sha256'],'Pinned metadata SHA mismatch')
    return path,raw


def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    require(len(raw)<=20_000_000,'Bounded structural report exceeds20MB')
    with Path(path).open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())


def strip_comments(text):
    output=list(text);quote=None;escaped=False;comment=False
    for index,char in enumerate(text):
        if comment:
            if char=='\n':comment=False
            else:output[index]=' '
        elif quote:
            if escaped:escaped=False
            elif char=='\\':escaped=True
            elif char==quote:quote=None
        elif char in '\"\'':quote=char
        elif char=='#':output[index]=' ';comment=True
    require(quote is None,'Unterminated R string; literal mapping is ambiguous')
    return ''.join(output)


def closing(text,start):
    require(text[start]=='(','Literal call opening parenthesis required')
    depth=0;quote=None;escaped=False
    for index in range(start,len(text)):
        char=text[index]
        if quote:
            if escaped:escaped=False
            elif char=='\\':escaped=True
            elif char==quote:quote=None
        elif char in '\"\'':quote=char
        elif char=='(':depth+=1
        elif char==')':
            depth-=1
            if depth==0:return index+1
    raise ValueError('Unclosed literal R call')


class Literals:
    """A deliberately small data grammar, with no R evaluation or function calls."""
    def __init__(self,text):self.text=text;self.position=0
    def ws(self):
        while self.position<len(self.text) and self.text[self.position].isspace():self.position+=1
    def take(self,char):
        self.ws();require(self.text.startswith(char,self.position),'Unsupported R literal syntax')
        self.position+=len(char)
    def word(self):
        self.ws();match=re.match(r'[A-Za-z_][A-Za-z_0-9.]*',self.text[self.position:])
        require(match is not None,'Literal identifier required');self.position+=len(match[0]);return match[0]
    def value(self):
        self.ws();require(self.position<len(self.text),'Missing literal value');char=self.text[self.position]
        if char in '\"\'':
            quote=char;self.position+=1;result=[]
            while self.position<len(self.text):
                char=self.text[self.position];self.position+=1
                if char in '\r\n':raise ValueError('Multiline mapping string refused')
                # Keep only the ordinary quoted-string escape subset needed by
                # source paths/labels, rather than interpreting arbitrary R text.
                if char==quote:return ''.join(result)
                if char=='\\':
                    require(self.position<len(self.text),'Truncated string escape')
                    escape=self.text[self.position];self.position+=1
                    require(escape in {'\\','\"',"'",'n','r','t'},'Unsupported R string escape')
                    char={'n':'\n','r':'\r','t':'\t'}.get(escape,escape)
                result.append(char)
            raise ValueError('Unterminated literal string')
        if char.isdigit():
            match=re.match(r'[0-9]+',self.text[self.position:]);self.position+=len(match[0]);return int(match[0])
        name=self.word();self.ws()
        if self.position>=len(self.text) or self.text[self.position]!='(':return {'identifier':name}
        require(name in {'list','c'},'Executable/nonliteral mapping expression refused');self.take('(')
        values=[];mapping={}
        self.ws();require(self.position<len(self.text),'Truncated literal call')
        if self.text[self.position]==')':self.take(')');return {} if name=='list' else []
        while True:
            if name=='list':
                key=self.word();self.take('=');require(key not in mapping,'Duplicate literal mapping name')
                mapping[key]=self.parse_value()
            else:values.append(self.parse_value())
            self.ws();require(self.position<len(self.text),'Truncated literal call')
            if self.text[self.position]==')':self.take(')');break
            self.take(',')
        return mapping if name=='list' else values
    def parse_value(self):
        self.ws()
        return self.value()
    def complete(self):
        result=self.parse_value();self.ws();require(self.position==len(self.text),'Trailing executable mapping syntax refused');return result


def span(code,start,end):
    return {'first_line':code.count('\n',0,start)+1,'last_line':code.count('\n',0,end)+1,
            'source_text_sha256':sha(code[start:end].encode('utf8'))}


def parse_code(raw):
    text=raw.decode('utf-8-sig');masked=strip_comments(text);mappings={};mapping_spans={}
    for match in re.finditer(r'(?m)^\s*(exp[0-9]+_channels)\s*<-\s*list\s*\(',masked):
        name=match[1];start=masked.index('list',match.start());end=closing(masked,masked.index('(',start))
        require(name not in mappings,'Repeated mapping assignment requires human review')
        mappings[name]=Literals(masked[start:end]).complete();mapping_spans[name]=span(text,match.start(),end)
    columns=[]
    for monitor in ('df_m1','df_m2'):
        pattern=r'colnames\('+monitor+r'\)\s*\[1:10\]\s*<-\s*c\s*\('
        found=list(re.finditer(pattern,masked));require(len(found)==1,'One explicit ten-column metadata label assignment required')
        start=masked.rfind('c',found[0].start(),found[0].end());end=closing(masked,masked.index('(',start))
        columns.append(Literals(masked[start:end]).complete())
    require(columns==[FIELDS,FIELDS],'Deposited metadata column labels differ from qualified parser')
    require(re.search(r'for\s*\(i\s+in\s+1:32\)',masked) and 'orders="d b y H M S"' in re.sub(r'\s*=\s*','=',masked)
            and 'tz="UTC"' in re.sub(r'\s*=\s*','=',masked),'Explicit32 channels/author UTC date parser required')
    filters=[]
    for match in re.finditer(r'Data_type\s*%in%\s*c\s*\(',masked):
        start=masked.index('c',match.start()+len('Data_type'));end=closing(masked,masked.index('(',start))
        filters.append(Literals(masked[start:end]).complete())
    require(filters==[['MT','CT'],['MT','CT']],'Exact separate MT/CT selection required')
    experiments=[];used_mappings=set()
    for match in re.finditer(r'(?m)^\s*(exp[0-9]+)\s*<-\s*process_experiment\s*\(',masked):
        start=masked.index('(',match.start());end=closing(masked,start)
        values=Literals('c'+masked[start:end]).complete()
        require(len(values)==7 and all(isinstance(v,str) for v in values[:6])
                and isinstance(values[6],dict) and set(values[6])=={'identifier'},'Explicit seven-argument experiment call required')
        mapping_name=values[6]['identifier'];require(mapping_name in mappings,'Unresolved experiment channel mapping');used_mappings.add(mapping_name)
        monitors=mappings[mapping_name];require(set(monitors)=={'monitor1','monitor2'},'Two explicitly mapped source monitors required')
        files=[]
        for monitor in (1,2):
            path=values[monitor-1].replace('\\','/');parts=path.split('/')
            require(len(parts)>=2 and parts[-1]==f'Monitor{monitor}.txt','Explicit monitor filename suffix required')
            conditions=monitors[f'monitor{monitor}'];require(isinstance(conditions,dict),'Literal condition/channel dictionary required')
            by_channel={str(channel):[] for channel in range(1,33)}
            for condition,channels in conditions.items():
                require(isinstance(channels,list) and channels and all(type(c) is int and 1<=c<=32 for c in channels)
                        and len(set(channels))==len(channels),'Literal unique channel numbers1..32 required')
                for channel in channels:by_channel[str(channel)].append(condition)
            files.append({'monitor_label_in_author_code':monitor,'source_path_literal':path,
                'provider_name':parts[-2]+'_'+parts[-1],'condition_channels':conditions,'channel_conditions':by_channel,
                'unmapped_channels':[int(k) for k,v in by_channel.items() if not v],
                'ambiguous_channels':{k:v for k,v in by_channel.items() if len(v)>1}})
        begin=datetime.strptime(values[4],'%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        finish=datetime.strptime(values[5],'%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        require(finish>begin and (finish-begin).total_seconds()%60==0,'Finite minute-aligned author window required')
        experiments.append({'id':values[2],'name_literal':values[3],'source_assignment':match[1],
            'window':{'start':begin.isoformat(),'end':finish.isoformat(),'inclusive':True,'expected_minute_ticks':int((finish-begin).total_seconds()/60)+1,
                'timezone_assigned_by_author_code':'UTC','hardware_timezone_verified':False,'exposure_time_verified':False},
            'mapping_source':mapping_spans[mapping_name],'call_source':span(text,match.start(),end),'files':files})
    require(len(experiments)==EXPECTED_EXPERIMENTS and len({e['id'] for e in experiments})==len(experiments)
            and used_mappings==set(mappings),'Complete unique deposited experiment-call/mapping roster required')
    labels=sorted({condition for experiment in experiments for file in experiment['files'] for condition in file['condition_channels']})
    virus_comments=[{'text':line.strip().lstrip('#').strip(),'line':number,'source_text_sha256':sha(line.encode())}
                    for number,line in enumerate(text.splitlines(),1) if line.lstrip().startswith('#') and 'Virus as fixed effect' in line]
    return {'code_sha256':sha(raw),'R_executed':False,'metadata_fields':FIELDS,'activity_columns':[f'Channel_{i}' for i in range(1,33)],
        'literal_condition_labels':labels,'author_condition_description_comments':virus_comments,
        'condition_interpretation':'author coded groups only; no verified individual infection, genotype, randomization, dose or exposure onset',
        'selected_data_types':['MT','CT'],'physical_units_interpretation':'separate moves/counts activity totals; no XY/distance reconstruction',
        'instrument_documentation_reference':MANUAL_URL,'manual_bytes_bound':False,'experiments':experiments}


def parse_time(date,clock):
    match=re.fullmatch(r'\s*([0-9]{1,2})\s*([A-Za-z]{3})\s*([0-9]{2}|[0-9]{4})\s*',date)
    require(match is not None,'Unparseable source date');day,month,year=match.groups();year=int(year)
    if year<100:year+=2000 if year<=68 else 1900
    require(month.lower() in MONTHS,'Unknown source month');parsed=datetime.strptime(clock.strip(),'%H:%M:%S')
    return int(datetime(year,MONTHS[month.lower()],int(day),parsed.hour,parsed.minute,parsed.second,tzinfo=timezone.utc).timestamp())


def _integer(value):
    return int(value.strip()) if re.fullmatch(r'[+-]?[0-9]+',value.strip()) else None


def scan(path,experiment,file_mapping,deadline):
    begin=int(datetime.fromisoformat(experiment['window']['start']).timestamp());end=int(datetime.fromisoformat(experiment['window']['end']).timestamp())
    expected=experiment['window']['expected_minute_ticks'];streams={};widths=Counter();types=Counter();statuses=Counter();physical=Counter()
    md5=hashlib.md5();checksum=hashlib.sha256();size=0;bad_dates=0;bad_metadata=0;rows=0;examples=[];numeric_metadata=Counter()
    before=path.stat()
    with path.open('rb') as source:
        for rows,raw in enumerate(source,1):
            if rows%2048==0 and time.monotonic()>deadline:raise TimeoutError('Bounded structural audit deadline reached')
            md5.update(raw);checksum.update(raw);size+=len(raw)
            require(len(raw)<=32_768,'Oversized source row requires a separately reviewed parser')
            line=raw.decode('utf-8-sig' if rows==1 else 'utf-8').rstrip('\r\n');cells=line.split('\t');widths[len(cells)]+=1
            if len(cells)!=42 and len(examples)<10:examples.append({'line':rows,'issue':'column_count','columns':len(cells)})
            if len(cells)<10:bad_metadata+=1;continue
            datatype=cells[7];types[datatype]+=1;statuses[cells[3]]+=1;physical[cells[5]]+=1
            require(all(len(cells[index])<=128 for index in range(10)),'Bounded source metadata limit exceeded')
            monitor,tube,status=_integer(cells[5]),_integer(cells[6]),_integer(cells[3])
            if monitor is None or tube is None or status is None:bad_metadata+=1
            for index in (0,3,4,5,6,8,9):
                if _integer(cells[index]) is None:numeric_metadata[FIELDS[index]]+=1
            try:stamp=parse_time(cells[1],cells[2])
            except (ValueError,OverflowError):bad_dates+=1;continue
            key=(cells[5],cells[6],datatype)
            require(key in streams or len(streams)<4096,'Bounded source stream limit exceeded')
            state=streams.setdefault(key,{'rows':0,'first':stamp,'last':stamp,'previous':None,'out_of_order':0,'duplicate_clocks':0,
                'conflicting_duplicate_clocks':0,'seen':{},'window_seen':set(),'off_grid_in_window':0,'status_not_one_in_window':0,
                'selected_total':datatype in {'MT','CT'} and tube==0,
                'quality':[{'missing':0,'nonfinite':0,'nonnumeric':0,'negative':0,'noninteger':0,'valid_status_one_grid':0,'bits':0} for _ in range(32)]})
            state['rows']+=1;state['first']=min(state['first'],stamp);state['last']=max(state['last'],stamp)
            if state['previous'] is not None and stamp<state['previous']:state['out_of_order']+=1
            state['previous']=stamp;payload=hashlib.sha256('\t'.join(cells[3:]).encode()).digest()
            if stamp in state['seen']:
                state['duplicate_clocks']+=1;state['conflicting_duplicate_clocks']+=int(state['seen'][stamp]!=payload)
            else:state['seen'][stamp]=payload
            inside=begin<=stamp<=end
            if inside:
                state['window_seen'].add(stamp);state['off_grid_in_window']+=int((stamp-begin)%60!=0)
                state['status_not_one_in_window']+=int(status!=1)
            if state['selected_total'] and inside:
                for index,quality in enumerate(state['quality']):
                    raw_value=cells[10+index].strip() if len(cells)>10+index else ''
                    if raw_value in {'','NA','NaN','nan','NULL'}:quality['missing']+=1;continue
                    try:value=float(raw_value)
                    except ValueError:quality['nonnumeric']+=1;continue
                    if not math.isfinite(value):quality['nonfinite']+=1;continue
                    if value<0:quality['negative']+=1
                    if not value.is_integer():quality['noninteger']+=1
                    if len(cells)==42 and status==1 and value>=0 and value.is_integer() and (stamp-begin)%60==0:
                        quality['valid_status_one_grid']+=1;quality['bits']|=1<<((stamp-begin)//60)
    after=path.stat();require((before.st_size,before.st_mtime_ns,before.st_ino)==(after.st_size,after.st_mtime_ns,after.st_ino),
                            'Source file changed during structural read')
    output=[]
    for (monitor,tube,datatype),state in sorted(streams.items()):
        clock={key:state[key] for key in ('rows','out_of_order','duplicate_clocks','conflicting_duplicate_clocks','off_grid_in_window','status_not_one_in_window')}
        present={stamp for stamp in state['window_seen'] if (stamp-begin)%60==0}
        clock.update({'first':datetime.fromtimestamp(state['first'],timezone.utc).isoformat(),
            'last':datetime.fromtimestamp(state['last'],timezone.utc).isoformat(),
            'timezone_basis':'author_code_assigned_UTC_not_verified_hardware_timezone',
            'window_unique_grid_ticks':len(present),'window_missing_record_ticks':expected-len(present)})
        entry={'raw_monitor_number':monitor,'raw_tube_number':tube,'raw_data_type':datatype,'selected_MT_CT_monitor_total':state['selected_total'],'clock':clock}
        if state['selected_total']:
            quality=[]
            for channel,value in enumerate(state['quality'],1):
                bits=value.pop('bits');quality.append({'channel':channel,'conditions_from_literal_R':file_mapping['channel_conditions'][str(channel)],
                    **value,'unique_valid_status_one_grid_ticks':bits.bit_count(),'missing_or_invalid_value_grid_ticks':expected-bits.bit_count()})
            entry['channel_value_quality_in_author_window']=quality
        output.append(entry)
    return {'path':str(path),'bytes':size,'md5':md5.hexdigest(),'sha256':checksum.hexdigest(),'raw_rows':rows,
        'column_count_histogram':dict(sorted(widths.items())),'data_type_histogram':dict(sorted(types.items())),
        'monitor_status_histogram':dict(sorted(statuses.items())),'raw_monitor_number_histogram':dict(sorted(physical.items())),
        'invalid_datetime_rows':bad_dates,'invalid_metadata_rows':bad_metadata,'schema_examples':examples,'streams':output,
        'invalid_integral_metadata_fields':dict(numeric_metadata),
        'auditor_channel_quality_scope':'MT/CT tube-number0 only; other streams retained as structural metadata',
        'author_monitor_label':file_mapping['monitor_label_in_author_code'],
        'raw_monitor_numbers_differing_from_author_label':[key for key in sorted(physical) if _integer(key)!=file_mapping['monitor_label_in_author_code']],
        'missing_selected_monitor_total_types':[kind for kind in ('MT','CT') if not any(entry['raw_data_type']==kind and entry['selected_MT_CT_monitor_total'] for entry in output)],
        'all_records_retained_in_source':True,'records_repaired_or_imputed':False,'effect_statistics_computed':False}


def component_binding(item,provider,record_sha):
    require(set(item)=={'name','path','bytes','provider_md5','sha256','transfer_receipt_path','transfer_receipt_sha256'},'Exact source component schema required')
    require(item['name']==provider['key'] and item['bytes']==provider['size'] and provider['checksum']=='md5:'+item['provider_md5']
        and re.fullmatch('[0-9a-f]{32}',item['provider_md5']) and re.fullmatch('[0-9a-f]{64}',item['sha256']),
        'Provider name/bytes/MD5 differs from manifest')
    path=ordinary(item['path']);require(path.name==item['name'] and path.stat().st_size==item['bytes'],'Source name/size differs')
    receipt_binding={'path':item['transfer_receipt_path'],'sha256':item['transfer_receipt_sha256']}
    _,raw=locator(receipt_binding);receipt=json.loads(raw)
    if receipt.get('status')=='small_public_file_management_transfer':
        require(receipt.get('provider_code_executed') is False and receipt.get('scientific_gates_cleared') is False
                and receipt.get('effects_calculated') is False,'Code transfer flags changed')
        candidates=[value for value in receipt.get('files',[]) if value.get('record_id')==RECORD_ID and value.get('name')==item['name']]
        require(len(candidates)==1,'Unique record/file code-transfer receipt required');proof=candidates[0]
    else:
        proof=receipt
        require(receipt.get('metadata_sha256')==record_sha and receipt.get('expected_provider_md5')==provider['checksum']
                and receipt.get('source_url')==provider['links']['self'] and receipt.get('effects_inspected') is False
                and receipt.get('provider_code_executed') is False and receipt.get('scientific_gates_cleared') is False,'Transfer provider/record/flags changed')
    require(proof.get('status')=='provider_verified' and proof.get('provider_md5_verified') is True
            and all(proof.get(key)==item[key] for key in ('path','bytes','sha256')),'Positive exact provider-verified transfer binding required')
    return path,{'path':str(path),'provider_name':item['name'],'provider_bytes':item['bytes'],'provider_md5':item['provider_md5'],
        'manifest_sha256':item['sha256'],'transfer_receipt':receipt_binding,'range_original_partial_reference':receipt.get('original_partial_preserved'),
        'partial_reference_independently_rehashed':False}


def audit(manifest_path,manifest_sha,output,*,max_seconds=5400):
    started=time.monotonic();require(type(max_seconds) is int and 60<=max_seconds<=5400,'Finite structural-only audit duration required')
    manifest_path,raw=locator({'path':str(manifest_path),'sha256':manifest_sha});manifest=json.loads(raw)
    require(manifest.get('schema_version')==1 and manifest.get('effects_inspected') is False
            and manifest.get('scientific_gates_cleared') is False and manifest.get('provider_code_executed') is False,'Prospective structural source manifest required')
    _,record_raw=locator(manifest['record']);record=json.loads(record_raw)
    require(str(record['id'])==RECORD_ID and isinstance(record['files'],list),'Exact pinned official record roster required')
    providers={item['key']:item for item in record['files']};items=[manifest['code'],*manifest['files']]
    require(len(providers)==len(record['files'])==len(items)==EXPECTED_FILES+1 and len(manifest['files'])==EXPECTED_FILES
            and {item['name'] for item in items}==set(providers) and manifest['code']['name']==CODE_NAME,'Complete unique official file/code roster required')
    code_path,code_binding=component_binding(manifest['code'],providers[CODE_NAME],manifest['record']['sha256'])
    code_raw=bounded(code_path,1_000_000)
    require(len(code_raw)==manifest['code']['bytes'] and sha(code_raw)==manifest['code']['sha256']
            and hashlib.md5(code_raw).hexdigest()==manifest['code']['provider_md5'],'Code bytes/MD5/SHA differ from provider')
    mapping=parse_code(code_raw);mapped={file['provider_name']:(experiment,file) for experiment in mapping['experiments'] for file in experiment['files']}
    require(len(mapped)==EXPECTED_FILES and set(mapped)=={item['name'] for item in manifest['files']},'Literal author paths do not join exactly to provider file roster')
    deadline=started+max_seconds
    if os.environ.get('SLURM_JOB_END_TIME'):
        remaining=float(os.environ['SLURM_JOB_END_TIME'])-time.time()-180
        require(math.isfinite(remaining) and remaining>=60,'Verified allocation finalization margin exhausted')
        deadline=min(deadline,time.monotonic()+remaining)
    output=ordinary(output,fresh=True);require(output.is_relative_to(ROOT.resolve()/'maintenance'),'Fresh personal maintenance audit namespace required')
    output.mkdir(parents=True);exclusive(output/'literal-code-mapping.json',mapping)
    audits=[];bindings=[]
    try:
        for item in manifest['files']:
            require(time.monotonic()<deadline,'Structural audit deadline exhausted')
            path,binding=component_binding(item,providers[item['name']],manifest['record']['sha256']);experiment,file=mapped[item['name']]
            result=scan(path,experiment,file,deadline)
            require(result['bytes']==item['bytes'] and result['sha256']==item['sha256'] and result['md5']==item['provider_md5'],
                    'Independent full-data provider byte/MD5/SHA verification failed')
            result.update(provider_name=item['name'],experiment_id=experiment['id'],monitor_label_in_author_code=file['monitor_label_in_author_code'])
            audits.append(result);bindings.append(binding)
            exclusive(output/('file-'+str(len(audits))+'.json'),result)
        locator({'path':str(manifest_path),'sha256':manifest_sha});locator(manifest['record'])
        require(sha(bounded(code_path,1_000_000))==manifest['code']['sha256'],'Deposited code changed during audit')
        for binding in [code_binding,*bindings]:locator(binding['transfer_receipt'])
        result={'status':'complete_read_only_structural_audit','schema_version':1,'record_id':RECORD_ID,
            'official_record_url':'https://zenodo.org/records/'+RECORD_ID,'manifest':{'path':str(manifest_path),'sha256':manifest_sha},
            'record':manifest['record'],'code':code_binding,'auditor_sha256':sha(Path(__file__).read_bytes()),'file_bindings':bindings,
            'literal_mapping_path':str(output/'literal-code-mapping.json'),'literal_mapping_sha256':sha((output/'literal-code-mapping.json').read_bytes()),
            'structural_file_receipts':[{'path':str(output/('file-'+str(index)+'.json')),'sha256':sha((output/('file-'+str(index)+'.json')).read_bytes())} for index in range(1,len(audits)+1)],
            'files_audited':len(audits),'all_provider_identities_reverified':True,'elapsed_seconds':time.monotonic()-started,
            'measurement_capability':'minute-bin tube activity totals; other raw data types preserved but not repurposed asXY',
            'hardware_timezone_verified':False,'infection_or_exposure_timestamp_verified':False,'XY_available':False,
            'beam_count_to_distance_conversion_qualified':False,'original_H1_H2_H3_gates_cleared':False,
            'effects_inspected':False,'effects_calculated':False,'provider_code_executed':False,'scientific_gates_cleared':False,
            'scientific_result':False,'analysis_admitted':False,'complete_declared_study':False,'admission_authority':False}
        exclusive(output/'receipt.json',result);return result
    except BaseException as error:
        exclusive(output/'failure.json',{'status':'incomplete_structural_audit','finished_files':len(audits),'error_type':type(error).__name__,
            'message':str(error),'source_manifest_sha256':manifest_sha,'scientific_result':False,'scientific_gates_cleared':False,'admission_authority':False})
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--manifest',type=Path,required=True)
    parser.add_argument('--manifest-sha256',required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--max-seconds',type=int,default=5400);args=parser.parse_args()
    result=audit(args.manifest,args.manifest_sha256,args.output,max_seconds=args.max_seconds)
    print(json.dumps({key:result[key] for key in ('status','record_id','files_audited','scientific_gates_cleared','admission_authority')},sort_keys=True))


if __name__=='__main__':main()
