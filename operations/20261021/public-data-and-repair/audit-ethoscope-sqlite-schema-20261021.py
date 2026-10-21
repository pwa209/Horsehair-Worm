"""Read-only, outcome-blind structural audit of the frozen ethoscope DB canary.

Only schema, bounded variable/clock metadata, ROI IDs and raw timestamp columns
are inspected. Position, velocity, activity and all other outcome columns are
denied by the SQLite authorizer. Unknown units, sample grids and exposure clocks
remain unknown. This helper grants no scientific or resource admission.
"""
from __future__ import annotations

from collections import Counter
import argparse
import csv
import getpass
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from urllib.parse import quote
import zipfile

ROOT=Path('/scratch/pwa209/horsehair-worm')
PINS={
 'record':'2514b6e65c2892c69db797263ee545d546a76beee4ac931fc3b1257f45992cce',
 'checksum':'39d6f17957c394d95b3746df1aa4ff75ba15ae4722ffede293ed0c6de9c729c6',
 'index':'ffc58aa960a5fdaec167107c4fece8fd7789fb5daad79675c9aa85980dee46b6',
 'documentation':'499d00647a734caa528ee33cc4ab7e1f66217581355c3cdae59ac82f9fdfcee6',
}
ACQUISITION_HELPER_SHA='30a843a2771196a3eec1e202f8e695fd1d41e58b6423a73218ec74b6285ea17c'
RAW_BYTES=13594624
RAW_MD5='e261acc31375cb122164c92ed957206e'
RAW_CRC='7a767f04'
COMPRESSED_BYTES=6594774
DB_NAME='2022-04-22_11-21-12_15517bc8ec75403fb282ef8e166d150a.db'
METADATA_SHA='83230109101cb01e57fb5f0d64b48b1965f07d9f45cf9c41db8105357c3d1616'
SELECTED_DATE='2022-04-22'
SELECTED_MACHINE='ETHOSCOPE_155'
MAX_TABLES=512
MAX_ROWS=2_000_000
MAX_METADATA_ROWS=4096
MAX_CELL_BYTES=65536
SCHEMA_COLUMNS={'name','type','tbl_name','rootpage','sql'}
CLOCK_METADATA_KEYS={'date_time','start_time','date','start_datetime','epoch',
 'time_unit','timestamp_unit','reference_hour','timezone','utc_offset',
 'machine_name','machine_id','version','tracker','roi_builder'}


def require(condition,message):
    if not condition:raise ValueError(message)


def hashes(path):
    before=path.stat();digests={name:hashlib.new(name) for name in ('sha256','md5')};count=0
    with path.open('rb') as handle:
        require((os.fstat(handle.fileno()).st_ino,os.fstat(handle.fileno()).st_size)==(before.st_ino,before.st_size),'File changed while opening')
        for block in iter(lambda:handle.read(65536),b''):
            count+=len(block)
            require(count<=20_000_000,'Bounded canary/document bytes exceeded')
            for value in digests.values():value.update(block)
    after=path.stat()
    require((before.st_size,before.st_mtime_ns,before.st_ino)==(after.st_size,after.st_mtime_ns,after.st_ino),'File changed while hashing')
    return {'bytes':count,**{name:value.hexdigest() for name,value in digests.items()}}


def ordinary(path,*,fresh=False):
    path=Path(path);root=ROOT.resolve(strict=True);resolved=path.resolve(strict=not fresh)
    require(path.is_absolute() and root in resolved.parents,'Personal scratch path required')
    lexical=ROOT.absolute()
    require(lexical in path.parents,'Declared personal scratch namespace required')
    current=lexical
    for part in path.relative_to(lexical).parts:
        current=current/part;require(not current.is_symlink(),'Subordinate symlink refused')
    if fresh:require(not path.exists(),'Fresh immutable output required')
    else:
        info=path.stat();require(stat.S_ISREG(info.st_mode) and info.st_nlink==1,'Ordinary single-link input required')
    return resolved


def bound_json(path,digest):
    path=ordinary(path);require(path.stat().st_size<=2_000_000,'Bounded metadata JSON required')
    raw=path.read_bytes();require(hashlib.sha256(raw).hexdigest()==digest,'Metadata SHA binding failed')
    def pairs(items):
        value={}
        for key,item in items:
            require(key not in value,'Duplicate JSON key refused');value[key]=item
        return value
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('Nonfinite JSON refused')))


def exclusive(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    require(len(raw)<=10_000_000,'Bounded structural result exceeded')
    with Path(path).open('xb') as handle:handle.write(raw);handle.flush();os.fsync(handle.fileno())


def native_context():
    job=os.environ.get('SLURM_JOB_ID','')
    require(re.fullmatch('[1-9][0-9]*',job) is not None and getpass.getuser()=='pwa209','Native owned Slurm allocation required')
    require(os.environ.get('SLURM_CPUS_PER_TASK')=='1','Exactly1CPU auditor required')
    for key in ('SLURM_JOB_GPUS','SLURM_STEP_GPUS','CUDA_VISIBLE_DEVICES'):
        require(os.environ.get(key,'') in {'','-1','NoDevFiles','none','None'},'CPU-only auditor refuses any GPU device assignment')
    end=float(os.environ.get('SLURM_JOB_END_TIME','nan'))
    require(math.isfinite(end) and end-time.time()>=240,'Native allocation finalization margin required')
    return {'job_id':job,'cpus':1,'gpus':0,'allocation_end_unix':end}


def identifier(value):
    require(isinstance(value,str) and re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,63}',value),'Unsupported SQL identifier; inventory only')
    return '"'+value+'"'


class ReadOnlyDatabase:
    """Strict read capability: schema + selected metadata + ROI IDs/t only."""
    def __init__(self,path,deadline):
        self.path=Path(path);self.deadline=deadline;self.allowed={
            'sqlite_master':set(SCHEMA_COLUMNS),'sqlite_schema':set(SCHEMA_COLUMNS)}
        self.tables=set();self.denied=[]
        uri='file:'+quote(str(self.path.resolve()).replace('\\','/'),safe='/:')+'?mode=ro&immutable=1'
        self.connection=sqlite3.connect(uri,uri=True,timeout=1)
        try:
            self.connection.execute('PRAGMA query_only=ON')
            self.connection.execute('PRAGMA trusted_schema=OFF')
            self.connection.execute('PRAGMA temp_store=MEMORY')
            self.connection.enable_load_extension(False)
            if hasattr(self.connection,'setlimit'):
                self.connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,1_000_000)
                self.connection.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH,100_000)
                self.connection.setlimit(sqlite3.SQLITE_LIMIT_COLUMN,256)
                self.connection.setlimit(sqlite3.SQLITE_LIMIT_ATTACHED,0)
            self.connection.set_progress_handler(self._progress,1000)
            self.connection.set_authorizer(self._authorizer)
        except BaseException:
            self.connection.close();raise

    def _progress(self):return int(time.monotonic()>=self.deadline)

    def _authorizer(self,action,arg1,arg2,database,origin):
        allowed=(action==sqlite3.SQLITE_SELECT
            or action==sqlite3.SQLITE_READ and database=='main' and arg2 in self.allowed.get(arg1,set())
            or action==sqlite3.SQLITE_PRAGMA and arg1=='table_info' and arg2 in self.tables
            or action==sqlite3.SQLITE_FUNCTION and isinstance(arg2,str) and arg2.lower() in {'count','min','max'})
        if not allowed:
            self.denied.append({'action':action,'table_or_operation':arg1,'column_or_argument':arg2,'database':database})
        return sqlite3.SQLITE_OK if allowed else sqlite3.SQLITE_DENY

    def check(self):require(time.monotonic()<self.deadline,'Finite structural audit deadline exhausted')

    def rows(self,sql,parameters=(),*,limit=MAX_METADATA_ROWS):
        self.check();cursor=self.connection.execute(sql,parameters);result=[]
        for row in cursor:
            self.check();require(len(result)<limit,'Bounded structural row limit exceeded')
            require(all(not isinstance(v,(str,bytes)) or len(v.encode() if isinstance(v,str) else v)<=MAX_CELL_BYTES for v in row),'Bounded structural cell exceeded')
            result.append(row)
        return result

    def close(self):self.connection.close()
    def __enter__(self):return self
    def __exit__(self,*args):self.close()


def _metadata_value(value,*,literal=False):
    if value is None:return {'kind':'null'}
    raw=value if isinstance(value,bytes) else str(value).encode()
    result={'kind':type(value).__name__,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
    if literal and isinstance(value,(str,int,float)) and re.fullmatch(r'[A-Za-z0-9_:.+ TZ/\-]{1,128}',str(value)):
        result['literal_clock_or_instrument_descriptor']=str(value)
    return result


def _time_inventory(reader,table,columns):
    if 't' not in columns:
        return {'table':table,'status':'unrecognized_time_column_schema','time_values_read':False}
    reader.allowed[table]={'t','ROWID','rowid','_rowid_','oid',''}
    use_rowid=not any(key.lower() in {'rowid','_rowid_','oid'} for key in columns)
    order=' ORDER BY _rowid_' if use_rowid else ''
    cursor=reader.connection.execute('SELECT "t" FROM '+identifier(table)+order)
    rows=finite=missing=nonfinite=nonnumeric=reverse=duplicates=integerlike=0
    minimum=maximum=previous=None;seen=set();spacing=Counter()
    for row in cursor:
        reader.check();rows+=1;require(rows<=MAX_ROWS,'Bounded raw clock row limit exceeded')
        value=row[0]
        if value is None:missing+=1;continue
        if not isinstance(value,(int,float)):nonnumeric+=1;continue
        if not math.isfinite(value):nonfinite+=1;continue
        finite+=1;integerlike+=int(float(value).is_integer())
        minimum=value if minimum is None else min(minimum,value)
        maximum=value if maximum is None else max(maximum,value)
        if value in seen:duplicates+=1
        seen.add(value)
        if previous is not None:
            delta=value-previous;reverse+=int(delta<0)
            require(len(spacing)<10000 or delta in spacing,'Bounded raw tick-spacing cardinality exceeded')
            spacing[delta]+=1
        previous=value
    return {'table':table,'status':'raw_time_column_structurally_read','time_values_read':True,
        'rows':rows,'finite_raw_ticks':finite,'null_time_rows':missing,'nonfinite_time_rows':nonfinite,
        'nonnumeric_time_rows':nonnumeric,'integer_valued_finite_ticks':integerlike,
        'minimum_raw_tick':minimum,'maximum_raw_tick':maximum,'duplicate_finite_ticks':duplicates,
        'reverse_adjacent_ticks':reverse,'raw_adjacent_tick_spacing':[{'delta':k,'count':v} for k,v in sorted(spacing.items())],
        'order_basis':'SQLite hidden rowid storage order; producer acquisition order unverified' if use_rowid else 'unspecified table order; shadowed rowid aliases',
        'timestamp_unit_verified':False,'expected_sampling_interval_known':False,
        'expected_missing_clock_ticks':None,'missingness_scope':'Null/invalid/duplicate timestamps only; no expected grid inferred',
        'coordinate_values_read':False,'conflicting_coordinate_duplicates_assessed':False}


def inspect_database(path,deadline,expected_rois):
    with ReadOnlyDatabase(path,deadline) as reader:
        objects=reader.rows('SELECT name,type,tbl_name,rootpage,sql FROM sqlite_master ORDER BY name',limit=MAX_TABLES)
        schema=[];tables={};unknown=[]
        for name,kind,table_name,rootpage,sql in objects:
            sql=sql or ''
            safe=bool(re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,63}',name))
            ordinary_table=kind=='table' and safe and re.match(r'^\s*CREATE\s+TABLE\b',sql,re.I) is not None
            item={'name':name,'type':kind,'table_name':table_name,'rootpage':rootpage,
                'sql_bytes':len(sql.encode()),'sql_sha256':hashlib.sha256(sql.encode()).hexdigest(),
                'provider_sql_executed':False,'sql_literal_exported':False,'ordinary_table':ordinary_table}
            if ordinary_table:
                reader.tables.add(name)
                info=reader.rows('PRAGMA table_info('+identifier(name)+')',limit=256)
                item['columns']=[{'cid':c[0],'name':c[1],'declared_type':c[2],'notnull':c[3],
                    'default_value':_metadata_value(c[4]),'primary_key_position':c[5]} for c in info]
                tables[name]={'columns':[c[1] for c in info],'without_rowid':'WITHOUT ROWID' in sql.upper()}
            elif kind!='index':unknown.append({'name':name,'type':kind,'reason':'Nonordinary/view/trigger/virtual/unsafe schema not queried'})
            schema.append(item)
        metadata={};varmap={'status':'VAR_MAP missing'};roimap={'status':'ROI_MAP missing'}
        if 'VAR_MAP' in tables:
            cols=tables['VAR_MAP']['columns'];allowed={'var_name','var_type','var_functional_type','sql_type','functional_type','name','type','unit','units'}
            selected=[c for c in cols if c in allowed]
            if selected:
                reader.allowed['VAR_MAP']=set(selected)
                values=reader.rows('SELECT '+','.join(identifier(c) for c in selected)+' FROM "VAR_MAP"')
                varmap={'status':'bounded_variable_descriptors_read','columns':selected,
                    'rows':[[str(v) if v is not None else None for v in row] for row in values],
                    'units_independently_qualified':False,'unread_unknown_columns':[c for c in cols if c not in selected]}
            else:varmap={'status':'unrecognized_VAR_MAP_columns','columns':cols}
        if 'METADATA' in tables:
            cols=tables['METADATA']['columns'];pairs=[p for p in [('field','value'),('name','value'),('key','value')] if set(p)<=set(cols)]
            if len(pairs)==1:
                key,value=pairs[0];reader.allowed['METADATA']={key,value}
                values=reader.rows('SELECT '+identifier(key)+','+identifier(value)+' FROM "METADATA"')
                metadata={'status':'bounded_metadata_keys_and_hashed_values','entries':[
                    {'key':str(k),'value':_metadata_value(v,literal=isinstance(k,str) and k.lower() in CLOCK_METADATA_KEYS)} for k,v in values],
                    'unknown_values_exported_as_literals':False,'hardware_timezone_verified':False}
            else:metadata={'status':'unrecognized_METADATA_columns','columns':cols}
        if 'ROI_MAP' in tables:
            cols=tables['ROI_MAP']['columns'];ids=[c for c in ('roi_idx','roi_id','region_id') if c in cols]
            if len(ids)==1:
                key=ids[0];reader.allowed['ROI_MAP']={key}
                raw_ids=[row[0] for row in reader.rows('SELECT '+identifier(key)+' FROM "ROI_MAP"',limit=1024)]
                valid=[v for v in raw_ids if type(v) is int and 1<=v<=1024]
                roimap={'status':'raw_ROI_identifier_roster_read','id_column':key,'roi_ids':valid,
                    'invalid_id_rows':len(raw_ids)-len(valid),'duplicate_ids':len(valid)-len(set(valid)),
                    'matches_source_metadata_roi_roster':set(valid)==set(expected_rois) and len(valid)==len(expected_rois)}
            else:roimap={'status':'unrecognized_ROI_MAP_identifier','columns':cols}
        roi_tables=sorted((name for name in tables if re.fullmatch(r'ROI_[1-9][0-9]{0,3}',name)),key=lambda name:int(name[4:]))
        clocks=[];capabilities=[]
        for table in roi_tables:
            cols=tables[table]['columns']
            if tables[table]['without_rowid']:
                clocks.append({'table':table,'status':'WITHOUT_ROWID_time_order_unqualified','time_values_read':False})
            else:clocks.append(_time_inventory(reader,table,cols))
            capabilities.append({'table':table,'roi_id':int(table[4:]),'raw_x_column_present':'x' in cols,
                'raw_y_column_present':'y' in cols,'raw_xy_columns_present':{'x','y'}<=set(cols),
                'raw_time_column_present':'t' in cols,'outcome_values_read':False,
                'two_spatial_columns_are_physical_2d_trajectory':False,'physical_units_qualified':False})
        return {'sqlite_version':sqlite3.sqlite_version,'schema':schema,'unqueried_objects':unknown,
            'VAR_MAP':varmap,'METADATA':metadata,'ROI_MAP':roimap,'raw_ROI_table_names':roi_tables,
            'missing_source_ROI_tables':sorted(set(expected_rois)-{int(t[4:]) for t in roi_tables}),
            'unexpected_ROI_tables':sorted({int(t[4:]) for t in roi_tables}-set(expected_rois)),
            'roi_time_inventories':clocks,'spatial_schema_capabilities':capabilities,
            'sqlite_authorizer_denied_operations':reader.denied,
            'outcome_values_read':False,'provider_SQL_or_code_executed':False,
            'clock_units_qualified':False,'expected_grid_missingness_qualified':False}


def _source_inputs(acquisition,acquisition_sha,paths):
    receipt=bound_json(acquisition,acquisition_sha)
    require(receipt.get('status')=='provider_verified_member_acquired' and receipt.get('final_promoted') is True
        and receipt.get('helper_sha256')==ACQUISITION_HELPER_SHA
        and receipt.get('sqlite_or_scientific_analysis_performed') is False,'Exact positive acquisition proof required')
    require(receipt.get('h1_cleared') is False and receipt.get('source_replication_passed') is False
        and receipt.get('biological_transfer_supported') is False,'Acquisition scientific flags changed')
    pins=[]
    for kind,path in paths.items():
        path=ordinary(path);h=hashes(path);require(h['sha256']==PINS[kind],'Exact official documentation/index SHA required')
        pins.append({'role':kind,'path':str(path),'sha256':h['sha256'],'bytes':h['bytes']})
    expected_acq={(str(Path(paths[k])),PINS[k]) for k in ('record','checksum','index')}
    require({(p['path'],p['sha256']) for p in receipt['source_pins']}==expected_acq,'Acquisition and current metadata paths differ')
    v=receipt['verification']
    require((v.get('raw_bytes'),v.get('compressed_bytes'),v.get('crc32'),v.get('provider_md5'))==(
        RAW_BYTES,COMPRESSED_BYTES,RAW_CRC,RAW_MD5),'Frozen raw member verification differs')
    db=ordinary(receipt['database']);require(db.name==DB_NAME and db.parent==Path(receipt['case']),'Frozen acquisition DB namespace differs')
    measured=hashes(db);require(measured['bytes']==RAW_BYTES and measured['md5']==RAW_MD5 and measured['sha256']==v['sha256'],'Raw DB bytes/provider MD5/SHA changed')
    with zipfile.ZipFile(paths['documentation']) as archive:
        entry=archive.getinfo('html_notebooks/METADATA.txt')
        require(entry.file_size<=2_000_000,'Bounded source animal metadata required')
        raw=archive.read(entry)
    require(hashlib.sha256(raw).hexdigest()==METADATA_SHA,'Original animal metadata member SHA differs')
    rows=list(csv.DictReader(io.StringIO(raw.decode()),delimiter='\t'))
    selected=[r for r in rows if r['date']==SELECTED_DATE and r['machine_name']==SELECTED_MACHINE]
    ids=[int(r['region_id']) for r in selected]
    require(len(ids)==20 and set(ids)==set(range(1,21)),'Frozen20-ROI source roster differs')
    return receipt,db,measured,pins,selected


def audit(acquisition,acquisition_sha,record,checksum,index,documentation,output,*,max_seconds=900):
    require(type(max_seconds) is int and 60<=max_seconds<=1800,'Finite Slurm audit duration required')
    native=native_context();started=time.monotonic();deadline=min(started+max_seconds,started+native['allocation_end_unix']-time.time()-180)
    paths={key:Path(value) for key,value in [('record',record),('checksum',checksum),('index',index),('documentation',documentation)]}
    source_sha=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    receipt,db,measured,pins,selected=_source_inputs(Path(acquisition),acquisition_sha,paths)
    output=ordinary(output,fresh=True)
    require((ROOT/'maintenance').resolve() in output.parents,'Fresh personal maintenance output required')
    output.mkdir()
    base={'schema':'ethoscope_read_only_structural_inventory_v1','native':native,
        'auditor_sha256':source_sha,'acquisition':{'path':str(acquisition),'sha256':acquisition_sha},
        'source_pins':pins,'database':{'path':str(db),**measured},
        'source_animal_metadata_rows':selected,'source_metadata_exposure_timestamp_present':False,
        'scientific_result':False,'analysis_admitted':False,'admission_authority':False,
        'h1_cleared':False,'source_replication_passed':False,'biological_transfer_supported':False,
        'per_animal_exposure_time_verified':False,'physical_2d_units_qualified':False,
        'activity_or_position_values_read':False,'data_repaired_or_imputed':False}
    exclusive(output/'audit-start.json',base)
    try:
        inventory=inspect_database(db,deadline,[int(r['region_id']) for r in selected])
        require(hashes(db)==measured,'DB changed during read-only schema/clock audit')
        _source_inputs(Path(acquisition),acquisition_sha,paths)
        require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==source_sha,'Auditor source changed during audit')
        require(time.monotonic()<deadline,'Finite audit finalization deadline exhausted')
        exclusive(output/'schema-and-clock-inventory.json',inventory)
        result={**base,'status':'complete_read_only_structural_inventory',
            'inventory':{'path':str(output/'schema-and-clock-inventory.json'),'sha256':hashes(output/'schema-and-clock-inventory.json')['sha256']},
            'all_raw_and_documentation_identities_reverified':True,'elapsed_seconds':time.monotonic()-started,
            'unknown_schema_does_not_clear_a_scientific_gate':True}
        exclusive(output/'receipt.json',result);return result
    except BaseException as error:
        exclusive(output/'failure.json',{**base,'status':'incomplete_structural_inventory',
            'error_type':type(error).__name__,'error':str(error),'elapsed_seconds':time.monotonic()-started})
        raise


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('acquisition','record','checksum','index','documentation','output'):parser.add_argument('--'+key,required=True,type=Path)
    parser.add_argument('--acquisition-sha256',required=True)
    parser.add_argument('--max-seconds',type=int,default=900)
    a=parser.parse_args();result=audit(a.acquisition,a.acquisition_sha256,a.record,a.checksum,a.index,a.documentation,a.output,max_seconds=a.max_seconds)
    print(json.dumps({'status':result['status'],'inventory':result['inventory'],'h1_cleared':False}))
