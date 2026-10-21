"""Synthetic SQLite fixtures only; never downloaded provider databases."""
import csv
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import time
import zipfile
import zlib

import pytest

SOURCE=Path(__file__).with_name('audit-ethoscope-sqlite-schema-20261021.py')
spec=importlib.util.spec_from_file_location('ethoscope_sqlite_auditor_private',SOURCE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def make_database(path,*,unknown=False):
    with sqlite3.connect(path) as c:
        c.execute('CREATE TABLE VAR_MAP(var_name TEXT, sql_type TEXT, functional_type TEXT)')
        c.executemany('INSERT INTO VAR_MAP VALUES(?,?,?)',[
            ('t','INTEGER','time'),('x','INTEGER','position'),('y','INTEGER','position'),
            ('xy_dist_log10x1000','INTEGER','distance')])
        c.execute('CREATE TABLE METADATA(field TEXT,value TEXT)')
        c.executemany('INSERT INTO METADATA VALUES(?,?)',[
            ('date_time','1650626472'),('timezone','UTC'),('time_unit','ms'),
            ('unclassified_value','SYNTHETIC_SECRET_METADATA_NOT_EXPORTED')])
        c.execute('CREATE TABLE ROI_MAP(roi_idx INTEGER,roi_x INTEGER,roi_y INTEGER)')
        c.executemany('INSERT INTO ROI_MAP VALUES(?,?,?)',[(1,1,2),(2,3,4)])
        c.execute("CREATE TABLE ROI_1(t REAL,x TEXT,y TEXT,velocity REAL DEFAULT 'SYNTHETIC_DEFAULT_SECRET')")
        c.executemany('INSERT INTO ROI_1(t,x,y) VALUES(?,?,?)',[
            (0,'SYNTHETIC_X_SECRET','SYNTHETIC_Y_SECRET'),(10,'secret','secret'),
            (5,'secret','secret'),(10,'secret','secret'),(None,'secret','secret'),
            ('bad-clock','secret','secret'),(float('inf'),'secret','secret')])
        c.execute('CREATE TABLE ROI_2(t INTEGER,beam_count INTEGER)')
        c.executemany('INSERT INTO ROI_2 VALUES(?,?)',[(0,91),(10,2000),(20,700)])
        if unknown:
            c.execute('CREATE VIEW ROI_3 AS SELECT load_extension(x) AS t FROM ROI_1')
            c.execute('CREATE TABLE ROI_4(t INTEGER PRIMARY KEY,x INTEGER,y INTEGER) WITHOUT ROWID')
            c.execute('CREATE TABLE ROI_5(clock INTEGER,x INTEGER,y INTEGER)')


def inspect(path,ids=(1,2)):
    return m.inspect_database(path,time.monotonic()+60,ids)


def test_schema_and_raw_clock_counts_are_exact_without_outcome_values(tmp_path):
    p=tmp_path/'synthetic.db';make_database(p)
    before=p.read_bytes();result=inspect(p)
    assert p.read_bytes()==before
    assert result['ROI_MAP']['roi_ids']==[1,2]
    assert result['ROI_MAP']['matches_source_metadata_roi_roster'] is True
    assert result['missing_source_ROI_tables']==[]
    first=result['roi_time_inventories'][0]
    assert first['rows']==7 and first['finite_raw_ticks']==4 and first['null_time_rows']==1
    assert first['nonnumeric_time_rows']==1 and first['nonfinite_time_rows']==1
    assert (first['minimum_raw_tick'],first['maximum_raw_tick'])==(0,10)
    assert first['duplicate_finite_ticks']==1 and first['reverse_adjacent_ticks']==1
    assert first['raw_adjacent_tick_spacing']==[{'delta':-5.,'count':1},{'delta':5.,'count':1},{'delta':10.,'count':1}]
    assert first['expected_missing_clock_ticks'] is None and first['timestamp_unit_verified'] is False
    capabilities=result['spatial_schema_capabilities']
    assert capabilities[0]['raw_xy_columns_present'] is True
    assert capabilities[1]['raw_xy_columns_present'] is False
    assert all(c['physical_units_qualified'] is False for c in capabilities)
    text=json.dumps(result,allow_nan=False)
    for secret in ['SYNTHETIC_X_SECRET','SYNTHETIC_Y_SECRET','SYNTHETIC_SECRET_METADATA_NOT_EXPORTED','SYNTHETIC_DEFAULT_SECRET']:
        assert secret not in text
    assert result['outcome_values_read'] is False
    assert result['clock_units_qualified'] is result['expected_grid_missingness_qualified'] is False


@pytest.mark.parametrize('sql',[
    'INSERT INTO ROI_1(t) VALUES(3)','UPDATE ROI_1 SET t=2','DELETE FROM ROI_1',
    'DROP TABLE ROI_1','CREATE TABLE extra(x)','ATTACH DATABASE ":memory:" AS other',
    'PRAGMA journal_mode=WAL','PRAGMA query_only=OFF','SELECT load_extension("bad")',
    'SELECT x FROM ROI_1','SELECT y FROM ROI_1','SELECT velocity FROM ROI_1',
    'SELECT max(x) FROM ROI_1',
])
def test_authorizer_refuses_writes_attachments_functions_and_outcome_reads(tmp_path,sql):
    p=tmp_path/'synthetic.db';make_database(p);before=p.read_bytes()
    with m.ReadOnlyDatabase(p,time.monotonic()+60) as reader:
        reader.allowed['ROI_1']={'t','rowid','ROWID','_rowid_',''}
        with pytest.raises(sqlite3.DatabaseError):reader.connection.execute(sql).fetchall()
        assert reader.denied
    assert p.read_bytes()==before


def test_views_without_rowid_and_unknown_time_schema_are_inventory_only(tmp_path):
    p=tmp_path/'synthetic.db';make_database(p,unknown=True)
    result=inspect(p,ids=(1,2,3,4,5))
    assert {'name':'ROI_3','type':'view','reason':'Nonordinary/view/trigger/virtual/unsafe schema not queried'} in result['unqueried_objects']
    assert result['missing_source_ROI_tables']==[3]
    by={r['table']:r for r in result['roi_time_inventories']}
    assert by['ROI_4']['status']=='WITHOUT_ROWID_time_order_unqualified'
    assert by['ROI_5']['status']=='unrecognized_time_column_schema'
    assert by['ROI_4']['time_values_read'] is by['ROI_5']['time_values_read'] is False


def test_missing_unknown_metadata_and_duplicate_roi_ids_preserved(tmp_path):
    p=tmp_path/'synthetic.db'
    with sqlite3.connect(p) as c:
        c.execute('CREATE TABLE VAR_MAP(unfamiliar TEXT)')
        c.execute('CREATE TABLE METADATA(unfamiliar TEXT)')
        c.execute('CREATE TABLE ROI_MAP(roi_idx INTEGER)')
        c.executemany('INSERT INTO ROI_MAP VALUES(?)',[(1,),(1,),(None,)])
        c.execute('CREATE TABLE ROI_9(t INTEGER,x INTEGER,y INTEGER)')
    result=inspect(p)
    assert result['VAR_MAP']['status']=='unrecognized_VAR_MAP_columns'
    assert result['METADATA']['status']=='unrecognized_METADATA_columns'
    assert result['ROI_MAP']['duplicate_ids']==1 and result['ROI_MAP']['invalid_id_rows']==1
    assert result['ROI_MAP']['matches_source_metadata_roi_roster'] is False
    assert result['missing_source_ROI_tables']==[1,2] and result['unexpected_ROI_tables']==[9]


def test_expired_deadline_interrupts_sql_and_finite_loop(tmp_path):
    p=tmp_path/'synthetic.db';make_database(p)
    with m.ReadOnlyDatabase(p,time.monotonic()-1) as reader:
        with pytest.raises(ValueError,match='deadline'):reader.rows('SELECT name FROM sqlite_master')
        assert reader._progress()==1


def test_sqlite_progress_handler_interrupts_large_builtin_query(tmp_path):
    p=tmp_path/'synthetic.db';make_database(p)
    with sqlite3.connect(p) as c:c.executemany('INSERT INTO ROI_2(t) VALUES(?)',[(i,) for i in range(2000)])
    with m.ReadOnlyDatabase(p,time.monotonic()+60) as reader:
        reader.allowed['ROI_2']={'t'}
        reader.deadline=time.monotonic()-1
        with pytest.raises(sqlite3.OperationalError,match='interrupted'):
            reader.connection.execute('SELECT t FROM ROI_2 WHERE t>=0').fetchall()


@pytest.mark.parametrize('bad',['a"; DROP TABLE x;--','../x','a.b','a b','', 'x'*65])
def test_untrusted_sql_identifier_never_interpolated(bad):
    with pytest.raises(ValueError):m.identifier(bad)


def source_fixture(tmp_path,monkeypatch):
    root=tmp_path/'SYNTHETIC_personal';(root/'maintenance').mkdir(parents=True)
    source=root/'maintenance'/'SYNTHETIC_acquisition';source.mkdir()
    db=source/'synthetic.db';make_database(db)
    measured=m.hashes(db)
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'DB_NAME',db.name)
    monkeypatch.setattr(m,'RAW_BYTES',measured['bytes']);monkeypatch.setattr(m,'RAW_MD5',measured['md5'])
    monkeypatch.setattr(m,'RAW_CRC',f'{zlib.crc32(db.read_bytes()):08x}')
    docs=root/'metadata';docs.mkdir();paths={}
    for key in ('record','checksum','index'):
        path=docs/(key+'.json');path.write_text(json.dumps({'synthetic_metadata_only':key}));paths[key]=path
    text=io.StringIO();writer=csv.DictWriter(text,fieldnames=['date','machine_name','region_id','genotype','treatment'],delimiter='\t');writer.writeheader()
    for i in range(1,21):writer.writerow({'date':m.SELECTED_DATE,'machine_name':m.SELECTED_MACHINE,'region_id':str(i),'genotype':'SYNTHETIC','treatment':'SYNTHETIC'})
    metadata=text.getvalue().encode();monkeypatch.setattr(m,'METADATA_SHA',hashlib.sha256(metadata).hexdigest())
    doc=docs/'synthetic-documentation.zip'
    with zipfile.ZipFile(doc,'w') as z:z.writestr('html_notebooks/METADATA.txt',metadata)
    paths['documentation']=doc
    monkeypatch.setattr(m,'PINS',{key:m.hashes(path)['sha256'] for key,path in paths.items()})
    receipt={'status':'provider_verified_member_acquired','final_promoted':True,
        'helper_sha256':m.ACQUISITION_HELPER_SHA,'sqlite_or_scientific_analysis_performed':False,
        'h1_cleared':False,'source_replication_passed':False,'biological_transfer_supported':False,
        'source_pins':[{'path':str(paths[k]),'sha256':m.PINS[k]} for k in ('record','checksum','index')],
        'verification':{'raw_bytes':m.RAW_BYTES,'compressed_bytes':m.COMPRESSED_BYTES,'crc32':m.RAW_CRC,'provider_md5':m.RAW_MD5,'sha256':measured['sha256']},
        'database':str(db),'case':str(source)}
    rp=source/'acquisition-result.json';rp.write_text(json.dumps(receipt))
    monkeypatch.setattr(m,'native_context',lambda:{'job_id':'999999','cpus':1,'gpus':0,'allocation_end_unix':time.time()+3600})
    return {'root':root,'acquisition':rp,'acquisition_sha':m.hashes(rp)['sha256'],**paths,
            'output':root/'maintenance'/'fresh-audit','db':db,'receipt_value':receipt}


def run_audit(fixture):
    return m.audit(**{key:fixture[key] for key in ['acquisition','acquisition_sha','record','checksum','index','documentation','output']},max_seconds=60)


def test_complete_source_bound_audit_stays_structural_and_preserves_all_inputs(tmp_path,monkeypatch):
    fixture=source_fixture(tmp_path,monkeypatch)
    before={p:p.read_bytes() for p in fixture['root'].rglob('*') if p.is_file()}
    result=run_audit(fixture)
    assert result['status']=='complete_read_only_structural_inventory'
    assert len(result['source_animal_metadata_rows'])==20
    assert result['all_raw_and_documentation_identities_reverified'] is True
    for key in ['scientific_result','analysis_admitted','admission_authority','h1_cleared',
                'source_replication_passed','biological_transfer_supported','per_animal_exposure_time_verified',
                'physical_2d_units_qualified','activity_or_position_values_read','data_repaired_or_imputed']:
        assert result[key] is False
    assert all(p.read_bytes()==raw for p,raw in before.items())
    assert m.hashes(Path(result['inventory']['path']))['sha256']==result['inventory']['sha256']
    with pytest.raises(ValueError,match='Fresh'):run_audit(fixture)


@pytest.mark.parametrize('change',['acquisition_sha','status','publication','helper','raw_sha','raw_md5','raw_bytes','raw_crc','database_bytes','record','checksum','index','documentation','paths','clearance'])
def test_source_or_receipt_corruption_cannot_produce_success(tmp_path,monkeypatch,change):
    f=source_fixture(tmp_path,monkeypatch);v=f['receipt_value']
    if change=='acquisition_sha':f['acquisition_sha']='0'*64
    elif change in ('record','checksum','index','documentation'):f[change].write_bytes(f[change].read_bytes()+b'X')
    elif change=='database_bytes':f['db'].write_bytes(f['db'].read_bytes()+b'X')
    else:
        if change=='status':v['status']='partial'
        elif change=='publication':v['final_promoted']=False
        elif change=='helper':v['helper_sha256']='0'*64
        elif change=='raw_sha':v['verification']['sha256']='0'*64
        elif change=='raw_md5':v['verification']['provider_md5']='0'*32
        elif change=='raw_bytes':v['verification']['raw_bytes']+=1
        elif change=='raw_crc':v['verification']['crc32']='00000000'
        elif change=='paths':v['source_pins'][0]['path']=str(f['root']/'another.json')
        elif change=='clearance':v['h1_cleared']=True
        f['acquisition'].write_text(json.dumps(v));f['acquisition_sha']=m.hashes(f['acquisition'])['sha256']
    with pytest.raises(ValueError):run_audit(f)
    assert not (f['output']/'receipt.json').exists()


def test_mid_audit_error_keeps_start_and_failure_no_complete_receipt(tmp_path,monkeypatch):
    f=source_fixture(tmp_path,monkeypatch)
    def stop(*args):raise TimeoutError('SYNTHETIC deadline')
    monkeypatch.setattr(m,'inspect_database',stop)
    with pytest.raises(TimeoutError):run_audit(f)
    assert (f['output']/'audit-start.json').exists() and (f['output']/'failure.json').exists()
    assert not (f['output']/'receipt.json').exists()
    assert f['db'].is_file()


def test_source_changed_after_inventory_refused(tmp_path,monkeypatch):
    f=source_fixture(tmp_path,monkeypatch);old=m.inspect_database
    def change(*args):
        value=old(*args);f['db'].write_bytes(f['db'].read_bytes()+b'X');return value
    monkeypatch.setattr(m,'inspect_database',change)
    with pytest.raises(ValueError,match='changed'):run_audit(f)
    assert (f['output']/'failure.json').exists() and not (f['output']/'receipt.json').exists()


@pytest.mark.parametrize('gpu',['0','1','0,1','gpu:1'])
def test_native_cpu_contract_refuses_device_zero_and_all_gpu_assignments(monkeypatch,gpu):
    monkeypatch.setattr(m.getpass,'getuser',lambda:'pwa209')
    for key in ('SLURM_JOB_GPUS','SLURM_STEP_GPUS','CUDA_VISIBLE_DEVICES'):monkeypatch.delenv(key,raising=False)
    monkeypatch.setenv('SLURM_JOB_ID','12345');monkeypatch.setenv('SLURM_CPUS_PER_TASK','1')
    monkeypatch.setenv('SLURM_JOB_END_TIME',str(time.time()+3600));monkeypatch.setenv('CUDA_VISIBLE_DEVICES',gpu)
    with pytest.raises(ValueError,match='GPU'):m.native_context()


def test_valid_native_contract_records_only_cpu_and_no_admission(monkeypatch):
    monkeypatch.setattr(m.getpass,'getuser',lambda:'pwa209')
    for key in ('SLURM_JOB_GPUS','SLURM_STEP_GPUS','CUDA_VISIBLE_DEVICES'):monkeypatch.delenv(key,raising=False)
    monkeypatch.setenv('SLURM_JOB_ID','12345');monkeypatch.setenv('SLURM_CPUS_PER_TASK','1')
    monkeypatch.setenv('SLURM_JOB_END_TIME',str(time.time()+3600))
    assert m.native_context()['cpus']==1 and m.native_context()['gpus']==0


@pytest.mark.parametrize('duration',[0,59,1801,True,float('nan')])
def test_invalid_deadlines_refused_before_any_source_read(duration):
    with pytest.raises(ValueError,match='duration'):m.audit(*(['unused']*7),max_seconds=duration)
