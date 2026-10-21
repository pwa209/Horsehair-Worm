"""Synthetic structural fixtures only; these are not Zenodo provider examples."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
import math
import time

import pytest

spec=importlib.util.spec_from_file_location('synthetic_dam_auditor',Path(__file__).with_name('audit-af-dam-secondary-v1-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def toy_code():
    labels=','.join(json.dumps(label) for label in m.FIELDS)
    return f'''# SYNTHETIC STRUCTURAL FIXTURE ONLY, not a deposited provider example.
process_experiment <- function(a,b,c,d,e,f,g) {{
 colnames(df_m1)[1:10] <- c({labels})
 colnames(df_m2)[1:10] <- c({labels})
 for(i in 1:32) {{ ignored <- i }}
 filter(Data_type %in% c("MT","CT"))
 filter(Data_type %in% c("MT","CT"))
 parse_date_time(x, orders="d b y H M S", tz="UTC")
}}
exp1_channels <- list(
 monitor1=list(SyntheticA=c(1,2),SyntheticB=c(3),Empty=c(32)),
 monitor2=list(SyntheticA=c(1),SyntheticB=c(2,3),Empty=c(32)))
exp1 <- process_experiment("../SYNTHETIC_run/Monitor1.txt", "../SYNTHETIC_run/Monitor2.txt",
 "SyntheticExp", "Synthetic fixture, not a provider experiment",
 "2025-01-01 00:00:00", "2025-01-01 00:02:00", exp1_channels)
# Virus as fixed effect, Experiment as random effect (synthetic metadata comment)
system("THIS TEXT MUST NEVER BE EXECUTED")
'''.encode()


@pytest.fixture
def parsed(monkeypatch):
    monkeypatch.setattr(m,'EXPECTED_EXPERIMENTS',1)
    result=m.parse_code(toy_code())
    return result,result['experiments'][0],result['experiments'][0]['files'][0]


def row(clock='00:00:00',kind='MT',*,monitor=1,tube=0,status=1,values=None,index=1,date='1 Jan 25'):
    values=[0]*32 if values is None else values
    return ('\t'.join(map(str,[index,date,clock,status,0,monitor,tube,kind,0,1,*values]))+'\n').encode()


def run_scan(tmp_path,parsed,raw):
    path=tmp_path/'SYNTHETIC_raw.txt';path.write_bytes(raw)
    return m.scan(path,parsed[1],parsed[2],time.monotonic()+60)


def test_literal_mapping_is_source_bound_and_keeps_empty_unmapped_unknowns(parsed):
    result,experiment,file=parsed
    assert result['code_sha256']==m.sha(toy_code()) and result['R_executed'] is False
    assert result['literal_condition_labels']==['Empty','SyntheticA','SyntheticB']
    assert 'synthetic metadata comment' in result['author_condition_description_comments'][0]['text']
    assert file['channel_conditions']['1']==['SyntheticA'] and file['channel_conditions']['32']==['Empty']
    assert file['unmapped_channels']==list(range(4,32)) and file['ambiguous_channels']=={}
    assert file['provider_name']=='SYNTHETIC_run_Monitor1.txt'
    assert experiment['window']['expected_minute_ticks']==3 and experiment['window']['inclusive'] is True
    assert experiment['window']['exposure_time_verified'] is False
    assert result['manual_bytes_bound'] is False and experiment['window']['hardware_timezone_verified'] is False
    lines=toy_code().decode().splitlines(keepends=True);source=file=experiment['mapping_source']
    assert source['first_line']<source['last_line'] and 'exp1_channels' in ''.join(lines[source['first_line']-1:source['last_line']])


@pytest.mark.parametrize('old,new,message',[
    ('SyntheticA=c(1,2)','SyntheticA=system("bad")','Executable'),
    ('SyntheticA=c(1,2)','SyntheticA=c(1,1)','unique channel'),
    ('SyntheticA=c(1,2)','SyntheticA=c(0,33)','unique channel'),
    ('monitor1=list(','monitor1=list(SyntheticA=c(5),','Duplicate'),
    ('exp1_channels <- list(','exp1_channels <- list()\nexp1_channels <- list(','Repeated'),
    ('"MT","CT"','"MT","Rt"','MT/CT'),
    ('"Monitor_status"','"Changed_status"','column labels'),
    ('"2025-01-01 00:02:00"','"2024-12-31 23:59:00"','window'),
])
def test_code_metadata_drift_is_refused(monkeypatch,old,new,message):
    monkeypatch.setattr(m,'EXPECTED_EXPERIMENTS',1)
    with pytest.raises(ValueError,match=message):m.parse_code(toy_code().replace(old.encode(),new.encode()))


def test_ambiguous_channel_mapping_is_reported_not_first_match_chosen(monkeypatch):
    monkeypatch.setattr(m,'EXPECTED_EXPERIMENTS',1)
    value=m.parse_code(toy_code().replace(b'SyntheticB=c(3)',b'SyntheticB=c(2,3)'))
    file=value['experiments'][0]['files'][0]
    assert file['ambiguous_channels']=={'2':['SyntheticA','SyntheticB']}
    assert file['channel_conditions']['2']==['SyntheticA','SyntheticB']


@pytest.mark.parametrize('date,clock,expected',[
    ('11 Apr 25','18:50:00','2025-04-11T18:50:00+00:00'),
    ('11Apr25','18:50:00','2025-04-11T18:50:00+00:00'),
    ('1 Jan 1969','00:00:00','1969-01-01T00:00:00+00:00'),
])
def test_source_clock_parser_reports_assigned_utc(date,clock,expected):
    assert m.parse_time(date,clock)==int(datetime.fromisoformat(expected).timestamp())


@pytest.mark.parametrize('date,clock',[('31 Feb25','00:00:00'),('1 Xxx25','00:00:00'),('1 Jan25','25:00:00')])
def test_invalid_source_clocks_are_not_coerced(date,clock):
    with pytest.raises(ValueError):m.parse_time(date,clock)


def test_duplicate_identity_includes_monitor_tube_and_type(tmp_path,parsed):
    result=run_scan(tmp_path,parsed,row()+row(kind='CT')+row(kind='Pn')+row(kind='MT',tube=1))
    assert len(result['streams'])==4 and all(x['clock']['duplicate_clocks']==0 for x in result['streams'])
    assert sum(x['selected_MT_CT_monitor_total'] for x in result['streams'])==2
    assert result['missing_selected_monitor_total_types']==[]
    assert all('channel_value_quality_in_author_window' not in x for x in result['streams'] if not x['selected_MT_CT_monitor_total'])


def test_duplicate_conflict_out_of_order_and_offgrid_are_distinct(tmp_path,parsed):
    raw=row('00:01:00')+row('00:00:00')+row('00:00:00')+row('00:00:00',values=[1]*32)+row('00:01:30')
    result=run_scan(tmp_path,parsed,raw);clock=result['streams'][0]['clock']
    assert clock['rows']==5 and clock['duplicate_clocks']==2 and clock['conflicting_duplicate_clocks']==1
    assert clock['out_of_order']==1 and clock['off_grid_in_window']==1
    assert clock['window_unique_grid_ticks']==2 and clock['window_missing_record_ticks']==1
    assert result['streams'][0]['channel_value_quality_in_author_window'][0]['unique_valid_status_one_grid_ticks']==2
    assert result['missing_selected_monitor_total_types']==['CT']
    assert (result['bytes'],result['sha256'],result['md5'])==(len(raw),m.sha(raw),hashlib.md5(raw).hexdigest())


def test_bad_status_zeros_missing_finite_and_shape_are_not_clean_activity(tmp_path,parsed):
    values=['NA','Inf','not-a-number',-1,1.5]+[0]*27
    raw=row(status=51)+row('00:01:00',values=values)+row('00:02:00',values=[0]*33)
    result=run_scan(tmp_path,parsed,raw);stream=result['streams'][0];quality=stream['channel_value_quality_in_author_window']
    assert stream['clock']['status_not_one_in_window']==1
    assert quality[0]['missing']==1 and quality[1]['nonfinite']==1 and quality[2]['nonnumeric']==1
    assert quality[3]['negative']==1 and quality[4]['noninteger']==1
    assert all(x['unique_valid_status_one_grid_ticks']==0 for x in quality[:5])
    assert quality[5]['unique_valid_status_one_grid_ticks']==1
    assert result['column_count_histogram']=={42:2,43:1}
    assert result['records_repaired_or_imputed'] is result['effect_statistics_computed'] is False


def test_invalid_metadata_date_and_foreign_monitor_are_preserved(tmp_path,parsed):
    raw=row(date='bad-date')+row(monitor=2,index='bad-index')+b'too\tfew\tcolumns\n'
    result=run_scan(tmp_path,parsed,raw)
    assert result['invalid_datetime_rows']==1 and result['invalid_metadata_rows']==1
    assert result['invalid_integral_metadata_fields']=={'Index':1}
    assert result['raw_monitor_numbers_differing_from_author_label']==['2']


def fixture_source(tmp_path,monkeypatch):
    root=tmp_path/'SYNTHETIC_personal_root';root.mkdir();(root/'maintenance').mkdir()
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'EXPECTED_EXPERIMENTS',1);monkeypatch.setattr(m,'EXPECTED_FILES',2)
    monkeypatch.setattr(m,'RECORD_ID','SYNTHETIC_RECORD_NO_PROVIDER_CLAIM');monkeypatch.setattr(m,'CODE_NAME','synthetic-code.R')
    record_path=root/'SYNTHETIC_record.json';source=root/'sources';source.mkdir()
    contents={'synthetic-code.R':toy_code(),'SYNTHETIC_run_Monitor1.txt':row()+row('00:01:00')+row('00:02:00'),
              'SYNTHETIC_run_Monitor2.txt':row(monitor=2)+row('00:01:00',monitor=2)+row('00:02:00',monitor=2)}
    providers=[{'key':name,'size':len(raw),'checksum':'md5:'+hashlib.md5(raw).hexdigest(),
                'links':{'self':'https://example.invalid/SYNTHETIC-NOT-PROVIDER/'+name}} for name,raw in contents.items()]
    record={'id':m.RECORD_ID,'files':providers,'synthetic_test_fixture':True};record_raw=json.dumps(record).encode();record_path.write_bytes(record_raw)
    items=[]
    for provider in providers:
        name=provider['key'];raw=contents[name];path=source/name;path.write_bytes(raw)
        proof={'status':'provider_verified','provider_md5_verified':True,'path':str(path),'bytes':len(raw),'sha256':m.sha(raw)}
        if name==m.CODE_NAME:
            proof.update(record_id=m.RECORD_ID,name=name)
            receipt={'status':'small_public_file_management_transfer','files':[proof],
                     'effects_calculated':False,'provider_code_executed':False,'scientific_gates_cleared':False}
        else:
            receipt={**proof,'metadata_sha256':m.sha(record_raw),'expected_provider_md5':provider['checksum'],
                'source_url':provider['links']['self'],'effects_inspected':False,'provider_code_executed':False,'scientific_gates_cleared':False}
        receipt_path=root/(name+'.transfer.json');receipt_raw=json.dumps(receipt).encode();receipt_path.write_bytes(receipt_raw)
        items.append({'name':name,'path':str(path),'bytes':len(raw),'provider_md5':hashlib.md5(raw).hexdigest(),'sha256':m.sha(raw),
                      'transfer_receipt_path':str(receipt_path),'transfer_receipt_sha256':m.sha(receipt_raw)})
    manifest={'schema_version':1,'record':{'path':str(record_path),'sha256':m.sha(record_raw)},'code':items[0],'files':items[1:],
              'effects_inspected':False,'provider_code_executed':False,'scientific_gates_cleared':False}
    path=root/'manifest.json';path.write_text(json.dumps(manifest));return root,path,manifest


def test_complete_synthetic_audit_independently_rehashes_all_inputs_and_has_no_inference(tmp_path,monkeypatch):
    root,path,manifest=fixture_source(tmp_path,monkeypatch)
    before={str(p):p.read_bytes() for p in root.rglob('*') if p.is_file()}
    output=root/'maintenance/audit';result=m.audit(path,m.sha(path.read_bytes()),output,max_seconds=60)
    assert result['status']=='complete_read_only_structural_audit' and result['files_audited']==2
    assert result['all_provider_identities_reverified'] is True and len(result['structural_file_receipts'])==2
    assert all(value is False for key,value in result.items() if key in {'scientific_result','scientific_gates_cleared','effects_inspected',
        'effects_calculated','provider_code_executed','admission_authority','complete_declared_study','analysis_admitted','original_H1_H2_H3_gates_cleared',
        'hardware_timezone_verified','infection_or_exposure_timestamp_verified','XY_available','beam_count_to_distance_conversion_qualified'})
    for binding in result['structural_file_receipts']:assert m.sha(Path(binding['path']).read_bytes())==binding['sha256']
    for name,raw in before.items():assert Path(name).read_bytes()==raw
    with pytest.raises(ValueError,match='Fresh'):m.audit(path,m.sha(path.read_bytes()),output,max_seconds=60)


@pytest.mark.parametrize('change',['file_bytes','manifest_hash','record_hash','provider_md5','receipt_status','receipt_source_url','receipt_flag','receipt_record','receipt_path'])
def test_manifest_provider_or_transfer_tampering_never_yields_complete_audit(tmp_path,monkeypatch,change):
    root,path,manifest=fixture_source(tmp_path,monkeypatch);item=manifest['files'][0];given_sha=None
    if change=='file_bytes':Path(item['path']).write_bytes(b'X'*item['bytes'])
    elif change=='manifest_hash':given_sha='0'*64
    elif change=='record_hash':manifest['record']['sha256']='0'*64
    elif change=='provider_md5':item['provider_md5']='0'*32
    else:
        receipt_path=Path(item['transfer_receipt_path']);receipt=json.loads(receipt_path.read_text())
        if change=='receipt_status':receipt['status']='partial'
        elif change=='receipt_source_url':receipt['source_url']='https://example.invalid/other'
        elif change=='receipt_flag':receipt['provider_code_executed']=True
        elif change=='receipt_record':receipt['metadata_sha256']='0'*64
        else:receipt['path']=str(root/'foreign-file.txt')
        receipt_path.write_text(json.dumps(receipt));item['transfer_receipt_sha256']=m.sha(receipt_path.read_bytes())
    path.write_text(json.dumps(manifest));output=root/'maintenance/audit'
    with pytest.raises(ValueError):m.audit(path,given_sha or m.sha(path.read_bytes()),output,max_seconds=60)
    assert not (output/'receipt.json').exists()


def test_output_outside_personal_maintenance_is_refused(tmp_path,monkeypatch):
    root,path,manifest=fixture_source(tmp_path,monkeypatch)
    with pytest.raises(ValueError,match='maintenance'):m.audit(path,m.sha(path.read_bytes()),root/'outside-maintenance',max_seconds=60)


@pytest.mark.parametrize('duration',[0,59,5401,float('nan'),True])
def test_finite_deadline_is_required(duration):
    with pytest.raises(ValueError,match='Finite'):m.audit('unused','unused','unused',max_seconds=duration)


def test_mid_read_failure_preserves_partial_evidence_and_raises(tmp_path,monkeypatch):
    root,path,manifest=fixture_source(tmp_path,monkeypatch);original=m.scan;calls=[]
    def scan(*args):
        calls.append(1)
        if len(calls)==2:raise TimeoutError('Synthetic bounded stop')
        return original(*args)
    monkeypatch.setattr(m,'scan',scan);output=root/'maintenance/audit'
    with pytest.raises(TimeoutError):m.audit(path,m.sha(path.read_bytes()),output,max_seconds=60)
    failure=json.loads((output/'failure.json').read_text())
    assert failure['finished_files']==1 and failure['scientific_result'] is False and (output/'file-1.json').exists()
    assert not (output/'receipt.json').exists()
