import copy
from datetime import datetime, time
import importlib.util
import io
import json
from pathlib import Path
import pytest
from openpyxl import Workbook

spec=importlib.util.spec_from_file_location('audit_metadata',Path(__file__).resolve().parents[1]/'scripts/audit_dataverse_metadata.py')
m=importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

def original():
    w=Workbook();s=w.active
    s.append(['ROI','Rx','ExposureStart','Clock units','Geno','Outcome'])
    s.append([1,None,datetime(2020,1,1,8), 'hours', 'line A','survivor'])
    s.append([2,'Uninfected',None,None,'control','=1+1'])
    b=io.BytesIO();w.save(b)
    return b.getvalue()

def item(data=None):
    data=original() if data is None else data
    return dict(file_id='6802609',name='original.xlsx',size_in_bytes=len(data),checksum={'type':'MD5','value':m.digest(data,'md5')},url='https://dataverse.harvard.edu/api/access/datafile/6802609')

def metadata():
    return dict(label='original.xlsx',directoryLabel='source-exp',restricted=False,id=111)

def test_parser_retains_unknown_rx_outcome_formula_and_typed_clock():
    s=m.inspect_workbook(original())[0]
    assert s['rows'][1][1] is None
    assert s['rows'][1][5]=='survivor'
    assert s['rows'][2][5]=='=1+1'
    assert s['rows'][1][2]=={'type':'datetime','value':'2020-01-01T08:00:00'}
    assert {x['literal'] for x in s['explicit_header_candidates']} >= {'Rx','ExposureStart','ROI'}
    assert not any(k in s for k in ['exposure_status','animal_id','gene_id','family'])

def test_native_date_time_values():
    assert m.native_value(time(8,1))=={'type':'time','value':'08:01:00'}

def test_verified_checkpoint_does_not_redownload(tmp_path,monkeypatch):
    (tmp_path/'downloads').mkdir();data=original();i=item(data)
    responses=iter([data,json.dumps(metadata()).encode()])
    monkeypatch.setattr(m,'request',lambda *a,**k: next(responses))
    r=m.one(i,tmp_path)
    assert r['status']=='verified_original_metadata'
    assert r['directory_label']=='source-exp'
    assert r['file_id']=='6802609' # metadata id111 must never replace datafile ID
    monkeypatch.setattr(m,'request',lambda *a,**k: (_ for _ in ()).throw(AssertionError('redownload')))
    assert m.one(i,tmp_path)['reused_verified_checkpoint'] is True

def test_changed_checkpoint_fails_and_preserves_existing(tmp_path,monkeypatch):
    (tmp_path/'downloads').mkdir();data=original();i=item(data)
    responses=iter([data,json.dumps(metadata()).encode()])
    monkeypatch.setattr(m,'request',lambda *a,**k: next(responses))
    r=m.one(i,tmp_path);p=Path(r['verified_artifact_directory'])/'original.xlsx'
    p.write_bytes(b'changed')
    assert m.one(i,tmp_path)['status']=='changed_verified_checkpoint'
    assert p.read_bytes()==b'changed'

def test_bad_provider_candidate_preserved_then_success(tmp_path,monkeypatch):
    (tmp_path/'downloads').mkdir();data=original();i=item(data)
    responses=iter([b'ERROR XML',data,json.dumps(metadata()).encode()])
    monkeypatch.setattr(m,'request',lambda *a,**k: next(responses));monkeypatch.setattr(m.timer,'sleep',lambda _:None)
    r=m.one(i,tmp_path)
    assert r['status']=='verified_original_metadata' and len(r['attempts'])==1
    assert (tmp_path/'downloads'/'6802609'/'uninitialized-attempt-1'/'original-candidate.xlsx').read_bytes()==b'ERROR XML'

def test_bulk_version_map_binds_ids_bytes_checksums_labels_directory():
    i=item();f=dict(metadata(),dataFile={'id':int(i['file_id']),'filesize':i['size_in_bytes'],'checksum':i['checksum']})
    obj={'status':'OK','data':{'versionNumber':1,'versionMinorNumber':0,'files':[f]}}
    assert json.loads(m.version_directory_map(json.dumps(obj).encode(),[i],expected_files=1)[i['file_id']])['directoryLabel']=='source-exp'
    for key,new in [('label','different.xlsx'),('restricted',True),('directoryLabel','')]:
        bad=copy.deepcopy(obj);bad['data']['files'][0][key]=new
        with pytest.raises(ValueError):m.version_directory_map(json.dumps(bad).encode(),[i],expected_files=1)
    for key,new in [('filesize',1),('checksum',{'type':'MD5','value':'0'*32})]:
        bad=copy.deepcopy(obj);bad['data']['files'][0]['dataFile'][key]=new
        with pytest.raises(ValueError):m.version_directory_map(json.dumps(bad).encode(),[i],expected_files=1)

def test_incomplete_or_duplicate_version_roster_rejected():
    i=item();f=dict(metadata(),dataFile={'id':int(i['file_id']),'filesize':i['size_in_bytes'],'checksum':i['checksum']})
    obj={'status':'OK','data':{'versionNumber':1,'versionMinorNumber':0,'files':[f,f]}}
    with pytest.raises(ValueError):m.version_directory_map(json.dumps(obj).encode(),[i],expected_files=1)
    with pytest.raises(ValueError):m.version_directory_map(json.dumps(obj).encode(),[i],expected_files=2)

def test_deadline_prevents_http(monkeypatch):
    monkeypatch.setattr(m,'END_TIME',0)
    monkeypatch.setattr(m.urllib.request,'urlopen',lambda *a,**k: (_ for _ in ()).throw(AssertionError('network')))
    with pytest.raises(TimeoutError):m.request('https://dataverse.harvard.edu/api/files/1/metadata')
