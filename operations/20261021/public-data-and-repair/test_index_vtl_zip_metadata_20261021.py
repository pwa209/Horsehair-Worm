"""Synthetic ZIP metadata/HTTP only; no publisher track data or network calls."""
import copy
import hashlib
import http.client
from http.client import IncompleteRead
import importlib.util
import io
import json
from pathlib import Path
import struct
import zipfile

import pytest

spec=importlib.util.spec_from_file_location('vtl_zip_metadata',Path(__file__).with_name('index-vtl-zip-metadata-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class Response:
    def __init__(self,raw,headers,*,status=206,url='',broken=False):
        self.raw=io.BytesIO(raw);self.headers=headers;self.status=status;self.url=url;self.closed=False;self.reads=0;self.broken=broken
    def geturl(self):return self.url
    def close(self):self.closed=True
    def read(self,size):
        self.reads+=1
        if self.broken:
            self.broken=False;raise http.client.IncompleteRead(self.raw.read(min(size,5)),size)
        return self.raw.read(size)


class HTTP:
    def __init__(self,raw,*,change=None):self.raw,self.change,self.calls,self.responses=raw,change,[],[]
    def __call__(self,method,url,headers,timeout):
        self.calls.append((method,url,copy.deepcopy(headers),timeout))
        common={'Content-Length':str(len(self.raw)),'Accept-Ranges':'bytes','ETag':'"fixture"','Content-Type':'application/zip'}
        if method=='HEAD':
            response=Response(b'',common,status=200,url=url)
            if self.change=='head_range':common.pop('Accept-Ranges')
            elif self.change=='head_encoding':common['Content-Encoding']='gzip'
            elif self.change=='head_length':common['Content-Length']='-1'
        else:
            start,end=map(int,headers['Range'][6:].split('-'));body=self.raw[start:end+1]
            common.update({'Content-Length':str(end-start+1),'Content-Range':f'bytes {start}-{end}/{len(self.raw)}'})
            response=Response(body,common,url=url)
            if self.change=='status200':response.status=200;response.raw=io.BytesIO(b'WHOLE SCIENTIFIC PAYLOAD MUST NEVER BE READ')
            elif self.change=='wrong_range':common['Content-Range']=f'bytes {start+1}-{end}/{len(self.raw)}'
            elif self.change=='wrong_total':common['Content-Range']=f'bytes {start}-{end}/{len(self.raw)+1}'
            elif self.change=='wrong_length':common['Content-Length']=str(end-start+2)
            elif self.change=='encoding':common['Content-Encoding']='gzip'
            elif self.change=='etag':common['ETag']='"changed"'
            elif self.change=='redirect':response.url=url+'?different'
            elif self.change=='truncated':response.raw=io.BytesIO(body[:-1])
            elif self.change=='oversized':response.raw=io.BytesIO(body+b'x')
            elif self.change=='partial':response.broken=True
        self.responses.append(response);return response


def classic(*,name='dataset1/2019-11-10_1_10.vtl'):
    raw=io.BytesIO()
    with zipfile.ZipFile(raw,'w',compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(name,b'SYNTHETIC TRACK VALUES NOT TO BE REQUESTED'*4)
        z.writestr('dataset1/',b'')
    return raw.getvalue()


def zip64(*,extension=b'',member64=False):
    # All body bytes are deliberately inaccessible through the probe's requester.
    body=b'PRIVATE SYNTHETIC PAYLOAD'*10;name=b'2019-11-10_1_10.vtl';extra=b''
    compressed,raw,offset,disk=20,40,0,0
    if member64:
        extra=struct.pack('<HHQQQ',1,24,raw,compressed,offset)
        compressed=raw=offset=0xffffffff
    central=struct.pack('<4s6H3I5H2I',b'PK\x01\x02',45,45,0,8,0,0,0xabcdef01,compressed,raw,
        len(name),len(extra),0,disk,0,0,offset)+name+extra
    start=len(body);position=start+len(central)
    record=struct.pack('<4sQ2H2I4Q',b'PK\x06\x06',44+len(extension),45,45,0,0,1,1,len(central),start)+extension
    locator=struct.pack('<4sIQI',b'PK\x06\x07',0,position,1)
    eocd=struct.pack('<4s4H2IH',b'PK\x05\x06',0,0,0xffff,0xffff,0xffffffff,0xffffffff,0)
    return body+central+record+locator+eocd


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    root=tmp_path/'study';root.mkdir();mapping=root/'maintenance/mapping.xlsx';mapping.parent.mkdir();mapping.write_bytes(b'ONLY SYNTHETIC MAPPING BYTES')
    monkeypatch.setattr(m,'ROOT',root);monkeypatch.setattr(m,'MAPPING_SHA256',m.sha(mapping.read_bytes()));monkeypatch.setattr(m,'MAPPING_BYTES',mapping.stat().st_size)
    return root,mapping


@pytest.mark.parametrize('kind',['classic','zip64','zip64_extension','zip64_member'])
def test_complete_metadata_probe_requests_only_exact_footer_and_central_records(fixture,kind):
    root,mapping=fixture
    raw=classic() if kind=='classic' else zip64(extension=b'extra metadata' if kind=='zip64_extension' else b'',member64=kind=='zip64_member')
    http=HTTP(raw);case=root/'maintenance/index'
    result=m.acquire(1,case,mapping_workbook=mapping,expected_total=len(raw),requester=http)
    assert result['status']=='complete_publisher_zip_directory_metadata_only'
    assert result['archive_bytes']==len(raw) and result['metadata_bytes_received']<=m.MAX_METADATA_BYTES
    assert result['raw_track_values_read'] is result['raw_member_verified'] is result['full_archive_verified'] is False
    assert result['gene_control_mapping_verified'] is result['individual_identity_clock_units_verified'] is False
    assert result['scientific_result'] is result['admission_authority'] is False
    assert result['central_directory']['zip64']==(kind!='classic')
    assert len(result['members'])==(2 if kind=='classic' else 1)
    boundary=result['central_directory']['offset']
    for method,url,headers,timeout in http.calls:
        assert timeout<=30 and headers['Accept-Encoding']=='identity'
        if method=='GET':
            start,end=map(int,headers['Range'][6:].split('-'))
            assert start>=boundary and end<len(raw) and headers['If-Match']=='"fixture"'
    assert all(r.closed for r in http.responses)
    assert not any(p.suffix=='.vtl' for p in case.iterdir())
    before={p.name:p.read_bytes() for p in case.iterdir()}
    with pytest.raises(ValueError,match='Fresh'):m.acquire(1,case,mapping_workbook=mapping,requester=http)
    assert before=={p.name:p.read_bytes() for p in case.iterdir()}


@pytest.mark.parametrize('change',['head_range','head_encoding','head_length','status200','wrong_range','wrong_total',
    'wrong_length','encoding','etag','redirect'])
def test_wrong_http_or_ignored_ranges_never_read_scientific_body_and_preserve_error(fixture,change):
    root,mapping=fixture;http=HTTP(classic(),change=change);case=root/'maintenance/rejected'
    with pytest.raises(ValueError):m.acquire(1,case,mapping_workbook=mapping,requester=http)
    assert all(response.reads==0 for response in http.responses)
    assert all(response.closed for response in http.responses)
    failure=json.loads((case/'failure.json').read_bytes())
    assert failure['automatic_replay_permitted'] is False and failure['scientific_result'] is False
    assert not (case/'metadata-index.json').exists()


@pytest.mark.parametrize('change',['truncated','oversized','partial'])
def test_range_failures_retain_exact_partial_chunks_without_any_automatic_replay(fixture,change):
    root,mapping=fixture;http=HTTP(classic(),change=change);case=root/'maintenance/partial'
    with pytest.raises((ValueError,IncompleteRead)):
        m.acquire(1,case,mapping_workbook=mapping,requester=http)
    chunks=list(case.glob('range-*.bin'))
    assert len(chunks)==1 and chunks[0].stat().st_size>0
    assert sum(method=='GET' for method,*_ in http.calls)==1
    assert not (case/'metadata-index.json').exists() and (case/'failure.json').exists()


@pytest.mark.parametrize('change',['wrong_total','mapping_sha','foreign_case','dataset0','dataset_bool','existing_case'])
def test_explicit_source_and_case_boundaries_fail_closed(fixture,change):
    root,mapping=fixture;case=root/'maintenance/case';dataset=1;http=HTTP(classic());total=None
    if change=='wrong_total':total=len(http.raw)+1
    elif change=='mapping_sha':mapping.write_bytes(b'changed')
    elif change=='foreign_case':case=root.parent/'outside'
    elif change=='dataset0':dataset=0
    elif change=='dataset_bool':dataset=True
    else:case.mkdir();(case/'original').write_bytes(b'preserved')
    with pytest.raises(ValueError):m.acquire(dataset,case,mapping_workbook=mapping,expected_total=total,requester=http)
    if change!='wrong_total':assert not http.calls
    if change=='existing_case':assert (case/'original').read_bytes()==b'preserved'


@pytest.mark.parametrize('change',['comment','split','different_counts','central_gap','central_oversized','zip64_bad_locator',
    'zip64_record_overlap','zip64_disagree'])
def test_footer_bounds_refuse_before_any_undeclared_body_range(fixture,change):
    root,mapping=fixture;raw=bytearray(zip64() if change.startswith('zip64') else classic())
    if change=='comment':raw[-2:]=b'\x01\x00'
    elif change=='split':struct.pack_into('<H',raw,len(raw)-18,1)
    elif change=='different_counts':struct.pack_into('<H',raw,len(raw)-14,99)
    elif change=='central_gap':struct.pack_into('<I',raw,len(raw)-6,1)
    elif change=='central_oversized':struct.pack_into('<I',raw,len(raw)-10,m.MAX_METADATA_BYTES+1)
    elif change=='zip64_bad_locator':raw[-42:-38]=b'BAD!'
    elif change=='zip64_record_overlap':
        pos=raw.find(b'PK\x06\x06');struct.pack_into('<Q',raw,pos+4,1000)
    else:struct.pack_into('<I',raw,len(raw)-10,1)
    http=HTTP(bytes(raw));case=root/'maintenance/bad-footer'
    with pytest.raises(ValueError):m.acquire(1,case,mapping_workbook=mapping,requester=http)
    assert not (case/'metadata-index.json').exists()
    assert all(int(headers['Range'][6:].split('-')[0])>=len(raw)-98 for method,url,headers,t in http.calls if method=='GET')


@pytest.mark.parametrize('change',['truncated','signature','count','duplicate','traversal','absolute','windows','disk','offset','missing64','extra_truncated'])
def test_central_directory_invariants_inventory_no_dropped_members(change):
    raw=zip64(member64=change=='missing64');start=raw.find(b'PK\x01\x02');end=raw.find(b'PK\x06\x06')
    central=bytearray(raw[start:end]);declared={'offset':start,'entries':1}
    if change=='truncated':central=central[:-1]
    elif change=='signature':central[:4]=b'BAD!'
    elif change=='count':declared['entries']=2
    elif change=='duplicate':central=central+central;declared['entries']=2
    elif change in {'traversal','absolute','windows'}:
        name={'traversal':b'../2019-11-10.vtl','absolute':b'/2019-11-10.vtl','windows':b'C:\\2019-11-10.vtl'}[change]
        struct.pack_into('<H',central,28,len(name));central=central[:46]+name
    elif change=='disk':struct.pack_into('<H',central,34,1)
    elif change=='offset':struct.pack_into('<I',central,42,start)
    elif change=='missing64':
        size=struct.unpack_from('<H',central,28)[0];central[46+size:46+size+2]=b'\x02\x00'
    else:
        struct.pack_into('<H',central,30,1);central=central+b'x'
    with pytest.raises((ValueError,UnicodeError)):m.central_records(bytes(central),declared,len(raw))


def test_finite_metadata_budget_refuses_directory_request_and_deadline(fixture,monkeypatch):
    root,mapping=fixture;http=HTTP(classic());case=root/'maintenance/bounded'
    monkeypatch.setattr(m,'MAX_METADATA_BYTES',22)
    with pytest.raises(ValueError):m.acquire(1,case,mapping_workbook=mapping,requester=http)
    assert sum(method=='GET' for method,*_ in http.calls)==1
    assert (case/'range-00-eocd.bin').stat().st_size==22
    case=root/'maintenance/deadline';case.mkdir();probe=m.Probe('https://publisher.test',case,requester=HTTP(classic()))
    probe.deadline=-1
    with pytest.raises(ValueError,match='deadline'):probe.read(0,1,'never')


def test_no_redirect_network_handler_refuses_without_following():
    with pytest.raises(ValueError,match='redirect'):m.NoRedirect().redirect_request(None,None,None,None,None,None)
