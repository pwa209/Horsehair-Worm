"""Mock-only ZIP/range acquisition tests: no provider or scientific calls."""
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import struct
import zlib

import pytest


SOURCE=Path(__file__).with_name('acquire-ethoscope-db-canary-20261021.py')
spec=importlib.util.spec_from_file_location('ethoscope_canary_private',SOURCE)
m=importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Response:
    def __init__(self, body, start, end, size, *, status=206, header_delta=None):
        self.body=body
        self.status_code=status
        self.headers={'Content-Range':f'bytes {start}-{end}/{size}',
                      'Content-Length':str(end-start+1)}
        self.headers.update(header_delta or {})
        self.iterated=False
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def iter_content(self,_):
        self.iterated=True
        for i in range(0,len(self.body),11):
            yield self.body[i:i+11]


class Session:
    def __init__(self,fixture,change=None):
        self.fixture=fixture
        self.change=change
        self.calls=[]
        self.responses=[]
    def get(self,url,**kwargs):
        assert url==m.PART_URL
        assert kwargs['allow_redirects'] is False
        assert kwargs['stream'] is True
        assert kwargs['headers']['Accept-Encoding']=='identity'
        start,end=map(int,kwargs['headers']['Range'].removeprefix('bytes=').split('-'))
        self.calls.append((start,end))
        response=Response(self.fixture['part'][start:end+1],start,end,len(self.fixture['part']))
        if self.change:
            self.change(response,len(self.calls),start,end)
        self.responses.append(response)
        return response


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    root=tmp_path/'personal'
    (root/'maintenance').mkdir(parents=True)
    inputs=root/'inputs'
    inputs.mkdir()
    # Use independently changing bytes so multiple bounded network pieces and
    # decompression boundaries are exercised, rather than a tiny repeated run.
    rng=random.Random(23)
    raw=bytes(rng.randrange(256) for _ in range(3000))
    compressor=zlib.compressobj(wbits=-15)
    payload=compressor.compress(raw)+compressor.flush()
    name='plos_path_results/machine/ETHOSCOPE_155/date/date_machine.db'
    name_bytes=name.encode()
    extra=struct.pack('<HH5s',0x5455,5,b'\x01\x00\x00\x00\x00')
    crc=zlib.crc32(raw)
    offset=16
    header=struct.pack('<4s5H3I2H',b'PK\x03\x04',20,0,8,0,0,crc,len(payload),len(raw),len(name_bytes),len(extra))
    data_start=offset+len(header)+len(name_bytes)+len(extra)
    next_offset=data_start+len(payload)
    part=b'prefix__________'+header+name_bytes+extra+payload+b'PK\x03\x04tail'
    assert len(part[:offset])==offset
    target={'name':name,'disk_start':51,'local_header_offset':offset,
            'compressed_bytes':len(payload),'uncompressed_bytes':len(raw),
            'crc32':f'{crc:08x}','compression':8,'flags':0,
            'raw_provider_md5':hashlib.md5(raw).hexdigest()}
    record={'id':6567039,'doi':'10.5281/zenodo.6567039','files':[
        {'key':m.PART_KEY,'size':len(part),'checksum':'md5:'+hashlib.md5(part).hexdigest(),
         'links':{'self':m.PART_URL}}]}
    selected={k:v for k,v in target.items() if k!='raw_provider_md5'}
    index={'central_directory_disk':58,'eocd_disk':58,'entries':[
        selected,{'name':'next','disk_start':51,'local_header_offset':next_offset}]}
    paths={}
    for label,value in [('record',record),('index',index)]:
        p=inputs/(label+'.json')
        p.write_text(json.dumps(value))
        paths[label]=p
    checksum=inputs/'checksums.txt'
    checksum.write_text(target['raw_provider_md5']+'  ./'+name.split('/',1)[1]+'\n')
    paths['checksum']=checksum
    monkeypatch.setattr(m,'ROOT',root)
    monkeypatch.setattr(m,'TARGET',target)
    monkeypatch.setattr(m,'PART_BYTES',len(part))
    monkeypatch.setattr(m,'PART_MD5',hashlib.md5(part).hexdigest())
    monkeypatch.setattr(m,'MAX_RANGE',256)
    for label in ['record','checksum','index']:
        monkeypatch.setattr(m,label.upper()+'_SHA',m._sha(paths[label]))
    return {'root':root,'paths':paths,'raw':raw,'part':part,'header':header,
            'data_start':data_start,'payload':payload,'next_offset':next_offset,
            'case':root/'maintenance'/'new-case'}


def invoke(f,change=None):
    s=Session(f,change)
    value=m.run(f['case'],**f['paths'],session=s)
    return value,s


def test_complete_verified_member_has_exact_bytes_and_false_scientific_flags(fixture):
    f=fixture
    result,session=invoke(f)
    final=Path(result['database'])
    assert final.read_bytes()==f['raw']
    assert result['verification']['sha256']==hashlib.sha256(f['raw']).hexdigest()
    assert result['status']=='provider_verified_member_acquired'
    assert result['final_promoted'] is True
    assert final.stat().st_nlink==1
    assert not (f['case']/'verified-db-candidate.partial').exists()
    assert result['sqlite_or_scientific_analysis_performed'] is False
    assert result['actual_database_schema_verified'] is False
    assert result['per_animal_exposure_timestamp_verified'] is False
    assert result['h1_cleared'] is False
    assert result['whole_part_provider_md5_verified'] is False
    intents=list((f['case']/'ranges').glob('*-intent.json'))
    responses=list((f['case']/'ranges').glob('*-response.json'))
    assert len(intents)==len(responses)==len(session.calls)>3
    assert all(json.loads(p.read_text())['verified'] for p in responses)
    assert all(e-s+1<=256 for s,e in session.calls)


@pytest.mark.parametrize('delta',[
    {'Content-Range':'bytes 0-29/999999'},
    {'Content-Length':'9999'},
    {'Content-Encoding':'gzip'},
    {'Content-Encoding':'br'},
])
def test_bad_response_headers_fail_before_body(fixture,delta):
    session=Session(fixture,lambda r,n,*_:r.headers.update(delta) if n==1 else None)
    with pytest.raises(ValueError):
        m.run(fixture['case'],**fixture['paths'],session=session)
    assert not session.responses[0].iterated
    assert not list(fixture['case'].glob('*.db'))
    failure=json.loads((fixture['case']/'acquisition-failure.json').read_text())
    assert failure['final_promoted'] is False
    assert len(list((fixture['case']/'ranges').glob('*-response.json')))==1


@pytest.mark.parametrize('status',[200,302,401,403,500])
def test_non206_is_not_read_or_retried(fixture,status):
    def change(r,n,*_):
        if n==1:r.status_code=status
    session=Session(fixture,change)
    with pytest.raises(ValueError,match='HTTP206'):
        m.run(fixture['case'],**fixture['paths'],session=session)
    assert len(session.calls)==1
    assert session.responses[0].iterated is False


@pytest.mark.parametrize('kind',['short','long','timeout'])
def test_partial_chunk_and_error_retained_no_promotion(fixture,kind):
    def change(r,n,*_):
        if n==3:
            if kind=='short':r.body=r.body[:-1]
            elif kind=='long':r.body+=b'X'
            else:
                def timed(_):
                    yield r.body[:5]
                    raise TimeoutError('mock transfer interruption')
                r.iter_content=timed
    session=Session(fixture,change)
    with pytest.raises((ValueError,TimeoutError)):
        m.run(fixture['case'],**fixture['paths'],session=session)
    assert len(session.calls)==3
    partial=fixture['case']/'ranges'/'0003-compressed-data.bin'
    assert partial.exists() and partial.stat().st_size>0
    receipt=json.loads(partial.with_name('0003-compressed-data-response.json').read_text())
    assert receipt['verified'] is False
    assert not list(fixture['case'].glob('*.db'))


@pytest.mark.parametrize('mutation',[
    ('signature',0,b'WRNG'),('version',4,struct.pack('<H',45)),
    ('flags_descriptor',6,struct.pack('<H',8)),('flags_encryption',6,struct.pack('<H',1)),
    ('method',8,struct.pack('<H',0)),('crc',14,struct.pack('<I',123)),
    ('compressed_size',18,struct.pack('<I',123)),('raw_size',22,struct.pack('<I',123)),
])
def test_local_header_variants_refused(fixture,mutation):
    _,offset,value=mutation
    part=bytearray(fixture['part'])
    start=m.TARGET['local_header_offset']+offset
    part[start:start+len(value)]=value
    fixture['part']=bytes(part)
    with pytest.raises(ValueError):invoke(fixture)
    assert not list(fixture['case'].glob('*.db'))


def test_wrong_local_filename_refused(fixture):
    part=bytearray(fixture['part'])
    part[m.TARGET['local_header_offset']+30]^=1
    fixture['part']=bytes(part)
    with pytest.raises(ValueError,match='filename'):invoke(fixture)


def test_zip64_extra_refused_even_with_plain_sizes(fixture):
    part=bytearray(fixture['part'])
    offset=m.TARGET['local_header_offset']+30+len(m.TARGET['name'])
    part[offset:offset+2]=b'\x01\x00'
    fixture['part']=bytes(part)
    with pytest.raises(ValueError,match='ZIP64'):invoke(fixture)


def test_successor_disagreement_prevents_hidden_descriptor(fixture,monkeypatch):
    p=fixture['paths']['index']
    value=json.loads(p.read_text())
    value['entries'][1]['local_header_offset']+=16
    p.write_text(json.dumps(value))
    monkeypatch.setattr(m,'INDEX_SHA',m._sha(p))
    with pytest.raises(ValueError,match='boundary'):invoke(fixture)


@pytest.mark.parametrize('kind',['corrupt_deflate','wrong_provider_md5','wrong_crc','raw_bound','compressed_trailing'])
def test_full_member_checks_refuse_corruption(fixture,monkeypatch,kind):
    f=fixture
    if kind=='corrupt_deflate':
        part=bytearray(f['part']);part[f['data_start']+20]^=0x80;f['part']=bytes(part)
    elif kind=='wrong_provider_md5':
        monkeypatch.setitem(m.TARGET,'raw_provider_md5','0'*32)
        p=f['paths']['checksum'];p.write_text('0'*32+'  ./'+m.TARGET['name'].split('/',1)[1]+'\n')
        monkeypatch.setattr(m,'CHECKSUM_SHA',m._sha(p))
    elif kind in {'wrong_crc','raw_bound'}:
        field='crc32' if kind=='wrong_crc' else 'uncompressed_bytes'
        value='00000001' if kind=='wrong_crc' else len(f['raw'])-1
        monkeypatch.setitem(m.TARGET,field,value)
        p=f['paths']['index'];index=json.loads(p.read_text());index['entries'][0][field]=value
        p.write_text(json.dumps(index));monkeypatch.setattr(m,'INDEX_SHA',m._sha(p))
        part=bytearray(f['part'])
        at=m.TARGET['local_header_offset']+(14 if kind=='wrong_crc' else 22)
        part[at:at+4]=struct.pack('<I',int(value,16) if kind=='wrong_crc' else value)
        f['part']=bytes(part)
    else:
        # Explicit decompressor test: an otherwise complete raw stream followed
        # by more compressed input cannot be admitted as one exact member.
        p=f['root']/'trailing.deflate';p.write_bytes(f['payload']+b'X')
        monkeypatch.setitem(m.TARGET,'compressed_bytes',len(f['payload'])+1)
        with pytest.raises(ValueError,match='trailing'):
            m._inflate([p],f['root']/'candidate')
        return
    with pytest.raises((ValueError,zlib.error)):invoke(f)
    assert not list(f['case'].glob('*.db'))


@pytest.mark.parametrize('key',['record','checksum','index'])
def test_changed_metadata_source_refused_without_network(fixture,key):
    fixture['paths'][key].write_bytes(fixture['paths'][key].read_bytes()+b' ')
    session=Session(fixture)
    with pytest.raises(ValueError,match='hash'):
        m.run(fixture['case'],**fixture['paths'],session=session)
    assert not session.calls


def test_fresh_case_required_no_ambiguity_replay(fixture):
    result,_=invoke(fixture)
    before={p.relative_to(fixture['case']):p.read_bytes() for p in fixture['case'].rglob('*') if p.is_file()}
    session=Session(fixture)
    with pytest.raises(FileExistsError):m.run(fixture['case'],**fixture['paths'],session=session)
    after={p.relative_to(fixture['case']):p.read_bytes() for p in fixture['case'].rglob('*') if p.is_file()}
    assert before==after and not session.calls


def test_publication_collision_keeps_existing_and_verified_candidate(fixture,monkeypatch):
    original=m._inflate
    def collide(chunks,candidate):
        result=original(chunks,candidate)
        (fixture['case']/Path(m.TARGET['name']).name).write_bytes(b'existing evidence')
        return result
    monkeypatch.setattr(m,'_inflate',collide)
    with pytest.raises(FileExistsError):invoke(fixture)
    assert (fixture['case']/Path(m.TARGET['name']).name).read_bytes()==b'existing evidence'
    assert (fixture['case']/'verified-db-candidate.partial').read_bytes()==fixture['raw']
    assert (fixture['case']/'promotion-intent.json').exists()
    failure=json.loads((fixture['case']/'acquisition-failure.json').read_text())
    assert failure['final_path_exists'] is True and failure['final_promoted'] is False


@pytest.mark.parametrize('change',['candidate','source_metadata'])
def test_change_after_deflate_verification_refused_before_promotion(fixture,monkeypatch,change):
    original=m._inflate
    def mutate(chunks,candidate):
        result=original(chunks,candidate)
        target=candidate if change=='candidate' else fixture['paths']['record']
        target.write_bytes(target.read_bytes()+b'X')
        return result
    monkeypatch.setattr(m,'_inflate',mutate)
    with pytest.raises(ValueError):invoke(fixture)
    assert not list(fixture['case'].glob('*.db'))


def test_outside_personal_case_refused(fixture):
    session=Session(fixture)
    with pytest.raises(ValueError,match='outside'):
        m.run(fixture['root'].parent/'outside',**fixture['paths'],session=session)
    assert not session.calls


def test_subordinate_directory_symlink_refused(fixture):
    link=fixture['root']/'maintenance'/'redirect'
    try:link.symlink_to(fixture['root']/'inputs',target_is_directory=True)
    except OSError:pytest.skip('host cannot create test symlink')
    with pytest.raises(ValueError,match='symlink'):
        m.run(link/'new-case',**fixture['paths'],session=Session(fixture))


def test_no_sqlite_import_or_query_in_gateway_source():
    text=SOURCE.read_text()
    assert 'import sqlite3' not in text
    assert 'sqlite3.connect' not in text
    assert 'SELECT ' not in text
