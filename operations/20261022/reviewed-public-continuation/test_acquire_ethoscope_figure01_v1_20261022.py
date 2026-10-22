"""Private fixture-only adapter qualification; no real HTTP or scientific DB."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import struct
import subprocess
import sys
from types import SimpleNamespace
import zlib

import pytest


SOURCE = Path(__file__).with_name("acquire-ethoscope-figure01-v1-20261022.py")
SPEC = importlib.util.spec_from_file_location("figure01_adapter",SOURCE)
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)
REAL_SOURCES = m._sources
REAL_DEADLINE = m._hard_deadline
METADATA = SOURCE.parent/"public-cohort-metadata-20261021"
MANIFEST = SOURCE.parent/"ethoscope-figure01-acquisition-manifest-v2-20261022.json"


class FakeTime:
    def __init__(self):
        self.now,self.sleeps = 0.0,[]
    def monotonic(self):
        return self.now
    def time(self):
        return 1_700_000_000+self.now
    def sleep(self,seconds):
        assert seconds > 0
        self.sleeps.append(seconds)
        self.now += seconds


class Response:
    def __init__(self,body,start,end,total):
        self.body,self.status_code,self.iterated = body,206,False
        self.headers = {"Content-Range":f"bytes {start}-{end}/{total}","Content-Length":str(end-start+1)}
    def __enter__(self):
        return self
    def __exit__(self,*args):
        return False
    def iter_content(self,size):
        self.iterated = True
        for offset in range(0,len(self.body),size):
            yield self.body[offset:offset+size]


class Session:
    def __init__(self,f,ordinal=1,change=None,one_part=False):
        self.f,self.change,self.calls,self.responses = f,change,[],[]
        member = f.context["manifest"]["members"][ordinal-1]
        e = member["central_entry"]
        extra = struct.pack("<HH",0x5455,5)+b"\x00"*5+struct.pack("<HH",0x7875,15)+b"\x00"*15
        assert len(extra) == 28 and len(e["name"].encode()) == 140
        header = struct.pack("<4s5H3I2H",b"PK\x03\x04",20,0,8,0,0,
            int(e["crc32"],16),e["compressed_bytes"],e["uncompressed_bytes"],140,28)+e["name"].encode()+extra
        joined = b"P"*f.header_offset+header+f.payload+b"PK\x03\x04successor"
        first_n = f.context["manifest"]["source_parts"][0]["bytes"]
        parts = f.context["manifest"]["source_parts"]
        self.parts = {parts[0]["url"]:joined[:first_n],parts[1]["url"]:joined[first_n:]}
        for part in parts:
            data = self.parts[part["url"]]
            self.parts[part["url"]] = data+b"T"*(part["bytes"]-len(data))
        self.member,self.header = member,header
    def get(self,url,**kwargs):
        assert url in self.parts
        assert kwargs["allow_redirects"] is False and kwargs["stream"] is True
        assert kwargs["timeout"] == (15,90)
        assert kwargs["headers"]["Accept-Encoding"] == "identity"
        start,end = map(int,kwargs["headers"]["Range"].removeprefix("bytes=").split("-"))
        self.calls.append((url,start,end,self.f.time.now))
        response = Response(self.parts[url][start:end+1],start,end,len(self.parts[url]))
        if self.change:
            self.change(response,len(self.calls),url,start,end)
        self.responses.append(response)
        return response


@pytest.fixture
def f(tmp_path,monkeypatch):
    root = tmp_path/"personal"
    root.mkdir()
    maintenance = root/"maintenance"
    maintenance.mkdir()
    metadata = root/"metadata"
    metadata.mkdir()
    marker = metadata/"fixture-metadata.txt"
    marker.write_bytes(b"synthetic source provenance, no scientific payload\n")
    manifest_path = root/"fixture-manifest.json"
    manifest_path.write_bytes(b"synthetic immutable manifest fixture\n")
    rng = random.Random(23)
    raw = bytes(rng.randrange(256) for _ in range(3000))
    compressor = zlib.compressobj(wbits=-15)
    payload = compressor.compress(raw)+compressor.flush()
    header_offset = 16
    first_bytes = header_offset+198+len(payload)//2
    second_bytes = len(payload)-len(payload)//2+64
    parts,logical = [],0
    for disk,size in enumerate((first_bytes,second_bytes)):
        key = f"db_files.z{disk+1:02d}"
        parts.append({"disk":disk,"key":key,"bytes":size,"provider_md5":"a"*32,
            "url":f"https://zenodo.org/api/records/6567039/files/{key}/content",
            "logical_start":logical,"logical_end_exclusive":logical+size})
        logical += size
    members = []
    for ordinal in range(1,37):
        machine = f"ETHOSCOPE_{ordinal:03d}"
        recording = "2017-03-15_10-00-00"
        name = f"plos_path_results/{'a'*32}/{machine}/{recording}/{recording}_{'a'*32}.db"
        assert len(name) == 140
        e = {"name":name,"disk_start":0,"local_header_offset":header_offset,"flags":0,
            "compression":8,"version_needed":20,"crc32":f"{zlib.crc32(raw):08x}",
            "compressed_bytes":len(payload),"uncompressed_bytes":len(raw)}
        start = header_offset+198
        members.append({"source_ordinal":ordinal,"source_id":"2017-03-15/"+machine,
            "central_entry":e,"raw_provider_md5":hashlib.md5(raw).hexdigest(),
            "compressed_logical_start_inferred":start,"compressed_logical_end_exclusive_inferred":start+len(payload),
            "successor_central_entry":{"logical_local_header_offset":start+len(payload)},
            "compressed_segments_inferred_pending_local_header_validation":m._segments(parts,start,len(payload))})
    time = FakeTime()
    context = {"manifest":{"members":members,"source_parts":parts},"manifest_path":str(manifest_path),
        "metadata_dir":str(metadata),"manifest_sha256":hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "source_pins":[{"path":str(marker),"sha256":hashlib.sha256(marker.read_bytes()).hexdigest()}],
        "helper_sha256":m._helper_sha()}
    monkeypatch.setattr(m,"ROOT",root)
    monkeypatch.setattr(m,"EXPECTED_UID",root.stat().st_uid)
    monkeypatch.setattr(m,"time",time)
    @contextmanager
    def mock_deadline(seconds):
        assert seconds in (900,600)
        yield
    monkeypatch.setattr(m,"_hard_deadline",mock_deadline)
    def sources(path,directory):
        assert path == manifest_path and directory == metadata
        assert hashlib.sha256(path.read_bytes()).hexdigest() == context["manifest_sha256"]
        for pin in context["source_pins"]:
            if hashlib.sha256(Path(pin["path"]).read_bytes()).hexdigest() != pin["sha256"]:
                raise ValueError("Fixture metadata changed")
        current = deepcopy(context)
        current["helper_sha256"] = m._helper_sha()
        return current
    monkeypatch.setattr(m,"_sources",sources)
    monkeypatch.setenv("SLURM_JOB_ID","123456")
    monkeypatch.setenv("SLURM_CPUS_PER_TASK","1")
    monkeypatch.setenv("SLURM_NTASKS","1")
    monkeypatch.delenv("SLURM_ARRAY_TASK_ID",raising=False)
    return SimpleNamespace(root=root,maintenance=maintenance,metadata=metadata,manifest=manifest_path,
        marker=marker,context=context,raw=raw,payload=payload,header_offset=header_offset,time=time,
        transfer_case=maintenance/"transfer",verification_case=maintenance/"verification")


def transfer(f,session=None,ordinal=1,case=None):
    session = session or Session(f,ordinal)
    result = m.acquire_one(f.manifest,f.metadata,ordinal,case or f.transfer_case,session=session)
    return result,session


def verify(f,ordinal=1,case=None,receipt=None,sha=None):
    receipt = receipt or f.transfer_case/"transfer-result.json"
    sha = sha or hashlib.sha256(receipt.read_bytes()).hexdigest()
    return m.verify_one(f.manifest,f.metadata,ordinal,receipt,sha,case or f.verification_case)


def rewrite(path,value):
    path.write_bytes(m._encoded(value))
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_positive_two_part_transfer_then_native_verification_has_exact_raw_bytes(f):
    result,session = transfer(f)
    assert result["status"] == "compressed_member_acquired_pending_native_verification"
    assert result["raw_member_integrity_verified"] is False
    assert not list(f.transfer_case.glob("*.db"))
    assert len(session.calls) == 4
    assert session.calls[-1][1] == 0
    assert all(b[3]-a[3] >= 1 for a,b in zip(session.calls,session.calls[1:]))
    native = verify(f)
    final = Path(native["database"])
    assert final.read_bytes() == f.raw and final.stat().st_nlink == 1
    assert not (f.verification_case/"verified-db-candidate.partial").exists()
    assert native["verification"]["sha256"] == hashlib.sha256(f.raw).hexdigest()
    assert native["verification"]["provider_md5"] == hashlib.md5(f.raw).hexdigest()
    assert native["status"] == "provider_verified_member_acquired"
    assert native["native_environment"]["root_native_ownership_join_required"] is True
    assert all(native[key] is False for key in m.FALSE_FLAGS)


def test_one_part_member_also_works_without_full_part_request(f):
    parts = f.context["manifest"]["source_parts"]
    parts[0]["bytes"] += len(f.payload)
    parts[0]["logical_end_exclusive"] = parts[0]["bytes"]
    parts[1]["logical_start"] = parts[0]["bytes"]
    parts[1]["logical_end_exclusive"] = parts[0]["bytes"]+parts[1]["bytes"]
    for member in f.context["manifest"]["members"]:
        member["compressed_segments_inferred_pending_local_header_validation"] = m._segments(parts,214,len(f.payload))
    result,session = transfer(f)
    assert len(session.calls) == 3 and len({r[0] for r in session.calls}) == 1
    assert verify(f)["verification"]["raw_bytes"] == len(f.raw)
    assert all(end-start+1 < len(session.parts[url]) for url,start,end,_ in session.calls)


@pytest.mark.parametrize("first_payload_bytes",[1,2,3003,3004])
def test_single_decoder_handles_split_inside_block_or_one_byte_from_end(f,first_payload_bytes):
    assert 0 < first_payload_bytes < len(f.payload)
    parts=f.context["manifest"]["source_parts"]
    parts[0]["bytes"]=f.header_offset+198+first_payload_bytes
    parts[0]["logical_end_exclusive"]=parts[0]["bytes"]
    parts[1]["bytes"]=len(f.payload)-first_payload_bytes+64
    parts[1]["logical_start"]=parts[0]["bytes"]
    parts[1]["logical_end_exclusive"]=parts[0]["bytes"]+parts[1]["bytes"]
    for member in f.context["manifest"]["members"]:
        member["compressed_segments_inferred_pending_local_header_validation"]=m._segments(parts,214,len(f.payload))
    transfer(f)
    assert Path(verify(f)["database"]).read_bytes()==f.raw


def high_compression_fixture(f,raw_size,split):
    f.raw=b"\x00"*raw_size
    compressor=zlib.compressobj(wbits=-15)
    f.payload=compressor.compress(f.raw)+compressor.flush()
    first_payload=(len(f.payload)//2) if split else len(f.payload)+64
    parts=f.context["manifest"]["source_parts"]
    parts[0]["bytes"]=f.header_offset+198+first_payload
    parts[0]["logical_end_exclusive"]=parts[0]["bytes"]
    parts[1]["bytes"]=(len(f.payload)-first_payload+64) if split else 64
    parts[1]["logical_start"]=parts[0]["bytes"]
    parts[1]["logical_end_exclusive"]=parts[0]["bytes"]+parts[1]["bytes"]
    for member in f.context["manifest"]["members"]:
        e=member["central_entry"]
        e.update(compressed_bytes=len(f.payload),uncompressed_bytes=raw_size,crc32=f"{zlib.crc32(f.raw):08x}")
        member["raw_provider_md5"]=hashlib.md5(f.raw).hexdigest()
        member["compressed_logical_end_exclusive_inferred"]=214+len(f.payload)
        member["successor_central_entry"]["logical_local_header_offset"]=214+len(f.payload)
        member["compressed_segments_inferred_pending_local_header_validation"]=m._segments(parts,214,len(f.payload))


@pytest.mark.parametrize("raw_size",[65_537,131_073,262_145])
@pytest.mark.parametrize("split",[False,True])
def test_high_compression_larger_than_output_buffer_verifies_whole_raw_stream(f,raw_size,split):
    high_compression_fixture(f,raw_size,split)
    assert len(f.payload)<65536 and len(f.raw)>65536
    transfer(f)
    result=verify(f)
    assert Path(result["database"]).read_bytes()==f.raw
    assert result["verification"]["raw_bytes"]==raw_size
    assert result["verification"]["provider_md5"]==hashlib.md5(f.raw).hexdigest()


def test_buffered_output_with_no_unconsumed_input_requires_bounded_empty_drain(f,monkeypatch):
    high_compression_fixture(f,65_537,False)
    transfer(f)
    calls=[]
    class Buffered:
        eof=False
        unused_data=b""
        unconsumed_tail=b""
        def decompress(self,pending,limit):
            calls.append((pending,limit))
            if len(calls)==1:
                assert pending==f.payload and limit==65536
                return b"\x00"*65536
            assert len(calls)==2 and pending==b"" and limit==2
            self.eof=True
            return b"\x00"
    monkeypatch.setattr(m.zlib,"decompressobj",lambda *args:Buffered())
    result=verify(f)
    assert len(calls)==2 and Path(result["database"]).read_bytes()==f.raw


def test_buffered_empty_drain_cannot_exceed_the_raw_plus_one_limit(f,monkeypatch):
    high_compression_fixture(f,65_537,False)
    transfer(f)
    class Overrun:
        eof=False
        unused_data=b""
        unconsumed_tail=b""
        calls=0
        def decompress(self,pending,limit):
            self.calls+=1
            if self.calls==1:
                assert limit==65536
                return b"\x00"*65536
            assert pending==b"" and limit==2
            self.eof=True
            return b"\x00\x00"
    monkeypatch.setattr(m.zlib,"decompressobj",lambda *args:Overrun())
    with pytest.raises(ValueError,match="raw bound"):
        verify(f)
    assert (f.verification_case/"verified-db-candidate.partial").stat().st_size==65536
    assert not list(f.verification_case.glob("*.db"))


def test_decoder_unchanged_nonempty_tail_without_output_fails_no_progress(f,monkeypatch):
    transfer(f)
    class Stalled:
        eof=False
        unused_data=b""
        unconsumed_tail=b""
        def decompress(self,pending,limit):
            self.unconsumed_tail=pending
            return b""
    monkeypatch.setattr(m.zlib,"decompressobj",lambda *args:Stalled())
    with pytest.raises(ValueError,match="no progress"):
        verify(f)
    assert not list(f.verification_case.glob("*.db"))


def test_empty_drain_without_output_or_eof_stops_and_refuses_incomplete_stream(f,monkeypatch):
    transfer(f)
    calls=[]
    class Incomplete:
        eof=False
        unused_data=b""
        unconsumed_tail=b""
        def decompress(self,pending,limit):
            calls.append(pending)
            if pending:
                return b"\x00"
            return b""
    monkeypatch.setattr(m.zlib,"decompressobj",lambda *args:Incomplete())
    with pytest.raises(ValueError,match="Incomplete/extra"):
        verify(f)
    assert len(calls)==4 and calls[1]==calls[3]==b""


def test_high_compression_empty_drain_retains_trailing_payload_refusal(f):
    high_compression_fixture(f,65_537,False)
    transfer(f)
    member=deepcopy(f.context["manifest"]["members"][0])
    chunk=next((f.transfer_case/"ranges").glob("*compressed-data.bin"))
    chunk.write_bytes(chunk.read_bytes()+b"trailing")
    member["central_entry"]["compressed_bytes"]+=len(b"trailing")
    candidate=f.maintenance/"high-compression-trailing.partial"
    with pytest.raises(ValueError,match="Trailing"):
        m._inflate(member,[chunk],candidate,m.Clock(600))


@pytest.mark.parametrize("status",[200,302,401,403,429,500])
def test_non206_body_is_never_consumed_or_retried(f,status):
    session = Session(f,change=lambda r,n,*_:setattr(r,"status_code",status) if n==1 else None)
    with pytest.raises(ValueError,match="HTTP206"):
        transfer(f,session)
    assert len(session.calls) == 1 and not session.responses[0].iterated
    assert (f.transfer_case/"transfer-failure.json").exists()
    assert not (f.transfer_case/"transfer-result.json").exists()


@pytest.mark.parametrize("delta",[{"Content-Range":"bytes 0-29/1"},{"Content-Length":"31"},
    {"Content-Encoding":"gzip"},{"Content-Encoding":"br"}])
def test_bad_headers_fail_before_body_and_preserve_response(f,delta):
    session = Session(f,change=lambda r,n,*_:r.headers.update(delta) if n==1 else None)
    with pytest.raises(ValueError):
        transfer(f,session)
    assert not session.responses[0].iterated
    response = json.loads((f.transfer_case/"ranges/0001-local-header-response.json").read_text())
    assert response["headers"] and response["verified"] is False


def test_oversized_header_is_bounded_and_refused_without_body(f):
    session = Session(f,change=lambda r,n,*_:r.headers.update({"Location":"x"*5000}) if n==1 else None)
    with pytest.raises(ValueError,match="Oversized"):
        transfer(f,session)
    response = json.loads((f.transfer_case/"ranges/0001-local-header-response.json").read_text())
    assert len(response["headers"]["Location"]) == 4097 and not session.responses[0].iterated


def test_escaped_header_byte_bound_refuses_before_body_but_preserves_error_receipt(f):
    def changed(response,n,*args):
        if n==1:
            for key in ("ETag","Last-Modified","Location","Retry-After","X-RateLimit-Limit","X-RateLimit-Remaining"):
                response.headers[key]="\u00e9"*4000
    session=Session(f,change=changed)
    with pytest.raises(ValueError,match="header proof"):
        transfer(f,session)
    assert not session.responses[0].iterated
    receipt=f.transfer_case/"ranges/0001-local-header-response.json"
    assert receipt.exists() and receipt.stat().st_size<=m.MAX_RANGE_RESPONSE_JSON


@pytest.mark.parametrize("encoding",[None,"identity","IDENTITY"])
def test_identity_encoding_is_case_insensitive(f,encoding):
    session=Session(f,change=lambda response,*args:response.headers.update({"Content-Encoding":encoding}))
    result,_=transfer(f,session)
    assert result["status"]=="compressed_member_acquired_pending_native_verification"


def test_explicit_receipt_byte_limit_refuses_before_creating_file(f):
    path=f.maintenance/"bounded.json"
    with pytest.raises(ValueError,match="output bound"):
        m._json(path,{"value":"x"*100},limit=20)
    assert not path.exists()


@pytest.mark.parametrize("kind",["short","long","timeout"])
def test_second_part_failure_preserves_prior_ranges_and_partial_without_resume(f,kind):
    def change(response,n,*_):
        if n == 4:
            if kind == "short":
                response.body = response.body[:-1]
            elif kind == "long":
                response.body += b"X"
            else:
                def interrupted(size):
                    yield response.body[:5]
                    raise TimeoutError("fixture interruption")
                response.iter_content = interrupted
    session = Session(f,change=change)
    with pytest.raises((ValueError,TimeoutError)):
        transfer(f,session)
    assert len(session.calls) == 4
    partial = f.transfer_case/"ranges/0004-compressed-data.bin"
    assert partial.exists() and partial.stat().st_size > 0
    assert (f.transfer_case/"ranges/0003-compressed-data-response.json").exists()
    if kind == "long":
        assert partial.stat().st_size == len(session.responses[-1].body)
    with pytest.raises(FileExistsError):
        transfer(f,Session(f))


@pytest.mark.parametrize("offset,value",[(0,b"WRNG"),(4,struct.pack("<H",45)),
    (6,struct.pack("<H",8)),(6,struct.pack("<H",1)),(8,struct.pack("<H",0)),
    (26,struct.pack("<H",139)),(28,struct.pack("<H",29))])
def test_unsupported_fixed_header_fails_before_continuation_or_payload(f,offset,value):
    session = Session(f)
    url = f.context["manifest"]["source_parts"][0]["url"]
    body = bytearray(session.parts[url])
    body[f.header_offset+offset:f.header_offset+offset+len(value)] = value
    session.parts[url] = bytes(body)
    with pytest.raises(ValueError):
        transfer(f,session)
    assert len(session.calls) == 1


@pytest.mark.parametrize("kind",["crc","compressed_size","raw_size","name","zip64_extra","truncated_extra"])
def test_header_crc_size_name_and_member_extra_variants_refused(f,kind):
    session = Session(f)
    url = f.context["manifest"]["source_parts"][0]["url"]
    part = bytearray(session.parts[url]);base=f.header_offset
    if kind in {"crc","compressed_size","raw_size"}:
        offset = {"crc":14,"compressed_size":18,"raw_size":22}[kind]
        part[base+offset:base+offset+4] = struct.pack("<I",123)
    elif kind == "name":
        part[base+30] ^= 1
    elif kind == "zip64_extra":
        part[base+170:base+172] = struct.pack("<H",1)
    else:
        part[base+172:base+174] = struct.pack("<H",999)
    session.parts[url] = bytes(part)
    with pytest.raises(ValueError):
        transfer(f,session)
    assert len(session.calls) == 2 and not (f.transfer_case/"transfer-result.json").exists()


def test_indexed_successor_disagreement_cannot_accept_hidden_descriptor(f):
    f.context["manifest"]["members"][0]["successor_central_entry"]["logical_local_header_offset"] += 16
    with pytest.raises(ValueError,match="successor"):
        transfer(f)
    assert len(list((f.transfer_case/"ranges").glob("*-intent.json"))) == 2


@pytest.mark.parametrize("kind",["changed_boundary","swapped_chunks","missing_chunk","duplicate_chunk","trailing_data","wrong_md5","wrong_crc","raw_bound"])
def test_native_inflate_refuses_corrupt_spanning_stream_without_promotion(f,kind):
    transfer(f)
    member = deepcopy(f.context["manifest"]["members"][0])
    chunks = sorted((f.transfer_case/"ranges").glob("*compressed-data.bin"))
    if kind == "changed_boundary":
        body = bytearray(chunks[1].read_bytes());body[0] ^= 1;chunks[1].write_bytes(body)
    elif kind == "swapped_chunks":
        chunks.reverse()
    elif kind == "missing_chunk":
        chunks.pop()
    elif kind == "duplicate_chunk":
        chunks.insert(1,chunks[0])
    elif kind == "trailing_data":
        chunks[1].write_bytes(chunks[1].read_bytes()+b"X")
        member["central_entry"]["compressed_bytes"] += 1
    elif kind == "wrong_md5":
        member["raw_provider_md5"] = "0"*32
    elif kind == "wrong_crc":
        member["central_entry"]["crc32"] = "00000001"
    else:
        member["central_entry"]["uncompressed_bytes"] -= 1
    candidate = f.maintenance/"synthetic-inflate.partial"
    with pytest.raises((ValueError,zlib.error)):
        m._inflate(member,chunks,candidate,m.Clock(600))
    assert candidate.exists()


@pytest.mark.parametrize("kind",["chunk","intent","response","receipt","wrong_sha","wrong_member","failed_case"])
def test_closed_transfer_proof_cannot_be_substituted_for_native_verification(f,kind):
    result,_ = transfer(f)
    path = f.transfer_case/"transfer-result.json"
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    ordinal = 1
    if kind == "chunk":
        p = Path(result["ranges"][2]["body"]["path"]);p.write_bytes(p.read_bytes()+b"X")
    elif kind in {"intent","response"}:
        p = Path(result["ranges"][2][kind]["path"]);p.write_bytes(p.read_bytes()+b" ")
    elif kind == "receipt":
        path.write_bytes(path.read_bytes()+b" ")
    elif kind == "wrong_sha":
        sha = "0"*64
    elif kind == "wrong_member":
        ordinal = 2
    else:
        (f.transfer_case/"transfer-failure.json").write_bytes(b"{}")
    with pytest.raises(ValueError):
        verify(f,ordinal=ordinal,sha=sha)
    assert not list(f.verification_case.glob("*.db"))


@pytest.mark.parametrize("field,value",[("SLURM_JOB_ID",""),("SLURM_JOB_ID","123_1"),
    ("SLURM_CPUS_PER_TASK","2"),("SLURM_NTASKS","2"),("SLURM_ARRAY_TASK_ID","1")])
def test_native_verification_requires_scalar_one_cpu_environment(f,monkeypatch,field,value):
    monkeypatch.setenv(field,value)
    with pytest.raises(ValueError):
        m.verify_one(f.manifest,f.metadata,1,f.transfer_case/"none","0"*64,f.verification_case)
    assert not f.verification_case.exists()


def test_metadata_source_change_after_transfer_prevents_native_output(f):
    transfer(f)
    f.marker.write_bytes(b"changed source")
    with pytest.raises(ValueError,match="metadata"):
        verify(f)
    assert not f.verification_case.exists()


@pytest.mark.parametrize("kind",["candidate","source","helper","compressed_chunk"])
def test_changes_after_inflate_are_refused_before_promotion(f,monkeypatch,kind):
    result,_ = transfer(f)
    original = m._inflate
    def changed(*args):
        raw = original(*args)
        if kind == "candidate":
            p = Path(args[2]);p.write_bytes(p.read_bytes()+b"X")
        elif kind == "source":
            f.marker.write_bytes(b"changed metadata")
        elif kind == "helper":
            monkeypatch.setattr(m,"_helper_sha",lambda:"0"*64)
        else:
            p=Path(result["ranges"][2]["body"]["path"]);p.write_bytes(p.read_bytes()+b"X")
        return raw
    monkeypatch.setattr(m,"_inflate",changed)
    with pytest.raises(ValueError):
        verify(f)
    assert not list(f.verification_case.glob("*.db"))
    assert (f.verification_case/"verification-failure.json").exists()


def test_existing_final_is_kept_on_exclusive_publication_collision(f,monkeypatch):
    transfer(f)
    original = m._inflate
    def collide(*args):
        raw = original(*args)
        (f.verification_case/Path(args[0]["central_entry"]["name"]).name).write_bytes(b"existing evidence")
        return raw
    monkeypatch.setattr(m,"_inflate",collide)
    with pytest.raises(FileExistsError):
        verify(f)
    final, = f.verification_case.glob("*.db")
    assert final.read_bytes() == b"existing evidence"
    assert (f.verification_case/"verified-db-candidate.partial").read_bytes() == f.raw
    assert json.loads((f.verification_case/"verification-failure.json").read_text())["final_promoted"] is False


def test_crash_after_link_retains_two_link_ambiguity_and_forbids_replay(f,monkeypatch):
    transfer(f)
    original = m._read_file
    def interrupted(path,*args,**kwargs):
        if Path(path).suffix == ".db" and kwargs.get("nlinks") == (2,):
            raise TimeoutError("fixture crash after link")
        return original(path,*args,**kwargs)
    monkeypatch.setattr(m,"_read_file",interrupted)
    with pytest.raises(TimeoutError):
        verify(f)
    final, = f.verification_case.glob("*.db")
    assert final.stat().st_nlink == 2
    assert (f.verification_case/"verified-db-candidate.partial").exists()
    with pytest.raises(FileExistsError):
        verify(f)


def test_verification_case_is_never_replayed(f):
    transfer(f);verify(f)
    before={p.name:p.read_bytes() for p in f.verification_case.iterdir() if p.is_file()}
    with pytest.raises(FileExistsError):verify(f)
    assert before == {p.name:p.read_bytes() for p in f.verification_case.iterdir() if p.is_file()}


def test_network_deadline_retains_opened_headers_and_stops_without_payload(f):
    session = Session(f,change=lambda r,n,*_:setattr(f.time,"now",901) if n==1 else None)
    with pytest.raises(TimeoutError):transfer(f,session)
    proof=json.loads((f.transfer_case/"ranges/0001-local-header-response.json").read_text())
    assert proof["status"] == 206 and proof["headers"]["Content-Range"]
    assert not session.responses[0].iterated


def test_native_deadline_preserves_partial_candidate(f,monkeypatch):
    transfer(f)
    original=m.zlib.decompressobj
    class Slow:
        def __init__(self,real):self.real=real
        def __getattr__(self,name):return getattr(self.real,name)
        def decompress(self,*args):
            f.time.now += 601
            return self.real.decompress(*args)
    monkeypatch.setattr(m.zlib,"decompressobj",lambda *args:Slow(original(*args)))
    with pytest.raises(TimeoutError):verify(f)
    assert (f.verification_case/"verified-db-candidate.partial").exists()
    assert not list(f.verification_case.glob("*.db"))


@pytest.mark.parametrize("start,count",[(-1,1),(0,0),(0,-1),(True,1),(0,False),(0,1.0)])
def test_invalid_range_parameters_are_refused_before_intent(f,start,count):
    part=f.context["manifest"]["source_parts"][0]
    with pytest.raises(ValueError):m._range_intent(f.context,f.context["manifest"]["members"][0],part,start,count,"compressed-data",1)


def test_exact8MiB_maximum_and_overflow_or_whole_part_refusal(f):
    part=deepcopy(f.context["manifest"]["source_parts"][0]);part["bytes"]=m.MAX_RANGE+100
    member=f.context["manifest"]["members"][0]
    assert m._range_intent(f.context,member,part,0,m.MAX_RANGE,"compressed-data",1)["bytes"] == 8_388_608
    for start,count in [(0,m.MAX_RANGE+1),(101,m.MAX_RANGE),(0,part["bytes"])]:
        with pytest.raises(ValueError):m._range_intent(f.context,member,part,start,count,"compressed-data",1)


@pytest.mark.parametrize("overflow",[False,True])
def test_actual8MiB_response_stream_and_plus_one_evidence_cap(f,overflow):
    part=deepcopy(f.context["manifest"]["source_parts"][0]);part["bytes"]=m.MAX_RANGE+128
    session=Session(f)
    session.parts[part["url"]]=b"X"*part["bytes"]
    if overflow:
        def changed(response,n,*_):
            response.iter_content=lambda size:iter([b"X"*(m.MAX_RANGE+100)])
        session.change=changed
    case=m._fresh_case(f.maintenance/"range-bound")
    reader=m.RangeReader(f.context,f.context["manifest"]["members"][0],case,session,m.Clock(900))
    if overflow:
        with pytest.raises(ValueError,match="exceeded"):
            reader.read(part,0,m.MAX_RANGE,"compressed-data")
        assert (case/"ranges/0001-compressed-data.bin").stat().st_size==m.MAX_RANGE+1
    else:
        body=reader.read(part,0,m.MAX_RANGE,"compressed-data")
        assert body.stat().st_size==m.MAX_RANGE
        assert reader.records[0]["body"]["sha256"]==hashlib.sha256(b"X"*m.MAX_RANGE).hexdigest()


@pytest.mark.parametrize("ordinal",[0,37,True,1.0])
def test_invalid_member_ordinal_is_refused_without_network_or_case(f,ordinal):
    session=Session(f)
    with pytest.raises(ValueError):transfer(f,session,ordinal=ordinal)
    assert not session.calls and not f.transfer_case.exists()


def test_outside_personal_root_and_maintenance_or_traversal_are_refused(f):
    for path in [f.root.parent/"outside",f.root/"not-maintenance",f.maintenance/".."/"escape"]:
        with pytest.raises(ValueError):transfer(f,Session(f),case=path)


def test_subordinate_symlink_check_and_wrong_uid_refuse_before_network(f,monkeypatch):
    original=Path.is_symlink
    monkeypatch.setattr(Path,"is_symlink",lambda p: True if p==f.maintenance else original(p))
    with pytest.raises(ValueError,match="symlink"):transfer(f,Session(f))
    monkeypatch.setattr(Path,"is_symlink",original)
    monkeypatch.setattr(m,"EXPECTED_UID",f.root.stat().st_uid+1)
    with pytest.raises(ValueError,match="UID"):transfer(f,Session(f))


def test_hardlinked_source_file_is_refused(f):
    p=f.maintenance/"owned.txt";p.write_bytes(b"preserve")
    os.link(p,f.maintenance/"second.txt")
    with pytest.raises(ValueError,match="link"):m._read_file(p,100)


def synthetic_receipt_index(f):
    rows=[]
    for member in f.context["manifest"]["members"]:
        case=f.maintenance/f"synthetic-native-{member['source_ordinal']:02d}"
        case.mkdir()
        final=case/Path(member["central_entry"]["name"]).name
        final.write_bytes(f.raw)
        raw={"compressed_bytes":len(f.payload),"raw_bytes":len(f.raw),"crc32":f"{zlib.crc32(f.raw):08x}",
             "provider_md5":hashlib.md5(f.raw).hexdigest(),"sha256":hashlib.sha256(f.raw).hexdigest()}
        full_proof=m._json(case/"verified-full-member.json",raw)
        result={**m._start(f.context,member,case,"verification"),"status":"provider_verified_member_acquired",
            "final_promoted":True,"database":str(final),"verification":raw,"native_environment":m._native_environment(),
            "verified_full_member":full_proof}
        proof=m._json(case/"verification-result.json",result)
        rows.append({"path":proof["path"],"sha256":proof["sha256"]})
    index=f.maintenance/"synthetic-receipt-index.json"
    data={"manifest_sha256":f.context["manifest_sha256"],"receipts":rows}
    sha=rewrite(index,data)
    return index,sha,data


def test_full36_assembly_preserves_scope_and_requires_external_native_join(f):
    index,sha,_=synthetic_receipt_index(f)
    result=m.assemble_cohort(f.manifest,f.metadata,index,sha,f.maintenance/"assembly")
    assert result["provider_verified_member_count"]==36 and result["all370_metadata_rows_preserved"] is True
    assert result["other_obs_rows_retained"]==8 and result["scientific_study_complete"] is False
    assert result["root_native_ownership_and_qualification_join_required"] is True
    assert all(result[key] is False for key in m.FALSE_FLAGS)


@pytest.mark.parametrize("kind",["missing","duplicate","wrong_manifest","wrong_helper","wrong_raw","changed_proof","negative_case"])
def test_assembly_cannot_promote_partial_duplicate_changed_or_cross_manifest_set(f,kind):
    index,sha,data=synthetic_receipt_index(f)
    first=Path(data["receipts"][0]["path"])
    if kind=="missing":data["receipts"].pop()
    elif kind=="duplicate":data["receipts"][-1]=data["receipts"][0]
    elif kind in {"wrong_manifest","wrong_helper"}:
        result=json.loads(first.read_text());result["manifest_sha256" if kind=="wrong_manifest" else "helper_sha256"]="0"*64
        data["receipts"][0]["sha256"]=rewrite(first,result)
    elif kind=="wrong_raw":
        final=next(first.parent.glob("*.db"));final.write_bytes(b"changed raw bytes")
    elif kind=="changed_proof":
        p=first.parent/"verified-full-member.json";p.write_bytes(p.read_bytes()+b" ")
    else:(first.parent/"verification-failure.json").write_bytes(b"{}")
    sha=rewrite(index,data)
    with pytest.raises(ValueError):m.assemble_cohort(f.manifest,f.metadata,index,sha,f.maintenance/"assembly")
    assert not (f.maintenance/"assembly/assembly-result.json").exists()


def test_actual_frozen_complete_sources_validate_without_raw_payload(tmp_path,monkeypatch):
    root=tmp_path/"root";root.mkdir();metadata=root/"inputs";metadata.mkdir()
    manifest_path=root/"manifest.json";manifest_path.write_bytes(MANIFEST.read_bytes())
    for name in m.SOURCE_PINS:(metadata/name).write_bytes((METADATA/name).read_bytes())
    monkeypatch.setattr(m,"ROOT",root);monkeypatch.setattr(m,"EXPECTED_UID",root.stat().st_uid)
    context=REAL_SOURCES(manifest_path,metadata)
    assert len(context["manifest"]["members"])==36
    assert sum(len(e["metadata_roi_rows"]) for e in context["manifest"]["members"])==370
    assert sum(len(m._range_plan(context,e)) for e in context["manifest"]["members"])==1045
    assert sum(len(e["compressed_segments_inferred_pending_local_header_validation"])>1 for e in context["manifest"]["members"])==6


@pytest.mark.parametrize("kind",["missing_member","roi_drop","roi_duplicate","part_order","part_url","part_md5","range_join","gate_true"])
def test_altered_manifest_even_with_test_pin_override_cannot_change_source_universe(tmp_path,monkeypatch,kind):
    root=tmp_path/"root";root.mkdir();metadata=root/"inputs";metadata.mkdir()
    for name in m.SOURCE_PINS:(metadata/name).write_bytes((METADATA/name).read_bytes())
    value=json.loads(MANIFEST.read_text())
    if kind=="missing_member":value["members"].pop()
    elif kind=="roi_drop":value["members"][0]["metadata_roi_rows"].pop()
    elif kind=="roi_duplicate":value["members"][0]["metadata_roi_rows"].append(value["members"][0]["metadata_roi_rows"][0])
    elif kind=="part_order":value["source_parts"][0],value["source_parts"][1]=value["source_parts"][1],value["source_parts"][0]
    elif kind=="part_url":value["source_parts"][0]["url"]="https://example.invalid/alternate"
    elif kind=="part_md5":value["source_parts"][0]["provider_md5"]="0"*32
    elif kind=="range_join":value["members"][0]["compressed_segments_inferred_pending_local_header_validation"][0]["start"]+=1
    else:value["scientific_gates"]["assay_qualified"]=True
    path=root/"manifest.json";sha=rewrite(path,value)
    monkeypatch.setattr(m,"ROOT",root);monkeypatch.setattr(m,"EXPECTED_UID",root.stat().st_uid)
    monkeypatch.setattr(m,"MANIFEST_SHA",sha)
    with pytest.raises(ValueError):REAL_SOURCES(path,metadata)


@pytest.mark.parametrize("raw",[b'{"same":1,"same":2}',b'{"x":NaN}',b'{"x":Infinity}',
    b'{"x":1e999}',b'{"x":-1e999}',b'{"nested":{"x":1e999}}',b'{"nested":[0,{"x":-1e999}]}'])
def test_duplicate_or_nonfinite_json_proofs_refused(raw):
    with pytest.raises(ValueError):m._decode(raw)


def test_finite_json_exponents_are_preserved():
    assert m._decode(b'{"x":1e2,"nested":[-1e2,1e-3]}') == {"x":100.0,"nested":[-100.0,0.001]}


def test_native_posix_hard_alarm_terminates_blocking_child_or_refuses_nonnative():
    if os.name != "posix":
        with pytest.raises(RuntimeError,match="POSIX"):
            with REAL_DEADLINE(1):pass
    else:
        code=("import importlib.util,time\n"
              "s=importlib.util.spec_from_file_location('a',"+repr(str(SOURCE))+")\n"
              "m=importlib.util.module_from_spec(s)\ns.loader.exec_module(m)\n"
              "with m._hard_deadline(1):\n    time.sleep(30)\n")
        result=subprocess.run([sys.executable,"-c",code],capture_output=True,text=True,timeout=5)
        assert result.returncode != 0 and "TimeoutError" in result.stderr


def test_cli_has_no_deadline_override_or_raw_query_or_deposited_program_execution():
    source=SOURCE.read_text()
    assert "import sqlite3" not in source and "sqlite3.connect" not in source and "SELECT " not in source
    assert "extractall" not in source and "subprocess" not in source
    assert "--max-seconds" not in source and "--retry" not in source
    assert m.MAX_TRANSFER_SECONDS==900 and m.MAX_VERIFY_SECONDS==m.MAX_ASSEMBLY_SECONDS==600
