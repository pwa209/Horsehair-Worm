"""Private frozen-roster range transfer, native byte verification, and assembly.

No scientific database query, coordinate interpretation, or deposited program is
used. Source qualification, owned Slurm authorization, quota/admission and job
registration belong to the external native wrapper, never this adapter.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import struct
import time
import zlib


ROOT = Path("/scratch/pwa209/horsehair-worm")
EXPECTED_UID = 3164768
MANIFEST_SHA = "92248b2b213fe7db425ee1fbe9c18c0699c7638943f7edd34bcf2081eeb91490"
MAX_RANGE = 8_388_608
MAX_TRANSFER_SECONDS = 900
MAX_VERIFY_SECONDS = 600
MAX_ASSEMBLY_SECONDS = 600
MAX_COMPRESSED = 328_694_564
MAX_RAW = 654_524_416
MAX_REQUESTS = 42
MAX_RANGE_INTENT_JSON = 4096
MAX_RANGE_RESPONSE_JSON = 262_144
SOURCE_PINS = {
    "zenodo-6567039.json": "2514b6e65c2892c69db797263ee545d546a76beee4ac931fc3b1257f45992cce",
    "ethoscope-METADATA.txt": "83230109101cb01e57fb5f0d64b48b1965f07d9f45cf9c41db8105357c3d1616",
    "db_files_with_checksum.txt": "39d6f17957c394d95b3746df1aa4ff75ba15ae4722ffede293ed0c6de9c729c6",
    "zip_files_checksums.csv": "8cb455520bd4382d136010f6ef5fe0507cbcccaa0dd20b6ab60f2b01f6cbdff5",
    "ethoscope-split-zip-index.json": "ffc58aa960a5fdaec167107c4fece8fd7789fb5daad79675c9aa85980dee46b6",
    "ethoscope-split-zip-directory.bin": "c2a550fe715ab0de9bc70499b04f94c747da50845b9725cd96fa130420382528",
    "ethoscope-split-zip-tail.bin": "d80d5a2e9b380bfed81126a67cbdb29bb574a6c5fd298de79d4a2788b7d97385",
}
PROVENANCE = {
    "unchanged_canary_source_sha256": "30a843a2771196a3eec1e202f8e695fd1d41e58b6423a73218ec74b6285ea17c",
    "unchanged_canary_test_sha256": "6a7d31cdcc08674aee39af0361cfe234d1eb669548634daf3458c4dd592f4ab3",
    "new_paths_requiring_independent_qualification": ["multipart ranges", "8MiB range bound",
        "explicit immutable member parameters", "separate transfer and verification",
        "whole36 roster assembly", "native environment checks", "hard wall budgets"],
}
FALSE_FLAGS = {"scientific_analysis_performed": False, "sqlite_or_deposited_code_executed": False,
    "actual_database_schema_verified": False, "assay_eligibility_established": False,
    "per_animal_exposure_timestamp_verified": False, "whole_part_provider_md5_verified": False,
    "original_H1_H2_H3_cleared": False, "original_D_or_E2_cleared": False,
    "biological_transfer_supported": False, "owned_native_authorization_established_by_adapter": False}


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _integer(value, minimum, maximum, label):
    _require(type(value) is int and minimum <= value <= maximum, "Invalid "+label)
    return value


def _digest(value):
    _require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
             "Exact SHA256 required")
    return value


def _encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False)+"\n").encode()


def _decode(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            _require(key not in out, "Duplicate JSON key")
            out[key] = value
        return out
    def constant(value):
        raise ValueError("Nonfinite JSON literal refused")
    def finite_float(value):
        parsed = float(value)
        _require(math.isfinite(parsed), "Nonfinite JSON number refused")
        return parsed
    return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                      parse_constant=constant, parse_float=finite_float)


def _sync_directory(path):
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _confined(path, *, regular=False, nlinks=(1,)):
    path = Path(path)
    _require(path.is_absolute() and ".." not in path.parts, "Absolute personal path required")
    _require(ROOT.is_absolute() and ROOT.exists(), "Personal scratch root missing")
    lexical = ROOT.absolute()
    _require(lexical in path.parents, "Path must be below declared personal scratch")
    root = ROOT.resolve(strict=True)
    resolved = path.resolve(strict=False)
    _require(root in resolved.parents, "Path resolves outside personal scratch")
    current = lexical
    _require(current.stat().st_uid == EXPECTED_UID, "Personal root UID mismatch")
    for part in path.relative_to(lexical).parts:
        current = current/part
        _require(not current.is_symlink(), "Subordinate symlink refused")
        try:
            entry = current.lstat()
        except FileNotFoundError:
            continue
        _require(entry.st_uid == EXPECTED_UID, "Personal path UID mismatch")
    if regular:
        entry = path.lstat()
        _require(stat.S_ISREG(entry.st_mode) and entry.st_nlink in nlinks,
                 "Owned ordinary file with exact link count required")
    return path


def _identity(st):
    # Windows fstat/lstat expose inconsistent creation-time values for a just
    # flushed handle. This affects local metadata/mock qualification only:
    # every production entry point requires POSIX, where full ctime is pinned.
    return (st.st_dev, st.st_ino, st.st_mode, st.st_uid, st.st_nlink,
            st.st_size, st.st_mtime_ns, st.st_ctime_ns if os.name == "posix" else 0)


def _read_file(path, maximum, *, expected_sha=None, retain=False, nlinks=(1,), clock=None):
    path = _confined(path, regular=True, nlinks=nlinks)
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os,"O_BINARY",0))
    chunks, size, digest = [], 0, hashlib.sha256()
    try:
        before = os.fstat(fd)
        _require(stat.S_ISREG(before.st_mode) and before.st_uid == EXPECTED_UID
                 and before.st_nlink in nlinks and 0 <= before.st_size <= maximum,
                 "Owned bounded ordinary file required")
        while True:
            if clock:
                clock.check()
            block = os.read(fd, min(65536, maximum-size+1))
            if not block:
                break
            size += len(block)
            _require(size <= maximum, "File exceeds exact read bound")
            digest.update(block)
            if retain:
                chunks.append(block)
        after = os.fstat(fd)
        _require(_identity(before) == _identity(after) and size == before.st_size,
                 "File changed while reading")
        _require(_identity(path.lstat()) == _identity(after), "File path changed while reading")
    finally:
        os.close(fd)
    result = {"bytes": size, "sha256": digest.hexdigest(), "identity": list(_identity(after))}
    if expected_sha is not None:
        _require(result["sha256"] == _digest(expected_sha), "File SHA256 mismatch")
    return (b"".join(chunks), result) if retain else result


def _json(path, value, *, limit=2_000_000):
    raw = _encoded(value)
    _require(len(raw) <= min(limit,2_000_000), "Receipt output bound exceeded")
    path = _confined(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
                 | getattr(os,"O_BINARY",0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    _sync_directory(path.parent)
    return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def _read_json(path, expected_sha=None, limit=2_000_000, clock=None):
    raw, proof = _read_file(path, limit, expected_sha=expected_sha, retain=True, clock=clock)
    return _decode(raw), proof


class Clock:
    def __init__(self, seconds):
        self.end = time.monotonic()+seconds

    def check(self):
        if time.monotonic() >= self.end:
            raise TimeoutError("Bounded operation deadline exceeded; no automatic replay")


@contextmanager
def _hard_deadline(seconds):
    _integer(seconds, 1, MAX_TRANSFER_SECONDS, "hard deadline")
    if os.name != "posix" or not hasattr(signal, "SIGALRM"):
        raise RuntimeError("Native POSIX hard wall deadline required")
    previous = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    _require(previous_timer == (0.0, 0.0), "Existing process alarm refused; use isolated child")
    def expired(signum, frame):
        raise TimeoutError("Native hard wall deadline exceeded; no automatic replay")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _helper_sha():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _sources(manifest_path, metadata_dir):
    manifest, proof = _read_json(manifest_path, MANIFEST_SHA)
    metadata_dir = _confined(metadata_dir)
    documents, pins = {}, [{"path": str(manifest_path), "sha256": MANIFEST_SHA}]
    for name, expected in SOURCE_PINS.items():
        raw, checked = _read_file(metadata_dir/name, 2_000_000, expected_sha=expected, retain=True)
        documents[name] = raw
        pins.append({"path": str(metadata_dir/name), "sha256": checked["sha256"]})
    _validate_manifest(manifest, documents)
    return {"manifest": manifest, "manifest_path": str(manifest_path),
        "metadata_dir": str(metadata_dir), "manifest_sha256": proof["sha256"],
        "source_pins": pins, "helper_sha256": _helper_sha()}


def _validate_manifest(manifest, documents):
    _require(manifest["scope"] == "complete_Figure01_metadata_only_acquisition_design",
             "Wrong prospective manifest scope")
    _require({Path(x["local_relative_path"]).name:x["sha256"] for x in manifest["source_documents"]} == SOURCE_PINS,
             "Manifest documentation closure mismatch")
    parts = manifest["source_parts"]
    _require(len(parts) == 59, "Complete59 part order required")
    logical = 0
    provider = _decode(documents["zenodo-6567039.json"])
    files = {f["key"]:f for f in provider["files"]}
    _require(provider["id"] == 6567039 and provider["doi"] == "10.5281/zenodo.6567039", "Wrong provider")
    table = list(csv.DictReader(io.StringIO(documents["zip_files_checksums.csv"].decode()), delimiter="\t"))
    part_checks = {row["filename"]:row for row in table}
    for disk, part in enumerate(parts):
        key = f"db_files.z{disk+1:02d}" if disk < 58 else "db_files.zip"
        expected_bytes = 2_097_152_000 if disk < 58 else 2_023_204_779
        url = f"https://zenodo.org/api/records/6567039/files/{key}/content"
        _require(part["disk"] == disk and part["key"] == key and part["bytes"] == expected_bytes
                 and part["url"] == url and part["logical_start"] == logical
                 and part["logical_end_exclusive"] == logical+expected_bytes,
                 "Part geometry/order/URL mismatch")
        f, row = files[key], part_checks[key]
        _require(f["size"] == expected_bytes and f["links"]["self"] == url
                 and f["checksum"] == "md5:"+part["provider_md5"]
                 and row["Local checksum"] == row["uploaded checksum"] == part["provider_md5"]
                 and row["Match"] == "OK", "Provider part binding mismatch")
        logical += expected_bytes
    index = _decode(documents["ethoscope-split-zip-index.json"])
    central = {e["name"]:e for e in index["entries"]}
    raw_checks = {}
    for line in documents["db_files_with_checksum.txt"].decode().splitlines():
        md5, name = line.split("  ", 1)
        name = "plos_path_results/"+name.removeprefix("./")
        _require(name not in raw_checks, "Duplicate member checksum")
        raw_checks[name] = md5
    raw_lines = documents["ethoscope-METADATA.txt"].splitlines(keepends=True)
    metadata = list(csv.DictReader(io.StringIO(documents["ethoscope-METADATA.txt"].decode()), delimiter="\t"))
    expected_rows = {i:r for i,r in enumerate(metadata,2) if r["figure"] == "Figure01"}
    members = manifest["members"]
    _require(len(members) == 36 and len(expected_rows) == 370, "Complete36/370 universe required")
    selected_lines, selected_ids, compressed, raw = set(), [], 0, 0
    for ordinal, member in enumerate(members, 1):
        _require(member["source_ordinal"] == ordinal, "Member ordinal/order mismatch")
        e = member["central_entry"]
        _require(all(e.get(k) == v for k,v in central[e["name"]].items()), "Central entry mismatch")
        _require(e["flags"] == 0 and e["compression"] == 8 and e["version_needed"] == 20,
                 "Unsupported member flags/method/version")
        _integer(e["compressed_bytes"], 1, MAX_COMPRESSED, "compressed size")
        _integer(e["uncompressed_bytes"], 1, MAX_RAW, "raw size")
        _require(raw_checks[e["name"]] == member["raw_provider_md5"], "Provider raw MD5 mismatch")
        _require(re.fullmatch(r"[0-9a-f]{32}", member["raw_provider_md5"]) is not None, "Invalid raw MD5")
        disk, offset = e["disk_start"], e["local_header_offset"]
        start = parts[disk]["logical_start"]+offset+198
        end = start+e["compressed_bytes"]
        _require(offset+198 <= parts[disk]["bytes"]
                 and member["compressed_logical_start_inferred"] == start
                 and member["compressed_logical_end_exclusive_inferred"] == end
                 and member["successor_central_entry"]["logical_local_header_offset"] == end,
                 "Header/successor geometry mismatch")
        expected_segments = _segments(parts, start, e["compressed_bytes"])
        actual = member["compressed_segments_inferred_pending_local_header_validation"]
        _require(actual == expected_segments, "Physical part joins mismatch")
        fields = e["name"].split("/")
        expected_id = fields[3][:10]+"/"+fields[2]
        _require(member["source_id"] == expected_id, "Source date-machine identity mismatch")
        selected_ids.append(expected_id)
        for row in member["metadata_roi_rows"]:
            line = row["source_line_1_based"]
            _require(type(line) is int and line in expected_rows and line not in selected_lines
                     and row["source_metadata"] == expected_rows[line]
                     and row["source_row_sha256"] == hashlib.sha256(raw_lines[line-1]).hexdigest()
                     and row["exclusion_applied"] is False and row["assay_eligible"] is False,
                     "ROI omission/duplication/metadata/exclusion mismatch")
            value = row["source_metadata"]
            _require(value["date"]+"/"+value["machine_name"] == expected_id, "ROI mapped to wrong source")
            selected_lines.add(line)
        compressed += e["compressed_bytes"]
        raw += e["uncompressed_bytes"]
    _require(selected_lines == set(expected_rows) and selected_ids == sorted(set(selected_ids)),
             "Incomplete or duplicated full metadata roster")
    _require((compressed,raw) == (7_970_156_585,15_499_844_608), "Cohort byte totals changed")
    _require(all(v is False for v in manifest["scientific_gates"].values()), "Scientific gates must stay false")


def _segments(parts, start, count):
    result, remaining, current = [], count, start
    for part in parts:
        if not remaining:
            break
        if not part["logical_start"] <= current < part["logical_end_exclusive"]:
            continue
        length = min(remaining, part["logical_end_exclusive"]-current)
        local = current-part["logical_start"]
        result.append({"segment_ordinal": len(result), "disk": part["disk"], "part_key": part["key"],
            "url": part["url"], "part_provider_md5": part["provider_md5"], "part_bytes": part["bytes"],
            "start": local, "end_inclusive": local+length-1, "bytes": length,
            "logical_start": current, "logical_end_exclusive": current+length,
            "max_request_bytes": MAX_RANGE, "request_count": (length+MAX_RANGE-1)//MAX_RANGE})
        current += length
        remaining -= length
    _require(remaining == 0 and count > 0, "Invalid split interval")
    return result


def _member(context, ordinal):
    _integer(ordinal, 1, 36, "member ordinal")
    member = context["manifest"]["members"][ordinal-1]
    _require(member["source_ordinal"] == ordinal, "Wrong immutable member ordinal")
    return member


def _again(context):
    fresh = _sources(Path(context["manifest_path"]), Path(context["metadata_dir"]))
    _require(fresh == context, "Source closure changed during operation")


def _fresh_case(path):
    path = _confined(path)
    maintenance = ROOT/"maintenance"
    _require(maintenance in path.parents, "Fresh case must be strictly below personal maintenance")
    _require(path.parent.is_dir(), "Existing owned parent required; no implicit recursive creation")
    path.mkdir(mode=0o700, exist_ok=False)
    _sync_directory(path.parent)
    return path


def _start(context, member, case, phase):
    return {"schema": "ethoscope_figure01_"+phase+"_v1", "case": str(case),
        "source_ordinal": member["source_ordinal"], "source_id": member["source_id"],
        "manifest_sha256": context["manifest_sha256"], "source_pins": context["source_pins"],
        "helper_sha256": context["helper_sha256"], "started_unix": time.time(),
        "no_automatic_retry_resume_or_same_case_replay": True, **FALSE_FLAGS}


def _range_plan(context, member):
    e = member["central_entry"]
    part = context["manifest"]["source_parts"][e["disk_start"]]
    plan = [(part, e["local_header_offset"], 30, "local-header"),
            (part, e["local_header_offset"]+30, 168, "local-name-extra")]
    for segment in member["compressed_segments_inferred_pending_local_header_validation"]:
        part = context["manifest"]["source_parts"][segment["disk"]]
        offset, remaining = segment["start"], segment["bytes"]
        while remaining:
            count = min(MAX_RANGE, remaining)
            plan.append((part, offset, count, "compressed-data"))
            offset += count
            remaining -= count
    _require(2 < len(plan) <= MAX_REQUESTS, "Frozen request count bound exceeded")
    return plan


def _range_intent(context, member, part, start, count, role, ordinal):
    _integer(start, 0, part["bytes"]-1, "physical start")
    _integer(count, 1, MAX_RANGE, "range count")
    _require(start+count <= part["bytes"] and count < part["bytes"], "Part overflow/whole-part request refused")
    _require(role in {"local-header", "local-name-extra", "compressed-data"}, "Unknown range role")
    return {"ordinal": ordinal, "source_ordinal": member["source_ordinal"],
        "source_id": member["source_id"], "manifest_sha256": context["manifest_sha256"],
        "part_disk": part["disk"], "part_key": part["key"], "part_provider_md5": part["provider_md5"],
        "part_md5_verified": False, "url": part["url"], "part_bytes": part["bytes"],
        "start": start, "end_inclusive": start+count-1, "bytes": count, "role": role,
        "no_retry": True, "whole_part_request": False}


class RangeReader:
    def __init__(self, context, member, case, session, clock):
        self.context, self.member, self.case = context, member, case
        self.session, self.clock = session, clock
        self.folder = case/"ranges"
        self.folder.mkdir(mode=0o700)
        self.records, self.last_start = [], None

    def read(self, part, start, count, role):
        ordinal = len(self.records)+1
        _require(ordinal <= MAX_REQUESTS, "Too many range requests")
        intent = _range_intent(self.context,self.member,part,start,count,role,ordinal)
        stem = f"{ordinal:04d}-{role}"
        intent_path = self.folder/(stem+"-intent.json")
        response_path = self.folder/(stem+"-response.json")
        target = self.folder/(stem+".bin")
        intent_proof = _json(intent_path,intent,limit=MAX_RANGE_INTENT_JSON)
        response_info = {"status": "not_opened", "bytes_preserved": 0, "verified": False}
        try:
            self.clock.check()
            if self.last_start is not None:
                wait = 1-(time.monotonic()-self.last_start)
                if wait > 0:
                    time.sleep(wait)
                self.clock.check()
            self.last_start = time.monotonic()
            with self.session.get(part["url"], headers={"Range": f"bytes={start}-{start+count-1}",
                    "Accept-Encoding": "identity", "User-Agent": "Horsehair-Worm bounded public member"},
                    allow_redirects=False, stream=True, timeout=(15,90)) as response:
                headers = {}
                overflow = False
                for key in ("Content-Range","Content-Length","Content-Encoding","ETag","Last-Modified",
                            "Location","Retry-After","X-RateLimit-Limit","X-RateLimit-Remaining"):
                    value = response.headers.get(key)
                    _require(value is None or isinstance(value,str), "HTTP header text required")
                    if value is not None and len(value) > 4096:
                        overflow = True
                        value = value[:4097]
                    headers[key] = value
                response_info.update(status=response.status_code, headers=headers, header_overflow=overflow)
                self.clock.check()
                _require(not overflow, "Oversized HTTP header refused")
                _require(len(_encoded(response_info)) <= 60_000,
                         "Encoded HTTP header proof too large; refuse before body")
                _response_headers(response_info,part,start,count)
                with target.open("xb") as stream:
                    try:
                        for chunk in response.iter_content(65536):
                            self.clock.check()
                            if not chunk:
                                continue
                            _require(isinstance(chunk,bytes), "HTTP byte chunk required")
                            remaining = count+1-response_info["bytes_preserved"]
                            piece = chunk[:remaining]
                            stream.write(piece)
                            response_info["bytes_preserved"] += len(piece)
                            _require(len(chunk) <= remaining and response_info["bytes_preserved"] <= count,
                                     "Range body exceeded exact bound")
                    finally:
                        stream.flush()
                        os.fsync(stream.fileno())
                _require(response_info["bytes_preserved"] == count, "Truncated range body")
                body = _read_file(target,count,clock=self.clock)
                response_info.update(sha256=body["sha256"],verified=True)
        except BaseException as error:
            response_info.update(verified=False,error_type=type(error).__name__,error=str(error)[:4096])
            raise
        finally:
            response_proof = _json(response_path,response_info,limit=MAX_RANGE_RESPONSE_JSON)
        record = {"ordinal": ordinal,"role":role,"intent":intent_proof,"response":response_proof,
            "body":{"path":str(target),"bytes":count,"sha256":response_info["sha256"]}}
        self.records.append(record)
        return target


def _response_headers(response,part,start,count):
    _require(type(response["status"]) is int and response["status"] == 206,
             "Exact HTTP206 required; non-range body not consumed")
    headers = response["headers"]
    encoding = headers.get("Content-Encoding")
    _require(encoding is None or (isinstance(encoding,str) and encoding.lower() == "identity"),
             "Encoded range refused")
    _require(headers.get("Content-Range") == f"bytes {start}-{start+count-1}/{part['bytes']}"
             and headers.get("Content-Length") == str(count), "Exact Content-Range/Length required")


def _header(member, parts, fixed, name_extra):
    _require(len(fixed) == 30, "Exact local fixed header required")
    magic,version,flags,method,mtime,mdate,crc,compressed,raw,name_n,extra_n = struct.unpack("<4s5H3I2H",fixed)
    e = member["central_entry"]
    _require(magic == b"PK\x03\x04" and version == 20 and flags == 0 and method == 8,
             "Unsupported local signature/version/flags/method")
    _require((f"{crc:08x}",compressed,raw) == (e["crc32"],e["compressed_bytes"],e["uncompressed_bytes"]),
             "Local CRC/size disagreement")
    _require(name_n == 140 and extra_n == 28 and len(name_extra) == name_n+extra_n,
             "Local filename/extra length disagreement")
    _require(name_extra[:name_n] == e["name"].encode("ascii"), "Local filename disagreement")
    extra, position = name_extra[name_n:], 0
    while position < len(extra):
        _require(position+4 <= len(extra), "Truncated local extra header")
        kind,length = struct.unpack_from("<HH",extra,position)
        position += 4
        _require(kind != 1 and position+length <= len(extra), "ZIP64/truncated local extra refused")
        position += length
    data_start = parts[e["disk_start"]]["logical_start"]+e["local_header_offset"]+30+name_n+extra_n
    _require(data_start == member["compressed_logical_start_inferred"]
             and data_start+compressed == member["successor_central_entry"]["logical_local_header_offset"],
             "Exact descriptor-free successor boundary disagreement")
    return {"flags":flags,"compression":method,"version_needed":version,"crc32":f"{crc:08x}",
        "compressed_bytes":compressed,"raw_bytes":raw,"filename_bytes":name_n,"local_extra_bytes":extra_n,
        "logical_data_start":data_start,"logical_data_end_exclusive":data_start+compressed,
        "descriptor_absent_by_flag_and_exact_successor":True,"local_header_verified":True}


def _session():
    import requests
    session = requests.Session()
    session.trust_env = False
    session.mount("https://",requests.adapters.HTTPAdapter(max_retries=0))
    return session


def acquire_one(manifest_path, metadata_dir, ordinal, case, *, session=None):
    with _hard_deadline(MAX_TRANSFER_SECONDS):
        return _acquire_one(manifest_path,metadata_dir,ordinal,case,session=session)


def _acquire_one(manifest_path,metadata_dir,ordinal,case,*,session=None):
    clock = Clock(MAX_TRANSFER_SECONDS)
    context = _sources(Path(manifest_path),Path(metadata_dir))
    member = _member(context,ordinal)
    plan = _range_plan(context,member)
    case = _fresh_case(Path(case))
    start = _start(context,member,case,"transfer")
    _json(case/"transfer-start.json",start)
    own_session = session is None
    try:
        session = session if session is not None else _session()
        reader = RangeReader(context,member,case,session,clock)
        first = reader.read(*plan[0])
        # Parse fixed values before asking for the frozen168 B continuation.
        fixed, _ = _read_file(first,30,retain=True,clock=clock)
        values = struct.unpack("<4s5H3I2H",fixed)
        _require(values[0] == b"PK\x03\x04" and values[1] == 20 and values[2] == 0
                 and values[3] == 8 and values[-2:] == (140,28),
                 "Unsupported local fixed header refused before continuation")
        second = reader.read(*plan[1])
        name_extra, _ = _read_file(second,168,retain=True,clock=clock)
        header = _header(member,context["manifest"]["source_parts"],fixed,name_extra)
        header_proof = _json(case/"verified-local-header.json",header)
        for request in plan[2:]:
            reader.read(*request)
        _again(context)
        clock.check()
        result = {**start,"status":"compressed_member_acquired_pending_native_verification",
            "finished_unix":time.time(),"ranges":reader.records,"verified_local_header":header_proof,
            "compressed_bytes":member["central_entry"]["compressed_bytes"],
            "raw_member_integrity_verified":False,"full_cohort_acquired":False}
        _json(case/"transfer-result.json",result)
        return result
    except BaseException as error:
        _json(case/"transfer-failure.json",{**start,"status":"failed_no_automatic_replay",
            "error_type":type(error).__name__,"error":str(error)[:4096],"finished_unix":time.time()})
        raise
    finally:
        if own_session and session is not None:
            session.close()


def _native_environment():
    job = os.environ.get("SLURM_JOB_ID", "")
    _require(re.fullmatch(r"[1-9][0-9]{0,11}",job) is not None, "Native scalar Slurm job required")
    _require(os.environ.get("SLURM_CPUS_PER_TASK") == "1"
             and os.environ.get("SLURM_NTASKS","1") == "1", "Exactly one CPU/task required")
    _require(not os.environ.get("SLURM_ARRAY_TASK_ID"), "Scalar native verification required")
    if hasattr(os,"getuid"):
        _require(os.getuid() == EXPECTED_UID, "Native effective UID mismatch")
    if hasattr(os,"geteuid"):
        _require(os.geteuid() == EXPECTED_UID, "Native effective UID mismatch")
    return {"job_id":job,"cpus_per_task":1,"tasks":1,"uid":EXPECTED_UID,
        "environment_only_not_owner_authorization":True,"root_native_ownership_join_required":True}


def _transfer(context,member,receipt_path,receipt_sha,clock):
    receipt_path = _confined(receipt_path,regular=True)
    result, result_proof = _read_json(receipt_path,_digest(receipt_sha),clock=clock)
    case = receipt_path.parent
    _require(receipt_path.name == "transfer-result.json" and result["case"] == str(case)
             and not (case/"transfer-failure.json").exists(), "Closed successful transfer case required")
    _require(result["schema"] == "ethoscope_figure01_transfer_v1"
             and result["status"] == "compressed_member_acquired_pending_native_verification"
             and result["manifest_sha256"] == context["manifest_sha256"]
             and result["helper_sha256"] == context["helper_sha256"]
             and result["source_ordinal"] == member["source_ordinal"]
             and result["source_id"] == member["source_id"]
             and result["source_pins"] == context["source_pins"]
             and all(result.get(k) is False for k in FALSE_FLAGS), "Wrong transfer source/closure/scope")
    plan = _range_plan(context,member)
    _require(len(result["ranges"]) == len(plan), "Missing/extra transfer ranges")
    payloads, header_bytes = [], []
    for ordinal, ((part,start,count,role),record) in enumerate(zip(plan,result["ranges"]),1):
        stem = f"{ordinal:04d}-{role}"
        folder = case/"ranges"
        paths = {"intent":folder/(stem+"-intent.json"),"response":folder/(stem+"-response.json"),
                 "body":folder/(stem+".bin")}
        _require(record["ordinal"] == ordinal and record["role"] == role
                 and all(record[key]["path"] == str(path) for key,path in paths.items()), "Range proof path/order mismatch")
        intent,_ = _read_json(paths["intent"],record["intent"]["sha256"],limit=65536,clock=clock)
        _require(intent == _range_intent(context,member,part,start,count,role,ordinal), "Exact range intent mismatch")
        response,_ = _read_json(paths["response"],record["response"]["sha256"],limit=65536,clock=clock)
        _response_headers(response,part,start,count)
        _require(response["verified"] is True and response["bytes_preserved"] == count
                 and record["body"]["bytes"] == count and response.get("header_overflow") is False,
                 "Positive complete range response required")
        body = _read_file(paths["body"],count,expected_sha=record["body"]["sha256"],clock=clock)
        _require(body["bytes"] == count and body["sha256"] == response["sha256"], "Range byte proof mismatch")
        if ordinal <= 2:
            raw,_ = _read_file(paths["body"],count,retain=True,clock=clock)
            header_bytes.append(raw)
        else:
            payloads.append(paths["body"])
    header = _header(member,context["manifest"]["source_parts"],*header_bytes)
    header_proof = result["verified_local_header"]
    _require(header_proof["path"] == str(case/"verified-local-header.json"), "Header receipt locator mismatch")
    saved,_ = _read_json(Path(header_proof["path"]),header_proof["sha256"],limit=65536,clock=clock)
    _require(saved == header, "Verified header receipt changed")
    return result,payloads,result_proof


def _inflate(member,chunks,candidate,clock):
    e = member["central_entry"]
    decoder = zlib.decompressobj(-15)
    hashes = {name:hashlib.new(name) for name in ("md5","sha256")}
    crc = compressed = raw_bytes = 0
    candidate = _confined(candidate)
    with candidate.open("xb") as output:
        try:
            for path in chunks:
                path = _confined(path,regular=True)
                fd = os.open(path,os.O_RDONLY | getattr(os,"O_NOFOLLOW",0) | getattr(os,"O_BINARY",0))
                try:
                    before = os.fstat(fd)
                    _require(before.st_uid == EXPECTED_UID and before.st_nlink == 1
                             and stat.S_ISREG(before.st_mode) and before.st_size <= MAX_RANGE,
                             "Owned exact bounded compressed chunk required")
                    while True:
                        clock.check()
                        block = os.read(fd,65536)
                        if not block:
                            break
                        compressed += len(block)
                        _require(compressed <= e["compressed_bytes"] and not decoder.eof,
                                 "Compressed extra bytes refused")
                        pending = block
                        while True:
                            clock.check()
                            limit = min(65536,e["uncompressed_bytes"]-raw_bytes+1)
                            raw = decoder.decompress(pending,limit)
                            remaining = decoder.unconsumed_tail
                            raw_bytes += len(raw)
                            _require(raw_bytes <= e["uncompressed_bytes"], "Deflate raw bound exceeded")
                            output.write(raw)
                            crc = zlib.crc32(raw,crc)
                            for digest in hashes.values():
                                digest.update(raw)
                            _require(not decoder.unused_data, "Trailing deflate payload refused")
                            if remaining:
                                _require(bool(raw) or len(remaining) < len(pending),
                                         "Deflate decoder made no progress")
                                pending = remaining
                                continue
                            if decoder.eof or not raw:
                                break
                            # A max-length return can consume all compressed
                            # input yet retain output internally. Drain with
                            # the same +1 raw bound; zero output waits for the
                            # next source block, and EOF remains mandatory.
                            pending = b""
                    _require(_identity(before) == _identity(os.fstat(fd))
                             and _identity(before) == _identity(path.lstat()), "Compressed chunk changed while inflating")
                finally:
                    os.close(fd)
        finally:
            output.flush()
            os.fsync(output.fileno())
    _require(decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail,
             "Incomplete/extra deflate stream")
    _require((compressed,raw_bytes,f"{crc:08x}",hashes["md5"].hexdigest()) ==
             (e["compressed_bytes"],e["uncompressed_bytes"],e["crc32"],member["raw_provider_md5"]),
             "Complete compressed/raw size,CRC,provider-MD5 verification failed")
    return {"compressed_bytes":compressed,"raw_bytes":raw_bytes,"crc32":f"{crc:08x}",
        "provider_md5":hashes["md5"].hexdigest(),"sha256":hashes["sha256"].hexdigest()}


def verify_one(manifest_path,metadata_dir,ordinal,transfer_receipt,transfer_sha,case):
    with _hard_deadline(MAX_VERIFY_SECONDS):
        return _verify_one(manifest_path,metadata_dir,ordinal,transfer_receipt,transfer_sha,case)


def _verify_one(manifest_path,metadata_dir,ordinal,transfer_receipt,transfer_sha,case):
    clock = Clock(MAX_VERIFY_SECONDS)
    native = _native_environment()
    context = _sources(Path(manifest_path),Path(metadata_dir))
    member = _member(context,ordinal)
    case = _fresh_case(Path(case))
    start = {**_start(context,member,case,"verification"),"native_environment":native,
        "transfer_receipt":{"path":str(transfer_receipt),"sha256":_digest(transfer_sha)}}
    _json(case/"verification-start.json",start)
    candidate,final = case/"verified-db-candidate.partial",case/Path(member["central_entry"]["name"]).name
    promoted = False
    try:
        transfer,chunks,_ = _transfer(context,member,Path(transfer_receipt),transfer_sha,clock)
        raw = _inflate(member,chunks,candidate,clock)
        _again(context)
        # Revalidate all compressed proofs after decoding; in-memory paths do
        # not become authority if a producer or another writer changes them.
        _transfer(context,member,Path(transfer_receipt),transfer_sha,clock)
        proof = _read_file(candidate,raw["raw_bytes"],expected_sha=raw["sha256"],clock=clock)
        _require(proof["bytes"] == raw["raw_bytes"], "Verified candidate size changed")
        raw_proof = _json(case/"verified-full-member.json",raw)
        _json(case/"promotion-intent.json",{"final":str(final),"candidate":str(candidate),**raw})
        clock.check()
        os.link(candidate,final,follow_symlinks=False)
        promoted = True
        _sync_directory(case)
        linked = _read_file(final,raw["raw_bytes"],expected_sha=raw["sha256"],nlinks=(2,),clock=clock)
        current = _confined(candidate,regular=True,nlinks=(2,)).lstat()
        _require((current.st_dev,current.st_ino) == tuple(linked["identity"][:2]) == tuple(proof["identity"][:2]),
                 "Promotion inode changed; preserve both paths for review")
        candidate.unlink()  # Its exact verified publication link only; no source-data cleanup.
        _sync_directory(case)
        _read_file(final,raw["raw_bytes"],expected_sha=raw["sha256"],clock=clock)
        _transfer(context,member,Path(transfer_receipt),transfer_sha,clock)
        _again(context)
        clock.check()
        result = {**start,"status":"provider_verified_member_acquired","finished_unix":time.time(),
            "database":str(final),"verification":raw,"final_promoted":True,"full_cohort_acquired":False,
            "verified_full_member":raw_proof,
            "compressed_sources_and_all_failure_evidence_retained":True}
        _json(case/"verification-result.json",result)
        return result
    except BaseException as error:
        _json(case/"verification-failure.json",{**start,"status":"failed_no_automatic_replay",
            "finished_unix":time.time(),"error_type":type(error).__name__,"error":str(error)[:4096],
            "final_promoted":promoted,"final_path_exists":final.exists() or final.is_symlink()})
        raise


def assemble_cohort(manifest_path,metadata_dir,receipt_index,index_sha,case):
    with _hard_deadline(MAX_ASSEMBLY_SECONDS):
        return _assemble_cohort(manifest_path,metadata_dir,receipt_index,index_sha,case)


def _assemble_cohort(manifest_path,metadata_dir,receipt_index,index_sha,case):
    clock = Clock(MAX_ASSEMBLY_SECONDS)
    native = _native_environment()
    context = _sources(Path(manifest_path),Path(metadata_dir))
    index,index_proof = _read_json(Path(receipt_index),_digest(index_sha),clock=clock)
    _require(index["manifest_sha256"] == context["manifest_sha256"] and len(index["receipts"]) == 36,
             "Exactly36 receipts for same immutable manifest required")
    case = _fresh_case(Path(case))
    start = {"schema":"ethoscope_figure01_assembly_v1","case":str(case),"manifest_sha256":context["manifest_sha256"],
        "helper_sha256":context["helper_sha256"],"source_pins":context["source_pins"],"started_unix":time.time(),
        "native_environment":native,"index":index_proof,**FALSE_FLAGS}
    _json(case/"assembly-start.json",start)
    try:
        receipts,seen,paths = [],set(),set()
        for row in index["receipts"]:
            path = _confined(Path(row["path"]),regular=True)
            result,proof = _read_json(path,_digest(row["sha256"]),clock=clock)
            ordinal = _integer(result["source_ordinal"],1,36,"receipt member ordinal")
            _require(ordinal not in seen and str(path) not in paths, "Duplicate member receipt")
            member = _member(context,ordinal)
            _require(path.name == "verification-result.json" and result["case"] == str(path.parent)
                     and not (path.parent/"verification-failure.json").exists()
                     and not (path.parent/"verified-db-candidate.partial").exists(), "Closed positive member case required")
            _require(result["schema"] == "ethoscope_figure01_verification_v1"
                     and result["status"] == "provider_verified_member_acquired" and result["final_promoted"] is True
                     and result["source_id"] == member["source_id"]
                     and result["manifest_sha256"] == context["manifest_sha256"]
                     and result["helper_sha256"] == context["helper_sha256"]
                     and result["source_pins"] == context["source_pins"]
                     and result["native_environment"]["cpus_per_task"] == 1
                     and result["native_environment"]["tasks"] == 1
                     and result["native_environment"]["uid"] == EXPECTED_UID
                     and re.fullmatch(r"[1-9][0-9]{0,11}",result["native_environment"]["job_id"]) is not None
                     and result["native_environment"]["environment_only_not_owner_authorization"] is True
                     and result["native_environment"]["root_native_ownership_join_required"] is True
                     and all(result.get(k) is False for k in FALSE_FLAGS), "Wrong/unqualified member receipt")
            full_proof = result["verified_full_member"]
            _require(full_proof["path"] == str(path.parent/"verified-full-member.json"), "Full member receipt locator mismatch")
            full,_ = _read_json(Path(full_proof["path"]),full_proof["sha256"],limit=65536,clock=clock)
            _require(full == result["verification"], "Closed full member proof mismatch")
            expected_path = path.parent/Path(member["central_entry"]["name"]).name
            _require(result["database"] == str(expected_path), "Verified DB locator mismatch")
            raw = _read_file(expected_path,member["central_entry"]["uncompressed_bytes"],
                expected_sha=result["verification"]["sha256"],clock=clock)
            expected = member["central_entry"]
            _require(raw["bytes"] == expected["uncompressed_bytes"]
                     and result["verification"]["raw_bytes"] == expected["uncompressed_bytes"]
                     and result["verification"]["compressed_bytes"] == expected["compressed_bytes"]
                     and result["verification"]["crc32"] == expected["crc32"]
                     and result["verification"]["provider_md5"] == member["raw_provider_md5"],
                     "Full member raw size/checksum proof mismatch")
            receipts.append({"source_ordinal":ordinal,"source_id":member["source_id"],"receipt":proof,
                "database":str(expected_path),"verification":result["verification"],
                "native_environment":result["native_environment"]})
            seen.add(ordinal)
            paths.add(str(path))
        _require(seen == set(range(1,37)), "Complete source set1..36 required")
        _again(context)
        _read_file(Path(receipt_index),2_000_000,expected_sha=index_sha,clock=clock)
        clock.check()
        result = {**start,"status":"all36_provider_verified_members_present_native_accounting_join_pending",
            "finished_unix":time.time(),"members":sorted(receipts,key=lambda r:r["source_ordinal"]),
            "provider_verified_member_count":36,"all370_metadata_rows_preserved":True,
            "other_obs_rows_retained":8,"root_native_ownership_and_qualification_join_required":True,
            "scientific_study_complete":False}
        _json(case/"assembly-result.json",result)
        return result
    except BaseException as error:
        _json(case/"assembly-failure.json",{**start,"status":"incomplete_cohort_no_automatic_replay",
            "finished_unix":time.time(),"error_type":type(error).__name__,"error":str(error)[:4096]})
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode",choices=("transfer","verify","assemble"))
    parser.add_argument("--manifest",required=True,type=Path)
    parser.add_argument("--metadata-dir",required=True,type=Path)
    parser.add_argument("--case",required=True,type=Path)
    parser.add_argument("--member",type=int)
    parser.add_argument("--transfer-receipt",type=Path)
    parser.add_argument("--transfer-sha256")
    parser.add_argument("--receipt-index",type=Path)
    parser.add_argument("--index-sha256")
    args = parser.parse_args(argv)
    if args.mode in ("transfer","verify"):
        if args.member is None:
            parser.error("--member1..36 required")
    if args.mode == "transfer":
        _require(args.transfer_receipt is None and args.transfer_sha256 is None
                 and args.receipt_index is None and args.index_sha256 is None,"Unexpected mode inputs")
        result = acquire_one(args.manifest,args.metadata_dir,args.member,args.case)
    elif args.mode == "verify":
        _require(args.transfer_receipt is not None and args.transfer_sha256 is not None,"Exact transfer receipt/hash required")
        _require(args.receipt_index is None and args.index_sha256 is None,"Unexpected mode inputs")
        result = verify_one(args.manifest,args.metadata_dir,args.member,args.transfer_receipt,args.transfer_sha256,args.case)
    else:
        _require(args.receipt_index is not None and args.index_sha256 is not None,"Exact receipt index/hash required")
        _require(args.member is None and args.transfer_receipt is None and args.transfer_sha256 is None,"Unexpected mode inputs")
        result = assemble_cohort(args.manifest,args.metadata_dir,args.receipt_index,args.index_sha256,args.case)
    print(json.dumps({"status":result["status"],"case":str(args.case),"manifest_sha256":MANIFEST_SHA}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
