"""Metadata and exact split geometry only; no network or scientific raw data."""
from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import struct

import pytest


SOURCE = Path(__file__).with_name("build-ethoscope-figure01-manifest-20261022.py")
SPEC = importlib.util.spec_from_file_location("figure01_manifest_design", SOURCE)
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


@pytest.fixture(scope="module")
def documents():
    return {name: (m.BASE/name).read_bytes() for name in m.PINS}


@pytest.fixture(scope="module")
def manifest(documents):
    assert all(m.sha(raw) == m.PINS[name] for name, raw in documents.items())
    return m.build(documents)


def test_complete_roster_keeps_all_flags_and_every_original_metadata_line(manifest, documents):
    members = manifest["members"]
    rows = [r for e in members for r in e["metadata_roi_rows"]]
    assert len(members) == 36 and len(rows) == 370
    assert len({r["source_line_1_based"] for r in rows}) == 370
    assert manifest["summary"]["obs_literal_counts"] == {"ok": 362, "no_fly": 1, "ethoscope_killing": 7}
    assert all(r["exclusion_applied"] is False and r["assay_eligible"] is False for r in rows)
    original = documents["ethoscope-METADATA.txt"].splitlines(keepends=True)
    for r in rows:
        assert m.sha(original[r["source_line_1_based"]-1]) == r["source_row_sha256"]


def test_exact_totals_are_full_cohort_not_canary_or_partial_subset(manifest):
    assert manifest["summary"]["compressed_bytes"] == 7_970_156_585
    assert manifest["summary"]["raw_bytes"] == 15_499_844_608
    assert len(manifest["summary"]["split_spanning_members"]) == 6
    assert manifest["summary"]["touched_part_count"] == 24
    assert manifest["summary"]["proposed_request_count_including_72_header_requests"] == 1045
    assert manifest["summary"]["requests_if_existing_1MiB_range_limit_were_retained"] == 7694
    assert all("2022-04-22_11-21-12" not in e["central_entry"]["name"] for e in manifest["members"])


def test_no_missing_or_overlapping_compressed_byte_across_all_six_joins(manifest):
    for e in manifest["members"]:
        segments = e["compressed_segments_inferred_pending_local_header_validation"]
        assert sum(s["bytes"] for s in segments) == e["central_entry"]["compressed_bytes"]
        assert segments[0]["logical_start"] == e["compressed_logical_start_inferred"]
        assert segments[-1]["logical_end_exclusive"] == e["successor_central_entry"]["logical_local_header_offset"]
        assert all(a["logical_end_exclusive"] == b["logical_start"] for a,b in zip(segments, segments[1:]))
        assert all(s["end_inclusive"] < s["part_bytes"] and s["bytes"] < s["part_bytes"] for s in segments)


def test_global_zip64_end_record_is_not_per_member_zip64(manifest):
    assert manifest["archive_end_layout"]["global_zip64_end_record_present"] is True
    assert manifest["archive_end_layout"]["per_member_ZIP64_supported"] is False
    assert all(e["central_entry"]["version_needed"] == 20 for e in manifest["members"])
    assert all(x["id"] != 1 for e in manifest["members"] for x in e["central_entry"]["extra_fields"])
    assert all(e["local_header_expectation"]["inferred_extra_bytes"] == 28 for e in manifest["members"])
    assert all(len(bytes.fromhex(e["central_entry"]["extra_hex"])) == 24 for e in manifest["members"])


def test_design_does_not_claim_runtime_header_schema_assay_or_scientific_clearance(manifest):
    assert manifest["raw_scientific_data_read"] is False
    assert manifest["download_adapter_created_or_executed"] is False
    assert manifest["author_contact_used"] is False
    assert all(value is False for value in manifest["scientific_gates"].values())
    assert all(e["acquired_by_this_design"] is False and e["database_schema_verified"] is False for e in manifest["members"])
    assert all("NOT_read_or_verified" in e["local_header_expectation"]["status"] for e in manifest["members"])
    assert all(p["whole_part_md5_verified_by_this_design"] is False for p in manifest["source_parts"])


def test_peak_reservation_covers_preserved_ranges_and_all_raw_files(manifest):
    a = manifest["proposed_admission_not_yet_qualified"]
    assert a["whole_cohort_requested_byte_reservation"] > a["whole_cohort_single_attempt_peak_retained_bytes_floor"]
    assert a["whole_cohort_requested_file_reservation"] > a["range_evidence_files_at_3_per_request"] + 36*a["fixed_file_reservation_per_member"]
    assert a["network_concurrency"] == a["native_verification_cpus"] == 1
    assert a["compressed_or_raw_source_unlink_authorized"] is False
    assert a["actual_throughput_or_completion_time_benchmarked"] is False


def toy_parts():
    return [{"disk": i, "key": f"part{i}", "bytes": 10, "url": f"https://example.invalid/part{i}",
             "provider_md5": "a"*32, "logical_start": i*10, "logical_end_exclusive": (i+1)*10}
            for i in range(3)]


@pytest.mark.parametrize("start,count,expected", [(0,1,[(0,0,1)]), (9,1,[(0,9,1)]),
    (10,10,[(1,0,10)]), (9,2,[(0,9,1),(1,0,1)]), (4,23,[(0,4,6),(1,0,10),(2,0,7)]),
    (20,10,[(2,0,10)])])
def test_split_exact_boundary_geometry(start, count, expected):
    actual = m.split_interval(start, count, toy_parts())
    assert [(s["disk"], s["start"], s["bytes"]) for s in actual] == expected


@pytest.mark.parametrize("start,count", [(-1,1),(0,0),(0,-1),(29,2),(30,1),(True,1),(0,False),(0,1.0)])
def test_invalid_interval_is_refused(start, count):
    with pytest.raises(ValueError):
        m.split_interval(start, count, toy_parts())


@pytest.mark.parametrize("name", ["../x", "plos_path_results/../x", "/plos_path_results/x", "plos_path_results//x", "plos_path_results/a\\x", "plos_path_results/a\x00x"])
def test_unsafe_member_name_refused(name):
    with pytest.raises(ValueError):
        m.safe_member(name)


@pytest.mark.parametrize("extra", [b"\x55", struct.pack("<HH",0x5455,5)+b"x", struct.pack("<HH",1,0)])
def test_malformed_or_member_zip64_extra_refused(extra):
    with pytest.raises(ValueError):
        m.extra_fields(extra)


@pytest.mark.parametrize("mutation", ["provider_part_md5", "index_crc", "global_zip64_end", "missing_obs_row"])
def test_source_disagreement_cannot_produce_partial_valid_manifest(documents, mutation):
    docs = deepcopy(documents)
    if mutation == "provider_part_md5":
        record = json.loads(docs["zenodo-6567039.json"])
        next(f for f in record["files"] if f["key"] == "db_files.z02")["checksum"] = "md5:"+"0"*32
        docs["zenodo-6567039.json"] = json.dumps(record).encode()
    elif mutation == "index_crc":
        index = json.loads(docs["ethoscope-split-zip-index.json"])
        next(e for e in index["entries"] if e["name"].endswith(".db"))["crc32"] = "00000000"
        docs["ethoscope-split-zip-index.json"] = json.dumps(index).encode()
    elif mutation == "global_zip64_end":
        tail = bytearray(docs["ethoscope-split-zip-tail.bin"])
        tail[-98] ^= 1
        docs["ethoscope-split-zip-tail.bin"] = bytes(tail)
        index = json.loads(docs["ethoscope-split-zip-index.json"])
        index["tail_sha256"] = m.sha(tail)
        docs["ethoscope-split-zip-index.json"] = json.dumps(index).encode()
    else:
        lines = docs["ethoscope-METADATA.txt"].splitlines(keepends=True)
        del lines[next(i for i,line in enumerate(lines) if b"\tno_fly\tFigure01\t" in line)]
        docs["ethoscope-METADATA.txt"] = b"".join(lines)
    with pytest.raises(ValueError):
        m.build(docs)


def test_production_builder_has_no_network_database_or_deposited_program_execution():
    source = SOURCE.read_text()
    assert "import requests" not in source and "import sqlite3" not in source
    assert "subprocess" not in source and "urllib" not in source
