"""Freeze a complete Figure01 source roster from existing documentation only.

No network, scientific database, SQLite, deposited program, coordinate or effect
is opened. The generated manifest is an acquisition design, not an executable
transfer plan or assay-eligibility decision. Existing metadata is untrusted data.
"""
from __future__ import annotations

import bisect
from collections import Counter, defaultdict
import csv
import hashlib
import io
import json
from pathlib import Path
import re
import stat
import struct


BASE = Path("work/public-cohort-metadata-20261021")
OUTPUT = Path("work/ethoscope-figure01-acquisition-manifest-v2-20261022.json")
PROTOCOL_PATH = Path("docs/PROSPECTIVE_SECONDARY_ANALYSES_20261021.md")
PROTOCOL_SHA = "df47c02ef2e401b86b824618b36d58733679fd1be485c738169fdc4ac1a3e9a5"
PINS = {
    "zenodo-6567039.json": "2514b6e65c2892c69db797263ee545d546a76beee4ac931fc3b1257f45992cce",
    "ethoscope-METADATA.txt": "83230109101cb01e57fb5f0d64b48b1965f07d9f45cf9c41db8105357c3d1616",
    "db_files_with_checksum.txt": "39d6f17957c394d95b3746df1aa4ff75ba15ae4722ffede293ed0c6de9c729c6",
    "zip_files_checksums.csv": "8cb455520bd4382d136010f6ef5fe0507cbcccaa0dd20b6ab60f2b01f6cbdff5",
    "ethoscope-split-zip-index.json": "ffc58aa960a5fdaec167107c4fece8fd7789fb5daad79675c9aa85980dee46b6",
    "ethoscope-split-zip-directory.bin": "c2a550fe715ab0de9bc70499b04f94c747da50845b9725cd96fa130420382528",
    "ethoscope-split-zip-tail.bin": "d80d5a2e9b380bfed81126a67cbdb29bb574a6c5fd298de79d4a2788b7d97385",
}
FIELDS = ["date", "machine_name", "region_id", "genotype", "sex", "treatment",
    "baseline_temp", "after_inj_temp", "baseline", "incubator", "food_after_inj",
    "exp", "obs", "figure", "to_count", "fly", "dose", "time_correction"]
MAX_RANGE_BYTES = 8_388_608  # Proposed NEW adapter; existing canary stays 1 MiB.


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def safe_member(name):
    require(isinstance(name, str) and name.isascii() and len(name) <= 1024,
            "Bounded ASCII ZIP member required")
    require(name.startswith("plos_path_results/") and not name.startswith("/")
            and "\\" not in name and "\x00" not in name
            and not any(p in {".", "..", ""} for p in name.rstrip("/").split("/")),
            "Unsafe or out-of-namespace ZIP name")


def extra_fields(raw):
    result, position = [], 0
    while position < len(raw):
        require(position + 4 <= len(raw), "Truncated central extra header")
        kind, length = struct.unpack_from("<HH", raw, position)
        position += 4
        require(position + length <= len(raw), "Truncated central extra body")
        require(kind != 1, "ZIP64 is outside this acquisition design")
        result.append({"id": kind, "bytes": length, "body_hex": raw[position:position+length].hex()})
        position += length
    return result


def parse_central(raw):
    entries, position = [], 0
    while position < len(raw):
        require(position + 46 <= len(raw), "Truncated central fixed header")
        value = struct.unpack_from("<4s6H3I5H2I", raw, position)
        (magic, made, needed, flags, method, mtime, mdate, crc, compressed,
         uncompressed, name_n, extra_n, comment_n, disk, internal, external, offset) = value
        require(magic == b"PK\x01\x02", "Unexpected central record signature")
        require(0 < name_n <= 1024 and extra_n <= 4096 and comment_n <= 4096,
                "Bounded central variable fields required")
        require(0 <= disk < 59 and 0 <= offset < 2**32-1
                and compressed < 2**32-1 and uncompressed < 2**32-1,
                "ZIP64 or invalid physical position refused")
        end = position + 46 + name_n + extra_n + comment_n
        require(end <= len(raw), "Truncated central variable fields")
        name_raw = raw[position+46:position+46+name_n]
        name = name_raw.decode("ascii")
        safe_member(name)
        extra = raw[position+46+name_n:position+46+name_n+extra_n]
        entries.append({"name": name, "flags": flags, "compression": method,
            "crc32": f"{crc:08x}", "compressed_bytes": compressed,
            "uncompressed_bytes": uncompressed, "disk_start": disk,
            "local_header_offset": offset, "extra_hex": extra.hex(),
            "external_attributes": external, "version_made_by": made,
            "version_needed": needed, "modification_time_DOS": mtime,
            "modification_date_DOS": mdate, "internal_attributes": internal,
            "central_comment_hex": raw[end-comment_n:end].hex() if comment_n else "",
            "central_record_offset_in_directory": position,
            "central_record_bytes": end-position, "extra_fields": extra_fields(extra)})
        position = end
    require(len({e["name"] for e in entries}) == len(entries), "Duplicate central member name")
    return entries


def split_interval(start, count, parts):
    """Map one logical contiguous interval to exact ordered physical segments."""
    require(type(start) is int and type(count) is int and start >= 0 and count > 0,
            "Positive integer interval required")
    offsets = [p["logical_start"] for p in parts] + [parts[-1]["logical_end_exclusive"]]
    require(start + count <= offsets[-1], "Interval extends beyond archive")
    segments, current, remaining = [], start, count
    while remaining:
        disk = bisect.bisect_right(offsets, current) - 1
        require(0 <= disk < len(parts), "Invalid split disk")
        part = parts[disk]
        length = min(remaining, part["logical_end_exclusive"] - current)
        local = current - part["logical_start"]
        require(0 <= local < part["bytes"] and 0 < length <= part["bytes"]-local,
                "Invalid physical segment")
        segments.append({"segment_ordinal": len(segments), "disk": disk,
            "part_key": part["key"], "url": part["url"],
            "part_provider_md5": part["provider_md5"], "part_bytes": part["bytes"],
            "start": local, "end_inclusive": local+length-1, "bytes": length,
            "logical_start": current, "logical_end_exclusive": current+length,
            "max_request_bytes": MAX_RANGE_BYTES,
            "request_count": (length+MAX_RANGE_BYTES-1)//MAX_RANGE_BYTES})
        remaining -= length
        current += length
    require(sum(s["bytes"] for s in segments) == count, "Split byte loss")
    return segments


def build(documents):
    provider = json.loads(documents["zenodo-6567039.json"])
    require(provider["id"] == 6567039 and provider["doi"] == "10.5281/zenodo.6567039",
            "Wrong official provider version")
    require(provider["metadata"]["license"]["id"] == "cc-by-4.0", "Unexpected provider license")
    files = {f["key"]: f for f in provider["files"]}
    require(len(files) == len(provider["files"]) == 63, "Unexpected provider file roster")
    csv_rows = list(csv.DictReader(io.StringIO(documents["zip_files_checksums.csv"].decode()), delimiter="\t"))
    csv_map = {r["filename"]: r for r in csv_rows}
    require(len(csv_map) == len(csv_rows) == 60, "Part checksum table duplicates/count")
    for name in ("db_files_with_checksum.txt", "zip_files_checksums.csv"):
        raw = documents[name]
        require(len(raw) == files[name]["size"] and "md5:" + hashlib.md5(raw).hexdigest() == files[name]["checksum"],
                "Provider documentation MD5 mismatch")
    parts, logical = [], 0
    for disk in range(59):
        key = f"db_files.z{disk+1:02d}" if disk < 58 else "db_files.zip"
        f = files[key]
        md5 = f["checksum"].removeprefix("md5:")
        require(re.fullmatch(r"[0-9a-f]{32}", md5) is not None, "Invalid part provider MD5")
        require(f["size"] == (2_097_152_000 if disk < 58 else 2_023_204_779), "Part-size roster changed")
        url = f"https://zenodo.org/api/records/6567039/files/{key}/content"
        require(f["links"]["self"] == url, "Part URL not source bound")
        c = csv_map[key]
        require(c["Local checksum"] == c["uploaded checksum"] == md5 and c["Match"] == "OK",
                "Official part checksum records disagree")
        parts.append({"disk": disk, "key": key, "bytes": f["size"], "provider_md5": md5,
            "url": url, "logical_start": logical, "logical_end_exclusive": logical+f["size"],
            "whole_part_md5_verified_by_this_design": False})
        logical += f["size"]
    index = json.loads(documents["ethoscope-split-zip-index.json"])
    require(index["directory_sha256"] == sha(documents["ethoscope-split-zip-directory.bin"])
            and index["tail_sha256"] == sha(documents["ethoscope-split-zip-tail.bin"]), "Index/raw-directory disagreement")
    require(index["central_directory_disk"] == index["eocd_disk"] == 58
            and index["entry_count"] == 1305 and index["provider_file_bytes"] == parts[58]["bytes"],
            "Split-directory layout changed")
    tail = documents["ethoscope-split-zip-tail.bin"]
    position = tail.rfind(b"PK\x05\x06")
    require(position >= 0 and position + 22 == len(tail), "Exact uncommented EOCD required")
    eocd = struct.unpack_from("<4s4H2IH", tail, position)
    require(eocd == (b"PK\x05\x06", 58, 58, 1305, 1305, 223898, 2022980783, 0),
            "EOCD version/layout disagreement")
    # The >4GB split archive has ZIP64 *end* records even though the selected
    # members use ordinary 32-bit local/central size fields. Do not conflate
    # this global layout with unsupported per-member ZIP64 headers/extras.
    require(position >= 76, "Global ZIP64 end/locator missing")
    zip64_end = struct.unpack("<4sQ2H2I4Q", tail[position-76:position-20])
    zip64_locator = struct.unpack("<4sIQI", tail[position-20:position])
    require(zip64_end == (b"PK\x06\x06", 44, 798, 45, 58, 58, 1305, 1305, 223898, 2022980783),
            "Global ZIP64 end-record disagreement")
    require(zip64_locator == (b"PK\x06\x07", 58, 2023204681, 59),
            "Global ZIP64 locator disagreement")
    require(eocd[6]+eocd[5] == zip64_locator[2] == parts[58]["bytes"]-98,
            "Central-directory/global-ZIP64-end boundary disagreement")
    central = parse_central(documents["ethoscope-split-zip-directory.bin"])
    require(len(central) == len(index["entries"]) == 1305, "Complete central-entry count required")
    for parsed, indexed in zip(central, index["entries"]):
        require(all(parsed.get(k) == v for k, v in indexed.items()), "Binary/index central-entry mismatch")
    all_positions = {}
    for e in central:
        require(e["local_header_offset"] < parts[e["disk_start"]]["bytes"], "Local header outside its source part")
        e["logical_local_header_offset"] = parts[e["disk_start"]]["logical_start"]+e["local_header_offset"]
        require(e["logical_local_header_offset"] not in all_positions, "Duplicate local-header location")
        all_positions[e["logical_local_header_offset"]] = e
    ordered = sorted(central, key=lambda e: e["logical_local_header_offset"])
    by_name = {e["name"]: i for i, e in enumerate(ordered)}
    checksums = {}
    for line in documents["db_files_with_checksum.txt"].decode().splitlines():
        md5, relative = line.split("  ", 1)
        require(re.fullmatch(r"[0-9a-f]{32}", md5) is not None and relative.startswith("./"), "Invalid raw-member checksum row")
        name = "plos_path_results/" + relative[2:]
        safe_member(name)
        require(name not in checksums, "Duplicate raw-member provider checksum")
        checksums[name] = md5
    databases = [e for e in ordered if e["name"].endswith(".db")]
    require(len(databases) == len(checksums) == 522
            and {e["name"] for e in databases} == set(checksums), "Complete DB/checksum source roster mismatch")
    metadata = documents["ethoscope-METADATA.txt"]
    lines = metadata.splitlines(keepends=True)
    reader = csv.DictReader(io.StringIO(metadata.decode("utf-8")), delimiter="\t")
    require(reader.fieldnames == FIELDS, "Unknown metadata schema")
    rows = list(reader)
    require(len(rows) == 9842 and len(lines) == len(rows)+1, "Metadata row/multiline ambiguity")
    selected = []
    for source_line, row in enumerate(rows, 2):
        require(set(row) == set(FIELDS) and all(isinstance(v, str) for v in row.values()), "Malformed metadata row")
        if row["figure"] == "Figure01":
            require(re.fullmatch(r"[1-9][0-9]*", row["region_id"]) is not None, "Invalid literal ROI")
            selected.append({"source_line_1_based": source_line, "source_row_sha256": sha(lines[source_line-1]),
                "source_metadata": row, "assay_eligible": False, "exclusion_applied": False})
    require(len(selected) == 370, "Complete 370-row prospective Figure01 universe required")
    require(len({(r["source_metadata"]["date"], r["source_metadata"]["machine_name"], r["source_metadata"]["region_id"])
                 for r in selected}) == 370, "Duplicate Figure01 physical ROI key")
    groups = defaultdict(list)
    for row in selected:
        value = row["source_metadata"]
        groups[(value["date"], value["machine_name"])].append(row)
    require(len(groups) == 36, "Complete 36-source Figure01 cohort required")
    members, touched = [], set()
    for ordinal, ((date, machine), roi_rows) in enumerate(sorted(groups.items()), 1):
        candidates = [e for e in databases if e["name"].split("/")[2] == machine
                      and e["name"].split("/")[3][:10] == date]
        require(len(candidates) == 1, "Missing/ambiguous metadata-to-DB join")
        e = candidates[0]
        require(e["flags"] == 0 and e["compression"] == 8 and e["version_needed"] == 20,
                "Unsupported member flags, compression or required ZIP version")
        require(stat.S_ISREG(e["external_attributes"] >> 16), "Selected DB is not an indexed regular file")
        i = by_name[e["name"]]
        require(i + 1 < len(ordered), "Independent indexed successor required")
        successor = ordered[i+1]
        start = e["logical_local_header_offset"]
        end = successor["logical_local_header_offset"]
        header_bytes = end-start-e["compressed_bytes"]
        extra_bytes = header_bytes-30-len(e["name"].encode("ascii"))
        require(header_bytes == 198 and extra_bytes == 28, "Unexpected metadata-inferred header boundary")
        require(e["local_header_offset"] + header_bytes <= parts[e["disk_start"]]["bytes"],
                "Cross-part local header requires a separately qualified adapter")
        data_start = start+header_bytes
        segments = split_interval(data_start, e["compressed_bytes"], parts)
        touched.update(s["disk"] for s in segments)
        member = {"source_ordinal": ordinal, "source_id": date+"/"+machine,
            "selection_basis": "all exact Figure01 date-machine keys; no obs/to_count/outcome filtering",
            "central_entry": e, "raw_provider_md5": checksums[e["name"]],
            "raw_sha256_known_before_acquisition": False,
            "successor_central_entry": {k: successor[k] for k in
                ("name", "disk_start", "local_header_offset", "logical_local_header_offset")},
            "local_header_expectation": {"status": "inferred_from_central_successor_NOT_read_or_verified",
                "fixed_bytes": 30, "filename_bytes": len(e["name"].encode("ascii")),
                "inferred_extra_bytes": extra_bytes, "inferred_total_bytes": header_bytes,
                "expected_version_needed": 20, "expected_flags": 0, "expected_compression": 8,
                "expected_crc32": e["crc32"], "expected_compressed_bytes": e["compressed_bytes"],
                "expected_raw_bytes": e["uncompressed_bytes"],
                "runtime_required": "read and parse exact fixed/name/extra header; refuse ZIP64/descriptor/encryption, names/sizes/CRC/version disagreement, and any data-end/successor disagreement"},
            "compressed_logical_start_inferred": data_start,
            "compressed_logical_end_exclusive_inferred": end,
            "compressed_segments_inferred_pending_local_header_validation": segments,
            "split_spanning": len(segments) > 1, "header_range_requests": 2,
            "compressed_range_requests": sum(s["request_count"] for s in segments),
            "metadata_roi_rows": roi_rows,
            "obs_literal_counts": dict(sorted(Counter(r["source_metadata"]["obs"] for r in roi_rows).items())),
            "acquired_by_this_design": False, "database_schema_verified": False,
            "source_identity_in_database_verified": False, "scientific_eligibility_established": False}
        members.append(member)
    compressed = sum(e["central_entry"]["compressed_bytes"] for e in members)
    raw = sum(e["central_entry"]["uncompressed_bytes"] for e in members)
    require((compressed, raw) == (7_970_156_585, 15_499_844_608), "Full-cohort byte universe changed")
    counts = Counter(r["source_metadata"]["obs"] for r in selected)
    require(counts == Counter({"ok": 362, "no_fly": 1, "ethoscope_killing": 7}), "Observation flag universe changed")
    n_requests = sum(e["header_range_requests"]+e["compressed_range_requests"] for e in members)
    return {"schema_version": 1, "scope": "complete_Figure01_metadata_only_acquisition_design",
        "protocol_relation": "prospective secondary host-response cohort, not original H1-H3 replication",
        "prospective_protocol_binding": {"local_relative_path": PROTOCOL_PATH.as_posix(), "sha256": PROTOCOL_SHA},
        "source_selection": "figure literal exactly Figure01, then sorted date and machine; retain every matching row",
        "source_record": {"id": 6567039, "doi": provider["doi"], "license": "CC-BY-4.0",
            "api_url": "https://zenodo.org/api/records/6567039"},
        "source_documents": [{"local_relative_path": (BASE/name).as_posix(), "bytes": len(documents[name]),
            "sha256": PINS[name]} for name in PINS],
        "source_metadata_only": True, "raw_scientific_data_read": False,
        "deposited_program_executed": False, "effects_computed": False,
        "archive_end_layout": {"central_directory_disk": 58, "central_directory_offset": 2022980783,
            "central_directory_bytes": 223898, "central_entries": 1305,
            "global_zip64_end_record_present": True, "global_zip64_end_record_bytes": 56,
            "global_zip64_locator_bytes": 20, "global_zip64_end_record_offset_disk58": 2023204681,
            "total_disks": 59, "ordinary_EOCD_bytes": 22,
            "per_member_ZIP64_supported": False,
            "scope": "end-record metadata validated, whole-archive payload and checksum unverified"},
        "source_parts": parts, "members": members,
        "summary": {"databases": 36, "metadata_roi_rows": 370,
            "obs_literal_counts": dict(sorted(counts.items())),
            "other_obs_rows_retained": [r for r in selected if r["source_metadata"]["obs"] != "ok"],
            "treatment_literal_counts": dict(sorted(Counter(r["source_metadata"]["treatment"] for r in selected).items())),
            "experiment_literal_counts": dict(sorted(Counter(r["source_metadata"]["exp"] for r in selected).items())),
            "genotype_literal_counts": dict(sorted(Counter(r["source_metadata"]["genotype"] for r in selected).items())),
            "sex_literal_counts": dict(sorted(Counter(r["source_metadata"]["sex"] for r in selected).items())),
            "compressed_bytes": compressed, "raw_bytes": raw,
            "split_spanning_members": [e["source_id"] for e in members if e["split_spanning"]],
            "touched_part_disks": sorted(touched), "touched_part_count": len(touched),
            "inferred_local_header_bytes": 36*198, "proposed_max_range_bytes": MAX_RANGE_BYTES,
            "proposed_request_count_including_72_header_requests": n_requests,
            "existing_canary_max_range_bytes": 1_048_576,
            "requests_if_existing_1MiB_range_limit_were_retained": 72+sum(
                (s["bytes"]+1_048_575)//1_048_576 for e in members
                for s in e["compressed_segments_inferred_pending_local_header_validation"])},
        "proposed_admission_not_yet_qualified": {
            "whole_cohort_single_attempt_peak_retained_bytes_floor": compressed+raw+36*198,
            "whole_cohort_requested_byte_reservation": 26_000_000_000,
            "whole_cohort_requested_file_reservation": 5000,
            "range_evidence_files_at_3_per_request": 3*n_requests,
            "fixed_file_reservation_per_member": 20,
            "file_reservation_includes_source_fixture_logs_and_all_failed_or_partial_evidence": True,
            "network_concurrency": 1, "new_range_start_spacing_seconds_minimum": 1,
            "network_deadline_seconds_per_member": 900, "network_http_timeouts_connect_read_seconds": [15, 90],
            "native_verification_cpus": 1, "native_verification_memory_GiB": 4,
            "native_verification_deadline_seconds_per_member": 600,
            "whole_cohort_sequential_deadline_seconds_upper_design_bound": 36*(900+600),
            "direct_compute_node_internet_access_verified": False,
            "deployment_location": "only admitted fresh cases below personal /scratch/pwa209/horsehair-worm/maintenance",
            "fresh_storage_accounting_and_source_qualification_required": True,
            "gateway_work": "bounded network range streaming and receipts only; full DB decompression/hash verification under Slurm",
            "compressed_or_raw_source_unlink_authorized": False,
            "retries_resume_or_same_case_replay_authorized": False,
            "actual_throughput_or_completion_time_benchmarked": False},
        "completion_requirements": ["all 36 exact members have positive immutable acquisition and native verification receipts",
            "CRC32, exact compressed/raw byte counts and deposited raw MD5 verify for every member",
            "computed raw SHA256 and all range byte hashes retained; full-part MD5 remains unverified",
            "all 370 metadata rows and 8 non-ok flags remain linked to their original source lines",
            "no subset receipt or process exit alone may represent complete cohort acquisition"],
        "scientific_gates": {"assay_qualified": False, "raw_coverage_verified": False,
            "exact_per_animal_exposure_clock_verified": False, "causal_assignment_verified": False,
            "biological_independence_verified": False, "original_H1_H2_H3_cleared": False,
            "original_D_or_E2_cleared": False, "mechanism_or_gene_transfer_established": False},
        "full_parts_or_archive_downloaded": False, "download_adapter_created_or_executed": False,
        "author_contact_used": False}


def main():
    require(sha(PROTOCOL_PATH.read_bytes()) == PROTOCOL_SHA, "Prospective protocol bytes changed")
    documents = {}
    for name, expected in PINS.items():
        p = BASE/name
        require(p.is_file() and not p.is_symlink(), "Ordinary source metadata required")
        raw = p.read_bytes()
        require(len(raw) <= 2_000_000 and sha(raw) == expected, "Exact bounded metadata source required: "+name)
        documents[name] = raw
    result = build(documents)
    script = Path(__file__)
    result["manifest_builder"] = {"local_relative_path": script.resolve().relative_to(Path.cwd().resolve()).as_posix(),
        "sha256": sha(script.read_bytes())}
    for name, expected in PINS.items():
        require(sha((BASE/name).read_bytes()) == expected, "Source metadata changed before publication")
    require(sha(PROTOCOL_PATH.read_bytes()) == PROTOCOL_SHA, "Prospective protocol changed before publication")
    encoded = (json.dumps(result, indent=2, sort_keys=True, allow_nan=False)+"\n").encode()
    require(len(encoded) <= 2_000_000, "Manifest output bound exceeded")
    with OUTPUT.open("xb") as handle:
        handle.write(encoded)
    print(json.dumps({"output": str(OUTPUT), "bytes": len(encoded), "sha256": sha(encoded),
        "summary": result["summary"], "design_only": True}, sort_keys=True))


if __name__ == "__main__":
    main()
