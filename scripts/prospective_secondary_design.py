"""Metadata-only design inventory for the prospective secondary protocols.

No monitor count, database coordinate, ordered track, fitted effect or deposited
program is read/executed. R is restricted to a small literal data grammar.
This inventory establishes label support, never scientific eligibility.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re


DAM_SHA256 = "a78a423d650f56a9297526a762b2fdd84871eceb44cfd6fe05e770d6cc06275c"
ETHOSCOPE_SHA256 = "83230109101cb01e57fb5f0d64b48b1965f07d9f45cf9c41db8105357c3d1616"
FIELDS = ["Index", "Date", "Time", "Monitor_status", "Extras", "Monitor_number",
          "Tube_number", "Data_type", "unused", "Light_sensor"]
ETHOSCOPE_FIELDS = ["date", "machine_name", "region_id", "genotype", "sex", "treatment",
    "baseline_temp", "after_inj_temp", "baseline", "incubator", "food_after_inj",
    "exp", "obs", "figure", "to_count", "fly", "dose", "time_correction"]
FALSE_FLAGS = dict.fromkeys([
    "raw_scientific_observations_read", "deposited_code_executed", "effects_computed",
    "individual_infection_verified", "randomization_verified", "exposure_onset_verified",
    "biological_identity_verified", "source_labels_are_causal_assignments",
    "raw_assay_eligibility_established", "original_H1_H2_H3_cleared",
    "original_D_or_E2_cleared", "gene_transfer_validated"], False)


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def strip_comments(text):
    output = list(text)
    quote = None
    escaped = comment = False
    for index, char in enumerate(text):
        if comment:
            if char == "\n":
                comment = False
            else:
                output[index] = " "
        elif quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
        elif char in "\"'":
            quote = char
        elif char == "#":
            output[index] = " "
            comment = True
    require(quote is None, "Unterminated R string")
    return "".join(output)


def closing(text, start):
    require(text[start] == "(", "Literal call opening required")
    depth = 0
    quote = None
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
        elif char in "\"'":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index + 1
    raise ValueError("Unclosed literal R call")


class Literals:
    """Parse list/c/string/unsigned-integer/identifier data; never evaluate R."""

    def __init__(self, text):
        self.text, self.position = text, 0

    def ws(self):
        while self.position < len(self.text) and self.text[self.position].isspace():
            self.position += 1

    def take(self, token):
        self.ws()
        require(self.text.startswith(token, self.position), "Unsupported R literal syntax")
        self.position += len(token)

    def word(self):
        self.ws()
        match = re.match(r"[A-Za-z_][A-Za-z_0-9.]*", self.text[self.position:])
        require(match is not None, "Literal identifier required")
        self.position += len(match[0])
        return match[0]

    def value(self):
        self.ws()
        require(self.position < len(self.text), "Missing literal value")
        char = self.text[self.position]
        if char in "\"'":
            quote = char
            self.position += 1
            result = []
            while self.position < len(self.text):
                char = self.text[self.position]
                self.position += 1
                require(char not in "\r\n", "Multiline literal string refused")
                if char == quote:
                    return "".join(result)
                if char == "\\":
                    require(self.position < len(self.text), "Truncated string escape")
                    escape = self.text[self.position]
                    self.position += 1
                    require(escape in {"\\", '"', "'", "n", "r", "t"}, "Unsupported string escape")
                    char = {"n": "\n", "r": "\r", "t": "\t"}.get(escape, escape)
                result.append(char)
            raise ValueError("Unterminated literal string")
        if char.isdigit():
            match = re.match(r"[0-9]+", self.text[self.position:])
            self.position += len(match[0])
            return int(match[0])
        name = self.word()
        self.ws()
        if self.position == len(self.text) or self.text[self.position] != "(":
            return {"identifier": name}
        require(name in {"list", "c"}, "Executable mapping expression refused")
        self.take("(")
        result = {} if name == "list" else []
        self.ws()
        require(self.position < len(self.text), "Truncated literal call")
        if self.text[self.position] == ")":
            self.take(")")
            return result
        while True:
            if name == "list":
                key = self.word()
                self.take("=")
                require(key not in result, "Duplicate literal mapping name")
                result[key] = self.value()
            else:
                result.append(self.value())
            self.ws()
            require(self.position < len(self.text), "Truncated literal call")
            if self.text[self.position] == ")":
                self.take(")")
                return result
            self.take(",")

    def complete(self):
        result = self.value()
        self.ws()
        require(self.position == len(self.text), "Trailing executable mapping syntax refused")
        return result


def source_span(text, start, end):
    return {"first_line": text.count("\n", 0, start) + 1,
            "last_line": text.count("\n", 0, end) + 1,
            "source_text_sha256": sha(text[start:end].encode())}


def dam_design(raw):
    require(len(raw) <= 100_000, "Bounded DAM documentation required")
    text = raw.decode("utf-8-sig")
    masked = strip_comments(text)
    mappings, spans = {}, {}
    for match in re.finditer(r"(?m)^\s*(exp[0-9]+_channels)\s*<-\s*list\s*\(", masked):
        start = masked.index("list", match.start())
        end = closing(masked, masked.index("(", start))
        require(match[1] not in mappings, "Repeated mapping assignment refused")
        mappings[match[1]] = Literals(masked[start:end]).complete()
        spans[match[1]] = source_span(text, match.start(), end)
    columns = []
    for monitor in ("df_m1", "df_m2"):
        matches = list(re.finditer(r"colnames\(" + monitor + r"\)\s*\[1:10\]\s*<-\s*c\s*\(", masked))
        require(len(matches) == 1, "One explicit metadata-column assignment required")
        start = masked.rfind("c", matches[0].start(), matches[0].end())
        end = closing(masked, masked.index("(", start))
        columns.append(Literals(masked[start:end]).complete())
    require(columns == [FIELDS, FIELDS], "Unknown DAM metadata schema")
    compact = re.sub(r"\s*=\s*", "=", masked)
    require(re.search(r"for\s*\(i\s+in\s+1:32\)", masked)
            and 'orders="d b y H M S"' in compact and 'tz="UTC"' in compact,
            "Explicit channel count and author clock parser required")
    experiments, used, assignments = [], set(), set()
    totals, roster, contrast_roster = Counter(), [], []
    for match in re.finditer(r"(?m)^\s*(exp[0-9]+)\s*<-\s*process_experiment\s*\(", masked):
        require(match[1] not in assignments, "Repeated experiment assignment refused")
        assignments.add(match[1])
        start = masked.index("(", match.start())
        end = closing(masked, start)
        values = Literals("c" + masked[start:end]).complete()
        require(len(values) == 7 and all(isinstance(v, str) for v in values[:6])
                and isinstance(values[6], dict) and set(values[6]) == {"identifier"},
                "Explicit seven-argument experiment call required")
        name = values[6]["identifier"]
        require(name in mappings, "Unresolved literal channel mapping")
        require(name not in used, "A channel mapping reused by multiple experiments")
        used.add(name)
        mapping = mappings[name]
        require(set(mapping) == {"monitor1", "monitor2"}, "Two mapped monitors required")
        begin, finish = [datetime.strptime(v, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                         for v in values[4:6]]
        seconds = (finish - begin).total_seconds()
        require(seconds > 0 and seconds % 60 == 0, "Positive minute-aligned source window required")
        monitors = []
        for monitor in (1, 2):
            original_path = values[monitor - 1]
            parts = original_path.replace("\\", "/").split("/")
            require(len(parts) >= 2 and parts[-1] == f"Monitor{monitor}.txt", "Monitor suffix required")
            conditions = mapping[f"monitor{monitor}"]
            require(isinstance(conditions, dict), "Literal condition mapping required")
            assignment = {}
            for condition, channels in conditions.items():
                require(isinstance(channels, list) and channels
                        and all(type(c) is int and 1 <= c <= 32 for c in channels)
                        and len(set(channels)) == len(channels), "Unique channels1..32 required")
                for channel in channels:
                    require(channel not in assignment, "Overlapping condition assignments refused")
                    assignment[channel] = condition
            require("Uninfected" in conditions and "Empty" in conditions, "Explicit control/Empty labels required")
            counts = Counter(assignment.values())
            counts["unmapped"] = 32 - len(assignment)
            totals.update(counts)
            controls = [f"{values[2]}/monitor{monitor}/channel{c}" for c in sorted(conditions["Uninfected"])]
            for channel in range(1, 33):
                condition = assignment.get(channel)
                roster.append({"channel_id": f"{values[2]}/monitor{monitor}/channel{channel}",
                               "condition_literal": condition, "source_experiment": values[2],
                               "source_monitor": monitor, "source_channel": channel,
                               "role": "unmapped" if condition is None else "Empty" if condition == "Empty" else "coded_nonempty"})
            for condition in sorted(set(conditions) - {"Empty", "Uninfected"}):
                contrast_roster.append({"source_experiment": values[2], "source_monitor": monitor,
                    "condition_literal": condition, "reference_literal": "Uninfected",
                    "case_channel_ids": [f"{values[2]}/monitor{monitor}/channel{c}" for c in sorted(conditions[condition])],
                    "shared_reference_channel_ids": controls})
            monitors.append({"monitor": monitor, "source_path_value": original_path,
                "normalized_provider_name": parts[-2] + "_" + parts[-1],
                "condition_counts": dict(sorted(counts.items())),
                "condition_channels": {k: sorted(v) for k, v in sorted(conditions.items())}})
        experiments.append({"id": values[2], "name_literal": values[3], "assignment": match[1],
            "mapping_source": spans[name], "call_source": source_span(text, match.start(), end),
            "window": {"start_author_UTC": begin.isoformat(), "end_author_UTC": finish.isoformat(),
                "endpoints_inclusive": True, "minute_positions": int(seconds / 60) + 1,
                "minute_intervals": int(seconds / 60), "hardware_timezone_verified": False,
                "exposure_clock_verified": False}, "monitors": monitors})
    require(len(experiments) == 3 and len({e["id"] for e in experiments}) == 3
            and used == set(mappings), "Complete three-experiment source roster required")
    roster.sort(key=lambda row: row["channel_id"])
    require(len({r["channel_id"] for r in roster}) == 192, "Unique192 source channel slots required")
    nonempty = [r for r in roster if r["role"] == "coded_nonempty"]
    shared = sorted({c for r in contrast_roster for c in r["shared_reference_channel_ids"]})
    return {"source_sha256": sha(raw), "experiments": experiments, "channel_roster": roster,
        "condition_channel_counts": dict(sorted(totals.items())), "source_channel_slots": len(roster),
        "unique_coded_nonempty_channels": len(nonempty), "source_experiments": len(experiments),
        "source_monitors": sum(len(e["monitors"]) for e in experiments),
        "shared_reference_unique_channels": len(shared), "contrast_strata": contrast_roster,
        "contrast_rows_with_reused_controls": sum(len(r["case_channel_ids"]) + len(r["shared_reference_channel_ids"]) for r in contrast_roster),
        "stream_plan": {"primary": "CT", "sensitivity": "MT", "streams_are_replicates": False,
            "manual_byte_binding_required_before_analysis": True},
        "heldout_experiments": [{"test": e["id"], "train": sorted(o["id"] for o in experiments if o != e),
            "status": "candidate_metadata_split_only_raw_coverage_and_assay_gates_pending"} for e in sorted(experiments, key=lambda e: e["id"])],
        **FALSE_FLAGS}


def counts(rows, key):
    return dict(sorted(Counter(row[key] for row in rows).items()))


def ethoscope_design(raw):
    require(len(raw) <= 2_000_000, "Bounded ethoscope documentation required")
    text = raw.decode("utf-8-sig")
    require(all(len(line) <= 16_384 for line in text.splitlines()), "Oversized metadata row")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    require(reader.fieldnames == ETHOSCOPE_FIELDS, "Unknown ethoscope metadata schema")
    rows = list(reader)
    require(0 < len(rows) <= 20_000, "Bounded nonempty metadata roster required")
    by_identity, by_figure = defaultdict(list), defaultdict(list)
    seen = set()
    for row in rows:
        require(set(row) == set(ETHOSCOPE_FIELDS) and all(isinstance(v, str) for v in row.values()), "Malformed metadata row")
        require(re.fullmatch(r"[1-9][0-9]*", row["region_id"]), "Explicit positive ROI identifier required")
        require(all(row[k] for k in ("date", "machine_name", "figure", "exp", "treatment")), "Missing design identifier")
        identity = (row["date"], row["machine_name"], int(row["region_id"]))
        key = (*identity, row["figure"])
        require(key not in seen, "Duplicate ROI within source figure refused")
        seen.add(key)
        by_identity[identity].append(row)
        by_figure[row["figure"]].append(row)
    inconsistent = []
    for identity, copies in sorted(by_identity.items()):
        differing = [k for k in ETHOSCOPE_FIELDS if k != "figure" and len({r[k] for r in copies}) > 1]
        if differing:
            inconsistent.append({"date": identity[0], "machine_name": identity[1], "region_id": identity[2],
                                 "conflicting_metadata_fields": differing, "figures": sorted({r["figure"] for r in copies})})
    figures = []
    for figure, group in sorted(by_figure.items()):
        experiments = []
        for experiment in sorted({r["exp"] for r in group}):
            subset = [r for r in group if r["exp"] == experiment]
            treatments = []
            for treatment in sorted({r["treatment"] for r in subset}):
                selected = [r for r in subset if r["treatment"] == treatment]
                treatments.append({"label_literal": treatment, "metadata_ROIs": len(selected),
                    "obs_literals": counts(selected, "obs"), "genotype_literals": counts(selected, "genotype"),
                    "dose_literals": counts(selected, "dose")})
            experiments.append({"label_literal": experiment, "metadata_ROIs": len(subset), "treatments": treatments,
                "database_keys": [{"date": d, "machine_name": m, "metadata_ROIs": sum(r["date"] == d and r["machine_name"] == m for r in subset)}
                                  for d, m in sorted({(r["date"], r["machine_name"]) for r in subset})]})
        figures.append({"figure_literal": figure, "metadata_ROIs": len(group),
            "genotype_literals": counts(group, "genotype"), "sex_literals": counts(group, "sex"),
            "time_correction_literals": counts(group, "time_correction"),
            "obs_literals": counts(group, "obs"), "experiments": experiments,
            "status": "metadata_label_support_only_no_assay_or_clock_qualification"})
    return {"source_sha256": sha(raw), "metadata_rows": len(rows), "unique_date_machine_ROIs": len(by_identity),
        "cross_figure_repeated_rows": len(rows) - len(by_identity),
        "cross_figure_metadata_conflicts": inconsistent, "figures": figures,
        "obs_or_to_count_filters_applied": False, "physical_ROI_key": ["date", "machine_name", "region_id"],
        "biological_independence_of_ROI_key_verified": False, **FALSE_FLAGS}


def read_document(path, expected_sha, limit):
    require(re.fullmatch(r"[0-9a-f]{64}", expected_sha) is not None, "Exact documentation SHA required")
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "Ordinary metadata file required")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    require(len(raw) <= limit and sha(raw) == expected_sha, "Pinned documentation bytes mismatch")
    return raw


def inventory(dam_raw, ethoscope_raw):
    return {"schema_version": 1, "scope": "prospective_metadata_design_only",
            "DAM": dam_design(dam_raw), "ethoscope": ethoscope_design(ethoscope_raw), **FALSE_FLAGS}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dam-code", required=True, type=Path)
    parser.add_argument("--ethoscope-metadata", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    dam = read_document(args.dam_code, DAM_SHA256, 100_000)
    ethoscope = read_document(args.ethoscope_metadata, ETHOSCOPE_SHA256, 2_000_000)
    value = inventory(dam, ethoscope)
    # No current-file claim is published if either bounded documentation source changes.
    require(read_document(args.dam_code, DAM_SHA256, 100_000) == dam
            and read_document(args.ethoscope_metadata, ETHOSCOPE_SHA256, 2_000_000) == ethoscope,
            "Documentation changed before publication")
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    require(len(raw) <= 5_000_000, "Bounded design inventory exceeded")
    with args.output.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
