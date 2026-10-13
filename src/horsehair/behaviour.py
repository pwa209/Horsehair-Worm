"""Source-specific behavioural adapters and independently grouped analyses."""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import copy
import hashlib
import json
import math
import re
import time
from typing import Any

import numpy as np
import pandas as pd
from scipy import io, optimize, special, stats


CANONICAL = ("source_file", "experiment_id", "animal_id", "roi_id", "genotype",
             "intervention", "control_id", "sex", "exposure_status", "outcome_status",
             "record_time_s", "x", "y", "geometry", "food_end", "tracking_valid")
FORBIDDEN_PREDICTORS = {"death_time", "time_of_death", "last_mvmt", "best_tau",
                        "outcome", "outcome_status", "status", "sporulation",
                        "survival", "fictive_death_time", "tod", "todmatrix"}

POLARIZATION_DOI = "10.5061/dryad.jh9w0vtkb"
MATING_DOI = "10.5061/dryad.4j0zpc8jw"


def _native_headers(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result.columns = [re.sub(r"\s+", "_", str(c).strip()).lower() for c in result.columns]
    if result.columns.duplicated().any():
        raise ValueError("Native header normalization creates duplicate columns")
    return result


def adapt_polarization_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Published Dryad Data S2 schema; ecological sheets never enter the cohort."""
    table = _native_headers(frame)
    mapping = {"response": "result", "infection": "inf", "dop": "degree",
               "illuminance": "light_intensity", "stimulus_side": "pola_window",
               "individual_id": "id", "trial_id": "no"}
    required = {"id", "inf", "degree", "result", "pola_window", "light_intensity", "year", "box", "date"}
    if required - set(table):
        raise ValueError(f"Dryad Data S2 native schema missing {sorted(required - set(table))}")
    table = table.rename(columns={value: key for key, value in mapping.items()})
    table["source_row_index"] = np.arange(len(table)) + 2
    table["source_response"] = table.response.copy()
    table["source_stimulus_side"] = table.stimulus_side.copy()
    table.response = table.response.astype(str).str.strip()
    categories = {"hori-pola": "polarized", "unpola": "unpolarized", "M": "middle"}
    if not set(table.response).issubset(categories):
        raise ValueError("Unrecognized native response category; result_hori cannot recover nonchoice")
    table.response = table.response.map(categories)
    ambiguous_reasons = [[] for _ in range(len(table))]
    record_disagreement = lambda mask, reason: [ambiguous_reasons[i].append(reason) for i in np.flatnonzero(np.asarray(mask, bool))]
    secondary_missing_count = 0
    if "result_hori" in table:
        table["source_result_hori"] = table.result_hori.copy()
        expected = table.response.eq("polarized").astype(int)
        present = table.result_hori.notna()
        secondary_missing_count = int((~present).sum())
        if not set(table.loc[present, "result_hori"]).issubset({0, 1}):
            raise ValueError("Native result_hori contains an unrecognized binary code")
        # The README defines 0 for a middle/nonchoice too. A recorded M with
        # result_hori=1 is contradictory, not a polarized response to invent.
        record_disagreement(present & ~table.result_hori.eq(expected), "result/result_hori contradiction")
    table.stimulus_side = table.stimulus_side.astype(str).str.strip()
    if not set(table.stimulus_side).issubset({"R", "L"}):
        raise ValueError("Native polarized stimulus side is not R/L")
    if "location" in table:
        table["source_location"] = table.location.copy()
        location = table.location.astype(str).str.strip()
        if not set(location).issubset({"R", "L", "M"}):
            raise ValueError("Native location contains an unrecognized third-of-box code")
        expected = np.where(table.response.eq("middle"), "M",
                            np.where(table.response.eq("polarized"), table.stimulus_side,
                                     table.stimulus_side.map({"R": "L", "L": "R"})))
        record_disagreement(~location.eq(expected), "result/location/window contradiction")
        table["response_location_window"] = np.where(location.eq("M"), "middle",
            np.where(location.eq(table.stimulus_side), "polarized", "unpolarized"))
    table["coding_ambiguous"] = [bool(reasons) for reasons in ambiguous_reasons]
    table["coding_ambiguity_reason"] = ["; ".join(reasons) for reasons in ambiguous_reasons]
    table["analysis_included"] = ~table.coding_ambiguous
    if table.individual_id.isna().any():
        raise ValueError("Missing source individual identifier")
    table.individual_id = table.individual_id.astype(str)
    if "trial_id" in table and table.duplicated(["individual_id", "year", "date", "trial_id"]).any():
        raise ValueError("Duplicate source trial number; reconcile sheet rows before analysis")
    report = {"doi": POLARIZATION_DOI, "sheet_role": "Data S2 laboratory two-choice test",
              "native_choice_field": "result", "three_response_categories": categories,
              "trials": len(table), "individuals": int(table.individual_id.nunique()),
              "repeated_trials": int(len(table) - table.individual_id.nunique()),
              "mutually_consistent_trials": int(table.analysis_included.sum()),
              "ambiguous_trials": int(table.coding_ambiguous.sum()),
              "ambiguous_source_rows": table.loc[table.coding_ambiguous, "source_row_index"].astype(int).tolist(),
              "missing_secondary_binary_labels": secondary_missing_count,
              "source_coding_reconciled": not bool(table.coding_ambiguous.any()),
              "parser_repair_amendment": {"id": "mantis-coding-ambiguity-20261013-v1",
                  "type": "prospective parser-repair amendment after source-coding audit, before fitting outcomes",
                  "rule": "Preserve every deposited row and endpoint; exclude mutually contradictory endpoint annotations from the conditional consistent-row fit. Recorded-result and location/window alternatives are separately labelled sensitivity evidence, never raw corrections.",
                  "confirmatory_limit": "Unresolved deposited contradictions prevent an unqualified author replication or confirmatory claim; completing a conditional fit does not reconcile source coding"},
              "dop_units": "source", "dop_recode": "none; native degree scale retained",
              "rights": "CC0", "schema_source": "Published Dryad README on dataset landing page",
              "source_metadata_url": "https://datadryad.org/dataset/doi:10.5061/dryad.jh9w0vtkb"}
    return table, report


def read_polarization_source(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    book = pd.ExcelFile(path)
    candidates = []
    required = {"id", "inf", "degree", "result", "pola_window", "light_intensity", "year", "box", "date"}
    for sheet in book.sheet_names:
        for header in range(8):
            frame = pd.read_excel(book, sheet_name=sheet, header=header)
            normalized = _native_headers(frame)
            if required.issubset(normalized):
                candidates.append((sheet, header, frame))
                break
    if len(candidates) != 1:
        raise ValueError(f"Expected one laboratory Data S2 sheet; found {len(candidates)} matching schemas")
    sheet, header, frame = candidates[0]
    table, report = adapt_polarization_frame(frame)
    table.source_row_index += header
    report["ambiguous_source_rows"] = table.loc[table.coding_ambiguous, "source_row_index"].astype(int).tolist()
    report.update(sheet=sheet, header_row=header + 1,
                  source_path=str(Path(path).resolve()), source_sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest())
    readme = Path(path).parent / "README.md"
    if readme.exists():
        report.update(readme_path=str(readme.resolve()), readme_sha256=hashlib.sha256(readme.read_bytes()).hexdigest())
    return table, report


def adapt_mating_frame(frame: pd.DataFrame, source_sha256: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Published mating CSV, preserving censoring and unresolved individual reuse."""
    table = _native_headers(frame)
    mapping = {"male_infection": "m_inf", "female_infection": "f_inf",
        "male_size": "m_size", "female_size": "f_size", "observation_duration": "obs_time",
        "male_escape": "m_esc", "male_mounting": "mount",
        "male_escape_time": "lat_m_esc", "male_escape_censored": "lat_m_esc_cens",
        "male_mounting_time": "lat_mount", "male_mounting_censored": "lat_mount_cens",
        "female_escape": "f_esc", "female_escape_time": "lat_f_esc", "female_escape_censored": "lat_f_esc_cens",
        "female_attack": "f_attack", "female_attack_time": "lat_f_attack", "female_attack_censored": "lat_f_attack_cens"}
    required = {mapping[name] for name in ("male_infection", "female_infection", "male_size", "female_size",
        "observation_duration", "male_escape", "male_mounting", "male_escape_time", "male_escape_censored",
        "male_mounting_time", "male_mounting_censored")} | {"year"}
    if required - set(table):
        raise ValueError(f"Native mating CSV missing {sorted(required - set(table))}")
    table = table.rename(columns={value: key for key, value in mapping.items()})
    for endpoint in ("male_escape", "male_mounting"):
        censor = table[f"{endpoint}_censored"]
        if not set(censor).issubset({0, 1}) or not set(table[endpoint]).issubset({0, 1}):
            raise ValueError("Unrecognized mating event/censor coding")
        if not table[endpoint].eq(1 - censor).all():
            raise ValueError("Event occurrence contradicts first-event censor flag")
    # A record key is deliberately not a claim that individual reuse is absent.
    if "pair_id" not in table:
        table["pair_id"] = [f"{source_sha256}:source-row:{i + 2}" for i in range(len(table))]
        pair_identity = "file-hash/CSV-row provenance key; biological pair identity not supplied"
    else:
        pair_identity = "explicit source pair_id"
    for source, target in (("m_id", "male_id"), ("f_id", "female_id")):
        if source in table:
            table[target] = table[source]
    report = {"doi": MATING_DOI, "source_sha256": source_sha256, "pair_records": len(table),
        "pair_identity": pair_identity, "male_ids_available": "male_id" in table,
        "female_ids_available": "female_id" in table,
        "independence_verified": "male_id" in table and "female_id" in table,
        "censor_convention": "1=right-censored; censored observation ends at obs_time",
        "latency_units": "minutes", "rights": "CC0",
        "schema_source": "Published Dryad README on dataset landing page",
        "source_metadata_url": "https://datadryad.org/dataset/doi:10.5061/dryad.4j0zpc8jw"}
    return table, report


def read_mating_source(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    sha = hashlib.sha256(Path(path).read_bytes()).hexdigest()
    table, report = adapt_mating_frame(pd.read_csv(path), sha)
    report["source_path"] = str(Path(path).resolve())
    return table, report


def load_mat(path: Path) -> dict[str, Any]:
    """Read conventional and v7.3 MAT without executing source MATLAB scripts."""
    try:
        return {k: v for k, v in io.loadmat(path, simplify_cells=True).items()
                if not k.startswith("__")}
    except (NotImplementedError, ValueError):
        import h5py
        with h5py.File(path, "r") as handle:
            def read(node: Any) -> Any:
                if isinstance(node, h5py.Group):
                    return {key: read(value) for key, value in node.items() if key != "#refs#"}
                data = node[()]
                if h5py.check_dtype(ref=node.dtype) is not None:
                    values = [read(handle[ref]) if ref else None for ref in data.flat]
                    return values[0] if len(values) == 1 else values
                matlab_class = node.attrs.get("MATLAB_class", b"")
                if matlab_class == b"char":
                    return "".join(chr(int(value)) for value in data.T.flat if value)
                return np.asarray(data).transpose(tuple(reversed(range(np.ndim(data)))))
            return read(handle)


def nested(value: Any, path: str) -> Any:
    for component in path.split("."):
        if isinstance(value, dict) and component in value:
            value = value[component]
        else:
            raise ValueError(f"Source schema does not contain {path!r}")
    return value


def mat_inventory(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    def walk(value: Any, prefix: str = "") -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                walk(child, f"{prefix}.{key}".strip("."))
        elif isinstance(value, (np.ndarray, list, tuple)):
            array = np.asarray(value)
            result[prefix] = {"shape": list(array.shape), "dtype": str(array.dtype)}
        else:
            result[prefix] = {"type": type(value).__name__, "value": str(value)[:200]}
    walk(load_mat(path))
    return result


def _validated_roi_metadata(metadata: pd.DataFrame, rois: int, source: dict[str, Any]) -> tuple[pd.DataFrame, pd.Series, list[int]]:
    column = source.get("roi_column", "ROI")
    dropped = []
    if column in metadata:
        raw = metadata[column]
        blank = raw.isna() | raw.astype(str).str.strip().eq("")
        dropped = (metadata.index[blank] + 2).tolist()
        metadata = metadata.loc[~blank].copy()
        numbers = pd.to_numeric(metadata[column], errors="raise")
        if not np.isfinite(numbers).all() or not np.equal(numbers, np.floor(numbers)).all():
            raise ValueError("ROI IDs must be finite integers, not rounded labels")
        indices = numbers.astype(int)
    elif source.get("roi_by_row_verified") and len(metadata) == rois:
        indices = pd.Series(np.arange(1, rois + 1), index=metadata.index)
    else:
        raise ValueError("ROI-to-animal mapping is ambiguous; inspect source identities")
    if indices.duplicated().any() or not indices.between(1, rois).all():
        raise ValueError("Duplicate or out-of-range metadata ROI")
    if metadata.empty:
        raise ValueError("No identified native ROI rows")
    return metadata, indices, dropped


def processed_clock_proof(directory: Path, root: Path) -> dict[str, Any] | None:
    """Use the deposited author validator, rather than a universal rate guess."""
    from .core import digest
    candidates = []
    for base in (Path(directory), Path(root) / "metadata"):
        if base.exists():
            candidates.extend(path for path in base.rglob("*.m") if path.name.lower() == "triggerondeath2019fun.m")
    valid = []
    for path in sorted(set(candidates)):
        text = path.read_text(encoding="utf-8", errors="replace")
        if re.search(r"data\.params\.fr\s*=\s*3\s*;", text) and re.search(r"expmt\.parameters\.target_rate\s*~=\s*3", text):
            valid.append(path)
    if not valid:
        return None
    return {"author_processed_rate_hz": 3., "source_result_paths": [str(path.resolve()) for path in valid],
            "source_result_hashes": {str(path.resolve()): digest(path) for path in valid},
            "reference": "https://lab.debivort.org/zombie-summiting/Scripts/Summit%20behavior%20analysis/triggerOnDeath2019Fun.m",
            "interpretation": "Published processed arrays use a uniform three-Hz frame index and author code rejects other declared rates; actual per-frame timestamp jitter is unavailable"}


def discover_fly_sources(directory: Path, root: Path, config: dict[str, Any] | None = None
                         ) -> dict[str, Any]:
    """Resolve publication-associated chronological pairs; retain every ambiguity.

    Per-file MARGO target_rate is a nominal frame clock, not measured elapsed
    timestamps. An explicit configuration permits that documented clock route.
    Geometry is resolved from inspected metadata or an audited assay declaration.
    """
    config = config or {}
    directory, root = Path(directory), Path(root)
    uniform_proof = processed_clock_proof(directory, root)
    candidates, resolved, unresolved = [], [], []
    sources = sorted(directory.rglob("*margoConvert*.mat"))
    for path in sources:
        try:
            value = load_mat(path)
            centroid = np.asarray(nested(value, "expmt.Centroid.data"))
            if centroid.ndim not in (2, 3):
                raise ValueError("Unexpected centroid dimensions")
            count = centroid.shape[-1] if centroid.ndim == 3 else 1
            stem = path.stem.replace("_margoConvert", "").replace("-margoConvert", "")
            sheets = list(path.parent.glob("*.xlsx"))
            preferred = [p for p in sheets if "survival" in p.name.lower() and
                         (stem in p.stem or p.stem in stem)]
            if len(preferred) != 1:
                preferred = [p for p in sheets if "survival" in p.name.lower()]
            if len(preferred) != 1:
                raise ValueError("No unique paired survival metadata workbook")
            metadata = read_table(preferred[0])
            metadata, roi_indices, excluded_rows = _validated_roi_metadata(metadata, count,
                {"roi_by_row_verified": True})
            if not {"Geno", "Sex"}.issubset(metadata):
                raise ValueError("Survival rows/columns do not match source ROIs")
            source = {"mat": str(path.relative_to(root)), "metadata": str(preferred[0].relative_to(root)),
                "native_roi_count": count,
                "experiment_id": stem, "centroid_path": "expmt.Centroid.data",
                "axis_order": "time,coordinate,roi", "roi_by_row_verified": True,
                "roi_mapping_provenance": "Author triggerOnDeath2019Fun.m rows 1:height(survival), validated against centroid ROI dimension",
                "excluded_blank_roi_excel_rows": excluded_rows,
                "exposure_values": config.get("exposure_values", {})}
            time_paths = config.get("time_paths", ["expmt.Time.data", "expmt.timestamps", "expmt.time"])
            found = []
            for field in time_paths:
                try:
                    array = np.asarray(nested(value, field), dtype=float).squeeze()
                    valid_clock = (np.all(array > 0) if field == "expmt.Time.data" else np.all(np.diff(array) > 0))
                    if array.shape == (centroid.shape[0],) and np.isfinite(array).all() and valid_clock:
                        found.append(field)
                except (ValueError, TypeError):
                    pass
            if len(found) == 1:
                field = found[0]
                units = config.get("time_units", {}).get(field, "seconds" if field == "expmt.Time.data" else None)
                if units not in ("seconds", "milliseconds", "days"):
                    raise ValueError(f"Inspect timestamp units for {field}; cannot infer them from magnitude")
                source.update(time_path=field, time_multiplier={"seconds": 1, "milliseconds": .001, "days": 86400}[units],
                    time_kind="intervals" if field == "expmt.Time.data" else "timestamps",
                    timestamp_provenance=("Native source expmt.Time.data inter-frame intervals in seconds: author convertMargo.m preserves expmt.data.time.raw(); MARGO autoTrack.m at commit e9d9d0756d3bb18c1a837622ecd5a4e180806012 writes trackDat.ifi"
                                          if field == "expmt.Time.data" else f"Source array {field}; explicitly declared {units}"))
                match = re.search(r"\d{2}-\d{2}-20\d{2}-(\d{2})-(\d{2})-(\d{2})", stem)
                if match:
                    source["start_time_s"] = int(match[1]) * 3600 + int(match[2]) * 60 + int(match[3])
            elif not found and (uniform_proof is not None or config.get("allow_documented_nominal_frame_clock", False)):
                rate = float(np.asarray(nested(value, "expmt.parameters.target_rate")).squeeze())
                if not np.isfinite(rate) or rate <= 0:
                    raise ValueError("Invalid per-experiment MARGO target_rate")
                if uniform_proof is not None and rate != uniform_proof["author_processed_rate_hz"]:
                    raise ValueError("Converted frame rate conflicts with the deposited author three-Hz validator")
                match = re.search(r"(?P<m>\d{2})-(?P<d>\d{2})-(?P<y>20\d{2})-(?P<h>\d{2})-(?P<min>\d{2})-(?P<s>\d{2})", stem)
                if not match:
                    raise ValueError("No source recording start clock in experiment identity")
                start = int(match["h"]) * 3600 + int(match["min"]) * 60 + int(match["s"])
                source.update(frame_rate=rate, start_time_s=start,
                    timestamp_provenance="Per-file expmt.parameters.target_rate, filename start clock and deposited triggerOnDeath2019Fun.m validator; processed uniform frame index, no observed per-frame jitter",
                    clock_kind="source_processed_uniform_frame_clock", clock_source_proof=uniform_proof)
            else:
                raise ValueError("Missing or ambiguous recording timestamps")
            declaration = config.get("assays", {}).get(stem, config.get("assays", {}).get(path.parent.name, {}))
            classifier_directory = any(re.fullmatch(r"Fig\s*4\s*\+\s*Supp", part, flags=re.I)
                                       for part in path.parts)
            if not declaration and classifier_directory:
                # Explicit author companion-page Fig 4 directory, not a guessed
                # genotype match across unrelated intervention experiments.
                bounds_array = np.asarray(nested(value, "expmt.ROI.bounds"), dtype=float)
                if bounds_array.shape != (count, 4) or not np.isfinite(bounds_array).all() or (bounds_array[:, 2:] <= 0).any():
                    raise ValueError("Classifier native ROI rectangles are missing or ambiguous")
                mapped = {str(i + 1): {"x": [float(b[0]), float(b[0] + b[2])],
                                      "y": [float(b[1]), float(b[1] + b[3])]}
                          for i, b in enumerate(bounds_array)}
                try:
                    mm_scale = float(np.asarray(nested(value, "expmt.parameters.mm_per_pix")).squeeze())
                except (ValueError, TypeError):
                    mm_scale = None
                physical = bool(mm_scale and mm_scale > 0 and mm_scale != 1)
                declaration = {"geometry": 30, "food_end": 0, "roi_arena_bounds": mapped,
                    "invert_y": True, "analysis_role": "behavioural_development",
                    "arena_normalization": "local_physical" if physical else "relative_long_axis",
                    "coordinate_scale": mm_scale if physical else 1,
                    "coordinate_units": "mm" if physical else "native_ROI_height",
                    "cohort_exposure_status": 1,
                    "cohort_exposure_provenance": "eLife85410 Methods Real-time summiting classifier: all1306 training flies and the novel test cohort exposed to E.muscae; companion page maps these15 classifier experiments. Blank Rx does not denote an unexposed animal.",
                    "provenance": "Author companion page https://lab.debivort.org/zombie-summiting/ explicitly maps Fig 4 + Supp to classifier tracking; publication eLife.85410 Methods standard 30-degree food-bottom arenas; convertMargo.m preserves ROI.bounds; autoDenoise.m establishes [x,y,width,height]; scaleHt.m establishes reversed image-y orientation. Per-file native rectangles used, not observed-position extrema."}
            if not declaration:
                declaration = config.get("documented_classifier_assay", {})
            if not declaration.get("provenance") or "food_end" not in declaration or "geometry" not in declaration:
                raise ValueError("No audited arena geometry and food endpoint in matching coordinate units")
            source.update({key: value for key, value in declaration.items() if key != "provenance"})
            source["geometry_provenance"] = declaration["provenance"]
            # This final adapter call checks both clock and ROI mapping now.
            # Validate each native animal without retaining a many-experiment
            # raw-coordinate DataFrame on the audit node.
            for _ in iter_fly_source(source, root):
                pass
            source["formal_exposure_readiness"] = audit_exposure_source(source, root)
            resolved.append(source)
            candidates.append({"mat": str(path), "status": "resolved",
                "formal_fit_ready": source["formal_exposure_readiness"]["ready"]})
        except Exception as error:
            unresolved.append({"mat": str(path), "reason": str(error), "error_type": type(error).__name__})
    return {"fly_sources": resolved, "unresolved": unresolved,
            "candidates": candidates, "candidate_files": len(sources),
            "all_candidates_resolved": bool(sources) and not unresolved}


def audit_exposure_source(source: dict[str, Any], root: Path) -> dict[str, Any]:
    metadata = read_table(Path(root) / source["metadata"], source.get("sheet"))
    metadata = metadata.rename(columns={value: key for key, value in source.get("metadata_columns", {}).items()})
    if source.get("native_roi_count"):
        metadata, _, _ = _validated_roi_metadata(metadata, int(source["native_roi_count"]), source)
    field = "exposure_status" if "exposure_status" in metadata else "Rx"
    if field not in metadata:
        return {"ready": False, "pending": ["Original source exposure/Rx column absent"]}
    raw = metadata[field]
    # Pandas 3's astype(str) preserves missing entries as floating NaN. Source
    # labels are metadata, so serialize observed strings and missing counts
    # separately rather than exporting a nonstandard JSON NaN or inventing Rx.
    labels = raw.map(lambda value: None if pd.isna(value) else str(value)).astype(object)
    missing_labels = labels.isna() | labels.fillna("").str.strip().eq("")
    codes = labels.map(source.get("exposure_values", {}))
    if source.get("cohort_exposure_provenance") and source.get("cohort_exposure_status") in (0, 1):
        codes.loc[missing_labels] = source["cohort_exposure_status"]
    codes.loc[labels.fillna("").str.lower().isin(["uninfected", "unexposed"])] = 0
    pending = []
    unknown = sorted({"<missing>" if missing_labels.loc[index] else labels.loc[index]
                      for index in labels.index[codes.isna()]})
    if unknown or not set(codes.dropna()).issubset({0, 1}):
        pending.append(f"Published exposed/unexposed encoding unresolved for native labels {unknown}")
    offset = metadata.get("exposure_elapsed_at_start_hours", pd.Series(source.get("exposure_elapsed_at_start_hours", np.nan), index=metadata.index))
    offset = pd.to_numeric(offset, errors="coerce")
    if not np.isfinite(offset[codes.eq(1)]).all() or (offset[codes.eq(1)] < 0).any():
        pending.append("Recording-start offset from exposure is unavailable; Day3 directory labels do not establish elapsed hours")
    if codes.eq(1).any() and not source.get("exposure_clock_provenance"):
        pending.append("Exposure-clock/protocol provenance is not recorded")
    return {"ready": not pending,
            "native_exposure_levels": sorted(labels.loc[~missing_labels].unique()),
            "native_exposure_missing_count": int(missing_labels.sum()),
            "exposure_label_column": field,
            "pending": pending, "exposure_clock_provenance": source.get("exposure_clock_provenance"),
            "rule": "Exposure is measured from original treatment assignment, never from future cadaver/survival labels"}


def formal_behavior_readiness(config: dict[str, Any], root: Path) -> dict[str, Any]:
    """Separate ingested processed clocks from validated biological progression."""
    from .core import canonical_hash, digest, source_identity
    root = Path(root)
    path = root / "manifests" / "fly_adapters.json"
    sources = config.get("behaviour", {}).get("fly_sources", [])
    if not sources and path.exists():
        manifest = json.loads(path.read_text(encoding="utf-8"))
        sources = manifest if isinstance(manifest, list) else manifest.get("fly_sources", [])
    development = [source for source in sources if source.get("analysis_role") == "behavioural_development"]
    pending, audits = [], {}
    if not development:
        pending.append("No validated chronological development source adapters")
    for source in development:
        try:
            audit = audit_exposure_source(source, root)
            audits[source["experiment_id"]] = audit
            pending.extend(f"{source['experiment_id']}: {reason}" for reason in audit["pending"])
            if not source.get("timestamp_provenance") or ("time_path" not in source and "frame_rate" not in source):
                pending.append(f"{source['experiment_id']}: processed clock provenance missing")
            clock_proof = source.get("clock_source_proof")
            if clock_proof:
                for referenced, expected in clock_proof.get("source_result_hashes", {}).items():
                    if digest(Path(referenced)) != expected:
                        pending.append(f"{source['experiment_id']}: author clock-validator code changed")
        except (ValueError, OSError, KeyError) as error:
            pending.append(f"{source.get('experiment_id', 'unknown')}: {error}")
    required = int(config.get("behaviour", {}).get("minimum_experiments", 10))
    if len({source["experiment_id"] for source in development}) < required:
        pending.append(f"Fewer than {required} independent development experiments")
    native = root / "data" / "fly_observations.parquet"
    if not native.exists():
        pending.append("Validated native observations have not been ingested")
    return {"ready": not pending, "pending": pending, "ingestion_ready": native.exists(),
        "development_experiments": len(development), "exposure_audits": audits,
        "clock_interpretation": "Source processed uniform frame indices are permitted only with per-file metadata and unchanged deposited author validator; no claim of observed frame jitter",
        "config_sha256": canonical_hash(config), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "source_result_paths": [str(path.resolve())] if path.exists() else [],
        "source_result_hashes": {str(path.resolve()): digest(path)} if path.exists() else {}}


def read_table(path: Path, sheet: str | int | None = None) -> pd.DataFrame:
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path, sheet_name=0 if sheet is None else sheet)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, sep="\t" if path.suffix.lower() == ".tsv" else ",")


def source_summit_metric(speed: np.ndarray, height: np.ndarray, parameters: dict[str, Any],
                         data_parameters: dict[str, Any]) -> dict[str, Any]:
    """Numerical author-method replication only; never forecasting features."""
    speed, height = np.asarray(speed, float), np.asarray(height, float)
    if speed.ndim == 1:
        speed, height = speed[:, None], height[:, None]
    if speed.ndim != 2 or height.shape != speed.shape:
        raise ValueError("Author death-aligned speed/height matrices must have identical time×fly dimensions")
    scalar = lambda mapping, key: float(np.asarray(mapping[key]).squeeze())
    window = int(scalar(parameters, "smoothWin"))
    start = int(scalar(parameters, "summitStart")) - 1  # MATLAB one-based index.
    last = int(scalar(data_parameters, "lastFrames")) - 1
    padding = int(scalar(parameters, "endPad"))
    rate = scalar(data_parameters, "fr")
    if rate != 3 or not (window > 0 and 0 <= start < len(speed) and start < last):
        raise ValueError("Unsupported source summit parameters; do not assume their clock/window")
    smooth = lambda array: pd.DataFrame(array).rolling(window, center=True, min_periods=1).mean().to_numpy()
    with np.errstate(invalid="ignore"):
        count = np.isfinite(speed).sum(1)
        average = np.nansum(speed, axis=1) / np.where(count > 0, count, np.nan)
        count_height = np.isfinite(height).sum(1)
        average_height = np.nansum(height, axis=1) / np.where(count_height > 0, count_height, np.nan)
    average = smooth(average[:, None])[:, 0]
    average_height = smooth(average_height[:, None])[:, 0]
    normalized = average - average[start]
    normalized[np.arange(len(normalized)) > last + padding] = -1
    possible = np.flatnonzero(normalized >= 0)
    if not len(possible):
        raise ValueError("Source genotype has no finite baseline/summit endpoint")
    end = max(int(possible[-1]), start + int(scalar(parameters, "minSumDur") * 60 * 3))
    if end >= len(speed) or end <= start:
        raise ValueError("Source summit window lies outside the deposited death-aligned data")
    smoothed, smooth_height = smooth(speed), smooth(height)
    normalized_individual = smoothed - smoothed[start]
    integral = np.trapezoid(normalized_individual[start:end + 1], axis=0) / (end - start)
    change = smooth_height[end] - smooth_height[start]
    presummit_speed, presummit_distance = [], []
    for column in range(speed.shape[1]):
        values = smoothed[:start + 1, column]
        values = values[np.isfinite(values)]
        presummit_distance.append(float(np.trapezoid(values) / len(values)) if len(values) else np.nan)
        values = speed[:start + 1, column]
        values = values[values > scalar(parameters, "moveThresh")]
        presummit_speed.append(float(np.mean(values)) if len(values) else np.nan)
    finite_mean = lambda values: float(np.mean(np.asarray(values)[np.isfinite(values)])) if np.isfinite(values).any() else np.nan
    return {"sumStart": start + 1, "sumEnd": end + 1, "sumDur": (end - start) / (rate * 3600),
        "sumInt": integral, "sumHt": change, "psWalkSpeed": np.asarray(presummit_speed),
        "psDist": np.asarray(presummit_distance), "numCad": speed.shape[1],
        "meanSumInt": float(np.trapezoid(normalized[start:end + 1]) / (end - start)),
        "meanSumHt": float(average_height[end] - average_height[start]),
        "meanPsWalkSpeed": finite_mean(presummit_speed), "meanPsDist": finite_mean(presummit_distance)}


def replicate_source_summit(config: dict[str, Any], root: Path) -> dict[str, Any]:
    from .core import atomic_json, canonical_hash, digest, source_identity
    root = Path(root)
    directory = root / "data" / "extracted" / "fly"
    files = sorted(directory.rglob("*QUANT*.mat"))
    designated = config.get("behaviour", {}).get("source_replication_files")
    if designated:
        files = [root / path for path in designated]
    rows, pending, inputs = [], [], []
    for quant_path in files:
        try:
            expected = nested(load_mat(quant_path), "summit")
            original = str(expected.get("sourceStruct", "")).replace("\\", "/").split("/")[-1]
            paired = list(directory.rglob(original)) if original else []
            if len(paired) != 1:
                alias = quant_path.name.replace("_QUANT", "").replace("QUANT", "")
                paired = list(directory.rglob(alias))
            if len(paired) != 1:
                raise ValueError("No unique deposited data struct for the source quantitative result")
            data_path = paired[0]
            data = nested(load_mat(data_path), "data")
            computed = source_summit_metric(nested(data, "Cadavers.speed.toDmatrix"),
                nested(data, "Cadavers.ypos.toDmatrix"), expected["params"], data["params"])
            for name, value in computed.items():
                if name not in expected:
                    raise ValueError(f"Published quantitative source lacks {name}")
                observed = np.asarray(expected[name], float).squeeze()
                calculated = np.asarray(value, float).squeeze()
                if observed.shape != calculated.shape:
                    raise ValueError(f"Source {name} dimensions disagree")
                finite = np.isfinite(observed) & np.isfinite(calculated)
                matching_missingness = np.array_equal(np.isfinite(observed), np.isfinite(calculated))
                error = float(np.max(np.abs(observed[finite] - calculated[finite]))) if finite.any() else 0.
                passed = bool(matching_missingness and np.allclose(observed, calculated, rtol=1e-5, atol=1e-7, equal_nan=True))
                rows.append({"source_quant": str(quant_path.resolve()), "source_data": str(data_path.resolve()),
                    "quantity": name, "maximum_absolute_error": error, "passed": passed,
                    "source_array_values": int(observed.size), "finite_values": int(finite.sum())})
            inputs.extend([quant_path, data_path])
        except (ValueError, KeyError, OSError, TypeError) as error:
            pending.append({"source_quant": str(quant_path.resolve()), "reason": str(error)})
    output = root / "results" / "behaviour" / "source_replication"
    output.mkdir(parents=True, exist_ok=True)
    table_path = output / "summit_metric_reproduction.tsv"
    pd.DataFrame(rows).to_csv(table_path, sep="\t", index=False)
    status = "pending" if not rows or pending else "complete" if all(row["passed"] for row in rows) else "failed"
    inputs = sorted(set(inputs + [table_path]))
    result = {"status": status, "source_replication_passed": status == "complete", "comparisons": len(rows),
        "pending": pending or ([{"reason": "No paired deposited quantitative/data source structs"}] if not rows else []),
        "evidence_class": "published source numerical replication; not prospective biological inference",
        "analysis_scope": "Author death-aligned descriptive summit metric with stored original parameters and source arrays; no values enter prediction features or increase biological replication",
        "algorithm_reference": "https://lab.debivort.org/zombie-summiting/Scripts/Dependencies/integrateSummitBump.m",
        "config_sha256": canonical_hash(config), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "source_result_paths": [str(path.resolve()) for path in inputs],
        "source_result_hashes": {str(path.resolve()): digest(path) for path in inputs}}
    atomic_json(output / "source_replication.json", result)
    return result


def validate_observations(frame: pd.DataFrame, allow_identical_duplicates: bool = False
                          ) -> tuple[pd.DataFrame, pd.DataFrame]:
    missing = set(CANONICAL) - set(frame)
    if missing:
        raise ValueError(f"Missing chronological-contract columns: {sorted(missing)}")
    keys = ["experiment_id", "animal_id", "record_time_s"]
    if frame[keys].isna().any().any():
        raise ValueError("Independent-unit keys and recording clock cannot be missing")
    if not np.isfinite(frame.record_time_s).all():
        raise ValueError("Nonfinite recording clock")
    duplicated = frame.duplicated(keys, keep=False)
    duplicate_report = frame.loc[duplicated, keys + ["source_file", "roi_id"]].copy()
    if duplicated.any():
        compare = [c for c in CANONICAL if c != "source_file"]
        for _, group in frame.loc[duplicated].groupby(keys, dropna=False):
            if len(group[compare].drop_duplicates()) > 1:
                raise ValueError("Conflicting representations of the same experiment/ROI/time")
        if not allow_identical_duplicates:
            raise ValueError("Duplicate animal/time representation; source crosswalk must resolve it")
        frame = frame.drop_duplicates(keys, keep="first").copy()
    frame = frame.sort_values(keys).reset_index(drop=True)
    for (_, _), group in frame.groupby(["experiment_id", "animal_id"], sort=False):
        if (np.diff(group.record_time_s) <= 0).any():
            raise ValueError("Chronological time is not strictly increasing")
        if group.roi_id.nunique() != 1:
            raise ValueError("An animal maps to several ROI identifiers")
    return frame, duplicate_report


def read_fly_source(source: dict[str, Any], root: Path) -> pd.DataFrame:
    """Materialized adapter for small inspected sources and unit fixtures."""
    return validate_observations(pd.concat(list(iter_fly_source(source, root)), ignore_index=True))[0]


def iter_fly_source(source: dict[str, Any], root: Path) -> Any:
    """Adapt an author MARGO expmt + survival XLSX, with explicit clock provenance.

    The primary author code establishes Centroid.data as T × coordinate × ROI;
    aggregate death-aligned products are intentionally insufficient for this API.
    """
    mat = root / source["mat"]
    metadata_path = root / source["metadata"]
    value = load_mat(mat)
    centroid_path = source.get("centroid_path", "expmt.Centroid.data")
    if "tod" in centroid_path.lower() or source.get("alignment", "chronological") != "chronological":
        raise ValueError("Death-aligned trajectories cannot be forecast inputs")
    positions = np.asarray(nested(value, centroid_path), dtype=float)
    if positions.ndim == 2 and positions.shape[1] in (1, 2):
        positions = positions[:, :, None]
    order = source.get("axis_order", "time,coordinate,roi").split(",")
    if set(order) != {"time", "coordinate", "roi"} or positions.ndim != 3:
        raise ValueError("Ambiguous centroid axes; provide an inspected axis_order")
    positions = positions.transpose([order.index(x) for x in ("time", "coordinate", "roi")])
    n, dimension, rois = positions.shape
    if dimension not in (1, 2):
        raise ValueError("Centroids must contain one or two actually measured coordinates")
    if "time_path" in source:
        times = np.asarray(nested(value, source["time_path"]), dtype=float).squeeze()
        if times.shape != (n,):
            raise ValueError("Recording timestamp array does not match centroid frames")
        times = times * float(source.get("time_multiplier", 1))
        if source.get("time_kind", "timestamps") == "intervals":
            if (times <= 0).any():
                raise ValueError("Native inter-frame intervals must be positive")
            times = np.cumsum(times) + float(source.get("start_time_s", 0))
        elif source.get("time_kind", "timestamps") != "timestamps":
            raise ValueError("Timestamp semantic kind is not validated")
    elif "frame_rate" in source and source.get("timestamp_provenance"):
        rate = float(source["frame_rate"])
        if rate <= 0:
            raise ValueError("Invalid validated source frame rate")
        times = np.arange(n, dtype=float) / rate + float(source.get("start_time_s", 0))
    else:
        raise ValueError("Recover the source recording clock; a universal 3-Hz assumption is prohibited")
    if not np.isfinite(times).all() or (np.diff(times) <= 0).any():
        raise ValueError("Invalid source timestamps")
    metadata = read_table(metadata_path, source.get("sheet"))
    mapping = source.get("metadata_columns", {})
    metadata = metadata.rename(columns={v: k for k, v in mapping.items()})
    metadata, indices, _ = _validated_roi_metadata(metadata, rois, source)
    experiment = str(source["experiment_id"])
    if not experiment or experiment.lower() == "nan":
        raise ValueError("Missing independent experiment identifier")
    exposure_values = source.get("exposure_values", {})
    bounds = source.get("arena_bounds")
    roi_bounds = source.get("roi_arena_bounds", {})
    scale = float(source.get("coordinate_scale", 1))
    for row_index, roi in zip(metadata.index, indices):
        info = metadata.loc[row_index]
        xy = positions[:, :, roi - 1].copy()
        spatial_valid = np.isfinite(xy).all(1)
        local_bounds = roi_bounds.get(str(int(roi)), bounds)
        if local_bounds:
            for axis_index, axis in enumerate(("x", "y")[:dimension]):
                lower, upper = map(float, local_bounds[axis])
                if upper <= lower:
                    raise ValueError("Arena calibration has invalid bounds")
                spatial_valid &= (xy[:, axis_index] >= lower) & (xy[:, axis_index] <= upper)
                if source.get("arena_normalization", "normalized_arena") == "local_physical":
                    xy[:, axis_index] = (xy[:, axis_index] - lower) * scale
                elif source.get("arena_normalization") == "relative_long_axis":
                    xy[:, axis_index] = (xy[:, axis_index] - lower) / (local_bounds["y"][1] - local_bounds["y"][0])
                else:
                    xy[:, axis_index] = (xy[:, axis_index] - lower) / (upper - lower)
            if source.get("invert_y", False):
                if source.get("arena_normalization", "normalized_arena") == "local_physical":
                    lower, upper = local_bounds["y"]
                    xy[:, -1] = (upper - lower) * scale - xy[:, -1]
                else:
                    xy[:, -1] = 1 - xy[:, -1]
        else:
            xy *= scale
        rx = info.get("exposure_status", info.get("Rx", None))
        exposure = exposure_values.get(str(rx), np.nan)
        if (pd.isna(rx) or not str(rx).strip()) and source.get("cohort_exposure_provenance") and source.get("cohort_exposure_status") in (0, 1):
            exposure = source["cohort_exposure_status"]
        if pd.isna(exposure) and str(rx).lower() in ("uninfected", "unexposed"):
            exposure = 0
        # A survivor is never automatically an unexposed animal.
        animal = f"{experiment}:roi:{int(roi)}"
        output = pd.DataFrame({"source_file": str(mat), "experiment_id": experiment,
            "animal_id": animal, "roi_id": int(roi),
            "genotype": info.get("genotype", info.get("Geno", np.nan)),
            "intervention": info.get("intervention", source.get("intervention", np.nan)),
            "control_id": info.get("control_id", source.get("control_id", np.nan)),
            "sex": info.get("sex", info.get("Sex", np.nan)),
            "exposure_status": exposure,
            "exposure_elapsed_at_start_hours": info.get("exposure_elapsed_at_start_hours", source.get("exposure_elapsed_at_start_hours", np.nan)),
            "outcome_status": info.get("Outcome", info.get("outcome_status", np.nan)),
            "record_time_s": times,
            "x": xy[:, 0] if dimension == 2 else np.nan,
            "y": xy[:, 1] if dimension == 2 else xy[:, 0],
            "geometry": source.get("geometry", np.nan),
            "food_end": source.get("food_end", np.nan),
            "tracking_valid": spatial_valid,
            "coordinate_units": (source.get("coordinate_units", "mm")
                                 if local_bounds and source.get("arena_normalization") == "local_physical"
                                 else source.get("coordinate_units", "normalized_arena") if local_bounds else source.get("coordinate_units", "source_units")),
            "timestamp_provenance": source.get("timestamp_provenance", source.get("time_path")),
            "analysis_role": source.get("analysis_role", "undeclared")})
        # Future outcomes remain separate annotations and are never features.
        for field in ("Status", "Last_mvmt", "Best_tau"):
            if field in info:
                output[f"annotation_{field.lower()}"] = info[field]
        for covariate in source.get("observed_covariates", []):
            if covariate.lower() in FORBIDDEN_PREDICTORS:
                raise ValueError(f"Forbidden future-derived covariate {covariate}")
            if covariate not in info:
                raise ValueError(f"Missing declared source covariate {covariate}")
            output[covariate] = info[covariate]
        yield validate_observations(output)[0]


def interpolate_short_gaps(times: np.ndarray, positions: np.ndarray,
                           valid: np.ndarray, maximum_seconds: float = 2.0,
                           maximum_speed: float | None = None
                           ) -> tuple[np.ndarray, np.ndarray]:
    """Both-bound interpolation with an availability clock to prevent leakage."""
    result = positions.copy()
    available = times.copy()
    good = np.flatnonzero(valid & np.isfinite(positions).all(1))
    for left, right in zip(good[:-1], good[1:]):
        if right == left + 1 or times[right] - times[left] > maximum_seconds:
            continue
        displacement = np.linalg.norm(positions[right] - positions[left])
        if maximum_speed is not None and displacement / (times[right] - times[left]) > maximum_speed:
            continue
        fraction = (times[left + 1:right] - times[left]) / (times[right] - times[left])
        result[left + 1:right] = positions[left] + fraction[:, None] * (positions[right] - positions[left])
        available[left + 1:right] = times[right]
    return result, available


def bin_trajectories(frame: pd.DataFrame, bin_seconds: float = 10,
                     maximum_gap_seconds: float = 2, missing_threshold: float = .2,
                     maximum_speed: float | None = None) -> pd.DataFrame:
    """One-second displacements, then right-closed state bins, on a real clock."""
    frame, _ = validate_observations(frame)
    outputs = []
    for (experiment, animal), group in frame.groupby(["experiment_id", "animal_id"], sort=False):
        times = group.record_time_s.to_numpy(float)
        dimension = 2 if group.x.notna().any() else 1
        coordinates = ["x", "y"] if dimension == 2 else ["y"]
        positions = group[coordinates].to_numpy(float, copy=True)
        valid = group.tracking_valid.to_numpy(bool) & np.isfinite(positions).all(1)
        positions[~valid] = np.nan
        positions, available = interpolate_short_gaps(times, positions, valid,
                                                      maximum_gap_seconds, maximum_speed)
        # Last available observation at each second. A two-bound interpolation
        # can enter a feature only once its right bound has actually been seen.
        seconds = np.arange(math.ceil(times[0]), math.floor(times[-1]) + 1, dtype=float)
        second_position = np.full((len(seconds), dimension), np.nan)
        for i, second in enumerate(seconds):
            index = np.searchsorted(times, second, side="right") - 1
            if index < 0:
                continue
            if available[index] <= second and second - times[index] <= maximum_gap_seconds:
                second_position[i] = positions[index]
        if len(seconds) < 2:
            continue
        displacement = np.vstack([np.full((1, dimension), np.nan), np.diff(second_position, axis=0)])
        if maximum_speed is not None:
            displacement[np.linalg.norm(displacement, axis=1) > maximum_speed] = np.nan
        bins = np.ceil(seconds / bin_seconds).astype(np.int64)
        first_bin, last_bin = int(bins.min()), int(bins.max())
        endpoint_previous = None
        for label in range(first_bin, last_bin + 1):
            selection = bins == label
            endpoint_time = label * bin_seconds
            chosen = np.flatnonzero(selection)
            # Partial leading/trailing windows are masked, preserving clock gaps.
            complete = len(chosen) >= int(bin_seconds) and seconds[chosen[-1]] >= endpoint_time
            observed_fraction = (np.isfinite(displacement[selection]).all(1).mean()
                                 if len(chosen) else 0.)
            endpoint = second_position[chosen[-1]] if len(chosen) else np.full(dimension, np.nan)
            observed = (complete and observed_fraction >= 1 - missing_threshold
                        and np.isfinite(endpoint).all() and endpoint_previous is not None
                        and np.isfinite(endpoint_previous).all())
            delta = endpoint - endpoint_previous if observed else np.full(dimension, np.nan)
            meta = group.iloc[0].to_dict()
            meta.update(record_time_s=endpoint_time, state_bin_seconds=bin_seconds,
                        missing_fraction=1 - observed_fraction, observed=bool(observed),
                        dx=delta[0] if dimension == 2 else np.nan, dy=delta[-1],
                        x=endpoint[0] if dimension == 2 else np.nan, y=endpoint[-1],
                        path_length=float(np.nansum(np.linalg.norm(displacement[selection], axis=1))),
                        n_one_second_bins=int(len(chosen)))
            outputs.append(meta)
            endpoint_previous = endpoint
    if not outputs:
        raise ValueError("No eligible chronological trajectory bins")
    return pd.DataFrame(outputs)


def _trajectory_parquet_schemas(sources: list[dict[str, Any]], root: Path) -> tuple[Any, Any]:
    """Fix nullable storage types before streaming any large trajectory arrays.

    Binning rebuilds scalar metadata records, so an all-missing first ROI loses
    its pandas string dtype. Its inferred Arrow null type cannot define the
    dataset schema. Canonical types and the small, complete metadata roster do.
    """
    import pyarrow as pa
    text = {"source_file", "experiment_id", "animal_id", "genotype", "intervention", "control_id", "sex",
            "outcome_status", "coordinate_units", "timestamp_provenance", "analysis_role"}
    integer = {"roi_id"}
    boolean = {"tracking_valid"}
    numeric = {"exposure_status", "exposure_elapsed_at_start_hours", "record_time_s", "x", "y", "geometry", "food_end"}
    types = {name: pa.large_string() for name in text}
    types.update({name: pa.int64() for name in integer})
    types.update({name: pa.bool_() for name in boolean})
    types.update({name: pa.float64() for name in numeric})
    optional: dict[str, Any] = {}
    for source in sources:
        metadata = read_table(root / source["metadata"], source.get("sheet"))
        metadata = metadata.rename(columns={value: key for key, value in source.get("metadata_columns", {}).items()})
        if source.get("native_roi_count"):
            metadata, _, _ = _validated_roi_metadata(metadata, int(source["native_roi_count"]), source)
        elif source.get("roi_column", "ROI") in metadata:
            # Explicit blank ROI rows are outside the native adapter's roster;
            # their footer labels cannot determine measured covariate types.
            identities = metadata[source.get("roi_column", "ROI")]
            metadata = metadata.loc[identities.notna() & ~identities.astype(str).str.strip().eq("")]
        for field in ("Status", "Last_mvmt", "Best_tau"):
            if field in metadata:types[f"annotation_{field.lower()}"] = pa.large_string()
        for name in source.get("observed_covariates", []):
            if name.lower() in FORBIDDEN_PREDICTORS:raise ValueError(f"Forbidden future-derived covariate {name}")
            if name not in metadata:raise ValueError(f"Missing declared source covariate {name}")
            if name in types:continue
            observed = metadata[name].dropna()
            # An all-missing Excel column often has a float dtype even if a
            # later source measures text. Missing data imply no observed type.
            inferred = (pa.Table.from_pandas(observed.to_frame(), preserve_index=False).schema.field(name).type
                        if len(observed) else pa.null())
            previous = optional.get(name, pa.null())
            optional[name] = pa.unify_schemas([pa.schema([(name, previous)]), pa.schema([(name, inferred)])],
                promote_options="permissive").field(name).type
    types.update(optional)
    order = [*CANONICAL, *sorted(set(types)-set(CANONICAL))]
    native = pa.schema([pa.field(name, types[name], nullable=True) for name in order])
    binned = pa.schema([*native, *[pa.field(name, kind, nullable=True) for name, kind in [
        ("state_bin_seconds", pa.float64()), ("missing_fraction", pa.float64()), ("observed", pa.bool_()),
        ("dx", pa.float64()), ("dy", pa.float64()), ("path_length", pa.float64()), ("n_one_second_bins", pa.int64())]]])
    return native, binned


def _trajectory_arrow_table(frame: pd.DataFrame, schema: Any) -> Any:
    import pyarrow as pa
    extra = set(frame)-set(schema.names)
    if extra:raise ValueError(f"Undeclared chronological storage columns: {sorted(extra)}")
    table = frame.reindex(columns=schema.names).copy()
    for field in schema:
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            table[field.name] = table[field.name].astype("string")
    # Explicit typed nulls preserve missing observations. Safe casting refuses
    # incompatible measured values rather than substituting them or dropping rows.
    return pa.Table.from_pandas(table, schema=schema, preserve_index=False, safe=True)


def to_sequences(bins: pd.DataFrame, config: dict[str, Any] | None = None,
                  exclusions: list[dict[str, Any]] | None = None) -> list[Any]:
    from .semimarkov import Sequence
    config = config or {}
    covariates = config.get("covariates", ["circadian_sin", "circadian_cos", "previous_y", "previous_missing"])
    modulation = config.get("modulation", ["elapsed_exposure_hours"])
    for name in [*covariates, *modulation]:
        low = name.lower()
        if low in FORBIDDEN_PREDICTORS or low.startswith("annotation_") or "death" in low:
            raise ValueError(f"Future-information predictor is prohibited: {name}")
    sequences = []
    for (experiment, animal), group in bins.groupby(["experiment_id", "animal_id"], sort=False):
        group = group.sort_values("record_time_s").copy()
        times = group.record_time_s.to_numpy(float)
        dimension = 2 if group.dx.notna().any() else 1
        columns = ["dx", "dy"] if dimension == 2 else ["dy"]
        y = group[columns].to_numpy(float)
        observed = group.observed.to_numpy(bool) & np.isfinite(y).all(1)
        if not observed.any():
            if exclusions is not None:
                exclusions.append({"experiment_id": str(experiment), "animal_id": str(animal),
                                   "reason": "No eligible observed window after tracking/missingness checks",
                                   "n_masked_bins": len(group)})
                continue
            raise ValueError(f"No eligible observed windows for {animal}")
        previous = np.vstack([np.zeros((1, dimension)), np.nan_to_num(y[:-1])])
        group["circadian_sin"] = np.sin(2 * np.pi * times / 86400)
        group["circadian_cos"] = np.cos(2 * np.pi * times / 86400)
        group["elapsed_hours"] = (times - times[0]) / 3600
        if "elapsed_exposure_hours" in modulation:
            if not np.isfinite(group.exposure_status).all() or not set(group.exposure_status).issubset({0, 1}):
                raise ValueError("Primary infection progression needs audited exposed/unexposed Rx coding; eventual outcome cannot supply it")
            offset = group.get("exposure_elapsed_at_start_hours", pd.Series(np.nan, index=group.index))
            exposed = group.exposure_status.eq(1)
            if not np.isfinite(offset[exposed]).all() or (offset[exposed] < 0).any():
                raise ValueError("Exposed animals need the audited recording-start offset from exposure; Day3 filenames alone do not establish its clock origin")
            group["elapsed_exposure_hours"] = (group.elapsed_hours + offset.fillna(0)) * group.exposure_status
        group["previous_y"] = group.y.shift(1)
        group["previous_x"] = group.x.shift(1)
        # First displacement is masked; its known initial endpoint anchors the
        # subsequent forecast/simulation without using any future displacement.
        group.loc[group.index[0], "previous_y"] = group.y.iloc[0]
        group.loc[group.index[0], "previous_x"] = group.x.iloc[0]
        group["previous_y"] = group.previous_y.fillna(0)
        group["previous_x"] = group.previous_x.fillna(0)
        group["previous_missing"] = np.r_[1, ~observed[:-1]].astype(float)
        missing = set([*covariates, *modulation]) - set(group)
        if missing:
            raise ValueError(f"Missing declared past-only features: {sorted(missing)}")
        x, m = group[covariates].to_numpy(float), group[modulation].to_numpy(float)
        if not np.isfinite(x).all() or not np.isfinite(m).all():
            raise ValueError("Missing source context must be resolved or the estimand reduced before fitting")
        food = np.zeros_like(y)
        if group.food_end.notna().all():
            food[:, -1] = group.food_end.to_numpy(float) - group.previous_y.to_numpy(float)
        else:
            raise ValueError("Food-end geometry missing; cannot silently omit its primary drift term")
        axis = np.zeros_like(y)
        angle = config.get("arena_axis", {}).get(str(experiment))
        if angle is not None and dimension == 2:
            axis[:] = [math.cos(float(angle)), math.sin(float(angle))]
        else:
            # Measured y is the arena's vertical/longitudinal coordinate.
            axis[:, -1] = 1
        sequence = Sequence(str(animal), str(experiment), y, x, m, previous,
                            food, axis, observed, times,
                            float(group.state_bin_seconds.iloc[0]),
                            diagnostics={"covariate_names": covariates,
                                         "modulation_names": modulation,
                                         "path_length": group.path_length.to_numpy(float),
                                         "initial_position": ([float(group.previous_x.iloc[0]), float(group.previous_y.iloc[0])]
                                                              if dimension == 2 else [float(group.previous_y.iloc[0])]),
                                         "initial_position_known": bool(np.isfinite(group.y.iloc[0]) and
                                             (dimension == 1 or np.isfinite(group.x.iloc[0])))})
        sequence.validate()
        sequences.append(sequence)
    if not sequences:
        raise ValueError("No independent animal sequences")
    return sequences


def grouped_folds(sequences: list[Any], inner_folds: int = 3) -> list[tuple[list[Any], list[Any]]]:
    experiments = sorted({s.experiment_id for s in sequences})
    if len(experiments) < 2:
        raise ValueError("Grouped validation requires at least two independent experiments")
    n = min(inner_folds, len(experiments))
    return [([s for s in sequences if s.experiment_id not in set(experiments[i::n])],
             [s for s in sequences if s.experiment_id in set(experiments[i::n])])
            for i in range(n)]


def _comparator_network(kind: str, dimension: int, context_features: int,
                         hidden_size: int = 32, dropout: float = 0) -> Any:
    import torch
    from torch import nn
    from .semimarkov import student_t_logpdf
    class PredictiveNetwork(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.kind = kind
            if kind == "GRU":
                self.recurrent = nn.GRU(dimension + 1, hidden_size, batch_first=True)
                self.dropout = nn.Dropout(dropout)
                self.head = nn.Linear(hidden_size + context_features, 2 * dimension + (dimension == 2))
            elif kind == "AR":
                self.head = nn.Linear(60 * (dimension + 1) + context_features, dimension)
                self.raw_scale = nn.Parameter(torch.zeros(dimension))
                self.raw_correlation = nn.Parameter(torch.zeros(()))
            else:
                raise ValueError("Comparator must be GRU or regularized AR")

        def forward(self, history: Any, context: Any, output_scale: Any) -> tuple[Any, Any]:
            if self.kind == "GRU":
                _, hidden = self.recurrent(history)
                prediction = self.head(torch.cat([self.dropout(hidden[-1]), context], -1))
                mean = prediction[:, :dimension]
                sd = torch.nn.functional.softplus(prediction[:, dimension:2 * dimension]).clamp_min(1e-4)
                rho = .95 * prediction[:, -1].tanh() if dimension == 2 else None
            else:
                mean = self.head(torch.cat([history.flatten(1), context], -1))
                sd = torch.nn.functional.softplus(self.raw_scale).clamp_min(1e-4).expand(len(mean), -1)
                rho = .95 * self.raw_correlation.tanh().expand(len(mean)) if dimension == 2 else None
            mean, sd = mean * output_scale, sd * output_scale
            if dimension == 2:
                zero = torch.zeros_like(sd[:, 0])
                chol = torch.stack([torch.stack([sd[:, 0], zero], -1),
                                    torch.stack([rho * sd[:, 1], torch.sqrt(1 - rho.square()) * sd[:, 1]], -1)], -2)
            else:
                chol = sd[:, :, None]
            return mean, chol
    model = PredictiveNetwork()
    if sum(p.numel() for p in model.parameters()) > 100000:
        raise ValueError("Neural comparator exceeds the proposal's 100,000-parameter cap")
    return model


def _history_dataset(sequences: list[Any], output_scale: np.ndarray,
                      warmup_seconds: float = 0, history_bins: int = 60) -> Any:
    import torch
    from torch.utils.data import Dataset
    class HistoryDataset(Dataset):
        def __init__(self) -> None:
            self.indices = [(i, t) for i, s in enumerate(sequences)
                            for t in np.flatnonzero(s.observed & (s.times - s.times[0] >= warmup_seconds))]
        def __len__(self) -> int:
            return len(self.indices)
        def __getitem__(self, index: int) -> tuple[Any, Any, Any]:
            i, t = self.indices[index]
            s = sequences[i]
            dimension = s.observations.shape[1]
            history = np.zeros((history_bins, dimension + 1), dtype=np.float32)
            history[:, -1] = 1  # missingness mask includes unavailable padding.
            start = max(t - history_bins, 0)
            length = t - start
            if length:
                history[-length:, :dimension] = np.nan_to_num(s.observations[start:t]) / output_scale
                history[-length:, -1] = ~s.observed[start:t]
            context = np.r_[s.covariates[t], s.modulation[t], s.food_vector[t] / output_scale, s.arena_vector[t]]
            return (torch.as_tensor(history), torch.as_tensor(context, dtype=torch.float32),
                    torch.as_tensor(s.observations[t], dtype=torch.float32))
    return HistoryDataset()


class ComparatorFit:
    def __init__(self, model: Any, scaler: Any, output_scale: np.ndarray,
                 constructor: dict[str, Any], diagnostics: dict[str, Any]):
        self.model, self.scaler, self.output_scale = model, scaler, output_scale
        self.constructor, self.diagnostics = constructor, diagnostics

    def score(self, sequence: Any, warmup_seconds: float = 600) -> dict[str, Any]:
        import torch
        from torch.utils.data import DataLoader
        from .semimarkov import student_t_logpdf
        dataset = _history_dataset([self.scaler.transform(sequence)], self.output_scale, warmup_seconds)
        if not len(dataset):
            raise ValueError("Comparator has no observed targets after its warm-up")
        device = next(self.model.parameters()).device
        output_scale = torch.as_tensor(self.output_scale, device=device, dtype=torch.float32)
        values = []
        self.model.eval()
        with torch.no_grad():
            for history, context, target in DataLoader(dataset, batch_size=512, shuffle=False):
                mean, chol = self.model(history.to(device), context.to(device), output_scale)
                values.append(student_t_logpdf(target.to(device), mean, chol).cpu().numpy())
        return {"animal_id": sequence.animal_id, "experiment_id": sequence.experiment_id,
                "model": self.constructor["kind"], "n_scored": len(dataset),
                "log_score": float(np.concatenate(values).mean())}

    def save(self, path: Path) -> None:
        import torch
        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({"state_dict": self.model.state_dict(), "constructor": self.constructor,
                    "scaler": asdict(self.scaler), "output_scale": self.output_scale.tolist(),
                    "diagnostics": self.diagnostics}, path)

    @classmethod
    def load(cls, path: Path, device: str = "cpu") -> Any:
        import torch
        from .semimarkov import TrainingScaler
        value = torch.load(path, map_location=device, weights_only=False)
        model = _comparator_network(**value["constructor"]).to(device)
        model.load_state_dict(value["state_dict"])
        return cls(model, TrainingScaler(**value["scaler"]), np.asarray(value["output_scale"]),
                   value["constructor"], value["diagnostics"])


def simulate_comparator(fit: ComparatorFit, template: Any, seed: int, output_gain: float = 0,
                        modulation_shift: float = 0, modulation_column: int = 0) -> Any:
    """Frozen nonlinear predictor with a one-amplitude development-covariate shift."""
    import torch
    sequence = fit.scaler.transform(template)
    information = sequence.diagnostics or {}
    names = information.get("covariate_names", [])
    mean = np.asarray(information.get("covariate_mean", np.zeros(sequence.covariates.shape[1])))
    sd = np.asarray(information.get("covariate_scale", np.ones(sequence.covariates.shape[1])))
    d, n = sequence.observations.shape[1], len(sequence.times)
    position = np.asarray(information.get("initial_position", np.zeros(d)), float).copy()
    food_target = position + sequence.food_vector[0]
    food_axes = np.any(np.abs(sequence.food_vector) > 0, axis=0)
    generated = np.zeros((n, d), dtype=float)
    rng = np.random.default_rng(seed)
    device = next(fit.model.parameters()).device
    scale = torch.as_tensor(fit.output_scale, dtype=torch.float32, device=device)
    fit.model.eval()
    with torch.no_grad():
        for t in range(n):
            if t == 0 and not sequence.observed[0] and information.get("initial_position_known"):
                continue
            context = sequence.covariates[t].copy()
            for j, coordinate in enumerate(("previous_x", "previous_y") if d == 2 else ("previous_y",)):
                if coordinate in names:
                    index = names.index(coordinate)
                    context[index] = (position[j] - mean[index]) / sd[index]
            history = np.zeros((60, d + 1), dtype=np.float32)
            history[:, -1] = 1
            start = max(0, t - 60)
            length = t - start
            if length:
                valid = sequence.observed[start:t]
                history[-length:, :d] = np.where(valid[:, None], generated[start:t] / fit.output_scale, 0)
                history[-length:, -1] = ~valid
            modulation = sequence.modulation[t].copy()
            if not 0 <= modulation_column < len(modulation):
                raise ValueError("Frozen unrestricted modulation column is out of bounds")
            modulation[modulation_column] += float(modulation_shift)
            context = np.r_[context, modulation, np.where(food_axes, food_target - position, 0.) / fit.output_scale,
                            sequence.arena_vector[t]]
            conditional_mean, chol = fit.model(torch.as_tensor(history[None], device=device),
                torch.as_tensor(context[None], dtype=torch.float32, device=device), scale)
            gain = math.exp(float(output_gain))
            generated[t] = gain * (conditional_mean[0].cpu().numpy() +
                chol[0].cpu().numpy() @ rng.standard_normal(d) / math.sqrt(rng.chisquare(5) / 5))
            position += generated[t]
    result = copy.deepcopy(template)
    result.observations = generated
    result.observations[~result.observed] = np.nan
    return result


def fit_comparator(train: list[Any], kind: str, config: dict[str, Any],
                    validation: list[Any] | None = None) -> ComparatorFit:
    import torch
    from torch.utils.data import DataLoader
    from .semimarkov import TrainingScaler, assert_disjoint, student_t_logpdf
    if validation:
        assert_disjoint(train, validation, experiment=True)
    scaler = TrainingScaler.fit(train)
    scaled = [scaler.transform(s) for s in train]
    observed = np.concatenate([s.observations[s.observed] for s in train])
    output_scale = np.maximum(np.std(observed, axis=0), 1e-4)
    constructor = {"kind": kind, "dimension": observed.shape[1],
                   "context_features": len(scaler.mean) + len(scaler.modulation_mean) + 2 * observed.shape[1],
                   "hidden_size": int(config.get("hidden_size", 32)),
                   "dropout": float(config.get("dropout", 0))}
    device = config.get("comparator_device", config.get("device", "cpu"))
    if str(device) == "cpu":
        torch.set_num_threads(int(config.get("threads", 1)))
    dataset = _history_dataset(scaled, output_scale)
    if not len(dataset):
        raise ValueError("No observed comparator targets")
    candidates = []
    for start in range(int(config.get("starts", 5))):
        torch.manual_seed(int(config.get("seed", 0)) + start)
        model = _comparator_network(**constructor).to(device)
        optimizer = torch.optim.Adam(model.parameters(), lr=float(config.get("learning_rate", .001)))
        loader = DataLoader(dataset, batch_size=int(config.get("comparator_batch_size", 512)), shuffle=True)
        scale_tensor = torch.as_tensor(output_scale, dtype=torch.float32, device=device)
        best, stale, best_state = np.inf, 0, None
        history_records = []
        for epoch in range(int(config.get("max_epochs", 500))):
            model.train()
            total, count = 0., 0
            for history, context, target in loader:
                optimizer.zero_grad()
                mean, chol = model(history.to(device), context.to(device), scale_tensor)
                prior = .5 * sum((p / 1.5).square().sum() for p in model.parameters()) / len(dataset)
                loss = -student_t_logpdf(target.to(device), mean, chol).mean() + prior
                if not torch.isfinite(loss):
                    raise FloatingPointError("Nonfinite comparator loss")
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), 5)
                optimizer.step()
                total += float(loss.detach().cpu()) * len(target)
                count += len(target)
            fit = ComparatorFit(model, scaler, output_scale, constructor, {})
            if validation:
                rows = pd.DataFrame([fit.score(s, config.get("warmup_seconds", 600)) for s in validation])
                criterion = -float(rows.groupby("experiment_id").log_score.mean().mean())
            else:
                criterion = total / count
            history_records.append({"epoch": epoch, "objective": total / count, "criterion": criterion})
            if criterion < best - 1e-7:
                best, stale, best_state = criterion, 0, copy.deepcopy(model.state_dict())
            else:
                stale += 1
            if stale >= int(config.get("patience", 30)):
                break
        model.load_state_dict(best_state)
        iterations = int(config.get("lbfgs_iterations", 200))
        if iterations:
            # Deterministic full-data refinement uses the same declared budget as
            # the interpretable models; dropout is disabled for the objective.
            model.eval()
            refinement = torch.optim.LBFGS(model.parameters(), max_iter=iterations, line_search_fn="strong_wolfe")
            deterministic_loader = DataLoader(dataset, batch_size=int(config.get("comparator_batch_size", 512)), shuffle=False)
            def closure() -> Any:
                refinement.zero_grad()
                prior = .5 * sum((p / 1.5).square().sum() for p in model.parameters()) / len(dataset)
                prior.backward()
                total = prior.detach().clone()
                for history, context, target in deterministic_loader:
                    mean, chol = model(history.to(device), context.to(device), scale_tensor)
                    loss = -student_t_logpdf(target.to(device), mean, chol).sum() / len(dataset)
                    loss.backward()
                    total = total + loss.detach()
                return total
            refinement.step(closure)
            if validation:
                refined = ComparatorFit(model, scaler, output_scale, constructor, {})
                rows = pd.DataFrame([refined.score(s, config.get("warmup_seconds", 600)) for s in validation])
                refined_criterion = -float(rows.groupby("experiment_id").log_score.mean().mean())
                if refined_criterion > best:
                    model.load_state_dict(best_state)
                else:
                    best = refined_criterion
        candidates.append((best, copy.deepcopy(model), history_records))
    candidates.sort(key=lambda x: x[0])
    best, model, records = candidates[0]
    return ComparatorFit(model, scaler, output_scale, constructor,
                         {"selection_criterion": best, "history": records,
                          "training_animals": scaler.training_animals,
                          "parameter_count": sum(p.numel() for p in model.parameters()),
                          "history_bins": 60, "observation_distribution": "Student-t df=5"})


def nested_validation(sequences: list[Any], config: dict[str, Any], output: Path,
                      outer_experiment: str | None = None) -> pd.DataFrame:
    from .semimarkov import assert_disjoint, fit_model
    output.mkdir(parents=True, exist_ok=True)
    experiments = sorted({s.experiment_id for s in sequences})
    minimum = int(config.get("minimum_experiments", 10))
    if len(experiments) < minimum:
        raise ValueError(f"G3 failed: {len(experiments)} independent experiments < {minimum}")
    models = [*config.get("models", ["M0", "MG", "MT", "MD", "MX"]),
              *config.get("comparators", ["AR", "HMM", "GRU"])]
    targets = [outer_experiment] if outer_experiment else experiments
    records, selection_records = [], []
    for held_out in targets:
        if held_out not in experiments:
            raise ValueError(f"Unknown outer experiment {held_out}")
        train = [s for s in sequences if s.experiment_id != held_out]
        test = [s for s in sequences if s.experiment_id == held_out]
        assert_disjoint(train, test, experiment=True)
        for model in models:
            settings = []
            model_settings = []
            if model == "GRU":
                model_settings = [{"states": 2, "hidden_size": hidden, "dropout": dropout}
                    for hidden in config.get("neural_hidden_sizes", config.get("hidden_sizes", [16, 32, 64]))
                    for dropout in config.get("neural_dropout", config.get("dropouts", [0, .1]))]
            else:
                model_settings = [{"states": states} for states in
                                   ([2] if model in ("AR", "HMM") else config.get("state_counts", [2]))]
            def fit_candidate(units: list[Any], candidate: dict[str, Any], valid: list[Any] | None = None) -> Any:
                if model in ("AR", "GRU"):
                    return fit_comparator(units, model, candidate, valid)
                fit = fit_model(units, "MX" if model == "HMM" else model,
                                dict(candidate, hmm_baseline=True) if model == "HMM" else candidate,
                                validation=valid)
                if model == "HMM":
                    fit.diagnostics["display_model"] = "HMM"
                return fit
            for structural in model_settings:
                for rate in config.get("learning_rates", [.0003, .001, .003]):
                    candidate = dict(config, learning_rate=rate, **structural)
                    states = structural["states"]
                    scores = []
                    for inner_index, (inner_train, inner_test) in enumerate(grouped_folds(train, int(config.get("inner_folds", 3)))):
                        fit = fit_candidate(inner_train, candidate, inner_test)
                        rows = pd.DataFrame([fit.score(s, config.get("warmup_seconds", 600))
                                             for s in inner_test])
                        scores.extend(rows.groupby("experiment_id").log_score.mean().tolist())
                        fit.save(output / f"outer-{held_out}" / model /
                                 f"inner-{inner_index}-k{states}-h{candidate.get('hidden_size', 0)}-d{candidate.get('dropout', 0)}-lr{rate}.pt")
                    score = float(np.mean(scores))
                    settings.append((score, candidate))
                    selection_records.append({"outer_experiment": held_out, "model": model,
                        "states": states, "learning_rate": rate, "hidden_size": candidate.get("hidden_size"),
                        "dropout": candidate.get("dropout"), "inner_log_score": score})
            settings.sort(key=lambda item: (-item[0], item[1]["states"]))
            best = settings[0][1]
            fit = fit_candidate(train, best)
            if model not in ("AR", "GRU") and fit.diagnostics["converged_starts"] < int(config.get("minimum_agreeing_starts", 2)):
                fit = fit_candidate(train, dict(best, starts=int(config.get("final_starts", 20))))
            fit.save(output / f"outer-{held_out}" / f"{model}.pt")
            for sequence in test:
                record = fit.score(sequence, config.get("warmup_seconds", 600))
                record.update(model=model, outer_fold=held_out, states=best["states"], learning_rate=best["learning_rate"])
                records.append(record)
    destination = output / f"outer-{outer_experiment}" if outer_experiment else output
    destination.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(selection_records).to_csv(destination / "inner_selection.tsv", sep="\t", index=False)
    result = pd.DataFrame(records)
    result.to_csv(destination / "model_comparison.tsv", sep="\t", index=False)
    return result


def aggregate_outer_folds(output: Path, expected_experiments: list[str]) -> pd.DataFrame:
    output = Path(output)
    scores, inner = [], []
    for experiment in sorted(expected_experiments):
        directory = output / f"outer-{experiment}"
        fold_scores = read_table(directory / "model_comparison.tsv")
        fold_inner = read_table(directory / "inner_selection.tsv")
        if set(fold_scores.experiment_id.astype(str)) != {str(experiment)} or set(fold_inner.outer_experiment.astype(str)) != {str(experiment)}:
            raise ValueError("Fold output does not match its held-out independent experiment")
        scores.append(fold_scores)
        inner.append(fold_inner)
    combined, selection = pd.concat(scores, ignore_index=True), pd.concat(inner, ignore_index=True)
    if combined.duplicated(["experiment_id", "animal_id", "model"]).any():
        raise ValueError("Repeated animal/model predictions across fold outputs")
    combined.to_csv(output / "model_comparison.tsv", sep="\t", index=False)
    selection.to_csv(output / "inner_selection.tsv", sep="\t", index=False)
    return combined


def freeze_development(sequences: list[Any], config: dict[str, Any], root: Path,
                       model_directory: Path) -> dict[str, Any]:
    """Refit development-selected models before inspecting intervention outcomes."""
    from .core import atomic_json, canonical_hash, digest, source_identity
    from .semimarkov import fit_model
    from .interventions import summarize
    root, model_directory = Path(root), Path(model_directory)
    settings = config.get("behaviour", {})
    inner_path = model_directory / "inner_selection.tsv"
    inner = read_table(inner_path)
    expected = {s.experiment_id for s in sequences}
    if set(inner.outer_experiment.astype(str)) != expected:
        raise ValueError("Freeze needs completed inner selection for every development experiment")
    models = ["MG", "MT", "MX", "GRU"]
    candidate_scores = inner[inner.model.isin(["MT", "MX"])].groupby(["outer_experiment", "model"]).inner_log_score.max().unstack()
    candidate = "MT" if candidate_scores.MT.mean() >= candidate_scores.MX.mean() else "MX"
    speed = np.concatenate([np.linalg.norm(s.observations[s.observed], axis=1) / s.bin_seconds for s in sequences])
    threshold = float(np.quantile(speed[speed > 0], .5))
    if not np.isfinite(threshold) or threshold <= 0:
        raise ValueError("Development movements do not identify a finite positive operational threshold")
    summaries = pd.DataFrame([summarize(s, threshold, float(config.get("interventions", {}).get("duration_horizon", 600)))
        for s in sequences])
    components = ["movement_entry", "bout_rmst", "occupancy", "conditional_speed"]
    scales = {component: float(summaries[component].std(ddof=1)) for component in components}
    if any(not np.isfinite(value) or value <= 0 for value in scales.values()):
        raise ValueError("Development data cannot identify all primary component scales")
    freeze_directory = root / "results" / "behaviour" / "frozen_development"
    freeze_directory.mkdir(parents=True, exist_ok=True)
    paths, selected_hyperparameters = {}, {}
    for model in models:
        rows = inner[inner.model == model]
        grouping = ["states", "learning_rate"] + (["hidden_size", "dropout"] if model == "GRU" else [])
        means = rows.groupby(grouping, dropna=False).inner_log_score.mean().sort_values(ascending=False)
        if means.empty:
            raise ValueError(f"No grouped inner-development selection available for {model}")
        values = means.index[0]
        hyperparameters = dict(zip(grouping, values if isinstance(values, tuple) else (values,)))
        for key in ("states", "hidden_size"):
            if key in hyperparameters:
                hyperparameters[key] = int(hyperparameters[key])
        candidate_settings = dict(settings, **hyperparameters)
        fit = (fit_comparator(sequences, model, candidate_settings) if model == "GRU"
               else fit_model(sequences, model, candidate_settings))
        if model != "GRU" and fit.diagnostics["converged_starts"] < int(settings.get("minimum_agreeing_starts", 2)):
            fit = fit_model(sequences, model, dict(candidate_settings, starts=int(settings.get("final_starts", 20))))
        path = freeze_directory / f"{model}.pt"
        fit.save(path)
        paths[model] = str(path.relative_to(root))
        selected_hyperparameters[model] = hyperparameters
    record = {"status": "development_only_frozen", "primary_candidate": candidate,
        "unrestricted_reference": "GRU", "frozen_fits": paths, "movement_threshold": threshold,
        "movement_threshold_rule": "Positive displacement speeds' median in development animals; fixed before targeted outcomes",
        "component_scales": scales, "hyperparameters": selected_hyperparameters,
        "training_splits": {"training_animals": sorted(s.animal_id for s in sequences), "training_experiments": sorted(expected),
                            "excluded_role": "targeted intervention"},
        "config_sha256": canonical_hash(config), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "source_result_paths": [str(inner_path.resolve())] + [str((root / path).resolve()) for path in paths.values()]}
    record["source_result_hashes"] = {path: digest(Path(path)) for path in record["source_result_paths"]}
    atomic_json(root / "manifests" / "wp1_development_freeze.json", record)
    export_parameter_readiness(root, config, paths)
    return record


def export_parameter_readiness(root: Path, config: dict[str, Any], fits: dict[str, str] | None = None) -> dict[str, Any]:
    from .core import atomic_json, canonical_hash, digest, source_identity
    from .semimarkov import Fit
    root = Path(root)
    fits = fits or config.get("interventions", {}).get("frozen_fits", {})
    parameters, training_splits, paths = {}, {}, []
    for name, specified in fits.items():
        if name not in ("M0", "MG", "MT", "MD", "MX"):
            continue
        path = root / specified
        fit = Fit.load(path)
        if fit.diagnostics.get("benchmark_only"):
            raise ValueError("Pilot fits cannot supply biological parameter evidence")
        parameters[name] = {"transition_modulation": fit.model.transition_mod.detach().cpu().tolist(),
            "moving_gain_modulation": fit.model.gain_mod.detach().cpu().tolist(),
            "drift_rotation_modulation": fit.model.direction_mod.detach().cpu().tolist(),
            "modulation_mean": fit.scaler.modulation_mean, "modulation_scale": fit.scaler.modulation_scale}
        training_splits[name] = {"animals": fit.diagnostics.get("training_animals", fit.scaler.training_animals),
                                "experiments": fit.diagnostics.get("training_experiments", [])}
        paths.append(path)
    record = {"evidence_status": "pending_neural_bridge", "behavioural_parameters": parameters,
        "classes": {}, "unavailable_classes": ["input_gain", "recurrent_gate", "hidden_displacement", "output_gain"],
        "reason": "Identified behavioural hazards/gain/drift do not determine signs in neural input, recurrent, hidden-state or output parameter bases. A prospective bridge and development-only signature calibration are required.",
        "prospective_parameter_class_map": {"recurrent_gate": "transition hazard signature (bridge hypothesis)",
            "output_gain": "moving emission mean/covariance gain signature (bridge hypothesis)",
            "input_gain": "unavailable: cue valuation, perception and drift rotation are not separately identified",
            "hidden_displacement": "unavailable: semi-Markov occupancy does not identify neural hidden-state displacement"},
        "training_splits": training_splits, "config_sha256": canonical_hash(config),
        "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "source_result_paths": [str(path.resolve()) for path in paths],
        "source_result_hashes": {str(path.resolve()): digest(path) for path in paths}}
    atomic_json(root / "results" / "behaviour" / "biological_direction_readiness.json", record)
    return record


def diagnose_outer_folds(sequences: list[Any], config: dict[str, Any], output: Path,
                         outer_experiment: str | None = None) -> pd.DataFrame:
    from .core import canonical_hash
    from .semimarkov import Fit
    from .interventions import summarize
    settings = config.get("behaviour", config)
    experiments = sorted({s.experiment_id for s in sequences})
    rows = []
    for held_out in ([outer_experiment] if outer_experiment else experiments):
        train = [s for s in sequences if s.experiment_id != held_out]
        test = [s for s in sequences if s.experiment_id == held_out]
        if not train or not test:
            raise ValueError("Posterior checks need genuine held-out experiment and training animals")
        speed = np.concatenate([np.linalg.norm(s.observations[s.observed], axis=1) / s.bin_seconds for s in train])
        threshold = float(np.median(speed[speed > 0]))
        summaries = pd.DataFrame([summarize(s, threshold) for s in train])
        scales = {component: float(summaries[component].std(ddof=1))
                  for component in ("bout_rmst", "occupancy", "conditional_speed")}
        if any(not np.isfinite(value) or value <= 0 for value in scales.values()):
            raise ValueError("Training animals cannot identify diagnostic scales")
        training_sha = canonical_hash(sorted(s.animal_id for s in train))
        for name in ("MG", "MT", "MX"):
            fit = Fit.load(Path(output) / f"outer-{held_out}" / f"{name}.pt")
            if set(fit.scaler.training_animals) != {s.animal_id for s in train}:
                raise ValueError("Posterior-check checkpoint has a different training split")
            for sequence in test:
                row = trajectory_diagnostics(fit, sequence, threshold, scales,
                    int(settings.get("posterior_predictive_repetitions", 64)), int(settings.get("seed", 0)))
                row.update(movement_threshold=threshold, development_scale_hash=canonical_hash(scales),
                           training_animals_sha256=training_sha)
                rows.append(row)
    destination = Path(output) / f"outer-{outer_experiment}" if outer_experiment else Path(output)
    result = pd.DataFrame(rows)
    result.to_csv(destination / "posterior_predictive_checks.tsv", sep="\t", index=False)
    return result


def hierarchical_bootstrap_comparison(scores: pd.DataFrame, candidate: str,
                                       reference: str = "MG", repetitions: int = 10000,
                                       seed: int = 0) -> dict[str, Any]:
    required = ["experiment_id", "animal_id", "model", "log_score"]
    if scores.duplicated(required[:3]).any():
        raise ValueError("Repeated fold/animal score")
    wide = scores.pivot(index=["experiment_id", "animal_id"], columns="model", values="log_score")
    if candidate not in wide or reference not in wide or wide[[candidate, reference]].isna().any().any():
        raise ValueError("Incomplete paired model comparison")
    differences = wide[candidate] - wide[reference]
    by_experiment = {key: group.to_numpy() for key, group in differences.groupby(level=0)}
    names = list(by_experiment)
    if len(names) < 2:
        raise ValueError("Experiment-level uncertainty needs independent experiments")
    rng, draws = np.random.default_rng(seed), []
    for _ in range(repetitions):
        values = []
        for index in rng.integers(len(names), size=len(names)):
            animals = by_experiment[names[index]]
            values.append(rng.choice(animals, len(animals), replace=True).mean())
        draws.append(np.mean(values))
    estimates = np.asarray([values.mean() for values in by_experiment.values()])
    sign = stats.binomtest(int((estimates > 0).sum()), len(estimates), .5, alternative="greater")
    return {"candidate": candidate, "reference": reference,
            "estimate": float(estimates.mean()), "bootstrap_ci": np.quantile(draws, [.025, .975]).tolist(),
            "sign_test_p": float(sign.pvalue), "n_experiments": len(names),
            "leave_one_experiment_estimates": {name: float(np.delete(estimates, i).mean())
                                                for i, name in enumerate(names)}}


def select_primary_from_inner(scores: pd.DataFrame, inner: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    """Select MT/MX from inner validation only, before reading outer scores."""
    if {"outer_experiment", "model", "inner_log_score"} - set(inner):
        raise ValueError("Missing grouped inner-selection records")
    choices = {}
    for experiment, rows in inner.groupby("outer_experiment", sort=True):
        candidates = rows[rows.model.isin(["MT", "MX"])].copy()
        if set(candidates.model) != {"MT", "MX"} or not np.isfinite(candidates.inner_log_score).all():
            raise ValueError("Both prespecified selective alternatives need finite inner-validation scores")
        best = candidates.groupby("model").inner_log_score.max()
        choices[str(experiment)] = "MT" if best["MT"] >= best["MX"] else "MX"
    selected = []
    for experiment, model in choices.items():
        rows = scores[(scores.experiment_id.astype(str) == experiment) & (scores.model == model)].copy()
        if rows.empty:
            raise ValueError("Selected primary model has no corresponding outer-fold predictions")
        rows["selected_model"] = model
        rows["model"] = "selected_transition"
        selected.append(rows)
    return pd.concat(selected, ignore_index=True), choices


def trajectory_diagnostics(fit: Any, sequence: Any, threshold: float, scales: dict[str, float],
                            repetitions: int = 64, seed: int = 0, horizon: float = 600) -> dict[str, Any]:
    from .interventions import predicted_summary, summarize
    observed = summarize(sequence, threshold, horizon)
    # No additional intervention: the frozen fitted model generates trajectories
    # with the held-out clock/missingness and a causally updated position/history.
    direction = {"transition_shift": np.zeros(fit.model.states),
                 "gain_shift": np.zeros(fit.model.states - 1),
                 "direction_shift": np.zeros(fit.model.states - 1)}
    predicted = predicted_summary(fit, [sequence], 0., direction, threshold, repetitions, seed, horizon)
    components = ["bout_rmst", "occupancy", "conditional_speed"]
    if any(c not in scales or scales[c] <= 0 or not np.isfinite(scales[c]) for c in components):
        raise ValueError("Freeze diagnostic component scales on development animals")
    if any(not np.isfinite(observed[c]) or not np.isfinite(predicted[c]) for c in components):
        raise ValueError("A diagnostic component is not identifiable in this held-out animal")
    error = float(np.mean([((observed[c] - predicted[c]) / scales[c]) ** 2 for c in components]))
    return {"experiment_id": sequence.experiment_id, "animal_id": sequence.animal_id,
            "model": fit.model.model, "diagnostic_error": error,
            **{f"observed_{c}": observed[c] for c in components},
            **{f"predicted_{c}": predicted[c] for c in components}}


def write_h1(root: Path, config: dict[str, Any], model_directory: Path) -> dict[str, Any]:
    from .core import atomic_json, canonical_hash, digest, source_identity
    from .inference import experiment_weighted_contrast
    root, model_directory = Path(root), Path(model_directory)
    score_path, inner_path = model_directory / "model_comparison.tsv", model_directory / "inner_selection.tsv"
    scores, inner = read_table(score_path), read_table(inner_path)
    if scores.experiment_id.nunique() < int(config.get("behaviour", {}).get("minimum_experiments", 10)):
        raise ValueError("H1 full benchmark does not meet its independent-experiment G3 threshold")
    selected, choices = select_primary_from_inner(scores, inner)
    paired = pd.concat([selected, scores[scores.model == "MG"]]).pivot(
        index=["experiment_id", "animal_id"], columns="model", values="log_score")
    if paired.isna().any().any():
        raise ValueError("H1 needs complete same-animal selected-versus-gain predictions")
    paired["difference"] = paired.selected_transition - paired.MG
    result = experiment_weighted_contrast(paired.reset_index(),
        replicates=int(config.get("inference", {}).get("bootstrap_replicates", 10000)),
        seed=int(config.get("seed", 20261012)))
    recovery_path = root / "results" / "behaviour" / "recovery" / "recovery_summary.json"
    recovery = json.loads(recovery_path.read_text(encoding="utf-8")) if recovery_path.exists() else None
    false_positive = recovery.get("gain_only_false_selective_fraction") if recovery else None
    recovery_complete = bool(recovery and recovery.get("status") == "complete" and
        recovery.get("parameter_recovery_replicates") == int(config.get("behaviour", {}).get("parameter_recovery_replicates", 100)) and
        recovery.get("config_sha256") == canonical_hash(config) and recovery.get("source_sha256") == source_identity(Path(__file__).resolve().parents[2])["sha256"])
    if recovery_complete:
        for referenced in recovery.get("source_result_paths", []):
            if recovery.get("source_result_hashes", {}).get(referenced) != digest(Path(referenced)):
                raise ValueError("Recovery evidence changed after validation")
    recovery_passed = bool(recovery_complete and false_positive is not None and false_positive <=
        config.get("behaviour", {}).get("maximum_gain_false_selective_fraction", .05))
    identifiability_path = root / "results" / "behaviour" / "parameter_identifiability.json"
    identifiability = json.loads(identifiability_path.read_text(encoding="utf-8")) if identifiability_path.exists() else None
    if identifiability and (identifiability.get("config_sha256") != canonical_hash(config) or
        identifiability.get("source_sha256") != source_identity(Path(__file__).resolve().parents[2])["sha256"]):
        raise ValueError("Parameter identifiability evidence has a different configuration/source")
    identifiability_passed = bool(identifiability and identifiability.get("parameter_identifiability_passed") is True
        and identifiability.get("source_result_paths") and identifiability.get("source_result_hashes"))
    if identifiability:
        for path in identifiability["source_result_paths"]:
            if identifiability["source_result_hashes"].get(path) != digest(Path(path)):
                raise ValueError("Identifiability evidence changed after validation")
    supplemental = {}
    for label, filename in (("sensitivity", "sensitivity_checks.json"), ("bayesian", "bayesian_subset_diagnostics.json")):
        supplement_path = root / "results" / "behaviour" / filename
        supplement = json.loads(supplement_path.read_text(encoding="utf-8")) if supplement_path.exists() else None
        if supplement:
            if supplement.get("config_sha256") != canonical_hash(config) or supplement.get("source_sha256") != source_identity(Path(__file__).resolve().parents[2])["sha256"]:
                raise ValueError(f"{label} evidence has changed scientific configuration/source")
            for referenced in supplement.get("source_result_paths", []):
                if supplement.get("source_result_hashes", {}).get(referenced) != digest(Path(referenced)):
                    raise ValueError(f"{label} evidence input changed")
        supplemental[label] = (supplement_path, supplement)
    sensitivity_passed = bool(supplemental["sensitivity"][1] and supplemental["sensitivity"][1].get("status") == "complete" and supplemental["sensitivity"][1].get("sign_consistency") is True)
    bayesian_passed = bool(supplemental["bayesian"][1] and supplemental["bayesian"][1].get("adequate_mixing") is True)
    diagnostic_path = model_directory / "posterior_predictive_checks.tsv"
    diagnostic_improvement = None
    if diagnostic_path.exists():
        diagnostics = read_table(diagnostic_path)
        chosen = []
        for experiment, model in choices.items():
            part = diagnostics[(diagnostics.experiment_id.astype(str) == experiment) & (diagnostics.model == model)].copy()
            part["model"] = "selected_transition"
            chosen.append(part)
        wide = pd.concat([*chosen, diagnostics[diagnostics.model == "MG"]]).pivot(
            index=["experiment_id", "animal_id"], columns="model", values="diagnostic_error")
        if wide.isna().any().any() or not np.isfinite(wide.to_numpy()).all():
            raise ValueError("Incomplete multivariate posterior-predictive diagnostic comparison")
        means = wide.groupby(level=0).mean().mean()
        if means.MG <= 0:
            raise ValueError("A zero-error gain reference cannot support a diagnostic improvement ratio")
        diagnostic_improvement = float((means.MG - means.selected_transition) / means.MG)
    supported_components = bool(result["estimate"] > 0 and result["ci95"][0] > 0
                                and result["pvalue"] < .05 and recovery_passed and identifiability_passed
                                and diagnostic_improvement is not None and diagnostic_improvement >= .05 and sensitivity_passed and bayesian_passed)
    manifest = root / "manifests" / "source_manifest.json"
    if not manifest.exists():
        raise FileNotFoundError("A frozen actual source manifest is required for H1 provenance")
    result.update(claim_id="H1", evidence_class="biological_reanalysis",
        config_sha256=canonical_hash(config), source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"],
        data_manifest_sha256=digest(manifest), selected_by_outer_fold=choices,
        selection_rule="MT versus MX selected by grouped inner validation only; outer scores never select candidates",
        parameter_recovery_passed=recovery_passed, gain_false_selective_fraction=false_positive,
        parameter_recovery_status="complete" if recovery_complete else "pending: complete declared recovery roster required",
        parameter_identifiability_passed=identifiability_passed,
        parameter_identifiability_status=("complete" if identifiability and identifiability.get("status") == "complete" else
            "pending: parameter separation, bias/coverage evidence unavailable or incomplete"),
        posterior_check_status="complete" if diagnostic_improvement is not None else "pending: full posterior checks required",
        sensitivity_check_status=("complete" if supplemental["sensitivity"][1] and supplemental["sensitivity"][1].get("status") == "complete" else "pending: declared 5/10/20-second fits required"),
        sensitivity_check_passed=sensitivity_passed,
        bayesian_check_status="complete" if bayesian_passed else "pending: adequate representative posterior sampling required",
        bayesian_check_passed=bayesian_passed,
        multivariate_diagnostic_improvement=diagnostic_improvement,
        operational_gates_passed=bool(identifiability_passed and recovery_passed and diagnostic_improvement is not None and diagnostic_improvement >= .05 and sensitivity_passed and bayesian_passed),
        required_checks={"inner_only_candidate_selection": True, "complete_paired_outer_predictions": True,
            "parameter_identifiability": identifiability_passed,
            "gain_false_selective_recovery_passed": recovery_passed,
            "five_percent_multivariate_diagnostic_improvement": diagnostic_improvement is not None and diagnostic_improvement >= .05,
            "time_scale_sensitivity": sensitivity_passed, "adequate_posterior_sampling": bayesian_passed},
        claim_status="pending_joint_Holm" if supported_components else "unsupported_or_required_diagnostic_pending",
        multiplicity_status="H1/H2/H3 joint Holm adjustment pending",
        source_result_paths=[str(score_path.resolve()), str(inner_path.resolve())] +
            ([str(recovery_path.resolve())] if recovery_path.exists() else []) +
            ([str(identifiability_path.resolve())] if identifiability_path.exists() else []) +
            ([str(diagnostic_path.resolve())] if diagnostic_path.exists() else []) +
            [str(path.resolve()) for path, document in supplemental.values() if document is not None])
    result["source_result_hashes"] = {path: digest(Path(path)) for path in result["source_result_paths"]}
    atomic_json(root / "results" / "inference" / "H1.json", result)
    per_experiment = paired.difference.groupby(level=0).mean()
    panels = [{"kind": "scatter", "x": list(range(len(per_experiment))),
               "y": per_experiment.tolist(), "title": "Independent held-out experiments",
               "xlabel": "Experiment index", "ylabel": "Selected − gain predictive log score"}]
    if diagnostic_improvement is not None:
        panels.append({"kind": "scatter", "x": [0], "y": [diagnostic_improvement],
                       "title": "Multivariate trajectory checks", "xlabel": "Primary comparison",
                       "ylabel": "Relative diagnostic-error reduction"})
    atomic_json(root / "results" / "figure_inputs" / "02.json", {
        "config_sha256": result["config_sha256"], "source_sha256": result["source_sha256"],
        "evidence_class": "biological_reanalysis", "source_result_paths": result["source_result_paths"],
        "source_result_hashes": result["source_result_hashes"],
        "panels": panels,
        "caption": "Chronological fly predictions hold out complete experiments and animals. MT/MX selection uses grouped inner folds only. Each plotted point is one experiment with equal experiment weight. Gain can win; raw statistical evidence, parameter recovery and multivariate predictive checks are evaluated separately before joint multiplicity adjustment."})
    return result


class NonestimableDesignError(ValueError):
    def __init__(self, diagnostics: dict[str, Any]):
        self.diagnostics = diagnostics
        super().__init__("Nonestimable behavioural design: protected primary coefficients are confounded; priors do not identify them")


def _resolve_behavioural_design(x: np.ndarray, names: list[str], protected: list[str],
                               prior_covariance_unit: np.ndarray | None = None) -> tuple[np.ndarray, list[str], dict[str, Any]]:
    """Remove nuisance aliases only, carrying the original Gaussian prior.

    A protected coefficient is estimable iff every likelihood nullspace vector
    has zero at that coefficient. This tests the full original model before
    selecting any basis; selecting a convenient column cannot identify it.
    """
    x = np.asarray(x, float)
    if x.ndim != 2 or x.shape[1] != len(names) or not np.isfinite(x).all():
        raise ValueError("Invalid finite behavioural design matrix")
    _, singular, vh = np.linalg.svd(x, full_matrices=True)
    tolerance = max(x.shape) * np.finfo(float).eps * (singular[0] if len(singular) else 0.)
    rank = int(np.sum(singular > tolerance))
    nullspace = vh[rank:]
    protected_indices = [names.index(name) for name in protected if name in names]
    norms = {names[index]: float(np.linalg.norm(nullspace[:, index])) for index in protected_indices}
    estimable = all(value <= 1e-8 for value in norms.values())
    diagnostics = {"rows": len(x), "original_terms": names, "rank": rank, "columns": len(names),
        "rank_tolerance": float(tolerance), "singular_values": singular.tolist(),
        "protected_terms": [names[index] for index in protected_indices], "protected_nullspace_norms": norms,
        "protected_primary_estimable": estimable,
        "nullspace_relations": [{name: float(value) for name, value in zip(names, vector) if abs(value) > 1e-8} for vector in nullspace],
        "criterion": "Every original-model likelihood nullspace vector has zero protected-primary components; prior curvature is not used for identification"}
    if not estimable:
        raise NonestimableDesignError(diagnostics)
    covariance = np.eye(len(names)) if prior_covariance_unit is None else np.asarray(prior_covariance_unit, float)
    if covariance.shape != (len(names), len(names)) or not np.isfinite(covariance).all():
        raise ValueError("Invalid original coefficient prior covariance")
    if rank == len(names):
        selected, transform = list(range(len(names))), np.eye(len(names))
    else:
        selected = protected_indices.copy()
        for index in range(len(names)):
            if index not in selected and np.linalg.matrix_rank(x[:, selected + [index]]) > len(selected):
                selected.append(index)
        selected.sort()
        if len(selected) != rank:
            raise ValueError("Could not preserve the original behavioural model column space")
        transform = np.linalg.lstsq(x[:, selected], x, rcond=None)[0]
    reduced = x[:, selected]
    residual = float(np.max(np.abs(reduced @ transform - x))) if x.size else 0.
    if residual > 1e-9 * max(1., float(np.max(np.abs(x)))):
        raise ValueError("Nuisance-basis reduction changed the predictor column space")
    unit_covariance = transform @ covariance @ transform.T
    precision = np.linalg.inv(unit_covariance) / 2.25
    dropped = [index for index in range(len(names)) if index not in selected]
    diagnostics.update(kept_terms=[names[index] for index in selected],
        dropped_nuisance_terms=[names[index] for index in dropped],
        nuisance_aliases={names[index]: {names[chosen]: float(transform[row, index]) for row, chosen in enumerate(selected)
            if abs(transform[row, index]) > 1e-9} for index in dropped},
        original_column_transform=transform.tolist(), full_column_space_preserved=True,
        maximum_column_reconstruction_error=residual, prior_covariance_unit=unit_covariance.tolist(), prior_precision=precision.tolist(),
        prior_rule="Original Normal(0,1.5) prior pushed through the exact identifiable nuisance basis; no fresh independent prior replaces a duplicated context column")
    return reduced, diagnostics["kept_terms"], diagnostics


def design_matrix(frame: pd.DataFrame, continuous: list[str], categorical: list[str],
                  interaction: tuple[str, str] | None = None, *, protected_terms: list[str] | None = None,
                  return_diagnostics: bool = False) -> Any:
    parts = pd.DataFrame({"Intercept": np.ones(len(frame))}, index=frame.index)
    constant_covariates = {}
    for column in continuous:
        values = pd.to_numeric(frame[column], errors="raise").astype(float)
        if not np.isfinite(values).all():
            raise ValueError(f"Nonfinite {column}")
        # Covariates are standardized; binary infection remains 0/1 for the
        # interpretable infection-by-polarization coefficient.
        if values.nunique() == 1 and column in ("log_illuminance", "male_size", "female_size"):
            constant_covariates[column] = float(values.iloc[0])
            continue
        parts[column] = values if set(values.unique()).issubset({0, 1}) else (values - values.mean()) / max(values.std(ddof=0), 1e-10)
    for column in categorical:
        if frame[column].isna().any():
            raise ValueError(f"Missing categorical context {column}")
        codes = pd.get_dummies(frame[column].astype(str), prefix=column, drop_first=True, dtype=float)
        parts = pd.concat([parts, codes], axis=1)
    if interaction:
        a, b = interaction
        parts[f"{a}:{b}"] = parts[a] * parts[b]
    x = parts.to_numpy(float)
    protected = protected_terms if protected_terms is not None else continuous + ([f"{interaction[0]}:{interaction[1]}"] if interaction else [])
    result = _resolve_behavioural_design(x, parts.columns.tolist(), protected)
    result[2]["constant_training_covariates"] = constant_covariates
    return result if return_diagnostics else result[:2]


def _binary_map_value_gradient(theta: np.ndarray, x: np.ndarray, y: np.ndarray,
                               precision: np.ndarray, group_index: np.ndarray | None = None
                               ) -> tuple[float, np.ndarray]:
    """Current binary MAP objective and its exact noncentered gradient."""
    p = x.shape[1]
    beta = theta[:p]
    eta = x @ beta
    prior = .5 * beta @ precision @ beta
    if group_index is not None:
        z, sd = theta[p:-1], np.exp(theta[-1])
        random_predictor = z[group_index] * sd
        eta = eta + random_predictor
        prior += .5 * np.sum(z ** 2) + .5 * (sd / .5) ** 2 - theta[-1]
    residual = special.expit(eta) - y
    # Symmetrization gives the exact derivative of the scalar quadratic even
    # when inverse-covariance arithmetic leaves tiny antisymmetric roundoff.
    beta_gradient = x.T @ residual + .5 * (precision + precision.T) @ beta
    if group_index is None:
        gradient = beta_gradient
    else:
        group_gradient = sd * np.bincount(group_index, weights=residual, minlength=len(z)) + z
        log_sd_gradient = residual @ random_predictor + (sd / .5) ** 2 - 1
        gradient = np.concatenate([beta_gradient, group_gradient, [log_sd_gradient]])
    return float(np.sum(np.logaddexp(0, eta) - y * eta) + prior), gradient


def _multinomial_map_value_gradient(theta: np.ndarray, x: np.ndarray, y: np.ndarray,
                                    precision: np.ndarray) -> tuple[float, np.ndarray]:
    """Three-category sensitivity objective; unpolarized is the reference."""
    p = x.shape[1]
    logits = np.column_stack([x @ theta[:p], x @ theta[p:], np.zeros(len(x))])
    normalizer = special.logsumexp(logits, axis=1)
    probabilities = np.exp(logits - normalizer[:, None])
    symmetric_precision = .5 * (precision + precision.T)
    gradient = np.concatenate([
        x.T @ (probabilities[:, 0] - (y == 0)) + symmetric_precision @ theta[:p],
        x.T @ (probabilities[:, 1] - (y == 1)) + symmetric_precision @ theta[p:]])
    value = (np.sum(normalizer - logits[np.arange(len(x)), y])
             + .5 * theta[:p] @ precision @ theta[:p]
             + .5 * theta[p:] @ precision @ theta[p:])
    return float(value), gradient


def _map_binary(x: np.ndarray, y: np.ndarray, names: list[str], groups: np.ndarray | None = None,
                prior_precision: np.ndarray | None = None
                ) -> dict[str, Any]:
    if len(np.unique(y)) != 2:
        raise ValueError("Binary endpoint needs both observed categories")
    group_names = np.unique(groups) if groups is not None else np.array([])
    index = np.searchsorted(group_names, groups) if groups is not None else None
    p, g = x.shape[1], len(group_names)
    precision = np.eye(p) / 2.25 if prior_precision is None else np.asarray(prior_precision, float)
    initial = np.zeros(p + g + bool(g))
    if g:
        initial[-1] = -1.5
    result = optimize.minimize(_binary_map_value_gradient, initial, args=(x, y, precision, index),
        jac=True, method="BFGS", options={"maxiter": 2000, "gtol": 1e-6})
    if (not np.isfinite(result.fun) or not np.isfinite(result.x).all()
            or not np.isfinite(result.jac).all() or np.linalg.norm(result.jac) > .02):
        raise RuntimeError(f"Binary MAP did not converge: {result.message}")
    covariance = np.asarray(result.hess_inv)[:p, :p]
    estimates = result.x[:p]
    return {"terms": names, "coefficient": estimates.tolist(),
            "standard_error_laplace": np.sqrt(np.maximum(np.diag(covariance), 0)).tolist(),
            "covariance_laplace": covariance.tolist(), "n": len(y),
            "group_sd": float(np.exp(result.x[-1])) if g else None,
            "objective": float(result.fun), "converged": True,
            "gradient_method": "exact_analytic", "gradient_norm": float(np.linalg.norm(result.jac)),
            "uncertainty_method": "penalized MAP / Laplace approximation"}


def fit_mantis_choice(frame: pd.DataFrame, config: dict[str, Any] | None = None) -> dict[str, Any]:
    config = config or {}
    table = frame.rename(columns={v: k for k, v in config.get("columns", {}).items()}).copy()
    needed = {"response", "infection", "dop", "illuminance", "stimulus_side", "year", "box"}
    if needed - set(table):
        raise ValueError(f"Mantis choice schema needs {sorted(needed - set(table))}")
    codes = config.get("response_values", {"polarized": 1, "unpolarized": 2, "middle": 0})
    response = table.response.map(codes)
    if response.isna().any() or not set(response).issubset({0, 1, 2}):
        raise ValueError("Every mantis response must map explicitly to middle, polarized or unpolarized")
    if not set(table.infection).issubset({0, 1}):
        raise ValueError("Mantis infection coding must be validated 0/1")
    if (table.illuminance <= 0).any():
        raise ValueError("Log-illuminance requires positive recorded values")
    table["log_illuminance"] = np.log(table.illuminance)
    if config.get("dop_units") == "percent":
        table.dop = table.dop / 100
    elif config.get("dop_units") not in ("proportion", "source"):
        raise ValueError("Inspect and declare source degree-of-polarization units")
    primary = ["infection", "dop", "infection:dop"]
    x, names, design = design_matrix(table, ["infection", "dop", "log_illuminance"],
        ["stimulus_side", "year", "box"], ("infection", "dop"), protected_terms=primary, return_diagnostics=True)
    groups = (table.individual_id.astype(str).to_numpy()
              if "individual_id" in table and table.individual_id.duplicated().any() else None)
    chooser = response.to_numpy() != 0
    engagement = _map_binary(x, chooser.astype(float), names, groups, np.asarray(design["prior_precision"]))
    preference_x, preference_names, preference_design = _resolve_behavioural_design(
        x[chooser], names, primary, np.asarray(design["prior_covariance_unit"]))
    preference = _map_binary(preference_x, (response.to_numpy()[chooser] == 1).astype(float),
        preference_names, groups[chooser] if groups is not None else None, np.asarray(preference_design["prior_precision"]))
    # Multinomial sensitivity uses unpolarized as its reference category.
    y = response.to_numpy(int)
    p = x.shape[1]
    mn = optimize.minimize(_multinomial_map_value_gradient, np.zeros(p * 2),
        args=(x, y, np.asarray(design["prior_precision"])), jac=True, method="BFGS")
    if (not np.isfinite(mn.fun) or not np.isfinite(mn.x).all()
            or not np.isfinite(mn.jac).all() or np.linalg.norm(mn.jac) > .02):
        raise RuntimeError("Multinomial sensitivity fit did not converge")
    return {"engagement": engagement, "conditional_preference": preference,
            "design_audit": {"engagement": design, "conditional_preference": preference_design},
            "multinomial_sensitivity": {"terms": names, "reference": "unpolarized",
                "middle_coefficients": mn.x[:p].tolist(), "polarized_coefficients": mn.x[p:].tolist(),
                "covariance_laplace": np.asarray(mn.hess_inv).tolist(), "objective": float(mn.fun),
                "gradient_method": "exact_analytic", "gradient_norm": float(np.linalg.norm(mn.jac))},
            "denominators": {"trials": len(table), "choosers": int(chooser.sum()),
                              "nonchoices": int((~chooser).sum()),
                              "animals": int(table.individual_id.nunique()) if "individual_id" in table else None},
            "identification_limit": "Conditional preference cannot distinguish perception from value",
            "independence_verified": "individual_id" in table}


def _apply_training_design(matrix: np.ndarray, names: list[str], diagnostics: dict[str, Any]) -> tuple[np.ndarray, list[str]]:
    if names != diagnostics["original_terms"]:
        raise ValueError("Held-out terms differ from the original training context basis")
    retained = diagnostics["kept_terms"]
    reduced = matrix[:, [names.index(name) for name in retained]]
    reconstructed = reduced @ np.asarray(diagnostics["original_column_transform"])
    error = float(np.max(np.abs(reconstructed - matrix))) if matrix.size else 0.
    if error > 1e-9 * max(1., float(np.max(np.abs(matrix)))):
        raise ValueError("Held-out covariates are outside identifiable training contexts: original nuisance alias relations do not hold")
    return reduced, retained


def _mantis_design_contract(train: pd.DataFrame, test: pd.DataFrame,
                            design_diagnostics: dict[str, Any] | None = None) -> tuple[np.ndarray, list[str], dict[str, Any]]:
    continuous = ["infection", "dop", "log_illuminance"]
    categorical = ["stimulus_side", "year", "box"]
    parts = {"Intercept": np.ones(len(test))}
    contract = {"continuous": {}, "categorical_levels": {}}
    for column in continuous:
        values = train[column].to_numpy(float)
        if np.unique(values).size == 1 and column == "log_illuminance":
            constant = float(values[0])
            if not np.allclose(test[column].to_numpy(float), constant, rtol=1e-9, atol=1e-10):
                raise ValueError(f"Held-out {column} is outside identifiable training contexts: the omitted training covariate was constant")
            contract["continuous"][column] = {"constant_training_value": constant, "omitted_nuisance": True,
                                              "identified_only_at_training_value": True}
            continue
        raw = set(np.unique(values)).issubset({0, 1})
        mean, scale = (0., 1.) if raw else (float(values.mean()), max(float(values.std()), 1e-10))
        parts[column] = (test[column].to_numpy(float) - mean) / scale
        contract["continuous"][column] = {"mean": mean, "scale": scale}
    for column in categorical:
        levels = sorted(train[column].astype(str).unique())
        unknown = set(test[column].astype(str)) - set(levels)
        if unknown:
            raise ValueError(f"Held-out {column} levels are outside training context support: {sorted(unknown)}")
        contract["categorical_levels"][column] = levels
        for level in levels[1:]:
            parts[f"{column}_{level}"] = test[column].astype(str).eq(level).to_numpy(float)
    parts["infection:dop"] = parts["infection"] * parts["dop"]
    names = list(parts)
    matrix = np.column_stack(list(parts.values()))
    if not np.isfinite(matrix).all():
        raise ValueError("Nonfinite held-out mantis design")
    if design_diagnostics:
        matrix, names = _apply_training_design(matrix, names, design_diagnostics)
        contract["training_nuisance_basis"] = design_diagnostics
    return matrix, names, contract


def validate_mantis_choice(frame: pd.DataFrame, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Training-only preprocessing; hold out all records of tested individuals."""
    config = config or {}
    table = frame.rename(columns={value: key for key, value in config.get("columns", {}).items()}).copy()
    if not {"individual_id", "date", "year"}.issubset(table) or table.individual_id.isna().any():
        return {"status": "pending", "pending": ["Independent individual IDs and assay dates are required"]}
    table["individual_id"] = table.individual_id.astype(str)
    table["log_illuminance"] = np.log(table.illuminance)
    if config.get("dop_units") == "percent":
        table.dop = table.dop / 100
    elif config.get("dop_units") not in ("proportion", "source"):
        raise ValueError("Mantis validation requires declared native polarization units")
    table["assay_date"] = table.year.astype(str) + ":" + table.date.astype(str)
    rules = config.get("response_values", {"middle": 0, "polarized": 1, "unpolarized": 2})
    all_ids = sorted(table.individual_id.unique(), key=lambda value: hashlib.sha256(("mantis-holdout-v1\0" + value).encode()).hexdigest())
    folds = []
    if len(all_ids) >= 3:
        for fold in range(3):
            folds.append(("individual", str(fold), table.individual_id.isin(all_ids[fold::3])))
    dates = sorted(table.assay_date.unique())
    if len(dates) >= 2:
        folds.extend(("assay_date", date, table.assay_date.eq(date)) for date in dates)
    pending, rows, contracts = [], [], []
    if len(all_ids) < 3:
        pending.append("Fewer than three independent individuals for grouped validation")
    if len(dates) < 2:
        pending.append("Fewer than two distinct assay dates for date generalization")
    nodes, weights = np.polynomial.hermite.hermgauss(9)
    log_weights = np.log(weights / np.sqrt(np.pi))
    for scheme, label, selection in folds:
        test = table.loc[selection].copy()
        test_ids = set(test.individual_id)
        # Dates can contain individuals measured on other dates. All their other
        # records are excluded from training as well, even outside this date.
        train = table.loc[~selection & ~table.individual_id.isin(test_ids)].copy()
        try:
            if train.empty or test.empty or set(train.individual_id) & test_ids:
                raise ValueError("No independent training individuals after complete date/individual holdout")
            # Reject absent categorical levels or extrapolation of a constant
            # nuisance covariate before costly MAP fits. Full training-alias
            # and chooser-specific basis checks still follow the fit below.
            _mantis_design_contract(train, test)
            fitting_config = dict(config, dop_units="source", columns={})
            fitted = fit_mantis_choice(train, fitting_config)
            x, names, contract = _mantis_design_contract(train, test, fitted["design_audit"]["engagement"])
            if names != fitted["engagement"]["terms"]:
                raise ValueError("Training and held-out design terms disagree")
            engagement, preference = fitted["engagement"], fitted["conditional_preference"]
            eta_engagement = x @ np.asarray(engagement["coefficient"])
            # Three-category predictions require an identified preference at
            # every test context. Checking only observed choosers would use the
            # test outcome to hide unsupported conditional predictions.
            preference_x, preference_names = _apply_training_design(x, names, fitted["design_audit"]["conditional_preference"])
            if preference_names != preference["terms"]:
                raise ValueError("Held-out conditional-preference terms disagree")
            eta_preference = preference_x @ np.asarray(preference["coefficient"])
            e = special.expit(eta_engagement[:, None] + np.sqrt(2) * float(engagement["group_sd"] or 0) * nodes)
            p = special.expit(eta_preference[:, None] + np.sqrt(2) * float(preference["group_sd"] or 0) * nodes)
            response = test.response.map(rules).to_numpy(int)
            marginal_e, marginal_p = e @ np.exp(log_weights), p @ np.exp(log_weights)
            probabilities = np.column_stack([1 - marginal_e, marginal_e * marginal_p, marginal_e * (1 - marginal_p)])
            mn = fitted["multinomial_sensitivity"]
            logits = np.column_stack([x @ np.asarray(mn["middle_coefficients"]), x @ np.asarray(mn["polarized_coefficients"]), np.zeros(len(x))])
            multinomial_log_score = logits - special.logsumexp(logits, axis=1, keepdims=True)
            for individual in sorted(test_ids):
                which = test.individual_id.eq(individual).to_numpy()
                y, e_values, p_values = response[which], e[which], p[which]
                # Integrate one shared intercept per withheld individual and
                # hurdle, not a different latent intercept for every trial.
                likelihood = np.zeros((len(nodes), len(nodes)))
                for category, eng, pref in zip(y, e_values, p_values):
                    probability = ((1 - eng)[:, None] * np.ones((1, len(nodes))) if category == 0 else
                        eng[:, None] * (pref[None, :] if category == 1 else 1 - pref[None, :]))
                    likelihood += np.log(np.maximum(probability, 1e-300))
                joint = float(special.logsumexp(likelihood + log_weights[:, None] + log_weights[None, :]))
                brier = float(np.mean(np.sum((probabilities[which] - np.eye(3)[y]) ** 2, axis=1)))
                rows.append({"scheme": scheme, "fold": label, "individual_id": individual,
                    "trials": int(which.sum()), "hurdle_log_score_per_trial": joint / which.sum(),
                    "multinomial_log_score_per_trial": float(multinomial_log_score[which, y].mean()),
                    "three_category_brier": brier})
            contracts.append({"scheme": scheme, "fold": label, "training_individuals": sorted(set(train.individual_id)),
                "test_individuals": sorted(test_ids), "training_context": contract})
        except (ValueError, RuntimeError, KeyError) as error:
            pending.append(f"{scheme}/{label}: {error}")
    return {"status": "complete" if folds and not pending else "pending", "pending": pending,
        "scores": rows, "training_contracts": contracts,
        "independent_unit": "individual with nested trials; separate whole-assay-date generalization",
        "future_outcomes_used_for_preprocessing": False,
        "summary": (pd.DataFrame(rows).groupby("scheme")[["hurdle_log_score_per_trial", "multinomial_log_score_per_trial", "three_category_brier"]].mean().to_dict() if rows else {})}


def analyze_mantis_choice(frame: pd.DataFrame, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Preserve contradictory observations and bound their inferential use.

    Conditional exclusion and alternative endpoint interpretations are a declared
    parser amendment. No observed field is corrected, and neither successful
    conditional fitting nor sensitivity agreement reconciles the source.
    """
    config = config or {}
    consistent = frame.loc[frame.analysis_included].copy() if "analysis_included" in frame else frame.copy()
    ambiguous = int(len(frame) - len(consistent))
    if consistent.empty:
        raise ValueError("No mutually consistent source endpoint annotations for a conditional choice fit")
    try:
        fitted = fit_mantis_choice(consistent, config)
        fitted["status"] = "complete"
        validation = validate_mantis_choice(consistent, config)
    except (ValueError, RuntimeError) as error:
        fitted = {"status": "pending_nonestimable_design" if isinstance(error, NonestimableDesignError) else "pending_statistical_fit",
                  "pending": [str(error)], "design_audit": getattr(error, "diagnostics", None),
                  "numeric_claim_available": False}
        validation = {"status": "pending", "pending": [str(error)], "scores": [], "training_contracts": [],
                      "independent_unit": "individual with nested trials; separate whole-assay-date generalization",
                      "future_outcomes_used_for_preprocessing": False}
    descriptive_columns = [column for column in ["infection", "dop", "response", "year", "box", "stimulus_side", "analysis_included"] if column in frame]
    descriptive = frame.groupby(descriptive_columns, dropna=False).size().reset_index(name="trials")
    fitted["source_descriptive_counts"] = descriptive.astype(object).where(pd.notna(descriptive), None).to_dict(orient="records")
    fitted.update(source_coding_reconciled=ambiguous == 0, ambiguous_source_trials=ambiguous,
        included_source_trials=len(consistent), deposited_source_trials=len(frame),
        inference_scope=("Conditional diagnostic fit of mutually consistent deposited annotations; source coding remains unresolved"
                         if ambiguous else "Recorded source endpoint with individual/trial hierarchy"),
        confirmatory_ready=bool(ambiguous == 0 and fitted["status"] == "complete" and validation.get("status") == "complete"))
    validation["source_coding_reconciled"] = ambiguous == 0
    validation["confirmatory_ready"] = fitted["confirmatory_ready"]
    if ambiguous:
        validation["conditional_validation_status"] = validation["status"]
        validation["status"] = "pending_source_reconciliation"
        validation["pending"].append("Deposited endpoint contradictions remain; conditional-row validation does not reconcile source coding")
    sensitivity = {"status": "not_required" if not ambiguous else "pending", "source_coding_reconciled": ambiguous == 0,
        "confirmatory_ready": False if ambiguous else fitted["confirmatory_ready"],
        "ambiguous_source_trials": ambiguous, "variants": {}, "pending": [],
        "interpretation": "Alternatives use recorded source result or observed location/window separately; they are diagnostic analyses of unresolved annotations, never corrections or independent replications"}
    if fitted["status"] != "complete":
        sensitivity["status"] = "pending"
        sensitivity["pending"].extend(fitted["pending"])
    if ambiguous:
        alternatives = {"recorded_result_all_rows": frame.copy()}
        if "response_location_window" in frame:
            location = frame.copy()
            location["response"] = location.response_location_window
            alternatives["location_window_all_rows"] = location
        else:
            sensitivity["pending"].append("No observed location/window alternative endpoint")
        for label, alternative in alternatives.items():
            try:
                alternative_fit = fit_mantis_choice(alternative, config)
                sensitivity["variants"][label] = {"status": "complete", "trials": len(alternative),
                    "independent_source_rows": alternative.source_row_index.astype(int).tolist() if "source_row_index" in alternative else None,
                    "fit": alternative_fit}
            except (ValueError, RuntimeError) as error:
                sensitivity["variants"][label] = {"status": "pending", "reason": str(error),
                    "design_audit": getattr(error, "diagnostics", None), "numeric_claim_available": False}
                sensitivity["pending"].append(f"{label}: {error}")
        # Report the source-unit interaction slope, undoing each variant's own
        # training DOP standardization before comparing point estimates.
        summaries = {}
        for label, alternative, fit in [(label, alternative, fit) for label, alternative, fit in [("mutually_consistent_rows", consistent, fitted)] if fit["status"] == "complete"] + [
            *[(label, alternatives[label], record["fit"]) for label, record in sensitivity["variants"].items() if record["status"] == "complete"]]:
            raw = alternative.dop.to_numpy(float)
            divisor = 100. if config.get("dop_units") == "percent" else 1.
            dop = raw / divisor
            scale = 1. if set(np.unique(dop)).issubset({0, 1}) else max(float(dop.std()), 1e-10)
            endpoint_summaries = {}
            for endpoint in ("engagement", "conditional_preference"):
                index = fit[endpoint]["terms"].index("infection:dop")
                estimate = float(fit[endpoint]["coefficient"][index] / (scale * divisor))
                standard_error = float(fit[endpoint]["standard_error_laplace"][index] / (scale * divisor))
                endpoint_summaries[endpoint] = {"infection_by_dop_source_unit_slope": estimate,
                    "ci95_laplace": [estimate - stats.norm.ppf(.975) * standard_error, estimate + stats.norm.ppf(.975) * standard_error]}
            summaries[label] = endpoint_summaries
        sensitivity["source_unit_interaction_summaries"] = summaries
        sensitivity["status"] = "complete_diagnostic_source_ambiguity_pending" if not sensitivity["pending"] else "pending"
        sensitivity["interaction_sign_consistency"] = {endpoint: bool(len(summaries) == 3 and
            len({np.sign(summary[endpoint]["infection_by_dop_source_unit_slope"]) for summary in summaries.values()}) == 1)
            for endpoint in ("engagement", "conditional_preference")}
    return {"choice": fitted, "choice_validation": validation, "choice_sensitivity": sensitivity}


def aft_objective(theta: np.ndarray, x: np.ndarray, times: np.ndarray,
                  censored: np.ndarray, prior_precision: np.ndarray | None = None) -> float:
    sigma = np.exp(theta[-1])
    z = (np.log(times) - x @ theta[:-1]) / sigma
    # Source convention: 1 denotes right censoring, not event occurrence.
    log_event = stats.norm.logpdf(z) - np.log(sigma) - np.log(times)
    log_survival = special.log_ndtr(-z)
    precision = np.eye(x.shape[1]) / 2.25 if prior_precision is None else np.asarray(prior_precision, float)
    return float(-np.where(censored, log_survival, log_event).sum()
                 + .5 * theta[:-1] @ precision @ theta[:-1] + .5 * theta[-1] ** 2)


def fit_censored_latency(frame: pd.DataFrame, outcome: str,
                         config: dict[str, Any] | None = None) -> dict[str, Any]:
    config = config or {}
    table = frame.rename(columns={v: k for k, v in config.get("columns", {}).items()}).copy()
    time_column, censor_column = f"{outcome}_time", f"{outcome}_censored"
    needed = {time_column, censor_column, "observation_duration", "male_infection",
              "female_infection", "male_size", "female_size", "year", "pair_id"}
    if needed - set(table):
        raise ValueError(f"Mating latency schema needs {sorted(needed - set(table))}")
    if not set(table[censor_column]).issubset({0, 1}):
        raise ValueError("Censoring must use verified 1=censored convention")
    censored = table[censor_column].to_numpy(bool)
    times = table[time_column].to_numpy(float)
    duration = table.observation_duration.to_numpy(float)
    if not np.isfinite(duration).all() or (duration <= 0).any():
        raise ValueError("Invalid observation duration")
    # Missing event times are allowed only for censored animals.
    times = np.where(censored, duration, times)
    if not np.isfinite(times).all() or (times <= 0).any() or (times > duration).any():
        raise ValueError("Event latency must be positive and inside its observation window")
    x, names, design = design_matrix(table, ["male_infection", "female_infection", "male_size", "female_size"], ["year"],
        protected_terms=["male_infection", "female_infection"], return_diagnostics=True)
    initial = np.zeros(x.shape[1] + 1)
    initial[0] = float(np.mean(np.log(times)))
    result = optimize.minimize(aft_objective, initial, args=(x, times, censored, np.asarray(design["prior_precision"])), method="BFGS",
                               options={"maxiter": 2000, "gtol": 1e-6})
    if not np.isfinite(result.fun) or np.linalg.norm(result.jac) > .02:
        raise RuntimeError(f"AFT fit did not converge: {result.message}")
    return {"outcome": outcome, "terms": names, "coefficient": result.x[:-1].tolist(),
            "design_audit": design,
            "log_sigma": float(result.x[-1]), "covariance_laplace": np.asarray(result.hess_inv).tolist(),
            "n_pairs": int(table.pair_id.nunique()), "n_events": int((~censored).sum()),
            "n_censored": int(censored.sum()), "censor_convention": "1=right-censored",
            "uncertainty_method": "penalized MAP / Laplace approximation"}


def fit_mating_specificity(frame: pd.DataFrame, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Estimate the male-infection coefficient contrast with shared-pair resampling."""
    config = config or {}
    table = frame.rename(columns={v: k for k, v in config.get("columns", {}).items()}).copy()
    needed = {"male_escape", "male_mounting", "male_infection", "female_infection",
              "male_size", "female_size", "year", "pair_id"}
    if needed - set(table):
        raise ValueError(f"Mating specificity schema needs {sorted(needed - set(table))}")
    if table.pair_id.isna().any() or not set(table.male_escape).issubset({0, 1}) or not set(table.male_mounting).issubset({0, 1}):
        raise ValueError("Mating event coding and pair identifiers require source validation")
    def fit_pair(data: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any], float]:
        x, names, design = design_matrix(data, ["male_infection", "female_infection", "male_size", "female_size"], ["year"],
            protected_terms=["male_infection", "female_infection"], return_diagnostics=True)
        if "male_id" in data and data.male_id.duplicated().any():
            groups = data.male_id.astype(str).to_numpy()
        else:
            groups = None
        escape = _map_binary(x, data.male_escape.to_numpy(float), names, groups, np.asarray(design["prior_precision"]))
        mounting = _map_binary(x, data.male_mounting.to_numpy(float), names, groups, np.asarray(design["prior_precision"]))
        escape["design_audit"] = mounting["design_audit"] = design
        index = names.index("male_infection")
        contrast = escape["coefficient"][index] - mounting["coefficient"][index]
        return escape, mounting, float(contrast)
    escape, mounting, contrast = fit_pair(table)
    # When individual reuse is known, connected pairs are one resampling cluster.
    # The same sampled pairs enter both endpoints, preserving their covariance.
    parent = {pair: pair for pair in table.pair_id.unique()}
    def find(pair: Any) -> Any:
        while parent[pair] != pair:
            parent[pair] = parent[parent[pair]]
            pair = parent[pair]
        return pair
    for identifier in ("male_id", "female_id"):
        if identifier in table:
            if table[identifier].isna().any():
                raise ValueError("Missing reused-individual identifiers")
            for _, group in table.groupby(identifier):
                pairs = group.pair_id.tolist()
                for pair in pairs[1:]:
                    parent[find(pair)] = find(pairs[0])
    clusters = table.pair_id.map(find)
    cluster_names = list(clusters.unique())
    rng = np.random.default_rng(int(config.get("seed", 0)))
    repetitions = int(config.get("bootstrap_repetitions", 1000))
    draws, failures = [], 0
    for _ in range(repetitions):
        sampled = pd.concat([table.loc[clusters == name] for name in
                             rng.choice(cluster_names, len(cluster_names), replace=True)], ignore_index=True)
        try:
            draws.append(fit_pair(sampled)[2])
        except (ValueError, RuntimeError):
            failures += 1
    if len(draws) < .8 * repetitions:
        raise ValueError("Mating bootstrap lacks estimable independent clusters")
    return {"escape": escape, "mounting": mounting, "male_infection_coefficient_difference": contrast,
            "design_audit": escape["design_audit"],
            "cluster_bootstrap_interval": np.quantile(draws, [.025, .975]).tolist(),
            "n_resampling_clusters": len(cluster_names), "failed_bootstrap_samples": failures,
            "individual_reuse_verified": {key: key in table for key in ("male_id", "female_id")},
            "contrast_definition": "male-infection log-odds coefficient for escape minus mounting"}


def analyze_mating_behaviour(frame: pd.DataFrame, config: dict[str, Any] | None = None) -> dict[str, Any]:
    config = config or {}
    result = {}
    for outcome in config.get("latency_outcomes", ["male_escape", "male_mounting"]):
        try:
            result[outcome] = dict(fit_censored_latency(frame, outcome, config), status="complete")
        except (ValueError, RuntimeError) as error:
            result[outcome] = {"status": "pending_nonestimable_design" if isinstance(error, NonestimableDesignError) else "pending_statistical_fit",
                "pending": [str(error)], "design_audit": getattr(error, "diagnostics", None), "numeric_claim_available": False}
    try:
        result["specificity"] = dict(fit_mating_specificity(frame, config), status="complete")
    except (ValueError, RuntimeError) as error:
        result["specificity"] = {"status": "pending_nonestimable_design" if isinstance(error, NonestimableDesignError) else "pending_statistical_fit",
            "pending": [str(error)], "design_audit": getattr(error, "diagnostics", None), "numeric_claim_available": False}
    result["statistical_models_complete"] = all(record["status"] == "complete" for record in result.values())
    canonical = frame.rename(columns={value: key for key, value in config.get("columns", {}).items()})
    columns = [column for column in ["male_infection", "female_infection", "year", "male_escape", "male_mounting"] if column in canonical]
    counts = (canonical.groupby(columns, dropna=False).size().reset_index(name="pair_records") if columns else
              pd.DataFrame({"pair_records": [len(canonical)]}))
    result["source_descriptive_counts"] = counts.astype(object).where(pd.notna(counts), None).to_dict(orient="records")
    return result


def run(config: dict[str, Any], root: Path, stage: str, **kwargs: Any) -> dict[str, Any]:
    root = Path(root)
    settings = config.get("behaviour", config)
    output = root / "results" / "behaviour"
    output.mkdir(parents=True, exist_ok=True)
    aliases = {f"{operation}-{value}s": (operation, value) for operation in ("fit", "posterior-checks", "aggregate-folds") for value in (5, 20)}
    if stage in aliases:
        stage, kwargs["bin_seconds"] = aliases[stage]
    if stage == "readiness":
        from .core import atomic_json
        result = formal_behavior_readiness(config, root)
        atomic_json(root / "state" / "behaviour-readiness.json", result)
        return result
    if stage == "source-replication":
        return replicate_source_summit(config, root)
    if stage == "identifiability":
        return parameter_identifiability_from_recovery(config, root)
    if stage == "sensitivity-checks":
        return sensitivity_checks(config, root)
    if stage == "benchmark-workload":
        return benchmark_workload(config, root)
    if stage == "inference":
        model_directory = root / "results" / "behaviour" / f"models_{float(settings.get('primary_bin_seconds', 10)):g}s"
        return write_h1(root, config, model_directory)
    if stage == "export-parameters":
        return export_parameter_readiness(root, config)
    operational_sources = config.get("fly_sources", settings.get("fly_sources", []))
    adapter_file = root / "manifests" / "fly_adapters.json"
    if not operational_sources and adapter_file.exists():
        body = adapter_file.read_bytes()
        document = json.loads(body)
        operational_sources = document if isinstance(document, list) else document.get("fly_sources", [])
        (output / "adapter_manifest_provenance.json").write_text(json.dumps({
            "path": str(adapter_file), "sha256": hashlib.sha256(body).hexdigest(),
            "n_source_adapters": len(operational_sources)}, indent=2), encoding="utf-8")
    if stage in ("ingest", "validate"):
        sources = operational_sources
        if not sources:
            raise ValueError("No audited fly_sources: complete clock/ROI/geometry source audit first")
        import pyarrow.parquet as pq
        from .core import atomic_json, canonical_hash, digest, source_identity
        inventories, counts, seen, duplicate_sources = {}, {}, {}, []
        directory = root / "data"
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / "fly_observations.parquet"
        temporary = directory / "fly_observations.partial.parquet"
        writers: dict[float, Any] = {}
        native_writer = None
        bin_values = sorted(set([float(settings.get("primary_bin_seconds", 10)),
            *map(float, settings.get("sensitivity_bins_seconds", [5, 20]))]))
        protected = [destination, temporary, output / "streaming_bin_provenance.json",
            *[output / f"fly_bins_{value:g}s{suffix}.parquet" for value in bin_values for suffix in ("", ".partial")]]
        existing = [str(path) for path in protected if path.exists()]
        if existing:
            raise FileExistsError("Existing complete or partial chronological ingestion outputs must be preserved before retry: " + ", ".join(existing))
        schema, bin_schema = _trajectory_parquet_schemas(sources, root)
        for source in sources:
            inventories[source["mat"]] = mat_inventory(root / source["mat"])
        try:
            for source in sources:
                for frame in iter_fly_source(source, root):
                    animal, experiment = str(frame.animal_id.iloc[0]), str(frame.experiment_id.iloc[0])
                    content = frame.drop(columns="source_file")
                    fingerprint = hashlib.sha256(pd.util.hash_pandas_object(content, index=False).to_numpy().tobytes()).hexdigest()
                    if animal in seen:
                        if seen[animal] != fingerprint:
                            raise ValueError("Conflicting repeated native experiment/ROI representation")
                        duplicate_sources.append({"animal_id": animal, "source_file": str(source["mat"]),
                                                  "n_duplicate_rows": len(frame)})
                        continue
                    seen[animal] = fingerprint
                    counts[experiment] = counts.get(experiment, 0) + 1
                    table = _trajectory_arrow_table(frame, schema)
                    if native_writer is None:
                        native_writer = pq.ParquetWriter(temporary, schema, compression="zstd")
                    native_writer.write_table(table)
                    for value in bin_values:
                        binned = bin_trajectories(frame, value,
                            missing_threshold=float(settings.get("missing_threshold", .2)),
                            maximum_speed=settings.get("maximum_speed"))
                        table = _trajectory_arrow_table(binned, bin_schema)
                        if value not in writers:
                            writers[value] = pq.ParquetWriter(output / f"fly_bins_{value:g}s.partial.parquet", bin_schema, compression="zstd")
                        writers[value].write_table(table)
        finally:
            if native_writer is not None:
                native_writer.close()
            for writer in writers.values():
                writer.close()
        if not seen:
            raise ValueError("Validated native adapters contain no independent animals")
        temporary.replace(destination)
        bin_paths = []
        for value in bin_values:
            target = output / f"fly_bins_{value:g}s.parquet"
            (output / f"fly_bins_{value:g}s.partial.parquet").replace(target)
            bin_paths.append(target)
        atomic_json(output / "streaming_bin_provenance.json", {
            "status": "complete", "effective_ingestion_config_sha256": canonical_hash(config),
            "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
            "adapter_roster_sha256": canonical_hash(sources),
            "storage_schemas": {"native": {field.name: str(field.type) for field in schema},
                                "binned": {field.name: str(field.type) for field in bin_schema}},
            "schema_policy": "Nullable canonical types and observed source-metadata type union; missing values remain null, no endpoint or covariate imputation",
            "native_input_sha256": digest(destination), "native_path": str(destination.resolve()),
            "bin_files": {str(path.resolve()): digest(path) for path in bin_paths},
            "method": "One independently identified native ROI per Parquet row group; identical repeated representations collapsed by complete content hash"})
        pd.DataFrame(duplicate_sources).to_csv(output / "duplicate_representations.tsv", sep="\t", index=False)
        (output / "mat_schemas.json").write_text(json.dumps(inventories, indent=2), encoding="utf-8")
        return {"stage": stage, "animals_by_experiment": counts,
                "duplicate_rows_collapsed": sum(row["n_duplicate_rows"] for row in duplicate_sources), "output": str(destination)}
    if stage == "mantis":
        results = {}
        declared = config.get("mantis_sources", {})
        if not declared:
            choice_files = list((root / "raw" / "polarization").rglob("data_deposited.xlsx"))
            mating_files = list((root / "raw" / "mating").rglob("mating_experiment.csv"))
            if len(choice_files) != 1 or len(mating_files) != 1:
                raise ValueError("Exactly one frozen public polarization workbook and mating CSV are required")
            choice_table, choice_audit = read_polarization_source(choice_files[0])
            mating_table, mating_audit = read_mating_source(mating_files[0])
            choice_table.to_parquet(output / "mantis_choice_canonical.parquet", index=False)
            mating_table.to_parquet(output / "mantis_mating_canonical.parquet", index=False)
            (output / "mantis_source_audit.json").write_text(json.dumps({"choice": choice_audit, "mating": mating_audit}, indent=2), encoding="utf-8")
            choice_settings = dict(settings.get("mantis_choice", {}), dop_units="source")
            mating_settings = settings.get("mantis_mating", {})
            results.update(analyze_mantis_choice(choice_table, choice_settings))
            results["choice"]["source_audit"] = choice_audit
            choice_table.loc[choice_table.coding_ambiguous].to_parquet(output / "mantis_ambiguous_observations.parquet", index=False)
            results["mating"] = analyze_mating_behaviour(mating_table, mating_settings)
            results["mating"]["source_audit"] = mating_audit
            results["mating"]["inference_scope"] = ("Recorded pairs with verified individual identifiers"
                if mating_audit["independence_verified"] else
                "Pair-record uncertainty conditional on independence; individual reuse is unresolved in source identifiers")
        for name, source in declared.items():
            frame = read_table(root / source["path"], source.get("sheet"))
            if name == "choice":
                results.update(analyze_mantis_choice(frame, source))
            elif name == "mating":
                results[name] = analyze_mating_behaviour(frame, source)
        if not results:
            raise ValueError("No audited mantis source schemas")
        (output / "mantis_models.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
        from .core import atomic_json, canonical_hash, digest, source_identity
        validation = results.get("choice_validation", {"status": "pending", "pending": ["Choice validation unavailable"]})
        validation.update(config_sha256=canonical_hash(config), source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"],
            source_result_paths=[str((output / "mantis_models.json").resolve())],
            source_result_hashes={str((output / "mantis_models.json").resolve()): digest(output / "mantis_models.json")})
        atomic_json(output / "mantis_choice_validation.json", validation)
        sensitivity = results.get("choice_sensitivity", {"status": "pending", "pending": ["Choice sensitivity unavailable"]})
        sensitivity.update(config_sha256=canonical_hash(config), source_sha256=validation["source_sha256"],
            source_result_paths=validation["source_result_paths"], source_result_hashes=validation["source_result_hashes"])
        atomic_json(output / "mantis_choice_sensitivity.json", sensitivity)
        audit_inputs = [candidate for candidate in [output / "mantis_models.json", output / "mantis_choice_canonical.parquet",
            output / "mantis_mating_canonical.parquet", output / "mantis_source_audit.json",
            output / "mantis_choice_validation.json", output / "mantis_choice_sensitivity.json"] if candidate.exists()]
        choice_source = results.get("choice", {}).get("source_audit", {})
        atomic_json(output / "mantis_statistical_design_audit.json", {
            "config_sha256": canonical_hash(config), "source_sha256": validation["source_sha256"],
            "choice_status": results.get("choice", {}).get("status"), "choice_design": results.get("choice", {}).get("design_audit"),
            "choice_pending": results.get("choice", {}).get("pending", []),
            "choice_source_coding_reconciled": choice_source.get("source_coding_reconciled", False),
            "choice_source_trials": choice_source.get("trials"),
            "choice_mutually_consistent_trials": choice_source.get("mutually_consistent_trials"),
            "choice_ambiguous_trials": choice_source.get("ambiguous_trials"),
            "choice_validation_status": validation.get("status"),
            "choice_sensitivity_status": sensitivity.get("status"),
            "choice_confirmatory_ready": results.get("choice", {}).get("confirmatory_ready", False),
            "mating_models_complete": results.get("mating", {}).get("statistical_models_complete", False),
            "mating_independence_verified": results.get("mating", {}).get("source_audit", {}).get("independence_verified", False),
            "mating_models": {name: {key: record.get(key) for key in ["status", "pending", "design_audit"]}
                for name, record in results.get("mating", {}).items() if isinstance(record, dict) and "status" in record},
            "repair_rule": "Only likelihood nuisance aliases may be reparameterized, preserving the exact predictor span and the induced original Gaussian prior; protected primary confounding remains pending without coefficients or P values",
            "source_result_paths": [str(candidate.resolve()) for candidate in audit_inputs],
            "source_result_hashes": {str(candidate.resolve()): digest(candidate) for candidate in audit_inputs}})
        audit = results.get("choice", {}).get("source_audit", {})
        amendment = audit.get("parser_repair_amendment")
        if amendment:
            inputs = [candidate for candidate in [output / "mantis_choice_canonical.parquet", output / "mantis_ambiguous_observations.parquet", output / "mantis_source_audit.json"] if candidate.exists()]
            atomic_json(output / "mantis_coding_amendment.json", dict(amendment,
                source_coding_reconciled=audit["source_coding_reconciled"], ambiguous_trials=audit["ambiguous_trials"],
                mutually_consistent_trials=audit["mutually_consistent_trials"], total_source_trials=audit["trials"],
                config_sha256=canonical_hash(config), source_sha256=validation["source_sha256"],
                source_result_paths=[str(candidate.resolve()) for candidate in inputs],
                source_result_hashes={str(candidate.resolve()): digest(candidate) for candidate in inputs}))
        return results
    path = root / "data" / "fly_observations.parquet"
    if stage == "pilot" and not path.exists() and operational_sources:
        # This materializes validated native inputs; it does not lower the full
        # study's starts, epochs or biological-control requirements.
        run(dict(config, fly_sources=operational_sources), root, "ingest")
    if not path.exists():
        raise FileNotFoundError("Validated chronological fly_observations.parquet is required")
    bin_seconds = float(kwargs.get("bin_seconds", settings.get("primary_bin_seconds", 10)))
    bin_path = output / f"fly_bins_{bin_seconds:g}s.parquet"
    bin_provenance = output / "streaming_bin_provenance.json"
    cached_bins = bin_path.exists() and bin_provenance.exists()
    if cached_bins:
        from .core import digest
        proof = json.loads(bin_provenance.read_text(encoding="utf-8"))
        if proof.get("native_input_sha256") != digest(path) or proof.get("bin_files", {}).get(str(bin_path.resolve())) != digest(bin_path):
            raise ValueError("Native/bin content changed after validated streaming ingestion")
        pilot_metadata = native_pilot_selection(bin_path, settings) if stage == "pilot" else None
        filters = [("animal_id", "in", pilot_metadata["selected_animal_ids"])] if pilot_metadata else None
        data = pd.read_parquet(bin_path, filters=filters)
    else:
        data = pd.read_parquet(path)
    if stage in ("fit", "recover", "bayesian-check", "pilot", "freeze-development", "aggregate-folds", "posterior-checks"):
        selectors = settings.get("development_selectors")
        if selectors:
            for column, allowed in selectors.items():
                if column.lower() in FORBIDDEN_PREDICTORS or column.startswith("annotation_"):
                    raise ValueError("Do not select development animals by eventual death/sporulation")
                if column not in data:
                    raise ValueError(f"Development selector {column} is missing")
                values = allowed if isinstance(allowed, list) else [allowed]
                data = data[data[column].isin(values)]
        elif "analysis_role" in data and data.analysis_role.eq("behavioural_development").any():
            data = data[data.analysis_role == "behavioural_development"]
        else:
            raise ValueError("Freeze wild-type development selectors or source analysis_role before fitting; targeted interventions must remain held out")
        if data.empty:
            raise ValueError("Frozen development selection contains no chronological animals")
    bins = (data if cached_bins else bin_trajectories(data, bin_seconds,
        missing_threshold=float(settings.get("missing_threshold", .2)), maximum_speed=settings.get("maximum_speed")))
    if not cached_bins:
        bins.to_parquet(bin_path, index=False)
    exclusions: list[dict[str, Any]] = []
    sequence_settings = dict(settings, modulation=["elapsed_hours"]) if stage == "pilot" else settings
    sequences = to_sequences(bins, sequence_settings, exclusions)
    pd.DataFrame(exclusions).to_csv(output / f"excluded_windows_{bin_seconds:g}s.tsv", sep="\t", index=False)
    if stage == "pilot":
        pilot_settings = dict(settings, _complete_pilot_membership=pilot_metadata["complete_experiment_membership"],
            pilot_experiments=pilot_metadata["native_experiments"]) if cached_bins else settings
        result = native_fly_pilot(sequences, pilot_settings, output / "pilot")
        import os
        from .core import canonical_hash, digest, source_identity
        result.update(full_config_sha256=canonical_hash(config),
                      benchmark_modulation=["elapsed_hours"],
                      infection_progression_analysis=False,
                      source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"],
                      slurm_job_id=os.environ.get("SLURM_JOB_ID"),
                      allocated_cpus=int(os.environ.get("SLURM_CPUS_PER_TASK", 1)),
                      native_input_path=str(path.resolve()), native_input_sha256=digest(path),
                      working_bytes=int(path.stat().st_size + (output / f"fly_bins_{bin_seconds:g}s.parquet").stat().st_size),
                      full_study_admission="pending full-tail and complete fit/recovery workload benchmarks")
        (output / "pilot" / "native_fly_benchmark.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        return result
    if stage in ("split", "reproduce"):
        counts = bins.groupby(["experiment_id", "animal_id"]).agg(
            n_bins=("observed", "size"), n_observed=("observed", "sum"),
            chronological_distance=("path_length", "sum"), missing_fraction=("missing_fraction", "mean"))
        counts.to_csv(output / "chronological_summaries.tsv", sep="\t")
        splits = [{"outer_experiment": experiment,
                   "training_animals": [s.animal_id for s in sequences if s.experiment_id != experiment],
                   "test_animals": [s.animal_id for s in sequences if s.experiment_id == experiment]}
                  for experiment in sorted({s.experiment_id for s in sequences})]
        (output / "grouped_splits.json").write_text(json.dumps(splits, indent=2), encoding="utf-8")
        return {"summaries": str(output / "chronological_summaries.tsv"), "splits": len(splits),
                "source_reproduction_status": "chronological summaries only; source SM reproduction is a separate contract"}
    if stage == "fit":
        result = nested_validation(sequences, settings, output / f"models_{bin_seconds:g}s",
                                   kwargs.get("outer_experiment"))
        return {"scores": len(result), "experiments": result.experiment_id.nunique()}
    model_directory = output / f"models_{bin_seconds:g}s"
    if stage == "aggregate-folds":
        result = aggregate_outer_folds(model_directory, sorted({s.experiment_id for s in sequences}))
        diagnostic_paths = [model_directory / f"outer-{experiment}" / "posterior_predictive_checks.tsv"
                            for experiment in sorted({s.experiment_id for s in sequences})]
        if all(path.exists() for path in diagnostic_paths):
            pd.concat([read_table(path) for path in diagnostic_paths], ignore_index=True).to_csv(
                model_directory / "posterior_predictive_checks.tsv", sep="\t", index=False)
        return {"scores": len(result), "experiments": result.experiment_id.nunique(),
                "posterior_checks_complete": all(path.exists() for path in diagnostic_paths)}
    if stage == "freeze-development":
        return freeze_development(sequences, config, root, model_directory)
    if stage == "posterior-checks":
        result = diagnose_outer_folds(sequences, config, model_directory, kwargs.get("outer_experiment"))
        return {"posterior_predictive_rows": len(result)}
    if stage == "recover":
        from .core import atomic_json, canonical_hash, digest, source_identity
        result = recovery_analysis(sequences, settings, output / "recovery")
        result.update(config_sha256=canonical_hash(config), source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"],
            source_result_paths=[str(path.resolve()) for path in [output / "recovery" / "parameter_recovery.tsv", output / "recovery" / "model_recovery_confusion.tsv"]])
        result["source_result_hashes"] = {path: digest(Path(path)) for path in result["source_result_paths"]}
        atomic_json(output / "recovery" / "recovery_summary.json", result)
        return result
    if stage == "bayesian-check":
        from .semimarkov import bayesian_check, fit_model
        selected = settings.get("bayesian_subset_animals")
        if not selected:
            # Clock-length strata and identity hashes use no infection/death
            # outcome. Serialize the prospective roster before refitting.
            lengths = {experiment: max(len(s.times) for s in sequences if s.experiment_id == experiment)
                       for experiment in sorted({s.experiment_id for s in sequences})}
            ordered = sorted(lengths, key=lambda experiment: (lengths[experiment], experiment))
            strata = sorted(set([ordered[0], ordered[len(ordered) // 2], ordered[-1]]))
            selected = [s.animal_id for experiment in strata for s in sorted(
                [s for s in sequences if s.experiment_id == experiment],
                key=lambda s: hashlib.sha256(("bayesian-clock-roster-v1\0" + s.animal_id).encode()).hexdigest())[:2]]
        from .core import atomic_json, canonical_hash, digest, source_identity
        roster_path = output / "bayesian_subset_roster.json"
        roster = {"animal_ids": sorted(selected), "selection": "Declared IDs or shortest/median/longest experiment clocks and two identity-hashed animals per experiment; full original clocks",
                  "config_sha256": canonical_hash(config), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"]}
        if roster_path.exists() and json.loads(roster_path.read_text(encoding="utf-8")) != roster:
            raise ValueError("Prospective Bayesian subset roster changed")
        atomic_json(roster_path, roster)
        subset = [s for s in sequences if s.animal_id in selected]
        if len(subset) != len(selected):
            raise ValueError("Bayesian subset includes unavailable or duplicated animals")
        fit = fit_model(subset, settings.get("bayesian_subset_model", "MX"), settings)
        fit.save(output / "bayesian_subset_map.pt")
        result = bayesian_check(fit, subset, settings.get("bayesian_sampler", {}))
        np.save(output / "bayesian_subset_samples.npy", result.pop("samples"))
        result.update(status="complete", config_sha256=canonical_hash(config), source_sha256=roster["source_sha256"],
            source_result_paths=[str(p.resolve()) for p in [roster_path, output / "bayesian_subset_map.pt", output / "bayesian_subset_samples.npy"]])
        result["source_result_hashes"] = {p: digest(Path(p)) for p in result["source_result_paths"]}
        atomic_json(output / "bayesian_subset_diagnostics.json", result)
        return result
    raise ValueError(f"Unsupported behavioural stage {stage!r}")


def _process_peak_ram_bytes() -> int | None:
    import os
    if os.name != "nt":
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value * (1 if os.uname().sysname == "Darwin" else 1024))
    import ctypes
    from ctypes import wintypes
    class Counters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    get_process = ctypes.windll.kernel32.GetCurrentProcess
    get_process.restype = wintypes.HANDLE
    memory = ctypes.windll.psapi.GetProcessMemoryInfo
    memory.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    memory.restype = wintypes.BOOL
    if memory(get_process(), ctypes.byref(counters), counters.cb):
        return int(counters.PeakWorkingSetSize)
    return None


def native_fly_pilot(sequences: list[Any], config: dict[str, Any], output: Path) -> dict[str, Any]:
    from .semimarkov import fit_model
    output.mkdir(parents=True, exist_ok=True)
    experiments = sorted({s.experiment_id for s in sequences})
    chosen = config.get("pilot_experiments", experiments[:2])
    if len(chosen) != 2 or len(set(chosen)) != 2 or not set(chosen).issubset(experiments):
        raise ValueError("The fly benchmark requires two distinct validated native development experiments")
    membership = config.get("_complete_pilot_membership", {experiment: sorted(s.animal_id for s in sequences if s.experiment_id == experiment)
                  for experiment in chosen})
    limit = int(config.get("pilot_animals_per_experiment", 4))
    if limit < 1:
        raise ValueError("Pilot must measure at least one native animal per experiment")
    selected_ids = {animal for experiment in chosen for animal in
        sorted(membership[experiment], key=lambda name: hashlib.sha256(("native-hardware-pilot-v1\0" + name).encode()).hexdigest())[:limit]}
    selected = [copy.deepcopy(s) for s in sequences if s.animal_id in selected_ids]
    max_bins = int(config.get("pilot_max_bins", 720))
    full_bin_counts = {s.animal_id: len(s.times) for s in selected}
    for sequence in selected:
        for field in ("observations", "covariates", "modulation", "previous", "food_vector", "arena_vector", "observed", "times"):
            setattr(sequence, field, getattr(sequence, field)[:max_bins].copy())
        if sequence.diagnostics and "path_length" in sequence.diagnostics:
            sequence.diagnostics["path_length"] = np.asarray(sequence.diagnostics["path_length"])[:max_bins].copy()
    rows = []
    epochs = int(config.get("pilot_max_epochs", 10))
    for name in config.get("pilot_models", ["MG", "MT", "GRU"]):
        settings = dict(config, starts=int(config.get("pilot_starts", 1)), max_epochs=epochs,
                        lbfgs_iterations=0, patience=epochs + 1,
                        hidden_size=int(config.get("pilot_hidden_size", 16)), dropout=0,
                        max_duration_bins=config.get("pilot_max_duration_bins", 256),
                        tail_tolerance=float("inf"))
        start = time.perf_counter()
        fit = fit_comparator(selected, name, settings) if name in ("AR", "GRU") else fit_model(selected, name, settings)
        seconds = time.perf_counter() - start
        fit.diagnostics["benchmark_only"] = True
        fit.save(output / f"{name}-benchmark-only.pt")
        epoch_count = (sum(record["epochs"] for record in fit.diagnostics["starts"])
                       if "starts" in fit.diagnostics else len(fit.diagnostics["history"]))
        n_bins = sum(len(s.times) for s in selected)
        row = {"model": name, "experiments": json.dumps(chosen), "animals": len(selected),
               "state_bins": n_bins, "observed_bins": sum(int(s.observed.sum()) for s in selected),
               "seconds_total": seconds, "epochs_executed": epoch_count,
               "seconds_per_epoch": seconds / max(epoch_count, 1),
               "process_peak_ram_bytes": _process_peak_ram_bytes(),
               "duration_cap_bins": settings["max_duration_bins"] if name not in ("AR", "GRU") else None,
               "duration_tail_probability": (max(record["max_tail_probability"] for record in fit.diagnostics["starts"])
                                               if "starts" in fit.diagnostics else None),
               "scientific_fit": False, "full_primary_defaults": json.dumps({
                    "starts": config.get("starts", 5), "max_epochs": config.get("max_epochs", 500)})}
        rows.append(row)
        pd.DataFrame(rows).to_csv(output / "native_fly_benchmark.tsv", sep="\t", index=False)
    result = {"benchmark_only": True, "native_experiments": chosen, "animals": len(selected),
              "complete_experiment_membership": membership, "selected_animal_ids": sorted(selected_ids),
              "animal_selection": "Fixed SHA256 order of identities, independent of observed outcomes",
              "full_native_bins_by_selected_animal": full_bin_counts, "pilot_first_bins_limit": max_bins,
              "rows": rows, "source": "validated chronological native dataset; no synthetic observations",
              "peak_memory_interpretation": "Process lifetime peak RSS/working set, including native data loading",
              "duration_interpretation": "Short native windows and finite age cap are hardware timings only; no complete-experiment/full-tail/recovery cost is inferred"}
    (output / "native_fly_benchmark.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def native_pilot_selection(parquet_path: Path, settings: dict[str, Any]) -> dict[str, Any]:
    """Read constant identity/role row-group statistics; do not load outcomes."""
    import pyarrow.parquet as pq
    metadata = pq.ParquetFile(parquet_path).metadata
    membership: dict[str, set[str]] = {}
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        identity = {}
        for field in ("experiment_id", "animal_id", "analysis_role"):
            columns = [group.column(i) for i in range(group.num_columns) if group.column(i).path_in_schema == field]
            if len(columns) != 1 or columns[0].statistics is None:
                raise ValueError("Native pilot requires validated per-animal row-group identity statistics")
            statistics = columns[0].statistics
            if not statistics.has_min_max or statistics.min != statistics.max:
                raise ValueError("A pilot row group mixes independent animal/experiment/role identities")
            identity[field] = str(statistics.min)
        if identity["analysis_role"] == "behavioural_development":
            membership.setdefault(identity["experiment_id"], set()).add(identity["animal_id"])
    experiments = settings.get("pilot_experiments", sorted(membership)[:2])
    if len(experiments) != 2 or not set(experiments).issubset(membership):
        raise ValueError("Pilot needs two explicitly validated development experiments")
    limit = int(settings.get("pilot_animals_per_experiment", 4))
    selected = [animal for experiment in experiments for animal in sorted(membership[experiment],
        key=lambda name: hashlib.sha256(("native-hardware-pilot-v1\0" + name).encode()).hexdigest())[:limit]]
    return {"native_experiments": experiments, "selected_animal_ids": selected,
            "complete_experiment_membership": {experiment: sorted(membership[experiment]) for experiment in experiments}}


def sensitivity_checks(config: dict[str, Any], root: Path) -> dict[str, Any]:
    from .core import atomic_json, canonical_hash, digest, source_identity
    settings = config.get("behaviour", {})
    values = sorted(set([float(settings.get("primary_bin_seconds", 10)), *map(float, settings.get("sensitivity_bins_seconds", [5, 20]))]))
    pending, rows, inputs, rosters = [], [], [], []
    for value in values:
        directory = Path(root) / "results" / "behaviour" / f"models_{value:g}s"
        paths = [directory / "model_comparison.tsv", directory / "inner_selection.tsv"]
        if not all(path.exists() for path in paths):
            pending.append(f"{value:g}s grouped outer/inner fit outputs are unavailable")
            continue
        scores, inner = map(read_table, paths)
        selected, _ = select_primary_from_inner(scores, inner)
        paired = pd.concat([selected, scores[scores.model.eq("MG")]]).pivot(
            index=["experiment_id", "animal_id"], columns="model", values="log_score")
        if paired.isna().any().any() or not np.isfinite(paired.to_numpy()).all():
            raise ValueError("Sensitivity comparison lacks complete same-animal predictions")
        difference = (paired.selected_transition - paired.MG).groupby(level=0).mean()
        rosters.append(set(difference.index.astype(str)))
        rows.extend({"bin_seconds": value, "experiment_id": str(experiment), "selected_minus_gain_log_score": float(score)}
                    for experiment, score in difference.items())
        inputs.extend(paths)
    if rosters and any(roster != rosters[0] for roster in rosters[1:]):
        pending.append("Sensitivity scales do not retain the same independent experiment roster")
    summaries = {str(value): float(np.mean([row["selected_minus_gain_log_score"] for row in rows if row["bin_seconds"] == value]))
                 for value in values if any(row["bin_seconds"] == value for row in rows)}
    result = {"status": "complete" if not pending and len(summaries) == len(values) else "pending", "pending": pending,
        "scores": rows, "experiment_weighted_means": summaries,
        "sign_consistency": bool(summaries and (all(score > 0 for score in summaries.values()) or all(score <= 0 for score in summaries.values()))),
        "interpretation": "Scores within each time scale retain paired animals and equal experiment weights; absolute log scores across bin widths are not pooled",
        "config_sha256": canonical_hash(config), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "source_result_paths": [str(path.resolve()) for path in inputs],
        "source_result_hashes": {str(path.resolve()): digest(path) for path in inputs}}
    atomic_json(Path(root) / "results" / "behaviour" / "sensitivity_checks.json", result)
    return result


def benchmark_workload(config: dict[str, Any], root: Path) -> dict[str, Any]:
    """Full native numerical kernels; missing whole-design coverage stays pending.

    This does not shorten clocks, cap duration support or optimize fewer starts
    and pretend to have measured a complete model fit. Actual kernel counts and
    the prospective full design are bound separately. Admission remains closed
    until optimizer, full curvature/sampler, neural, mantis and H2 work units
    have also been measured with their native dimensions.
    """
    import os
    import torch
    import pyarrow.parquet as pq
    from .core import atomic_json, canonical_hash, digest, native_job_id, source_identity
    from .semimarkov import Fit, MovementModel, TrainingScaler, simulate, training_duration_knots
    root, settings = Path(root), config.get("behaviour", {})
    output = root / "state" / "behaviour-workload-benchmark.json"
    identity = {"full_config_sha256": canonical_hash(config), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
                "slurm_job_id": native_job_id(), "allocated_cpus": int(os.environ.get("SLURM_CPUS_PER_TASK", "1"))}
    result = dict(identity, status="pending", benchmark_only=True, scientific_fit=False,
        complete_declared_workload=False, tasks=[], pending=[],
        biological_time_interpretation="Technical recording-relative covariate only; no inferred infection/exposure time or H1 output",
        additional_working_bytes=0, additional_working_files=0)
    native = root / "data" / "fly_observations.parquet"
    provenance = root / "results" / "behaviour" / "streaming_bin_provenance.json"
    if not native.exists() or not provenance.exists():
        result["pending"] = ["Verified chronological ingestion and bin provenance are required"]
        atomic_json(output, result)
        return result
    proof = json.loads(provenance.read_text(encoding="utf-8"))
    if proof.get("native_input_sha256") != digest(native):
        raise ValueError("Native observations changed after validated ingestion")
    result["input_sha256"] = proof["native_input_sha256"]
    widths = sorted(set([float(settings.get("primary_bin_seconds", 10)), *map(float, settings.get("sensitivity_bins_seconds", [5, 20]))]))
    rosters, paths = {}, {}
    for width in widths:
        path = root / "results" / "behaviour" / f"fly_bins_{width:g}s.parquet"
        if not path.exists() or proof.get("bin_files", {}).get(str(path.resolve())) != digest(path):
            result["pending"].append(f"Verified {width:g}s chronological bins missing")
            continue
        rows = []
        metadata = pq.ParquetFile(path).metadata
        for i in range(metadata.num_row_groups):
            group = metadata.row_group(i)
            fields = {}
            for name in ("animal_id", "experiment_id", "analysis_role"):
                columns = [group.column(j) for j in range(group.num_columns) if group.column(j).path_in_schema == name]
                if len(columns) != 1 or columns[0].statistics is None or columns[0].statistics.min != columns[0].statistics.max:
                    raise ValueError("Workload roster requires constant independent identity/role per row group")
                fields[name] = str(columns[0].statistics.min)
            if fields["analysis_role"] == "behavioural_development":
                rows.append(dict(fields, bins=group.num_rows))
        rosters[f"{width:g}s"], paths[width] = rows, path
    plan = {"clock_rosters": rosters, "models": settings.get("models", ["M0", "MG", "MT", "MD", "MX"]),
        "learning_rates": settings.get("learning_rates", [.0003, .001, .003]), "starts": int(settings.get("starts", 5)),
        "final_starts": int(settings.get("final_starts", 20)), "max_epochs": int(settings.get("max_epochs", 500)),
        "lbfgs_iterations": int(settings.get("lbfgs_iterations", 200)), "inner_folds": int(settings.get("inner_folds", 3)),
        "parameter_recovery_replicates": int(settings.get("parameter_recovery_replicates", 100)),
        "recovery_models": settings.get("recovery_models", ["MG", "MT", "MD", "MX"]),
        "recovery_effects": settings.get("recovery_effects", [0, .25, .5, 1]),
        "selection": "Maximum native clock length, then identity lexicographic tie-break; no outcomes inspected"}
    result.update(prospective_workload=plan, prospective_workload_sha256=canonical_hash(plan),
        source_result_paths=[str(provenance.resolve()), *[str(path.resolve()) for path in paths.values()]],
        source_result_hashes={str(path.resolve()): digest(path) for path in [provenance, *paths.values()]})
    atomic_json(output, result)
    torch.set_num_threads(max(1, int(settings.get("threads", identity["allocated_cpus"]))))
    for width, path in paths.items():
        roster = rosters[f"{width:g}s"]
        if not roster:
            result["pending"].append(f"No development animals at {width:g}s")
            continue
        chosen = sorted(roster, key=lambda row: (-row["bins"], row["animal_id"]))[0]
        frame = pd.read_parquet(path, filters=[("animal_id", "=", chosen["animal_id"])])
        sequences = to_sequences(frame, dict(settings, modulation=["elapsed_hours"]))
        if len(sequences) != 1 or len(sequences[0].times) != chosen["bins"]:
            raise ValueError("Native benchmark must retain the full prospectively selected clock")
        sequence = sequences[0]
        scaler = TrainingScaler.fit(sequences)
        transformed = scaler.transform(sequence)
        animals = [row["animal_id"] for row in roster]
        experiments = sorted({row["experiment_id"] for row in roster})
        for name in plan["models"]:
            model = MovementModel(name, len(scaler.mean), len(scaler.modulation_mean), sequence.observations.shape[1],
                training_duration_knots(sequences), animals, experiments, states=int(settings.get("states", 2)),
                max_duration_bins=settings.get("max_duration_bins"), bin_seconds=width)
            fit = Fit(model, scaler, {"tail_tolerance": settings.get("tail_tolerance", 1e-8)})
            # Registered group dimensions match the entire source roster; timing
            # one animal is explicitly one kernel, not a completed full-data fit.
            def gradient_operation():
                model.zero_grad()
                objective = -model(transformed)["log_likelihood"].sum() - model.log_prior()
                objective.backward()
                if not torch.isfinite(objective) or any(p.grad is not None and not p.grad.isfinite().all() for p in model.parameters()):
                    raise FloatingPointError("Full native benchmark gradient is nonfinite")
            unseen = copy.deepcopy(sequence)
            unseen.animal_id, unseen.experiment_id = "benchmark-unseen-animal", "benchmark-unseen-experiment"
            for operation, callback in [("likelihood_gradient", gradient_operation),
                    ("unseen_random_effect_score", lambda: fit.score(unseen, settings.get("warmup_seconds", 600))),
                    ("causal_full_clock_simulation", lambda: simulate(model, transformed, int(config.get("seed", 0))))]:
                start = time.perf_counter()
                callback()
                elapsed = time.perf_counter() - start
                task = dict(identity, name=f"{name}_{width:g}s_{operation}", operation=operation, model=name,
                    bin_seconds=width, native_animal_id=sequence.animal_id, native_bins=len(sequence.times),
                    input_sha256=digest(path), elapsed_seconds=elapsed, measured_work_units=1,
                    planned_work_units=None, maximum_job_work_units=None, concurrency=1,
                    benchmark_uses_full_primary_numerics=True, full_roster_parameter_count=sum(p.numel() for p in model.parameters()),
                    process_peak_ram_bytes=_process_peak_ram_bytes(),
                    forecast_status="pending: kernel count is not a complete optimizer/uncertainty workload")
                result["tasks"].append(task)
                atomic_json(output, result)
    result["pending"].extend(["Complete optimizer closure/line-search and neural comparator workload has not been measured",
        "Full-data posterior curvature, recovery fitted intervals and representative MALA workload not measured",
        "Mantis validation/bootstrap and curated H2 contrast/calibration workload not measured"])
    result["status"] = "partial_native_measurements_complete"
    atomic_json(output, result)
    return result


def full_parameter_laplace(fit: Any, sequences: list[Any], *, include_curvature: bool = False) -> dict[str, Any]:
    """Observed posterior curvature including every nuisance and group effect.

    Each animal likelihood depends on all global parameters and exactly its own
    animal/experiment rows. Scattering those exact local Hessians is algebraically
    identical to a dense full-data Hessian, while bounding the autograd graph to
    one animal. No conditional covariance or ridge repair is substituted.
    """
    import torch
    from torch.func import functional_call
    from scipy.linalg import cho_factor, cho_solve
    model = fit.model
    transformed = [fit.scaler.transform(sequence) for sequence in sequences]
    parameters = {name: value.detach() for name, value in model.named_parameters() if value.requires_grad}
    offsets, cursor = {}, 0
    for name, value in parameters.items():
        offsets[name] = np.arange(cursor, cursor + value.numel()).reshape(tuple(value.shape))
        cursor += value.numel()
    hessian, gradient = np.zeros((cursor, cursor)), np.zeros(cursor)
    for name, value in parameters.items():
        indices = offsets[name].ravel()
        array = value.cpu().numpy().ravel()
        if name in ("animal_z", "experiment_z"):
            diagonal, derivative = np.ones(len(array)), array
        elif name in ("log_animal_sd", "log_experiment_sd"):
            diagonal, derivative = 8 * np.exp(2 * array), 4 * np.exp(2 * array) - 1
        else:
            diagonal, derivative = np.full(len(array), 1 / 2.25), array / 2.25
        hessian[indices, indices] += diagonal
        gradient[indices] += derivative
    for sequence in transformed:
        local, pieces, descriptors = [], [], []
        for name, value in parameters.items():
            row = (model.animal_ids.index(sequence.animal_id) if name == "animal_z" else
                   model.experiment_ids.index(sequence.experiment_id) if name == "experiment_z" else None)
            selected = value if row is None else value[row]
            selected_indices = offsets[name] if row is None else offsets[name][row]
            local.extend(np.asarray(selected_indices).ravel().tolist())
            pieces.append(selected.flatten())
            descriptors.append((name, row, selected.shape, selected.numel()))
        initial = torch.cat(pieces).requires_grad_(True)
        def objective(theta):
            values, at = {}, 0
            for name, row, shape, size in descriptors:
                replacement = theta[at:at + size].reshape(shape)
                at += size
                values[name] = replacement if row is None else parameters[name].index_copy(
                    0, torch.as_tensor([row], device=theta.device), replacement[None])
            return -functional_call(model, values, (sequence,))["log_likelihood"].sum()
        value = objective(initial)
        local_gradient = torch.autograd.grad(value, initial)[0].detach().cpu().numpy()
        local_hessian = torch.autograd.functional.hessian(objective, initial).detach().cpu().numpy()
        hessian[np.ix_(local, local)] += local_hessian
        gradient[local] += local_gradient
    observed = sum(int(sequence.observed.sum()) for sequence in sequences)
    stationarity = float(np.max(np.abs(gradient)) / max(observed, 1))
    result = {"parameter_uncertainty_method": "full_parameter_laplace", "parameter_uncertainty_validated": False,
              "normalized_maximum_gradient": stationarity, "parameter_count": cursor,
              "curvature_interpretation": "Full posterior Hessian including nuisance, animal and experiment effects; no diagonal covariance approximation"}
    if include_curvature:
        result["hessian"], result["gradient"] = hessian, gradient
    if not np.isfinite(hessian).all() or not np.isfinite(gradient).all():
        result["uncertainty_failure"] = "Nonfinite exact posterior curvature"
        return result
    try:
        factor = cho_factor((hessian + hessian.T) / 2, check_finite=True)
    except np.linalg.LinAlgError:
        result["uncertainty_failure"] = "Full posterior curvature is not positive definite; no artificial ridge interval generated"
        return result
    if stationarity > .001:
        result["uncertainty_failure"] = "MAP normalized maximum gradient exceeds .001; Laplace intervals are not validated"
        return result
    scale = np.asarray(fit.scaler.modulation_scale)
    for component, name in (("transition", "transition_mod"), ("gain", "gain_mod"), ("direction", "direction_mod")):
        if name not in parameters:
            continue
        indices = offsets[name].ravel()
        rhs = np.zeros((cursor, len(indices)))
        rhs[indices, np.arange(len(indices))] = 1
        covariance_columns = cho_solve(factor, rhs)
        standard_error = np.sqrt(covariance_columns[indices, np.arange(len(indices))]).reshape(parameters[name].shape) / scale
        estimate = parameters[name].cpu().numpy() / scale
        result[f"{component}_interval_low"] = (estimate - stats.norm.ppf(.975) * standard_error).tolist()
        result[f"{component}_interval_high"] = (estimate + stats.norm.ppf(.975) * standard_error).tolist()
    result["parameter_uncertainty_validated"] = True
    return result


def recovery_analysis(sequences: list[Any], config: dict[str, Any], output: Path) -> dict[str, Any]:
    from .semimarkov import MovementModel, TrainingScaler, fit_model, simulate, training_duration_knots
    output.mkdir(parents=True, exist_ok=True)
    # Recovery uses exact acquisition schedules. Its settings are archived and
    # cannot manufacture independent biological replication.
    scaler = TrainingScaler.fit(sequences)
    templates = [scaler.transform(s) for s in sequences]
    records = []
    models = config.get("recovery_models", ["MG", "MT", "MD", "MX"])
    repetitions = int(config.get("parameter_recovery_replicates", config.get("recovery_repetitions", 100)))
    if "parameter_recovery_replicates" in config and "recovery_repetitions" in config and config["parameter_recovery_replicates"] != config["recovery_repetitions"]:
        raise ValueError("Conflicting declared parameter-recovery replication counts")
    effects = config.get("recovery_effects", [0, .25, .5, 1.0])
    seed = int(config.get("seed", 0))
    for generating in models:
        for effect in effects:
            for replicate in range(repetitions):
                truth = MovementModel(generating, len(scaler.mean), len(scaler.modulation_mean),
                    templates[0].observations.shape[1], training_duration_knots(sequences),
                    bin_seconds=templates[0].bin_seconds)
                import torch
                with torch.no_grad():
                    truth.log_rest_scale.fill_(-1.5)
                    truth.exit_intercept.copy_(truth._tensor([-2.5, -1.5]))
                    truth.axis_beta.fill_(.5)
                    if generating in ("MG", "MX"):
                        truth.gain_mod[:, 0] = effect
                    if generating in ("MT", "MX"):
                        truth.transition_mod[:, 0] = truth._tensor([effect, -effect])
                    if generating in ("MD", "MX"):
                        truth.direction_mod[:, 0] = effect
                synthetic = [simulate(truth, template, seed + replicate * len(templates) + i)[0]
                             for i, template in enumerate(templates)]
                # Completely independent experiment validation for recovery too.
                for candidate in config.get("models", ["M0", "MG", "MT", "MD", "MX"]):
                    for fold, (train, test) in enumerate(grouped_folds(synthetic, int(config.get("inner_folds", 3)))):
                        fit = fit_model(train, candidate, config)
                        uncertainty = full_parameter_laplace(fit, train) if candidate == generating else {}
                        scale = np.asarray(fit.scaler.modulation_scale)
                        for sequence in test:
                            record = fit.score(sequence, config.get("warmup_seconds", 600))
                            record.update(generating=generating, effect=effect, replicate=replicate,
                                recovery_fold=fold,
                                transition_estimate=(fit.model.transition_mod.detach().cpu().numpy() / scale).tolist(),
                                gain_estimate=(fit.model.gain_mod.detach().cpu().numpy() / scale).tolist(),
                                direction_estimate=(fit.model.direction_mod.detach().cpu().numpy() / scale).tolist(),
                                transition_true=truth.transition_mod.detach().cpu().tolist(),
                                gain_true=truth.gain_mod.detach().cpu().tolist(),
                                direction_true=truth.direction_mod.detach().cpu().tolist(),
                                parameter_scale="Generating normalized covariate units; refit training scales explicitly undone")
                            record.update(uncertainty)
                            records.append(record)
    frame = pd.DataFrame(records)
    frame.to_csv(output / "parameter_recovery.tsv", sep="\t", index=False)
    grouped = frame.groupby(["generating", "effect", "replicate", "model", "experiment_id"]).log_score.mean()
    scores = grouped.groupby(level=[0, 1, 2, 3]).mean().unstack("model")
    winners = scores.idxmax(axis=1)
    confusion = pd.crosstab(winners.index.get_level_values("generating"), winners)
    confusion.to_csv(output / "model_recovery_confusion.tsv", sep="\t")
    gain = winners[winners.index.get_level_values("generating") == "MG"]
    false_selective = float(gain.isin(["MT", "MX"]).mean()) if len(gain) else None
    result = {"status": "complete", "n_fits_scored": len(frame), "gain_only_false_selective_fraction": false_selective,
              "parameter_recovery_replicates": repetitions, "settings": config,
              "precision_interpretation": "Simulated observations are not additional brains or flies"}
    (output / "recovery_summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def parameter_identifiability_from_recovery(config: dict[str, Any], root: Path) -> dict[str, Any]:
    """Bias/precision/coverage from actual known-truth runs, never invented SEs."""
    from .core import atomic_json, canonical_hash, digest, source_identity
    root = Path(root)
    settings = config.get("behaviour", {})
    path = root / "results" / "behaviour" / "recovery" / "parameter_recovery.tsv"
    summary_path = path.parent / "recovery_summary.json"
    pending, diagnostics = [], []
    if not path.exists():
        pending.append("Known-truth recovery outputs have not been generated")
        frame = pd.DataFrame()
    else:
        frame = read_table(path)
    required = {"generating", "effect", "replicate", "model", "recovery_fold", "experiment_id"}
    required.update(f"{component}_{suffix}" for component in ("transition", "gain", "direction") for suffix in ("true", "estimate"))
    if required - set(frame):
        pending.append(f"Recovery table lacks auditable truth/fold/scale fields {sorted(required - set(frame))}")
    else:
        frame = frame[frame.model.eq(frame.generating)].copy()
        keys = ["generating", "effect", "replicate", "model", "recovery_fold"]
        # One fitted vector appears on every held-out animal score; these are
        # repeated measurements of that fit, not additional recovery replicates.
        for component in ("transition", "gain", "direction"):
            for suffix in ("true", "estimate"):
                if (frame.groupby(keys)[f"{component}_{suffix}"].nunique() != 1).any():
                    raise ValueError("Recovery fit parameters differ across its repeated animal score rows")
        fits = frame.drop_duplicates(keys).copy()
        expected_replicates = int(settings.get("parameter_recovery_replicates", 100))
        for (model, effect, fold), rows in fits.groupby(["model", "effect", "recovery_fold"]):
            if set(rows.replicate) != set(range(expected_replicates)):
                pending.append(f"{model}/{effect}/fold{fold}: incomplete independent simulation roster")
                continue
            components = {"MG": ["gain"], "MT": ["transition"], "MD": ["direction"],
                          "MX": ["transition", "gain", "direction"]}[model]
            for component in components:
                decode = lambda value: np.asarray(json.loads(value) if isinstance(value, str) else value, float).ravel()
                estimated = np.vstack([decode(value) for value in rows[f"{component}_estimate"]])
                truth = np.vstack([decode(value) for value in rows[f"{component}_true"]])
                if estimated.shape != truth.shape or not np.isfinite(estimated).all() or not np.isfinite(truth).all():
                    raise ValueError("Recovery parameters have incompatible dimensions or nonfinite values")
                bias = (estimated - truth).mean(0)
                rmse = np.sqrt(np.mean((estimated - truth) ** 2, axis=0))
                coverage = None
                low_key, high_key = f"{component}_interval_low", f"{component}_interval_high"
                if low_key in rows and high_key in rows:
                    if not {"parameter_uncertainty_method", "parameter_uncertainty_validated"}.issubset(rows):
                        pending.append("Recovery intervals lack their full-parameter uncertainty validation")
                    elif not rows.parameter_uncertainty_validated.eq(True).all():
                        pending.append("At least one recovery uncertainty calculation was not validated")
                    elif not set(rows.parameter_uncertainty_method).issubset({"full_parameter_laplace", "adequate_posterior_sampling"}):
                        pending.append("Conditional or empirical across-simulation SD cannot validate nominal fitted-parameter coverage")
                    else:
                        low, high = np.vstack([decode(value) for value in rows[low_key]]), np.vstack([decode(value) for value in rows[high_key]])
                        if low.shape != truth.shape or high.shape != truth.shape or not np.isfinite(low).all() or not np.isfinite(high).all() or (low > high).any():
                            raise ValueError("Recovery fitted-parameter intervals are invalid")
                        coverage = np.mean((low <= truth) & (truth <= high), axis=0).tolist()
                else:
                    pending.append(f"{model}/{effect}/fold{fold}/{component}: nominal fitted-parameter interval coverage not measured")
                diagnostics.append({"generating_model": model, "effect": float(effect), "recovery_fold": int(fold),
                    "parameter_class": component, "independent_simulations": len(rows),
                    "bias": bias.tolist(), "rmse": rmse.tolist(), "coverage95": coverage,
                    "point_recovery_passed": bool(np.max(np.abs(bias)) <= .1 and np.max(rmse) <= .25),
                    "coverage_passed": bool(coverage is not None and min(coverage) >= .9 and max(coverage) <= .99)})
        planned = len(settings.get("recovery_models", ["MG", "MT", "MD", "MX"])) * len(settings.get("recovery_effects", [0, .25, .5, 1])) * min(int(settings.get("inner_folds", 3)), frame.experiment_id.nunique())
        if len(fits.groupby(["model", "effect", "recovery_fold"])) != planned:
            pending.append("Recovery does not cover every declared generating-model/effect/fold cell")
    passed = bool(diagnostics and not pending and all(row["point_recovery_passed"] and row["coverage_passed"] for row in diagnostics))
    inputs = [candidate for candidate in (path, summary_path) if candidate.exists()]
    result = {"status": "pending" if pending else "complete", "parameter_identifiability_passed": passed,
        "pending": sorted(set(pending)), "diagnostics": diagnostics,
        "criteria": {"absolute_bias_max": .1, "rmse_max": .25, "nominal_fitted_interval": .95,
                     "empirical_coverage_range": [.9, .99], "independent_simulations_per_cell": int(settings.get("parameter_recovery_replicates", 100))},
        "independent_unit": "independent known-truth simulation, with grouped-fold estimates kept separate",
        "evidence_class": "parameter recovery diagnostic using original clocks; simulations are not additional biological subjects",
        "config_sha256": canonical_hash(config), "source_sha256": source_identity(Path(__file__).resolve().parents[2])["sha256"],
        "source_result_paths": [str(candidate.resolve()) for candidate in inputs],
        "source_result_hashes": {str(candidate.resolve()): digest(candidate) for candidate in inputs}}
    atomic_json(root / "results" / "behaviour" / "parameter_identifiability.json", result)
    return result
