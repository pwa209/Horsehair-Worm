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
    table.response = table.response.astype(str).str.strip()
    categories = {"hori-pola": "polarized", "unpola": "unpolarized", "M": "middle"}
    if not set(table.response).issubset(categories):
        raise ValueError("Unrecognized native response category; result_hori cannot recover nonchoice")
    table.response = table.response.map(categories)
    if "result_hori" in table:
        expected = table.response.eq("polarized").astype(int)
        choice = ~table.response.eq("middle")
        if not table.loc[choice, "result_hori"].eq(expected[choice]).all():
            raise ValueError("Native result and result_hori contradict each other")
    if not set(table.stimulus_side).issubset({"R", "L"}):
        raise ValueError("Native polarized stimulus side is not R/L")
    if "location" in table:
        expected = np.where(table.response.eq("middle"), "M",
                            np.where(table.response.eq("polarized"), table.stimulus_side,
                                     table.stimulus_side.map({"R": "L", "L": "R"})))
        if not table.location.astype(str).eq(expected).all():
            raise ValueError("Native location, window side and response coding contradict each other")
    if table.individual_id.isna().any():
        raise ValueError("Missing source individual identifier")
    table.individual_id = table.individual_id.astype(str)
    if "trial_id" not in table:
        # Row keys are provenance only, never additional animal identities.
        table["source_row_index"] = np.arange(len(table)) + 2
    elif table.duplicated(["individual_id", "year", "date", "trial_id"]).any():
        raise ValueError("Duplicate source trial number; reconcile sheet rows before analysis")
    report = {"doi": POLARIZATION_DOI, "sheet_role": "Data S2 laboratory two-choice test",
              "native_choice_field": "result", "three_response_categories": categories,
              "trials": len(table), "individuals": int(table.individual_id.nunique()),
              "repeated_trials": int(len(table) - table.individual_id.nunique()),
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
    report.update(sheet=sheet, header_row=header + 1,
                  source_path=str(Path(path).resolve()), source_sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest())
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


def discover_fly_sources(directory: Path, root: Path, config: dict[str, Any] | None = None
                         ) -> dict[str, Any]:
    """Resolve publication-associated chronological pairs; retain every ambiguity.

    Per-file MARGO target_rate is a nominal frame clock, not measured elapsed
    timestamps. An explicit configuration permits that documented clock route.
    Geometry is resolved from inspected metadata or an audited assay declaration.
    """
    config = config or {}
    directory, root = Path(directory), Path(root)
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
            if len(metadata) != count or not {"Geno", "Sex"}.issubset(metadata):
                raise ValueError("Survival rows/columns do not match source ROIs")
            source = {"mat": str(path.relative_to(root)), "metadata": str(preferred[0].relative_to(root)),
                "experiment_id": stem, "centroid_path": "expmt.Centroid.data",
                "axis_order": "time,coordinate,roi", "roi_by_row_verified": True,
                "roi_mapping_provenance": "Author triggerOnDeath2019Fun.m rows 1:height(survival), validated against centroid ROI dimension",
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
            elif not found and config.get("allow_documented_nominal_frame_clock", False):
                rate = float(np.asarray(nested(value, "expmt.parameters.target_rate")).squeeze())
                if not np.isfinite(rate) or rate <= 0:
                    raise ValueError("Invalid per-experiment MARGO target_rate")
                match = re.search(r"(?P<m>\d{2})-(?P<d>\d{2})-(?P<y>20\d{2})-(?P<h>\d{2})-(?P<min>\d{2})-(?P<s>\d{2})", stem)
                if not match:
                    raise ValueError("No source recording start clock in experiment identity")
                start = int(match["h"]) * 3600 + int(match["min"]) * 60 + int(match["s"])
                source.update(frame_rate=rate, start_time_s=start,
                    timestamp_provenance="Per-experiment expmt.parameters.target_rate and source filename recording start; nominal frame clock, publication author conversion checks rate",
                    clock_kind="nominal_frame_clock")
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
                declaration = {"geometry": 30, "food_end": 0, "roi_arena_bounds": mapped,
                    "invert_y": True, "analysis_role": "behavioural_development",
                    "arena_normalization": "local_physical" if mm_scale and mm_scale > 0 else "normalized_arena",
                    "coordinate_scale": mm_scale if mm_scale and mm_scale > 0 else 1,
                    "coordinate_units": "mm" if mm_scale and mm_scale > 0 else "normalized_arena",
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
    field = "exposure_status" if "exposure_status" in metadata else "Rx"
    if field not in metadata:
        return {"ready": False, "pending": ["Original source exposure/Rx column absent"]}
    raw = metadata[field]
    codes = raw.astype(str).map(source.get("exposure_values", {}))
    codes.loc[raw.astype(str).str.lower().isin(["uninfected", "unexposed"])] = 0
    pending = []
    unknown = sorted(raw[codes.isna()].astype(str).unique())
    if unknown or not set(codes.dropna()).issubset({0, 1}):
        pending.append(f"Published exposed/unexposed encoding unresolved for native labels {unknown}")
    offset = metadata.get("exposure_elapsed_at_start_hours", pd.Series(source.get("exposure_elapsed_at_start_hours", np.nan), index=metadata.index))
    offset = pd.to_numeric(offset, errors="coerce")
    if not np.isfinite(offset[codes.eq(1)]).all() or (offset[codes.eq(1)] < 0).any():
        pending.append("Recording-start offset from exposure is unavailable; Day3 directory labels do not establish elapsed hours")
    if codes.eq(1).any() and not source.get("exposure_clock_provenance"):
        pending.append("Exposure-clock/protocol provenance is not recorded")
    return {"ready": not pending, "native_exposure_levels": sorted(raw.astype(str).unique()),
            "pending": pending, "exposure_clock_provenance": source.get("exposure_clock_provenance"),
            "rule": "Exposure is measured from original treatment assignment, never from future cadaver/survival labels"}


def read_table(path: Path, sheet: str | int | None = None) -> pd.DataFrame:
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path, sheet_name=0 if sheet is None else sheet)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, sep="\t" if path.suffix.lower() == ".tsv" else ",")


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
    roi_column = source.get("roi_column", "ROI")
    if roi_column in metadata:
        indices = pd.to_numeric(metadata[roi_column], errors="raise").astype(int)
    elif source.get("roi_by_row_verified") and len(metadata) == rois:
        indices = pd.Series(np.arange(1, rois + 1), index=metadata.index)
    else:
        raise ValueError("ROI-to-animal mapping is ambiguous; verify source ROI column or row convention")
    if indices.duplicated().any() or not indices.between(1, rois).all():
        raise ValueError("Duplicate or out-of-range metadata ROI")
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
                                 else "normalized_arena" if local_bounds else source.get("coordinate_units", "source_units")),
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
            context = np.r_[context, modulation, (food_target - position) / fit.output_scale,
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
    recovery_passed = bool(false_positive is not None and false_positive <=
        config.get("behaviour", {}).get("maximum_gain_false_selective_fraction", .05))
    identifiability_path = root / "results" / "behaviour" / "parameter_identifiability.json"
    identifiability = json.loads(identifiability_path.read_text(encoding="utf-8")) if identifiability_path.exists() else None
    identifiability_passed = bool(identifiability and identifiability.get("parameter_identifiability_passed") is True
        and identifiability.get("source_result_paths") and identifiability.get("source_result_hashes"))
    if identifiability_passed:
        for path in identifiability["source_result_paths"]:
            if identifiability["source_result_hashes"].get(path) != digest(Path(path)):
                raise ValueError("Identifiability evidence changed after validation")
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
                                and diagnostic_improvement is not None and diagnostic_improvement >= .05)
    manifest = root / "manifests" / "source_manifest.json"
    if not manifest.exists():
        raise FileNotFoundError("A frozen actual source manifest is required for H1 provenance")
    result.update(claim_id="H1", evidence_class="biological_reanalysis",
        config_sha256=canonical_hash(config), source_sha256=source_identity(Path(__file__).resolve().parents[2])["sha256"],
        data_manifest_sha256=digest(manifest), selected_by_outer_fold=choices,
        selection_rule="MT versus MX selected by grouped inner validation only; outer scores never select candidates",
        parameter_recovery_passed=recovery_passed, gain_false_selective_fraction=false_positive,
        parameter_identifiability_passed=identifiability_passed,
        parameter_identifiability_status=("validated evidence supplied" if identifiability_passed else
            "pending: parameter separation, bias/coverage and posterior adequacy have not all been validated"),
        multivariate_diagnostic_improvement=diagnostic_improvement,
        operational_gates_passed=bool(identifiability_passed and recovery_passed and diagnostic_improvement is not None and diagnostic_improvement >= .05),
        required_checks={"inner_only_candidate_selection": True, "complete_paired_outer_predictions": True,
            "parameter_identifiability": identifiability_passed,
            "gain_false_selective_recovery_passed": recovery_passed,
            "five_percent_multivariate_diagnostic_improvement": diagnostic_improvement is not None and diagnostic_improvement >= .05},
        claim_status="pending_joint_Holm" if supported_components else "unsupported_or_required_diagnostic_pending",
        multiplicity_status="H1/H2/H3 joint Holm adjustment pending",
        source_result_paths=[str(score_path.resolve()), str(inner_path.resolve())] +
            ([str(recovery_path.resolve())] if recovery_path.exists() else []) +
            ([str(identifiability_path.resolve())] if identifiability_path.exists() else []) +
            ([str(diagnostic_path.resolve())] if diagnostic_path.exists() else []))
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


def design_matrix(frame: pd.DataFrame, continuous: list[str], categorical: list[str],
                  interaction: tuple[str, str] | None = None) -> tuple[np.ndarray, list[str]]:
    parts = pd.DataFrame({"Intercept": np.ones(len(frame))}, index=frame.index)
    for column in continuous:
        values = pd.to_numeric(frame[column], errors="raise").astype(float)
        if not np.isfinite(values).all():
            raise ValueError(f"Nonfinite {column}")
        # Covariates are standardized; binary infection remains 0/1 for the
        # interpretable infection-by-polarization coefficient.
        if values.nunique() == 1 and column in ("log_illuminance", "male_size", "female_size"):
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
    if np.linalg.matrix_rank(x) < x.shape[1]:
        raise ValueError("Nonestimable behavioural design; confounded effects cannot be repaired by priors")
    return x, parts.columns.tolist()


def _map_binary(x: np.ndarray, y: np.ndarray, names: list[str], groups: np.ndarray | None = None
                ) -> dict[str, Any]:
    if len(np.unique(y)) != 2:
        raise ValueError("Binary endpoint needs both observed categories")
    group_names = np.unique(groups) if groups is not None else np.array([])
    index = np.searchsorted(group_names, groups) if groups is not None else None
    p, g = x.shape[1], len(group_names)
    def objective(theta: np.ndarray) -> float:
        eta = x @ theta[:p]
        prior = .5 * np.sum((theta[:p] / 1.5) ** 2)
        if g:
            sd = np.exp(theta[-1])
            eta = eta + theta[p:p + g][index] * sd
            prior += .5 * np.sum(theta[p:p + g] ** 2) + .5 * (sd / .5) ** 2 - theta[-1]
        return float(np.sum(np.logaddexp(0, eta) - y * eta) + prior)
    initial = np.zeros(p + g + bool(g))
    if g:
        initial[-1] = -1.5
    result = optimize.minimize(objective, initial, method="BFGS", options={"maxiter": 2000, "gtol": 1e-6})
    if not np.isfinite(result.fun) or np.linalg.norm(result.jac) > .02:
        raise RuntimeError(f"Binary MAP did not converge: {result.message}")
    covariance = np.asarray(result.hess_inv)[:p, :p]
    estimates = result.x[:p]
    return {"terms": names, "coefficient": estimates.tolist(),
            "standard_error_laplace": np.sqrt(np.maximum(np.diag(covariance), 0)).tolist(),
            "covariance_laplace": covariance.tolist(), "n": len(y),
            "group_sd": float(np.exp(result.x[-1])) if g else None,
            "objective": float(result.fun), "converged": True,
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
    x, names = design_matrix(table, ["infection", "dop", "log_illuminance"],
                             ["stimulus_side", "year", "box"], ("infection", "dop"))
    groups = (table.individual_id.astype(str).to_numpy()
              if "individual_id" in table and table.individual_id.duplicated().any() else None)
    chooser = response.to_numpy() != 0
    engagement = _map_binary(x, chooser.astype(float), names, groups)
    preference = _map_binary(x[chooser], (response.to_numpy()[chooser] == 1).astype(float),
                             names, groups[chooser] if groups is not None else None)
    # Multinomial sensitivity uses unpolarized as its reference category.
    y = response.to_numpy(int)
    p = x.shape[1]
    def multinomial_loss(theta: np.ndarray) -> float:
        logits = np.column_stack([x @ theta[:p], x @ theta[p:], np.zeros(len(x))])
        return float(np.sum(special.logsumexp(logits, axis=1) - logits[np.arange(len(x)), y])
                     + .5 * np.sum((theta / 1.5) ** 2))
    mn = optimize.minimize(multinomial_loss, np.zeros(p * 2), method="BFGS")
    if not np.isfinite(mn.fun) or np.linalg.norm(mn.jac) > .02:
        raise RuntimeError("Multinomial sensitivity fit did not converge")
    return {"engagement": engagement, "conditional_preference": preference,
            "multinomial_sensitivity": {"terms": names, "reference": "unpolarized",
                "middle_coefficients": mn.x[:p].tolist(), "polarized_coefficients": mn.x[p:].tolist(),
                "covariance_laplace": np.asarray(mn.hess_inv).tolist()},
            "denominators": {"trials": len(table), "choosers": int(chooser.sum()),
                              "nonchoices": int((~chooser).sum()),
                              "animals": int(table.individual_id.nunique()) if "individual_id" in table else None},
            "identification_limit": "Conditional preference cannot distinguish perception from value",
            "independence_verified": "individual_id" in table}


def aft_objective(theta: np.ndarray, x: np.ndarray, times: np.ndarray,
                  censored: np.ndarray) -> float:
    sigma = np.exp(theta[-1])
    z = (np.log(times) - x @ theta[:-1]) / sigma
    # Source convention: 1 denotes right censoring, not event occurrence.
    log_event = stats.norm.logpdf(z) - np.log(sigma) - np.log(times)
    log_survival = special.log_ndtr(-z)
    return float(-np.where(censored, log_survival, log_event).sum()
                 + .5 * np.sum((theta[:-1] / 1.5) ** 2) + .5 * theta[-1] ** 2)


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
    x, names = design_matrix(table, ["male_infection", "female_infection", "male_size", "female_size"], ["year"])
    initial = np.zeros(x.shape[1] + 1)
    initial[0] = float(np.mean(np.log(times)))
    result = optimize.minimize(aft_objective, initial, args=(x, times, censored), method="BFGS",
                               options={"maxiter": 2000, "gtol": 1e-6})
    if not np.isfinite(result.fun) or np.linalg.norm(result.jac) > .02:
        raise RuntimeError(f"AFT fit did not converge: {result.message}")
    return {"outcome": outcome, "terms": names, "coefficient": result.x[:-1].tolist(),
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
        x, names = design_matrix(data, ["male_infection", "female_infection", "male_size", "female_size"], ["year"])
        if "male_id" in data and data.male_id.duplicated().any():
            groups = data.male_id.astype(str).to_numpy()
        else:
            groups = None
        escape = _map_binary(x, data.male_escape.to_numpy(float), names, groups)
        mounting = _map_binary(x, data.male_mounting.to_numpy(float), names, groups)
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
            "cluster_bootstrap_interval": np.quantile(draws, [.025, .975]).tolist(),
            "n_resampling_clusters": len(cluster_names), "failed_bootstrap_samples": failures,
            "individual_reuse_verified": {key: key in table for key in ("male_id", "female_id")},
            "contrast_definition": "male-infection log-odds coefficient for escape minus mounting"}


def run(config: dict[str, Any], root: Path, stage: str, **kwargs: Any) -> dict[str, Any]:
    root = Path(root)
    settings = config.get("behaviour", config)
    output = root / "results" / "behaviour"
    output.mkdir(parents=True, exist_ok=True)
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
        import pyarrow as pa
        import pyarrow.parquet as pq
        from .core import atomic_json, digest
        inventories, counts, seen, duplicate_sources = {}, {}, {}, []
        for source in sources:
            inventories[source["mat"]] = mat_inventory(root / source["mat"])
        directory = root / "data"
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / "fly_observations.parquet"
        temporary = directory / "fly_observations.partial.parquet"
        writers: dict[float, Any] = {}
        native_writer = None
        schema = None
        bin_values = sorted(set([float(settings.get("primary_bin_seconds", 10)),
            *map(float, settings.get("sensitivity_bins_seconds", [5, 20]))]))
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
                    for column in ("source_file", "experiment_id", "animal_id", "genotype", "intervention",
                                   "control_id", "sex", "outcome_status", "coordinate_units", "timestamp_provenance", "analysis_role"):
                        frame[column] = frame[column].astype("string")
                    for column in frame:
                        if column.startswith("annotation_"):
                            frame[column] = frame[column].astype("string")
                    # Arrow fixes categorical context as strings and stores NaN
                    # numerics consistently even when one ROI lacks annotations.
                    table = pa.Table.from_pandas(frame, preserve_index=False)
                    if native_writer is None:
                        schema = table.schema
                        native_writer = pq.ParquetWriter(temporary, schema, compression="zstd")
                    table = table.cast(schema)
                    native_writer.write_table(table)
                    for value in bin_values:
                        binned = bin_trajectories(frame, value,
                            missing_threshold=float(settings.get("missing_threshold", .2)),
                            maximum_speed=settings.get("maximum_speed"))
                        table = pa.Table.from_pandas(binned, preserve_index=False)
                        if value not in writers:
                            writers[value] = pq.ParquetWriter(output / f"fly_bins_{value:g}s.partial.parquet", table.schema, compression="zstd")
                        writers[value].write_table(table.cast(writers[value].schema))
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
            results["choice"] = fit_mantis_choice(choice_table, choice_settings)
            results["choice"]["source_audit"] = choice_audit
            results["mating"] = {outcome: fit_censored_latency(mating_table, outcome, mating_settings)
                                  for outcome in ("male_escape", "male_mounting")}
            results["mating"]["specificity"] = fit_mating_specificity(mating_table, mating_settings)
            results["mating"]["source_audit"] = mating_audit
            results["mating"]["inference_scope"] = ("Recorded pairs with verified individual identifiers"
                if mating_audit["independence_verified"] else
                "Pair-record uncertainty conditional on independence; individual reuse is unresolved in source identifiers")
        for name, source in declared.items():
            frame = read_table(root / source["path"], source.get("sheet"))
            if name == "choice":
                results[name] = fit_mantis_choice(frame, source)
            elif name == "mating":
                results[name] = {outcome: fit_censored_latency(frame, outcome, source)
                                 for outcome in source.get("latency_outcomes", ["male_escape", "male_mounting"])}
                results[name]["specificity"] = fit_mating_specificity(frame, source)
        if not results:
            raise ValueError("No audited mantis source schemas")
        (output / "mantis_models.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
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
        return recovery_analysis(sequences, settings, output / "recovery")
    if stage == "bayesian-check":
        from .semimarkov import bayesian_check, fit_model
        selected = settings.get("bayesian_subset_animals")
        if not selected:
            raise ValueError("Freeze representative Bayesian subset animal IDs before posterior checking")
        subset = [s for s in sequences if s.animal_id in selected]
        if len(subset) != len(selected):
            raise ValueError("Bayesian subset includes unavailable or duplicated animals")
        fit = fit_model(subset, settings.get("bayesian_subset_model", "MX"), settings)
        fit.save(output / "bayesian_subset_map.pt")
        result = bayesian_check(fit, subset, settings.get("bayesian_sampler", {}))
        np.save(output / "bayesian_subset_samples.npy", result.pop("samples"))
        (output / "bayesian_subset_diagnostics.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
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
                    for train, test in grouped_folds(synthetic):
                        fit = fit_model(train, candidate, config)
                        for sequence in test:
                            record = fit.score(sequence, config.get("warmup_seconds", 600))
                            record.update(generating=generating, effect=effect, replicate=replicate,
                                          transition_estimate=fit.model.transition_mod.detach().cpu().tolist(),
                                          gain_estimate=fit.model.gain_mod.detach().cpu().tolist())
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
    result = {"n_fits_scored": len(frame), "gain_only_false_selective_fraction": false_selective,
              "parameter_recovery_replicates": repetitions, "settings": config,
              "precision_interpretation": "Simulated observations are not additional brains or flies"}
    (output / "recovery_summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result
