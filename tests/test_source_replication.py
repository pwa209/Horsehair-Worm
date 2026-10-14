"""Source amendments preserve absent treatment metadata and deposited identities."""
import numpy as np
import pandas as pd
import pytest
from scipy import io

from horsehair.behaviour import audit_exposure_source
from horsehair.behaviour import (_summit_source_text, _summit_data_pair,
                                replicate_source_summit)


def test_legacy_classifier_cohort_claim_cannot_promote_blank_original_rx(tmp_path):
    pd.DataFrame({"Rx": [None, None], "Outcome": [0, 1]}).to_excel(tmp_path / "metadata.xlsx", index=False)
    source = {"metadata": "metadata.xlsx", "cohort_exposure_status": 1,
        "cohort_exposure_provenance": "eLife85410 Methods Real-time summiting classifier: all1306 training flies exposed",
        "exposure_elapsed_at_start_hours": 72, "exposure_clock_provenance": "later provided clock"}
    audit = audit_exposure_source(source, tmp_path)
    assert not audit["ready"]
    assert audit["requires_exposure_annotation_regeneration"]
    assert audit["exposed_animals"] == 0 and audit["unresolved_exposure_animals"] == 2
    assert any("regenerate" in reason for reason in audit["pending"])


def test_explicit_assay_treatment_proof_still_requires_exposure_clock(tmp_path):
    pd.DataFrame({"Rx": ["Exposed", "Unexposed"],
        "exposure_elapsed_at_start_hours": [np.nan, np.nan]}).to_excel(tmp_path / "metadata.xlsx", index=False)
    source = {"metadata": "metadata.xlsx", "exposure_values": {"Exposed": 1, "Unexposed": 0}}
    audit = audit_exposure_source(source, tmp_path)
    assert not audit["ready"] and audit["exposed_animals_missing_offset"] == 1
    assert audit["exposed_animals"] == audit["unexposed_animals"] == 1
    assert audit["unresolved_exposure_animals"] == 0
    assert any("offset" in reason for reason in audit["pending"])
    frame = pd.read_excel(tmp_path / "metadata.xlsx")
    frame.loc[0, "exposure_elapsed_at_start_hours"] = 48.5
    frame.to_excel(tmp_path / "metadata.xlsx", index=False)
    source["exposure_clock_provenance"] = "Original treatment log matched to recording start"
    assert audit_exposure_source(source, tmp_path)["ready"]


def matlab_singleton_string(text, index=1):
    encoded = text.encode("utf-16-le")
    units = len(encoded) // 2
    padded = encoded + bytes((-len(encoded)) % 8)
    payload = np.r_[np.array([1, 2, 1, 1, units], np.uint64), np.frombuffer(padded, dtype="<u8")]
    cells = [np.zeros(1), np.zeros(1), *[np.zeros(1) for _ in range(index)]]
    cells[index + 1] = payload
    marker = np.array([3707764736, 2, 1, 1, index, 1], dtype=np.uint32)[:, None]
    return marker, {"#subsystem#": {"MCOS": cells}}


def test_original_mcos_source_path_and_unicode_are_decoded_without_executing_matlab():
    path = r"D:\Harvard\Sex diff\Cs WF_Female_last12 hours.mat"
    marker, contents = matlab_singleton_string(path)
    assert _summit_source_text(marker, contents) == path
    marker, contents = matlab_singleton_string("Cs WF_♀😀", index=2)
    assert _summit_source_text(marker, contents) == "Cs WF_♀😀"
    assert _summit_source_text("original source", {}) == "original source"


@pytest.mark.parametrize("fault", ["magic", "class", "index", "shape", "payload_header", "length", "padding", "missing", "subsystem"])
def test_unrecognized_mcos_payload_never_becomes_a_filename(fault):
    marker, contents = matlab_singleton_string("female.mat")
    if fault == "magic": marker[0] = 0
    if fault == "class": marker[1] = 3
    if fault == "index": marker[4] = 7
    if fault == "shape": marker = marker[:5]
    if fault == "payload_header": contents["#subsystem#"]["MCOS"][2][1] = 4
    if fault == "length": contents["#subsystem#"]["MCOS"][2][4] = 123
    if fault == "padding": contents["#subsystem#"]["MCOS"][2][-1] |= np.uint64(1 << 48)
    if fault == "missing": contents = {}
    if fault == "subsystem": contents["#subsystem#"] = np.arange(6)
    assert _summit_source_text(marker, contents) is None
    assert _summit_source_text([np.arange(3), np.arange(8)], {}) is None


def source_pair_fixture(root, name="Cs_tallwells_30deg", *, double_extension=True):
    directory = root / "data/extracted/fly/Fig 1 + Supp"
    directory.mkdir(parents=True)
    speed = np.array([0, 0, 2, 4, 4, 1, 0, 2, 0, 0, 0, 0], float)[:, None]
    data = {"Cadavers": {"speed": {"toDmatrix": speed},
        "ypos": {"toDmatrix": np.arange(12, dtype=float)[:, None]}},
        "params": {"lastFrames": 9, "fr": 3}, "dateStamp": "2020-6-29", "timeStamp": "19_58_47"}
    # These values are hand-integrated from the fixture. They are not produced
    # by calling the implementation to manufacture its own expected results.
    expected = {"numCad": 1, "sumStart": 3, "sumEnd": 8, "sumDur": 5 / 10800,
        "sumInt": [.2], "sumHt": [5.], "psWalkSpeed": [2.], "psDist": [1 / 3],
        "meanSumInt": .2, "meanSumHt": 5., "meanPsWalkSpeed": 2., "meanPsDist": 1 / 3,
        "sourceStamp": "2020-6-29_19_58_47", "sourceStruct": r"D:\author\original_last12 hours.mat",
        "params": {"smoothWin": 1, "summitStart": 3, "endPad": 0, "minSumDur": 0, "moveThresh": .1}}
    data_path = directory / (name + (".mat.mat" if double_extension else ".mat"))
    quant_path = directory / (name + "_QUANT.mat")
    io.savemat(data_path, {"data": data})
    io.savemat(quant_path, {"summit": expected})
    return directory, data_path, quant_path, data, expected


def test_double_mat_archive_alias_reproduces_hand_integrated_metrics_in_new_output(tmp_path):
    directory, data_path, quant_path, _, _ = source_pair_fixture(tmp_path)
    original = tmp_path / "results/behaviour/source_replication"
    original.mkdir(parents=True)
    original_receipt = original / "source_replication.json"
    original_receipt.write_bytes(b'{"status":"original-pending"}')
    output = tmp_path / "maintenance/source-amendment/numeric"
    result = replicate_source_summit({}, tmp_path, output_dir=output)
    assert result["status"] == "complete" and result["source_replication_passed"]
    assert result["comparisons"] == 12
    assert result["source_pair_audits"][0]["source_data"] == str(data_path.resolve())
    assert str(quant_path.resolve()) in result["source_result_hashes"]
    assert original_receipt.read_bytes() == b'{"status":"original-pending"}'
    before = (output / "source_replication.json").read_bytes()
    with pytest.raises(FileExistsError):
        replicate_source_summit({}, tmp_path, output_dir=output)
    assert (output / "source_replication.json").read_bytes() == before


@pytest.mark.parametrize("fault", ["cohort", "version", "unknown_version", "quant_vector"])
def test_source_version_or_column_mismatch_remains_pending_without_partial_false_passes(tmp_path, fault):
    _, data_path, quant_path, data, expected = source_pair_fixture(tmp_path, "Cs_30deg_food0", double_extension=False)
    if fault == "cohort":
        data["Cadavers"]["speed"]["toDmatrix"] = np.tile(data["Cadavers"]["speed"]["toDmatrix"], (1, 2))
        data["Cadavers"]["ypos"]["toDmatrix"] = np.tile(data["Cadavers"]["ypos"]["toDmatrix"], (1, 2))
    if fault == "version": data["dateStamp"] = "2021-6-29"
    if fault == "unknown_version": expected.pop("sourceStamp")
    if fault == "quant_vector": expected["sumInt"] = [.2, .2]
    io.savemat(data_path, {"data": data}); io.savemat(quant_path, {"summit": expected})
    original_data = data_path.read_bytes()
    result = replicate_source_summit({}, tmp_path, output_dir=tmp_path / "maintenance/numeric")
    assert result["status"] == "pending" and not result["source_replication_passed"]
    assert result["comparisons"] == 0 and not result["source_pair_audits"][0]["paired_source_ready"]
    assert result["pending"][0]["reason_code"] == "deposited_source_version_or_cohort_mismatch"
    assert str(data_path.resolve()) in result["source_result_hashes"]
    assert data_path.read_bytes() == original_data


@pytest.mark.parametrize("sex", ["F", "M"])
def test_absent_sex_specific_sources_are_not_synthesized_from_mixed_sex_data(tmp_path, sex):
    directory, _, quant_path, _, _ = source_pair_fixture(tmp_path, "Cs_all_standard", double_extension=False)
    quant_path.rename(directory / f"Cs_all_standard_{sex}_QUANT.mat")
    result = replicate_source_summit({}, tmp_path, output_dir=tmp_path / "maintenance/numeric")
    assert result["status"] == "pending" and result["comparisons"] == 0
    assert "No unique deposited" in result["pending"][0]["reason"]
    assert result["source_pair_audits"] == []


def test_pair_resolution_refuses_ambiguous_aliases_and_escaped_outputs(tmp_path):
    directory, data_path, quant_path, _, _ = source_pair_fixture(tmp_path)
    data_path.with_suffix("").write_bytes(data_path.read_bytes())
    with pytest.raises(ValueError, match="No unique deposited"):
        _summit_data_pair(directory, quant_path, None)
    with pytest.raises(ValueError, match="specified study root"):
        replicate_source_summit({}, tmp_path, output_dir=tmp_path.parent / "escaped-source-results")


@pytest.mark.parametrize("fault", ["absolute", "parent", "outside_source_directory", "non_quant", "symlink"])
def test_designated_quantitative_sources_cannot_escape_or_replace_the_inspected_roster(tmp_path, monkeypatch, fault):
    from pathlib import Path
    directory, data_path, quant_path, _, _ = source_pair_fixture(tmp_path)
    if fault in {"absolute", "parent"}:
        foreign = tmp_path.parent / "foreign_QUANT.mat"
        foreign.write_bytes(b"Unrelated bytes must never be loaded or hashed")
        designated = str(foreign) if fault == "absolute" else "../foreign_QUANT.mat"
    elif fault == "outside_source_directory":
        foreign = tmp_path / "foreign_QUANT.mat"
        foreign.write_bytes(b"Unrelated bytes must never be loaded or hashed")
        designated = "foreign_QUANT.mat"
    elif fault == "non_quant":
        designated = str(data_path.relative_to(tmp_path))
    else:
        designated = str(quant_path.relative_to(tmp_path))
        actual = Path.is_symlink
        monkeypatch.setattr(Path, "is_symlink", lambda path: path == quant_path or actual(path))
    output = tmp_path / "maintenance/numeric"
    with pytest.raises(ValueError, match="regular QUANT paths beneath"):
        replicate_source_summit({"behaviour": {"source_replication_files": [designated]}},
                                tmp_path, output_dir=output)
    assert not output.exists()
