"""Source amendments preserve absent treatment metadata and deposited identities."""
import numpy as np
import pandas as pd

from horsehair.behaviour import audit_exposure_source


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
