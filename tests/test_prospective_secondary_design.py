"""Synthetic documentation only: no raw scientific files or external programs."""
import copy
import importlib.util
import json
from pathlib import Path

import pytest


SPEC = importlib.util.spec_from_file_location("prospective_design", Path(__file__).parents[1] / "scripts/prospective_secondary_design.py")
d = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(d)


def r_source():
    fields = ",".join(json.dumps(field) for field in d.FIELDS)
    header = f'''colnames(df_m1)[1:10] <- c({fields})
colnames(df_m2)[1:10] <- c({fields})
for(i in 1:32) {{}}
parse_date_time(Datetime_raw, orders="d b y H M S", tz="UTC")
'''
    for number in range(1, 4):
        header += f'''exp{number}_channels <- list(
monitor1 = list(DAV=c(1,2), Bloomfield=c(3), DCV=c(4), Nora=c(5), Uninfected=c(6,7), Empty=c(8)),
monitor2 = list(DAV=c(1), Bloomfield=c(2), DCV=c(3), Nora=c(4), Uninfected=c(5), Empty=c(6)))
exp{number} <- process_experiment(".../folder{number}/Monitor1.txt", ".../folder{number}/Monitor2.txt",
"Exp{number}", "example{number}", "2025-04-01 08:00:00", "2025-04-04 08:00:00", exp{number}_channels)
'''
    return header.encode()


def row(**updates):
    result = dict(zip(d.ETHOSCOPE_FIELDS, ["2020-01-01", "ETHOSCOPE_1", "1", "stockA", "male", "PBS",
        "25C", "29C", "0", "5", "regular", "A", "ok", "Figure01", "1", "no_need", "no_need", "0"]))
    result.update(updates)
    return result


def tsv(rows):
    return ("\t".join(d.ETHOSCOPE_FIELDS) + "\n" + "\n".join("\t".join(r[k] for k in d.ETHOSCOPE_FIELDS) for r in rows) + "\n").encode()


def test_dam_unique_units_and_reused_controls_are_separate():
    design = d.dam_design(r_source())
    assert design["source_channel_slots"] == 192
    assert design["unique_coded_nonempty_channels"] == 36
    assert design["source_experiments"] == 3
    assert design["source_monitors"] == 6
    assert design["shared_reference_unique_channels"] == 9
    assert len(design["contrast_strata"]) == 24
    assert design["contrast_rows_with_reused_controls"] == 63
    assert design["stream_plan"]["streams_are_replicates"] is False
    assert design["experiments"][0]["window"]["minute_positions"] == 4321
    assert design["experiments"][0]["window"]["minute_intervals"] == 4320
    assert all(len(split["train"]) == 2 and split["test"] not in split["train"] for split in design["heldout_experiments"])


def test_shared_controls_are_exact_same_channel_ids():
    contrasts = d.dam_design(r_source())["contrast_strata"]
    group = [r for r in contrasts if r["source_experiment"] == "Exp1" and r["source_monitor"] == 1]
    assert len(group) == 4
    assert all(r["shared_reference_channel_ids"] == ["Exp1/monitor1/channel6", "Exp1/monitor1/channel7"] for r in group)


def test_comments_and_hash_inside_string_remain_data():
    source = r_source().replace(b'"example1"', b'"example#1"') + b'\n# exp4 <- process_experiment(system("bad"))\n'
    assert d.dam_design(source)["experiments"][0]["name_literal"] == "example#1"


def test_original_path_value_preserved_separately_from_normalized_name():
    source = r_source().replace(b".../folder1/Monitor1.txt", b"C:\\\\folder1\\\\Monitor1.txt")
    monitor = d.dam_design(source)["experiments"][0]["monitors"][0]
    assert monitor["source_path_value"] == "C:\\folder1\\Monitor1.txt"
    assert monitor["normalized_provider_name"] == "folder1_Monitor1.txt"


@pytest.mark.parametrize("change", [
    lambda raw: raw.replace(b"DAV=c(1,2)", b"DAV=system(\"bad\")", 1),
    lambda raw: raw.replace(b"DAV=c(1,2)", b"DAV=c(1,1)", 1),
    lambda raw: raw.replace(b"DAV=c(1,2)", b"DAV=c(1,33)", 1),
    lambda raw: raw.replace(b"Bloomfield=c(3)", b"Bloomfield=c(2)", 1),
    lambda raw: raw.replace(b"Empty=c(8)", b"Unknown=c(8)", 1),
    lambda raw: raw.replace(b"exp3 <-", b"exp2 <-", 1),
    lambda raw: raw.replace(b'"2025-04-04 08:00:00"', b'"2025-04-01 08:00:00"', 1),
    lambda raw: raw.replace(b"exp3_channels)", b"missing_channels)", 1),
    lambda raw: raw.replace(b'"Tube_number"', b'"Unknown_column"', 1),
    lambda raw: raw.replace(b'tz="UTC"', b'tz="unknown"', 1),
])
def test_ambiguous_or_executable_dam_design_refused(change):
    with pytest.raises(ValueError):
        d.dam_design(change(r_source()))


@pytest.mark.parametrize("text", ['c(1);system("bad")', 'list(a=c(1),a=c(2))', 'c(-1)', 'c(1+2)', 'c("a\\x41")'])
def test_small_literal_grammar_refuses_execution_and_unsupported_syntax(text):
    with pytest.raises(ValueError):
        d.Literals(text).complete()


def test_ethoscope_cross_figure_rows_not_independent_animals():
    rows = [row(), row(figure="FigureS01"), row(region_id="2", treatment="infection", obs="no_fly")]
    design = d.ethoscope_design(tsv(rows))
    assert design["metadata_rows"] == 3
    assert design["unique_date_machine_ROIs"] == 2
    assert design["cross_figure_repeated_rows"] == 1
    assert not design["cross_figure_metadata_conflicts"]
    figure = next(f for f in design["figures"] if f["figure_literal"] == "Figure01")
    assert figure["metadata_ROIs"] == 2
    assert figure["obs_literals"] == {"no_fly": 1, "ok": 1}
    assert design["obs_or_to_count_filters_applied"] is False


def test_ethoscope_conflicts_preserved_without_label_invention():
    design = d.ethoscope_design(tsv([row(), row(figure="FigureS01", treatment="001Fnov")]))
    assert design["cross_figure_metadata_conflicts"][0]["conflicting_metadata_fields"] == ["treatment"]
    assert design["figures"][1]["experiments"][0]["treatments"][0]["label_literal"] == "001Fnov"


@pytest.mark.parametrize("rows", [[row(), row()], [row(region_id="0")], [row(region_id="1.0")], [row(treatment="")]])
def test_ethoscope_ambiguous_identity_or_missing_design_refused(rows):
    with pytest.raises(ValueError):
        d.ethoscope_design(tsv(rows))


def test_design_deterministic_and_all_scientific_clearance_false():
    raw = tsv([row(), row(region_id="2", treatment="different")])
    first = d.inventory(r_source(), raw)
    second = d.inventory(r_source(), raw)
    assert json.dumps(first, sort_keys=True, allow_nan=False) == json.dumps(second, sort_keys=True, allow_nan=False)
    for value in (first, first["DAM"], first["ethoscope"]):
        assert all(value[key] is False for key in d.FALSE_FLAGS)
    assert first["DAM"]["source_sha256"] == d.sha(r_source())


def test_read_document_requires_exact_sha_and_bound(tmp_path):
    path = tmp_path / "doc.txt"
    path.write_bytes(b"metadata")
    assert d.read_document(path, d.sha(b"metadata"), 8) == b"metadata"
    with pytest.raises(ValueError, match="mismatch"):
        d.read_document(path, "0" * 64, 8)
    with pytest.raises(ValueError, match="mismatch"):
        d.read_document(path, d.sha(b"metadata"), 7)


def test_cli_source_pins_and_exclusive_publication(tmp_path, monkeypatch):
    code, metadata = tmp_path / "code.R", tmp_path / "metadata.txt"
    code.write_bytes(r_source())
    metadata.write_bytes(tsv([row()]))
    monkeypatch.setattr(d, "DAM_SHA256", d.sha(code.read_bytes()))
    monkeypatch.setattr(d, "ETHOSCOPE_SHA256", d.sha(metadata.read_bytes()))
    output = tmp_path / "inventory.json"
    args = ["--dam-code", str(code), "--ethoscope-metadata", str(metadata), "--output", str(output)]
    assert d.main(args) == 0
    before = output.read_bytes()
    with pytest.raises(FileExistsError):
        d.main(args)
    assert output.read_bytes() == before
    metadata.write_bytes(b"changed")
    with pytest.raises(ValueError, match="mismatch"):
        d.main(args[:-1] + [str(tmp_path / "never.json")])
    assert not (tmp_path / "never.json").exists()
