"""Native array queries have no parent/member selection overlap."""
from types import SimpleNamespace

import pytest

from horsehair import repair


def native_row(job, raw, *, state="COMPLETED", seconds="100", owner="pwa209", account="def-ptewarie_cpu"):
    return "|".join((job, raw, state, "0:0", seconds, "4", "cpu=4,mem=32G,node=1", "nc10902",
                    "2026-10-21T10:00:00", "2026-10-21T10:01:40", owner, account, "10000K"))


def test_parent_covers_leased_children_without_omitting_any_frozen_index():
    commands = []
    def runner(command, **kwargs):
        commands.append(command)
        # A real sacct --array parent query returns the full roster, including
        # members which have no separate running/finished lease entry.
        return SimpleNamespace(returncode=0, stdout="\n".join(native_row(f"100_{i}", str(200 + i)) for i in range(4)))
    result = repair._repair_native_states(["100", "100_0", "100_2", "100_0"], {"100": {0, 1, 2, 3}}, runner=runner,
                                         owner="pwa209", account="def-ptewarie_cpu")
    assert commands[0][commands[0].index("-j") + 1] == "100"
    assert "--array" in commands[0]
    assert set(result) == {f"100_{i}" for i in range(4)}


def test_only_explicitly_requested_parents_cover_children_and_preserve_other_requests():
    commands = []
    def runner(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=0, stdout="\n".join((native_row("100_1", "201"),
            native_row("300_2", "302"), native_row("400", "400"))))
    result = repair._repair_native_states(["100_1", "300_2", "100", "400", "300_2"],
        {"100": {0, 1}, "300": {1, 2}}, runner=runner)
    assert commands[0][commands[0].index("-j") + 1] == "300_2,100,400"
    assert set(result) == {"100_1", "300_2", "400"}


@pytest.mark.parametrize("ids,rosters", [(["100", "100_9"], {"100": {0, 1}}),
    (["100_9", "100"], {"100": {0, 1}}), (["100", "100_0"], {}), (["100_0"], {})])
def test_unfrozen_child_is_rejected_before_overlap_pruning_or_query(ids, rosters):
    def forbidden(*args, **kwargs):
        raise AssertionError("Unfrozen member must never reach sacct")
    with pytest.raises(ValueError, match="outside the frozen"):
        repair._repair_native_states(ids, rosters, runner=forbidden)


@pytest.mark.parametrize("ids", [["100;other"], ["100.batch"], ["100_"], ["100_[0-3]"]])
def test_invalid_request_id_is_rejected_before_query(ids):
    with pytest.raises(ValueError, match="query identity"):
        repair._repair_native_states(ids, {"100": {0}}, runner=lambda *a, **k: pytest.fail("unexpected query"))


@pytest.mark.parametrize("state", ["RUNNING", "COMPLETED", "FAILED"])
def test_conflicting_returned_records_remain_errors_after_query_normalization(state):
    rows = "\n".join((native_row("100_0", "200", state=state),
                       native_row("100_0", "200", state=state, seconds="101")))
    with pytest.raises(ValueError, match="Conflicting repair native records"):
        repair._repair_native_states(["100", "100_0"], {"100": {0}},
            runner=lambda *a, **k: SimpleNamespace(returncode=0, stdout=rows))


@pytest.mark.parametrize("field,value", [("owner", "other"), ("account", "def-other_cpu")])
def test_returned_native_owner_and_account_are_still_verified(field, value):
    with pytest.raises(ValueError, match="another owner/account"):
        repair._repair_native_states(["100", "100_0"], {"100": {0}}, owner="pwa209", account="def-ptewarie_cpu",
            runner=lambda *a, **k: SimpleNamespace(returncode=0, stdout=native_row("100_0", "200", **{field: value})))


def test_returned_outside_roster_and_conflicting_physical_identity_are_not_hidden():
    with pytest.raises(ValueError, match="outside the frozen"):
        repair._repair_native_states(["100", "100_0"], {"100": {0}},
            runner=lambda *a, **k: SimpleNamespace(returncode=0, stdout=native_row("100_9", "209")))
    rows = "\n".join((native_row("100_0", "200"), native_row("100_1", "200")))
    with pytest.raises(ValueError, match="conflicting logical identities"):
        repair._repair_native_states(["100", "100_0"], {"100": {0, 1}},
            runner=lambda *a, **k: SimpleNamespace(returncode=0, stdout=rows))
