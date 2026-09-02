"""Type 2 history: the merge must version changes and ignore repeats."""
from datetime import date

import pytest

SNAP_A = """LocationID,Borough,Zone,service_zone
1,EWR,Newark Airport,EWR
2,Queens,Jamaica Bay,Boro Zone
3,Bronx,Allerton,Boro Zone
"""

# Zone 2 reclassified, zone 3 unchanged, zone 4 is new, zone 1 disappeared.
SNAP_B = """LocationID,Borough,Zone,service_zone
2,Queens,Jamaica Bay,Yellow Zone
3,Bronx,Allerton,Boro Zone
4,Brooklyn,Bushwick North,Boro Zone
"""


@pytest.fixture
def dim(sandbox):
    from tlc import scd2
    return scd2, sandbox


def _write(tmp, name, body):
    p = tmp / name
    p.write_text(body)
    return p


def test_first_load_opens_a_version_per_zone(dim):
    scd2, tmp = dim
    stats = scd2.merge(_write(tmp, "a.csv", SNAP_A), date(2024, 1, 1))
    assert (stats["new_zones"], stats["changed_zones"], stats["retired_zones"]) == (3, 0, 0)
    assert stats["rows_current"] == 3
    scd2.validate()


def test_reapplying_the_same_snapshot_is_a_noop(dim):
    """Idempotency: a repeated run must not create a second version."""
    scd2, tmp = dim
    src = _write(tmp, "a.csv", SNAP_A)
    scd2.merge(src, date(2024, 1, 1))
    stats = scd2.merge(src, date(2024, 1, 1))
    assert (stats["new_zones"], stats["changed_zones"]) == (0, 0)
    assert stats["rows_total"] == 3
    scd2.validate()


def test_changed_attribute_creates_a_second_version(dim):
    scd2, tmp = dim
    scd2.merge(_write(tmp, "a.csv", SNAP_A), date(2024, 1, 1))
    stats = scd2.merge(_write(tmp, "b.csv", SNAP_B), date(2024, 6, 1))

    assert stats["changed_zones"] == 1     # zone 2 reclassified
    assert stats["new_zones"] == 1         # zone 4 appeared
    assert stats["retired_zones"] == 1     # zone 1 gone
    scd2.validate()

    with scd2.warehouse(read_only=True) as con:
        rows = con.execute(
            """SELECT service_zone, valid_from, valid_to, is_current
               FROM dim_zone WHERE location_id = 2 ORDER BY valid_from"""
        ).fetchall()
    assert len(rows) == 2
    old, new = rows
    assert old[0] == "Boro Zone" and old[3] is False
    assert new[0] == "Yellow Zone" and new[3] is True
    # Ranges abut without overlapping: old closes the day before the new opens.
    assert old[2] == date(2024, 5, 31)
    assert new[1] == date(2024, 6, 1)


def test_history_answers_point_in_time_questions(dim):
    """The whole reason for Type 2: what did this zone look like back then."""
    scd2, tmp = dim
    scd2.merge(_write(tmp, "a.csv", SNAP_A), date(2024, 1, 1))
    scd2.merge(_write(tmp, "b.csv", SNAP_B), date(2024, 6, 1))
    with scd2.warehouse(read_only=True) as con:
        as_of_march = con.execute(
            "SELECT service_zone FROM dim_zone WHERE location_id = 2 AND ? BETWEEN valid_from AND valid_to",
            [date(2024, 3, 15)],
        ).fetchone()[0]
        as_of_today = con.execute(
            "SELECT service_zone FROM dim_zone WHERE location_id = 2 AND is_current"
        ).fetchone()[0]
    assert as_of_march == "Boro Zone"
    assert as_of_today == "Yellow Zone"


def test_retired_zone_keeps_its_history(dim):
    scd2, tmp = dim
    scd2.merge(_write(tmp, "a.csv", SNAP_A), date(2024, 1, 1))
    scd2.merge(_write(tmp, "b.csv", SNAP_B), date(2024, 6, 1))
    with scd2.warehouse(read_only=True) as con:
        row = con.execute(
            "SELECT zone, valid_to, is_current FROM dim_zone WHERE location_id = 1"
        ).fetchone()
    assert row[0] == "Newark Airport"      # still queryable
    assert row[2] is False                 # but no longer current
    assert row[1] == date(2024, 5, 31)


def test_out_of_order_snapshot_is_refused(dim):
    """Applying an older snapshot would create overlapping validity ranges."""
    scd2, tmp = dim
    scd2.merge(_write(tmp, "b.csv", SNAP_B), date(2024, 6, 1))
    with pytest.raises(ValueError, match="predates current dimension state"):
        scd2.merge(_write(tmp, "a.csv", SNAP_A), date(2024, 1, 1))
