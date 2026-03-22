from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from scripts.location_visits_logic import (
    build_runs,
    build_segments,
    normalize_location_time_series,
    place_info,
    reindex_to_minute_grid,
)


def test_place_info_treats_missing_coordinates_as_unknown() -> None:
    info = place_info({"address": None, "lat": float("nan"), "long": float("nan")})
    assert info["place_label"] == "Unknown"
    assert info["place_key"] == "Unknown"
    assert info["is_place_candidate"] is False


def test_place_info_does_not_treat_highway_reverse_geocode_as_precise_place() -> None:
    sample = {
        "lat": 43.067151,
        "long": -70.791296,
        "address": "Spaulding Turnpike, Portsmouth, NH 03801, United States of America",
        "location_details": {
            "components": {
                "road": "Spaulding Turnpike",
                "city": "Portsmouth",
                "state_code": "NH",
                "postcode": "03801",
                "_category": "road",
            }
        },
    }
    info = place_info(sample)
    assert info["place_label"] == "43.0672, -70.7913"
    assert info["place_key"] == "Spaulding Turnpike|Portsmouth|NH|03801"
    assert info["is_place_candidate"] is False


def test_bridge_gap_merges_short_jitter_back_into_one_visit() -> None:
    zone = ZoneInfo("America/New_York")
    minutes = pd.date_range("2026-03-17T12:00:00+00:00", periods=7, freq="1min")
    df = pd.DataFrame(
        {
            "minute_time": minutes,
            "sample_time": minutes,
            "place": [
                "97 Fourth Street, Dover, NH 03820, United States of America",
                "97 Fourth Street, Dover, NH 03820, United States of America",
                "Movement-like blip",
                "97 Fourth Street, Dover, NH 03820, United States of America",
                "97 Fourth Street, Dover, NH 03820, United States of America",
                "97 Fourth Street, Dover, NH 03820, United States of America",
                "97 Fourth Street, Dover, NH 03820, United States of America",
            ],
            "place_key": [
                "Fourth Street|Dover|NH|03820",
                "Fourth Street|Dover|NH|03820",
                "Unknown",
                "Fourth Street|Dover|NH|03820",
                "Fourth Street|Dover|NH|03820",
                "Fourth Street|Dover|NH|03820",
                "Fourth Street|Dover|NH|03820",
            ],
            "is_place_candidate": [True, True, False, True, True, True, True],
            "distance_change_m": [10, 8, 300, 12, 9, 7, 8],
            "sample_found": [True] * 7,
        }
    )

    segments = build_segments(
        df,
        min_visit_minutes=2,
        bridge_gap_minutes=1,
        stationary_distance_threshold_m=150,
        local_zone=zone,
    )

    assert len(segments) == 1
    assert segments.iloc[0]["segment_type"] == "Visit"
    assert segments.iloc[0]["minutes"] == 7


def test_high_motion_rows_do_not_become_visits_even_with_addresses() -> None:
    records = [
        {
            "slice_time": "2026-03-17T15:20:00+00:00",
            "time": "2026-03-17T15:20:00+00:00",
            "lat": 43.095770,
            "long": -70.808222,
            "distance_change_m": 1841.165233,
            "address": "1107 Spaulding Turnpike, Newington, Rockingham County, NH 03801, United States of America",
            "location_details": {
                "components": {
                    "house_number": "1107",
                    "road": "Spaulding Turnpike",
                    "city": "Newington",
                    "state_code": "NH",
                    "postcode": "03801",
                    "_category": "building",
                }
            },
        },
        {
            "slice_time": "2026-03-17T15:21:00+00:00",
            "time": "2026-03-17T15:21:00+00:00",
            "lat": 43.083400,
            "long": -70.793931,
            "distance_change_m": 1800.715700,
            "address": "441 Spaulding Turnpike, Portsmouth, NH 03801, United States of America",
            "location_details": {
                "components": {
                    "house_number": "441",
                    "road": "Spaulding Turnpike",
                    "city": "Portsmouth",
                    "state_code": "NH",
                    "postcode": "03801",
                    "_category": "building",
                }
            },
        },
    ]
    start = datetime.fromisoformat("2026-03-17T11:20:00-04:00")
    end = datetime.fromisoformat("2026-03-17T11:22:00-04:00")

    minute_df = reindex_to_minute_grid(normalize_location_time_series(records), start, end)
    segments = build_segments(
        minute_df,
        min_visit_minutes=2,
        bridge_gap_minutes=1,
        stationary_distance_threshold_m=150,
        local_zone=ZoneInfo("America/New_York"),
    )

    assert len(segments) == 1
    assert segments.iloc[0]["segment_type"] == "Movement"


def test_build_runs_uses_place_key_not_display_label() -> None:
    df = pd.DataFrame(
        {
            "minute_time": pd.date_range("2026-03-17T12:00:00+00:00", periods=3, freq="1min"),
            "sample_time": pd.date_range("2026-03-17T12:00:00+00:00", periods=3, freq="1min"),
            "place": [
                "One Broadway, 1 Broadway, Cambridge, MA 02142, United States of America",
                "Eastern Bank, 1 Broadway, Cambridge, MA 02142, United States of America",
                "One Broadway, 1 Broadway, Cambridge, MA 02142, United States of America",
            ],
            "place_key": ["Broadway|Cambridge|MA|02142"] * 3,
            "is_place_candidate": [True, True, True],
            "distance_change_m": [5, 3, 4],
            "sample_found": [True, True, True],
        }
    )

    runs = build_runs(df)
    assert len(runs) == 1
    assert runs.iloc[0]["minutes"] == 3


def test_null_only_rows_are_removed_from_final_segments() -> None:
    df = pd.DataFrame(
        {
            "minute_time": pd.date_range("2026-03-17T04:00:00+00:00", periods=3, freq="1min"),
            "sample_time": [pd.NaT, pd.NaT, pd.NaT],
            "place": ["Unknown", "Unknown", "Unknown"],
            "place_key": ["Unknown", "Unknown", "Unknown"],
            "is_place_candidate": [False, False, False],
            "distance_change_m": [float("nan"), float("nan"), float("nan")],
            "sample_found": [False, False, False],
        }
    )

    segments = build_segments(
        df,
        min_visit_minutes=2,
        bridge_gap_minutes=1,
        stationary_distance_threshold_m=150,
        local_zone=ZoneInfo("America/New_York"),
    )

    assert segments.empty


def test_empty_time_series_slices_do_not_count_as_found_samples() -> None:
    records = [
        {"slice_time": "2026-03-17T04:00:00+00:00"},
        {"slice_time": "2026-03-17T04:01:00+00:00"},
        {
            "slice_time": "2026-03-17T04:02:00+00:00",
            "time": "2026-03-17T04:01:30+00:00",
            "lat": 43.2,
            "long": -70.886,
            "distance_change_m": 0.0,
            "address": "97 Fourth Street, Dover, NH 03820, United States of America",
            "location_details": {
                "components": {
                    "house_number": "97",
                    "road": "Fourth Street",
                    "city": "Dover",
                    "state_code": "NH",
                    "postcode": "03820",
                    "_category": "building",
                }
            },
        },
    ]
    start = datetime.fromisoformat("2026-03-17T00:00:00-04:00")
    end = datetime.fromisoformat("2026-03-17T00:03:00-04:00")

    minute_df = reindex_to_minute_grid(normalize_location_time_series(records), start, end)
    assert minute_df["sample_found"].tolist() == [False, False, True]

    segments = build_segments(
        minute_df,
        min_visit_minutes=1,
        bridge_gap_minutes=1,
        stationary_distance_threshold_m=150,
        local_zone=ZoneInfo("America/New_York"),
    )

    assert len(segments) == 1
    assert segments.iloc[0]["segment_type"] == "Visit"
