from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd


def is_missing(value: Any) -> bool:
    return value is None or bool(pd.isna(value))


def as_mapping(sample: Any) -> dict[str, Any]:
    if isinstance(sample, dict):
        return sample
    if hasattr(sample, "to_dict"):
        return sample.to_dict()
    return {}


def first_text(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def place_info(sample: Any) -> dict[str, Any]:
    sample_map = as_mapping(sample)
    location_details = sample_map.get("location_details")
    if not isinstance(location_details, dict):
        location_details = {}

    components = location_details.get("components")
    if not isinstance(components, dict):
        components = {}

    house_number = first_text(components.get("house_number"))
    road = first_text(components.get("road"))
    city = first_text(
        components.get("_normalized_city"),
        components.get("city"),
        components.get("town"),
        components.get("village"),
        components.get("hamlet"),
    )
    state = first_text(components.get("state_code"), components.get("state"))
    postcode = first_text(components.get("postcode"))
    category = first_text(components.get("_category"), components.get("_type"))

    formatted = first_text(
        sample_map.get("address"),
        sample_map.get("display_name"),
        location_details.get("formatted"),
        location_details.get("display_name"),
        location_details.get("name"),
    )

    lat = sample_map.get("lat", sample_map.get("latitude_degrees"))
    lon = sample_map.get("long", sample_map.get("longitude_degrees"))
    has_coords = not is_missing(lat) and not is_missing(lon)
    coord_label = None
    if has_coords:
        coord_label = f"{round(float(lat), 4)}, {round(float(lon), 4)}"

    place_candidate_categories = {
        "building",
        "amenity",
        "commercial",
        "education",
        "office",
        "shop",
        "tourism",
        "university",
    }
    is_precise_place = bool(house_number) or (category in place_candidate_categories)

    if road and city and state and postcode:
        place_key = f"{road}|{city}|{state}|{postcode}"
    elif road and city and state:
        place_key = f"{road}|{city}|{state}"
    elif formatted and is_precise_place:
        place_key = formatted
    elif coord_label:
        place_key = f"coords:{round(float(lat), 3)},{round(float(lon), 3)}"
    else:
        place_key = "Unknown"

    if is_precise_place and formatted:
        place_label = formatted
    elif coord_label:
        place_label = coord_label
    else:
        place_label = "Unknown"

    return {
        "place_label": place_label,
        "place_key": place_key,
        "is_place_candidate": is_precise_place,
    }


def normalize_location_time_series(records: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "minute_time",
                "sample_time",
                "place",
                "place_key",
                "is_place_candidate",
                "distance_change_m",
                "sample_found",
            ]
        )

    place_details = df.apply(place_info, axis=1, result_type="expand")

    minute_time = pd.to_datetime(
        df.get("slice_time"),
        utc=True,
        format="ISO8601",
        errors="coerce",
    )
    sample_time = pd.to_datetime(
        df.get("time"),
        utc=True,
        format="ISO8601",
        errors="coerce",
    )
    distance_change_m = pd.to_numeric(df.get("distance_change_m"), errors="coerce")
    has_coordinates = pd.to_numeric(df.get("lat"), errors="coerce").notna() & pd.to_numeric(
        df.get("long"), errors="coerce"
    ).notna()
    has_place_text = (
        place_details["place_label"].fillna("Unknown") != "Unknown"
    ) | (
        place_details["place_key"].fillna("Unknown") != "Unknown"
    )
    sample_found = sample_time.notna() | has_coordinates | has_place_text | distance_change_m.notna()

    return pd.DataFrame(
        {
            "minute_time": minute_time,
            "sample_time": sample_time,
            "place": place_details["place_label"],
            "place_key": place_details["place_key"],
            "is_place_candidate": place_details["is_place_candidate"],
            "distance_change_m": distance_change_m,
            "sample_found": sample_found,
        }
    )


def reindex_to_minute_grid(df: pd.DataFrame, start_time_dt: datetime, end_time_dt: datetime) -> pd.DataFrame:
    start_time_utc = pd.Timestamp(start_time_dt).tz_convert("UTC")
    end_time_utc = pd.Timestamp(end_time_dt).tz_convert("UTC")
    minute_grid = pd.date_range(start=start_time_utc, end=end_time_utc, freq="1min", inclusive="left")
    grid = pd.DataFrame({"minute_time": minute_grid})
    normalized = grid.merge(df, on="minute_time", how="left")
    normalized["place"] = normalized["place"].fillna("Unknown")
    normalized["place_key"] = normalized["place_key"].fillna("Unknown")
    normalized["is_place_candidate"] = normalized["is_place_candidate"].fillna(False)
    normalized["distance_change_m"] = pd.to_numeric(normalized["distance_change_m"], errors="coerce")
    normalized["sample_found"] = normalized["sample_found"].fillna(False)
    return normalized


def build_runs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "start_time",
                "end_time",
                "place",
                "place_key",
                "is_place_candidate",
                "minutes",
                "sample_found_minutes",
                "median_distance_change_m",
                "max_distance_change_m",
            ]
        )

    ordered = df.sort_values("minute_time").copy()
    ordered["place_change"] = ordered["place_key"].ne(
        ordered["place_key"].shift(fill_value=ordered["place_key"].iloc[0])
    )
    ordered["run_id"] = ordered["place_change"].cumsum()

    runs = (
        ordered.groupby("run_id", as_index=False)
        .agg(
            start_time=("minute_time", "min"),
            last_minute=("minute_time", "max"),
            place=("place", lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0]),
            place_key=("place_key", "first"),
            is_place_candidate=("is_place_candidate", "max"),
            minutes=("minute_time", "size"),
            sample_found_minutes=("sample_found", "sum"),
            median_distance_change_m=("distance_change_m", "median"),
            max_distance_change_m=("distance_change_m", "max"),
        )
    )
    runs["end_time"] = runs["last_minute"] + pd.Timedelta(minutes=1)
    return runs.drop(columns=["last_minute"])


def build_segments(
    df: pd.DataFrame,
    min_visit_minutes: int,
    bridge_gap_minutes: int,
    stationary_distance_threshold_m: float,
    local_zone: Any,
) -> pd.DataFrame:
    runs = build_runs(df)
    if runs.empty:
        return pd.DataFrame(columns=["time_range", "place", "segment_type", "minutes"])

    runs["segment_type"] = runs.apply(
        lambda row: "Visit"
        if row["place_key"] != "Unknown"
        and bool(row["is_place_candidate"])
        and row["minutes"] >= min_visit_minutes
        and (
            pd.isna(row["median_distance_change_m"])
            or float(row["median_distance_change_m"]) <= stationary_distance_threshold_m
        )
        else "Movement",
        axis=1,
    )
    runs["segment_name"] = runs["place"].where(runs["segment_type"] == "Visit", "Movement")

    rows = runs.to_dict("records")
    bridged_rows: list[dict[str, Any]] = []
    index = 0
    while index < len(rows):
        current = rows[index].copy()
        while (
            current["segment_type"] == "Visit"
            and index + 2 < len(rows)
            and rows[index + 1]["segment_type"] == "Movement"
            and rows[index + 1]["minutes"] <= bridge_gap_minutes
            and rows[index + 2]["segment_type"] == "Visit"
            and rows[index + 2]["place_key"] == current["place_key"]
        ):
            current["end_time"] = rows[index + 2]["end_time"]
            current["minutes"] += rows[index + 1]["minutes"] + rows[index + 2]["minutes"]
            current["sample_found_minutes"] += (
                rows[index + 1]["sample_found_minutes"] + rows[index + 2]["sample_found_minutes"]
            )
            index += 2
        bridged_rows.append(current)
        index += 1

    merged_rows: list[dict[str, Any]] = []
    for current in bridged_rows:
        if merged_rows:
            previous = merged_rows[-1]
            same_visit = (
                current["segment_type"] == "Visit"
                and previous["segment_type"] == "Visit"
                and current["place_key"] == previous["place_key"]
            )
            same_movement = (
                current["segment_type"] == "Movement"
                and previous["segment_type"] == "Movement"
            )
            if same_visit or same_movement:
                previous["end_time"] = current["end_time"]
                previous["minutes"] += current["minutes"]
                previous["sample_found_minutes"] += current["sample_found_minutes"]
                continue
        merged_rows.append(current)

    segments = pd.DataFrame(merged_rows).sort_values("start_time").reset_index(drop=True)
    segments = segments[segments["sample_found_minutes"] > 0].copy()
    if segments.empty:
        return pd.DataFrame(columns=["time_range", "place", "segment_type", "minutes"])
    segments["start_local"] = segments["start_time"].dt.tz_convert(local_zone)
    segments["end_local"] = segments["end_time"].dt.tz_convert(local_zone)
    segments["time_range"] = (
        segments["start_local"].dt.strftime("%Y-%m-%d %I:%M %p")
        + " - "
        + segments["end_local"].dt.strftime("%Y-%m-%d %I:%M %p")
    )
    segments["place"] = segments["segment_name"]
    return segments[["time_range", "place", "segment_type", "minutes"]]
