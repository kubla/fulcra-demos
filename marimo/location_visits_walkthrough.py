import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    from datetime import datetime, time, timedelta
    from pathlib import Path
    from zoneinfo import ZoneInfo

    import marimo as mo
    import pandas as pd
    from fulcra_api.core import FulcraAPI

    repo_root = Path(__file__).resolve().parent.parent
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)

    from scripts.location_visits_logic import (
        build_runs,
        build_segments,
        normalize_location_time_series,
        place_info,
        reindex_to_minute_grid,
    )

    return (
        FulcraAPI,
        ZoneInfo,
        build_runs,
        build_segments,
        datetime,
        mo,
        normalize_location_time_series,
        pd,
        reindex_to_minute_grid,
        time,
        timedelta,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Visit Reconstruction from a Shared, Tested Module

        This notebook is the presentation version of the workflow and uses the shared
        `scripts/location_visits_logic.py` module directly.

    That matters for two reasons:

    - the helper script and this notebook now use the same transformation code
    - the core visit-building logic is covered by executable tests outside the notebook UI

    The notebook still aims to be readable on a screen share:

    1. fetch one day of `location_time_series`
    2. explain the tuning knobs in plain language
    3. show the final visit table first
    4. keep the lower-level debugging tables available, but out of the way
    """)
    return


@app.cell
def _(FulcraAPI):
    fulcra = FulcraAPI()
    return (fulcra,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Authorize

    Run the next cell first. Fulcra will open a browser-based login flow.
    """)
    return


@app.cell
def _(fulcra):
    authorization_result = fulcra.authorize()
    return


@app.cell
def _(mo):
    analysis_date = mo.ui.date(
        value="2026-03-17",
        label="Day to analyze",
        full_width=True,
    )
    sample_rate_seconds = mo.ui.slider(
        steps=[60, 120, 300, 900],
        value=60,
        show_value=True,
        include_input=True,
        label="Sample rate (seconds)",
        full_width=True,
    )
    window_size_seconds = mo.ui.number(
        start=60,
        step=60,
        value=14400,
        label="Look-back window (seconds)",
        full_width=True,
    )
    min_visit_minutes = mo.ui.number(
        start=1,
        step=1,
        value=5,
        label="Minimum visit length (minutes)",
        full_width=True,
    )
    bridge_gap_minutes = mo.ui.number(
        start=0,
        step=1,
        value=3,
        label="Bridge gap (minutes)",
        full_width=True,
    )
    stationary_distance_threshold_m = mo.ui.number(
        start=0,
        step=10,
        value=150,
        label="Stationary distance threshold (meters)",
        full_width=True,
    )
    reverse_geocode = mo.ui.checkbox(
        value=True,
        label="Request reverse geocoding",
    )

    mo.vstack(
        [
            analysis_date,
            sample_rate_seconds,
            window_size_seconds,
            min_visit_minutes,
            bridge_gap_minutes,
            stationary_distance_threshold_m,
            reverse_geocode,
        ]
    )
    return (
        analysis_date,
        bridge_gap_minutes,
        min_visit_minutes,
        reverse_geocode,
        sample_rate_seconds,
        stationary_distance_threshold_m,
        window_size_seconds,
    )


@app.cell
def _(ZoneInfo, analysis_date, datetime, pd, time, timedelta):
    local_zone = ZoneInfo("America/New_York")
    selected_date = pd.Timestamp(analysis_date.value)
    start_time_dt = datetime.combine(selected_date.date(), time.min, tzinfo=local_zone)
    end_time_dt = start_time_dt + timedelta(days=1)
    return end_time_dt, local_zone, selected_date, start_time_dt


@app.cell
def _(
    bridge_gap_minutes,
    end_time_dt,
    min_visit_minutes,
    mo,
    reverse_geocode,
    sample_rate_seconds,
    selected_date,
    start_time_dt,
    stationary_distance_threshold_m,
    window_size_seconds,
):
    summary = mo.md(
        f"""
        ## Parameters

        `analysis_date`: `{selected_date.date()}`

        `start_time`: `{start_time_dt.isoformat()}`

        `end_time`: `{end_time_dt.isoformat()}`

        `sample_rate`: `{sample_rate_seconds.value}` seconds

        `look_back`: `{int(window_size_seconds.value)}` seconds

        `reverse_geocode`: `{reverse_geocode.value}`
        """
    )

    explainer = mo.accordion(
        {
            "Minimum visit length": mo.md(
                f"""
                A run must last at least **`{int(min_visit_minutes.value)}` minutes** before we call it a visit.

                Shorter runs are treated as movement. This keeps us from turning a brief curbside pause,
                a single geocode hiccup, or a moment in traffic into a real place visit.
                """
            ),
            "Bridge gap minutes": mo.md(
                f"""
                We currently bridge gaps of up to **`{int(bridge_gap_minutes.value)}` minutes**.

                In practical terms, this means that if the data says
                **home -> two minutes of jitter -> home**, we merge that back into one home visit.

                This helps when GPS drift or a briefly different reverse-geocode label would otherwise
                fragment one obvious stay into many small pieces.
                """
            ),
            "Stationary distance threshold": mo.md(
                f"""
                A candidate visit also has to look physically stationary.

                We currently require the run's median `distance_change_m` to stay at or below
                **`{int(stationary_distance_threshold_m.value)}` meters**.

                This is what prevents a moving car from becoming a fake visit just because one
                minute of road travel happened to reverse-geocode to a street address.
                """
            ),
            "Why this notebook is more trustworthy": mo.md(
                r"""
                The transformation logic is imported from the shared `scripts/location_visits_logic.py` module.

                That same module is also used by the helper script and covered by automated tests.
                So when we adjust the logic, we can validate it outside the notebook UI as well.
                """
            ),
        },
        multiple=True,
    )

    mo.vstack([summary, explainer])
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Fetch Fulcra Data

    This is the single API call in the notebook. Everything after this point is local processing.
    """)
    return


@app.cell
def _(
    bridge_gap_minutes,
    build_runs,
    build_segments,
    end_time_dt,
    fulcra,
    local_zone,
    min_visit_minutes,
    mo,
    normalize_location_time_series,
    pd,
    reindex_to_minute_grid,
    reverse_geocode,
    sample_rate_seconds,
    start_time_dt,
    stationary_distance_threshold_m,
    window_size_seconds,
):
    try:
        location_records = fulcra.location_time_series(
            start_time=start_time_dt.isoformat(),
            end_time=end_time_dt.isoformat(),
            sample_rate=int(sample_rate_seconds.value),
            look_back=int(window_size_seconds.value),
            reverse_geocode=reverse_geocode.value,
        )
        raw_df = pd.DataFrame(location_records)
        minute_df = reindex_to_minute_grid(
            normalize_location_time_series(location_records),
            start_time_dt,
            end_time_dt,
        )
        runs_df = build_runs(minute_df)
        visit_df = build_segments(
            minute_df,
            int(min_visit_minutes.value),
            int(bridge_gap_minutes.value),
            float(stationary_distance_threshold_m.value),
            local_zone,
        )
        status = mo.md(
            f"""
            Retrieved `{len(raw_df)}` raw rows, created `{len(runs_df)}` raw runs,
            and reduced them to `{len(visit_df)}` final visit/movement rows.
            """
        )
    except Exception as exc:
        raw_df = pd.DataFrame()
        minute_df = pd.DataFrame()
        runs_df = pd.DataFrame()
        visit_df = pd.DataFrame()
        status = mo.md(f"Fetch or processing failed.\n\n`{exc}`")
    return minute_df, raw_df, runs_df, status, visit_df


@app.cell
def _(status):
    status
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Final Visit Table

    This is the main artifact to discuss with colleagues. The table is intentionally simple:

    - a time range
    - a place name when we believe you were stationary
    - `Movement` when the data looks like travel or transient noise
    """)
    return


@app.cell
def _(visit_df):
    visit_df
    return


@app.cell
def _(minute_df, mo, pd, raw_df, runs_df, visit_df):
    if raw_df.empty:
        diagnostic_block = mo.md("No diagnostic summary available yet.")
    else:
        summary_df = (
            pd.DataFrame(
                [
                    {"metric": "Raw rows", "value": len(raw_df)},
                    {"metric": "Minute grid rows", "value": len(minute_df)},
                    {"metric": "Raw runs", "value": len(runs_df)},
                    {"metric": "Final segments", "value": len(visit_df)},
                    {
                        "metric": "Distinct minute-level labels",
                        "value": int(minute_df["place"].nunique()),
                    },
                ]
            )
        )
        diagnostic_block = mo.vstack(
            [
                mo.md(
                    """
                    ## What the Algorithm Did

                    The raw Fulcra feed is minute-level and noisy. The run table is the unsmoothed view.
                    The final visit table is the presentation view after applying the tested logic.
                    """
                ),
                summary_df,
            ]
        )

    diagnostic_block
    return


@app.cell
def _(minute_df, mo):
    if minute_df.empty:
        appendix = mo.md("No appendix data available yet.")
    else:
        appendix = mo.accordion(
            {
                "Minute-level preview": minute_df[
                    [
                        "minute_time",
                        "sample_time",
                        "place",
                        "place_key",
                        "is_place_candidate",
                        "distance_change_m",
                    ]
                ].head(20),
                "High-motion rows": minute_df[
                    minute_df["distance_change_m"].fillna(0) > 1000
                ][["minute_time", "place", "distance_change_m"]].head(15),
            },
            multiple=True,
        )

    appendix
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Run Table

    This appendix shows the unsmoothed contiguous runs before the final bridging and merging steps.
    It is useful when someone asks why two blocks were merged or why an address-like label was still
    treated as movement.
    """)
    return


@app.cell
def _(runs_df):
    runs_df[
        [
            "start_time",
            "end_time",
            "place",
            "place_key",
            "is_place_candidate",
            "minutes",
            "median_distance_change_m",
            "max_distance_change_m",
        ]
    ].head(40)
    return


@app.cell
def _(datetime, mo):
    generated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    mo.md(
        f"""
        ## Notes

        Generated locally with marimo on `{generated_at}`.

        This notebook is intended to replace the older ad hoc notebook path for this workflow.
        The important difference is not just formatting: it relies on the same shared module that
        now has executable tests.
        """
    )
    return


if __name__ == "__main__":
    app.run()
