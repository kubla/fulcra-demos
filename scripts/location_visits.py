#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from fulcra_api.core import FulcraAPI

REPO_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT_STR = str(REPO_ROOT)
if REPO_ROOT_STR not in sys.path:
    sys.path.insert(0, REPO_ROOT_STR)

from fulcra_demos.location_visits import (
    build_runs,
    build_segments,
    normalize_location_time_series,
    reindex_to_minute_grid,
)


STATE_DIR = Path.home() / ".fulcra-demos"
TOKEN_CACHE_PATH = STATE_DIR / "auth.json"
DEVICE_STATE_PATH = STATE_DIR / "device_auth.json"
DEFAULT_OUTPUT_DIR = Path("/tmp/fulcra-demos-location-visits")


@dataclass
class TokenCache:
    access_token: str
    access_token_expiration: str
    refresh_token: str | None = None


def ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def load_token_cache(path: Path) -> TokenCache | None:
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return TokenCache(
        access_token=data["access_token"],
        access_token_expiration=data["access_token_expiration"],
        refresh_token=data.get("refresh_token"),
    )


def save_token_cache(path: Path, client: FulcraAPI) -> None:
    ensure_state_dir()
    payload = {
        "access_token": client.get_cached_access_token(),
        "access_token_expiration": client.get_cached_access_token_expiration().isoformat(),
        "refresh_token": client.get_cached_refresh_token(),
    }
    path.write_text(json.dumps(payload, indent=2))


def load_client(token_cache_path: Path) -> FulcraAPI:
    cached = load_token_cache(token_cache_path)
    if cached is None:
        return FulcraAPI()

    expiration = datetime.fromisoformat(cached.access_token_expiration)
    return FulcraAPI(
        access_token=cached.access_token,
        access_token_expiration=expiration,
        refresh_token=cached.refresh_token,
    )


def has_valid_token(client: FulcraAPI) -> bool:
    expiration = client.get_cached_access_token_expiration()
    token = client.get_cached_access_token()
    return bool(token and expiration and expiration > datetime.now())


def start_device_auth(args: argparse.Namespace) -> int:
    ensure_state_dir()
    client = load_client(args.token_cache)
    if has_valid_token(client):
        print("A valid cached Fulcra token already exists.")
        return 0

    device_code, uri, user_code = client._request_device_code(  # noqa: SLF001
        client.oidc_domain,
        client.oidc_client_id,
        client.oidc_scope,
        client.oidc_audience,
    )
    state = {
        "device_code": device_code,
        "verification_uri_complete": uri,
        "user_code": user_code,
        "created_at": datetime.now().isoformat(),
    }
    args.device_state.write_text(json.dumps(state, indent=2))

    print("Visit this URL in your browser to authorize Fulcra:")
    print(uri)
    print()
    print(f"User code: {user_code}")
    print(f"Saved device auth state to: {args.device_state}")
    return 0


def poll_for_auth(args: argparse.Namespace) -> int:
    client = load_client(args.token_cache)
    if has_valid_token(client):
        print("A valid cached Fulcra token already exists.")
        return 0

    if not args.device_state.exists():
        print(
            f"No device auth state found at {args.device_state}. Run `start-auth` first.",
            file=sys.stderr,
        )
        return 1

    state = json.loads(args.device_state.read_text())

    device_code = state["device_code"]
    stop_at = time.time() + args.timeout_seconds

    while time.time() < stop_at:
        token, expiration_date = client.get_token(device_code)
        if token is not None and expiration_date is not None:
            client.set_cached_access_token(token)
            client.set_cached_access_token_expiration(expiration_date)
            client.set_cached_refresh_token(None)
            save_token_cache(args.token_cache, client)
            args.device_state.unlink(missing_ok=True)
            print("Authorization succeeded.")
            print(f"Saved token cache to: {args.token_cache}")
            return 0
        time.sleep(args.poll_interval_seconds)

    print(
        "Authorization did not complete before the timeout. "
        "If the browser flow is still pending, run `poll-auth` again.",
        file=sys.stderr,
    )
    return 2


def parse_datetime_arg(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Datetime arguments must include an explicit UTC offset.")
    return parsed


def visits_table(args: argparse.Namespace) -> int:
    client = load_client(args.token_cache)
    if not has_valid_token(client):
        print(
            "No valid cached Fulcra token found. Run `start-auth`, open the URL, then run `poll-auth`.",
            file=sys.stderr,
        )
        return 1

    start_time_dt = parse_datetime_arg(args.start_time)
    end_time_dt = parse_datetime_arg(args.end_time)
    if end_time_dt <= start_time_dt:
        print("`end-time` must be after `start-time`.", file=sys.stderr)
        return 1

    records = client.location_time_series(
        start_time=start_time_dt.isoformat(),
        end_time=end_time_dt.isoformat(),
        sample_rate=args.sample_rate,
        look_back=args.window_size,
        change_meters=args.change_meters,
        reverse_geocode=args.reverse_geocode,
    )

    raw_df = pd.DataFrame(records)
    minute_df = reindex_to_minute_grid(
        normalize_location_time_series(records),
        start_time_dt,
        end_time_dt,
    )
    runs_df = build_runs(minute_df)
    segments_df = build_segments(
        minute_df,
        min_visit_minutes=args.min_visit_minutes,
        bridge_gap_minutes=args.bridge_gap_minutes,
        stationary_distance_threshold_m=args.stationary_distance_threshold_m,
        local_zone=start_time_dt.tzinfo,
    )

    if args.output_dir is not None:
        output_dir = args.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        raw_df.to_csv(output_dir / "location_time_series_raw.csv", index=False)
        minute_df.to_csv(output_dir / "location_time_series_minutes.csv", index=False)
        runs_df.to_csv(output_dir / "location_time_series_runs.csv", index=False)
        segments_df.to_csv(output_dir / "location_time_series_segments.csv", index=False)
        summary = {
            "start_time": start_time_dt.isoformat(),
            "end_time": end_time_dt.isoformat(),
            "sample_rate": args.sample_rate,
            "window_size": args.window_size,
            "change_meters": args.change_meters,
            "reverse_geocode": args.reverse_geocode,
            "min_visit_minutes": args.min_visit_minutes,
            "bridge_gap_minutes": args.bridge_gap_minutes,
            "stationary_distance_threshold_m": args.stationary_distance_threshold_m,
            "raw_rows": len(raw_df),
            "minute_rows": len(minute_df),
            "run_count": len(runs_df),
            "segment_count": len(segments_df),
        }
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    if segments_df.empty:
        print("No visit segments found.")
        return 0

    print(segments_df.to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fulcra location visits CLI.")
    parser.set_defaults(func=None)
    parser.add_argument("--token-cache", type=Path, default=TOKEN_CACHE_PATH)
    parser.add_argument("--device-state", type=Path, default=DEVICE_STATE_PATH)

    subparsers = parser.add_subparsers(dest="command")

    start_auth = subparsers.add_parser("start-auth", help="Request a device authorization URL and save pending auth state.")
    start_auth.set_defaults(func=start_device_auth)

    poll_auth = subparsers.add_parser("poll-auth", help="Poll for completion of a previously started device authorization.")
    poll_auth.add_argument("--timeout-seconds", type=int, default=300)
    poll_auth.add_argument("--poll-interval-seconds", type=float, default=1.0)
    poll_auth.set_defaults(func=poll_for_auth)

    visits = subparsers.add_parser("visits-table", help="Print a tidy visit table from location_time_series.")
    visits.add_argument("--start-time", required=True)
    visits.add_argument("--end-time", required=True)
    visits.add_argument("--sample-rate", type=int, default=60)
    visits.add_argument("--window-size", type=int, default=14400)
    visits.add_argument("--change-meters", type=float, default=None)
    visits.add_argument("--reverse-geocode", action="store_true", default=True)
    visits.add_argument("--no-reverse-geocode", dest="reverse_geocode", action="store_false")
    visits.add_argument("--min-visit-minutes", type=int, default=5)
    visits.add_argument("--bridge-gap-minutes", type=int, default=3)
    visits.add_argument("--stationary-distance-threshold-m", type=float, default=150.0)
    visits.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    visits.set_defaults(func=visits_table)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.func is None:
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
