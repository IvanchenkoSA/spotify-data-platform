"""Backfill and incrementally ingest completed Last.fm scrobbles."""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from psycopg import Error as PsycopgError

from .database import (
    LASTFM_WATERMARK_NAME,
    connect,
    get_watermark,
    initialize_database,
    load_lastfm_scrobbles,
    set_watermark,
    finish_pipeline_run,
    start_pipeline_run,
)


LASTFM_URL = "https://ws.audioscrobbler.com/2.0/"
RAW_DATA_DIR = Path("data/raw")
PAGE_SIZE = 200


def fetch_page(username: str, api_key: str, page: int, after: datetime | None) -> dict:
    """Request one page; the overlap protects same-second Last.fm events."""
    params: dict[str, str | int] = {
        "method": "user.getrecenttracks",
        "user": username,
        "api_key": api_key,
        "format": "json",
        "limit": PAGE_SIZE,
        "page": page,
    }
    if after:
        params["from"] = max(0, int(after.timestamp()) - 1)

    response = requests.get(LASTFM_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(
            f"Last.fm API error {payload['error']}: {payload.get('message', 'Unknown error')}"
        )
    return payload


def save_raw_response(payload: dict, page: int) -> Path:
    """Persist each API page before it is loaded into PostgreSQL."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output_file = RAW_DATA_DIR / f"lastfm_recent_tracks_{timestamp}_page_{page}.json"
    output_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_file


def tracks_from_payload(payload: dict) -> list[dict]:
    recent_tracks = payload.get("recenttracks", {})
    tracks = recent_tracks.get("track", [])
    return tracks if isinstance(tracks, list) else [tracks]


def total_pages(payload: dict) -> int:
    attributes = payload.get("recenttracks", {}).get("@attr", {})
    return max(1, int(attributes.get("totalPages", 1)))


def newest_completed_scrobble(items: list[dict]) -> datetime | None:
    """Return the newest timestamp from completed scrobbles in a response page."""
    timestamps = [
        int(item["date"]["uts"])
        for item in items
        if isinstance(item.get("date"), dict) and item["date"].get("uts")
    ]
    return datetime.fromtimestamp(max(timestamps), tz=timezone.utc) if timestamps else None


def main() -> None:
    load_dotenv()
    api_key = os.getenv("LASTFM_API_KEY")
    username = os.getenv("LASTFM_USERNAME")
    if not api_key or not username:
        raise SystemExit(
            "LASTFM_API_KEY and LASTFM_USERNAME must be configured in .env before ingestion."
        )

    run_id: int | None = None
    try:
        with connect() as connection:
            initialize_database(connection)
            run_id = start_pipeline_run(connection, "lastfm_scrobbles")
            watermark = get_watermark(connection, LASTFM_WATERMARK_NAME)
            page = 1
            inserted_count = 0
            extracted_count = 0
            saved_pages = 0
            newest_scrobble: datetime | None = None

            while True:
                payload = fetch_page(username, api_key, page, watermark)
                output_file = save_raw_response(payload, page)
                items = tracks_from_payload(payload)
                extracted_count += len(items)
                inserted_count += load_lastfm_scrobbles(connection, username, items)
                page_newest = newest_completed_scrobble(items)
                if page_newest and (newest_scrobble is None or page_newest > newest_scrobble):
                    newest_scrobble = page_newest
                saved_pages += 1
                pages = total_pages(payload)
                print(f"Loaded page {page}/{pages}: {output_file}", flush=True)
                if page >= pages:
                    break
                page += 1
                time.sleep(0.2)

            # Only mark the batch complete after every page succeeds. If a backfill fails,
            # its idempotent raw rows are safely reread on the next run instead of skipped.
            if newest_scrobble:
                set_watermark(connection, LASTFM_WATERMARK_NAME, newest_scrobble)
                connection.commit()

            finish_pipeline_run(
                connection,
                run_id,
                "succeeded",
                extracted_count=extracted_count,
                inserted_count=inserted_count,
            )

    except (OSError, requests.RequestException, PsycopgError, RuntimeError, ValueError) as error:
        if run_id is not None:
            try:
                with connect() as connection:
                    finish_pipeline_run(connection, run_id, "failed", error_message=str(error))
            except PsycopgError:
                pass
        raise SystemExit(f"Last.fm ingestion failed: {error}") from error

    print(f"Saved {saved_pages} raw Last.fm page(s).")
    print(f"Inserted {inserted_count} new Last.fm scrobble(s) into PostgreSQL.")


if __name__ == "__main__":
    main()
