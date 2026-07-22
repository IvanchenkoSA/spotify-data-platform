"""Fetch a raw snapshot of the current user's recently played tracks."""

import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from psycopg import Error as PsycopgError

from .auth import CLIENT_ID, CLIENT_SECRET, TOKEN_FILE, TOKEN_URL, save_tokens
from .database import connect, get_watermark, initialize_database, load_recently_played


RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"
RAW_DATA_DIR = Path("data/raw")


def load_tokens() -> dict:
    """Load the tokens created by ``python -m ingestion.src.auth``."""
    if not TOKEN_FILE.exists():
        raise FileNotFoundError(
            f"{TOKEN_FILE} was not found. Run `python -m ingestion.src.auth` first."
        )

    return json.loads(TOKEN_FILE.read_text(encoding="utf-8"))


def refresh_access_token(tokens: dict) -> dict:
    """Refresh an expired access token and preserve the existing refresh token."""
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise RuntimeError("No refresh token is available. Authorize again with `python -m ingestion.src.auth`.")

    response = requests.post(
        TOKEN_URL,
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=30,
    )
    response.raise_for_status()

    refreshed_tokens = response.json()
    refreshed_tokens["refresh_token"] = refreshed_tokens.get("refresh_token", refresh_token)
    save_tokens(refreshed_tokens)
    return refreshed_tokens


def get_recently_played(access_token: str, after: datetime | None) -> requests.Response:
    params = {"limit": 50}
    if after:
        # Include the last millisecond again: the database UPSERT removes the duplicate,
        # while a second event in that millisecond cannot be missed.
        params["after"] = max(0, int(after.timestamp() * 1000) - 1)

    return requests.get(
        RECENTLY_PLAYED_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        params=params,
        timeout=30,
    )


def save_raw_response(payload: dict) -> Path:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output_file = RAW_DATA_DIR / f"recently_played_{timestamp}.json"
    output_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_file


def main() -> None:
    try:
        with connect() as connection:
            initialize_database(connection)
            watermark = get_watermark(connection)
            tokens = load_tokens()
            response = get_recently_played(tokens["access_token"], watermark)

            if response.status_code == 401:
                tokens = refresh_access_token(tokens)
                response = get_recently_played(tokens["access_token"], watermark)

            response.raise_for_status()
            payload = response.json()
            output_file = save_raw_response(payload)
            loaded_count = load_recently_played(connection, payload)

    except (FileNotFoundError, requests.RequestException, PsycopgError, RuntimeError) as error:
        raise SystemExit(f"Ingestion failed: {error}") from error

    print(f"Saved raw Spotify response to {output_file}")
    print(f"Inserted {loaded_count} new event(s) into PostgreSQL.")


if __name__ == "__main__":
    main()
