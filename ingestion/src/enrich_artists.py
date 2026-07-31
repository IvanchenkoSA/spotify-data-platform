"""Enrich listening-history artists with Last.fm genre tags."""

import os
import requests
from dotenv import load_dotenv
from psycopg import Error as PsycopgError

from .database import (
    cache_lastfm_genres,
    connect,
    get_unenriched_artists,
    initialize_database,
)


LASTFM_URL = "https://ws.audioscrobbler.com/2.0/"


def find_artist(artist_id: str, artist_name: str, api_key: str) -> dict:
    """Fetch Last.fm artist tags, accepting only an exact returned name."""
    response = requests.get(
        LASTFM_URL,
        params={
            "method": "artist.getinfo",
            "artist": artist_name,
            "api_key": api_key,
            "autocorrect": "0",
            "format": "json",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        if payload["error"] == 6:
            return {"id": artist_id, "name": artist_name, "genres": []}
        raise RuntimeError(f"Last.fm API error {payload['error']}: {payload.get('message', 'Unknown error')}")

    artist = payload.get("artist", {})
    if artist.get("name", "").casefold() != artist_name.casefold():
        return {"id": artist_id, "name": artist_name, "genres": []}

    tags = artist.get("tags", {}).get("tag", [])
    if isinstance(tags, dict):
        tags = [tags]
    genres = sorted({tag["name"].strip() for tag in tags if tag.get("name", "").strip()}, key=str.casefold)
    return {"id": artist_id, "name": artist_name, "genres": genres}


def main() -> None:
    load_dotenv()
    api_key = os.getenv("LASTFM_API_KEY")
    if not api_key:
        raise SystemExit("LASTFM_API_KEY is not configured. Add it to .env before enrichment.")

    try:
        with connect() as connection:
            initialize_database(connection)
            uncached_artists = get_unenriched_artists(connection)
            if not uncached_artists:
                print("No artists need Last.fm enrichment.")
                return

            cached_count = 0
            genre_count = 0
            for artist_id, artist_name in uncached_artists:
                artist = find_artist(artist_id, artist_name, api_key)
                artist_count, artist_genre_count = cache_lastfm_genres(connection, [artist])
                cached_count += artist_count
                genre_count += artist_genre_count
                print(f"Processed {cached_count}/{len(uncached_artists)} artist(s).", flush=True)
    except (requests.RequestException, PsycopgError, RuntimeError) as error:
        raise SystemExit(f"Last.fm enrichment failed: {error}") from error

    print(f"Cached Last.fm metadata for {cached_count} artist(s) and {genre_count} genre tag(s).")


if __name__ == "__main__":
    main()
