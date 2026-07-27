"""Enrich listening-history artists with MusicBrainz genres."""

import time

import requests
from psycopg import Error as PsycopgError

from .database import (
    cache_musicbrainz_genres,
    connect,
    get_unenriched_artists,
    initialize_database,
)


MUSICBRAINZ_URL = "https://musicbrainz.org/ws/2"
USER_AGENT = "spotify-data-platform/1.0 (local personal analytics project)"
REQUEST_INTERVAL_SECONDS = 1.1
MAX_RETRIES = 3


def musicbrainz_get(path: str, params: dict[str, str]) -> dict:
    """Call MusicBrainz politely, respecting its one-request-per-second limit."""
    for attempt in range(MAX_RETRIES):
        response = requests.get(
            f"{MUSICBRAINZ_URL}{path}",
            params={**params, "fmt": "json"},
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        if response.status_code != 503:
            response.raise_for_status()
            time.sleep(REQUEST_INTERVAL_SECONDS)
            return response.json()
        time.sleep(5 * (attempt + 1))

    response.raise_for_status()
    raise RuntimeError("MusicBrainz did not return a response.")


def find_artist(artist_id: str, artist_name: str) -> dict:
    """Match by exact name to avoid assigning genres from a namesake artist."""
    search_result = musicbrainz_get("/artist", {"query": f'artist:"{artist_name}"', "limit": "5"})
    candidates = search_result.get("artists", [])
    match = next(
        (
            candidate
            for candidate in candidates
            if candidate.get("name", "").casefold() == artist_name.casefold()
            and int(candidate.get("score", 0)) == 100
        ),
        None,
    )
    if match is None:
        return {"id": artist_id, "name": artist_name, "musicbrainz_id": None, "genres": []}

    details = musicbrainz_get(f"/artist/{match['id']}", {"inc": "genres"})
    genres = [genre["name"] for genre in details.get("genres", [])]
    return {
        "id": artist_id,
        "name": artist_name,
        "musicbrainz_id": match["id"],
        "genres": genres,
    }


def main() -> None:
    try:
        with connect() as connection:
            initialize_database(connection)
            uncached_artists = get_unenriched_artists(connection)
            if not uncached_artists:
                print("No new artists need MusicBrainz enrichment.")
                return

            cached_count = 0
            genre_count = 0
            for artist_id, artist_name in uncached_artists:
                artist = find_artist(artist_id, artist_name)
                artist_count, artist_genre_count = cache_musicbrainz_genres(connection, [artist])
                cached_count += artist_count
                genre_count += artist_genre_count
                print(f"Processed {cached_count}/{len(uncached_artists)} artist(s).", flush=True)
    except (requests.RequestException, PsycopgError) as error:
        raise SystemExit(f"MusicBrainz enrichment failed: {error}") from error

    print(f"Cached MusicBrainz metadata for {cached_count} artist(s) and {genre_count} genre tag(s).")


if __name__ == "__main__":
    main()
