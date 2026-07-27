"""PostgreSQL storage for raw Spotify listening events."""

import os
from datetime import datetime

import psycopg
from psycopg.types.json import Jsonb


DEFAULT_DATABASE_URL = "postgresql://spotify:spotify@localhost:5433/spotify"
WATERMARK_NAME = "recently_played"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_recently_played (
    played_at TIMESTAMPTZ PRIMARY KEY,
    track_id TEXT,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion_watermarks (
    stream_name TEXT PRIMARY KEY,
    played_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_settings (
    setting_name TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart_listening_history (
    played_at TIMESTAMPTZ PRIMARY KEY,
    track_id TEXT,
    track_name TEXT NOT NULL,
    artist_names TEXT[] NOT NULL,
    album_id TEXT,
    album_name TEXT,
    album_release_date TEXT,
    duration_ms INTEGER,
    explicit BOOLEAN,
    popularity SMALLINT,
    spotify_url TEXT,
    raw_ingested_at TIMESTAMPTZ NOT NULL,
    transformed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS mart_listening_history_track_id_idx
    ON mart_listening_history (track_id);

CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id TEXT PRIMARY KEY,
    artist_name TEXT NOT NULL,
    spotify_url TEXT,
    popularity SMALLINT,
    genres_fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE dim_artists
    ADD COLUMN IF NOT EXISTS musicbrainz_id UUID,
    ADD COLUMN IF NOT EXISTS musicbrainz_fetched_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS artist_genres (
    artist_id TEXT NOT NULL REFERENCES dim_artists (artist_id) ON DELETE CASCADE,
    genre TEXT NOT NULL,
    PRIMARY KEY (artist_id, genre)
);

CREATE INDEX IF NOT EXISTS artist_genres_genre_idx
    ON artist_genres (genre);

DROP VIEW IF EXISTS analytics_top_tracks;
DROP VIEW IF EXISTS analytics_top_artists;
DROP VIEW IF EXISTS analytics_listening_by_day;
DROP VIEW IF EXISTS analytics_listening_by_hour;
DROP VIEW IF EXISTS analytics_top_genres;

CREATE OR REPLACE VIEW analytics_top_tracks AS
SELECT
    track_id,
    track_name,
    artist_names,
    COUNT(*) AS play_count,
    MIN(played_at) AS first_played_at,
    MAX(played_at) AS last_played_at
FROM mart_listening_history
GROUP BY track_id, track_name, artist_names;

CREATE OR REPLACE VIEW analytics_top_artists AS
SELECT
    artist.artist_name,
    COUNT(*) AS play_count,
    COUNT(DISTINCT listening.track_id) AS unique_tracks,
    MIN(listening.played_at) AS first_played_at,
    MAX(listening.played_at) AS last_played_at
FROM mart_listening_history AS listening
CROSS JOIN LATERAL UNNEST(listening.artist_names) AS artist(artist_name)
GROUP BY artist.artist_name;

CREATE OR REPLACE VIEW analytics_listening_by_day AS
SELECT
    settings.setting_value AS timezone_name,
    (played_at AT TIME ZONE settings.setting_value)::DATE AS played_date_local,
    COUNT(*) AS play_count,
    COUNT(DISTINCT track_id) AS unique_tracks,
    COUNT(DISTINCT track_id) FILTER (WHERE explicit) AS explicit_track_plays
FROM mart_listening_history
CROSS JOIN (
    SELECT setting_value
    FROM pipeline_settings
    WHERE setting_name = 'analytics_timezone'
) AS settings
GROUP BY settings.setting_value, (played_at AT TIME ZONE settings.setting_value)::DATE;

CREATE OR REPLACE VIEW analytics_listening_by_hour AS
SELECT
    settings.setting_value AS timezone_name,
    EXTRACT(HOUR FROM played_at AT TIME ZONE settings.setting_value)::SMALLINT AS hour_local,
    COUNT(*) AS play_count,
    COUNT(DISTINCT track_id) AS unique_tracks
FROM mart_listening_history
CROSS JOIN (
    SELECT setting_value
    FROM pipeline_settings
    WHERE setting_name = 'analytics_timezone'
) AS settings
GROUP BY settings.setting_value, EXTRACT(HOUR FROM played_at AT TIME ZONE settings.setting_value);

CREATE OR REPLACE VIEW analytics_top_genres AS
WITH listening_genres AS (
    SELECT DISTINCT
        listening.played_at,
        genre.genre
    FROM raw_recently_played AS raw
    JOIN mart_listening_history AS listening ON listening.played_at = raw.played_at
    CROSS JOIN LATERAL jsonb_array_elements(raw.payload->'track'->'artists') AS track_artist
    JOIN artist_genres AS genre ON genre.artist_id = track_artist->>'id'
)
SELECT
    genre,
    COUNT(*) AS play_count
FROM listening_genres
GROUP BY genre;
"""


def database_url() -> str:
    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


def initialize_database(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(SCHEMA_SQL)
    connection.commit()


def get_watermark(connection: psycopg.Connection) -> datetime | None:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT played_at FROM ingestion_watermarks WHERE stream_name = %s",
            (WATERMARK_NAME,),
        )
        row = cursor.fetchone()
    return row[0] if row else None


def set_analytics_timezone(connection: psycopg.Connection, timezone_name: str) -> None:
    """Validate and persist the local timezone used by analytics views."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT NOW() AT TIME ZONE %s", (timezone_name,))
        cursor.execute(
            """
            INSERT INTO pipeline_settings (setting_name, setting_value)
            VALUES ('analytics_timezone', %s)
            ON CONFLICT (setting_name) DO UPDATE
            SET setting_value = EXCLUDED.setting_value, updated_at = NOW()
            """,
            (timezone_name,),
        )
    connection.commit()


def load_recently_played(connection: psycopg.Connection, payload: dict) -> int:
    """Insert raw events and move the watermark in one transaction."""
    items = payload.get("items", [])
    rows = [
        (item["played_at"], item.get("track", {}).get("id"), Jsonb(item))
        for item in items
    ]

    inserted_count = 0
    with connection.cursor() as cursor:
        for row in rows:
            cursor.execute(
                """
                INSERT INTO raw_recently_played (played_at, track_id, payload)
                VALUES (%s, %s, %s)
                ON CONFLICT (played_at) DO NOTHING
                RETURNING played_at
                """,
                row,
            )
            inserted_count += int(cursor.fetchone() is not None)

        if rows:
            newest_played_at = max(item["played_at"] for item in items)
            cursor.execute(
                """
                INSERT INTO ingestion_watermarks (stream_name, played_at)
                VALUES (%s, %s)
                ON CONFLICT (stream_name) DO UPDATE
                SET played_at = EXCLUDED.played_at, updated_at = NOW()
                WHERE ingestion_watermarks.played_at < EXCLUDED.played_at
                """,
                (WATERMARK_NAME, newest_played_at),
            )

    connection.commit()
    return inserted_count


def transform_listening_history(connection: psycopg.Connection) -> int:
    """Flatten raw Spotify JSON into an analysis-ready listening history table."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO mart_listening_history (
                played_at,
                track_id,
                track_name,
                artist_names,
                album_id,
                album_name,
                album_release_date,
                duration_ms,
                explicit,
                popularity,
                spotify_url,
                raw_ingested_at
            )
            SELECT
                raw.played_at,
                raw.payload->'track'->>'id',
                raw.payload->'track'->>'name',
                COALESCE(
                    ARRAY(
                        SELECT artist->>'name'
                        FROM jsonb_array_elements(raw.payload->'track'->'artists') AS artist
                    ),
                    ARRAY[]::TEXT[]
                ),
                raw.payload->'track'->'album'->>'id',
                raw.payload->'track'->'album'->>'name',
                raw.payload->'track'->'album'->>'release_date',
                (raw.payload->'track'->>'duration_ms')::INTEGER,
                (raw.payload->'track'->>'explicit')::BOOLEAN,
                (raw.payload->'track'->>'popularity')::SMALLINT,
                raw.payload->'track'->'external_urls'->>'spotify',
                raw.ingested_at
            FROM raw_recently_played AS raw
            ON CONFLICT (played_at) DO NOTHING
            RETURNING played_at
            """
        )
        inserted_count = len(cursor.fetchall())

    connection.commit()
    return inserted_count


def get_unenriched_artists(connection: psycopg.Connection) -> list[tuple[str, str]]:
    """Return artists whose MusicBrainz genre lookup has not yet run."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT
                artist->>'id' AS artist_id,
                artist->>'name' AS artist_name
            FROM raw_recently_played AS raw
            CROSS JOIN LATERAL jsonb_array_elements(raw.payload->'track'->'artists') AS artist
            LEFT JOIN dim_artists AS cached ON cached.artist_id = artist->>'id'
            WHERE artist->>'id' IS NOT NULL
              AND (cached.artist_id IS NULL OR cached.musicbrainz_fetched_at IS NULL)
            ORDER BY artist->>'id'
            """
        )
        return list(cursor.fetchall())


def cache_musicbrainz_genres(connection: psycopg.Connection, artists: list[dict]) -> tuple[int, int]:
    """Store the MusicBrainz match status and replace its artist genres."""
    genre_count = 0
    with connection.cursor() as cursor:
        for artist in artists:
            cursor.execute(
                """
                INSERT INTO dim_artists (artist_id, artist_name, musicbrainz_id, musicbrainz_fetched_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (artist_id) DO UPDATE
                SET musicbrainz_id = EXCLUDED.musicbrainz_id,
                    musicbrainz_fetched_at = NOW()
                """,
                (
                    artist["id"],
                    artist["name"],
                    artist.get("musicbrainz_id"),
                ),
            )
            cursor.execute("DELETE FROM artist_genres WHERE artist_id = %s", (artist["id"],))
            cursor.executemany(
                """
                INSERT INTO artist_genres (artist_id, genre)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                [(artist["id"], genre) for genre in artist["genres"]],
            )
            genre_count += len(artist["genres"])

    connection.commit()
    return len(artists), genre_count


def connect() -> psycopg.Connection:
    return psycopg.connect(database_url())
