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


def connect() -> psycopg.Connection:
    return psycopg.connect(database_url())
