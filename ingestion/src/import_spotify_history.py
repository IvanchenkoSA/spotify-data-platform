"""Import a Spotify privacy export into an immutable raw table.

Both current Extended Streaming History (``endsong_*.json``) and legacy
Streaming History (``Streaming_History_Audio_*.json``) files are supported.
"""

import argparse
import json
import zipfile
from datetime import datetime
from pathlib import Path

from psycopg import Error as PsycopgError

from .database import (
    connect,
    finish_pipeline_run,
    initialize_database,
    load_spotify_extended_history,
    start_pipeline_run,
)


def export_json_paths(input_path: Path) -> list[Path]:
    """Find only music-history JSON files in an extracted Spotify export."""
    if input_path.is_file():
        return [input_path]
    if not input_path.is_dir():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    paths = [
        path
        for path in input_path.rglob("*.json")
        if path.name.casefold().startswith(("endsong_", "streaming_history_audio_"))
    ]
    if not paths:
        raise FileNotFoundError(
            "No endsong_*.json or Streaming_History_Audio_*.json files were found."
        )
    return sorted(paths)


def load_json_events(source_name: str, content: str) -> list[dict]:
    """Validate one export file before normalization."""
    payload = json.loads(content)
    if not isinstance(payload, list):
        raise ValueError(f"{source_name} must contain a JSON array.")
    return [event for event in payload if isinstance(event, dict)]


def parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError(f"Timestamp must include a timezone: {value}")
    return timestamp


def normalize_music_event(payload: dict) -> dict | None:
    """Normalize legacy and current export fields; exclude podcast-only rows."""
    track_name = payload.get("master_metadata_track_name") or payload.get("trackName")
    artist_name = payload.get("master_metadata_album_artist_name") or payload.get("artistName")
    timestamp = payload.get("ts") or payload.get("endTime")
    if not track_name or not artist_name or not timestamp:
        return None

    milliseconds = payload.get("ms_played", payload.get("msPlayed"))
    return {
        "ended_at": parse_timestamp(str(timestamp)),
        "track_uri": payload.get("spotify_track_uri"),
        "track_name": str(track_name).strip(),
        "artist_name": str(artist_name).strip(),
        "album_name": (
            str(payload.get("master_metadata_album_album_name")).strip()
            if payload.get("master_metadata_album_album_name")
            else None
        ),
        "ms_played": int(milliseconds) if milliseconds is not None else None,
        "payload": payload,
    }


def import_file(connection, source_name: str, content: str) -> tuple[int, int]:
    events = load_json_events(source_name, content)
    music_events = [
        normalized
        for event in events
        if (normalized := normalize_music_event(event)) is not None
    ]
    return len(music_events), load_spotify_extended_history(connection, source_name, music_events)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import Spotify Extended Streaming History JSON files or ZIP export."
    )
    parser.add_argument("input_path", type=Path, help="Export ZIP, JSON file, or extracted directory")
    arguments = parser.parse_args()

    run_id: int | None = None
    try:
        with connect() as connection:
            initialize_database(connection)
            run_id = start_pipeline_run(connection, "spotify_extended_history_import")
            normalized_count = 0
            inserted_count = 0
            if arguments.input_path.suffix.casefold() == ".zip":
                with zipfile.ZipFile(arguments.input_path) as archive:
                    names = [
                        name
                        for name in archive.namelist()
                        if Path(name).name.casefold().startswith(
                            ("endsong_", "streaming_history_audio_")
                        )
                        and name.casefold().endswith(".json")
                    ]
                    if not names:
                        raise FileNotFoundError("The ZIP does not contain Spotify music-history JSON files.")
                    for name in sorted(names):
                        count, inserted = import_file(
                            connection, name, archive.read(name).decode("utf-8")
                        )
                        normalized_count += count
                        inserted_count += inserted
                        print(f"Imported {name}: {inserted}/{count} new music event(s).")
            else:
                for path in export_json_paths(arguments.input_path):
                    count, inserted = import_file(connection, path.name, path.read_text(encoding="utf-8"))
                    normalized_count += count
                    inserted_count += inserted
                    print(f"Imported {path.name}: {inserted}/{count} new music event(s).")
            finish_pipeline_run(
                connection,
                run_id,
                "succeeded",
                extracted_count=normalized_count,
                inserted_count=inserted_count,
            )
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError, zipfile.BadZipFile, PsycopgError) as error:
        if run_id is not None:
            try:
                with connect() as connection:
                    finish_pipeline_run(connection, run_id, "failed", error_message=str(error))
            except PsycopgError:
                pass
        raise SystemExit(f"Spotify history import failed: {error}") from error

    print(f"Processed {normalized_count} music event(s); inserted {inserted_count} new raw row(s).")


if __name__ == "__main__":
    main()
