# Spotify data platform

A local ingestion pipeline for Spotify's recently played tracks.

```text
Spotify API + Last.fm API → raw ingestion → data/raw/*.json + PostgreSQL raw tables → transform → mart_listening_history
```

The pipeline keeps the full API response as raw JSON and loads each listening event into PostgreSQL. A watermark records the most recent event already processed, so later runs request only new listening history. The `played_at` primary key makes reruns idempotent.

## Prerequisites

- Python 3.13+
- Docker Desktop
- A Spotify application configured with the redirect URI `http://127.0.0.1:8888/callback`

Install Python packages:

```powershell
py -3.13 -m pip install -r ingestion/requirements.txt
```

Create a local `.env` file in the repository root. It is ignored by Git.

```env
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8888/callback
LASTFM_API_KEY=your_lastfm_api_key
LASTFM_USERNAME=your_lastfm_username
```

Optionally set the analytics timezone explicitly. If it is omitted, the transform command uses the timezone configured on the machine where it runs.

```env
ANALYTICS_TIMEZONE=Asia/Novosibirsk
```

## First run

1. Authorize with Spotify. A browser window will open; sign in and approve access.

   ```powershell
   py -3.13 -B -m ingestion.src.auth
   ```

   The generated `.spotify-token.json` remains local and is ignored by Git.

2. Start PostgreSQL.

   ```powershell
   docker compose up -d
   ```

3. Fetch and load the listening history.

   ```powershell
   py -3.13 -B -m ingestion.src.main
   ```

4. Transform raw JSON events into an analysis-ready table.

   ```powershell
   py -3.13 -B -m ingestion.src.transform
   ```

5. Enrich cached artists with Last.fm genre tags.

   ```powershell
   py -3.13 -B -m ingestion.src.enrich_artists
   ```

6. Backfill Last.fm scrobbles. On the first run this requests every page in the
   profile history; later runs fetch only scrobbles newer than the saved watermark.

   ```powershell
   py -3.13 -B -m ingestion.src.ingest_lastfm
   ```

## What happens on each ingestion run

1. Read the watermark from PostgreSQL.
2. Request recently played Spotify events after that time.
3. Save the API response to `data/raw/`.
4. Insert unseen events into `raw_recently_played` with `ON CONFLICT DO NOTHING`.
5. Advance the watermark in the same database transaction.

The first run receives up to 50 recent events. Subsequent runs add only newly played tracks. Repeating the command without new listening activity inserts zero rows.

## Last.fm history ingestion

Connect Spotify to Last.fm in its account settings to enable scrobbling. The project reads the public Last.fm profile configured in `LASTFM_USERNAME`; it does not need a Last.fm password or session token. `ingest_lastfm` stores every completed scrobble in `raw_lastfm_scrobbles`, persists every response page under `data/raw/`, and uses a separate `lastfm_scrobbles` watermark. The watermark advances only after every requested page succeeds, while the stable event key makes its one-second overlap idempotent.

Last.fm and Spotify events are intentionally kept separate in the raw layer: a Last.fm scrobble may omit a short skip, while Spotify can include it. A later staging model will reconcile the sources into a canonical listening-events mart without hiding their provenance.

## Spotify Extended Streaming History import

Request **Extended Streaming History** from Spotify's Account Privacy download tool. It is a personal-data ZIP archive, so keep it outside Git (for example, under `data/private/`). The importer reads either the ZIP directly, a single JSON file, or an extracted export directory. It supports current `endsong_*.json` files and legacy `Streaming_History_Audio_*.json` files.

```powershell
py -3.13 -B -m ingestion.src.import_spotify_history data/private/spotify-history.zip
```

The import stores music tracks only in `raw_spotify_extended_history`, preserving the original record JSON. Podcast-only entries are excluded from this music mart input. A stable event key makes reruns safe; the original ZIP is not copied or uploaded anywhere.

## Pipeline observability

Every Spotify, Last.fm, and privacy-export import creates a row in `pipeline_runs`. It records start and finish times, terminal status, extracted and inserted record counts, and an error message when a run fails. Inspect the latest runs locally:

```powershell
docker compose exec postgres psql -U spotify -d spotify -c "SELECT pipeline_name, status, extracted_count, inserted_count, started_at, finished_at FROM pipeline_runs ORDER BY run_id DESC LIMIT 20;"
```

## Transform layer

`raw_recently_played` deliberately keeps the Spotify response intact. The transform command flattens its JSON into `mart_listening_history`, where each row is one listening event and the useful fields are ordinary columns: `track_name`, `artist_names`, `album_name`, `duration_ms`, `popularity`, and `played_at`.

The transform is also idempotent. Run it after ingestion:

```powershell
py -3.13 -B -m ingestion.src.transform
```

## Inspect the data

The database uses port `5433` by default, avoiding a common conflict with locally installed PostgreSQL. Its development-only connection string is:

```text
postgresql://spotify:spotify@localhost:5433/spotify
```

Show the most recent tracks:

```powershell
docker compose exec postgres psql -U spotify -d spotify -c "SELECT played_at, payload->'track'->>'name' AS track_name, payload->'track'->'artists'->0->>'name' AS artist_name FROM raw_recently_played ORDER BY played_at DESC LIMIT 20;"
```

Show the current watermark:

```powershell
docker compose exec postgres psql -U spotify -d spotify -c "SELECT * FROM ingestion_watermarks;"
```

Show the most-played tracks from the mart:

```powershell
docker compose exec postgres psql -U spotify -d spotify -c "SELECT track_name, artist_names, COUNT(*) AS play_count FROM mart_listening_history GROUP BY track_name, artist_names ORDER BY play_count DESC, track_name LIMIT 20;"
```

## Analytics views

The database also provides SQL views. They always use the current contents of `mart_listening_history`, so no separate refresh is required.

| View | Purpose |
| --- | --- |
| `analytics_top_tracks` | Play count and first/last play time for each track |
| `analytics_top_artists` | Play count and number of distinct tracks for each artist |
| `analytics_listening_by_day` | Listening activity by local calendar day |
| `analytics_listening_by_hour` | Listening activity by local hour of day |
| `analytics_top_genres` | Play count by Last.fm tags attached to track artists |

Examples:

```powershell
docker compose exec postgres psql -U spotify -d spotify -c "SELECT track_name, artist_names, play_count FROM analytics_top_tracks ORDER BY play_count DESC, track_name LIMIT 20;"

docker compose exec postgres psql -U spotify -d spotify -c "SELECT artist_name, play_count, unique_tracks FROM analytics_top_artists ORDER BY play_count DESC, artist_name LIMIT 20;"

docker compose exec postgres psql -U spotify -d spotify -c "SELECT * FROM analytics_listening_by_hour ORDER BY hour_local;"
```

## Artist and genre enrichment

Spotify's recently-played response includes artist IDs but not track genres. The enrichment command calls Last.fm's `artist.getInfo` endpoint and stores its zero-or-more public tags in `artist_genres`. `LASTFM_API_KEY` is required in `.env` and is kept local.

The returned artist name must exactly match the Spotify artist name, which avoids assigning tags from a namesake. Run the command again only after adding new listening data; artists already looked up are cached.

Tags belong to artists, not individual tracks. A collaboration can therefore contribute to multiple tags; `analytics_top_genres` counts a play once per distinct tag associated with any artist on that track.

Run enrichment after ingestion and transform:

```powershell
py -3.13 -B -m ingestion.src.enrich_artists
```

Stop the local database when it is not needed:

```powershell
docker compose stop
```

The default database credentials are for local development only. Do not use them outside a local environment.

## Dashboard

The Streamlit dashboard visualizes listening activity by local day and hour, top tracks, top artists, top genres, and the latest listening events. The latest-events table includes Last.fm tags and a human-readable local timestamp with weekday and month names. Use the sidebar date-range selector to apply one local-date period to every metric and visualization.

Start the database and make sure the mart is current:

```powershell
docker compose up -d
py -3.13 -B -m ingestion.src.main
py -3.13 -B -m ingestion.src.transform
py -3.13 -B -m ingestion.src.enrich_artists
```

Then run the dashboard:

```powershell
py -3.13 -m streamlit run dashboard/app.py
```

Open the local address printed by Streamlit, normally `http://localhost:8501`.
