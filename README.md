# Spotify data platform

A local ingestion pipeline for Spotify's recently played tracks.

```text
Spotify API → ingestion → data/raw/*.json + PostgreSQL
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

## What happens on each ingestion run

1. Read the watermark from PostgreSQL.
2. Request recently played Spotify events after that time.
3. Save the API response to `data/raw/`.
4. Insert unseen events into `raw_recently_played` with `ON CONFLICT DO NOTHING`.
5. Advance the watermark in the same database transaction.

The first run receives up to 50 recent events. Subsequent runs add only newly played tracks. Repeating the command without new listening activity inserts zero rows.

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

Stop the local database when it is not needed:

```powershell
docker compose stop
```

The default database credentials are for local development only. Do not use them outside a local environment.
