"""Interactive dashboard for the Spotify listening-history mart."""

import pandas as pd
import psycopg
import streamlit as st

from ingestion.src.database import database_url


st.set_page_config(page_title="Spotify listening dashboard", page_icon="🎧", layout="wide")


@st.cache_data(ttl=60)
def query_dataframe(query: str, params: tuple = ()) -> pd.DataFrame:
    with psycopg.connect(database_url()) as connection:
        return pd.read_sql(query, connection, params=params)


def main() -> None:
    st.title("🎧 Spotify listening dashboard")
    st.caption("Данные обновляются после запуска ingestion и transform.")
    if st.sidebar.button("Обновить данные"):
        query_dataframe.clear()
        st.rerun()

    try:
        date_bounds = query_dataframe(
            """
            SELECT
                MIN((listening.played_at AT TIME ZONE settings.setting_value)::DATE) AS first_play_date,
                MAX((listening.played_at AT TIME ZONE settings.setting_value)::DATE) AS last_play_date
            FROM mart_listening_history AS listening
            CROSS JOIN (
                SELECT setting_value
                FROM pipeline_settings
                WHERE setting_name = 'analytics_timezone'
            ) AS settings
            """
        )
        if date_bounds.empty or pd.isna(date_bounds.loc[0, "first_play_date"]):
            st.info("В аналитической таблице пока нет данных. Сначала запусти ingestion и transform.")
            return

        first_play_date = date_bounds.loc[0, "first_play_date"]
        last_play_date = date_bounds.loc[0, "last_play_date"]
        selected_period = st.sidebar.date_input(
            "Период прослушиваний",
            value=(first_play_date, last_play_date),
            min_value=first_play_date,
            max_value=last_play_date,
        )
        if not isinstance(selected_period, tuple) or len(selected_period) != 2:
            st.sidebar.info("Выбери дату начала и дату окончания периода.")
            return

        start_date, end_date = selected_period
        filter_params = (start_date, end_date)
        filtered_listening = """
            FROM mart_listening_history AS listening
            CROSS JOIN (
                SELECT setting_value
                FROM pipeline_settings
                WHERE setting_name = 'analytics_timezone'
            ) AS settings
            WHERE (listening.played_at AT TIME ZONE settings.setting_value)::DATE
                BETWEEN %s AND %s
        """

        summary = query_dataframe(
            f"""
            SELECT
                COUNT(DISTINCT listening.played_at) AS total_plays,
                COUNT(DISTINCT listening.track_id) AS unique_tracks,
                COUNT(DISTINCT artist.artist_name) AS unique_artists,
                MIN(listening.played_at) AS first_play,
                MAX(listening.played_at) AS last_play
            FROM mart_listening_history AS listening
            CROSS JOIN (
                SELECT setting_value
                FROM pipeline_settings
                WHERE setting_name = 'analytics_timezone'
            ) AS settings
            CROSS JOIN LATERAL UNNEST(listening.artist_names) AS artist(artist_name)
            WHERE (listening.played_at AT TIME ZONE settings.setting_value)::DATE
                BETWEEN %s AND %s
            """,
            filter_params,
        )
        daily = query_dataframe(
            f"""
            SELECT
                (played_at AT TIME ZONE settings.setting_value)::DATE AS played_date_local,
                COUNT(*) AS play_count
            {filtered_listening}
            GROUP BY played_date_local
            ORDER BY played_date_local
            """,
            filter_params,
        )
        hourly = query_dataframe(
            f"""
            SELECT
                EXTRACT(HOUR FROM played_at AT TIME ZONE settings.setting_value)::SMALLINT AS hour_local,
                COUNT(*) AS play_count
            {filtered_listening}
            GROUP BY hour_local
            ORDER BY hour_local
            """,
            filter_params,
        )
        top_tracks = query_dataframe(
            f"""
            SELECT track_name, array_to_string(artist_names, ', ') AS artists, COUNT(*) AS play_count
            {filtered_listening}
            GROUP BY track_name, artist_names
            ORDER BY play_count DESC, track_name
            LIMIT 10
            """,
            filter_params,
        )
        top_artists = query_dataframe(
            f"""
            SELECT
                artist.artist_name,
                COUNT(*) AS play_count,
                COUNT(DISTINCT listening.track_id) AS unique_tracks
            FROM mart_listening_history AS listening
            CROSS JOIN (
                SELECT setting_value
                FROM pipeline_settings
                WHERE setting_name = 'analytics_timezone'
            ) AS settings
            CROSS JOIN LATERAL UNNEST(listening.artist_names) AS artist(artist_name)
            WHERE (listening.played_at AT TIME ZONE settings.setting_value)::DATE
                BETWEEN %s AND %s
            GROUP BY artist.artist_name
            ORDER BY play_count DESC, artist.artist_name
            LIMIT 10
            """,
            filter_params,
        )
        top_genres = query_dataframe(
            """
            SELECT genre.genre, COUNT(DISTINCT listening.played_at) AS play_count
            FROM raw_recently_played AS raw
            JOIN mart_listening_history AS listening ON listening.played_at = raw.played_at
            CROSS JOIN (
                SELECT setting_value
                FROM pipeline_settings
                WHERE setting_name = 'analytics_timezone'
            ) AS settings
            CROSS JOIN LATERAL jsonb_array_elements(raw.payload->'track'->'artists') AS track_artist
            JOIN artist_genres AS genre ON genre.artist_id = track_artist->>'id'
            WHERE (listening.played_at AT TIME ZONE settings.setting_value)::DATE
                BETWEEN %s AND %s
            GROUP BY genre.genre
            ORDER BY play_count DESC, genre.genre
            LIMIT 10
            """,
            filter_params,
        )
        recent = query_dataframe(
            f"""
            SELECT
                played_at AT TIME ZONE settings.setting_value AS played_at_local,
                track_name,
                array_to_string(artist_names, ', ') AS artists,
                album_name
            {filtered_listening}
            ORDER BY played_at DESC
            LIMIT 20
            """,
            filter_params,
        )
    except (psycopg.Error, KeyError) as error:
        st.error(f"Не удалось загрузить данные: {error}")
        st.info("Запусти `docker compose up -d`, затем ingestion и transform.")
        return

    if summary.empty or not summary.loc[0, "total_plays"]:
        st.info("За выбранный период прослушиваний нет.")
        return

    metrics = summary.loc[0]
    metric_one, metric_two, metric_three = st.columns(3)
    metric_one.metric("Прослушиваний", int(metrics["total_plays"]))
    metric_two.metric("Уникальных треков", int(metrics["unique_tracks"]))
    metric_three.metric("Уникальных исполнителей", int(metrics["unique_artists"]))

    left, right = st.columns(2)
    with left:
        st.subheader("Прослушивания по дням")
        st.line_chart(daily, x="played_date_local", y="play_count")
    with right:
        st.subheader("В какое время слушаешь")
        st.bar_chart(hourly, x="hour_local", y="play_count")

    left, right = st.columns(2)
    with left:
        st.subheader("Топ треков")
        st.dataframe(top_tracks, hide_index=True, use_container_width=True)
    with right:
        st.subheader("Топ исполнителей")
        st.dataframe(top_artists, hide_index=True, use_container_width=True)

    st.subheader("Последние прослушивания")
    st.dataframe(recent, hide_index=True, use_container_width=True)

    st.subheader("Top genres (from artist metadata)")
    if top_genres.empty:
        st.info("Run artist enrichment to populate genre analytics.")
    else:
        st.bar_chart(top_genres, x="genre", y="play_count", horizontal=True)


if __name__ == "__main__":
    main()
