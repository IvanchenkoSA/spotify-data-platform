"""Interactive dashboard for the Spotify listening-history mart."""

import pandas as pd
import psycopg
import streamlit as st

from ingestion.src.database import database_url


st.set_page_config(page_title="Spotify listening dashboard", page_icon="🎧", layout="wide")


@st.cache_data(ttl=60)
def query_dataframe(query: str) -> pd.DataFrame:
    with psycopg.connect(database_url()) as connection:
        return pd.read_sql(query, connection)


def main() -> None:
    st.title("🎧 Spotify listening dashboard")
    st.caption("Данные обновляются после запуска ingestion и transform.")
    if st.sidebar.button("Обновить данные"):
        query_dataframe.clear()
        st.rerun()

    try:
        summary = query_dataframe(
            """
            SELECT
                (SELECT COUNT(*) FROM mart_listening_history) AS total_plays,
                (SELECT COUNT(DISTINCT track_id) FROM mart_listening_history) AS unique_tracks,
                (
                    SELECT COUNT(DISTINCT artist_name)
                    FROM mart_listening_history
                    CROSS JOIN LATERAL UNNEST(artist_names) AS artist(artist_name)
                ) AS unique_artists,
                (SELECT MIN(played_at) FROM mart_listening_history) AS first_play,
                (SELECT MAX(played_at) FROM mart_listening_history) AS last_play
            """
        )
        daily = query_dataframe(
            """
            SELECT played_date_local, play_count
            FROM analytics_listening_by_day
            ORDER BY played_date_local
            """
        )
        hourly = query_dataframe(
            """
            SELECT hour_local, play_count
            FROM analytics_listening_by_hour
            ORDER BY hour_local
            """
        )
        top_tracks = query_dataframe(
            """
            SELECT track_name, array_to_string(artist_names, ', ') AS artists, play_count
            FROM analytics_top_tracks
            ORDER BY play_count DESC, track_name
            LIMIT 10
            """
        )
        top_artists = query_dataframe(
            """
            SELECT artist_name, play_count, unique_tracks
            FROM analytics_top_artists
            ORDER BY play_count DESC, artist_name
            LIMIT 10
            """
        )
        recent = query_dataframe(
            """
            SELECT
                played_at AT TIME ZONE (
                    SELECT setting_value FROM pipeline_settings
                    WHERE setting_name = 'analytics_timezone'
                ) AS played_at_local,
                track_name,
                array_to_string(artist_names, ', ') AS artists,
                album_name
            FROM mart_listening_history
            ORDER BY played_at DESC
            LIMIT 20
            """
        )
    except (psycopg.Error, KeyError) as error:
        st.error(f"Не удалось загрузить данные: {error}")
        st.info("Запусти `docker compose up -d`, затем ingestion и transform.")
        return

    if summary.empty or not summary.loc[0, "total_plays"]:
        st.info("В аналитической таблице пока нет данных. Сначала запусти ingestion и transform.")
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


if __name__ == "__main__":
    main()
