"""Transform raw Spotify listening events into an analysis-ready table."""

import os

from psycopg import Error as PsycopgError
from tzlocal import get_localzone_name

from .database import (
    connect,
    initialize_database,
    set_analytics_timezone,
    transform_listening_history,
)


def main() -> None:
    timezone_name = os.getenv("ANALYTICS_TIMEZONE", get_localzone_name())

    try:
        with connect() as connection:
            initialize_database(connection)
            set_analytics_timezone(connection, timezone_name)
            transformed_count = transform_listening_history(connection)
    except PsycopgError as error:
        raise SystemExit(f"Transformation failed: {error}") from error

    print(f"Analytics timezone: {timezone_name}")
    print(f"Inserted {transformed_count} new row(s) into mart_listening_history.")


if __name__ == "__main__":
    main()
