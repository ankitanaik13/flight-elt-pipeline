"""
Lookup table loader for airport and airline reference data.

Downloads the OpenFlights .dat files (CSV without headers) and
writes them into staging.airport_raw and staging.airline_raw.
"""

import logging
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# Positional column names for airports.dat (14 fields)
AIRPORT_COLUMNS = [
    "openflights_id", "airport_name", "city", "country",
    "iata_code", "icao_code", "latitude", "longitude",
    "altitude", "utc_offset", "dst", "timezone",
    "type", "source",
]

# Positional column names for airlines.dat (8 fields)
AIRLINE_COLUMNS = [
    "openflights_id", "airline_name", "alias", "iata_code",
    "icao_code", "callsign", "country", "active",
]


class LookupLoader:
    """Loads airport and airline reference data into the staging schema."""

    def __init__(self, conn_string: str) -> None:
        """
        Initialise the loader.

        Args:
            conn_string: SQLAlchemy connection URL for pipeline_db.
        """
        self.conn_string = conn_string
        self.engine = create_engine(conn_string)
        logger.info("LookupLoader initialised")

    def _fetch_dat(self, url: str) -> str:
        """Download a .dat file and return its text content."""
        logger.info("Fetching %s", url)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    # ------------------------------------------------------------------
    # Airports
    # ------------------------------------------------------------------

    def load_airports(self, url: str) -> dict:
        """
        Download airports.dat and write valid IATA-coded airports to staging.airport_raw.

        Filters to rows where iata_code is a 3-letter code (excludes '\\N' nulls).

        Args:
            url: URL of the OpenFlights airports.dat file.

        Returns:
            Dict with rows_loaded count.
        """
        raw = self._fetch_dat(url)
        df = pd.read_csv(
            StringIO(raw),
            header=None,
            names=AIRPORT_COLUMNS,
            quotechar='"',
            on_bad_lines="skip",
        )

        # Keep only the columns we need
        df = df[["iata_code", "icao_code", "airport_name", "city", "country",
                  "latitude", "longitude", "altitude", "timezone", "dst"]].copy()

        # Clean and filter
        df["iata_code"] = df["iata_code"].str.strip().replace(r"^\\N$", None, regex=True)
        df = df[df["iata_code"].notna() & (df["iata_code"].str.len() == 3)]
        df["ingested_at"] = datetime.utcnow()

        # Truncate first to avoid DROP dependency issues
        with self.engine.connect() as con:
            con.execute("TRUNCATE TABLE staging.airport_raw")
        df.to_sql(
            "airport_raw",
            self.engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
        )

        rows = len(df)
        logger.info("Airports loaded: %d rows → staging.airport_raw", rows)
        return {"rows_loaded": rows}

    # ------------------------------------------------------------------
    # Airlines
    # ------------------------------------------------------------------

    def load_airlines(self, url: str) -> dict:
        """
        Download airlines.dat and write active IATA-coded airlines to staging.airline_raw.

        Filters to rows where iata_code is not '\\N' and active == 'Y'.

        Args:
            url: URL of the OpenFlights airlines.dat file.

        Returns:
            Dict with rows_loaded count.
        """
        raw = self._fetch_dat(url)
        df = pd.read_csv(
            StringIO(raw),
            header=None,
            names=AIRLINE_COLUMNS,
            quotechar='"',
            on_bad_lines="skip",
        )

        # Keep only the columns we need
        df = df[["iata_code", "icao_code", "airline_name", "country", "active"]].copy()

        # Clean and filter
        df["iata_code"] = df["iata_code"].str.strip().replace(r"^\\N$", None, regex=True)
        df = df[df["iata_code"].notna() & (df["active"] == "Y")]
        df["ingested_at"] = datetime.utcnow()

        # Truncate first to avoid DROP dependency issues
        with self.engine.connect() as con:
            con.execute("TRUNCATE TABLE staging.airline_raw")
        df.to_sql(
            "airline_raw",
            self.engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
        )

        rows = len(df)
        logger.info("Airlines loaded: %d rows → staging.airline_raw", rows)
        return {"rows_loaded": rows}
