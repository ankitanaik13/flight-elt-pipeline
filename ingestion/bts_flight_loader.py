"""Flight ingestion utilities for BTS on-time performance data."""

import logging
import os
import time
import zipfile
from pathlib import Path
from typing import Dict, List

import pandas as pd
import io
import psycopg2
import requests
from sqlalchemy import create_engine


class BtsFlightLoader:
    """Download, extract, validate, and stage BTS flight data."""

    def __init__(self, conn_string: str, zip_url: str, local_zip: str, local_csv: str) -> None:
        """Create the loader with database and file configuration."""
        self.conn_string = conn_string
        self.zip_url = zip_url
        self.local_zip = local_zip
        self.local_csv = local_csv
        self.engine = create_engine(conn_string)
        self.logger = logging.getLogger("BtsFlightLoader")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def download_zip(self) -> str:
        """Download the monthly BTS zip archive to a local path."""
        self.logger.info("Downloading BTS archive from %s", self.zip_url)
        os.makedirs(Path(self.local_zip).parent, exist_ok=True)
        start = time.time()
        response = requests.get(self.zip_url, stream=True, timeout=300)
        response.raise_for_status()
        total = 0
        with open(self.local_zip, "wb") as handler:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handler.write(chunk)
                    total += len(chunk)
                    if total >= 10 * 1024 * 1024 and total % (10 * 1024 * 1024) < len(chunk):
                        self.logger.info("Downloaded %s MB", round(total / (1024 * 1024), 2))
        duration = round(time.time() - start, 2)
        self.logger.info("Downloaded %s in %s seconds", self.local_zip, duration)
        return self.local_zip

    def extract_csv(self, zip_path: str) -> str:
        """Extract the CSV file from the zip archive."""
        self.logger.info("Extracting archive %s", zip_path)
        with zipfile.ZipFile(zip_path, "r") as archive:
            csv_files = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_files:
                raise ValueError("No CSV files found in BTS archive")
            archive.extract(csv_files[0], Path(self.local_csv).parent)
            extracted = str(Path(self.local_csv).parent / csv_files[0])
        self.logger.info("Extracted %s (%s MB)", extracted, round(Path(extracted).stat().st_size / (1024 * 1024), 2))
        return extracted

    def validate_csv(self, csv_path: str) -> Dict[str, object]:
        """Sample the extracted CSV file to confirm columns and size."""
        sample = pd.read_csv(csv_path, nrows=5)
        file_size_mb = round(Path(csv_path).stat().st_size / (1024 * 1024), 2)
        estimated_rows = int(Path(csv_path).stat().st_size / max(sample.memory_usage(deep=True).sum() / max(len(sample), 1), 1))
        self.logger.info("CSV columns: %s", list(sample.columns))
        self.logger.info("Sample rows: %s", len(sample))
        return {
            "columns": list(sample.columns),
            "sample_row_count": len(sample),
            "estimated_total_rows": estimated_rows,
            "file_size_mb": file_size_mb,
        }


    def _pg_conn(self):
        """Return a raw psycopg2 connection."""
        u = urlparse(self.conn_string.replace("postgresql+psycopg2", "postgresql"))
        return psycopg2.connect(
            host=u.hostname, port=u.port or 5432,
            dbname=u.path.lstrip("/"),
            user=u.username, password=u.password,
        )

    def load_to_staging(self, csv_path: str, chunk_size: int = 50000) -> Dict[str, object]:
        """Load BTS CSV file into the staging.flight_raw table in chunks."""
        rename_map = {
            "FlightDate": "fl_date",
            "Reporting_Airline": "reporting_airline",
            "Tail_Number": "tail_number",
            "Flight_Number_Reporting_Airline": "flight_number",
            "OriginAirportID": "origin_airport_id",
            "Origin": "origin",
            "OriginCityName": "origin_city_name",
            "OriginStateName": "origin_state",
            "DestAirportID": "dest_airport_id",
            "Dest": "dest",
            "DestCityName": "dest_city_name",
            "DestStateName": "dest_state",
            "CRSDepTime": "crs_dep_time",
            "DepTime": "dep_time",
            "DepDelay": "dep_delay",
            "DepDelayMinutes": "dep_delay_minutes",
            "TaxiOut": "taxi_out",
            "WheelsOff": "wheels_off",
            "WheelsOn": "wheels_on",
            "TaxiIn": "taxi_in",
            "CRSArrTime": "crs_arr_time",
            "ArrTime": "arr_time",
            "ArrDelay": "arr_delay",
            "ArrDelayMinutes": "arr_delay_minutes",
            "Cancelled": "cancelled",
            "CancellationCode": "cancellation_code",
            "Diverted": "diverted",
            "CRSElapsedTime": "crs_elapsed_time",
            "ActualElapsedTime": "actual_elapsed_time",
            "AirTime": "air_time",
            "Distance": "distance",
            "CarrierDelay": "carrier_delay",
            "WeatherDelay": "weather_delay",
            "NASDelay": "nas_delay",
            "SecurityDelay": "security_delay",
            "LateAircraftDelay": "late_aircraft_delay",
        }
        self.logger.info("Starting chunked load from %s", csv_path)
        start = time.time()
        total_rows = 0
        chunks = 0
        from urllib.parse import urlparse
        u = urlparse(self.conn_string.replace("postgresql+psycopg2", "postgresql"))
        pg_conn = psycopg2.connect(
            host=u.hostname, port=u.port or 5432,
            dbname=u.path.lstrip("/"),
            user=u.username, password=u.password,
        )
        pg_conn.autocommit = False
        cur = pg_conn.cursor()
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
            # Step 1: strip BTS trailing _mNNNNN duplicate suffixes
            chunk.columns = chunk.columns.str.replace(r'_m\d+$', '', regex=True)
            # Step 2: drop duplicate columns keeping first
            chunk = chunk.loc[:, ~chunk.columns.duplicated()]
            # Step 3: drop unnamed columns
            chunk = chunk.loc[:, ~chunk.columns.str.startswith('Unnamed')]
            # Step 4: rename to snake_case
            chunk = chunk.rename(columns=rename_map)
            # Step 5: keep only columns we need
            keep = [c for c in rename_map.values() if c in chunk.columns]
            chunk = chunk[keep]
            # Step 6: add metadata columns
            chunk["ingested_at"] = pd.Timestamp.utcnow()
            chunk["source_file"] = Path(csv_path).name
            buf = io.StringIO()
            chunk.to_csv(buf, index=False, header=False, na_rep="")
            buf.seek(0)
            cols = ", ".join(chunk.columns)
            cur.copy_expert(
                f"COPY staging.flight_raw ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '')",
                buf,
            )
            pg_conn.commit()
            chunks += 1
            total_rows += len(chunk)
            self.logger.info(
                "Loaded chunk %s with %s rows (cumulative %s)",
                chunks, len(chunk), total_rows,
            )
        cur.close()
        pg_conn.close()
        duration = round(time.time() - start, 2)
        return {
            "total_rows": total_rows,
            "chunks": chunks,
            "duration_seconds": duration,
            "start_time": start,
            "end_time": time.time(),
        }

    def validate_load(self) -> Dict[str, object]:
        """Run a verification query against the staging table."""
        query = """
        SELECT
          COUNT(*) AS total_rows,
          MIN(fl_date) AS min_date,
          MAX(fl_date) AS max_date,
          COUNT(DISTINCT reporting_airline) AS unique_airlines,
          COUNT(DISTINCT origin) AS unique_origins,
          SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
          ROUND(AVG(arr_delay_minutes) FILTER (WHERE arr_delay_minutes > 0)::NUMERIC, 2) AS avg_arr_delay_minutes
        FROM staging.flight_raw;
        """
        result = pd.read_sql(query, con=self.engine).iloc[0].to_dict()
        self.logger.info("Validation report: %s", result)
        if result["total_rows"] == 0:
            raise ValueError("No flights were loaded into staging.flight_raw")
        return result

    def cleanup(self, *paths: str) -> None:
        """Delete temporary source files after a successful run."""
        for path in paths:
            if path and os.path.exists(path):
                os.remove(path)
                self.logger.info("Removed temporary file %s", path)
