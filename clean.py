import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("clean.log"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)

DB_FILE = "emissions.duckdb"

def main():
    con = None
    try:
        # Connect
        con = duckdb.connect(DB_FILE, read_only=False)
        logger.info("Connected to DuckDB")

        # Unify Yellow + Green into a consistent staging view
        logger.info("Creating unified raw view")
        con.execute("""
        CREATE OR REPLACE VIEW trips_2024_unified_raw AS
        SELECT
            'yellow' AS taxi_color,
            tpep_pickup_datetime  AS pickup_datetime,
            tpep_dropoff_datetime AS dropoff_datetime,
            passenger_count,
            trip_distance,
            PULocationID AS pickup_location_id,
            DOLocationID AS dropoff_location_id,
            fare_amount,
            tip_amount,
            total_amount,
            payment_type
        FROM yellow_trips_2024
        UNION ALL
        SELECT
            'green' AS taxi_color,
            lpep_pickup_datetime  AS pickup_datetime,
            lpep_dropoff_datetime AS dropoff_datetime,
            passenger_count,
            trip_distance,
            PULocationID AS pickup_location_id,
            DOLocationID AS dropoff_location_id,
            fare_amount,
            tip_amount,
            total_amount,
            payment_type
        FROM green_trips_2024
        """)

        # Materialize with clean types and duration_seconds
        logger.info("Materializing stage table with typed columns + duration")
        con.execute("""
        CREATE OR REPLACE TABLE trips_2024_stage AS
        SELECT
            taxi_color,
            CAST(pickup_datetime  AS TIMESTAMP)        AS pickup_datetime,
            CAST(dropoff_datetime AS TIMESTAMP)        AS dropoff_datetime,
            CAST(passenger_count  AS INTEGER)          AS passenger_count,
            CAST(trip_distance    AS DOUBLE)           AS trip_distance_miles,
            CAST(pickup_location_id  AS INTEGER)       AS pickup_location_id,
            CAST(dropoff_location_id AS INTEGER)       AS dropoff_location_id,
            CAST(fare_amount      AS DOUBLE)           AS fare_amount,
            CAST(tip_amount       AS DOUBLE)           AS tip_amount,
            CAST(total_amount     AS DOUBLE)           AS total_amount,
            CAST(payment_type     AS INTEGER)          AS payment_type,
            date_diff('second',
                      CAST(pickup_datetime  AS TIMESTAMP),
                      CAST(dropoff_datetime AS TIMESTAMP)) AS duration_seconds
        FROM trips_2024_unified_raw
        """)

        # De-duplicate (keep first row per logical trip key)
        logger.info("Removing duplicates")
        con.execute("""
        CREATE OR REPLACE TABLE trips_2024_dedup AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        taxi_color,
                        pickup_datetime, dropoff_datetime,
                        pickup_location_id, dropoff_location_id,
                        passenger_count, trip_distance_miles,
                        fare_amount, tip_amount, total_amount, payment_type
                    ORDER BY pickup_datetime
                ) AS rn
            FROM trips_2024_stage
        )
        SELECT * FROM ranked WHERE rn = 1
        """)

        # 4) Apply cleaning rules
        logger.info("Applying cleaning rules")
        con.execute("""
        CREATE OR REPLACE TABLE trips_2024_clean AS
        SELECT *
        FROM trips_2024_dedup
        WHERE passenger_count > 0                       
          AND trip_distance_miles > 0                   
          AND trip_distance_miles <= 100                
          AND duration_seconds >= 0                     
          AND duration_seconds <= 86400                 
        """)

        # Verification to prove the conditions no longer exist
        logger.info("Running verification checks")

        checks = {
            "duplicates_remaining": """
                SELECT COUNT(*) FROM (
                  SELECT
                    taxi_color, pickup_datetime, dropoff_datetime,
                    pickup_location_id, dropoff_location_id,
                    passenger_count, trip_distance_miles,
                    fare_amount, tip_amount, total_amount, payment_type,
                    COUNT(*) AS c
                  FROM trips_2024_clean
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
                  HAVING COUNT(*) > 1
                );
            """,
            "zero_passengers":   "SELECT COUNT(*) FROM trips_2024_clean WHERE passenger_count = 0;",
            "zero_miles":        "SELECT COUNT(*) FROM trips_2024_clean WHERE trip_distance_miles = 0;",
            "over_100_miles":    "SELECT COUNT(*) FROM trips_2024_clean WHERE trip_distance_miles > 100;",
            "over_1_day":        "SELECT COUNT(*) FROM trips_2024_clean WHERE duration_seconds > 86400;",
            "negative_duration": "SELECT COUNT(*) FROM trips_2024_clean WHERE duration_seconds < 0;",
        }

        print("\n--- CLEAN VERIFICATION ---")
        for label, sql in checks.items():
            val = con.execute(sql).fetchone()[0]
            msg = f"{label}: {val}"
            print(msg)
            logger.info(msg)

        # Helpful totals
        total_clean = con.execute("SELECT COUNT(*) FROM trips_2024_clean;").fetchone()[0]
        print(f"\nrows in trips_2024_clean: {total_clean:,}")
        logger.info(f"rows in trips_2024_clean: {total_clean}")

        by_color = con.execute("""
            SELECT taxi_color, COUNT(*) AS n
            FROM trips_2024_clean
            GROUP BY taxi_color
            ORDER BY taxi_color
        """).fetchall()
        print("rows by taxi_color:")
        for color, n in by_color:
            line = f"  {color}: {n:,}"
            print(line)
            logger.info(line)

        logger.info("Cleaning complete")

    except Exception as e:
        logger.exception(f"Error during cleaning: {e}")
        print(f"Error during cleaning: {e}")
    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    main()
