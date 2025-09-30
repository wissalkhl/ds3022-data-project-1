import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

DB_FILE = "emissions.duckdb"

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute(f"""
            DROP TABLE IF EXISTS yellow_trips_2024;
            DROP TABLE IF EXISTS green_trips_2024;
            DROP TABLE IF EXISTS vehicle_emissions;
            
            CREATE TABLE yellow_trips_2024 AS
            SELECT * FROM read_parquet('data/yellow_tripdata_2024-*.parquet');

            CREATE TABLE green_trips_2024 AS
            SELECT * FROM read_parquet('data/green_tripdata_2024-*.parquet');         

            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('data/vehicle_emissions.csv', HEADER=TRUE);          
         
            
        """)
        logger.info("Created/loaded yellow_trips_2024, green_trips_2024, vehicle_emissions")

        print("\n--- RAW COUNTS BEFORE CLEANING ---")
        for t in ("yellow_trips_2024", "green_trips_2024", "vehicle_emissions"):
            try:
                cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                print(f"RAW ROW COUNT for {t}: {cnt:,}")
                logger.info(f"RAW ROW COUNT for {t}: {cnt}")
            except Exception as e:
                msg = f"Could not count {t}: {e}"
                print(msg)
                logger.warning(msg)



    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        try:
            if con is not None:
                con.close()
                logger.info("Closed DuckDB connection")
        except Exception:
            pass

if __name__ == "__main__":
    load_parquet_files()