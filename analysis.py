import duckdb
import logging
import os
import math
import matplotlib.pyplot as plt
from collections import defaultdict
from calendar import month_abbr


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("analysis.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

DB_FILE = "/Users/wissalkhlouf/ds3022-data-project-1/emissions.duckdb"


DOW_NAME = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
MONTH_NAME = {i: month_abbr[i] for i in range(1, 13)}

def print_and_log(msg: str):
    print(msg)
    logger.info(msg)

def one_row(con, sql, params=None):
    return con.execute(sql, params or {}).fetchone()

def all_rows(con, sql, params=None):
    return con.execute(sql, params or {}).fetchall()

def main():
    # connect
    con = duckdb.connect(DB_FILE, read_only=True)
    print_and_log("Connected to DuckDB for analysis")

    #  Largest CO2 trip of the year (per color)
    largest_sql = """
    SELECT taxi_color, trip_co2_kgs, trip_distance_miles, pickup_datetime, dropoff_datetime
    FROM (
      SELECT
        taxi_color, trip_co2_kgs, trip_distance_miles, pickup_datetime, dropoff_datetime,
        ROW_NUMBER() OVER (PARTITION BY taxi_color ORDER BY trip_co2_kgs DESC) AS rn
      FROM trips_2024_transformed
    )
    WHERE rn = 1
    ORDER BY taxi_color;
    """
    print_and_log("\n== Largest CO₂ trip of 2024 (by taxi color) ==")
    for color, co2, dist, pu, do in all_rows(con, largest_sql):
        print_and_log(f"{color.upper()}: {co2:.3f} kg CO₂, distance={dist:.2f} miles, pickup={pu}, dropoff={do}")

    # Helper to report heavy/light for an aggregation dimension
    def heavy_light(label: str, column: str, label_map=None):
        sql = f"""
        WITH stats AS (
          SELECT taxi_color, {column} AS k, AVG(trip_co2_kgs) AS avg_co2
          FROM trips_2024_transformed
          GROUP BY taxi_color, {column}
        ),
        ranked AS (
          SELECT
            taxi_color, k, avg_co2,
            ROW_NUMBER() OVER (PARTITION BY taxi_color ORDER BY avg_co2 DESC) AS rmax,
            ROW_NUMBER() OVER (PARTITION BY taxi_color ORDER BY avg_co2 ASC)  AS rmin
          FROM stats
        )
        SELECT taxi_color,
               MAX(CASE WHEN rmax=1 THEN k END) AS heaviest_k,
               MAX(CASE WHEN rmax=1 THEN avg_co2 END) AS heaviest_avg,
               MAX(CASE WHEN rmin=1 THEN k END) AS lightest_k,
               MAX(CASE WHEN rmin=1 THEN avg_co2 END) AS lightest_avg
        FROM ranked
        GROUP BY taxi_color
        ORDER BY taxi_color;
        """
        print_and_log(f"\n== Most carbon heavy/light {label} (average CO₂ per trip) ==")
        for row in all_rows(con, sql):
            taxi_color, heavy_k, heavy_v, light_k, light_v = row
            hk = label_map.get(int(heavy_k), heavy_k) if label_map else heavy_k
            lk = label_map.get(int(light_k), light_k) if label_map else light_k
            print_and_log(f"{taxi_color.upper()}: heavy {label}={hk} ({heavy_v:.3f} kg), light {label}={lk} ({light_v:.3f} kg)")

    #  Heaviest/lightest HOUR (1–24 per spec)
    heavy_light("hour of day (1–24 label)", "hour_of_day",
            {i: i+1 for i in range(24)})


    # Heaviest/lightest DAY OF WEEK (map 0..6 -> Sun..Sat)
    heavy_light("day of week", "day_of_week", DOW_NAME)

    #  Heaviest/lightest WEEK NUMBER (1–52)
    heavy_light("week of year", "week_of_year")

    #  Heaviest/lightest MONTH (map 1..12 -> Jan..Dec)
    heavy_light("month of year", "month_of_year", MONTH_NAME)

     #  Plot monthly CO₂ totals (two panels in ONE PNG):
    #  Top = single-axis (rubric-compliant). Bottom = dual-axis (clearer scale).

    monthly = all_rows(con, """
        SELECT taxi_color, month_of_year, SUM(trip_co2_kgs) AS total_co2
        FROM trips_2024_transformed
        GROUP BY taxi_color, month_of_year
        ORDER BY month_of_year, taxi_color
    """)

    # Organize into 12-month series per color
    series = defaultdict(lambda: [0.0] * 12)
    for color, mo, total in monthly:
        if mo is None:
            continue
        series[color][int(mo) - 1] = float(total)

    x = list(range(1, 13))
    month_labels = [MONTH_NAME[m] for m in x]
    color_map = {"yellow": "gold", "green": "green"}

    # One figure, two rows (subplots)
    fig, (ax_single, ax_dual_left) = plt.subplots(2, 1, figsize=(10, 10))
    ax_dual_right = ax_dual_left.twinx()

    # --- TOP: Single-axis (required by rubric) ---
    for color in sorted(series.keys()):
        y = series[color]
        ax_single.plot(
            x, y, marker="o", linewidth=2,
            label=color.upper(), color=color_map.get(color, None)
        )
    ax_single.set_title("Monthly Taxi CO₂ Totals (2024) — Single Axis")
    ax_single.set_xlabel("Month")
    ax_single.set_ylabel("Total CO₂ (kg)")
    ax_single.set_xticks(x)
    ax_single.set_xticklabels(month_labels)
    ax_single.grid(alpha=0.3)
    ax_single.legend(loc="upper left")

    # --- BOTTOM: Dual-axis (clarity) ---
    y_yellow = series.get("yellow", [0.0] * 12)
    y_green  = series.get("green",  [0.0] * 12)

    ax_dual_left.plot(x, y_yellow, marker="o", linewidth=2, color="gold",  label="YELLOW")
    ax_dual_right.plot(x, y_green,  marker="o", linewidth=2, color="green", label="GREEN")

    ax_dual_left.set_title("Monthly Taxi CO₂ Totals (2024) — Dual Axis")
    ax_dual_left.set_xlabel("Month")
    ax_dual_left.set_ylabel("Yellow CO₂ (kg)", color="gold")
    ax_dual_right.set_ylabel("Green CO₂ (kg)",  color="green")
    ax_dual_left.set_xticks(x)
    ax_dual_left.set_xticklabels(month_labels)
    ax_dual_left.tick_params(axis="y", labelcolor="gold")
    ax_dual_right.tick_params(axis="y", labelcolor="green")
    ax_dual_left.grid(alpha=0.3)

    # One legend combining both lines
    lines = ax_dual_left.get_lines() + ax_dual_right.get_lines()
    labels = [l.get_label() for l in lines]
    ax_dual_left.legend(lines, labels, loc="upper left")

    plt.tight_layout()
    out_file = "monthly_co2.png"   # single PNG with both panels
    plt.savefig(out_file, dpi=150)
    print_and_log(f"\nSaved plot: {out_file}")
    plt.close('all')

    con.close() 
    print_and_log("Analysis complete.") 

if __name__ == "__main__":
    main()