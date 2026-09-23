"""Plot avg speed by day-of-week x hour from the mart_speed_by_hour_dow dbt model.

Run `dbt run --select +mart_speed_by_hour_dow` first so the table exists in
nyc_taxi.duckdb, then:

    python plot_speed_by_hour_dow.py
"""

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import seaborn as sns

DB_PATH = Path(__file__).parent / "../nyc_taxi.duckdb"
OUTPUT_PATH = Path(__file__).parent / "speed_by_hour_dow.png"

# DuckDB's dayofweek() is 0=Sunday..6=Saturday; reorder Mon-Sun for a
# commute-week-first reading of the heatmap.
DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(
        "select pickup_day_name, pickup_hour, avg_speed_mph from mart_speed_by_hour_dow"
    ).df()
    con.close()

    pivot = df.pivot(index="pickup_day_name", columns="pickup_hour", values="avg_speed_mph")
    pivot = pivot.reindex(DAY_ORDER)

    sns.set_theme(style="white")
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.heatmap(
        pivot,
        cmap="RdYlGn",
        annot=True,
        fmt=".1f",
        linewidths=0.5,
        cbar_kws={"label": "Avg Speed (mph)"},
        ax=ax,
    )

    ax.set_title("NYC Yellow Taxi Avg Speed by Day of Week and Hour (2025)")
    ax.set_xlabel("Pickup Hour (0-23)")
    ax.set_ylabel("")

    fig.tight_layout()
    fig.savefig(OUTPUT_PATH, dpi=150)
    print(f"Saved plot to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
