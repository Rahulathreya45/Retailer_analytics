from pathlib import Path
import duckdb
from deltalake import DeltaTable
from config.settings import GOLD,DUCK_DB

DELTA_BASE = Path(GOLD)
DUCKDB_DIR = Path(DUCK_DB)
DUCKDB_FILE = DUCKDB_DIR / "lucky_draw_gold.duckdb"

GOLD_TABLES = [
    "fact_lucky_draw_entries",
    "dim_date",
    "dim_retailer",
    "agg_retailer_leaderboard",
]

DUCKDB_DIR.mkdir(
    parents=True,
    exist_ok=True
)

con = duckdb.connect(str(DUCKDB_FILE))

for table in GOLD_TABLES:
    delta_path = str(DELTA_BASE / table)
    dt = DeltaTable(delta_path)
    arrow_table = dt.to_pyarrow_table()
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(
        f"""
        CREATE TABLE {table} AS
        SELECT *
        FROM arrow_table
        """
    )
    print(f"Exported {table} to DuckDB")
con.close()
print(f"DuckDB export completed: {DUCKDB_FILE}")