import sqlite3, os

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "data", "screener.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "data", "init.sql")

os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)

con = sqlite3.connect(DB_PATH)
with open(SCHEMA_PATH, "r") as f:
    con.executescript(f.read())

symbols = [
    ("AAPL","Apple Inc.","NASDAQ","Technology","Consumer Electronics", 15500000000),
    ("MSFT","Microsoft Corp.","NASDAQ","Technology","Software—Infrastructure", 7450000000),
    ("NVDA","NVIDIA Corp.","NASDAQ","Technology","Semiconductors", 2460000000),
]
con.executemany("INSERT OR REPLACE INTO symbols VALUES (?,?,?,?,?,?)", symbols)

snap = [
    ("AAPL","2025-09-30", 6.4, 140_000_000_000, 75_000_000_000, 200_000_000_000, 110_000_000_000, 0.55),
    ("MSFT","2025-09-30",11.2, 130_000_000_000,175_000_000_000, 220_000_000_000, 100_000_000_000, 0.45),
    ("NVDA","2025-09-30",17.0,  80_000_000_000, 60_000_000_000,  70_000_000_000,  60_000_000_000, 0.86),
]
con.executemany(
    """INSERT OR REPLACE INTO factor_snapshot
       (symbol,asof,ttm_eps,ebitda_ttm,book_ttm,invested_capital_ttm,nopat_ttm,roic)
       VALUES (?,?,?,?,?,?,?,?)""",
    snap,
)

con.commit()
con.close()
print("DB ready at", DB_PATH)
