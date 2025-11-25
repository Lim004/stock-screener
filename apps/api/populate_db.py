# apps/api/populate_db.py
import sqlite3, os, json

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "screener.db")
UNIVERSE_DIR = os.path.join(DATA_DIR, "universe")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(UNIVERSE_DIR, exist_ok=True)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS symbols(
  symbol TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  exchange TEXT,
  sector TEXT,
  industry TEXT,
  shares_out REAL
);

CREATE TABLE IF NOT EXISTS factor_snapshot(
  symbol TEXT PRIMARY KEY,
  asof TEXT,
  ttm_eps REAL,
  ebitda_ttm REAL,
  book_ttm REAL,
  invested_capital_ttm REAL,
  nopat_ttm REAL,
  roic REAL,
  debt REAL,
  cash REAL
);

CREATE TABLE IF NOT EXISTS last_price(
  symbol TEXT PRIMARY KEY,
  price REAL,
  ts INTEGER
);

CREATE TABLE IF NOT EXISTS trailing_eps_cache(
  symbol TEXT PRIMARY KEY,
  eps REAL,
  ts  INTEGER
);
"""

def ensure_schema(con: sqlite3.Connection):
    con.executescript(SCHEMA_SQL)
    # Λίγες βελτιστοποιήσεις
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")

def upsert_seed(con: sqlite3.Connection):
    symbols = [
        ("AAPL","Apple Inc.","NASDAQ","Technology","Consumer Electronics", 15_500_000_000),
        ("MSFT","Microsoft Corp.","NASDAQ","Technology","Software—Infrastructure", 7_450_000_000),
        ("NVDA","NVIDIA Corp.","NASDAQ","Technology","Semiconductors", 2_460_000_000),
    ]
    con.executemany("""
        INSERT INTO symbols(symbol,name,exchange,sector,industry,shares_out)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
          name=excluded.name,
          exchange=excluded.exchange,
          sector=excluded.sector,
          industry=excluded.industry,
          shares_out=excluded.shares_out
    """, symbols)

    snap = [
        ("AAPL","2025-09-30", 6.4, 140_000_000_000, 75_000_000_000,
         200_000_000_000, 110_000_000_000, 0.55, 120_000_000_000, 70_000_000_000),
        ("MSFT","2025-09-30",11.2, 130_000_000_000,175_000_000_000,
         220_000_000_000, 100_000_000_000, 0.45, 100_000_000_000, 90_000_000_000),
        ("NVDA","2025-09-30",17.0,  80_000_000_000, 60_000_000_000,
          70_000_000_000,  60_000_000_000, 0.86,  60_000_000_000, 40_000_000_000),
    ]
    # UPSERT στο factor_snapshot
    for r in snap:
        con.execute("""
            INSERT INTO factor_snapshot
            (symbol,asof,ttm_eps,ebitda_ttm,book_ttm,invested_capital_ttm,nopat_ttm,roic,debt,cash)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
              asof=excluded.asof,
              ttm_eps=excluded.ttm_eps,
              ebitda_ttm=excluded.ebitda_ttm,
              book_ttm=excluded.book_ttm,
              invested_capital_ttm=excluded.invested_capital_ttm,
              nopat_ttm=excluded.nopat_ttm,
              roic=excluded.roic,
              debt=excluded.debt,
              cash=excluded.cash
        """, r)

def write_universes():
    # Μικρά subsets για δοκιμή. Η API σου πλέον μπορεί να δουλέψει ΚΑΙ αν κάποια δεν είναι στη DB
    sp500 = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","BRK-B","LLY","UNH","XOM"]
    dow30 = ["AAPL","MSFT","JPM","V","PG","KO","MCD","DIS","IBM","CAT"]
    nasdaq100 = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","AVGO","COST","ADBE","PEP"]
    sp400 = ["BLDR","CROX","ENPH","DKS","TXT","MTZ","UAL","DECK","ALGN","NTRS"]

    universes = {
        "sp500.json": sp500,
        "dow30.json": dow30,
        "nasdaq100.json": nasdaq100,
        "sp400.json": sp400,
    }
    for fname, syms in universes.items():
        path = os.path.join(UNIVERSE_DIR, fname)
        # Δεν διαγράφω υπάρχοντα—αν υπάρχει, το αντικαθιστώ για να είναι deterministic
        with open(path, "w") as f:
            json.dump(syms, f, indent=2)

def main():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    ensure_schema(con)
    upsert_seed(con)
    con.commit()
    con.close()

    write_universes()
    print("DB ready at", DB_PATH)
    print("Universes written under", UNIVERSE_DIR)

if __name__ == "__main__":
    main()
