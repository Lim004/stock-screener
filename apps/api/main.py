# apps/api/main.py
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import sqlite3, time, io, csv, json, os
import yfinance as yf

app = FastAPI(title="Screener API")

# ---- CORS (Next.js dev on 3000/3001) ---------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", "http://127.0.0.1:3000",
        "http://localhost:3001", "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Paths -----------------------------------------------------------------
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "screener.db")
UNIVERSE_DIR = os.path.join(DATA_DIR, "universe")

# ---- DB helper --------------------------------------------------------------
def db():
    os.makedirs(DATA_DIR, exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    # Φρόντισε να υπάρχουν τα βασικά tables (safe-guard)
    con.executescript("""
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
    """)
    return con

# ---- Universe helpers -------------------------------------------------------
KNOWN_UNIVERSES = {
    "sp500": "S&P 500",
    "dow30": "Dow 30",
    "nasdaq100": "Nasdaq 100",
    "sp400": "S&P 400",
}

def load_universe_symbols(name: str) -> list[str]:
    fn = os.path.join(UNIVERSE_DIR, f"{name}.json")
    if not os.path.exists(fn):
        return []
    try:
        return [s.strip().upper() for s in json.load(open(fn))]
    except Exception:
        return []

@app.get("/api/universes")
def list_universes():
    out = []
    for key, label in KNOWN_UNIVERSES.items():
        syms = load_universe_symbols(key)
        out.append({"key": key, "label": label, "count": len(syms)})
    return {"universes": out}

@app.get("/api/universe")
def get_universe(name: str = Query(..., description="sp500|dow30|nasdaq100|sp400")):
    return {"name": name, "symbols": load_universe_symbols(name)}

# ---- Sectors ----------------------------------------------------------------
@app.get("/api/sectors")
def sectors():
    con = db()
    cur = con.execute("""
        SELECT DISTINCT sector
        FROM symbols
        WHERE sector IS NOT NULL AND sector <> ''
        ORDER BY sector
    """)
    return {"sectors": [r[0] for r in cur.fetchall()]}

# ---- Screener (JSON) --------------------------------------------------------
# Σημαντικό:
# - Αν περαστεί ?symbols=...: χρησιμοποιούμε CTE "syms" ώστε να φαίνονται σύμβολα
#   ακόμα κι αν ΔΕΝ υπάρχουν στη DB. Κάνουμε LEFT JOIN σε symbols/factor_snapshot.
# - Αν ΔΕΝ περαστεί symbols: δείχνουμε από τη DB (LEFT JOIN για να μη «κόβονται» όσα
#   δεν έχουν factors ακόμα).
@app.get("/api/screener")
def screener(
    roic_min: float | None = None,
    sector: str | None = None,
    pe_max: float | None = None,   # soft filter (ttm_eps > 0)
    symbols: str | None = None,    # CSV από universe/watchlist
    limit: int = 200,
    offset: int = 0,
):
    params: list = []

    if symbols:
        # CTE με explicit σύμβολα
        syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if not syms:
            return {"items": []}
        placeholders = ",".join(["(?)"] * len(syms))
        q = f"""
        WITH syms(symbol) AS ( VALUES {placeholders} )
        SELECT
          syms.symbol,
          COALESCE(s.name, syms.symbol) AS name,
          f.roic, f.ttm_eps, f.ebitda_ttm, f.book_ttm,
          s.shares_out, f.debt, f.cash, s.sector
        FROM syms
        LEFT JOIN symbols s ON s.symbol = syms.symbol
        LEFT JOIN factor_snapshot f ON f.symbol = syms.symbol
        WHERE 1=1
        """
        params.extend(syms)
    else:
        # Όλα από DB
        q = """
        SELECT
            s.symbol, s.name,
            f.roic, f.ttm_eps, f.ebitda_ttm, f.book_ttm,
            s.shares_out, f.debt, f.cash, s.sector
        FROM symbols s
        LEFT JOIN factor_snapshot f USING(symbol)
        WHERE 1=1
        """

    # Φίλτρα
    if roic_min is not None and roic_min > 0:
        q += " AND f.roic IS NOT NULL AND f.roic >= ?"; params.append(roic_min)
    # Αν είναι 0 ή None, δεν βάζουμε φίλτρο ώστε να εμφανίζονται και όσα έχουν NULL ROIC.
    if sector:
        q += " AND (s.sector LIKE ? OR s.sector IS NULL)"; params.append(f"%{sector}%")
    if pe_max is not None:
        q += " AND f.ttm_eps > 0"  # soft φίλτρο (P/E θα υπολογιστεί client-side από live price/eps)

    q += " ORDER BY 1 LIMIT ? OFFSET ?"; params.extend([limit, offset])

    con = db()
    rows = con.execute(q, params).fetchall()
    out = [{
        "symbol": r[0], "name": r[1], "roic": r[2], "ttm_eps": r[3],
        "ebitda_ttm": r[4], "book_ttm": r[5], "shares_out": r[6],
        "debt": r[7], "cash": r[8], "sector": r[9],
    } for r in rows]
    return {"items": out}

# ---- Screener (CSV export) --------------------------------------------------
@app.get("/api/screener.csv")
def screener_csv(
    roic_min: float | None = None,
    sector: str | None = None,
    pe_max: float | None = None,
    symbols: str | None = None,
    limit: int = 1000,
    offset: int = 0,
):
    params: list = []

    if symbols:
        syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if not syms:
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow([
                "symbol","name","sector","roic","ttm_eps","ebitda_ttm","book_ttm",
                "shares_out","debt","cash"
            ])
            buf.seek(0)
            headers = {"Content-Disposition": "attachment; filename=screener.csv"}
            return StreamingResponse(buf, media_type="text/csv", headers=headers)

        placeholders = ",".join(["(?)"] * len(syms))
        q = f"""
        WITH syms(symbol) AS ( VALUES {placeholders} )
        SELECT
          syms.symbol,
          COALESCE(s.name, syms.symbol) AS name,
          s.sector,
          f.roic, f.ttm_eps, f.ebitda_ttm, f.book_ttm,
          s.shares_out, f.debt, f.cash
        FROM syms
        LEFT JOIN symbols s ON s.symbol = syms.symbol
        LEFT JOIN factor_snapshot f ON f.symbol = syms.symbol
        WHERE 1=1
        """
        params.extend(syms)
    else:
        q = """
        SELECT
            s.symbol, s.name, s.sector,
            f.roic, f.ttm_eps, f.ebitda_ttm, f.book_ttm,
            s.shares_out, f.debt, f.cash
        FROM symbols s
        LEFT JOIN factor_snapshot f USING(symbol)
        WHERE 1=1
        """

    if roic_min is not None:
        q += " AND f.roic IS NOT NULL AND f.roic >= ?"; params.append(roic_min)
    if sector:
        q += " AND (s.sector LIKE ? OR s.sector IS NULL)"; params.append(f"%{sector}%")
    if pe_max is not None:
        q += " AND f.ttm_eps > 0"

    q += " ORDER BY 1 LIMIT ? OFFSET ?"; params.extend([limit, offset])

    con = db()
    cur = con.execute(q, params)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "symbol","name","sector","roic","ttm_eps","ebitda_ttm","book_ttm",
        "shares_out","debt","cash"
    ])
    for r in cur.fetchall():
        writer.writerow(r)

    buf.seek(0)
    headers = {"Content-Disposition": "attachment; filename=screener.csv"}
    return StreamingResponse(buf, media_type="text/csv", headers=headers)

# ---- LIVE Prices + LIVE P/E (price / trailing EPS) --------------------------
@app.get("/api/price")
def price(symbols: str = Query(..., description="CSV e.g. AAPL,MSFT")):
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    con = db()
    now = int(time.time())

    # 1) Price cache 60s
    prices: dict[str, float] = {}
    for s in syms:
        row = con.execute("SELECT price, ts FROM last_price WHERE symbol=?", (s,)).fetchone()
        if row and now - (row[1] or 0) < 60:
            prices[s] = float(row[0])

    # 2) Trailing EPS cache 6 ώρες
    eps_cache: dict[str, float] = {}
    for s in syms:
        row = con.execute("SELECT eps, ts FROM trailing_eps_cache WHERE symbol=?", (s,)).fetchone()
        if row and now - (row[1] or 0) < 6 * 3600:
            eps_cache[s] = float(row[0])

    # 3) Fetch όσα λείπουν από yfinance
    need_price = [s for s in syms if s not in prices]
    need_eps   = [s for s in syms if s not in eps_cache]

    for s in set(need_price + need_eps):
        try:
            t = yf.Ticker(s)

            # --- current price (fast_info ή history) ---
            if s in need_price:
                p = None
                try:
                    fi = getattr(t, "fast_info", None)
                    if isinstance(fi, dict):
                        p = fi.get("last_price") or fi.get("last_close")
                    else:
                        p = getattr(fi, "last_price", None) or getattr(fi, "last_close", None)
                except Exception:
                    p = None
                if p is None:
                    hist = t.history(period="1d")
                    if not hist.empty:
                        p = float(hist["Close"].tail(1).iloc[0])
                if isinstance(p, (int, float)):
                    prices[s] = float(p)
                    con.execute(
                        "INSERT OR REPLACE INTO last_price(symbol,price,ts) VALUES(?,?,?)",
                        (s, prices[s], now)
                    )

            # --- trailing EPS ---
            if s in need_eps:
                eps = None
                try:
                    info = t.info or {}
                    val = info.get("trailingEps", None)
                    if isinstance(val, (int, float)):
                        eps = float(val)
                except Exception:
                    eps = None
                if eps is not None:
                    eps_cache[s] = eps
                    con.execute(
                        "INSERT OR REPLACE INTO trailing_eps_cache(symbol,eps,ts) VALUES(?,?,?)",
                        (s, eps, now)
                    )

        except Exception:
            # Αν αποτύχει κάποιο ticker, το αγνοούμε σιωπηλά
            pass

    con.commit()

    # 4) Υπολόγισε P/E
    pe_live: dict[str, float] = {}
    for s in syms:
        p = prices.get(s)
        e = eps_cache.get(s)
        if isinstance(p, (int, float)) and isinstance(e, (int, float)) and e != 0:
            pe_live[s] = float(p) / float(e)

    return {
        "prices": prices,
        "trailing_eps": eps_cache,
        "pe_live": pe_live,
        "asof": now,
    }
