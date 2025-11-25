# apps/api/main.py
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import sqlite3, time, io, csv, json, os
import yfinance as yf
from typing import List

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
def db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
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

def load_universe_symbols(name: str) -> List[str]:
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
@app.get("/api/screener")
def screener(
    roic_min: float | None = None,
    sector: str | None = None,
    pe_max: float | None = None,   # soft: απαιτεί θετικό EPS, το P/E βγαίνει client-side
    symbols: str | None = None,    # CSV από universe/watchlist
    limit: int = 200,
    offset: int = 0,
):
    params: List = []

    if symbols:
        # CTE για explicit σύμβολα (ώστε να φαίνονται και όσα δεν υπάρχουν στη DB)
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
        q += " AND f.roic IS NOT NULL AND f.roic >= ?"
        params.append(roic_min)

    if sector:
        # ΣΗΜΑΝΤΙΚΟ: όταν έχουμε CTE (symbols), ΜΗΝ κόβεις όσα δεν έχουν sector στη DB
        q += " AND (s.sector LIKE ? OR s.sector IS NULL)"
        params.append(f"%{sector}%")

    if pe_max is not None:
        q += " AND (f.ttm_eps > 0 OR f.ttm_eps IS NULL)"

    q += " ORDER BY 1 LIMIT ? OFFSET ?"
    params.extend([limit, offset])

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
    params: List = []

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
        q += " AND f.roic IS NOT NULL AND f.roic >= ?"
        params.append(roic_min)
    if sector:
        # ίδιο rule με το JSON endpoint
        q += " AND (s.sector LIKE ? OR s.sector IS NULL)"
        params.append(f"%{sector}%")
    if pe_max is not None:
        q += " AND f.ttm_eps > 0"

    q += " ORDER BY 1 LIMIT ? OFFSET ?"
    params.extend([limit, offset])

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

    need_price = [s for s in syms if s not in prices]
    need_eps   = [s for s in syms if s not in eps_cache]

    fundamentals: dict[str, dict] = {}

    for s in set(need_price + need_eps):
        try:
            t = yf.Ticker(s)

            # --- current price ---
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
            info = {}
            try:
                info = t.info or {}
            except Exception:
                info = {}

            if s in need_eps:
                eps = None
                try:
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

            # --- fundamentals (best-effort) για σύμβολα εκτός DB ---
            try:
                shares_out = info.get("sharesOutstanding")
                ebitda_ttm = info.get("ebitda")
                total_equity = info.get("totalStockholderEquity")
                total_debt = info.get("totalDebt")
                total_cash = info.get("totalCash")
                sector = info.get("sector")
                name = info.get("longName") or info.get("shortName") or s

                f = {}
                if isinstance(shares_out, (int, float)): f["shares_out"] = float(shares_out)
                if isinstance(ebitda_ttm, (int, float)): f["ebitda_ttm"] = float(ebitda_ttm)
                if isinstance(total_equity, (int, float)): f["book_ttm"] = float(total_equity)
                if isinstance(total_debt, (int, float)): f["debt"] = float(total_debt)
                if isinstance(total_cash, (int, float)): f["cash"] = float(total_cash)
                if sector: f["sector"] = sector
                if name: f["name"] = name
                if f:
                    fundamentals[s] = f
            except Exception:
                pass

        except Exception:
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
        "fundamentals": fundamentals,  # metadata για merge στο UI
        "asof": now,
    }

# --- NEW: Fill/refresh fundamentals στη DB -----------------------------------
@app.get("/api/fundamentals")
def fundamentals(symbols: str = Query(..., description="CSV e.g. AAPL,MSFT")):
    """
    Γεμίζει/ενημερώνει για τα δοσμένα symbols τα:
    - symbols: name, sector, shares_out
    - factor_snapshot: debt, cash, book_ttm (equity), ebitda_ttm
    """
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    con = db()
    now = int(time.time())

    out: dict[str, dict] = {}

    def _to_float(x):
        try:
            return None if x is None else float(x)
        except Exception:
            return None

    for s in syms:
        info_name = None
        info_sector = None
        shares_out = None
        debt = None
        cash = None
        book_ttm = None
        ebitda_ttm = None

        try:
            t = yf.Ticker(s)

            # --- info (name/sector/shares) ---
            try:
                info = t.info or {}
                info_name = info.get("shortName") or info.get("longName") or s
                info_sector = info.get("sector") or None
                shares_out = _to_float(info.get("sharesOutstanding"))
            except Exception:
                pass

            # --- balance sheet (debt/cash/equity) ---
            try:
                bs = t.balance_sheet
                if bs is not None and not bs.empty:
                    col = bs.columns[0]
                    debt = _to_float(
                        bs.get("Total Debt", {}).get(col)
                        if "Total Debt" in bs.index
                        else (
                            (bs.loc["Long Term Debt", col] if "Long Term Debt" in bs.index else 0)
                            + (bs.loc.get("Short Long Term Debt", col) if "Short Long Term Debt" in bs.index else 0)
                        )
                    )
                    if "Cash And Cash Equivalents" in bs.index:
                        cash = _to_float(bs.loc["Cash And Cash Equivalents", col])
                    elif "Cash" in bs.index:
                        cash = _to_float(bs.loc["Cash", col])
                    if "Total Stockholder Equity" in bs.index:
                        book_ttm = _to_float(bs.loc["Total Stockholder Equity", col])
                    elif "Stockholders Equity" in bs.index:
                        book_ttm = _to_float(bs.loc["Stockholders Equity", col])
            except Exception:
                pass

            # --- income statement (EBITDA best-effort) ---
            try:
                inc = getattr(t, "income_stmt", None)
                if inc is not None and hasattr(inc, "empty") and not inc.empty:
                    col = inc.columns[0]
                    if "EBITDA" in inc.index:
                        ebitda_ttm = _to_float(inc.loc["EBITDA", col])
                else:
                    fin = getattr(t, "financials", None)
                    if fin is not None and hasattr(fin, "empty") and not fin.empty:
                        col = fin.columns[0]
                        if "EBITDA" in fin.index:
                            ebitda_ttm = _to_float(fin.loc["EBITDA", col])
            except Exception:
                pass

        except Exception:
            pass

        # Upsert σε symbols
        try:
            if info_name or info_sector or shares_out is not None:
                prev = con.execute(
                    "SELECT name, sector, shares_out FROM symbols WHERE symbol=?",
                    (s,)
                ).fetchone()
                name_final = info_name or (prev[0] if prev else s)
                sector_final = info_sector if info_sector is not None else (prev[1] if prev else None)
                shares_final = shares_out if shares_out is not None else (prev[2] if prev else None)

                con.execute("""
                    INSERT INTO symbols(symbol, name, sector, shares_out)
                    VALUES(?,?,?,?)
                    ON CONFLICT(symbol) DO UPDATE SET
                      name=excluded.name,
                      sector=COALESCE(excluded.sector, symbols.sector),
                      shares_out=COALESCE(excluded.shares_out, symbols.shares_out)
                """, (s, name_final, sector_final, shares_final))
        except Exception:
            pass

        # Upsert σε factor_snapshot
        try:
            cur = con.execute("SELECT symbol FROM factor_snapshot WHERE symbol=?", (s,)).fetchone()
            if cur:
                if ebitda_ttm is not None:
                    con.execute("UPDATE factor_snapshot SET ebitda_ttm=? WHERE symbol=?", (ebitda_ttm, s))
                if book_ttm is not None:
                    con.execute("UPDATE factor_snapshot SET book_ttm=? WHERE symbol=?", (book_ttm, s))
                if debt is not None:
                    con.execute("UPDATE factor_snapshot SET debt=? WHERE symbol=?", (debt, s))
                if cash is not None:
                    con.execute("UPDATE factor_snapshot SET cash=? WHERE symbol=?", (cash, s))
            else:
                con.execute("""
                    INSERT INTO factor_snapshot(symbol, asof, ttm_eps, ebitda_ttm, book_ttm, invested_capital_ttm,
                                                nopat_ttm, roic, debt, cash)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                """, (s, time.strftime("%Y-%m-%d"), None, ebitda_ttm, book_ttm, None,
                      None, None, debt, cash))
        except Exception:
            pass

        out[s] = {
            "name": info_name, "sector": info_sector, "shares_out": shares_out,
            "debt": debt, "cash": cash, "book_ttm": book_ttm, "ebitda_ttm": ebitda_ttm
        }

    con.commit()
    return {"fundamentals": out, "asof": now}
