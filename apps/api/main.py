from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import sqlite3, time, os
import yfinance as yf

app = FastAPI(title="Screener API")

# CORS για Next.js dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "data", "screener.db")

def db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

@app.get("/api/screener")
def screener(roic_min: float | None = None, limit: int = 200):
    q = """SELECT s.symbol, s.name, f.roic, f.ttm_eps, f.ebitda_ttm, f.book_ttm, s.shares_out
           FROM factor_snapshot f JOIN symbols s USING(symbol)
           WHERE 1=1"""
    params = []
    if roic_min is not None:
        q += " AND f.roic >= ?"; params.append(roic_min)
    q += " ORDER BY s.symbol LIMIT ?"; params.append(limit)
    cur = db().execute(q, params)
    out = [{
        "symbol": r[0], "name": r[1], "roic": r[2], "ttm_eps": r[3],
        "ebitda_ttm": r[4], "book_ttm": r[5], "shares_out": r[6]
    } for r in cur.fetchall()]
    return {"items": out}

@app.get("/api/price")
def price(symbols: str = Query(..., description="CSV e.g. AAPL,MSFT")):
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    conn = db()
    now = int(time.time())
    prices: dict[str, float] = {}

    # cache 60s
    for s in syms:
        row = conn.execute("SELECT price, ts FROM last_price WHERE symbol=?", (s,)).fetchone()
        if row and now - (row[1] or 0) < 60:
            prices[s] = row[0]

    missing = [s for s in syms if s not in prices]
    for s in missing:
        try:
            p = float(yf.Ticker(s).history(period="1d").tail(1)["Close"].iloc[0])
            prices[s] = p
            conn.execute(
                "INSERT OR REPLACE INTO last_price(symbol,price,ts) VALUES(?,?,?)",
                (s, p, now)
            )
        except Exception:
            # αγνόησε προσωρινά failures από provider
            pass

    conn.commit()
    return {"prices": prices, "asof": now}
