# US Stock Screener

A simple but powerful **US stock screener** built for educational / research use (πτυχιακή), focusing on:

- **Quality metrics** like ROIC
- **Live P/E** (based on real-time prices + trailing EPS from Yahoo Finance)
- Basic **valuation ratios** (EV/EBITDA, P/B, D/E proxy, Market Cap)
- Custom **universes** (S&P 500, Dow 30, Nasdaq 100, S&P 400) and **watchlist**

---

## Tech Stack

**Backend**

- Python 3
- FastAPI
- SQLite
- `yfinance` (Yahoo Finance data)
- Uvicorn (ASGI server)

**Frontend**

- Next.js (App Router, `app/` directory)
- TypeScript + React
- Tailwind CSS

---
git clone https://github.com/<your-username>/stock-screener.git
cd stock-screener

cd apps/api
python -m venv .venv
source .venv/bin/activate        # On Windows: .venv\Scripts\activate
pip install -r requirements.txt  # If requirements.txt exists
python populate_db.py

cd apps/web
npm install        # or pnpm install / yarn
// const API = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8000";
npm run dev
