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
  roic REAL
);

CREATE TABLE IF NOT EXISTS last_price(
  symbol TEXT PRIMARY KEY,
  price REAL,
  ts INTEGER
);
