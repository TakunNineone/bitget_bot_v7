from __future__ import annotations
import os
import sqlite3
from typing import List, Tuple

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS raw_ws_ticker (
  instType TEXT NOT NULL,
  instId   TEXT NOT NULL,
  ts       INTEGER NOT NULL,
  recv_ts  INTEGER NOT NULL,
  payload  TEXT NOT NULL,
  PRIMARY KEY (instType, instId, ts)
);

CREATE TABLE IF NOT EXISTS raw_ws_trade (
  instType TEXT NOT NULL,
  instId   TEXT NOT NULL,
  tradeId  TEXT NOT NULL,
  ts       INTEGER NOT NULL,
  recv_ts  INTEGER NOT NULL,
  price    TEXT NOT NULL,
  size     TEXT NOT NULL,
  side     TEXT NOT NULL,
  payload  TEXT NOT NULL,
  PRIMARY KEY (instType, instId, tradeId)
);

CREATE TABLE IF NOT EXISTS raw_ws_books15 (
  instType TEXT NOT NULL,
  instId   TEXT NOT NULL,
  ts       INTEGER NOT NULL,
  recv_ts  INTEGER NOT NULL,
  bids_json TEXT NOT NULL,
  asks_json TEXT NOT NULL,
  checksum  INTEGER,
  payload   TEXT NOT NULL,
  PRIMARY KEY (instType, instId, ts)
);

CREATE TABLE IF NOT EXISTS raw_ws_candle (
  instType TEXT NOT NULL,
  instId   TEXT NOT NULL,
  interval TEXT NOT NULL,
  candle_ts INTEGER NOT NULL,
  recv_ts INTEGER NOT NULL,
  o TEXT NOT NULL,
  h TEXT NOT NULL,
  l TEXT NOT NULL,
  c TEXT NOT NULL,
  v TEXT NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (instType, instId, interval, candle_ts)
);

CREATE TABLE IF NOT EXISTS raw_rest_symbol_price (
  productType TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts INTEGER NOT NULL,
  recv_ts INTEGER NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (productType, symbol, ts)
);

CREATE TABLE IF NOT EXISTS raw_rest_open_interest (
  productType TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts INTEGER NOT NULL,
  recv_ts INTEGER NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (productType, symbol, ts)
);

CREATE TABLE IF NOT EXISTS raw_rest_current_fund_rate (
  productType TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts INTEGER NOT NULL,
  recv_ts INTEGER NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (productType, symbol, ts)
);

CREATE TABLE IF NOT EXISTS raw_rest_funding_time (
  productType TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts INTEGER NOT NULL,
  recv_ts INTEGER NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (productType, symbol, ts)
);

-- Агрегаты (1s)
CREATE TABLE IF NOT EXISTS agg_1s_micro (
  instId TEXT NOT NULL,
  sec_ts INTEGER NOT NULL, -- unix seconds (ms rounded down)
  mid REAL,
  spread REAL,
  spread_bps REAL,
  microprice REAL,
  imbalance15 REAL,
  imbalance5 REAL,
  top_bid_qty REAL,
  top_ask_qty REAL,
  top5_bid_qty REAL,
  top5_ask_qty REAL,
  depth_ratio5 REAL,
  trade_count INTEGER,
  buy_vol REAL,
  sell_vol REAL,
  delta_vol REAL,
  vwap REAL,
  ret_1s REAL,
  PRIMARY KEY (instId, sec_ts)
);

-- Агрегаты (1m)
CREATE TABLE IF NOT EXISTS agg_1m_context (
  instId TEXT NOT NULL,
  min_ts INTEGER NOT NULL,
  mark_last REAL,
  index_last REAL,
  oi REAL,
  doi REAL,
  funding REAL,
  PRIMARY KEY (instId, min_ts)
);

-- Агрегаты (15m trend/regime)
CREATE TABLE IF NOT EXISTS agg_15m_trend (
  instId TEXT NOT NULL,
  candle_ts INTEGER NOT NULL,
  close REAL,
  atr14 REAL,
  ema20 REAL,
  ema50 REAL,
  ema_diff REAL,
  ema_slope REAL,
  trend_dir INTEGER,
  PRIMARY KEY (instId, candle_ts)
);

CREATE INDEX IF NOT EXISTS idx_agg_15m_trend_inst_ts
  ON agg_15m_trend(instId, candle_ts);

CREATE TABLE IF NOT EXISTS agg_trend (
  instId TEXT NOT NULL,
  interval TEXT NOT NULL,
  candle_ts INTEGER NOT NULL,
  close REAL,
  atr14 REAL,
  ema20 REAL,
  ema50 REAL,
  ema_diff REAL,
  ema_slope REAL,
  trend_dir INTEGER,
  PRIMARY KEY (instId, interval, candle_ts)
);

CREATE INDEX IF NOT EXISTS idx_agg_trend_inst_interval_ts
  ON agg_trend(instId, interval, candle_ts);
"""

class SQLiteStore:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.executescript(SCHEMA_SQL)
        self._ensure_columns()

    def _ensure_columns(self) -> None:
        needed = {
            "agg_1s_micro": {
                "imbalance5": "REAL",
                "top5_bid_qty": "REAL",
                "top5_ask_qty": "REAL",
                "depth_ratio5": "REAL",
            },
        }
        for table, cols in needed.items():
            existing = {row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")}
            for col, col_type in cols.items():
                if col not in existing:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def insert_many(self, sql: str, rows: List[Tuple]) -> None:
        if not rows:
            return
        cur = self.conn.cursor()
        try:
            cur.executemany(sql, rows)
        finally:
            cur.close()

    # Insert helpers
    def put_raw_ws_ticker(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR IGNORE INTO raw_ws_ticker(instType,instId,ts,recv_ts,payload) VALUES (?,?,?,?,?)",
            rows,
        )

    def put_raw_ws_trade(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR IGNORE INTO raw_ws_trade(instType,instId,tradeId,ts,recv_ts,price,size,side,payload) VALUES (?,?,?,?,?,?,?,?,?)",
            rows,
        )

    def put_raw_ws_books15(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR IGNORE INTO raw_ws_books15(instType,instId,ts,recv_ts,bids_json,asks_json,checksum,payload) VALUES (?,?,?,?,?,?,?,?)",
            rows,
        )

    def put_raw_ws_candle(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR IGNORE INTO raw_ws_candle(instType,instId,interval,candle_ts,recv_ts,o,h,l,c,v,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )

    def put_raw_rest_symbol_price(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR IGNORE INTO raw_rest_symbol_price(productType,symbol,ts,recv_ts,payload) VALUES (?,?,?,?,?)",
            rows,
        )

    def put_raw_rest_open_interest(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR IGNORE INTO raw_rest_open_interest(productType,symbol,ts,recv_ts,payload) VALUES (?,?,?,?,?)",
            rows,
        )

    def put_raw_rest_current_fund_rate(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR IGNORE INTO raw_rest_current_fund_rate(productType,symbol,ts,recv_ts,payload) VALUES (?,?,?,?,?)",
            rows,
        )

    def put_raw_rest_funding_time(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR IGNORE INTO raw_rest_funding_time(productType,symbol,ts,recv_ts,payload) VALUES (?,?,?,?,?)",
            rows,
        )

    def put_agg_1s_micro(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR REPLACE INTO agg_1s_micro(instId,sec_ts,mid,spread,spread_bps,microprice,imbalance15,imbalance5,top_bid_qty,top_ask_qty,top5_bid_qty,top5_ask_qty,depth_ratio5,trade_count,buy_vol,sell_vol,delta_vol,vwap,ret_1s) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )

    def put_agg_1m_context(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR REPLACE INTO agg_1m_context(instId,min_ts,mark_last,index_last,oi,doi,funding) VALUES (?,?,?,?,?,?,?)",
            rows,
        )

    def put_agg_15m_trend(self, rows: List[Tuple]) -> None:
        self.insert_many(
            "INSERT OR REPLACE INTO agg_15m_trend(instId,candle_ts,close,atr14,ema20,ema50,ema_diff,ema_slope,trend_dir) VALUES (?,?,?,?,?,?,?,?,?)",
            rows,
        )
