from __future__ import annotations

import argparse
import sqlite3
import time
from typing import Optional, List, Dict, Any, Tuple

from .features import FeatureService


# ---------- helpers ----------
def col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == col for r in rows)


def add_col(conn: sqlite3.Connection, table: str, col: str, ddl_type: str) -> None:
    if not col_exists(conn, table, col):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl_type}")


def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def safe_int(x) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def pick_first_existing_col(conn: sqlite3.Connection, table: str, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if col_exists(conn, table, c):
            return c
    return None


def fetch_mid_at(conn: sqlite3.Connection, instId: str, ts_ms: int) -> Optional[float]:
    # Take latest mid at or before ts_ms
    r = conn.execute(
        """
        SELECT mid
        FROM agg_1s_micro
        WHERE instId=? AND sec_ts<=?
        ORDER BY sec_ts DESC
        LIMIT 1
        """,
        (instId, ts_ms),
    ).fetchone()
    if not r:
        return None
    return safe_float(r[0])


def fetch_mid_window(conn: sqlite3.Connection, instId: str, ts_from_ms: int, ts_to_ms: int) -> List[Tuple[int, float]]:
    rows = conn.execute(
        """
        SELECT sec_ts, mid
        FROM agg_1s_micro
        WHERE instId=? AND sec_ts>=? AND sec_ts<=? AND mid IS NOT NULL
        ORDER BY sec_ts ASC
        """,
        (instId, ts_from_ms, ts_to_ms),
    ).fetchall()
    out: List[Tuple[int, float]] = []
    for r in rows:
        t = safe_int(r[0])
        m = safe_float(r[1])
        if t is None or m is None:
            continue
        out.append((t, m))
    return out


def fetch_15m_trend_at(conn: sqlite3.Connection, instId: str, ts_ms: int) -> Dict[str, Any]:
    """Fetch latest agg_15m_trend row with candle_ts<=ts_ms."""
    r = conn.execute(
        """
        SELECT candle_ts, close, atr14, ema20, ema50, ema_diff, ema_slope, trend_dir
        FROM agg_15m_trend
        WHERE instId=? AND candle_ts<=?
        ORDER BY candle_ts DESC
        LIMIT 1
        """,
        (instId, ts_ms),
    ).fetchone()
    if not r:
        return {
            "candle_ts": None,
            "close": None,
            "atr14": None,
            "ema20": None,
            "ema50": None,
            "ema_diff": None,
            "ema_slope": None,
            "trend_dir": None,
        }
    return {
        "candle_ts": safe_int(r[0]),
        "close": safe_float(r[1]),
        "atr14": safe_float(r[2]),
        "ema20": safe_float(r[3]),
        "ema50": safe_float(r[4]),
        "ema_diff": safe_float(r[5]),
        "ema_slope": safe_float(r[6]),
        "trend_dir": safe_int(r[7]),
    }


def pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    # (a - b) / b
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return (a - b) / b


# ---------- schema / columns ----------
def ensure_enrich_columns(conn: sqlite3.Connection, table: str) -> None:
    # entry snapshot
    for col, typ in [
        ("e_atr_pct", "REAL"),
        ("e_atr14", "REAL"),
        ("e_close15m", "REAL"),
        ("e_candle15m_ts", "INTEGER"),
        ("e_dz", "REAL"),
        ("e_imb", "REAL"),
        ("e_flow60", "INTEGER"),
        ("e_spike", "REAL"),
        ("e_range60", "REAL"),
        ("e_std60", "REAL"),
        ("e_spread_bps", "REAL"),
        ("e_mark_z", "REAL"),
        ("e_oi", "REAL"),
        ("e_doi", "REAL"),
        ("e_funding", "REAL"),
        ("e_ema_fast_15m", "REAL"),
        ("e_ema_slow_15m", "REAL"),
        ("e_ema_diff_15m", "REAL"),
        ("e_ema_slope_15m", "REAL"),
        ("e_trend_dir_15m", "INTEGER"),
    ]:
        add_col(conn, table, col, typ)

    # exit snapshot
    for col, typ in [
        ("x_atr_pct", "REAL"),
        ("x_atr14", "REAL"),
        ("x_close15m", "REAL"),
        ("x_candle15m_ts", "INTEGER"),
        ("x_dz", "REAL"),
        ("x_imb", "REAL"),
        ("x_flow60", "INTEGER"),
        ("x_spike", "REAL"),
        ("x_range60", "REAL"),
        ("x_std60", "REAL"),
        ("x_spread_bps", "REAL"),
        ("x_mark_z", "REAL"),
        ("x_oi", "REAL"),
        ("x_doi", "REAL"),
        ("x_funding", "REAL"),
        ("x_ema_fast_15m", "REAL"),
        ("x_ema_slow_15m", "REAL"),
        ("x_ema_diff_15m", "REAL"),
        ("x_ema_slope_15m", "REAL"),
        ("x_trend_dir_15m", "INTEGER"),
    ]:
        add_col(conn, table, col, typ)

    # NEW: MFE/MAE + timing
    for col, typ in [
        ("mfe_pct", "REAL"),
        ("mae_pct", "REAL"),
        ("time_to_mfe_s", "INTEGER"),
        ("time_to_mae_s", "INTEGER"),
    ]:
        add_col(conn, table, col, typ)

    # NEW: returns before entry
    for col, typ in [
        ("ret_1m_before", "REAL"),
        ("ret_5m_before", "REAL"),
        ("ret_15m_before", "REAL"),
    ]:
        add_col(conn, table, col, typ)

    # NEW: position in last 15m range
    for col, typ in [
        ("dist_from_15m_high", "REAL"),
        ("dist_from_15m_low", "REAL"),
    ]:
        add_col(conn, table, col, typ)

    # NEW: micro before entry
    for col, typ in [
        ("dz_mean_5s_before", "REAL"),
        ("dz_mean_15s_before", "REAL"),
        ("imb_mean_5s_before", "REAL"),
        ("imb_mean_15s_before", "REAL"),
        ("flow_accel_30s", "REAL"),
        ("x_flow_accel_30s", "REAL"),
    ]:
        add_col(conn, table, col, typ)

    # NEW: holding stats
    for col, typ in [
        ("duration_sec", "INTEGER"),
        ("bars_1m_held", "INTEGER"),
        ("bars_5s_held", "INTEGER"),
    ]:
        add_col(conn, table, col, typ)

    # meta
    add_col(conn, table, "enriched_at", "INTEGER")


def snapshot_from_feature(f) -> Dict[str, Any]:
    px = getattr(f, "mid", None)
    if px is None:
        px = getattr(f, "vwap", None)
    atr15m = getattr(f, "atr15m", None)
    atr_pct = None
    if px is not None and atr15m is not None:
        try:
            atr_pct = float(atr15m) / float(px)
        except Exception:
            atr_pct = None

    return {
        "atr_pct": safe_float(atr_pct),
        "dz": safe_float(getattr(f, "delta_rate_z", None)),
        "imb": safe_float(getattr(f, "imb_shift", None)),
        "flow60": int(getattr(f, "flow_60s", 0) or 0),
        "spike": safe_float(getattr(f, "flow_spike", None)),
        "range60": safe_float(getattr(f, "range_60s_pct", None)),
        "std60": safe_float(getattr(f, "ret_std_60s", None)),
        "spread_bps": safe_float(getattr(f, "spread_bps", None)),
        "mark_z": safe_float(getattr(f, "mark_dev_z", None)),
        "oi": safe_float(getattr(f, "oi", None)),
        "doi": safe_float(getattr(f, "doi", None)),
        "funding": safe_float(getattr(f, "funding", None)),
        "ema_fast_15m": safe_float(getattr(f, "ema_fast_15m", None)),
        "ema_slow_15m": safe_float(getattr(f, "ema_slow_15m", None)),
        "ema_diff_15m": safe_float(getattr(f, "ema_diff_15m", None)),
        "ema_slope_15m": safe_float(getattr(f, "ema_slope_15m", None)),
        "trend_dir_15m": getattr(f, "trend_dir_15m", None),
    }


# ---------- feature computations ----------
def compute_mfe_mae(
    conn: sqlite3.Connection,
    instId: str,
    side: str,
    entry_ts: int,
    exit_ts: int,
    entry_px: Optional[float],
) -> Tuple[Optional[float], Optional[float], Optional[int], Optional[int]]:
    """
    Uses agg_1s_micro mids between entry_ts..exit_ts.
    mfe_pct: max favorable excursion (%)
    mae_pct: max adverse excursion (%)
    time_to_* in seconds from entry.
    """
    if entry_px is None:
        entry_px = fetch_mid_at(conn, instId, entry_ts)
    if entry_px is None:
        return None, None, None, None

    w = fetch_mid_window(conn, instId, entry_ts, exit_ts)
    if not w:
        return None, None, None, None

    best_mfe = None
    best_mae = None
    t_mfe = None
    t_mae = None

    for (t, mid) in w:
        if mid is None:
            continue
        ret = (mid - entry_px) / entry_px
        if side.lower() == "short":
            ret = -ret

        # favorable
        if best_mfe is None or ret > best_mfe:
            best_mfe = ret
            t_mfe = int((t - entry_ts) / 1000)

        # adverse (most negative)
        if best_mae is None or ret < best_mae:
            best_mae = ret
            t_mae = int((t - entry_ts) / 1000)

    return safe_float(best_mfe), safe_float(best_mae), t_mfe, t_mae


def compute_ret_before(conn: sqlite3.Connection, instId: str, entry_ts: int) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    px_e = fetch_mid_at(conn, instId, entry_ts)
    px_1m = fetch_mid_at(conn, instId, entry_ts - 60_000)
    px_5m = fetch_mid_at(conn, instId, entry_ts - 5 * 60_000)
    px_15m = fetch_mid_at(conn, instId, entry_ts - 15 * 60_000)
    return (
        safe_float(pct_change(px_e, px_1m)),
        safe_float(pct_change(px_e, px_5m)),
        safe_float(pct_change(px_e, px_15m)),
    )


def compute_dist_from_15m_extremes(conn: sqlite3.Connection, instId: str, entry_ts: int) -> Tuple[Optional[float], Optional[float]]:
    # Use last 15 minutes mids from agg_1s_micro
    w = fetch_mid_window(conn, instId, entry_ts - 15 * 60_000, entry_ts)
    if len(w) < 10:
        return None, None
    mids = [m for (_t, m) in w if m is not None]
    if not mids:
        return None, None
    px = mids[-1]
    hi = max(mids)
    lo = min(mids)
    if px == 0:
        return None, None
    # distance as fraction of px
    return safe_float((hi - px) / px), safe_float((px - lo) / px)


def compute_micro_before(
    fs: FeatureService,
    instId: str,
    entry_ts: int
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Pulls micro series via FeatureService._fetch_micro by using compute_features at different timestamps.
    We use lightweight proxy: compute_features at entry_ts gives last 600s micro;
    then compute means over last N seconds excluding current second.
    """
    f = fs.compute_features(instId, entry_ts)
    if f is None:
        return None, None, None, None, None

    # We don't have raw micro array exposed in FeatureRow, so we approximate means using
    # per-second fields already stored in agg_1s_micro around entry.
    # This function is kept for compatibility; actual computation is done in SQL below.
    return None, None, None, None, None


def compute_flow_accel_30s(fs: FeatureService, instId: str, entry_ts: int) -> Optional[float]:
    f0 = fs.compute_features(instId, entry_ts)
    f1 = fs.compute_features(instId, entry_ts - 30_000)
    if f0 is None or f1 is None:
        return None
    a = getattr(f0, "flow_60s", None)
    b = getattr(f1, "flow_60s", None)
    if a is None or b is None:
        return None
    try:
        return float(a) - float(b)
    except Exception:
        return None


def mean_from_agg(conn: sqlite3.Connection, instId: str, ts_from: int, ts_to: int, col: str) -> Optional[float]:
    # col must exist in agg_1s_micro
    r = conn.execute(
        f"""
        SELECT AVG({col})
        FROM agg_1s_micro
        WHERE instId=? AND sec_ts>? AND sec_ts<=? AND {col} IS NOT NULL
        """,
        (instId, ts_from, ts_to),
    ).fetchone()
    if not r:
        return None
    return safe_float(r[0])


# ---------- main enrich ----------
def enrich(
    db_path: str,
    table: str,
    only_symbol: Optional[str],
    only_missing: bool,
    limit: Optional[int],
) -> None:
    fs = FeatureService(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    ensure_enrich_columns(conn, table)
    conn.commit()

    where = []
    params: List[Any] = []

    if only_symbol:
        where.append("instId=?")
        params.append(only_symbol)

    if only_missing:
        where.append("enriched_at IS NULL")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    limit_sql = f"LIMIT {int(limit)}" if limit else ""

    # IMPORTANT: do not rely on entry_px/exit_px columns
    rows = conn.execute(
        f"""
        SELECT id, instId, side, entry_ts, exit_ts
        FROM {table}
        {where_sql}
        ORDER BY id ASC
        {limit_sql}
        """,
        tuple(params),
    ).fetchall()

    if not rows:
        print("Nothing to enrich.")
        fs.close()
        conn.close()
        return

    # optional trade table px columns (if exist)
    entry_px_col = pick_first_existing_col(conn, table, ["entry_px", "entry_price", "entry_mid", "entry"])
    exit_px_col = pick_first_existing_col(conn, table, ["exit_px", "exit_price", "exit_mid", "exit"])

    t0 = time.time()
    updated = 0

    conn.execute("BEGIN")
    try:
        for r in rows:
            trade_id = int(r["id"])
            instId = r["instId"]
            side = (r["side"] or "").lower()
            entry_ts = int(r["entry_ts"])
            exit_ts = int(r["exit_ts"])

            # prices (prefer trade table if present, else agg_1s_micro mid)
            entry_px = None
            exit_px = None
            if entry_px_col:
                try:
                    entry_px = safe_float(conn.execute(f"SELECT {entry_px_col} FROM {table} WHERE id=?", (trade_id,)).fetchone()[0])
                except Exception:
                    entry_px = None
            if exit_px_col:
                try:
                    exit_px = safe_float(conn.execute(f"SELECT {exit_px_col} FROM {table} WHERE id=?", (trade_id,)).fetchone()[0])
                except Exception:
                    exit_px = None

            if entry_px is None:
                entry_px = fetch_mid_at(conn, instId, entry_ts)
            if exit_px is None:
                exit_px = fetch_mid_at(conn, instId, exit_ts)

            # snapshots
            fe = fs.compute_features(instId, entry_ts)
            fx = fs.compute_features(instId, exit_ts)

            empty_keys = snapshot_from_feature(type("X", (), {})())
            e = snapshot_from_feature(fe) if fe else {k: None for k in empty_keys.keys()}
            x = snapshot_from_feature(fx) if fx else {k: None for k in empty_keys.keys()}

            # MFE/MAE
            mfe, mae, t_mfe, t_mae = compute_mfe_mae(conn, instId, side, entry_ts, exit_ts, entry_px)

            # Returns before
            ret_1m, ret_5m, ret_15m = compute_ret_before(conn, instId, entry_ts)

            # Dist from extremes
            d_hi, d_lo = compute_dist_from_15m_extremes(conn, instId, entry_ts)

            # Micro means before (from agg_1s_micro fields)
            dz_mean_5s = mean_from_agg(conn, instId, entry_ts - 5_000, entry_ts, "delta_vol")  # fallback proxy
            dz_mean_15s = mean_from_agg(conn, instId, entry_ts - 15_000, entry_ts, "delta_vol")  # fallback proxy

            # Better: if you have z already in agg_1s_micro as delta_rate_z, use it:
            if col_exists(conn, "agg_1s_micro", "delta_rate_z"):
                dz_mean_5s = mean_from_agg(conn, instId, entry_ts - 5_000, entry_ts, "delta_rate_z")
                dz_mean_15s = mean_from_agg(conn, instId, entry_ts - 15_000, entry_ts, "delta_rate_z")

            imb_mean_5s = None
            imb_mean_15s = None
            if col_exists(conn, "agg_1s_micro", "imb_shift"):
                imb_mean_5s = mean_from_agg(conn, instId, entry_ts - 5_000, entry_ts, "imb_shift")
                imb_mean_15s = mean_from_agg(conn, instId, entry_ts - 15_000, entry_ts, "imb_shift")
            elif col_exists(conn, "agg_1s_micro", "imbalance15"):
                imb_mean_5s = mean_from_agg(conn, instId, entry_ts - 5_000, entry_ts, "imbalance15")
                imb_mean_15s = mean_from_agg(conn, instId, entry_ts - 15_000, entry_ts, "imbalance15")

            # Flow accel
            flow_accel_30s = compute_flow_accel_30s(fs, instId, entry_ts)
            x_flow_accel_30s = compute_flow_accel_30s(fs, instId, exit_ts)

            # Holding stats
            duration_sec = int((exit_ts - entry_ts) / 1000)
            bars_1m = int(max(0, (exit_ts - entry_ts) // 60_000))
            bars_5s = int(max(0, (exit_ts - entry_ts) // 5_000))

            conn.execute(
                f"""
                UPDATE {table}
                SET
                  e_atr_pct=?, e_atr14=?, e_close15m=?, e_candle15m_ts=?, e_dz=?, e_imb=?, e_flow60=?, e_spike=?, e_range60=?, e_std60=?, e_spread_bps=?, e_mark_z=?, e_oi=?, e_doi=?, e_funding=?,
                  e_ema_fast_15m=?, e_ema_slow_15m=?, e_ema_diff_15m=?, e_ema_slope_15m=?, e_trend_dir_15m=?,

                  x_atr_pct=?, x_atr14=?, x_close15m=?, x_candle15m_ts=?, x_dz=?, x_imb=?, x_flow60=?, x_spike=?, x_range60=?, x_std60=?, x_spread_bps=?, x_mark_z=?, x_oi=?, x_doi=?, x_funding=?,
                  x_ema_fast_15m=?, x_ema_slow_15m=?, x_ema_diff_15m=?, x_ema_slope_15m=?, x_trend_dir_15m=?,

                  mfe_pct=?, mae_pct=?, time_to_mfe_s=?, time_to_mae_s=?,
                  ret_1m_before=?, ret_5m_before=?, ret_15m_before=?,
                  dist_from_15m_high=?, dist_from_15m_low=?,
                  dz_mean_5s_before=?, dz_mean_15s_before=?,
                  imb_mean_5s_before=?, imb_mean_15s_before=?,
                  flow_accel_30s=?, x_flow_accel_30s=?,
                  duration_sec=?, bars_1m_held=?, bars_5s_held=?,

                  enriched_at=?
                WHERE id=?
                """,
                (
                    e["atr_pct"], e["dz"], e["imb"], e["flow60"], e["spike"], e["range60"], e["std60"], e["spread_bps"], e["mark_z"], e["oi"], e["doi"], e["funding"],
                    e["ema_fast_15m"], e["ema_slow_15m"], e["ema_diff_15m"], e["ema_slope_15m"], e["trend_dir_15m"],

                    x["atr_pct"], x["dz"], x["imb"], x["flow60"], x["spike"], x["range60"], x["std60"], x["spread_bps"], x["mark_z"], x["oi"], x["doi"], x["funding"],
                    x["ema_fast_15m"], x["ema_slow_15m"], x["ema_diff_15m"], x["ema_slope_15m"], x["trend_dir_15m"],

                    mfe, mae, t_mfe, t_mae,
                    ret_1m, ret_5m, ret_15m,
                    d_hi, d_lo,
                    dz_mean_5s, dz_mean_15s,
                    imb_mean_5s, imb_mean_15s,
                    flow_accel_30s, x_flow_accel_30s,
                    duration_sec, bars_1m, bars_5s,
                    int(time.time()),
                    trade_id,
                )
            )

            updated += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        fs.close()
        conn.close()

    dt = time.time() - t0
    print(f"Enriched {updated}/{len(rows)} trades in {dt:.2f}s into table {table}.")


def main():
    p = argparse.ArgumentParser(description="Enrich backtest trades with entry/exit feature snapshots + MFE/MAE + context.")
    p.add_argument("--db", required=True)
    p.add_argument("--table", default="backtest_trades2", help="Trades table to enrich (default backtest_trades2)")
    p.add_argument("--symbol", default=None, help="Only enrich this symbol (e.g. SOLUSDT)")
    p.add_argument("--only-missing", action="store_true", help="Only enrich rows where enriched_at is NULL")
    p.add_argument("--limit", type=int, default=None, help="Limit number of trades to enrich")
    args = p.parse_args()

    enrich(args.db, args.table, args.symbol, args.only_missing, args.limit)


if __name__ == "__main__":
    main()
