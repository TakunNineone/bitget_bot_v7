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
        ("e_mid", "REAL"),
        ("e_vwap", "REAL"),
        ("e_ret_1s", "REAL"),
        ("e_trade_count", "INTEGER"),
        ("e_atr_pct", "REAL"),
        ("e_atr1h", "REAL"),
        ("e_atr4h", "REAL"),
        ("e_dz", "REAL"),
        ("e_imb", "REAL"),
        ("e_imbalance15", "REAL"),
        ("e_imb5", "REAL"),
        ("e_depth_ratio5", "REAL"),
        ("e_flow60", "INTEGER"),
        ("e_trade_count_15m", "INTEGER"),
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
        ("e_ema_diff_1h", "REAL"),
        ("e_ema_slope_1h", "REAL"),
        ("e_trend_dir_1h", "INTEGER"),
        ("e_ema_diff_4h", "REAL"),
        ("e_ema_slope_4h", "REAL"),
        ("e_trend_dir_4h", "INTEGER"),
    ]:
        add_col(conn, table, col, typ)

    # exit snapshot
    for col, typ in [
        ("x_mid", "REAL"),
        ("x_vwap", "REAL"),
        ("x_ret_1s", "REAL"),
        ("x_trade_count", "INTEGER"),
        ("x_atr_pct", "REAL"),
        ("x_atr1h", "REAL"),
        ("x_atr4h", "REAL"),
        ("x_dz", "REAL"),
        ("x_imb", "REAL"),
        ("x_imbalance15", "REAL"),
        ("x_imb5", "REAL"),
        ("x_depth_ratio5", "REAL"),
        ("x_flow60", "INTEGER"),
        ("x_trade_count_15m", "INTEGER"),
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
        ("x_ema_diff_1h", "REAL"),
        ("x_ema_slope_1h", "REAL"),
        ("x_trend_dir_1h", "INTEGER"),
        ("x_ema_diff_4h", "REAL"),
        ("x_ema_slope_4h", "REAL"),
        ("x_trend_dir_4h", "INTEGER"),
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
        "mid": safe_float(getattr(f, "mid", None)),
        "vwap": safe_float(getattr(f, "vwap", None)),
        "ret_1s": safe_float(getattr(f, "ret_1s", None)),
        "trade_count": safe_int(getattr(f, "trade_count", None)),
        "atr_pct": safe_float(atr_pct),
        "atr1h": safe_float(getattr(f, "atr1h", None)),
        "atr4h": safe_float(getattr(f, "atr4h", None)),
        "dz": safe_float(getattr(f, "delta_rate_z", None)),
        "imb": safe_float(getattr(f, "imb_shift", None)),
        "imbalance15": safe_float(getattr(f, "imbalance15", None)),
        "imb5": safe_float(getattr(f, "imbalance5", None)),
        "depth_ratio5": safe_float(getattr(f, "depth_ratio5", None)),
        "flow60": int(getattr(f, "flow_60s", 0) or 0),
        "trade_count_15m": safe_int(getattr(f, "trade_count_15m", None)),
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
        "ema_diff_1h": safe_float(getattr(f, "ema_diff_1h", None)),
        "ema_slope_1h": safe_float(getattr(f, "ema_slope_1h", None)),
        "trend_dir_1h": getattr(f, "trend_dir_1h", None),
        "ema_diff_4h": safe_float(getattr(f, "ema_diff_4h", None)),
        "ema_slope_4h": safe_float(getattr(f, "ema_slope_4h", None)),
        "trend_dir_4h": getattr(f, "trend_dir_4h", None),
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

            # Holding stats
            duration_sec = int((exit_ts - entry_ts) / 1000)
            bars_1m = int(max(0, (exit_ts - entry_ts) // 60_000))
            bars_5s = int(max(0, (exit_ts - entry_ts) // 5_000))

            conn.execute(
                f"""
                UPDATE {table}
                SET
                  e_mid=?, e_vwap=?, e_ret_1s=?, e_trade_count=?,
                  e_atr_pct=?, e_atr1h=?, e_atr4h=?, e_dz=?, e_imb=?, e_imbalance15=?, e_imb5=?, e_depth_ratio5=?, e_flow60=?, e_trade_count_15m=?,
                  e_spike=?, e_range60=?, e_std60=?, e_spread_bps=?, e_mark_z=?, e_oi=?, e_doi=?, e_funding=?,
                  e_ema_fast_15m=?, e_ema_slow_15m=?, e_ema_diff_15m=?, e_ema_slope_15m=?, e_trend_dir_15m=?,
                  e_ema_diff_1h=?, e_ema_slope_1h=?, e_trend_dir_1h=?, e_ema_diff_4h=?, e_ema_slope_4h=?, e_trend_dir_4h=?,

                  x_mid=?, x_vwap=?, x_ret_1s=?, x_trade_count=?,
                  x_atr_pct=?, x_atr1h=?, x_atr4h=?, x_dz=?, x_imb=?, x_imbalance15=?, x_imb5=?, x_depth_ratio5=?, x_flow60=?, x_trade_count_15m=?,
                  x_spike=?, x_range60=?, x_std60=?, x_spread_bps=?, x_mark_z=?, x_oi=?, x_doi=?, x_funding=?,
                  x_ema_fast_15m=?, x_ema_slow_15m=?, x_ema_diff_15m=?, x_ema_slope_15m=?, x_trend_dir_15m=?,
                  x_ema_diff_1h=?, x_ema_slope_1h=?, x_trend_dir_1h=?, x_ema_diff_4h=?, x_ema_slope_4h=?, x_trend_dir_4h=?,

                  mfe_pct=?, mae_pct=?, time_to_mfe_s=?, time_to_mae_s=?,
                  ret_1m_before=?, ret_5m_before=?, ret_15m_before=?,
                  dist_from_15m_high=?, dist_from_15m_low=?,
                  dz_mean_5s_before=?, dz_mean_15s_before=?,
                  imb_mean_5s_before=?, imb_mean_15s_before=?,
                  flow_accel_30s=?,
                  duration_sec=?, bars_1m_held=?, bars_5s_held=?,

                  enriched_at=?
                WHERE id=?
                """,
                (
                    e["mid"], e["vwap"], e["ret_1s"], e["trade_count"],
                    e["atr_pct"], e["atr1h"], e["atr4h"], e["dz"], e["imb"], e["imbalance15"], e["imb5"], e["depth_ratio5"], e["flow60"], e["trade_count_15m"],
                    e["spike"], e["range60"], e["std60"], e["spread_bps"], e["mark_z"], e["oi"], e["doi"], e["funding"],
                    e["ema_fast_15m"], e["ema_slow_15m"], e["ema_diff_15m"], e["ema_slope_15m"], e["trend_dir_15m"],
                    e["ema_diff_1h"], e["ema_slope_1h"], e["trend_dir_1h"], e["ema_diff_4h"], e["ema_slope_4h"], e["trend_dir_4h"],

                    x["mid"], x["vwap"], x["ret_1s"], x["trade_count"],
                    x["atr_pct"], x["atr1h"], x["atr4h"], x["dz"], x["imb"], x["imbalance15"], x["imb5"], x["depth_ratio5"], x["flow60"], x["trade_count_15m"],
                    x["spike"], x["range60"], x["std60"], x["spread_bps"], x["mark_z"], x["oi"], x["doi"], x["funding"],
                    x["ema_fast_15m"], x["ema_slow_15m"], x["ema_diff_15m"], x["ema_slope_15m"], x["trend_dir_15m"],
                    x["ema_diff_1h"], x["ema_slope_1h"], x["trend_dir_1h"], x["ema_diff_4h"], x["ema_slope_4h"], x["trend_dir_4h"],

                    mfe, mae, t_mfe, t_mae,
                    ret_1m, ret_5m, ret_15m,
                    d_hi, d_lo,
                    dz_mean_5s, dz_mean_15s,
                    imb_mean_5s, imb_mean_15s,
                    flow_accel_30s,
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
