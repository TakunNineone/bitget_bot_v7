# save as tools/trend_stats.py then run: python tools/trend_stats.py --db data/bitget.db --sym SOLUSDT
import argparse, sqlite3
from app.backtest_skeleton_fast import load_15m_candles, compute_regime_series

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--sym", required=True)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    candles = load_15m_candles(conn, args.sym)
    ts, ema_fast, ema_slow, ema_diff, ema_slope, trend_dir = compute_regime_series(candles)

    c_up = sum(1 for x in trend_dir if x == 1)
    c_dn = sum(1 for x in trend_dir if x == -1)
    c_fl = sum(1 for x in trend_dir if x == 0)
    print("candles:", len(trend_dir), "up:", c_up, "down:", c_dn, "flat:", c_fl)

if __name__ == "__main__":
    main()
