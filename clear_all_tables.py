import sqlite3
import sys

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/bitget.db"

TABLES = [
    # raw ws
    "raw_ws_trade",
    "raw_ws_books15",
    "raw_ws_ticker",
    "raw_ws_candle",

    # raw rest
    "raw_rest_symbol_price",
    "raw_rest_open_interest",
    "raw_rest_current_fund_rate",
    "raw_rest_funding_time",

    # aggregates
    "agg_1s_micro",
    "agg_1m_context",
]

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for t in TABLES:
        try:
            cur.execute(f"DELETE FROM {t};")
            print(f"cleared {t}")
        except Exception as e:
            print(f"skip {t}: {e}")

    conn.commit()
    conn.close()
    print("DONE: database fully cleared")

if __name__ == "__main__":
    main()
