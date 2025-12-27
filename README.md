# Bitget Micro‑Swing Data Pipeline (15m) — Python

Это проект-сборщик данных (public WS + public REST) под микро‑свинг на 15m и цель 1–2%,
с хранением **raw** и **агрегатов 1s/1m/15m** в SQLite.

## Что собирает (Futures / Mix)
WebSocket (public):
- `ticker` (каждые ~300–400ms при изменениях)
- `trade` (лента сделок)
- `books15` (15 уровней стакана, snapshot, 100–200ms при изменениях)
- `candle1m`
- `candle15m`

REST (polling):
- `symbol-price` (market/index/mark)
- `open-interest`
- `current-fund-rate`
- `funding-time`

## Требования
- Python 3.10+
- Linux/Mac/Windows

## Установка
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Настройка
Отредактируй `config.yaml`:
- `symbols`: список символов (например BTCUSDT)
- `inst_type`: `USDT-FUTURES` (или COIN/USDC)
- `rest_product_type`: `usdt-futures` (строкой как в REST)
- частоты polling и шардирование WS

## Запуск
```bash
python -m app.main --config config.yaml
```

SQLite будет в `data/bitget.db`.

## Полезные заметки
- WS лимиты и keepalive: отправляй `"ping"` каждые ~30с, иначе сервер разорвёт соединение; также лимит 10 сообщений/сек и лимиты подписок/коннектов.  
- Для `books15` приходят только snapshots (без incremental updates), поэтому state строится как «последний снапшот».
- Для `books` (полный стакан) есть incremental updates + checksum; в этом проекте реализован checksum‑валидатор, если поле `checksum` присутствует.


## Новое: FeatureEngine + Strategy skeleton (без торговли)
Добавлены:
- `app/features.py` — вычисление rolling-фич из SQLite:
  - `delta_z` (z-score дельты за окно, по agg_1s_micro)
  - `imb_shift` (сдвиг imbalance за окно)
  - `mark_dev_z` (z-score отклонения mark-last за 60 минут)
  - `oi_z` (z-score dOI за 60 минут)
  - `atr15m` (ATR(14) по свечам candle15m)
- `app/strategy_skeleton.py` — каркас принятия решений:
  - жесткие гейты (spread/vol)
  - триггеры (delta_z + imb_shift + mark_dev_z)
  - риск-параметры SL/TP/Trailing из ATR с ограничениями под цель 1–2%

### Запуск скелета сигналов
Запусти сборщик, подожди пару минут, затем:
```bash
python -m app.strategy_skeleton --db data/bitget.db --symbols BTCUSDT ETHUSDT --every 2
```

> Важно: это **не** торговый бот и не отправляет ордера. Это генератор сигналов/параметров,
который тебе нужно бэктестить и калибровать (включая комиссии и проскальзывание).
