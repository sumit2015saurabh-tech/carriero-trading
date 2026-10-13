# MACD Bot Strategy

Intraday trading bot for US stocks implementing the **QTI MACD model
(`!JFMX3X_NONCORR_MACD_V3.x`)**, per the *Entry & Exit Logic Specification v1.5*.
Market data and execution go through Interactive Brokers (TWS API / IB Gateway).

> **Status:** this is the code snapshot from 10 Aug 2026. At that point orders were routed
> to the built-in **shadow broker** (simulated fills on live IBKR data) rather than to IBKR.

## How it works

```
IB Gateway ──10s bars──► bar_aggregator ──► indicators (MACD per symbol)
                                               │
                                               ▼
                              entry_logic ──► macd_bot ──► IBKR_TWS / shadow_broker
                                               │
                              exit_logic ◄─────┘  (trail · target · stop · EoD)
```

1. **Data** — one 10-second bar stream per symbol, rolled up locally into the entry
   resolution (e.g. 5-min), exit resolution (e.g. 3-min) and index resolutions.
2. **Indicators** — MACD fast/slow/signal lines, histogram, zero-line and mean values,
   computed per symbol with per-symbol parameters (fallback to global defaults).
3. **Entry** — long/short signals from either a MACD/signal-line **crossover** or a
   **consecutive-candle pattern** (`Signal_L` / `Signal_S`), filtered by zero-line and
   mean gates (active after `MeanStartTime`), an optional index filter, trading-time
   windows and an optional ML gate. Entries are limit orders.
4. **Exit** — bracket with stop-loss and target; stop moves to break-even, then trails
   using either a **5-bar** high/low trail or a **percentage** trail (per symbol);
   everything is flattened at `EoDExitTime`.
5. **Control** — settings live in `config.json`; `excel_sync.py` keeps it in sync with an
   Excel control sheet so parameters can be changed while the bot runs.

## Key files (`MACD_ENTRY_EXIT/`)

| File | Role |
|---|---|
| `macd_bot.py` | Main loop: seeds pipelines, routes bars to entry/exit, manages positions |
| `indicators.py` | MACD calculation, per-symbol pipelines, index MACD store |
| `entry_logic.py` | Entry conditions (crossover / pattern, zero-line & mean gates) |
| `exit_logic.py` | Break-even, 5-bar / % trailing stop, target, stop, end-of-day exit |
| `bar_aggregator.py` | Rolls 10s bars into larger, correctly aligned candles |
| `IBKR_TWS.py` | IBKR TWS API wrapper: connection, market data, bracket orders |
| `shadow_broker.py`, `shadow_server.py` | Simulated broker and its web viewer (port 8090) |
| `positions_server.py`, `positions_ui.html` | Live positions/orders dashboard |
| `data_dashboard.py`, `cvd_flow.py` | Market-data dashboard with MACD and order-flow (CVD) analytics |
| `excel_sync.py` | Excel → `config.json` live sync |
| `ml_trade_model.py`, `ml_predict.py` | Optional ML entry filter (training and inference) |
| `backtest_full.py`, `backtest_exits.py`, `regime_backtest.py` | Backtests |
| `notifier.py`, `discord_monitor.py` | Discord notifications |
| `flatten_*.py`, `check_*.py`, `pull_*.py` | Operational utilities (close positions, inspect state, download history) |
| `test_*.py` | Tests |
| `config.json` | Global and per-symbol strategy settings |

`ibkr_broker.py` is the older engine that `macd_bot.py` replaced; it is kept for reference.

## Main settings (`config.json`)

| Setting | Meaning |
|---|---|
| `MACD_Fast`, `MACD_Slow`, `MACD_Signal`, `MACD_Resolution` | MACD parameters and candle size (defaults; overridable per symbol) |
| `Signal_L`, `Signal_S` | `0` = crossover entry, `1–3` = consecutive-candle pattern |
| `MACDLE_BLW_ZLine`, `MACDSE_ABV_ZLine` | Require longs below / shorts above the zero line |
| `MeanStartTime`, `MeanMultiplier_L/S` | When the mean gate starts, and its strength |
| `SLMultiplier`, `TargetMultiplier`, `BEMultiplier` | Stop, target and break-even distances |
| `TrailingStop_Perc`, `TrailingPerc` | Use a % trail instead of the 5-bar trail |
| `TimeToStartTrading`, `TimeToStopTrading`, `EoDExitTime` | Trading window and forced exit time (ET) |
| `PositionSize`, `MPL_dollar_Amount` | Position sizing |

## Running

Requirements: Python 3.12, a running IB Gateway or TWS with API access enabled.

```bash
pip install ibapi pandas numpy openpyxl requests
cd MACD_ENTRY_EXIT
python macd_bot.py
```

The `scripts/` folder contains the shell scripts used on the Linux server
(`start_bot.sh`, `stop_bot.sh`, dashboard starters, cleanup). Paths in them point to
`/home/azureuser/...`; adjust for your machine.

## Configuration that is not in this repo

Removed before publishing — provide your own:

- **IBKR account ID** — set `iAccount` in `config.json` (placeholder `DUXXXXXXX`).
- **Data-account credentials** — `data_account.json` (not included).
- **Discord alerts** — set the `DISCORD_WEBHOOK_URL` environment variable.
- **Server address** — `<VM_HOST>` placeholders in the dashboards.

Trade logs and price-history spreadsheets are also not included.

## Disclaimer

Published for reference and research. Not investment advice. Test on a paper account;
live trading carries risk of loss.
