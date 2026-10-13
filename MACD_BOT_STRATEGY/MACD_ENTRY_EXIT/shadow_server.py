"""
shadow_server.py — SHADOW TRADER dashboard  (added 2026-08-06)
================================================================================
A dedicated read-only web UI for the shadow book, served on port 8090:

    http://<VM_HOST>:8090/

  • Net P&L for the day — realized, open mark-to-market, commission, win/loss
  • Open shadow positions with entry, live price, CURRENT stop and how many
    times that stop has moved (click a row to see every move)
  • Closed trades — entry/exit, reason, gross → commission → net
  • Stop-loss update log — old → new, why (BREAKEVEN / TRAIL / EOD), the market
    price at the moment it moved, and the P&L that move locked in
  • Every order punched into the shadow book, with its lifecycle status
  • A DATE PICKER: pick any past date and the whole page replays that session
    from its ledger file. Works even when the bot is not running.

Two ways to run it:
  1. In-process — macd_bot.py calls start_shadow_dashboard(); it reads the live
     ShadowBroker object directly, so today's numbers are instant.
  2. Standalone — `venv/bin/python shadow_server.py` reads the ledger files in
     shadow/. Use this after hours to review any day.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    _DEPS_OK = True
except ImportError:                                    # pragma: no cover
    _DEPS_OK = False

from shadow_broker import SHADOW, rebuild_day, available_days, ShadowLedger

logger = logging.getLogger("SHADOW_UI")

SHADOW_PORT = int(os.getenv("SHADOW_PORT", "8090"))
_STATE_FILE = SHADOW.ledger.state_path()


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _live_state() -> Dict[str, Any]:
    """
    Today's book.

    In-process (started from macd_bot) we ask the live broker directly — that is
    always complete and current.

    Standalone we rebuild the day from the append-only ledger, which is the
    authoritative record, and then overlay the OPEN positions from
    state_today.json when that snapshot is fresh. Rebuilding rather than trusting
    the snapshot alone matters: if the bot has stopped, the snapshot is frozen at
    whatever second it died, while the ledger still holds the full session.
    """
    try:
        if SHADOW.enabled and (SHADOW._orders or SHADOW._positions or SHADOW._closed):
            return SHADOW.snapshot()
    except Exception as exc:
        logger.debug("live snapshot unavailable, rebuilding from ledger: %s", exc)

    state = rebuild_day(_today())
    state["historical"] = False
    try:
        if _STATE_FILE.exists():
            age = time.time() - _STATE_FILE.stat().st_mtime
            with _STATE_FILE.open("r", encoding="utf-8") as fh:
                snap = json.load(fh)
            if age <= 120 and snap.get("day") == _today():
                state["positions"] = snap.get("positions", [])
                state["quotes"] = snap.get("quotes", {})
                pnl = state.get("pnl", {})
                pnl["unrealized"] = (snap.get("pnl") or {}).get("unrealized", 0.0)
                pnl["net"] = round(pnl.get("realized", 0.0) + pnl["unrealized"], 2)
                pnl["open_positions"] = len(state["positions"])
                state["pnl"] = pnl
            else:
                state["stale_snapshot_sec"] = int(age)
    except Exception as exc:
        logger.debug("state snapshot overlay failed: %s", exc)
    return state


def _state_for(day: Optional[str]) -> Dict[str, Any]:
    if not day or day == _today():
        st = _live_state()
        st["is_today"] = True
        return st
    st = rebuild_day(day)
    st["is_today"] = False
    return st


# ── page ──────────────────────────────────────────────────────────────────────
#
# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  THIS DESIGN IS FROZEN  —  user directive, 2026-08-06                     ║
# ║                                                                          ║
# ║  Do not restructure, restyle, re-lay-out, reorder, rename or drop        ║
# ║  anything on this page without asking first. The section order is fixed: ║
# ║      header (badge · live stamp · Session picker · Refresh · auto)       ║
# ║      P&L cards                                                           ║
# ║      Open positions → Closed trades → Stop-loss updates → Orders → Fills ║
# ║                                                                          ║
# ║  New work belongs BEHIND the page — the broker, the ledger, /api/shadow, ║
# ║  /ws, latency, correctness. If something genuinely needs a new value,    ║
# ║  fit it into an existing column or row rather than adding a column or a  ║
# ║  section, and confirm before doing it. Fixing a wrong value in a cell    ║
# ║  that already exists is fine.                                            ║
# ╚══════════════════════════════════════════════════════════════════════════╝

_PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Shadow Trader — QTI MACD</title>
<style>
 :root{--bg:#0d1117;--panel:#161b22;--line:#232c38;--tx:#e6edf3;--dim:#8b949e;
       --up:#3fb950;--dn:#f85149;--acc:#58a6ff;--warn:#d29922;}
 *{box-sizing:border-box}
 body{margin:0;background:var(--bg);color:var(--tx);
      font:13px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
 header{display:flex;flex-wrap:wrap;gap:12px;align-items:center;
        padding:12px 18px;background:var(--panel);border-bottom:1px solid var(--line);
        position:sticky;top:0;z-index:5}
 h1{font-size:16px;margin:0;letter-spacing:.4px}
 .badge{background:#3b2a00;color:var(--warn);border:1px solid #5a4300;
        padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
 .spacer{flex:1}
 select,button{background:#0d1117;color:var(--tx);border:1px solid var(--line);
        border-radius:6px;padding:5px 9px;font-size:12px}
 button{cursor:pointer} button:hover{border-color:var(--acc)}
 .cards{display:flex;flex-wrap:wrap;gap:10px;padding:14px 18px}
 .card{background:var(--panel);border:1px solid var(--line);border-radius:8px;
       padding:10px 14px;min-width:132px}
 .card .k{color:var(--dim);font-size:10.5px;text-transform:uppercase;letter-spacing:.6px}
 .card .v{font-size:19px;font-weight:600;margin-top:3px;font-variant-numeric:tabular-nums}
 section{padding:0 18px 18px}
 h2{font-size:12px;text-transform:uppercase;letter-spacing:.8px;color:var(--dim);
    margin:18px 0 7px;font-weight:600}
 table{width:100%;border-collapse:collapse;background:var(--panel);
       border:1px solid var(--line);border-radius:8px;overflow:hidden}
 th{background:#111721;color:var(--dim);font-size:10.5px;text-transform:uppercase;
    letter-spacing:.5px;text-align:right;padding:7px 9px;font-weight:600;
    border-bottom:1px solid var(--line);white-space:nowrap}
 td{padding:6px 9px;text-align:right;border-bottom:1px solid #1c232e;
    font-variant-numeric:tabular-nums;white-space:nowrap}
 th:first-child,td:first-child,th.l,td.l{text-align:left}
 tbody tr:hover{background:#1b2230}
 .up{color:var(--up)} .dn{color:var(--dn)} .dim{color:var(--dim)}
 .pill{padding:1px 6px;border-radius:9px;font-size:10.5px;border:1px solid var(--line)}
 .LONG,.BUY{color:var(--up);border-color:#1d4429} .SHORT,.SELL{color:var(--dn);border-color:#4a1f1f}
 .Filled{color:var(--up)} .Cancelled{color:var(--dim)} .Submitted{color:var(--acc)}
 .PreSubmitted{color:var(--warn)}
 .clickable{cursor:pointer} .sub{background:#101620}
 .sub td{color:var(--dim);font-size:11.5px}
 .empty{padding:14px;color:var(--dim);text-align:center;background:var(--panel);
        border:1px solid var(--line);border-radius:8px}
 .foot{padding:10px 18px 30px;color:var(--dim);font-size:11px;line-height:1.7}
 code{background:#0b0f15;padding:1px 5px;border-radius:4px;color:var(--acc)}
</style></head><body>
<header>
  <h1>Shadow Trader</h1>
  <span class="badge" id="mode">SHADOW — no orders sent to IBKR</span>
  <span class="dim" id="stamp"></span>
  <span class="spacer"></span>
  <label class="dim" for="day">Session</label>
  <select id="day"></select>
  <button onclick="load()">Refresh</button>
  <label class="dim"><input type="checkbox" id="auto" checked> auto</label>
</header>

<div class="cards" id="cards"></div>

<section>
  <h2>Open positions <span class="dim" id="posn"></span></h2>
  <div id="positions"></div>

  <h2>Closed trades — history</h2>
  <div id="trades"></div>

  <h2>Stop-loss updates</h2>
  <div id="sl"></div>

  <h2>Orders</h2>
  <div id="orders"></div>

  <h2>Fills</h2>
  <div id="fills"></div>
</section>

<div class="foot" id="foot"></div>

<script>
const $ = id => document.getElementById(id);
const n2 = v => (v==null||isNaN(v)) ? '—' : Number(v).toFixed(2);
const n4 = v => (v==null||isNaN(v)) ? '—' : Number(v).toFixed(4);
const cls = v => Number(v) > 0 ? 'up' : (Number(v) < 0 ? 'dn' : 'dim');
const sgn = v => (Number(v) > 0 ? '+' : '') + n2(v);

function tbl(cols, rows, empty){
  if(!rows.length) return `<div class="empty">${empty}</div>`;
  return `<table><thead><tr>${cols.map(c=>`<th class="${c.l?'l':''}">${c.h}</th>`).join('')}
    </tr></thead><tbody>${rows.join('')}</tbody></table>`;
}

function render(d){
  const p = d.pnl || {};
  $('mode').textContent = d.is_today
      ? 'SHADOW — no orders sent to IBKR'
      : 'SHADOW — replay of ' + d.day;
  $('stamp').textContent = (d.is_today ? (wsAlive ? 'live (push) · ' : 'live · ')
                                       : 'closed session · ') + (d.generated||'');
  $('cards').innerHTML = [
    ['Net P&L',      sgn(p.net),        cls(p.net)],
    ['Realized',     sgn(p.realized),   cls(p.realized)],
    ['Open (M2M)',   sgn(p.unrealized), cls(p.unrealized)],
    ['Commission',   '-' + n2(p.commission), 'dim'],
    ['Trades',       (p.trades||0) + '', ''],
    ['Win / Loss',   (p.wins||0)+' / '+(p.losses||0), ''],
    ['Open pos.',    (p.open_positions||0)+'', ''],
  ].map(([k,v,c])=>`<div class="card"><div class="k">${k}</div>
      <div class="v ${c}">${v}</div></div>`).join('');

  // ── positions (click to expand the stop's journey) ────────────────────────
  $('posn').textContent = d.positions.length ? '' : '';
  $('positions').innerHTML = tbl(
    [{h:'Symbol',l:1},{h:'Side',l:1},{h:'Qty'},{h:'Entry'},{h:'Entry @'},{h:'Last'},
     {h:'Stop now'},{h:'Stop init'},{h:'Moves'},{h:'At risk / Locked'},
     {h:'Unreal $'},{h:'Unreal %'}],
    d.positions.map((r,i)=>`
      <tr class="clickable" onclick="tog('ps${i}')">
        <td class="l"><b>${r.symbol}</b></td>
        <td class="l"><span class="pill ${r.side}">${r.side}</span></td>
        <td>${r.qty}</td><td>${n4(r.avg)}</td><td class="dim">${r.entry_time||'—'}</td>
        <td class="${r.price_fresh?'':'dim'}">${n4(r.last)}</td>
        <td class="${r.stop_moves?'up':''}">${r.stop?n4(r.stop):'—'}</td>
        <td class="dim">${r.stop_initial?n4(r.stop_initial):'—'}</td>
        <td>${r.stop_moves||0}</td>
        <!-- once the stop is trailed past entry the risk turns into locked profit -->
        <td class="${r.locked>0?'up':'dim'}">${!r.stop ? '—'
            : (r.locked > 0 ? 'locked +' + n2(r.locked) : 'risk ' + n2(r.risk))}</td>
        <td class="${cls(r.unrealized)}">${sgn(r.unrealized)}</td>
        <td class="${cls(r.unrealized)}">${sgn(r.unrealized_pct)}%</td></tr>
      <tr id="ps${i}" class="sub" style="display:none"><td class="l" colspan="12">
        ${(r.stop_history||[]).length
          ? (r.stop_history||[]).map(h=>`${h.t} &nbsp; stop ${n4(h.from)} → <b>${n4(h.to)}</b>
              &nbsp;(${h.reason}) &nbsp; price then ${n4(h.price_at_move)}`).join('<br>')
          : 'stop has not moved yet'}</td></tr>`),
    'No open shadow positions.');

  // ── closed trades ─────────────────────────────────────────────────────────
  $('trades').innerHTML = tbl(
    [{h:'Symbol',l:1},{h:'Dir',l:1},{h:'Qty'},{h:'In @'},{h:'Entry'},{h:'Out @'},
     {h:'Exit'},{h:'Reason',l:1},{h:'Gross'},{h:'Comm'},{h:'Net'},{h:'%'},
     {h:'Stop moves'},{h:'Held'}],
    (d.trades||[]).map((t,i)=>`
      <tr class="clickable" onclick="tog('tr${i}')">
        <td class="l"><b>${t.symbol}</b></td>
        <td class="l"><span class="pill ${t.direction}">${t.direction}</span></td>
        <td>${t.qty}</td><td class="dim">${t.entry_time}</td><td>${n4(t.entry_price)}</td>
        <td class="dim">${t.exit_time}</td><td>${n4(t.exit_price)}</td>
        <td class="l dim">${t.exit_reason||''}</td>
        <td class="${cls(t.gross_pnl)}">${sgn(t.gross_pnl)}</td>
        <td class="dim">-${n2(t.commission)}</td>
        <td class="${cls(t.net_pnl)}"><b>${sgn(t.net_pnl)}</b></td>
        <td class="${cls(t.pnl_pct)}">${sgn(t.pnl_pct)}%</td>
        <td>${t.stop_moves||0}</td>
        <td class="dim">${Math.round((t.hold_sec||0)/60)}m</td></tr>
      <tr id="tr${i}" class="sub" style="display:none"><td class="l" colspan="14">
        stop ${n4(t.stop_initial)} → ${n4(t.stop_final)} &nbsp;·&nbsp;
        entry oid ${t.entry_oid} / exit oid ${t.exit_oid}<br>
        ${(t.stop_history||[]).map(h=>`${h.t} &nbsp; ${n4(h.from)} → <b>${n4(h.to)}</b>
           (${h.reason}) price then ${n4(h.price_at_move)}`).join('<br>') || ''}
      </td></tr>`),
    'No closed trades for this session.');

  // ── stop-loss updates ─────────────────────────────────────────────────────
  $('sl').innerHTML = tbl(
    [{h:'Time',l:1},{h:'Symbol',l:1},{h:'oid'},{h:'Type',l:1},{h:'From'},{h:'To'},
     {h:'Δ'},{h:'Reason',l:1},{h:'Price then'},{h:'Locked $'}],
    (d.sl_events||[]).slice().reverse().map(h=>`
      <tr><td class="l dim">${h.t}</td><td class="l"><b>${h.symbol}</b></td>
      <td class="dim">${h.oid}</td><td class="l dim">${h.order_type||''}</td>
      <td>${n4(h.from)}</td><td><b>${n4(h.to)}</b></td>
      <td class="${cls(h.delta)}">${sgn(h.delta)}</td>
      <td class="l">${h.reason}</td><td class="dim">${n4(h.price_at_move)}</td>
      <td class="${cls(h.locked_pnl)}">${h.locked_pnl?sgn(h.locked_pnl):'—'}</td></tr>`),
    'No stop-loss updates yet.');

  // ── orders ────────────────────────────────────────────────────────────────
  $('orders').innerHTML = tbl(
    [{h:'oid'},{h:'Placed',l:1},{h:'Symbol',l:1},{h:'Role',l:1},{h:'Side',l:1},
     {h:'Qty'},{h:'Type',l:1},{h:'Price'},{h:'Status',l:1},{h:'Filled @'},
     {h:'Fill px'},{h:'Moves'}],
    (d.orders||[]).slice().reverse().map(o=>`
      <tr><td class="dim">${o.oid}</td><td class="l dim">${o.placed||''}</td>
      <td class="l"><b>${o.symbol}</b></td><td class="l dim">${o.role||''}</td>
      <td class="l"><span class="pill ${o.side}">${o.side}</span></td>
      <td>${o.qty}</td><td class="l dim">${o.type}</td><td>${n4(o.price)}</td>
      <td class="l ${o.status}">${o.status}</td>
      <td class="dim">${o.filled_at||''}</td>
      <td>${o.avg_fill?n4(o.avg_fill):'—'}</td><td>${o.moves||0}</td></tr>`),
    'No orders punched yet.');

  // ── fills ─────────────────────────────────────────────────────────────────
  $('fills').innerHTML = tbl(
    [{h:'Time',l:1},{h:'Symbol',l:1},{h:'Side',l:1},{h:'Qty'},{h:'Price'},
     {h:'Model',l:1},{h:'Comm'},{h:'Realized'},{h:'execId',l:1}],
    (d.fills||[]).slice().reverse().map(f=>`
      <tr><td class="l dim">${f.t}</td><td class="l"><b>${f.symbol}</b></td>
      <td class="l"><span class="pill ${f.side}">${f.side}</span></td>
      <td>${f.qty}</td><td>${n4(f.price)}</td><td class="l dim">${f.reason||''}</td>
      <td class="dim">-${n2(f.commission)}</td>
      <td class="${cls(f.realized_pnl)}">${f.realized_pnl?sgn(f.realized_pnl):'—'}</td>
      <td class="l dim">${f.exec_id||''}</td></tr>`),
    'No fills yet.');

  const c = d.config || {};
  $('foot').innerHTML = `Fill model — MKT: BUY at ask+${c.slippage}, SELL at bid−${c.slippage}.
    LMT: fills only when the market ${c.limit_rule==='touch'?'touches':'trades through'} the
    limit, at the limit price (never better). STP: arms on trigger, then fills as a market
    order ${c.slippage} against you. Commission $${c.comm_per_share}/share, min $${c.comm_min}.
    Assumed spread when no bid/ask: $${c.spread}. Partial fills and queue position are not
    modelled — every fill is all-or-nothing. Market data is real, streamed live from IBKR;
    only the order side is simulated. Ledger: <code>shadow/shadow_${d.day}.jsonl</code>`;
}

function tog(id){ const e = $(id); if(e) e.style.display = e.style.display==='none'?'':'none'; }

async function loadDays(){
  const r = await fetch('/api/days'); const days = await r.json();
  const sel = $('day'); const cur = sel.value;
  sel.innerHTML = days.map(d=>`<option value="${d}">${d}</option>`).join('');
  if(cur && days.includes(cur)) sel.value = cur;
}

async function load(){
  try{
    const day = $('day').value || '';
    const r = await fetch('/api/shadow' + (day?`?date=${day}`:''));
    render(await r.json());
  }catch(e){ $('cards').innerHTML = `<div class="empty">shadow server unreachable: ${e}</div>`; }
}

// ── Live push ──────────────────────────────────────────────────────────────
// Today's session streams over a WebSocket: the server pushes the moment the
// broker republishes, so a fill or a stop move lands here in well under a
// second. Polling stays as the fallback (socket down, or a past date selected,
// which is static anyway).
let ws = null, wsAlive = false;
function connect(){
  if (ws) { try { ws.close(); } catch(e){} ws = null; }
  const day = $('day').value || '';
  if (day && day !== new Date().toISOString().slice(0,10)) { wsAlive = false; return; }
  try {
    ws = new WebSocket((location.protocol==='https:'?'wss://':'ws://') + location.host + '/ws');
    ws.onopen    = () => { wsAlive = true;  $('stamp').classList.add('live'); };
    ws.onmessage = e  => { if($('auto').checked) render(JSON.parse(e.data)); };
    ws.onclose   = () => { wsAlive = false; setTimeout(connect, 3000); };
    ws.onerror   = () => { wsAlive = false; };
  } catch(e){ wsAlive = false; }
}

$('day').addEventListener('change', () => { connect(); load(); });
loadDays().then(load).then(connect);
// fallback poll — only does work when the push socket is not carrying us
setInterval(()=>{ if($('auto').checked && !wsAlive) load(); }, 2000);
setInterval(loadDays, 60000);
</script></body></html>"""


# ── app ───────────────────────────────────────────────────────────────────────

def build_app() -> "FastAPI":
    app = FastAPI(title="Shadow Trader", docs_url=None, redoc_url=None)
    app.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_methods=["*"], allow_headers=["*"])

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _PAGE

    @app.get("/api/days")
    def days() -> JSONResponse:
        got = available_days()
        today = _today()
        if today not in got:
            got.insert(0, today)
        return JSONResponse(got)

    @app.get("/api/shadow")
    def shadow(date: Optional[str] = None) -> JSONResponse:
        try:
            return JSONResponse(_state_for(date))
        except Exception as exc:
            logger.error("shadow api error: %s", exc)
            return JSONResponse({"error": str(exc), "pnl": {}, "positions": [],
                                 "orders": [], "fills": [], "trades": [],
                                 "sl_events": []}, status_code=200)

    @app.get("/api/trades.csv")
    def trades_csv(date: Optional[str] = None):
        """Raw CSV for a session — the same file build_daily_trades.py can pick up."""
        path = SHADOW.ledger.trades_path(date or _today())
        body = path.read_text(encoding="utf-8") if path.exists() else ""
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(body, media_type="text/csv")

    @app.get("/api/health")
    def health() -> JSONResponse:
        return JSONResponse({"ok": True, "mode": "SHADOW" if SHADOW.enabled else "LIVE",
                             "port": SHADOW_PORT, "day": _today(),
                             "push": True})

    @app.websocket("/ws")
    async def live(ws: "WebSocket") -> None:
        """
        Push today's book to the browser the moment it changes.

        The broker republishes state_today.json immediately on any material
        event — an order, a fill, a stop move — and at most 5×/s otherwise. This
        socket watches that file's mtime, so a trade reaches the page in well
        under a second instead of waiting out a poll. The page keeps its polling
        path as a fallback, so an old browser or a dropped socket still works.
        """
        import asyncio
        await ws.accept()
        try:
            # Send the current book straight away. Waiting for the first change
            # would leave a freshly-opened page blank until the next event —
            # and before the bot has published anything, blank forever.
            # Seed last_sent from the file we just served, so the very next loop
            # does not immediately resend the same snapshot.
            last_sent = _STATE_FILE.stat().st_mtime if _STATE_FILE.exists() else 0.0
            await ws.send_json(_state_for(None))
            while True:
                try:
                    mtime = _STATE_FILE.stat().st_mtime if _STATE_FILE.exists() else 0.0
                except OSError:
                    mtime = 0.0
                if mtime > last_sent:
                    last_sent = mtime
                    await ws.send_json(_state_for(None))
                await asyncio.sleep(0.15)
        except WebSocketDisconnect:
            return
        except Exception as exc:                       # pragma: no cover
            logger.debug("shadow ws closed: %s", exc)
            return

    return app


def start_shadow_dashboard(port: int = SHADOW_PORT) -> None:
    """Launch the Shadow Trader UI on a daemon thread (called from macd_bot)."""
    if not _DEPS_OK:
        logger.warning("Shadow dashboard NOT started — fastapi/uvicorn missing")
        return

    def _run() -> None:
        try:
            uvicorn.run(build_app(), host="0.0.0.0", port=port,
                        log_level="warning", access_log=False)
        except Exception as exc:
            logger.error("Shadow dashboard crashed: %s", exc)

    threading.Thread(target=_run, daemon=True, name="shadow-dashboard").start()
    logger.warning("Shadow Trader dashboard → http://0.0.0.0:%d/", port)


if __name__ == "__main__":                             # standalone review mode
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [SHADOW_UI] %(levelname)s - %(message)s")
    if not _DEPS_OK:
        raise SystemExit("fastapi + uvicorn required: venv/bin/pip install fastapi uvicorn")
    print(f"Shadow Trader → http://0.0.0.0:{SHADOW_PORT}/  (ledger: {SHADOW.ledger.base_dir})")
    uvicorn.run(build_app(), host="0.0.0.0", port=SHADOW_PORT,
                log_level="warning", access_log=False)
