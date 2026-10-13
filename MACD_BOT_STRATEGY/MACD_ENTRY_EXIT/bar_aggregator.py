"""10-second base-candle aggregator: rolls completed 10s bars up into larger
resolutions (5-min entry, 3-min exit, index res, ...) with correct time-window
alignment. Emits ONLY completed, FULL windows as (name, bar_dict) — the first
window a target sees is usually partial (stream started mid-window) and is
skipped so it never injects a wrong OHLC bar into the MACD.

Pure/stateless of IBKR: caller passes an epoch (seconds) parsed from bar.date
plus OHLCV. Window alignment is TZ-agnostic (US/EDT offset and IST are multiples
of 5- and 3-min, so floor-to-window is identical in any of them)."""


def _win_start(epoch, secs):
    return epoch - (epoch % secs)


class BarAggregator:
    def __init__(self, targets=None):
        self.targets = dict(targets or {})          # name -> window_seconds
        self._cur = {}                              # name -> window state dict

    def set_target(self, name, secs):
        self.targets[name] = int(secs)
        self._cur.setdefault(name, None)

    def remove_target(self, name):
        self.targets.pop(name, None)
        self._cur.pop(name, None)

    def on_bar(self, epoch, o, h, l, c, v=0):
        """Feed ONE completed 10s bar. Returns [(name, bar_dict), ...] for every
        target whose window just CLOSED as a full window."""
        epoch = int(epoch)
        out = []
        for name, secs in self.targets.items():
            ws = _win_start(epoch, secs)
            cur = self._cur.get(name)
            if cur is None or ws != cur["start"]:
                if cur is not None and not cur["partial"] and ws != cur["start"]:
                    out.append((name, self._bar(cur)))         # prev full window closed
                # open new window; partial unless this bar sits exactly on the boundary
                self._cur[name] = {"start": ws, "o": o, "h": h, "l": l,
                                   "c": c, "v": v, "partial": epoch != ws}
            else:                                              # extend current window
                cur["h"] = max(cur["h"], h); cur["l"] = min(cur["l"], l)
                cur["c"] = c; cur["v"] += v
        return out

    @staticmethod
    def _bar(w):
        return {"date": w["start"], "open": w["o"], "high": w["h"],
                "low": w["l"], "close": w["c"], "volume": w["v"]}
