#!/usr/bin/env python3
"""
ml_predict.py — load trade_ml_model.joblib and decide "take this trade or not"
from the MACD setup at an entry. FAIL-SAFE: if the model or sklearn can't load,
every call returns take=True (allow) so the live bot never gets blocked by a
broken model. Self-contained (no retraining on import).
"""
import numpy as np

_MODEL = None; _FEATURES = []; _MODEL_NAME = "none"
try:
    import joblib
    _PKG = joblib.load("/home/azureuser/MACD_ENTRY_EXIT/trade_ml_model.joblib")
    _MODEL = _PKG["model"]; _FEATURES = _PKG["features"]; _MODEL_NAME = _PKG.get("model_name", "?")
except Exception as _e:               # missing file / sklearn / corrupt → disabled
    _MODEL = None

def is_ready():
    return _MODEL is not None

def _lag(a, i):
    return a[-1-i] if len(a) > i else (a[0] if len(a) else 0.0)

def predict_from_features(feat):
    if _MODEL is None:
        return {"decision": "ALLOW", "prob_yes": None, "take": True}
    try:
        x = np.array([[float(feat.get(k, 0.0)) for k in _FEATURES]])
        prob = float(_MODEL.predict_proba(x)[0, 1])
        return {"decision": "YES" if prob >= 0.5 else "NO", "prob_yes": round(prob, 4),
                "take": prob >= 0.5}
    except Exception:
        return {"decision": "ALLOW", "prob_yes": None, "take": True}

def predict_from_macd_series(macd_list, sig_list, side, entry_sec):
    """FAST live path. macd_list / sig_list are NEWEST-FIRST (index 0 = current
    bar), as the engine stores them. close_ret is set 0 (not in live state;
    low-importance feature). Returns dict with take=True/False."""
    if _MODEL is None or not macd_list or not sig_list:
        return {"decision": "ALLOW", "prob_yes": None, "take": True}
    try:
        m = [float(x) for x in list(macd_list)[:6]]
        s = [float(x) for x in list(sig_list)[:6]]
        def g(a, i): return a[i] if len(a) > i else (a[0] if a else 0.0)
        m0, m1, m2 = g(m, 0), g(m, 1), g(m, 2)
        s0, s1 = g(s, 0), g(s, 1)
        h0, h1 = m0 - s0, m1 - s1
        feat = {
            "macd": m0, "signal": s0, "hist": h0, "ema_diff": m0,
            "macd_d1": m0 - m1, "macd_d2": (m0 - m1) - (m1 - m2),
            "signal_d1": s0 - s1, "hist_d1": h0 - h1,
            "macd_lag1": m1, "macd_lag2": m2,
            "gap": abs(m0 - s0), "macd_abs": abs(m0), "macd_sign": float(np.sign(m0)),
            "close_ret": 0.0,
            "side_long": 1.0 if str(side).upper() == "LONG" else 0.0,
            "mins_since_open": max(0.0, (entry_sec - (9 * 3600 + 30 * 60)) / 60.0),
            "nbars": float(len(m)),
        }
        return predict_from_features(feat)
    except Exception:
        return {"decision": "ALLOW", "prob_yes": None, "take": True}

# back-compat helper used by tests / batch scoring
def build_features(bars, side, entry_sec):
    b = [x for x in bars if x.get("t", entry_sec) <= entry_sec] if bars else []
    b = b[-6:]
    if not b:
        return None
    macd = [float(x["macd"]) for x in b][::-1]   # newest-first
    sig  = [float(x["signal"]) for x in b][::-1]
    r = predict_from_macd_series(macd, sig, side, entry_sec)
    return r

if __name__ == "__main__":
    print("model ready:", is_ready(), "| name:", _MODEL_NAME)
    demo_m = [0.05, -0.05, -0.15, -0.25, -0.30, -0.32]   # newest-first, fast rising
    demo_s = [-0.10, -0.12, -0.14, -0.16, -0.18, -0.20]
    print("demo LONG @ 12:00:", predict_from_macd_series(demo_m, demo_s, "LONG", 12*3600))
