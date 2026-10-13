from fpdf import FPDF

OUT = "/home/azureuser/MACD_ENTRY_EXIT/regime_strategy.pdf"
NAVY = (31, 78, 120); TEAL = (13, 120, 110)

class PDF(FPDF):
    def footer(self):
        self.set_y(-15); self.set_font("Helvetica", "I", 8); self.set_text_color(150)
        self.cell(0, 10, f"Regime-Gated MACD Strategy  -  page {self.page_no()}", align="C")

def H1(t):
    pdf.set_x(pdf.l_margin); pdf.set_font("Helvetica", "B", 19); pdf.set_text_color(*NAVY); pdf.multi_cell(0, 8.5, t); pdf.ln(1)
def H2(t):
    pdf.ln(2); pdf.set_x(pdf.l_margin); pdf.set_font("Helvetica", "B", 13); pdf.set_text_color(*TEAL); pdf.multi_cell(0, 7, t); pdf.ln(1)
def P(t):
    pdf.set_x(pdf.l_margin); pdf.set_font("Helvetica", "", 11); pdf.set_text_color(40); pdf.multi_cell(0, 5.5, t); pdf.ln(1)
def B(items):
    pdf.set_font("Helvetica", "", 11); pdf.set_text_color(40)
    for it in items:
        pdf.set_x(pdf.l_margin); pdf.multi_cell(0, 5.5, "   -  " + it)
    pdf.ln(1)

pdf = PDF(); pdf.set_auto_page_break(True, 18); pdf.add_page()

H1("Regime-Gated MACD Strategy")
pdf.set_x(pdf.l_margin); pdf.set_font("Helvetica", "", 11); pdf.set_text_color(110)
pdf.multi_cell(0, 5.5, "MACD entry with an ML approval gate, and an exit that switches between a volatility trail and a fast momentum exit based on whether the stock is trending or chopping. 3-minute bars, trading window 9:30 am to 3:50 pm EST, flat by 3:50 pm.")
pdf.ln(2)

H2("1. Indicators used")
B([
 "MACD (12, 26, 9): fast line = EMA12 - EMA26 of close; signal = EMA9 of the fast line. Drives entries and the choppy-market exit.",
 "ATR (14): the stock's typical move per bar. Sizes the trending-market trailing stop.",
 "Efficiency Ratio (ER): the trend test. ER = net distance moved over the last 10 bars divided by the total path length. Near 1 = clean trend, near 0 = chop.",
 "ML model (trade_ml_model.joblib): a take-or-skip classifier trained on labelled trades, used as an entry gate from 11:30 to 14:00.",
])

H2("2. Entry rules (LONG; SHORT is the mirror)")
P("A LONG signal forms when EITHER trigger fires:")
B([
 "PATTERN: the MACD fast line turns up (macd > previous macd) while still BELOW its signal line. Fires irrespective of the zero line.",
 "CROSS: the fast line crosses up through the signal line AND the mean gate passes (MACD-long-mean >= MeanMultiplier x signal).",
])
P("ML GATE: if the signal occurs between 11:30 and 14:00, the trained model must also approve it (predict take = yes); otherwise the entry is skipped. Outside that window the model is not consulted.")

H2("3. The regime test (which exit to use)")
P("On every bar while in a trade, the Efficiency Ratio decides the mode:")
B([
 "ER >= 0.40  ->  TRENDING  ->  exit with the ATR chandelier trail.",
 "ER <  0.40  ->  CHOPPY    ->  exit with delta/gamma + the slope gate.",
 "LATCHING: once a trade becomes TRENDING it STAYS on the ATR exit for the rest of that trade (a temporary pullback does not flip it back). The mode resets fresh on the next new entry.",
])

H2("4. Exit rules")
P("TRENDING mode (ATR chandelier):")
B([
 "Stop ratchets to (highest price since entry) minus 3 x ATR, moving only in your favour. A normal pullback does not exit; only a real 3 x ATR reversal off the high does.",
])
P("CHOPPY mode (delta/gamma + slope):")
B([
 "DELTA/GAMMA exit: close immediately when the fast line rolls over (delta < 0 and gamma < 0 for a long). Before the trade reaches 3 x MPL of profit, this only fires if the rollover is at least as steep as the entry move (|slope now| >= |slope at entry|); past 3 x MPL it fires on any rollover.",
 "MPL-zone trailing stop: in profit the stop trails 0.5 x MPL per 1 x MPL up to 3 x MPL, then 1 x MPL per 1 x MPL.",
])
P("ALWAYS active in both modes: the initial hard stop at entry minus MPL, and the end-of-day flat.")

pdf.add_page()
H2("5. Exit reason labels (in the Excel)")
B([
 "ATR trail stop  -  the trending-mode chandelier stop was hit.",
 "Delta/gamma exit  -  the choppy-mode momentum-rollover exit fired.",
 "MPL trail stop  -  the choppy-mode profit trail stop was hit.",
 "Stop loss  -  the initial hard stop was hit before any trail moved up.",
 "End of day flat  -  the position was closed at the session's last bar.",
])

H2("6. Position sizing")
B([
 "Each stock uses its own MPL (0.5 to 2.0 dollars) and share quantity from the strategy Excel.",
 "One open position per stock at a time; re-entry allowed on the next valid signal.",
])

H2("7. 7-day backtest result (real IBKR data, all 34 stocks, ML gate ON)")
B([
 "This strategy (9:30 am to 3:50 pm EST, ML gate ON): +184,587 dollars, 1,670 trades.",
 "For reference (same 7 days): pure delta/gamma + slope +108,790 ; pure ATR chandelier +200,719 ; regime-gated without ML +179,150.",
 "Exit-reason counts: ATR trail 694, delta/gamma 306, stop loss 471, end-of-day 129, MPL trail 70.",
])
P("Note: the pure ATR chandelier still scored highest on this sample because it never gives up on a trend, but it bleeds on choppy names; the regime-gated version trades a little total return for lower variance and chop protection. The ML gate slightly reduced the total here (it thinned some 11:30-2 entries, including a few winners).")
P("Caveat: idealised backtest - no slippage, no commissions, no missed fills, no order delays. The real-money result will be lower. Treat as a relative comparison, not a promise.")

H2("8. Accompanying file")
P("trades_regime_7d.xlsx contains every entry and exit from this strategy over the 7 days, one sheet per stock, sorted by time, with date, side, entry and exit time, entry and exit price, P&L, and the exit reason.")

pdf.output(OUT)
print("Saved", OUT)
