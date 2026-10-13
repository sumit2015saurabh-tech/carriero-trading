"""
One-time helper: add MeanCalcStartTime + MeanStartTime columns to the
Stocks and Indexes sheets of strategy_config.xlsx, populating defaults
of 10:30 (calc) and 10:45 (use).

Run once. Idempotent — checks for existing columns by header.
"""
import sys, shutil
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment

XL_PATH = sys.argv[1] if len(sys.argv) > 1 else "strategy_config.xlsx"

DEFAULT_CALC_START = "10:30"
DEFAULT_USE_START  = "10:45"

shutil.copy(XL_PATH, XL_PATH + ".bak.before_meantime")
wb = load_workbook(XL_PATH)

def add_pair(ws, sheet_name: str, start_col: int, banner_text_calc: str,
             banner_text_use: str):
    cur_max = ws.max_column
    # Check if MeanCalcStartTime already exists
    for c in range(1, cur_max + 1):
        h = (ws.cell(row=2, column=c).value or "")
        if "meancalcstart" in str(h).lower().replace(" ", ""):
            print(f"[{sheet_name}] MeanCalcStartTime already present at col {c} — skipping")
            return
    # New columns at cur_max+1, cur_max+2
    c_calc = cur_max + 1
    c_use  = cur_max + 2
    # Row 1 banner (informative)
    ws.cell(row=1, column=c_calc, value=banner_text_calc)
    ws.cell(row=1, column=c_use,  value=banner_text_use)
    # Row 2 column header
    ws.cell(row=2, column=c_calc, value="MeanCalcStartTime")
    ws.cell(row=2, column=c_use,  value="MeanStartTime")
    # Populate defaults for every data row
    for r in range(3, ws.max_row + 1):
        if ws.cell(row=r, column=1).value:                # has a Symbol
            if not ws.cell(row=r, column=c_calc).value:
                ws.cell(row=r, column=c_calc, value=DEFAULT_CALC_START)
            if not ws.cell(row=r, column=c_use).value:
                ws.cell(row=r, column=c_use,  value=DEFAULT_USE_START)
    # Light styling on header row 2
    for c in (c_calc, c_use):
        cell = ws.cell(row=2, column=c)
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")
    print(f"[{sheet_name}] added MeanCalcStartTime at col {c_calc}, "
          f"MeanStartTime at col {c_use}")

add_pair(wb["Stocks"],  "Stocks",
         start_col=23,
         banner_text_calc="Per-stock mean calc start",
         banner_text_use ="Per-stock mean filter start")
add_pair(wb["Indexes"], "Indexes",
         start_col=17,
         banner_text_calc="Per-index mean calc start",
         banner_text_use ="Per-index mean filter start")

wb.save(XL_PATH)
print(f"\nSaved {XL_PATH}.  Backup: {XL_PATH}.bak.before_meantime")
