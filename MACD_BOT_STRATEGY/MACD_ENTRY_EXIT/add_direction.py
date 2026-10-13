import openpyxl, shutil, time
from openpyxl.styles import Alignment
P="strategy_config.xlsx"
bak=P+".bak."+time.strftime("%H%M")+"_1006_direction"
shutil.copy(P,bak)
wb=openpyxl.load_workbook(P)
ws=wb["Stocks"]
COL=28  # AB
ws.cell(1,COL).value="Direction"
ws.cell(2,COL).value="L/S/B"
ws.column_dimensions["AB"].width=16
n=0
last=max((r for r in range(3,ws.max_row+1) if ws.cell(r,1).value not in (None,"")), default=2)
# default B for every named-symbol row; also fill the formatted buffer up to last+100 so new rows inherit
for r in range(3, max(last, ws.max_row)+1):
    has_sym = ws.cell(r,1).value not in (None,"")
    if has_sym:
        ws.cell(r,COL).value="B"; n+=1
    ws.cell(r,COL).alignment=Alignment(horizontal="center", vertical="center")
wb.save(P)
print("backup:",bak)
print("Direction column added at AB; set 'B' on", n, "named stock rows")
