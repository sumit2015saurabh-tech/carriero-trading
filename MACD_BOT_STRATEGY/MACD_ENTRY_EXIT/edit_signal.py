import json, shutil, time
tag = time.strftime("%H%M_%d%m")
bak = "config.json.bak.test_xover_" + tag
shutil.copy("config.json", bak)
c = json.load(open("config.json"))
n = 0
def walk(o):
    global n
    if isinstance(o, dict):
        for k, v in list(o.items()):
            if k in ("signal_l", "signal_s") and isinstance(v, (int, float)) and int(v) == 1:
                o[k] = 0; n += 1
            else:
                walk(v)
    elif isinstance(o, list):
        for x in o:
            walk(x)
walk(c)
json.dump(c, open("config.json", "w"), indent=4)
json.load(open("config.json"))     # validate
z = one = 0
def chk(o):
    global z, one
    if isinstance(o, dict):
        for k, v in o.items():
            if k in ("signal_l", "signal_s"):
                z += int(v) == 0; one += int(v) == 1
            else:
                chk(v)
    elif isinstance(o, list):
        [chk(x) for x in o]
chk(json.load(open("config.json")))
print("backup:", bak)
print("changed", n, "signal_l/s 1->0 (crossover mode); final ==0:", z, " ==1:", one)
