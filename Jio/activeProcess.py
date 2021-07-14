import psutil
from revolveSolutions.main import  Enrichment
import json
with open("F:/Interview/revolve Solutions/customerJob/customer.json", "r") as conf:
    conf = json.load(conf)
print(Enrichment(conf))
def ch(pid):
    if pid in psutil.pids():
        print("active")
    else:
        print("not active")