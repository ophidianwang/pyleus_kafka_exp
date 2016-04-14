# -*- coding: utf-8 -*-
"""
References:
    https://docs.python.org/2/library/subprocess.html
"""

import os
import re
import time
from datetime import datetime
import subprocess
from pymongo import MongoClient

# client = MongoClient(['172.17.24.217', '172.17.24.218', '172.17.24.219'], 40000)
client = MongoClient(['172.17.24.211'], 27017)
db = client.cep_storm
collection = db.pyleus_result

topology_path = os.path.dirname(os.path.abspath(__file__)) + "/"
submit_sh = topology_path + "cluster_run.sh"
kill_sh = topology_path + "cluster_kill.sh"

# kill topology
subprocess.call(["bash", kill_sh])
time.sleep(15)
# to make sure topology is over
print("kill topology done at {0}.".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

# clear mongodb records
collection.delete_many({})
print("clear collection done at {0}.".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
check_count = collection.count()
if check_count != 0:
    print("error happened, collection.count() is not 0")
    raise SystemExit("mongodb error")

# start topology
subprocess.call(["bash", submit_sh])
print("submit topology done at {0}.".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
time.sleep(5)  # topology deploying
print("start monitoring mongodb at {0}.".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

# then check mongodb, out input data should be remodeled from 1085420 to 570720
row_count = 0
row_progress = 0
dump_start_timestamp = 0
over_counter = 0
diff_threshold = 1000
tolerance_count = 60
sleep_time = 0.5
tolerance_time = tolerance_count * sleep_time
while True:
    row_count = collection.count()  # get current row count
    if dump_start_timestamp == 0 and row_count != 0:
        dump_start_timestamp = time.time()  # mark timestamp which means start writing
    diff = row_count - row_progress
    if diff > 1000:
        print("There are {0} records in mongodb now.".format(row_count))
        row_progress = row_count
        over_counter = 0
    elif row_count != 0:
        over_counter += 1
        if over_counter > 50:
            print("over_counter: {0}".format(over_counter))

    if over_counter >= tolerance_count:
        break
    time.sleep(sleep_time)  # in case too many queries affect mongodb performance...

dump_end_timestamp = time.time() - tolerance_time
print("Bolt start dumping records at {0}".format(datetime.fromtimestamp(dump_start_timestamp).strftime(
        "%Y-%m-%d %H:%M:%S")))
print("Bolt finish dumping records at {0}".format(datetime.fromtimestamp(dump_end_timestamp).strftime(
        "%Y-%m-%d %H:%M:%S")))
print("===")
print("{0} records".format(row_count))

print("Bolt spend {0} seconds dumping {1} records.".format(dump_end_timestamp - dump_start_timestamp, row_count))