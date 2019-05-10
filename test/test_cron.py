import sys
import os
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
import cron_manager

def keep_printing(args1, args2):
    with open("C:\\cron_logs.log", "a") as fp:
        fp.write(str(args1) + str(args2) + "\n")

mgr = cron_manager.cron_manager()
mgr.start_cron()
mgr.new_job(keep_printing, ['arg1', 'arg2'], minutes=[1, 3, 10, 35, 40])

while True:
    time.sleep(10)