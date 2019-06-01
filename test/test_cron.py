import sys
import os
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
import cron_manager

def keep_printing1(args1, args2):
    with open("C:\\cron_logs1.log", "a") as fp:
        fp.write(str(args1) + str(args2) + "\n")

def keep_printing2(args1, args2):
    with open("C:\\cron_logs2.log", "a") as fp:
        fp.write(str(args1) + str(args2) + "\cmdn")

def keep_printing3(args1, args2):
    with open("C:\\cron_logs3.log", "a") as fp:
        fp.write(str(args1) + str(args2) + "\n")

mgr = cron_manager.cron_manager()
mgr.start_cron()
j1 = mgr.new_job(keep_printing1, ['arg1', 'arg2'], every_seconds=2, minutes=-1)
j2 = mgr.new_job(keep_printing2, ['arg3', 'arg4'], minutes=-1)
j3 = mgr.new_job(keep_printing3, ['arg5', 'arg6'], minutes=-1)

while True:
    time.sleep(10)