import cron_manager
import time

def keep_printing(args1, args2):
    with open("C:\\cron_logs.log", "a") as fp:
        fp.write(str(args1) + str((args2)))

mgr = cron_manager.cron_manager()
mgr.start_cron()
mgr.new_job(keep_printing, ['arg1', 'arg2'])

while True:
    time.sleep(10)