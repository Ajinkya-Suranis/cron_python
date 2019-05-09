import string
import time
import random
import cron_job
import threading
import queue
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
import heap
import exceptions

SLEEP_TIME = 5

def loop_execute_jobs(args):
    h = args
    wait_time = SLEEP_TIME
    while True:
        # TODO: Do all this checking and updating with lock
        if h.heap_nitems == 0:
            time.sleep(SLEEP_TIME)
            continue
        with open("C:\\cronfile.log", "a") as fp:
            fp.write(str(h.heap_nitems) + "\n")
            fp.write(str(h.heap_current_min))
        wait_time = h.heap_current_min - int(time.time())
        if wait_time > 0:
            time.sleep(wait_time)
            continue
        # time to execute the task(s)
        tasks = h.remove_min()
        for task in tasks["task_items"]:
            new_th = threading.Thread(target=task["function"], args=task["args"])
            new_th.daemon = True
            # We really don't care about the exit status of thread.
            new_th.start()
        # We need to recalculate the next run time of the tasks we just
        # removed and reinsert them inside the heap.
        for task in tasks["task_items"]:
            cronobj = task["obj"]
            cronobj.update_next_time()
            # TODO: maintain a hash of epochs for this cron manager
            # so that we don't need to search in the heap
            cron = h.search_heap({"epoch": cronobj.next_time})
            if cron:
                cron["task_items"].append(task)
            else:
                new_cron_group = {}
                new_cron_group["epoch"] = cronobj.next_time
                new_cron_group["task_items"] = [task]
                h.insert_heap(new_cron_group)
        # after the nearest task(s) are updated, take note of upcoming
        # nearest ones and adjust the next wait time accordingly.
        wait_time = min([h.heap_current_min - int(time.time()), SLEEP_TIME]) \
            if h.heap_current_min != None else SLEEP_TIME
        time.sleep(wait_time)

class cron_manager:
    def __init__(self):
        self.heap = heap.heap("epoch")
        self.executor_tid = None
        self.mgr_started = False

    def new_job(self, func, args=None, minutes=-1, hours=-1, dom=-1, months=-1):
        if not self.mgr_started:
            raise exceptions.CronManagerNotStarted("The Cron manager wasn't started")
        if minutes == -1:
            minutes = list(range(1, 61))
        if hours == -1:
            hours = list(range(1, 25))
        if dom == -1:
            dom = list(range(1, 32))
        if months == -1:
            months = list(range(1, 13))
        if not (type(minutes) == type(hours) == type(dom) == type(months) == list):
            raise ValueError("Type of time unit should be either -1 or list")
        # Generate unique identifier for the new job.
        job_uuid = ''.join([random.choice(string.ascii_letters + string.digits) \
                        for n in range(8)])
        job_spec = {}
        job_spec["uuid"] = job_uuid
        new_cron_job = cron_job.cron_job(minutes, hours, dom, months)
        job_spec["obj"] = new_cron_job
        job_spec["function"] = func
        job_spec["args"] = args
        # TODO: Take a lock first before traversing and inserting into heap
        cron = self.heap.search_heap({"epoch": new_cron_job.next_time})
        if cron:
            # The heap entry for the calculated epoch already exists.
            # We just need to add the new cron to the list.
            cron["task_items"].append(job_spec)
            return job_spec
        new_cron_group = {}
        new_cron_group["epoch"] = new_cron_job.next_time
        new_cron_group["task_items"] = [job_spec]
        self.heap.insert_heap(new_cron_group)
        return job_spec

    def start_cron(self):
        if self.mgr_started:
            raise exceptions.CronManagerAlreadyStarted("Cron Manager has already started")
        self.executor_tid = threading.Thread(target=loop_execute_jobs, args=[self.heap])
        self.executor_tid.daemon = True
        self.executor_tid.start()
        self.mgr_started = True