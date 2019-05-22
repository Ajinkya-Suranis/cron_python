import string
import time
import random
import threading
import queue
import sys
import os
from cron_job import cron_job

sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
import heap
from cron_exceptions import *

SLEEP_TIME = 5

def loop_execute_jobs(args):
    h = args
    wait_time = SLEEP_TIME
    while True:
        # TODO: Do all this checking and updating with lock
        if h.heap_nitems == 0:
            time.sleep(SLEEP_TIME)
            continue
        wait_time = h.heap_current_min - int(time.time())
        if wait_time > 0:
            time.sleep(wait_time)
            continue
        # time to execute the task(s)
        cron_group = h.remove_min()
        for task in cron_group["task_items"]:
            new_th = threading.Thread(target=task.function, args=task.args)
            new_th.daemon = True
            # We really don't care about the exit status of thread.
            new_th.start()
        # We need to recalculate the next run time of the tasks we just
        # removed and reinsert them inside the heap.
        for cronobj in cron_group["task_items"]:
            cronobj.update_next_time()
            # TODO: maintain a hash of epochs for this cron manager
            # so that we don't need to search in the heap
            hcron_group, _index = h.search_heap({"epoch": cronobj.next_time})
            if hcron_group:
                hcron_group["task_items"].append(cronobj)
            else:
                new_cron_group = {}
                new_cron_group["epoch"] = cronobj.next_time
                new_cron_group["task_items"] = [cronobj]
                h.insert_heap(new_cron_group)


class cron_manager:
    def __init__(self):
        self.heap = heap.heap("epoch")
        self.executor_tid = None
        self.mgr_started = False
        self.job_dict = {}

    def new_job(self, func, args=None, minutes=-1, hours=-1, dom=-1, months=-1):
        if not self.mgr_started:
            raise CronManagerNotStarted("The Cron manager wasn't started")
        if minutes == -1:
            minutes = list(range(1, 61))
        if hours == -1:
            hours = list(range(1, 25))
        if dom == -1:
            dom = list(range(1, 32))
        if months == -1:
            months = list(range(1, 13))
        if not (type(minutes) == type(hours) == type(dom) == type(months) == list):
            raise ValueError("time unit should be either -1 or of type list")
        new_cron_job = cron_job(func, args, minutes, hours, dom, months)
        # TODO: Take a lock first before traversing and inserting into heap
        cron_group, _index = self.heap.search_heap({"epoch": new_cron_job.next_time})
        if cron_group:
            # The heap entry for the calculated epoch already exists.
            # We just need to add the new cron to the list.
            cron_group["task_items"].append(new_cron_job)
            self.job_dict[new_cron_job.job_uuid] = cron_group
            return new_cron_job
        new_cron_group = {}
        new_cron_group["epoch"] = new_cron_job.next_time
        new_cron_group["task_items"] = [new_cron_job]
        self.heap.insert_heap(new_cron_group)
        self.job_dict[new_cron_job.job_uuid] = new_cron_group
        return new_cron_job

    def remove_job(self, job_obj):
        if not isinstance(job_obj, cron_job):
            raise BadCronJob("The cron job object isn't valid")
        if job_obj.job_uuid not in self.job_dict:
            raise CronjobNotFound("The cron job with uuid " + job_obj.job_uuid + " not present")
        cron_group = self.job_dict[job_obj.job_uuid]
        assert "epoch" in cron_group, "The 'epoch' field must be present in cron group"
        assert len(cron_group["task_items"]) > 0, "No task present in cron group"
        cron_group["task_items"].remove(job_obj)

        # If no cron jobs remain in the cron group after removal,
        # then remove the group from heap.
        if len(cron_group["task_items"]) == 0:
            self.heap.remove(cron_group)
        # remove the job uuid from the global dict of jobs
        self.job_dict.pop(job_obj.job_uuid)

    def start_cron(self):
        if self.mgr_started:
            raise CronManagerAlreadyStarted("Cron Manager has already started")
        self.executor_tid = threading.Thread(target=loop_execute_jobs, args=[self.heap])
        self.executor_tid.daemon = True
        self.executor_tid.start()
        self.mgr_started = True