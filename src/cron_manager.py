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

SEC_NEEDS_SCHEDULE = 1
SEC_CRON_REMOVE = 2
SEC_CRON_PAUSE = 3

def schedule_seconds_jobs(mgr_obj):
    if mgr_obj.seconds_job_dict == {}:
        return
    for job_uuid in list(mgr_obj.seconds_job_dict):
        # If the job status is SEC_CRON_REMOVE, then the cron job is
        # scheduled to be removed while it was waiting to be
        # scheduled. Remove the job entry from global seconds dict.
        # TODO: Do this removal inside a lock.
        job_details = mgr_obj.seconds_job_dict[job_uuid]
        if job_details["status"] == SEC_CRON_REMOVE:
            mgr_obj.seconds_job_dict.pop(job_details["job_obj"].job_uuid)
        elif job_details["status"] == SEC_NEEDS_SCHEDULE:
            job_obj = job_details["job_obj"]
            cron_group, _index = mgr_obj.heap.search_heap({"epoch": job_obj.next_time})
            if cron_group:
                # The heap entry for the calculated epoch already exists.
                # We just need to add the new cron to the list.
                cron_group["task_items"].append(job_obj)
                job_details["status"] = None
                continue
            new_cron_group = {}
            new_cron_group["epoch"] = job_obj.next_time
            new_cron_group["task_items"] = [job_obj]
            mgr_obj.heap.insert_heap(new_cron_group)
            job_details["status"] = None


def loop_execute_jobs(mgr_obj):
    h = mgr_obj.heap
    wait_time = SLEEP_TIME
    while True:
        schedule_seconds_jobs(mgr_obj)
        # TODO: Do all this checking and updating with lock
        if h.heap_nitems == 0:
            time.sleep(SLEEP_TIME)
            continue
        wait_time = h.heap_current_min - int(time.time())
        if wait_time > 0:
            if wait_time > SLEEP_TIME:
                wait_time = SLEEP_TIME
            time.sleep(wait_time)
            continue
        # time to execute the task(s)
        cron_group = h.remove_min()
        for task in cron_group["task_items"]:
            if task.schedule_units["every_seconds"] != None:
                # TODO: put an assert here
                mgr_obj.seconds_job_dict[task.job_uuid] = {"job_obj": task, "status": None}
                sec_thread = threading.Thread(target=handle_seconds_job, args=[mgr_obj, task])
                sec_thread.daemon = True
                sec_thread.start()
                continue
            new_th = threading.Thread(target=task.function, args=task.args)
            new_th.daemon = True
            # We really don't care about the exit status of thread.
            new_th.start()
        # We need to recalculate the next run time of the tasks we just
        # removed and reinsert them inside the heap.
        for cronobj in cron_group["task_items"]:
            if cronobj.schedule_units["every_seconds"] != None:
                continue
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


def handle_seconds_job(mgr_obj, job_obj):
    wait_time = job_obj.next_time - int(time.time())
    assert wait_time < 60, "The time difference should be" \
        " less than 60 seconds"
    assert job_obj.schedule_units["every_seconds"] != None, "'every_seconds' parameter" \
        " value is None"
    while True:

        # If the flag SEC_CRON_REMOVE is set, then job is planned to be removed.
        # Thread should exit in that case.
        # TODO: Do this checking and setting SEC_NEEDS_SCHEDULE inside a lock
        # (which is supposed to protect the 'seconds_job_dict' dictionary).
        # Otherwise a race condition would occur where the SEC_CRON_REMOVE flag
        # is overwritten by SEC_NEEDS_SCHEDULE and we'll completely miss the
        # cron job removal request!

        if mgr_obj.seconds_job_dict[job_obj.job_uuid]["status"] == SEC_CRON_REMOVE:
            mgr_obj.seconds_job_dict.pop(job_obj.job_uuid)
            return
        job_thr = threading.Thread(target=job_obj.function, args=job_obj.args)
        job_thr.daemon = True
        job_thr.start()
        job_obj.update_next_time()
        wait_time = job_obj.next_time - int(time.time())
        if wait_time >= 60:
            assert job_obj.job_uuid in mgr_obj.seconds_job_dict, "Job not found in dictionary"
            mgr_obj.seconds_job_dict[job_obj.job_uuid]["status"] = SEC_NEEDS_SCHEDULE
            return
        time.sleep(job_obj.schedule_units["every_seconds"])


class cron_manager:
    def __init__(self):
        self.heap = heap.heap("epoch")
        self.seconds_job_dict = {}
        self.executor_tid = None
        self.mgr_started = False

    def new_job(self, func, args=None, every_seconds=None, minutes=-1, hours=-1, \
                dom=-1, months=-1):
        if not self.mgr_started:
            raise CronManagerNotStarted("The Cron manager wasn't started")
        new_cron_job = cron_job(func, args, every_seconds, minutes, hours, dom, months)
        # If the job is to be executed every 'n' seconds (i.e. 'every_seconds' parameter
        # is non-None), then either a thread is created for execution or it's added
        # to the heap, depending on its next execution time.
        diff = new_cron_job.next_time - int(time.time())
        if diff < 60:
            self.seconds_job_dict[new_cron_job.job_uuid] = {"job_obj": new_cron_job, "status": None}
            sec_thread = threading.Thread(target=handle_seconds_job, args=[self, new_cron_job])
            sec_thread.daemon = True
            sec_thread.start()
            return new_cron_job
        # TODO: Take a lock first before traversing and inserting into heap
        cron_group, _index = self.heap.search_heap({"epoch": new_cron_job.next_time})
        if cron_group:
            # The heap entry for the calculated epoch already exists.
            # We just need to add the new cron to the list.
            cron_group["task_items"].append(new_cron_job)
            return new_cron_job
        new_cron_group = {}
        new_cron_group["epoch"] = new_cron_job.next_time
        new_cron_group["task_items"] = [new_cron_job]
        self.heap.insert_heap(new_cron_group)
        return new_cron_job

    def modify_job(self, job_obj, minutes=-1, hours=-1, dom=-1, months=-1):
        if not isinstance(job_obj, cron_job):
            raise BadCronJob("The cron job object isn't valid")
        cron_group, _index = self.heap.search_heap({"epoch": job_obj.next_time})
        cron_group["task_items"].remove(job_obj)
        if len(cron_group["task_items"]) == 0:
            # The cron group became empty after removing
            # the job. Need to remove it from heap as well.
            self.heap.remove(cron_group)
        job_obj.modify_schedule(minutes, hours, dom, months)
        cron_group, _index = self.heap.search_heap({"epoch": job_obj.next_time})
        if cron_group:
            # The heap entry for the calculated epoch already exists.
            # We just need to add the new cron to the list.
            cron_group["task_items"].append(job_obj)
            return
        new_cron_group = {}
        new_cron_group["epoch"] = job_obj.next_time
        new_cron_group["task_items"] = [job_obj]
        self.heap.insert_heap(new_cron_group)

    def remove_job(self, job_obj):
        if not isinstance(job_obj, cron_job):
            raise BadCronJob("The cron job object isn't valid")
        cron_group, _index = self.heap.search_heap({"epoch": job_obj.next_time})
        if cron_group == None:
            # The cron job isn't present in the heap. It must be
            # present in the seconds jobs dict.
            # TODO: Set the SEC_CRON_REMOVE inside a lock to avoid race condition.
            assert job_obj.job_uuid in self.seconds_job_dict, "Cron job with uuid " + \
                job_obj.job_uuid + " not found!"
            self.seconds_job_dict[job_obj.job_uuid]["status"] = SEC_CRON_REMOVE
            return
        cron_group["task_items"].remove(job_obj)

        # If no cron jobs remain in the cron group after removal,
        # then remove the group from heap.
        if len(cron_group["task_items"]) == 0:
            self.heap.remove(cron_group)

    def start_cron(self):
        if self.mgr_started:
            raise CronManagerAlreadyStarted("Cron Manager has already started")
        self.executor_tid = threading.Thread(target=loop_execute_jobs, args=[self])
        self.executor_tid.daemon = True
        self.executor_tid.start()
        self.mgr_started = True