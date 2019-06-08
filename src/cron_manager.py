import time
import random
import threading
import sys
import os
from cron_job import cron_job

sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
import heap
from cron_exceptions import *

SLEEP_TIME = 5

# Cron job states.
# Only applicable to the jobs which are scheduled at second-level intervals.
# Could be in only one state at a time. (That's why sequential numbers are used)

SEC_CRON_NEEDS_SCHEDULE = 1
SEC_CRON_PAUSE = 2
SEC_CRON_SCHEDULED = 3
SEC_CRON_RUNNING = 4
SEC_CRON_RESCHEDULING = 5

def schedule_seconds_jobs(mgr_obj):
    if mgr_obj.seconds_job_dict == {}:
        return
    for job_uuid in list(mgr_obj.seconds_job_dict):
        job_details = mgr_obj.seconds_job_dict[job_uuid]
        if job_details["state"] == SEC_CRON_NEEDS_SCHEDULE:
            job_obj = job_details["job_obj"]
            cron_group, _index = mgr_obj.heap.search_heap({"epoch": job_obj.next_time})
            if cron_group:
                # The heap entry for the calculated epoch already exists.
                # We just need to add the new cron to the list.
                cron_group["task_items"].append(job_obj)
                job_details["state"] = SEC_CRON_SCHEDULED
                continue
            new_cron_group = {}
            new_cron_group["epoch"] = job_obj.next_time
            new_cron_group["task_items"] = [job_obj]
            mgr_obj.heap.insert_heap(new_cron_group)
            job_details["state"] = SEC_CRON_SCHEDULED


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
                mgr_obj.seconds_job_dict[task.job_uuid] = \
                                {"job_obj": task, "state": SEC_CRON_RUNNING}
                sec_thread = threading.Thread(target=handle_seconds_job, \
                                args=[mgr_obj, task, task.gen_count])
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


def handle_seconds_job(mgr_obj, job_obj, gen_count):
    wait_time = job_obj.next_time - int(time.time())
    assert wait_time < 60, "The time difference should be" \
        " less than 60 seconds. It is " + str(wait_time)
    assert job_obj.schedule_units["every_seconds"] != None, "'every_seconds' parameter" \
        " value is None"
    # Set the cron job state to running.
    # TODO: inside a lock please! Also, before directly setting to the running
    # state, check whether it's marked for removal. If so, we've nothing to do here..
    if not job_obj.job_uuid in mgr_obj.seconds_job_dict:
        return
    # It is possible that between the time the the thread was created and
    # the time it actually starts to execute a job, the request for
    # modification of the same job was executed and hence job generation
    # number would have changed. In this case, however rare, the current
    # thread should just exit.
    if job_obj.gen_count != gen_count:
        return
    mgr_obj.seconds_job_dict[job_obj.job_uuid]["state"] = SEC_CRON_RUNNING
    time.sleep(wait_time)

    while True:
        if not job_obj.job_uuid in mgr_obj.seconds_job_dict:
            return
        if job_obj.gen_count != gen_count:
            # The cron job is modified. This thread should no longer
            # continue executing the old schedule.
            # TODO: Do the checking inside a lock.
            with open("C:\\threxit.txt", "a") as fp:
                fp.write("Thread Exiting due to change in gencount\n")
            return
        job_thr = threading.Thread(target=job_obj.function, args=job_obj.args)
        job_thr.daemon = True
        job_thr.start()
        job_obj.update_next_time()
        wait_time = job_obj.next_time - int(time.time())
        if wait_time >= 60:
            assert job_obj.job_uuid in mgr_obj.seconds_job_dict, "Job not found in dictionary"
            mgr_obj.seconds_job_dict[job_obj.job_uuid]["state"] = SEC_CRON_NEEDS_SCHEDULE
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
        if every_seconds != None and diff < 60:
            self.seconds_job_dict[new_cron_job.job_uuid] = \
                        {"job_obj": new_cron_job, "state": SEC_CRON_RUNNING}
            sec_thread = threading.Thread(target=handle_seconds_job, \
                        args=[self, new_cron_job, new_cron_job.gen_count])
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


    def _modify_sec2sec(self, job_obj, every_seconds, minutes, hours, dom, months):
        # First check the state of the job. If it's in scheduled state, it means
        # it's in the heap. We need to remove it from heap.
        if self.seconds_job_dict[job_obj.job_uuid]["state"] == SEC_CRON_SCHEDULED:
            cron_group, _index = self.heap.search_heap({"epoch": job_obj.next_time})
            assert cron_group != None, "Job not found in the heap"
            cron_group["task_items"].remove(job_obj)
            if len(cron_group["task_items"]) == 0:
                # The cron group became empty after removing
                # the job. Need to remove it from heap as well.
                self.heap.remove(cron_group)
            self.seconds_job_dict[job_obj.job_uuid]["state"] = SEC_CRON_RESCHEDULING
        job_obj.modify_schedule(every_seconds, minutes, hours, dom, months)
        wait_time = job_obj.next_time - int(time.time())
        if wait_time < 60:
            # The next run time of this modified cron job is less than
            # a minute away. Hence create a thread which would execute
            # the function iteratively.
            self.seconds_job_dict[job_obj.job_uuid]["state"] = SEC_CRON_RUNNING
            sec_thread = threading.Thread(target=handle_seconds_job, \
                        args=[self, job_obj, job_obj.gen_count])
            sec_thread.daemon = True
            sec_thread.start()
        else:
            # The time difference is more than 60 seconds. CHange the state
            # of task appropriately so that the scheduler will schedule it.
            self.seconds_job_dict[job_obj.job_uuid]["state"] = SEC_CRON_NEEDS_SCHEDULE


    def _modify_sec2nonsec(self, job_obj, minutes, hours, dom, months):
        # If the job is scheduled, it needs to be removed from heap.
        if self.seconds_job_dict[job_obj.job_uuid]["state"] == SEC_CRON_SCHEDULED:
            cron_group, _index = self.heap.search_heap({"epoch": job_obj.next_time})
            assert cron_group != None, "Job not found in the heap"
            cron_group["task_items"].remove(job_obj)
            if len(cron_group["task_items"]) == 0:
                # The cron group became empty after removing
                # the job. Need to remove it from heap as well.
                self.heap.remove(cron_group)
        # Delete the dictionary entry corresponding to the current job.
        # TODO: Do this removal inside a lock.
        self.seconds_job_dict.pop(job_obj.job_uuid)
        job_obj.modify_schedule(None, minutes, hours, dom, months)
        # Reinsert the cron job object into heap.
        cron_group, _index = self.heap.search_heap({"epoch": job_obj.next_time})
        if cron_group:
            # The heap entry for the calculated epoch already exists.
            # We just need to add the new cron to the list.
            cron_group["task_items"].append(job_obj)
        else:
            new_cron_group = {}
            new_cron_group["epoch"] = job_obj.next_time
            new_cron_group["task_items"] = [job_obj]
            self.heap.insert_heap(new_cron_group)


    def _modify_from_nonsec(self, job_obj, every_seconds, minutes, hours, dom, months):
        # First see whether we're converting from non-seconds to seconds or
        # non-seconds to non-seconds.
        assert job_obj.schedule_units["every_seconds"] == None, "_modify_from_nonsec: " \
                                "Attempt to modify invalid job schedule"
        assert job_obj.job_uuid not in self.seconds_job_dict, \
                        "Job found unexpectedly in seconds dictionary"
        cron_group, _index = self.heap.search_heap({"epoch": job_obj.next_time})
        if cron_group == None:
            raise CronjobNotFound("Cron job not found")
        cron_group["task_items"].remove(job_obj)
        if len(cron_group["task_items"]) == 0:
            # The cron group became empty after removing
            # the job. Need to remove it from heap as well.
            self.heap.remove(cron_group)
        job_obj.modify_schedule(every_seconds, minutes, hours, dom, months)
        # If the modified job is non-seconds, it needs to be added to heap.
        if every_seconds == None:
            cron_group, _index = self.heap.search_heap({"epoch": job_obj.next_time})
            if cron_group:
                # The heap entry for the calculated epoch already exists.
                # We just need to add the new cron to the list.
                cron_group["task_items"].append(job_obj)
            else:
                new_cron_group = {}
                new_cron_group["epoch"] = job_obj.next_time
                new_cron_group["task_items"] = [job_obj]
                self.heap.insert_heap(new_cron_group)
        else:
            # Converting from non-seconds to seconds!
            # If the next run time is >=60 seconds away, then we
            # weed to create a dictionary entry for modified job
            # and set its status to SEC_CRON_NEEDS_SCHEDULE, so that
            # the scheduler will pick it up.
            # Otherwise, create the execution thread right away.
            # TODO: Do this insertion within lock.
            if job_obj.next_time - int(time.time()) < 60:
                self.seconds_job_dict[job_obj.job_uuid] = \
                        {"job_obj": job_obj, "status": SEC_CRON_RUNNING}
                sec_thread = threading.Thread(target=handle_seconds_job, \
                        args=[self, job_obj, job_obj.gen_count])
                sec_thread.daemon = True
                sec_thread.start()
            else:
                self.seconds_job_dict[job_obj.job_uuid] = \
                            {"job_obj": job_obj, "status": SEC_CRON_NEEDS_SCHEDULE}


    def modify_job(self, job_obj, every_seconds=None, minutes=-1, hours=-1, dom=-1, months=-1):
        if not isinstance(job_obj, cron_job):
            raise BadCronJob("The cron job object isn't valid")
        if job_obj.schedule_units["every_seconds"] != None:
            # We're converting seconds-job to either seconds or non-seconds.
            if every_seconds != None:
                # seconds to seconds!
                self._modify_sec2sec(job_obj, every_seconds, minutes, hours, dom, months)
            else:
                # seconds to non-seconds!
                self._modify_sec2nonsec(job_obj, minutes, hours, dom, months)
        else:
            # Converting the non-seconds job into seconds or non-seconds one.
            self._modify_from_nonsec(job_obj, every_seconds, minutes, hours, dom, months)
        cron_group, _index = self.heap.search_heap({"epoch": job_obj.next_time})
        cron_group["task_items"].remove(job_obj)
        if len(cron_group["task_items"]) == 0:
            # The cron group became empty after removing
            # the job. Need to remove it from heap as well.
            self.heap.remove(cron_group)
        job_obj.modify_schedule(every_seconds, minutes, hours, dom, months)
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
            assert job_obj.job_uuid in self.seconds_job_dict, "Cron job with uuid " + \
                job_obj.job_uuid + " not found!"
            self.seconds_job_dict.pop(job_obj.job_uuid)
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