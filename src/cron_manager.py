import string
import random
import cron_job
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "lib"))
import heap

class cron_manager:
    def __init__(self):
        self.heap = heap.heap("epoch")

    def new_job(self, minutes=-1, hours=-1, dom=-1, months=-1):
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
        # TODO: Take a lock first before traversing and inserting into heap
        cron = heap.search_heap(new_cron_job.next_time)
        if cron:
            # The heap entry for the calculated epoch already exists.
            # We just need to add the new cron to the list.
            cron["items"].append(job_spec)
            return
        new_cron_group = {}
        new_cron_group["epoch"] = new_cron_job.next_time
        new_cron_group["items"] = [job_spec]
        heap.insert_heap(new_cron_group)