import time
import random
import string

CRON_UUID_NCHAR = 8
days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
is_leap_year = lambda year: year % 400 == 0 or year % 4 == 0 and year % 100 != 0

def get_month_days(year, month):
    return days_in_month[month - 1] if month != 2 or not is_leap_year(year) \
                else days_in_month[month - 1] + 1

class cron_job:
    def __init__(self, func, args, every_seconds, minutes, hours, dom, months):
        self.function = func
        self.args = args
        self.job_uuid = self.generate_uuid()
        self.schedule_units = {}
        self._set_sched_units(every_seconds, minutes, hours, dom, months)
        self.next_time = int(time.time()/60) * 60 if every_seconds == None else int(time.time())
        self.gen_count = 1
        self.update_next_time(adjust_seconds=True)

    def _set_sched_units(self, every_seconds, minutes, hours, dom, months):
        if minutes == -1:
            minutes = list(range(0, 60))
        if hours == -1:
            hours = list(range(0, 24))
        if dom == -1:
            dom = list(range(1, 32))
        if months == -1:
            months = list(range(1, 13))
        if not (type(minutes) == type(hours) == type(dom) == type(months) == list):
            raise ValueError("time unit should be either -1 or of type list")
        self.schedule_units["every_seconds"] = every_seconds
        self.schedule_units["minutes"] = minutes
        self.schedule_units["hours"] = hours
        self.schedule_units["dom"] = dom
        self.schedule_units["months"] = months

    def generate_uuid(self):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) \
                        for _ in range(CRON_UUID_NCHAR))

    # Modify the schedule of job.
    # We allow modification of the schedule units only,
    # not other parameters like function, args.
    # If they need to be modified then better
    # create a new job.
    # TODO: do this after taking an appropriate lock
    def modify_schedule(self, every_seconds, minutes, hours, dom, months):
        self._set_sched_units(every_seconds, minutes, hours, dom, months)
        self.next_time = int(time.time()/60) * 60 if every_seconds == None else int(time.time())
        self.gen_count += 1
        self.update_next_time(adjust_seconds=True)

    def _get_diff(self, unit_value):
        ltime = time.localtime(self.next_time)
        if unit_value == "minutes":
            val = ltime.tm_min
            mult_factor = 60
            max_val = 60
        elif unit_value == "hours":
            val = ltime.tm_hour
            mult_factor = 3600
            max_val = 24
        elif unit_value == "dom":
            val = ltime.tm_mday
            mult_factor = 86400
            max_val = days_in_month[ltime.tm_mon - 1] + 1 if ltime.tm_mon == 2 and \
                is_leap_year(ltime.tm_year) else days_in_month[ltime.tm_mon]
        elif unit_value == "months":
            val = ltime.tm_mon
            # TODO: This is variable. Need to revisit this.
            mult_factor = 2592000
            max_val = 12
        for i in self.schedule_units[unit_value]:
            if val <= i:
                return True, ((i - val) * mult_factor)
        else:
            return False, (max_val - val + self.schedule_units[unit_value][0]) * mult_factor

    def update_next_time(self, adjust_seconds=False):
        prev_next_time = self.next_time
        if self.schedule_units["every_seconds"] != None:
            self.next_time = self.next_time + self.schedule_units["every_seconds"]
        else:
            self.next_time = self.next_time + 60
        extra_seconds = self.next_time - (int(self.next_time/60) * 60)
        for unit in self.schedule_units:
            if unit != "every_seconds":
                done, diff = self._get_diff(unit)
                self.next_time += diff
                if self.schedule_units["every_seconds"] != None and unit == "minutes" \
                            and self.next_time - prev_next_time >= 60 and adjust_seconds == True:
                    self.next_time -= extra_seconds
                if done:
                    return