import time

days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
is_leap_year = lambda year: year % 400 == 0 or year % 4 == 0 and year % 100 != 0

def get_month_days(year, month):
    return days_in_month[month - 1] if month != 2 or not is_leap_year(year) \
                else days_in_month[month - 1] + 1

class cron_job:
    def __init__(self, minutes, hours, dom, months):
        self.schedule_units = {}
        self.schedule_units["minutes"] = minutes
        self.schedule_units["hours"] = hours
        self.schedule_units["dom"] = dom
        self.schedule_units["months"] = months
        self.next_time = int(time.time())
        self.next_time = self.get_next_time()

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

    def get_next_time(self):
        self.next_time = self.next_time + 60
        for unit in self.schedule_units:
            done, diff = self._get_diff(unit)
            self.next_time += diff
            if done:
                return self.next_time
        return self.next_time