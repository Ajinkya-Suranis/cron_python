class CronManagerNotStarted(Exception):
    pass

class CronManagerAlreadyStarted(Exception):
    pass

class HeapEmpty(Exception):
    pass

class BadCronJob(Exception):
    pass

class CronjobNotFound(Exception):
    pass