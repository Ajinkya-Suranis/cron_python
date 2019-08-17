# cron_python
Cron Job implementation in python

* cron_manager: The class to be imported for using cron job functionality.
It's methods are used to add, remove and modify the cron jobs.

* cron_manager class methods:

1) start_cron():
Start the cron manager. It needs to be called immediately after creating the cron_manager instance. It's a pre-requisite to all other functions.

2) new_job(function, args[, every_seconds=None, minutes=-1, hours=-1, dom=-1, months=-1]):
   Add a new cron job.

Arguments:

'function'       :   Mandatory argument. Any python function can be specified.
'args'           :   Mandatory argument. Arguments to the function. It should be provided as a list.
'every_seconds'  :   Optional. Its value should be an integer. If it's specified, the cron job is executed every 'n' seconds
                     where 'n' is the value of argument. Default value is None.
'minutes'        :   Optional. It accepts argument of type list. It's the list of minute values at which the cron job is executed.
                     Default value is -1, which is a special value meaning that the cron job is executed at every minute.
                     The list values should be in range [0, 59].
'hours'          :   Optional. It accepts argument of type list. It's the list of hour values at which the cron job is executed.
                     Default value is -1, which is a special value meaning that the cron job is executed at every hour.
                     The list values should be in range [0, 23].
'dom':           :   Optional. It's short form for 'day of month'. It accepts argument of type list.
                     It's the list of days of month values at which the cron job is executed.
                     Default value is -1, which is a special value meaning that the cron job is executed every day.
                     The list values should be in range [0, 31].
'months'         :   Optional. It accepts argument of type list. It's the list of days of month values at which the cron job is executed.
                     Default value is -1, which is a special value meaning that the cron job is executed every month.
                     The list values should be in range [0, 11]. (0-January, 11-December).
The return value is an object of type 'cron_obj'. This object needs to be passed to other functions (remove_job(), modify_job(), etc)

3. remove_job(job_object):
Remove the cron job, passed as an argument.

4. modify_job(job_object[, every_seconds=None, minutes=-1, hours=-1, dom=-1, months=-1]):
   Modify the cron job pass as an argument. The optional arguments are same as that of new_job() function.
   
Example:
========

import cron_manager

#Function to be passed to cron job.
def keep_printing(args1, args2):
    with open("C:\\cron_logs.log", "a") as fp:
        fp.write(str(args1) + str(args2) + "\n")

mgr = cron_manager.cron_manager()
#Start the cron daemon.
mgr.start_cron()
#Create a cron job which gets executed every 2 seconds at minute values 1, 20 and 30 every hour.
j1 = mgr.new_job(keep_printing, ['arg1', 'arg2'], every_seconds=2, minutes=[1, 20, 30])
