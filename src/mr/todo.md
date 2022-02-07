In coordinator
--------------
switch from Task to *Task
switch from Client to *Client
timer to exit coordiator when the Exit phase starts
look at log messages
don't rely on timer.Reset(millisecond) to move between select cases,
    instead pull out a common function

In worker
---------
file read/write
do the actual work