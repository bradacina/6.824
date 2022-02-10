In coordinator
--------------
look at log messages
(done) switch from Task to *Task
(done) switch from Client to *Client
(done )timer to exit coordiator when the Exit phase starts

In worker
---------
don't rely on timer.Reset(millisecond) to move between select cases,
    instead pull out a common function
(done) file read/write - reduce
(done) do the actual work - reduce
(done) do the actual work - map
(done) file read/write - map