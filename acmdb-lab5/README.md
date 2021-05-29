# SimpleDB Lab-5 Doc

- Exercise 1&2: for these two exercises, I create the `Lock` and `LockManager` class. The former class records all locking information on a specific page, and the latter one deals with acquiring and releasing locks.
- Exercise 3: only evict pages that is not dirty. There is not much to say. 
- Exercise 4: this exercise is straightforward. When we commit, flush all pages; when we abort, discard all pages. Also remember to release certain locks.
- Exercise 5: since I am not clever, I simply implement a timeout-based detection method, i.e., it waits until the lock is released, or the time is up.

I did not make any essential changes to the API in this lab but create several new classes described above.

I spent entirely two days on it. Debugging on multi-threading is challenging.

