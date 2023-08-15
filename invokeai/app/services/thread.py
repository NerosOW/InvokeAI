import inspect
from threading import RLock, current_thread

# use re-entrant lock in case we make some recursive calls
sqlite_lock: RLock = RLock()

# count of threads waiting for the lock
waiting: int = 0

# whether to log lock activity
log: bool = False


class SqliteLock(object):
    log_ctx: str

    def __init__(self):
        self.log_ctx = f"{current_thread().name}: {inspect.stack()[1].function}()"

    def __enter__(self):
        global waiting
        waiting = waiting + 1
        if log:
            print(f"{self.log_ctx} -> Waiting for lock ({waiting} waiting)")
        sqlite_lock.acquire()
        waiting = waiting - 1
        if log:
            print(f"{self.log_ctx} -> Acquired lock ({waiting} waiting)")

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if log:
            print(f"{self.log_ctx} -> Releasing lock ({waiting} waiting)")
        sqlite_lock.release()
        if log:
            print(f"{self.log_ctx} -> Released lock ({waiting} waiting)")
