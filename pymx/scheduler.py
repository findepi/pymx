from __future__ import absolute_import, with_statement

from time import time
from functools import partial
from Queue import Queue
from heapq import heappush, heappop
from threading import Condition, Thread, RLock

from .atomic import Atomic

class Scheduler(object):

    __creation_counter = Atomic(0)

    def __init__(self):
        object.__init__(self)
        self._tasks = []
        self._lock = RLock()
        self._task_waiter = Condition(self._lock)
        self._is_closing = False
        self._complete_pending = None

        self._thread = th = Thread(target=self._worker_thread,
                name='TimerThread-#' + str(Scheduler.__creation_counter.inc()))
        th.setDaemon(True)
        th.start()

    def _shutdown(self, complete):
        with self._lock:
            self._is_closing = True
            self._complete_pending = complete
            self._task_waiter.notify()

    def close(self, complete=True):
        self._shutdown(complete=complete)
        self._thread.join()

    def __del__(self):
        self._shutdown(complete=True)

    def _worker_thread(self):
        while True:
            with self._lock:
                while True:
                    if self.__worker_should_stop():
                        return

                    if not self._tasks:
                        remaining = None
                    else:
                        remaining = max(self._tasks[0][0] - time(), 0)
                        if not remaining:
                            break

                    self._task_waiter.wait(remaining)
                    if self.__worker_should_stop():
                        return
                    continue # infinite unless broken

                assert remaining == 0
                when, callback = heappop(self._tasks)
                assert time() >= when

            callback()

    def __worker_should_stop(self):
        return self._is_closing and (not self._complete_pending or not
                self._tasks)

    def schedule(self, delay, callback, *args, **kwargs):
        if args or kwargs:
            callback = partial(callback, *args, **kwargs)
        when = time() + delay
        with self._lock:
            if self._is_closing:
                raise RuntimeError("Scheduler already closing")
            heappush(self._tasks, (when, callback))
            self._task_waiter.notify()
