
from functools import wraps
from threading import RLock


def synchronized(func):
    """Decorated method with be surrounded with ``with self._lock`` statement.
    """
    @wraps(func)
    def synchronized_wrapper(self, *args, **kwargs):
        with self._lock:
            return func(self, *args, **kwargs)
    return synchronized_wrapper


class Atomic(object):

    def __init__(self, initial):
        object.__init__(self)
        self._lock = RLock()
        self._state = initial

    @synchronized
    def get(self):
        return self._state

    @synchronized
    def inc(self, how=1):
        self._state += how
        return self._state

    @synchronized
    def dec(self, how=1):
        self._state -= how
        return self._state

    @synchronized
    def set(self, new_state):
        self._state = new_state
        return self._state
