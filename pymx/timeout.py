
from time import time


class Timeout(object):
    """Makes calculating remaining time before timeout easy.

    Example use:

    .. python::
        q = Queue()
        timer = Timeout(5)
        while timer.remaining:
            # wait for at most 5 seconds for an acceptable element
            x = q.get(timeout=timer.timeout)
            if accept(x):
                return x
    """

    def __init__(self, timeout):
        """Creates new reusable, total-limiting timer. If `timeout` is
        ``None``, the timer created has no limits. """
        if timeout is None:
            self._time_end = None
        else:
            self._time_end = time() + timeout

        self._undefined = object()
        self._next_timeout = self._undefined

    @property
    def remaining(self):
        """Returns value convertible to ``True`` iff there is some time
        remaining. Also caches next value of `timeout` property, so it safe to
        call ``wait(timeout=timer.timeout)`` after ``timer.remaining`` returned
        ``True``-like value. """
        if self._time_end is None:
            self._next_timeout = None
            return True
        else:
            self._next_timeout = max(self._time_end - time(), 0)
            return self._next_timeout

    @property
    def timeout(self):
        """Returns time remaining or None if no time limit was specified. """
        if self._next_timeout is self._undefined:
            remaining = self.remaining # force recalcualtion of _next_timeout
        next, self._next_timeout = self._next_timeout, self._undefined
        return next
