
from sys import exc_info
from threading import Event
from traceback import format_exception

from .timeout import Timeout

class FutureException(Exception):
    pass

class FutureError(FutureException):
    pass

class FutureTimeout(FutureException):
    pass

class Future(object):

    """Future is a placeholder for a function result calculated presumably in
    another thread. """

    def __init__(self):
        object.__init__(self)
        self._has_value = Event()
        assert not self._has_value.isSet()
        self._value = None

    def set(self, value):
        """Set internal value. Not safe to call after `set` or `set_error` have
        been called. """
        assert not self._has_value.isSet()
        assert not isinstance(value, FutureError)
        self._set(value)

    def _set(self, value):
        self._value = value
        self._has_value.set()

    def set_error(self, message=None, exc=None):
        """Set internal value to an error. Not safe to call after `set` or
        `set_error` have been called. """
        try:
            if message is not None:
                self._set(FutureError(message))
            else:
                if exc is None:
                    exc = exc_info()
                self._set(FutureError("Future error:\n" +
                    ''.join(format_exception(*exc))))
        finally:
            exc = None

    @property
    def value(self):
        """Get value or raise exception if an error occurred. Not safe to
        called before `wait` has been called. """
        value = self._value
        if isinstance(value, FutureError):
            raise value
        return value

    def wait(self, timeout=None):
        """Wait for internal value to be set and return it. """
        self._has_value.wait(timeout)
        if self._has_value.isSet():
            return self.value
        else:
            raise FutureTimeout("Future internal value has not been set.")

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        try:
            if exc_info != (None, None, None):
                self.set_error(exc=exc_info)
        finally:
            exc_info = None


def wait_all(*futures, **options):
    timeout = options.pop('timeout', None)
    assert not options, "redundant arguments passed"
    timer = Timeout(timeout)
    values = []
    for future in futures:
        if not timer.remaining:
            raise FutureTimeout("Total wait-all timeout exceeded")
        values.append(future.wait(timeout=timer.timeout))
    return values
