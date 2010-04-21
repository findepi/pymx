
from traceback import format_exc
from threading import Thread, enumerate

from nose.tools import make_decorator

class ThreadError(Exception):
    pass

class TestThread(Thread):

    _exc = None

    def run(self):
        try:
            Thread.run(self)
        except Exception, e:
            self._exc = ThreadError("Uncaught exception in a thread %s\n%s" %
                    (self.getName(), '\n'.join(
                        ' | ' + line for line in format_exc().splitlines())))


    def join(self, timeout=60):
        Thread.join(self, timeout)
        assert not self.isAlive()
        if self._exc is not None:
            raise self._exc

def check_threads(func):
    @make_decorator(func)
    def _check_threads(*args, **kwargs):
        start_threads = set(enumerate())
        func(*args, **kwargs)
        end_threads = set(enumerate())
        if not start_threads.issuperset(end_threads):
            raise ThreadError("Those threads are new: %r" % (end_threads,))
    return _check_threads

