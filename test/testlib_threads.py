
from traceback import format_exc
from threading import Thread

__all__ = ['TestThread', 'ThreadError']

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
