
from time import sleep
from .testlib_timed import timedcontext, TimeExpired

from nose.tools import raises

def test_timed():
    def long():
        with timedcontext(0.01):
            sleep(0.02)

    def short():
        with timedcontext(0.08):
            sleep(0.01)

    raises(TimeExpired)(long)()
    short()
