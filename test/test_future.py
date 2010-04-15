from __future__ import with_statement

from time import sleep

from pymx.future import Future, FutureError

from nose.tools import timed, eq_, raises

from .testlib_threads import TestThread


@timed(0.05)
def test_simple_future():
    f = Future()
    f.set(3)
    eq_(f.wait(4), 3)
    eq_(f.wait(4), 3)


@timed(0.1)
def test_threaded_future():
    f = Future()
    def setter():
        sleep(0.01)
        f.set(13)

    th = TestThread(target=setter)
    th.setDaemon(True)
    th.start()

    eq_(f.wait(1), 13)
    eq_(f.value, 13)
    th.join()

def test_future_exception():
    class Exc(Exception):
        pass

    future = Future()
    try:
        with future:
            raise Exc("nic")
    except Exc:
        pass

    raises(FutureError)(future.wait)()
