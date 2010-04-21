
from time import time
from Queue import Queue, Empty

from nose.tools import assert_almost_equal, timed

from pymx.timeout import Timeout

def test_timeout():
    timer = Timeout(None)
    assert timer.remaining
    assert timer.timeout is None
    assert timer.timeout is None

    timer = Timeout(0.1)
    assert timer.remaining
    assert_almost_equal(timer.timeout, 0.1, 3)

@timed(0.7)
def test_timeout_partition():
    q = Queue()
    timer = Timeout(0.5)
    i = 0
    while timer.remaining:
        try:
            q.get(timeout=min(timer.timeout, 0.1))
        except Empty:
            pass
        i += 1

    assert_almost_equal(i, 5, -1)

