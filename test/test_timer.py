
from nose.tools import eq_, raises

from pymx.timer import Timer

def test_timer():
    timer = Timer()
    timer = Timer()
    timer = Timer()

    l = ['a']
    timer = Timer()
    timer.schedule(0.01, lambda: l.append('x'))
    timer.schedule(0.06, lambda: l.append('c'))
    timer.schedule(0.011, lambda: l.append('b'))
    timer.schedule(0.01, l.pop, 1)
    timer.close()
    eq_(l, ['a', 'b', 'c'])

def test_timer_fast_close():
    l = []
    timer = Timer()
    timer.schedule(0.1, lambda: l.append('a'))
    timer.schedule(0.6, lambda: l.append('c'))
    timer.schedule(0.11, lambda: l.append('b'))
    timer.close(complete=False)
    eq_(l, [])
    raises(RuntimeError)(lambda: timer.schedule(0.1, l.pop, 3))()
