
from nose.tools import eq_

from pymx.timer import Timer

def test_timer():
    timer = Timer()
    timer = Timer()
    timer = Timer()

    l = []
    timer = Timer()
    timer.schedule(0.01, lambda: l.append('a'))
    timer.schedule(0.06, lambda: l.append('c'))
    timer.schedule(0.011, lambda: l.append('b'))
    timer.close()
    eq_(''.join(l), 'abc')

def test_timer_fast_close():
    l = []
    timer = Timer()
    timer.schedule(0.1, lambda: l.append('a'))
    timer.schedule(0.6, lambda: l.append('c'))
    timer.schedule(0.11, lambda: l.append('b'))
    timer.close(complete=False)
    eq_(''.join(l), '')
