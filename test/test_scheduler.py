
from nose.tools import eq_, raises

from pymx.scheduler import Scheduler

def test_timer():
    scheduler = Scheduler()
    scheduler = Scheduler()
    scheduler = Scheduler()

    l = ['a']
    scheduler = Scheduler()
    scheduler.schedule(0.01, lambda: l.append('x'))
    scheduler.schedule(0.06, lambda: l.append('c'))
    scheduler.schedule(0.011, lambda: l.append('b'))
    scheduler.schedule(0.01, l.pop, 1)
    scheduler.close()
    eq_(l, ['a', 'b', 'c'])

def test_timer_fast_close():
    l = []
    scheduler = Scheduler()
    scheduler.schedule(0.1, lambda: l.append('a'))
    scheduler.schedule(0.6, lambda: l.append('c'))
    scheduler.schedule(0.11, lambda: l.append('b'))
    scheduler.close(complete=False)
    eq_(l, [])
    raises(RuntimeError)(lambda: scheduler.schedule(0.1, l.pop, 3))()
