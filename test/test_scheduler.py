from __future__ import with_statement

from contextlib import closing

from nose.tools import eq_, raises

from pymx.scheduler import Scheduler

def test_timer():
    l = ['a']
    scheduler = Scheduler()
    scheduler.schedule(0.01, lambda: l.append('x'))
    scheduler.schedule(0.06, lambda: l.append('c'))
    scheduler.schedule(0.011, lambda: l.append('b'))
    scheduler.schedule(0.01, l.pop, 1)
    scheduler.close()
    eq_(l, ['a', 'b', 'c'])

def test_three_timers():
    x = []
    l = [0]
    with closing(Scheduler()) as a:
        with closing(Scheduler()) as b:
            with closing(Scheduler()) as c:
                a.schedule(0.050, x.append, 'a')
                b.schedule(0.025, x.append, 'b')
                c.schedule(0, x.append, 'c')
                c.schedule(0, b.schedule, 0, a.schedule, 0, l.pop)
    eq_(x, ['c', 'b', 'a'])
    eq_(l, [])

def test_timer_fast_close():
    l = []
    scheduler = Scheduler()
    scheduler.schedule(0.1, lambda: l.append('a'))
    scheduler.schedule(0.6, lambda: l.append('c'))
    scheduler.schedule(0.11, lambda: l.append('b'))
    scheduler.close(complete=False)
    eq_(l, [])
    raises(RuntimeError)(lambda: scheduler.schedule(0.1, l.pop, 3))()
