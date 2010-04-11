
from nose.tools import eq_

from pymx.atomic import Atomic

def test_atomic():
    a = Atomic(10)
    eq_(11, a.inc())
    eq_(10, a.inc(-1))
    eq_(9, a.dec())
    eq_(9.6, a.set(9.6))
    eq_(9.6, a.get())
