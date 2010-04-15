
from pymx.decorator import parametrizable_decorator

from nose.tools import eq_

def test_decorator():

    @parametrizable_decorator
    def x(fn, a=None, b=None):
        def wrapper():
            return (fn, a, b)
        return wrapper

    def foo():
        """c"""
        pass

    foo_x = x(1, b=3)(foo)
    eq_(foo_x(), (foo, 1, 3))

    foo_x = x(b=3)(foo)
    eq_(foo_x(), (foo, None, 3))

    foo_x = x(2)(foo)
    eq_(foo_x(), (foo, 2, None))
    eq_(foo_x.__doc__, 'c')
