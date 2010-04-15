
from pymx.limitedset import LimitedSet

def test_limitedset():
    capacity = 10
    ls = LimitedSet(capacity=capacity)
    assert ls.add(1)
    for x in xrange(2, 2 + capacity):
        assert not ls.add(1), "1 not in ls when adding %d" % x
        assert ls.add(x), "error adding %d" % x
        assert not ls.add(x), "error adding %d" % x
    assert ls.add(1)
