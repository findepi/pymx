
from collections import defaultdict, deque

class LimitedSet(object):

    """A set remebering only ``capacity`` recently added elements and no
    explicit removals. """

    def __init__(self, capacity=20000):
        object.__init__(self)
        self._capacity = max(capacity, 1)
        self._counters = defaultdict(int)
        self._elements = deque()
        self._size = 0

    def _shrink(self, capacity):
        while self._size >= capacity:
            rm = self._elements.popleft()
            rmc = self._counters[rm]
            assert rmc
            if rmc > 1:
                self._counters[rm] = rmc - 1
            else:
                del self._counters[rm]
                self._size -= 1

    def add(self, element):
        if element not in self._counters:
            self._shrink(self._capacity - 1)
            self._size += 1
            is_new = True
        else:
            is_new = False
        self._counters[element] += 1
        self._elements.append(element)
        return is_new
