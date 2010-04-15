
from collections import defaultdict, deque

class LimitedSet(object):

    """A set remebering only ``capacity`` recently added elements and no
    explicit removals. """

    def __init__(self, capacity=20000):
        object.__init__(self)
        self._capacity = max(capacity, 1)
        self._elements = set()
        self._recent = deque()

    def __len__(self):
        assert len(self._elements) == len(self._recent), "%d != %d" % \
                (len(self._elements), len(self._recent))
        return len(self._elements)

    @property
    def size(self):
        return len(self)

    def _shrink(self, capacity):
        while self.size > capacity:
            rm = self._recent.popleft()
            self._elements.remove(rm)

    def add(self, element):
        if element not in self._elements:
            self._shrink(self._capacity - 1)
            self._elements.add(element)
            self._recent.append(element)
            return True
        return False
