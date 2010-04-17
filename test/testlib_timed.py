
from contextlib import contextmanager
from time import time

from nose.tools import TimeExpired

@contextmanager
def timedcontext(limit):
    start = time()
    yield
    end = time()
    if end - start > limit:
        raise TimeExpired("Time limit (%s) exceeded (%.6f)" % (limit, end -
            start))
