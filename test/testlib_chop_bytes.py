
from .testlib_random import get_random

random = get_random()

def chop_bytes(bytes, chunk_length=None, min_length=5, max_length=5):
    if chunk_length is not None:
        min_length = max_length = chunk_length

    index = 0
    while index < len(bytes):
        next_length = random.randint(min_length, max_length)
        next_chunk = bytes[index:index + next_length]
        yield next_chunk
        index += len(next_chunk)
