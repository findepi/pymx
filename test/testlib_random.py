
def get_random(seed=18753):
    assert seed is not None
    from random import Random
    inst = Random(seed)
    return inst
