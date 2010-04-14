
from functools import wraps, update_wrapper

def _update_wrapper(wrapper, wrapped, *args, **kwargs):
    """Run functools.update_wrapper iff `wrapper` is not `wrapped`."""
    if wrapper is not wrapped:
        update_wrapper(wrapper, wrapped, *args, **kwargs)
    return wrapper

def parametrizable_decorator(decorator):
    @wraps(decorator)
    def wrapper(fn=None, *args, **kwargs):
        if fn is not None and not callable(fn):
            args = (fn,) + args
            fn = None
        if fn is not None:
            return _update_wrapper(decorator(fn, *args, **kwargs), fn)
        else:
            return lambda fn: \
                    _update_wrapper(decorator(fn, *args, **kwargs), fn)
    return wrapper
