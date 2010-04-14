"""
Utilities for handling Google Protocol Buffers objects.

Copyright Azouk Network Ltd. <http://www.azouk.com/>
"""

from google.protobuf.message import Message, DecodeError, EncodeError
from google.protobuf.reflection import containers

def initialize_message(*args, **kwargs):
    """Initialize a message with fields passed as python built-ins objects.

    Fields are specified as a dict (``fields dict``) mapping ``message field
    name`` to ``initializer``. Valid initializers include:

        - for a scalar field: scalar value
        - for a list of scalars: list of scalar values
        - for a message field: a message or a dict
        - for a list of messages: a list of messages or dicts (can be mixed)

    :Parameters:
        - `first-positional-argument`: a message, which will be updated
        - `second-positional-argument`: optional fields dict
        - `keyword-arguments`: field initializers
    """

    if len(args) == 1:
        message, = args
    elif len(args) == 2:
        message, kw = args
        kwargs = dict(kw, **kwargs)
    else:
        raise TypeError("initialize_message() takes at most 2 positional "
                "arguments (%d given)" % len(args))

    for key, value in kwargs.items():

        target = getattr(message, key)

        if value is None:
            pass

        elif isinstance(target, containers.RepeatedScalarFieldContainer):
            assert isinstance(value, (list, tuple)), \
                        "Initializer for RepeatedScalarFieldContainer must " \
                        "be a list or tuple."

            for element in value:
                target.append(element)

        elif isinstance(target, containers.RepeatedCompositeFieldContainer):
            assert isinstance(value, (list, tuple)), \
                        "Initializer for RepeatedScalarFieldContainer must " \
                        "be a list or tuple."

            for element in value:
                assert isinstance(element, dict), \
                        "Initializer for RepeatedCompositeFieldContainer's " \
                        "item must be a dict."

                initialize_message(target.add(), **element)

        elif isinstance(target, Message):
            if isinstance(value, dict):
                initialize_message(target, **value)

            elif isinstance(value, Message):
                target.CopyFrom(value)

            else:
                raise ValueError("Initializer for a Message field must be a "
                        "dict or Message.")

        else:
            assert isinstance(target, (basestring, int, long, float, bool))
            setattr(message, key, value)

    return message

def make_message(*args, **kwargs):
    """Create a message and initialize it.

    Returns newly created message.

    Parameters are the same as for `initialize_message` except first argument
    must be a message class (subclass of ``google.protobuf.message.Message``)
    instead of message instance.
    """
    message = args[0]()
    args = (message,) + args[1:]
    initialize_message(*args, **kwargs)
    return message

def dict_message(message, all_fields=False, recursive=False):
    """Operation reverse to make_message.  If not ``all_fields``, only set
    fields are provied. """

    def _converter(m):
        # The actual conversion implementation.
        if not isinstance(m, Message):
            return m

        converted = {}
        if all_fields:
            for fd in m.DESCRIPTOR.fields:
                converted[fd.name] = getattr(m, fd.name)
        else:
            for fd, value in m.ListFields():
                converted[fd.name] = value

        for k, v in converted.items():
            if isinstance(v, (containers.RepeatedScalarFieldContainer,
                containers.RepeatedCompositeFieldContainer)):
                converted[k] = list(v)

        if recursive:
            for k, v in converted.items():
                if isinstance(v, Message):
                    converted[k] = _converter(v)
                elif isinstance(v, (list, tuple)):
                    converted[k] = map(_converter, v)
        return converted

    assert isinstance(message, Message)
    return _converter(message)

def message_getattr(message, field, default=None):
    """Same as ``getattr(message, field, default)`` but works with Protobuf
    Messages, for which `getattr` will return field value even if the field has
    not been set by the sender. Uses ``message._has_field`` to detect, if the
    field "exists".

    If `message` is not a Protobuf Message, then standard `getattr` is used.
    """
    if not isinstance(message, Message):
        return getattr(message, field, default)
    if message.HasField(field):
        return getattr(message, field)
    return default

def parse_message(type, buffer, partial=False):
    """Create new instance of Protocol Buffer `type` and parse data from string
    ``buffer``. """
    t = type()
    t.ParseFromString(buffer)
    if not partial and not t.IsInitialized():
        try:
            t.SerializeToString() # this triggers meaningful exception
        except EncodeError, e:
            raise DecodeError(*e.args)
        else:
            raise AssertionError("this may never happen: `t' was not "
                    "initialized just a moment ago but SerializeToString "
                    "succeeded")
    return t
