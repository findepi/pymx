
class MultiplexerException(Exception):
    """All exception riased by pyMX library should inherit from
    `MultiplexerException`. """
    pass

class OperationFailed(MultiplexerException):
    pass

class OperationTimedOut(OperationFailed):
    pass
