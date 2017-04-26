class Error(Exception):
    """Raised when something failed in an unexpected and unrecoverable way"""
    pass

class OperationError(Error):
    """Raised when an operation failed in an expected but unrecoverable way"""
    pass

class NotifyError(Error):
    """Raised when an notify operation failed in an expected but unrecoverable way"""
    pass

class DBConnectionError(OperationError):
    """Raised when a db connection error occurs"""
    pass

class DBAuthenticationError(OperationError):
    """Raised when a db authentication error occurs"""
    pass

class DBOperationError(OperationError):
    """Raised when a db operation error occurs"""
    pass
