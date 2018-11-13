class P2PException(Exception):
    pass


class P2PSlugTaken(P2PException):
    pass


class P2PNotFound(P2PException):
    pass


class P2PUniqueConstraintViolated(P2PException):
    pass


class P2PEncodingMismatch(P2PException):
    pass


class P2PUnknownAttribute(P2PException):
    pass


class P2PInvalidAccessDefinition(P2PException):
    pass


class P2PSearchError(P2PException):
    pass


class P2PFileError(P2PException):
    pass


class P2PPhotoUploadError(P2PFileError):
    pass


class P2PInvalidFileType(P2PFileError):
    pass


class P2PFileURLNotFound(P2PFileError):
    pass

class P2PRedirectedToLogin(P2PException):
    """
    An exception when for some reason the client gets redirected to the login page
    instead of returning a result.
    """
    pass

class P2PThrottled(P2PException):
    """
    An exception where the api is being throttled
    """
    pass

class P2PRetryableError(P2PException):
    """
    A base exception for errors we want to retry when they fail.
    """
    pass


class P2PForbidden(P2PRetryableError):
    """
    To be raised when you credentials are refused due to a throttle.
    """
    pass


class P2PTimeoutError(P2PRetryableError):
    """
    To be raised when P2P throws a 500 error due to a timeout on its end.
    """
    pass
