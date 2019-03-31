

"""
Exceptions Classes
"""


class MesosException(Exception):
    """
    Base Exception class to be inherited by all Mesos exceptions.
    """
    def __init__(self, message=None, original_exception=None):
        if original_exception is not None:
            message = "{msg}: {exception}".format(
                msg=message, exception=original_exception)
        self.original_exception = original_exception
        super(MesosException, self).__init__(message)


class MesosHTTPException(MesosException):
    """
    A wrapper around Response objects for HTTP error codes.
    :param response: requests Response object
    :type response: Response
    """
    STATUS_CODE = None

    def __init__(self, response):
        super(MesosHTTPException, self).__init__()
        self.response = response

    def status(self):
        """
        Return status code from response.
        :return: status code
        :rtype: int
        """
        if self.STATUS_CODE is None:
            return self.response.status_code
        return self.STATUS_CODE

    def __str__(self):
        return "The url '{url}' returned HTTP {status_code}: {text}"\
            .format(url=self.response.request.url,
                    status_code=self.response.status_code,
                    text=self.response.text)


class MesosAuthenticationException(MesosHTTPException):
    """
    Indicates an authentication failure.
    """
    STATUS_CODE = 401


class MesosUnprocessableException(MesosHTTPException):
    """
    The request was well-formed but was unable to be followed due to semantic
    errors.
    """
    STATUS_CODE = 422


class MesosAuthorizationException(MesosHTTPException):
    """
    The request was valid but the server is refusing action.
    """
    STATUS_CODE = 403


class MesosBadRequestException(MesosHTTPException):
    """
    The server cannot or will not process the request due to an apparent client
    error.
    """
    STATUS_CODE = 400


class MesosServiceUnavailableException(MesosHTTPException):
    """
    The server is currently unavailable (because it is overloaded or down for
    maintenance).
    """
    STATUS_CODE = 503


class MesosInternalServerErrorException(MesosHTTPException):
    """
    An unexpected condition was encountered and no more specific
    message is suitable.
    """
    STATUS_CODE = 500
