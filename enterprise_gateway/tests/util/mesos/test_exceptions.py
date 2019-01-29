
# pylint: disable=redefined-outer-name,missing-docstring

import mock
from parameterized import parameterized

from enterprise_gateway.services.util.mesos.exceptions import MesosHTTPException
from enterprise_gateway.services.util.mesos.exceptions import MesosAuthenticationException
from enterprise_gateway.services.util.mesos.exceptions import MesosAuthorizationException
from enterprise_gateway.services.util.mesos.exceptions import MesosBadRequestException
from enterprise_gateway.services.util.mesos.exceptions import MesosUnprocessableException


class TestExceptions(object):

    @parameterized.expand([
        (MesosHTTPException,
         400,
         "The url 'http://some.url' returned HTTP 400: something bad happened"),
        (MesosAuthenticationException,
         401,
         "The url 'http://some.url' returned HTTP 401: something bad happened"),
        (MesosAuthorizationException,
         403,
         "The url 'http://some.url' returned HTTP 403: something bad happened"),
        (MesosBadRequestException,
         400,
         "The url 'http://some.url' returned HTTP 400: something bad happened"),
        (MesosUnprocessableException,
         422,
         "The url 'http://some.url' returned HTTP 422: something bad happened"),
    ])
    def test_exceptions(self, exception, status_code, expected_string):
        """
        Test exceptions
        """
        mock_resp = mock.Mock()
        mock_resp.status_code = status_code
        mock_resp.reason = 'some_reason'
        mock_resp.request.url = 'http://some.url'
        mock_resp.text = 'something bad happened'

        # Test MesosHTTPException
        err = exception(mock_resp)
        assert str(err) == expected_string
        assert err.status() == status_code
