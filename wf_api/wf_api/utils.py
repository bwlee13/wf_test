import functools
from django.shortcuts import render
from requests.exceptions import ConnectionError, RequestException
from wf_api.wf_api import settings
import logging

logger = logging.getLogger(__name__)

BASE_ERROR_MESSAGE = 'Could not connect to your box because {error_reason}'


def handle_view_exception(func):
    """Decorator for handling exceptions."""
    @functools.wraps(func)
    def wrapper(request, *args, **kwargs):
        try:
            response = func(request, *args, **kwargs)
            logger.info(response)
        except RequestException as e:
            error_reason = 'of an unknown error.'
            if isinstance(e, ConnectionError):
                error_reason = 'the host is unknown.'
            context = {
              'error_message': BASE_ERROR_MESSAGE.format(error_reason=error_reason),
            }
            logger.debug(error_reason)
            response = render(request, 'box/error.html', context)
            logger.debug(response)
        return response

    return wrapper
