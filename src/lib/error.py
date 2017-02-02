
from falcon import HTTPError as FalconHTTPError
from webargs.falconparser import status_map


class BaseError(Exception):
    def __init__(self, code, description, status):
        self.code = code
        self.description = description
        self.status = status
        super(BaseError, self).__init__(self.description)


class HTTPError(FalconHTTPError):
    def __init__(self, status, code, description, errors=None):
        self.errors = errors
        status = status_map.get(status)
        if status is None:
            raise LookupError('Status code {0} not supported'.format(status))
        super(HTTPError, self).__init__(status, description=description, code=code)

    def to_dict(self, *args, **kwargs):
        ret = super(HTTPError, self).to_dict(*args, **kwargs)
        if self.errors is not None:
            ret['error_map'] = self.errors
        ret['code'] = self.code
        ret['description'] = self.description
        return ret


class InvalidIdError(BaseError):
    def __init__(self):
        super(InvalidIdError, self).__init__(
            "DEFAULT.INVALID_ID_ERROR",
            'Invalid id',
            400
        )


class AccessDeniedError(BaseError):
    def __init__(self):
        super(AccessDeniedError, self).__init__(
            "DEFAULT.ACCESS_DENIED_ERROR",
            'Access denied error',
            403
        )


class ItemNotFoundError(BaseError):
    def __init__(self, item):
        super(ItemNotFoundError, self).__init__(
            "DEFAULT.ITEM_NOT_FOUND_ERROR",
            '{} not found'.format(item),
            404
        )



def http_throws(func):
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BaseError as e:
            raise HTTPError(e.status, e.code, e.description)

    return wrapped


def value_or_404(value, item):
    if value:
        return value
    raise ItemNotFoundError(item)
