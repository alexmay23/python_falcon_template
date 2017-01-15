import falcon
from falcon import Request


class CustomAPI(falcon.API):
    def __init__(self, *args, **kwargs):
        self.routes = []
        super(CustomAPI, self).__init__(*args, **kwargs)

    def add_route(self, uri_template, resource, *args, **kwargs):
        self.routes.append((uri_template, resource))
        super(CustomAPI, self).add_route(uri_template, resource, *args, **kwargs)


class ApiRequest(Request):
    def __init__(self, env, options=None):
        super(ApiRequest, self).__init__(env, options)
        self.context = {}