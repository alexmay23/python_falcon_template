import importlib
import os
import six
from lib.main import MetaSingleton


@six.add_metaclass(MetaSingleton)
class Config(dict):
    def __init__(self, **kwargs):
        super(Config, self).__init__(**kwargs)
        self.reload()

    def reload(self):
        self._load_from_module()
        self._load_from_env()

    def _load_from_module(self):
        try:
            module = importlib.import_module(os.environ.get('CONFIG_MODULE', 'settings'))
            for key in dir(module):
                if key.isupper():
                    self[key] = getattr(module, key)
        except ImportError:
            pass

    def _load_from_env(self):
        for key, value in os.environ.items():
            if key.isupper():
                self[key] = value

    def get_as_int(self, key, default=None):
        if key not in self:
            return default
        if isinstance(self[key], int):
            return self[key]
        try:
            value = int(self[key])
            self[key] = value
            return value
        except ValueError:
            return default

    def get_as_list(self, key, default=None):
        if key not in self:
            return default
        if isinstance(self[key], list):
            return self[key]
        if isinstance(self[key], six.string_types):
            value = list(six.moves.urllib.parse.parse_qs(self[key], keep_blank_values=True).keys())
            self[key] = value
            return value
        return list(self[key])

    def get_as_bool(self, key, default=None):
        if key not in self:
            return default
        if isinstance(self[key], bool):
            return self[key]
        if isinstance(self[key], six.string_types):
            value = self[key].lower() in ('1', 'true', 'yes')
            self[key] = value
            return value
        return bool(self[key])

    def get_as_dict(self, key, default=None):
        if key not in self:
            return default
        if isinstance(self[key], dict):
            return self[key]
        if isinstance(self[key], six.string_types):
            value = {a: b[0] if len(b) == 1 else b for a, b in six.moves.urllib.parse.parse_qs(self[key]).items()}
            self[key] = value
            return value
        return dict(self[key])
