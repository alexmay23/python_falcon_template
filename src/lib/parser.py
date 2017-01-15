# coding=utf-8

import collections
import functools
from collections import defaultdict

from bson import ObjectId
from bson.errors import InvalidId
from marshmallow import fields, validate, Schema
from webargs import argmap2schema
from webargs.falconparser import parser, status_map

fields = fields
validate = validate
Schema = Schema


def get_response_from_view_args(*args, **kwargs):
    if kwargs.get('resp') is not None:
        return kwargs['resp']
    else:
        return args[2]


def use_schema(schema, callback=None, use_mock=False):
    """
    Если указан callback - schema должен быть словарем
    callback принимает в качестве аргумента результат и возвращает ключ словаря schema (какую схему использовать)
    """
    if callback is not None and not isinstance(schema, dict):
        raise ValueError('If callback, schema must be dictionary')

    def decorator(func):
        func.schema = schema

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            resp_obj = get_response_from_view_args(*args, **kwargs)
            result = func(*args, **kwargs)
            if callback is not None:
                _schema = schema[callback(result)]
            else:
                _schema = schema
            resp_obj.body = _schema().dumps(result).data

        return wrapper

    return decorator


class Nested(fields.Nested):
    """
    Кастомное поле Nested, для использования с множественными схемами
    Если указан callback - nested должен быть словарем
    callback принимает в качестве аргумента результат и возвращает ключ словаря nested (какую схему использовать)
    """

    def __init__(self, nested, callback=None, *args, **kwargs):
        self.callback = None
        if callback is not None:
            self.callback = callback
            nested = {a: self.__argmap2schema(b) for a, b in nested.items()}
            self._nested = nested
        else:
            nested = self.__argmap2schema(nested)
        super(Nested, self).__init__(nested, *args, **kwargs)

    def __argmap2schema(self, schema):
        if isinstance(schema, dict):
            schema = argmap2schema(schema)
        return schema

    def _serialize(self, nested_obj, attr, obj):
        if self.callback is not None:
            self.__schema = None
            self.nested = self._nested[self.callback(nested_obj)]
        result = super(Nested, self)._serialize(nested_obj, attr, obj)
        if self.callback is not None:
            self.nested = self._nested
        return result


class MongoId(fields.String):
    def _serialize(self, value, attr, obj):
        return super(MongoId, self)._serialize(value, attr, obj)

    def _deserialize(self, value, attr, data):
        value = super(MongoId, self)._deserialize(value, attr, data)
        try:
            return ObjectId(value)
        except InvalidId:
            self.fail('invalid')


class DateTimeReplaced(fields.DateTime):
    def _serialize(self, value, attr, obj):
        value = value.replace(microsecond=0) if value is not None else None
        return super(DateTimeReplaced, self)._serialize(value, attr, obj)

    def _deserialize(self, value, attr, obj):
        return super(DateTimeReplaced, self)._serialize(value, attr, obj)


fields.MongoId = MongoId