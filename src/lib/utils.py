import datetime
import hashlib
from random import randint

from bson import DBRef
from bson import ObjectId
from bson.errors import InvalidId
from collections import defaultdict

from lib.error import InvalidIdError


def date_or_now(value):
    return value if value else datetime.datetime.utcnow()


def password_hash(value):
    return hashlib.sha256(value)


def filter_only_keys(dictionary, keys):
    filtered = {}
    for k in keys:
        value = dictionary.get(k)
        if value is not None:
            filtered[k] = value
    return filtered


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return randint(range_start, range_end)


def to_oid(value):
    try:
        return ObjectId(value)
    except InvalidId:
        raise InvalidIdError()


def get_from_dict(data, path):
    path = path.split('.')
    if len(path) == 1:
        try:
            return data.get(path[0])
        except AttributeError:
            try:
                return getattr(data, path[0])
            except AttributeError:
                return None
    else:
        try:
            data = data.get(path[0])
        except AttributeError:
            data = getattr(data, path[0])
        return get_from_dict(data, '.'.join(path[1:]))


def collect_db_ref(items):
    values = []
    if isinstance(items, dict):
        items = items.values()
    for item in items:
        if isinstance(item, DBRef):
            values.append(DBRef(item.collection, item.id, item.database))
            values += collect_db_ref(item._DBRef__kwargs)
        elif isinstance(item, (list, dict)):
            values += collect_db_ref(item)
    return values


def append_db_ref(item, data):
    if isinstance(item, DBRef):
        item._DBRef__kwargs = append_db_ref(item._DBRef__kwargs, data)
        return data[item.database][item.collection].get(item.id) or item
    elif isinstance(item, list):
        return [append_db_ref(i, data) for i in item]
    elif isinstance(item, dict):
        return {k: append_db_ref(i, data) for k, i in item.items()}
    return item


def deref(data, items):
    refs = defaultdict(list)
    for r in set(collect_db_ref(data)):
        if r.database + '.' + r.collection in items:
            refs[(r.database, r.collection)].append(r.id)
    docs = defaultdict(lambda: defaultdict(dict))
    for key, _id in refs.items():
        docs[key[0]][key[1]] = {i['_id']: i for i in DB(key[0], key[1]).filter({'_id': {'$in': _id}})}
    return append_db_ref(data, docs)