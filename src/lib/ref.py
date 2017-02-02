# coding=utf-8
import bson

from lib.db import DBManager, DenormDBManager


class RefLoader(object):
    def __init__(self, db_name, collection_name, _id, extra=None, item=None):
        self.db_name = db_name
        self.collection_name = collection_name
        self._id = _id
        self.extra = extra
        self.item = item


class RefListLoader(object):
    def __init__(self, db_name, collection_name, _id_list, extra=None):
        self.db_name = db_name
        self.collection_name = collection_name
        self._id_list = _id_list
        self.extra = extra


class RefCallback(object):
    @classmethod
    def init_with_db(cls, db, query_path, set_path=None, query=None):
        return cls(db.db_name, db.collection_name, query_path, set_path)

    def __init__(self, db_name, collection_name, query_path, set_path=None, query=None):
        self.query = query
        self.query_path = query_path
        self.collection_name = collection_name
        self.db_name = db_name
        if set_path is None:
            set_path = query_path
        self.set_path = set_path


def simple_get_ref(db_name, collection_name, _id):
    if not isinstance(_id, bson.ObjectId):
        _id = bson.ObjectId(_id)
    return bson.dbref.DBRef(collection_name, _id, db_name)


def get_ref(loader, callback=None):
    if isinstance(loader, RefLoader):
        return _get_one_ref(loader, callback)
    elif isinstance(loader, RefListLoader):
        return _get_many_ref(loader, callback)


def _get_one_ref(loader, callback):
    deref_db = DenormDBManager()
    if callback is not None:
        deref_db.create(loader, callback)
    if loader.extra is not None:
        if loader.item is None:
            db = DBManager(db_name=loader.db_name, collection=loader.collection_name)
            item = db.get_by_id(loader._id)
        else:
            item = loader.item
        extra = deref_db.prepare_extra(item, loader.extra)
        item = bson.dbref.DBRef(loader.collection_name, loader._id, loader.db_name, _extra=extra)
    else:
        item = bson.dbref.DBRef(loader.collection_name, loader._id, loader.db_name)
    return item


def _get_many_ref(loader, callback):
    deref_db = DenormDBManager()
    if callback is not None:
        deref_db.create(loader, callback)
    if loader.extra is not None:
        db = DBManager(db_name=loader.db_name, collection=loader.collection_name)
        items = []
        for item in db.find_by_id_list(loader._id_list):
            extra = deref_db.prepare_extra(item, loader.extra)
            items.append(bson.dbref.DBRef(loader.collection_name, item.get('_id'), loader.db_name, _extra=extra))
    else:
        items = [bson.dbref.DBRef(loader.collection_name, i, loader.db_name) for i in loader._id_list]
    return items
