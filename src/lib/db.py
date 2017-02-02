import flatdict
import pymongo
import six
from bson import DBRef

from lib.main import MetaSingleton
from lib.utils import get_from_dict


@six.add_metaclass(MetaSingleton)
class Database(object):

    def __init__(self):
        self.connection_pool = {}

    def get_client(self, read_preference):
        if self.connection_pool.get(read_preference) is None:
            self.connection_pool[read_preference] = pymongo.MongoClient('mongodb://127.0.0.1',
                                                                        read_preference=read_preference)
        return self.connection_pool[read_preference]

    def get_database(self, name, read_primary, *args, **kwargs):
        return pymongo.MongoClient.MongoDatabase(self.get_client(read_primary), name, *args, **kwargs)

    def get_collection(self, db_name, name, read_primary, *args, **kwargs):
        return pymongo.MongoClient.MongoCollection(self.get_database(db_name, read_primary), name, *args, **kwargs)


class DBManager(object):

    def __init__(self, db_name, collection, read_preference=pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED):
        self.read_preference = read_preference
        self.collection = collection
        self.db_name = db_name
        self._db = None

    @property
    def db(self):

        """
        :rtype: pymongo.collection.Collection
        """
        if self._db is None:
            self._db = Database().get_collection(self.db_name, self.collection, self.read_preference)
        return self._db

    def create_indexes(self):
        raise NotImplementedError()

    def find_by_id_list(self, id_list):
        return self.db.find({'_id': {'$in': id_list}})

    def get_by_id(self, _id):
        return self.db.find_one({'_id': _id})

    def delete_one_or_many(self, _item_one_or_list, key='_id'):
        if isinstance(_item_one_or_list, list):
            return self.db.delete_many({key: {'$in':_item_one_or_list}})
        else:
            return self.db.delete_one({key: _item_one_or_list})

    def cursor_map(self, cursor):
        return {str(i['_id']): i for i in list(cursor)}

    def update_many_denormalized(self, query, data, *args, **kwargs):
        old_cursor_map = self.cursor_map(self.db.find(query))
        result = self.collection.update_many(query, data, *args, **kwargs)
        if result.modified_count > 0:
            new_cursor = self.db.find(query)
            if new_cursor.count() > 0:
                new_cursor_map = self.cursor_map(new_cursor)
                denorm_db = DenormDBManager()
                denorm_db.denorm_collection(self.db, old_cursor_map, new_cursor_map)
        return result

    def update_one_denormalized(self, query, data, *args, **kwargs):
        doc_before = self.db.find_one(query)
        doc_after = self.collection.find_one_and_update(
            query, data, return_document=pymongo.ReturnDocument.AFTER, *args, **kwargs
        )
        if doc_after:
            denorm_db = DenormDBManager()
            denorm_db.denorm(self.db, doc_after['_id'], doc_before, doc_after)

    def update_one_or_many(self, _item_one_or_list, key='_id', update=None):
        if update is None:
            raise ValueError('update must be not empty')
        if isinstance(_item_one_or_list, list):
            return self.db.update_many({key: {'$in': _item_one_or_list}}, update)
        else:
            return self.db.update_one({key: _item_one_or_list}, update)

    def _create(self, parameters):
        parameters['_id'] = self.db.insert_one(parameters).inserted_id
        return parameters



class DenormDBManager(DBManager):

    def __init__(self):
        super(DenormDBManager, self).__init__('sys', 'denorm')

    def create_indexes(self):
        self.collection.create_index([
            ('db_name', pymongo.ASCENDING),
            ('collection_name', pymongo.ASCENDING),
            ('ref_id', pymongo.ASCENDING)
        ], background=True, name='denorm')

    def create(self, loader, callback):
        if loader.extra is not None:
            for _id in loader._id_list if hasattr(loader, '_id_list') else [loader._id]:
                data = {
                    '$set': {
                        'db_name': loader.db_name,
                        'collection_name': loader.collection_name,
                        'ref_id': _id
                    },
                    '$addToSet': {
                        'callback': {
                            'db_name': callback.db_name,
                            'collection_name': callback.collection_name,
                            'extra': loader.extra,
                            'query_path': callback.query_path,
                            'set_path': callback.set_path,
                            'query': callback.query
                        }
                    }
                }
                self.db.update_one({
                    'db_name': loader.db_name,
                    'collection_name': loader.collection_name,
                    'ref_id': _id}, data, upsert=True)

    def denorm_collection(self, db, old_map, new_map):
        for key, value in new_map.items():
            self.denorm(db, value['_id'], old_map[key], value)

    def denorm(self, db, _id, old, new):
        denorm_pack = self.db.find_one({'db_name': db.db_name, 'collection_name': db.collection_name, 'ref_id': _id})
        if denorm_pack is None:
            return
        denorm_items = [i for i in denorm_pack.get('callback', []) if
                        (i.get('extra') is not None and i.get('db_name') is not None)]
        keys_for_check = self.get_keys_for_check(denorm_items)
        updated_keys = self.get_updated_keys(old, new, keys_for_check)
        for denorm_item in denorm_items:
            extra_keys = denorm_item.get('extra')
            if not self.any_in_array(extra_keys, updated_keys):
                continue
            item = new
            extra = self.prepare_extra(item, denorm_item.get('extra'))
            item = DBRef(
                db.collection_name, _id, db.db_name, _extra=extra
            )
            ref_db = DBManager(
                db_name=denorm_item.get('db_name'),
                collection=denorm_item.get('collection_name'),
            )
            query = {denorm_item.get('query_path', '') + '.$id': _id}
            if denorm_item.get('query'):
                query.update(denorm_item.get('query'))
            ref_db.update_many_denormalized(
                query,
                {'$set': {denorm_item.get('set_path'): item}}
            )

    def get_keys_for_check(self, items):
        return set([keypath for item in items for keypath in item.get('extra')])

    def get_updated_keys(self, old, new, keys_for_check):
        updated_keys = []
        for key in keys_for_check:
            if get_from_dict(old, key) != get_from_dict(new, key):
                updated_keys.append(key)
        return updated_keys

    def any_in_array(self, array1, array2):
        return len(set(array2).intersection(set(array1))) > 0

    def prepare_extra(self, item, fields):
        extra = flatdict.FlatDict({}, delimiter='.')
        item = flatdict.FlatDict(item, delimiter='.')
        for key in fields:
            extra[key] = item.get(key)
        extra = extra.as_dict()
        return extra




def cursor_to_result(cursor, skip=None, limit=None):
    """

    :type cursor: pymongo.Cursor
    """
    try:
        if limit is not None:
            limit = int(limit)
        if skip is not None:
            skip = int(skip)
    except (ValueError, TypeError):
        limit = None
        skip = None
    total = cursor.count()
    if skip is not None:
        cursor.skip(skip)
    if limit is not None:
        cursor.limit(limit)
    return {
        'objects': list(cursor),
        'total': total
    }


