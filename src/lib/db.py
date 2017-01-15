import pymongo
import six
from lib.main import MetaSingleton


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


