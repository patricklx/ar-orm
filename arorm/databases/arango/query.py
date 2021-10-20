import typing

from arango_orm.exceptions import DocumentNotFoundError

from arorm.databases.abstract import Filter, RawQuery
from arango_orm.query import Query as ArangoQuery

if typing.TYPE_CHECKING:
    from orm import Store


class ArRawQuery(RawQuery):
    def __init__(self, database, query, kwargs):
        self.database = database
        self.query = query
        self.kwargs = kwargs

    def execute(self):
        self.database.aql.execute(self.query, **self.kwargs)

    def iter(self):
        return self.database.aql.execute(self.query, **self.kwargs)

    def all(self):
        return [x for x in self.database.aql.execute(self.query, **self.kwargs)]

    def scalar(self):
        return next(self.database.aql.execute(self.query, **self.kwargs))

    def one(self):
        return next(self.database.aql.execute(self.query, **self.kwargs))


class ArangoStoreQuery(ArangoQuery):
    def __init__(self, store: 'Store', entity_type: str):
        super(ArangoStoreQuery, self).__init__(entity_type, store.database)
        self.store = store
        self.entity_type = entity_type

    def get(self, id):
        return self.store.get(self.entity_type, id)

    def _get(self, id):
        try:
            obj = super(ArangoStoreQuery, self).by_key(id)
            obj = self.store.add(obj)
        except DocumentNotFoundError as e:
            return None
        return obj

    def all(self):
        values = super(ArangoStoreQuery, self).all()
        values = [self.store.add(obj) for obj in values]
        return values

    def make_aql(self):
        return super(ArangoStoreQuery, self)._make_aql()

    def count(self):
        return super(ArangoStoreQuery, self).count()

    @staticmethod
    def raw(database, query, **kwargs):
        return ArRawQuery(database, query, kwargs)

    def aql(self, query, **kwargs):
        for obj in super(ArangoStoreQuery, self).aql(query, **kwargs):
            obj = self.store.add(obj)
            yield obj

    def filter(self, *args, **kwargs):
        from core.orm.databases.arango import ArangoFilter
        for arg in args:
            if isinstance(arg, Filter):
                f = ArangoFilter(arg.name, arg.op, arg.var)
                print('filter', f.expression, arg.prepend, arg.or_)
                super(ArangoStoreQuery, self).filter(f.expression, _or=arg.or_, prepend_rec_name=f.prepend, **f.vars)
            else:
                if not isinstance(arg, str):
                    raise Exception('arg is not a string:' + str(arg))
                super(ArangoStoreQuery, self).filter(arg, **kwargs)
        return self

    def delete(self):
        self.store.queue_ops.append([self, self.delete])
