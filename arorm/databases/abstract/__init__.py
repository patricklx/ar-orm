import typing
from typing import Type


class Database:
    def commit(self, new, changed, removed):
        pass

    def setup_db(self, models):
        pass


class DatabaseFactory:
    Query: Type['StoreQuery']
    Filter = Type['Filter']

    @staticmethod
    def create_database(settings):
        pass


T = typing.TypeVar('T')


class RawQuery:
    def execute(self):
        pass

    def iter(self):
       pass

    def all(self):
        pass

    def scalar(self):
        pass

    def one(self):
        pass


class StoreQuery(typing.Generic[T]):
    def __init__(self, store: 'Store', entity_type: T):
        super(StoreQuery, self).__init__(entity_type, store.database)
        self.store = store
        self.entity_type = entity_type

    def get(self, id) -> T:
        pass

    def all(self) -> typing.List[T]:
        pass

    def one(self) -> T:
        pass

    @staticmethod
    def raw(database, q) -> RawQuery:
        pass

    def count(self):
        pass

    def filter(self, *args, **kwargs) -> 'StoreQuery[T]':
        pass


class Filter:
    def __init__(self, name, op, var):
        self.vars = {}
        self.expression = ''
        self.or_ = False
        self.prepend = True
        self.rec_name = None
        self.name = name
        self.var = var
        self.op = op
