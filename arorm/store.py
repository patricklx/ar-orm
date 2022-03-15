import typing
from typing import Set, Dict, List, TYPE_CHECKING, TypeVar
import event_emitter as events

from .databases import databases
from .databases.abstract import StoreQuery

if TYPE_CHECKING:
    from arorm import Model, ORM

T = TypeVar('T')


class Store:
    _cache: Dict[str, 'Model']
    _cache_by_type: Dict[str, List['Model']]
    _cache_by_type_index: Dict[str, List['Model']]
    _new: Set['Model']
    _removed: Set['Model']
    queue_ops: List['StoreQuery']
    events: events.EventEmitter

    def __init__(self, config=None):
        self.clear()
        self.run_after_commit_callbacks = []
        if config:
            self.database = databases[config.driver].create_database(config)
            self.__query = databases[config.driver].Query

    def clear(self):
        self._cache = {}
        self._cache_by_type = {}
        self._cache_by_type_index = {}
        self._new = set()
        self._removed = set()
        self.events = events.EventEmitter()
        self.run_after_commit_callbacks = []
        self.queue_ops = []

    def fork(self):
        s = Store()
        s.database = self.database
        s.__query = self.__query
        return s

    def graph(self, name):
      return self.database.graph(name)

    def create(self, model: typing.Type[T], data) -> T:
        data = data or {}
        d = model(data=data, store=self)
        return self.add(d)

    def get(self, type: T, id) -> T:
        from . import ORM
        type = ORM.model(type)
        if '/' not in id:
            id = type.__collection__ + '/' + id
        value = self._cache.get(id, None)
        if value: return value
        return self.query(type)._get(id)

    def add(self, entity: 'Model'):
        from arorm import ReferenceId, Reference
        if entity.full_id in self._cache:
            if self._cache[entity.full_id] == entity:
                return self._cache[entity.full_id]
            self._cache[entity.full_id].update(entity._dump())
            entity._data = self._cache[entity.full_id]._data
            return self._cache[entity.full_id]
        if entity.__collection__ not in self._cache_by_type:
            self._cache_by_type[entity.__collection__] = []
        if not entity._id:
            self._new.add(entity)
            self._cache_by_type[entity.__collection__].append(entity)
            if entity._key:
                self._cache[entity.full_id] = entity
        else:
            self._cache[entity.full_id] = entity
            self._cache_by_type[entity.__collection__].append(entity)
            for name, f in entity._fields.items():
                if isinstance(f, (ReferenceId, Reference)):
                    if not getattr(entity, f._name): continue
                    if isinstance(f, ReferenceId):
                        value = getattr(entity, f._name)
                    if isinstance(f, Reference):
                        value = getattr(entity, f.ref_field)
                    idx = entity.__collection__ + '_' + f._name + '_' + value
                    if idx not in self._cache_by_type_index:
                        self._cache_by_type_index[idx] = []
                    l = self._cache_by_type_index[idx]
                    l.append(entity)
        entity._setup_store(self)
        self.events.emit('add', entity)
        return entity

    def get_all(self, entity_type: 'Model', index=None, index_value=None):
        if index:
            idx = entity_type.__collection__ + '_' + index + '_' + index_value
            if idx in self._cache_by_type_index:
                return self._cache_by_type_index[idx]
            return []
        return self._cache_by_type.get(entity_type.__collection__, [])

    def commit(self):
        changes = self._get_changed()
        self.database.commit(self._new, changes, self._removed, self.queue_ops)
        for e in changes:
            e._dirty = set()
        for n in self._new:
            self._cache[n.full_id] = n

        self._new = set()
        self._removed = set()
        self.queue_ops = []
        for fn in self.run_after_commit_callbacks:
            try:
                fn()
            except Exception as e:
                print('run_after_commit_callbacks failed', e)
        self.run_after_commit_callbacks = []

    def _get_changed(self):
        return [e for e in self._cache.values() if len(e._dirty) and e not in self._new]

    def remove(self, entity: 'Model'):
        if entity._id and entity not in self._removed:
            self._removed.add(entity)
        if entity.full_id and entity.full_id in self._cache:
            del self._cache[entity.full_id]
            if entity in self._cache_by_type[entity.__collection__]:
                self._cache_by_type[entity.__collection__].remove(entity)
        elif entity in self._new:
            self._new.remove(entity)
        self.events.emit('remove', entity)

    def query(self, entity_type: T) -> StoreQuery[T]:
        return self.__query(self, entity_type)

    def raw_query(self, q):
        return self.__query.raw(self.database, q)

    def setup_db(self):
        self.database.setup_db([m for m in ORM.all_models.values() if not m._embedded])

    def run_after_commit(self, fn):
        self.run_after_commit_callbacks.append(fn)
