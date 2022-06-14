import copy
import typing
from abc import ABC
from itertools import chain
from typing import Dict, Union, Set, Any, TYPE_CHECKING, List, Type

import inflect
import inflection
from marshmallow.fields import Field as MarshmellowField

from arorm.databases.abstract import Filter

if TYPE_CHECKING:
    from arorm.store import Store


class Symbol:
    def __init__(self, name=''):
        self.name = f"Symbol({name})"

    def __repr__(self):
        return self.name


Internal = Symbol('internal')


class FieldMeta(type):
    def __new__(mcs, name, bases, attrs):
        super_new = super(FieldMeta, mcs).__new__

        new_fields = {}
        new_attrs = {**attrs}
        for obj_name, obj in attrs.items():
            if isinstance(obj, MarshmellowField):
                obj = Field()
            if 'Field' in globals() and isinstance(obj, Field):
                # add to schema fields
                new_fields[obj_name] = attrs.get(obj_name)
                obj._name = obj_name
            if 'ReferenceId' in globals() and isinstance(obj, ReferenceId):
                # add to schema fields
                new_fields[obj_name] = attrs.get(obj_name)
                obj._name = obj_name
            if 'Reference' in globals() and isinstance(obj, Reference):
                obj._name = obj_name
            if 'ReferenceList' in globals() and isinstance(obj, ReferenceList):
                obj._name = obj_name
            if 'RemoteReference' in globals() and isinstance(obj, RemoteReference):
                obj._name = obj_name
            if 'ModelProperty' in globals() and isinstance(obj, ModelProperty):
                new_fields[obj_name] = obj
                obj._name = obj_name
                new_attrs[obj_name] = ModelPropertyAccessor(new_fields[obj_name]._fields)
                new_attrs[obj_name]._name = obj_name
                new_attrs[obj_name].kwargs = obj.kwargs
                new_attrs[obj_name].__impl__ = obj.__class__

        new_class = super_new(mcs, name, bases, new_attrs)
        new_class._fields = dict(
            getattr(new_class, "_fields", {}), **new_fields
        )

        for obj_name, obj in attrs.items():
            if isinstance(obj, (Reference, ReferenceList)):
                obj.ref_field.ref_name = obj._name

        for obj_name, obj in attrs.items():
            if 'Field' in globals() and isinstance(obj, Field):
                obj._name = obj_name
            if 'Reference' in globals() and isinstance(obj, Reference):
                obj._name = obj_name
            if 'ReferenceList' in globals() and isinstance(obj, ReferenceList):
                obj._name = obj_name
            if 'ModelProperty' in globals() and isinstance(obj, ModelProperty):
                obj._name = obj_name

        for f,v in new_class._fields.items():
             setattr(v, '_property_path_parent', new_class)
        return new_class


class Filterable:
    _name: str

    def __eq__(self, b):
        return Filter(name=self.get_property_path(), op='eq', var=b)

    def __le__(self, b):
        return Filter(name=self.get_property_path(), op='le', var=b)

    def __ge__(self, b):
        return Filter(name=self.get_property_path(), op='ge', var=b)

    def __lt__(self, b):
        return Filter(name=self.get_property_path(), op='lt', var=b)

    def __gt__(self, b):
        return Filter(name=self.get_property_path(), op='gt', var=b)

    def __contains__(self, b):
        raise Exception ('does not make sense')

    @property
    def not_(self):
        def __in(x):
            return self.in_(x, False)
        class Obj(object): pass
        o = Obj()
        o.in_ = __in
        return o

    @property
    def len_(self):
        class Obj:
            @staticmethod
            def __eq__(b):
                return Filter(name=self.get_property_path(), op='len_eq', var=b)
        return Obj()

    def in_(self, b, is_in=True):
        return Filter(name=self.get_property_path(), op='in_', var=(b, is_in))

    def contains_(self, b, is_in=True):
        return Filter(name=self.get_property_path(), op='contains_', var=(b, is_in))

    def get_property_path(self):
        if hasattr(self, '_property_path_parent'):
            if hasattr (self._property_path_parent, 'get_property_path'):
                return self._property_path_parent.get_property_path() + '.' + self._name
        return self._name

    def has_prop(self, name, value):
        return Filter(name=self.get_property_path(), op='has_prop', var=(name, value))


class Field(Filterable):
    _name: str

    @staticmethod
    def to_db(value):
        return value

    @staticmethod
    def from_db(value):
        return value

    def __init__(self, default=None, nullable=True, hidden=False):
        self.default = default
        self.nullable = nullable
        self.hidden = hidden
        self.ref_name = None

    def __get__(self, obj: 'Model', objtype=None) -> Any:
        if obj is None:
            return self
        v = obj._data.get(self._name, None)
        if v is not None:
            return v
        if hasattr(self, 'default'):
            if callable(self.default):
                return self.default()
            return copy.copy(self.default)
        return None

    def __set__(self, obj: 'Model', value: Any) -> None:
        obj._data[self._name] = value
        if not hasattr(obj, '_dirty'): return # e.g. not in store yet
        if hasattr(obj, '_property_key') and obj._property_key is not None:
            obj._dirty.add(obj._property_key + '.' + self._name)
        else:
            obj._dirty.add(self._name)


class RemoteReferenceList:
    _name: str
    ref_field: str

    def __init__(self, ref: str, model: Union[Type['Model'], str]):
        self.ref_field = ref
        self._model = model

    @property
    def model(self) -> 'Model':
        if type(self._model) == str:
            self._model = ORM.all_models[self._model]
        return self._model

    def __get__(self, obj: 'Model', owner=None) -> Any:
        if not obj: return self.ref_field
        if self._name not in obj._ref_vals:
            obj._ref_vals[self._name] = obj._store.query(self.model).filter(f'{self.ref_field}=="{obj.id}"').all()
        return obj._ref_vals[self._name]


class RemoteReference:
    _name: str
    ref_field: str

    def __init__(self, ref: str, model: Union[Type['Model'], str], use_full_id=False):
        self.ref_field = ref
        self._model = model
        self.use_full_id = use_full_id

    @property
    def model(self) -> 'Model':
        if type(self._model) == str:
            self._model = ORM.all_models[self._model]
        return self._model

    def __get__(self, obj: 'Model', owner=None) -> Any:
        if not obj: return self.ref_field
        if self._name not in obj._ref_vals:
            ref_id = self.use_full_id and obj.full_id or obj.id
            obj._ref_vals[self._name] = obj._store.query(self.model).filter(f'{self.ref_field}=="{ref_id}"').one()
        return obj._ref_vals[self._name]

    def __set__(self, obj: 'Model', value: 'Model') -> None:
        obj._ref_vals[self._name] = value
        obj._store.add(value)


class ReferenceId(Field):
    ref_name: str

    def __init__(self, use_full_id=False, **metadata):
        super().__init__(**metadata)
        self.use_full_id = use_full_id

    def __get__(self, obj: 'Model', objtype=None) -> Any:
        if not obj: return self
        if self.ref_name and self.ref_name in obj._ref_vals:
            return self.use_full_id and obj._ref_vals[self.ref_name]._id or obj._ref_vals[self.ref_name].id
        return super(ReferenceId, self).__get__(obj, objtype)

    def __set__(self, obj: 'Model', value: Any) -> None:
        obj._dirty.add(self._name)
        super(ReferenceId, self).__set__(obj, value)


class Reference:
    _name: str
    ref_field: Field
    back_ref: str

    def __init__(self, ref: Union[Field, str], model: Union[Type['Model'], str]):
        self.ref_field = ref
        self._model = model

    @property
    def model(self) -> 'Model':
        if self._model == 'any':
            return None
        if type(self._model) == str:
            self._model = ORM.all_models[self._model]
        return self._model

    def get_model_from_id(self, obj):
        name = obj._data[self.ref_field._name].split("/")[0]
        name = inflection.singularize(name)
        name = ''.join([n.capitalize() for n in name.split('_')])
        return ORM.all_models[name]

    def __get__(self, obj: 'Model', owner=None) -> Any:
        if not obj: return self.ref_field
        if self._name in obj._ref_vals:
            return obj._ref_vals[self._name]
        if obj._data.get(self.ref_field._name, None) is None:
            return None
        return obj._store.get(self.model or self.get_model_from_id(obj), obj._data[self.ref_field._name])

    def __set__(self, obj: 'Model', value: 'Model') -> None:
        if self._name in obj._ref_vals:
            del obj._ref_vals[self._name]
        if value.id:
            obj._data[self.ref_field._name] = value._id if self.ref_field.use_full_id else value.id
            obj._dirty.add(self.ref_field._name)
            if obj._store:
                obj._store.add(value)
        else:
            obj._ref_vals[self._name] = value
            obj._dirty.add(self.ref_field._name)



class ReferenceIdList(Field):
    ref_name: str

    def __get__(self, obj: 'Model', objtype=None) -> Any:
        if not obj: return self
        if self.ref_name and self.ref_name in obj._ref_vals:
            return [r.id for r in obj._ref_vals[self.ref_name]]
        l = super(ReferenceIdList, self).__get__(obj, objtype)
        if not l:
            obj._data[self._name] = []
            #super(ReferenceIdList, self).__set__(obj, [])
        return super(ReferenceIdList, self).__get__(obj, objtype)

    def __set__(self, obj: 'Model', value: Any) -> None:
        obj._dirty.add(self._name)
        super(ReferenceIdList, self).__set__(obj, value)


class ReferenceListImpl:

    def __init__(self, collection, obj, ref_field):
        super().__init__()
        self.collection = collection
        self.obj = obj
        self.ref_field = ref_field
        self.added = []
        self._refs = {}
        self.__list = self.obj._data.get(self.ref_field._name, []) or []

    def __contains__(self, item):
        return item in self.all()

    def __iter__(self):
        self.iter = chain((self.obj._store.get(self.collection, x) for x in self.__list), self.added)
        return self.iter

    def __next__(self):
        return next(self.iter)

    def all(self):
        return [self.obj._store.get(self.collection, x) for x in self.__list] + self.added

    def append(self, obj: 'Model'):
        if obj not in self.added:
            self.added.append(obj)
        self.obj._dirty.add(self.ref_field._name)

    def remove(self, obj: 'Model'):
        if obj.id:
            self.__list.remove(obj.id)
        else:
            self.added.remove(obj)


class ReferenceList:
    name: str
    ref_field: Field

    def __init__(self, ref: Union[Field, str], model: Union[Type['Model'], str]):
        self.ref_field = ref
        self._model = model

    @property
    def model(self) -> 'Model':
        if type(self._model) == str:
            self._model = ORM.all_models[self._model]
        return self._model

    def __get__(self, obj: 'Model', owner=None) -> Any:
        if not obj: return self.ref_field
        if self._name in obj._ref_vals:
            return obj._ref_vals[self._name]

        obj._ref_vals[self._name] = ReferenceListImpl(self.model, obj, self.ref_field)
        return obj._ref_vals[self._name]


class CollectionList(list):
    _store: 'Store'

    def __init__(self, owner: 'Model', model, filter, own_prop=None, key_only=False):
        super().__init__()
        self.owner = owner
        self._model = model
        self.filter = filter
        self.own_prop = own_prop
        self._store = owner._store
        self.is_loaded = False
        self.key_only = key_only

        collection = self
        class InternalList(list):
            def append(self, __object) -> None:
                collection.append(__object)

            def refresh(self):
                collection.refresh()
                self.clear()
                self.extend(collection.get_list())

        self.InternalListClass = InternalList

        filter = self.filter
        if isinstance(filter, str):
            def func(entity):
                if getattr(entity, self.filter) == getattr(self.owner, own_prop): return True
                if getattr(entity, entity._fields[self.filter].ref_name) == self.owner: return True
                return False
            self._filter_fun = func
        if isinstance(filter, (list, dict)):
            def func(entity):
                for f in self.filter:
                    if getattr(entity, f) == getattr(self.owner, own_prop): return True
                    if getattr(entity, entity._fields[f].ref_name) == self.owner: return True
                return False
            self._filter_fun = func

        if self._store:
            self._setup_store(self._store)

    def get_list(self):
        inter_list = []
        if isinstance(self.filter, str):
            ref = getattr(self.__model, self.filter)
            f = ref._name
            value = self.key_only and self.owner.id or self.owner.full_id
            if value is not None:
                inter_list += self._store.get_all(self.__model, index=f, index_value=value)
            else:
                value = str(id(self.owner))
                inter_list += self._store.get_all(self.__model, index=f+'_no_id', index_value=value)

        if isinstance(self.filter, (list, dict)):
            for f in self.filter:
                ref = getattr(self.__model, f)
                f = ref._name
                value = self.key_only and self.owner.id or self.owner.full_id
                if value is not None:
                    inter_list += self._store.get_all(self.__model, index=f, index_value=value)
                else:
                    value = str(id(self.owner))
                    inter_list += self._store.get_all(self.__model, index=f+'_no_id', index_value=value)
        l = self.InternalListClass()
        l.extend(set(inter_list))
        return l

    def _setup_store(self, store):
        self._store = store

    def refresh(self):
        self.is_loaded = False
        self.load()

    def load(self):
        if self.is_loaded:
            return
        value = getattr(self.owner, self.own_prop)
        if value is None:
            return
        q = self._store.query(self.__model)
        if isinstance(self.filter, (list, dict)):
            for f in [getattr(self.__model, ff) == value for ff in self.filter]:
                f.or_ = True
                q = q.filter(f)
            print(q.make_aql())
        else:
            f: Filter = getattr(self.__model, self.filter) == value
            q = q.filter(f)
        q.all()
        self.is_loaded = True

    @property
    def __model(self):
        if type(self._model) == str:
            self._model = ORM.all_models[self._model]
        return self._model

    def append(self, obj: 'Model') -> None:
        if type(self.filter) == str:
            if self.owner.id and getattr(obj, self.filter) != self.owner.id:
                setattr(obj, self.filter, self.owner.id)
            elif getattr(obj, obj._fields[self.filter].ref_name) != self.owner:
                setattr(obj, obj._fields[self.filter].ref_name, self.owner)
        else:
            raise Exception('cannot add to this collection')


class Collection:
    _name: str

    def __init__(self, model, ref_prop, own_prop=None, key_only=False):
        self._model = model
        self.ref_prop = ref_prop
        self.own_prop = own_prop or (key_only and 'id' or 'full_id')
        self.key_only = key_only

    @property
    def model(self):
        if type(self._model) == str:
            self._model = ORM.all_models[self._model]
        return self._model

    def __get__(self, instance: 'Model', owner=None):
        if self._name not in instance._collection_vals:
            instance._collection_vals[self._name] = CollectionList(instance, self.model, self.ref_prop, self.own_prop, self.key_only)
            if self._name not in instance._collection_loaded:
                instance._collection_vals[self._name].load()
        return instance._collection_vals[self._name].get_list()


class ModelPropertyAccessor(Filterable):
    kwargs: Dict[str, Any]
    _fields: Dict[str, Any]
    _name: str

    @staticmethod
    def to_db(value):
        return value

    @staticmethod
    def from_db(value):
        return value

    def get_data(self, instance):
        return instance._data.data if hasattr(instance._data, 'data') else instance._data

    def __get__(self, instance: 'Model', owner):
        if not instance:
            return self
        if self._name not in instance._properties:
            data = self.get_data(instance)
            d = data.get(self._name, self.__impl__.default.__class__()) or self.__impl__.default.__class__()
            data[self._name] = d
            instance._properties[self._name] = self.__impl__(**self.kwargs, **dict(data=d, parent=instance, name=self._name, store=instance._store))
        return instance._properties[self._name]

    def __set__(self, instance, value):
        if value is None:
            data = self.get_data(instance)
            data[self._name] = None
            instance._properties[self._name] = None
        else:
            prop = self.__get__(instance, None)
            if prop is None:
                del instance._properties[self._name]
                prop = self.__get__(instance, None)
            prop.set_to(value)

    def __init__(self, fields):
        self.kwargs = {}
        self.default = {}
        self._fields = fields
        for f, v in fields.items():
            setattr(self, f, v)
        

    __impl__ = None


T = typing.TypeVar('T', bound='Parent')


class ModelProperty(metaclass=FieldMeta):
    _name: str
    _property_key: str
    _dirty: Set
    kwargs: Dict[str, Any]
    default = {}

    @classmethod
    def create(cls: Type[T]) -> T:
        return cls(data={})

    @staticmethod
    def to_db(v):
        return v

    @staticmethod
    def from_db(v):
        return v

    def get_property_path(self):
        if hasattr(self, '_property_path_parent'):
            if hasattr (self._property_path_parent, 'get_property_path'):
                return self._property_path_parent.get_property_path() + '.' + self._name
        return self._name

    def _setup_store(self, store):
        self._store = store

    def update(self, value):
        raise Exception('not implemented')

    def set_to(self, value):
        raise Exception('not implemented')

    def reset(self):
        self._data.clear()
        self._dirty.add(self._property_key)

    def __init__(self, data=None, parent=None, name=None, store=None, **kwargs):
        super().__init__()
        for f, v in self._fields.items():
            v._property_path_parent = self
        self.hidden = kwargs.get('hidden', False)
        self.nullable = kwargs.get('nullable', True)
        if parent is None and data is None:
            self.kwargs = kwargs
            return
        self._name = name
        self._data = data
        self._properties = {}
        if hasattr(self, 'parent') and self.parent and parent:
            raise Exception('already set')
        self._property_key = ''
        self._store: Store = store
        self._dirty = set()
        if name:
            self._name = name
        self._setup_parent(parent)

    def _setup_parent(self, parent, name=None):
        self._properties = self._properties or {}
        if name:
            self._name = name
        if parent is not None:
            self._store = parent._store
        self.parent = parent
        if self.parent is not None:
            self._dirty = parent._dirty
            if hasattr(self.parent, '_property_key'):
                self._property_key = self.parent._property_key + '.' + self._name
            else:
                self._property_key = self._name
        if self.parent is None:
            self._property_key = self._name

    def _dump(self, changes_only=False):
        if not len(self._dirty):
            return self._data
        data = {}
        changes = set()
        for x in self._dirty:
            if x.startswith(self._property_key + '.'):
                changes.add(x.replace(self._property_key + '.', '').split('.')[0])

        d = self._data
        if not d:
            return d
        for key in d.keys():
            if changes_only and key not in changes: continue
            x = d[key]
            if hasattr(x, '_dump'):
                data[key] = x._dump(changes_only=changes_only)
            else:
                data[key] = x
        return data


class DictProperty(ModelProperty):

    def __init__(self, dict_default=None, **kwargs):
        super().__init__(**kwargs)
        if 'parent' not in kwargs and 'data' not in kwargs:
            self.kwargs.update({'dict_default': dict_default})
            return
        self.dict_default = dict_default

    def items(self):
        if not self._data:
            return []
        return self._data.items()

    def keys(self):
        if not self._data:
            return []
        return self._data.keys()

    def __contains__(self, item):
        return item in self._data

    def __getitem__(self, item):
        return self._data.get(item, self.dict_default)

    def __setitem__(self, key, value):
        self._data[key] = value
        self._dirty.add(self._property_key + '.' + str(key))

    def __delitem__(self, key):
        del self._data[key]
        self._dirty.add(self._property_key + '.' + str(key))

    def update(self, data):
        self._data.update(data)
        self._dirty.add(self._property_key)

    def set_to(self, data):
        self._data.clear()
        self._data.update(data)
        self._dirty.add(self._property_key)

    def to_json(self):
        return self._data.copy()

    def _dump(self, changes_only=False):
        return self._data.copy()


class ObjectProperty(ModelProperty):
    def __init__(self, data=None, **kwargs):
        kwargs['data'] = data
        super().__init__(**kwargs)
        if 'parent' not in kwargs and data is None:
            return
        self._ref_vals = {}
        self._properties = {}
        self._setup()

    def update(self, value):
        self._data.update(value)

    def set_to(self, value):
        if value is None:
            self._data.clear()
            self._data = None
        else:
            self._data.set(value)
        self._dirty.add(self._property_key)

    def _setup(self):
        self._data = ObjectView(self._data)

    def to_json(self):
        return self._dump()

    def _dump(self, changes_only=False):
        if not len(self._dirty):
            return self._data.json
        data = {}
        if self._data is None:
            return None
        for key in self._fields.keys():
            x = getattr(self, key)
            if hasattr(x, '_dump'):
                data[key] = x._dump()
            else:
                data[key] = x
        return data


class ListProperty(list, ModelProperty):
    default = []

    def __init__(self, model=None, **kwargs):
        list.__init__(self)
        ModelProperty.__init__(self, **kwargs)
        if 'parent' not in kwargs:
            self.kwargs.update({'model': model})
            return
        self.model = model
        self._added = []
        self._removed = []
        self._setup()

    def __clear(self):
        self._added = []
        self._removed = []

    def create_item(self):
        return self.model(data={}, parent=self)

    def _setup(self):
        if issubclass(self.model, Model) or issubclass(self.model, ModelProperty):
            self[:] = [self.model(data=x, parent=self, name='[*]') for x in self._data]
        else:
            self[:] = [self.model(x) for x in self._data]

    def append_all(self, arr):
        for a in arr:
            self.append(a)

    def append(self, other):
        if issubclass(self.model, Model) or issubclass(self.model, ModelProperty):
            if hasattr(other, '_setup_parent') and other._setup_parent:
                other._setup_parent(self, '<idx>')

        if not isinstance(other, self.model):
            m = self.model(other)
            other = m
            if hasattr(other, '_setup_parent') and other._setup_parent:
                other._setup_parent(self, '<idx>')

        list.append(self, other)
        self._dirty.add(self._property_key + '.[*]')
        self._added.append(other)
        return other

    def remove(self, value) -> None:
        list.remove(self, value)
        self._removed.append(value)
        self._dirty.add(self._property_key + '.[*]')

    def to_json(self):
        return self._dump()

    def _dump(self, changes_only=False):
        if not len(self._dirty):
            return self._data
        if hasattr(self.model, '_dump'):
            return [x._dump(changes_only=changes_only) for x in self]
        return [x for x in self]


class GraphRelationship:
    pass


class ORM:
    all_models: Dict[str, 'Model'] = {}

    @staticmethod
    def model(model) -> 'Model':
        if type(model) == str:
            model = ORM.all_models[model]
        return model


class ModelMeta(type):
    def __new__(mcs, name, bases, attrs):
        super_new = super(ModelMeta, mcs).__new__

        new_fields = {}
        new_attrs = {**attrs}
        refs = {}

        for obj_name, obj in attrs.items():
            if isinstance(obj, MarshmellowField):
                obj = Field()
            if isinstance(obj, Field):
                # add to schema fields
                new_fields[obj_name] = obj
            if isinstance(obj, ModelProperty):
                # add to schema fields
                new_fields[obj_name] = obj
                new_attrs[obj_name] = ModelPropertyAccessor(new_fields[obj_name]._fields)
                new_attrs[obj_name]._name = obj_name
                new_attrs[obj_name].kwargs = obj.kwargs
                new_attrs[obj_name].__impl__ = obj.__class__
            elif isinstance(obj, Reference):
                refs[obj_name] = obj
            elif isinstance(obj, ReferenceList):
                refs[obj_name] = obj

        new_class = super_new(mcs, name, bases, new_attrs)
        new_class._fields = dict(
            getattr(new_class, "_fields", {}), **new_fields
        )
        new_class._refs = refs
        if not attrs.get('__collection__'):
            new_class.__collection__ = inflection.pluralize(inflection.underscore(name))
        else:
            new_class.__collection__ = attrs.get('__collection__')

        for obj_name, obj in attrs.items():
            if isinstance(obj, (Field, Reference, ReferenceList, MarshmellowField, Collection, RemoteReference, ModelProperty)):
                obj._name = obj_name

        for obj_name, obj in attrs.items():
            if isinstance(obj, (Reference, ReferenceList)):
                obj.ref_field.ref_name = obj._name

        if '_embedded' in attrs and attrs['_embedded']:
            return new_class

        ORM.all_models[name] = new_class

        for f,v in new_class._fields.items():
             setattr(v, '_property_path_parent', new_class)

        return new_class


class DictObject(object):
    def __init__(self, data=None, parent=None, name=None):
        if data is None:
            data = {}
        self.__data = data

        def __setattr__(self, key, value):
            self.__data[key] = value
        setattr(self, '__setattr__', __setattr__)

    def __getattr__(self, item):
        return self.__data.get(item)

    def __contains__(self, item):
        return item in self.__data

    def _dump(self, changes_only=False):
        return self.__data



class ObjectView(object):
    def __init__(self, data=None):
        if data is None:
            data = {}
        self.data = data
        self.get = self.data.get

    def set(self, d):
        self.data = {}
        self.data.update(d)
        self.get = self.data.get

    def update(self, d):
        self.data.update(d)

    def clear(self):
        self.data.clear()

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    @property
    def json(self):
        return self.data


class Model(metaclass=ModelMeta):
    _id = Field()
    _key = Field()
    _rev = Field()
    _fields: Dict[str, Field]
    _ref_vals: Dict[str, 'Model']
    _properties: Dict[str, Any]
    _collection_vals: Dict[str, CollectionList]
    _store: 'Store'
    _embedded: bool = False

    @property
    def rev(self):
        return self._rev

    def __getitem__(self, item):
        return getattr(self, item)

    def __init__(self, data=None, from_db=False, parent=None, name=None, store=None):
        if data is None:
            data = {}
        self._name = name
        self.parent = parent
        self._dirty = set()
        self._store = store
        self._ref_vals = {}
        self._properties = {}
        self._collection_vals = {}
        self._collection_loaded = {}
        self._data = ObjectView()
        if from_db:
            for key, val in self._fields.items():
                if hasattr(val, 'from_db'):
                    data[key] = val.from_db(data.get(key, val.default or None))
            self._dirty.clear()
        self._data.set(data)
        self._setup_parent(parent)

    def _setup_parent(self, parent, name=None):
        if name:
            self._name = name
        if parent is not None:
            self._store = parent._store
            self._dirty = parent._dirty
            self._setup_store(self._store)

    def _setup_store(self, store):
        if self._store and self._store != store:
            raise Exception('already in a store')
        self._store = store
        for key, val in self._fields.items():
            if isinstance(val, ModelProperty):
                getattr(self, key)._setup_store(store)

    def _add_dirty(self, key):
        self._dirty.add(key)

    def set_loaded(self, prop: str):
        if prop not in self._collection_loaded:
            self._collection_loaded[prop] = True

    @property
    def full_id(self):
        if self._id:
            return self._id
        if self._key:
            return self.__collection__ + '/' + self._key
        return None

    @property
    def id(self):
        return self._key

    @id.setter
    def id(self, v):
        self._key = str(v).split('/')[-1]

    @classmethod
    def _load(cls, data, db=None, only=None):
        return cls(data, from_db=True)

    def load(self, data):
        self._data.set(data)

    def update(self, data: dict):
        for k,v in data.items():
            setattr(self, k, v)

    def to_json(self, with_defaults=False):
        data = self._dump(with_defaults=with_defaults)
        for key, field in self._fields.items():
            if field.hidden:
                del data[key]
        return data

    def _dump(self, changes_only=False, with_defaults=False):
        if self._store and not len(self._dirty) and self not in self._store._new and not with_defaults:
            return self._data.json
        data = {}
        changes = set()
        for x in self._dirty:
            changes.add(x.split('.')[0])
        changes.add('_key')
        changes.add('_rev')
        changes_only = changes_only and self not in self._store._new

        for key, field in self._fields.items():
            if changes_only and key not in changes: continue
            x = getattr(self, key)
            x = field.to_db(x)
            if x is None and not field.nullable:
                raise Exception('field {} on {} not nullable'.format(key, self.__class__.__name__))
            if hasattr(x, '_dump'):
                data[key] = x._dump(changes_only=changes_only)
            else:
                data[key] = x
        if '_id' in data and data['_id'] is None:
            del data['_id']
        if data['_key'] is None:
            del data['_key']
        if data['_rev'] is None:
            del data['_rev']

        return data


class PasswordField(Field):

    @staticmethod
    def to_db(value):
        return value.hex()

    @staticmethod
    def from_db(value):
        if isinstance(value, bytes):
            return value
        return bytes.fromhex(value)


class String(Field):
    pass


class Number(Field):
    @staticmethod
    def to_db(value):
        return float(value) if value is not None else None


class Boolean(Field):
    pass


ListField = typing.Union[typing.List, Field]
