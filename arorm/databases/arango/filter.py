from arorm.databases.arango.query import ArangoStoreQuery


class ArangoFilter:
    def __init__(self, name, op, var):
        self.vars = {}
        self.expression = ''
        self.or_ = False
        self.prepend = True
        self.rec_name = None
        getattr(self, op)(name, var)

    def eq(self, name: str, var):
        v = name.replace('.', '_')
        self.expression = '{0}==@{1}'.format(name, v)
        self.vars[v] = var

    def le(self, name, var):
        v = name.replace('.', '_')
        self.expression = '{0}<=@{1}'.format(name, v)
        self.vars[v] = var

    def ge(self, name, var):
        v = name.replace('.', '_')
        self.expression = '{0}>=@{1}'.format(name, v)
        self.vars[v] = var

    def lt(self, name, var):
        v = name.replace('.', '_')
        self.expression = '{0}<@{1}'.format(name, v)
        self.vars[v] = var

    def gt(self, name, var):
        v = name.replace('.', '_')
        self.expression = '{0}>@{1}'.format(name, v)
        self.vars[v] = var

    def in_(self, name, var):
        v = name.replace('.', '_')
        b, is_in = var
        self.expression = f'CONTAINS_ARRAY(@{v}_in, rec.{name}) == {is_in}'
        self.vars[v+'_in'] = isinstance(b, ArangoStoreQuery) and b._make_aql() or b
        self.prepend = False

    def len_eq(self, name, var):
        v = name.replace('.', '_')
        self.expression = 'LENGTH(rec.{0})==@{1}'.format(name, v + '_count')
        self.vars[v + '_count'] = var
        self.prepend = False

    def contains_(self, name, var):
        v = name.replace('.', '_')
        b, is_in = var
        self.expression = f'CONTAINS_ARRAY(rec.{name}, @{v}_in) == {is_in}'
        self.vars[v +'_in'] = isinstance(b, ArangoStoreQuery) and b._make_aql() or b
        self.prepend = False

    def has_prop(self, name, var):
        sub_name, val = var
        v = sub_name.replace('.', '_')
        self.expression = f'rec.{name}.{sub_name} == @{v}_val'
        self.vars[v+'_val'] = isinstance(val, ArangoStoreQuery) and val._make_aql() or val
        self.prepend = False


def filter(self, *args, **kwargs):
    for a in args:
        if isinstance(a, ArangoFilter):
            old_filter(self, a.expression, a.or_ or kwargs.get('_or', False), a.prepend, a.rec_name, **a.vars)
        else:
            old_filter(self, a, **kwargs)
    return self
