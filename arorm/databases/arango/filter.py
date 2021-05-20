from arorm.databases.arango.query import ArangoStoreQuery


class ArangoFilter:
    def __init__(self, name, op, var):
        self.vars = {}
        self.expression = ''
        self.or_ = False
        self.prepend = True
        self.rec_name = None
        getattr(self, op)(name, var)

    def eq(self, name, var):
        self.expression = '{0}==@{1}'.format(name, name)
        self.vars[name] = var

    def le(self, name, var):
        self.expression = '{0}<=@{1}'.format(name, name)
        self.vars[name] = var

    def ge(self, name, var):
        self.expression = '{0}>=@{1}'.format(name, name)
        self.vars[name] = var

    def lt(self, name, var):
        self.expression = '{0}<@{1}'.format(name, name)
        self.vars[name] = var

    def gt(self, name, var):
        self.expression = '{0}>@{1}'.format(name, name)
        self.vars[name] = var

    def _in(self, name, var):
        b, is_in = var
        self.expression = f'CONTAINS_ARRAY(@{name}_in, rec.{name}, true) == {is_in}'
        self.vars[name+'_in'] = isinstance(b, ArangoStoreQuery) and b._make_aql() or b
        self.prepend = False

    def len_eq(self, name, var):
        self.expression = 'LENGTH(rec.{0})==@{1}'.format(name, name + '_count')
        self.vars[name + '_count'] = var
        self.prepend = False

    def contains_(self, name, var):
        b, is_in = var
        self.expression = f'CONTAINS_ARRAY(rec.{name}, @{name}_in, true) == {is_in}'
        self.vars[name+'_in'] = isinstance(b, ArangoStoreQuery) and b._make_aql() or b
        self.prepend = False

    def has_prop(self, name, var):
        sub_name, val = var
        self.expression = f'rec.{name}.{sub_name} == @{sub_name}_val'
        self.vars[sub_name+'_val'] = isinstance(val, ArangoStoreQuery) and val._make_aql() or val
        self.prepend = False


def filter(self, *args, **kwargs):
    for a in args:
        if isinstance(a, ArangoFilter):
            old_filter(self, a.expression, a.or_ or kwargs.get('_or', False), a.prepend, a.rec_name, **a.vars)
        else:
            old_filter(self, a, **kwargs)
    return self
