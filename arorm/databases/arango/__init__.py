from typing import List, Tuple

from arango import ArangoClient, exceptions
from arango_orm.database import Database

from arorm.databases.abstract import DatabaseFactory, AbstractDatabase, RawQuery
from arorm.databases.arango.filter import ArangoFilter
from arorm.databases.arango.query import ArangoStoreQuery


class ArangoDatabaseFactory(DatabaseFactory):
    Query = ArangoStoreQuery
    Filter = ArangoFilter

    @staticmethod
    def create_database(database):
        client = ArangoClient(hosts='http://' + database.host + ':' + str(database.port))
        db = client.db(name=database.db_name, username=database.user, password=database.password)
        return ArangoDatabase(db)


class ArangoDatabase(Database, AbstractDatabase):

    def _find_deps(self, items, entity: 'Model', collected: set):
        insertions = set()
        from core.orm import ReferenceListImpl
        if entity not in items: return insertions
        if entity not in collected:
            insertions.add(entity)
            collected.add(entity)
        else:
            return insertions
        if isinstance(entity, ReferenceListImpl):
            all = entity.all()
            for a in all:
                if a in items:
                    insertions |= (self._find_deps(items, getattr(entity, a), collected))
            return insertions
        else:
            for name in entity._refs.keys():
                if getattr(entity, name):
                    insertions |= (self._find_deps(items, getattr(entity, name), collected))
        return insertions

    def _compute_batch_order(self, items, collected: set):
        insertions = set(items)
        batches = []
        b = set()
        while len(insertions):
            for i in insertions:
                from core.orm import ReferenceListImpl
                if isinstance(i, ReferenceListImpl):
                    insertions = insertions - i
                    continue
                if all((r in collected or isinstance(r, ReferenceListImpl) or r._id for r in i._ref_vals.values())):
                    b.add(i)
            if not len(b):
                i = insertions.pop()
                raise Exception('unable to commit, possible loop -> ' + str(i.to_json()))
            insertions = insertions - b
            collected.update(b)
            batches.append(list(b))
            b = set()
        batches.append(b)
        return batches

    def commit(self, new, changes, removed, query_ops: List[Tuple['ArangoStoreQuery', str]]):
        collected = set()
        insertions = self._compute_batch_order(new, collected)
        changes = self._compute_batch_order(changes, collected)
        collections = set((e.__collection__ for b in insertions for e in b))
        collections = collections | (set((e.__collection__ for b in changes for e in b)))
        collections = collections | (set((e.__collection__ for e in removed)))
        collections = collections | set(coll for ops in query_ops for coll in ops[2])
        tx = self.begin_transaction(write=list(collections))

        for op in query_ops:
            if op[1] == 'delete':
                aql = op[0]._make_aql()
                aql += "\n REMOVE {_key: rec._key} IN @@collection"
                tx.aql.execute(aql, bind_vars=op[0]._bind_vars)
                query_ops.remove(op)

        for n in removed:
            tx.delete_document(n._id)

        for batch in insertions:
            collection_dict = {}
            for b in batch:
                if b._rev:
                    print(b._id, b._rev, b._dump())
                    raise Exception('cannot be new')
                collection_dict[b.__collection__] = collection_dict.get(b.__collection__, [])
                collection_dict[b.__collection__].append(b)
            if not len(collection_dict.keys()):
                continue
            full_aql = ''
            order = []
            for collection, items in collection_dict.items():
                order.extend(items)
                aql = f"""
                 let {collection}_result = (FOR doc in @docs.{collection}
                   INSERT doc INTO {collection}
                   LET inserted = NEW
                   RETURN {{ _id: inserted._id, _key: inserted._key, _rev: inserted._rev }}
                )
                """
                full_aql += aql
            for collection in collection_dict.keys():
                collection_dict[collection] = [x._dump() for x in collection_dict[collection]]

            if len(collection_dict.keys()) > 1:
                aql_return = 'UNION({0})'.format(','.join([k + '_result' for k in collection_dict.keys()]))
            else:
                aql_return = list(collection_dict.keys())[0] + '_result'
            full_aql += 'FOR r in ' + aql_return + ' RETURN r'

            print('full_aql', full_aql)
            try:
                pass

                #print('json len', json.dumps(collection_dict))
            except Exception:
                pass
            result = tx.aql.execute(full_aql, bind_vars={'docs': collection_dict})
            for i, r in enumerate(result):
                order[i].update(r)
                order[i]._dirty.clear()
        for batch in changes:
            collection_dict = {}
            for b in batch:
                collection_dict[b.__collection__] = collection_dict.get(b.__collection__, [])
                collection_dict[b.__collection__].append(b)
            if not len(collection_dict.keys()):
                continue
            full_aql = ''
            order = []
            for collection, items in collection_dict.items():
                order.extend(items)
                aql = f"""
                 let {collection}_result = (FOR doc in @docs.{collection}
                   UPDATE doc IN {collection}
                   LET inserted = NEW
                   RETURN {{ _id: inserted._id, _key: inserted._key, _rev: inserted._rev }}
                )
                """
                full_aql += aql
            for collection in collection_dict.keys():
                collection_dict[collection] = [x._dump(changes_only=True) for x in collection_dict[collection]]

            if len(collection_dict.keys()) > 1:
                aql_return = 'UNION({0})'.format(','.join([k + '_result' for k in collection_dict.keys()]))
            else:
                aql_return = list(collection_dict.keys())[0] + '_result'
            full_aql += 'FOR r in ' + aql_return + ' RETURN r'

            print('full_aql', full_aql)
            #print('json len', json.dumps(collection_dict))
            result = tx.aql.execute(full_aql, bind_vars={'docs': collection_dict})
            for i, r in enumerate(result):
                order[i].update(r)

        for op in query_ops:
            if op[1] == 'execute':
                q: RawQuery = op[0]
                tx.aql.execute(q.query, **q.kwargs)
            if op[1] == 'delete':
                aql = op[0]._make_aql()
                aql += "\n REMOVE {_key: rec._key} IN @@collection"
                tx.aql.execute(aql, bind_vars=op[0]._bind_vars)
        tx.commit_transaction()

    def setup_db(self, models, graphs=[]):
        print('creating models', [m.__collection__ for m in models])
        for m in models:
            try:
                self.create_collection(m)
            except exceptions.CollectionCreateError as e:
                if e.http_code != 409: raise e

        for g in graphs:
            graph_instance = g(connection=self)
            try:
                self.create_graph(graph_instance)
            except exceptions.GraphCreateError as e:
                if e.http_code != 409: raise e

    def _verify_collection(self, col):
        return

