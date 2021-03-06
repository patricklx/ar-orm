from typing import Dict, Type
from arorm.databases.abstract import AbstractDatabase, DatabaseFactory
from arorm.databases.arango import ArangoDatabaseFactory

databases: Dict[str, Type[DatabaseFactory]] = {
    'arango': ArangoDatabaseFactory
}


def register(name, db):
    databases[name] = db
