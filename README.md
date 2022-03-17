# arorm
arango orm with identity pattern


## examples

```python
import typing
from arorm import ListProperty, Field, ObjectProperty, PasswordField, Model, ReferenceId, Reference, ReferenceIdList, ReferenceList, Store, RemoteReferenceList

class UserAttributes(ObjectProperty):
    class Settings(ObjectProperty):
        notifications = Field(default=False)
        html5Notifications = Field(default=False)
    last_update: float = Field(default=0)
    last_login: float = Field(default=0)
    settings = Settings()
    
class User(Model):
    __collection__ = 'users'
    name = Field()
    password = PasswordField(hidden=True)
    email = Field()
    attributes = UserAttributes()
    achievements: typing.List[str] = ListProperty(str)
    books: typing.List['Book'] = RemoteReferenceList('author_id', 'Book')

class Book(Model):
    __collection__ = 'books'
    author_id = ReferenceId()
    author = Reference(author_id, User)
    co_authors_ids = ReferenceIdList()
    co_authors: typing.List[User] = ReferenceList(co_authors_ids, User)

db = dict(host='127.0.01', user='root', password='root', port=8529, driver='arango')
store = Store(db)
store.setup_db()
user = store.create(User) # will create a user on commit
book = store.create(Book) # will create a book on commit
book.author = user
book.co_authors.append(user)

store.query(User).filter(User.name == 'admin').delete() # queued to be executed on commit

store.run_after_commit(lambda: print('did commit'))

store.commit()

user.to_json() # -> will now have _id, _rev
book.to_json() # -> will now have author_id set to same id as user
```
