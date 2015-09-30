# MongoFS
Access Mongo documents in a FUSE filesystem

## Background
Sometimes a human needs to be view or edit a [Mongo](https://www.mongodb.org/) document. And when humans are involved, a good UI helps a lot!

I have been using [Robomongo](http://robomongo.org/) for that. It is a fine UI, but it doesn't compare with being able to use my favorite `$EDITOR`.

When I realized I was copy-pasting between robomongo and my text editor all the time, I decided It was time to fix that!

## Features

MongoFS exposes documents as `/{database}/{collection}/{document_id}.json`

### Databases

Supported operations:

- List
- Create (Currently implemented by creating and removing a subcollection)
- Remove
- Rename (Currently implemented as a copydb + drop_database)

### Collections

Supported operations:

- List
- Create
- Remove
- Rename

### Documents

Supported operations:

- List
- Create
- Remove
- Read
- Write, _but has issues..._

TODO:

- Support for truncate/ftruncate (Otherwise we can only save larger files)
- Make it compatible with [Kate](kate-editor.org).
- Read/Write documents using [Mongo's Query Syntax](http://docs.mongodb.org/manual/reference/mongodb-extended-json/)
- Preserve field order on Load/Store
- Support for custom filename attributes ("{field_name}/{field_value}.json" instead of "{id}.json")
- Support for O_APPEND _?_
- Support for rename
- getattr is quite slow, since it needs to read the whole document, transform to a JSon string and calculate the length.
