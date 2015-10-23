# MongoFS
Access Mongo documents in a FUSE filesystem

This filesystem exposes mongo's structure, features and constrainst.

This is meant to ease the administration of a mongo database, it is NOT a general-purpose filesystem that uses Mongo for storage (look for GridFS if you want this)

## Installation

    # pip install mongofs
    
If you run into any problems, you may need to install fuse-python from your distributions repositories:

    # apt-get install python-fuse

    
## Mount it:

    # mount mongodb://localhost ~/mongo -t mongofs

    
## Navigate it

The filesystem is structured as `/database/collection/field_name/field_value.json`

If the field value is not unique, multiple field names/values pairs are stacked together: `/database/collection/field1/value1/field2/value2/.../fieldN/valueN.json`. This is equivalente to the mongo query `{"field1":value1, "field2":value2, ..., "fieldN":valueN}`.

Note values are JSON strings. Therefore, if your field type is a string, you will need to quote it. E.g., `/db/collection/name/"John Smith".json`. (Yes, it is a bit cumbersome)

### Database operations
    $ mkdir ~/mongo/new_db                        #Create a database
    $ rmdir ~/mongo/old_db                        #Remove a database
    $ mv ~/mongo/old_db_name ~/mongo/new_db_name  #Rename a database

    
### Collection operations
    $ mkdir ~/mongo/db/new_collection                                   #Create a database
    $ rmdir ~/mongo/db/old_collection                                   #Remove a database
    $ mv ~/mongo/db/old_collection_name ~/mongo/db/new_collection_name  #Rename a database

    
### Document operations

#### View a document:
    
    $ cat ~/mongo/example_db/example_collection/foo/\"bar\".json
    {
        "_id": {
            "$oid": "56297a874971f41c1b6e151b"
        }, 
        "foo": "bar", 
        "baz": 56, 
        "abc": {
            "a": "A", 
            "b": "B", 
            "c": "C"
        }
    }

    
#### Create/Update a document:

    $ echo '{"a":"b"}' > ~/mongo/example_db/example_collection/_id/56.json 
    $ cat ~/mongo/example_db/example_collection/_id/56.json
    {
        "_id": 56, 
        "a": "b"
    }
    

#### Delete document:

    $ rm ~/mongo/example_db/example_collection/_id/56.json
    
    
## Background
Sometimes a human needs to be view or edit a [Mongo](https://www.mongodb.org/) document. And when humans are involved, a good UI helps a lot!

I have been using [Robomongo](http://robomongo.org/) for that. It is a fine UI, but it doesn't compare with being able to use my favorite `$EDITOR`.

When I realized I was copy-pasting between robomongo and my text editor all the time, I decided It was time to fix that!


## TODO:

- Read/Write documents using [Mongo's Query Syntax](http://docs.mongodb.org/manual/reference/mongodb-extended-json/)
- Support for renaming filters and files
