#!/usr/bin/python


import stat
import errno
import fuse
import os
from routefs import RouteFS, main, TreeEntry, RouteStat
from routes import Mapper
from pymongo import MongoClient
from bson.json_util import loads, dumps
from bson.objectid import ObjectId
from StringIO import StringIO
from cStringIO import StringIO as cStringIO

class MongoFS(RouteFS):
    def __init__(self, *args, **kwargs):
        RouteFS.__init__(self, *args, **kwargs)
        self.fuse_args.add("allow_other", True)
        self.mongo = MongoClient("localhost")

    def make_map(self):
        m = Mapper()
        m.connect('/', controller='getDatabaseList')
        m.connect('/{database}', controller='getDatabase')
        m.connect('/{database}/{collection}', controller='getCollection')
        m.connect('/{database}/{collection}/{document_id}', controller='getDocument')
        #m.connect('/README.txt', controller='getReadme')
        #m.connect('/{action}', controller='getLocker')
        return m
      
    def getDatabaseList(self, **kwargs):
        return MongoServer(self)

    def getDatabase(self, database, **kwargs):
        return MongoDatabase(self, database)

    def getCollection(self, database, collection, **kwargs):
        return MongoCollection(self, database, collection)

    def getDocument(self, database, collection, document_id, **kwargs):
        return MongoDocument(self, database, collection, ObjectId(document_id))


class MongoServer():
    def __init__(self, mongofs):
        self.mongofs = mongofs
        self.mongo = mongofs.mongo

    def getattr(self):
        st = RouteStat()
        st.st_mode = stat.S_IFDIR | 0777
        st.st_nlink = 2
        return st

    def readdir(self, offset):
        for member in ['.', '..'] + self.mongo.database_names():
            yield fuse.Direntry(str(member))


class MongoDatabase():
    def __init__(self, mongofs, database):
        self.mongofs = mongofs
        self.mongo = mongofs.mongo
        self.database = database

    def getattr(self):
        if self.database not in self.mongo.database_names():
            return -errno.ENOENT
      
        st = RouteStat()
        st.st_mode = stat.S_IFDIR | 0777
        st.st_nlink = 2
        return st

    def mkdir(self, mode):
        if self.database in self.mongo.database_names():
            return -errno.EEXIST
          
        # There is no explicit "createDatabase" method. We must create something inside it.
        self.mongo[self.database].create_collection("_")
        self.mongo[self.database].drop_collection("_")
        return 0

    def rmdir(self):
        if self.database not in self.mongo.database_names():
            return -errno.ENOENT
          
        self.mongo.drop_database(self.database)
        return 0

    def rename(self, target):
        target = self.mongofs._get_file(target)
        
        if not isinstance(target, MongoDatabase):
            return -errno.EINVAL
        if self.database not in self.mongo.database_names():
            return -errno.ENOENT
        if target.database in self.mongo.database_names():
            return -errno.EEXIST

        # There is no explicit "renameDatabase" method. We must clone a new DB and drop the old one.              
        self.mongo.admin.command('copydb', fromdb=self.database, todb=target.database)
        self.mongo.drop_database(self.database)
        return 0

    def readdir(self, offset):
        for member in ['.', '..'] + self.mongo[self.database].collection_names(include_system_collections=False):
            yield fuse.Direntry(str(member))
            

class MongoCollection():
    def __init__(self, mongofs, database, collection):
        self.mongofs = mongofs
        self.mongo = mongofs.mongo
        self.database = database
        self.collection = collection

    def getattr(self):
        if self.collection not in self.mongo[self.database].collection_names():
            return -errno.ENOENT
      
        st = RouteStat()
        st.st_mode = stat.S_IFDIR | 0777
        st.st_nlink = 2
        return st

    def mkdir(self, mode):
        if self.collection in self.mongo[self.database].collection_names():
            return -errno.EEXIST
          
        self.mongo[self.database].create_collection(self.collection)
        return 0

    def rmdir(self):
        if self.collection not in self.mongo[self.database].collection_names():
            return -errno.ENOENT
          
        self.mongo[self.database].drop_collection(self.collection)
        return 0

    def rename(self, target):
        target = self.mongofs._get_file(target)
        
        if not isinstance(target, MongoCollection):
            return -errno.EINVAL
        if self.collection not in self.mongo[self.database].collection_names():
            return -errno.ENOENT
        if target.collection in self.mongo[self.database].collection_names():
            return -errno.EEXIST

        # TODO
        return 0

    def readdir(self, offset):
        for member in ['.', '..']:
            yield fuse.Direntry(str(member))
        for doc in self.mongo[self.database][self.collection].find({}, {}):
            yield fuse.Direntry(str(doc["_id"]))
            

class MongoDocument():
    def __init__(self, mongofs, database, collection, document_id):
        self.mongofs = mongofs
        self.mongo = mongofs.mongo
        self.database = database
        self.collection = collection
        self.document_id = document_id

    def fetch_doc_json(self):
        doc = self.mongo[self.database][self.collection].find_one({"_id": self.document_id}, {"_id": 0})
        if doc:
            return dumps(doc, indent=4) + "\n"
          
    def store_doc_json(self, json):
        try:
            doc = loads(json)
        except:
            return -errno.EINVAL
              
        print ">>>>>>", doc
              
        doc["_id"] = self.document_id
        self.mongo[self.database][self.collection].update({"_id": self.document_id}, doc)
        return 0
      
    def getattr(self):
        json = self.fetch_doc_json()
        if json is None:
            return -errno.ENOENT
      
        st = RouteStat()
        st.st_mode =  stat.S_IFREG | 0666
        st.st_nlink = 1
        st.st_size = len(json)
        return st
      
    def open(self, flags):
        if flags & os.O_RDWR: #RW buffer
            return StringIO(self.fetch_doc_json())
        elif flags & os.O_WRONLY: #Empty writeable buffer
            return StringIO()
        else: #Readonly buffer
            return cStringIO(self.fetch_doc_json()) 
    
    def release(self, flags, fh):
        return 0

    def flush(self, fh):
        #If is writeable, needs to sync with DB
        if hasattr(fh, "write"):
            print("WRITE!")
            return self.store_doc_json(fh.getvalue())
      
    def read(self, length, offset, fh):
        fh.seek(offset)
        return fh.read(length)
      
    def write(self, buffer, offset, fh):
        fh.seek(offset)
        fh.write(buffer)
        return len(buffer)

    def truncate(self, *args):
        print("===========", args)

if __name__ == '__main__':
    main(MongoFS)
