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
from io import BytesIO

class MongoFS(RouteFS):
    def __init__(self, *args, **kwargs):
        RouteFS.__init__(self, *args, **kwargs)
        self.fuse_args.add("allow_other", True)
        self.host = "localhost"
        self.json_escaping = False
        self.json_encoding = "utf8"
        self.json_indent = 4
        self.file_cache = {}
        
        self.parser.add_option(mountopt="host",
            metavar="HOSTNAME", 
            default=self.host,
            help="Adress of mongo server. Either host, host:port or a mongo URI [default: %default]")
        self.parser.add_option(mountopt="json_escaping",
            action="store_true", dest="json_escaping",
            default=self.json_escaping,
            help="Escapes all non-ascii characters on the JSON strings [default: %default]")
        self.parser.add_option(mountopt="json_encoding",
            metavar="ENCODING", 
            default=self.json_encoding,
            help="Character encoding of JSON document [default: %default]")
        self.parser.add_option(mountopt="json_indent",
            metavar="INDENTATION", 
            default=self.json_indent,
            type=int,
            help="Size of indentation on pretty-printed JSON documents [default: %default]")
        
        
    def fsinit(self):
        self.mongo = MongoClient(self.host)
        
    def statfs(self):
        return fuse.StatVfs(
            f_bsize=4096,
            f_blocks=1048576,
            f_bfree=1048576,
            f_bavail=1048576,
            f_files=1048576,
            f_ffree=1048576,
            f_favail=1048576)

    def make_map(self):
        m = Mapper()
        m.connect('/', controller='getDatabaseList')
        m.connect('/{database}', controller='getDatabase')
        m.connect('/{database}/{collection}', controller='getCollection')
        m.connect('/{database}/{collection}/{document_id}.json', controller='getDocument')
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
        return fuse.Stat(
            st_mode=stat.S_IFDIR | 0777,
            st_nlink=2)

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
      
        return fuse.Stat(
            st_mode=stat.S_IFDIR | 0777,
            st_nlink=2)

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
      
        return fuse.Stat(
            st_mode=stat.S_IFDIR | 0777,
            st_nlink=2)

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
        if target.collection in self.mongo[target.database].collection_names():
            return -errno.EEXIST

        self.mongo.admin.command(
            "renameCollection", "%s.%s" % (self.database, self.collection),
            to="%s.%s" % (target.database, target.collection)
        )

        return 0

    def readdir(self, offset):
        for member in ['.', '..']:
            yield fuse.Direntry(str(member))
        for doc in self.mongo[self.database][self.collection].find({}, {}):
            yield fuse.Direntry(str(doc["_id"]) + ".json")
            

# truncate() runs without a file handler.
# To make it work, we must share state outside of the "FileHandle" abstraction
class MongoSharedFileHandle:
    def __init__(self, buffer):
        self.buffer = buffer
        self.dirty = False
        self.refs = 0

class MongoDocument():
    def __init__(self, mongofs, database, collection, document_id):
        self.mongofs = mongofs
        self.mongo = mongofs.mongo
        self.database = database
        self.collection = collection
        self.document_id = document_id

    def fetch_doc_json(self):
        doc = self.mongo[self.database][self.collection].find_one({"_id": self.document_id}, {"_id": 0})
        if doc is not None:
            if len(doc) == 0:
                return ""
            else:
                return dumps(doc, indent=self.mongofs.json_indent, ensure_ascii=self.mongofs.json_escaping).encode(self.mongofs.json_encoding, errors='replace') + "\n"
          
    def store_doc_json(self, json):
        try:
            if len(json.strip()):
                doc = loads(json.decode(self.mongofs.json_encoding, errors='replace'))
            else:
                doc = {}
        except:
            return -errno.EINVAL
              
        doc["_id"] = self.document_id
        self.mongo[self.database][self.collection].update({"_id": self.document_id}, doc)
        return 0
      
    def getattr(self):
        json = self.fetch_doc_json()
        if json is None:
            return -errno.ENOENT
      
        return fuse.Stat(
            st_mode=stat.S_IFREG | 0666,
            st_nlink=1,
            st_size=len(json))
      
    def create(self, flags, mode):
        self.mongo[self.database][self.collection].insert_one({"_id": self.document_id})
        return self.open(flags)
    
    def unlink(self):
        self.mongo[self.database][self.collection].delete_one({"_id": self.document_id})
        return 0

    def truncate(self, len):
        fh = self.open(0)
        fh.buffer.truncate(len)
        self.release(0, fh)
        return 0        
    
    def open(self, flags):
        fh = self.mongofs.file_cache.get( (self.database, self.collection, self.document_id), None )
        if fh is None:
            fh = MongoSharedFileHandle(BytesIO(self.fetch_doc_json()))
            self.mongofs.file_cache[ (self.database, self.collection, self.document_id) ] = fh
        fh.refs += 1
        return fh
    
    def release(self, flags, fh):
        fh.refs -= 1
        if fh.refs == 0:
            del self.mongofs.file_cache[ (self.database, self.collection, self.document_id) ]
            self.flush(fh)

    def flush(self, fh):
        if fh.dirty:
            fh.dirty=False
            return self.store_doc_json(fh.buffer.getvalue())
      
    def read(self, length, offset, fh):
        fh.buffer.seek(offset)
        return fh.buffer.read(length)
      
    def write(self, buffer, offset, fh):
        fh.dirty = True
        fh.buffer.seek(offset)
        fh.buffer.write(buffer)
        return len(buffer)


if __name__ == '__main__':
    main(MongoFS)
