#!/usr/bin/python
# -*- coding: utf-8 -*-

import stat
import errno
import fuse
import os
from routefs import RouteFS, main, TreeEntry, RouteStat
from routes import Mapper
from pymongo import MongoClient
from bson.json_util import dumps, object_hook as bson_object_hook
from json import loads as json_loads
from bson.objectid import ObjectId
from bson import SON, Code
from io import BytesIO


#Hack to preserve order of object fields
def loads(*args, **kwargs):
    kwargs['object_pairs_hook'] = lambda x: bson_object_hook(SON(x))
    return json_loads(*args, **kwargs)
    

class MongoFS(RouteFS):
    def __init__(self, *args, **kwargs):
        RouteFS.__init__(self, *args, **kwargs)
        self.fuse_args.add("allow_other", True)
        self.host = "localhost"
        self.json_escaping = False
        self.json_encoding = "utf8"
        self.json_indent = 4
        self.open_file_cache = {}
        
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
        
    def escape(self, name):
        print(repr(name))
        if name == '.':
            return '&period;'
        if name == '.,':
            return '&period;&period;'
        return name \
            .replace("&" , "&amp;")  \
            .replace("/" , "&sol;")  \
            .replace("\\", "&bsol;") \
            .replace("|" , "&vert;")
            
    def unescape(self, name):
        return name \
            .replace("&sol;"   , "/")  \
            .replace("&bsol;"  , "\\") \
            .replace("&vert;"  , "|")  \
            .replace("&period;", ".")  \
            .replace("&amp;"   , "&")
        
    def fsinit(self):
        self.mongo = MongoClient(self.host, document_class=SON)
        
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
        m.connect('/', controller='getRoot')
        m.connect('/{database}', controller='getDatabase')
        m.connect('/{database}/{collection}', controller='getCollection')
        m.connect('/{database}/{collection}/{filter_path:.*}.json', controller='getDocument')
        m.connect('/{database}/{collection}/{filter_path:.*}', controller='getFilter')
        #m.connect('/README.txt', controller='getReadme')
        #m.connect('/{action}', controller='getLocker')
        return m
      
    def getRoot(self, **kwargs):
        try:
            return MongoRoot(self)
        except:
            return None
        
    def getDatabase(self, database, **kwargs):
        try:
            return MongoDatabase(self, self.unescape(database))
        except:
            return None
        
    def getCollection(self, database, collection, **kwargs):
        try:
            return MongoCollection(self, self.unescape(database), self.unescape(collection))
        except:
            return None
        
    def parse_path(self, filter_path):
        try: 
            filter_path = tuple(map(self.unescape, filter_path.split("/")))
            filter = SON(zip(filter_path[0::2], map(loads, list(filter_path[1::2]))))
            current_field = filter_path[-1] if (len(filter_path) % 2) == 1 else None
            return (filter, current_field)
        except:
            return None
          
    def getFilter(self, database, collection, filter_path, **kwargs):
        try:
            filter, current_field = self.parse_path(filter_path)
            return MongoFilter(self, self.unescape(database), self.unescape(collection), filter, current_field)
        except:
            return None
          
    def getDocument(self, database, collection, filter_path, **kwargs):
        try:
            filter, current_field = self.parse_path(filter_path)
            if current_field is None:
                return MongoDocument(self, self.unescape(database), self.unescape(collection), filter)
        except:
            return None

class DirEntry(fuse.Direntry):
    def __init__(self, name, **kwargs):
        fuse.Direntry.__init__(self, name.encode("utf8"), **kwargs)

class MongoRoot():
    def __init__(self, mongofs):
        self.mongofs = mongofs
        self.mongo = mongofs.mongo

    def getattr(self):
        return fuse.Stat(
            st_mode=stat.S_IFDIR | 0777,
            st_nlink=2)

    def readdir(self, offset):
        yield DirEntry('.')
        yield DirEntry('..')
        for member in self.mongo.database_names():
            yield DirEntry(self.mongofs.escape(member))


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
            return -errno.EACCES
        if self.database not in self.mongo.database_names():
            return -errno.ENOENT
        if target.database in self.mongo.database_names():
            return -errno.EEXIST

        # There is no explicit "renameDatabase" method. We must clone a new DB and drop the old one.              
        self.mongo.admin.command('copydb', fromdb=self.database, todb=target.database)
        self.mongo.drop_database(self.database)
        return 0

    def readdir(self, offset):
        yield DirEntry('.')
        yield DirEntry('..')
        for member in self.mongo[self.database].collection_names(include_system_collections=False):
            yield DirEntry(self.mongofs.escape(member))
            

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
            return -errno.EACCES
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
        return MongoFilter(self.mongofs, self.database, self.collection, {}, None).readdir(offset)


class MongoFilter():
    def __init__(self, mongofs, database, collection, filter, current_field):
        self.mongofs = mongofs
        self.mongo = mongofs.mongo
        self.database = database
        self.collection = collection
        self.filter = filter
        self.current_field = current_field

    def getattr(self):
        return fuse.Stat(
            st_mode=stat.S_IFDIR | 0777,
            st_nlink=2)

    def mkdir(self, mode):
        return -errno.EACCES

    def rmdir(self):
        if self.current_field is None:
            self.mongo[self.database][self.collection].delete_many(self.filter)
        else:
            self.mongo[self.database][self.collection].update_many(self.filter, {"$unset": {self.current_field:1}})
        return 0
      
    def rename(self, target):
        return -errno.EACCES

    def readdir(self, offset):
        yield DirEntry('.')
        yield DirEntry('..')
        
        # TODO: Needs optimisation
        if self.current_field is None:
            attrs = set()
            for doc in self.mongo[self.database][self.collection].find(self.filter, limit=50):
                for key, value in doc.iteritems():
                    if key not in self.filter and not isinstance(value, dict) and not isinstance(value, list):
                        attrs.add(key)
            attrs -= set(self.filter.keys())
            
            for attr in attrs:
                yield DirEntry(self.mongofs.escape(attr))
        else:
            # Count the distinct values of the field
            q = dict(self.filter)
            q[self.current_field] = { "$exists": True }
            values = self.mongo[self.database][self.collection].inline_map_reduce(
                map    = Code("""function() { emit(this[fieldName], 1); }"""),
                reduce = Code("""function(key, values) { return Array.sum(values); }"""),
                query  = q,
                scope  = {"fieldName": self.current_field})
            values = ([(entry["_id"], entry["value"]) for entry in values])
            for (value, count) in values:
                if not isinstance(value, dict) and not isinstance(value, list):
                    yield DirEntry(self.mongofs.escape(dumps(value, ensure_ascii=False)) + (".json" if count == 1 else ""))
                
                
# truncate() runs without a file handler.
# To make it work, we must share state outside of the "FileHandle" abstraction
class MongoSharedFileHandle:
    def __init__(self, buffer, id):
        self.buffer = buffer
        self.id = id
        self.dirty = False
        self.refs = 0
        self.flush_ret = 0


class MongoDocument():
    def __init__(self, mongofs, database, collection, filter):
        self.mongofs = mongofs
        self.mongo = mongofs.mongo
        self.database = database
        self.collection = collection
        self.filter = filter
        self.id = (database, collection) + tuple([item for pair in filter.iteritems() for item in pair])
        print(self.id)

    def getattr(self):
        fh = self.open(0)
        if not isinstance(fh, MongoSharedFileHandle):
            return -errno.ENOENT
      
        ret = fuse.Stat(
            st_mode=stat.S_IFREG | 0666,
            st_nlink=1,
            st_size=len(fh.buffer.getvalue()))
      
        self.release(0, fh)
        return ret
      
    def create(self, flags, mode):
        self.mongo[self.database][self.collection].insert_one(self.filter)
        return self.open(flags)
    
    def unlink(self):
        self.mongo[self.database][self.collection].delete_one(self.filter)
        return 0

    def truncate(self, len):
        fh = self.open(0)
        fh.buffer.truncate(len)
        self.release(0, fh)
        return 0        
    
    def open(self, flags):
        fh = self.mongofs.open_file_cache.get(self.id, None)
        if fh is None:
            doc = self.mongo[self.database][self.collection].find_one(self.filter)
            if doc is None:
                return -errno.ENOENT
  
            id = doc.pop("_id")
            if len(doc) == 0:
                json = ""
            else:
                json = dumps(doc, indent=self.mongofs.json_indent, ensure_ascii=self.mongofs.json_escaping).encode(self.mongofs.json_encoding, errors='replace') + "\n"
            
            fh = MongoSharedFileHandle(BytesIO(json), id)
            self.mongofs.open_file_cache[self.id] = fh
        fh.refs += 1
        return fh
    
    def release(self, flags, fh):
        fh.refs -= 1
        if fh.refs == 0:
            del self.mongofs.open_file_cache[self.id]
            return self.flush(fh)

    def flush(self, fh):
        if fh.dirty:
            fh.dirty=False
            try:
                json = fh.buffer.getvalue()
                if len(json.strip()):
                    doc = loads(json.decode(self.mongofs.json_encoding, errors='replace'))
                else:
                    doc = {}
                doc["_id"] = fh.id
                self.mongo[self.database][self.collection].update({"_id": fh.id}, doc)
                fh.flush_ret = 0
            except:
                fh.flush_ret = -errno.EINVAL              
        return fh.flush_ret

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
