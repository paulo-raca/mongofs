#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import stat
import errno
import fuse
import os
import urllib
from io import BytesIO
from routefs import RouteFS
from routes import Mapper
from pymongo import MongoClient
from bson import SON, Code
from expiringdict import ExpiringDict

from mongofs.notify import notify
from mongofs.escaping import escape, unescape
from mongofs.json import dumps, loads

def path2url(path):
    return "file://" + urllib.pathname2url(os.path.abspath(path))

class MongoFS(RouteFS):
    def __init__(self, *args, **kwargs):
        RouteFS.__init__(self, *args, **kwargs)
        self.fuse_args.add("allow_other", True)
        self.host = "localhost"
        self.fetch_file_length = False
        self.hide_id = False
        self.json_escaping = False
        self.json_encoding = "utf8"
        self.json_indent = 4
        self.open_file_cache = {}
        # There is a massive performance gain if we cache a directory's contents.
        self.directory_cache = ExpiringDict(max_len=100, max_age_seconds=10)

        
        self.parser.add_option(mountopt="host",
            metavar="HOSTNAME", 
            default=self.host,
            help="Adress of mongo server. Either host, host:port or a mongo URI [default: %default]")
        self.parser.add_option(mountopt="hide_id",
            action="store_true", dest="hide_id",
            default=self.hide_id,
            help="Hides '_id' field in document contents [default: %default]")
        self.parser.add_option(mountopt="json_escaping",
            action="store_true", dest="json_escaping",
            default=self.json_escaping,
            help="Escapes all non-ascii characters on the JSON strings [default: %default]")
        self.parser.add_option(mountopt="fetch_file_length",
            action="store_true", dest="fetch_file_length",
            default=self.fetch_file_length,
            help="Escapes all non-ascii characters on the JSON strings [default: %default]")
        self.parser.add_option(mountopt="json_encoding",
            metavar="ENCODING", 
            default=self.json_encoding,
            help="Character encoding of JSON document [default: %default]")
        self.parser.add_option(mountopt="json_indent",
            metavar="INDENTATION", 
            default=self.json_indent,
            type=int,
            help="Size of indentation on pretty-printed JSON documents (Use -1 for compact JSON) [default: %default]")
        
    def fsinit(self):
        self.mongo = MongoClient(self.host, document_class=SON, connectTimeoutMS=2000, socketTimeoutMS=2000, socketKeepAlive=True)
        notify("MongoFS", "Mounted <i>%s</i> on <a href=\"%s\"><i>%s</i></a>" % (self.host, path2url(self.fuse_args.mountpoint), os.path.abspath(self.fuse_args.mountpoint)), icon="dialog-information")
        
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
        return m
      
    def getRoot(self, **kwargs):
        try:
            return MongoRoot(self)
        except:
            return None
        
    def getDatabase(self, database, **kwargs):
        try:
            return MongoDatabase(self, unescape(database))
        except:
            return None
        
    def getCollection(self, database, collection, **kwargs):
        try:
            return MongoCollection(self, unescape(database), unescape(collection))
        except:
            return None
        
    def parse_path(self, filter_path):
        try: 
            filter_path = tuple(map(unescape, filter_path.split("/")))
            filter = SON(zip(filter_path[0::2], map(loads, list(filter_path[1::2]))))
            current_field = filter_path[-1] if (len(filter_path) % 2) == 1 else None
            return (filter, current_field)
        except:
            return None
          
    def getFilter(self, database, collection, filter_path, **kwargs):
        try:
            filter, current_field = self.parse_path(filter_path)
            return MongoFilter(self, unescape(database), unescape(collection), filter, current_field)
        except:
            return None
          
    def getDocument(self, database, collection, filter_path, **kwargs):
        try:
            filter, current_field = self.parse_path(filter_path)
            if current_field is None:
                return MongoDocument(self, unescape(database), unescape(collection), filter)
        except:
            return None

class BaseMongoNode():
    def __init__(self, mongofs, id):
        self.mongofs = mongofs
        self.mongo = mongofs.mongo
        self.id = id

    def list_files_impl(self):
        return None
      
    def list_files(self, cached=True):
        try:
            if not cached:
                raise Exception("No caching")
            elements = self.mongofs.directory_cache[self.id]
        except:
            try:
                elements = self.list_files_impl()
            except:
                elements = None
            self.mongofs.directory_cache[self.id] = elements
        return elements
        
    def readdir(self, offset):
        elements = self.list_files()
            
        if elements is None:
            return

        yield fuse.Direntry('.')
        yield fuse.Direntry('..')
        for x in elements:
            yield fuse.Direntry(escape(x).encode("utf8"))


class MongoRoot(BaseMongoNode):
    def __init__(self, mongofs):
        BaseMongoNode.__init__(self, mongofs, ())

    def getattr(self):
        return fuse.Stat(
            st_mode=stat.S_IFDIR | 0777,
            st_nlink=2)

    def list_files_impl(self):
        return self.mongo.database_names()


class MongoDatabase(BaseMongoNode):
    def __init__(self, mongofs, database):
        self.database = database
        BaseMongoNode.__init__(self, mongofs, (database,))

    def getattr(self):
        if self.database not in MongoRoot(self.mongofs).list_files():
            return -errno.ENOENT

        return fuse.Stat(
            st_mode=stat.S_IFDIR | 0777,
            st_nlink=2)

    def mkdir(self, mode):
        if self.database in MongoRoot(self.mongofs).list_files():
            return -errno.EEXIST
          
        # There is no explicit "createDatabase" method. We must create something inside it.
        self.mongo[self.database].create_collection("_")
        self.mongo[self.database].drop_collection("_")
        self.mongofs.directory_cache.clear()
        return 0

    def rmdir(self):
        if self.database not in MongoRoot(self.mongofs).list_files():
            return -errno.ENOENT
          
        self.mongo.drop_database(self.database)
        self.mongofs.directory_cache.clear()
        return 0

    def rename(self, target):
        target = self.mongofs._get_file(target)
        
        if not isinstance(target, MongoDatabase):
            return -errno.EACCES
        if self.database not in MongoRoot(self.mongofs).list_files():
            return -errno.ENOENT
        if target.database in MongoRoot(self.mongofs).list_files():
            return -errno.EEXIST

        # There is no explicit "renameDatabase" method. We must clone a new DB and drop the old one.              
        self.mongo.admin.command('copydb', fromdb=self.database, todb=target.database)
        self.mongo.drop_database(self.database)
        self.mongofs.directory_cache.clear()
        return 0

    def list_files_impl(self):
        return self.mongo[self.database].collection_names(include_system_collections=False)
            
class MongoCollection(BaseMongoNode):
    def __init__(self, mongofs, database, collection):
        self.database = database
        self.collection = collection
        BaseMongoNode.__init__(self, mongofs, (database, collection))

    def getattr(self):
        if self.collection not in MongoDatabase(self.mongofs, self.database).list_files():
            return -errno.ENOENT
        
        return fuse.Stat(
            st_mode=stat.S_IFDIR | 0777,
            st_nlink=2)

    def mkdir(self, mode):
        if self.collection in MongoDatabase(self.mongofs, self.database).list_files():
            return -errno.EEXIST
          
        self.mongo[self.database].create_collection(self.collection)
        self.mongofs.directory_cache.clear()
        return 0

    def rmdir(self):
        if self.collection not in MongoDatabase(self.mongofs, self.database).list_files():
            return -errno.ENOENT
          
        self.mongo[self.database].drop_collection(self.collection)
        self.mongofs.directory_cache.clear()
        return 0

    def rename(self, target):
        target = self.mongofs._get_file(target)
        
        if not isinstance(target, MongoCollection):
            return -errno.EACCES
        if self.collection not in MongoDatabase(self.mongofs, self.database).list_files():
            return -errno.ENOENT
        if target.collection in MongoDatabase(self.mongofs, self.database).list_files():
            return -errno.EEXIST

        self.mongo.admin.command(
            "renameCollection", "%s.%s" % (self.database, self.collection),
            to="%s.%s" % (target.database, target.collection)
        )
        self.mongofs.directory_cache.clear()
        return 0

    def list_files_impl(self):
        return MongoFilter(self.mongofs, self.database, self.collection, {}, None).list_files()


class MongoFilter(BaseMongoNode):
    def __init__(self, mongofs, database, collection, filter, current_field):
        BaseMongoNode.__init__(self, mongofs, (database, collection) + tuple([item for pair in filter.iteritems() for item in pair]) + (() if current_field is None else (current_field,)))
        self.database = database
        self.collection = collection
        self.filter = filter
        self.current_field = current_field

    def getattr(self):
        return fuse.Stat(
            st_mode=stat.S_IFDIR | 0777,
            st_nlink=2)

    def mkdir(self, mode):
        #All filters "exist" already, even if they are not listed
        return -errno.EEXIST

    def rmdir(self):
        if self.current_field is None:
            self.mongo[self.database][self.collection].delete_many(self.filter)
        else:
            self.mongo[self.database][self.collection].update_many(self.filter, {"$unset": {self.current_field:1}})
        self.mongofs.directory_cache.clear()
        return 0
      
    def rename(self, target):
        #TODO
        return -errno.EACCES

    def list_files_impl(self):
        if self.current_field is None:
            # TODO: Needs optimisation
            attrs = set()
            for doc in self.mongo[self.database][self.collection].find(self.filter, limit=50):
                for key, value in doc.iteritems():
                    if key not in self.filter and not isinstance(value, dict) and not isinstance(value, list):
                        attrs.add(key)
            attrs -= set(self.filter.keys())
            return list(attrs)
        else:
            # Count the distinct values of the field
            q = dict(self.filter)
            q[self.current_field] = { "$exists": True }
            values = self.mongo[self.database][self.collection].inline_map_reduce(
                map    = Code("""function() { emit(this[fieldName], 1); }"""),
                reduce = Code("""function(key, values) { return Array.sum(values); }"""),
                query  = q,
                scope  = {"fieldName": self.current_field})
            return [
                dumps(entry["_id"], ensure_ascii=False) + (".json" if entry["value"] == 1 else "")
                for entry in values
            ]
                
                
# truncate() runs without a file handler.
# To make it work, we must share state outside of the "FileHandle" abstraction
class MongoSharedFileHandle:
    def __init__(self, buffer, id):
        self.buffer = buffer
        self.id = id
        self.dirty = False
        self.refs = 0
        self.flush_ret = 0


class MongoDocument(BaseMongoNode):
    def __init__(self, mongofs, database, collection, filter):
        BaseMongoNode.__init__(self, mongofs, (database, collection) + tuple([item for pair in filter.iteritems() for item in pair]))        
        self.database = database
        self.collection = collection
        self.filter = filter

    def getattr(self):
        # If the file is already open, return the size of the buffer
        if self.mongofs.fetch_file_length:
            fh = self.open(0) 
        else:
            fh = self.mongofs.open_file_cache.get(self.id, None)
        if isinstance(fh, MongoSharedFileHandle):
            if self.mongofs.fetch_file_length:
                self.release(0, fh)
            return fuse.Stat(
                st_mode=stat.S_IFREG | 0666,
                st_nlink=1,
                st_size=len(fh.buffer.getvalue()))
      
        # It is faster to check the cached response of readdir() instead of looking up Mongo
        parent_field = self.id[-2]
        parent_filter = dict(self.filter)
        del parent_filter[parent_field]
        for valid_file in MongoFilter(self.mongofs, self.database, self.collection, parent_filter, parent_field).list_files():
            if valid_file.endswith(".json"):
                valid_file = valid_file[:-5]
                if self.id[-1] == loads(valid_file):
                    return fuse.Stat(
                        st_mode=stat.S_IFREG | 0666,
                        st_nlink=1,
                        st_size=1)
              
        return -errno.ENOENT  
      
    def create(self, flags, mode):
        fh = MongoSharedFileHandle(BytesIO(), None)
        self.mongofs.open_file_cache[self.id] = fh
        fh.refs += 1
        return fh
    
    def unlink(self):
        self.mongo[self.database][self.collection].delete_one(self.filter)
        self.mongofs.directory_cache.clear()
        return 0

    def unlink(self):
        self.mongo[self.database][self.collection].delete_one(self.filter)
        self.mongofs.directory_cache.clear()
        return 0

    def rename(self, target):
        #TODO
        return -errno.EACCES

    def truncate(self, len):
        fh = self.open(0)
        fh.dirty = True
        fh.buffer.truncate(len)
        self.release(0, fh)
        return 0        
    
    def open(self, flags):
        fh = self.mongofs.open_file_cache.get(self.id, None)
        if fh is None:
            doc = self.mongo[self.database][self.collection].find_one(self.filter)
            if doc is None:
                return -errno.ENOENT
  
            id = doc["_id"]
            if self.mongofs.hide_id:
                del doc["_id"]

            if len(doc) == 0:
                json = ""
            else:
                json = dumps(doc, indent=self.mongofs.json_indent if self.mongofs.json_indent >= 0 else None, ensure_ascii=self.mongofs.json_escaping).encode(self.mongofs.json_encoding, errors='replace') + "\n"
            
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
        if not fh.dirty:
            return fh.flush_ret

        try:
            json = fh.buffer.getvalue()
            if len(json.strip()):
                doc = loads(json.decode(self.mongofs.json_encoding, errors='replace'))
            else:
                doc = {}
        except ValueError,e:
            notify("Invalid MongoFS document", "\n".join(str(e).split(":")))
            fh.dirty = False
            fh.flush_ret = -errno.EIO            
            return fh.flush_ret
          
        try:
            doc.update(self.filter)

            if fh.id is None:
                fh.id = self.mongo[self.database][self.collection].insert_one(doc).inserted_id
            else:
                self.mongo[self.database][self.collection].update({"_id": fh.id}, doc)
            self.mongofs.directory_cache.clear()
            fh.dirty = False
            fh.flush_ret = 0
            return fh.flush_ret
        except:
            fh.flush_ret = -errno.EIO
            return fh.flush_ret

        

    def read(self, length, offset, fh):
        fh.buffer.seek(offset)
        return fh.buffer.read(length)

    def write(self, buffer, offset, fh):
        fh.dirty = True
        fh.buffer.seek(offset)
        fh.buffer.write(buffer)
        return len(buffer)
