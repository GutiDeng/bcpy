from Interface import Interface

import redis
import time
import types
import json
import os

class Implementation(Interface):
    
    def __init__(self, connstr, *args, **kwargs):
        self._connstr = connstr
        self._conn_params = {}
        for piece in connstr.split():
            k, v = piece.split('=')
            if k in ('port', 'db'):
                v = int(v)
            self._conn_params[k] = v
        
        self._ns = kwargs.get('namespace', '_KvDB')
        
        self._list_length = kwargs.get('list_length', 2048)
        
    
    # Keys
    def exists(self, k, pt=3):
        if pt == 3:
            return self._exists_dict(k) or self._exists_list(k)
        elif pt == 1:
            return self._exists_dict(k)
        elif pt == 2:
            return self._exists_list(k)
    
    def delete(self, k, pt=3):
        if pt == 3:
            return True if self._delete_dict(k) + self._delete_list(k) > 0 else False
        elif pt == 1:
            return True if self._delete_dict(k) == 1 else False
        elif pt == 2:
            return True if self._delete_list(k) == 1 else False
    
    def keys(self, pattern='*', pt=3):
        if pt == 3:
            return self._keys_ambiguous(pattern)
        elif pt == 1:
            return self._keys_dict(pattern)
        elif pt == 2:
            return self._keys_list(pattern)
    
    def flushdb(self, pt=3, namespace_only=True):
        if namespace_only is False:
            self._get_conn().flushdb()
        if pt == 3:
            self._flushdb_dict()
            self._flushdb_list()
        if pt == 1:
            self._flushdb_dict()
        if pt == 2:
            self._flushdb_list()
    
    # Dict
    def hset(self, k, f, v=None):
        if type(f) == types.DictType and v is None:
            for _f, _v in f.iteritems():
                self.hset(k, _f, _v)
        else:
            self._get_conn().hset(self._encode_key_dict(k), f, v)
    
    def hget(self, k, f=None, v=None):
        if f is None:
            return self._get_conn().hgetall(self._encode_key_dict(k)) or v
        else:
            return self._get_conn().hget(self._encode_key_dict(k), f) or v
    
    def hexists(self, k, f):
        return self._get_conn().hexists(self._encode_key_dict(k), f)
    
    def hdel(self, k, f):
        return self._get_conn().hdel(self._encode_key_dict(k), f)
    
    
    # List
    def rpush(self, k, v):
        self._get_conn().rpush(self._encode_key_list(k), v)
    
    def rpop(self, k, v=None):
        return self._get_conn().rpop(self._encode_key_list(k)) or v
    
    def lpush(self, k, v):
        self._get_conn().lpush(self._encode_key_list(k), v)
    
    def lpop(self, k, v=None):
        return self._get_conn().lpop(self._encode_key_list(k)) or v
    
    def lrange(self, k, start=None, stop=None):
        redis_start, redis_stop = 0, -1
        if start is not None:
            redis_start = start
        if stop is not None:
            redis_stop = stop - 1
        for v in self._get_conn().lrange(self._encode_key_list(k), redis_start, redis_stop):
            yield v
            
        # return Iterator
    
    ############################################
    # Implementations
    
    def _exists_dict(self, k):
        return self._get_conn().exists(self._encode_key_dict(k))
    def _exists_list(self, k):
        return self._get_conn().exists(self._encode_key_list(k))
    
    def _delete_dict(self, k):
        return self._get_conn().delete(self._encode_key_dict(k))
    def _delete_list(self, k):
        return self._get_conn().delete(self._encode_key_list(k))
        
    def _keys_dict(self, pattern='*'):
        for k_encoded in self._get_conn().keys(self._encode_key_dict(pattern)):
            yield self._decode_key_dict(k_encoded)
    def _keys_list(self, pattern='*'):
        for k_encoded in self._get_conn().keys(self._encode_key_list(pattern)):
            yield self._decode_key_list(k_encoded)
    def _keys_ambiguous(self, pattern='*'):
        s = set()
        for k_encoded in self._get_conn().keys(self._encode_key_ambiguous(pattern)):
            s.add(self._decode_key_ambiguous(k_encoded))
        for k in s.__iter__():
            yield k
    
    def _flushdb_dict(self):
        for k in self._keys_dict():
            self._delete_dict(k)
    def _flushdb_list(self):
        for k in self._keys_list():
            self._delete_list(k)
    
    def _get_conn(self):
        return redis.StrictRedis(**self._conn_params)
    
    def _encode_key_dict(self, k):
        return self._ns + '_0_' + k
    def _decode_key_dict(self, k_encoded):
        return k_encoded[len(self._ns) + 3 : ]
    def _encode_key_list(self, k):
        return self._ns + '_1_' + k
    def _decode_key_list(self, k_encoded):
        return k_encoded[len(self._ns) + 3 : ]
    def _encode_key_ambiguous(self, k):
        return self._ns + '_?_' + k
    def _decode_key_ambiguous(self, k_encoded):
        return k_encoded[len(self._ns) + 3 : ]
        
    
