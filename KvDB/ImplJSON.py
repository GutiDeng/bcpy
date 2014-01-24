from Interface import Interface

import types
import json
import re
import os

class Implementation(Interface):
    
    def __init__(self, connstr, *args, **kwargs):
        self._connstr = connstr
        self._conn_params = {}
        for piece in connstr.split():
            k, v = piece.split('=')
            self._conn_params[k] = v
        
        self._ns = kwargs.get('namespace', '_KvDB')
        
        self._list_length = kwargs.get('list_length', 2048)
    
        self._j = None
    
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
            self._j = {}
            self._write()
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
            self._read()
            k = self._encode_key_dict(k)
            if k not in self._j:
                self._j[k] = {}
            self._j[k][f] = v
            self._write()
    
    def hget(self, k, f=None, v=None):
        if f is None:
            return self._read().get(self._encode_key_dict(k), v)
        else:
            return self._read().get(self._encode_key_dict(k), {}).get(f, v)
    
    def hexists(self, k, f):
        return f in self._read().get(self._encode_key_dict(k), {})
    
    def hdel(self, k, f):
        self._read()
        k = self._encode_key_dict(k)
        if k in self._j and f in self._j[k]:
            del self._j[k][f]
            self._write()
            return True
        return False
    
    
    # List
    def rpush(self, k, v):
        self._read()
        k = self._encode_key_list(k)
        if k not in self._j:
            self._j[k] = []
        self._j[k].append(v)
        self._write()
    
    def rpop(self, k, v=None):
        self._read()
        k = self._encode_key_list(k)
        if k in self._j and len(self._j[k]) > 0:
            return self._j[k].pop()
        return v
    
    def lpush(self, k, v):
        self._read()
        k = self._encode_key_list(k)
        if k not in self._j:
            self._j[k] = []
        self._j[k].insert(0, v)
        self._write()
    
    def lpop(self, k, v=None):
        self._read()
        k = self._encode_key_list(k)
        if k in self._j and len(self._j[k]) > 0:
            return self._j[k].pop(0)
        return v
    
    def lrange(self, k, start=None, stop=None):
        self._read()
        k = self._encode_key_list(k)
        if k in self._j:
            for v in self._j[k][start:stop]:
                yield v
        # return Iterator
    
    ############################################
    # Implementations
    
    def _exists_dict(self, k):
        return self._encode_key_dict(k) in self._read()
    def _exists_list(self, k):
        return self._encode_key_list(k) in self._read()
    
    def _delete_dict(self, k):
        self._read()
        k = self._encode_key_dict(k)
        if k in self._j:
            del self._j[k]
            self._write()
            return 1
        return 0
    def _delete_list(self, k):
        self._read()
        k = self._encode_key_list(k)
        if k in self._j:
            del self._j[k]
            self._write()
            return 1
        return 0
        
    def _keys_dict(self, pattern='*'):
        pattern = self._encode_pattern(self._encode_key_dict(pattern))
        e = re.compile(pattern)
        for k_encoded in self._read().keys():
            if e.match(k_encoded):
                yield self._decode_key_dict(k_encoded)
    def _keys_list(self, pattern='*'):
        pattern = self._encode_pattern(self._encode_key_list(pattern))
        e = re.compile(pattern)
        for k_encoded in self._read().keys():
            if e.match(k_encoded):
                yield self._decode_key_list(k_encoded)
    def _keys_ambiguous(self, pattern='*'):
        s = set()
        pattern = self._encode_pattern(self._encode_key_ambiguous(pattern))
        e = re.compile(pattern)
        for k_encoded in self._read().keys():
            if e.match(k_encoded):
                s.add(self._decode_key_ambiguous(k_encoded))
        for k in s.__iter__():
            yield k
    
    def _flushdb_dict(self):
        for k in self._keys_dict():
            self._delete_dict(k)
    def _flushdb_list(self):
        for k in self._keys_list():
            self._delete_list(k)
    
    def _read(self):
        if self._j is None:
            if not os.access(self._conn_params['path'], os.F_OK):
                self._j = {}
            else:
                self._j = json.load(open(self._conn_params['path']))
            
        return self._j
    def _write(self):
        json.dump(self._j, open(self._conn_params['path'], 'w'), indent=2, sort_keys=True)
    
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
        
    def _encode_pattern(self, pattern):
        return '^' + pattern.replace('?', '\\w').replace('*', '\\w*') + '$'
    
