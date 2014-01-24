from Interface import Interface

from psycopg2 import pool
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
            self._conn_params[k] = v
        
        self._ns = kwargs.get('namespace', '_KvDB')
        
        self._list_length = kwargs.get('list_length', 2048)
        
        self._autocommit = kwargs.get('autocommit', True)
        
        self._minconn = kwargs.get('minconn', 1)
        self._maxconn = kwargs.get('minconn', 10)
        self._pool = pool.ThreadedConnectionPool(self._minconn, self._maxconn, 
                **self._conn_params)
    
    
    # Keys 
    def exists(self, k, pt=3):
        try:
            return self._exists(k, pt)
        except Exception:
            self._init_namespace()
            return self._exists(k, pt)
    
    def delete(self, k, pt=3):
        try:
            return self._delete(k, pt)
        except Exception:
            self._init_namespace()
            return self._delete(k, pt)
    
    def keys(self, ptn='*', pt=3):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            if pt == 3:
                cursor.execute("SELECT DISTINCT k FROM {ns} WHERE k LIKE %s".format(ns=self._ns), (self._replace_wildcard(self._encode_key_ambiguous(ptn)),))
            elif pt == 1:
                cursor.execute("SELECT DISTINCT k FROM {ns} WHERE k LIKE %s".format(ns=self._ns), (self._replace_wildcard(self._encode_key_dict(ptn)),))
            elif pt == 3:
                cursor.execute("SELECT DISTINCT k FROM {ns} WHERE k LIKE %s".format(ns=self._ns), (self._replace_wildcard(self._encode_key_list(ptn)),))
            while True:
                row = cursor.fetchone()
                if not row: break
                yield self._decode_key_ambiguous(row[0])
            cursor.close()
        except Exception, e:
            self._init_namespace()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def flushdb(self, pt=3, namespace_only=True):
        if not namespace_only:
            pass    # implement later
        try:
            return self._flushdb(pt)
        except Exception:
            self._init_namespace()
            return self._flushdb(pt)
    
    
    # Dict
    def hset(self, k, f, v=None):
        try:
            return self._hset(k, f, v)
        except Exception:
            self._init_namespace()
            return self._hset(k, f, v)
    
    def hget(self, k, f=None, v=None):
        try:
            return self._hget(k, f, v)
        except Exception:
            self._init_namespace()
            return self._hget(k, f, v)
    
    def hexists(self, k, f):
        try:
            return self._hexists(k, f)
        except Exception:
            self._init_namespace()
            return self._hexists(k, f)
    
    def hdel(self, k, f):
        try:
            return self._hdel(k, f)
        except Exception:
            self._init_namespace()
            return self._hdel(k, f)
        
    
    # List
    def rpush(self, k, v):
        try:
            return self._rpush(k, v)
        except Exception:
            self._init_namespace()
            return self._rpush(k, v)
    
    def rpop(self, k, v=None):
        try:
            return self._rpop(k, v)
        except Exception:
            self._init_namespace()
            return self._rpop(k, v)
    
    def lpush(self, k, v):
        try:
            return self._lpush(k, v)
        except Exception:
            self._init_namespace()
            return self._lpush(k, v)
    
    def lpop(self, k, v=None):
        try:
            return self._lpop(k, v)
        except Exception:
            self._init_namespace()
            return self._lpop(k, v)
    
    def lrange(self, k, start=None, stop=None):
        try:
            return self._lrange(k, start, stop)
        except Exception:
            self._init_namespace()
            return self._lrange(k, start, stop)
    
    
    ### Implementations
    
    def _exists(self, k, pt):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            if pt == 3:
                cursor.execute("SELECT 1 FROM {ns} WHERE k=%s OR k=%s LIMIT 1".format(ns=self._ns), (self._encode_key_dict(k), self._encode_key_list(k)) )
            elif pt == 1:
                cursor.execute("SELECT 1 FROM {ns} WHERE k=%s LIMIT 1".format(ns=self._ns), (self._encode_key_dict(k),) )
            elif pt == 2:
                cursor.execute("SELECT 1 FROM {ns} WHERE k=%s LIMIT 1".format(ns=self._ns), (self._encode_key_list(k),) )
            return True if cursor.rowcount > 0 else False
            cursor.close
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    def _delete(self, k, pt):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            if pt == 3:
                cursor.execute("DELETE FROM {ns} WHERE k=%s OR k=%s".format(ns=self._ns), (self._encode_key_dict(k), self._encode_key_list(k)) )
            elif pt == 1:
                cursor.execute("DELETE FROM {ns} WHERE k=%s".format(ns=self._ns), (self._encode_key_dict(k),) )
            elif pt == 2:
                cursor.execute("DELETE FROM {ns} WHERE k=%s".format(ns=self._ns), (self._encode_key_list(k),) )
            return True if cursor.rowcount > 0 else False
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    def _keys(self, ptn, pt):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            if pt == 3:
                cursor.execute("SELECT DISTINCT k FROM {ns} WHERE k LIKE %s".format(ns=self._ns), (self._replace_wildcard(self._encode_key_ambiguous(ptn)),))
            elif pt == 1:
                cursor.execute("SELECT DISTINCT k FROM {ns} WHERE k LIKE %s".format(ns=self._ns), (self._replace_wildcard(self._encode_key_dict(ptn)),))
            elif pt == 3:
                cursor.execute("SELECT DISTINCT k FROM {ns} WHERE k LIKE %s".format(ns=self._ns), (self._replace_wildcard(self._encode_key_list(ptn)),))
            while True:
                row = cursor.fetchone()
                if not row: break
                yield self._decode_key_ambiguous(row[0])
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _flushdb(self, pt):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            if pt == 3:
                cursor.execute("TRUNCATE TABLE {ns}".format(ns=self._ns))
            elif pt == 1:
                cursor.execute("DELETE FROM {ns} WHERE k LIKE %s".format(ns=self._ns), (self._replace_wildcard(self._encode_key_dict('*'))) )
            elif pt == 2:
                cursor.execute("DELETE FROM {ns} WHERE k LIKE %s".format(ns=self._ns), (self._replace_wildcard(self._encode_key_list('*'))) )
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass

    def _hset(self, k, f, v):
        if type(f) is not types.DictType and v is not None:
            f = {f: v}
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            for _f, _v in f.iteritems():
                try:
                    cursor.execute("INSERT INTO {ns} VALUES (%s, %s, %s)".format(ns=self._ns), (self._encode_key_dict(k), _f, _v) )
                except:
                    cursor.execute("UPDATE {ns} SET v=%s WHERE k=%s AND f=%s".format(ns=self._ns), (_v, self._encode_key_dict(k), _f) )
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _hget(self, k, f, v):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            if f is not None:
                cursor.execute("SELECT v FROM {ns} WHERE k=%s AND f=%s".format(ns=self._ns), (self._encode_key_dict(k), f) )
                row = cursor.fetchone()
                return row[0] if row else v
            else:
                r = {}
                cursor.execute("SELECT f, v FROM {ns} WHERE k=%s".format(ns=self._ns), (self._encode_key_dict(k),) )
                while True:
                    row = cursor.fetchone()
                    if not row: break
                    r[row[0]] = row[1]
                return r if r else v
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _hexists(self, k, f):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM {ns} WHERE k=%s AND f=%s".format(ns=self._ns), (self._encode_key_dict(k), f) )
            return True if cursor.rowcount > 0 else False
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _hdel(self, k, f):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM {ns} WHERE k=%s AND f=%s".format(ns=self._ns), (self._encode_key_dict(k), f) )
            return True if cursor.rowcount > 0 else False
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _rpush(self, k, v):
        # drop the right most one if exceed length limit
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO {ns} VALUES (%s, EXTRACT('epoch' FROM now()), %s)".format(ns=self._ns), (self._encode_key_list(k), v) )
            cursor.execute("DELETE FROM {ns} WHERE k=%s AND f > (SELECT f FROM {ns} WHERE k=%s ORDER BY f LIMIT 1 OFFSET {offset})".format(
                ns=self._ns, offset=self._list_length), (self._encode_key_list(k), self._encode_key_list(k)) )
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _rpop(self, k, v):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT f, v FROM {ns} WHERE k=%s ORDER BY f DESC LIMIT 1".format(ns=self._ns), (self._encode_key_list(k),) )
            if cursor.rowcount > 0:
                row = cursor.fetchone()
                f, v = row[0], row[1]
                cursor.execute("DELETE FROM {ns} WHERE k=%s AND f=%s".format(ns=self._ns), (self._encode_key_list(k), f) )
            return v
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _lpush(self, k, v):
        # drop the left most one if exceed length limit
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO {ns} VALUES (%s, CAST((SELECT min(f) FROM {ns}) AS FLOAT) - 0.00001, %s)".format(ns=self._ns), 
                (self._encode_key_list(k), v) )
            cursor.execute("DELETE FROM {ns} WHERE k=%s AND f < (SELECT f FROM {ns} WHERE k=%s ORDER BY f DESC LIMIT 1 OFFSET {offset})".format(
                ns=self._ns, offset=self._list_length), (self._encode_key_list(k), self._encode_key_list(k)) )
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _lpop(self, k, v):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT f, v FROM {ns} WHERE k=%s ORDER BY f LIMIT 1".format(ns=self._ns), (self._encode_key_list(k),) )
            if cursor.rowcount > 0:
                row = cursor.fetchone()
                f, v = row[0], row[1]
                cursor.execute("DELETE FROM {ns} WHERE k=%s AND f=%s".format(ns=self._ns), (self._encode_key_list(k), f) )
            return v
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _lrange(self, k, start=None, stop=None):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            #if start is None and stop is None:
            cursor.execute("SELECT v FROM {ns} WHERE k=%s ORDER BY f".format(ns=self._ns), (self._encode_key_list(k),) )
            while True:
                row = cursor.fetchone()
                if not row: break
                yield row[0]
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _init_namespace(self):
        print 'init'
        try:
            conn = self._get_conn()
            cursor = self._get_conn().cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS {ns} (k VARCHAR, f VARCHAR, v VARCHAR)".format(ns=self._ns))
            try:
                cursor.execute("CREATE INDEX _idx_{ns}_k ON {ns} (k)".format(ns=self._ns))
                cursor.execute("CREATE UNIQUE INDEX _idx_{ns}_kf ON {ns} (k, f)".format(ns=self._ns))
            except:
                pass
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
        
    def _get_conn(self):
        conn = self._pool.getconn()
        conn.autocommit = self._autocommit
        return conn
    def _put_conn(self, conn):
        self._pool.putconn(conn)
    
    def _encode_key_dict(self, k):
        return '_0_' + k
    def _decode_key_dict(self, k_encoded):
        return k_encoded[3 : ]
    def _encode_key_list(self, k):
        return '_1_' + k
    def _decode_key_list(self, k_encoded):
        return k_encoded[3 : ]
    def _encode_key_ambiguous(self, k):
        return '_?_' + k
    def _decode_key_ambiguous(self, k_encoded):
        return k_encoded[3 : ]
    
    def _replace_wildcard(self, s):
        return s.replace('?', '_').replace('*', '%')
