from Interface import Interface

import MySQLdb
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
            
            # MySQLdb requires an integer for port
            if k == 'port':
                v = int(v)
            
            self._conn_params[k] = v
        
        self._ns = kwargs.get('namespace', '_KvDB')
        
        self._list_length = kwargs.get('list_length', 2048)
        
        self._autocommit = kwargs.get('autocommit', True)
        
    
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
            for k in self._keys(ptn, pt):
                yield k
        except Exception, e:
            self._init_namespace()
            for k in self._keys(ptn, pt):
                yield k
    
    def flushdb(self, pt=3):
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
                cursor.execute('''
SELECT 1 FROM {tb_dict} WHERE k=%s 
UNION ALL 
SELECT 1 FROM {tb_list} WHERE k=%s
'''.format(tb_dict=self._tb_dict(), tb_list=self._tb_list()), (k, k) )
            
            elif pt == 1:
                cursor.execute('''
SELECT 1 FROM {tb_dict} WHERE k=%s 
'''.format(tb_dict=self._tb_dict()), (k,) )
            elif pt == 2:
            
                cursor.execute('''
SELECT 1 FROM {tb_list} WHERE k=%s
'''.format(tb_list=self._tb_list()), (k,) )
            
            rc = cursor.rowcount
            cursor.close()
            return True if rc > 0 else False
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    def _delete(self, k, pt):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            rc = 0
            if pt == 3:
                cursor.execute('''
DELETE FROM {tb_dict} WHERE k=%s 
'''.format(tb_dict=self._tb_dict()), (k,) )
                rc += cursor.rowcount
                cursor.execute('''
DELETE FROM {tb_list} WHERE k=%s
'''.format(tb_list=self._tb_list()), (k,) )
                rc += cursor.rowcount
            
            elif pt == 1:
                cursor.execute('''
DELETE FROM {tb_dict} WHERE k=%s 
'''.format(tb_dict=self._tb_dict()), (k,) )
                rc += cursor.rowcount
            
            elif pt == 2:
                cursor.execute('''
DELETE FROM {tb_list} WHERE k=%s
'''.format(tb_list=self._tb_list()), (k,) )
                rc += cursor.rowcount
            
            cursor.close()
            return True if rc > 0 else False
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
                cursor.execute('''
SELECT k FROM {tb_dict} WHERE k LIKE %s
UNION DISTINCT
SELECT 1 FROM {tb_list} WHERE k LIKE %s
'''.format(tb_dict=self._tb_dict(), tb_list=self._tb_list()), 
        (self._trans_pattern(ptn), self._trans_pattern(ptn) ) )
            
            elif pt == 1:
                cursor.execute('''
SELECT k FROM {tb_dict} WHERE k LIKE %s
'''.format(tb_dict=self._tb_dict()), 
        (self._trans_pattern(ptn),) )
            
            elif pt == 2:
                cursor.execute('''
SELECT 1 FROM {tb_list} WHERE k LIKE %s
'''.format(tb_list=self._tb_list()), 
        (self._trans_pattern(ptn),) )
            
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
    
    def _flushdb(self, pt):
        try:
            conn = self._get_conn()
            self._execute_sql_script('dropIndex', conn)
            cursor = conn.cursor()
            if pt == 3:
                cursor.execute('DROP TABLE IF EXISTS {tb}'.format(tb=self._tb_dict()))
                cursor.callproc('bcpy_KvDB_dropIndex', (self._tb_dict(), self._idx_dict_k()))
                cursor.callproc('bcpy_KvDB_dropIndex', (self._tb_dict(), self._idx_dict_kf()))
                
                cursor.execute('DROP TABLE IF EXISTS {tb}'.format(tb=self._tb_list()))
                cursor.callproc('bcpy_KvDB_dropIndex', (self._tb_list(), self._idx_list_k()))
                cursor.callproc('bcpy_KvDB_dropIndex', (self._tb_list(), self._idx_list_kf()))
                
                cursor.execute('DROP TABLE IF EXISTS {tb}'.format(tb=self._tb_meta()))
                cursor.execute('DROP TABLE IF EXISTS {tb}'.format(tb=self._tb_log()))
                cursor.close()
                self._init_namespace()
            elif pt == 1:
                cursor.execute('DROP TABLE IF EXISTS {tb}'.format(tb=self._tb_dict()))
                cursor.callproc('bcpy_KvDB_dropIndex', (self._tb_dict(), self._idx_dict_k()))
                cursor.callproc('bcpy_KvDB_dropIndex', (self._tb_dict(), self._idx_dict_kf()))
                
                cursor.close()
            elif pt == 2:
                cursor.execute('DROP TABLE IF EXISTS {tb}'.format(tb=self._tb_list()))
                cursor.callproc('bcpy_KvDB_dropIndex', (self._tb_list(), self._idx_list_k()))
                cursor.callproc('bcpy_KvDB_dropIndex', (self._tb_list(), self._idx_list_kf()))
                
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
                    cursor.execute('''
INSERT INTO {tb_dict} VALUES (%s, %s, %s)
'''.format(tb_dict=self._tb_dict()), (k, _f, _v) )
                except:
                    cursor.execute('''
UPDATE {tb_dict} SET v=%s WHERE k=%s AND f=%s
'''.format(tb_dict=self._tb_dict()), (_v, k, _f) )
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
                cursor.execute('''
SELECT v FROM {tb_dict} WHERE k=%s AND f=%s
'''.format(tb_dict=self._tb_dict()), (k, f) )
                row = cursor.fetchone()
                cursor.close()
                return row[0] if row else v
            else:
                r = {}
                cursor.execute('''
SELECT f, v FROM {tb_dict} WHERE k=%s
'''.format(tb_dict=self._tb_dict()), (k,) )
                while True:
                    row = cursor.fetchone()
                    if not row: break
                    r[row[0]] = row[1]
                cursor.close()
                return r if r else v
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _hexists(self, k, f):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute('''
SELECT 1 FROM {tb_dict} WHERE k=%s AND f=%s
'''.format(tb_dict=self._tb_dict()), (k, f) )
            rc = cursor.rowcount
            cursor.close()
            return True if rc > 0 else False
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _hdel(self, k, f):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute('''
DELETE FROM {tb_dict} WHERE k=%s AND f=%s
'''.format(tb_dict=self._tb_dict()), (k, f) )
            rc = cursor.rowcount
            cursor.close()
            return True if rc > 0 else False
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    
    def _rpush(self, k, v):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.callproc('bcpy_KvDB_rpush', (self._ns, k, v, self._list_length))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return row[0]
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _rpop(self, k, v):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.callproc('bcpy_KvDB_rpop', (self._ns, k, self._list_length))
            if cursor.rowcount > 0:
                row = cursor.fetchone()
                if row:
                    v = row[0]
            cursor.close()
            return v
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _lpop(self, k, v):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.callproc('bcpy_KvDB_lpop', (self._ns, k, self._list_length))
            if cursor.rowcount > 0:
                row = cursor.fetchone()
                if row:
                    v = row[0]
            cursor.close()
            return v
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _lpush(self, k, v):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.callproc('bcpy_KvDB_lpush', (self._ns, k, v, self._list_length))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return row[0]
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    
    def _lrange(self, k, start=None, stop=None):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            if start is None:
                start = 0
            if stop is None:
                stop = self._list_length
            cursor.callproc('bcpy_KvDB_lrange', (self._ns, k, start, stop, self._list_length))
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
        try:
            tb_dict = self._tb_dict()
            tb_list = self._tb_list()
            tb_meta = self._tb_meta()
            tb_log = self._tb_log()
            conn = self._get_conn()
            self._execute_sql_script('dropIndex', conn)
            self._execute_sql_script('createIndex', conn)
            self._execute_sql_script('encNames', conn)
            self._execute_sql_script('rpush', conn)
            self._execute_sql_script('lpush', conn)
            self._execute_sql_script('lrange', conn)
            self._execute_sql_script('rpop', conn)
            self._execute_sql_script('lpop', conn)
            cursor = self._get_conn().cursor()
            
            cursor.execute("CREATE TABLE IF NOT EXISTS {tb} (k VARCHAR(255), f VARCHAR(255), v BLOB) \
DEFAULT CHARACTER SET utf8 COLLATE utf8_bin".format(tb=tb_dict))
            cursor.callproc('bcpy_KvDB_createIndex', (self._conn_params['db'], 
                    tb_dict, self._idx_dict_k(), 'k', False))
            cursor.callproc('bcpy_KvDB_createIndex', (self._conn_params['db'], 
                    tb_dict, self._idx_dict_kf(), 'k, f', True))
            
            cursor.execute("CREATE TABLE IF NOT EXISTS {tb} (k VARCHAR(255), f INTEGER, v BLOB) \
DEFAULT CHARACTER SET utf8 COLLATE utf8_bin".format(tb=tb_list))
            cursor.callproc('bcpy_KvDB_createIndex', (self._conn_params['db'], 
                    tb_list, self._idx_list_k(), 'k', False))
            cursor.callproc('bcpy_KvDB_createIndex', (self._conn_params['db'], 
                    tb_list, self._idx_list_kf(), 'k, f', True))
            
            cursor.execute("CREATE TABLE IF NOT EXISTS {tb} (k VARCHAR(255), listStart INTEGER, listStop INTEGER) \
DEFAULT CHARACTER SET utf8 COLLATE utf8_bin".format(tb=tb_meta))
            cursor.callproc('bcpy_KvDB_createIndex', (self._conn_params['db'], 
                    tb_meta, self._idx_meta_k(), 'k', True))
            
            cursor.execute("CREATE TABLE IF NOT EXISTS {tb} (t DATETIME, m VARCHAR(1024)) \
DEFAULT CHARACTER SET utf8 COLLATE utf8_bin".format(tb=tb_log))
            
            cursor.close()
        finally:
            try:
                self._put_conn(conn)
            except:
                pass
    def _execute_sql_script(self, script_file_name, conn):
        sqls = []
        p = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'sqlMySQL', script_file_name + '.sql')
        with open(p) as f:
            sql = ''
            for line in f.readlines():
                if line.startswith('/*COMMIT'):
                    sqls.append(sql)
                    sql = ''
                else:
                    sql += line
            if sql:
                sqls.append(sql)
        for sql in sqls:
            if not sql.strip():
                continue
            sql = sql.replace('__DBNAME__', self._conn_params['db'])
            sql = sql.replace('__TABLENAME__', self._ns)
            cursor = conn.cursor()
            cursor.execute(sql)
            cursor.close()
        
    def _get_conn(self):
        self._conn = MySQLdb.connect(**self._conn_params)
        self._conn.autocommit(self._autocommit)
        return self._conn
    def _put_conn(self, conn):
        self._conn.close()
    
    def _tb_dict(self):
        return self._ns + '_' + 'dict'
    def _tb_list(self):
        return self._ns + '_' + 'list'
    def _tb_meta(self):
        return self._ns + '_' + 'meta'
    def _tb_log(self):
        return self._ns + '_' + 'log'
    
    def _idx_dict_k(self):
        return 'idx_' + self._ns + '_dict_k'
    def _idx_dict_kf(self):
        return 'idx_' + self._ns + '_dict_kf'
    def _idx_list_k(self):
        return 'idx_' + self._ns + '_list_k'
    def _idx_list_kf(self):
        return 'idx_' + self._ns + '_list_kf'
    def _idx_meta_k(self):
        return 'idx_' + self._ns + '_meta_k'
    def _idx_meta_kf(self):
        return 'idx_' + self._ns + '_meta_kf'
    
    def _enc_dict(self, k):
        return '_1_' + k
    def _dec_dict(self, k_encoded):
        return k_encoded[3 : ]
    def _enc_list(self, k):
        return '_2_' + k
    def _dec_list(self, k_encoded):
        return k_encoded[3 : ]
    def _enc_meta(self, k):
        return '_meta_' + k
    def _dec_meta(self, k_encoded):
        return k_encoded[6 : ]
    def _enc_any_pt(self, k):
        return '_?_' + k
    def _dec_any_pt(self, k_encoded):
        return k_encoded[3 : ]
    
    def _trans_pattern(self, s):
        return s.replace('?', '_').replace('*', '%')
