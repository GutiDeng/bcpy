from Interface import Interface

import sqlite3
import time
import types
import json
import os

class Implementation(Interface):
    
    def __init__(self, connstr, *args, **kwargs):
        dbfile = connstr
        
        # automatically create parent directory
        pdir = os.path.dirname(dbfile)
        if not os.access(pdir, os.F_OK): os.makedirs(pdir, 0755)
        
        self._tablename = args[0] if len(args) > 0 else 'ISLETDEFAULTTABLE'
        
        self._conn = sqlite3.connect(dbfile, isolation_level=None)
        self._cursor = self._conn.cursor()
        
        self._list_length = kwargs.get('list_length', 2048)
    
    
    # keys 
    def exists(self, k, f=None, format='dict'):
        """Tells whether the given k (or field f under k) exists."""
        try:
            return self._exists(k, f, format)
        except sqlite3.OperationalError:
            self._init_dict()
            return self._exists(k, f, format)
    
    def delete(self, k, f=None, format='dict'):
        """Deletes the k (or field f under k)."""
        try:
            self._delete(k, f, format)
        except sqlite3.OperationalError:
            self._init_dict()
            self._delete(k, f, format)
    
    def keys(self, format='dict'):
        """Acts as an iterator over the existing keys."""
        tablename = self._get_tablename(format)
        try:
            for k in self._keys(format):
                yield k
        except sqlite3.OperationalError:
            self._init_dict()
            for k in self._keys(format):
                yield k
    
    def flushdb(self, format='dict'):
        """Remove all data."""
        try:
            tablename = self._get_tablename(format)
            self._cursor.execute('''DELETE FROM %s''' % (tablename,))
        except sqlite3.OperationalError:
            pass
    
    # Dict
    def set(self, k, f, v=None):
        """Sets value v to the field f under k."""
        if type(f) == types.DictType:
            for _f, _v in f.iteritems():
                self.set(k, _f, _v)
            return
        try:
            self._set(k, f, v)
        except sqlite3.OperationalError:
            self._init_dict()
            try:
                self._set(k, f, v)
            except sqlite3.OperationalError:
                raise
    
    def get(self, k, f=None, v=None):
        """Gets the value of field f under k, or v when absence."""
        try:
            return self._get(k, f, v)
        except sqlite3.OperationalError:
            self._init_dict()
            try:
                return self._get(k, f, v)
            except sqlite3.OperationalError:
                raise
    
    # List
    def append(self, k, v):
        """Appends v to the list under k."""
        try:
            return self._append(k, v)
        except sqlite3.OperationalError:
            self._init_list()
            try:
                return self._append(k, v)
            except sqlite3.OperationalError:
                raise
    
    def pop(self, k, v=None):
        """Removes and return the last value from list k, or v when the list is empty."""
        try:
            return self._pop(k, v)
        except sqlite3.OperationalError:
            self._init_list()
            try:
                return self._pop(k, v)
            except sqlite3.OperationalError:
                raise
    
    def prepend(self, k, v):
        """Prepends v to the list under k."""
        try:
            return self._prepend(k, v)
        except sqlite3.OperationalError:
            self._init_list()
            try:
                return self._prepend(k, v)
            except sqlite3.OperationalError:
                raise

    def shift(self, k, v=None):
        """Removes and return the first value from list k, or v when the list is empty."""
        try:
            return self._shift(k, v)
        except sqlite3.OperationalError:
            self._init_list()
            try:
                return self._shift(k, v)
            except sqlite3.OperationalError:
                raise
    
    
    ############################################
    # Implementations
    
    def _exists(self, k, f=None, format='dict'):
        """Tells whether the given k (or field f under k) exists."""
        tablename = self._get_tablename(format)
        if f is None:
            self._cursor.execute('''SELECT k FROM %s WHERE k=?''' % (tablename,), (k,))
            return True if self._cursor.fetchone() else False
        else:
            self._cursor.execute('''SELECT k FROM %s WHERE k=? AND f=?''' % (tablename,), (k, f))
            return True if self._cursor.fetchone() else False
    
    def _delete(self, k, f=None, format='dict'):
        """Deletes the k (or field f under k)."""
        tablename = self._get_tablename(format)
        if f is None:
            self._cursor.execute('''DELETE FROM %s WHERE k=?''' % (tablename,), (k,))
        else:
            self._cursor.execute('''DELETE FROM %s WHERE k=? AND f=?''' % (tablename,), (k, f))
    
    def _keys(self, format='dict'):
        """Acts as an iterator over the existing keys."""
        tablename = self._get_tablename(format)
        cursor = self._conn.cursor()
        cursor.execute('''SELECT DISTINCT k FROM %s''' % (tablename,))
        while True:
            row = cursor.fetchone()
            if not row: break
            yield row[0]    # row[k]
        # return Iterator
    
    def _set(self, k, f, v):
        self._cursor.execute('''INSERT OR REPLACE INTO %s VALUES (?, ?, ?)''' % (self._get_tablename(),), (k, f, v))
    
    def _get(self, k, f=None, v=None):
        """Gets the value of field f under k, or v when absence."""
        tablename = self._get_tablename()
        if f is not None:
            self._cursor.execute('''SELECT v FROM %s WHERE k=? AND f=?''' % (tablename,), (k, f))
            row = self._cursor.fetchone()
            return row[0] if row else v
        else:
            res = {}
            self._cursor.execute('''SELECT f, v FROM %s WHERE k=?''' % (tablename,), (k, ))
            while True:
                row = self._cursor.fetchone()
                if not row: break
                res[row[0]] = row[1]
            return res
    
    def _append(self, k, v):
        """Appends v to the list under k."""
        tablename = self._get_tablename('list')
        self._cursor.execute('''INSERT INTO %s VALUES (?, ?, ?)''' % (tablename,), (k, time.time(), v))
        self._cursor.execute('''DELETE FROM {tb} WHERE t < (SELECT t FROM {tb} ORDER BY t LIMIT 1 OFFSET {offset})\
                '''.format(tb=tablename, offset=self._list_length))
    def _pop(self, k, v):
        tablename = self._get_tablename('list')
        self._cursor.execute('''SELECT t, v FROM %s WHERE k=? ORDER BY t DESC LIMIT 1''' % (tablename,), (k,))
        row = self._cursor.fetchone()
        if row:
            t, v = row[0], row[1]
            self._cursor.execute('''DELETE FROM %s WHERE k=? AND t=?''' % (tablename,), (k, t))
            return v
        else:
            return v
    def _prepend(self, k, v):
        """Prepends v to the list under k."""
        tablename = self._get_tablename('list')
        self._cursor.execute('''INSERT INTO %s VALUES (?, (SELECT MIN(t)-1 FROM %s), ?)''' % (tablename, tablename), (k, v))
        self._cursor.execute('''DELETE FROM {tb} WHERE t < (SELECT t FROM {tb} ORDER BY t LIMIT 1 OFFSET {offset})\
                '''.format(tb=tablename, offset=self._list_length))
    def _shift(self, k, v):
        tablename = self._get_tablename('list')
        self._cursor.execute('''SELECT t, v FROM %s WHERE k=? ORDER BY t LIMIT 1''' % (tablename,), (k,))
        row = self._cursor.fetchone()
        if row:
            t, v = row[0], row[1]
            self._cursor.execute('''DELETE FROM %s WHERE k=? AND t=?''' % (tablename,), (k, t))
            return v
        else:
            return v
    
    def _init_dict(self, force=False):
        tablename = self._get_tablename()
        if force:
            self._cursor.execute('''DROP TABLE IF EXISTS %s''' % (tablename,))
        self._cursor.execute('''CREATE TABLE IF NOT EXISTS %s (k BLOB, f BLOB, v BLOB)''' % (tablename,))
        self._cursor.execute('''CREATE INDEX IF NOT EXISTS idx_k ON %s (k)''' % (tablename,))
        self._cursor.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_kf ON %s (k, f)''' % (tablename,))
    
    def _init_list(self, force=False):
        tablename = self._get_tablename('list')
        if force:
            self._cursor.execute('''DROP TABLE IF EXISTS %s''' % (tablename,))
        self._cursor.execute('''CREATE TABLE IF NOT EXISTS %s (k BLOB, t REAL, v BLOB)''' % (tablename,))
        self._cursor.execute('''CREATE INDEX IF NOT EXISTS idx_k ON %s (k)''' % (tablename,))
        self._cursor.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_kf ON %s (k, t)''' % (tablename,))
    
    def _get_tablename(self, format='dict'):
        if format == 'dict':    return '__DICT_' + self._tablename
        elif format == 'list':  return '__LIST_' + self._tablename
