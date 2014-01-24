#!/usr/bin/env python
# -*- coding: utf-8 -*-

from KvDB import KvDB

import os, sys, time


def unit(idx, desc, expect_value, test_value):
    ok = 'OK' if expect_value == test_value else 'FAILED!'
    print ok, idx, desc, expect_value, test_value, '...'
    
def test(db, details=False):
    
    unit( 's000', 'flushdb()', None, db.flushdb() )

    unit( 's001', 'exists() on nonexistent key', False, db.exists('k') )

    unit( 's002', 'delete() on nonexistent key', False, db.delete('k') )

    unit( 's003', 'keys() on empty namespace', [], [x for x in db.keys()] )


    unit( 's100', 'hexists() on nonexistent key', False, db.hexists('k', 'f') )

    unit( 's101', 'hdel() on nonexistent key', False, db.hexists('k', 'f') )

    unit( 's102', 'hset() on field', None, db.hset('k', 'f', 'vf') )

    unit( 's103', 'hget() on existent field', 'vf', db.hget('k', 'f') )

    unit( 's104', 'hget() on existent field with ineffective default value', 'vf', db.hget('k', 'f', 'vg') )

    unit( 's105', 'hget() for nonexistent field', None, db.hget('k', 'g') )

    unit( 's106', 'hget() for nonexistent field with effective default value', 'vg', db.hget('k', 'g', 'vg') )

    unit( 's107', 'hset() for fields at once', None, db.hset('k', {'f':'nvf', 'g':'', 'h':None}) )

    unit( 's108', 'hget() for fields', {'f':'nvf', 'g':'', 'h':None}, db.hget('k') )

    unit( 's109', 'hget() for fields with ineffective default values', {'f':'nvf', 'g':'', 'h':None}, db.hget('k', None, {'i': 'vi'}) )

    unit( 's110', 'hget() for fields with effective default values', {'i': 'vi'}, db.hget('m', None, {'i': 'vi'}) )

    unit( 's111', 'hexists() for existent field', True, db.hexists('k', 'f') )

    unit( 's112', 'hdel() for exsitent field', True, db.hdel('k', 'f') )

    unit( 's113', 'hexists() for not existing field', False, db.hexists('k', 'f') )

    unit( 's114', 'hdel() for existent field', False, db.hdel('k', 'f') )


    unit( 's200', 'rpush() for nonexistent key', 0, db.rpush('k', 'c') )

    unit( 's201', 'lpush() for existent key', 0, db.lpush('k', 'b') )

    unit( 's203', 'rpush() for existent key', 0, db.rpush('k', 'd') )

    unit( 's204', 'lpush() for existent key', 0, db.lpush('k', 'a') )

    unit( 's205', 'lrange() for existent key', ['a', 'b', 'c', 'd'], [x for x in db.lrange('k')] )

    unit( 's206', 'lrange() with start but stop', ['c', 'd'], [x for x in db.lrange('k', 2)] )

    unit( 's207', 'lrange() with stop but stop', ['a', 'b', 'c'], [x for x in db.lrange('k', stop=2)] )

    unit( 's208', 'lrange() with negtive start and stop', ['b', 'c', 'd'], [x for x in db.lrange('k', start=-3, stop=-1)] )

    unit( 's209', 'rpush() for full list', -1, db.rpush('k', 'e') )
    
    unit( 's210', 'lpush() for full list', -1, db.lpush('k', 'e') )
    
    unit( 's211', 'rpop() for exsitent key', 'c', db.rpop('k') )

    unit( 's212', 'lpop() for exsitent key', 'e', db.lpop('k') )

    unit( 's213', 'lrange() for the entire list', ['a', 'b'], [x for x in db.lrange('k')] )

    unit( 's214', 'rpop() for exsitent key', 'b', db.rpop('k') )

    unit( 's215', 'lpop() for exsitent key', 'a', db.lpop('k') )

    unit( 's216', 'rpop() for nonexsitent key', None, db.rpop('k') )

    unit( 's217', 'lpop() for nonexsitent key', None, db.lpop('k') )

    unit( 's050', 'exists() for existent dict part', True, db.exists('k', pt=1) )

    unit( 's051', 'delete() for existent dict part', True, db.delete('k', pt=1) )

    unit( 's052', 'exists() for nonexistent dict part', False, db.exists('k', pt=1) )

    unit( 's053', 'delete() for nonexistent dict part', False, db.delete('k', pt=1) )

    unit( 's060', 'exists() for existent list part', True, db.exists('k', pt=2) )

    unit( 's061', 'delete() for existent list part', True, db.delete('k', pt=2) )

    unit( 's062', 'exists() for nonexistent list part', False, db.exists('k', pt=2) )

    unit( 's063', 'delete() for not existing list part', False, db.delete('k', pt=2) )

    unit( 's090', 'hset() unicode', None, db.hset('k', 'u', '中文') )

    unit( 's091', 'hget() unicode', '中文', db.hget('k', 'u') )

    sys.exit()
    unit( 's098', 'flushdb() again', None, db.flushdb() )


    unit( 's099', 'keys() for empty namespace', [], [x for x in db.keys()] )
    
    
if __name__ == '__main__':
    imp = sys.argv[1]
    if imp == 'SQLite':
        db = KvDB('SQLite', '/tmp/KvDB_test.sqlite', namespace='_KvDBtest', list_length=4)
        test(db)
    if imp == 'Postgres':
        db = KvDB('Postgres', 'port=5432', namespace='_KvDBtest', list_length=4)
        test(db)
    if imp == 'Redis':
        db = KvDB('Redis', 'port=6379 db=0', namespace='_KvDBtest')
        test(db)
    if imp == 'JSON':
        db = KvDB('JSON', 'path=/tmp/KvDB_test.json', namespace='_KvDBtest')
        test(db)
    if imp == 'MySQL':
        db = KvDB('MySQL', 'port=3306 user=test passwd=test db=test', namespace='_KvDBtest', list_length=4)
        test(db)

