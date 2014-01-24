
# You can edit this dict to shut off some of the implementations which are not 
# necessary in your project, so that you don't have to prepare the drivers.
IMPL_ACTIVITIES = {
    'Redis': True,
    'SQLite': True,
    'Postgres': True,
    'JSON': True,
    'MySQL': True,
}


IMPL_CLASSES = {}

if 'Redis' in IMPL_ACTIVITIES and IMPL_ACTIVITIES['Redis']:
    try:
        from ImplRedis import Implementation as ImplRedis
        IMPL_CLASSES['Redis'] = ImplRedis
    except:
        pass

if 'SQLite' in IMPL_ACTIVITIES and IMPL_ACTIVITIES['SQLite']:
    try:
        from ImplSQLite import Implementation as ImplSQLite
        IMPL_CLASSES['SQLite'] = ImplSQLite
    except:
        pass

if 'Postgres' in IMPL_ACTIVITIES and IMPL_ACTIVITIES['Postgres']:
    try:
        from ImplPostgres import Implementation as ImplPostgres
        IMPL_CLASSES['Postgres'] = ImplPostgres
    except:
        pass

if 'JSON' in IMPL_ACTIVITIES and IMPL_ACTIVITIES['JSON']:
    try:
        from ImplJSON import Implementation as ImplJSON
        IMPL_CLASSES['JSON'] = ImplJSON
    except:
        pass

if 'MySQL' in IMPL_ACTIVITIES and IMPL_ACTIVITIES['MySQL']:
    try:
        from ImplMySQL import Implementation as ImplMySQL
        IMPL_CLASSES['MySQL'] = ImplMySQL
    except Exception, e:
        print e
        pass

def KvDB(impl, *args, **kwargs):
    if impl in IMPL_CLASSES:
        return IMPL_CLASSES[impl](*args, **kwargs)
    else:
        raise Exception('Implementation for ' + impl + ' is not found.')

