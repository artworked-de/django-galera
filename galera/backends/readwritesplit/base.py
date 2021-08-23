import copy
import hashlib
import logging
import multiprocessing
import pprint
import random
import time

from django.db import DEFAULT_DB_ALIAS, DatabaseError
from django.db.backends.mysql import base

LOGGER = logging.getLogger(__name__)


class NodeState:
    RETRY_INTERVAL = 30

    def __init__(self, store):
        self.nodes = store

    def add_nodes(self, nodes):
        for node in nodes:
            if node not in self.nodes:
                self.nodes[node] = None

    def mark_online(self, node):
        if node in self.nodes:
            self.nodes[node] = None

    def mark_offline(self, node):
        if node in self.nodes:
            self.nodes[node] = time.time()

    def get_online_nodes(self):
        return (x for x, y in self.nodes.items() if y is None or time.time() > y + self.RETRY_INTERVAL)


NODE_STATE = NodeState(multiprocessing.Manager().dict())


class CursorWrapper:
    def __init__(self, backend):
        self._backend = backend
        self._cursor = None
        self._primary = False
        self._in_handle_exc = False

    @property
    def cursor(self):
        if self._cursor is None:
            self._backend.failover_history.append([])
            if self._primary:
                self._cursor = self._backend.create_primary_cursor()
            else:
                self._cursor = self._backend.create_secondary_cursor()
        return self._cursor

    def prepare(self, query):
        if query is None:
            rw_query = True
        else:
            query = query.strip()
            rw_query = not query.startswith('SELECT ')
            rw_query = rw_query or query.endswith(' FOR UPDATE') or ' INTO ' in query
        if rw_query:
            self._backend.secondary_synced = False
            if not self._backend.autocommit and not self._backend.in_write_transaction:
                self._backend.in_write_transaction = True
        primary_required = rw_query or self._backend.in_write_transaction
        if primary_required and not self._primary:
            self._primary = True
            self.close()
        elif not self._backend.secondary_synced:
            self._backend.sync_wait_secondary()
        LOGGER.debug('%s: %s' % ('primary' if self._primary else 'secondary', query))
        return self.cursor

    def callproc(self, procname, args=None):
        self.prepare(None)
        return self._failover_cursor('callproc')(procname, args=args)

    def execute(self, query, args=None):
        self.prepare(query)
        return self._failover_cursor('execute')(query, args=args)

    def executemany(self, query, args=None):
        self.prepare(query)
        return self._failover_cursor('executemany')(query, args=args)

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

    def _failover_cursor(self, item):
        if not self._backend.failover_active:
            return getattr(self.cursor, item)

        try:
            obj = getattr(self.cursor, item)
        except DatabaseError as e:
            self._handle_exc(e)
            obj = getattr(self._cursor, item)

        def wrap(func):
            def decor(*args, **kwargs):
                try:
                    ret = func(*args, **kwargs)
                except DatabaseError as exc:
                    self._handle_exc(exc)
                    ret = getattr(self._cursor, func.__name__)(*args, **kwargs)
                if self._backend.failover_history:
                    self._backend.failover_history[-1].append((
                        func.__name__,
                        args,
                        kwargs,
                        hashlib.sha1(pprint.pformat(ret).encode()).hexdigest()
                    ))
                    self._backend.failover_history_size += 1
                return ret
            return decor
        if hasattr(obj, '__call__'):
            obj = wrap(obj)
        else:
            if self._backend.failover_history:
                self._backend.failover_history[-1].append((
                    item,
                    None,
                    None,
                    hashlib.sha1(pprint.pformat(obj).encode()).hexdigest()
                ))
                self._backend.failover_history_size += 1

        return obj

    def __enter__(self):
        # at this moment it is unknown if this cursor will be used for write queries, force creating a primary cursor
        self._primary = True
        return self.cursor.__enter__()

    def __exit__(self, *exc_info):
        return self.cursor.__exit__(*exc_info)

    def __getattr__(self, item):
        return self._failover_cursor(item)

    def __next__(self):
        return self.cursor.__next__()

    def __iter__(self):
        return self.cursor.__iter__()

    def _handle_exc(self, exc):
        if self._in_handle_exc:
            raise exc
        self._in_handle_exc = True
        if self._backend.failover_active:
            error_code = exc.args[0]
            if error_code in (
                    2006,  # MySQL server has gone away
                    2013,  # Lost connection to MySQL server during query
            ):
                autocommit = self._backend.autocommit
                history = copy.copy(self._backend.failover_history)
                history_size = self._backend.failover_history_size
                # try to gracefully close the original cursor and connection even if it will most certainly fail
                try:
                    self._cursor.close()
                except Exception as e:
                    LOGGER.debug('Could not close cursor after error: ' + str(e), exc_info=True)
                try:
                    self._backend.close()
                except Exception as e:
                    LOGGER.debug('Could not close connection after error: ' + str(e), exc_info=True)
                self._backend.connection = None
                self._backend.primary_connected = False
                self._backend.connect()
                self._backend.set_autocommit(autocommit)
                self._backend.failover_history = history
                self._backend.failover_history_size = history_size
                LOGGER.warning('Replaying %d cursors from failover history after %s' % (len(history), str(exc)))
                self._cursor = self._backend.replay_history()
                self._in_handle_exc = False
                return
        self._in_handle_exc = False
        raise exc


class DatabaseWrapper(base.DatabaseWrapper):
    base_settings = None
    failover_history = list()
    failover_history_size = 0
    in_write_transaction = False
    primary_connected = False
    secondary_synced = True

    _failover_active = None
    _failover_enable = None
    _secondary_wrapper = None

    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        self.base_settings = copy.deepcopy(settings_dict)
        NODE_STATE.add_nodes(self.base_settings.get('NODES', ()))
        if 'OPTIONS' not in self.base_settings:
            self.base_settings['OPTIONS'] = dict()
        self.base_settings['OPTIONS'].pop('unix_socket', None)
        self.failover_enable = self.base_settings['OPTIONS'].pop('failover_enable', True)
        self.failover_history_limit = self.base_settings['OPTIONS'].pop('failover_history_limit', 1000)
        self.wsrep_sync_after_write = self.base_settings['OPTIONS'].pop('wsrep_sync_after_write', True)
        self.wsrep_sync_use_gtid = self.base_settings['OPTIONS'].pop('wsrep_sync_use_gtid', False)
        super(DatabaseWrapper, self).__init__(self.base_settings, alias=alias)

    def close(self):
        if self._secondary_wrapper is not None:
            self._secondary_wrapper.close()
            self._secondary_wrapper = None
        super(DatabaseWrapper, self).close()
        self.failover_history_reset()

    def connect(self):
        self.connect_to_node(primary=True)
        self.primary_connected = True

    def connect_to_node(self, primary=True):
        if primary:
            nodes = sorted(list(NODE_STATE.get_online_nodes()))
        else:
            nodes = list(NODE_STATE.get_online_nodes())
            random.shuffle(nodes)
            preferred_host = self.base_settings.get('HOST', '')
            if preferred_host:
                if preferred_host in nodes:
                    nodes.remove(preferred_host)
                nodes.insert(0, preferred_host)
        for node in nodes:
            LOGGER.info('connect %s to node %s' % ('primary' if primary else 'secondary', node))
            settings_dict = copy.deepcopy(self.base_settings)
            settings_dict['ENGINE'] = 'django.db.backends.mysql'
            for k, v in settings_dict['NODES'].get(node, {}).items():
                settings_dict[k] = v
            del settings_dict['NODES']
            try:
                if primary:
                    self.settings_dict = settings_dict
                    super(DatabaseWrapper, self).connect()
                    connection = self.connection
                else:
                    self._secondary_wrapper = base.DatabaseWrapper(settings_dict, alias=self.alias)
                    self._secondary_wrapper.connect()
                    connection = self._secondary_wrapper.connection
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT variable_value "
                    "FROM information_schema.global_status "
                    "WHERE variable_name = 'wsrep_ready'")
                result = cursor.fetchone()
                cursor.close()
                if result[0] == 'ON':
                    NODE_STATE.mark_online(node)
                    break
                else:
                    raise base.Database.Error('wsrep not ready')
            except base.Database.Error as e:
                LOGGER.info(e, exc_info=True)
                NODE_STATE.mark_offline(node)
        else:
            raise DatabaseError('No nodes available. Tried: %s' % ', '.join(nodes))

    def create_cursor(self, name=None):
        if not self.primary_connected:
            return super(DatabaseWrapper, self).create_cursor(name=name)
        else:
            if self.autocommit:
                self.failover_history_reset()
            return CursorWrapper(self)

    def create_primary_cursor(self):
        cursor = self.connection.cursor()
        return base.CursorWrapper(cursor)

    def create_secondary_cursor(self):
        cursor = self.secondary_wrapper.cursor()
        return base.CursorWrapper(cursor)

    @property
    def failover_active(self):
        if self._failover_active and self.failover_history_size >= self.failover_history_limit:
            self.failover_active = False
        return self._failover_active

    @failover_active.setter
    def failover_active(self, value):
        if not value:
            self.failover_history_reset()
        self._failover_active = value

    @property
    def failover_enable(self):
        return self._failover_enable

    @failover_enable.setter
    def failover_enable(self, value):
        self.failover_active = value
        self._failover_enable = value

    def failover_history_reset(self):
        self.failover_history.clear()
        self.failover_history_size = 0

    @property
    def secondary_wrapper(self):
        if self._secondary_wrapper is None:
            self.connect_to_node(primary=False)
        return self._secondary_wrapper

    def _set_autocommit(self, autocommit):
        if autocommit:
            self.in_write_transaction = False
        if autocommit != self.autocommit:
            self.failover_history_reset()
            self.failover_active = self.failover_enable
        return super(DatabaseWrapper, self)._set_autocommit(autocommit)

    def sync_wait_secondary(self):
        if self.wsrep_sync_after_write and not self.secondary_synced:
            try:
                t = time.perf_counter()
                if self.wsrep_sync_use_gtid:
                    self._wsrep_sync_wait_upto_gtid()
                else:
                    self._wsrep_sync_wait()
                t = time.perf_counter() - t
                LOGGER.debug('Secondary synced in %f seconds' % t)
            except Exception as e:
                LOGGER.warning('Error while syncing secondary: %s' % str(e), exc_info=True)
            self.secondary_synced = True

    def replay_history(self):
        for x, entry in enumerate(self.failover_history, start=1):
            if self.in_write_transaction:
                cursor = self.connection.cursor()
            else:
                cursor = self.secondary_wrapper.connection.cursor()
            for attr_name, args, kwargs, check in entry:
                attr = getattr(cursor, attr_name)
                if args is None:
                    result = attr
                else:
                    result = attr(*args, **kwargs)
                if check != hashlib.sha1(pprint.pformat(result).encode()).hexdigest():
                    raise DatabaseError('Replay checksum does not match')

            # do not close the cursor if this is the last history entry
            # the cursor should then have a comparable state as the original cursor
            if x != len(self.failover_history):
                cursor.close()
            else:
                return cursor

    def _wsrep_sync_wait(self):
        with self.secondary_wrapper.connection.cursor() as cursor:
            cursor.execute(
                'SET @wsrep_sync_wait_orig = @@wsrep_sync_wait;'
                'SET SESSION wsrep_sync_wait = GREATEST(@wsrep_sync_wait_orig, 1);'
                'SELECT 1;'
                'SET SESSION wsrep_sync_wait = @wsrep_sync_wait_orig;'
            )

    def _wsrep_sync_wait_upto_gtid(self):
        with self.connection.cursor() as primary_cursor:
            primary_cursor.execute('SELECT WSREP_LAST_WRITTEN_GTID()')
            result = primary_cursor.fetchone()
            primary_gtid = result[0].decode('utf-8')
        with self.secondary_wrapper.connection.cursor() as secondary_cursor:
            secondary_cursor.execute('SELECT WSREP_SYNC_WAIT_UPTO_GTID(%s)', (primary_gtid,))
            LOGGER.debug('Secondary sync upto %s' % primary_gtid)
