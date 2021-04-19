import logging
import random
import time

from django.db import DEFAULT_DB_ALIAS, DatabaseError
from django.db.backends.mysql import base
from django.utils.asyncio import async_unsafe

LOGGER = logging.getLogger(__name__)

SET_SECONDARY_READ_ONLY = False


class NodeState:
    RETRY_INTERVAL = 30

    def __init__(self):
        self.nodes = dict()

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


NODE_STATE = NodeState()


class CursorWrapper:
    def __init__(self, backend):
        self._backend = backend
        self._cursor = None
        self._primary = False

    @property
    def cursor(self):
        if self._cursor is None:
            if self._primary:
                self._cursor = self._backend.create_primary_cursor()
            else:
                self._cursor = self._backend.create_secondary_cursor()
        return self._cursor

    def prepare(self, query):
        rw_query = query is None or not query.startswith('SELECT ') or ' FOR UPDATE' in query or ' INTO ' in query
        if rw_query and not query.startswith('SET SESSION '):
            self._backend.secondary_synced = False
        rw_query = rw_query or query.startswith('SELECT @')
        if rw_query and not self._backend.autocommit and not self._backend.in_write_transaction:
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
        return self.cursor.callproc(procname, args=args)

    def execute(self, query, args=None):
        self.prepare(query)
        return self.cursor.execute(query, args=args)

    def executemany(self, query, args=None):
        self.prepare(query)
        return self.cursor.executemany(query, args=args)

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

    def __getattr__(self, item):
        return getattr(self.cursor, item)


class DatabaseWrapper(base.DatabaseWrapper):
    base_settings = None
    in_write_transaction = False
    secondary_synced = True
    _secondary_connection = None

    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        self.base_settings = settings_dict.copy()
        if 'NODES' in settings_dict:
            NODE_STATE.add_nodes(settings_dict['NODES'])
        if 'OPTIONS' in settings_dict and 'unix_socket' in settings_dict['OPTIONS']:
            del settings_dict['OPTIONS']['unix_socket']
        super(DatabaseWrapper, self).__init__(settings_dict, alias=alias)

    @async_unsafe
    def close(self):
        if self._secondary_connection is not None:
            self._secondary_connection.close()
            self._secondary_connection = None
        super(DatabaseWrapper, self).close()

    @async_unsafe
    def create_cursor(self, name=None):
        return CursorWrapper(self)

    @async_unsafe
    def create_primary_cursor(self, name=None):
        cursor = self.connection.cursor()
        return base.CursorWrapper(cursor)

    @async_unsafe
    def create_secondary_cursor(self, name=None):
        cursor = self.secondary_connection.cursor()
        return base.CursorWrapper(cursor)

    def _set_autocommit(self, autocommit):
        if autocommit and self.in_write_transaction:
            self.in_write_transaction = False
        return super(DatabaseWrapper, self)._set_autocommit(autocommit)

    @property
    def secondary_connection(self):
        if self._secondary_connection is None:
            nodes = list(NODE_STATE.get_online_nodes())
            random.shuffle(nodes)
            preferred_host = self.base_settings.get('HOST', '')
            if preferred_host:
                if preferred_host in nodes:
                    nodes.remove(preferred_host)
                nodes.insert(0, preferred_host)
            for node in nodes:
                LOGGER.info('connect %s to node %s' % ('secondary', node))
                settings_dict = self.base_settings.copy()
                settings_dict['ENGINE'] = 'django.db.backends.mysql'
                for k, v in settings_dict['NODES'].get(node, {}).items():
                    settings_dict[k] = v
                del settings_dict['NODES']
                self._secondary_connection = base.DatabaseWrapper(settings_dict, alias=self.alias)
                try:
                    self._secondary_connection.connect()
                    cursor = self._secondary_connection.connection.cursor()
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
                        LOGGER.info('wsrep not ready')
                except base.Database.Error as e:
                    LOGGER.info(e, exc_info=True)
                    NODE_STATE.mark_offline(node)
            else:
                raise DatabaseError('No nodes available. Tried: %s' % ', '.join(nodes))

            if SET_SECONDARY_READ_ONLY:
                q = 'SET SESSION TRANSACTION READ ONLY'
                LOGGER.debug('Secondary: %s' % q)
                cursor = self._secondary_connection.connection.cursor()
                cursor.execute(q)
                cursor.close()

        return self._secondary_connection

    def sync_wait_secondary(self):
        cursor = self.connection.cursor()
        cursor.execute('SELECT WSREP_LAST_SEEN_GTID()')
        result = cursor.fetchone()
        cursor.close()
        primary_gtid = result[0].decode('utf-8')
        if primary_gtid != '00000000-0000-0000-0000-000000000000:-1':
            cursor = self.secondary_connection.connection.cursor()
            cursor.execute('SELECT WSREP_SYNC_WAIT_UPTO_GTID(%s)', (primary_gtid,))
            cursor.close()
            LOGGER.debug('Secondary sync upto %s' % primary_gtid)
        self.secondary_synced = True

    @async_unsafe
    def connect(self):
        nodes = list(NODE_STATE.get_online_nodes())
        nodes = sorted(nodes)
        for node in nodes:
            LOGGER.info('connect %s to node %s' % ('primary', node))
            settings_dict = self.base_settings.copy()
            settings_dict['ENGINE'] = 'django.db.backends.mysql'
            for k, v in settings_dict['NODES'].get(node, {}).items():
                settings_dict[k] = v
            del settings_dict['NODES']
            self.settings_dict = settings_dict
            try:
                super(DatabaseWrapper, self).connect()
                cursor = self.connection.cursor()
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
