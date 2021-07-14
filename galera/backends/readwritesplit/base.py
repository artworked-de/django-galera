import logging
import random
import time

from django.db import DEFAULT_DB_ALIAS, DatabaseError
from django.db.backends.mysql import base

LOGGER = logging.getLogger(__name__)


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
    primary_connected = False
    secondary_synced = True
    _secondary_wrapper = None

    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        self.base_settings = settings_dict.copy()
        if 'NODES' in settings_dict:
            NODE_STATE.add_nodes(settings_dict['NODES'])
        else:
            raise DatabaseError('No nodes defined in database settings')
        if 'OPTIONS' in settings_dict and 'unix_socket' in settings_dict['OPTIONS']:
            del settings_dict['OPTIONS']['unix_socket']
        super(DatabaseWrapper, self).__init__(settings_dict, alias=alias)

    def close(self):
        if self._secondary_wrapper is not None:
            self._secondary_wrapper.close()
            self._secondary_wrapper = None
        super(DatabaseWrapper, self).close()

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
            settings_dict = self.base_settings.copy()
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
            return CursorWrapper(self)

    def create_primary_cursor(self):
        cursor = self.connection.cursor()
        return base.CursorWrapper(cursor)

    def create_secondary_cursor(self):
        cursor = self.secondary_wrapper.cursor()
        return base.CursorWrapper(cursor)

    @property
    def secondary_wrapper(self):
        if self._secondary_wrapper is None:
            self.connect_to_node(primary=False)
        return self._secondary_wrapper

    def _set_autocommit(self, autocommit):
        if autocommit and self.in_write_transaction:
            self.in_write_transaction = False
        return super(DatabaseWrapper, self)._set_autocommit(autocommit)

    def sync_wait_secondary(self):
        cursor = self.connection.cursor()
        cursor.execute('SELECT WSREP_LAST_SEEN_GTID()')
        result = cursor.fetchone()
        cursor.close()
        primary_gtid = result[0].decode('utf-8')
        cursor = self.secondary_wrapper.connection.cursor()
        try:
            cursor.execute('SELECT WSREP_SYNC_WAIT_UPTO_GTID(%s)', (primary_gtid,))
            LOGGER.debug('Secondary sync upto %s' % primary_gtid)
        except base.Database.OperationalError as e:
            LOGGER.warning('Could not sync secondary upto %s: %s' % (primary_gtid, str(e)))
            cursor.close()
        self.secondary_synced = True
