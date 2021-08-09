import hashlib
import logging
import pprint
import random
import time
from copy import copy

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
        self._in_handle_exc = False

    @property
    def cursor(self):
        if self._cursor is None:
            if self._primary:
                self._cursor = self._backend.create_primary_cursor()
                self._backend.failover_history.append([])
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
            if not self._backend.autocommit and not self._backend.in_write_transaction:
                self._backend.secondary_synced = False
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
        # clear and disable query history if size exceeds limit for this connection
        if self._backend.failover_enable:
            if self._backend.failover_history_size >= self._backend.failover_history_limit:
                LOGGER.info('Failover history limit reached. Disabling history for this connection.')
                self._backend.failover_history.clear()
                self._backend.failover_history_size = 0
                self._backend.failover_history_enable = False

        try:
            obj = getattr(self.cursor, item)
        except base.Database.OperationalError as e:
            self._handle_exc(e)
            obj = getattr(self._cursor, item)

        if item != '_executed' and self._primary and self._backend.failover_enable:
            def wrap(func):
                def decor(*args, **kwargs):
                    try:
                        ret = func(*args, **kwargs)
                    except base.Database.OperationalError as e:
                        self._handle_exc(e)
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

    def __getattr__(self, item):
        return self._failover_cursor(item)

    def _handle_exc(self, exc):
        if self._in_handle_exc:
            raise exc
        self._in_handle_exc = True
        if self._backend.failover_enable:
            error_code = exc.args[0]
            if error_code == 2006: # server has gone away
                autocommit = self._backend.autocommit
                history = copy(self._backend.failover_history)
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
                try:
                    raise exc
                except Exception as e:
                    logging.getLogger('django').error(
                        'Replaying %d cursors from failover history after %s' % (len(history), str(e)),
                        exc_info=True
                    )
                self._cursor = self._backend.replay_history()
                self._in_handle_exc = False
                return
        self._in_handle_exc = False
        raise exc


class DatabaseWrapper(base.DatabaseWrapper):
    base_settings = None
    in_write_transaction = False
    primary_connected = False
    secondary_synced = True
    _secondary_wrapper = None

    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        self.base_settings = settings_dict.copy()
        self.failover_enable = False
        self.failover_history = list()
        self.failover_history_limit = 1000
        self.failover_history_size = 0
        if 'NODES' in settings_dict:
            NODE_STATE.add_nodes(settings_dict['NODES'])
        if 'OPTIONS' in settings_dict:
            if 'failover_enable' in settings_dict['OPTIONS']:
                self.failover_enable = settings_dict['OPTIONS']['failover_enable']
                del settings_dict['OPTIONS']['failover_enable']
            if 'failover_history_limit' in settings_dict['OPTIONS']:
                self.failover_history_limit = settings_dict['OPTIONS']['failover_history_limit']
                del settings_dict['OPTIONS']['failover_history_limit']
            if 'unix_socket' in settings_dict['OPTIONS']:
                del settings_dict['OPTIONS']['unix_socket']
        super(DatabaseWrapper, self).__init__(settings_dict, alias=alias)

    def close(self):
        if self._secondary_wrapper is not None:
            self._secondary_wrapper.close()
            self._secondary_wrapper = None
        super(DatabaseWrapper, self).close()
        self.failover_history.clear()
        self.failover_history_size = 0

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
            if self.autocommit:
                self.failover_history.clear()
                self.failover_history_size = 0
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
        if autocommit:
            self.in_write_transaction = False
            self.failover_history.clear()
            self.failover_history_size = 0
        return super(DatabaseWrapper, self)._set_autocommit(autocommit)

    def sync_wait_secondary(self):
        cursor = self.connection.cursor()
        cursor.execute('SELECT WSREP_LAST_SEEN_GTID()')
        result = cursor.fetchone()
        cursor.close()
        primary_gtid = result[0].decode('utf-8')
        if primary_gtid != '00000000-0000-0000-0000-000000000000:-1':
            cursor = self.secondary_wrapper.connection.cursor()
            cursor.execute('SELECT WSREP_SYNC_WAIT_UPTO_GTID(%s)', (primary_gtid,))
            cursor.close()
            LOGGER.debug('Secondary sync upto %s' % primary_gtid)
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
                    raise Exception('Replay checksum does not match')

            # do not close the cursor if this is the last history entry
            # the cursor should then have a comparable state as the original cursor
            if x != len(self.failover_history):
                cursor.close()
            else:
                return cursor
