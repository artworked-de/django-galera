import copy
import hashlib
import logging
import pprint
import random
import re
import time

from django.db import DEFAULT_DB_ALIAS, DatabaseError
from django.db.backends.mysql import base
from django.utils.functional import cached_property

LOGGER = logging.getLogger(__name__)


class NodeState:
    RETRY_INTERVAL = 30

    def __init__(self, store=None):
        if store is None:
            store = dict()
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

    def get_all_nodes(self):
        return tuple(self.nodes.keys())

    def get_online_nodes(self):
        return tuple(x for x, y in self.nodes.items() if y is None or time.time() > y + self.RETRY_INTERVAL)


NODE_STATE = NodeState()


class CursorWrapper:
    def __init__(self, backend):
        self._backend = backend
        self._cursor = None
        self._primary = False
        self._in_handle_exc = False
        self._history_entry_index = None

    @property
    def cursor(self):
        if self._cursor is None:
            if self._primary:
                self._cursor = self._backend.create_primary_cursor()
            else:
                self._cursor = self._backend.create_secondary_cursor()
        return self._cursor

    def add_history(self, attr, args, kwargs, return_value):
        if self._backend.failover_active and attr != '_executed':
            if self._history_entry_index is None or len(self._backend.failover_history) == 0:
                self._history_entry_index = len(self._backend.failover_history)
                self._backend.failover_history.append([])
            self._backend.failover_history[self._history_entry_index].append((
                attr,
                args,
                kwargs,
                hashlib.sha1(pprint.pformat(return_value).encode()).hexdigest()
            ))
            self._backend.failover_history_size += 1

            # store the insert id of an auto field, so it does not change when the history is replayed
            if self._backend.failover_history[self._history_entry_index][-1][0] in ('fetchone', 'fetchall') \
                    and self._backend.failover_history[self._history_entry_index][-2][0] == 'execute' \
                    and self._backend.failover_history[self._history_entry_index][-2][1][0].startswith('INSERT '):
                insert_entry = self._backend.failover_history[self._history_entry_index][-2]
                insert_sql = insert_entry[1][0]
                match = re.match(
                    r'^INSERT INTO `([^`]+)` \((.+)\) VALUES (.+) RETURNING `([^`]+)`.`([^`]+)`$',
                    insert_sql
                )
                if match:
                    table_name, fields, values, auto_table, auto_field = match.groups()
                    values_new = values.replace('(%s, ', '(%s, %s, ')
                    new_sql = (
                        f'INSERT INTO `{table_name}` '
                        f'(`{auto_field}`, {fields}) '
                        f'VALUES {values_new} '
                        f'RETURNING `{auto_table}`.`{auto_field}`'
                    )
                    if new_sql != insert_sql:
                        kwargs = copy.deepcopy(insert_entry[2])
                        kwargs['args'] = list(kwargs['args'])
                        values_count = int(round(len(kwargs['args']) / len(return_value)))
                        if len(return_value) == 1:
                            kwargs['args'].insert(0, str(return_value[0]))
                        else:
                            for x in range(len(return_value)):
                                kwargs['args'].insert(x * values_count + x, str(return_value[x][0]))
                        kwargs['args'] = tuple(kwargs['args'])
                        self._backend.failover_history[self._history_entry_index][-2] = (
                            insert_entry[0],
                            (new_sql,),
                            kwargs,
                            insert_entry[3],
                        )
                    else:
                        LOGGER.warning('SQL unchanged: %s' % insert_sql)
                else:
                    LOGGER.warning('No match: %s' % insert_sql)

    def prepare(self, query):
        if query is None:
            rw_query = True
        else:
            query = query.strip()
            rw_query = not query.startswith('SELECT ')
            rw_query = rw_query or query.endswith(' FOR UPDATE') or ' INTO ' in query
        if rw_query:
            self._backend.primary_synced = False
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
        except Exception as e:
            self._handle_exc(e)
            obj = getattr(self._cursor, item)

        def wrap(func):
            def decor(*args, **kwargs):
                try:
                    ret = func(*args, **kwargs)
                except Exception as exc:
                    self._handle_exc(exc)
                    ret = getattr(self._cursor, func.__name__)(*args, **kwargs)
                self.add_history(item, args, kwargs, ret)
                return ret
            return decor
        if hasattr(obj, '__call__'):
            obj = wrap(obj)
        else:
            self.add_history(item, None, None, obj)
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
        if self._in_handle_exc or not exc.args:
            raise exc
        self._in_handle_exc = True
        if self._backend.failover_active and len(exc.args):
            error_code = str(exc.args[0])
            if error_code in (
                    '1047',  # Unknown command (wsrep_reject_queries)
                    '1180',  # Got error 6 "No such device or address" during COMMIT
                    '1205',  # Lock wait timeout exceeded; try restarting transaction
                    '1213',  # Deadlock found when trying to get lock; try restarting transaction
                    '2006',  # MySQL server has gone away
                    '2013',  # Lost connection to MySQL server during query
            ):
                autocommit = self._backend.autocommit
                in_atomic_block = self._backend.in_atomic_block
                in_write_transaction = self._backend.in_write_transaction
                needs_rollback = self._backend.needs_rollback
                savepoint_ids = self._backend.savepoint_ids
                history = copy.deepcopy(self._backend.failover_history)
                history_size = self._backend.failover_history_size

                # try to gracefully close the original cursor and connection even if it will most certainly fail
                try:
                    self._cursor.close()
                except Exception as e:
                    LOGGER.debug('Could not close cursor after error: ' + str(e), exc_info=True)
                try:
                    self._backend.connection.rollback()
                except Exception as e:
                    LOGGER.debug('Could not rollback connection after error: ' + str(e), exc_info=True)
                try:
                    self._backend.close()
                except Exception as e:
                    LOGGER.debug('Could not close connection after error: ' + str(e), exc_info=True)

                LOGGER.warning('Replaying %d cursors from failover history after %s' % (len(history), str(exc)))

                self._backend.connect()
                self._backend.set_autocommit(False, force_begin_transaction_with_broken_autocommit=True)
                self._backend.needs_rollback = True
                self._cursor = self._backend.replay_history(history)
                self._backend.needs_rollback = needs_rollback
                if autocommit:
                    self._backend.connection.commit()
                    self._backend.set_autocommit(autocommit)
                self._backend.in_atomic_block = in_atomic_block
                self._backend.in_write_transaction = in_write_transaction
                self._backend.savepoint_ids = savepoint_ids
                self._backend.failover_history = copy.deepcopy(history)
                self._backend.failover_history_size = history_size
                self._in_handle_exc = False
                return
        self._in_handle_exc = False
        raise exc


class DatabaseFeatures(base.DatabaseFeatures):
    @cached_property
    def update_can_self_select(self):
        if self.connection.disable_update_can_self_select:
            return False
        return super(DatabaseFeatures, self).update_can_self_select


class DatabaseWrapper(base.DatabaseWrapper):
    features_class = DatabaseFeatures

    base_settings = None
    failover_history = None
    failover_history_size = 0
    in_write_transaction = False
    primary_connected = False
    primary_synced = True
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
        self.disable_update_can_self_select = self.base_settings['OPTIONS'].pop('disable_update_can_self_select', True)
        self.failover_enable = self.base_settings['OPTIONS'].pop('failover_enable', True)
        self.failover_history = list()
        self.failover_history_limit = self.base_settings['OPTIONS'].pop('failover_history_limit', 1000)
        self.optimistic_transactions = self.base_settings['OPTIONS'].pop('optimistic_transactions', True)
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
        self.connection = None
        self.primary_connected = False
        self.connect_to_node(primary=True)
        self.primary_connected = True

    def connect_to_node(self, primary=True):
        if primary:
            nodes = sorted(NODE_STATE.get_online_nodes() or NODE_STATE.get_all_nodes())
        else:
            nodes = list(NODE_STATE.get_online_nodes() or NODE_STATE.get_all_nodes())
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
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT variable_name, variable_value "
                        "FROM information_schema.global_status "
                        "WHERE variable_name IN ("
                        "'WSREP_CLUSTER_STATUS', 'WSREP_LOCAL_STATE', 'WSREP_READY'"
                        ") "
                        "UNION "
                        "SELECT variable_name, variable_value "
                        "FROM information_schema.global_variables "
                        "WHERE variable_name IN ("
                        "'WSREP_DESYNC', 'WSREP_REJECT_QUERIES', 'WSREP_SST_DONOR_REJECTS_QUERIES'"
                        ")"
                    )
                    results = {k.upper(): v.upper() for k, v in cursor.fetchall()}
                if results['WSREP_READY'] != 'ON':
                    raise base.Database.Error('WSREP_READY: %s' % results['WSREP_READY'])
                if results['WSREP_CLUSTER_STATUS'] != 'PRIMARY':
                    raise base.Database.Error('WSREP_CLUSTER_STATUS: %s' % results['WSREP_CLUSTER_STATUS'])
                if results['WSREP_DESYNC'] != 'OFF':
                    raise base.Database.Error('WSREP_DESYNC')
                if results['WSREP_LOCAL_STATE'] != '4':
                    if results['WSREP_LOCAL_STATE'] != '2':
                        raise base.Database.Error('WSREP_LOCAL_STATE: %s' % results['WSREP_LOCAL_STATE'])
                    elif results['WSREP_SST_DONOR_REJECTS_QUERIES'] == 'ON':
                        raise base.Database.Error('WSREP_SST_DONOR_REJECTS_QUERIES')
                if results['WSREP_REJECT_QUERIES'] != 'NONE':
                    raise base.Database.Error('WSREP_REJECT_QUERIES: %s' % results['WSREP_REJECT_QUERIES'])
                NODE_STATE.mark_online(node)
                break
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
        self.failover_history = list()
        self.failover_history_size = 0

    @property
    def secondary_wrapper(self):
        if self._secondary_wrapper is None:
            self.connect_to_node(primary=False)
        return self._secondary_wrapper

    def _set_autocommit(self, autocommit):
        super(DatabaseWrapper, self)._set_autocommit(autocommit)
        if autocommit:
            self.in_write_transaction = False
        if not autocommit and not self.optimistic_transactions:
            self.in_write_transaction = True
        if autocommit != self.autocommit:
            self.failover_history_reset()
            self.failover_active = self.failover_enable

    def sync_wait_secondary(self):
        if self.wsrep_sync_after_write and not self.secondary_synced:
            t = time.perf_counter()
            retry = 0
            while not self.secondary_synced:
                try:
                    if self.wsrep_sync_use_gtid:
                        self._wsrep_sync_wait_upto_gtid()
                    else:
                        self._wsrep_sync_wait(self.secondary_wrapper.connection)
                    self.secondary_synced = True
                except Exception as e:
                    error_code = str(e.args[0]) if e.args else ''
                    if retry < 3 and error_code == '1205':  # Lock wait timeout exceeded; try restarting transaction
                        LOGGER.info('Retry syncing secondary after: %s' % str(e), exc_info=True)
                        retry += 1
                        continue
                    raise e
            t = time.perf_counter() - t
            LOGGER.debug('Secondary synced in %f seconds' % t)

    def replay_history(self, history):
        for x, entry in enumerate(history, start=1):
            cursor = self.connection.cursor()
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
            if x != len(history):
                cursor.close()
            else:
                return cursor
        # return a new cursor if history is empty
        return self.connection.cursor()

    def _wsrep_sync_wait(self, connection):
        with connection.cursor() as cursor:
            cursor.execute(
                'SET @wsrep_sync_wait_orig = @@wsrep_sync_wait;'
                'SET SESSION lock_wait_timeout = 5;'
                'SET SESSION wsrep_sync_wait = GREATEST(@wsrep_sync_wait_orig, 1);'
                'SELECT 1;'
                'SET SESSION wsrep_sync_wait = @wsrep_sync_wait_orig;'
            )

    def _wsrep_sync_wait_upto_gtid(self):
        with self.connection.cursor() as primary_cursor:
            primary_cursor.execute('SELECT WSREP_LAST_SEEN_GTID()')
            result = primary_cursor.fetchone()
            primary_gtid = result[0].decode('utf-8')
        with self.secondary_wrapper.connection.cursor() as secondary_cursor:
            secondary_cursor.execute('SELECT WSREP_SYNC_WAIT_UPTO_GTID(%s)', (primary_gtid,))
            LOGGER.debug('Secondary sync upto %s' % primary_gtid)
