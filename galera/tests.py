from unittest import mock

from django import db
from django.db.backends.mysql import base
from django.test import SimpleTestCase

from galera.backends.readwritesplit import base as backend


class ReadWriteSplitBackendTestCase(SimpleTestCase):
    def setUp(self):
        backend.NODE_STATE = backend.NodeState()
        self.connection = db.ConnectionHandler(settings={
            db.DEFAULT_DB_ALIAS: {
                'ENGINE': 'galera.backends.readwritesplit',
                'NODES': {
                    'db1': {},
                    'db2': {},
                }
            }
        })[db.DEFAULT_DB_ALIAS]

    @mock.patch('django.db.backends.mysql.base.Database.connect')
    def test_primary_online_none(self, mock_connect):
        """Ensure DatabaseError is raised when no primary node is online"""
        mock_connect.side_effect = base.Database.Error()

        # connection should fail on all nodes
        with self.assertRaises(db.DatabaseError):
            self.connection.connect()

        # no node should be marked online
        self.assertEqual(len(backend.NODE_STATE.nodes), 2)
        self.assertEqual(len(list(backend.NODE_STATE.get_online_nodes())), 0)

    @mock.patch('django.db.backends.mysql.base.Database.connect')
    def test_primary_online_any(self, mock_connect):
        """Ensure a connection can be established when any primary node is online"""
        online_mock = mock.MagicMock()
        cursor = online_mock.cursor.return_value
        # simulate status variable wsrep_ready = ON
        cursor.fetchone.side_effect = [mock.MagicMock(), ('ON',)]
        mock_connect.side_effect = [base.Database.Error(), online_mock]

        # connection to atleast one node should succeed
        self.connection.connect()

        # one node should be marked online
        self.assertEqual(len(backend.NODE_STATE.nodes), 2)
        self.assertEqual(len(list(backend.NODE_STATE.get_online_nodes())), 1)

    @mock.patch('django.db.backends.mysql.base.Database.connect')
    def test_primary_online_after_offline(self, mock_connect):
        """Ensure a connection to a primary peer is successful after it has been marked offline"""
        online_mock = mock.MagicMock()
        cursor = online_mock.cursor.return_value
        # simulate status variable wsrep_ready = ON
        cursor.fetchone.side_effect = [mock.MagicMock(), ('ON',), ('ON',)]
        mock_connect.side_effect = [base.Database.Error(), online_mock, online_mock]

        # connection should still succeed if one node is online
        self.connection.connect()

        # and one node should be marked online
        self.assertEqual(len(backend.NODE_STATE.nodes), 2)
        self.assertEqual(len(list(backend.NODE_STATE.get_online_nodes())), 1)

        # retry connecting to the first node
        backend.NODE_STATE.RETRY_INTERVAL = 0
        self.connection.connect()

        # all nodes should be marked online
        self.assertEqual(len(backend.NODE_STATE.nodes), 2)
        self.assertEqual(len(list(backend.NODE_STATE.get_online_nodes())), 2)

    @mock.patch('django.db.backends.mysql.base.Database.connect')
    def test_secondary_nodes_online_none(self, mock_connect):
        """Ensure DatabaseError is raised when no secondary node is online"""
        mock_connect.side_effect = base.Database.Error()

        # connection should fail on all nodes
        with self.assertRaises(db.DatabaseError):
            self.connection.secondary_wrapper.ensure_connection()

        # no node should be marked online
        self.assertEqual(len(backend.NODE_STATE.nodes), 2)
        self.assertEqual(len(list(backend.NODE_STATE.get_online_nodes())), 0)

    @mock.patch('django.db.backends.mysql.base.Database.connect')
    def test_secondary_nodes_online_any(self, mock_connect):
        """Ensure a connection can be established when any secondary node is online"""
        online_mock = mock.MagicMock()
        cursor = online_mock.cursor.return_value
        # simulate status variable wsrep_ready = ON
        cursor.fetchone.side_effect = [mock.MagicMock(), ('ON',)]
        mock_connect.side_effect = [base.Database.Error(), online_mock]

        # connection to atleast one node should succeed
        self.connection.secondary_wrapper.ensure_connection()

        # one node should be marked online
        self.assertEqual(len(backend.NODE_STATE.nodes), 2)
        self.assertEqual(len(list(backend.NODE_STATE.get_online_nodes())), 1)

    @mock.patch('django.db.backends.mysql.base.Database.connect')
    def test_secondary_nodes_online_after_offline(self, mock_connect):
        """Ensure a connection to a secondary peer is successful after it has been marked offline"""
        online_mock = mock.MagicMock()
        cursor = online_mock.cursor.return_value
        # simulate status variable wsrep_ready = ON
        cursor.fetchone.side_effect = [mock.MagicMock(), ('ON',), ('ON',)]
        mock_connect.side_effect = [base.Database.Error(), online_mock, online_mock]

        # connection should still succeed if one node is online
        self.connection.secondary_wrapper.ensure_connection()

        # and one node should be marked online
        self.assertEqual(len(backend.NODE_STATE.nodes), 2)
        self.assertEqual(len(list(backend.NODE_STATE.get_online_nodes())), 1)

        # retry connecting to the first node
        backend.NODE_STATE.RETRY_INTERVAL = 0
        self.connection.secondary_wrapper.ensure_connection()

        # all nodes should be marked online
        self.assertEqual(len(backend.NODE_STATE.nodes), 2)
        self.assertEqual(len(list(backend.NODE_STATE.get_online_nodes())), 2)
