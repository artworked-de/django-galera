=============
django-galera
=============
A Django application providing a database backend for MariaDB Galera Cluster.

Features
--------
* Read/Write Split: All writes will be routed to a single primary node, which greatly reduces deadlocks often seen with busy Django sites using Galera.
* Loadbalancing: Randomly choose a secondary node for readonly queries to balance load over multiple database servers.
* Optimistic transactions on secondary nodes: With Djangos ATOMIC_REQUESTS every request will be processed inside a transaction. Instead of connecting to the primary node once a transaction starts, django-galera will begin these transactions on a secondary node and only switches to the primary node once data is going to be changed.
* Automatic and transparent failover: When a node fails, the backend will reconnect to a different node. After that a transaction replay will ensure data consistency, making a failure of both primary and secondary nodes transparent to the application.

User guide
----------

Installation
############

You can install django-galera using pip:

.. code-block:: console

    $ python -m pip install django-galera


Configure as database backend
#############################

To start using django-galera, you'll have to change the **ENGINE** entry in your database configuration from **django.db.backends.mysql** to **galera.backends.readwritesplit** and add a **NODES** entry containing the nodes you want to use:

.. code-block:: python

    DATABASES = {
        'default': {
            'ATOMIC_REQUESTS': True,
            'ENGINE': 'galera.backends.readwritesplit',
            'PORT': 3306,
            'NAME': 'db_name',
            'USER': 'db_username',
            'PASSWORD': 'db_password',
            'OPTIONS' : {},
            'NODES': {
                'db1': {'HOST': '10.0.0.1'},
                'db2': {'HOST': '10.0.0.2'},
                'db3': {'HOST': '10.0.0.3'},
            }
        }
    }


Backend specific options
########################

Some features of django-galera can be configured to suit your needs by adding them to the OPTIONS entry of the database settings:

.. code-block:: python

    'OPTIONS' : {
        'disable_update_can_self_select': True,
        'failover_enable': True,
        'failover_history_limit': 10000,
        'optimistic_transactions': True,
        'reconnect_wait_time': 5.0,
        'wsrep_sync_after_write': True,
        'wsrep_sync_timeout': 5,
        'wsrep_sync_use_gtid': False,
    }


.. list-table:: Options
    :widths: 20 15 15 50
    :header-rows: 1

    * - Option
      - Type
      - Default
      - Description
    * - disable_update_can_self_select
      - bool
      - True
      - Django uses sub queries for updates on MariaDB >= 10.3.2. This causes excessive locking and even
        crashes in conjunction with Galera Cluster. Setting this to True will disable this behaviour and makes the SQL
        compiler use the classic approach instead
    * - failover_enable
      - bool
      - True
      - Enable failover and transaction replay on another node when the current node fails.
    * - failover_history_limit
      - int
      - 10000
      - Transaction replay keeps a list of every query and checksums of their results. In case of failure, they will be replayed on another node and the results compared to ensure data consistency. If there are more than this many entries in the list, failover and transaction replay will be disabled for the current transaction to prevent ever growing memory consumption.
    * - optimistic_transactions
      - bool
      - True
      - Enable optimistic transaction execution on secondary nodes, switching to primary node only once data is going to be changed. Depending on your application, you can disable this option if you have issues with data being changed by concurrent queries.
    * - reconnect_wait_time
      - float
      - 5.0
      - Wait time in seconds before reconnecting to another node after the current one failed
    * - wsrep_sync_after_write
      - bool
      - True
      - Although Galera allows replication to be almost instantaneous, it is still possible that changes written to the primary node have not yet been applied to the secondary node. If this option is set to True, django-galera will block until all changes have been written to the secondary node by making use of the variable **wsrep_sync_wait**.
    * - wsrep_sync_timeout
      - int
      - 5
      - Wait upto this number of seconds for the transaction to be applied. If replication takes longer, consider the current secondary node as failed and connect to the next available one.
    * - wsrep_sync_use_gtid
      - bool
      - False
      - Instead of using **wsrep_sync_wait**, django-galera can also utilize the more granular functions **wsrep_last_written_gtid** and **wsrep_sync_wait_upto_gtid**. As GTIDs are still not fully consistent and may drift away between nodes, this feature is disabled by default and should not be used until the drifting is fixed in MariaDB Galera Cluster.


Application and database on the same machine
############################################

In case the application and database server are running on the same machine, you can improve performance by having the application either connect to localhost using TCP/IP or using a socket.
Setting the **HOST** entry in your database settings will make django-galera try this node first when choosing a secondary node. This can greatly improve performance by reducing network roundtrip time.

.. code-block:: python

    DATABASES = {
        'default': {
            # ...
            'HOST': 'localhost',
            # or 'HOST': '/var/run/mysqld/mysqld.sock',
            # ...
        }
    }


Per node settings
#################

Database settings like **PORT**, **USER** or **PASSWORD** can be changed per node by simply specifying them in the nodes settings:

.. code-block:: python

    'NODES': {
        'db1': {'HOST': '10.0.0.1', 'PORT': 3306, 'USERNAME': 'user1'},
        'db2': {'HOST': '10.0.0.2', 'PORT': 3307, 'USERNAME': 'user2'},
        'db3': {'HOST': '10.0.0.3', 'PORT': 3308, 'USERNAME': 'user3'},
    }


Example configuration
#####################
This is an annotated example configuration for a 3-node cluster.


.. code-block:: python

    DATABASES = {
        'default': {
            'ATOMIC_REQUESTS': True,
            'ENGINE': 'galera.backends.readwritesplit',
            # if HOST is omitted, a random node will be used for secondary (readonly) access
            # HOST can still be set to prioritize a node (useful if app and db are running on the same machine)
            # 'HOST': 'localhost',                      # prefer a local node for faster access times
            # 'HOST': '/var/run/mysqld/mysqld.sock',    # also works with sockets
            'PORT': 3306,
            'NAME': 'db_name',
            'USER': 'db_username',
            'PASSWORD': 'db_password',
            'OPTIONS': {
                'disable_update_can_self_select': True,  # fixes issues with large updates leading to excessive locking and crashes
                'failover_enable': True,  # enable transparent failover with transaction replay
                'failover_history_limit': 10000,  # disable replay for transactions reaching this limit (saves memory)
                'optimistic_transactions': True,  # enable optimistic transaction execution on secondary node
                'reconnect_wait_time': 5.0,  # wait time before connecting to a new node after the current one failed
                'wsrep_sync_after_write': True,  # explicitly wait until writes from primary have been applied before reading from secondary
                'wsrep_sync_timeout': 5,  # wait upto this number of seconds for writes being applied to secondary
                'wsrep_sync_use_gtid': False,  # use WSREP_SYNC_UPTO_GTID for syncing secondary node (currently not recommended because of drifting GTIDs)
                # options are also attributes of django.db.connection and can be changed on the fly for the current connection
            },
            'NODES': {
                'db1': {'HOST': '10.0.0.1'},  # first node becomes primary and is preferred for read/write transactions
                'db2': {'HOST': '10.0.0.2'},  # following nodes are secondary nodes, used for readonly transactions
                'db3': {'HOST': '10.0.0.3'},
                # 'db4': {'HOST': '10.0.0.3', 'PORT': 3307},    # node settings inherit but can be overwritten
            }
        }
    }
