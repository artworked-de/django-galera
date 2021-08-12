*************
django-galera
*************
A Django application providing a database backend for MariaDB Galera Cluster.

Features
########
* Loadbalancing: Randomly choose a secondary node for readonly queries to balance load over multiple database servers
* Read/Write Splitting: Route all writes to a single primary node, which will greatly reduce deadlocks
* Optimistic transactions on secondary nodes: Switching to the primary node only once a write happens
* Automatic and transparent failover: On connection failures the backend will reconnect to a different node. After that
  a transaction replay will ensure data consistency, making a failure of both primary and secondary nodes transparent to
  the application

Setup
#####

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
                'failover_enable': True,  # enable transparent failover with transaction replay
                'failover_history_limit': 1000,  # disable replay for connections reaching this limit (saves memory)
                'wsrep_sync_after_write': True,  # explicitly wait until writes from primary have been applied before reading from secondary
                # options are also attributes of django.db.connection and can be changed on the fly for the current connection
            },
            'NODES': {
                'db1': {'HOST': '10.0.1.2'},  # first node becomes primary and is preferred for read/write transactions
                'db2': {'HOST': '10.0.1.3'},  # following nodes are secondary nodes, used for readonly transactions
                'db3': {'HOST': '10.0.1.4'},
                # 'db4': {'HOST': '10.0.1.4', 'PORT': 3307},    # node settings inherit but can be overwritten
            }
        }
    }

    MIDDLEWARE = [
        # Optional: the middleware repeats a request if a deadlock occurs
        'galera.middleware.GaleraMiddleware'
    ]
