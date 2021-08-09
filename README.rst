*************
django-galera
*************
A Django application providing a database backend for MariaDB Galera Cluster.

.. important:: This project is in development and not recommended for being used in production.

Features
########
* Loadbalancing
* Read/Write Splitting
* Optimistic transactions on slaves
* Automatic and transparent failover on connection failures with transaction replay

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
