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
* Automatic failover on connection failures

Setup
#####

.. code-block:: python

    DATABASES = {
        'default': {
            'ATOMIC_REQUESTS': True,
            'ENGINE': 'galera.backends.readwritesplit',
            # 'HOST': 'localhost',                      # prefer a local node for faster access times
            # 'HOST': '/var/run/mysqld/mysqld.sock',    # also works with sockets
            'PORT': 3306,
            'NAME': 'db_name',
            'USER': 'db_username',
            'PASSWORD': 'db_password',
            'NODES': {
                'db1': {'HOST': '10.0.1.2'},
                'db2': {'HOST': '10.0.1.3'},
                'db3': {'HOST': '10.0.1.4'},
                # 'db4': {'HOST': '10.0.1.4', 'PORT': 3307},    # node settings inherit but can be overwritten
            }
        }
    }

    MIDDLEWARE = [
        # Optional: the middleware repeats a request if a deadlock occurs
        'galera.middleware.GaleraMiddleware'
    ]
