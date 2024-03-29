import os.path

from setuptools import setup

readme = os.path.join(os.path.dirname(__file__), 'README.rst')
with open(readme) as f:
    long_description = f.read()

setup(
    name='django-galera',
    version='1.1.3',
    description='Django database backend for MariaDB Galera Cluster',
    long_description=long_description,
    url='https://github.com/artworked-de/django-galera',
    author='Steve Hunger',
    author_email='steve@artworked.de',
    license='MIT',
    packages=[
        'galera',
        'galera.backends',
        'galera.backends.readwritesplit',
        'galera.migrations',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Framework :: Django',
        'Framework :: Django :: 2.2',
        'Framework :: Django :: 3.0',
        'Framework :: Django :: 3.1',
        'Framework :: Django :: 3.2',
        'Framework :: Django :: 4.0',
        'Framework :: Django :: 4.1',
    ],
    install_requires=[
        'django>=3.2'
    ]
)
