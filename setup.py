import os.path

from setuptools import setup

readme = os.path.join(os.path.dirname(__file__), 'README.rst')
with open(readme) as f:
    long_description = f.read()

setup(
    name='django-galera',
    version='0.4',
    description='Django database backend for MariaDB Galera Cluster',
    long_description=long_description,
    url='https://github.com/pogowurst/django-galera',
    author='Steve Hunger',
    author_email='pogowurst87@googlemail.com',
    license='MIT',
    packages=(
        'galera',
        'galera.backends',
        'galera.backends.readwritesplit',
        'galera.migrations',
    ),
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Framework :: Django',
        'Framework :: Django :: 2.2',
        'Framework :: Django :: 3.0',
        'Framework :: Django :: 3.1',
        'Framework :: Django :: 3.2',
    ),
    install_requires=(
        'django>=2.2'
    )
)
