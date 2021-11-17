#!/usr/bin/env python
from setuptools import config, find_packages, setup

try:
    from dunamai import Version
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'dunamai'])
    from dunamai import Version

metadata = config.read_configuration('setup.cfg')['metadata']

setup(
    name=metadata['name'],
    version=Version.from_any_vcs().serialize(),
    author=metadata['author'],
    author_email=metadata['author_email'],
    description=metadata['description'],
    long_description=metadata['long_description'],
    long_description_content_type='text/markdown',
    url=metadata['url'],
    packages=find_packages(),
    python_requires='>=3.8',
    setup_requires=['dunamai', 'setuptools>=41.2'],
    install_requires=[
        'psycopg2-binary',
        'pyproj',
        'python-dateutil',
        'shapely',
        'spatialite',
        'sshtunnel',
        'typepigeon>=1.0.2',
    ],
    extras_require={
        'testing': ['pytest', 'pytest-cov', 'pytest-xdist'],
        'development': ['dunamai', 'flake8', 'isort', 'oitnb'],
        'documentation': ['dunamai', 'm2r2', 'sphinx', 'sphinx-rtd-theme'],
    },
)
