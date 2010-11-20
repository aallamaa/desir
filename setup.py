#!/usr/bin/env python

"""
@file setup.py
@author Abdelkader ALLAM
@date 10/30/2010
@brief Setuptools configuration for redis client
"""

version = '0.1'

sdict = {
    'name' : 'desir',
    'version' : version,
    'description' : 'Python client for Redis key-value store',
    'long_description' : 'Python client for Redis key-value store',
    'url': 'http://github.com/aallamaa/desir',
    'download_url' : 'http://cloud.github.com/downloads/aallamaa/desir/desir-%s.tar.gz' % version,
    'author' : 'Abdelkader ALLAM',
    'author_email' : 'abdelkader.allam@gmail.com',
    'maintainer' : 'Abdelkader ALLAM',
    'maintainer_email' : 'abdelkader.allam@gmail.com',
    'keywords' : ['Redis', 'key-value store'],
    'license' : 'New BSD License',
    'packages' : ['desir'],
    'package_data' : {
        '': ['*.json'],
    },
    'classifiers' : [
       'Development Status :: 0.1 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: New BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python'],
}

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
    
setup(**sdict)

