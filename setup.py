#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from pip.req import parse_requirements
from setuptools import setup, find_packages

del os.link

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [str(ir.req) for ir in
                parse_requirements('requirements.txt', session=False)]

test_requirements = [str(ir.req) for ir in
                     parse_requirements('requirements_dev.txt', session=False)]


setup(
    name='proxysql_tools',
    version='0.3.4',
    description="ProxySQL Tools",
    long_description=readme + '\n\n' + history,
    author="TwinDB Development Team",
    author_email='dev@twindb.com',
    url='https://github.com/twindb/proxysql-tools',
    packages=find_packages(exclude=('tests*',)),
    package_dir={'proxysql_tools':
                 'proxysql_tools'},
    entry_points={
        'console_scripts': [
            'proxysql-tool=proxysql_tools.cli:main',
            'proxysql_tools=proxysql_tools.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='proxysql_tools',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
