#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pip.req import parse_requirements
from setuptools import setup

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
    version='0.2.2',
    description="ProxySQL Tools",
    long_description=readme + '\n\n' + history,
    author="TwinDB Development Team",
    author_email='dev@twindb.com',
    url='https://github.com/twindb/proxysql_tools',
    packages=[
        'proxysql_tools',
    ],
    package_dir={'proxysql_tools':
                 'proxysql_tools'},
    entry_points={
        'console_scripts': [
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
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
