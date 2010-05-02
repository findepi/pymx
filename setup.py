#!/usr/bin/env python

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup, find_packages

setup(
        name = "pyMX",
        version = "0.1dev",
        description = "Python client for Multiplexer",
        author = "Piotr Findeisen",
        author_email = "piotr.findeisen@gmail.com",
        packages = find_packages(),
        test_suite = "nose.collector",

        install_requires = [
            'protobuf >= 2.1.0'
            ],

        tests_require = [
            'nose >= 0.10'
            ],

        entry_points = {
            "distutils.commands": [
                    "protoc = pymx.setuputils.protoc:RunProtoc",
                ],
            },

        # This makes 'setup.py protoc' available after 'setup.py build' is run.
        # This is not required if you run 'setup.py bdist_egg', but the latter
        # is definitely not at all intuitive.
        include_package_data = True,
    )
