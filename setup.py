#!/usr/bin/env python

from ez_setup import use_setuptools
use_setuptools(version='0.6a0')
from setuptools import setup, find_packages

setup(
        name="pyMX",
        version="0.1dev",
        description="Python client for Multiplexer",
        author="Piotr Findeisen",
        author_email="piotr.findeisen@gmail.com",
        packages=find_packages(),
        test_suite = "nose.collector",

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
