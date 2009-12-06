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
    )
