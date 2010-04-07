import os
import sys
import subprocess

from distutils.spawn import find_executable
from setuptools import Command


class RunProtoc(Command):

    description = "compile all *.proto files into *_pb2.py using protoc"

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        self._protoc = find_executable("protoc")

    def run(self):

        for (dirpath, _, filenames) in os.walk('.'):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if filepath.endswith('.proto'):
                    self.generate_proto(filepath)


    def generate_proto(self, source):
        # generate_proto is borrowed from Google Protocol Buffers setup.py
        # with my further modifications
        """Invokes the Protocol Compiler to generate a _pb2.py from the given
        .proto file.  Does nothing if the output already exists and is newer
        than the input."""

        output = source.replace(".proto", "_pb2.py")

        if not os.path.exists(source):
            raise IOError("Can't find required file: " + source)

        if (not os.path.exists(output) or
                os.path.getmtime(source) > os.path.getmtime(output)):
            print "Generating %s..." % output

        if self._protoc is None:
            raise RuntimeError("protoc is not installed.")

        protoc_command = [self._protoc, "-I.", "--python_out=.", source]
        if subprocess.call(protoc_command) != 0:
            raise RuntimeError("Execution of %r failed")
