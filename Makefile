# This Makefile is not required for the pyMX to be built, installed or
# packaged. It's intended as a collection of shortcuts/scripts/helpers only and
# for those that type 'make tea' without thinking instead of turning on a
# kettle.

all:
	@ echo
	@ echo "This is only a complementary Makefile, you should not need to use it." >&2
	@ echo "Use 'python setup.py' instead."
	@ echo
	@ echo "If you really want to use this Makefile, type 'make {build|test|build-only|clean|...}'."
	@ echo
	@ exit 1

build: protoc
	python setup.py build
	python setup.py bdist_egg

protoc:
	python setup.py build # make protoc command available
	python setup.py protoc

test: protoc
	python setup.py test

test-coverage:
	nosetests --with-coverage --cover-package=pymx

clean::
	find . \( -name \*~ -o -name \*.py\[oc\] \) -delete -printf 'removed %p\n'
	rm -vrf pyMX.egg-info

.PHONY: all build protoc test test-coverage clean
