all:
	@ echo
	@ echo "This is only a complementary Makefile, you should not need to use it." >&2
	@ echo "Use 'python setup.py' instead."
	@ echo
	@ echo "If you really want to use this Makefile, type 'make {build|test|build-only|clean|...}'."
	@ echo
	@ exit 1

build: protoc test build-only

protoc:
	python setup.py build # make protoc command available
	python setup.py protoc

build-only:
	python setup.py build
	python setup.py bdist_egg

test:
	python setup.py test

clean::
	find . \( -name \*~ -o -name \*.py\[oc\] \) -delete -printf 'removed %p\n'
	rm -vrf pyMX.egg-info

.PHONY: all build protoc test build-only clean
