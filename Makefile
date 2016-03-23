short_ver = 1.0.1
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)

all: py-egg

PYTHON ?= python3
PYLINT_DIRS = journalpump/

test: pylint unittest

unittest:
	$(PYTHON) -m pytest -vv test/

pylint:
	$(PYTHON) -m pylint.lint --rcfile .pylintrc $(PYLINT_DIRS)

coverage:
	$(PYTHON) -m pytest $(PYTEST_ARG) --cov-report term-missing --cov journalpump test/

clean:
	$(RM) -r *.egg-info/ build/ dist/
	$(RM) ../journalpump_* test-*.xml

deb:
	cp debian/changelog.in debian/changelog
	dch -v $(long_ver) "Automatically built package"
	dpkg-buildpackage -A -uc -us

rpm:
	git archive --output=journalpump-rpm-src.tar.gz --prefix=journalpump/ HEAD
	rpmbuild -bb journalpump.spec \
		--define '_sourcedir $(shell pwd)' \
                --define '_topdir $(shell pwd)/rpm' \
		--define 'major_version $(shell git describe --tags --abbrev=0 | cut -f1-)' \
		--define 'minor_version $(subst -,.,$(shell git describe --tags --long | cut -f2- -d-))'
	$(RM) journalpump-rpm-src.tar.gz

build-dep-fed:
	sudo dnf -y install \
		python-kafka python3-kafka pytest python3-pytest \
		pylint python3-pylint \
		systemd-python systemd-python3

build-dep-deb:
	sudo apt-get install \
		build-essential devscripts dh-systemd \
		python-all python-setuptools python3-systemd python3-kafka

pep8:
	$(PYTHON) -m pep8 --ignore=E501,E123 $(PYLINT_DIRS)

.PHONY: rpm
