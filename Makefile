SHELL=/bin/bash
short_ver = 2.1.3
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)

all: py-egg

PYTHON ?= python3
PYLINT_DIRS = journalpump/ test/ systest/

.PHONY: unittest
unittest:
	$(PYTHON) -m pytest -vv test/

.PHONY: pylint
pylint:
	$(PYTHON) -m pylint --rcfile .pylintrc $(PYLINT_DIRS) || exit $$(($$?&2))  # only fail on errors

.PHONY: systest
systest:
	$(PYTHON) -m pytest -vv systest/

py-egg:
	VERSION=$(shell git describe --tags) $(PYTHON) setup.py bdist_egg

.PHONY: coverage
coverage:
	$(PYTHON) -m pytest $(PYTEST_ARG) --cov-report term-missing --cov-report xml:coverage.xml \
		--cov journalpump test/

clean:
	$(RM) -r *.egg-info/ build/ dist/
	$(RM) ../journalpump_* test-*.xml coverage.xml

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
	sudo dnf -y --best --allowerasing install \
		python3-flake8 python3-kafka python3-pytest python3-pylint \
		python3-requests python3-responses systemd-python3 python3-botocore \
		python3-websockets python3-aiohttp-socks python3-snappy \
		python3-google-api-client python3-google-auth

build-dep-deb:
	sudo apt-get install \
		build-essential devscripts dh-systemd \
		python-all python-setuptools python3-systemd python3-kafka \
		python3-websockets python3-aiohttp-socks python3-snappy \
		python3-botocore python3-googleapi python3-google-auth


.PHONY: rpm
