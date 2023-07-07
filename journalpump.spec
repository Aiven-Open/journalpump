Name:           journalpump
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/aiven/journalpump
Summary:        Pump messages from systemd journal to Elasticsearch, Kafka, Logplex or AWS CloudWatch
License:        ASL 2.0
Source0:        journalpump-rpm-src.tar.gz
Requires:       python3-kafka, systemd-python3, python3-requests, python3-botocore, python3-google-api-client
Requires:       python3-google-auth, python3-geoip2, python3-typing-extensions
Requires:	python3-websockets, python3-aiohttp-socks, python3-snappy
BuildRequires:  python3-kafka, systemd-python3, python3-requests, python3-botocore, python3-google-api-client
BuildRequires:  python3-devel, python3-pytest, python3-pylint python3-responses
BuildRequires:	python3-websockets, python3-aiohttp-socks, python3-snappy
BuildArch:      noarch
Obsoletes:      kafkajournalpump

# The dependency generator uses names which are not compatible with
# Fedora Packaging naming guidelines
%{?python_disable_dependency_generator}

%description
journalpump is a daemon that takes log messages from journald and pumps them
to a given output. Currently supported outputs are Elasticsearch, Kafka, Websocket,
logplex, AWS CloudWatch and Google Cloud Logging.  It reads messages from journald
and optionally checks if they match a config rule and forwards them as JSON
messages to the desired output.


%prep
%setup -q -n journalpump


%build
# Nothing to do, shut up rpmlint


%install
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
%{__install} -Dm0644 journalpump.unit %{buildroot}%{_unitdir}/journalpump.service
%{__mkdir_p} %{buildroot}%{_localstatedir}/lib/journalpump


%check
make test


%files
%defattr(-,root,root,-)
%doc LICENSE README.rst journalpump.json
%{_bindir}/journalpump*
%{_unitdir}/journalpump.service
%{_localstatedir}/lib/journalpump
%{python3_sitelib}/*


%changelog
* Fri Mar 25 2016 Oskari Saarenmaa <os@aiven.io> - 1.0.1
- Renamed to journalpump
- We're Python 3 only now

* Mon Jul 27 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 1.0.0
- Initial RPM package spec
