Name:           kafkajournalpump
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/aiven/kafkajournalpump
Summary:        Pump messages from systemd journal to Kafka or Logplex
License:        ASL 2.0
Source0:        kafkajournalpump-rpm-src.tar.gz
Requires:       python3-kafka, systemd-python3, python3-requests
BuildRequires:  %{requires}
BuildRequires:  python3-devel, python3-pytest, python3-pylint
BuildArch:      noarch

%description
kafkajournalpump is a daemon to pump journald messages into a given kafka topic.


%prep
%setup -q -n kafkajournalpump


%build
# Nothing to do, shut up rpmlint


%install
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
%{__install} -Dm0644 kafkajournalpump.unit %{buildroot}%{_unitdir}/kafkajournalpump.service
%{__mkdir_p} %{buildroot}%{_localstatedir}/lib/kafkajournalpump


%check
make test


%files
%defattr(-,root,root,-)
%doc LICENSE README.rst kafkajournalpump.json
%{_bindir}/kafkajournalpump*
%{_unitdir}/kafkajournalpump.service
%{_localstatedir}/lib/kafkajournalpump
%{python3_sitelib}/*


%changelog
* Tue Feb 16 2016 Oskari Saarenmaa <os@aiven.io> - 1.0.1
- We're Python 3 only now

* Mon Aug 03 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 1.0.1
- bugfix release

* Mon Jul 27 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 1.0.0
- Initial RPM package spec
