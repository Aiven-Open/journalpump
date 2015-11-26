%if %{?python3_sitelib:1}0
%global use_python3 1
%else
%global use_python3 0
%endif

Name:           kafkajournalpump
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/ohmu/kafkajournalpump
Summary:        Pump messages from systemd journal to Kafka
License:        ASL 2.0
Source0:        kafkajournalpump-rpm-src.tar.gz
%if %{use_python3}
Requires:       python3-kafka, systemd-python3
BuildRequires:  python3-pytest, python3-pylint
%else
Requires:       python-kafka, systemd-python
BuildRequires:  pytest, pylint
%endif
BuildRequires:  %{requires}
BuildArch:      noarch

%description
kafkajournalpump is a daemon to pump journald messages into a given kafka topic.


%prep
%setup -q -n kafkajournalpump


%install
%if %{use_python3}
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
%else
python2 setup.py install --prefix=%{_prefix} --root=%{buildroot}
%endif
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
%{__install} -Dm0644 kafkajournalpump.unit %{buildroot}%{_unitdir}/kafkajournalpump.service
%{__mkdir_p} %{buildroot}%{_localstatedir}/lib/kafkajournalpump


%check
%if %{use_python3}
make test PYTHON=python3
%else
make test PYTHON=python2
%endif


%files
%defattr(-,root,root,-)
%doc LICENSE README.rst kafkajournalpump.json
%{_bindir}/kafkajournalpump*
%{_unitdir}/kafkajournalpump.service
%{_localstatedir}/lib/kafkajournalpump
%if %{use_python3}
%{python3_sitelib}/*
%else
%{python_sitelib}/*
%endif


%changelog
* Mon Aug 03 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 1.0.1
- bugfix release

* Mon Jul 27 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 1.0.0
- Initial RPM package spec
