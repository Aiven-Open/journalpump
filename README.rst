journalpump |BuildStatus|_
========================

.. |BuildStatus| image:: https://travis-ci.org/aiven/journalpump.png?branch=master
.. _BuildStatus: https://travis-ci.org/aiven/journalpump

journalpump is a daemon that takes log messages from journald and
pumps them to a given output. Currently supported outputs are Kafka
and logplex. It reads messages from journald and optionally checks if
they match a config rule and forwards them as JSON messages
to the desired output.

Building
========

To build an installation package for your distribution, go to the root
directory of a journalpump Git checkout and then run:

Debian::

  make deb

This will produce a .deb package into the parent directory of the Git checkout.

Fedora::

  make rpm

This will produce an RPM in rpm/RPMS/noarch/.

Python/Other::

  python setup.py bdist_egg

This will produce an egg file into a dist directory within the same folder.

journalpump requires Python 3.4 or newer.

Installation
============

To install it run as root:

Debian::

  dpkg -i ../journalpump*.deb

Fedora::

  su -c 'dnf install rpm/RPMS/noarch/*'

On Fedora it is recommended to simply run journalpump under systemd::

  systemctl enable journalpump.service

and eventually after the setup section, you can just run::

  systemctl start journalpump.service

Python/Other::

  easy_install dist/journalpump-1.0.1-py3.4.egg

On Debian/Other systems it is recommended that you run journalpump within
a supervisord (http://supervisord.org) Process control system.


Setup
=====

After this you need to create a suitable JSON configuration file for your
installation.


General notes
=============

If correctly installed, journalpump comes with a single executable,
``journalpump`` that takes as an argument the path to journalpump's
JSON configuration file.

``journalpump`` is the main process that should be run under systemd or
supervisord.

While journalpump is running it may be useful to read the JSON state
file that will be created  as ``journalpump_state.json`` to the current working
directory.. The JSON state file is human readable and should give an understandable
description of the current state of the journalpump.


Configuration keys
==================

``ca`` (default ``null``)

Kafka Certificate Authority path, needed when you're using Kafka with SSL
authentication.

``certfile`` (default ``null``)

Kafka client certificate path, needed when you're using Kafka with SSL
authentication.

``kafka_topic`` (default ``null``)

Which Kafka topic do you want the journalpump to write to.

``kafka_address`` (default ``null``)

The address of the kafka server which to write to.

``keyfile`` (default ``null``)

Kafka client key path, needed when you're using Kafka with SSL
authentication.

``match_key`` (default ``null``)

If you want to match against a single journald field, this configuration key
defines the key to match against.

``match_value`` (default ``null``)

If you want to match against a single journald field, this configuration key
defines the value to match against. Currently only equality is allowed.

``journal_path`` (default ``null``)

Path to the directory containing journal files if you want to override the
default one.

``json_state_file_path`` (default ``"journalpump_state.json"``)

Location of a JSON state file which describes the state of the
journalpump process.

``units_to_match`` (default ``[]``)

Require that the logs message matches only against certain
_SYSTEMD_UNITs. If not set, we allow log events from all units.

``log_level`` (default ``"INFO"``)

Determines log level of journalpump.

``statsd`` (default ``null``)

Enables metrics sending to a statsd daemon that supports the influxdb-statsd/telegraf
syntax with tags.

The value is a JSON object::

  {
      "host": "<statsd address>",
      "port": "<statsd port>",
      "tags": {
          "<tag>": "<value>"
      }
  }

The ``tags`` setting can be used to enter optional tag values for the metrics.

Metrics sending follows the Telegraf spec: https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

License
=======

journalpump is licensed under the Apache License, Version 2.0. Full license
text is available in the ``LICENSE`` file and at http://www.apache.org/licenses/LICENSE-2.0.txt


Credits
=======

journalpump was created by Hannu Valtonen <hannu.valtonen@aiven.io>
and is now maintained by Aiven hackers <opensource@aiven.io>.

Recent contributors are listed on the GitHub project page,
https://github.com/aiven/journalpump/graphs/contributors


Contact
=======

Bug reports and patches are very welcome, please post them as GitHub issues
and pull requests at https://github.com/aiven/journalpump .  Any
possible vulnerabilities or other serious issues should be reported directly
to the maintainers <opensource@aiven.io>.
