kafkajournalpump |BuildStatus|_
========================

.. |BuildStatus| image:: https://travis-ci.org/aiven/kafkajournalpump.png?branch=master
.. _BuildStatus: https://travis-ci.org/aiven/kafkajournalpump

kafkajournalpump is a journald to Kafka msg pump. It reads messages from journald
and optionally checks if they match a config rule and forwards them as JSON messages
to Kafka to the configured topic.

Building
========

To build an installation package for your distribution, go to the root
directory of a kafkajournalpump Git checkout and then run:

Debian::

  make deb

This will produce a .deb package into the parent directory of the Git checkout.

Fedora::

  make rpm

This will produce an RPM in rpm/RPMS/noarch/.

Python/Other::

  python setup.py bdist_egg

This will produce an egg file into a dist directory within the same folder.

Installation
============

To install it run as root:

Debian::

  dpkg -i ../kafkajournalpump*.deb

Fedora::

  su -c 'dnf install rpm/RPMS/noarch/*'

On Fedora it is recommended to simply run kafkajournalpump under systemd::

  systemctl enable kafkajournalpump.service

and eventually after the setup section, you can just run::

  systemctl start kafkajournalpump.service

Python/Other::

  easy_install dist/kafkajournalpump-1.0.0-py2.7.egg

On Debian/Other systems it is recommended that you run kafkajournalpump within
a supervisord (http://supervisord.org) Process control system.


Setup
=====

After this you need to create a suitable JSON configuration file for your
installation.


General notes
=============

If correctly installed, kafkajournalpump comes with a single executable,
``kafkajournalpump`` that takes as an argument the path to kafkajournalpump's
 JSON configuration file.

``kafkajournalpump`` is the main process that should be run under systemd or
supervisord.

While kafkajournalpump is running it may be useful to read the JSON state
file that will be created  as ``kafkajournalpump_state.json`` to the current working
directory.. The JSON state file is human readable and should give an understandable
description of the current state of the kafkajournalpump.


Configuration keys
==================

``ca`` (default ``N/A``)

Kafka Certificate Authority path, needed when you're using Kafka with SSL
authentication.

``certfile`` (default ``N/A``)

Kafka client certificate path, needed when you're using Kafka with SSL
authentication.

``kafka_topic`` (default ``N/A``)

Which Kafka topic do you want the kafkajournalpump to write to.

``kafka_address`` (default ``N/A``)

The address of the kafka server which to write to.

``keyfile`` (default ``N/A``)

Kafka client key path, needed when you're using Kafka with SSL
authentication.

``match_key`` (default ``N/A``)

If you want to match against a single journald field, this configuration key
defines the key to match against.

``match_value`` (default ``N/A``)

If you want to match against a single journald field, this configuration key
defines the value to match against. Currently only equality is allowed.

``journal_path`` (default ``N/A``)

Path to the directory containing journal files if you want to override the
default one.

``json_state_file_path`` (default ``"kafkajournalpump_state.json"``)

Location of a JSON state file which describes the state of the
kafkajournalpump process.

``units_to_match`` (default ``[]``)

Require that the logs message matches only against certain
_SYSTEMD_UNITs. If not set, we allow log events from all units.

``log_level`` (default ``"INFO"``)

Determines log level of kafkajournalpump.

``statsd`` (default: disabled)

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

Metrics sendindg follows the Telegraf spec: https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

License
=======

kafkajournalpump is licensed under the Apache License, Version 2.0. Full license
text is available in the ``LICENSE`` file and at http://www.apache.org/licenses/LICENSE-2.0.txt


Credits
=======

kafkajournalpump was created by Hannu Valtonen <hannu.valtonen@aiven.io>
and is now maintained by Aiven hackers <opensource@aiven.io>.

Recent contributors are listed on the GitHub project page,
https://github.com/aiven/kafkajournalpump/graphs/contributors


Contact
=======

Bug reports and patches are very welcome, please post them as GitHub issues
and pull requests at https://github.com/aiven/kafkajournalpump .  Any
possible vulnerabilities or other serious issues should be reported directly
to the maintainers <opensource@aiven.io>.
