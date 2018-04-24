journalpump |BuildStatus|_
========================

.. |BuildStatus| image:: https://travis-ci.org/aiven/journalpump.png?branch=master
.. _BuildStatus: https://travis-ci.org/aiven/journalpump

journalpump is a daemon that takes log messages from journald and pumps them
to a given output.  Currently supported outputs are Elasticsearch, Kafka and
logplex.  It reads messages from journald and optionally checks if they
match a config rule and forwards them as JSON messages to the desired
output.


Building
========

To build an installation package for your distribution, go to the root
directory of a journalpump Git checkout and then run:

Debian::

  make deb

This will produce a .deb package into the parent directory of the Git
checkout.

Fedora::

  make rpm

This will produce an RPM in rpm/RPMS/noarch/.

Other::

  python3 setup.py bdist_egg

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

Other::

  python3 setup.py install

On systems without systemd it is recommended that you run journalpump within
supervisord_ or similar process control system.

.. _supervisord : http://supervisord.org


Setup
=====

After installation you need to create a suitable JSON configuration file for
your installation.


General notes
=============

If correctly installed, journalpump comes with a single executable,
``journalpump`` that takes as an argument the path to journalpump's JSON
configuration file.

``journalpump`` is the main process that should be run under systemd or
supervisord.

While journalpump is running it may be useful to read the JSON state file
that will be created as ``journalpump_state.json`` to the current working
directory.  The JSON state file is human readable and should give an
understandable description of the current state of the journalpump.


Top level configuration
=======================
Example::

  {
      "field_filters": {
         ...
      },
      "json_state_file_path": "/var/lib/journalpump/journalpump_state.json",
      "readers": {
         ...
      },
      "statsd":   {
          "host": "127.0.0.1",
          "port": 12345,
          "tags": {
              "sometag": "somevalue"
          }
      }
  }


``json_state_file_path`` (default ``"journalpump_state.json"``)

Location of a JSON state file which describes the state of the
journalpump process.

``statsd`` (default ``null``)

Enables metrics sending to a statsd daemon that supports the influxdb-statsd
/ telegraf syntax with tags.

The ``tags`` setting can be used to enter optional tag values for the metrics.

Metrics sending follows the `Telegraf spec`_.

.. _`Telegraf spec`: https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

``log_level`` (default ``"INFO"``)

Determines log level of journalpump.

Field filter configuration
==========================

Field filters can be used to restrict the journald fields that journalpump sends forward.
Field filter configuration structure::

  {
      "field_filters": {
          "filter_name": {
              "type": "whitelist|blacklist",
              "fields": ["field1", "field2"]
          }
      }
  }

``filter_name``

Name of the filter. The filters can be configured per sender and depending
on the use case the filters for different senders may vary.

``type`` (default ``whitelist``)

Specifies whether the listed fields will be included (``whitelist``) or
excluded (``blacklist``).

``fields``

The actual fields to include or exclude. Field name matching is case
insensitive and underscores in the beginning of the fields are trimmed.

Reader configuration
====================
Reader configuration structure::

  {
      "readers": {
          "some_reader": {
              "senders": {
                  "some_log": {
                      ...
                  },
                  "another_log": {
                      ...
                  }
              }
          },
          "another_reader": {
              "senders": {
                  "some_kafka": {
                      ...
                  }
              }
          }
      }
  }

Example configuration for a single reader::

  {
      "field_filters": {
          "drop_process_id": {
              "fields": ["process_id"],
              "type": "blacklist"
          }
      },
      "journal_path": "/var/lib/machines/container1/var/log/journal/b09ffd62229f4bd0829e883c6bb12c4e",
      "senders": {
          "k1": {
              "output_type": "kafka",
              "field_filter": "drop_process_id",
              "ca": "/etc/journalpump/ca-bundle.crt",
              "certfile": "/etc/journalpump/node.crt",
              "kafka_address": "kafka.somewhere.com:12345",
              "kafka_topic": "journals",
              "keyfile": "/etc/journalpump/node.key",
              "ssl": true
          },
      },
      "searches": [
          {
              "fields": {
                  "MESSAGE": "kernel: Out of memory: Kill process .+ \\((?P<process>[^ ]+)\\)"
              },
              "name": "journal.oom_killer"
          }
      ],
      "tags": {
          "type": "container"
      }
  }


``initial_position`` (default ``head``)

Controls where the readers starts when the journalpump is launched for the first time:

* ``head``: First entry in the journal
* ``tail``: Last entry in the journal
* ``<integer>``: Seconds from current boot session

``match_key`` (default ``null``)

If you want to match against a single journald field, this configuration key
defines the key to match against.

``match_value`` (default ``null``)

If you want to match against a single journald field, this configuration key
defines the value to match against.  Currently only equality is allowed.

``msg_buffer_max_length`` (default ``50000``)

How many journal entries to read at most into a memory buffer from
which the journalpump feeds the configured logsender.

``journal_path`` (default ``null``)

Path to the directory containing journal files if you want to override the
default one.

``units_to_match`` (default ``[]``)

Require that the logs message matches only against certain _SYSTEMD_UNITs.
If not set, we allow log events from all units.

``flags`` (default ``LOCAL_ONLY``)

``"LOCAL_ONLY"`` opens journal on local machine only; ``"RUNTIME_ONLY"`` opens only volatile journal files;
and ``"SYSTEM"`` opens journal files of system services and the kernel, ``"CURRENT_USER"`` opens files of the
current user; and ``"OS_ROOT"`` is used to open the journal from directories relative to the specified
directory path or file descriptor. Multiple flags can be OR'ed together using a list:
``["LOCAL_ONLY", "CURRENT_USER"]``.


Sender Configuration
--------------------
``output_type`` (default ``null``)

Output to write journal events to.  Options are `elasticsearch`, `kafka`,
`file` and `logplex`.

``field_filter`` (default ``null``)

Name of the field filter to apply for this sender, if any.


File Sender Configuration
-------------------------
Writes journal entries as JSON to a text file, one entry per line.

``file_output`` sets the path to the output file.


Elasticsearch Sender Configuration
----------------------------------
``ca`` (default ``null``)

Elasticsearch Certificate Authority path, needed when you're using Elasticsearch
with self-signed certificates.

``elasticsearch_index_days_max`` (default ``3``)

Maximum number of days of logs to keep in Elasticsearch.  Relevant when
using output_type ``elasticsearch``.

``elasticsearch_index_prefix`` (default ``journalpump``)

Elasticsearch index name to use when Maximum number of days of logs to keep
in Elasticsearch.  Relevant when using output_type ``elasticsearch``.

``elasticsearch_timeout`` (default ``10.0``)

Elasticsearch request timeout limit.  The default should work for most
people but you might need to increase it in case you have a large latency to
server or the server is very congested.  Required when using output_type
``elasticsearch``.

``elasticsearch_url`` (default ``null``)

Fully qualified elasticsearch url of the form
``https://username:password@hostname.com:port``.
Required when using output_type ``elasticsearch``.


Kafka Sender Configuration
--------------------------
``ca`` (default ``null``)

Kafka Certificate Authority path, needed when you're using Kafka with SSL
authentication.

``certfile`` (default ``null``)

Kafka client certificate path, needed when you're using Kafka with SSL
authentication.

``kafka_api_version`` (default ``0.9``)

Which Kafka server API version to use.

``kafka_topic`` (default ``null``)

Which Kafka topic do you want the journalpump to write to.
Required when using output_type ``kafka``.

``kafka_address`` (default ``null``)

The address of the kafka server which to write to.
Required when using output_type ``kafka``.

``keyfile`` (default ``null``)

Kafka client key path, needed when you're using Kafka with SSL
authentication.



License
=======

journalpump is licensed under the Apache License, Version 2.0.
Full license text is available in the ``LICENSE`` file and at
http://www.apache.org/licenses/LICENSE-2.0.txt


Credits
=======

journalpump was created by Hannu Valtonen <hannu.valtonen@aiven.io>
and is now maintained by Aiven hackers <opensource@aiven.io>.

Recent contributors are listed on the project's GitHub `contributors page`_.

.. _`contributors page`: https://github.com/aiven/journalpump/graphs/contributors


Contact
=======

Bug reports and patches are very welcome, please post them as GitHub issues
and pull requests at https://github.com/aiven/journalpump .  Any
possible vulnerabilities or other serious issues should be reported directly
to the maintainers <opensource@aiven.io>.
