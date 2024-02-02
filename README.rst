journalpump |BuildStatus|_
==========================

.. |BuildStatus| image:: https://github.com/aiven/journalpump/actions/workflows/build.yml/badge.svg?branch=master
.. _BuildStatus: https://github.com/aiven/journalpump/actions
.. image:: https://codecov.io/gh/aiven/journalpump/branch/master/graph/badge.svg?token=nLr7M7hvCx
   :target: https://codecov.io/gh/aiven/journalpump

journalpump is a daemon that takes log messages from journald and pumps them
to a given output.  Currently supported outputs are Elasticsearch, Apache Kafka®,
logplex, rsyslog, websocket and AWS CloudWatch.  It reads messages from
journald and optionally checks if they match a config rule and forwards them
as JSON messages to the desired output.


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

For a source install the dependency `python-systemd <https://github.com/systemd/python-systemd>`_ has
to be installed through your distribution's package manager (The PyPI `systemd` package is not the
same!).

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
      "log_level": "INFO",
      "field_filters": {
         ...
      },
      "unit_log_levels": {
         ...
      },
      "json_state_file_path": "/var/lib/journalpump/journalpump_state.json",
      "readers": {
         ...
      },
      "statsd":   {
          "host": "127.0.0.1",
          "port": 12345,
          "prefix": "user-",
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

The ``prefix`` setting can be used to enter an optional prefix for all metric names.

Metrics sending follows the `Telegraf spec`_.

.. _`Telegraf spec`: https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

``log_level`` (default ``"INFO"``)

Determines log level of journalpump. `Available log levels <https://docs.python.org/3/library/logging.html#logging-levels>`_.

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

Unit log levels configuration
=============================

Unit log levels can be used to specify which log levels you want to set on a per unit basis. Matching supports glob
patterns. For example, to only process messsages for a systemd-unit called ``test-unit`` with severity ``WARNING`` or higher,
your config could look like this::

  {
      "unit_log_levels": {
          "log_level_name": [
              {
                  "service_glob": "test-unit*",
                  "log_level": "WARNING"
              },
              {
                  "service_glob": "*-unit",
                  "log_level": "INFO"
              }
          ]
      }
  }

Note that if your unit would match multiple patterns (like "test-unit" would in the example above), the first match will
get used, i.e "WARNING" in this case.

``log_level_name``

Name of the log level configuration. This can be configured per sender and depending
on the use case the settings for different senders may vary.

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
      "unit_log_levels": {
          "drop_everything_below_warning": [
              {
                  "service_glob": "*",
                  "log_level": "WARNING"
              }
          ]
      },
      "journal_path": "/var/lib/machines/container1/var/log/journal/b09ffd62229f4bd0829e883c6bb12c4e",
      "senders": {
          "k1": {
              "output_type": "kafka",
              "field_filter": "drop_process_id",
              "unit_log_level": "drop_everything_below_warning",
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
      "secret_filter_metrics": true,
      "secret_filters": [
        {
          "pattern": "SENSITIVE",
          "replacement": "[REDACTED]"
        }],
      "threshold_for_metric_emit": 10
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
Note this means if you specify ``match_key`` and not ``match_value``, then the reader
will match all entries that do not contain the ``match_key``.

``msg_buffer_max_length`` (default ``50000``)

How many journal entries to read at most into a memory buffer from
which the journalpump feeds the configured logsender.

``journal_path`` (default ``null``)

Path to the directory containing journal files if you want to override the
default one.

``journal_namespace`` (default ``null`` - read from default systemd namespace)

Journal namespace to read logs from.
This feature requires latest version of ``python-systemd`` `with namespace support <https://github.com/systemd/python-systemd/pull/87>`_

``units_to_match`` (default ``[]``)

Require that the logs message matches only against certain _SYSTEMD_UNITs.
If not set, we allow log events from all units.

``flags`` (default ``LOCAL_ONLY``)

``"LOCAL_ONLY"`` opens journal on local machine only; ``"RUNTIME_ONLY"`` opens only volatile journal files;
and ``"SYSTEM"`` opens journal files of system services and the kernel, ``"CURRENT_USER"`` opens files of the
current user; and ``"OS_ROOT"`` is used to open the journal from directories relative to the specified
directory path or file descriptor. Multiple flags can be OR'ed together using a list:
``["LOCAL_ONLY", "CURRENT_USER"]``.

``secret_filters`` (default ``[]``)

Secret filters can be used to redact sensitive data which matches known patterns in logs before forwarding the message along
to it's final destination. To use: add a number of filters following the pattern below to the reader config. The ``pattern`` is a standard
python regex, and the matching substring will be subbed with ``replacement``. Patterns are compiled at runtime.

Simple pattern example:

This simple pattern should be used for most cases. It will replace SECRET with [REDACTED] but will leave the rest of the message intact.

"secret_filters": [
  {
    "pattern": "SECRET",
    "replacement": "[REDACTED]"
  }
]

Complex pattern example:

For more complex requirements, a python regex with capture groups can be provided, and the contents of the message restructured using backrefs.
This example will only replace SENSITIVE with [REDACTED] as long as foo and bar are also part of the pattern.

"secret_filters": [
  {
    "pattern": "(bar)(SENSITIVE)(foo)",
    "replacement": "\\1[REDACTED]\\3",
  }
]

Using backrefs, the message can also be restructured into a new format.
"secret_filters": [
  {
    "pattern": "(bar)(SENSITIVE)(foo)",
    "replacement": "\\1\\3 pattern was [REDACTED]",
  }
]


``secret_filter_metrics`` ( default: ``false``)
Change this setting to true to emit metrics to the metrics host whenever a secret pattern is matched.
This matching happens before other filtering to help catch secrets being leaked to disk.

``threshold_for_metric_emit`` ( default: ``10``)
For the regex searches in journalpump, if search takes longer than this value, default 10 seconds, a metric will be emitted.
type: int unit: second

Sender Configuration
--------------------
``output_type`` (default ``null``)

Output to write journal events to.  Options are `elasticsearch`, `kafka`,
`file`, `websocket` and `logplex`.

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


Apache Kafka Sender Configuration
---------------------------------
``ca`` (default ``null``)

Apache Kafka Certificate Authority path, needed when you're using Kafka with SSL
authentication.

``certfile`` (default ``null``)

Apache Kafka client certificate path, needed when you're using Kafka with SSL
authentication.

``kafka_api_version`` (default ``0.9``)

Which Apache Kafka server API version to use.

``kafka_topic`` (default ``null``)

Which Kafka topic do you want the journalpump to write to.
Required when using output_type ``kafka``.

``kafka_topic_config`` (default ``null``)

If this key is present, its value must be another mapping with the default
configuration used to create the topic, if it does not exist yet.

The mapping must have these values::

  {
      "num_partitions": 3,
      "replication_factor": 3
  }


``kafka_address`` (default ``null``)

The address of the Kafka server which to write to.
Required when using output_type ``kafka``.

``kafka_msg_key`` (default ``null``)

The key to use when writing messages into Kafka. Can be used
for partition selection.

``keyfile`` (default ``null``)

Kafka client key path, needed when you're using Kafka with SSL
authentication.

``socks5_proxy`` (default ``null``)

Defined socks5 proxy to use for Kafka connections. This feature
is currently only supported in Aiven fork of kafka-python library.

AWS CloudWatch Logs Sender Configuration
----------------------------------------
``aws_cloudwatch_log_group``

The log group used in AWS CloudWatch.

``aws_cloudwatch_log_stream``

The log stream used in AWS CloudWatch.

``aws_region`` (default ``null``)

AWS region used.

``aws_access_key_id`` (default ``null``)

AWS access key id used.

``aws_secret_access_key`` (default ``null``)

AWS secret access key used.

The AWS credentials and region are optional. In case they are not included
credentials are configured automatically by the ``botocore`` module.

The AWS credentials that are used need the following permissions:
``logs:CreateLogGroup``, ``logs:CreateLogStream``, ``logs:PutLogEvents``
and ``logs:DescribeLogStreams``.

Google Cloud Logging Sender Configuration
-----------------------------------------
``google_cloud_logging_project_id``

The GCP project id to which logs will be sent.

``google_cloud_logging_log_id``

The log id to be used for this particular sender.

``google_cloud_logging_resource_labels``

A dictionary containing the labels added to the monitored resource.
Find the allowed labels from https://cloud.google.com/monitoring/api/resources#tag_generic_node.

``google_service_account_credentials``

The service account credentials to be used for this sender. If not
defined, the sender will try to find credentials from the system.

Rsyslog Sender Configuration
----------------------------

``rsyslog_server`` (default ``null``)

Address of the remote syslog server.

``rsyslog_port`` (default ``514``)

Port used by the remote syslog server.

``default_facility`` (default ``1``)

Facility for the syslog message if not provided by the entry being relayed.
(see RFC5424 for list of facilities.)

``default_severity`` (default ``6``)

Severity for the syslog message if not provided by the entry being relayed.
(see RFC5424 for list of priorities.)

``format`` (default ``rfc5424``)

Log format to use. Can be rfc3164, rfc5424 or custom.

``logline`` (default ``null``)

Custom logline format (ignored unless format is set to custom). The format is a limited version
of the formatting used by rsyslog. Supported tags are pri, procotol-version, timestamp,
timestamp:::date-rfc3339, HOSTNAME, app-name, procid, msgid, msg and structured-data.

For example the rfc3164 log format would be defined as `<%pri%>%timestamp% %HOSTNAME% %app-name%[%procid%]: %msg%`

``structured_data`` (default ``null``)

Content of structured data section (optional, required by some services to identify the sender).

``ssl`` (default ``false``)

Require encrypted connection.

``ca_certs`` (default ``null``)

CA path. Note! setting ca will automatically also set ssl to True

``client_cert`` (default ``null``)

Client certificate path, required if remote syslog requires SSL authentication.

``client_key`` (default ``null``)

Client key path, required if remote syslog requires SSL authentication.

``format`` (default ``rfc5424``)

Format message according to rfc5424 or rfc3164

Websocket Sender Configuration
------------------------------
``websocket_uri`` (default ``null``)

Which Websocket URI do you want the journalpump to write to.
Required when using output_type ``websocket``.

``ca`` (default ``null``)

Websocket Certificate Authority path, needed when you're using SSL
authentication.

``certfile`` (default ``null``)

Websocket client certificate path, needed when you're using SSL
authentication.

``keyfile`` (default ``null``)

Websocket client key path, needed when you're using SSL
authentication.

``socks5_proxy`` (default ``null``)

Defined socks5 proxy to use for Websocket connections.

``max_batch_size`` (default ``1048576``)

Adjust message batch size, set to 0 to disable batching.  When batching is
enabled, multiple journal messages are sent in a single websocket message,
separated by a single NUL byte.

``compression`` (default ``"snappy"``)

Compress messages on application level using the specified algorithm.
Decompression is done by an application behind the websocket server,
allowing end-to-end compression.  When batching is enabled, compression is
done on complete batches.  Supported values: ``"snappy"``, ``"none"``.

``websocket_compression`` (default ``"none"``)

Enable compression of websocket messages using the ``permessage-deflate``
extension.  The messages will be decompressed by the websocket server.  When
batching is enabled, compression is done on complete batches.  Supported
values: ``"deflate"``, ``"none"``.



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

Trademark
=========

Apache Kafka is either registered trademark or trademark of the Apache Software
Foundation in the United States and/or other countries. Elasticsearch,
AWS CloudWatch, logplex and rsyslog are trademarks and property of their respective
owners. All product and service names used in this website are for identification
purposes only and do not imply endorsement.


Contact
=======

Bug reports and patches are very welcome, please post them as GitHub issues
and pull requests at https://github.com/aiven/journalpump .  Any
possible vulnerabilities or other serious issues should be reported directly
to the maintainers <opensource@aiven.io>.
