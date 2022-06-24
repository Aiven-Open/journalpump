from journalpump.senders.elasticsearch_opensearch_sender import (
    Config,
    ElasticsearchSender,
    OpenSearchSender,
    SenderType,
    Version,
)
from typing import Any, Dict
from unittest import mock

import logging
import pytest

logging.getLogger("elasticsearch_opensearch_sender").setLevel(logging.DEBUG)

_JSON_MESSAGE = {
    "field1": 42,
    "field2": True,
    "field3": 42.0,
    "field4": "aaa bbb ccc",
    "field5": None,
    "field6": {
        "a": 1,
        "b": 2.0,
        "c": True,
        "d": "dddd",
        "e": None,
    },
    "field7": [1, 2],
}


@pytest.mark.parametrize(
    "sender_type",
    [
        SenderType.elasticsearch,
        SenderType.opensearch,
    ],
)
def test_config(sender_type: SenderType) -> None:
    # pylint:disable=protected-access
    default_config = Config.create(
        sender_type=sender_type,
        config={f"{sender_type.value}_url": "http://aaa"},
    )
    assert default_config.sender_type == sender_type
    assert default_config.session_url == "http://aaa"
    assert default_config.request_timeout == Config._DEFAULT_REQUEST_TIMEOUT
    assert default_config.index_lifetime_in_days == Config._DEFAULT_INDEX_LIFETIME_IN_DAYS
    assert default_config.index_name == Config._DEFAULT_INDEX_PREFIX

    custom_config = Config.create(
        sender_type=sender_type,
        config={
            f"{sender_type.value}_url": "http://bbb/",
            f"{sender_type.value}_index_prefix": "some_idx_prefix",
            f"{sender_type.value}_timeout": 1000,
            f"{sender_type.value}_index_days_max": 42,
        },
    )
    assert custom_config.sender_type == sender_type
    assert custom_config.session_url == "http://bbb"
    assert custom_config.request_timeout == 1000
    assert custom_config.index_lifetime_in_days == 42
    assert custom_config.index_name == "some_idx_prefix"


@pytest.mark.parametrize(
    "sender_type",
    [
        SenderType.elasticsearch,
        SenderType.opensearch,
    ],
)
def test_request_url(sender_type: SenderType) -> None:
    config = Config.create(
        sender_type=sender_type,
        config={
            f"{sender_type.value}_url": "http://aaa",
        },
    )

    assert config.request_url("b/c/d") == "http://aaa/b/c/d"
    assert config.request_url("b/c?d") == "http://aaa/b/c?d"


@pytest.mark.parametrize(
    "sender_type",
    [
        SenderType.elasticsearch,
        SenderType.opensearch,
    ],
)
def test_config_raises_value_error_for_empty_url(sender_type: SenderType) -> None:
    with pytest.raises(ValueError, match=f"{sender_type.value}_url hasn't been defined"):
        Config.create(sender_type=sender_type, config={})


@pytest.mark.parametrize(
    "clazz, sender_type",
    [
        (OpenSearchSender, SenderType.opensearch),
        (ElasticsearchSender, SenderType.elasticsearch),
    ],
)
def test_sender_set_max_send_interval_config(clazz: type, sender_type: SenderType) -> None:
    # pylint:disable=protected-access
    config: Dict[str, Any] = {f"{sender_type.value}_url": "http://aaa"}
    default_sender = clazz(
        config=config,
        name=sender_type.value,
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )
    assert default_sender.max_send_interval == default_sender._DEFAULT_MAX_SENDER_INTERVAL

    config.update(
        {
            "max_send_interval": 42,
        }
    )
    custom_sender = clazz(
        config=config,
        name=sender_type.value,
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )
    assert custom_sender.max_send_interval == 42


@pytest.mark.parametrize(
    "clazz, sender_type, version",
    [
        (OpenSearchSender, SenderType.opensearch, Version(major=1, minor=2, patch=4)),
        (
            ElasticsearchSender,
            SenderType.elasticsearch,
            Version(major=8, minor=0, patch=0),
        ),
    ],
)
def test_index_message_header_without_type(clazz: type, sender_type: SenderType, version: Version) -> None:
    # pylint:disable=protected-access
    sender = clazz(
        config={f"{sender_type.value}_url": "http://aaa"},
        name=sender_type.value,
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )
    sender._version = version
    header = sender._message_header("some_index")
    assert {
        "index": {
            "_index": "some_index",
        }
    } == header


@pytest.mark.parametrize(
    "clazz, sender_type, version",
    [
        (OpenSearchSender, SenderType.opensearch, Version(major=1, minor=2, patch=4)),
        (
            ElasticsearchSender,
            SenderType.elasticsearch,
            Version(major=8, minor=0, patch=0),
        ),
    ],
)
def test_index_mapping_without_type(clazz: type, sender_type: SenderType, version: Version) -> None:
    # pylint:disable=protected-access
    sender = clazz(
        config={f"{sender_type.value}_url": "http://aaa"},
        name=sender_type.value,
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )
    sender._version = version
    mapping = sender._create_mapping(_JSON_MESSAGE)
    assert {
        "mappings": {
            "properties": {
                "SYSTEMD_SESSION": {"type": "text"},
                "SESSION_ID": {"type": "text"},
                "field1": {"type": "integer"},
                "field2": {"type": "boolean"},
                "field3": {"type": "float"},
                "field4": {"type": "text"},
                "field6": {
                    "properties": {
                        "a": {"type": "integer"},
                        "b": {"type": "float"},
                        "c": {"type": "boolean"},
                        "d": {"type": "text"},
                    },
                },
            },
        },
    } == mapping


def test_elasticsearch_message_header_with_type() -> None:
    # pylint:disable=protected-access
    sender = ElasticsearchSender(
        config={"elasticsearch_url": "http://aaa"},
        name="Additional mapping fields",
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )
    sender._version = Version(major=7, minor=10, patch=2)
    header = sender._message_header("some_index")
    assert {
        "index": {
            "_index": "some_index",
            "_type": ElasticsearchSender._LEGACY_TYPE,
        }
    } == header


def test_index_url_for_elasticsearch() -> None:
    # pylint:disable=protected-access
    sender = ElasticsearchSender(
        config={"elasticsearch_url": "http://aaa"},
        name="Additional mapping fields",
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )
    sender._version = Version(major=7, minor=10, patch=2)
    assert sender._index_url("some_index") == "http://aaa/some_index?include_type_name=true"
    sender._version = Version(major=1, minor=3, patch=3)  # emulate OS
    assert sender._index_url("some_index") == "http://aaa/some_index?include_type_name=true"
    sender._version = Version(major=8, minor=0, patch=0)  # emulate OS
    assert sender._index_url("some_index") == "http://aaa/some_index"


def test_elasticsearch_index_mapping_with_type() -> None:
    # pylint:disable=protected-access
    sender = ElasticsearchSender(
        config={"elasticsearch_url": "http://aaa"},
        name="Additional mapping fields",
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )
    sender._version = Version(major=7, minor=10, patch=2)
    mapping = sender._create_mapping(_JSON_MESSAGE)
    assert {
        "mappings": {
            "journal_msg": {
                "properties": {
                    "SYSTEMD_SESSION": {"type": "text"},
                    "SESSION_ID": {"type": "text"},
                    "field1": {"type": "integer"},
                    "field2": {"type": "boolean"},
                    "field3": {"type": "float"},
                    "field4": {"type": "text"},
                    "field6": {
                        "properties": {
                            "a": {"type": "integer"},
                            "b": {"type": "float"},
                            "c": {"type": "boolean"},
                            "d": {"type": "text"},
                        },
                    },
                },
            },
        },
    } == mapping
