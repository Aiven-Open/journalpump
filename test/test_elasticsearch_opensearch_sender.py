from journalpump.senders.elasticsearch_opensearch_sender import Config, ElasticsearchSender, OpenSearchSender, SenderType
from typing import Any, Dict
from unittest import mock

import pytest


@pytest.mark.parametrize("sender_type", [
    SenderType.elasticsearch,
    SenderType.opensearch,
])
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


@pytest.mark.parametrize("sender_type", [
    SenderType.elasticsearch,
    SenderType.opensearch,
])
def test_request_url(sender_type: SenderType) -> None:
    config = Config.create(
        sender_type=sender_type,
        config={
            f"{sender_type.value}_url": "http://aaa",
        },
    )

    assert config.request_url("b/c/d") == "http://aaa/b/c/d"
    assert config.request_url("b/c?d") == "http://aaa/b/c?d"


@pytest.mark.parametrize("sender_type", [
    SenderType.elasticsearch,
    SenderType.opensearch,
])
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

    config.update({
        "max_send_interval": 42,
    })
    custom_sender = clazz(
        config=config,
        name=sender_type.value,
        reader=mock.Mock(),
        stats=mock.Mock(),
        field_filter=None,
    )
    assert custom_sender.max_send_interval == 42
