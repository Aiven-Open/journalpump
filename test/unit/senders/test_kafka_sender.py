from journalpump.senders.kafka_sender import KafkaSender
from unittest import mock

import pytest


class TestKafkaSenderInit:
    """Tests for KafkaSender initialization logic"""

    def test_basic_initialization(self):
        """Test basic KafkaSender initialization with minimal config"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )

        assert sender.kafka_producer is None
        assert sender.topic == "test-topic"
        assert sender.kafka_msg_key is None

    def test_initialization_with_message_key(self):
        """Test KafkaSender initialization with kafka_msg_key"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "kafka_msg_key": "my-key",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )

        assert sender.kafka_msg_key == b"my-key"
        assert sender.topic == "test-topic"

    def test_initialization_with_max_send_interval(self):
        """Test KafkaSender initialization with custom max_send_interval"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "max_send_interval": 1.5,
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )

        assert sender.max_send_interval == 1.5

    def test_initialization_default_max_send_interval(self):
        """Test KafkaSender initialization uses default max_send_interval of 0.3"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )

        assert sender.max_send_interval == 0.3


class TestGenerateClientConfig:
    """Tests for _generate_client_config method"""

    def test_basic_client_config(self):
        """Test basic client configuration with PLAINTEXT protocol"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        client_config = sender._generate_client_config()

        assert client_config["bootstrap_servers"] == "localhost:9092"
        assert client_config["security_protocol"] == "PLAINTEXT"
        assert client_config["reconnect_backoff_ms"] == 1000
        assert client_config["reconnect_backoff_max_ms"] == 10000

    def test_client_config_with_explicit_security_protocol_ssl(self):
        """Test client configuration with explicit SSL security protocol"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "security_protocol": "SSL",
            "ca": "/path/to/ca.pem",
            "certfile": "/path/to/cert.pem",
            "keyfile": "/path/to/key.pem",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        client_config = sender._generate_client_config()

        assert client_config["security_protocol"] == "SSL"
        assert client_config["ssl_cafile"] == "/path/to/ca.pem"
        assert client_config["ssl_certfile"] == "/path/to/cert.pem"
        assert client_config["ssl_keyfile"] == "/path/to/key.pem"

    def test_client_config_with_sasl_plaintext(self):
        """Test client configuration with SASL_PLAINTEXT protocol"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "user",
            "sasl_plain_password": "password",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        client_config = sender._generate_client_config()

        assert client_config["security_protocol"] == "SASL_PLAINTEXT"
        assert client_config["sasl_mechanism"] == "PLAIN"
        assert client_config["sasl_plain_username"] == "user"
        assert client_config["sasl_plain_password"] == "password"

    def test_client_config_with_sasl_ssl(self):
        """Test client configuration with SASL_SSL protocol"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "user",
            "sasl_plain_password": "password",
            "ca": "/path/to/ca.pem",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        client_config = sender._generate_client_config()

        assert client_config["security_protocol"] == "SASL_SSL"
        assert client_config["sasl_mechanism"] == "SCRAM-SHA-256"
        assert client_config["sasl_plain_username"] == "user"
        assert client_config["sasl_plain_password"] == "password"
        assert client_config["ssl_cafile"] == "/path/to/ca.pem"

    def test_client_config_sasl_default_mechanism(self):
        """Test SASL configuration uses PLAIN as default mechanism"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_plain_username": "user",
            "sasl_plain_password": "password",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        client_config = sender._generate_client_config()

        assert client_config["sasl_mechanism"] == "PLAIN"

    def test_client_config_sasl_missing_username(self):
        """Test that SASL configuration without username raises ValueError"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_plain_password": "password",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )

        with pytest.raises(ValueError, match="SASL username and password must be provided"):
            sender._generate_client_config()

    def test_client_config_sasl_missing_password(self):
        """Test that SASL configuration without password raises ValueError"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_plain_username": "user",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )

        with pytest.raises(ValueError, match="SASL username and password must be provided"):
            sender._generate_client_config()


class TestGenerateProducerConfig:
    """Tests for _generate_producer_config method"""

    def test_producer_config_inherits_client_config(self):
        """Test that producer config includes client configuration"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "kafka_api_version": "2.5.0",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        producer_config = sender._generate_producer_config()

        assert producer_config["bootstrap_servers"] == "localhost:9092"
        assert producer_config["api_version"] == "2.5.0"
        assert producer_config["security_protocol"] == "PLAINTEXT"

    def test_producer_config_sets_linger_ms(self):
        """Test that producer config sets linger_ms to 500"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        producer_config = sender._generate_producer_config()

        assert producer_config["linger_ms"] == 500

    def test_producer_config_compression_gzip_default(self):
        """Test that producer config uses gzip compression by default"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        producer_config = sender._generate_producer_config()

        # Will be gzip if snappy/zstd not available
        assert "compression_type" in producer_config
        assert producer_config["compression_type"] in ["gzip", "snappy", "zstd"]

    def test_producer_config_with_socks5_proxy(self):
        """Test producer config includes socks5_proxy when configured"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "socks5_proxy": "socks5://proxy:1080",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        producer_config = sender._generate_producer_config()

        assert producer_config["socks5_proxy"] == "socks5://proxy:1080"

    def test_producer_config_without_socks5_proxy(self):
        """Test producer config doesn't include socks5_proxy when not configured"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
        }
        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        producer_config = sender._generate_producer_config()

        assert "socks5_proxy" not in producer_config


class TestValidSecurityProtocols:
    """Test all valid security protocol combinations"""

    @pytest.mark.parametrize(
        "protocol",
        ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"],
    )
    def test_valid_security_protocols(self, protocol):
        """Test that all valid security protocols are accepted"""
        config = {
            "kafka_address": "localhost:9092",
            "kafka_topic": "test-topic",
            "security_protocol": protocol,
        }

        # Add required fields for SASL protocols
        if "SASL" in protocol:
            config["sasl_plain_username"] = "user"
            config["sasl_plain_password"] = "password"

        # Add required fields for SSL protocols
        if "SSL" in protocol:
            config["ca"] = "/path/to/ca.pem"
            config["certfile"] = "/path/to/cert.pem"
            config["keyfile"] = "/path/to/key.pem"

        sender = KafkaSender(
            config=config,
            name="test-sender",
            reader=mock.Mock(),
            stats=mock.Mock(),
            field_filter=None,
        )
        client_config = sender._generate_client_config()

        assert client_config["security_protocol"] == protocol
