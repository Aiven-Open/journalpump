# AGENTS.md

**Created**: 2026-01-20
**Last Updated**: 2026-01-20

## Project Overview

`journalpump` is a Python-based service that reads log messages from `journald` and forwards them to various destinations. It is designed to be a flexible and reliable log shipping solution.

### Key Technologies

*   **Python 3**: The core language of the project.
*   **systemd/journald**: The primary source of log messages.
*   **Kafka**: One of the supported output destinations.
*   **Elasticsearch/OpenSearch**: Supported output destinations for log indexing and analysis.
*   **AWS CloudWatch**: Supported output destination for cloud-based log management.
*   **Google Cloud Logging**: Supported output destination for cloud-based log management.
*   **Rsyslog**: Supported output destination for traditional log forwarding.
*   **Websockets**: Supported output destination for real-time log streaming.
*   **Logplex**: Supported output destination for log aggregation.
*   **File**: Supported output destination for writing logs to local files.

### Architecture

The project consists of a main `journalpump` daemon that manages one or more `JournalReader` instances. Each `JournalReader` is responsible for reading from a specific journald source, applying filters and transformations, and sending the log messages to one or more configured output "senders".

The application is configured through a JSON file, which defines the readers, senders, filters, and other settings.

### Sender Architecture

The sending logic is encapsulated in `LogSender` classes, which are defined in the `journalpump/senders` directory. The `journalpump/senders/base.py` file contains the base `LogSender` class, which provides common functionality for all senders, such as message buffering, batching, and status reporting. Each specific sender implementation (e.g., `kafka_sender.py`, `elasticsearch_opensearch_sender.py`) inherits from this base class and implements the sender-specific logic for connecting to the destination and sending messages.

## Building and Running

### Building

The project uses `make` to simplify the build process for different package formats.

*   **Debian package**: `make deb`
*   **RPM package**: `make rpm`
*   **Python egg**: `python3 setup.py bdist_egg`

### Running

The `journalpump` service can be run directly or as a systemd service.

*   **Directly**: `journalpump /path/to/journalpump.json`
*   **systemd**: `systemctl start journalpump.service`

### Testing

The project uses `pytest` for testing. The `Makefile` provides the following targets for running tests:

*   **Unit tests**: `make unittest`
*   **System tests**: `make systest`
*   **Coverage**: `make coverage`

To run all tests, you can use:

```bash
make unittest systest
```

## Development Conventions

*   **Coding Style**: The project uses `black` for code formatting and `isort` for import sorting. It also uses `.flake8`, `.pylintrc`, and `.style.yapf` to enforce coding style. Note that a number of `pylint` checks are disabled in the `.pylintrc` file.
*   **Dependencies**: Project dependencies are managed in `requirements.txt` and `requirements.dev.txt`.
*   **Pre-commit Hooks**: `.pre-commit-config.yaml` is present, indicating the use of pre-commit hooks to maintain code quality. The following hooks are configured:
    *   `check-yaml`: Checks YAML files for syntax errors.
    *   `end-of-file-fixer`: Ensures that files end with a newline.
    *   `trailing-whitespace`: Trims trailing whitespace.
    *   `flake8`: Checks for Python style and quality issues.
    *   `black`: Formats Python code.
    *   `pylint`: Analyzes Python code for errors and code smells.
    *   `mypy`: Performs static type checking.
    *   `isort`: Sorts Python imports.
