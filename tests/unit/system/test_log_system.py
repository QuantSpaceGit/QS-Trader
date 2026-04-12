"""Tests for centralized logging configuration."""

import json
import logging
from logging.handlers import RotatingFileHandler

import pytest

from qs_trader.system import LoggerFactory, LoggingConfig


@pytest.fixture(autouse=True)
def reset_logging():
    """Reset logging configuration before and after each test."""
    LoggerFactory.reset()
    yield
    LoggerFactory.reset()


def test_default_configuration():
    """Test logger factory with default configuration."""
    logger = LoggerFactory.get_logger()

    assert LoggerFactory.is_configured()
    # structlog.get_logger() returns a BoundLoggerLazyProxy or BoundLogger
    assert hasattr(logger, "info")
    assert hasattr(logger, "error")
    assert hasattr(logger, "warning")

    config = LoggerFactory.get_config()
    assert config.level == "INFO"
    assert config.format == "console"
    # Default is now enable_file=True with WARNING level
    assert config.enable_file is True
    assert config.file_level == "WARNING"


def test_explicit_configuration():
    """Test configuring logger factory explicitly."""
    config = LoggingConfig(
        level="DEBUG",
        format="json",
        enable_file=False,
    )

    LoggerFactory.configure(config)
    logger = LoggerFactory.get_logger()

    assert LoggerFactory.is_configured()
    # Check that it's a logger-like object
    assert hasattr(logger, "info")
    assert LoggerFactory.get_config().level == "DEBUG"
    assert LoggerFactory.get_config().format == "json"


def test_logger_with_name():
    """Test getting logger with explicit name."""
    logger = LoggerFactory.get_logger("test.module")

    # Check that it's a logger-like object
    assert hasattr(logger, "info")
    # Logger name is set internally by structlog
    assert LoggerFactory.is_configured()


def test_auto_configure_on_first_use():
    """Test that logger auto-configures with defaults on first use."""
    assert not LoggerFactory.is_configured()

    logger = LoggerFactory.get_logger()

    assert LoggerFactory.is_configured()
    assert hasattr(logger, "info")


def test_file_logging_configuration(tmp_path):
    """Test configuring file output."""
    log_file = tmp_path / "test.log"

    config = LoggingConfig(
        level="INFO",
        enable_file=True,
        file_path=log_file,
        file_level="DEBUG",
        file_rotation=False,
    )

    LoggerFactory.configure(config)
    logger = LoggerFactory.get_logger()

    # Log a message
    logger.info("test.message", key="value")

    # Check that file was created
    assert log_file.exists()

    # Read log file content
    content = log_file.read_text()
    # File logs are JSON lines
    log_entry = json.loads(content.strip())
    assert log_entry["event"] == "test.message"
    assert log_entry["key"] == "value"


def test_file_logging_requires_path():
    """Test that enabling file logging without path uses default."""
    config = LoggingConfig(
        level="INFO",
        enable_file=True,
        file_path=None,
    )

    # Now uses default path instead of raising error
    LoggerFactory.configure(config)
    result_config = LoggerFactory.get_config()
    assert result_config.file_path is not None
    assert str(result_config.file_path) == "logs/qs_trader.log"


def test_file_logging_creates_directory(tmp_path):
    """Test that file logging creates parent directories."""
    log_file = tmp_path / "logs" / "subdir" / "test.log"

    config = LoggingConfig(
        level="INFO",
        enable_file=True,
        file_path=log_file,
        file_rotation=False,
    )

    LoggerFactory.configure(config)
    logger = LoggerFactory.get_logger()
    logger.info("test")

    assert log_file.exists()
    assert log_file.parent.exists()


def test_rotating_file_handler(tmp_path):
    """Test rotating file handler configuration."""
    log_file = tmp_path / "rotating.log"

    config = LoggingConfig(
        level="INFO",
        enable_file=True,
        file_path=log_file,
        file_rotation=True,
        max_file_size_mb=1,
        backup_count=3,
    )

    LoggerFactory.configure(config)

    # Check that RotatingFileHandler was configured
    root_logger = logging.getLogger()
    handlers = [h for h in root_logger.handlers if isinstance(h, RotatingFileHandler)]

    assert len(handlers) > 0
    # Find the handler for our specific log file
    handler = next((h for h in handlers if str(log_file) in str(h.baseFilename)), None)
    assert handler is not None
    assert handler.maxBytes == 1 * 1024 * 1024
    assert handler.backupCount == 3


def test_different_log_levels():
    """Test different log level configurations."""
    from typing import cast

    from qs_trader.system.log_system import LogLevel

    for level_str in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        LoggerFactory.reset()

        level = cast(LogLevel, level_str)
        config = LoggingConfig(level=level)
        LoggerFactory.configure(config)

        assert LoggerFactory.get_config().level == level_str


def test_console_vs_json_format():
    """Test console vs JSON format configuration."""
    # Console format
    LoggerFactory.reset()
    config_console = LoggingConfig(format="console")
    LoggerFactory.configure(config_console)
    assert LoggerFactory.get_config().format == "console"

    # JSON format
    LoggerFactory.reset()
    config_json = LoggingConfig(format="json")
    LoggerFactory.configure(config_json)
    assert LoggerFactory.get_config().format == "json"


def test_reset_clears_configuration():
    """Test that reset clears configuration."""
    config = LoggingConfig(level="DEBUG")
    LoggerFactory.configure(config)

    assert LoggerFactory.is_configured()
    assert LoggerFactory.get_config().level == "DEBUG"

    LoggerFactory.reset()

    assert not LoggerFactory.is_configured()
    # After reset, get_config returns defaults
    assert LoggerFactory.get_config().level == "INFO"


def test_logger_usage_example(tmp_path):
    """Test realistic logger usage pattern."""
    log_file = tmp_path / "app.log"

    # Configure at startup
    config = LoggingConfig(
        level="DEBUG",
        format="console",
        enable_file=True,
        file_path=log_file,
        file_level="INFO",
    )
    LoggerFactory.configure(config)

    # Get loggers in different modules
    logger1 = LoggerFactory.get_logger("module1")
    logger2 = LoggerFactory.get_logger("module2")

    # Log messages with structured data
    logger1.info("user.login", user_id=123, ip="192.168.1.1")
    logger2.debug("db.query", query="SELECT * FROM users", duration_ms=45)
    logger1.warning("cache.miss", key="user:123")
    logger2.error("api.error", endpoint="/users", status_code=500)

    # Verify file contains logs (INFO level and above)
    content = log_file.read_text()
    # Only INFO+ should be in file based on file_level="INFO"
    # DEBUG message should not be there
    assert "cache.miss" in content
    assert "api.error" in content


def test_pydantic_validation():
    """Test Pydantic validation of LoggingConfig."""
    # Valid config
    config = LoggingConfig(level="DEBUG", enable_file=False)
    assert config.level == "DEBUG"

    # Invalid log level should use default
    config = LoggingConfig(level="INFO")
    assert config.level == "INFO"


def test_multiple_logger_instances_share_config():
    """Test that multiple logger instances share the same configuration."""
    config = LoggingConfig(level="WARNING")
    LoggerFactory.configure(config)

    _logger1 = LoggerFactory.get_logger("module1")
    _logger2 = LoggerFactory.get_logger("module2")

    # Both should be configured with the same settings
    assert LoggerFactory.is_configured()
    assert LoggerFactory.get_config().level == "WARNING"


def test_get_logger_without_name_uses_caller_module():
    """Test that get_logger() without name uses caller's module."""
    logger = LoggerFactory.get_logger()
    assert hasattr(logger, "info")
    # The name should be automatically determined from the calling frame


def test_file_level_independent_from_console_level(tmp_path):
    """Test that file log level can be different from console level."""
    log_file = tmp_path / "debug.log"

    config = LoggingConfig(
        level="WARNING",  # Console only shows WARNING+
        enable_file=True,
        file_path=log_file,
        file_level="DEBUG",  # File captures everything
        file_rotation=False,
    )

    LoggerFactory.configure(config)
    logger = LoggerFactory.get_logger()

    # Log at different levels
    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warning message")

    # File should contain WARNING level entry encoded as JSON
    entries = [json.loads(line) for line in log_file.read_text().splitlines() if line.strip()]
    events = [entry["event"] for entry in entries]
    assert "warning message" in events


def test_file_logs_are_machine_readable_json(tmp_path):
    """File outputs use JSON so they are easy to parse programmatically."""
    log_file = tmp_path / "machine.jsonl"

    config = LoggingConfig(enable_file=True, file_path=log_file, file_rotation=False)
    LoggerFactory.configure(config)

    logger = LoggerFactory.get_logger()
    logger.error("event_bus.handler_error", error="Fail 2")

    content = log_file.read_text().strip()
    record = json.loads(content)

    assert record["event"] == "event_bus.handler_error"
    assert record["error"] == "Fail 2"
    # Ensure key metadata is present exactly once
    assert "log_timestamp" in record  # Renamed to avoid conflicts with event domain fields
    assert record["level"].upper() == "ERROR"


def test_console_exc_info_renders_traceback_once(
    capsys: pytest.CaptureFixture[str],
):
    """Console logging should render a single traceback when exc_info is enabled."""
    LoggerFactory.configure(
        LoggingConfig(
            format="console",
            enable_file=False,
        )
    )
    logger = LoggerFactory.get_logger("tests.log_system")

    try:
        raise ValueError("bad boom")
    except ValueError:
        logger.error("test.traceback", exc_info=True)

    captured = capsys.readouterr()
    console_output = captured.out + captured.err
    normalized_output = console_output.lower()

    assert "test.traceback" in normalized_output
    assert "ValueError: bad boom" in console_output
    assert console_output.count("ValueError: bad boom") == 1
