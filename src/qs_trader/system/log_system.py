"""Centralized logging configuration for QS-Trader."""

import logging
import sys
from datetime import datetime, timezone
from io import StringIO
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Literal

import structlog
from pydantic import BaseModel, Field

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
ExcInfo = tuple[type[BaseException], BaseException, TracebackType | None]


def _render_plain_traceback(exc_info: ExcInfo) -> str:
    """Render exception info using structlog's plain traceback formatter."""
    string_buffer = StringIO()
    structlog.dev.plain_traceback(string_buffer, exc_info)
    return string_buffer.getvalue().rstrip()


class LoggingConfig(BaseModel):
    """Configuration for logging system.

    Logging Levels Guide (User-Centric):

    INFO (Default - User-Facing):
    - Backtest started/completed
    - Universe loaded (summary)
    - Strategy signals
    - Orders placed/filled
    - Performance milestones
    - High-level summaries

    DEBUG (Developer Mode):
    - Service initialization details
    - EventBus subscriptions
    - Data loading per-symbol
    - Internal operations
    - Adapter details

    WARNING:
    - Recoverable issues (missing symbols, partial data)
    - Strategy skipped signals

    ERROR:
    - Order rejections
    - Data loading failures
    - Failures affecting results

    CRITICAL:
    - System-level failures

    Recommended Usage:
    - Normal backtesting: level="INFO" (clean, user-friendly output)
    - Development/debugging: level="DEBUG" (verbose, all operations)
    - Production monitoring: level="WARNING" (issues only)

    Timestamp Format Options:
    - "iso": 2025-10-22T20:50:07.288824Z (full ISO format)
    - "compact": 251022-205007.28 (YYMMDD-HHMMSS.ms) - recommended
    - "time": 20:50:07.28 (time only, good for same-day logs)
    - "short": 1022T205007 (MMDDTHHMMSS, very compact)
    """

    level: LogLevel = Field(
        default="INFO",
        description="Minimum log level (INFO=user-friendly, DEBUG=verbose, WARNING=issues only)",
    )
    format: Literal["console", "json"] = Field(
        default="console",
        description="Output format: console, or json",
    )
    timestamp_format: Literal["iso", "compact", "time", "short"] = Field(
        default="compact",
        description="Timestamp format for console output",
    )
    enable_file: bool = Field(
        default=True,
        description="Enable logging to file (WARNING and above by default)",
    )
    file_path: Path | None = Field(
        default=None,
        description="Path to log file (uses logs/qs_trader.log if None)",
    )
    file_level: LogLevel = Field(
        default="WARNING",
        description="Minimum log level for file output",
    )
    file_rotation: bool = Field(
        default=True,
        description="Enable log file rotation (when file gets too large)",
    )
    max_file_size_mb: int = Field(
        default=10,
        description="Maximum log file size in MB before rotation",
    )
    backup_count: int = Field(
        default=3,
        description="Number of rotated log files to keep",
    )
    console_width: int = Field(
        default=0,
        description="Maximum console line width (0 = no limit)",
    )
    enable_event_display: bool = Field(
        default=True,
        description="Enable Rich formatting for event display (bar, signal, order, fill)",
    )


class LoggerFactory:
    """
    Factory for creating and configuring structured loggers.

    Provides centralized configuration for all QS-Trader logging.
    Call configure() once at application startup, then use get_logger()
    to get configured logger instances throughout the codebase.

    Example:
        # At startup
        config = LoggingConfig(level="DEBUG", enable_file=True, file_path=Path("qs_trader.log"))
        LoggerFactory.configure(config)

        # In modules
        logger = LoggerFactory.get_logger()
        logger.info("trading.order_placed", symbol="AAPL", quantity=100)
    """

    _config: LoggingConfig | None = None
    _configured: bool = False

    @classmethod
    def configure(cls, config: LoggingConfig | None = None) -> None:
        """
        Configure the logging system.

        Should be called once at application startup before any logging occurs.

        Args:
            config: LoggingConfig instance. If None, uses default configuration.
        """
        if config is None:
            config = LoggingConfig()

        cls._config = config

        processors = cls._build_common_processors(config.timestamp_format)

        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setLevel(getattr(logging, config.level))
        console_processor: Any
        if config.format == "console":
            console_processor = cls._custom_console_renderer()
        else:
            console_processor = structlog.processors.JSONRenderer()
        console_handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(
                processor=console_processor,
                foreign_pre_chain=processors,
            )
        )

        handlers: list[logging.Handler] = [console_handler]
        root_level = getattr(logging, config.level)

        # Configure file logging if enabled
        if config.enable_file:
            # Use default if file_path not provided
            if config.file_path is None:
                config.file_path = Path("logs/qs_trader.log")

            file_handler = cls._configure_file_logging(config, processors)
            handlers.append(file_handler)
            root_level = min(root_level, getattr(logging, config.file_level))

        logging.basicConfig(level=root_level, handlers=handlers, force=True)

        # Configure structlog
        configured_processors = list(processors)
        if config.format == "console":
            configured_processors.extend(
                [
                    structlog.dev.set_exc_info,
                    structlog.processors.ExceptionRenderer(
                        _render_plain_traceback,
                    ),
                ]
            )
        else:
            configured_processors.append(structlog.processors.format_exc_info)
        configured_processors.append(structlog.stdlib.ProcessorFormatter.wrap_for_formatter)

        structlog.configure(
            processors=configured_processors,
            wrapper_class=structlog.stdlib.BoundLogger,
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        cls._configured = True

    @classmethod
    def _build_common_processors(cls, timestamp_format: str) -> list[Any]:
        """Processors shared by both structlog and stdlib handlers before rendering."""
        return [
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            cls._get_timestamper(timestamp_format),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.CallsiteParameterAdder(
                [
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.LINENO,
                ]
            ),
        ]

    @staticmethod
    def _get_timestamper(fmt: str) -> Any:
        """Get appropriate timestamper based on format with milliseconds.

        Uses 'log_timestamp' key to avoid conflicts with event domain fields
        that use 'timestamp' (e.g., PriceBarEvent.timestamp is market time).
        """

        def add_timestamp_processor(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
            """Add formatted timestamp with milliseconds."""
            now = datetime.now(timezone.utc)
            ms = now.microsecond // 10000  # Get 2-digit milliseconds

            if fmt == "iso":
                event_dict["log_timestamp"] = now.isoformat()
            elif fmt == "compact":
                # YYMMDD-HHMMSS.ms format
                event_dict["log_timestamp"] = now.strftime(f"%y%m%d-%H%M%S.{ms:02d}")
            elif fmt == "time":
                # Just time with milliseconds
                event_dict["log_timestamp"] = now.strftime(f"%H:%M:%S.{ms:02d}")
            elif fmt == "short":
                # MMDDTHHMMSS format (no separators, no ms for brevity)
                event_dict["log_timestamp"] = now.strftime("%m%dT%H%M%S")
            else:
                event_dict["log_timestamp"] = now.isoformat()

            return event_dict

        return add_timestamp_processor

    @staticmethod
    def _custom_console_renderer() -> Callable[[Any, str, dict[str, Any]], str]:
        """Custom console renderer with file:line info and Rich event formatting."""

        # Event counters and state tracking for display
        # Mixed type dict: event_type→int, last_timestamp→str, uuid_map→dict
        event_counters: dict[str, Any] = {}

        # Get config to check if event display is enabled
        config = LoggerFactory.get_config()

        def renderer(logger: Any, name: str, event_dict: dict[str, Any]) -> str:
            """Render log with timestamp, level, message, and location."""
            # Check early if this is an event display log (from qs_trader.events.*)
            # For event logs, preserve all fields including 'timestamp' (bar market time)
            logger_name = event_dict.get("logger", "")
            event = event_dict.get("event", "")

            if logger_name.startswith("qs_trader.events.") and event == "event.display":
                # If event display is disabled (silent mode), drop these logs entirely
                if not config.enable_event_display:
                    raise structlog.DropEvent

                # Remove only structlog metadata, preserve event domain fields
                event_dict.pop("log_timestamp", None)  # Remove structlog timestamp
                event_dict.pop("level", None)
                event_dict.pop("event", None)
                event_dict.pop("filename", None)
                event_dict.pop("lineno", None)
                event_dict.pop("logger", None)
                # Note: Keep 'timestamp' - it's a domain field for bar/signal/order events
                return LoggerFactory._format_event_rich(event_dict, event_counters)

            # Standard system logs - extract structlog metadata
            timestamp = event_dict.pop("log_timestamp", "")  # Use log_timestamp for system logs
            level = event_dict.pop("level", "info").upper()
            event = event_dict.pop("event", "")
            filename = event_dict.pop("filename", "")
            lineno = event_dict.pop("lineno", "")
            logger_name = event_dict.pop("logger", "")

            # Suppress redundant event-related logs when event display is enabled
            # These are already shown in Rich format by EventBus._log_event() or rejection logger
            if config.enable_event_display:
                redundant_events = {
                    # INFO level (event display duplicates)
                    "Order emitted",
                    "strategy.signal.emitted",
                    "execution.fill.generated",
                    "portfolio_service.fill_applied",
                    "portfolio_service.position_closed",  # Position close shown in Fill display
                    "portfolio_service.position_opened",  # Position open shown in Fill display
                    "portfolio_service.split_processed",  # Corporate actions shown in Rich format
                    "portfolio_service.dividend_processed",  # Corporate actions shown in Rich format
                    "resolver.adapter_created",  # Too verbose for event display mode
                    # WARNING level (rejections shown in Rich format)
                    "manager.signal.rejected.policy_violation",
                    "manager.signal.rejected.no_position",
                    "manager.signal.rejected.wrong_direction",
                }
                if event in redundant_events:
                    raise structlog.DropEvent  # Completely drop this log (no output, no newline)

            # Try Rich formatting for all system logs (all levels)
            rich_formatted = LoggerFactory._try_format_system_rich(event, event_dict, level, timestamp)
            if rich_formatted:
                return rich_formatted

            # Fallback to standard formatting for unmatched patterns
            # Color codes
            colors = {
                "DEBUG": "\033[36m",  # Cyan
                "INFO": "\033[32m",  # Green
                "WARNING": "\033[33m",  # Yellow
                "ERROR": "\033[31m",  # Red
                "CRITICAL": "\033[35m",  # Magenta
            }
            reset = "\033[0m"
            gray = "\033[90m"

            # Format level with color
            level_color = colors.get(level, "")
            level_str = f"[{level_color}{level.lower()}{reset}]"

            # Format context (key=value pairs)
            context_parts = []
            for key, value in sorted(event_dict.items()):
                if key.startswith("_"):
                    continue
                context_parts.append(f"{key}={value}")

            context_str = " ".join(context_parts) if context_parts else ""

            # Format location (module.path:lineno)
            if filename and lineno:
                # Remove .py extension
                module_file = Path(filename).stem
                # Combine logger name with filename for full path
                if logger_name and logger_name != "qs_trader":
                    location = f"{gray}({logger_name}.{module_file}:{lineno}){reset}"
                else:
                    location = f"{gray}({module_file}:{lineno}){reset}"
            else:
                location = ""

            # Build line: timestamp level event | context (location)
            parts = [timestamp, level_str, event]

            if context_str:
                parts.append(f"{gray}|{reset} {context_str}")

            if location:
                parts.append(location)

            return " ".join(parts)

        return renderer

    @staticmethod
    def _format_event_rich(event_dict: dict[str, Any], counters: dict[str, Any]) -> str:
        """
        Format event with Rich styling - delegates to specialized formatters.

        Implements timestamp grouping: events at the same timestamp are grouped together
        with a timestamp header shown once, followed by indented event details.

        Also tracks UUID → (event_type, counter) mappings for back-reference display.
        """
        event_type = event_dict.get("event_type", "unknown")

        # Initialize state tracking if needed
        if "last_timestamp" not in counters:
            counters["last_timestamp"] = None
        if "uuid_map" not in counters:
            counters["uuid_map"] = {}  # Maps UUID → (event_type, counter)

        # Increment counter for this event type
        if event_type not in counters:
            counters[event_type] = 0
        counters[event_type] += 1
        count = counters[event_type]

        # Record UUID → (event_type, counter) for back-references
        # Store BOTH event_id and signal_id for signals (other events reference signal_id, not event_id)
        event_id = event_dict.get("event_id")
        if event_id:
            counters["uuid_map"][str(event_id)] = (event_type, count)

        # For signals, also store signal_id mapping (orders reference this, not event_id)
        if event_type == "signal":
            signal_id = event_dict.get("signal_id")
            if signal_id:
                counters["uuid_map"][str(signal_id)] = (event_type, count)

        # Get timestamp for grouping (use domain timestamp, not log_timestamp)
        current_timestamp = LoggerFactory._extract_event_timestamp(event_dict)

        # Build output with optional timestamp header
        output_lines = []

        # Add timestamp header if timestamp changed
        if current_timestamp and current_timestamp != counters["last_timestamp"]:
            # Add blank line before new timestamp group (except for first group)
            if counters["last_timestamp"] is not None:
                output_lines.append("")
            output_lines.append(f"\033[1m\033[36m{current_timestamp}\033[0m")
            counters["last_timestamp"] = current_timestamp

        # Delegate to specialized formatter
        formatter_map = {
            "bar": _EventFormatters.format_bar,
            "signal": _EventFormatters.format_signal,
            "indicator": _EventFormatters.format_indicator,
            "order": _EventFormatters.format_order,
            "fill": _EventFormatters.format_fill,
            "trade": _EventFormatters.format_trade,
            "portfolio_state": _EventFormatters.format_portfolio_state,
            "performance_metrics": _EventFormatters.format_performance_metrics,
            "corporate_action": _EventFormatters.format_corporate_action,
            "corporate_action_impact": _EventFormatters.format_corporate_action_impact,
            "policy_violation": _EventFormatters.format_policy_violation,
            "signal_rejected": _EventFormatters.format_signal_rejected,
        }

        formatter = formatter_map.get(event_type)
        if formatter:
            event_line = formatter(event_dict, count, counters["uuid_map"])
        else:
            # Generic fallback
            cyan = "\033[36m"
            dim = "\033[2m"
            reset = "\033[0m"
            event_line = f"{dim}• {event_type} #{count}{reset} | {cyan}{event_dict}{reset}"

        output_lines.append(event_line)
        return "\n".join(output_lines)

    @staticmethod
    def _extract_event_timestamp(event_dict: dict[str, Any]) -> str | None:
        """
        Extract the domain timestamp from an event for grouping.

        Returns the timestamp in a display-friendly format (date and time).
        """
        # Try different timestamp fields based on event type
        timestamp = (
            event_dict.get("timestamp")  # bar, signal, order, fill
            or event_dict.get("snapshot_datetime")  # portfolio_state
            or event_dict.get("ex_date")  # corporate_action
        )

        if timestamp:
            # Truncate to just datetime (YYYY-MM-DDTHH:MM:SS)
            if len(str(timestamp)) > 19:
                return str(timestamp)[:19]
            return str(timestamp)

        return None

    @staticmethod
    def _try_format_system_rich(event: str, event_dict: dict[str, Any], level: str, timestamp: str) -> str | None:
        """
        Try to format system logs with Rich styling based on event patterns.

        Returns formatted string if pattern matches, None otherwise to fallback.
        """
        # Get _SystemLogFormatters to handle all system log formatting
        return _SystemLogFormatters.format_system_log(event, event_dict, level, timestamp)

    @classmethod
    def _configure_file_logging(cls, config: LoggingConfig, pre_chain: list[Any]) -> logging.Handler:
        """Configure file output for logging."""
        file_path = config.file_path
        assert file_path is not None  # Already validated in configure()

        # Create log directory if needed
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Configure file handler
        handler: logging.Handler
        if config.file_rotation:
            handler = RotatingFileHandler(
                filename=str(file_path),
                maxBytes=config.max_file_size_mb * 1024 * 1024,
                backupCount=config.backup_count,
                encoding="utf-8",
            )
        else:
            handler = logging.FileHandler(
                filename=str(file_path),
                encoding="utf-8",
            )

        # Set file handler level to the configured file_level
        handler.setLevel(getattr(logging, config.file_level))

        handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(
                processor=structlog.processors.JSONRenderer(),
                foreign_pre_chain=pre_chain,
            )
        )

        return handler

    @classmethod
    def get_logger(cls, name: str | None = None):
        """
        Get a configured logger instance.

        Args:
            name: Optional logger name. If None, uses the calling module's __name__.

        Returns:
            Configured structlog BoundLogger instance.
        """
        if not cls._configured:
            # Auto-configure with defaults if not explicitly configured
            cls.configure()

        if name is None:
            # Get caller's module name
            import inspect

            frame = inspect.currentframe()
            if frame and frame.f_back:
                name = frame.f_back.f_globals.get("__name__", "qs_trader")
            else:
                name = "qs_trader"

        return structlog.get_logger(name)

    @classmethod
    def get_config(cls) -> LoggingConfig:
        """Get current logging configuration."""
        if cls._config is None:
            return LoggingConfig()
        return cls._config

    @classmethod
    def is_configured(cls) -> bool:
        """Check if logging has been configured."""
        return cls._configured

    @classmethod
    def reset(cls) -> None:
        """Reset logging configuration (mainly for testing)."""
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        root_logger.handlers.clear()
        root_logger.setLevel(logging.NOTSET)
        cls._config = None
        cls._configured = False
        # Reset structlog to defaults
        structlog.reset_defaults()


class _EventFormatters:
    """Rich formatters for different event types."""

    # ANSI color codes
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    BLUE = "\033[34m"
    DIM = "\033[2m"
    RESET = "\033[0m"

    @staticmethod
    def format_bar(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format bar event with OHLCV data (tree-style, no timestamp)."""
        symbol = event_dict.get("symbol", "?")
        open_price = event_dict.get("open", 0)
        high = event_dict.get("high", 0)
        low = event_dict.get("low", 0)
        close = event_dict.get("close", 0)
        volume = event_dict.get("volume", 0)

        # Tree-style with indentation
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{_EventFormatters.CYAN}📊 {'Bar':<12}#{count:<3}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
        ]

        # OHLCV data
        ohlcv = (
            f"O: {_EventFormatters.YELLOW}{float(open_price):7.2f}{_EventFormatters.RESET} "
            f"H: {_EventFormatters.GREEN}{float(high):7.2f}{_EventFormatters.RESET} "
            f"L: {_EventFormatters.RED}{float(low):7.2f}{_EventFormatters.RESET} "
            f"C: {_EventFormatters.BLUE}{float(close):7.2f}{_EventFormatters.RESET}"
        )
        parts.append(ohlcv)
        parts.append(f"Vol: {int(volume):,}")

        return " | ".join(parts)

    @staticmethod
    def format_signal(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format signal event with intention and confidence (tree-style, no timestamp)."""
        symbol = event_dict.get("symbol", "?")
        intention = event_dict.get("intention", "?")
        confidence = event_dict.get("confidence", 0)

        # Determine color based on intention
        if "LONG" in intention:
            color = _EventFormatters.GREEN
        elif "SHORT" in intention:
            color = _EventFormatters.RED
        else:
            color = _EventFormatters.YELLOW

        # Tree-style with indentation
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{_EventFormatters.YELLOW}📊 {'Signal':<12}#{count:<3}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
            f"{color}{intention}{_EventFormatters.RESET}",
            f"Conf: {_EventFormatters.CYAN}{float(confidence):.2f}{_EventFormatters.RESET}",
        ]

        return " | ".join(parts)

    @staticmethod
    def format_indicator(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format indicator event with indicator values (tree-style, compact)."""
        symbol = event_dict.get("symbol", "?")
        indicators = event_dict.get("indicators", {})
        metadata = event_dict.get("metadata")

        # Format indicators compactly (max 3 shown, then "...")
        indicator_items = list(indicators.items())
        indicator_strs = []

        for key, value in indicator_items[:3]:
            # Format value based on type
            if isinstance(value, bool):
                val_str = f"{_EventFormatters.CYAN}{value}{_EventFormatters.RESET}"
            elif isinstance(value, (int, float)):
                val_str = f"{_EventFormatters.CYAN}{value:.2f}{_EventFormatters.RESET}"
            elif isinstance(value, str):
                val_str = f"{_EventFormatters.CYAN}{value}{_EventFormatters.RESET}"
            elif isinstance(value, dict):
                val_str = f"{_EventFormatters.DIM}{{...}}{_EventFormatters.RESET}"
            elif isinstance(value, list):
                val_str = f"{_EventFormatters.DIM}[...]{_EventFormatters.RESET}"
            else:
                val_str = f"{_EventFormatters.DIM}{str(value)[:20]}{_EventFormatters.RESET}"

            indicator_strs.append(f"{_EventFormatters.DIM}{key}:{_EventFormatters.RESET} {val_str}")

        if len(indicator_items) > 3:
            indicator_strs.append(f"{_EventFormatters.DIM}+{len(indicator_items) - 3} more{_EventFormatters.RESET}")

        indicators_display = ", ".join(indicator_strs)

        # Tree-style with indentation
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{_EventFormatters.BLUE}📈 {'Indicator':<12}#{count:<3}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
            indicators_display,
        ]

        # Add metadata hint if present
        if metadata:
            parts.append(f"{_EventFormatters.DIM}({len(metadata)} meta){_EventFormatters.RESET}")

        return " | ".join(parts)

    @staticmethod
    def format_order(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format order event with side, quantity, type, and signal back-reference (tree-style)."""
        symbol = event_dict.get("symbol", "?")
        side = event_dict.get("side", "?")
        quantity = event_dict.get("quantity", 0)
        order_type = event_dict.get("order_type", "market")

        side_color = _EventFormatters.GREEN if side == "buy" else _EventFormatters.RED
        qty_display = int(float(quantity)) if quantity else 0

        # Tree-style with indentation
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{_EventFormatters.BLUE}→  {'Order':<12}#{count:<3}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
            f"{side_color}{side.upper()}{_EventFormatters.RESET} "
            f"{qty_display} shares | "
            f"{_EventFormatters.DIM}{order_type}{_EventFormatters.RESET}",
        ]

        # Add back-reference to signal if available
        intent_id = event_dict.get("intent_id")
        if intent_id and intent_id in uuid_map:
            ref_type, ref_count = uuid_map[intent_id]
            parts.append(f"{_EventFormatters.DIM}← Signal #{ref_count}{_EventFormatters.RESET}")

        return " | ".join(parts)

    @staticmethod
    def format_fill(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format fill event with execution details and order back-reference (tree-style)."""
        symbol = event_dict.get("symbol", "?")
        side = event_dict.get("side", "?")
        quantity = event_dict.get("filled_quantity", 0)
        price = event_dict.get("fill_price", 0)
        commission = event_dict.get("commission", 0)

        side_color = _EventFormatters.GREEN if side == "buy" else _EventFormatters.RED
        qty_display = int(float(quantity)) if quantity else 0

        # Add action hint: BUY can be OPEN or COVER, SELL can be CLOSE or SHORT
        action_hint = ""
        if side == "buy":
            action_hint = (
                f"{side_color}BUY{_EventFormatters.RESET} {_EventFormatters.DIM}(open/cover){_EventFormatters.RESET}"
            )
        else:
            action_hint = (
                f"{side_color}SELL{_EventFormatters.RESET} {_EventFormatters.DIM}(close/short){_EventFormatters.RESET}"
            )

        # Tree-style with indentation
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{_EventFormatters.GREEN}✓  {'Fill':<12}#{count:<3}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
            f"{action_hint} "
            f"{qty_display} @ {_EventFormatters.CYAN}${float(price):.2f}{_EventFormatters.RESET} | "
            f"{_EventFormatters.DIM}Fee: ${float(commission):.2f}{_EventFormatters.RESET}",
        ]

        # Add trade_id if available (assigned by PortfolioService)
        trade_id = event_dict.get("trade_id")
        if trade_id:
            parts.append(f"{_EventFormatters.CYAN}Trade: {trade_id}{_EventFormatters.RESET}")

        # Add back-reference to order if available
        source_order_id = event_dict.get("source_order_id")
        if source_order_id and source_order_id in uuid_map:
            ref_type, ref_count = uuid_map[source_order_id]
            parts.append(f"{_EventFormatters.DIM}← Order #{ref_count}{_EventFormatters.RESET}")

        return " | ".join(parts)

    @staticmethod
    def format_trade(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format trade event with entry/exit details and P&L (tree-style)."""
        trade_id = event_dict.get("trade_id", "?")
        symbol = event_dict.get("symbol", "?")
        status = event_dict.get("status", "?")
        side = event_dict.get("side", "?")
        current_quantity = event_dict.get("current_quantity", 0)
        entry_price = event_dict.get("entry_price")
        exit_price = event_dict.get("exit_price")
        realized_pnl = event_dict.get("realized_pnl")
        commission_total = event_dict.get("commission_total", 0)

        # Color based on side and status
        if side == "long":
            side_color = _EventFormatters.GREEN
            side_display = "LONG"
        else:
            side_color = _EventFormatters.RED
            side_display = "SHORT"

        # Status display
        if status == "open":
            status_icon = "📈"
            status_display = f"{_EventFormatters.YELLOW}OPENED{_EventFormatters.RESET}"
        else:
            status_icon = "✓"
            status_display = f"{_EventFormatters.GREEN}CLOSED{_EventFormatters.RESET}"

        # Tree-style with indentation
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{status_icon}  {'Trade':<12}#{count:<3}",
            f"{_EventFormatters.CYAN}{trade_id}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
            f"{side_color}{side_display}{_EventFormatters.RESET}",
            f"{status_display}",
        ]

        # Entry details
        if entry_price is not None:
            qty_display = int(float(current_quantity)) if current_quantity else 0
            parts.append(
                f"Qty: {qty_display} @ {_EventFormatters.CYAN}${float(entry_price):.2f}{_EventFormatters.RESET}"
            )

        # Exit details (only for closed trades)
        if status == "closed" and exit_price is not None:
            parts.append(f"Exit: {_EventFormatters.CYAN}${float(exit_price):.2f}{_EventFormatters.RESET}")

        # P&L (only for closed trades)
        if status == "closed" and realized_pnl is not None:
            pnl_value = float(realized_pnl)
            if pnl_value >= 0:
                pnl_color = _EventFormatters.GREEN
                pnl_sign = "+"
            else:
                pnl_color = _EventFormatters.RED
                pnl_sign = ""
            parts.append(f"P&L: {pnl_color}{pnl_sign}${pnl_value:.2f}{_EventFormatters.RESET}")

        # Commission
        if commission_total:
            parts.append(f"{_EventFormatters.DIM}Fees: ${float(commission_total):.2f}{_EventFormatters.RESET}")

        return " | ".join(parts)

    @staticmethod
    def format_corporate_action(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format corporate action event with all available details."""
        symbol = event_dict.get("symbol", "?")
        action_type = event_dict.get("action_type", "?")
        # Use ex_date as the primary datetime (the action effective date in simulation)
        timestamp = event_dict.get("ex_date", "")[:10] if event_dict.get("ex_date") else ""

        # Build base display
        parts = [
            f"{_EventFormatters.YELLOW}🔔 {'Corp Action':<18}#{count:<3}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
        ]

        if timestamp:
            parts.append(f"{_EventFormatters.DIM}{timestamp}{_EventFormatters.RESET}")

        parts.append(f"{action_type.upper()}")

        # Add optional fields if present
        if "ratio" in event_dict and event_dict["ratio"] is not None:
            parts.append(f"Ratio: {event_dict['ratio']}")
        if "dividend_amount" in event_dict and event_dict["dividend_amount"]:
            parts.append(f"Amount: ${event_dict['dividend_amount']}")
        if "payment_date" in event_dict and event_dict["payment_date"]:
            parts.append(f"Pay: {event_dict['payment_date']}")

        return " | ".join(parts)

    @staticmethod
    def format_corporate_action_impact(
        event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]
    ) -> str:
        """Format portfolio impact from corporate action processing."""
        symbol = event_dict.get("symbol", "?")
        action_type = event_dict.get("action_type", "?")

        parts = [
            f"{_EventFormatters.YELLOW}{'   └─ Portfolio Impact':<25}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
        ]

        if action_type == "SPLIT":
            # Split impact: show old/new quantity and prices
            old_qty = event_dict.get("old_quantity", 0)
            new_qty = event_dict.get("new_quantity", 0)
            old_price = event_dict.get("old_avg_price", 0)
            new_price = event_dict.get("new_avg_price", 0)

            if old_qty == 0 and new_qty == 0:
                # No position - show zero impact
                parts.append(f"{_EventFormatters.DIM}No position - no impact{_EventFormatters.RESET}")
            else:
                parts.append(
                    f"Shares: {_EventFormatters.YELLOW}{int(old_qty):,}{_EventFormatters.RESET} → "
                    f"{_EventFormatters.GREEN}{int(new_qty):,}{_EventFormatters.RESET}"
                )
                parts.append(
                    f"Avg Price: {_EventFormatters.YELLOW}${float(old_price):.2f}{_EventFormatters.RESET} → "
                    f"{_EventFormatters.GREEN}${float(new_price):.2f}{_EventFormatters.RESET}"
                )

        elif action_type == "DIVIDEND":
            # Dividend impact: show shares, amount per share, total received/paid
            quantity = event_dict.get("quantity", 0)
            amount_per_share = event_dict.get("amount_per_share", 0)
            total_amount = event_dict.get("total_amount", 0)
            position_type = event_dict.get("position_type", "LONG")
            note = event_dict.get("note")  # Special message for total_return mode

            if quantity == 0:
                # No position - show zero impact
                parts.append(f"${float(amount_per_share):.4f}/share")
                parts.append(f"{_EventFormatters.DIM}No position - no impact{_EventFormatters.RESET}")
            else:
                parts.append(f"Shares: {int(quantity):,}")
                parts.append(f"${float(amount_per_share):.4f}/share")

                # Check if this is a total_return mode dividend (zero cash flow)
                if note and total_amount == 0:
                    # Total return mode - dividend in price, no cash flow
                    parts.append(f"{_EventFormatters.CYAN}No cash flow - dividend in price{_EventFormatters.RESET}")
                elif position_type == "LONG":
                    parts.append(
                        f"Total Received: {_EventFormatters.GREEN}${float(total_amount):.2f}{_EventFormatters.RESET}"
                    )
                elif position_type == "SHORT":
                    parts.append(
                        f"Total Paid: {_EventFormatters.RED}${float(total_amount):.2f}{_EventFormatters.RESET}"
                    )
                else:
                    # NONE position type
                    parts.append(f"{_EventFormatters.DIM}$0.00{_EventFormatters.RESET}")

        return " | ".join(parts)

    @staticmethod
    def format_policy_violation(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format policy violation event with red warning display (tree-style, no timestamp)."""
        symbol = event_dict.get("symbol", "?")
        strategy_id = event_dict.get("strategy_id", "?")
        intention = event_dict.get("intention", "?")
        reason = event_dict.get("reason", "policy violation")

        # Tree-style with indentation
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{_EventFormatters.RED}🚫 {'Rejected':<12}#{count:<3}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
            f"{_EventFormatters.RED}{intention}{_EventFormatters.RESET}",
            f"Strategy: {_EventFormatters.CYAN}{strategy_id}{_EventFormatters.RESET}",
            f"{_EventFormatters.RED}{reason}{_EventFormatters.RESET}",
        ]

        return " | ".join(parts)

    @staticmethod
    def format_signal_rejected(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format signal rejection event (non-policy rejections like no position) (tree-style, no timestamp)."""
        symbol = event_dict.get("symbol", "?")
        strategy_id = event_dict.get("strategy_id", "?")
        intention = event_dict.get("intention", "?")
        reason = event_dict.get("reason", "rejected")

        # Tree-style with indentation
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{_EventFormatters.YELLOW}⚠️  {'Rejected':<12}#{count:<3}{_EventFormatters.RESET}",
            f"{_EventFormatters.MAGENTA}{symbol}{_EventFormatters.RESET}",
            f"{_EventFormatters.YELLOW}{intention}{_EventFormatters.RESET}",
            f"Strategy: {_EventFormatters.CYAN}{strategy_id}{_EventFormatters.RESET}",
            f"{_EventFormatters.YELLOW}{reason}{_EventFormatters.RESET}",
        ]

        return " | ".join(parts)

    @staticmethod
    def format_portfolio_state(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format portfolio state event with all available metrics (tree-style, no timestamp)."""
        # Core metrics
        equity = event_dict.get("current_portfolio_equity", 0)

        # Tree-style with indentation
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{_EventFormatters.CYAN}💼 {'Portfolio':<12}#{count:<3}{_EventFormatters.RESET}",
            f"Equity: {_EventFormatters.GREEN}${float(equity):,.2f}{_EventFormatters.RESET}",
        ]

        # Add optional fields if present in event
        if "cash_balance" in event_dict:
            parts.append(f"Cash: ${float(event_dict['cash_balance']):,.2f}")

        if "total_pl" in event_dict:
            pnl = float(event_dict["total_pl"])
            pnl_color = _EventFormatters.GREEN if pnl >= 0 else _EventFormatters.RED
            parts.append(f"P&L: {pnl_color}${pnl:,.2f}{_EventFormatters.RESET}")

        if "total_unrealized_pl" in event_dict:
            upnl = float(event_dict["total_unrealized_pl"])
            upnl_color = _EventFormatters.GREEN if upnl >= 0 else _EventFormatters.RED
            parts.append(f"Unrealized: {upnl_color}${upnl:,.2f}{_EventFormatters.RESET}")

        if "holdings_count" in event_dict:
            parts.append(f"Holdings: {event_dict['holdings_count']}")

        if "positions" in event_dict:
            parts.append(f"Positions: {len(event_dict['positions'])}")

        return " | ".join(parts)

    @staticmethod
    def format_performance_metrics(event_dict: dict[str, Any], count: int, uuid_map: dict[str, tuple[str, int]]) -> str:
        """Format performance metrics event (tree-style, no timestamp)."""
        equity = event_dict.get("equity", 0)
        total_return = event_dict.get("total_return_pct", 0)
        max_dd = event_dict.get("max_drawdown_pct", 0)
        current_dd = event_dict.get("current_drawdown_pct", 0)
        num_positions = event_dict.get("num_positions", 0)
        total_trades = event_dict.get("total_trades", 0)
        winning_trades = event_dict.get("winning_trades", 0)

        # Get risk-adjusted metrics
        sharpe = event_dict.get("sharpe_ratio", 0)
        cagr = event_dict.get("cagr", 0)
        win_rate_pct = event_dict.get("win_rate", 0)

        # Determine return color
        return_color = _EventFormatters.GREEN if float(total_return) >= 0 else _EventFormatters.RED

        # Tree-style with indentation (NO timestamp - uses grouped header)
        parts = [
            f"  {_EventFormatters.DIM}└─{_EventFormatters.RESET}",
            f"{_EventFormatters.CYAN}📈 {'Performance':<12}#{count:<3}{_EventFormatters.RESET}",
            f"Equity: {_EventFormatters.GREEN}${float(equity):,.2f}{_EventFormatters.RESET}",
            f"Return: {return_color}{float(total_return):+.2f}%{_EventFormatters.RESET}",
            f"CAGR: {return_color}{float(cagr):+.2f}%{_EventFormatters.RESET}",
            f"MaxDD: {_EventFormatters.RED}{float(max_dd):.2f}%{_EventFormatters.RESET}",
        ]

        # Add current drawdown only if underwater (greater than threshold)
        if float(current_dd) > 0.01:  # Show if > 0.01%
            parts.append(f"CurrDD: {_EventFormatters.YELLOW}{float(current_dd):.2f}%{_EventFormatters.RESET}")

        # Add Sharpe ratio if we have enough data
        if abs(float(sharpe)) > 0.01:
            sharpe_color = _EventFormatters.GREEN if float(sharpe) > 1.0 else _EventFormatters.YELLOW
            parts.append(f"Sharpe: {sharpe_color}{float(sharpe):.2f}{_EventFormatters.RESET}")

        # Add positions and trades count
        parts.extend([f"Pos: {num_positions}", f"Trades: {total_trades}"])

        # Add win rate if we have trades
        if total_trades > 0:
            # Use win_rate from event if available, otherwise calculate
            win_rate = float(win_rate_pct) if float(win_rate_pct) > 0 else (winning_trades / total_trades) * 100
            win_color = _EventFormatters.GREEN if win_rate >= 50 else _EventFormatters.YELLOW
            parts.append(f"WinRate: {win_color}{win_rate:.0f}%{_EventFormatters.RESET}")

        return " | ".join(parts)


class _SystemLogFormatters:
    """Rich formatters for system logs (non-event logs)."""

    # ANSI color codes (reuse from _EventFormatters)
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    BLUE = "\033[34m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    # Level colors
    LEVEL_COLORS = {
        "DEBUG": CYAN,
        "INFO": GREEN,
        "WARNING": YELLOW,
        "ERROR": RED,
        "CRITICAL": MAGENTA,
    }

    @classmethod
    def format_system_log(cls, event: str, event_dict: dict[str, Any], level: str, timestamp: str) -> str | None:
        """
        Format system log with Rich styling based on event pattern.

        Returns formatted string, or None to use fallback formatting.
        """
        level_color = cls.LEVEL_COLORS.get(level, cls.RESET)

        # Format based on common patterns
        if "backtest." in event:
            return cls._format_backtest_log(event, event_dict, level_color, timestamp)
        elif "strategy." in event or "strategy_" in event:
            return cls._format_strategy_log(event, event_dict, level_color, timestamp)
        elif "data_service." in event or "data." in event:
            return cls._format_data_log(event, event_dict, level_color, timestamp)
        elif ".initialized" in event or ".created" in event:
            return cls._format_service_log(event, event_dict, level_color, timestamp)
        else:
            # Generic Rich format for any other log
            return cls._format_generic_log(event, event_dict, level_color, timestamp)

    @classmethod
    def _format_backtest_log(cls, event: str, event_dict: dict[str, Any], color: str, timestamp: str) -> str:
        """Format backtest-related logs."""
        # Extract readable message from event name
        msg = event.replace("backtest.", "").replace("_", " ").title()

        parts = [
            f"{cls.DIM}{timestamp}{cls.RESET}",
            f"{color}Backtest{cls.RESET}",
            f"{cls.BOLD}{msg}{cls.RESET}",
        ]

        # Add important context
        if "start_date" in event_dict:
            parts.append(f"{cls.CYAN}{event_dict['start_date']}{cls.RESET}")
        if "end_date" in event_dict:
            parts.append(f"→ {cls.CYAN}{event_dict['end_date']}{cls.RESET}")
        if "universe_size" in event_dict:
            parts.append(f"Symbols: {cls.YELLOW}{event_dict['universe_size']}{cls.RESET}")
        if "strategies" in event_dict:
            parts.append(f"Strategies: {cls.YELLOW}{event_dict['strategies']}{cls.RESET}")
        if "bars_processed" in event_dict:
            parts.append(f"Bars: {cls.GREEN}{event_dict['bars_processed']:,}{cls.RESET}")
        if "duration_seconds" in event_dict:
            duration = float(event_dict["duration_seconds"])
            parts.append(f"Duration: {cls.DIM}{duration:.2f}s{cls.RESET}")

        return " | ".join(parts)

    @classmethod
    def _format_strategy_log(cls, event: str, event_dict: dict[str, Any], color: str, timestamp: str) -> str:
        """Format strategy-related logs."""
        msg = event.replace("strategy.", "").replace("_", " ").title()

        parts = [
            f"{cls.DIM}{timestamp}{cls.RESET}",
            f"{color}Strategy{cls.RESET}",
            f"{cls.BOLD}{msg}{cls.RESET}",
        ]

        if "strategy" in event_dict:
            parts.append(f"{cls.MAGENTA}{event_dict['strategy']}{cls.RESET}")
        if "strategy_id" in event_dict:
            parts.append(f"{cls.MAGENTA}{event_dict['strategy_id']}{cls.RESET}")
        if "strategy_count" in event_dict:
            parts.append(f"Count: {cls.YELLOW}{event_dict['strategy_count']}{cls.RESET}")
        if "strategy_names" in event_dict:
            names = event_dict["strategy_names"]
            if isinstance(names, list):
                parts.append(f"{cls.CYAN}{', '.join(names)}{cls.RESET}")

        return " | ".join(parts)

    @classmethod
    def _format_data_log(cls, event: str, event_dict: dict[str, Any], color: str, timestamp: str) -> str:
        """Format data service logs."""
        msg = event.replace("data_service.", "").replace("_", " ").title()

        parts = [
            f"{cls.DIM}{timestamp}{cls.RESET}",
            f"{color}Data{cls.RESET}",
            f"{cls.BOLD}{msg}{cls.RESET}",
        ]

        if "provider" in event_dict:
            parts.append(f"Provider: {cls.CYAN}{event_dict['provider']}{cls.RESET}")
        if "dataset" in event_dict:
            parts.append(f"Dataset: {cls.CYAN}{event_dict['dataset']}{cls.RESET}")
        if "symbol_count" in event_dict:
            parts.append(f"Symbols: {cls.YELLOW}{event_dict['symbol_count']}{cls.RESET}")
        if "total_bars" in event_dict:
            parts.append(f"Bars: {cls.GREEN}{event_dict['total_bars']:,}{cls.RESET}")

        return " | ".join(parts)

    @classmethod
    def _format_service_log(cls, event: str, event_dict: dict[str, Any], color: str, timestamp: str) -> str:
        """Format service initialization logs."""
        # Extract service name from event
        service = event.split(".")[0] if "." in event else "Service"
        msg = event.split(".")[-1].replace("_", " ").title()

        parts = [
            f"{cls.DIM}{timestamp}{cls.RESET}",
            f"{color}{service.replace('_', ' ').title()}{cls.RESET}",
            f"{cls.BOLD}{msg}{cls.RESET}",
        ]

        # Add any relevant context
        for key, value in sorted(event_dict.items()):
            if key.startswith("_") or key in ("log_timestamp", "level", "event", "filename", "lineno", "logger"):
                continue
            parts.append(f"{key}={cls.CYAN}{value}{cls.RESET}")

        return " | ".join(parts)

    @classmethod
    def _format_generic_log(cls, event: str, event_dict: dict[str, Any], color: str, timestamp: str) -> str:
        """Generic Rich format for any system log."""
        # Clean up event name for display
        msg = event.replace("_", " ").title()

        parts = [
            f"{cls.DIM}{timestamp}{cls.RESET}",
            f"{color}{msg}{cls.RESET}",
        ]

        # Add context fields
        context_parts = []
        for key, value in sorted(event_dict.items()):
            if key.startswith("_") or key in ("log_timestamp", "level", "event", "filename", "lineno", "logger"):
                continue
            context_parts.append(f"{key}={cls.CYAN}{value}{cls.RESET}")

        if context_parts:
            parts.append(" ".join(context_parts))

        return " | ".join(parts)
