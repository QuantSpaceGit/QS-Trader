"""Context-managed logging handler that captures WARNING+ records to a file.

Intended for use around a backtest run so that any warnings or errors
emitted during execution are persisted for offline inspection.

Usage::

    from pathlib import Path
    from qs_trader.utils.log_capture import LogCaptureHandler

    with LogCaptureHandler(Path("run_logs/warnings.log")):
        engine.run()

The handler attaches to the root logger on entry and detaches on exit.
If no WARNING+ records were captured the output file is **not** created,
avoiding empty artefact files for clean runs.

Only stdlib ``logging`` is used intentionally — structlog propagates its
records to the stdlib root logger when the stdlib integration is active,
so this handler transparently captures structlog output as well.
"""

import logging
import types
from pathlib import Path


class LogCaptureHandler(logging.Handler):
    """A ``logging.Handler`` that buffers WARNING+ records and writes them to
    a file when the context manager exits.

    The handler installs itself on the root logger at ``__enter__`` and
    removes itself at ``__exit__``, so it captures records from all loggers
    in the process for the lifetime of the context.

    Args:
        output_path: Destination file for buffered log records.  Parent
            directories are created automatically.  The file is written
            only when at least one record was captured.
    """

    _FORMATTER = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    def __init__(self, output_path: Path) -> None:
        """Initialise the handler.

        Args:
            output_path: Path where captured log records will be written
                on context-manager exit.
        """
        super().__init__(level=logging.WARNING)
        self._output_path = output_path
        self._records: list[logging.LogRecord] = []

    # ------------------------------------------------------------------
    # logging.Handler interface
    # ------------------------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        """Buffer a log record for later writing.

        Args:
            record: The log record to buffer.
        """
        self._records.append(record)

    # ------------------------------------------------------------------
    # Context-manager interface
    # ------------------------------------------------------------------

    def __enter__(self) -> "LogCaptureHandler":
        """Attach the handler to the root logger.

        Returns:
            self, so callers can optionally bind: ``with LogCaptureHandler(...) as h:``
        """
        # TODO(Phase 3): Root-logger attachment captures WARNING+ from all threads.
        # Under ThreadPoolExecutor sweeps each handler will receive cross-run records.
        # Fix: attach to a run-scoped logger name and propagate=False, or pass logger_name to __init__.
        logging.getLogger().addHandler(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Detach from the root logger and flush buffered records to disk.

        The output file is only created when at least one record was
        captured; clean runs produce no artefact file.

        Args:
            exc_type: Exception type, if any.
            exc_val: Exception value, if any.
            exc_tb: Traceback, if any.
        """
        logging.getLogger().removeHandler(self)
        self._flush_to_file()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _flush_to_file(self) -> None:
        """Write buffered records to ``output_path``.

        No-op when no records were captured.
        """
        if not self._records:
            return

        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        with self._output_path.open("w", encoding="utf-8") as fh:
            for record in self._records:
                fh.write(self._FORMATTER.format(record) + "\n")
