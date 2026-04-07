"""Unit tests for qs_trader.utils.log_capture.LogCaptureHandler.

Covers:
- Handler attaches to root logger on __enter__
- Handler detaches from root logger on __exit__
- WARNING+ records are buffered and written to file
- Records below WARNING are not written
- File is not created when no records were captured
- Multiple WARNING/ERROR/CRITICAL records land in one file
- Output file parent directories are created automatically
- Context manager returns self
"""

import logging
from pathlib import Path

import pytest

from qs_trader.utils.log_capture import LogCaptureHandler


class TestLogCaptureHandler:
    def test_context_manager_attaches_and_detaches(self) -> None:
        """Handler is on root logger inside block, gone after exit."""
        root = logging.getLogger()
        output = Path("/tmp/qs_test_noop.log")

        with LogCaptureHandler(output) as handler:
            assert handler in root.handlers

        assert handler not in root.handlers

    def test_warning_record_written_to_file(self, tmp_path: Path) -> None:
        """A WARNING log message lands in the output file."""
        output = tmp_path / "run.log"
        with LogCaptureHandler(output):
            logging.getLogger("test.capture").warning("something went wrong")

        assert output.exists()
        content = output.read_text()
        assert "something went wrong" in content
        assert "WARNING" in content

    def test_error_record_written_to_file(self, tmp_path: Path) -> None:
        """An ERROR log message is captured."""
        output = tmp_path / "run.log"
        with LogCaptureHandler(output):
            logging.getLogger("test.capture").error("fatal issue")

        assert "fatal issue" in output.read_text()

    def test_info_record_not_written(self, tmp_path: Path) -> None:
        """INFO-level records are ignored (handler level is WARNING)."""
        output = tmp_path / "run.log"
        root = logging.getLogger()
        original_level = root.level
        root.setLevel(logging.DEBUG)

        try:
            with LogCaptureHandler(output):
                logging.getLogger("test.capture").info("just info")
        finally:
            root.setLevel(original_level)

        # File should not be created because no WARNING+ records were buffered
        assert not output.exists()

    def test_no_records_no_file(self, tmp_path: Path) -> None:
        """Output file is not created when no WARNING+ records are emitted."""
        output = tmp_path / "empty_run.log"
        with LogCaptureHandler(output):
            pass  # no log calls

        assert not output.exists()

    def test_parent_directories_created(self, tmp_path: Path) -> None:
        """Parent directories of output_path are created automatically."""
        output = tmp_path / "nested" / "deep" / "run.log"
        with LogCaptureHandler(output):
            logging.getLogger("test.capture").warning("dir creation test")

        assert output.exists()

    def test_multiple_records_all_in_file(self, tmp_path: Path) -> None:
        """Multiple records are all written in order."""
        output = tmp_path / "multi.log"
        with LogCaptureHandler(output):
            log = logging.getLogger("test.multi")
            log.warning("first warning")
            log.error("second error")
            log.critical("third critical")

        content = output.read_text()
        assert "first warning" in content
        assert "second error" in content
        assert "third critical" in content
        # Ordering: first warning before second error
        assert content.index("first warning") < content.index("second error")

    def test_returns_self_from_enter(self, tmp_path: Path) -> None:
        """__enter__ returns the handler itself for optional binding."""
        output = tmp_path / "self.log"
        handler = LogCaptureHandler(output)
        with handler as h:
            assert h is handler

    def test_exception_in_block_still_flushes(self, tmp_path: Path) -> None:
        """Records are written even when the wrapped block raises."""
        output = tmp_path / "exc.log"
        with pytest.raises(RuntimeError):
            with LogCaptureHandler(output):
                logging.getLogger("test.exc").warning("before exception")
                raise RuntimeError("boom")

        assert output.exists()
        assert "before exception" in output.read_text()
