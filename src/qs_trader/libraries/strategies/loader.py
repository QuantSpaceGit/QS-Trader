"""
Strategy Loader - Auto-discovery of Strategy classes.

Scans a directory for Python files containing Strategy subclasses and
their associated StrategyConfig instances. Enables convention-based
strategy registration without manual imports.

Philosophy:
- Convention over configuration
- Strategies are self-contained .py files
- Config lives with strategy code (single file)
- No hardcoded class names required
- Graceful error handling

Usage:
    >>> from pathlib import Path
    >>> loader = StrategyLoader()
    >>> strategies = loader.load_from_directory(Path("my_library/strategies"))
    >>> # strategies = {"buy_and_hold": (BuyAndHoldStrategy, config), ...}
"""

import importlib.util
import inspect
import sys
from pathlib import Path
from typing import Any

import structlog

from qs_trader.libraries.strategies.base import Strategy, StrategyConfig

logger = structlog.get_logger(__name__)


class StrategyLoadError(Exception):
    """Raised when strategy loading fails."""

    pass


class StrategyLoader:
    """
    Loads Strategy classes from Python files.

    Discovers Strategy subclasses and their configs by:
    1. Scanning directory for .py files
    2. Dynamically importing each module
    3. Finding classes that inherit from Strategy
    4. Extracting StrategyConfig from same module
    5. Validating uniqueness of config.name

    Handles errors gracefully - logs and skips problematic files.
    """

    def __init__(self) -> None:
        """Initialize loader."""
        self._loaded_strategies: dict[str, tuple[type[Strategy], StrategyConfig]] = {}

    def load_from_directory(
        self, directory: Path, recursive: bool = False
    ) -> dict[str, tuple[type[Strategy], StrategyConfig]]:
        """
        Load all strategies from a directory.

        Args:
            directory: Path to directory containing strategy .py files
            recursive: If True, search subdirectories (default: False)

        Returns:
            Dict mapping strategy name (from config.name) to (StrategyClass, config)

        Raises:
            StrategyLoadError: If directory doesn't exist or is not accessible

        Example:
            >>> loader = StrategyLoader()
            >>> strategies = loader.load_from_directory(Path("my_library/strategies"))
            >>> for name, (cls, config) in strategies.items():
            ...     print(f"Loaded: {name} - {config.display_name}")
        """
        if not directory.exists():
            raise StrategyLoadError(f"Strategy directory does not exist: {directory}")

        if not directory.is_dir():
            raise StrategyLoadError(f"Strategy path is not a directory: {directory}")

        logger.debug(
            "strategy.loader.scanning",
            directory=str(directory),
            recursive=recursive,
        )

        # Find all Python files
        pattern = "**/*.py" if recursive else "*.py"
        py_files = list(directory.glob(pattern))

        # Filter out __init__.py and __pycache__
        py_files = [f for f in py_files if f.name != "__init__.py" and "__pycache__" not in f.parts]

        logger.debug(
            "strategy.loader.files_found",
            count=len(py_files),
            files=[f.name for f in py_files],
        )

        # Load each file
        self._loaded_strategies.clear()
        for py_file in py_files:
            try:
                self._load_file(py_file)
            except Exception as e:
                # Log error but continue with other files
                logger.warning(
                    "strategy.loader.file_failed",
                    file=str(py_file),
                    error=str(e),
                    error_type=type(e).__name__,
                )
                continue

        logger.debug(
            "strategy.loader.complete",
            strategies_loaded=len(self._loaded_strategies),
            strategy_names=list(self._loaded_strategies.keys()),
        )

        return self._loaded_strategies.copy()

    def _load_file(self, file_path: Path) -> None:
        """
        Load strategies from a single Python file.

        Args:
            file_path: Path to .py file

        Raises:
            StrategyLoadError: If file loading fails
        """
        logger.debug("strategy.loader.loading_file", file=str(file_path))

        # Create module spec
        module_name = f"qs_trader.strategies.dynamic.{file_path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, file_path)

        if spec is None or spec.loader is None:
            raise StrategyLoadError(f"Failed to create module spec for {file_path}")

        # Import module
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module

        try:
            spec.loader.exec_module(module)
        except Exception as e:
            # Clean up sys.modules on failure
            sys.modules.pop(module_name, None)
            raise StrategyLoadError(f"Failed to execute module {file_path}: {e}") from e

        # Find Strategy subclasses in module
        strategies_found = self._find_strategies_in_module(module, file_path)

        if not strategies_found:
            logger.debug(
                "strategy.loader.no_strategies",
                file=str(file_path),
            )
            return

        # Find StrategyConfig instance in module
        config = self._find_config_in_module(module, file_path)

        if config is None:
            logger.warning(
                "strategy.loader.no_config",
                file=str(file_path),
                strategies_found=[cls.__name__ for cls in strategies_found],
            )
            return

        # We expect exactly one Strategy subclass per file
        if len(strategies_found) > 1:
            logger.warning(
                "strategy.loader.multiple_strategies",
                file=str(file_path),
                strategies=[cls.__name__ for cls in strategies_found],
                note="Using first strategy found",
            )

        strategy_class = strategies_found[0]

        # Check for duplicate strategy names
        if config.name in self._loaded_strategies:
            existing_cls, _ = self._loaded_strategies[config.name]
            raise StrategyLoadError(
                f"Duplicate strategy name '{config.name}' found in {file_path}. "
                f"Already loaded from {existing_cls.__module__}"
            )

        # Store strategy
        self._loaded_strategies[config.name] = (strategy_class, config)

        logger.debug(
            "strategy.loader.strategy_loaded",
            name=config.name,
            display_name=config.display_name,
            class_name=strategy_class.__name__,
            file=file_path.name,
        )

    def _find_strategies_in_module(self, module: Any, file_path: Path) -> list[type[Strategy]]:
        """
        Find all Strategy subclasses in a module.

        Args:
            module: Imported module
            file_path: Path to module file (for logging)

        Returns:
            List of Strategy subclasses found
        """
        strategies = []

        for name, obj in inspect.getmembers(module, inspect.isclass):
            # Must be a subclass of Strategy (but not Strategy itself)
            if obj is Strategy:
                continue

            if not issubclass(obj, Strategy):
                continue

            # Must be defined in this module (not imported)
            if obj.__module__ != module.__name__:
                continue

            strategies.append(obj)

        return strategies

    def _find_config_in_module(self, module: Any, file_path: Path) -> StrategyConfig | None:
        """
        Find StrategyConfig instance in a module.

        Looks for:
        1. Variable named 'CONFIG' (convention)
        2. Any StrategyConfig instance

        Args:
            module: Imported module
            file_path: Path to module file (for logging)

        Returns:
            StrategyConfig instance or None if not found
        """
        # First try convention: CONFIG variable
        if hasattr(module, "CONFIG"):
            config = getattr(module, "CONFIG")
            if isinstance(config, StrategyConfig):
                return config
            else:
                logger.warning(
                    "strategy.loader.config_wrong_type",
                    file=str(file_path),
                    config_type=type(config).__name__,
                )

        # Fall back: find any StrategyConfig instance
        for name, obj in inspect.getmembers(module):
            if isinstance(obj, StrategyConfig):
                logger.debug(
                    "strategy.loader.config_found",
                    file=str(file_path),
                    variable_name=name,
                )
                return obj

        return None

    @property
    def loaded_count(self) -> int:
        """Get number of strategies loaded."""
        return len(self._loaded_strategies)

    def get_strategy_names(self) -> list[str]:
        """Get list of all loaded strategy names.

        Returns
        -------
        list[str]
            List of strategy names (from config.name).
        """
        return list(self._loaded_strategies.keys())

    def clear(self) -> None:
        """Clear all loaded strategies.

        Useful for testing or reloading strategies.
        """
        self._loaded_strategies.clear()
