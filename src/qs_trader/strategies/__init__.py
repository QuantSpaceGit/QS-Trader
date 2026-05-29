"""Shipped strategies used by the QS-Trader validation framework.

Strategies in this package are deterministic reference implementations that
the validation framework can rely on without requiring user-level
``custom_libraries.strategies`` configuration.  Project-level user strategies
continue to live under the configured custom library path.

Use :func:`register_builtin_strategies` to register every shipped strategy
into a :class:`~qs_trader.libraries.registry.StrategyRegistry`.  The function
scans this package via :func:`pkgutil.iter_modules`, imports each module, and
registers the first :class:`~qs_trader.libraries.strategies.base.Strategy`
subclass it finds together with the module-level ``CONFIG`` instance.
Strategies whose ``CONFIG.name`` is already registered (e.g. because the
operator provided their own override in ``custom_libraries.strategies``) are
skipped so user libraries always win.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from typing import TYPE_CHECKING

import structlog

from qs_trader.libraries.strategies.base import Strategy, StrategyConfig

if TYPE_CHECKING:
    from qs_trader.libraries.registry import StrategyRegistry

logger = structlog.get_logger(__name__)


def register_builtin_strategies(registry: "StrategyRegistry") -> list[str]:
    """Register every first-party strategy module shipped under this package.

    Args:
        registry: The strategy registry to populate.

    Returns:
        Sorted list of strategy names successfully registered by this call.
        Names already present in ``registry`` (e.g. user overrides) are not
        re-registered and not returned.
    """

    registered: list[str] = []
    for module_info in pkgutil.iter_modules(__path__, prefix=f"{__name__}."):
        if module_info.ispkg:
            continue
        try:
            module = importlib.import_module(module_info.name)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "strategy.builtin.import_failed",
                module=module_info.name,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            continue

        strategy_class: type[Strategy] | None = None
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is Strategy:
                continue
            if not issubclass(obj, Strategy):
                continue
            if obj.__module__ != module.__name__:
                continue
            strategy_class = obj
            break

        if strategy_class is None:
            continue

        config_obj = getattr(module, "CONFIG", None)
        if not isinstance(config_obj, StrategyConfig):
            logger.warning(
                "strategy.builtin.missing_config",
                module=module_info.name,
                strategy=strategy_class.__name__,
            )
            continue

        name = config_obj.name
        if name in registry.list_names():
            logger.debug(
                "strategy.builtin.skipped_existing",
                name=name,
                module=module_info.name,
            )
            continue

        metadata = {
            "source_type": "builtin",
            "class_name": strategy_class.__name__,
            "display_name": config_obj.display_name,
            "description": config_obj.description,
            "module": module_info.name,
        }
        registry.register(name, strategy_class, metadata, allow_override=False)
        registry._configs[name] = config_obj
        registered.append(name)
        logger.debug(
            "strategy.builtin.registered",
            name=name,
            module=module_info.name,
        )

    return sorted(registered)


__all__ = ["register_builtin_strategies"]
