"""
System configuration package.

Provides consolidated system-level configuration for all services.
Philosophy: "In real life, the system is one" - one configuration for the entire system.

Exports:
    - SystemConfig: Complete system configuration dataclass
    - get_system_config: Get system config singleton
    - reload_system_config: Force reload system config
    - LoggerFactory: Factory for creating configured loggers
    - LoggingConfig: Logging configuration model
"""

from qs_trader.system.config import SystemConfig, get_system_config, reload_system_config
from qs_trader.system.log_system import LoggerFactory, LoggingConfig

__all__ = [
    "SystemConfig",
    "get_system_config",
    "reload_system_config",
    "LoggerFactory",
    "LoggingConfig",
]
