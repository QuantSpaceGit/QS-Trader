"""
Data adapters for normalizing vendor data to canonical Bar.

Adapters are auto-discovered from:
- Built-in: src/qs_trader/services/data/adapters/builtin/
- Custom: Configurable via system.yaml custom_libraries.adapters

Use AdapterRegistry to access adapters:
    from qs_trader.libraries.registry import get_adapter_registry
    registry = get_adapter_registry()
    registry.discover()
    adapter_class = registry.get("yahoo_csv")
"""

__all__: list[str] = []
