"""Canonical lifecycle event models for the Phase 2 append-only ledger."""

from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from typing import Any, ClassVar, Optional

import jsonschema
from jsonschema import Draft202012Validator
from pydantic import Field, field_serializer, model_validator

from qs_trader.events.events import RESERVED_ENVELOPE_KEYS, BaseEvent, load_and_compile_schema

LIFECYCLE_RESERVED_ENVELOPE_KEYS = RESERVED_ENVELOPE_KEYS | {"experiment_id", "run_id", "sleeve_id"}


@lru_cache(maxsize=8)
def load_lifecycle_envelope_schema() -> Draft202012Validator:
    """Load and compile the lifecycle envelope schema."""
    return load_and_compile_schema("lifecycle-envelope.v1.json")


def _serialize_decimal(value: Decimal | None) -> str | None:
    """Serialize Decimal-like values without losing precision."""
    if value is None:
        return None
    return format(value, "f")


def _serialize_required_decimal(value: Decimal) -> str:
    """Serialize a required Decimal without losing precision."""
    return format(value, "f")


class LifecycleBaseEvent(BaseEvent):
    """Base event with the extended lifecycle envelope."""

    experiment_id: str
    run_id: str
    sleeve_id: str | None = None

    @model_validator(mode="after")
    def _validate_lifecycle_envelope(self) -> "LifecycleBaseEvent":
        """Validate lifecycle-specific envelope fields."""
        envelope_validator = load_lifecycle_envelope_schema()
        envelope_data = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "occurred_at": self.occurred_at.isoformat().replace("+00:00", "Z"),
            "source_service": self.source_service,
            "experiment_id": self.experiment_id,
            "run_id": self.run_id,
        }
        if self.correlation_id is not None:
            envelope_data["correlation_id"] = self.correlation_id
        if self.causation_id is not None:
            envelope_data["causation_id"] = self.causation_id
        if self.sleeve_id is not None:
            envelope_data["sleeve_id"] = self.sleeve_id

        try:
            envelope_validator.validate(envelope_data)
        except jsonschema.ValidationError as exc:
            raise ValueError(
                f"{self.__class__.__name__} lifecycle envelope validation failed: {exc.message}\n"
                f"Path: {list(exc.path)}\n"
                f"Schema path: {list(exc.schema_path)}"
            ) from exc

        return self


class LifecycleValidatedEvent(LifecycleBaseEvent):
    """Lifecycle event that validates payload fields against a JSON schema."""

    SCHEMA_BASE: ClassVar[Optional[str]] = None
    TYPE_FIELD: ClassVar[str]
    PRICE_BASIS_FIELD: ClassVar[str | None] = None

    def get_lifecycle_type(self) -> str:
        """Return the canonical lifecycle-type discriminator for ledger rows."""
        return str(getattr(self, self.TYPE_FIELD))

    def get_lifecycle_price_basis(self) -> str | None:
        """Return the event's explicit price basis for indexed ledger queries."""
        if self.PRICE_BASIS_FIELD is None:
            return None
        price_basis = getattr(self, self.PRICE_BASIS_FIELD)
        return None if price_basis is None else str(price_basis)

    @model_validator(mode="after")
    def _validate_payload(self) -> "LifecycleValidatedEvent":
        """Validate payload fields against the lifecycle JSON schema."""
        if self.SCHEMA_BASE is None:
            raise ValueError(f"{self.__class__.__name__} must specify SCHEMA_BASE")

        schema_base_name = self.SCHEMA_BASE.split("/")[-1]
        if self.event_type != schema_base_name:
            raise ValueError(
                f"{self.__class__.__name__}: event_type '{self.event_type}' must equal contract name "
                f"'{schema_base_name}' (from SCHEMA_BASE '{self.SCHEMA_BASE}')"
            )

        data = self.model_dump()
        payload_data = {k: v for k, v in data.items() if k not in LIFECYCLE_RESERVED_ENVELOPE_KEYS}
        schema_file = f"{self.SCHEMA_BASE}.v{self.event_version}.json"

        try:
            validator = load_and_compile_schema(schema_file)
            validator.validate(payload_data)
        except FileNotFoundError as exc:
            raise ValueError(f"{self.__class__.__name__}: Schema not found: {schema_file}") from exc
        except jsonschema.ValidationError as exc:
            raise ValueError(
                f"{self.__class__.__name__} payload validation failed against {schema_file}: {exc.message}\n"
                f"Path: {list(exc.path)}\n"
                f"Schema path: {list(exc.schema_path)}\n"
                f"Failed value: {exc.instance}"
            ) from exc

        return self


class StrategyDecisionEvent(LifecycleValidatedEvent):
    """Canonical record of the strategy's decision for a bar."""

    SCHEMA_BASE: ClassVar[Optional[str]] = "lifecycle/strategy_decision"
    TYPE_FIELD: ClassVar[str] = "decision_type"
    PRICE_BASIS_FIELD: ClassVar[str | None] = "decision_basis"

    event_type: str = "strategy_decision"

    decision_id: str
    strategy_id: str
    symbol: str
    bar_timestamp: str
    decision_type: str
    decision_price: Decimal
    decision_basis: str
    confidence: Decimal
    indicator_context: dict[str, Any] | None = None
    reason: str | None = None
    metadata: dict[str, Any] | None = None

    @field_serializer("decision_price", "confidence")
    def _serialize_decimals(self, value: Decimal) -> str:
        return _serialize_required_decimal(value)


class OrderIntentEvent(LifecycleValidatedEvent):
    """Canonical manager intent before any order is sent to execution."""

    SCHEMA_BASE: ClassVar[Optional[str]] = "lifecycle/order_intent"
    TYPE_FIELD: ClassVar[str] = "intent_state"
    PRICE_BASIS_FIELD: ClassVar[str | None] = "price_basis"

    event_type: str = "order_intent"

    intent_id: str
    strategy_id: str
    symbol: str
    intent_type: str
    intent_state: str
    direction: str
    target_quantity: Decimal | None = None
    price_basis: str | None = None
    suppression_reason: str | None = None
    cancellation_reason: str | None = None

    @field_serializer("target_quantity")
    def _serialize_target_quantity(self, value: Decimal | None) -> str | None:
        return _serialize_decimal(value)


class OrderLifecycleEvent(LifecycleValidatedEvent):
    """Canonical append-only order state transition."""

    SCHEMA_BASE: ClassVar[Optional[str]] = "lifecycle/order_lifecycle"
    TYPE_FIELD: ClassVar[str] = "order_state"
    PRICE_BASIS_FIELD: ClassVar[str | None] = "price_basis"

    event_type: str = "order_lifecycle"

    order_id: str
    intent_id: str | None = None
    strategy_id: str
    symbol: str
    order_state: str
    side: str
    quantity: Decimal
    filled_quantity: Decimal
    order_type: str
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    time_in_force: str = "GTC"
    price_basis: str | None = None
    idempotency_key: str
    rejection_reason: str | None = None
    expiry_reason: str | None = None
    cancellation_reason: str | None = None

    @field_serializer("quantity", "filled_quantity", "limit_price", "stop_price")
    def _serialize_decimals(self, value: Decimal | None) -> str | None:
        return _serialize_decimal(value)


class FillLifecycleEvent(LifecycleValidatedEvent):
    """Canonical record for each execution fill."""

    SCHEMA_BASE: ClassVar[Optional[str]] = "lifecycle/fill_lifecycle"
    TYPE_FIELD: ClassVar[str] = "fill_state"
    PRICE_BASIS_FIELD: ClassVar[str | None] = "price_basis"

    event_type: str = "fill_lifecycle"

    fill_id: str
    order_id: str
    intent_id: str | None = None
    strategy_id: str
    symbol: str
    fill_state: str = "filled"
    side: str
    filled_quantity: Decimal
    fill_price: Decimal
    price_basis: str
    commission: Decimal = Decimal("0")
    slippage_bps: int | None = None
    gross_value: Decimal | None = None
    net_value: Decimal | None = None
    order_received_at: str | None = None

    @field_serializer("filled_quantity", "fill_price", "commission", "gross_value", "net_value")
    def _serialize_decimals(self, value: Decimal | None) -> str | None:
        return _serialize_decimal(value)


class TradeLifecycleEvent(LifecycleValidatedEvent):
    """Canonical trade lifecycle transition event."""

    SCHEMA_BASE: ClassVar[Optional[str]] = "lifecycle/trade_lifecycle"
    TYPE_FIELD: ClassVar[str] = "trade_state"
    PRICE_BASIS_FIELD: ClassVar[str | None] = "price_basis"

    event_type: str = "trade_lifecycle"

    trade_id: str
    strategy_id: str
    symbol: str
    trade_state: str
    side: str
    open_quantity: Decimal
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    commission_total: Decimal
    entry_timestamp: str | None = None
    exit_timestamp: str | None = None
    fill_ids: list[str] = Field(default_factory=list)
    price_basis: str
    entry_basis: str
    exit_basis: str | None = None
    is_scale_in: bool = False
    causation_fill_id: str | None = None

    @field_serializer(
        "open_quantity",
        "entry_price",
        "exit_price",
        "realized_pnl",
        "unrealized_pnl",
        "commission_total",
    )
    def _serialize_decimals(self, value: Decimal | None) -> str | None:
        return _serialize_decimal(value)


class PositionLifecycleEvent(LifecycleValidatedEvent):
    """Canonical state for a (strategy, symbol) position."""

    SCHEMA_BASE: ClassVar[Optional[str]] = "lifecycle/position_lifecycle"
    TYPE_FIELD: ClassVar[str] = "position_state"
    PRICE_BASIS_FIELD: ClassVar[str | None] = "price_basis"

    event_type: str = "position_lifecycle"

    position_key: str
    strategy_id: str
    symbol: str
    position_state: str
    side: str
    quantity: Decimal
    average_cost: Decimal | None = None
    market_value: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    as_of_price: Decimal | None = None
    price_basis: str
    open_trade_ids: list[str] = Field(default_factory=list)
    transition_reason: str | None = None

    @field_serializer("quantity", "average_cost", "market_value", "unrealized_pnl", "as_of_price")
    def _serialize_decimals(self, value: Decimal | None) -> str | None:
        return _serialize_decimal(value)

    @model_validator(mode="after")
    def _validate_price_basis_contract(self) -> "PositionLifecycleEvent":
        """Enforce the non-null/sentinel price-basis invariant."""
        if self.as_of_price is None and self.price_basis != "none":
            raise ValueError("PositionLifecycleEvent requires price_basis='none' when as_of_price is null")
        if self.as_of_price is not None and self.price_basis == "none":
            raise ValueError("PositionLifecycleEvent price_basis cannot be 'none' when as_of_price is populated")
        return self


class PortfolioLifecycleEvent(LifecycleValidatedEvent):
    """Canonical portfolio snapshot tagged by its triggering lifecycle event."""

    SCHEMA_BASE: ClassVar[Optional[str]] = "lifecycle/portfolio_lifecycle"
    TYPE_FIELD: ClassVar[str] = "lifecycle_type"
    PRICE_BASIS_FIELD: ClassVar[str | None] = "price_basis"

    event_type: str = "portfolio_lifecycle"

    portfolio_id: str
    lifecycle_type: str
    bar_timestamp: str
    reporting_currency: str
    cash_balance: Decimal
    total_market_value: Decimal
    total_portfolio_equity: Decimal
    initial_portfolio_equity: Decimal
    total_unrealized_pnl: Decimal
    total_realized_pnl: Decimal
    total_pnl: Decimal
    long_exposure: Decimal
    short_exposure: Decimal
    net_exposure: Decimal
    gross_exposure: Decimal
    leverage: Decimal
    price_basis: str
    num_open_positions: int

    @field_serializer(
        "cash_balance",
        "total_market_value",
        "total_portfolio_equity",
        "initial_portfolio_equity",
        "total_unrealized_pnl",
        "total_realized_pnl",
        "total_pnl",
        "long_exposure",
        "short_exposure",
        "net_exposure",
        "gross_exposure",
        "leverage",
    )
    def _serialize_decimals(self, value: Decimal) -> str:
        return _serialize_required_decimal(value)


__all__ = [
    "LifecycleBaseEvent",
    "LifecycleValidatedEvent",
    "StrategyDecisionEvent",
    "OrderIntentEvent",
    "OrderLifecycleEvent",
    "FillLifecycleEvent",
    "TradeLifecycleEvent",
    "PositionLifecycleEvent",
    "PortfolioLifecycleEvent",
    "load_lifecycle_envelope_schema",
]
