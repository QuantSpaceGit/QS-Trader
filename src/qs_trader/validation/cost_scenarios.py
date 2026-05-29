"""Cost-scenario override application for the OOS validation framework.

A *cost scenario* is a named bundle of dot-notation overrides applied on top of
a base :class:`~qs_trader.engine.config.BacktestConfig` to produce a per-scenario
child config.  Overrides are applied as a **pure function**: the input config is
never mutated and the returned :class:`~qs_trader.engine.config.BacktestConfig`
is a fully-revalidated, immutable model instance.

Schema validation of override key paths against the live ``BacktestConfig``
schema lives in :func:`validate_override_path`; it is called from the
:class:`~qs_trader.validation.plan.ValidationPlan` model validator at plan-load
time so unknown paths are rejected before any execution (CLI dry-run included).
"""

from __future__ import annotations

import types
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin

from pydantic import BaseModel

if TYPE_CHECKING:
    from qs_trader.engine.config import BacktestConfig

__all__ = ["apply_scenario_overrides", "validate_override_path"]


def _unwrap_optional(annot: Any) -> Any:
    """Return the non-None argument of an ``Optional[X]`` annotation, else ``annot``.

    For a true ``Union[ModelA, ModelB, ...]`` (multiple non-None branches), a
    list of candidates is returned and the caller (:func:`validate_override_path`)
    descends into the **first** ``BaseModel`` candidate.  This heuristic is
    acceptable because :class:`~qs_trader.engine.config.BacktestConfig` does
    not currently use discriminated unions of multiple submodels at the same
    key.  If such a union is later introduced the resolver will need updating;
    track as a known limitation.
    """
    origin = get_origin(annot)
    if origin in (Union, types.UnionType):
        non_none = [a for a in get_args(annot) if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
        return non_none  # multiple non-None branches; caller handles
    return annot


def validate_override_path(model_cls: type[BaseModel], path: str) -> None:
    """Walk a dot-notation path against a Pydantic model schema.

    Raises:
        ValueError: When any segment of ``path`` does not correspond to a field
            on the current model, or when the path attempts to descend below a
            non-model leaf.
    """
    if not path or path.startswith(".") or path.endswith(".") or ".." in path:
        # Use the catalog reason-code prefix for all invalid-key conditions so
        # downstream consumers can pattern-match a single prefix (§4 of the
        # requirement doc).  The empty / malformed path is rendered verbatim
        # (an empty string falls back to the literal ``""`` marker).
        rendered = path if path else '""'
        raise ValueError(f"unknown_override_key:{rendered} (invalid path: empty segment or leading/trailing dot)")

    parts = path.split(".")
    current: type[BaseModel] = model_cls
    for i, part in enumerate(parts):
        if not hasattr(current, "model_fields") or part not in current.model_fields:
            raise ValueError(f"unknown_override_key:{path} ('{part}' is not a field of {current.__name__})")
        if i == len(parts) - 1:
            return
        annot = current.model_fields[part].annotation
        unwrapped = _unwrap_optional(annot)
        candidates: list[Any] = unwrapped if isinstance(unwrapped, list) else [unwrapped]
        next_model: type[BaseModel] | None = None
        for cand in candidates:
            if isinstance(cand, type) and issubclass(cand, BaseModel):
                next_model = cand
                break
        if next_model is None:
            raise ValueError(
                f"unknown_override_key:{path} ('{part}' on {current.__name__} is not a nested model; "
                "cannot descend further)"
            )
        current = next_model


def apply_scenario_overrides(
    base_config: "BacktestConfig",
    overrides: Mapping[str, Any],
) -> "BacktestConfig":
    """Return a new ``BacktestConfig`` with dot-notation overrides applied.

    The input ``base_config`` is never mutated.  When ``overrides`` is empty
    the original instance is returned unchanged (``BacktestConfig`` is
    immutable, so identity-preservation is safe).

    Args:
        base_config: Source config to derive from.
        overrides: Mapping of dot-notation paths (e.g. ``"feature_config.feature_version"``)
                   to their override values.

    Returns:
        A fully-revalidated new :class:`~qs_trader.engine.config.BacktestConfig`.

    Raises:
        ValidationError: If the merged result does not satisfy ``BacktestConfig``.
        ValueError: If any override path is structurally invalid.
    """
    from qs_trader.engine.config import BacktestConfig  # noqa: PLC0415

    if not overrides:
        return base_config

    data: dict[str, Any] = base_config.model_dump(mode="python")
    for path, value in overrides.items():
        parts = path.split(".")
        cursor: dict[str, Any] = data
        for segment in parts[:-1]:
            existing = cursor.get(segment)
            # Defensive: replace any non-dict intermediate with a fresh dict so
            # the override can land at a deeper path.  Path validity (each
            # segment corresponds to a real ``BaseModel`` field on
            # ``BacktestConfig``) is pre-checked at plan-load time by
            # ``validate_override_path``; in normal operation
            # ``model_dump(mode="python")`` always materialises nested
            # ``BaseModel`` fields as dicts, so this branch only fires when
            # ``existing`` is ``None`` (optional submodel left unset) — never
            # when it is a populated scalar leaf.
            if not isinstance(existing, dict):
                cursor[segment] = {}
            cursor = cursor[segment]
        cursor[parts[-1]] = value

    return BacktestConfig.model_validate(data)
