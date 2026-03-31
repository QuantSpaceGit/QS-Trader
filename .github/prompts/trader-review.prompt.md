---
description: Trader: Python code review — types, correctness, architecture, performance
---

Review the code as a senior Python engineer specialising in quantitative trading systems.

Check for:

- **Type safety**: missing type annotations, implicit `Any`, incorrect return types, use of `# type: ignore` without justification
- **Python correctness**: mutable default arguments, incorrect use of `__init__` vs `__post_init__`, improper exception handling (`except Exception` without re-raise or logging)
- **Domain logic**: incorrect position sizing or PnL calculations, wrong fill/order lifecycle transitions, misapplied event handling in the engine loop
- **Architecture**: tight coupling between services and engine, business logic leaking into CLI or config layers, missing abstraction at service boundaries
- **Data handling**: incorrect use of pandas/numpy (chained indexing, silent type coercions, NaN propagation), date/timezone handling errors, off-by-one errors on OHLCV bar alignment
- **Config & contracts**: schema fields not matching their usage in engine or services, missing Pydantic validators, unsafe config defaults
- **Concurrency & state**: shared mutable state between strategy and portfolio services, non-deterministic iteration over dicts or sets
- **Security**: hardcoded credentials or secrets in config/code, unsafe `eval`/`exec` usage, unvalidated external data fed directly into calculations
- **Testing surface**: untestable code due to hidden dependencies, missing dependency injection points, logic embedded in constructors
- **Logging & observability**: silent failures, missing log context (symbol, date, strategy name), overly verbose debug logs in hot paths
- **Logical bugs**: incorrect conditional logic, wrong comparison operators (e.g. `is` vs `==`), silent integer truncation
- **Edge cases**: empty dataframes, missing bars, strategies with zero positions, untested warmup period behaviour
- If more information is provided, adjust to the context of the code being reviewed.

Return only actionable recommendations in a bullet list format like:

- Finding (severity): Description of the issue.
- Location: File path and line number (if applicable).
- Recommendation: Suggested fix or improvement.

If the code is correct, explicitly confirm it.
