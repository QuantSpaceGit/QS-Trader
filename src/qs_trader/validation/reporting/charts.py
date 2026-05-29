"""Matplotlib chart generators for validation reports.

Matplotlib is imported lazily inside function bodies to avoid import-time
cost and to support environments without a display.

Set the ``MPLCONFIGDIR`` environment variable to a writable directory to
redirect font-cache writes in CI / headless environments::

    export MPLCONFIGDIR=/tmp/mpl_cache

The ``Agg`` (non-interactive PNG) backend is selected automatically
before any pyplot call, so no display server is required.
"""

from __future__ import annotations

__all__ = ["generate_equity_overlay"]


def generate_equity_overlay(
    strategy_equity: list[float],
    benchmark_equity: list[float],
    fold_boundaries: list[int],
    title: str = "Strategy vs Benchmark",
) -> bytes:
    """Return a PNG byte string of the equity overlay chart.

    Args:
        strategy_equity: Cumulative equity curve for the strategy,
            one point per OOS bar.
        benchmark_equity: Cumulative equity curve for the benchmark,
            same length as *strategy_equity*.
        fold_boundaries: Bar indices where fold boundaries should be
            drawn as dashed vertical lines.
        title: Chart title displayed at the top of the figure.

    Returns:
        Raw PNG bytes suitable for embedding in an HTML report via a
        ``data:image/png;base64,…`` URI.

    Raises:
        ValueError: When *strategy_equity* and *benchmark_equity* differ in
            length, or when any value in *fold_boundaries* is outside the
            valid index range ``[0, len(strategy_equity))``.
        ImportError: When ``matplotlib`` is not installed.  Add
            ``matplotlib>=3.7,<4`` to your environment dependencies to
            resolve this.
    """
    if len(strategy_equity) != len(benchmark_equity):
        raise ValueError(
            f"strategy_equity and benchmark_equity must have equal length "
            f"({len(strategy_equity)} vs {len(benchmark_equity)})"
        )
    n = len(strategy_equity)
    for b in fold_boundaries:
        if not (0 <= b < n):
            raise ValueError(f"fold_boundaries value {b!r} is out of range for equity series of length {n}")
    try:
        import matplotlib  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "matplotlib is required for equity overlay charts. "
            "Add 'matplotlib>=3.7,<4' to your environment dependencies."
        ) from exc

    import io  # noqa: PLC0415

    # Select non-interactive PNG backend before importing pyplot.
    # Use non-interactive backend; idempotent if called again in same process.
    matplotlib.use("Agg")

    import matplotlib.pyplot as plt  # noqa: PLC0415  # type: ignore[import-untyped]

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor("#0f172a")
    ax.set_facecolor("#0f172a")

    xs = list(range(len(strategy_equity)))
    ax.plot(xs, strategy_equity, color="#60a5fa", linewidth=1.5, label="Strategy")
    ax.plot(xs, benchmark_equity, color="#94a3b8", linewidth=1.5, label="Benchmark")

    for boundary in fold_boundaries:
        ax.axvline(x=boundary, color="#475569", linestyle="--", linewidth=0.8)

    ax.set_title(title, color="#e2e8f0", fontsize=11, pad=10)
    ax.tick_params(colors="#475569")
    ax.spines["bottom"].set_color("#334155")
    ax.spines["left"].set_color("#334155")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.label.set_color("#94a3b8")
    ax.xaxis.label.set_color("#94a3b8")
    ax.legend(
        facecolor="#1e293b",
        edgecolor="#334155",
        labelcolor="#e2e8f0",
        fontsize=9,
    )

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, bbox_inches="tight", facecolor="#0f172a")
    plt.close(fig)
    buf.seek(0)
    return buf.read()
