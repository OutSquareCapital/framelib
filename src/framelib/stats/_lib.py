import polars as pl


def rolling_perf_factor(
    expr: pl.Expr, window_size: int, min_samples: int | None = None
) -> pl.Expr:
    return (
        expr.rolling_mean(window_size=window_size, min_samples=min_samples)
        .truediv(
            other=expr.rolling_std(window_size=window_size, min_samples=min_samples)
        )
        .mul(
            other=expr.cum_count()
            .clip(upper_bound=window_size, lower_bound=1 or min_samples)
            .log1p()
        )
    )


def rolling_midpoint(
    expr: pl.Expr, window_size: int, min_samples: int | None = None
) -> pl.Expr:
    return (
        expr.rolling_max(window_size=window_size, min_samples=min_samples).add(
            other=expr.rolling_min(window_size=window_size, min_samples=min_samples)
        )
    ).truediv(other=2.0)


def rolling_z_score(
    expr: pl.Expr, window_size: int, min_samples: int | None = None
) -> pl.Expr:
    return expr.sub(
        expr.rolling_mean(window_size=window_size, min_samples=min_samples)
    ).truediv(other=expr.rolling_std(window_size=window_size, min_samples=min_samples))


def rolling_sharpe(
    expr: pl.Expr, window_size: int, min_samples: int | None = None
) -> pl.Expr:
    return expr.rolling_mean(window_size=window_size, min_samples=min_samples).truediv(
        other=expr.rolling_std(window_size=window_size, min_samples=min_samples)
    )


def rolling_normalization(
    expr: pl.Expr, window_size: int, min_samples: int | None = None
) -> pl.Expr:
    return (
        expr.rolling_median(window_size=window_size, min_samples=min_samples)
        .truediv(
            other=expr.rolling_max(
                window_size=window_size, min_samples=min_samples
            ).sub(
                other=expr.rolling_min(window_size=window_size, min_samples=min_samples)
            )
        )
        .mul(other=2.0)
    )


def rolling_scaling(
    expr: pl.Expr, window_size: int, min_samples: int | None = None
) -> pl.Expr:
    return expr.mul(
        pl.lit(value=1, dtype=pl.Float32).truediv(
            other=expr.abs().rolling_median(
                window_size=window_size, min_samples=min_samples
            )
        )
    ).clip(lower_bound=-2, upper_bound=2)


def drawdown(expr: pl.Expr) -> pl.Expr:
    return expr.truediv(expr.cum_max()).sub(1)


def price_to_log_price(expr: pl.Expr) -> pl.Expr:
    return expr.log().sub(expr.log().first()).add(1)
