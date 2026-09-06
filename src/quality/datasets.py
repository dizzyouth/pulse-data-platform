"""Pulse policies: reuse existing event values and warehouse column contracts."""

from src.streaming.silver_streaming import SUPPORTED_EVENT_TYPES
from src.warehouse.load_gold import TABLE_SPECS
from src.quality.models import (
    AllowedValues, NullRatio, NumericBounds, Pattern, RowCount, Rule, Severity, Uniqueness,
)


GOLD_GRAINS = {
    "daily_sales": ("event_date", "country", "currency"),
    "customer_metrics": ("customer_id",),
    "product_metrics": ("product_id",),
    "funnel_metrics": ("event_date", "country"),
}


def silver_rules() -> tuple[Rule, ...]:
    return (
        RowCount(check_name="row_count", min_rows=1, severity=Severity.WARNING),
        Uniqueness(check_name="event_id_unique", columns=("event_id",)),
        *(NullRatio(check_name=f"{name}_complete", column=name) for name in
          ("event_id", "event_timestamp", "event_date", "customer_id", "session_id")),
        *(Pattern(check_name=f"{name}_nonblank", column=name, pattern=r"\S") for name in
          ("event_id", "customer_id", "session_id")),
        AllowedValues(check_name="event_type_allowed", column="event_type", values=SUPPORTED_EVENT_TYPES),
        # The current Silver classifier rejects zero as well as negative quantities.
        NumericBounds(check_name="quantity_positive", column="quantity", minimum=0, minimum_inclusive=False),
        NumericBounds(check_name="unit_price_nonnegative", column="unit_price", minimum=0),
        Pattern(check_name="country_format", column="country", pattern="^[A-Z]{2}$"),
        Pattern(check_name="currency_format", column="currency", pattern="^[A-Z]{3}$"),
    )


def gold_rules(dataset_name: str, *, layer: str = "gold") -> tuple[Rule, ...]:
    """Use layer='analytics' to assess Spark frames against serving nullability.

    Gold country/currency may legitimately be null upstream. Flag those gaps as
    readiness warnings, not critical transformation failures. PostgreSQL's
    existing contract requires them for analytics tables.
    """
    if dataset_name not in GOLD_GRAINS or layer not in ("gold", "analytics"):
        raise ValueError("Expected a known Gold dataset and gold/analytics layer")
    spec = next(spec for spec in TABLE_SPECS if spec.name == dataset_name)
    nullable = {col.name for col in spec.columns if col.nullable}
    rules: list[Rule] = [
        RowCount(check_name="row_count", min_rows=1 if layer == "analytics" else 0,
                 severity=Severity.CRITICAL if layer == "analytics" else Severity.INFO),
        Uniqueness(check_name="grain_unique", columns=GOLD_GRAINS[dataset_name]),
    ]
    for col in spec.columns:
        if not col.nullable:
            rules.append(NullRatio(check_name=f"{col.name}_complete", column=col.name,
                                   severity=Severity.WARNING if layer == "gold" and col.name in ("country", "currency") else Severity.CRITICAL))
    for name in spec.nonnegative_columns:
        rules.append(NumericBounds(check_name=f"{name}_nonnegative", column=name,
                                   minimum=0, allow_null=name in nullable))
    for name in spec.rate_columns:
        rules.append(NumericBounds(check_name=f"{name}_range", column=name,
                                   minimum=0, maximum=1, allow_null=True))
    return tuple(rules)
