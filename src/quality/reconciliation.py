"""Count reconciliation for matching, stable, bounded pipeline snapshots."""

from dataclasses import replace

from pyspark.sql import DataFrame, functions as F

from src.analytics.gold_build import GoldTables
from src.streaming.silver_streaming import classify_silver_events
from src.quality.checks import result
from src.quality.models import CheckDetails, QualityContext, QualityResult, Rule


def _require_snapshot(bounded_snapshot: bool, *frames: DataFrame) -> None:
    if not bounded_snapshot or any(frame.isStreaming for frame in frames):
        raise ValueError("Reconciliation requires explicit bounded_snapshot=True and matching batch inputs")


def reconcile_bronze_silver(
    bronze_valid: DataFrame, silver_valid: DataFrame, silver_rejected: DataFrame,
    context: QualityContext, *, bounded_snapshot: bool = False,
) -> list[QualityResult]:
    """Reuse Silver classification; account for removed valid event-ID duplicates.

    This is a count check, not row/content equivalence. It is not valid for
    independent streaming checkpoints, watermark eviction, or overlapping runs.
    """
    _require_snapshot(bounded_snapshot, bronze_valid, silver_valid, silver_rejected)
    classified = classify_silver_events(bronze_valid, deduplicate=False)
    expected = classified.valid.agg(
        F.count(F.lit(1)).alias("rows"), F.countDistinct("event_id").alias("distinct_ids")
    ).first()
    rejected = classified.rejected.count()
    removed = expected["rows"] - expected["distinct_ids"]
    results = []
    for name, actual, required in (
        ("silver_valid", silver_valid.count(), expected["distinct_ids"]),
        ("silver_rejected", silver_rejected.count(), rejected),
    ):
        results.append(result(
            Rule(check_name=f"{name}_snapshot_count"), replace(context, dataset_name=name, layer="silver"),
            metric="row_count", observed=actual, expected=required, passed=actual == required,
            details=CheckDetails(row_count=actual, deduplicated_rows=removed,
                                 reference_count=expected["rows"] + rejected,
                                 message="Expected from the existing Silver classifier; rejected rows are not deduplicated"),
        ))
    return results


def reconcile_silver_gold(
    silver_valid: DataFrame, gold: GoldTables, context: QualityContext,
    *, bounded_snapshot: bool = False,
) -> list[QualityResult]:
    """Detect empty Gold outputs only when that table has eligible Silver input."""
    _require_snapshot(bounded_snapshot, silver_valid, gold.daily_sales, gold.customer_metrics,
                      gold.product_metrics, gold.funnel_metrics)
    source = silver_valid.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count(F.when(F.col("event_type") == "payment_completed", 1)).alias("payments"),
        F.count("product_id").alias("products"),
    ).first()
    eligible = {"daily_sales": source["payments"], "customer_metrics": source["rows"],
                "product_metrics": source["products"], "funnel_metrics": source["rows"]}
    results = []
    for name, input_count in eligible.items():
        rows = getattr(gold, name).count()
        minimum = int(input_count > 0)
        results.append(result(
            Rule(check_name="eligible_input_has_output"), replace(context, dataset_name=name, layer="gold"),
            metric="row_count", observed=rows, expected=minimum, passed=rows >= minimum,
            details=CheckDetails(row_count=rows, reference_count=input_count,
                                 message="Nonempty output required only for eligible input; aggregate row counts need not equal event counts"),
        ))
    return results
