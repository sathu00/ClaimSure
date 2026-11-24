"""
gold.py - build analytics-ready datasets from the SILVER layer.

Gold tables are small, business-friendly and contain NO personal data
(no claimant names or addresses - only city/state).

    claims_enriched   one row per valid claim, joined to policy, claimant (city/state)
                      and a payment summary. The "fact table" for most queries.
    claims_by_status  claim count and amounts per claim_status
    claims_by_type    claim count and amounts per claim_type
    monthly_claims    claims per loss month (+ average reporting delay)
    monthly_payments  payments per payment month and payment status
    claims_by_city    claims per state / city
"""

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

MONEY = DecimalType(18, 2)


def _money(col) -> "F.Column":
    return col.cast(MONEY)


def build_gold(
    claims: DataFrame,
    policies: DataFrame,
    claimants: DataFrame,
    payments: DataFrame,
) -> Dict[str, DataFrame]:
    gold: Dict[str, DataFrame] = {}

    # ---- claims_enriched ---------------------------------------------------
    pay_summary = payments.groupBy("claim_id").agg(
        F.count(F.lit(1)).alias("payment_count"),
        F.sum(F.when(F.col("payment_status") == "Processed", F.col("payment_amount"))).alias("_processed"),
        F.sum(F.when(F.col("payment_status") == "Pending", F.col("payment_amount"))).alias("_pending"),
        F.sum(F.when(F.col("payment_status") == "Failed", F.col("payment_amount"))).alias("_failed"),
    )
    pol = policies.select(
        "policy_id", "policy_type", "policy_status", "coverage_amount", "premium_amount",
        F.col("is_date_range_valid").alias("policy_dates_valid"),
    )
    clm = claimants.select("claimant_id", "city", "state")   # no names / addresses

    enriched = (
        claims.join(pol, "policy_id", "left")
        .join(clm, "claimant_id", "left")
        .join(pay_summary, "claim_id", "left")
        .select(
            "claim_id", "policy_id", "claimant_id",
            "loss_date",
            F.date_format("loss_date", "yyyy-MM").alias("loss_month"),
            "reported_date", "report_lag_days",
            "claim_type", "claim_status",
            "claim_amount", "reserve_amount", "source_system",
            "policy_type", "policy_status", "coverage_amount", "premium_amount", "policy_dates_valid",
            (F.col("claim_amount") > F.col("coverage_amount")).alias("claim_exceeds_coverage"),
            "city", "state",
            F.coalesce(F.col("payment_count"), F.lit(0)).alias("payment_count"),
            _money(F.coalesce(F.col("_processed"), F.lit(0))).alias("total_processed_amount"),
            _money(F.coalesce(F.col("_pending"), F.lit(0))).alias("total_pending_amount"),
            _money(F.coalesce(F.col("_failed"), F.lit(0))).alias("total_failed_amount"),
        )
    )
    gold["claims_enriched"] = enriched

    # ---- simple aggregates (built from silver claims) ------------------------
    gold["claims_by_status"] = claims.groupBy("claim_status").agg(
        F.count(F.lit(1)).alias("claim_count"),
        _money(F.sum("claim_amount")).alias("total_claim_amount"),
        _money(F.avg("claim_amount")).alias("avg_claim_amount"),
        _money(F.sum("reserve_amount")).alias("total_reserve_amount"),
    )

    gold["claims_by_type"] = claims.groupBy("claim_type").agg(
        F.count(F.lit(1)).alias("claim_count"),
        _money(F.sum("claim_amount")).alias("total_claim_amount"),
        _money(F.avg("claim_amount")).alias("avg_claim_amount"),
        _money(F.max("claim_amount")).alias("max_claim_amount"),
    )

    gold["monthly_claims"] = (
        claims.withColumn("loss_month", F.date_format("loss_date", "yyyy-MM"))
        .groupBy("loss_month")
        .agg(
            F.count(F.lit(1)).alias("claim_count"),
            _money(F.sum("claim_amount")).alias("total_claim_amount"),
            F.avg("report_lag_days").cast(DecimalType(10, 2)).alias("avg_report_lag_days"),
        )
    )

    gold["monthly_payments"] = (
        payments.withColumn("payment_month", F.date_format("payment_date", "yyyy-MM"))
        .groupBy("payment_month", "payment_status")
        .agg(
            F.count(F.lit(1)).alias("payment_count"),
            _money(F.sum("payment_amount")).alias("total_payment_amount"),
        )
    )

    gold["claims_by_city"] = (
        claims.join(clm, "claimant_id", "left")
        .groupBy("state", "city")
        .agg(
            F.count(F.lit(1)).alias("claim_count"),
            _money(F.sum("claim_amount")).alias("total_claim_amount"),
            _money(F.avg("claim_amount")).alias("avg_claim_amount"),
        )
    )
    return gold