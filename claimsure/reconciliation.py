"""
reconciliation.py - source-to-target reconciliation and the data-quality summary.

The idea: nothing may be silently lost or invented between layers.

    BRONZE (source)  ==  SILVER + QUARANTINE          (rows, distinct keys, amounts)
    SILVER           ==  GOLD aggregates              (counts and amounts tie out)
    SILVER           has no broken links / duplicates (referential + uniqueness checks)

Each check compares an EXPECTED value with an ACTUAL value. The job writes every
check to the audit layer and (by default) FAILS if any check does not match.
The target numbers are measured on what was actually WRITTEN to storage
(the job re-reads the Parquet), not on in-memory DataFrames.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from claimsure.config import RULES, RunContext, TABLES

KEY_COLUMN = {
    "claims": "claim_id",
    "policies": "policy_id",
    "claimants": "claimant_id",
    "payments": "payment_id",
}
MONEY = DecimalType(18, 2)
TWO_PLACES = Decimal("0.01")


@dataclass
class Check:
    group: str
    name: str
    expected: Decimal
    actual: Decimal

    @property
    def difference(self) -> Decimal:
        return self.actual - self.expected

    @property
    def status(self) -> str:
        return "PASS" if self.difference == 0 else "FAIL"


# --------------------------------------------------------------------------- #
# tiny helpers that pull ONE number out of a DataFrame
# --------------------------------------------------------------------------- #
def _count(df: DataFrame) -> Decimal:
    return Decimal(df.count())


def _sum(df: DataFrame, col: str) -> Decimal:
    value = df.agg(F.sum(col)).collect()[0][0]
    return Decimal(value) if value is not None else Decimal(0)


def _text_to_money(col: str):
    """Cast a raw TEXT amount to decimal; non-numeric text becomes NULL (ignored by SUM)."""
    return F.trim(F.col(col)).cast(MONEY)


def _clean_key(col: str):
    t = F.trim(F.col(col))
    return F.when(t == "", F.lit(None).cast("string")).otherwise(t)


# --------------------------------------------------------------------------- #
# the checks
# --------------------------------------------------------------------------- #
def build_checks(
    raw: Dict[str, DataFrame],
    silver: Dict[str, DataFrame],
    quarantine: Dict[str, DataFrame],
    gold: Dict[str, DataFrame],
) -> List[Check]:
    checks: List[Check] = []

    # 1) ROW COUNTS and 2) DISTINCT KEYS: bronze == silver + quarantine
    for t in TABLES:
        checks.append(
            Check("ROW_COUNT", f"{t}: bronze rows = silver + quarantine rows",
                  _count(raw[t]), _count(silver[t]) + _count(quarantine[t]))
        )
        key = KEY_COLUMN[t]
        src_keys = raw[t].select(_clean_key(key).alias("k")).where(F.col("k").isNotNull()).distinct()
        tgt_keys = (
            silver[t].select(F.col(key).alias("k"))
            .union(quarantine[t].select(_clean_key(key).alias("k")))
            .where(F.col("k").isNotNull())
            .distinct()
        )
        checks.append(Check("DISTINCT_KEYS", f"{t}: distinct {key} in bronze = silver + quarantine",
                            _count(src_keys), _count(tgt_keys)))

    # 3) AMOUNTS: the money must add up too
    for t, col in (("claims", "claim_amount"), ("payments", "payment_amount")):
        src = _sum(raw[t].select(_text_to_money(col).alias(col)), col)
        tgt = _sum(silver[t], col) + _sum(quarantine[t].select(_text_to_money(col).alias(col)), col)
        checks.append(Check("AMOUNT", f"{t}: sum({col}) bronze = silver + quarantine", src, tgt))

    # 4) UNIQUENESS of primary keys in silver (expected 0 duplicates)
    for t in TABLES:
        key = KEY_COLUMN[t]
        dupes = _count(silver[t]) - _count(silver[t].select(key).distinct())
        checks.append(Check("UNIQUENESS", f"silver_{t}: duplicate {key} values", Decimal(0), dupes))

    # 5) REFERENTIAL INTEGRITY inside silver (expected 0 orphans)
    orphan_pol = silver["claims"].join(silver["policies"], "policy_id", "left_anti")
    orphan_clm = silver["claims"].join(silver["claimants"], "claimant_id", "left_anti")
    orphan_pay = silver["payments"].join(silver["claims"], "claim_id", "left_anti")
    checks += [
        Check("REFERENTIAL", "silver claims with policy_id not in silver policies", Decimal(0), _count(orphan_pol)),
        Check("REFERENTIAL", "silver claims with claimant_id not in silver claimants", Decimal(0), _count(orphan_clm)),
        Check("REFERENTIAL", "silver payments with claim_id not in silver claims", Decimal(0), _count(orphan_pay)),
    ]

    # 6) GOLD tie-out: aggregates must add back up to silver
    s_claims_n, s_claims_amt = _count(silver["claims"]), _sum(silver["claims"], "claim_amount")
    checks.append(Check("GOLD_TIEOUT", "gold_claims_enriched rows = silver claims rows",
                        s_claims_n, _count(gold["claims_enriched"])))
    for g in ("claims_by_status", "claims_by_type", "monthly_claims", "claims_by_city"):
        checks.append(Check("GOLD_TIEOUT", f"gold_{g}: sum(claim_count) = silver claims rows",
                            s_claims_n, _sum(gold[g], "claim_count")))
        checks.append(Check("GOLD_TIEOUT", f"gold_{g}: sum(total_claim_amount) = silver claim_amount",
                            s_claims_amt, _sum(gold[g], "total_claim_amount")))
    checks.append(Check("GOLD_TIEOUT", "gold_monthly_payments: sum(payment_count) = silver payments rows",
                        _count(silver["payments"]), _sum(gold["monthly_payments"], "payment_count")))
    checks.append(Check("GOLD_TIEOUT", "gold_monthly_payments: sum(total_payment_amount) = silver payment_amount",
                        _sum(silver["payments"], "payment_amount"), _sum(gold["monthly_payments"], "total_payment_amount")))
    return checks


def checks_to_df(spark: SparkSession, checks: List[Check], ctx: RunContext) -> DataFrame:
    """Turn the Python list of checks into a DataFrame for the audit layer."""
    q = lambda d: d.quantize(TWO_PLACES)  # noqa: E731
    schema = StructType(
        [
            StructField("run_id", StringType()),
            StructField("run_ts", StringType()),
            StructField("check_group", StringType()),
            StructField("check_name", StringType()),
            StructField("expected_value", DecimalType(20, 2)),
            StructField("actual_value", DecimalType(20, 2)),
            StructField("difference", DecimalType(20, 2)),
            StructField("status", StringType()),
        ]
    )
    rows = [
        (ctx.run_id, ctx.run_ts, c.group, c.name, q(c.expected), q(c.actual), q(c.difference), c.status)
        for c in checks
    ]
    return spark.createDataFrame(rows, schema)


# --------------------------------------------------------------------------- #
# data-quality summary: one row per (dataset, rule), including rules with 0 hits
# --------------------------------------------------------------------------- #
def build_dq_summary(
    spark: SparkSession,
    silver: Dict[str, DataFrame],
    quarantine: Dict[str, DataFrame],
    raw_counts: Dict[str, int],
    ctx: RunContext,
) -> DataFrame:
    sep = r"\|"   # regex-escaped REASON_SEPARATOR
    found = None
    for t in TABLES:
        errors = (
            quarantine[t].select(F.explode(F.split("reject_reasons", sep)).alias("rule"))
            .groupBy("rule").count().withColumn("found_severity", F.lit("ERROR"))
        )
        warnings = (
            silver[t].where(F.col("dq_warnings").isNotNull())
            .select(F.explode(F.split("dq_warnings", sep)).alias("rule"))
            .groupBy("rule").count().withColumn("found_severity", F.lit("WARNING"))
        )
        part = errors.unionByName(warnings).withColumn("dataset", F.lit(t))
        found = part if found is None else found.unionByName(part)

    catalog = spark.createDataFrame(RULES, ["dataset", "rule", "severity", "description"])
    source_rows = spark.createDataFrame(
        [(t, int(n)) for t, n in raw_counts.items()], ["dataset", "source_rows"]
    )
    return (
        catalog.join(found, ["dataset", "rule"], "full_outer")
        .join(source_rows, "dataset", "left")
        .select(
            F.lit(ctx.run_id).alias("run_id"),
            F.lit(ctx.run_ts).alias("run_ts"),
            "dataset",
            "rule",
            F.coalesce("found_severity", "severity").alias("severity"),
            "description",
            F.coalesce(F.col("count"), F.lit(0)).cast("bigint").alias("rows_flagged"),
            F.col("source_rows").cast("bigint").alias("source_rows"),
        )
    )