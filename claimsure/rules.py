"""
rules.py - data-quality rules (PySpark).

Each public function takes the RAW (all-string) DataFrame of one dataset and
returns TWO DataFrames:

    silver_df      rows that passed every ERROR rule, with proper data types
    quarantine_df  rows that failed >= 1 ERROR rule, original text values kept,
                   plus a `reject_reasons` column such as
                   "MISSING_POLICY_ID|INVALID_CLAIM_AMOUNT"

How a rule works (same pattern everywhere):
    1. clean the text      -> trim spaces, turn "" into NULL
    2. convert the types   -> to date / decimal (bad values become NULL)
    3. list the failures   -> an array of rule names per row
    4. split on the array  -> empty array = valid, otherwise quarantine

WARNING rules never reject a row; they only add a note to `dq_warnings`.
"""

from typing import List, Tuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from claimsure.config import (
    ALLOWED_CLAIM_STATUSES,
    ALLOWED_CLAIM_TYPES,
    ALLOWED_PAYMENT_STATUSES,
    ALLOWED_POLICY_STATUSES,
    ALLOWED_POLICY_TYPES,
    ISO_DATE_REGEX,
    POSTAL_CODE_REGEX,
    REASON_SEPARATOR,
    RunContext,
)

MONEY = DecimalType(18, 2)


# =========================================================================== #
# Small reusable helpers
# =========================================================================== #
def _add_cleaned(raw: DataFrame) -> DataFrame:
    """Add a trimmed copy of every column named `_<column>`; blank -> NULL."""
    cleaned = []
    for c in raw.columns:
        trimmed = F.trim(F.col(c))
        cleaned.append(
            F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed).alias(f"_{c}")
        )
    return raw.select("*", *cleaned)


def _parse_date(text: Column) -> Column:
    """'2024-05-30' -> date.  Anything else ('not-a-date', '2024-02-31', NULL) -> NULL."""
    return F.when(text.rlike(ISO_DATE_REGEX), F.to_date(text, "yyyy-MM-dd"))


def _parse_money(text: Column) -> Column:
    """'123.45' -> decimal(18,2).  Non-numeric text -> NULL."""
    return text.cast(MONEY)


def _canonical(text: Column, allowed: Tuple[str, ...]) -> Column:
    """Case-insensitive match against the allowed list; returns the canonical spelling or NULL."""
    return F.coalesce(*[F.when(F.lower(text) == v.lower(), F.lit(v)) for v in allowed])


def _flags(*checks: Tuple[Column, str]) -> Column:
    """
    Build an array of rule names that FIRED for the row.
    A check is (condition, "RULE_NAME"). A NULL condition counts as 'did not fire'.
    """
    if not checks:                                   # e.g. no warnings configured
        return F.array().cast("array<string>")
    items = [F.when(F.coalesce(cond, F.lit(False)), F.lit(name)) for cond, name in checks]
    return F.filter(F.array(*items), lambda x: x.isNotNull())


def _rank_duplicates(df: DataFrame, key: str, tiebreak: List[Column]) -> DataFrame:
    """
    Number the copies of each key. Copy #1 is the one we KEEP:
    the copy with the fewest other errors wins, then `tiebreak` decides.
    """
    order = [F.size("_errors1").asc()] + tiebreak
    return df.withColumn(
        "_dup_rank", F.row_number().over(Window.partitionBy(key).orderBy(*order))
    ).withColumn("_dup_count", F.count(F.lit(1)).over(Window.partitionBy(key)))


def _finalise_errors(df: DataFrame, key: str, dup_rule: str) -> DataFrame:
    """Append the duplicate rule (for surplus copies) to the error list -> `_errors`."""
    is_surplus = (F.col("_dup_rank") > 1) & F.col(key).isNotNull()
    return df.withColumn(
        "_errors",
        F.filter(
            F.concat(F.col("_errors1"), F.array(F.when(is_surplus, F.lit(dup_rule)))),
            lambda x: x.isNotNull(),
        ),
    )


def _split(
    df: DataFrame,
    raw_columns: List[str],
    silver_cols: List[Column],
    ctx: RunContext,
) -> Tuple[DataFrame, DataFrame]:
    """Turn the validated frame into (silver, quarantine)."""
    silver = df.filter(F.size("_errors") == 0).select(
        *silver_cols,
        F.when(F.size("_warnings") > 0, F.concat_ws(REASON_SEPARATOR, "_warnings")).alias("dq_warnings"),
        F.lit(ctx.ingest_date).alias("ingest_date"),
        F.lit(ctx.run_id).alias("run_id"),
    )
    quarantine = df.filter(F.size("_errors") > 0).select(
        *[F.col(c) for c in raw_columns],          # original text, untouched
        F.concat_ws(REASON_SEPARATOR, "_errors").alias("reject_reasons"),
        F.lit(ctx.ingest_date).alias("ingest_date"),
        F.lit(ctx.run_id).alias("run_id"),
        F.lit(ctx.run_ts).alias("rejected_at"),
    )
    return silver, quarantine


def _marker(df: DataFrame, col: str, out_col: str, flag: str) -> DataFrame:
    """Distinct non-null values of `col`, renamed, plus a TRUE flag column (for lookups)."""
    return (
        df.select(F.col(col).alias(out_col))
        .where(F.col(out_col).isNotNull())
        .distinct()
        .withColumn(flag, F.lit(True))
    )


def _raw_tiebreak(raw: DataFrame) -> List[Column]:
    """Deterministic ordering using every raw column (so re-runs pick the same copy)."""
    return [F.col(c).asc_nulls_last() for c in raw.columns]


# =========================================================================== #
# POLICIES
# =========================================================================== #
def process_policies(raw: DataFrame, ctx: RunContext) -> Tuple[DataFrame, DataFrame]:
    df = _add_cleaned(raw)
    df = (
        df.withColumn("_effective_date", _parse_date(F.col("_effective_date")))
        .withColumn("_expiration_date", _parse_date(F.col("_expiration_date")))
        .withColumn("_coverage_amount", _parse_money(F.col("_coverage_amount")))
        .withColumn("_premium_amount", _parse_money(F.col("_premium_amount")))
        .withColumn("_policy_type", _canonical(F.col("_policy_type"), ALLOWED_POLICY_TYPES))
        .withColumn("_policy_status", _canonical(F.col("_policy_status"), ALLOWED_POLICY_STATUSES))
    )

    date_order_bad = F.col("_expiration_date") < F.col("_effective_date")
    checks = [
        (F.col("_policy_id").isNull(), "MISSING_POLICY_ID"),
        (F.col("_effective_date").isNull(), "INVALID_EFFECTIVE_DATE"),
        (F.col("_expiration_date").isNull(), "INVALID_EXPIRATION_DATE"),
        (F.col("_coverage_amount").isNull() | (F.col("_coverage_amount") <= 0), "INVALID_COVERAGE_AMOUNT"),
        (F.col("_premium_amount").isNull() | (F.col("_premium_amount") <= 0), "INVALID_PREMIUM_AMOUNT"),
        (F.col("_policy_type").isNull(), "INVALID_POLICY_TYPE"),
        (F.col("_policy_status").isNull(), "INVALID_POLICY_STATUS"),
    ]
    order_check = (date_order_bad, "EXPIRY_BEFORE_EFFECTIVE")
    if ctx.reject_policy_date_order:
        checks.append(order_check)          # promoted to an ERROR
        warnings = _flags()                  # nothing left as a warning
    else:
        warnings = _flags(order_check)       # stays a WARNING

    df = df.withColumn("_errors1", _flags(*checks)).withColumn("_warnings", warnings)
    df = _rank_duplicates(df, "_policy_id", _raw_tiebreak(raw))
    df = _finalise_errors(df, "_policy_id", "DUPLICATE_POLICY_ID")

    silver_cols = [
        F.col("_policy_id").alias("policy_id"),
        F.col("_policy_type").alias("policy_type"),
        F.col("_effective_date").alias("effective_date"),
        F.col("_expiration_date").alias("expiration_date"),
        F.col("_coverage_amount").alias("coverage_amount"),
        F.col("_premium_amount").alias("premium_amount"),
        F.col("_policy_status").alias("policy_status"),
        F.coalesce(~date_order_bad, F.lit(True)).alias("is_date_range_valid"),
    ]
    return _split(df, raw.columns, silver_cols, ctx)


# =========================================================================== #
# CLAIMANTS
# =========================================================================== #
def process_claimants(raw: DataFrame, ctx: RunContext) -> Tuple[DataFrame, DataFrame]:
    df = _add_cleaned(raw)
    df = df.withColumn("_errors1", _flags((F.col("_claimant_id").isNull(), "MISSING_CLAIMANT_ID"))).withColumn(
        "_warnings",
        _flags(
            (F.col("_city").isNull(), "MISSING_CITY"),
            (F.col("_state").isNull(), "MISSING_STATE"),
            (F.col("_claimant_name").isNull(), "MISSING_CLAIMANT_NAME"),
            (~F.coalesce(F.col("_postal_code").rlike(POSTAL_CODE_REGEX), F.lit(False)), "INVALID_POSTAL_CODE"),
        ),
    )
    df = _rank_duplicates(df, "_claimant_id", _raw_tiebreak(raw))
    df = _finalise_errors(df, "_claimant_id", "DUPLICATE_CLAIMANT_ID")

    silver_cols = [
        F.col("_claimant_id").alias("claimant_id"),
        F.col("_claimant_name").alias("claimant_name"),
        F.col("_address_line").alias("address_line"),
        F.coalesce(F.col("_city"), F.lit("Unknown")).alias("city"),   # handle missing city
        F.col("_state").alias("state"),
        F.col("_postal_code").alias("postal_code"),                  # keep as text (leading zeros)
        F.col("_city").isNull().alias("city_imputed"),
    ]
    return _split(df, raw.columns, silver_cols, ctx)


# =========================================================================== #
# CLAIMS
# =========================================================================== #
def process_claims(
    raw: DataFrame,
    silver_policies: DataFrame,
    silver_claimants: DataFrame,
    ctx: RunContext,
) -> Tuple[DataFrame, DataFrame]:
    """Claims are validated against the VALID policies and claimants only."""
    pol = F.broadcast(_marker(silver_policies, "policy_id", "_pol_id", "_pol_found"))
    clm = F.broadcast(_marker(silver_claimants, "claimant_id", "_clm_id", "_clm_found"))
    as_of = F.to_date(F.lit(ctx.as_of_date), "yyyy-MM-dd")

    df = _add_cleaned(raw)
    df = (
        df.withColumn("_loss_date", _parse_date(F.col("_loss_date")))
        .withColumn("_reported_date", _parse_date(F.col("_reported_date")))
        .withColumn("_claim_amount", _parse_money(F.col("_claim_amount")))
        .withColumn("_reserve_amount", _parse_money(F.col("_reserve_amount")))
        .withColumn("_claim_status", _canonical(F.col("_claim_status"), ALLOWED_CLAIM_STATUSES))
        .withColumn("_claim_type", _canonical(F.col("_claim_type"), ALLOWED_CLAIM_TYPES))
        .join(pol, F.col("_policy_id") == F.col("_pol_id"), "left")
        .join(clm, F.col("_claimant_id") == F.col("_clm_id"), "left")
    )

    df = df.withColumn(
        "_errors1",
        _flags(
            (F.col("_claim_id").isNull(), "MISSING_CLAIM_ID"),
            (F.col("_policy_id").isNull(), "MISSING_POLICY_ID"),
            (F.col("_claimant_id").isNull(), "MISSING_CLAIMANT_ID"),
            (F.col("_policy_id").isNotNull() & F.col("_pol_found").isNull(), "ORPHAN_POLICY_ID"),
            (F.col("_claimant_id").isNotNull() & F.col("_clm_found").isNull(), "ORPHAN_CLAIMANT_ID"),
            (F.col("_loss_date").isNull(), "INVALID_LOSS_DATE"),
            (F.col("_reported_date").isNull(), "INVALID_REPORTED_DATE"),
            (F.col("_reported_date") < F.col("_loss_date"), "REPORTED_BEFORE_LOSS"),
            (F.col("_claim_amount").isNull() | (F.col("_claim_amount") <= 0), "INVALID_CLAIM_AMOUNT"),
            (F.col("_reserve_amount").isNull() | (F.col("_reserve_amount") < 0), "INVALID_RESERVE_AMOUNT"),
            (F.col("_claim_status").isNull(), "INVALID_CLAIM_STATUS"),
            (F.col("_claim_type").isNull(), "INVALID_CLAIM_TYPE"),
        ),
    ).withColumn(
        "_warnings",
        _flags(
            (F.col("_loss_date") > as_of, "FUTURE_LOSS_DATE"),
            (F.col("_reported_date") > as_of, "FUTURE_REPORTED_DATE"),
        ),
    )

    tiebreak = [F.col("_reported_date").desc_nulls_last()] + _raw_tiebreak(raw)
    df = _rank_duplicates(df, "_claim_id", tiebreak)
    df = _finalise_errors(df, "_claim_id", "DUPLICATE_CLAIM_ID")

    silver_cols = [
        F.col("_claim_id").alias("claim_id"),
        F.col("_policy_id").alias("policy_id"),
        F.col("_claimant_id").alias("claimant_id"),
        F.col("_loss_date").alias("loss_date"),
        F.col("_reported_date").alias("reported_date"),
        F.col("_claim_type").alias("claim_type"),
        F.col("_claim_status").alias("claim_status"),
        F.col("_claim_amount").alias("claim_amount"),
        F.col("_reserve_amount").alias("reserve_amount"),
        F.col("_source_system").alias("source_system"),
        F.datediff(F.col("_reported_date"), F.col("_loss_date")).alias("report_lag_days"),
        # how many copies of this claim_id existed in the source (1 = no duplicates)
        F.when(F.col("_claim_id").isNull(), F.lit(1)).otherwise(F.col("_dup_count")).cast("int").alias(
            "source_duplicate_count"
        ),
    ]
    return _split(df, raw.columns, silver_cols, ctx)


# =========================================================================== #
# PAYMENTS
# =========================================================================== #
def process_payments(
    raw: DataFrame,
    raw_claims: DataFrame,
    silver_claims: DataFrame,
    ctx: RunContext,
) -> Tuple[DataFrame, DataFrame]:
    """
    A payment's claim_id is checked TWICE, against two different sets:
      * every claim_id in the source file  -> ORPHAN_CLAIM_REFERENCE if absent
      * claim_ids that survived to silver  -> PARENT_CLAIM_QUARANTINED if absent
    That tells you whether the payment is garbage or just belongs to a bad claim.
    """
    raw_ids = F.broadcast(
        _marker(
            raw_claims.select(
                F.when(F.trim("claim_id") == "", F.lit(None).cast("string")).otherwise(F.trim("claim_id")).alias("c")
            ),
            "c", "_bronze_claim_id", "_in_bronze",
        )
    )
    silver_ids = F.broadcast(
        silver_claims.select(
            F.col("claim_id").alias("_silver_claim_id"),
            F.col("claim_amount").alias("_silver_claim_amount"),
            F.lit(True).alias("_in_silver"),
        ).distinct()
    )

    df = _add_cleaned(raw)
    df = (
        df.withColumn("_payment_date", _parse_date(F.col("_payment_date")))
        .withColumn("_payment_amount", _parse_money(F.col("_payment_amount")))
        .withColumn("_payment_status", _canonical(F.col("_payment_status"), ALLOWED_PAYMENT_STATUSES))
        .join(raw_ids, F.col("_claim_id") == F.col("_bronze_claim_id"), "left")
        .join(silver_ids, F.col("_claim_id") == F.col("_silver_claim_id"), "left")
    )

    df = df.withColumn(
        "_errors1",
        _flags(
            (F.col("_payment_id").isNull(), "MISSING_PAYMENT_ID"),
            (F.col("_claim_id").isNull(), "MISSING_CLAIM_REFERENCE"),
            (F.col("_claim_id").isNotNull() & F.col("_in_bronze").isNull(), "ORPHAN_CLAIM_REFERENCE"),
            (
                F.col("_claim_id").isNotNull() & F.col("_in_bronze").isNotNull() & F.col("_in_silver").isNull(),
                "PARENT_CLAIM_QUARANTINED",
            ),
            (F.col("_payment_date").isNull(), "INVALID_PAYMENT_DATE"),
            (F.col("_payment_amount").isNull() | (F.col("_payment_amount") <= 0), "INVALID_PAYMENT_AMOUNT"),
            (F.col("_payment_status").isNull(), "INVALID_PAYMENT_STATUS"),
        ),
    ).withColumn(
        "_warnings",
        _flags((F.col("_payment_amount") > F.col("_silver_claim_amount"), "PAYMENT_EXCEEDS_CLAIM_AMOUNT")),
    )

    df = _rank_duplicates(df, "_payment_id", _raw_tiebreak(raw))
    df = _finalise_errors(df, "_payment_id", "DUPLICATE_PAYMENT_ID")

    silver_cols = [
        F.col("_payment_id").alias("payment_id"),
        F.col("_claim_id").alias("claim_id"),
        F.col("_payment_date").alias("payment_date"),
        F.col("_payment_amount").alias("payment_amount"),
        F.col("_payment_status").alias("payment_status"),
    ]
    return _split(df, raw.columns, silver_cols, ctx)