"""
config.py - shared constants for the ClaimSure pipeline.

This module is deliberately PURE PYTHON (no pyspark / pandas imports) so that
both the AWS Glue job and the lightweight local scripts can import it.

If a business rule changes (e.g. a new valid claim status), change it HERE.
"""

from dataclasses import dataclass

# --------------------------------------------------------------------------- #
# Tables and S3 layers
# --------------------------------------------------------------------------- #
TABLES = ("claims", "policies", "claimants", "payments")

LAYER_BRONZE = "bronze"          # raw CSV, never modified
LAYER_SILVER = "silver"          # cleaned + validated Parquet
LAYER_QUARANTINE = "quarantine"  # rejected rows + the reason(s)
LAYER_GOLD = "gold"              # analytics-ready aggregates
LAYER_AUDIT = "audit"            # reconciliation + data-quality summary

AWS_REGION = "ap-south-1"        # Asia Pacific (Mumbai)

# --------------------------------------------------------------------------- #
# Expected raw columns (taken from the real CSV headers)
# --------------------------------------------------------------------------- #
RAW_COLUMNS = {
    "claims": [
        "claim_id", "policy_id", "claimant_id", "loss_date", "reported_date",
        "claim_type", "claim_status", "claim_amount", "reserve_amount",
        "source_system",
    ],
    "policies": [
        "policy_id", "policy_type", "effective_date", "expiration_date",
        "coverage_amount", "premium_amount", "policy_status",
    ],
    "claimants": [
        "claimant_id", "claimant_name", "address_line", "city", "state",
        "postal_code",
    ],
    "payments": [
        "payment_id", "claim_id", "payment_date", "payment_amount",
        "payment_status",
    ],
}

# --------------------------------------------------------------------------- #
# Allowed values (matching is case-insensitive; the canonical spelling is kept)
# NOTE: "In Progress??" in the source data is NOT in this list on purpose -
# it is one of the injected bad values and must be quarantined.
# --------------------------------------------------------------------------- #
ALLOWED_CLAIM_STATUSES = ("Open", "Under Review", "Approved", "Rejected", "Closed")
ALLOWED_CLAIM_TYPES = (
    "Property Damage", "Bodily Injury", "Theft", "Collision", "Water Damage",
)
ALLOWED_POLICY_TYPES = ("Auto", "Home", "Commercial")
ALLOWED_POLICY_STATUSES = ("Active", "Expired", "Cancelled")
ALLOWED_PAYMENT_STATUSES = ("Pending", "Processed", "Failed")

ISO_DATE_REGEX = r"^\d{4}-\d{2}-\d{2}$"   # dates must look like 2024-05-30
POSTAL_CODE_REGEX = r"^\d{6}$"            # Indian PIN code

REASON_SEPARATOR = "|"                    # joins multiple reasons in one cell

# --------------------------------------------------------------------------- #
# Rule catalog: (dataset, rule_name, severity, description)
#   ERROR   -> the row is sent to quarantine
#   WARNING -> the row is accepted but flagged in the `dq_warnings` column
# Both the Spark job and the pandas script must only emit rules listed here
# (a unit test enforces this).
# --------------------------------------------------------------------------- #
RULES = [
    # ---- claims -------------------------------------------------------------
    ("claims", "MISSING_CLAIM_ID", "ERROR", "claim_id is blank"),
    ("claims", "MISSING_POLICY_ID", "ERROR", "policy_id is blank"),
    ("claims", "MISSING_CLAIMANT_ID", "ERROR", "claimant_id is blank"),
    ("claims", "ORPHAN_POLICY_ID", "ERROR", "policy_id not found in valid policies"),
    ("claims", "ORPHAN_CLAIMANT_ID", "ERROR", "claimant_id not found in valid claimants"),
    ("claims", "INVALID_LOSS_DATE", "ERROR", "loss_date missing or not a real yyyy-MM-dd date"),
    ("claims", "INVALID_REPORTED_DATE", "ERROR", "reported_date missing or not a real yyyy-MM-dd date"),
    ("claims", "REPORTED_BEFORE_LOSS", "ERROR", "reported_date is earlier than loss_date"),
    ("claims", "INVALID_CLAIM_AMOUNT", "ERROR", "claim_amount missing, non-numeric, zero or negative"),
    ("claims", "INVALID_RESERVE_AMOUNT", "ERROR", "reserve_amount missing, non-numeric or negative"),
    ("claims", "INVALID_CLAIM_STATUS", "ERROR", "claim_status not in the allowed list"),
    ("claims", "INVALID_CLAIM_TYPE", "ERROR", "claim_type not in the allowed list"),
    ("claims", "DUPLICATE_CLAIM_ID", "ERROR", "surplus copy of a claim_id (one copy is kept)"),
    ("claims", "FUTURE_LOSS_DATE", "WARNING", "loss_date is after the run's as-of date"),
    ("claims", "FUTURE_REPORTED_DATE", "WARNING", "reported_date is after the run's as-of date"),
    # ---- policies -----------------------------------------------------------
    ("policies", "MISSING_POLICY_ID", "ERROR", "policy_id is blank"),
    ("policies", "DUPLICATE_POLICY_ID", "ERROR", "surplus copy of a policy_id"),
    ("policies", "INVALID_EFFECTIVE_DATE", "ERROR", "effective_date missing or not a real date"),
    ("policies", "INVALID_EXPIRATION_DATE", "ERROR", "expiration_date missing or not a real date"),
    ("policies", "INVALID_COVERAGE_AMOUNT", "ERROR", "coverage_amount missing, non-numeric or <= 0"),
    ("policies", "INVALID_PREMIUM_AMOUNT", "ERROR", "premium_amount missing, non-numeric or <= 0"),
    ("policies", "INVALID_POLICY_TYPE", "ERROR", "policy_type not in the allowed list"),
    ("policies", "INVALID_POLICY_STATUS", "ERROR", "policy_status not in the allowed list"),
    ("policies", "EXPIRY_BEFORE_EFFECTIVE", "WARNING",
     "expiration_date < effective_date (ERROR if --reject_policy_date_order true)"),
    # ---- claimants ----------------------------------------------------------
    ("claimants", "MISSING_CLAIMANT_ID", "ERROR", "claimant_id is blank"),
    ("claimants", "DUPLICATE_CLAIMANT_ID", "ERROR", "surplus copy of a claimant_id"),
    ("claimants", "MISSING_CITY", "WARNING", "city blank - set to 'Unknown' in silver"),
    ("claimants", "MISSING_STATE", "WARNING", "state blank"),
    ("claimants", "MISSING_CLAIMANT_NAME", "WARNING", "claimant_name blank"),
    ("claimants", "INVALID_POSTAL_CODE", "WARNING", "postal_code is not 6 digits"),
    # ---- payments -----------------------------------------------------------
    ("payments", "MISSING_PAYMENT_ID", "ERROR", "payment_id is blank"),
    ("payments", "DUPLICATE_PAYMENT_ID", "ERROR", "surplus copy of a payment_id"),
    ("payments", "MISSING_CLAIM_REFERENCE", "ERROR", "claim_id on the payment is blank"),
    ("payments", "ORPHAN_CLAIM_REFERENCE", "ERROR", "claim_id does not exist in the source claims at all"),
    ("payments", "PARENT_CLAIM_QUARANTINED", "ERROR", "claim exists in source but was rejected into quarantine"),
    ("payments", "INVALID_PAYMENT_DATE", "ERROR", "payment_date missing or not a real date"),
    ("payments", "INVALID_PAYMENT_AMOUNT", "ERROR", "payment_amount missing, non-numeric, zero or negative"),
    ("payments", "INVALID_PAYMENT_STATUS", "ERROR", "payment_status not in the allowed list"),
    ("payments", "PAYMENT_EXCEEDS_CLAIM_AMOUNT", "WARNING", "single payment is larger than its claim_amount"),
]


@dataclass(frozen=True)
class RunContext:
    """Values that describe ONE execution of the pipeline."""

    ingest_date: str                 # which bronze folder to read, e.g. 2026-11-24
    run_id: str                      # unique + sortable, e.g. run_20261124T101500Z
    run_ts: str                      # ISO timestamp (stored as a string for Athena safety)
    as_of_date: str                  # "today" for the future-date warning, yyyy-MM-dd
    reject_policy_date_order: bool = False