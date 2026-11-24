"""
claimsure_etl_job.py - the AWS Glue (PySpark) job for ClaimSure.

What it does, in order:
    1. READ    bronze CSVs                  s3://<bucket>/bronze/<table>/ingest_date=<date>/
    2. VALIDATE + CLEAN each dataset        -> silver (valid) + quarantine (rejected)
    3. BUILD   gold analytics tables        from the persisted silver data
    4. RECONCILE source -> target           and write the audit tables
    5. FAIL the job if any reconciliation check does not match

Job parameters (Glue "Job parameters" -> key / value):
    --base_path       s3://<your-bucket>         (required)  where bronze/silver/... live
    --ingest_date     2026-11-24                 (required)  which bronze folder to read
    --as_of_date      2026-11-24                 (optional)  "today" for future-date warnings
    --run_id          run_20261124T101500Z       (optional)  default: generated from the clock
    --reject_policy_date_order  true|false       (optional)  default false (see README)
    --fail_on_recon_mismatch    true|false       (optional)  default true
    --JOB_NAME        added by Glue itself

The same script also runs on your laptop with a local folder as --base_path
(requires pyspark + Java) - see the README section "Run it locally".
"""

import argparse
import os
import sys
from datetime import datetime, timezone

# Make `import claimsure` work when run locally from the repo (on Glue the package
# arrives through the --extra-py-files zip, so this line simply does nothing).
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql import SparkSession  # noqa: E402

from claimsure.config import (  # noqa: E402
    LAYER_AUDIT, LAYER_GOLD, LAYER_QUARANTINE, LAYER_SILVER, TABLES, RunContext,
)
from claimsure.gold import build_gold  # noqa: E402
from claimsure.io_utils import read_bronze, read_parquet, table_path, write_parquet  # noqa: E402
from claimsure.reconciliation import build_checks, build_dq_summary, checks_to_df  # noqa: E402
from claimsure.rules import (  # noqa: E402
    process_claimants, process_claims, process_payments, process_policies,
)


# --------------------------------------------------------------------------- #
def _as_bool(value: str) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes", "y")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ClaimSure bronze -> silver -> gold ETL")
    p.add_argument("--base_path", required=True)
    p.add_argument("--ingest_date", required=True)
    p.add_argument("--as_of_date", default=None)
    p.add_argument("--run_id", default=None)
    p.add_argument("--reject_policy_date_order", default="false")
    p.add_argument("--fail_on_recon_mismatch", default="true")
    p.add_argument("--JOB_NAME", default="claimsure_etl_job")
    # Glue adds extra arguments of its own (e.g. --job-bookmark-option): ignore them.
    args, _unknown = p.parse_known_args(argv)
    return args


def build_context(args: argparse.Namespace) -> RunContext:
    now = datetime.now(timezone.utc)
    datetime.strptime(args.ingest_date, "%Y-%m-%d")                  # fail early on a typo
    return RunContext(
        ingest_date=args.ingest_date,
        run_id=args.run_id or now.strftime("run_%Y%m%dT%H%M%SZ"),   # sortable: latest = max()
        run_ts=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        as_of_date=args.as_of_date or now.strftime("%Y-%m-%d"),
        reject_policy_date_order=_as_bool(args.reject_policy_date_order),
    )


def get_spark(args: argparse.Namespace):
    """On Glue: use GlueContext. Elsewhere: a small local SparkSession."""
    try:
        from awsglue.context import GlueContext  # only exists inside AWS Glue
        from awsglue.job import Job
        from pyspark.context import SparkContext

        glue_context = GlueContext(SparkContext.getOrCreate())
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(args.JOB_NAME, vars(args))
    except ImportError:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("claimsure-local")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        job = None

    # Settings that make validation behave the same everywhere:
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # '2024-02-31' -> NULL, not an error
    spark.conf.set("spark.sql.ansi.enabled", "false")                 # bad casts -> NULL, not an error
    spark.conf.set("spark.sql.shuffle.partitions", "8")               # data is tiny; 200 would be wasteful
    return spark, job


def log(msg: str) -> None:
    print(f"[claimsure] {msg}", flush=True)


# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    args = parse_args(argv)
    ctx = build_context(args)
    spark, job = get_spark(args)
    base = args.base_path

    log(f"run_id={ctx.run_id} ingest_date={ctx.ingest_date} base_path={base}")

    # ---- 1. READ BRONZE ----------------------------------------------------
    raw = {t: read_bronze(spark, base, t, ctx.ingest_date).cache() for t in TABLES}
    raw_counts = {t: raw[t].count() for t in TABLES}
    log(f"bronze rows: {raw_counts}")

    # ---- 2. VALIDATE -> SILVER + QUARANTINE ----------------------------------
    # Order matters: claims need valid policies/claimants; payments need valid claims.
    silver, quarantine = {}, {}
    silver["policies"], quarantine["policies"] = process_policies(raw["policies"], ctx)
    silver["claimants"], quarantine["claimants"] = process_claimants(raw["claimants"], ctx)
    for t in ("policies", "claimants"):
        silver[t], quarantine[t] = silver[t].cache(), quarantine[t].cache()
        write_parquet(silver[t], table_path(base, LAYER_SILVER, t))
        write_parquet(quarantine[t], table_path(base, LAYER_QUARANTINE, t))

    silver["claims"], quarantine["claims"] = process_claims(
        raw["claims"], silver["policies"], silver["claimants"], ctx
    )
    silver["claims"], quarantine["claims"] = silver["claims"].cache(), quarantine["claims"].cache()
    write_parquet(silver["claims"], table_path(base, LAYER_SILVER, "claims"))
    write_parquet(quarantine["claims"], table_path(base, LAYER_QUARANTINE, "claims"))

    silver["payments"], quarantine["payments"] = process_payments(
        raw["payments"], raw["claims"], silver["claims"], ctx
    )
    silver["payments"], quarantine["payments"] = silver["payments"].cache(), quarantine["payments"].cache()
    write_parquet(silver["payments"], table_path(base, LAYER_SILVER, "payments"))
    write_parquet(quarantine["payments"], table_path(base, LAYER_QUARANTINE, "payments"))

    # From here on, work from what was actually WRITTEN (true source-to-target).
    silver = {t: read_parquet(spark, table_path(base, LAYER_SILVER, t)) for t in TABLES}
    quarantine = {t: read_parquet(spark, table_path(base, LAYER_QUARANTINE, t)) for t in TABLES}
    for t in TABLES:
        log(f"{t}: bronze={raw_counts[t]} silver={silver[t].count()} quarantine={quarantine[t].count()}")

    # ---- 3. GOLD -------------------------------------------------------------
    gold = build_gold(silver["claims"], silver["policies"], silver["claimants"], silver["payments"])
    for name, df in gold.items():
        write_parquet(df, table_path(base, LAYER_GOLD, name))
    gold = {name: read_parquet(spark, table_path(base, LAYER_GOLD, name)) for name in gold}
    log(f"gold tables written: {sorted(gold)}")

    # ---- 4. RECONCILE + DQ SUMMARY -> AUDIT -------------------------------------
    checks = build_checks(raw, silver, quarantine, gold)
    checks_df = checks_to_df(spark, checks, ctx)
    dq_df = build_dq_summary(spark, silver, quarantine, raw_counts, ctx)
    # Audit tables are APPENDED so you keep the history of every run (filter by run_id).
    write_parquet(checks_df, table_path(base, LAYER_AUDIT, "reconciliation"), mode="append")
    write_parquet(dq_df, table_path(base, LAYER_AUDIT, "dq_summary"), mode="append")

    failed = [c for c in checks if c.status == "FAIL"]
    log(f"reconciliation: {len(checks) - len(failed)} PASS / {len(failed)} FAIL")
    for c in failed:
        log(f"  FAIL  {c.name}: expected={c.expected} actual={c.actual}")

    # ---- 5. FINISH -----------------------------------------------------------
    if failed and _as_bool(args.fail_on_recon_mismatch):
        raise RuntimeError(f"{len(failed)} reconciliation check(s) failed - see audit/reconciliation")
    if job is not None:
        job.commit()
    log("done")
    return 0


if __name__ == "__main__":
    main()