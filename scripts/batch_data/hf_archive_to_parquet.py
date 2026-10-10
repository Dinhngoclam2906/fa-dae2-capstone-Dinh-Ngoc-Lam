"""
hf_archive_to_parquet.py

Transform the full Hugging Face `Stefan171/TheGuardian-Articles` CSV archive into
cleaned, publication-date-partitioned Parquet that matches the BigQuery `raw.raw_data`
schema.

This is the Phase 1 bulk-load transform. It replaces the 100-row *sample* produced by
`batch_data_collector.py` (which is kept for the Airflow demo path).

Input : data/batch/TheGuardian-Articles/articledb_part*.csv   (6 parts, ~550 MB)
Output: data/batch/hf_guardian_parquet/pub_year=YYYY/pub_month=MM/part-0.parquet

Row reality (measured 2026-09-09): the archive is ~138k articles, NOT the ~2.1M that a
raw `wc -l` suggests (Guardian bodies contain many newlines inside quoted CSV fields).
Of those, ~87.5k are `Data Quality = Full` with a usable body — that is what this script
emits.

Usage:
    uv run python scripts/batch_data/hf_archive_to_parquet.py
    uv run python scripts/batch_data/hf_archive_to_parquet.py --keep-partial --min-year 1990
"""

from __future__ import annotations

import argparse
import glob
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# The HF archive stops at 2024-05-07; the pipeline tags everything at/under this date as
# historical `batch` and anything after as `realtime` (Guardian Content API backfill).
DEFAULT_CUTOFF = "2024-05-11"

# Output columns, in `raw.raw_data` order. `loaded_at` is added by the BigQuery load step.
OUTPUT_COLUMNS = [
    "article_id",
    "crawl_timestamp",
    "web_publication_date",
    "web_title",
    "body_text",
    "web_url",
    "section_name",
    "data_source",
]


def read_archive(input_dir: Path) -> pl.DataFrame:
    """Read all CSV parts. Read per-file then concat: a single multi-file
    ``scan_csv`` glob mis-splits on the embedded newlines and inflates the row
    count ~12x."""
    parts = sorted(glob.glob(str(input_dir / "articledb_part*.csv")))
    if not parts:
        raise FileNotFoundError(f"No articledb_part*.csv under {input_dir}")
    logger.info("Reading %d CSV parts from %s", len(parts), input_dir)
    frames = [pl.read_csv(p, infer_schema_length=None) for p in parts]
    df = pl.concat(frames)
    logger.info("Read %s raw rows", f"{df.height:,}")
    return df


def titlecase_section(col: pl.Expr) -> pl.Expr:
    """`FILM AND MUSIC` -> `Film And Music` (matches batch_data_collector.py)."""
    return (
        col.str.split(" ")
        .list.eval(
            pl.element().str.slice(0, 1).str.to_uppercase()
            + pl.element().str.slice(1).str.to_lowercase()
        )
        .list.join(" ")
    )


def transform(
    df: pl.DataFrame,
    cutoff: str,
    keep_partial: bool,
    min_year: int | None,
) -> pl.DataFrame:
    crawl_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    cutoff_date = datetime.strptime(cutoff, "%Y-%m-%d").date()

    n_in = df.height
    if not keep_partial:
        df = df.filter(pl.col("Data Quality") == "Full")
    logger.info(
        "After quality filter (%s): %s rows",
        "Full only" if not keep_partial else "Full + Partial",
        f"{df.height:,}",
    )

    df = df.with_columns(
        pl.col("URL").str.extract(r"theguardian\.com/(.*)", 1).alias("article_id"),
        pl.col("Publication Date")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ", strict=False, time_zone="UTC")
        .dt.replace_time_zone(None)
        .alias("web_publication_date"),
        pl.col("Article Title").alias("web_title"),
        pl.col("Article Contents").alias("body_text"),
        pl.col("URL").alias("web_url"),
        titlecase_section(pl.col("Article Category")).alias("section_name"),
    )

    df = df.filter(
        pl.col("article_id").is_not_null()
        & (pl.col("article_id").str.len_chars() > 0)
        & pl.col("web_publication_date").is_not_null()
        & pl.col("web_title").is_not_null()
        & pl.col("body_text").is_not_null()
        & (pl.col("body_text").str.len_chars() > 0)
    )
    logger.info("After null/empty filter on key fields: %s rows", f"{df.height:,}")

    if min_year is not None:
        before = df.height
        df = df.filter(pl.col("web_publication_date").dt.year() >= min_year)
        logger.info(
            "After --min-year %d: %s rows (dropped %s)",
            min_year,
            f"{df.height:,}",
            f"{before - df.height:,}",
        )

    df = df.with_columns(
        pl.lit(crawl_ts).alias("crawl_timestamp"),
        pl.when(pl.col("web_publication_date").dt.date() <= cutoff_date)
        .then(pl.lit("batch"))
        .otherwise(pl.lit("realtime"))
        .alias("data_source"),
    )

    # De-dupe on article_id, keeping the most recently published version.
    before = df.height
    df = (
        df.sort("web_publication_date", descending=True)
        .unique(subset=["article_id"], keep="first")
        .sort("web_publication_date")
    )
    if before != df.height:
        logger.info(
            "De-duped on article_id: %s -> %s rows (%s dupes removed)",
            f"{before:,}",
            f"{df.height:,}",
            f"{before - df.height:,}",
        )

    logger.info("Transformed %s -> %s rows", f"{n_in:,}", f"{df.height:,}")
    return df.select(OUTPUT_COLUMNS)


def write_partitioned(df: pl.DataFrame, output_dir: Path) -> tuple[int, int]:
    """Write hive-style pub_year=/pub_month= Parquet. Returns (files, partitions)."""
    if output_dir.exists():
        logger.info("Clearing existing output dir %s", output_dir)
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    parted = df.with_columns(
        pl.col("web_publication_date").dt.year().alias("_yr"),
        pl.col("web_publication_date").dt.month().alias("_mo"),
    )
    n_files = 0
    partitions = parted.select("_yr", "_mo").unique().sort("_yr", "_mo")
    for row in partitions.iter_rows(named=True):
        yr, mo = row["_yr"], row["_mo"]
        part = parted.filter((pl.col("_yr") == yr) & (pl.col("_mo") == mo)).drop(
            "_yr", "_mo"
        )
        pdir = output_dir / f"pub_year={yr}" / f"pub_month={mo:02d}"
        pdir.mkdir(parents=True, exist_ok=True)
        part.write_parquet(pdir / "part-0.parquet", compression="zstd")
        n_files += 1
    return n_files, partitions.height


def print_report(df: pl.DataFrame) -> None:
    logger.info("=" * 64)
    logger.info("PHASE 1 TRANSFORM REPORT")
    logger.info("=" * 64)
    logger.info("Total rows out: %s", f"{df.height:,}")

    by_src = df.group_by("data_source").agg(pl.len().alias("n")).sort("data_source")
    for r in by_src.iter_rows(named=True):
        logger.info("  data_source=%-9s %s", r["data_source"], f"{r['n']:,}")

    dmin = df["web_publication_date"].min()
    dmax = df["web_publication_date"].max()
    logger.info("Publication date range: %s -> %s", dmin, dmax)

    by_year = (
        df.with_columns(pl.col("web_publication_date").dt.year().alias("yr"))
        .group_by("yr")
        .agg(pl.len().alias("n"))
        .sort("yr")
    )
    logger.info("Rows per publication year:")
    for r in by_year.iter_rows(named=True):
        logger.info("  %d  %s", r["yr"], f"{r['n']:,}")

    logger.info("Distinct sections: %d", df["section_name"].n_unique())
    body_stats = df.select(
        total_mb=(pl.col("body_text").str.len_bytes().sum() / 1_048_576).round(1),
        mean_kb=(pl.col("body_text").str.len_bytes().mean() / 1024).round(1),
    ).row(0, named=True)
    logger.info(
        "body_text: %.1f MB total, %.1f KB mean", body_stats["total_mb"], body_stats["mean_kb"]
    )
    logger.info("=" * 64)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/batch/TheGuardian-Articles"),
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/batch/hf_guardian_parquet"),
    )
    ap.add_argument("--cutoff", default=DEFAULT_CUTOFF, help="batch/realtime split date")
    ap.add_argument(
        "--keep-partial",
        action="store_true",
        help="also emit Data Quality = Partial rows (still requires a non-empty body)",
    )
    ap.add_argument(
        "--min-year",
        type=int,
        default=None,
        help="drop articles published before this year (a handful are mis-dated to 1855 etc.)",
    )
    args = ap.parse_args()

    if not args.input_dir.is_dir():
        logger.error("Input dir not found: %s", args.input_dir)
        return 1

    df = read_archive(args.input_dir)
    df = transform(df, args.cutoff, args.keep_partial, args.min_year)

    if df.height == 0:
        logger.error("No rows after transform - aborting")
        return 1

    n_files, n_parts = write_partitioned(df, args.output_dir)
    logger.info(
        "Wrote %d Parquet file(s) across %d pub_year/pub_month partition(s) to %s",
        n_files,
        n_parts,
        args.output_dir,
    )
    print_report(df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
