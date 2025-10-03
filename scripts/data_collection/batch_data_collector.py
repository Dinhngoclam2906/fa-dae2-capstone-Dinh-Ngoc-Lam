import logging
import os
from pathlib import Path
from datetime import datetime
import json
from ast import literal_eval
import warnings
from typing import Union, List
import pandas as pd
import multiprocessing

# Detect CPU count and set thread pool size before importing Polars
cpu_count = multiprocessing.cpu_count()
print(f"Detected {cpu_count} CPU cores.")
os.environ['POLARS_MAX_THREADS'] = str(min(cpu_count, 32))  # Cap at 32 to avoid overhead

# Now import Polars
import polars as pl
import cProfile

# Suppress warnings if needed
warnings.filterwarnings("ignore", message=".*un-recognized timezone.*")
warnings.filterwarnings("ignore", message=".*chrono.*")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchDataCollector:
    def __init__(self):
        self.data_dir = Path("data/batch")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_csv = self.data_dir / "guardian_historical_preprocessed.csv"  # Changed to CSV
        self.repo_id = "Stefan171/TheGuardian-Articles"
        self.local_dir = self.data_dir / self.repo_id.replace("/", "_")
        # Updated columns based on HF dataset schema
        self.usecols = ['URL', 'Article Category', 'Publication Date', 'Article Title', 'Article Contents', 'Data Quality']

    def download_dataset(self):
        """Download the Hugging Face dataset."""
        try:
            from huggingface_hub import snapshot_download
            local_dir_str = str(self.local_dir)
            snapshot_download(repo_id=self.repo_id, local_dir=local_dir_str, repo_type="dataset")
            # Find Parquet files (HF often uses shards)
            parquet_files = list(self.local_dir.rglob("*.parquet"))
            if parquet_files:
                self.raw_data_paths = parquet_files
                logger.info(f"Downloaded HF dataset to {self.local_dir}, using {len(parquet_files)} Parquet files")
            else:
                # Fallback to JSON/CSV if no Parquet
                jsonl_files = list(self.local_dir.rglob("*.jsonl")) + list(self.local_dir.rglob("*.json"))
                csv_files = list(self.local_dir.rglob("*.csv"))
                data_files = parquet_files + jsonl_files + csv_files
                if data_files:
                    self.raw_data_paths = data_files
                    logger.info(f"Downloaded HF dataset to {self.local_dir}, using {len(data_files)} data files (mixed formats)")
                else:
                    raise FileNotFoundError("No data files (Parquet/JSON/CSV) found in HF dataset.")
            
            # Cleanup: Keep only CSV files, delete others (e.g., .gitattributes, README.md, .cache)
            cleaned_count = 0
            for file_path in self.local_dir.rglob('*'):
                if file_path.is_file() and file_path.suffix != '.csv':
                    file_path.unlink()
                    cleaned_count += 1
                    logger.debug(f"Cleaned up non-CSV file: {file_path}")
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} non-CSV files from {self.local_dir}")
        except ImportError:
            logger.error("huggingface_hub not found. Install with 'pip install huggingface_hub'.")
            raise
        except Exception as e:
            logger.error(f"Failed to download HF dataset: {e}. Download manually from https://huggingface.co/datasets/{self.repo_id}.")
            raise

    def preprocess_data(self, data_paths: List[Path]):
        """Preprocess the historical data using Polars for speed and vectorization."""
        logger.info(f"Loading batch data from HF data files in {self.local_dir}")
        
        # Scan files lazily (concat if multiple; handle mixed formats)
        lfs = []
        for p in data_paths:
            if p.suffix == '.parquet':
                lfs.append(pl.scan_parquet(str(p)))
            elif p.suffix in ['.jsonl', '.json']:
                lfs.append(pl.scan_json(str(p), json_lines=p.suffix == '.jsonl'))
            elif p.suffix == '.csv':
                lfs.append(pl.scan_csv(str(p)))
        if lfs:
            lf = pl.concat(lfs)
        else:
            raise ValueError("No valid data files to scan.")
        
        # Verify columns without full collect
        try:
            available_cols = lf.collect_schema().names()
            logger.info(f"Available columns in dataset: {available_cols}")
            missing_cols = [col for col in self.usecols if col not in available_cols]
            if missing_cols:
                raise ValueError(f"Missing columns in dataset: {missing_cols}. Check dataset structure.")
        except Exception as e:
            logger.error(f"Failed to read dataset columns: {e}")
            raise

        # Set single crawlTimestamp
        crawl_timestamp = datetime.now().isoformat()
        logger.info(f"Using crawlTimestamp: {crawl_timestamp}")

        # Early filter on Data Quality == 'Full'
        lf = lf.filter(pl.col("Data Quality") == "Full").select(self.usecols)

        # Apply transforms lazily
        section_raw = pl.col("Article Category")
        lf = lf.with_columns([
            pl.lit(crawl_timestamp).alias('crawlTimestamp'),
            pl.col("URL").str.extract(r"theguardian\.com/(.*)", 1).alias('id'),
            (pl.col("Publication Date")
             .str.to_datetime("%Y-%m-%dT%H:%M:%S.%fZ")
             .alias('webPublicationDate')),
            pl.col('Article Title').alias('webTitle'),
            pl.col('Article Contents').alias('bodyText'),
            pl.col('URL').alias('webUrl'),
            # Full titlecase for sectionName
            section_raw.str.split(" ").list.eval(
                pl.concat_str([
                    pl.element().str.slice(0, 1).str.to_uppercase(),
                    pl.element().str.slice(1).str.to_lowercase()
                ], separator="")
            ).list.join(" ").alias('sectionName')
        ])

        # Eager collect once for stats + filter (combines everything; ~5-7s)
        df = lf.collect(engine='streaming')
        total_rows = df.height
        invalid_dates = df['webPublicationDate'].null_count()
        sample_lf = df.head(100)
        logger.info(f"Invalid dates dropped: {invalid_dates}")
        nan_counts = {
            'id': sample_lf['id'].null_count(),
            'webPublicationDate': sample_lf['webPublicationDate'].null_count(),
            'webTitle': sample_lf['webTitle'].null_count(),
            'bodyText': sample_lf['bodyText'].null_count()
        }
        logger.info(f"Sample NaN counts: {nan_counts}")

        # Filter in eager (fast in-memory)
        df = df.filter(
            pl.col('id').is_not_null() &
            pl.col('webPublicationDate').is_not_null() &
            pl.col('webTitle').is_not_null() &
            pl.col('bodyText').is_not_null()
        )

        if df.height == 0:
            logger.warning("No articles processed. Check dataset content or column mappings.")

        # Derive tags from category (since no original Tags column)
        def make_tags_from_category(category: pl.Expr) -> pl.Expr:
            cat_lower = category.str.to_lowercase().str.replace_all(r"\s+", "-", literal=False)
            id_tag = pl.concat_str([pl.lit("tag_"), cat_lower], separator="")
            json_tag = pl.concat_str([
                pl.lit('{"id": "'), id_tag, pl.lit('", "type": "keyword", "webTitle": "'), category, pl.lit('"}')
            ], separator="")
            full_json = pl.concat_str([pl.lit("["), json_tag, pl.lit("]")], separator="")
            return (
                pl.when(category.is_null())
                .then(pl.lit('[]'))
                .otherwise(full_json)
                .alias("tags")
            )

        # Apply tags + final select (eager)
        df = df.with_columns(
            make_tags_from_category(pl.col('sectionName'))
        ).select([
            'crawlTimestamp', 'id', 'webPublicationDate', 'webTitle',
            'bodyText', 'tags', 'webUrl', 'sectionName'
        ])

        # Write to CSV (DataFrame method)
        df.write_csv(str(self.output_csv))
        total_records = df.height  # Already eager—no query needed
        total_dropped = total_rows - total_records
        logger.info(f"Total dropped: {total_dropped} rows due to missing fields")
        logger.info(f"Preprocessed batch data saved to {self.output_csv} ({total_records} records)")

def main():
    collector = BatchDataCollector()
    try:
        local_dir = collector.local_dir
        # Check for existing data files (Parquet/JSON/CSV)
        parquet_files = list(local_dir.rglob("*.parquet"))
        jsonl_files = list(local_dir.rglob("*.jsonl")) + list(local_dir.rglob("*.json"))
        csv_files = list(local_dir.rglob("*.csv"))
        existing_files = parquet_files + jsonl_files + csv_files
        if not existing_files:
            collector.download_dataset()
            existing_files = collector.raw_data_paths
        else:
            logger.info(f"Using existing HF dataset in {local_dir}")
            collector.raw_data_paths = existing_files
        
        # Profile the preprocessing
        pr = cProfile.Profile()
        pr.enable()
        collector.preprocess_data(existing_files)
        pr.disable()
        pr.print_stats(sort='cumtime')
        
        # Display first row of preprocessed data
        try:
            first_row = pl.read_csv(collector.output_csv, n_rows=1).to_pandas().to_dict(orient='records')[0]
            print(f"First row of preprocessed data:\n{first_row}")
        except Exception as e:
            print(f"Failed to read first row of output CSV: {e}")
    except Exception as e:
        print(f"Batch data collection failed: {e}")

if __name__ == "__main__":
    main()