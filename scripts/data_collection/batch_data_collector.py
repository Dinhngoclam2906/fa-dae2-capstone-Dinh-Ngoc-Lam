import logging
import os
import zipfile
from pathlib import Path
from datetime import datetime
import json
from ast import literal_eval
import warnings
import pandas as pd
import multiprocessing

# Detect CPU count and set thread pool size before importing Polars
cpu_count = multiprocessing.cpu_count()
print(f"Detected {cpu_count} CPU cores.")
# os.environ['POLARS_MAX_THREADS'] = str(min(cpu_count, 32))  # Cap at 32 to avoid overhead
os.environ['POLARS_MAX_THREADS'] = '12'  # Cap at 32 to avoid overhead

# Now import Polars
import polars as pl
import cProfile

# Suppress warnings if needed
warnings.filterwarnings("ignore", message=".*un-recognized timezone.*")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchDataCollector:
    def __init__(self):
        self.data_dir = Path("data/batch")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_parquet = self.data_dir / "guardian_historical_preprocessed.parquet"
        self.output_csv_legacy = self.data_dir / "guardian_historical_preprocessed.csv"  # For exclusion
        self.kaggle_dataset = "kiet21042003/news-articles-of-the-guardian-112015-23112023"
        self.download_path = self.data_dir / f"{self.kaggle_dataset.split('/')[-1]}.zip"
        # Fixed columns based on CSV sample
        self.usecols = ['URL', 'Title', 'Content', 'Time', 'Tags']

    def download_dataset(self):
        """Download the Kaggle dataset using Kaggle API."""
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files(self.kaggle_dataset, path=str(self.data_dir), unzip=False)
            logger.info(f"Downloaded dataset to {self.download_path}")
        except ImportError:
            logger.error("Kaggle library not found. Install with 'pip install kaggle' and set up kaggle.json.")
            raise
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}. Download manually from https://www.kaggle.com/datasets/{self.kaggle_dataset} and place in data/batch.")
            raise

    def extract_zip(self):
        """Extract the downloaded ZIP file and return the raw CSV path."""
        if self.download_path.exists():
            with zipfile.ZipFile(self.download_path, 'r') as zip_ref:
                zip_ref.extractall(self.data_dir)
            logger.info(f"Extracted ZIP to {self.data_dir}")
            # Exclude output files (legacy CSV and current Parquet)
            exclude_names = {self.output_parquet.name, self.output_csv_legacy.name}
            csv_files = [f for f in self.data_dir.glob("*.csv") if f.name not in exclude_names]
            parquet_files = [f for f in self.data_dir.glob("*.parquet") if f.name not in exclude_names]
            all_files = csv_files + parquet_files
            if all_files:
                # Sort to prefer CSV over Parquet, and raw over processed
                all_files.sort(key=lambda f: (f.suffix, f.name))
                logger.info(f"Found raw CSV: {all_files[0]}")
                return all_files[0]
            else:
                raise FileNotFoundError("No raw CSV found in extracted files.")
        else:
            raise FileNotFoundError(f"ZIP file not found at {self.download_path}")

    def preprocess_data(self, csv_path: Path):
        """Preprocess the historical data using Polars for speed and vectorization."""
        logger.info(f"Loading batch data from {csv_path}")
        
        # Verify columns
        try:
            sample_df = pl.read_csv(csv_path, n_rows=1)
            available_cols = sample_df.columns
            logger.info(f"Available columns in CSV: {available_cols}")
            missing_cols = [col for col in self.usecols if col not in available_cols]
            if missing_cols:
                raise ValueError(f"Missing columns in CSV: {missing_cols}. Check dataset structure.")
        except Exception as e:
            logger.error(f"Failed to read CSV columns: {e}")
            raise

        # Set single crawlTimestamp
        crawl_timestamp = datetime.now().isoformat()
        logger.info(f"Using crawlTimestamp: {crawl_timestamp}")

        # Schema for all available columns to avoid mismatch
        schema = {col: pl.String for col in available_cols}

        # Scan CSV lazily, select early, apply basic transforms, then eager collect (fast for this size)
        section_raw = pl.col("URL").str.extract(r"theguardian\.com/([a-zA-Z0-9_-]+)", 1)
        lf = pl.scan_csv(
            str(csv_path), 
            schema=schema, 
            ignore_errors=True,
            infer_schema_length=10000
        ).select(self.usecols).with_columns([
            pl.lit(crawl_timestamp).alias('crawlTimestamp'),
            pl.col("URL").str.extract(r"theguardian\.com/(.*)", 1).alias('id'),
            (pl.col("Time")
             .str.replace(".", ":", literal=True)
             .str.strptime(pl.Datetime, format='%a %d %b %Y %H:%M %Z', strict=True)
             .alias('webPublicationDate')),
            pl.col('Title').alias('webTitle'),
            pl.col('Content').alias('bodyText'),
            pl.col('URL').alias('webUrl'),
            # Full titlecase for sectionName
            section_raw.str.split("-").list.eval(
                pl.concat_str([
                    pl.element().str.slice(0, 1).str.to_uppercase(),
                    pl.element().str.slice(1).str.to_lowercase()
                ], separator="")
            ).list.join(" ").alias('sectionName')
        ])

        # Eager collect once for stats + filter (combines everything; ~5-7s)
        df = lf.collect()  # No engine=streaming—eager is faster here
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
            logger.warning("No articles processed. Check CSV content or column mappings.")

        # Vectorized tag formatting (unchanged; works on DataFrame)
        def parse_tags_vectorized(tags_col: pl.Expr) -> pl.Expr:
            tags_clean = tags_col.str.replace_all(r"^\['", "", literal=False).str.replace_all(r"'\]$", "", literal=False)
            tags_list = tags_clean.str.split("', '")
            id_expr = pl.concat_str([
                pl.lit("tag_"), 
                pl.element().str.to_lowercase().str.replace_all(r"\s+", "-", literal=False)
            ], separator="").alias("id")
            tag_struct = pl.struct([
                id_expr,
                pl.lit("keyword").alias("type"),
                pl.element().alias("webTitle")
            ])
            tag_structs = tags_list.list.eval(tag_struct)
            json_part = pl.concat_str([
                pl.lit('{ "id": "'),
                pl.element().struct.field("id"),
                pl.lit('", "type": "keyword", "webTitle": "'),
                pl.element().struct.field("webTitle"),
                pl.lit('" }')
            ], separator="")
            tag_jsons = tag_structs.list.eval(json_part)
            joined = tag_jsons.list.join(", ")
            full_json = pl.concat_str([pl.lit("["), joined, pl.lit("]")], separator="")
            return (
                pl.when(tags_col.is_null() | (tags_col.str.len_chars() == 0))
                .then(pl.lit('[]'))
                .otherwise(
                    pl.when(tags_list.list.len() == 0)
                    .then(pl.lit('[]'))
                    .otherwise(full_json)
                )
                .alias("tags")
            )

        # Apply tags + final select (eager)
        df = df.with_columns(
            parse_tags_vectorized(pl.col('Tags'))
        ).select([
            'crawlTimestamp', 'id', 'webPublicationDate', 'webTitle',
            'bodyText', 'tags', 'webUrl', 'sectionName'
        ])

        # Write to Parquet (DataFrame method)
        df.write_parquet(str(self.output_parquet), compression='snappy')
        total_records = df.height  # Already eager—no query needed
        total_dropped = total_rows - total_records
        logger.info(f"Total dropped: {total_dropped} rows due to missing fields")
        logger.info(f"Preprocessed batch data saved to {self.output_parquet} ({total_records} records)")

def main():
    collector = BatchDataCollector()
    try:
        if not collector.download_path.exists():
            collector.download_dataset()
        csv_path = collector.extract_zip()
        
        # Profile the preprocessing
        pr = cProfile.Profile()
        pr.enable()
        collector.preprocess_data(csv_path)
        pr.disable()
        pr.print_stats(sort='cumtime')
        
        # Display first row of preprocessed data
        try:
            first_row = pl.read_parquet(collector.output_parquet, n_rows=1).to_pandas().to_dict(orient='records')[0]
            print(f"First row of preprocessed data:\n{first_row}")
        except Exception as e:
            print(f"Failed to read first row of output Parquet: {e}")
    except Exception as e:
        print(f"Batch data collection failed: {e}")

if __name__ == "__main__":
    main()