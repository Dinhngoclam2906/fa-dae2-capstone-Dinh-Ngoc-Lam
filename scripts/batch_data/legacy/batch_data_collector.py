# batch_data_collector.py - Optimized for Airflow
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import List
import warnings

# Suppress warnings
warnings.filterwarnings("ignore", message=".*un-recognized timezone.*")
warnings.filterwarnings("ignore", message=".*chrono.*")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchDataCollector:
    def __init__(self, max_articles: int = 100):
        self.max_articles = max_articles
        self.data_dir = Path("data/batch")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_parquet = self.data_dir / "guardian_historical_articles.parquet"
        self.presentation_csv = self.data_dir / "guardian_historical_articles_sample.csv"
        self.repo_id = "Stefan171/TheGuardian-Articles"
        self.local_dir = self.data_dir / "TheGuardian-Articles"  # Simplified name
        self.usecols = ['URL', 'Article Category', 'Publication Date', 'Article Title', 'Article Contents', 'Data Quality']

    def download_if_needed(self):
        """Only download if local data doesn't exist."""
        existing_data_files = list(self.local_dir.rglob("*.parquet")) + \
                              list(self.local_dir.rglob("*.json*")) + \
                              list(self.local_dir.rglob("*.csv"))
        if existing_data_files:
            logger.info(f"Found existing dataset files in {self.local_dir}. Skipping download.")
            self.raw_data_paths = existing_data_files
            return

        logger.info(f"Downloading Hugging Face dataset: {self.repo_id}")
        try:
            from huggingface_hub import snapshot_download
            snapshot_download(
                repo_id=self.repo_id,
                local_dir=str(self.local_dir),
                repo_type="dataset",
                local_dir_use_symlinks=False,  # Safer in containers
                resume_download=True,          # Resume if interrupted
            )
            # Find data files
            self.raw_data_paths = list(self.local_dir.rglob("*.parquet")) + \
                                  list(self.local_dir.rglob("*.json*")) + \
                                  list(self.local_dir.rglob("*.csv"))
            if not self.raw_data_paths:
                raise FileNotFoundError("No data files found after download")
            logger.info(f"Downloaded dataset. Found {len(self.raw_data_paths)} data files.")
        except ImportError:
            raise ImportError("huggingface_hub not installed. Run: pip install huggingface_hub")
        except Exception as e:
            raise RuntimeError(f"Failed to download dataset: {e}")

    def preprocess_and_save(self):
        """Main processing — only runs if output doesn't exist."""
        # if self.output_parquet.exists():
        #     logger.info(f"Preprocessed Parquet already exists: {self.output_parquet}")
        #     logger.info("Skipping preprocessing (idempotent run).")
        #     return self.output_parquet

        logger.info("Starting preprocessing of historical batch data")

        # Lazy load all files
        import polars as pl
        lfs = []
        for p in self.raw_data_paths:
            try:
                if p.suffix == '.parquet':
                    lfs.append(pl.scan_parquet(str(p)))
                elif p.suffix in ['.jsonl', '.ndjson']:
                    lfs.append(pl.scan_ndjson(str(p)))
                elif p.suffix == '.json':
                    lfs.append(pl.read_json(str(p)).lazy())
                elif p.suffix == '.csv':
                    lfs.append(pl.scan_csv(str(p)))
            except Exception as e:
                logger.warning(f"Could not read {p}: {e}")
                continue

        if not lfs:
            raise ValueError("No readable data files found")

        lf = pl.concat(lfs)

        # Filter and transform
        crawl_timestamp = datetime.now().isoformat()
        lf = (
            lf.filter(pl.col("Data Quality") == "Full")
              .select(self.usecols)
              .with_columns([
                  pl.lit(crawl_timestamp).alias('crawl_timestamp'),
                  pl.col("URL").str.extract(r"theguardian\.com/(.*)", 1).alias('article_id'),
                  pl.col("Publication Date").str.to_datetime("%Y-%m-%dT%H:%M:%S.%fZ").alias('web_publication_date'),
                  pl.col("Article Title").alias('web_title'),
                  pl.col("Article Contents").alias('body_text'),
                  pl.col("URL").alias('web_url'),
                  pl.col("Article Category")
                      .str.split(" ")
                      .list.eval(
                          pl.element().str.slice(0, 1).str.to_uppercase() +
                          pl.element().str.slice(1).str.to_lowercase()
                      )
                      .list.join(" ")
                      .alias('section_name')
              ])
              .filter(
                  pl.col('article_id').is_not_null() &
                  pl.col('web_publication_date').is_not_null() &
                  pl.col('web_title').is_not_null() &
                  pl.col('body_text').is_not_null()
              )
        )

        df = lf.collect(streaming=True)

        # Sample
        sample_size = min(self.max_articles, df.height)
        if sample_size == 0:
            raise ValueError("No valid articles after filtering")

        df = df.sample(n=sample_size, shuffle=True)
        df = df.with_row_index("id", offset=1)

        final_cols = ['id', 'crawl_timestamp', 'article_id', 'web_publication_date',
                      'web_title', 'body_text', 'web_url', 'section_name']
        df = df.select(final_cols)

        # Save
        df.write_parquet(str(self.output_parquet))
        logger.info(f"Saved {df.height} historical articles to {self.output_parquet}")

        # Sample CSV for presentation
        sample_df = df.head(100)
        sample_df.write_csv(str(self.presentation_csv))
        logger.info(f"Sample CSV saved: {self.presentation_csv}")

        return self.output_parquet

    def run(self):
        """Main entry point — safe and idempotent."""
        try:
            self.download_if_needed()
            output_path = self.preprocess_and_save()
            logger.info(f"Batch collection complete: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Batch collection failed: {e}", exc_info=True)
            raise

def main():
    collector = BatchDataCollector(max_articles=100)
    collector.run()

if __name__ == "__main__":
    main()