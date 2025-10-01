import logging
import os
import zipfile
from pathlib import Path
import pandas as pd
from datetime import datetime
import json
from ast import literal_eval
from tqdm import tqdm
import warnings

# Suppress pandas unrecognized timezone warnings
warnings.filterwarnings("ignore", message=".*un-recognized timezone.*")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchDataCollector:
    def __init__(self):
        self.data_dir = Path("data/batch")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_csv = self.data_dir / "guardian_historical_preprocessed.csv"
        self.kaggle_dataset = "kiet21042003/news-articles-of-the-guardian-112015-23112023"
        self.download_path = self.data_dir / f"{self.kaggle_dataset.split('/')[-1]}.zip"
        # Possible columns in Kaggle dataset
        self.usecols = ['URL', 'url', 'Title', 'title', 'Description', 'description', 'Content', 'content', 
                        'Time', 'time', 'Date', 'date', 'Tags', 'tags']

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
            # Exclude output CSV
            csv_files = [f for f in self.data_dir.glob("*.csv") if f.name != self.output_csv.name]
            if csv_files:
                logger.info(f"Found raw CSV: {csv_files[0]}")
                return csv_files[0]
            else:
                raise FileNotFoundError("No raw CSV found in extracted files.")
        else:
            raise FileNotFoundError(f"ZIP file not found at {self.download_path}")

    def parse_date(self, date_str):
        """Parse date string with multiple formats for robustness."""
        if pd.isna(date_str) or not isinstance(date_str, str):
            return pd.NaT
        date_str = date_str.strip().replace('.', ':')  # Fix Guardian dot-time to colon
        formats_to_try = [
            '%a %d %b %Y %H:%M %Z',   # "Sun 29 Mar 2015 21:40 BST" (post-fix)
            '%a %d %b %Y %H.%M %Z',   # Legacy dot variant (if any slip through)
            '%Y-%m-%d %H:%M:%S%z',    # ISO with tz "2015-01-01 23:11:00+0000"
            '%Y-%m-%d %H:%M:%S',      # ISO no tz "2015-01-01 23:11:00"
            '%d %b %Y %H:%M %Z',      # Short day "01 Jan 2015 23:11 GMT"
            '%d/%m/%Y %H:%M',         # UK "01/01/2015 23:11"
        ]
        for fmt in formats_to_try:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except ValueError:
                continue
        # Final broad fallback
        return pd.to_datetime(date_str, errors='coerce')

    def preprocess_data(self, csv_path: Path):
        """Preprocess the historical data to match real-time schema, using chunks."""
        logger.info(f"Loading batch data from {csv_path}")
        
        # Check available columns
        try:
            sample_df = pd.read_csv(csv_path, nrows=1)
            available_cols = sample_df.columns
            logger.info(f"Available columns in CSV: {list(available_cols)}")
            load_cols = [col for col in self.usecols if col in available_cols]
            if not load_cols:
                raise ValueError("No matching columns found in CSV. Check dataset structure.")
        except Exception as e:
            logger.error(f"Failed to read CSV columns: {e}")
            raise

        # Column mappings
        column_mapping = {
            'webUrl': next((col for col in ['URL', 'url', 'link'] if col in available_cols), None),
            'webTitle': next((col for col in ['Title', 'title', 'headline'] if col in available_cols), None),
            'bodyText': next((col for col in ['Content', 'content', 'article', 'body'] if col in available_cols), None),
            'webPublicationDate': next((col for col in ['Time', 'time', 'Date', 'date', 'publicationDate'] if col in available_cols), None),
            'tags': next((col for col in ['Tags', 'tags'] if col in available_cols), None)
        }
        logger.info(f"Column mappings: {column_mapping}")

        # Set single crawlTimestamp for the entire run
        crawl_timestamp = datetime.now().isoformat()
        logger.info(f"Using crawlTimestamp: {crawl_timestamp}")

        # Estimate total rows for progress bar
        total_rows = sum(1 for _ in open(csv_path, encoding='utf-8')) - 1  # Subtract header
        chunk_size = 10000  # Optimized size
        total_records = 0
        total_dropped = 0
        all_chunks = []  # Collect for single write

        final_columns = ['crawlTimestamp', 'id', 'webPublicationDate', 'webTitle', 'bodyText', 'tags', 'webUrl', 'sectionName']

        # Process CSV with progress bar (dtype=str for perf)
        with tqdm(total=total_rows, desc="Processing rows", unit="rows") as pbar:
            for chunk_idx, chunk in enumerate(pd.read_csv(csv_path, encoding='utf-8', usecols=load_cols, chunksize=chunk_size, low_memory=False, dtype=str)):
                # Rename columns
                chunk = chunk.rename(columns={v: k for k, v in column_mapping.items() if v})

                # Add missing columns
                chunk['id'] = [url.split('theguardian.com/')[1] if isinstance(url, str) and 'theguardian.com/' in url else f"historical_{i + total_records}" for i, url in enumerate(chunk.get('webUrl', pd.Series([''] * len(chunk))))]
                chunk['crawlTimestamp'] = crawl_timestamp
                if 'tags' not in chunk.columns:
                    chunk['tags'] = json.dumps([])
                if 'webUrl' not in chunk.columns:
                    chunk['webUrl'] = ''
                if 'webTitle' not in chunk.columns:
                    chunk['webTitle'] = ''
                if 'bodyText' not in chunk.columns:
                    chunk['bodyText'] = ''
                if 'webPublicationDate' not in chunk.columns:
                    chunk['webPublicationDate'] = pd.NaT

                # Derive sectionName from URL path (fallback to 'Unknown')
                def get_section(url):
                    if isinstance(url, str) and 'theguardian.com/' in url:
                        path = url.split('theguardian.com/')[1].split('/')[0]
                        return path.capitalize() if path else 'Unknown'
                    return 'Unknown'
                chunk['sectionName'] = chunk['webUrl'].apply(get_section)

                # Handle date parsing with custom function
                chunk['webPublicationDate'] = chunk['webPublicationDate'].apply(self.parse_date)

                # Summary stats (first chunk only)
                if chunk_idx == 0:
                    nan_counts = {
                        'id': chunk['id'].isna().sum(),
                        'webPublicationDate': chunk['webPublicationDate'].isna().sum(),
                        'webTitle': chunk['webTitle'].isna().sum(),
                        'bodyText': chunk['bodyText'].isna().sum()
                    }
                    logger.info(f"Sample NaN counts: {nan_counts}")

                # Drop rows with missing critical fields
                before_drop = len(chunk)
                chunk = chunk.dropna(subset=['id', 'webPublicationDate', 'webTitle', 'bodyText'])
                dropped = before_drop - len(chunk)
                total_dropped += dropped
                if dropped > 50:  # Threshold for noise
                    logger.info(f"Chunk {chunk_idx}: Dropped {dropped} rows due to missing fields")

                # Format tags to match real-time schema
                def format_tags(tag):
                    if pd.isna(tag):
                        return json.dumps([])
                    try:
                        if isinstance(tag, str):
                            # First, try as JSON (for list of dicts)
                            try:
                                parsed = json.loads(tag)
                                if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                                    return tag  # Already in desired format
                            except json.JSONDecodeError:
                                pass
                            # Fallback to literal_eval for Python list string like "['Canada', ...]"
                            parsed = literal_eval(tag)
                            if isinstance(parsed, list) and parsed and isinstance(parsed[0], str):
                                formatted = [{'id': f"tag_{t.lower().replace(' ', '-')}", 'type': 'keyword', 'webTitle': t} for t in parsed]
                                return json.dumps(formatted)
                        return json.dumps([])
                    except:
                        return json.dumps([])

                chunk['tags'] = chunk['tags'].apply(format_tags)

                # Reorder columns
                chunk = chunk[final_columns]

                # Collect for single write
                if not chunk.empty:
                    all_chunks.append(chunk)
                    total_records += len(chunk)
                    pbar.update(len(chunk))

        # Single write at end
        if all_chunks:
            final_df = pd.concat(all_chunks, ignore_index=True)
            final_df.to_csv(self.output_csv, index=False, encoding='utf-8')
            logger.info(f"Final write complete: {len(final_df)} records")

        logger.info(f"Total dropped across all chunks: {total_dropped}")
        if total_records == 0:
            logger.warning("No articles processed. Check CSV content or column mappings.")
        logger.info(f"Preprocessed batch data saved to {self.output_csv} ({total_records} records)")

def main():
    collector = BatchDataCollector()
    try:
        if not collector.download_path.exists():
            collector.download_dataset()
        csv_path = collector.extract_zip()
        collector.preprocess_data(csv_path)
        # Display first row of preprocessed data
        try:
            first_row = pd.read_csv(collector.output_csv, nrows=1)
            print(f"First row of preprocessed data:\n{first_row.to_dict(orient='records')[0]}")
        except Exception as e:
            print(f"Failed to read first row of output CSV: {e}")
    except Exception as e:
        print(f"Batch data collection failed: {e}")

if __name__ == "__main__":
    main()