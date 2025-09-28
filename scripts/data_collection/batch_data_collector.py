import logging
import os
import zipfile
from pathlib import Path
import pandas as pd
from datetime import datetime
import json
from ast import literal_eval
from tqdm import tqdm

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

        # Initialize output CSV with headers
        final_columns = ['crawlTimestamp', 'id', 'webPublicationDate', 'webTitle', 'bodyText', 'tags', 'webUrl', 'sectionName']
        pd.DataFrame(columns=final_columns).to_csv(self.output_csv, index=False, encoding='utf-8')

        # Set single crawlTimestamp for the entire run
        crawl_timestamp = datetime.now().isoformat()
        logger.info(f"Using crawlTimestamp: {crawl_timestamp}")

        # Estimate total rows for progress bar
        total_rows = sum(1 for _ in open(csv_path, encoding='utf-8')) - 1  # Subtract header
        chunk_size = 5000
        total_records = 0

        # Process CSV with progress bar
        with tqdm(total=total_rows, desc="Processing rows", unit="rows") as pbar:
            for chunk in pd.read_csv(csv_path, encoding='utf-8', usecols=load_cols, chunksize=chunk_size, low_memory=False):
                # Rename columns
                chunk = chunk.rename(columns={v: k for k, v in column_mapping.items() if v})

                # Add missing columns
                chunk['id'] = [url.split('theguardian.com/')[1] if isinstance(url, str) and 'theguardian.com/' in url else f"historical_{i + total_records}" for i, url in enumerate(chunk.get('webUrl', pd.Series([''] * len(chunk))))]
                chunk['crawlTimestamp'] = crawl_timestamp
                chunk['sectionName'] = 'Technology'
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

                # Handle date parsing with specific format
                chunk['webPublicationDate'] = pd.to_datetime(chunk['webPublicationDate'], format='%a %d %b %Y %H.%M %Z', errors='coerce')

                # Drop rows with missing critical fields
                chunk = chunk.dropna(subset=['id', 'webPublicationDate', 'webTitle', 'bodyText'])

                # Format tags to match real-time schema
                def format_tags(tag):
                    try:
                        tags_list = literal_eval(tag) if isinstance(tag, str) else []
                        return json.dumps([{'id': f"tag_{t.lower().replace(' ', '-')}", 'type': 'keyword', 'webTitle': t} for t in tags_list])
                    except:
                        return json.dumps([])

                chunk['tags'] = chunk['tags'].apply(format_tags)

                # Reorder columns
                chunk = chunk[final_columns]

                # Append to output CSV
                if not chunk.empty:
                    chunk.to_csv(self.output_csv, mode='a', header=False, index=False, encoding='utf-8')
                    total_records += len(chunk)
                    pbar.update(len(chunk))

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