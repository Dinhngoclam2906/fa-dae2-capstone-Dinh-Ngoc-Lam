import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional
import aiohttp
from aiohttp import ClientTimeout
import asyncio
from tqdm.asyncio import tqdm
import requests
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from textblob import TextBlob
import orjson
import random
import glob
import ijson
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Custom stopwords list
CUSTOM_STOPWORDS = {
    'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 'has', 'he',
    'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the', 'to', 'was', 'were', 'will',
    'with', 'this', 'but', 'or', 'not', 'all', 'any', 'some', 'such', 'no', 'only',
    'own', 'so', 'than', 'too', 'very', 'can', 'just', 'should', 'now', 'said',
    'also', 'would', 'like', 'could', 'new', 'one', 'two', 'get', 'use', 'first',
    'last', 'many', 'more', 'most', 'other', 'our', 'their', 'there', 'what', 'when',
    'where', 'which', 'who', 'why', 'how', 'been', 'being', 'have', 'had', 'do',
    'does', 'did', 'doing'
}

class APIDataCollector:
    def __init__(self, api_name: str, from_date: Optional[str] = None):
        self.api_name = api_name
        self.api_url = os.getenv("API_URL", "https://content.guardianapis.com")
        self.api_key = os.getenv("API_KEY")
        self.max_records = int(os.getenv("MAX_RECORDS", 1000))
        self.batch_size = int(os.getenv("BATCH_SIZE", 200))
        self.max_retries = int(os.getenv("MAX_RETRIES", 3))
        default_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        self.from_date = os.getenv("FROM_DATE", default_date) if from_date is None else from_date
        try:
            datetime.strptime(self.from_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid FROM_DATE format: {self.from_date}. Must be YYYY-MM-DD")
        
        self.data_dir = Path("data/real_time")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.data_dir / "checkpoint.json"
        self.temp_checkpoint = self.data_dir / "temp_checkpoint.json"
        self.temp_dir = self.data_dir / "temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.request_times = []
        
        try:
            from nltk.corpus import stopwords
            self.stopwords = set(stopwords.words('english'))
            logger.info("Using NLTK stopwords as base for word cloud")
        except (ImportError, LookupError) as e:
            logger.warning(f"NLTK stopwords not available ({e}), using custom stopwords list")
            self.stopwords = set()
        self.stopwords |= CUSTOM_STOPWORDS
        
        if self.checkpoint_file.exists():
            for _ in range(3):
                try:
                    with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                        checkpoint = orjson.loads(f.read())
                    if checkpoint.get('from_date') != self.from_date:
                        logger.info("from_date changed, resetting checkpoint")
                        self.checkpoint_file.unlink()
                    break
                except PermissionError:
                    logger.warning("Checkpoint file is locked, retrying...")
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Failed to reset checkpoint: {e}")
                    break
            else:
                logger.warning("Unable to reset checkpoint file due to persistent lock, using temp checkpoint")
                self.checkpoint_file = self.temp_checkpoint
        
        logger.info(f"API_URL: {self.api_url}")
        logger.info(f"API_KEY: {'Set' if self.api_key else 'Not set'}")
        logger.info(f"FROM_DATE: {self.from_date}")
        logger.info(f"MAX_RECORDS: {self.max_records}, BATCH_SIZE: {self.batch_size}, MAX_RETRIES: {self.max_retries}")

        if not self.api_key:
            raise ValueError("GUARDIAN_API_KEY not found in environment variables")
        if not self.api_url.startswith("https://content.guardianapis.com"):
            raise ValueError(f"Invalid API_URL: {self.api_url}. Must be https://content.guardianapis.com")

    def _load_checkpoint(self) -> int:
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'rb') as f:
                    checkpoint = orjson.loads(f.read())
                return checkpoint.get('last_page', 1)
            except PermissionError:
                logger.warning("Checkpoint file is locked, starting from page 1")
                return 1
        return 1

    def _save_checkpoint(self, page: int) -> None:
        try:
            with open(self.checkpoint_file, 'wb') as f:
                f.write(orjson.dumps({'last_page': page, 'from_date': self.from_date}))
        except PermissionError:
            logger.warning("Unable to write checkpoint file due to lock, continuing without saving checkpoint")

    def _clean_checkpoint(self) -> bool:
        """
        Clean up checkpoint files and cached JSON files in temp_dir, with retries for file locks.

        Returns:
            bool: True if all files and directory were successfully removed or didn't exist, False otherwise.
        """
        success = True
        initial_delay = 0.5  # Initial retry delay in seconds
        files_to_delete = [self.checkpoint_file, self.temp_checkpoint] + list(self.temp_dir.glob("*.*"))

        for file_path in files_to_delete:
            if not file_path.exists():
                logger.debug(f"File {file_path} does not exist, skipping")
                continue
            for attempt in range(self.max_retries):
                try:
                    file_path.unlink()
                    logger.info(f"File {file_path} successfully removed")
                    break
                except PermissionError as e:
                    if attempt < self.max_retries - 1:
                        delay = initial_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Permission denied for {file_path} (attempt {attempt + 1}/{self.max_retries}): {e}, retrying after {delay:.2f}s")
                        time.sleep(delay)
                    else:
                        logger.error(f"Failed to remove {file_path} after {self.max_retries} attempts: {e}")
                        success = False
                except OSError as e:
                    logger.error(f"Failed to remove {file_path}: {e}")
                    success = False
                    break
                except Exception as e:
                    logger.error(f"Unexpected error while removing {file_path}: {e}")
                    success = False
                    break

        # Attempt to remove temp_dir if empty
        if self.temp_dir.exists():
            try:
                self.temp_dir.rmdir()
                logger.info(f"Temporary directory {self.temp_dir} removed")
            except OSError as e:
                logger.warning(f"Failed to remove temporary directory {self.temp_dir}: {e}")
                success = False

        return success

    def _load_cached_page(self, page: int) -> Optional[List[Dict]]:
        cache_file = self.temp_dir / f"page_{page}.jsonl"
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    data = [orjson.loads(line) for line in f]
                logger.info(f"Loaded cached data for page {page} from {cache_file}")
                return data
            except (PermissionError, orjson.JSONDecodeError) as e:
                logger.warning(f"Failed to load cached page {page}: {e}")
        return None

    async def _fetch_page(self, session: aiohttp.ClientSession, url: str, params: Dict, page: int, retry: int = 0) -> tuple[List[Dict], Optional[int]]:
        # Return (results, total) for proper total access
        cache_file = self.temp_dir / f"page_{page}.jsonl"
        cached_data = self._load_cached_page(page)
        if cached_data:
            return cached_data, None  # No total in cache; will use inf

        params['page'] = page
        try:
            start_time = time.time()
            # Use ClientTimeout instance instead of int
            timeout = ClientTimeout(total=30)
            async with session.get(url, params=params, timeout=timeout) as response:
                logger.debug(f"All response headers for page {page}: {dict(response.headers)}")
                remaining = response.headers.get('X-RateLimit-Remaining', 'Unknown')
                reset_time = response.headers.get('X-RateLimit-Reset', 'Unknown')
                logger.info(f"Rate limit status for page {page}: Remaining={remaining}, Reset={reset_time}")
                if response.status == 429:
                    delay = int(reset_time) - time.time() if reset_time != 'Unknown' and reset_time.isdigit() else min(5 * (2 ** retry), 60)
                    logger.warning(f"Rate limit exceeded, waiting {delay:.2f} seconds")
                    await asyncio.sleep(max(delay, 1))
                    if retry < self.max_retries:
                        return await self._fetch_page(session, url, params, page, retry + 1)
                    raise ValueError(f"Max retries ({self.max_retries}) reached after 429 error")
                if response.status == 400:
                    logger.warning(f"400 Bad Request on page {page}: {await response.text()}")
                    return [], None
                response.raise_for_status()

                network_time = time.time() - start_time
                logger.info(f"Page {page} network time: {network_time:.2f}s")
                start_process = time.time()
                text = await response.text()
                data = await asyncio.to_thread(orjson.loads, text)
                process_time = time.time() - start_process
                logger.info(f"Page {page} JSON processing time: {process_time:.2f}s")

                if not isinstance(data, dict) or 'response' not in data or 'results' not in data['response']:
                    logger.error(f"Invalid response structure for page {page}")
                    raise ValueError(f"Unexpected API response format for page {page}")
                request_time = time.time() - start_time
                logger.info(f"Page {page} full request time: {request_time:.2f}s")
                self.request_times.append(request_time)
                results = data['response']['results']
                total = data['response'].get('total', float('inf'))  # Extract total here
                if not isinstance(results, list):
                    logger.warning(f"Results for page {page} is not a list: {type(results)}")
                    return [], total
                try:
                    with open(cache_file, 'wb') as f:
                        for result in results:
                            f.write(orjson.dumps(result) + b'\n')
                except PermissionError:
                    logger.warning(f"Unable to cache page {page} due to file lock")
                return results, total
        except aiohttp.ClientError as e:
            if retry < self.max_retries:
                logger.warning(f"Retry {retry + 1}/{self.max_retries} for page {page} after error: {e}")
                await asyncio.sleep(min(2 ** retry, 10))
                return await self._fetch_page(session, url, params, page, retry + 1)
            logger.error(f"Failed to fetch page {page} after {self.max_retries} retries: {e}")
            raise

    async def collect_data(self, endpoint: str, query_params: Optional[Dict] = None) -> Path:
        collected_ids = set()
        collected_ids_lock = asyncio.Lock()
        total_records_ref = [0]
        total_available_lock = asyncio.Lock()  # Added: For safe concurrent updates
        total_available = float('inf')
        page = self._load_checkpoint()
        # Updated: Make filename dynamic based on section
        # section = query_params.get('section', 'all') if query_params else 'all'
        base_filename = "guardian_real_time_articles"
        output_parquet_file = self.data_dir / f"{base_filename}.parquet"
        
        max_pages = (self.max_records + self.batch_size - 1) // self.batch_size
        query_params = query_params or {}
        query_params['from-date'] = self.from_date
        
        timeout = ClientTimeout(total=10)  # Use ClientTimeout
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=5)
        semaphore = asyncio.Semaphore(8)
        jsonl_files_written = []
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async def fetch_and_process(p):
                nonlocal total_available  # Declare for outer scope access
                async with semaphore:
                    params = {
                        'api-key': self.api_key,
                        'page-size': self.batch_size,
                        'show-fields': 'bodyText',
                        **query_params
                    }
                    page_data, page_total = await self._fetch_page(session, f"{self.api_url}{endpoint}", params, p)  # Changed: Unpack total
                    if not page_data:
                        return p, []

                    # Fixed: Check/set total only on starting page; use lock for safety
                    if p == page and page_total is not None:
                        async with total_available_lock:
                            total_available = min(total_available, page_total)
                            self.max_records = min(self.max_records, total_available)
                            logger.info(f"Total available articles: {total_available}")

                    # Process immediately: add timestamp, dedup, write JSONL
                    crawl_timestamp = datetime.now().isoformat()
                    valid_results = []
                    async with collected_ids_lock:
                        for r in page_data:
                            r_copy = r.copy()  # Avoid mutating original
                            r_copy['crawlTimestamp'] = crawl_timestamp
                            if r_copy.get('id') not in collected_ids:
                                valid_results.append(r_copy)
                                collected_ids.add(r_copy['id'])

                    # Write JSONL right away
                    jsonl_file = self.temp_dir / f"collected_{p}.jsonl"
                    with open(jsonl_file, 'wb') as f:
                        for vr in valid_results:
                            f.write(orjson.dumps(vr) + b'\n')
                    jsonl_files_written.append(jsonl_file)
                    
                    added_count = len(valid_results)
                    total_records_ref[0] += added_count
                    logger.info(f"Collected {total_records_ref[0]} records (page {p})")
                    
                    if total_records_ref[0] >= self.max_records:
                        logger.info("Record limit reached, cancelling remaining tasks")
                    
                    return p, valid_results
            
            # Create all tasks upfront
            pages_to_fetch = list(range(page, max_pages + 1))
            tasks = [asyncio.create_task(fetch_and_process(p)) for p in pages_to_fetch]
            
            # Fetch and process concurrently with async tqdm for real progress
            page_results = {}
            for future in tqdm.as_completed(tasks, total=len(tasks), desc="Fetching & Processing pages"):
                try:
                    current_page, _ = await future
                    page_results[current_page] = True  # Track completion
                except asyncio.CancelledError:
                    logger.info("Task cancelled due to record limit reached")
                except Exception as e:
                    logger.error(f"Error in async fetch/process: {e}", exc_info=True)
                    continue
            
            # Cancel any remaining unfinished tasks if limit reached
            if total_records_ref[0] >= self.max_records:
                for task in tasks:
                    if not task.done():
                        task.cancel()
        
        total_records = total_records_ref[0]
        if total_records < self.max_records:
            logger.warning(f"Collected only {total_records} articles, less than requested MAX_RECORDS={self.max_records}")
        
        if self.request_times:
            logger.info(f"Request times: Avg={sum(self.request_times)/len(self.request_times):.2f}s, Min={min(self.request_times):.2f}s, Max={max(self.request_times):.2f}s")
        
        # Early save checkpoint to next page
        self._save_checkpoint(max_pages + 1 if total_records >= self.max_records else page + len(pages_to_fetch))
        
        # Merge JSONL files using ijson for streaming (sorted by page)
        rows = []
        sorted_jsonl_files = sorted(jsonl_files_written, key=lambda f: int(f.stem.split('_')[-1]))
        for jsonl_file in sorted_jsonl_files:
            with open(jsonl_file, 'rb') as f:
                for article in ijson.items(f, '', multiple_values=True):
                    row = {
                        'crawlTimestamp': article.get('crawlTimestamp', ''),
                        'id': article.get('id', ''),
                        'webPublicationDate': article.get('webPublicationDate', ''),
                        'webTitle': article.get('webTitle', ''),
                        'bodyText': article.get('fields', {}).get('bodyText', ''),
                        'webUrl': article.get('webUrl', ''),
                        'sectionName': article.get('sectionName', '')
                    }
                    rows.append(row)
        
        df = pd.DataFrame(rows)
        df = df[['crawlTimestamp', 'id', 'webPublicationDate', 'webTitle', 'bodyText', 'webUrl', 'sectionName']]
        df.to_parquet(output_parquet_file, index=False, engine='pyarrow')
        logger.info(f"Converted JSON to Parquet with prioritized fields: {output_parquet_file}")

        # Optional CSV export for presentation
        export_csv = os.getenv("EXPORT_CSV", "false").lower() == "true"
        if export_csv:
            output_csv_file = self.data_dir / f"{base_filename}.csv"
            df.to_csv(output_csv_file, index=False, encoding='utf-8')
            logger.info(f"Exported CSV for presentation: {output_csv_file}")
        else:
            logger.info(f"Exported only Parquet (set EXPORT_CSV=true for CSV too): {output_parquet_file}")
        
        self._clean_checkpoint()
        return output_parquet_file

    def analyze_data(self, file_path: Path) -> None:
        logger.info(f"Starting analysis on {file_path}")
        try:
            df = pd.read_parquet(file_path)
        except PermissionError:
            logger.error(f"Cannot read {file_path} due to file lock")
            return
        
        df['webPublicationDate'] = pd.to_datetime(df['webPublicationDate'], errors='coerce')
        df['crawlTimestamp'] = pd.to_datetime(df['crawlTimestamp'], errors='coerce')
        total_articles = len(df)
        date_range = f"{df['webPublicationDate'].min().date()} to {df['webPublicationDate'].max().date()}" if total_articles > 0 else "No articles"
        crawl_range = f"{df['crawlTimestamp'].min()} to {df['crawlTimestamp'].max()}" if total_articles > 0 else "No crawls"
        
        # Vectorized computations
        df['word_count'] = df['bodyText'].astype(str).str.split().str.len()
        # Fixed: Suppress type error (textblob lacks stubs)
        df['sentiment'] = df['bodyText'].astype(str).apply(lambda x: TextBlob(str(x)).sentiment.polarity)  # type: ignore[attr-defined]
        
        word_counts = df['word_count'].tolist()
        sentiments = df['sentiment'].tolist()
        
        avg_word_count = df['word_count'].mean() if total_articles > 0 else 0
        print(f"Basic Statistics:\n- Total Articles: {total_articles}\n- Publication Date Range: {date_range}\n- Crawl Timestamp Range: {crawl_range}\n- Average Word Count: {avg_word_count:.2f}\n")
        
def main():
    parser = argparse.ArgumentParser(description="Collect and analyze Guardian API data for RAG")
    parser.add_argument('--from-date', type=str, help="Start date for articles (YYYY-MM-DD)")
    parser.add_argument('--section', type=str, default=None, help="Section to filter by (e.g., 'technology', 'world'); omit for all sections")
    parser.add_argument('--test', action='store_true', help="Test API connectivity")
    parser.add_argument('--test-cache', action='store_true', help="Test cache by simulating partial run")
    args = parser.parse_args()
    
    if args.test:
        test_api(args.section)
        return True
    
    collector = APIDataCollector("guardian_api", from_date=args.from_date)
    try:
        section_params = {'section': args.section} if args.section else {}
        if args.test_cache:
            original_max_records = collector.max_records
            collector.max_records = min(500, original_max_records)
            output_path = asyncio.run(collector.collect_data("/search", query_params=section_params))
            print(f"Cache test completed with {collector.max_records} records! Saved to {output_path}")
            collector.max_records = original_max_records
            return True
        else:
            output_path = asyncio.run(collector.collect_data("/search", query_params=section_params))
            print(f"Data collection completed successfully! Saved to {output_path}")
            collector.analyze_data(output_path)
            return True
    except Exception as e:
        logger.error(f"Data collection or analysis failed: {e}", exc_info=True)
        collector._clean_checkpoint()
        return False

def test_api(section: Optional[str] = None):
    api_url = os.getenv("API_URL", "https://content.guardianapis.com")
    api_key = os.getenv("API_KEY")
    from_date = os.getenv("FROM_DATE", (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"))
    
    if not api_key:
        print("Error: GUARDIAN_API_KEY not found in environment variables")
        return
    
    try:
        params = {
            'api-key': api_key,
            'page': 1,
            'page-size': 200,
            'show-fields': 'bodyText',
            'from-date': from_date,
        }
        if section:
            params['section'] = section
        response = requests.get(f"{api_url}/search", params=params, timeout=60)
        if response.status_code == 400:
            print(f"400 Bad Request: {response.text}")
            return
        response.raise_for_status()
        data = orjson.loads(response.content)
        total = data['response'].get('total', 0)
        results = data['response'].get('results', [])
        section_note = f" (section: {section})" if section else " (all sections)"
        print(f"API Test: Successfully fetched {len(results)} articles{section_note}, total available: {total}")
    except Exception as e:
        print(f"API Test failed: {e}")

if __name__ == "__main__":
    main()