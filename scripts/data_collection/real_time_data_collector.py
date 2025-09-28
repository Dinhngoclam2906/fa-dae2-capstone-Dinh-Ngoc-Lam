import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
from collections import Counter
import matplotlib.pyplot as plt
import ijson
import argparse
from datetime import datetime, timedelta
from wordcloud import WordCloud
from textblob import TextBlob

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Try to import nltk for stopwords, with fallback to custom list
try:
    from nltk.corpus import stopwords
    NLTK_STOPWORDS = set(stopwords.words('english'))
    logger.info("Using NLTK stopwords as base for word cloud")
except (ImportError, LookupError) as e:
    logger.warning(f"NLTK stopwords not available ({e}), using custom stopwords list")
    NLTK_STOPWORDS = set()

# Extended custom stopwords list including "said" and other non-meaningful terms
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

# Combine stopwords
STOPWORDS = NLTK_STOPWORDS | CUSTOM_STOPWORDS

class APIDataCollector:
    def __init__(self, api_name: str, from_date: Optional[str] = None):
        self.api_name = api_name
        self.api_url = os.getenv("API_URL", "https://content.guardianapis.com")
        self.api_key = os.getenv("API_KEY")
        self.max_records = int(os.getenv("MAX_RECORDS", 1000))
        self.batch_size = int(os.getenv("BATCH_SIZE", 100))
        self.max_retries = int(os.getenv("MAX_RETRIES", 3))
        # Set default from_date to one year ago if not provided
        default_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        self.from_date = os.getenv("FROM_DATE", default_date) if from_date is None else from_date
        try:
            datetime.strptime(self.from_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid FROM_DATE format: {self.from_date}. Must be YYYY-MM-DD")
        
        self.data_dir = Path("data/external")
        self.analysis_dir = Path("data/analysis")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.analysis_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.data_dir / "checkpoint.json"
        self.temp_checkpoint = self.data_dir / "temp_checkpoint.json"
        self.temp_dir = self.data_dir / "temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.request_times = []
        
        # Reset checkpoint if from_date doesn't match
        if self.checkpoint_file.exists():
            for _ in range(3):  # Retry up to 3 times
                try:
                    with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                        checkpoint = json.load(f)
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
        
        # Log environment variables for debugging
        logger.info(f"API_URL: {self.api_url}")
        logger.info(f"API_KEY: {'Set' if self.api_key else 'Not set'}")
        logger.info(f"FROM_DATE: {self.from_date}")
        logger.info(f"MAX_RECORDS: {self.max_records}, BATCH_SIZE: {self.batch_size}, MAX_RETRIES: {self.max_retries}")

        if not self.api_key:
            raise ValueError("GUARDIAN_API_KEY not found in environment variables")
        if not self.api_url.startswith("https://content.guardianapis.com"):
            raise ValueError(f"Invalid API_URL: {self.api_url}. Must be https://content.guardianapis.com")

    def _load_checkpoint(self) -> int:
        """Load the last page from checkpoint file, if it exists."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                return checkpoint.get('last_page', 1)
            except PermissionError:
                logger.warning("Checkpoint file is locked, starting from page 1")
                return 1
        return 1

    def _save_checkpoint(self, page: int) -> None:
        """Save the current page and from_date to checkpoint file."""
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({'last_page': page, 'from_date': self.from_date}, f)
        except PermissionError:
            logger.warning("Unable to write checkpoint file due to lock, continuing without saving checkpoint")

    def _clean_checkpoint(self) -> None:
        """Remove checkpoint file and temp directory after completion or error."""
        for checkpoint in [self.checkpoint_file, self.temp_checkpoint]:
            for _ in range(3):  # Retry up to 3 times
                try:
                    if checkpoint.exists():
                        checkpoint.unlink()
                        logger.info(f"Checkpoint file {checkpoint} removed")
                    break
                except PermissionError:
                    logger.warning(f"Checkpoint file {checkpoint} is locked, retrying...")
                    time.sleep(1)
            else:
                logger.warning(f"Unable to remove checkpoint file {checkpoint} due to persistent lock")
        
        for temp_file in self.temp_dir.glob("*.json"):
            try:
                temp_file.unlink()
            except PermissionError:
                logger.warning(f"Unable to remove temp file {temp_file} due to lock")
        if self.temp_dir.exists():
            try:
                self.temp_dir.rmdir()
                logger.info("Temporary directory removed")
            except OSError:
                logger.warning("Unable to remove temp directory due to remaining files or lock")

    def collect_data(self, endpoint: str, query_params: Optional[Dict] = None) -> Path:
        """Collect data from The Guardian API endpoint, filtering for Technology section."""
        collected_ids = set()
        page = self._load_checkpoint()
        total_records = 0
        total_available = float('inf')
        base_filename = "guardian_technology_articles"
        output_json_file = self.data_dir / f"{base_filename}.json"
        output_csv_file = self.data_dir / f"{base_filename}.csv"
        # counter = 1
        # while output_json_file.exists():  # Prevent overwriting
        #     output_json_file = self.data_dir / f"{base_filename}_{counter}.json"
        #     output_csv_file = self.data_dir / f"{base_filename}_{counter}.csv"
        #     counter += 1
        
        # Initialize output JSON file
        with open(output_json_file, 'w', encoding='utf-8') as f:
            json.dump([], f)
        
        # Calculate total pages needed
        max_pages = (self.max_records + self.batch_size - 1) // self.batch_size
        
        # Ensure query_params includes from_date
        query_params = query_params or {}
        query_params['from-date'] = self.from_date
        
        for page in tqdm(range(page, max_pages + 1), initial=page-1, total=max_pages, desc="Fetching pages"):
            if total_records >= self.max_records or page > (total_available + self.batch_size - 1) // self.batch_size:
                break
            retries = 0
            results = []  # Initialize results to avoid UnboundLocalError
            while retries < self.max_retries:
                try:
                    # Construct URL with required and custom parameters
                    params = {
                        'api-key': self.api_key,
                        'page': page,
                        'page-size': self.batch_size,
                        'show-fields': 'bodyText',  # Prioritize bodyText for RAG
                        'show-tags': 'all',
                        'section': 'technology'
                    }
                    params.update(query_params)
                    
                    url = f"{self.api_url}{endpoint}"
                    logger.info(f"Fetching data from {url}, page {page}")
                    
                    start_time = time.time()
                    timeout = max(30, sum(self.request_times) / max(1, len(self.request_times)) * 2) if self.request_times else 60
                    response = requests.get(url, params=params, timeout=timeout)
                    request_time = time.time() - start_time
                    self.request_times.append(request_time)
                    
                    if response.status_code == 429:  # Rate limit exceeded
                        logger.warning("Rate limit exceeded, waiting 60 seconds")
                        time.sleep(60)
                        continue
                        
                    if response.status_code == 400:  # Bad Request, possibly no more results
                        logger.warning(f"400 Bad Request on page {page}: {response.text}")
                        results = []
                        break
                        
                    response.raise_for_status()
                    
                    data = response.json()
                    if 'response' not in data or 'results' not in data['response']:
                        logger.error(f"Invalid API response: {data}")
                        raise ValueError("Unexpected API response format")
                    
                    # Update total available results and max_records
                    total_available = min(total_available, data['response'].get('total', float('inf')))
                    self.max_records = min(self.max_records, total_available)
                    max_pages = (self.max_records + self.batch_size - 1) // self.batch_size
                    
                    results = data['response']['results']
                    
                    if not results:
                        logger.info("No more results to fetch")
                        break
                        
                    # Add crawlTimestamp to each result
                    crawl_timestamp = datetime.now().isoformat()
                    for result in results:
                        result['crawlTimestamp'] = crawl_timestamp
                    
                    # Validate and filter results
                    valid_results = [
                        r for r in results
                        if r.get('sectionId') == 'technology' and r.get('id') not in collected_ids
                    ]
                    for r in valid_results:
                        collected_ids.add(r['id'])
                    
                    if valid_results:
                        # Append batch to JSON file
                        with open(output_json_file, 'r+', encoding='utf-8') as f:
                            existing_data = json.load(f)
                            existing_data.extend(valid_results)
                            f.seek(0)
                            f.truncate()
                            json.dump(existing_data[:self.max_records], f, indent=2)
                    
                    total_records += len(valid_results)
                    logger.info(f"Collected {total_records} records (page {page})")
                    
                    if total_records >= self.max_records:
                        break
                        
                    # Dynamic delay to stay under 12 requests/second
                    min_delay = max(0, 0.083 - request_time)  # 12 req/s = 0.083s/req
                    time.sleep(min_delay)
                    break
                    
                except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
                    retries += 1
                    if retries == self.max_retries:
                        logger.error(f"Failed to fetch data after {self.max_retries} retries: {e}")
                        raise
                    logger.warning(f"Retry {retries}/{self.max_retries} after error: {e}")
                    time.sleep(2 ** retries)  # Exponential backoff
                
            if not results or total_records >= self.max_records:
                break
        
            # Save checkpoint
            self._save_checkpoint(page + 1)
        
        if total_records < self.max_records:
            logger.warning(f"Collected only {total_records} articles, less than requested MAX_RECORDS={self.max_records} due to date filter or API limits")
        
        # Convert JSON to CSV with prioritized fields for PostgreSQL/Snowflake
        with open(output_json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Flatten data for key dimensions
        rows = []
        for article in data:
            row = {
                'crawlTimestamp': article.get('crawlTimestamp', ''),  # First column
                'id': article.get('id', ''),
                'webPublicationDate': article.get('webPublicationDate', ''),
                'webTitle': article.get('webTitle', ''),
                'bodyText': article.get('fields', {}).get('bodyText', ''),
                'tags': json.dumps([{
                    'id': tag.get('id', ''),
                    'type': tag.get('type', ''),
                    'webTitle': tag.get('webTitle', '')
                } for tag in article.get('tags', [])]),  # Store as JSON string
                'webUrl': article.get('webUrl', ''),
                'sectionName': article.get('sectionName', '')
            }
            rows.append(row)
        
        # Create DataFrame and save to CSV
        df = pd.DataFrame(rows)
        # Reorder columns to ensure crawlTimestamp is first
        df = df[['crawlTimestamp', 'id', 'webPublicationDate', 'webTitle', 'bodyText', 'tags', 'webUrl', 'sectionName']]
        df.to_csv(output_csv_file, index=False, encoding='utf-8')
        logger.info(f"Converted JSON to CSV with prioritized fields: {output_csv_file}")
        
        # Clean up
        self._clean_checkpoint()
        return output_csv_file

    def analyze_data(self, file_path: Path) -> None:
        """Analyze the collected data, focusing on prioritized fields for RAG."""
        logger.info(f"Starting analysis on {file_path}")
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except PermissionError:
            logger.error(f"Cannot read {file_path} due to file lock")
            return
        
        # Strategy 1: Basic Statistics
        df['webPublicationDate'] = pd.to_datetime(df['webPublicationDate'], errors='coerce')
        df['crawlTimestamp'] = pd.to_datetime(df['crawlTimestamp'], errors='coerce')
        total_articles = len(df)
        date_range = f"{df['webPublicationDate'].min().date()} to {df['webPublicationDate'].max().date()}" if total_articles > 0 else "No articles"
        crawl_range = f"{df['crawlTimestamp'].min()} to {df['crawlTimestamp'].max()}" if total_articles > 0 else "No crawls"
        avg_word_count = df['bodyText'].apply(lambda x: len(str(x).split())).mean() if total_articles > 0 else 0
        unique_contributors = len(df['tags'].apply(lambda x: [t['webTitle'] for t in json.loads(x) if t['type'] == 'contributor']).explode().unique()) if total_articles > 0 else 0
        print(f"Basic Statistics:\n- Total Articles: {total_articles}\n- Publication Date Range: {date_range}\n- Crawl Timestamp Range: {crawl_range}\n- Average Word Count: {avg_word_count:.2f}\n- Unique Contributors: {unique_contributors}\n")
        
        # Strategy 2: Tag and Contributor Analysis
        all_tags = []
        all_contributors = []
        for tags_json in df['tags']:
            tags = json.loads(tags_json)
            all_tags.extend([tag['webTitle'] for tag in tags if tag['type'] == 'keyword'])
            all_contributors.extend([tag['webTitle'] for tag in tags if tag['type'] == 'contributor'])
        
        tag_counts = Counter(all_tags)
        top_tags = tag_counts.most_common(10)
        print("Top 10 Tags:")
        for tag, count in top_tags:
            print(f"- {tag}: {count}")
        
        contributor_counts = Counter(all_contributors)
        top_contributors = contributor_counts.most_common(10)
        print("\nTop 10 Contributors:")
        for contrib, count in top_contributors:
            print(f"- {contrib}: {count}")
        
        # Strategy 3: Publication Trends Over Time
        if total_articles > 0:
            df['year'] = df['webPublicationDate'].dt.year
            yearly_counts = df.groupby('year').size()
            print("\nPublication Trends by Year:")
            print(yearly_counts)
        
        # Strategy 4: Text Content Analysis
        if total_articles > 0:
            word_counts = df['bodyText'].apply(lambda x: len(str(x).split()))
            print(f"\nText Analysis:\n- Min Word Count: {word_counts.min()}\n- Max Word Count: {word_counts.max()}\n- Median Word Count: {word_counts.median()}\n")
        
            # Keyword frequency for key tech terms
            tech_keywords = ['artificial intelligence', 'blockchain', 'cybersecurity', 'cloud computing', 'machine learning']
            keyword_counts = Counter()
            all_text = ' '.join(df['bodyText'].astype(str).str.lower())
            for keyword in tech_keywords:
                keyword_counts[keyword] = all_text.count(keyword.lower())
            print("Technology Keyword Frequency:")
            for keyword, count in keyword_counts.items():
                print(f"- {keyword}: {count}")
        
            # Sentiment Analysis
            sentiments = df['bodyText'].apply(lambda x: TextBlob(str(x)).sentiment.polarity)
            avg_sentiment = sentiments.mean()
            print(f"\nSentiment Analysis:\n- Average Sentiment Polarity: {avg_sentiment:.2f} (Positive > 0, Negative < 0)")

def main():
    """Main function to demonstrate API data collection and analysis."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Collect and analyze Guardian API data for RAG")
    parser.add_argument('--from-date', type=str, help="Start date for articles (YYYY-MM-DD)")
    parser.add_argument('--test', action='store_true', help="Test API connectivity")
    args = parser.parse_args()
    
    if args.test:
        test_api()
        return
    
    collector = APIDataCollector("guardian_api", from_date=args.from_date)
    try:
        # Example query parameters (optional)
        query_params = {
            # 'q': 'artificial intelligence',  # Uncomment to filter for AI articles
        }
        # Collect data from the search endpoint
        output_path = collector.collect_data("/search", query_params=query_params)
        print(f"Data collection completed successfully! Saved to {output_path}")
        
        # Analyze the collected data
        collector.analyze_data(output_path)
    except Exception as e:
        print(f"Data collection or analysis failed: {e}")
        collector._clean_checkpoint()
        raise

def test_api():
    """Test the API with a single request to verify connectivity and response."""
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
            'page-size': 10,
            'show-fields': 'bodyText',
            'show-tags': 'all',
            'section': 'technology',
            'from-date': from_date
        }
        response = requests.get(f"{api_url}/search", params=params, timeout=60)
        if response.status_code == 400:
            print(f"400 Bad Request: {response.text}")
            return
        response.raise_for_status()
        data = response.json()
        total = data['response'].get('total', 0)
        results = data['response'].get('results', [])
        print(f"API Test: Successfully fetched {len(results)} articles, total available: {total}")
    except Exception as e:
        print(f"API Test failed: {e}")

if __name__ == "__main__":
    main()