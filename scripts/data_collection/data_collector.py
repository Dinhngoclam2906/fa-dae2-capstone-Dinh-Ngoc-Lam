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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

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
        self.temp_dir = self.data_dir / "temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
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
                logger.warning("Unable to reset checkpoint file due to persistent lock, proceeding without reset")
        
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
        for _ in range(3):  # Retry up to 3 times
            try:
                if self.checkpoint_file.exists():
                    self.checkpoint_file.unlink()
                    logger.info("Checkpoint file removed")
                break
            except PermissionError:
                logger.warning("Checkpoint file is locked, retrying...")
                time.sleep(1)
        else:
            logger.warning("Unable to remove checkpoint file due to persistent lock")
        
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
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = self.data_dir / f"guardian_technology_articles_{timestamp}.json"
        
        # Initialize output file
        with open(output_file, 'w', encoding='utf-8') as f:
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
                        'show-fields': 'all',
                        'show-tags': 'all',
                        'section': 'technology'
                    }
                    params.update(query_params)
                    
                    url = f"{self.api_url}{endpoint}"
                    logger.info(f"Fetching data from {url}, page {page}")
                    
                    start_time = time.time()
                    response = requests.get(url, params=params, timeout=60)  # Increased timeout
                    request_time = time.time() - start_time
                    
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
                        
                    # Validate and filter results
                    valid_results = [
                        r for r in results
                        if r.get('sectionId') == 'technology' and r.get('id') not in collected_ids
                    ]
                    for r in valid_results:
                        collected_ids.add(r['id'])
                    
                    if valid_results:
                        # Append batch to final file using streaming
                        with open(output_file, 'r+', encoding='utf-8') as f:
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
        
        logger.info(f"Final data saved to {output_file}")
        
        # Clean up
        self._clean_checkpoint()
        return output_file

    def analyze_data(self, file_path: Path) -> None:
        """Analyze the collected data using prioritized strategies."""
        logger.info(f"Starting analysis on {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except PermissionError:
            logger.error(f"Cannot read {file_path} due to file lock")
            return
        
        # Strategy 1: Basic Statistics
        df = pd.DataFrame(data)
        df['webPublicationDate'] = pd.to_datetime(df['webPublicationDate'], errors='coerce')
        total_articles = len(df)
        date_range = f"{df['webPublicationDate'].min().date()} to {df['webPublicationDate'].max().date()}" if total_articles > 0 else "No articles"
        avg_word_count = df['fields'].apply(lambda x: len(x.get('bodyText', '').split()) if isinstance(x, dict) else 0).mean() if total_articles > 0 else 0
        unique_contributors = len(df['tags'].apply(lambda tags: [t['webTitle'] for t in tags if t['type'] == 'contributor']).explode().unique()) if total_articles > 0 else 0
        print(f"Basic Statistics:\n- Total Articles: {total_articles}\n- Date Range: {date_range}\n- Average Word Count: {avg_word_count:.2f}\n- Unique Contributors: {unique_contributors}\n")
        
        # Strategy 2: Tag and Contributor Analysis
        all_tags = [tag['webTitle'] for article in data for tag in article.get('tags', []) if tag['type'] == 'keyword']
        tag_counts = Counter(all_tags)
        top_tags = tag_counts.most_common(10)
        print("Top 10 Tags:")
        for tag, count in top_tags:
            print(f"- {tag}: {count}")
        
        all_contributors = [tag['webTitle'] for article in data for tag in article.get('tags', []) if tag['type'] == 'contributor']
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
        
        # Strategy 4: Text Content Analysis (Enhanced with Keyword Frequency)
        word_counts = df['fields'].apply(lambda x: len(x.get('bodyText', '').split()) if isinstance(x, dict) else 0)
        print(f"\nText Analysis:\n- Min Word Count: {word_counts.min() if total_articles > 0 else 0}\n- Max Word Count: {word_counts.max() if total_articles > 0 else 0}\n- Median Word Count: {word_counts.median() if total_articles > 0 else 0}\n")
        
        # Keyword frequency for key tech terms
        tech_keywords = ['artificial intelligence', 'blockchain', 'cybersecurity', 'cloud computing', 'machine learning']
        keyword_counts = Counter()
        all_text = ' '.join(article.get('fields', {}).get('bodyText', '').lower() for article in data)
        for keyword in tech_keywords:
            keyword_counts[keyword] = all_text.count(keyword.lower())
        print("Technology Keyword Frequency:")
        for keyword, count in keyword_counts.items():
            print(f"- {keyword}: {count}")
        
        # Strategy 5: Visualization
        if total_articles > 0:
            # Top Tags Bar Chart
            tags_df = pd.DataFrame(top_tags, columns=['Tag', 'Count'])
            tags_df.plot(kind='bar', x='Tag', y='Count', figsize=(10, 6), color='#1f77b4')
            plt.title('Top 10 Tags in Technology Articles')
            plt.xlabel('Tag')
            plt.ylabel('Count')
            plt.tight_layout()
            plot_path = self.analysis_dir / f"top_tags_{time.strftime('%Y%m%d_%H%M%S')}.png"
            plt.savefig(plot_path)
            plt.close()
            print(f"Saved top tags chart to {plot_path}")
            
            # Publication Trends Line Chart
            yearly_counts.plot(kind='line', marker='o', figsize=(10, 6), color='#ff7f0e')
            plt.title('Articles Published by Year')
            plt.xlabel('Year')
            plt.ylabel('Number of Articles')
            plt.tight_layout()
            trend_path = self.analysis_dir / f"publication_trends_{time.strftime('%Y%m%d_%H%M%S')}.png"
            plt.savefig(trend_path)
            plt.close()
            print(f"Saved publication trends chart to {trend_path}")
            
            # Word Cloud for Top Words
            if all_text:
                wordcloud = WordCloud(width=800, height=400, background_color='white').generate(all_text)
                plt.figure(figsize=(10, 5))
                plt.imshow(wordcloud, interpolation='bilinear')
                plt.axis('off')
                plt.title('Word Cloud of Technology Article Content')
                wordcloud_path = self.analysis_dir / f"wordcloud_{time.strftime('%Y%m%d_%H%M%S')}.png"
                plt.savefig(wordcloud_path)
                plt.close()
                print(f"Saved word cloud to {wordcloud_path}")

def main():
    """Main function to demonstrate API data collection and analysis."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Collect and analyze Guardian API data")
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
            'show-fields': 'all',
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