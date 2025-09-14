import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class APIDataCollector:
    def __init__(self, api_name: str):
        self.api_name = api_name
        self.api_url = os.getenv("API_URL", "https://content.guardianapis.com")
        self.api_key = os.getenv("API_KEY")
        self.max_records = int(os.getenv("MAX_RECORDS", 5000))
        self.batch_size = int(os.getenv("BATCH_SIZE", 100))
        self.max_retries = int(os.getenv("MAX_RETRIES", 3))
        self.data_dir = Path("data/external")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.data_dir / "checkpoint.json"
        
        # Log environment variables for debugging
        logger.info(f"API_URL: {self.api_url}")
        logger.info(f"API_KEY: {'Set' if self.api_key else 'Not set'}")
        logger.info(f"MAX_RECORDS: {self.max_records}, BATCH_SIZE: {self.batch_size}, MAX_RETRIES: {self.max_retries}")
        
        if not self.api_key:
            raise ValueError("GUARDIAN_API_KEY not found in environment variables")
        if not self.api_url.startswith("https://content.guardianapis.com"):
            raise ValueError(f"Invalid API_URL: {self.api_url}. Must be https://content.guardianapis.com")

    def _load_checkpoint(self) -> int:
        """Load the last page from checkpoint file, if it exists."""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                return checkpoint.get('last_page', 1)
        return 1

    def _save_checkpoint(self, page: int) -> None:
        """Save the current page to checkpoint file."""
        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump({'last_page': page}, f)

    def collect_data(self, endpoint: str, query_params: Optional[Dict] = None) -> List[Dict]:
        """Collect data from The Guardian API endpoint, filtering for Technology section."""
        collected_data = []
        page = self._load_checkpoint()
        total_records = 0
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = self.data_dir / f"guardian_technology_articles_{timestamp}.json"
        
        # Initialize output file with empty list
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump([], f)
        
        # Calculate total pages needed
        total_pages = (self.max_records + self.batch_size - 1) // self.batch_size
        
        for page in tqdm(range(page, total_pages + 1), initial=page-1, total=total_pages, desc="Fetching pages"):
            retries = 0
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
                    if query_params:
                        params.update(query_params)
                    
                    url = f"{self.api_url}{endpoint}"
                    logger.info(f"Fetching data from {url}, page {page}")
                    
                    response = requests.get(url, params=params, timeout=10)
                    if response.status_code == 429:  # Rate limit exceeded
                        logger.warning("Rate limit exceeded, waiting 60 seconds")
                        time.sleep(60)
                        continue
                        
                    response.raise_for_status()
                    
                    data = response.json()
                    if 'response' not in data or 'results' not in data['response']:
                        logger.error(f"Invalid API response: {data}")
                        raise ValueError("Unexpected API response format")
                    
                    results = data['response']['results']
                    
                    if not results:
                        logger.info("No more results to fetch")
                        break
                        
                    collected_data.extend(results)
                    total_records = len(collected_data)
                    
                    # Save batch incrementally
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(collected_data[:self.max_records], f, indent=2, ensure_ascii=False)
                    logger.info(f"Collected {total_records} records, saved to {output_file}")
                    
                    # Save checkpoint
                    self._save_checkpoint(page + 1)
                    
                    if total_records >= self.max_records:
                        break
                        
                    time.sleep(0.1)  # Respect rate limits (12 calls/second)
                    break
                    
                except requests.exceptions.HTTPError as e:
                    retries += 1
                    if retries == self.max_retries:
                        logger.error(f"Failed to fetch data after {self.max_retries} retries: {e}")
                        raise
                    logger.warning(f"Retry {retries}/{self.max_retries} after error: {e}")
                    time.sleep(2 ** retries)  # Exponential backoff
                
            if not results or total_records >= self.max_records:
                break
                
        return collected_data[:self.max_records]

    def save_data(self, data: List[Dict], filename: str) -> Path:
        """Save data to timestamped JSON file."""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = self.data_dir / f"{filename}_{timestamp}.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Data saved successfully to {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise

def main():
    """Main function to demonstrate API data collection."""
    collector = APIDataCollector("guardian_api")
    try:
        # Example query parameters (optional)
        query_params = {
            # 'q': 'artificial intelligence',  # Uncomment to filter for AI articles
            # 'from-date': '2024-01-01'      # Uncomment to limit to 2024+
        }
        # Collect data from the search endpoint
        data = collector.collect_data("/search", query_params=query_params)
        # Save the data (redundant since collect_data saves incrementally)
        output_path = collector.save_data(data, "guardian_technology_articles")
        print(f"Data collection completed successfully! Saved to {output_path}")
    except Exception as e:
        print(f"Data collection failed: {e}")
        raise

if __name__ == "__main__":
    main()