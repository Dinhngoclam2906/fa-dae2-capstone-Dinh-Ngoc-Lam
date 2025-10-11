import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import time

# Load environment variables early
load_dotenv()

try:
    sys.path.append(str(Path(__file__).parent /"scripts"/ "data_collection"))
    from batch_data_collector import main as run_batch_collection
    from real_time_data_collector import main as run_realtime_collection
    sys.path.append(str(Path(__file__).parent /"scripts"/ "ingestion"))
    from load_csv_to_snowflake import main as ingest_batch_to_snowflake
    from load_csv_to_postgre_then_snowflake import main as run_pipeline
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Ensure batch_collector.py, realtime_collector.py, batch_ingest.py, and pg_sf_pipeline.py are in the same directory.")
    sys.exit(1)

def main():
    """Main execution: Batch -> Snowflake (batch) -> Real-time -> Pipeline (PG + SF merge)."""
    print("🚀 Capstone Pipeline: Batch Historical + Real-time Data to PG/SF")
    print("=" * 80)

    overall_start = time.perf_counter()
    print("⏱️ Full script timer started")

    # Step 1: Batch collection and preprocessing
    print("1️⃣ Running batch historical data collection...")
    if not run_batch_collection():
        print("❌ Batch collection failed—exiting.")
        sys.exit(1)
    print(f"✅ Done fetching historical data.")

    # Step 2: Ingest batch to Snowflake (clear and load)
    print("2️⃣ Clearing and ingesting batch data to Snowflake...")
    if not ingest_batch_to_snowflake():
        print("❌ Batch ingestion to Snowflake failed—exiting.")
        sys.exit(1)
    print("✅ Batch ingestion completed.")

    # Step 3: Real-time API collection
    print("3️⃣ Running real-time API data collection...")
    if not run_realtime_collection():
        print("❌ Real-time collection failed—exiting.")
        sys.exit(1)
    print(f"✅ Done fetching real-time data.")

    # Step 4: Run the end-to-end pipeline for real-time (Docker, PG, SF merge)
    print("4️⃣ Running pipeline for real-time data...")
    if not run_pipeline():
        print("❌ Pipeline failed—exiting.")
        sys.exit(1)
    
    # Full script timer end
    overall_end = time.perf_counter()
    overall_time = overall_end - overall_start
    print(f"⏱️ Full script completed in {overall_time:.1f}s")

if __name__ == "__main__":
    main()