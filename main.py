import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import time
import subprocess

# Load environment variables early
load_dotenv()

try:
    sys.path.append(str(Path(__file__).parent /"scripts"/ "data_collection"))
    from batch_data_collector import main as run_batch_collection
    from real_time_data_collector import main as run_realtime_collection
    sys.path.append(str(Path(__file__).parent /"scripts"/ "ingestion"))
    from load_parquet_to_snowflake import main as ingest_batch_to_snowflake # type: ignore
    from load_parquet_to_postgre_then_snowflake import main as run_pipeline
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Ensure batch_collector.py, realtime_collector.py, batch_ingest.py, and pg_sf_pipeline.py are in the same directory.")
    sys.exit(1)

def run_dbt_command(command: str, *extra_args: str, full_refresh: bool = False, target: str | None = None) -> bool:
    """Run a dbt command in the project directory and handle output/errors.
    
    Supports subcommands via *extra_args (e.g., 'docs', 'serve').
    """
    dbt_dir = Path(__file__).parent / "capstone_dbt_project"
    if not dbt_dir.exists():
        print(f"❌ DBT project directory '{dbt_dir}' not found.")
        return False
    
    original_dir = os.getcwd()
    try:
        os.chdir(dbt_dir)
        cmd = ["dbt", command]
        cmd.extend(extra_args)
        if full_refresh:
            cmd.extend(["--full-refresh"])
        if target:
            cmd.extend(["-t", target])
        cmd.extend([
            "--project-dir", str(dbt_dir),
            "--profiles-dir", str(dbt_dir / ".dbt")
        ])
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        print(f"✅ DBT {command} {' '.join(extra_args)} succeeded." if extra_args else f"✅ DBT {command} succeeded.")
        if result.stdout:
            print("📄 DBT Output:\n" + result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ DBT {command} {' '.join(extra_args)} failed with return code {e.returncode}" if extra_args else f"❌ DBT {command} failed with return code {e.returncode}")
        if e.stdout:
            print("📄 DBT Output:\n" + e.stdout)
        if e.stderr:
            print("❌ DBT Errors:\n" + e.stderr)
        return False
    except FileNotFoundError:
        print("❌ 'dbt' command not found. Ensure dbt is installed and in your PATH.")
        return False
    finally:
        os.chdir(original_dir)

def main():

    """Main execution: Batch -> Snowflake (batch) -> Real-time -> Pipeline (PG + SF merge) -> DBT Run & Test."""
    print("🚀 Capstone Pipeline: Batch Historical + Real-time Data to PG/SF + DBT Transformations")
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

    # Step 5: DBT Run (transform all models in Snowflake with full refresh)
    print("5️⃣ Running DBT transformations (dbt run --full-refresh)...")
    if not run_dbt_command("run", full_refresh=True):
        print("❌ DBT run failed—exiting.")
        sys.exit(1)

    # Step 5.5: DBT Debug (CI demo integration: Validate profiles/connection before tests)
    print("5️⃣.5 Running DBT debug (dbt debug -t ci)...")
    if not run_dbt_command("debug", target="ci"):
        print("⚠️ DBT debug failed—proceeding with tests (may fail if connection issue).")

    # Step 6: DBT Test (run all tests)
    print("6️⃣ Running DBT tests (dbt test)...")
    if not run_dbt_command("test"):
        print("❌ DBT test failed—exiting.")
        sys.exit(1)

    # Step 7: DBT Docs Serve (start local server for interactive docs viewing)
    print("7️⃣ Running DBT docs serve...")
    if not run_dbt_command("docs", "serve"):
        print("⚠️ DBT docs serve failed—proceeding (docs optional for pipeline).")
   
    # Full script timer end
    overall_end = time.perf_counter()
    overall_time = overall_end - overall_start
    print(f"⏱️ Full script completed in {overall_time:.1f}s")
    
if __name__ == "__main__":
    main()