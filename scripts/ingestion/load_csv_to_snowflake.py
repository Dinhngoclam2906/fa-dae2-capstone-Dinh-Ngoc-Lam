import snowflake.connector
from dotenv import load_dotenv
import os
import time

def get_snowflake_connection():
    """Get Snowflake connection (shared helper)."""
    load_dotenv()
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
        private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        timezone="Asia/Ho_Chi_Minh"
    )

def setup_schema_and_stage(cursor, database, schema):
    """Shared setup for schema, stage, and context (avoids duplication)."""
    # Confirm session context
    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
    db, sch, role = cursor.fetchone()
    print(f"Current context: Database={db}, Schema={sch}, Role={role}")

    # Create schema if not exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
    print(f"✅ Schema {database}.{schema} created or already exists")

    # Set schema explicitly
    cursor.execute(f"USE SCHEMA {database}.{schema}")

    # Create stage if not exists
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {database}.{schema}.CSV_STAGE")
    print(f"✅ Stage {database}.{schema}.CSV_STAGE created or already exists")

def clear_snowflake_table():
    """Clear all historical data in Snowflake's raw_data table before batch load (TRUNCATE for speed)."""
    conn = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        table_name = f"{database}.{schema}.raw_data"
        
        # TRUNCATE (faster, keeps structure)
        cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
        print(f"✅ Truncated {table_name} (historical data cleared before batch load)")
        
        # Alternative: DROP + CREATE (uncomment if preferred for full reset)
        # cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        # create_table_command = f"""
        # CREATE TABLE {table_name} (
        #     id INTEGER PRIMARY KEY,
        #     crawl_timestamp TIMESTAMP,
        #     article_id VARCHAR(2000),
        #     web_publication_date TIMESTAMP,
        #     web_title STRING,
        #     body_text STRING,
        #     web_url VARCHAR(500),
        #     section_name VARCHAR(255),
        #     loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        # )
        # """
        # cursor.execute(create_table_command)
        # print(f"✅ Dropped and recreated {table_name} (historical data cleared)")
        
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Failed to clear Snowflake table: {e}")
        return False
    finally:
        if conn:
            conn.close()

def ingest_csv_to_snowflake(csv_file_path):
    """Full ingestion: Upload CSV to stage, then load to table (single connection)."""
    conn = get_snowflake_connection()
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    stage_name = f"@{database}.{schema}.CSV_STAGE"
    table_name = f"{database}.{schema}.raw_data"

    try:
        cursor = conn.cursor()
        setup_schema_and_stage(cursor, database, schema)

        # Clear old files
        remove_command = f"REMOVE {stage_name} PATTERN='guardian_historical_preprocessed.*'"
        cursor.execute(remove_command)
        print("✅ Cleared old 'guardian_historical_preprocessed.*' files from stage")

        # Upload with compression and timer
        file_size_bytes = os.path.getsize(csv_file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        estimated_speed_mbps = 30  # Tune to your avg upload speed (MB/s)
        estimated_time_mins = file_size_mb / estimated_speed_mbps
        print(f"🚀 Starting upload of {file_size_mb:.1f}MB file... Estimated time: {estimated_time_mins:.1f} mins (at {estimated_speed_mbps}MB/s)")

        start_time = time.time()
        put_command = f"PUT file://{csv_file_path} {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cursor.execute(put_command)
        upload_time = time.time() - start_time
        upload_speed_mbps = file_size_mb / upload_time if upload_time > 0 else 0
        print(f"✅ Upload completed in {upload_time:.1f}s ({upload_speed_mbps:.1f} MB/s)")

        # Get upload results
        result = cursor.fetchall()
        for row in result:
            print(f"✅ File uploaded: {row[0]} source, {row[1]} target")

        if not result:
            raise ValueError("No files uploaded.")

        # Extract relative path
        relative_path = result[0][1]
        print(f"File location in stage (from PUT): {relative_path}")

        # Verify stage contents
        cursor.execute(f"LIST {stage_name}")
        stage_contents = cursor.fetchall()
        print("Stage contents:", stage_contents)

        # Drop/recreate table
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✅ Dropped existing table {table_name} if it existed")

        # Create table
        create_table_command = f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY,
            crawl_timestamp TIMESTAMP,
            article_id VARCHAR(2000),
            web_publication_date TIMESTAMP,
            web_title STRING,
            body_text STRING,
            web_url VARCHAR(500),
            section_name VARCHAR(255),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(create_table_command)
        print(f"✅ Table {table_name} created (aligned to PostgreSQL schema)")

        # Validate COPY INTO
        validate_command = f"""
        COPY INTO {table_name}
        FROM {stage_name}/{relative_path}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            NULL_IF = ('NULL', 'null', '')
            EMPTY_FIELD_AS_NULL = TRUE
        )
        VALIDATION_MODE = 'RETURN_ERRORS'
        """
        cursor.execute(validate_command)
        validation_results = cursor.fetchall()
        if validation_results:
            print("❌ Validation errors:", validation_results)
            return False
        else:
            print("✅ Validation passed, no errors")

        # Load data
        load_start_time = time.time()
        copy_command = f"""
        COPY INTO {table_name} (
            id, crawl_timestamp, article_id, web_publication_date, web_title,
            body_text, web_url, section_name
        )
        FROM {stage_name}/{relative_path}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            NULL_IF = ('NULL', 'null', '')
            EMPTY_FIELD_AS_NULL = TRUE
        )
        ON_ERROR = 'CONTINUE'
        """
        cursor.execute(copy_command)
        load_time = time.time() - load_start_time
        print(f"🚀 Data load completed in {load_time:.1f}s")

        # Get loading results
        result = cursor.fetchall()
        if result:
            for row in result:
                print(f"✅ Data loaded: {row[0]} rows to {table_name}")
        else:
            print("⚠️ No results from COPY INTO (check file path)")

        # Verify loaded data
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"✅ Table contains {row_count} rows")

        # Display sample
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY id LIMIT 1")
        rows = cursor.fetchall()
        print("Sample table contents:", rows)

        return True

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"❌ Snowflake Programming Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
    finally:
        conn.close()

def main():
    """Main execution logic."""
    # NEW: Clear historical before batch load
    print("🔄 Clearing historical data in Snowflake...")
    if not clear_snowflake_table():
        print("❌ Clear failed—exiting.")
        return
    
    csv_file_path = r"D:\FoundryAIAcademy\fa-c002-hub\fa-dae2-capstone-Dinh-Ngoc-Lam\data\batch\guardian_historical_preprocessed.csv"
    success = ingest_csv_to_snowflake(csv_file_path)
    if not success:
        print("❌ Ingestion failed—check logs above.")

if __name__ == "__main__":
    main()