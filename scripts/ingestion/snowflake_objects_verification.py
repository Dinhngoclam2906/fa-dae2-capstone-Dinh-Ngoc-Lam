import snowflake.connector
from dotenv import load_dotenv
import os

def verify_snowflake_setup():
    """Verify all required Snowflake objects exist."""
    load_dotenv()

    # Debug prints
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH")
    print(f"Account: {account}")
    print(f"User: {user}")
    print(f"Key file path: {key_path}")
    print(f"Key file exists: {os.path.exists(key_path)}")
    if os.path.exists(key_path):
        print(f"Key file size: {os.path.getsize(key_path)} bytes")

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

    try:
        cursor = conn.cursor()

        # Check warehouse
        cursor.execute("SHOW WAREHOUSES LIKE 'WH_T26'")
        if not cursor.fetchall():
            print("❌ Warehouse WH_T26 not found")
            return False

        # Check database
        cursor.execute("SHOW DATABASES LIKE 'DB_T26'")
        if not cursor.fetchall():
            print("❌ Database DB_T26 not found")
            return False

        # Check schemas
        cursor.execute("SHOW SCHEMAS IN DATABASE DB_T26")
        schemas = [row[1] for row in cursor.fetchall()]
        required_schemas = ['RAW_DATA', 'STAGING', 'ANALYTICS']

        for schema in required_schemas:
            if schema not in schemas:
                print(f"❌ Schema {schema} not found")
                return False

        # Check stages
        cursor.execute("SHOW STAGES IN SCHEMA RAW_DATA")
        stages = [row[1] for row in cursor.fetchall()]
        required_stages = ['CSV_STAGE', 'JSON_STAGE']

        for stage in required_stages:
            if stage not in stages:
                print(f"❌ Stage {stage} not found")
                return False

        print("✅ All Snowflake objects verified!")
        return True

    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    verify_snowflake_setup()