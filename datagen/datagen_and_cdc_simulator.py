import argparse
import io
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
import boto3
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Supabase / PostgreSQL connection
PG_HOST     = os.getenv("PG_HOST")
PG_PORT     = int(os.getenv("PG_PORT", 5432))
PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Storage backends
S3_BUCKET      = os.getenv("S3_BUCKET_NAME")           # used when --storage s3
AWS_REGION     = os.getenv("AWS_REGION")
UC_VOLUME_PATH = os.getenv(                             # used when --storage volume
    "UC_VOLUME_PATH", "/Volumes/banking_lakehouse/bronze/bank-landing"
)

# Data-generation constants (unchanged from original)
TOTAL_CUSTOMERS             = 10_000
TOTAL_ACCOUNTS              = 15_000
TOTAL_INITIAL_TRANSACTIONS  = 100_000
INCREMENTAL_TRANSACTIONS    = 5_000

CUSTOMER_SEGMENTS   = ["Retail", "Affluent", "SME", "Corporate"]
KYC_STATUSES        = ["Verified", "Pending", "Action Required", "Rejected"]
ACCOUNT_TYPES       = ["Checking", "Savings", "Money Market", "Credit Card"]
ACCOUNT_STATUSES    = ["Active", "Dormant", "Frozen", "Closed"]
TRANSACTION_TYPES   = ["Deposit", "Withdrawal", "Transfer", "POS Purchase", "Online Payment", "Fee"]
CHANNELS            = ["Mobile App", "Online Banking", "Branch", "ATM", "Wire Transfer"]
MERCHANT_CATEGORIES = [
    "Groceries", "Dining", "Travel", "Utilities",
    "Retail", "Entertainment", "Healthcare", "Financial Services",
]


# ---------------------------------------------------------------------------
# CONNECTION HELPERS
# ---------------------------------------------------------------------------

def get_db_connection():
    """Return a psycopg2 connection to Supabase (PostgreSQL)."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode="require",
        connect_timeout=30,
    )


def get_storage_client(storage_backend: str):
    """
    Return the appropriate storage client.
      's3'     -> boto3 S3 client
      'volume' -> Databricks WorkspaceClient (UC Volume via Files API)
    """
    if storage_backend == "s3":
        return boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),  # None if not using STS/SSO
        )
    elif storage_backend == "volume":
        return WorkspaceClient()
    else:
        raise ValueError(f"Unsupported storage backend: {storage_backend!r}. Choose 's3' or 'volume'.")


def upload_ndjson(client, storage_backend: str, relative_path: str, ndjson_data: str) -> str:
    """
    Upload newline-delimited JSON to the chosen backend and return the
    canonical path/URI that was written.

    Args:
        client:          boto3 S3 client  OR  Databricks WorkspaceClient
        storage_backend: 's3' or 'volume'
        relative_path:   e.g. 'transactions/2026/11/22/tx_batch_20261122.json'
        ndjson_data:     the full NDJSON string to write
    """
    payload_bytes = ndjson_data.encode("utf-8")

    if storage_backend == "s3":
        client.put_object(
            Bucket=S3_BUCKET,
            Key=relative_path,
            Body=payload_bytes,
            ContentType="application/x-ndjson",
        )
        return f"s3://{S3_BUCKET}/{relative_path}"

    elif storage_backend == "volume":
        # UC Volume path: /Volumes/<catalog>/<schema>/<volume>/<relative_path>
        full_volume_path = f"{UC_VOLUME_PATH.rstrip('/')}/{relative_path.lstrip('/')}"
        client.files.upload(
            file_path=full_volume_path,
            contents=io.BytesIO(payload_bytes),
            overwrite=True,
        )
        return full_volume_path


# ---------------------------------------------------------------------------
# INITIAL SEEDING
# ---------------------------------------------------------------------------

def seed_initial_dataset(storage_backend: str):
    """
    1. Bulk-insert 10 000 customers into Supabase (public.customers).
    2. Bulk-insert 15 000 accounts  into Supabase (public.accounts).
    3. Generate 100 000 transactions split across 7 daily partitions and
       push each partition as NDJSON to the chosen storage backend.
    """
    conn   = get_db_connection()
    cursor = conn.cursor()
    storage_client = get_storage_client(storage_backend)

    # ------------------------------------------------------------------
    # 1. CUSTOMERS
    # ------------------------------------------------------------------
    print(f"[{datetime.now()}] Generating {TOTAL_CUSTOMERS} customers...")
    base_time    = datetime.now(timezone.utc) - timedelta(days=30)
    customer_ids = list(range(10001, 10001 + TOTAL_CUSTOMERS))

    customer_rows = []
    for cid in customer_ids:
        created = base_time + timedelta(days=random.randint(0, 10))
        customer_rows.append((
            cid,
            fake.first_name()[:50],
            fake.last_name()[:50],
            f"user_{cid}_{fake.domain_word()}@{fake.free_email_domain()}"[:100],
            fake.phone_number()[:25],
            fake.street_address()[:150],
            fake.city()[:60],
            fake.state_abbr()[:50],
            fake.postcode()[:20],
            "USA",
            random.choices(CUSTOMER_SEGMENTS, weights=[60, 20, 15, 5])[0],
            random.randint(300, 850),
            random.choices(KYC_STATUSES, weights=[85, 8, 5, 2])[0],
            created,
            created,
            0,  # is_deleted
        ))

    customer_sql = """
    INSERT INTO public.customers (
        customer_id, first_name, last_name, email, phone,
        address_street, city, state, postal_code, country,
        customer_segment, risk_score, kyc_status,
        created_at, last_updated, is_deleted
    ) VALUES %s
    ON CONFLICT (customer_id) DO NOTHING;
    """
    execute_values(cursor, customer_sql, customer_rows, page_size=2000)
    # Sync the serial sequence so future auto-inserts don't collide
    cursor.execute(
        "SELECT setval("
        "  pg_get_serial_sequence('public.customers', 'customer_id'),"
        "  COALESCE(MAX(customer_id), 10001)"
        ") FROM public.customers;"
    )
    conn.commit()
    print(f"[{datetime.now()}] Customers seeded successfully into Supabase.")

    # ------------------------------------------------------------------
    # 2. ACCOUNTS
    # ------------------------------------------------------------------
    print(f"[{datetime.now()}] Generating {TOTAL_ACCOUNTS} accounts...")
    account_rows = []
    account_ids  = []

    # Guarantee at least one account per customer
    for cid in customer_ids:
        acc_id = str(uuid.uuid4())
        account_ids.append(acc_id)
        opened = base_time + timedelta(days=random.randint(0, 15))
        account_rows.append((
            acc_id, cid,
            random.choices(ACCOUNT_TYPES, weights=[50, 30, 10, 10])[0],
            "USD", "Active",
            opened, opened, 0,
        ))

    # Assign remaining accounts randomly across the customer pool
    remaining = TOTAL_ACCOUNTS - len(account_ids)
    for _ in range(remaining):
        acc_id = str(uuid.uuid4())
        account_ids.append(acc_id)
        cid    = random.choice(customer_ids)
        opened = base_time + timedelta(days=random.randint(0, 20))
        account_rows.append((
            acc_id, cid,
            random.choice(ACCOUNT_TYPES),
            "USD",
            random.choices(ACCOUNT_STATUSES, weights=[85, 10, 3, 2])[0],
            opened, opened, 0,
        ))

    account_sql = """
    INSERT INTO public.accounts (
        account_id, customer_id, account_type, currency,
        status, opened_at, last_updated, is_deleted
    ) VALUES %s
    ON CONFLICT (account_id) DO NOTHING;
    """
    execute_values(cursor, account_sql, account_rows, page_size=2000)
    conn.commit()
    print(f"[{datetime.now()}] Accounts seeded successfully into Supabase.")
    cursor.close()
    conn.close()

    # ------------------------------------------------------------------
    # 3. TRANSACTIONS  →  storage backend (partitioned by day)
    # ------------------------------------------------------------------
    print(
        f"[{datetime.now()}] Generating {TOTAL_INITIAL_TRANSACTIONS} transactions "
        f"across 7 daily partitions → [{storage_backend}]..."
    )
    tx_per_day = TOTAL_INITIAL_TRANSACTIONS // 7
    start_date = datetime.now(timezone.utc).date() - timedelta(days=7)

    for day_offset in range(7):
        current_date = start_date + timedelta(days=day_offset)
        year_str  = f"{current_date.year:04d}"
        month_str = f"{current_date.month:02d}"
        day_str   = f"{current_date.day:02d}"

        records = []
        for _ in range(tx_per_day):
            tx_time = datetime.combine(
                current_date,
                (datetime.min + timedelta(seconds=random.randint(0, 86399))).time(),
            ).replace(tzinfo=timezone.utc)

            amount  = round(random.betavariate(1.5, 5.0) * 1500 + 1.0, 2)
            tx_type = random.choice(TRANSACTION_TYPES)

            records.append({
                "transaction_id":        str(uuid.uuid4()),
                "account_id":            random.choice(account_ids),
                "amount":                float(amount),
                "transaction_type":      tx_type,
                "channel":               random.choice(CHANNELS),
                "merchant_category":     random.choice(MERCHANT_CATEGORIES),
                "transaction_timestamp": tx_time.strftime("%Y-%m-%d %H:%M:%S"),
            })

        # Serialize as newline-delimited JSON (unchanged logic)
        ndjson_buffer = io.StringIO()
        for rec in records:
            ndjson_buffer.write(json.dumps(rec) + "\n")

        relative_key = (
            f"transactions/{year_str}/{month_str}/{day_str}/"
            f"tx_batch_{current_date.strftime('%Y%m%d')}.json"
        )
        destination = upload_ndjson(
            storage_client, storage_backend, relative_key, ndjson_buffer.getvalue()
        )
        print(f"  Uploaded partition [{storage_backend}]: {destination} ({tx_per_day} records)")

    print(f"[{datetime.now()}] Initial seed complete.")


# ---------------------------------------------------------------------------
# CDC SIMULATION (DAY 2 / INCREMENTAL)
# ---------------------------------------------------------------------------

def simulate_cdc_batch(storage_backend: str):
    """
    Simulate a CDC (change-data-capture) batch:
      1. SCD Type-2 trigger : update address / segment / risk for 500 customers.
      2. Soft-delete 50 customers.
      3. SCD Type-1 trigger : flip 300 active accounts to Dormant or Closed.
      4. Push 5 000 incremental transactions to the chosen storage backend.
    """
    conn   = get_db_connection()
    cursor = conn.cursor()
    storage_client = get_storage_client(storage_backend)
    now_utc = datetime.now(timezone.utc)

    print(f"[{datetime.now()}] Executing CDC batch simulation on Supabase → [{storage_backend}]...")

    # ------------------------------------------------------------------
    # 1. SCD Type-2 Trigger: update 500 customer profiles
    # ------------------------------------------------------------------
    # PostgreSQL equivalent of SQL Server's: SELECT TOP 500 ... ORDER BY NEWID()
    cursor.execute(
        "SELECT customer_id FROM public.customers "
        "WHERE is_deleted = 0 ORDER BY RANDOM() LIMIT 500;"
    )
    sampled_cust_ids = [row[0] for row in cursor.fetchall()]

    update_cust_rows = []
    for cid in sampled_cust_ids:
        update_cust_rows.append((
            fake.street_address()[:150],
            fake.city()[:60],
            fake.state_abbr()[:50],
            fake.postcode()[:20],
            random.choices(CUSTOMER_SEGMENTS, weights=[50, 25, 15, 10])[0],
            random.randint(300, 850),
            now_utc,
            cid,
        ))

    # Bulk UPDATE via execute_values (much faster than individual statements)
    execute_values(
        cursor,
        """
        UPDATE public.customers AS c SET
            address_street   = v.address_street,
            city             = v.city,
            state            = v.state,
            postal_code      = v.postal_code,
            customer_segment = v.customer_segment,
            risk_score       = v.risk_score,
            last_updated     = v.last_updated
        FROM (VALUES %s) AS v(
            address_street, city, state, postal_code,
            customer_segment, risk_score, last_updated, customer_id
        )
        WHERE c.customer_id = v.customer_id::int;
        """,
        update_cust_rows,
    )
    conn.commit()
    print(f"  [CDC - Customer Updates] Modified {len(update_cust_rows)} customer profiles (SCD2 candidates).")

    # ------------------------------------------------------------------
    # 2. Soft-Delete 50 customers
    # ------------------------------------------------------------------
    cursor.execute(
        "SELECT customer_id FROM public.customers "
        "WHERE is_deleted = 0 ORDER BY RANDOM() LIMIT 50;"
    )
    delete_cust_rows = [(now_utc, row[0]) for row in cursor.fetchall()]

    execute_values(
        cursor,
        """
        UPDATE public.customers AS c SET
            is_deleted   = 1,
            last_updated = v.last_updated
        FROM (VALUES %s) AS v(last_updated, customer_id)
        WHERE c.customer_id = v.customer_id::int;
        """,
        delete_cust_rows,
    )
    conn.commit()
    print(f"  [CDC - Customer Deletions] Soft-deleted {len(delete_cust_rows)} customers.")

    # ------------------------------------------------------------------
    # 3. SCD Type-1 Trigger: flip 300 active accounts to Dormant / Closed
    # ------------------------------------------------------------------
    cursor.execute(
        "SELECT account_id FROM public.accounts "
        "WHERE is_deleted = 0 AND status = 'Active' ORDER BY RANDOM() LIMIT 300;"
    )
    sampled_acc_ids = [row[0] for row in cursor.fetchall()]

    update_acc_rows = [
        (random.choice(["Dormant", "Closed"]), now_utc, aid)
        for aid in sampled_acc_ids
    ]

    execute_values(
        cursor,
        """
        UPDATE public.accounts AS a SET
            status       = v.status,
            last_updated = v.last_updated
        FROM (VALUES %s) AS v(status, last_updated, account_id)
        WHERE a.account_id = v.account_id::uuid;
        """,
        update_acc_rows,
    )
    conn.commit()
    print(f"  [CDC - Account Status] Updated {len(update_acc_rows)} accounts (SCD1 candidates).")

    # Fetch pool of valid active accounts for fresh transactions
    cursor.execute(
        "SELECT account_id FROM public.accounts "
        "WHERE is_deleted = 0 AND status = 'Active';"
    )
    active_accounts = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    # ------------------------------------------------------------------
    # 4. Incremental transaction file  →  storage backend
    # ------------------------------------------------------------------
    print(
        f"  [Auto Loader - {storage_backend.upper()}] "
        f"Exporting {INCREMENTAL_TRANSACTIONS} incremental transactions..."
    )
    inc_date   = now_utc.date()
    inc_records = []
    for _ in range(INCREMENTAL_TRANSACTIONS):
        tx_time = now_utc - timedelta(minutes=random.randint(1, 1440))
        inc_records.append({
            "transaction_id":        str(uuid.uuid4()),
            "account_id":            random.choice(active_accounts),
            "amount":                float(round(random.betavariate(1.2, 4.0) * 2000 + 0.5, 2)),
            "transaction_type":      random.choice(TRANSACTION_TYPES),
            "channel":               random.choice(CHANNELS),
            "merchant_category":     random.choice(MERCHANT_CATEGORIES),
            "transaction_timestamp": tx_time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    buffer = io.StringIO()
    for rec in inc_records:
        buffer.write(json.dumps(rec) + "\n")

    relative_key = (
        f"transactions/{inc_date.strftime('%Y/%m/%d')}/"
        f"tx_incremental_{now_utc.strftime('%H%M%S')}.json"
    )
    destination = upload_ndjson(
        storage_client, storage_backend, relative_key, buffer.getvalue()
    )
    print(f"  Incremental transaction partition uploaded [{storage_backend}]: {destination}")
    print(f"[{datetime.now()}] CDC batch simulation complete.")


# ---------------------------------------------------------------------------
# CLI ENTRYPOINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Banking Lakehouse Data Generator & CDC Simulator "
                    "(Supabase + S3 / Databricks UC Volume)"
    )
    parser.add_argument(
        "--mode",
        choices=["initial", "cdc"],
        required=True,
        help="'initial' to seed Supabase & push transaction history, "
             "'cdc' to generate incremental changes.",
    )
    parser.add_argument(
        "--storage",
        choices=["s3", "volume"],
        default="volume",
        help=(
            "Storage backend for transaction files. "
            "'s3' uses S3_BUCKET_NAME (requires AWS credentials). "
            "'volume' uses UC_VOLUME_PATH on the default Databricks Volume "
            "(default: volume)."
        ),
    )
    args = parser.parse_args()

    if args.mode == "initial":
        seed_initial_dataset(args.storage)
    elif args.mode == "cdc":
        simulate_cdc_batch(args.storage)
