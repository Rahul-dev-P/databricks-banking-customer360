import argparse
import csv
import io
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker
import pymssql
import boto3
from dotenv import load_dotenv

load_dotenv()

fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration Parameters (pymssql individual fields)
SQL_SERVER   = os.getenv("SQL_SERVER",   "localhost")
SQL_PORT     = int(os.getenv("SQL_PORT", "1433"))
SQL_DATABASE = os.getenv("SQL_DATABASE", "banking-oltp")
SQL_USER     = os.getenv("SQL_USER",     "sa")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "YourStrongPassword!")
S3_BUCKET    = os.getenv("S3_BUCKET_NAME", "fintech-lakehouse-landing")
AWS_REGION   = os.getenv("AWS_REGION",     "us-east-1")

TOTAL_CUSTOMERS = 10_000
TOTAL_ACCOUNTS = 15_000
TOTAL_INITIAL_TRANSACTIONS = 100_000
INCREMENTAL_TRANSACTIONS = 5_000

CUSTOMER_SEGMENTS = ["Retail", "Affluent", "SME", "Corporate"]
KYC_STATUSES = ["Verified", "Pending", "Action Required", "Rejected"]
ACCOUNT_TYPES = ["Checking", "Savings", "Money Market", "Credit Card"]
ACCOUNT_STATUSES = ["Active", "Dormant", "Frozen", "Closed"]
TRANSACTION_TYPES = ["Deposit", "Withdrawal", "Transfer", "POS Purchase", "Online Payment", "Fee"]
CHANNELS = ["Mobile App", "Online Banking", "Branch", "ATM", "Wire Transfer"]
MERCHANT_CATEGORIES = ["Groceries", "Dining", "Travel", "Utilities", "Retail", "Entertainment", "Healthcare", "Financial Services"]


def get_db_connection():
    return pymssql.connect(
        server=SQL_SERVER,
        port=SQL_PORT,
        database=SQL_DATABASE,
        user=SQL_USER,
        password=SQL_PASSWORD,
        login_timeout=30,
        conn_properties="",      # let pymssql negotiate TLS 1.2 with Azure SQL
    )


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),  # None if not using STS/SSO
    )


# ---------------------------------------------------------------------------
# INITIAL SEEDING
# ---------------------------------------------------------------------------
def seed_initial_dataset():
    conn = get_db_connection()
    cursor = conn.cursor()
    s3_client = get_s3_client()

    print(f"[{datetime.now()}] Generating {TOTAL_CUSTOMERS} customers...")
    base_time = datetime.now(timezone.utc) - timedelta(days=30)
    customer_ids = list(range(10001, 10001 + TOTAL_CUSTOMERS))
    
    # 1. Insert Customers in batches
    customer_rows = []
    for cid in customer_ids:
        created = base_time + timedelta(days=random.randint(0, 10))
        customer_rows.append((
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
            0 # is_deleted
        ))

    customer_sql = """
    INSERT INTO dbo.customers (
        first_name, last_name, email, phone, address_street, city, state, postal_code,
        country, customer_segment, risk_score, kyc_status, created_at, last_updated, is_deleted
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    batch_size = 2000
    for i in range(0, len(customer_rows), batch_size):
        cursor.executemany(customer_sql, customer_rows[i:i + batch_size])
        conn.commit()
    print(f"[{datetime.now()}] Customers seeded successfully.")

    # 2. Insert Accounts
    print(f"[{datetime.now()}] Generating {TOTAL_ACCOUNTS} accounts...")
    account_rows = []
    account_ids = []
    
    # Guarantee at least one account per customer
    for cid in customer_ids:
        acc_id = str(uuid.uuid4())
        account_ids.append(acc_id)
        opened = base_time + timedelta(days=random.randint(0, 15))
        account_rows.append((
            acc_id,
            cid,
            random.choices(ACCOUNT_TYPES, weights=[50, 30, 10, 10])[0],
            "USD",
            "Active",
            opened,
            opened,
            0
        ))
    
    # Assign remaining accounts randomly across customer pool
    remaining = TOTAL_ACCOUNTS - len(account_ids)
    for _ in range(remaining):
        acc_id = str(uuid.uuid4())
        account_ids.append(acc_id)
        cid = random.choice(customer_ids)
        opened = base_time + timedelta(days=random.randint(0, 20))
        account_rows.append((
            acc_id,
            cid,
            random.choice(ACCOUNT_TYPES),
            "USD",
            random.choices(ACCOUNT_STATUSES, weights=[85, 10, 3, 2])[0],
            opened,
            opened,
            0
        ))

    account_sql = """
    INSERT INTO dbo.accounts (
        account_id, customer_id, account_type, currency, status, opened_at, last_updated, is_deleted
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    for i in range(0, len(account_rows), batch_size):
        cursor.executemany(account_sql, account_rows[i:i + batch_size])
        conn.commit()
    print(f"[{datetime.now()}] Accounts seeded successfully.")
    cursor.close()
    conn.close()

    # 3. Generate Transactions and Push to S3 partitioned by day (last 7 days)
    print(f"[{datetime.now()}] Generating {TOTAL_INITIAL_TRANSACTIONS} transactions across 7 daily partitions...")
    tx_per_day = TOTAL_INITIAL_TRANSACTIONS // 7
    start_date = datetime.now(timezone.utc).date() - timedelta(days=7)

    for day_offset in range(7):
        current_date = start_date + timedelta(days=day_offset)
        year_str = f"{current_date.year:04d}"
        month_str = f"{current_date.month:02d}"
        day_str = f"{current_date.day:02d}"

        records = []
        for _ in range(tx_per_day):
            tx_time = datetime.combine(
                current_date,
                (datetime.min + timedelta(seconds=random.randint(0, 86399))).time()
            ).replace(tzinfo=timezone.utc)

            amount = round(random.betavariate(1.5, 5.0) * 1500 + 1.0, 2)
            tx_type = random.choice(TRANSACTION_TYPES)

            records.append({
                "transaction_id": str(uuid.uuid4()),
                "account_id": random.choice(account_ids),
                "amount": float(amount),
                "transaction_type": tx_type,
                "channel": random.choice(CHANNELS),
                "merchant_category": random.choice(MERCHANT_CATEGORIES),
                "transaction_timestamp": tx_time.strftime("%Y-%m-%d %H:%M:%S")
            })

        # Serialize as newline-delimited JSON
        ndjson_buffer = io.StringIO()
        for rec in records:
            ndjson_buffer.write(json.dumps(rec) + "\n")

        s3_key = f"transactions/{year_str}/{month_str}/{day_str}/tx_batch_{current_date.strftime('%Y%m%d')}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=ndjson_buffer.getvalue().encode("utf-8"),
            ContentType="application/x-ndjson"
        )
        print(f"  Uploaded partition: s3://{S3_BUCKET}/{s3_key} ({tx_per_day} records)")

    print(f"[{datetime.now()}] Initial seed complete.")


# ---------------------------------------------------------------------------
# CDC SIMULATION (DAY 2 / INCREMENTAL)
# ---------------------------------------------------------------------------
def simulate_cdc_batch():
    conn = get_db_connection()
    cursor = conn.cursor()
    s3_client = get_s3_client()
    now_utc = datetime.now(timezone.utc)

    print(f"[{datetime.now()}] Executing CDC batch simulation...")

    # 1. SCD Type 2 Trigger: Update 500 customers (Address, Segment, or Risk Score changes)
    cursor.execute("SELECT TOP 500 customer_id FROM dbo.customers WHERE is_deleted = 0 ORDER BY NEWID()")
    sampled_cust_ids = [row[0] for row in cursor.fetchall()]

    update_cust_rows = []
    for cid in sampled_cust_ids:
        new_street = fake.street_address()[:150]
        new_city = fake.city()[:60]
        new_state = fake.state_abbr()[:50]
        new_postal = fake.postcode()[:20]
        new_segment = random.choice(CUSTOMER_SEGMENTS)
        new_risk = random.randint(320, 840)
        update_cust_rows.append((new_street, new_city, new_state, new_postal, new_segment, new_risk, now_utc, cid))

    cursor.executemany("""
        UPDATE dbo.customers
        SET address_street = %s, city = %s, state = %s, postal_code = %s,
            customer_segment = %s, risk_score = %s, last_updated = %s
        WHERE customer_id = %s
    """, update_cust_rows)
    conn.commit()
    print(f"  [CDC - Customer Updates] Modified {len(update_cust_rows)} customer profiles (SCD2 candidates).")

    # 2. Soft-Delete 50 customers
    cursor.execute("SELECT TOP 50 customer_id FROM dbo.customers WHERE is_deleted = 0 ORDER BY NEWID()")
    delete_cust_ids = [[now_utc, row[0]] for row in cursor.fetchall()]

    cursor.executemany("""
        UPDATE dbo.customers
        SET is_deleted = 1, last_updated = %s
        WHERE customer_id = %s
    """, delete_cust_ids)
    conn.commit()
    print(f"  [CDC - Customer Deletions] Soft-deleted {len(delete_cust_ids)} customers.")

    # 3. SCD Type 1 Trigger: Update 300 accounts to Dormant or Closed
    cursor.execute("SELECT TOP 300 account_id FROM dbo.accounts WHERE is_deleted = 0 AND status = 'Active' ORDER BY NEWID()")
    sampled_acc_ids = [row[0] for row in cursor.fetchall()]

    update_acc_rows = []
    for aid in sampled_acc_ids:
        new_status = random.choice(["Dormant", "Closed"])
        update_acc_rows.append((new_status, now_utc, aid))

    cursor.executemany("""
        UPDATE dbo.accounts
        SET status = %s, last_updated = %s
        WHERE account_id = %s
    """, update_acc_rows)
    conn.commit()
    print(f"  [CDC - Account Status] Updated {len(update_acc_rows)} accounts (SCD1 candidates).")

    # Fetch pool of valid accounts for fresh transactions
    cursor.execute("SELECT account_id FROM dbo.accounts WHERE is_deleted = 0 AND status = 'Active'")
    active_accounts = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    # 4. Ingest Day-8 incremental transaction file to S3
    print(f"  [Auto Loader - S3] Exporting {INCREMENTAL_TRANSACTIONS} incremental transactions...")
    inc_date = now_utc.date()
    inc_records = []
    for _ in range(INCREMENTAL_TRANSACTIONS):
        tx_time = now_utc - timedelta(minutes=random.randint(1, 1440))
        inc_records.append({
            "transaction_id": str(uuid.uuid4()),
            "account_id": random.choice(active_accounts),
            "amount": float(round(random.betavariate(1.2, 4.0) * 2000 + 0.5, 2)),
            "transaction_type": random.choice(TRANSACTION_TYPES),
            "channel": random.choice(CHANNELS),
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
            "transaction_timestamp": tx_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    buffer = io.StringIO()
    for rec in inc_records:
        buffer.write(json.dumps(rec) + "\n")

    s3_key = f"transactions/{inc_date.strftime('%Y/%m/%d')}/tx_incremental_{now_utc.strftime('%H%M%S')}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="application/x-ndjson"
    )
    print(f"  Incremental transaction partition uploaded: s3://{S3_BUCKET}/{s3_key}")
    print(f"[{datetime.now()}] CDC batch simulation complete.")


# ---------------------------------------------------------------------------
# CLI ENTRYPOINT
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Banking Lakehouse Data Generator & CDC Simulator")
    parser.add_argument("--mode", choices=["initial", "cdc"], required=True,
                        help="'initial' to seed database & S3 history, 'cdc' to generate incremental changes.")
    args = parser.parse_args()

    if args.mode == "initial":
        seed_initial_dataset()
    elif args.mode == "cdc":
        simulate_cdc_batch()