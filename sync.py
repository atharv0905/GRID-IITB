import psycopg2
import json
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from psycopg2.extras import RealDictCursor

DB_CONFIG = dict(
    host="10.135.5.11",
    database="redb",
    user="reuser",
    password="repass",
    port="5432"
)

API_URL = "http://10.135.5.11:8001/data"

TABLE_CONFIG = {
    "regions": "created_at",
    "users": "user_id",
    "plants": "created_at",
    "forecast_runs": "created_at",
    "mi_predictions": "id",
    "ingested_files": "ingested_at"
}

def send_to_server(payload):
    try:
        headers = {
            "Content-Type": "application/json"
        }

        # 🔴 Convert Postgres types → JSON safe first
        safe_payload = json.loads(json.dumps(payload, default=str))

        # 🔴 Wrap inside "data"
        request_body = {
            "data": safe_payload
        }

        response = requests.post(
            API_URL,
            headers=headers,
            json=request_body,   # back to json=
            timeout=30
        )

        print("POST Status:", response.status_code)
#        print("Response:", response.text)

        if response.status_code in [200, 201]:
            print("✓ Data successfully pushed to server")
        else:
            print("✗ Server returned error")

    except Exception as e:
        print("API POST failed:", e)


def fetch_recent_records():
    conn = None
    try:
        print(f"\nJob started at {datetime.now()}")

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        final_data = {}

        for table, order_col in TABLE_CONFIG.items():
            query = f"""
                SELECT *
                FROM {table}
                ORDER BY {order_col} DESC
                LIMIT 5;
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            final_data[table] = rows

        # Save locally (optional backup)
        with open("recent_records.json", "w") as f:
            json.dump(final_data, f, indent=4, default=str)

        print("JSON created successfully")

        # 🔴 Send to server
        send_to_server(final_data)

    except Exception as e:
        print("Database error:", e)

    finally:
        if conn:
            conn.close()


# Scheduler
scheduler = BlockingScheduler()
fetch_recent_records()  # run once immediately
scheduler.add_job(fetch_recent_records, 'interval', minutes=5)

print("Scheduler running → pushing data every 5 minutes")
scheduler.start()
