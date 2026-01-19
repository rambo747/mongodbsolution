from datetime import datetime, timedelta
import sys
from pymongo import MongoClient
import getpass
import pandas as pd

# ────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────
COLLECTION_NAME     = "users"               # change if different
DAYS_THRESHOLD      = 365
OUTPUT_EXCEL        = "active_devices_report.xlsx"

# Fields we care about (will be flattened)
# ────────────────────────────────────────────────

class Logger:
    def __init__(self, level=1):
        self.level = level

    def info(self, msg):
        print(f"[INFO] {msg}")

    def debug(self, msg):
        if self.level > 1:
            print(f"[DEBUG] {msg}")


def ms_to_readable(ts_ms):
    """Convert millisecond Unix timestamp → readable string"""
    if not isinstance(ts_ms, (int, float)) or ts_ms <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return str(ts_ms)


def extract_flattened_devices(db, logger):
    now = datetime.now()
    one_year_ago_ms = int((now - timedelta(days=DAYS_THRESHOLD)).timestamp() * 1000)
    logger.info(f"Filtering last_access >= {one_year_ago_ms}  ({DAYS_THRESHOLD} days ago)")

    pipeline = [
        # Keep only relevant users
        {
            "$match": {
                "user_id": {"$exists": True, "$ne": None},
                "user_id": {
                    "$not": {"$regex": "^eph\\."},
                    "$not": {"$regex": "^[0-9]{18}$"}
                },
                # At least one recent access in devices or authenticators
                "$or": [
                    {"devices.last_access": {"$gte": one_year_ago_ms}},
                    {"authenticators.last_used": {"$gte": one_year_ago_ms}}
                ]
            }
        },
        # Unwind devices – one row per device
        {"$unwind": {"path": "$devices", "preserveNullAndEmptyArrays": True}},
        # Unwind authenticators – one row per authenticator
        {"$unwind": {"path": "$authenticators", "preserveNullAndEmptyArrays": True}},
        # Project the fields we want
        {
            "$project": {
                "uid": "$user_id",
                "device_id_from_devices": "$devices.device_id",
                "device_created": "$devices.created",
                "device_last_access": "$devices.last_access",
                "auth_method": "$authenticators.method",
                "auth_device_id": "$authenticators.device_id",
                "auth_last_used": "$authenticators.last_used",
                "auth_status": "$authenticators.status",
                "auth_provider_config_id": "$authenticators.provider_config_id",
                "auth_expired": "$authenticators.expired",
                "_id": 0
            }
        },
        # Optional: sort by most recent activity
        {
            "$sort": {
                "device_last_access": -1,
                "auth_last_used": -1
            }
        }
    ]

    logger.info(f"Executing aggregation on '{COLLECTION_NAME}' ...")
    cursor = db[COLLECTION_NAME].aggregate(pipeline, allowDiskUse=True)

    rows = []
    for doc in cursor:
        row = {
            "uid": doc.get("uid", ""),
            "device_id": doc.get("device_id_from_devices") or doc.get("auth_device_id") or "",
            "registered": ms_to_readable(
                doc.get("device_created") or doc.get("auth_last_used")  # fallback
            ),
            "last_access": ms_to_readable(
                doc.get("device_last_access") or doc.get("auth_last_used") or 0
            ),
            "method": doc.get("auth_method", ""),
            "status": doc.get("auth_status", ""),
            "provider_config_id": doc.get("auth_provider_config_id", ""),
            "expired": doc.get("auth_expired", False),
        }
        rows.append(row)

    logger.info(f"Extracted {len(rows)} flattened device/authenticator records")
    return rows


if __name__ == "__main__":
    logger = Logger(1)

    if len(sys.argv) not in (5, 6):
        print("Usage: python extract_active_devices.py <host> <port> <database> <username> [password]")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    db_name = sys.argv[3]
    username = sys.argv[4]
    password = sys.argv[5] if len(sys.argv) == 6 else getpass.getpass("Enter password: ")

    uri = f"mongodb://{username}:{password}@{host}:{port}/{db_name}?authSource=admin&authMechanism=SCRAM-SHA-256"
    client = MongoClient(uri, readPreference='secondary')
    db = client[db_name]

    start = datetime.now()
    logger.info(f"Started → {start}")

    data = extract_flattened_devices(db, logger)

    if data:
        df = pd.DataFrame(data)
        # Reorder columns (you can customize order)
        cols = ["uid", "device_id", "registered", "last_access", "method", "status", "provider_config_id", "expired"]
        df = df[[c for c in cols if c in df.columns]]

        df.to_excel(OUTPUT_EXCEL, index=False, engine='openpyxl')
        logger.info(f"Exported → {OUTPUT_EXCEL}  ({len(df)} rows)")
    else:
        print("No matching records found.")

    client.close()
    logger.info(f"Done in {datetime.now() - start}")