import sys
import getpass
from datetime import datetime
from pymongo import MongoClient

class Logger:
    def __init__(self, level=0):
        self.level = level

    def info(self, message):
        print(f"[INFO] {message}")

    def debug(self, message):
        if self.level >= 10:
            print(f"[DEBUG] {message}")


def count_users_with_valid_devices(db, logger):
    pipeline = [
        # Must have non-empty devices array
        {
            "$match": {
                "devices": {
                    "$exists": True,
                    "$type": "array",
                    "$ne": []
                }
            }
        },
        # At least one device must have all three required fields
        {
            "$match": {
                "devices": {
                    "$elemMatch": {
                        "device_id":    {"$exists": True},
                        "created":      {"$exists": True},
                        "last_access":  {"$exists": True}
                    }
                }
            }
        },
        # Count distinct users
        {
            "$group": {
                "_id": None,
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "countOfUsersWithValidDevice": "$count"
            }
        }
    ]

    result = list(db.users.aggregate(pipeline))

    count = result[0]["countOfUsersWithValidDevice"] if result else 0

    logger.info(
        f"Number of users with ≥1 valid device (device_id + created + last_access present): {count}"
    )
    return count


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python count_valid_device_users.py <host> <port> <database> <username>")
        sys.exit(1)

    host         = sys.argv[1]
    port         = int(sys.argv[2])
    database_name = sys.argv[3]
    username     = sys.argv[4]

    password = getpass.getpass("Enter MongoDB password: ")

    print("Connecting to MongoDB...")
    client = MongoClient(
        f"mongodb://{username}:{password}@{host}:{port}/{database_name}?authSource=admin",
        authMechanism="SCRAM-SHA-256",
        readPreference="secondary"
    )

    db = client[database_name]

    start_time = datetime.now()
    print(f"Started: {start_time}")

    logger = Logger(loglevel=0)  # set to 10 if you want debug messages

    count_users_with_valid_devices(db, logger)

    print(f"Completed in: {datetime.now() - start_time}")