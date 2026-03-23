#!/usr/bin/env python3
# config/flush_db.py
"""
Flush (delete) all documents in the configured MongoDB collection.

Behaviour:
 - If FLUSH_DRY_RUN env var is true (1/true/yes/on), this script will only count documents.
 - Otherwise it will delete all documents in the collection.
 - Intended to be run once/day (or manually). Use with caution.
"""

import os
from pathlib import Path
import sys

try:
    from dotenv import load_dotenv
    _root = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=_root / ".env")
except Exception:
    pass

try:
    from config.mongodb_config import get_collection
except Exception:
    try:
        from mongodb_config import get_collection
    except Exception as e:
        print("[!] Could not import mongodb_config.get_collection:", e)
        raise SystemExit(2)


def str_to_bool(v: str) -> bool:
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def main(dry_run: bool = False):
    uri = os.getenv("MONGO_URI")
    if not uri:
        print("[!] MONGO_URI not set; aborting flush.")
        return 2

    try:
        coll = get_collection()
    except Exception as e:
        print("[!] Unable to get MongoDB collection:", e)
        return 2

    try:
        if dry_run:
            count = coll.count_documents({})
            print(f"[DRY RUN] Documents that would be deleted: {count} (collection: {coll.name})")
            return 0
        else:
            res = coll.delete_many({})
            print(f"[+] Deleted {res.deleted_count} document(s) from collection '{coll.name}' in database '{coll.database.name}'.")
            return 0
    except Exception as e:
        print("[!] Error during delete_many:", e)
        return 3


if __name__ == "__main__":
    dr = str_to_bool(os.getenv("FLUSH_DRY_RUN", "false"))
    rc = main(dry_run=dr)
    sys.exit(rc)
