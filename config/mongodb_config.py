# mongodb_config.py
import os
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

def _mongo_client_kwargs():
    """Atlas / TLS-friendly client options (matches test/test_mongodb.py)."""
    timeout_ms = int(os.environ.get("MONGO_SERVER_TIMEOUT_MS", "10000"))
    try:
        import certifi
        return {
            "tlsCAFile": certifi.where(),
            "serverSelectionTimeoutMS": timeout_ms,
        }
    except ImportError:
        return {"serverSelectionTimeoutMS": timeout_ms}

def get_collection():
    """
    Returns a MongoDB collection object using env vars:
      - MONGO_URI (required)
      - MONGO_DB (default: linkedin_jobs)
      - MONGO_COLLECTION (default: jobs)
    Ensures a unique index on job_url to prevent duplicates.
    """
    uri = os.environ.get("MONGO_URI")
    if not uri or not str(uri).strip():
        raise RuntimeError("MONGO_URI environment variable not set. Set it to your MongoDB connection string.")
    dbname = os.environ.get("MONGO_DB", "linkedin_jobs")
    collname = os.environ.get("MONGO_COLLECTION", "jobs")

    kwargs = _mongo_client_kwargs()
    client = MongoClient(uri, **kwargs)
    db = client[dbname]
    coll = db[collname]

    try:
        coll.create_index("job_url", unique=True)
        coll.create_index("job_id")
    except Exception:
        pass

    return coll

def insert_job_if_new(collection, job):
    """
    Inserts job dict if job_url not already present.
    Returns (inserted: bool, inserted_id or None).
    job is expected to be a dict with keys like job_url, job_id, title, company, location, date_posted, apply_link
    """
    doc = {
        "job_url": job.get("job_url"),
        "job_id": job.get("job_id"),
        "title": job.get("title"),
        "company": job.get("company"),
        "location": job.get("location"),
        "date_posted": job.get("date_posted"),
        "apply_link": job.get("apply_link"),
        "is_reposted": bool(job.get("is_reposted")),
        "is_high_preference": bool(job.get("is_high_preference")),
        "scraped_at": datetime.utcnow()
    }
    try:
        res = collection.insert_one(doc)
        return True, res.inserted_id
    except DuplicateKeyError:
        return False, None
    except PyMongoError as e:
        print("[!] MongoDB insert error:", e)
        return False, None
