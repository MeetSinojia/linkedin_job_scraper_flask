# mongodb_config.py
import os
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError


def _mongo_client_kwargs():
    """Atlas / TLS-friendly client options."""
    timeout_ms = int(os.environ.get("MONGO_SERVER_TIMEOUT_MS", "10000"))
    try:
        import certifi
        return {
            "tlsCAFile": certifi.where(),
            "serverSelectionTimeoutMS": timeout_ms,
        }
    except ImportError:
        return {"serverSelectionTimeoutMS": timeout_ms}


def _make_client():
    uri = os.environ.get("MONGO_URI")
    if not uri or not str(uri).strip():
        raise RuntimeError("MONGO_URI environment variable not set.")
    return MongoClient(uri, **_mongo_client_kwargs())


def get_collection():
    """
    Returns the main jobs collection.
    Env vars: MONGO_URI, MONGO_DB (default: linkedin_jobs), MONGO_COLLECTION (default: jobs)
    """
    client = _make_client()
    dbname  = os.environ.get("MONGO_DB", "linkedin_jobs")
    collname = os.environ.get("MONGO_COLLECTION", "jobs")
    db   = client[dbname]
    coll = db[collname]
    try:
        coll.create_index("job_url", unique=True)
        coll.create_index("job_id")
    except Exception:
        pass
    return coll


def get_rejection_collection():
    """
    Returns the AI-rejection collection for high-pref jobs.
    Env vars: MONGO_URI, MONGO_DB (default: linkedin_jobs),
              MONGO_REJECTION_COLLECTION (default: ai_rejected_jobs)
    """
    client   = _make_client()
    dbname   = os.environ.get("MONGO_DB", "linkedin_jobs")
    collname = os.environ.get("MONGO_REJECTION_COLLECTION", "ai_rejected_jobs")
    db   = client[dbname]
    coll = db[collname]
    try:
        coll.create_index("job_url", unique=True)
        coll.create_index([("rejected_at", ASCENDING)])
        coll.create_index("job_id")
    except Exception:
        pass
    return coll


def insert_job_if_new(collection, job):
    """
    Inserts a job into the main jobs collection if job_url not already present.
    Returns (inserted: bool, inserted_id or None).
    """
    doc = {
        "job_url":           job.get("job_url"),
        "job_id":            job.get("job_id"),
        "title":             job.get("title"),
        "company":           job.get("company"),
        "location":          job.get("location"),
        "date_posted":       job.get("date_posted"),
        "apply_link":        job.get("apply_link"),
        "is_reposted":       bool(job.get("is_reposted")),
        "is_high_preference": bool(job.get("is_high_preference")),
        "scraped_at":        datetime.utcnow(),
    }
    try:
        res = collection.insert_one(doc)
        return True, res.inserted_id
    except DuplicateKeyError:
        return False, None
    except PyMongoError as e:
        print("[!] MongoDB insert error:", e)
        return False, None


def insert_rejection_if_new(collection, job, score: int, reason: str = ""):
    """
    Inserts an AI-rejected high-pref job into the rejection collection.
    Silently skips if the job_url was already rejected (unique index).
    Returns (inserted: bool, inserted_id or None).
    """
    doc = {
        "job_url":           job.get("job_url"),
        "job_id":            job.get("job_id"),
        "title":             job.get("title"),
        "company":           job.get("company"),
        "location":          job.get("location"),
        "date_posted":       job.get("date_posted"),
        "apply_link":        job.get("apply_link"),
        "is_high_preference": True,
        "ai_score":          score,
        "ai_reason":         reason,
        "rejected_at":       datetime.utcnow(),
    }
    try:
        res = collection.insert_one(doc)
        return True, res.inserted_id
    except DuplicateKeyError:
        return False, None
    except PyMongoError as e:
        print("[!] MongoDB rejection insert error:", e)
        return False, None
