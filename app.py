#!/usr/bin/env python3
"""
Flask wrapper for the LinkedIn job scraper.

Endpoints:
  GET  /                     → health check
  GET  /run-scraper          → triggers main scraper (urls.txt, MAX_PAGES)
  GET  /run-scraper-under10  → triggers under-10 scraper (urls_under10.txt, MAX_PAGES_UNDER10)
  GET  /status               → current scraper run state
  GET  /flush-db             → deletes all jobs from MongoDB
"""

import os
import sys
import traceback
import logging

from flask import Flask, jsonify

# Setup logging to show in Render logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = Flask(__name__)
logger.info("Flask app initialized")


# ── Helpers ──────────────────────────────────────────────────────────────────
def _env_int(name, default=None):
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
def health():
    logger.info("GET / - health check")
    return jsonify({"status": "ok", "service": "linkedin-job-scraper"}), 200


@app.get("/run-scraper")
def run_scraper():
    logger.info("GET /run-scraper - starting scraper")
    urls_file = os.getenv("URLS_FILE", "config/urls.txt")
    max_pages = _env_int("MAX_PAGES", None)

    try:
        import main as scraper_main
        logger.info(f"Running scraper with urls_file={urls_file}, max_pages={max_pages}")
        scraper_main.main(urls_file_override=urls_file, max_pages_override=max_pages)
        logger.info("Scraper completed successfully")
        return jsonify({"message": "scraper completed successfully"}), 200
    except Exception as e:
        logger.error(f"Scraper error: {e}", exc_info=True)
        return jsonify({"error": str(e), "traceback": traceback.format_exc(limit=5)}), 500


@app.get("/run-scraper-under10")
def run_scraper_under10():
    logger.info("GET /run-scraper-under10 - starting under10 scraper")
    urls_file = os.getenv("URLS_FILE_UNDER10", "config/urls_under10.txt")
    max_pages = _env_int("MAX_PAGES_UNDER10", 2)

    try:
        import main as scraper_main
        logger.info(f"Running under10 scraper with urls_file={urls_file}, max_pages={max_pages}")
        scraper_main.main(urls_file_override=urls_file, max_pages_override=max_pages)
        logger.info("Under10 scraper completed successfully")
        return jsonify({"message": "under10 scraper completed successfully"}), 200
    except Exception as e:
        logger.error(f"Under10 scraper error: {e}", exc_info=True)
        return jsonify({"error": str(e), "traceback": traceback.format_exc(limit=5)}), 500


@app.route("/flush-db", methods=["GET", "POST"])
def flush_db():
    logger.info("Flushing database")
    mongo_uri = (os.getenv("MONGO_URI") or "").strip()
    if not mongo_uri:
        logger.warning("MONGO_URI not set")
        return jsonify({"error": "MONGO_URI not set"}), 400
    try:
        from config.mongodb_config import get_collection
        coll = get_collection()
        result = coll.delete_many({})
        logger.info(f"Deleted {result.deleted_count} documents")
        return jsonify({
            "message": f"Deleted {result.deleted_count} document(s)",
            "collection": coll.name,
            "database": coll.database.name,
        }), 200
    except Exception as e:
        logger.error(f"Flush-db error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.get("/status")
def status():
    logger.info("GET /status - health check")
    return jsonify({"status": "ok", "service": "linkedin-job-scraper"}), 200


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)