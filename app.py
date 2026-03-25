#!/usr/bin/env python3
"""
Flask wrapper for the LinkedIn job scraper.

Endpoints:
  GET  /                     → health check
  GET  /run-scraper          → triggers main scraper (urls.txt, MAX_PAGES)
  GET  /run-scraper-under10  → triggers under-10 scraper (urls_under10.txt, MAX_PAGES_UNDER10)
  GET  /run-scraper-high-pref → triggers high preference scraper (urls.txt, MAX_PAGES_HIGH_PREF)
  GET  /status               → current scraper run state
  GET  /flush-db             → deletes all jobs from MongoDB
"""

import os
import sys
import traceback
import logging
from threading import Thread, Lock
from datetime import datetime

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

scraper_state = {
    "running": False,
    "last_run_at": None,
    "last_status": "idle",
    "last_error": None,
}
state_lock = Lock()


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
    logger.info("GET /run-scraper - trigger scraper")
    urls_file = os.getenv("URLS_FILE", "config/urls.txt")
    max_pages = _env_int("MAX_PAGES", None)

    with state_lock:
        if scraper_state["running"]:
            return jsonify({"message": "scraper already running"}), 409
        scraper_state["running"] = True
        scraper_state["last_run_at"] = datetime.utcnow().isoformat() + "Z"
        scraper_state["last_status"] = "running"
        scraper_state["last_error"] = None

    def background_job():
        try:
            import main as scraper_main
            logger.info(f"Running scraper in background with urls_file={urls_file}, max_pages={max_pages}")
            scraper_main.main(urls_file_override=urls_file, max_pages_override=max_pages)
            logger.info("Background scraper completed successfully")
            with state_lock:
                scraper_state["last_status"] = "completed"
        except Exception as e:
            logger.error(f"Background scraper error: {e}", exc_info=True)
            with state_lock:
                scraper_state["last_status"] = "error"
                scraper_state["last_error"] = str(e)
        finally:
            with state_lock:
                scraper_state["running"] = False

    Thread(target=background_job, daemon=True).start()
    return jsonify({"message": "scraper triggered", "status": "running"}), 202


@app.get("/run-scraper-under10")
def run_scraper_under10():
    logger.info("GET /run-scraper-under10 - trigger under10 scraper")
    urls_file = os.getenv("URLS_FILE_UNDER10", "config/urls_under10.txt")
    max_pages = _env_int("MAX_PAGES_UNDER10", 2)

    with state_lock:
        if scraper_state["running"]:
            return jsonify({"message": "scraper already running"}), 409
        scraper_state["running"] = True
        scraper_state["last_run_at"] = datetime.utcnow().isoformat() + "Z"
        scraper_state["last_status"] = "running"
        scraper_state["last_error"] = None

    def background_job():
        try:
            import main as scraper_main
            logger.info(f"Running under10 scraper in background with urls_file={urls_file}, max_pages={max_pages}")
            scraper_main.main(urls_file_override=urls_file, max_pages_override=max_pages)
            logger.info("Background under10 scraper completed successfully")
            with state_lock:
                scraper_state["last_status"] = "completed"
        except Exception as e:
            logger.error(f"Background under10 scraper error: {e}", exc_info=True)
            with state_lock:
                scraper_state["last_status"] = "error"
                scraper_state["last_error"] = str(e)
        finally:
            with state_lock:
                scraper_state["running"] = False

    Thread(target=background_job, daemon=True).start()
    return jsonify({"message": "under10 scraper triggered", "status": "running"}), 202


@app.get("/run-scraper-high-pref")
def run_scraper_high_pref():
    logger.info("GET /run-scraper-high-pref - trigger high preference scraper")
    urls_file = os.getenv("URLS_FILE", "config/urls.txt")
    max_pages = _env_int("MAX_PAGES_HIGH_PREF", None)

    with state_lock:
        if scraper_state["running"]:
            return jsonify({"message": "scraper already running"}), 409
        scraper_state["running"] = True
        scraper_state["last_run_at"] = datetime.utcnow().isoformat() + "Z"
        scraper_state["last_status"] = "running"
        scraper_state["last_error"] = None

    def background_job():
        try:
            import main as scraper_main
            logger.info(f"Running high preference scraper in background with urls_file={urls_file}, max_pages={max_pages}")
            scraper_main.main(urls_file_override=urls_file, max_pages_override=max_pages, high_pref_only=True)
            logger.info("Background high preference scraper completed successfully")
            with state_lock:
                scraper_state["last_status"] = "completed"
        except Exception as e:
            logger.error(f"Background high preference scraper error: {e}", exc_info=True)
            with state_lock:
                scraper_state["last_status"] = "error"
                scraper_state["last_error"] = str(e)
        finally:
            with state_lock:
                scraper_state["running"] = False

    Thread(target=background_job, daemon=True).start()
    return jsonify({"message": "high preference scraper triggered", "status": "running"}), 202


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
    with state_lock:
        state = scraper_state.copy()
    return jsonify({
        "status": "ok",
        "service": "linkedin-job-scraper",
        "scraper": state,
    }), 200


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)