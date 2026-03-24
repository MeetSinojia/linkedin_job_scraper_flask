#!/usr/bin/env python3
"""
Flask wrapper for the LinkedIn job scraper.

Endpoints:
  GET  /                     → health check
  POST /run-scraper          → triggers main scraper (urls.txt, MAX_PAGES)
  POST /run-scraper-under10  → triggers under-10 scraper (urls_under10.txt, MAX_PAGES_UNDER10)
  GET  /status               → current scraper run state
  POST /flush-db             → deletes all jobs from MongoDB
"""

import os
import threading
import traceback
from datetime import datetime, timezone

from flask import Flask, jsonify

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = Flask(__name__)

# ── Shared run-state ─────────────────────────────────────────────────────────
_state = {
    "status": "idle",       # idle | running | done | error
    "job": None,            # "main" | "under10"
    "started_at": None,
    "finished_at": None,
    "message": None,
}
_lock = threading.Lock()


def _set_state(**kwargs):
    with _lock:
        _state.update(kwargs)


def _get_state():
    with _lock:
        return dict(_state)


# ── Background runner ────────────────────────────────────────────────────────
def _run_bg(urls_file, max_pages, job_label):
    _set_state(
        status="running",
        job=job_label,
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at=None,
        message=f"{job_label} scraper started",
    )
    try:
        import main as scraper_main
        scraper_main.main(urls_file_override=urls_file, max_pages_override=max_pages)
        _set_state(
            status="done",
            finished_at=datetime.now(timezone.utc).isoformat(),
            message=f"{job_label} scraper finished successfully",
        )
    except Exception:
        _set_state(
            status="error",
            finished_at=datetime.now(timezone.utc).isoformat(),
            message=traceback.format_exc(limit=5),
        )


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
    return jsonify({"status": "ok", "service": "linkedin-job-scraper"}), 200


@app.get("/run-scraper")
def run_scraper():
    if _get_state()["status"] == "running":
        return jsonify({"message": "already running", "state": _get_state()}), 200

    urls_file = os.getenv("URLS_FILE", "config/urls.txt")
    max_pages = _env_int("MAX_PAGES", None)

    threading.Thread(target=_run_bg, args=(urls_file, max_pages, "main"), daemon=True).start()
    return jsonify({"message": "triggered"}), 200


@app.get("/run-scraper-under10")
def run_scraper_under10():
    if _get_state()["status"] == "running":
        return jsonify({"message": "already running", "state": _get_state()}), 200

    urls_file = os.getenv("URLS_FILE_UNDER10", "config/urls_under10.txt")
    max_pages = _env_int("MAX_PAGES_UNDER10", 2)

    threading.Thread(target=_run_bg, args=(urls_file, max_pages, "under10"), daemon=True).start()
    return jsonify({"message": "triggered"}), 200


@app.route("/flush-db", methods=["GET", "POST"])
def flush_db():
    mongo_uri = (os.getenv("MONGO_URI") or "").strip()
    if not mongo_uri:
        return jsonify({"error": "MONGO_URI not set"}), 400
    try:
        from config.mongodb_config import get_collection
        coll = get_collection()
        result = coll.delete_many({})
        return jsonify({
            "message": f"Deleted {result.deleted_count} document(s)",
            "collection": coll.name,
            "database": coll.database.name,
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/status")
def status():
    return jsonify(_get_state()), 200


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)