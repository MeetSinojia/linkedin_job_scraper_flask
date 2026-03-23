#!/usr/bin/env python3
"""
Flask wrapper for the LinkedIn job scraper.

Endpoints:
  GET  /              → health check
  POST /run-scraper   → triggers scraper in background thread
  POST /flush-db      → deletes all jobs from MongoDB
  GET  /status        → returns current scraper run status
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

# ── Shared run-state (in-memory, single worker process) ─────────────────────
_state = {
    "status": "idle",          # idle | running | done | error
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


# ── Background scraper runner ────────────────────────────────────────────────
def _run_scraper_bg():
    _set_state(status="running", started_at=datetime.now(timezone.utc).isoformat(),
               finished_at=None, message="Scraper started")
    try:
        import main as scraper_main
        scraper_main.main()
        _set_state(status="done",
                   finished_at=datetime.now(timezone.utc).isoformat(),
                   message="Scraper finished successfully")
    except Exception as e:
        _set_state(status="error",
                   finished_at=datetime.now(timezone.utc).isoformat(),
                   message=f"Scraper error: {traceback.format_exc(limit=5)}")


# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
def health():
    return jsonify({"status": "ok", "service": "linkedin-job-scraper"}), 200


@app.post("/run-scraper")
def run_scraper():
    state = _get_state()
    if state["status"] == "running":
        return jsonify({"error": "Scraper already running", "state": state}), 409

    t = threading.Thread(target=_run_scraper_bg, daemon=True)
    t.start()
    return jsonify({"message": "Scraper triggered", "state": _get_state()}), 202


@app.post("/flush-db")
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