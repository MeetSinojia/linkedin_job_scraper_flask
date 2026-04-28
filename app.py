#!/usr/bin/env python3
"""
Flask wrapper for the LinkedIn job scraper.

Endpoints:
  GET  /                          → health check / UI
  GET  /run-scraper               → triggers main scraper
  GET  /run-scraper-under10       → triggers under-10 scraper
  GET  /run-scraper-high-pref     → triggers high preference scraper
  GET  /status                    → current scraper run state
  GET  /flush-db                  → deletes all jobs from main jobs collection
  GET  /send-rejection-digest     → sends today's AI-rejected high-pref jobs to Telegram
  GET  /flush-rejections          → deletes all docs from rejection collection
"""

import os
import sys
import traceback
import logging
from threading import Thread, Lock
from datetime import datetime, timezone, timedelta
import tempfile
import subprocess
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS

# Setup logging
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
CORS(app)

scraper_state = {
    "running": False,
    "last_run_at": None,
    "last_status": "idle",
    "last_error": None,
}
state_lock = Lock()

IST = timezone(timedelta(hours=5, minutes=30))


# ── Helpers ──────────────────────────────────────────────────────────────────
def _env_int(name, default=None):
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _rejection_digest_body(jobs: list, date_label: str) -> str:
    """Build the Telegram HTML message for the rejection digest."""
    import html as _html
    lines = [f"🗂 <b>AI Rejection Digest — {date_label}</b>"]
    lines.append(f"Total high-pref jobs rejected by AI today: <b>{len(jobs)}</b>\n")

    for i, job in enumerate(jobs, 1):
        title   = _html.escape((job.get("title")   or "No title").strip())
        company = _html.escape((job.get("company") or "Unknown").strip())
        location = _html.escape((job.get("location") or "").strip())
        score   = job.get("ai_score", "?")
        link    = _html.escape((job.get("apply_link") or job.get("job_url") or ""), quote=True)
        lines.append(
            f"{i}) <b>{title}</b> — {company}"
            + (f" — {location}" if location else "")
            + f"\n   🤖 AI Score: {score}"
            + (f'\n   <a href="{link}">View Job</a>' if link else "")
        )

    return "\n".join(lines)


def _send_rejection_digest_now():
    """
    Query today's (IST) AI-rejected high-pref jobs and send to Telegram.
    Called by the scheduler at 11:50 IST and also by the /send-rejection-digest endpoint.
    Returns (sent: bool, message: str)
    """
    mongo_uri = (os.getenv("MONGO_URI") or "").strip()
    if not mongo_uri:
        return False, "MONGO_URI not set"

    try:
        from config.mongodb_config import get_rejection_collection
        from config.telegram_client import send_telegram_message
    except Exception as e:
        return False, f"Import error: {e}"

    try:
        rej_coll = get_rejection_collection()

        # Today's window in UTC (IST midnight → IST 11:59pm)
        now_ist = datetime.now(IST)
        start_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        end_ist   = now_ist.replace(hour=23, minute=59, second=59, microsecond=999999)
        start_utc = start_ist.astimezone(timezone.utc)
        end_utc   = end_ist.astimezone(timezone.utc)

        jobs = list(rej_coll.find(
            {"rejected_at": {"$gte": start_utc, "$lte": end_utc}},
            {"_id": 0, "title": 1, "company": 1, "location": 1,
             "ai_score": 1, "apply_link": 1, "job_url": 1}
        ).sort("ai_score", -1))  # highest score first so near-misses are visible

        date_label = now_ist.strftime("%d %b %Y")
        logger.info(f"[Digest] Found {len(jobs)} rejected jobs for {date_label}")

        if not jobs:
            body = f"🗂 <b>AI Rejection Digest — {date_label}</b>\nNo high-pref jobs were AI-rejected today."
        else:
            body = _rejection_digest_body(jobs, date_label)

        sent = send_telegram_message(body, parse_mode="HTML")
        return sent, f"{len(jobs)} jobs in digest"

    except Exception as e:
        logger.error(f"[Digest] Error: {e}", exc_info=True)
        return False, str(e)


# ── Scheduler (APScheduler) ──────────────────────────────────────────────────
def _start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = BackgroundScheduler(timezone=str(IST))
        scheduler.add_job(
            func=_send_rejection_digest_now,
            trigger=CronTrigger(hour=23, minute=50, timezone=str(IST)),  # 11:50 PM IST
            id="rejection_digest",
            name="Daily AI Rejection Digest",
            replace_existing=True,
        )
        scheduler.start()
        logger.info("[Scheduler] Daily rejection digest scheduled at 23:50 IST")
        return scheduler
    except ImportError:
        logger.warning("[Scheduler] APScheduler not installed — digest will only run via endpoint.")
        return None
    except Exception as e:
        logger.error(f"[Scheduler] Failed to start: {e}")
        return None


_scheduler = _start_scheduler()


# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    return send_file("index.html")


@app.get("/run-scraper")
def run_scraper():
    logger.info("GET /run-scraper")
    urls_file = os.getenv("URLS_FILE", "config/urls.txt")
    max_pages = _env_int("MAX_PAGES", None)

    with state_lock:
        if scraper_state["running"]:
            return jsonify({"message": "scraper already running"}), 409
        scraper_state.update(running=True, last_run_at=datetime.utcnow().isoformat()+"Z",
                             last_status="running", last_error=None)

    def background_job():
        try:
            import main as scraper_main
            scraper_main.main(urls_file_override=urls_file, max_pages_override=max_pages)
            with state_lock: scraper_state["last_status"] = "completed"
        except Exception as e:
            logger.error(f"Scraper error: {e}", exc_info=True)
            with state_lock:
                scraper_state["last_status"] = "error"
                scraper_state["last_error"] = str(e)
        finally:
            with state_lock: scraper_state["running"] = False

    Thread(target=background_job, daemon=True).start()
    return jsonify({"message": "scraper triggered", "status": "running"}), 202


@app.get("/run-scraper-under10")
def run_scraper_under10():
    logger.info("GET /run-scraper-under10")
    urls_file = os.getenv("URLS_FILE_UNDER10", "config/urls_under10.txt")
    max_pages = _env_int("MAX_PAGES_UNDER10", 2)

    with state_lock:
        if scraper_state["running"]:
            return jsonify({"message": "scraper already running"}), 409
        scraper_state.update(running=True, last_run_at=datetime.utcnow().isoformat()+"Z",
                             last_status="running", last_error=None)

    def background_job():
        try:
            import main as scraper_main
            scraper_main.main(urls_file_override=urls_file, max_pages_override=max_pages)
            with state_lock: scraper_state["last_status"] = "completed"
        except Exception as e:
            logger.error(f"Under10 scraper error: {e}", exc_info=True)
            with state_lock:
                scraper_state["last_status"] = "error"
                scraper_state["last_error"] = str(e)
        finally:
            with state_lock: scraper_state["running"] = False

    Thread(target=background_job, daemon=True).start()
    return jsonify({"message": "under10 scraper triggered", "status": "running"}), 202


@app.get("/run-scraper-high-pref")
def run_scraper_high_pref():
    logger.info("GET /run-scraper-high-pref")
    urls_file = os.getenv("URLS_FILE", "config/urls.txt")
    max_pages = _env_int("MAX_PAGES_HIGH_PREF", None)

    with state_lock:
        if scraper_state["running"]:
            return jsonify({"message": "scraper already running"}), 409
        scraper_state.update(running=True, last_run_at=datetime.utcnow().isoformat()+"Z",
                             last_status="running", last_error=None)

    def background_job():
        try:
            import main as scraper_main
            scraper_main.main(urls_file_override=urls_file, max_pages_override=max_pages, high_pref_only=True)
            with state_lock: scraper_state["last_status"] = "completed"
        except Exception as e:
            logger.error(f"High-pref scraper error: {e}", exc_info=True)
            with state_lock:
                scraper_state["last_status"] = "error"
                scraper_state["last_error"] = str(e)
        finally:
            with state_lock: scraper_state["running"] = False

    Thread(target=background_job, daemon=True).start()
    return jsonify({"message": "high preference scraper triggered", "status": "running"}), 202


@app.get("/send-rejection-digest")
def send_rejection_digest():
    """
    Manually trigger the daily AI-rejection digest for today (IST).
    Also called automatically by the scheduler at 23:50 IST.
    """
    logger.info("GET /send-rejection-digest")
    sent, msg = _send_rejection_digest_now()
    if sent:
        return jsonify({"message": f"Digest sent. {msg}"}), 200
    else:
        return jsonify({"message": f"Digest not sent (preview only or error). {msg}"}), 200


@app.route("/flush-db", methods=["GET", "POST"])
def flush_db():
    logger.info("Flushing main jobs collection")
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
        logger.error(f"Flush-db error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/flush-rejections", methods=["GET", "POST"])
def flush_rejections():
    """Delete all documents from the AI rejection collection."""
    logger.info("Flushing rejection collection")
    mongo_uri = (os.getenv("MONGO_URI") or "").strip()
    if not mongo_uri:
        return jsonify({"error": "MONGO_URI not set"}), 400
    try:
        from config.mongodb_config import get_rejection_collection
        coll = get_rejection_collection()
        result = coll.delete_many({})
        return jsonify({
            "message": f"Deleted {result.deleted_count} rejection(s)",
            "collection": coll.name,
            "database": coll.database.name,
        }), 200
    except Exception as e:
        logger.error(f"Flush-rejections error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.get("/status")
def status():
    with state_lock:
        state = scraper_state.copy()
    scheduler_status = "running" if (_scheduler and _scheduler.running) else "not running"
    return jsonify({
        "status": "ok",
        "service": "linkedin-job-scraper",
        "scraper": state,
        "scheduler": scheduler_status,
    }), 200


@app.route("/generate-resume", methods=["POST"])
def generate_resume():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data provided"}), 400

    languages  = data.get('languages', '')
    frameworks = data.get('frameworks', '')
    dbtools    = data.get('dbtools', '')
    concepts   = data.get('concepts', '')

    def escape_latex(text):
        return (text
            .replace('&',  r'\&')
            .replace('%',  r'\%')
            .replace('$',  r'\$')
            .replace('#',  r'\#')
            .replace('_',  r'\_'))

    template_path = os.path.join(os.path.dirname(__file__), "resume_template.tex")
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            latex = f.read()
    except FileNotFoundError:
        return jsonify({"error": "resume_template.tex not found on server"}), 500

    latex = latex.replace('{{LANGUAGES}}',  escape_latex(languages))
    latex = latex.replace('{{FRAMEWORKS}}', escape_latex(frameworks))
    latex = latex.replace('{{DBTOOLS}}',    escape_latex(dbtools))
    latex = latex.replace('{{CONCEPTS}}',   escape_latex(concepts))

    with tempfile.TemporaryDirectory() as tmpdir:
        tex_file = os.path.join(tmpdir, "resume.tex")
        pdf_file = os.path.join(tmpdir, "resume.pdf")

        with open(tex_file, "w", encoding="utf-8") as f:
            f.write(latex)

        try:
            for _ in range(2):
                result = subprocess.run(
                    ["pdflatex", "-interaction=nonstopmode", "-halt-on-error",
                     "-no-shell-escape", "-output-directory", tmpdir, tex_file],
                    capture_output=True, text=True, timeout=120
                )
            if result.returncode != 0 or not os.path.exists(pdf_file):
                return jsonify({
                    "error": "LaTeX compilation failed",
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }), 500
            return send_file(pdf_file, as_attachment=True,
                             download_name="Meet_Sinojia_Resume.pdf", mimetype="application/pdf")
        except subprocess.TimeoutExpired:
            return jsonify({"error": "LaTeX compilation timed out"}), 500
        except Exception as e:
            return jsonify({"error": str(e)}), 500


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
