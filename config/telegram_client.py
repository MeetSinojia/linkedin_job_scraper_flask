# telegram_client.py
import os
import requests
import json
from typing import Optional

def format_job_message(job: dict) -> str:
    title = job.get("title") or "No title"
    company = job.get("company") or "Unknown company"
    location = job.get("location") or "Unknown location"
    date_posted = job.get("date_posted") or "Unknown date"
    apply_link = job.get("apply_link") or job.get("job_url") or ""
    job_id = job.get("job_id") or ""
    lines = [
        f"*{title}*",
        f"{company} — {location}",
        f"Posted: {date_posted}",
    ]
    if job_id:
        lines.append(f"Job ID: {job_id}")
    if apply_link:
        lines.append(f"Apply: {apply_link}")
    return "\n".join(lines)


def send_telegram_message(body: str, to: Optional[str] = None, parse_mode: str = "Markdown") -> bool:
    """
    Sends a Telegram message to a chat id using the Bot API.

    Environment variables used:
      - TELEGRAM_BOT_TOKEN
      - TELEGRAM_CHAT_ID  (preferred); OR you can pass 'to' param (numeric id or @username)
    """
    
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = to or os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("[!] Telegram env vars (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID) not set. Message preview below:")
        print("---- MESSAGE PREVIEW ----")
        print(body)
        print("-------------------------")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": body,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }

    try:
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
        data = r.json()
        mid = data.get("result", {}).get("message_id")
        print(f"[+] Telegram message sent, message_id={mid}")
        return True
    except Exception as e:
        print("[!] Telegram send failed:", e)
        return False
