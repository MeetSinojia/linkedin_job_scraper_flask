#!/usr/bin/env python3
"""
linkedin_get_company_portal.py

Find LinkedIn job listings from one or more search URLs and return:
 job_url, job_id, title, company, location, date_posted, apply_link

Behaviors:
 - Pagination through LinkedIn guest API (count=start/count params)
 - Skip reposted jobs
 - Click "Apply" via Selenium to reveal external apply links when needed
 - Filter relevance using the external relevance_filter module and your resume
 - Read multiple search URLs from a file (default: config/urls.txt) or single --search
"""
import argparse, time, random, re, json, os, html
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, urlencode
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# import the relevance filter module you created
import relevance_filter

try:
    from config.mongodb_config import get_collection, insert_job_if_new
except Exception as e:
    get_collection = None
    insert_job_if_new = None
    print("[!] Could not import config.mongodb_config:", e)
try:
    from config.telegram_client import format_job_message, send_telegram_message
except Exception as e:
    format_job_message = None
    send_telegram_message = None
    print("[!] Could not import config.telegram_client:", e)

# optionally load .env if python-dotenv installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------- config ----------
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36")
PAGE_SIZE = 25
OUT_TEXT = "linkedin_jobs_company_portal.txt"
OUT_CSV = "linkedin_jobs_company_portal.csv"
REQUEST_TIMEOUT = 20
# words indicating a repost (skip these jobs)
REPOST_RE = re.compile(r'\b(re-?post(?:ed|ing)?|repost)\b', re.I)
# --------------------------

def rand_sleep(a=0.6, b=1.2):
    time.sleep(random.uniform(a, b))

def _norm_company(name):
    return re.sub(r"\s+", " ", (name or "").strip().lower())

def _load_company_list(path):
    items = set()
    if not path:
        return items
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for ln in fh:
                v = ln.strip()
                if not v or v.startswith("#"):
                    continue
                items.add(_norm_company(v))
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[!] Could not read company list '{path}': {e}")
    return items

def _extract_sheet_id(url: str) -> str:
    """Extract Google Sheets spreadsheet ID from any share/edit URL."""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else None


def _fetch_sheet_as_set(sheet_id: str, gid: str, label: str) -> set:
    """Fetch a sheet tab by gid. Returns normalised company names from column A."""
    import io, csv as _csv
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    print(f"[*] Fetching '{label}' (gid={gid})...")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        companies = set()
        rows = list(_csv.reader(io.StringIO(r.text)))
        print(f"[*] '{label}' first 3 rows: {rows[:3]}")
        for row in rows:
            if not row:
                continue
            name = row[0].strip()
            if not name or name.lower() in ("company", "company_name", "name") or name.startswith("#"):
                continue
            companies.add(_norm_company(name))
        print(f"[*] '{label}': loaded {len(companies)} companies")
        return companies
    except Exception as e:
        print(f"[!] Could not fetch '{label}': {e}")
        return set()


def _load_company_list_from_gdrive(url: str):
    """
    Load high_pref and skip sets from a public Google Sheet using gid.
    gid=0 → high_pref sheet, gid=1877286130 → skip sheet (from your sheet URLs).
    Override via GDRIVE_GID_HIGH_PREF / GDRIVE_GID_SKIP env vars if needed.
    """
    sheet_id = _extract_sheet_id(url)
    if not sheet_id:
        print(f"[!] Could not extract sheet ID from URL: {url}")
        return set(), set()

    gid_high = os.getenv("GDRIVE_GID_HIGH_PREF", "0")
    gid_skip = os.getenv("GDRIVE_GID_SKIP", "1877286130")

    high_pref = _fetch_sheet_as_set(sheet_id, gid_high, "high_pref")
    skip      = _fetch_sheet_as_set(sheet_id, gid_skip, "skip")
    return high_pref, skip


def _company_matches(company_name, company_set):
    c = _norm_company(company_name)
    if not c:
        return False
    for entry in company_set:
        if entry and (entry in c or c in entry):
            return True
    return False

def make_requests_session(li_at=None):
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.linkedin.com/"
    })
    if li_at:
        s.cookies.set("li_at", li_at, domain=".linkedin.com")
        s.cookies.set("li_at", li_at, domain="www.linkedin.com")
        s.cookies.set("li_at", li_at, domain="in.linkedin.com")
    return s

def selenium_make_driver(headful=True):
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    if not headful:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    return driver

def selenium_login_and_get_li_at(email, password, headful=True):
    driver = selenium_make_driver(headful=headful)
    try:
        driver.get("https://www.linkedin.com/login")
        wait = WebDriverWait(driver, 30)
        u = wait.until(EC.presence_of_element_located((By.ID, "username")))
        u.clear(); u.send_keys(email)
        p = driver.find_element(By.ID, "password")
        p.clear(); p.send_keys(password)
        p.send_keys(Keys.RETURN)
        print("[*] If LinkedIn requests 2FA/CAPTCHA, complete it manually in the opened browser.")
        try:
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "header")))
        except Exception:
            pass
        rand_sleep(1.2, 2.0)
        li_at = None
        for c in driver.get_cookies():
            if c.get("name") == "li_at":
                li_at = c.get("value"); break
        if not li_at:
            raise RuntimeError("li_at cookie not found after login.")
        driver.quit()
        return li_at
    except Exception as e:
        driver.quit()
        raise

def build_guest_api_url(search_url, start, count=PAGE_SIZE):
    parsed = urlparse(search_url)
    q = parse_qs(parsed.query, keep_blank_values=True)
    q['start'] = [str(start)]
    q['count'] = [str(count)]
    base = f"{parsed.scheme}://{parsed.netloc}/jobs-guest/jobs/api/seeMoreJobPostings/search"
    qs = urlencode(q, doseq=True)
    return f"{base}?{qs}"

def _normalized_query_for_guest(search_url):
    parsed = urlparse(search_url)
    q = parse_qs(parsed.query, keep_blank_values=True)
    # Remove session/personalized parameters that often break deep pagination.
    for k in ("currentJobId", "origin", "referralSearchId", "refId", "trackingId", "position", "pageNum"):
        q.pop(k, None)
    # Keep only stable filters/keywords.
    allowed = {
        "keywords", "location", "geoId", "f_TPR", "f_SAL", "f_EA",
        "f_JT", "f_WT", "f_AL", "distance", "start", "count"
    }
    q = {k: v for k, v in q.items() if k in allowed}
    return q

def parse_job_links(fragment_html):
    soup = BeautifulSoup(fragment_html, "lxml")
    results = []
    for a in soup.select("a[href*='/jobs/view/']"):
        href = a.get("href")
        if not href: continue
        href_base = href.split("?")[0]
        if href_base.startswith("//"):
            href_base = "https:" + href_base
        if href_base.startswith("/"):
            href_base = "https://www.linkedin.com" + href_base
        m = re.search(r"/jobs/view/(\d+)", href_base)
        jobid = m.group(1) if m else None
        results.append({"job_url": href_base, "job_id": jobid})
    return results

def looks_like_error_page(text):
    if not text: return True
    low = text.lower()
    if "tunnel connection failed" in low or "<title>sign in" in low or "are you a robot" in low:
        return True
    return False

def extract_fields_from_html(html, job_url):
    out = {"job_url": job_url, "job_id": None, "title": None, "company": None,
           "location": None, "date_posted": None, "apply_link": None}
    m = re.search(r"/jobs/view/(\d+)", job_url)
    if m: out["job_id"] = m.group(1)
    soup = BeautifulSoup(html, "lxml")
    for sel in ["h1", "h1.topcard__title", "div.jobs-unified-top-card__content h1"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            out["title"] = el.get_text(strip=True); break
    for sel in ["a[href*='/company/']", ".jobs-unified-top-card__company-name", ".topcard__org-name-link"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            out["company"] = el.get_text(strip=True); break
    for sel in [".jobs-unified-top-card__bullet", ".topcard__flavor--bullet", ".job-criteria__text--criteria"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            out["location"] = el.get_text(strip=True); break
    for sel in ["span.jobs-unified-top-card__posted-date", "span.posted-time-ago__text", "time"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            out["date_posted"] = el.get_text(strip=True); break
    # prefer non-linkedin external anchors
    external = None
    for a in soup.select("a[href^='http']"):
        href = a.get("href")
        if href and "linkedin.com" not in href:
            external = href; break
    out["apply_link"] = external or job_url
    return out

def looks_like_reposted(text):
    """
    Detect LinkedIn 'Reposted' job markers (e.g. "· Reposted 5 hours ago").

    LinkedIn often splits "Reposted" and the time across nodes; we check the
    job top-card text as well as raw HTML and attributes.
    """
    if not text:
        return False

    low = text.lower().replace("\u00a0", " ")
    collapsed = re.sub(r"\s+", " ", low)
    try:
        soup = BeautifulSoup(text, "lxml")
    except Exception:
        soup = None

    # --- High-signal plain-text patterns (UI copy like user's Cisco/Adobe examples) ---
    # "Reposted 5 hours ago", "Re-posted · 2 days ago", "India · Reposted 5 hours ago ·"
    if re.search(
        r"(?i)\bre-?posted\s+\d+\s*(?:sec|second|seconds|min|mins?|minute|minutes|hr|hrs?|hour|hours|day|days|week|weeks|month|months)s?\s+ago\b",
        collapsed,
    ):
        return True
    if re.search(
        r"(?i)[·•\|,]\s*re-?posted\s+\d+\s*(?:sec|second|seconds|min|mins?|minute|minutes|hr|hrs?|hour|hours|day|days|week|weeks)s?\s+ago\b",
        collapsed,
    ):
        return True
    # Cross-tag HTML: "Reposted</span>...5 hours ago"
    if re.search(
        r"(?is)\bre-?posted\b.{0,160}?\d+\s*(?:sec|second|seconds|min|mins?|minute|minutes|hr|hrs?|hour|hours|day|days|week|weeks)s?.{0,40}?\bago\b",
        text,
    ):
        return True

    # Explicit strong-tag copy (matches your pasted markup)
    # e.g. <strong>Reposted 5 hours ago</strong>
    if re.search(
        r"(?is)<strong[^>]*>\s*re-?posted\s+\d+\s*(?:sec|second|seconds|min|mins?|minute|minutes|hr|hrs?|hour|hours|day|days|week|weeks|month|months)\s+ago\s*</strong>",
        text,
    ):
        return True

    # Top-card only: "Bengaluru, India · Reposted 5 hours ago · Over 100 people..."
    if soup:
        for sel in (
            "div.jobs-unified-top-card",
            "div.jobs-unified-top-card__primary-description",
            "div.jobs-details-top-card",
            "section.top-card-layout",
            "div.job-details-jobs-unified-top-card",
            "div.jobs-unified-top-card__content",
        ):
            block = soup.select_one(sel)
            if not block:
                continue
            blob = re.sub(
                r"\s+",
                " ",
                block.get_text(" ", strip=True).lower().replace("\u00a0", " "),
            )
            if not blob:
                continue
            if "repost" in blob and re.search(
                r"\d+\s*(?:sec|second|seconds|min|mins?|minute|minutes|hr|hrs?|hour|hours|day|days|week|weeks)s?\s+ago",
                blob,
            ):
                return True

    # 1) DOM-level hint: classes / ids / attributes containing 'repost'
    if soup:
        # iterate elements but bail early if we find explicit 'repost' markers
        for el in soup.find_all(True):
            # check classes
            cls = el.get("class") or []
            for c in cls:
                try:
                    if "repost" in c.replace("-", "").lower():
                        return True
                except Exception:
                    continue
            # check common attributes (id, aria-label, data-*, title, role, etc.)
            for attr_val in el.attrs.values():
                if isinstance(attr_val, str):
                    if "repost" in attr_val.lower():
                        return True
                elif isinstance(attr_val, (list, tuple)):
                    for v in attr_val:
                        if isinstance(v, str) and "repost" in v.lower():
                            return True

    # 2) Explicit phrase patterns often used by LinkedIn UI:
    #    "Reposted · 10 minutes ago", "Re-posted · 2 days ago", "Reposted by NAME"
    if re.search(r'(?i)\bre-?posted\b[\s·\|\-:,]{0,8}\s*(?:by\b|[0-9])', low):
        return True
    if re.search(r'(?i)\bre-?posted\b', low) and re.search(r'(?i)\b(?:ago|just now|yesterday|\d+\s*(?:mins?|minutes?|hrs?|hours?|days?|weeks?))\b', low):
        # reposted + time indicator somewhere in the page
        return True

    # 3) Proximity regex: 'repost' near time words within a short window (forward or backward)
    prox_re = re.compile(
        r'(?i)(?:\b(re-?post(?:ed|ing)?|repost)\b.{0,120}?\b(?:ago|just now|yesterday|\d+\s*(?:mins?|minutes?|hrs?|hours?|days?|weeks?))\b'
        r'|'
        r'\b(?:ago|just now|yesterday|\d+\s*(?:mins?|minutes?|hrs?|hours?|days?|weeks?))\b.{0,120}?\b(re-?post(?:ed|ing)?|repost)\b)',
        re.S
    )
    if prox_re.search(low):
        return True

    # 4) Check time elements' nearby text (covers UI where repost badge sits beside timestamp)
    if soup:
        time_selectors = (
            "time, span.jobs-unified-top-card__posted-date, span.posted-time-ago__text, "
            "span.jobs-unified-top-card__subtitle-primary-grouping, "
            "div.jobs-unified-top-card__primary-description"
        )
        for t in soup.select(time_selectors):
            try:
                # parent text and next/previous sibling texts
                parent_text = (t.parent.get_text(" ", strip=True) or "").lower()
                if "repost" in parent_text and re.search(
                    r"\d+\s*(?:hour|minute|day|week)s?\s+ago", parent_text
                ):
                    return True
                # Walk up a few ancestors (split text across wrappers)
                el = t
                for _ in range(5):
                    if el is None:
                        break
                    anc = el.get_text(" ", strip=True).lower()
                    if "repost" in anc and re.search(
                        r"\d+\s*(?:hour|minute|day|week)s?\s+ago", anc
                    ):
                        return True
                    el = el.parent

                nxt = t.find_next_sibling()
                if nxt and "repost" in (nxt.get_text(" ", strip=True) or "").lower():
                    return True
                prev = t.find_previous_sibling()
                if prev and "repost" in (prev.get_text(" ", strip=True) or "").lower():
                    return True
            except Exception:
                continue

    # No strong indicator found
    return False


# ----------------- Selenium apply-click helpers (unchanged) -----------------
def find_apply_element(driver):
    selectors = [
        "a.jobs-apply-button", "a.jobs-apply-button--top-card", "a[data-control-name='jobdetails_topcard_inapply']",
        "button.jobs-apply-button", "button.jobs-apply-button--top-card", "button[data-test-form-submit-btn]",
        "button.jobs-apply-button", "button[data-control-name='jobdetails_topcard_inapply']"
    ]
    for sel in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            if el and el.is_displayed():
                return el
        except Exception:
            continue
    try:
        els = driver.find_elements(By.XPATH, "//a[contains(translate(., 'APPLY', 'apply'),'apply')]|//button[contains(translate(., 'APPLY', 'apply'),'apply')]")
        for el in els:
            try:
                if el.is_displayed():
                    return el
            except Exception:
                continue
    except Exception:
        pass
    return None

def click_apply_and_get_external(driver, job_url, li_at=None, timeout=8):
    try:
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except Exception:
        pass

    apply_el = find_apply_element(driver)
    if not apply_el:
        return None

    try:
        href = apply_el.get_attribute("href")
        if href and href.startswith("http") and "linkedin.com" not in href:
            return href
    except Exception:
        pass

    before_handles = set(driver.window_handles)
    cur_url_before = driver.current_url

    try:
        driver.execute_script("arguments[0].scrollIntoView(true);", apply_el)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", apply_el)
    except Exception:
        try:
            apply_el.click()
        except Exception:
            return None

    end = time.time() + timeout
    new_url = None
    while time.time() < end:
        handles = set(driver.window_handles)
        new_handles = handles - before_handles
        if new_handles:
            new_handle = new_handles.pop()
            try:
                driver.switch_to.window(new_handle)
                time.sleep(0.5)
                new_url = driver.current_url
                driver.close()
                driver.switch_to.window(list(before_handles)[0] if before_handles else driver.window_handles[0])
                break
            except Exception:
                try:
                    driver.switch_to.window(list(before_handles)[0])
                except Exception:
                    pass
                break
        try:
            cur = driver.current_url
            if cur and cur != cur_url_before and "linkedin.com" not in cur:
                new_url = cur
                try:
                    driver.back()
                except Exception:
                    pass
                break
        except Exception:
            pass
        try:
            modal_selectors = [
                "div.jobs-easy-apply-modal", "div.jobs-apply-modal", "div[role='dialog']",
                "div.jobs-easy-apply", "div.jobs-apply-modal__content"
            ]
            for ms in modal_selectors:
                try:
                    modal = driver.find_element(By.CSS_SELECTOR, ms)
                    anchors = modal.find_elements(By.CSS_SELECTOR, "a[href^='http']")
                    for a in anchors:
                        h = a.get_attribute("href")
                        if h and "linkedin.com" not in h:
                            new_url = h
                            break
                    if new_url:
                        try:
                            close_btn = modal.find_element(By.CSS_SELECTOR, "button[aria-label='Dismiss'], button[aria-label='Close']")
                            driver.execute_script("arguments[0].click();", close_btn)
                        except Exception:
                            pass
                        break
                except Exception:
                    continue
            if new_url:
                break
        except Exception:
            pass
        time.sleep(0.4)

    if new_url and new_url.startswith("http") and "linkedin.com" not in new_url:
        return new_url

    try:
        pane_candidates = driver.find_elements(By.CSS_SELECTOR, "div.jobs-search__job-details, div.jobs-job-details__main-content, div.jobs-details__main-content")
        for pane in pane_candidates:
            anchors = pane.find_elements(By.CSS_SELECTOR, "a[href^='http']")
            for a in anchors:
                h = a.get_attribute("href")
                if h and "linkedin.com" not in h:
                    return h
    except Exception:
        pass

    return None

# ----------------- scraping flow (same signature plus keywords) -----------------
def scrape(search_url, session, li_at=None, max_pages=None, keywords=None, headful=True):
    all_jobs = []
    start = 0
    seen = set()
    page_count = 0
    page_workers = 2
    page_batch_size = 2
    guest_count = _env_int("GUEST_COUNT", PAGE_SIZE)  # how many results to ask for
    guest_step = _env_int("GUEST_START_STEP", guest_count)  # how much to increment `start`

    def _process_job_card(jd):
        job_url = jd["job_url"]
        try:
            r = session.get(job_url, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200 or looks_like_error_page(r.text):
                raise RuntimeError("Requests fetch returned error or looks like login/error page")
            out = extract_fields_from_html(r.text, job_url)
            # User-requested behavior: keep LinkedIn URL, skip external apply-link reveal.
            out["apply_link"] = out.get("apply_link") or job_url
            out["is_reposted"] = bool(looks_like_reposted(r.text))
            return out
        except Exception:
            return {
                "job_url": job_url,
                "job_id": jd.get("job_id"),
                "title": None,
                "company": None,
                "location": None,
                "date_posted": None,
                "apply_link": job_url,
                "is_reposted": False,
            }

    def _fetch_cards_for_start(start_idx):
        api_url = build_guest_api_url(search_url, start_idx, count=guest_count)
        print(f"[*] Fetching guest API start={start_idx} count={guest_count}  (search: {search_url})")
        def _request_cards(url):
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            fragment = resp.text
            if fragment and fragment.strip().startswith("{"):
                try:
                    j = resp.json()
                    html_candidate = None
                    for key in ("html", "content", "elements", "data"):
                        if key in j:
                            v = j.get(key)
                            if isinstance(v, str) and "<a " in v:
                                html_candidate = v
                                break
                            if isinstance(v, list):
                                for it in v:
                                    if isinstance(it, str) and "<a " in it:
                                        html_candidate = it
                                        break
                                    if isinstance(it, dict):
                                        for sub in it.values():
                                            if isinstance(sub, str) and "<a " in sub:
                                                html_candidate = sub
                                                break
                                    if html_candidate:
                                        break
                                if html_candidate:
                                    break
                    if html_candidate:
                        fragment = html_candidate
                except Exception:
                    pass
            cards = parse_job_links(fragment)
            return cards

        try:
            cards = _request_cards(api_url)
            print(f"    parsed {len(cards)} job links from fragment (start={start_idx})")
            # Fallback for deep pages: retry with normalized query if no cards.
            if not cards and start_idx > 0:
                q = _normalized_query_for_guest(search_url)
                q["start"] = [str(start_idx)]
                q["count"] = [str(guest_count)]
                parsed = urlparse(search_url)
                retry_url = f"{parsed.scheme}://{parsed.netloc}/jobs-guest/jobs/api/seeMoreJobPostings/search?{urlencode(q, doseq=True)}"
                retry_cards = _request_cards(retry_url)
                print(f"    retry(normalized) parsed {len(retry_cards)} job links (start={start_idx})")
                return retry_cards
            return cards
        except Exception as e:
            print(f"[!] guest API fetch error at start={start_idx}: {e}")
            return []

    while True:
        if max_pages is not None and page_count >= max_pages:
            print(f"[*] Reached max_pages ({max_pages}). Stopping pagination for this search.")
            break
        # Fetch two pages in parallel: start and start+PAGE_SIZE
        starts = []
        for i in range(page_batch_size):
            next_page_idx = page_count + i
            if max_pages is not None and next_page_idx >= max_pages:
                break
            starts.append(start + i * guest_step)
        if not starts:
            break

        cards = []
        with ThreadPoolExecutor(max_workers=page_batch_size) as pool:
            futures = [pool.submit(_fetch_cards_for_start, s) for s in starts]
            for fut in as_completed(futures):
                cards.extend(fut.result())

        if not cards:
            break

        page_cards = []
        for jd in cards:
            job_url = jd["job_url"]
            if job_url in seen:
                continue
            seen.add(job_url)
            print("    -> processing", job_url)
            page_cards.append(jd)

        if not page_cards:
            print("[*] No new jobs found on this page. Stopping pagination for this search.")
            break

        with ThreadPoolExecutor(max_workers=page_workers) as pool:
            futures = [pool.submit(_process_job_card, jd) for jd in page_cards]
            for fut in as_completed(futures):
                out = fut.result()
                if out:
                    all_jobs.append(out)

        page_count += len(starts)
        start += guest_step * len(starts)
        rand_sleep(0.8, 1.4)
    return all_jobs

def save_output(jobs):
    import csv
    with open(OUT_TEXT, "w", encoding="utf-8") as fh:
        for i, j in enumerate(jobs, 1):
            jid = j.get("job_id") if j.get("job_id") else "None"
            fh.write(f"=== JOB #{i} ===\n")
            fh.write(f"job_url: {j.get('job_url')}\n")
            fh.write(f"job_id: {jid}\n")
            fh.write(f"title: {j.get('title') or ''}\n")
            fh.write(f"company: {j.get('company') or ''}\n")
            fh.write(f"location: {j.get('location') or ''}\n")
            fh.write(f"date_posted: {j.get('date_posted') or 'None'}\n")
            fh.write(f"apply_link: {j.get('apply_link') or ''}\n\n")
    if jobs:
        keys = ["job_url","job_id","title","company","location","date_posted","apply_link"]
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=keys)
            writer.writeheader()
            for j in jobs:
                writer.writerow({k: (j.get(k) or "") for k in keys})
    print(f"[+] Saved {len(jobs)} jobs to {OUT_TEXT} and {OUT_CSV}")


def _build_aggregated_messages(new_jobs, max_items_per_message=20, max_chars=1500):
    """
    Split `new_jobs` into a list of message strings.
    Each message will contain up to max_items_per_message entries and will not exceed max_chars.
    Returns a list of message strings.
    """
    if not new_jobs:
        return []

    messages = []
    total = len(new_jobs)
    idx_global = 1
    i = 0
    # build message by message
    while i < total:
        lines = []
        # header will be completed later (we need to know part counts). Put placeholder for now.
        # We'll temporarily add a simple header; we'll replace with final header after we know total parts.
        header = ""  # placeholder
        lines.append(header)
        items_added = 0

        while i < total and items_added < max_items_per_message:
            job = new_jobs[i]
            title = (job.get("title") or "No title").strip()
            company = (job.get("company") or "Unknown").strip()
            location = (job.get("location") or "Unknown").strip()
            date_posted = (job.get("date_posted") or "").strip()
            link = job.get("apply_link") or job.get("job_url") or ""
            # Telegram legacy Markdown does not use ** for bold; use HTML for reliable formatting.
            tags = []
            if job.get("is_high_preference"):
                tags.append("<b>[HIGH PREFERENCE]</b>")
            if job.get("is_reposted"):
                tags.append("<b>[Reposted]</b>")
            tag_text = f"{' '.join(tags)} " if tags else ""
            safe_title = html.escape(title)
            safe_co = html.escape(company)
            safe_loc = html.escape(location)
            safe_link = html.escape(link, quote=True)
            posted_line = f"   📅 Posted: {html.escape(date_posted)}\n" if date_posted else ""
            entry = (
                f"{idx_global}) {tag_text}{safe_title} — {safe_co} — {safe_loc}\n"
                f"{posted_line}"
                f'   <a href="{safe_link}">Apply</a>\n'
            )

            tentative = "".join(lines) + entry
            if len(tentative) > max_chars:
                # If nothing has been added yet and single entry exceeds max_chars,
                # still add it (can't split a single job further). Otherwise break to start a new message.
                if items_added == 0:
                    lines.append(entry)
                    i += 1
                    idx_global += 1
                    items_added += 1
                break
            lines.append(entry)
            i += 1
            idx_global += 1
            items_added += 1

        # footer / hint
        lines.append("\nTo stop receiving alerts, disable the script or edit env vars.")
        messages.append("\n".join(lines))

    # Now inject proper headers with part counts
    total_parts = len(messages)
    final_messages = []
    for part_idx, body in enumerate(messages, start=1):
        # Remove any leading newlines to keep header tidy
        body = body.lstrip()
        header = f"📌 New jobs found: {total} (part {part_idx}/{total_parts})\n\n"
        final_messages.append(header + body)
    return final_messages


def push_jobs_to_db_and_telegram(jobs, send_notifications=True, max_items_per_message=20, max_message_chars=1500):
    """
    Insert jobs into MongoDB and send Telegram messages listing all newly inserted jobs.
    If the aggregated content exceeds `max_message_chars` or `max_items_per_message`, multiple messages
    will be sent (only when needed).

    NOTE: This function now pre-filters out jobs already present in the DB (by job_url)
    so they won't be sent again.
    """
    if not jobs:
        print("[*] No jobs to process.")
        return

    # MongoDB: only connect when MONGO_URI is set (dedupe + persist new jobs)
    coll = None
    mongo_uri = (os.getenv("MONGO_URI") or "").strip()
    if not mongo_uri:
        print("[*] MongoDB disabled (MONGO_URI empty). All selected jobs will be notified each run.")
    elif get_collection is None or insert_job_if_new is None:
        print("[!] MongoDB helpers not loaded. Jobs won't be saved to MongoDB.")
    else:
        try:
            coll = get_collection()
            print(f"[*] MongoDB connected: db={os.getenv('MONGO_DB', 'linkedin_jobs')} collection={os.getenv('MONGO_COLLECTION', 'jobs')}")
        except Exception as e:
            print("[!] Unable to get MongoDB collection:", e)
            coll = None

    # If we have a DB, pre-query to find which job_urls already exist (avoid sending them)
    existing_urls = set()
    if coll is not None:
        try:
            job_urls = [j.get("job_url") for j in jobs if j.get("job_url")]
            if job_urls:
                # find existing docs with those job_urls
                cursor = coll.find({"job_url": {"$in": job_urls}}, {"job_url": 1})
                for d in cursor:
                    u = d.get("job_url")
                    if u:
                        existing_urls.add(u)
        except Exception as e:
            print("[!] Error querying DB for existing jobs:", e)
            existing_urls = set()

    new_jobs = []
    total_attempted = 0
    total_inserted = 0

    for job in jobs:
        total_attempted += 1
        job_url = job.get("job_url") or ""
        if not job_url:
            continue

        # Pre-filter: if job_url already in DB, skip entirely (don't send)
        if job_url in existing_urls:
            print(f"[-] Job already exists in DB (pre-filtered, skipping): {job_url}")
            continue

        inserted = False
        if coll is not None and insert_job_if_new is not None:
            try:
                inserted, inserted_id = insert_job_if_new(coll, job)
                if inserted:
                    total_inserted += 1
                    new_jobs.append(job)
                    existing_urls.add(job_url)
                    print(f"[+] Inserted job into DB: {job_url} (id={inserted_id})")
                else:
                    print(f"[-] Job already exists in DB (insert_job_if_new): {job_url}")
            except Exception as e:
                print("[!] Error inserting job into DB:", e)
                # do NOT treat as new if insert error occurred
                inserted = False
        else:
            new_jobs.append(job)
            inserted = True

    print(f"[*] Processed {total_attempted} jobs. Newly inserted (to be notified): {len(new_jobs)}")

    total_sent = 0
    if send_notifications and new_jobs:
        try:
            # build one-or-more aggregated messages
            messages = _build_aggregated_messages(new_jobs, max_items_per_message=max_items_per_message, max_chars=max_message_chars)
            if not messages:
                print("[*] No message bodies were built (nothing to send).")
            else:
                print(f"[*] Built {len(messages)} message(s) to send (each <= {max_message_chars} chars).")
                # send messages sequentially
                for part_idx, body in enumerate(messages, start=1):
                    sent = False
                    try:
                        if send_telegram_message is not None:
                            sent = send_telegram_message(body, parse_mode="HTML")
                        else:
                            print("[!] config.telegram_client.send_telegram_message not available. Message preview:")
                            print(body)
                            sent = False
                    except Exception as e:
                        print("[!] Telegram send error for part", part_idx, ":", e)
                        sent = False

                    if sent:
                        total_sent += 1
                        print(f"[+] Sent message part {part_idx}/{len(messages)}")
                    else:
                        print(f"[-] Message part {part_idx} not sent (previewed or failed).")
        except Exception as e:
            print("[!] Error while preparing/sending aggregated Telegram messages:", e)

    print(f"[*] Done. Inserted to DB: {len(new_jobs)}. Telegram messages sent: {total_sent}.")

def _env_bool(name, default=False):
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def _env_int(name, default=None):
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

def main(urls_file_override=None, max_pages_override=None):
    urls_file = urls_file_override or os.getenv("URLS_FILE", "config/urls.txt")
    headless = _env_bool("CHROMEDRIVER_HEADLESS", True)
    li_at = os.getenv("LINKEDIN_LI_AT") or None
    email = os.getenv("LINKEDIN_EMAIL") or None
    password = os.getenv("LINKEDIN_PASSWORD") or None
    max_pages = max_pages_override if max_pages_override is not None else _env_int("MAX_PAGES", None)
    resume_path = os.getenv("RESUME_PATH", "paarth_jain_resume.pdf")
    no_notify = _env_bool("NO_NOTIFY", False)
    high_pref_file = os.getenv("HIGH_PREFERENCE_COMPANIES_FILE", "config/high_preference_companies.txt")
    skip_comp_file = os.getenv("SKIP_COMPANIES_FILE", "config/skip_companies.txt")
    gdrive_companies_url = os.getenv("GDRIVE_COMPANIES_URL", "").strip()

    headful = not headless

    if not li_at and email and password:
        print("[*] Logging in to obtain li_at (Selenium). Open browser if required.")
        try:
            li_at = selenium_login_and_get_li_at(email, password, headful=headful)
            print("[*] Obtained li_at via Selenium.")
        except Exception as e:
            print("[!] Failed to login and get li_at:", e)

    session = make_requests_session(li_at)

    # Build keywords from resume (either provided or default upload path)
    raw_text = ""
    if resume_path and os.path.exists(resume_path):
        try:
            try:
                import PyPDF2
                with open(resume_path, "rb") as fh:
                    reader = PyPDF2.PdfReader(fh)
                    pages = []
                    for pg in reader.pages:
                        try:
                            pages.append(pg.extract_text() or "")
                        except Exception:
                            continue
                    raw_text = "\n".join(pages)
            except Exception:
                try:
                    with open(resume_path, "rb") as fh:
                        raw_text = fh.read().decode("utf-8", errors="ignore")
                except Exception:
                    raw_text = ""
        except Exception:
            raw_text = ""
    else:
        print(f"[*] Resume not found at {resume_path}; proceeding with default keyword seeds.")
    keywords = relevance_filter.build_keywords_from_resume_text(raw_text)
    print(f"[*] Built {len(keywords)} keywords from resume (sample): {', '.join(list(keywords)[:12])}")
    high_pref_companies = _load_company_list(high_pref_file)
    skip_companies = _load_company_list(skip_comp_file)

    # GDrive overrides txt files if set
    if gdrive_companies_url:
        gdrive_high, gdrive_skip = _load_company_list_from_gdrive(gdrive_companies_url)
        high_pref_companies = gdrive_high or high_pref_companies
        skip_companies = gdrive_skip or skip_companies

    print(f"[*] Loaded company lists: high_pref={len(high_pref_companies)}, skip={len(skip_companies)}")

    # Read URLs from file (or fallback to single LINKEDIN_SEARCH)
    single_search = os.getenv("LINKEDIN_SEARCH") or None
    urls = []
    if urls_file and os.path.exists(urls_file):
        with open(urls_file, "r", encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if not ln or ln.startswith("#"):
                    continue
                urls.append(ln)
    elif single_search:
        urls = [single_search]
    else:
        print(f"[!] URLs file '{urls_file}' not found and no LINKEDIN_SEARCH provided. Exiting.")
        return

    print(f"[*] Found {len(urls)} search URL(s) to process.")
    combined_jobs = []
    for i, u in enumerate(urls, 1):
        print(f"\n=== Processing ({i}/{len(urls)}): {u}")
        try:
            jobs = scrape(u, session, li_at=li_at, max_pages=max_pages, keywords=None, headful=headful)
            print(f"[*] Collected {len(jobs)} jobs from this search.")
            combined_jobs.extend(jobs)
            rand_sleep(0.8, 1.6)
        except Exception as e:
            print("[!] Error while scraping this search URL:", e)
            continue

    # dedupe by job_url (preserve first occurrence)
    unique = {}
    ordered = []
    for j in combined_jobs:
        ju = j.get("job_url")
        if not ju:
            continue
        if ju not in unique:
            unique[ju] = j
            ordered.append(j)
    print(f"[*] Total unique jobs after dedupe: {len(ordered)}")

    # Decide what to send only after full scraping completes.
    selected = []
    for job in ordered:
        company_name = job.get("company") or ""
        in_high = _company_matches(company_name, high_pref_companies)
        in_skip = _company_matches(company_name, skip_companies)

        # High pref: always include, no relevance check, no repost check, no extra fetch
        if in_high:
            job["is_high_preference"] = True
            selected.append(job)
            continue

        # Skip companies (not high pref)
        if in_skip:
            continue

        # Re-fetch job page for relevance decision if needed.
        html = ""
        try:
            rr = session.get(job.get("job_url"), timeout=REQUEST_TIMEOUT)
            if rr.status_code == 200 and not looks_like_error_page(rr.text):
                html = rr.text
        except Exception:
            html = ""

        try:
            if relevance_filter.is_relevant_job(html, job.get("title") or "", keywords):
                job["is_high_preference"] = False
                job["is_reposted"] = bool(looks_like_reposted(html)) or bool(
                    job.get("is_reposted")
                )
                selected.append(job)
        except Exception:
            # On parser errors, keep default behavior conservative and skip.
            continue

    print(f"[*] Jobs selected after post-scrape filtering: {len(selected)}")

    save_output(selected)

    # Insert new jobs into MongoDB (dedupe by job_url) + Telegram only for newly inserted
    send_notifications = not no_notify
    push_jobs_to_db_and_telegram(selected, send_notifications)

    print("[*] Done. Total jobs processed:", len(selected))


if __name__ == "__main__":
    main()