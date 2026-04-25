# relevance_filter.py
"""
Utilities to determine whether a LinkedIn job posting is relevant to the user's resume
(and to extract keywords from the resume).
"""

import re
from typing import Optional, Set
from bs4 import BeautifulSoup

# default keyword seeds (also strengthened from the uploaded resume)
DEFAULT_TECH_KEYWORDS = {
    "java","javascript","typescript","c++","spring","springboot","react","reactjs",
    "nextjs","django","express","kafka","azure","aws","cosmosdb","sql","postgresql",
    "mongodb","redis","docker","kubernetes","git","html","css","node","web3","graphql",
    "datastructure", "algorithm", "problem solving", "problem-solving", "problem-solving skills"
}

ROLE_KEYWORDS = [
    # Core SDE roles
    r"\bsoftware engineer\b", r"\bsoftware developer\b",
    r"\bsde\b", r"\bsde-?\d\b",
    r"\bsde ?1\b", r"\bsde ?2\b", r"\bsde ?3\b",

    # General roles
    r"\bdeveloper\b", r"\bengineer\b", r"\bsoftware\b",
    r"\bassociate developer\b", r"\bassociate software engineer\b",

    # Specific titles
    r"\bapplications engineer\b", r"\bapplication developer\b",
    r"\bsoftware engineer i\b", r"\bsoftware engineer ii\b",
    r"\bsenior software engineer\b",

    # DevOps + Backend infra roles
    r"\bdevops\b", r"\bdevops engineer\b",
    r"\bsite reliability engineer\b", r"\bsre\b",
    r"\bplatform engineer\b",
    r"\bcloud engineer\b",

    # Optional (keep if you want)
    r"\banalyst\b",
]

ROLE_RE = re.compile("|".join(ROLE_KEYWORDS), re.I)

EXCLUDE_PATTERNS = [
    # -------------------------
    # Data / ML roles
    # -------------------------
    r"\bdata engineer\b",
    r"\bdata scientist\b",
    r"\bmachine learning\b",
    r"\bml engineer\b",
    r"\bdata analyst\b",
    r"\bdata engineering\b",

    # -------------------------
    # QA / Testing roles (specific only)
    # -------------------------
    r"\bqa\b",
    r"\bquality assurance\b",
    r"\btest engineer\b",
    r"\bautomation engineer\b",
    r"\bsdet\b",
    r"\bsdet1\b",

    # -------------------------
    # Non-dev roles
    # -------------------------
    r"\bproduct manager\b",
    r"\bproject manager\b",
    r"\bbusiness analyst\b",
    r"\bproduct analyst\b",
    r"\bconsultant\b",
    r"\baccount executive\b",
    r"\btalent acquisition coordinator\b",
    r"\bhuman resource\b",
    r"\boperations associate\b",
    r"\bsocial media\b",

    # -------------------------
    # Support / non-core
    # -------------------------
    r"\bsupport engineer\b",
    r"\bcustomer support\b",

    # -------------------------
    # Senior / leadership (STRICT phrases only)
    # -------------------------
    r"\bprincipal engineer\b",
    r"\barchitect\b",
    r"\bstaff engineer\b",
    r"\blead software engineer\b",
    r"\bsenior software engineer\b",

    # -------------------------
    # Tech stacks you want to avoid
    # -------------------------
    r"\bphp developer\b",
    r"\blaravel developer\b",

    # -------------------------
    # Very specific unwanted roles
    # -------------------------
    r"\bapplied scientist\b",
    r"\bzeta global - programmatic associate\b",

    # -------------------------
    # Interns (optional)
    # -------------------------
    r"\bintern\b",
]

EXCLUDE_RE = re.compile("|".join(EXCLUDE_PATTERNS), re.I)

# Title-level exclusions: whole-word single terms to reject regardless of scraper mode
TITLE_EXCLUDE_WORDS = [
    r"\bworks\b",        # but NOT "works" fragments — matched as full word
    r"\bappears\b",
    r"\bsupport\b",
    r"\btest\b",
    r"\bsenior\b",
    r"\bstaff\b",
    r"\baccountant\b",
    r"\bintern\b",
    r"\bmanager\b",
]

# Title-level exclusions: exact full phrases
TITLE_EXCLUDE_PHRASES = [
    r"\bservicenow developer\b",
    r"\bcad engineer\b",
    r"\bquality analyst\b",
    r"\blead software engineer\b",
    r"\bzoho developer\b",
    r"\bbusiness analyst\b",
    r"\bcustomer service\b",
    r"\bdesign engineer\b",
    r"\bsite engineer\b",
    r"\bsap developer\b",
    r"\bdeployment engineer\b",
]

TITLE_EXCLUDE_RE = re.compile(
    "|".join(TITLE_EXCLUDE_WORDS + TITLE_EXCLUDE_PHRASES), re.I
)

# experience pattern: captures either '1-3 years', '2 years', '2+ years', '0 years', '3 yrs'
EXP_RANGE_RE = re.compile(r"(\d+)\s*[-to]{0,3}\s*(\d+)\s*(?:\+)?\s*(?:years|yrs|year)?", re.I)
EXP_SINGLE_RE = re.compile(r"(\d+)\s*(?:\+)?\s*(?:years|yrs|year)\b", re.I)
EXP_WORDS = {
    "entry": 0,
    "junior": 1,
    "fresher": 0,
    "graduate": 0,
    "mid": 2,
    "senior": 5,
}

def extract_description_text(html: str) -> str:
    """Try several LinkedIn description selectors and return combined text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    # common LinkedIn job description containers
    selectors = [
        "div.description", "div.jobs-description__container", "div.description__text",
        "div.jobs-box__html-content", "div.show-more-less-html__markup", "div.job-description",
        "#job-details", ".jobs-description-content__text", "section.description"
    ]
    parts = []
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            parts.append(el.get_text(separator=" ", strip=True))
    # fallback: whole text (but keep it short)
    if not parts:
        parts.append(soup.get_text(separator=" ", strip=True)[:20000])
    return "\n".join(parts).strip()

def parse_experience_years(text: str) -> Optional[int]:
    """
    Return the minimum explicit years required if found.
    - If a range '1-3 years' is found, return the lower bound (1).
    - If single '3 years' found, return 3.
    - If no numeric mention but contains keywords like 'entry', 'junior', return a hint number.
    - Returns None when no hint found.
    """
    if not text:
        return None
    # check ranges first
    for m in EXP_RANGE_RE.finditer(text):
        try:
            low = int(m.group(1))
            high = int(m.group(2))
            return min(low, high)
        except Exception:
            continue
    # single number mentions
    found_nums = []
    for m in EXP_SINGLE_RE.finditer(text):
        try:
            found_nums.append(int(m.group(1)))
        except Exception:
            continue
    if found_nums:
        return min(found_nums)
    # word-based hints
    low_hint = None
    low_hint_val = 999
    for word, val in EXP_WORDS.items():
        if re.search(r"\b" + re.escape(word) + r"\b", text, re.I):
            if val < low_hint_val:
                low_hint = val
                low_hint_val = val
    return low_hint

def build_keywords_from_resume_text(text: str) -> Set[str]:
    """
    Build a set of keywords from resume text by intersecting DEFAULT_TECH_KEYWORDS and
    anything that appears in the resume.
    """
    kws = set(DEFAULT_TECH_KEYWORDS)
    if not text:
        return kws
    lowered = text.lower()
    for tk in list(DEFAULT_TECH_KEYWORDS):
        if tk.lower() in lowered:
            kws.add(tk.lower())
    # also add any alphanumeric tokens that look like languages/frameworks (short tokens)
    tokens = set(re.findall(r"\b[A-Za-z0-9+\-#]{2,30}\b", lowered))
    # keep tokens that are plausible tech words (heuristic)
    for t in tokens:
        if len(t) <= 15 and any(ch.isalpha() for ch in t):
            kws.add(t)
    return {k.lower() for k in kws}

def is_relevant_job(html_text: str, title: str, keywords: Set[str]) -> bool:
    """
    Decide if the given job (html_text + title) is relevant:
      - MANDATORY: Title must contain role keywords OR tech keywords
      - Include if explicit numeric years <= 3 (0..3)
      - If numeric years required >3 => exclude
      - If no numeric mention => include if title/description match ROLE_RE and not EXCLUDE_RE
    """
    # MANDATORY: Title must contain role keywords OR tech keywords
    title_lower = (title or "").lower()
    has_role_keyword = ROLE_RE.search(title_lower)
    has_tech_keyword = any(tech_kw in title_lower for tech_kw in DEFAULT_TECH_KEYWORDS)

    if not (has_role_keyword or has_tech_keyword):
        return False

    # Title-level hard exclusions (whole-word single terms + exact phrases)
    if TITLE_EXCLUDE_RE.search(title_lower):
        return False

    combined = " ".join(filter(None, [title or "", extract_description_text(html_text) or ""]))
    # exclude data/ML roles first
    if EXCLUDE_RE.search(combined):
        return False

    years_req = parse_experience_years(combined)
    if years_req is not None:
        # include if required years is 0..3
        return years_req <= 3

    # no numeric requirement found; fallback to role keyword matching
    if ROLE_RE.search(combined):
        # extra filter: ensure one or more tech keywords from resume appear OR
        # title contains strong role hint (e.g., 'sde', 'developer', 'software engineer')
        found_kw = any((kw in combined.lower()) for kw in keywords)
        if found_kw:
            return True
        # allow if explicit role is present even without tech keywords (to avoid false negatives)
        return True

    return False
