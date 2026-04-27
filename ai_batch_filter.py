# ai_batch_filter.py

from openai import OpenAI
from relevance_filter import extract_description_text
import json
import re

client = OpenAI()


def evaluate_jobs_batch_ai(jobs: list) -> list:
    """
    Input: list of jobs with html + title
    Output: list of results [{index, decision, score}]

    All jobs are always included in the prompt — even those with no html —
    so index alignment between the batch list and AI results is never broken.
    Jobs without a description are scored on title only.
    """

    prepared_jobs = []

    for i, job in enumerate(jobs):
        desc = extract_description_text(job.get("html", ""))
        desc = desc[:1200].strip() if desc else ""

        if not desc:
            print(f"[AI WARNING] Job {i} ('{job.get('title')}') has no description — scoring on title only.")
            desc = "(Description unavailable — please evaluate based on job title only)"

        prepared_jobs.append({
            "index": i,
            "title": job.get("title", ""),
            "desc": desc,
        })

    # 🔥 Build prompt
    job_text = ""
    for j in prepared_jobs:
        job_text += f"\nJob {j['index']}:\nTitle: {j['title']}\nDescription: {j['desc']}\n"

    prompt = f"""You are an intelligent job screening assistant.

Candidate Profile:
- Experience: 2+ years Software Developer
- Skills: C++, Golang, Python, Java, Spring Boot, Kafka, Redis
- Open to: Backend, Frontend, DevOps, Fullstack

Rules:
- Score each job from 0 to 100
- Prefer backend / distributed systems roles
- Allow frontend/devops roles if reasonable
- Reject strong mismatch (QA, Data, HR, Sales, Support, etc.)
- Reject very senior roles (5+ years)
- You MUST return one entry per job. Do NOT skip any index.

Return ONLY a valid JSON array — no explanation, no markdown fences.
Every job index from 0 to {len(prepared_jobs) - 1} must appear exactly once.

Example:
[
  {{"index": 0, "decision": "PASS", "score": 80}},
  {{"index": 1, "decision": "FAIL", "score": 30}}
]

Jobs:
{job_text}"""

    try:
        response = client.responses.create(
            model="gpt-4o-mini",
            input=prompt,
            max_output_tokens=800,
            temperature=0
        )

        raw = response.output_text.strip()

        print("\n[AI RAW RESPONSE]")
        print(raw)
        print("=================================\n")

        # Strip markdown fences if model wraps response in ```json ... ```
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw).strip()

        try:
            parsed = json.loads(raw)
            print(f"[AI PARSED RESULT COUNT]: {len(parsed)}")

            # Safety: if AI skipped any index, fill in FAIL for the missing ones
            returned_indices = {r.get("index") for r in parsed}
            for j in prepared_jobs:
                if j["index"] not in returned_indices:
                    print(f"[AI WARNING] Index {j['index']} ('{j['title']}') missing from AI response — defaulting to FAIL.")
                    parsed.append({"index": j["index"], "decision": "FAIL", "score": 0})

            return parsed

        except json.JSONDecodeError as e:
            print("[AI PARSE ERROR]", e)
            return [{"index": j["index"], "decision": "FAIL", "score": 0} for j in prepared_jobs]

    except Exception as e:
        print("[AI ERROR]", e)
        return [{"index": j["index"], "decision": "FAIL", "score": 0} for j in prepared_jobs]
