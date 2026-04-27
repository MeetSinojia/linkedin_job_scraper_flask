# ai_batch_filter.py

from openai import OpenAI
from relevance_filter import extract_description_text
import json

client = OpenAI()


def evaluate_jobs_batch_ai(jobs: list) -> list:
    """
    Input: list of jobs with html + title
    Output: list of results [{index, decision, score}]
    """

    prepared_jobs = []

    # 🔥 Prepare jobs (clean + trimmed)
    for i, job in enumerate(jobs):
        desc = extract_description_text(job.get("html", ""))
        desc = desc[:1200] if desc else ""

        if not desc.strip():
            continue

        prepared_jobs.append({
            "index": i,
            "title": job.get("title", ""),
            "desc": desc
        })

    if not prepared_jobs:
        return []

    # 🔥 Build prompt
    job_text = ""
    for j in prepared_jobs:
        job_text += f"""
Job {j['index']}:
Title: {j['title']}
Description: {j['desc']}
"""

    prompt = f"""
You are an intelligent job screening assistant.

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

Return ONLY valid JSON.
Do NOT explain anything.
Do NOT add text outside JSON.

Example:
[
  {{"index": 0, "decision": "PASS", "score": 80}},
  {{"index": 1, "decision": "FAIL", "score": 30}}
]

Jobs:
{job_text}
"""

    try:
        response = client.responses.create(
            model="gpt-4o-mini",
            input=prompt,
            max_output_tokens=500,
            temperature=0
        )

        raw = response.output[0].content[0].text.strip()

        print("\n[AI RAW RESPONSE]")
        print(raw)
        print("=================================\n")

        # ✅ Parse JSON
        try:
            parsed = json.loads(raw)
            print(f"[AI PARSED RESULT COUNT]: {len(parsed)}")
            return parsed
        except Exception as e:
            print("[AI PARSE ERROR]", e)

            # 🔥 fallback → avoid empty result
            fallback = []
            for j in prepared_jobs:
                fallback.append({
                    "index": j["index"],
                    "decision": "FAIL",
                    "score": 0
                })

            return fallback

    except Exception as e:
        print("[AI ERROR]", e)

        # 🔥 fallback
        fallback = []
        for j in prepared_jobs:
            fallback.append({
                "index": j["index"],
                "decision": "FAIL",
                "score": 0
            })

        return fallback
