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

    for i, job in enumerate(jobs):
        desc = extract_description_text(job.get("html", ""))[:1200]

        if not desc.strip():
            continue

        prepared_jobs.append({
            "index": i,
            "title": job.get("title", ""),
            "desc": desc
        })

    if not prepared_jobs:
        return []

    # 🔥 Build ONE prompt
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
- Prefer backend / distributed systems
- Allow frontend/devops
- Reject strong mismatch (QA, Data, HR, etc.)
- Reject very senior roles (5+ years)

Return STRICT JSON ARRAY:
[
  {{
    "index": 0,
    "decision": "PASS or FAIL",
    "score": number
  }}
]

Jobs:
{job_text}
"""

    try:
        response = client.responses.create(
            model="gpt-4o-mini",
            input=prompt,
            max_output_tokens=100,
            temperature=0
        )

        raw = response.output[0].content[0].text.strip()

        print("[AI BATCH OUTPUT]")
        print(raw)

        try:
            parsed = json.loads(raw)
            return parsed
        except:
            print("[AI PARSE ERROR]")
            return []

    except Exception as e:
        print("[AI ERROR]", e)
        return []