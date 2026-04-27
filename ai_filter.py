# ai_filter.py

from openai import OpenAI
from relevance_filter import extract_description_text
import json

client = OpenAI()


def evaluate_job_ai(html: str, title: str) -> dict:
    """
    Returns:
    {
        "decision": "PASS" or "FAIL",
        "score": int
    }
    """

    # ✅ Extract + trim description (CRITICAL FIX)
    desc = extract_description_text(html)
    desc = desc[:1500] if desc else ""

    # ❌ If no description → skip early (VERY IMPORTANT)
    if not desc.strip():
        print(f"[AI SKIPPED - EMPTY DESC] {title}")
        return {"decision": "FAIL", "score": 0}

    # 🔍 Logs (safe + short)
    print("\n[AI INPUT]")
    print(f"TITLE: {title}")
    print(f"DESC LEN: {len(desc)}")
    print(f"DESC PREVIEW: {desc[:200]}...\n")

    prompt = f"""
You are an intelligent job screening assistant.

Candidate Profile:
- Experience: 2+ years Software Developer
- Skills: C++, Golang, Python, Java, Spring Boot, Kafka, Redis
- Comfortable with: Backend, Frontend, DevOps, Distributed Systems
- Open to: SDE, Backend, Frontend, DevOps, Fullstack roles

Evaluation Rules:
- Give a MATCH SCORE from 0 to 100
- Prefer backend / distributed systems roles
- Allow frontend & devops roles if reasonable
- Reject ONLY if:
  - Strong mismatch (QA, Data Scientist, Marketing, HR, etc.)
  - Very senior roles (strictly 5+ years required)

Output STRICT JSON:
{{
  "decision": "PASS or FAIL",
  "score": number
}}

Job Title:
{title}

Job Description:
{desc}
"""

    try:
        response = client.responses.create(
            model="gpt-4o-mini",
            input=prompt,
            max_output_tokens=100,
            temperature=0
        )

        raw_output = response.output[0].content[0].text.strip()

        print("[AI RAW OUTPUT]")
        print(raw_output)

        # ✅ Safe JSON parse
        try:
            parsed = json.loads(raw_output)
        except Exception:
            print("[AI PARSE ERROR]")
            return {"decision": "FAIL", "score": 0}

        # ✅ Normalize output
        decision = str(parsed.get("decision", "FAIL")).upper()
        score = parsed.get("score", 0)

        try:
            score = int(score)
        except Exception:
            score = 0

        # ✅ Clamp score
        score = max(0, min(score, 100))

        final = {
            "decision": "PASS" if decision == "PASS" else "FAIL",
            "score": score
        }

        print("[AI FINAL RESULT]", final)

        return final

    except Exception as e:
        print(f"[AI ERROR] {e}")
        return {"decision": "FAIL", "score": 0}