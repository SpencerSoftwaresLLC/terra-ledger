# TerraLedger/ai/service.py

import os

from openai import OpenAI

from ai.context_builder import build_ai_system_prompt


def ask_terraledger_ai(user_message, company_id=None):
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set")

    client = OpenAI(api_key=api_key)

    system_prompt, include_insights = build_ai_system_prompt(
        company_id=company_id,
        user_message=user_message,
    )

    model_name = (os.environ.get("OPENAI_MODEL") or "gpt-4.1-mini").strip()

    response = client.responses.create(
        model=model_name,
        input=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_message,
            },
        ],
        temperature=0.2,
    )

    text = ""
    try:
        text = response.output_text
    except Exception:
        text = ""

    if not text:
        text = "I could not generate a response right now."

    return {
        "answer": text.strip(),
        "used_business_insights": include_insights,
        "model": model_name,
    }