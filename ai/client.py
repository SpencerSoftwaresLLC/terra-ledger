# TerraLedger/ai/client.py

import os
from openai import OpenAI

from ai.prompts import (
    build_help_system_prompt,
    build_page_context,
    build_help_input_messages,
)


def get_openai_client():
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing.")

    return OpenAI(api_key=api_key)


def ask_terraledger_help(
    user_question,
    page_name=None,
    route=None,
    user_role=None,
    company_name=None,
    user_name=None,
    prior_messages=None,
):
    client = get_openai_client()

    model = (os.environ.get("OPENAI_HELP_MODEL") or "").strip() or "gpt-4.1-mini"

    system_prompt = build_help_system_prompt()
    page_context_text = build_page_context(
        page_title=page_name,
        route=route,
        company_name=company_name,
        user_name=user_name,
        user_role=user_role,
    )

    input_messages = build_help_input_messages(
        user_question=user_question,
        page_context_text=page_context_text,
        prior_messages=prior_messages or [],
    )

    try:
        response = client.responses.create(
            model=model,
            instructions=system_prompt,
            input=input_messages,
            temperature=0.2,
        )
    except Exception as e:
        raise RuntimeError(f"OpenAI request failed: {e}")

    answer = (getattr(response, "output_text", None) or "").strip()

    if answer:
        return answer

    # Fallback parsing in case output_text is empty but content exists
    try:
        collected = []

        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                text_value = getattr(content, "text", None)
                if isinstance(text_value, str) and text_value.strip():
                    collected.append(text_value.strip())
                elif hasattr(text_value, "value") and str(text_value.value).strip():
                    collected.append(str(text_value.value).strip())

        answer = "\n".join(part for part in collected if part).strip()
    except Exception:
        answer = ""

    if not answer:
        raise RuntimeError("OpenAI returned no usable response text.")

    return answer