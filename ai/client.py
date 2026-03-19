# TerraLedger/ai/client.py

import os
from openai import OpenAI

from .prompts import (
    build_help_system_prompt,
    build_page_context,
    build_help_input_messages,
)


def get_openai_client():
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
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

    model = os.environ.get("OPENAI_HELP_MODEL", "").strip() or "gpt-4.1-mini"

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
        prior_messages=prior_messages,
    )

    response = client.responses.create(
        model=model,
        instructions=system_prompt,
        input=input_messages,
    )

    answer = (getattr(response, "output_text", None) or "").strip()

    if not answer:
        raise RuntimeError("OpenAI returned no output_text.")

    return answer