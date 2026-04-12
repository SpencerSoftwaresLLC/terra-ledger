from flask import render_template, session
from flask_wtf.csrf import generate_csrf
from markupsafe import Markup

from db import get_db_connection


def _build_wrapped_content(content):
    return f"""
    {content}
    """


def _scroll_script():
    return """
    """


def csrf_input():
    token = generate_csrf()
    return Markup(
        f'<input type="hidden" name="csrf_token" value="{token}">'
    )


def get_company_language(company_id):
    if not company_id:
        return "en"

    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT language_preference
            FROM company_profile
            WHERE company_id = %s
            """,
            (company_id,),
        ).fetchone()

        if row and "language_preference" in row.keys():
            lang = (row["language_preference"] or "").strip().lower()
            if lang in {"en", "es"}:
                return lang

        return "en"
    except Exception:
        return "en"
    finally:
        conn.close()


def render_page(content, title="TerraLedger"):
    wrapped_content = _build_wrapped_content(content)
    company_id = session.get("company_id")
    lang = get_company_language(company_id)

    return render_template(
        "BASE_HTML.html",
        content=wrapped_content + _scroll_script(),
        title=title,
        page_title=title,
        csrf_input=csrf_input,
        _lang=lang,
    )


def render_public_page(content, title="TerraLedger"):
    wrapped_content = _build_wrapped_content(content)

    return render_template(
        "PUBLIC_BASE_HTML.html",
        content=wrapped_content + _scroll_script(),
        title=title,
        page_title=title,
        csrf_input=csrf_input,
        _lang="en",
    )