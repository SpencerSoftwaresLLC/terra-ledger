from flask import render_template
from flask_wtf.csrf import generate_csrf
from markupsafe import Markup


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


def render_page(content, title="TerraLedger"):
    wrapped_content = _build_wrapped_content(content)

    return render_template(
        "BASE_HTML.html",
        content=wrapped_content + _scroll_script(),
        title=title,
        page_title=title,
        csrf_input=csrf_input,
    )


def render_public_page(content, title="TerraLedger"):
    wrapped_content = _build_wrapped_content(content)

    return render_template(
        "PUBLIC_BASE_HTML.html",
        content=wrapped_content + _scroll_script(),
        title=title,
        page_title=title,
        csrf_input=csrf_input,
    )