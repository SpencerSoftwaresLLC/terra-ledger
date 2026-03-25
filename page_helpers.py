from flask import render_template


def _build_wrapped_content(content):
    return f"""
    {content}
    """


def _scroll_script():
    return """
    """


def render_page(content, title="TerraLedger"):
    wrapped_content = _build_wrapped_content(content)

    return render_template(
        "BASE_HTML.html",
        content=wrapped_content + _scroll_script(),
        title=title,
        page_title=title,
    )


def render_public_page(content, title="TerraLedger"):
    wrapped_content = _build_wrapped_content(content)

    return render_template(
        "PUBLIC_BASE_HTML.html",
        content=wrapped_content + _scroll_script(),
        title=title,
        page_title=title,
    )