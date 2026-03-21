from flask import render_template_string, session, url_for, get_flashed_messages


def render_page(content, title="TerraLedger"):
    flashes = get_flashed_messages()

    flash_html = "".join(
        f"<div class='flash-message'>{msg}</div>"
        for msg in flashes
    )

    company_name = session.get("company_name") or "TerraLedger"

    nav_links = []

    if session.get("user_id"):
        nav_links = [
            ("Dashboard", url_for("dashboard.dashboard")),
            ("Customers", url_for("customers.customers")),
            ("Quotes", url_for("quotes.quotes")),
            ("Jobs", url_for("jobs.jobs")),
            ("Invoices", url_for("invoices.invoices")),
            ("Bookkeeping", url_for("bookkeeping.bookkeeping")),
            ("Employees", url_for("employees.employees")),
            ("Settings", url_for("settings.settings")),
            ("Logout", url_for("auth.logout")),
        ]

    nav_html = "".join(
        f"<a href='{href}'>{label}</a>"
        for label, href in nav_links
    )

    html = f"""
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
        <title>{title}</title>

        <style>
            :root {{
                --bg: #13212b;
                --bg-deep: #0d1720;
                --panel: #1b2c38;
                --panel-soft: #223746;
                --border: #314756;
                --border-soft: #3b5364;
                --text: #f7f3ea;
                --muted: #b8c4cf;

                --sand: #d6c2a8;
                --blue: #5aa2d6;
                --orange: #f08c4a;
                --orange-soft: #ffb06b;
                --green: #6bbf72;
                --green-soft: #8fd49a;
                --red: #e46f6f;
                --yellow: #f0c36d;

                --shadow: 0 10px 25px rgba(0, 0, 0, 0.28);
                --radius: 16px;
                --radius-sm: 12px;
            }}

            * {{
                box-sizing: border-box;
            }}

            html, body {{
                margin: 0;
                padding: 0;
                background: linear-gradient(180deg, var(--bg-deep) 0%, var(--bg) 100%);
                color: var(--text);
                font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
            }}

            body {{
                min-height: 100vh;
            }}

            a {{
                color: inherit;
            }}

            .app-shell {{
                min-height: 100vh;
                display: flex;
                flex-direction: column;
            }}

            .topbar {{
                position: sticky;
                top: 0;
                z-index: 100;
                background: rgba(13, 23, 32, 0.92);
                backdrop-filter: blur(10px);
                border-bottom: 1px solid var(--border);
            }}

            .topbar-inner {{
                max-width: 1400px;
                margin: 0 auto;
                padding: 14px 18px;
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 16px;
                flex-wrap: wrap;
            }}

            .brand {{
                display: flex;
                align-items: center;
                gap: 10px;
                min-width: 0;
            }}

            .brand-badge {{
                width: 40px;
                height: 40px;
                border-radius: 12px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: linear-gradient(135deg, var(--orange), var(--orange-soft));
                color: #13212b;
                font-weight: 900;
                font-size: 1rem;
                flex: 0 0 auto;
                box-shadow: 0 8px 20px rgba(240, 140, 74, .28);
            }}

            .brand-text {{
                min-width: 0;
            }}

            .brand-title {{
                font-size: 1rem;
                font-weight: 800;
                line-height: 1.1;
                color: var(--text);
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }}

            .brand-subtitle {{
                font-size: .82rem;
                color: var(--muted);
                margin-top: 2px;
            }}

            .nav-wrap {{
                display: flex;
                align-items: center;
                gap: 10px;
                flex-wrap: wrap;
                justify-content: flex-end;
            }}

            .nav-wrap a {{
                text-decoration: none;
                font-size: .95rem;
                font-weight: 700;
                color: var(--muted);
                padding: 10px 12px;
                border-radius: 10px;
                transition: background .15s ease, color .15s ease, border-color .15s ease;
                border: 1px solid transparent;
            }}

            .nav-wrap a:hover {{
                background: rgba(90, 162, 214, 0.12);
                color: var(--text);
                border-color: rgba(90, 162, 214, 0.18);
            }}

            .page-wrap {{
                width: 100%;
                max-width: 1400px;
                margin: 0 auto;
                padding: 20px 18px 40px;
            }}

            .flash-stack {{
                display: grid;
                gap: 10px;
                margin-bottom: 18px;
            }}

            .flash-message {{
                background: rgba(240, 195, 109, .12);
                color: #ffe2a8;
                border: 1px solid rgba(240, 195, 109, .35);
                border-radius: 14px;
                padding: 14px 16px;
                font-weight: 700;
            }}

            .card {{
                background: linear-gradient(180deg, var(--panel) 0%, #182732 100%);
                border: 1px solid var(--border);
                border-radius: var(--radius);
                padding: 18px;
                box-shadow: var(--shadow);
                margin-bottom: 18px;
                overflow: hidden;
            }}

            .muted {{
                color: var(--muted);
            }}

            .small {{
                font-size: .88rem;
            }}

            h1, h2, h3 {{
                margin-top: 0;
                color: var(--text);
            }}

            h1 {{
                font-size: clamp(1.45rem, 2vw, 2rem);
            }}

            h2 {{
                font-size: clamp(1.12rem, 1.4vw, 1.4rem);
            }}

            h3 {{
                color: var(--sand);
            }}

            label {{
                display: block;
                font-size: .92rem;
                font-weight: 700;
                margin-bottom: 7px;
                color: var(--sand);
            }}

            input,
            select,
            textarea,
            button {{
                font: inherit;
            }}

            input,
            select,
            textarea {{
                width: 100%;
                border: 1px solid var(--border);
                background: #14232d;
                color: var(--text);
                border-radius: 12px;
                padding: 12px 14px;
                outline: none;
                transition: border-color .15s ease, box-shadow .15s ease, background .15s ease;
            }}

            input::placeholder,
            textarea::placeholder {{
                color: #8ea3b3;
            }}

            input:focus,
            select:focus,
            textarea:focus {{
                border-color: var(--blue);
                box-shadow: 0 0 0 4px rgba(90, 162, 214, 0.12);
                background: #172a35;
            }}

            textarea {{
                min-height: 110px;
                resize: vertical;
            }}

            .grid {{
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 16px;
            }}

            .stats-grid {{
                display: grid;
                grid-template-columns: repeat(4, minmax(0, 1fr));
                gap: 16px;
            }}

            .dashboard-grid,
            .settings-grid {{
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 18px;
            }}

            .stat-card {{
                text-align: center;
                background: linear-gradient(180deg, var(--panel-soft) 0%, var(--panel) 100%);
            }}

            .stat-label {{
                font-size: .92rem;
                color: var(--muted);
                margin-bottom: 8px;
            }}

            .stat-value {{
                font-size: 1.6rem;
                font-weight: 800;
                line-height: 1.1;
                color: var(--text);
            }}

            .section-head {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
                flex-wrap: wrap;
                margin-bottom: 12px;
            }}

            .row-actions {{
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                align-items: center;
            }}

            .inline-form {{
                display: inline;
            }}

            .btn {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 8px;
                min-height: 42px;
                padding: 10px 14px;
                border-radius: 12px;
                border: 1px solid var(--blue);
                background: var(--blue);
                color: #08131b;
                text-decoration: none;
                font-weight: 800;
                cursor: pointer;
                transition: transform .15s ease, background .15s ease, border-color .15s ease, color .15s ease;
            }}

            .btn:hover {{
                background: #6bb5ea;
                border-color: #6bb5ea;
                transform: translateY(-1px);
            }}

            .btn.secondary {{
                background: transparent;
                color: var(--text);
                border-color: var(--border);
            }}

            .btn.secondary:hover {{
                background: rgba(255,255,255,0.05);
                border-color: #4c6577;
            }}

            .btn.success {{
                background: var(--green);
                border-color: var(--green);
                color: #0d1b12;
            }}

            .btn.success:hover {{
                background: var(--green-soft);
                border-color: var(--green-soft);
            }}

            .btn.warning {{
                background: var(--orange);
                border-color: var(--orange);
                color: #1f140a;
            }}

            .btn.warning:hover {{
                background: var(--orange-soft);
                border-color: var(--orange-soft);
            }}

            .btn.danger {{
                background: var(--red);
                border-color: var(--red);
                color: #180b0b;
            }}

            .btn.danger:hover {{
                background: #ef8585;
                border-color: #ef8585;
            }}

            .btn.small {{
                min-height: 34px;
                padding: 7px 10px;
                font-size: .85rem;
                border-radius: 10px;
            }}

            .pill {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 5px 10px;
                border-radius: 999px;
                background: rgba(90, 162, 214, 0.14);
                color: #bfe4ff;
                border: 1px solid rgba(90, 162, 214, 0.25);
                font-size: .82rem;
                font-weight: 800;
                vertical-align: middle;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                min-width: 720px;
                color: var(--text);
            }}

            th, td {{
                border-bottom: 1px solid var(--border-soft);
                text-align: left;
                padding: 12px 10px;
                vertical-align: top;
            }}

            th {{
                background: #16252f;
                color: var(--sand);
                font-size: .9rem;
                font-weight: 800;
            }}

            tr:hover td {{
                background: rgba(255,255,255,0.02);
            }}

            .table-wrap,
            .table-scroll {{
                width: 100%;
                overflow-x: auto;
                -webkit-overflow-scrolling: touch;
                border-radius: 12px;
            }}

            .notice {{
                padding: 14px 16px;
                border-radius: 12px;
                margin-bottom: 14px;
                font-weight: 700;
            }}

            .notice.warning {{
                background: rgba(240, 195, 109, .10);
                color: #ffe2a8;
                border: 1px solid rgba(240, 195, 109, .32);
            }}

            .checkbox-field {{
                display: flex;
                flex-direction: column;
                justify-content: center;
            }}

            .checkbox-label {{
                display: flex;
                align-items: center;
                gap: 10px;
                font-weight: 700;
                margin-bottom: 4px;
                color: var(--text);
            }}

            .checkbox-label input[type="checkbox"] {{
                width: 18px;
                height: 18px;
                margin: 0;
            }}

            iframe {{
                max-width: 100%;
                background: #fff;
            }}

            img {{
                max-width: 100%;
                height: auto;
            }}

            @media (max-width: 1100px) {{
                .stats-grid {{
                    grid-template-columns: repeat(2, minmax(0, 1fr));
                }}

                .dashboard-grid,
                .settings-grid {{
                    grid-template-columns: 1fr;
                }}
            }}

            @media (max-width: 820px) {{
                .grid {{
                    grid-template-columns: 1fr;
                }}

                .topbar-inner {{
                    align-items: flex-start;
                }}

                .nav-wrap {{
                    width: 100%;
                    justify-content: flex-start;
                }}

                .nav-wrap a {{
                    padding: 9px 10px;
                    font-size: .92rem;
                }}

                .page-wrap {{
                    padding: 16px 12px 28px;
                }}

                .card {{
                    padding: 14px;
                    border-radius: 14px;
                }}

                .row-actions {{
                    width: 100%;
                }}

                .row-actions .btn,
                .row-actions button.btn {{
                    flex: 1 1 180px;
                }}
            }}

            @media (max-width: 640px) {{
                .stats-grid {{
                    grid-template-columns: 1fr;
                }}

                .brand-title {{
                    white-space: normal;
                }}

                .topbar-inner {{
                    padding: 12px;
                }}

                .page-wrap {{
                    padding: 12px 10px 24px;
                }}

                .card {{
                    padding: 12px;
                    margin-bottom: 14px;
                }}

                h1 {{
                    font-size: 1.35rem;
                }}

                h2 {{
                    font-size: 1.06rem;
                }}

                .btn,
                .btn.small {{
                    width: 100%;
                    min-height: 44px;
                }}

                .row-actions {{
                    flex-direction: column;
                    align-items: stretch;
                }}

                .section-head {{
                    align-items: stretch;
                }}

                .section-head .btn {{
                    width: 100%;
                }}

                table {{
                    min-width: 640px;
                }}

                input,
                select,
                textarea {{
                    padding: 12px;
                    font-size: 16px;
                }}
            }}
        </style>

        <link rel="stylesheet" href="/static/css/help_assistant.css">
    </head>
    <body>
        <div class="app-shell">
            <div class="topbar">
                <div class="topbar-inner">
                    <div class="brand">
                        <div class="brand-badge">TL</div>
                        <div class="brand-text">
                            <div class="brand-title">{company_name}</div>
                            <div class="brand-subtitle">TerraLedger</div>
                        </div>
                    </div>

                    {"<div class='nav-wrap'>" + nav_html + "</div>" if nav_html else ""}
                </div>
            </div>

            <main class="page-wrap">
                {"<div class='flash-stack'>" + flash_html + "</div>" if flash_html else ""}
                {content}
            </main>
        </div>

        <script src="/static/js/help_assistant.js"></script>
    </body>
    </html>
    """

    return render_template_string(html)