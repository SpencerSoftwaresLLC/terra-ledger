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
                --bg: #f5f7fb;
                --panel: #ffffff;
                --panel-soft: #f8fafc;
                --border: #dbe2ea;
                --border-soft: #e8edf3;
                --text: #0f172a;
                --muted: #64748b;

                --primary: #2563eb;
                --primary-dark: #1d4ed8;
                --secondary: #ffffff;
                --secondary-text: #0f172a;

                --success: #16a34a;
                --warning: #f59e0b;
                --danger: #dc2626;

                --shadow: 0 10px 25px rgba(15, 23, 42, 0.06);
                --radius: 16px;
                --radius-sm: 12px;
            }}

            * {{
                box-sizing: border-box;
            }}

            html, body {{
                margin: 0;
                padding: 0;
                background: var(--bg);
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
                background: rgba(255,255,255,.92);
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
                background: linear-gradient(135deg, #1e293b, #334155);
                color: #fff;
                font-weight: 800;
                font-size: 1rem;
                flex: 0 0 auto;
            }}

            .brand-text {{
                min-width: 0;
            }}

            .brand-title {{
                font-size: 1rem;
                font-weight: 800;
                line-height: 1.1;
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
                font-weight: 600;
                color: var(--muted);
                padding: 10px 12px;
                border-radius: 10px;
                transition: background .15s ease, color .15s ease;
            }}

            .nav-wrap a:hover {{
                background: #eef4ff;
                color: var(--primary);
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
                background: #eff6ff;
                color: #1d4ed8;
                border: 1px solid #bfdbfe;
                border-radius: 14px;
                padding: 14px 16px;
                font-weight: 600;
            }}

            .card {{
                background: var(--panel);
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

            label {{
                display: block;
                font-size: .92rem;
                font-weight: 700;
                margin-bottom: 7px;
                color: #334155;
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
                background: #fff;
                color: var(--text);
                border-radius: 12px;
                padding: 12px 14px;
                outline: none;
                transition: border-color .15s ease, box-shadow .15s ease;
            }}

            input:focus,
            select:focus,
            textarea:focus {{
                border-color: #93c5fd;
                box-shadow: 0 0 0 4px rgba(37, 99, 235, 0.10);
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
                border: 1px solid var(--primary);
                background: var(--primary);
                color: #fff;
                text-decoration: none;
                font-weight: 700;
                cursor: pointer;
                transition: transform .15s ease, background .15s ease, border-color .15s ease;
            }}

            .btn:hover {{
                background: var(--primary-dark);
                border-color: var(--primary-dark);
                transform: translateY(-1px);
            }}

            .btn.secondary {{
                background: #fff;
                color: var(--secondary-text);
                border-color: var(--border);
            }}

            .btn.secondary:hover {{
                background: #f8fafc;
                border-color: #cbd5e1;
            }}

            .btn.success {{
                background: var(--success);
                border-color: var(--success);
            }}

            .btn.success:hover {{
                background: #15803d;
                border-color: #15803d;
            }}

            .btn.warning {{
                background: var(--warning);
                border-color: var(--warning);
                color: #111827;
            }}

            .btn.warning:hover {{
                background: #d97706;
                border-color: #d97706;
                color: #fff;
            }}

            .btn.danger {{
                background: var(--danger);
                border-color: var(--danger);
            }}

            .btn.danger:hover {{
                background: #b91c1c;
                border-color: #b91c1c;
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
                background: #eef2ff;
                color: #4338ca;
                font-size: .82rem;
                font-weight: 700;
                vertical-align: middle;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                min-width: 720px;
            }}

            th, td {{
                border-bottom: 1px solid var(--border-soft);
                text-align: left;
                padding: 12px 10px;
                vertical-align: top;
            }}

            th {{
                background: #f8fafc;
                color: #334155;
                font-size: .9rem;
                font-weight: 800;
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
                font-weight: 600;
            }}

            .notice.warning {{
                background: #fff7ed;
                color: #9a3412;
                border: 1px solid #fdba74;
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
            }}

            .checkbox-label input[type="checkbox"] {{
                width: 18px;
                height: 18px;
                margin: 0;
            }}

            iframe {{
                max-width: 100%;
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