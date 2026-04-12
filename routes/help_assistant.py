# TerraLedger/routes/help_assistant.py

from flask import Blueprint, request, jsonify, session
from decorators import login_required
from ai.client import ask_terraledger_help
from ai.knowledge import calculate_material, get_help_knowledge
from ai.context_builder import should_include_business_insights
from ai.insights import format_sales_snapshot_for_ai
from extensions import csrf
from db import get_db_connection, table_columns

from datetime import date, datetime

help_assistant_bp = Blueprint("help_assistant", __name__)

MAX_MESSAGE_LENGTH = 4000
MAX_HISTORY_MESSAGES = 10


def _safe_text(value, default=""):
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _truncate_text(value, max_len=MAX_MESSAGE_LENGTH):
    text = _safe_text(value)
    if len(text) <= max_len:
        return text
    return text[:max_len]


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _money(value):
    return f"${_safe_float(value):,.2f}"


def _table_exists(conn, table_name):
    try:
        return len(table_columns(conn, table_name)) > 0
    except Exception:
        return False


def _has_col(conn, table_name, col_name):
    try:
        return col_name in table_columns(conn, table_name)
    except Exception:
        return False


def _normalize_text(value):
    return _safe_text(value).strip().lower()


def _get_chat_history():
    history = session.get("help_assistant_history")
    if not isinstance(history, list):
        return []

    cleaned = []
    for item in history:
        if not isinstance(item, dict):
            continue

        role = _safe_text(item.get("role"))
        content = _truncate_text(item.get("content", ""))

        if role in {"user", "assistant"} and content:
            cleaned.append({
                "role": role,
                "content": content,
            })

    return cleaned[-MAX_HISTORY_MESSAGES:]


def _save_chat_history(history):
    cleaned = []
    for item in history:
        if not isinstance(item, dict):
            continue

        role = _safe_text(item.get("role"))
        content = _truncate_text(item.get("content", ""))

        if role in {"user", "assistant"} and content:
            cleaned.append({
                "role": role,
                "content": content,
            })

    session["help_assistant_history"] = cleaned[-MAX_HISTORY_MESSAGES:]
    session.modified = True


def _format_list(lines):
    if not lines:
        return "- None"
    return "\n".join(f"- {line}" for line in lines)


def _knowledge_summary_block():
    knowledge = get_help_knowledge()

    modules = knowledge.get("modules", {})
    module_lines = []
    for module_name, module_data in modules.items():
        summary = _safe_text(module_data.get("summary", ""))
        if summary:
            module_lines.append(f"- {module_name}: {summary}")

    return f"""
TerraLedger product knowledge:

App Name:
{_safe_text(knowledge.get("app_name", "TerraLedger"))}

Summary:
{_safe_text(knowledge.get("summary", ""))}

Core Workflow:
{_format_list(knowledge.get("core_workflow", []))}

Workflow Notes:
{_format_list(knowledge.get("workflow_notes", []))}

Important Rules:
{_format_list(knowledge.get("important_rules", []))}

Response Rules:
{_format_list(knowledge.get("response_rules", []))}

Supported Help Topics:
{_format_list(knowledge.get("supported_help_topics", []))}

Known Modules:
{chr(10).join(module_lines) if module_lines else "- None listed"}
""".strip()


def _recent_terraledger_context():
    return """
Recent TerraLedger capabilities and updates to keep in mind:

- TerraLedger uses PostgreSQL now instead of SQLite.
- Security hardening has been added across the app, including CSRF protection for forms and admin routes.
- Payroll supports payroll entries, payroll previews, printable payroll checks, and payroll check stubs.
- Payroll includes federal, state, Social Security, Medicare, and local tax handling.
- Quotes support quote creation, quote items, emailing quote PDFs, and conversion from quote to job.
- Jobs support billable and cost-based job items, calendar scheduling, recurring schedule generation, and job-to-invoice workflows.
- Invoices support invoice creation, invoice items, payments, partial payments, payment tracking, email preview, PDF sending, and Stripe payment links.
- Bookkeeping combines ledger entries, payroll, invoice payments, and job costs into bookkeeping / P&L reporting.
- Bookkeeping supports manual entries, CSV export, P&L breakdowns, and printable bookkeeping checks.
- Settings include company info, branding, email settings, tax defaults, W-2 company profile, backups, backup restore, and time clock pay period settings.
- Users & Permissions supports role-based access and per-user permission management.
- Billing supports Stripe subscription handling, checkout, webhook syncing, billing history, and customer portal access.
- The platform has routes/modules for dashboard, customers, jobs, quotes, invoices, payroll, employees, users, settings, billing, bookkeeping, help assistant, mobile, calendar, messages, payment setup, and legal.
- Branding includes logo uploads, document headers, and footer notes.
- Email settings include sender identity, reply-to behavior, and test email sending.
- W-2 tools include W-2 readiness, company W-2 profile, employee year-end summaries, and printable W-2 summary PDFs.
- Backup tools include backup export and restore.
- Time clock supports current pay period hours, previous pay period summaries, and pay-period-based entry filtering.
- Stripe invoice payments can use connected accounts and platform fee handling when configured.
""".strip()


def _is_outlook_question(user_question, page_title="", route=""):
    text = " ".join([
        _normalize_text(user_question),
        _normalize_text(page_title),
        _normalize_text(route),
    ])

    keywords = [
        "yearly outlook",
        "year end",
        "year-end",
        "forecast",
        "projection",
        "projected",
        "outlook",
        "ytd",
        "rest of the year",
        "this year",
        "how am i doing",
        "where am i at",
        "profit this year",
        "revenue this year",
    ]
    return any(k in text for k in keywords)


def _is_bookkeeping_page(page_title="", route=""):
    text = f"{_normalize_text(page_title)} {_normalize_text(route)}"
    return any(part in text for part in [
        "ledger",
        "bookkeeping",
        "p&l",
        "profit",
        "loss",
        "/ledger",
        "/bookkeeping",
    ])


def _days_elapsed_this_year():
    today = date.today()
    start = date(today.year, 1, 1)
    return max((today - start).days + 1, 1)


def _days_in_this_year():
    today = date.today()
    start = date(today.year, 1, 1)
    end = date(today.year + 1, 1, 1)
    return (end - start).days


def _get_open_invoices_snapshot(conn, company_id):
    if not _table_exists(conn, "invoices"):
        return {
            "open_invoice_count": 0,
            "open_invoice_total": 0.0,
            "overdue_invoice_count": 0,
            "overdue_invoice_total": 0.0,
        }

    today_str = date.today().isoformat()

    total_col = "total" if _has_col(conn, "invoices", "total") else None
    status_col = "status" if _has_col(conn, "invoices", "status") else None
    due_col = "due_date" if _has_col(conn, "invoices", "due_date") else None

    amount_expr = f"COALESCE({total_col}, 0)" if total_col else "0"

    where_parts = ["company_id = %s"]
    params = [company_id]

    if status_col:
        where_parts.append("(status IS NULL OR status NOT IN ('Paid', 'Cancelled', 'Void'))")

    where_sql = " AND ".join(where_parts)

    open_row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS open_count,
            COALESCE(SUM({amount_expr}), 0) AS open_total
        FROM invoices
        WHERE {where_sql}
        """
        ,
        tuple(params),
    ).fetchone()

    overdue_count = 0
    overdue_total = 0.0

    if due_col:
        overdue_row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS overdue_count,
                COALESCE(SUM({amount_expr}), 0) AS overdue_total
            FROM invoices
            WHERE {where_sql}
              AND {due_col} IS NOT NULL
              AND {due_col} < %s
            """,
            tuple(params + [today_str]),
        ).fetchone()

        overdue_count = int(overdue_row["overdue_count"] or 0) if overdue_row else 0
        overdue_total = _safe_float(overdue_row["overdue_total"] if overdue_row else 0)

    return {
        "open_invoice_count": int(open_row["open_count"] or 0) if open_row else 0,
        "open_invoice_total": _safe_float(open_row["open_total"] if open_row else 0),
        "overdue_invoice_count": overdue_count,
        "overdue_invoice_total": overdue_total,
    }


def _get_scheduled_jobs_snapshot(conn, company_id):
    if not _table_exists(conn, "jobs"):
        return {
            "scheduled_jobs_count": 0,
            "future_scheduled_jobs_count": 0,
        }

    has_status = _has_col(conn, "jobs", "status")
    has_scheduled_date = _has_col(conn, "jobs", "scheduled_date")

    today_str = date.today().isoformat()

    if has_scheduled_date:
        future_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM jobs
            WHERE company_id = %s
              AND scheduled_date IS NOT NULL
              AND scheduled_date >= %s
            """,
            (company_id, today_str),
        ).fetchone()
        future_count = int(future_row["c"] or 0) if future_row else 0
    else:
        future_count = 0

    if has_status:
        scheduled_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM jobs
            WHERE company_id = %s
              AND status IN ('Scheduled', 'In Progress')
            """,
            (company_id,),
        ).fetchone()
        scheduled_count = int(scheduled_row["c"] or 0) if scheduled_row else 0
    else:
        scheduled_count = future_count

    return {
        "scheduled_jobs_count": scheduled_count,
        "future_scheduled_jobs_count": future_count,
    }


def _get_bookkeeping_outlook_block(company_id, user_question, page_title="", route=""):
    if not company_id:
        return ""

    if not (_is_bookkeeping_page(page_title, route) or _is_outlook_question(user_question, page_title, route)):
        return ""

    conn = get_db_connection()
    try:
        current_year = date.today().year
        start_date = f"{current_year}-01-01"
        today_str = date.today().isoformat()

        total_income = 0.0
        total_expenses = 0.0

        if _table_exists(conn, "invoice_payments"):
            payment_date_col = "payment_date" if _has_col(conn, "invoice_payments", "payment_date") else None
            amount_col = "amount" if _has_col(conn, "invoice_payments", "amount") else None

            if payment_date_col and amount_col:
                row = conn.execute(
                    f"""
                    SELECT COALESCE(SUM(COALESCE({amount_col}, 0)), 0) AS total
                    FROM invoice_payments
                    WHERE company_id = %s
                      AND {payment_date_col} BETWEEN %s AND %s
                    """,
                    (company_id, start_date, today_str),
                ).fetchone()
                total_income += _safe_float(row["total"] if row else 0)

        if _table_exists(conn, "ledger_entries"):
            ledger_cols = table_columns(conn, "ledger_entries")

            date_col = None
            for possible in ["entry_date", "date", "posted_at", "created_at"]:
                if possible in ledger_cols:
                    date_col = possible
                    break

            amount_col = "amount" if "amount" in ledger_cols else None
            entry_type_col = "entry_type" if "entry_type" in ledger_cols else None
            source_type_col = "source_type" if "source_type" in ledger_cols else None
            reference_type_col = "reference_type" if "reference_type" in ledger_cols else None
            category_col = "category" if "category" in ledger_cols else None
            description_col = "description" if "description" in ledger_cols else ("memo" if "memo" in ledger_cols else None)

            if date_col and amount_col:
                rows = conn.execute(
                    f"""
                    SELECT
                        {entry_type_col if entry_type_col else "NULL"} AS entry_type,
                        {source_type_col if source_type_col else "NULL"} AS source_type,
                        {reference_type_col if reference_type_col else "NULL"} AS reference_type,
                        {category_col if category_col else "NULL"} AS category,
                        {description_col if description_col else "NULL"} AS description,
                        COALESCE({amount_col}, 0) AS amount
                    FROM ledger_entries
                    WHERE company_id = %s
                      AND {date_col} BETWEEN %s AND %s
                    """,
                    (company_id, start_date, today_str),
                ).fetchall()

                for r in rows:
                    entry_type = _normalize_text(r["entry_type"] if "entry_type" in r.keys() else "")
                    source_type = _normalize_text(r["source_type"] if "source_type" in r.keys() else "")
                    reference_type = _normalize_text(r["reference_type"] if "reference_type" in r.keys() else "")
                    category = _normalize_text(r["category"] if "category" in r.keys() else "")
                    description = _normalize_text(r["description"] if "description" in r.keys() else "")
                    amount = abs(_safe_float(r["amount"] if "amount" in r.keys() else 0))

                    is_invoice_payment = (
                        source_type in {"invoice_payment", "invoice_paid", "invoice_mark_paid", "payment"}
                        or reference_type in {"invoice_payment", "invoice_paid", "invoice_mark_paid", "payment"}
                        or "invoice payment" in entry_type
                        or "invoice payment" in category
                        or ("invoice" in description and "payment" in description)
                    )
                    if is_invoice_payment:
                        continue

                    is_expense = False
                    if entry_type in {"expense", "cost"}:
                        is_expense = True
                    elif source_type in {"job_item", "job_line", "job_material", "job_labor", "job_cost", "payroll"}:
                        is_expense = True
                    elif reference_type in {"job_item", "payroll"}:
                        is_expense = True
                    elif category in {
                        "expense", "expenses", "cost", "job cost", "material", "materials", "mulch",
                        "stone", "dump fee", "dump_fee", "plants", "trees", "soil", "fertilizer",
                        "hardscape material", "hardscape_material", "labor", "labour", "fuel",
                        "equipment", "delivery", "misc", "payroll", "hand tools", "office supplies",
                        "maintenance", "power equipment", "vehicles", "insurance", "marketing",
                        "office and admin", "safety gear", "licensing & certifications",
                        "licensing and certifications"
                    }:
                        is_expense = True

                    if is_expense:
                        total_expenses += amount
                    else:
                        # only include non-payment income if it truly looks like income
                        if entry_type in {"income", "payment"} or category == "income":
                            total_income += amount

        if _table_exists(conn, "payroll_entries"):
            pay_date_col = "pay_date" if _has_col(conn, "payroll_entries", "pay_date") else None
            gross_col = "gross_pay" if _has_col(conn, "payroll_entries", "gross_pay") else None

            if pay_date_col and gross_col:
                row = conn.execute(
                    f"""
                    SELECT COALESCE(SUM(COALESCE({gross_col}, 0)), 0) AS total
                    FROM payroll_entries
                    WHERE company_id = %s
                      AND {pay_date_col} BETWEEN %s AND %s
                    """,
                    (company_id, start_date, today_str),
                ).fetchone()
                total_expenses += _safe_float(row["total"] if row else 0)

        open_invoice_data = _get_open_invoices_snapshot(conn, company_id)
        scheduled_jobs_data = _get_scheduled_jobs_snapshot(conn, company_id)

        days_elapsed = _days_elapsed_this_year()
        days_in_year = _days_in_this_year()

        projected_income = (total_income / days_elapsed) * days_in_year if days_elapsed > 0 else total_income
        projected_expenses = (total_expenses / days_elapsed) * days_in_year if days_elapsed > 0 else total_expenses
        projected_net = projected_income - projected_expenses
        current_net = total_income - total_expenses

        return f"""
Bookkeeping and year-end outlook context for this company:

Current year:
{current_year}

Today:
{today_str}

Year-to-date totals based on available bookkeeping and payment data:
- YTD income: {_money(total_income)}
- YTD expenses: {_money(total_expenses)}
- YTD net profit: {_money(current_net)}

Pace-based estimate for the full year:
- Estimated year-end income: {_money(projected_income)}
- Estimated year-end expenses: {_money(projected_expenses)}
- Estimated year-end net profit: {_money(projected_net)}

Receivables snapshot:
- Open invoices: {open_invoice_data['open_invoice_count']}
- Open invoice total: {_money(open_invoice_data['open_invoice_total'])}
- Overdue invoices: {open_invoice_data['overdue_invoice_count']}
- Overdue invoice total: {_money(open_invoice_data['overdue_invoice_total'])}

Workload snapshot:
- Scheduled / in-progress jobs: {scheduled_jobs_data['scheduled_jobs_count']}
- Future scheduled jobs: {scheduled_jobs_data['future_scheduled_jobs_count']}

Critical response instruction:
If the user asks for a yearly outlook, YTD summary, forecast, or how the business is doing, do NOT respond with generic instructions like "check invoices" or "review reports."
Instead, directly summarize the numbers above in plain English.
Lead with the actual YTD totals first, then give the estimated year-end outlook, then mention open invoices and scheduled jobs as additional context.
Always describe the year-end figures as estimates based on current pace, not guarantees.
""".strip()

    except Exception as e:
        return f"""
Bookkeeping outlook context could not be loaded cleanly.
Error: {e}

If the user asked for a yearly outlook, explain that the bookkeeping forecast data could not be calculated right now.
""".strip()
    finally:
        conn.close()


def _build_business_insight_block(company_id, user_question):
    if not company_id:
        return ""

    if not should_include_business_insights(user_question):
        return ""

    try:
        insight_text = format_sales_snapshot_for_ai(company_id)
        return f"""
Business insight context for this company:

{insight_text}

Use this context only when the user is asking about sales, revenue, trends, growth, forecasts, YTD progress, or year-end projections.
Describe forecasts as estimates based on historical paid invoice revenue and current pace, not guarantees.
""".strip()
    except Exception as e:
        return f"""
Business insight context could not be loaded cleanly for this request.
Error: {e}

If the user is asking for sales trends or forecasts, explain that the insight layer could not be loaded right now.
""".strip()


def _build_material_block(user_question):
    calc_result = calculate_material(user_question)
    if not calc_result:
        return "", None

    return f"""
Material calculation context:

The user's message appears to request a material estimate.
Use the following result directly if it answers the question:

{calc_result}
""".strip(), calc_result


def _build_augmented_question(user_question, company_id=None, page_title="", route=""):
    knowledge_block = _knowledge_summary_block()
    business_block = _build_business_insight_block(company_id, user_question)
    bookkeeping_outlook_block = _get_bookkeeping_outlook_block(
        company_id=company_id,
        user_question=user_question,
        page_title=page_title,
        route=route,
    )
    material_block, _ = _build_material_block(user_question)

    context_parts = [
        "You are the built-in TerraLedger help assistant.",
        "Answer using the current TerraLedger feature set and current app logic.",
        "Be direct, practical, and accurate.",
        "Do not invent features that do not exist.",
        "If a feature is not built yet, say so clearly.",
        "If the user is asking how to do something, give step-by-step instructions.",
        "If the user is asking about sales, bookkeeping, forecasting, yearly outlook, YTD results, revenue trends, or year-end projections, use any available sales/bookkeeping insight context and give the answer directly using the actual numbers provided.",
        "Never answer a yearly outlook question with only generic advice like 'check invoices' or 'review reports' when actual numeric context is available.",
        "When on the ledger, bookkeeping, or P&L pages, prefer bookkeeping-specific context over generic product help.",
        knowledge_block,
        _recent_terraledger_context(),
    ]

    if business_block:
        context_parts.append(business_block)

    if bookkeeping_outlook_block:
        context_parts.append(bookkeeping_outlook_block)

    if material_block:
        context_parts.append(material_block)

    if page_title:
        context_parts.append(f"Current page title: {page_title}")
    if route:
        context_parts.append(f"Current route: {route}")

    context_parts.append(f"User question: {user_question}")

    return "\n\n".join(part for part in context_parts if part)


@help_assistant_bp.route("/api/help-assistant", methods=["POST"])
@csrf.exempt
@login_required
def help_assistant_api():
    try:
        data = request.get_json(silent=True) or {}

        user_question = _truncate_text(data.get("message", ""))
        page_title = _truncate_text(data.get("page_title", ""), 300)
        route = _truncate_text(data.get("route", ""), 300)

        if not user_question:
            return jsonify({
                "ok": False,
                "error": "Please enter a question."
            }), 400

        history = _get_chat_history()
        company_id = session.get("company_id")

        material_block, calc_result = _build_material_block(user_question)

        if calc_result:
            answer = _safe_text(calc_result, "I could not calculate that.")
            used_business_insights = False
            used_bookkeeping_outlook = False
        else:
            augmented_question = _build_augmented_question(
                user_question=user_question,
                company_id=company_id,
                page_title=page_title,
                route=route,
            )

            used_business_insights = bool(company_id and should_include_business_insights(user_question))
            used_bookkeeping_outlook = bool(
                company_id and (_is_bookkeeping_page(page_title, route) or _is_outlook_question(user_question, page_title, route))
            )

            answer = ask_terraledger_help(
                user_question=augmented_question,
                page_name=page_title,
                route=route,
                user_role=_safe_text(session.get("role", "") or session.get("user_role", "")),
                company_name=_safe_text(session.get("company_name", "")),
                user_name=_safe_text(session.get("user_name", "")),
                prior_messages=history,
            )
            answer = _safe_text(answer, "I could not generate an answer right now.")

        history.append({"role": "user", "content": user_question})
        history.append({"role": "assistant", "content": answer})
        _save_chat_history(history)

        return jsonify({
            "ok": True,
            "answer": answer,
            "used_business_insights": used_business_insights,
            "used_bookkeeping_outlook": used_bookkeeping_outlook
        })

    except Exception as e:
        print("HELP ASSISTANT ERROR:", repr(e))
        return jsonify({
            "ok": False,
            "error": "Something went wrong while generating help."
        }), 500
    
@help_assistant_bp.route("/api/help-assistant/clear", methods=["POST"])
@csrf.exempt
@login_required
def help_assistant_clear():
    session["help_assistant_history"] = []
    session.modified = True
    return jsonify({"ok": True})


@help_assistant_bp.route("/api/help-assistant/history", methods=["GET"])
@csrf.exempt
@login_required
def help_assistant_history():
    return jsonify({
        "ok": True,
        "history": _get_chat_history()
    })