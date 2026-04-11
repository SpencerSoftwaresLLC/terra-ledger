# TerraLedger/routes/help_assistant.py

from flask import Blueprint, request, jsonify, session
from decorators import login_required
from ai.client import ask_terraledger_help
from ai.knowledge import calculate_material, get_help_knowledge
from ai.context_builder import should_include_business_insights
from ai.insights import format_sales_snapshot_for_ai
from extensions import csrf

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
    material_block, _ = _build_material_block(user_question)

    context_parts = [
        "You are the built-in TerraLedger help assistant.",
        "Answer using the current TerraLedger feature set and current app logic.",
        "Be direct, practical, and accurate.",
        "Do not invent features that do not exist.",
        "If a feature is not built yet, say so clearly.",
        "If the user is asking how to do something, give step-by-step instructions.",
        "If the user is asking about sales or forecasting, use the business insight context when available and clearly describe projections as estimates.",
        knowledge_block,
        _recent_terraledger_context(),
    ]

    if business_block:
        context_parts.append(business_block)

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
        else:
            augmented_question = _build_augmented_question(
                user_question=user_question,
                company_id=company_id,
                page_title=page_title,
                route=route,
            )

            used_business_insights = bool(company_id and should_include_business_insights(user_question))

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
            "used_business_insights": used_business_insights
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