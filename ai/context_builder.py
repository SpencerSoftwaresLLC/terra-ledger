# TerraLedger/ai/context_builder.py

from ai.knowledge import get_help_knowledge, calculate_material
from ai.insights import format_sales_snapshot_for_ai


INSIGHT_KEYWORDS = [
    "sales",
    "revenue",
    "forecast",
    "project",
    "projection",
    "projected",
    "year end",
    "year-end",
    "ytd",
    "month over month",
    "month-over-month",
    "year over year",
    "year-over-year",
    "growth",
    "pace",
    "trend",
    "trends",
    "estimate this year",
    "where will sales end up",
    "how are we doing",
    "how is business doing",
    "business performance",
    "income trend",
]


def _normalize_text(text):
    return (text or "").strip().lower()


def should_include_business_insights(user_message):
    text = _normalize_text(user_message)
    if not text:
        return False

    return any(keyword in text for keyword in INSIGHT_KEYWORDS)


def _format_list(lines):
    if not lines:
        return ""
    return "\n".join(f"- {line}" for line in lines)


def _build_product_knowledge_block():
    knowledge = get_help_knowledge()

    app_name = knowledge.get("app_name", "TerraLedger")
    summary = knowledge.get("summary", "")
    assistant_identity = knowledge.get("assistant_identity", {})
    core_workflow = knowledge.get("core_workflow", [])
    workflow_notes = knowledge.get("workflow_notes", [])
    important_rules = knowledge.get("important_rules", [])
    response_rules = knowledge.get("response_rules", [])
    modules = knowledge.get("modules", {})

    module_sections = []
    for module_name, module_data in modules.items():
        module_summary = module_data.get("summary", "")
        common_tasks = module_data.get("common_tasks", [])
        notes = module_data.get("notes", [])
        steps = module_data.get("steps", {})

        steps_text = []
        for step_name, step_lines in steps.items():
            steps_text.append(f"{step_name.title()}:\n" + "\n".join(f"  - {line}" for line in step_lines))

        module_section = (
            f"{module_name.upper()}\n"
            f"Summary: {module_summary}\n"
            f"Common Tasks:\n{_format_list(common_tasks) or '- None listed'}\n"
            f"Notes:\n{_format_list(notes) or '- None listed'}"
        )

        if steps_text:
            module_section += "\nSteps:\n" + "\n".join(steps_text)

        module_sections.append(module_section)

    assistant_style = assistant_identity.get("style", [])

    return f"""
APP NAME:
{app_name}

APP SUMMARY:
{summary}

ASSISTANT ROLE:
{assistant_identity.get("role", "TerraLedger Assistant")}

ASSISTANT PURPOSE:
{assistant_identity.get("purpose", "")}

ASSISTANT STYLE:
{_format_list(assistant_style)}

CORE WORKFLOW:
{_format_list(core_workflow)}

WORKFLOW NOTES:
{_format_list(workflow_notes)}

MODULE KNOWLEDGE:
{chr(10).join(module_sections)}

IMPORTANT RULES:
{_format_list(important_rules)}

RESPONSE RULES:
{_format_list(response_rules)}
""".strip()


def build_ai_system_prompt(company_id=None, user_message=None):
    user_message = (user_message or "").strip()

    product_knowledge = _build_product_knowledge_block()

    material_result = calculate_material(user_message)
    material_context = ""
    if material_result:
        material_context = f"""

MATERIAL CALCULATION CONTEXT:
The user's message appears to ask for a material estimate.
Use this calculation result if it directly answers the user's request:

{material_result}
""".rstrip()

    business_context = ""
    include_insights = False

    if company_id and should_include_business_insights(user_message):
        try:
            insight_text = format_sales_snapshot_for_ai(company_id)
            business_context = f"""

BUSINESS INSIGHT CONTEXT:
Use the following real business context when answering questions about sales, revenue, growth, trends, YTD performance, or projections.

{insight_text}
""".rstrip()
            include_insights = True
        except Exception as e:
            business_context = f"""

BUSINESS INSIGHT CONTEXT:
Business insight data could not be loaded cleanly for this request.
Error: {e}
""".rstrip()

    return f"""
You are the TerraLedger Assistant.

Your job is to help the user use TerraLedger correctly and explain how the app works.
Be practical, accurate, and concise.
Do not invent features.
If something is not built, say so clearly.
If the user asks how to do something, give step-by-step instructions.
If the user asks about business performance, use the business context when available and clearly describe forecasts as estimates, not guarantees.

{product_knowledge}
{material_context}
{business_context}

FINAL INSTRUCTIONS:
- Answer as the TerraLedger Assistant.
- Prefer direct answers over long intros.
- Use app-specific wording.
- When discussing projections, say they are estimated based on historical paid revenue and current pace.
- If business insight context is not available, say that historical business data is needed for a more intelligent estimate.
- If the user's request is about material quantity, use the material calculation context when available.
""".strip(), include_insights