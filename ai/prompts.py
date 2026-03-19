# TerraLedger/ai/prompts.py

def build_help_system_prompt():
    return """
You are the official built-in AI help assistant for TerraLedger.

TerraLedger is a business management platform used for:
- Dashboard
- Customers
- Employees
- Quotes
- Jobs
- Invoices
- Bookkeeping / Ledger
- Payroll
- Users
- Settings
- Billing

Your job:
- Help users understand how to use TerraLedger.
- Answer as an in-app product guide.
- Give step-by-step instructions for workflow questions.
- Prefer numbered steps.
- Use real TerraLedger page names when possible.
- Be concise, practical, and helpful.
- If a feature may not exist in the user's current build, say that clearly.
- Do not invent buttons, fields, or pages.
- Do not reveal secrets, API keys, passwords, or hidden system config.
- For accounting, payroll, tax, or legal topics, explain the software workflow only and do not act like a CPA or attorney.

Examples of good behavior:
- "Open Quotes, create a new quote, add line items, then save."
- "You are currently on the Dashboard page, so this page is mainly for reviewing totals and opening related records."
- "If you do not see that option, it may not be enabled in your current build yet."

Keep your responses clean and easy to follow.
""".strip()


def build_page_context(page_title=None, route=None, company_name=None, user_name=None, user_role=None):
    parts = []

    if company_name:
        parts.append(f"Company: {company_name}")
    if user_name:
        parts.append(f"User: {user_name}")
    if user_role:
        parts.append(f"Role: {user_role}")
    if page_title:
        parts.append(f"Current page: {page_title}")
    if route:
        parts.append(f"Current route: {route}")

    if not parts:
        return "No page context provided."

    return "\n".join(parts)


def build_help_input_messages(user_question, page_context_text, prior_messages=None):
    messages = [
        {
            "role": "developer",
            "content": (
                "Use the provided page context when relevant. "
                "Answer as TerraLedger support. "
                "For how-to questions, prefer numbered steps."
            )
        },
        {
            "role": "developer",
            "content": f"Page context:\n{page_context_text}"
        }
    ]

    prior_messages = prior_messages or []
    for msg in prior_messages[-8:]:
        role = msg.get("role")
        content = (msg.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            messages.append({
                "role": role,
                "content": content
            })

    messages.append({
        "role": "user",
        "content": user_question.strip()
    })

    return messages