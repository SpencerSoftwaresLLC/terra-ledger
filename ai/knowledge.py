# TerraLedger/ai/knowledge.py

import re

TERRALEDGER_HELP_KNOWLEDGE = {
    "app_name": "TerraLedger",
    "summary": (
        "TerraLedger is a business management platform for landscaping, hauling, "
        "material supply, field service, and contractor businesses. It connects "
        "customers, quotes, jobs, calendar scheduling, invoices, payments, payroll, "
        "time tracking, messaging, and bookkeeping into one system."
    ),

    "assistant_identity": {
        "role": "TerraLedger Assistant",
        "purpose": (
            "Help users understand how TerraLedger works, how to complete tasks in the app, "
            "and how connected business data flows between modules."
        ),
        "style": [
            "Be direct, practical, and clear.",
            "Use step-by-step instructions when the user is asking how to do something.",
            "Answer as an in-app business software assistant, not as a generic chatbot.",
            "Prefer short, actionable guidance over long explanations.",
            "Do not invent screens, buttons, reports, or automations that do not exist.",
            "If a feature is partially built, say what works and what still needs manual handling.",
        ],
    },

    "core_workflow": [
        "1. Create Customer",
        "2. Create Quote (optional)",
        "3. Convert Quote → Job",
        "4. Schedule and manage the Job",
        "5. Track job items such as materials, labor, and service costs",
        "6. Convert Job → Invoice",
        "7. Send Invoice by email",
        "8. Record Payment or collect online payment through Stripe",
        "9. Automatically reflect invoice and job activity in bookkeeping where supported",
    ],

    "workflow_notes": [
        "Quotes are optional. A user can create a job directly without starting from a quote.",
        "Jobs are the operational center of the system and often lead into invoicing and calendar scheduling.",
        "Invoices can be created manually or from jobs.",
        "Payments reduce invoice balance and affect invoice status.",
        "Recurring schedules can generate jobs in advance based on schedule settings.",
        "Time clock and payroll are connected but still depend on company setup and pay period rules.",
    ],

    "modules": {
        "dashboard": {
            "summary": "Main business overview with financial and operational information.",
            "common_tasks": [
                "View high-level income, expenses, and profit",
                "See unpaid invoices",
                "Review upcoming or active jobs",
                "Watch business performance at a glance",
            ],
            "notes": [
                "The dashboard is meant to summarize the business, not replace deeper module pages.",
            ],
        },

        "calendar": {
            "summary": "Schedule and visualize jobs by date and crew.",
            "common_tasks": [
                "View scheduled jobs",
                "See jobs in day, week, or month-style layouts depending on current implementation",
                "Assign jobs to employees or crews",
                "See time blocks and possible overlaps",
                "Manage recurring or mowing-related scheduled work",
            ],
            "notes": [
                "Calendar views are tied to job scheduling data.",
                "Jobs with assigned dates and times are what appear on the calendar.",
                "Overlapping work matters when multiple crews or assigned resources are used.",
            ],
        },

        "customers": {
            "summary": "Manage customer records and contact information.",
            "common_tasks": [
                "Add customer",
                "Edit customer contact info",
                "Store customer email and company name",
                "Use customers in quotes, jobs, and invoices",
                "Review customer-linked history",
            ],
            "notes": [
                "Customers are foundational because quotes, jobs, messages, and invoices usually point back to a customer.",
            ],
        },

        "quotes": {
            "summary": "Create estimates before work begins.",
            "common_tasks": [
                "Create quote",
                "Add quote line items",
                "Set quantities, units, and pricing",
                "Convert quote to job",
                "Convert quote to invoice where supported by workflow",
            ],
            "steps": {
                "create quote": [
                    "Go to Quotes.",
                    "Click New Quote.",
                    "Select a customer.",
                    "Add line items, pricing, and details.",
                    "Save the quote.",
                ],
                "convert quote to job": [
                    "Open the quote.",
                    "Use the convert action if available.",
                    "Review the new job details.",
                    "Save the job and schedule it if needed.",
                ],
            },
            "notes": [
                "Quotes are for estimated work before the job is actually performed.",
                "A converted quote should not be treated like a separate completed sale until a job or invoice is created.",
            ],
        },

        "jobs": {
            "summary": "Track actual work being performed and operational job details.",
            "common_tasks": [
                "Create a job directly or from a quote",
                "Add materials, labor, and service items",
                "Assign job date and time",
                "Schedule work on the calendar",
                "Track profitability-related information",
                "Convert jobs into invoices",
            ],
            "notes": [
                "Jobs often drive both revenue tracking and cost tracking.",
                "Job items can include service lines, materials, labor, and other cost categories depending on setup.",
                "Recurring schedules can create jobs ahead of time automatically.",
                "A job being invoiced does not always mean it is fully paid.",
            ],
        },

        "recurring_jobs": {
            "summary": "Recurring schedules automate repeated future job creation.",
            "common_tasks": [
                "Create recurring mowing or repeating schedules",
                "Set frequency such as weekly or biweekly where supported",
                "Generate jobs ahead of time",
                "Pause or resume a recurring schedule",
                "Edit future generation behavior",
            ],
            "notes": [
                "Recurring schedules generate future jobs, not just reminders.",
                "Generated jobs should still be reviewed on the calendar and can still flow into invoicing.",
                "The app may generate future jobs using a horizon such as several weeks or months depending on current settings.",
                "Recurring logic should avoid duplicate job creation for the same scheduled date.",
            ],
        },

        "invoices": {
            "summary": "Bill customers and track outstanding balances and payments.",
            "common_tasks": [
                "Create invoice manually",
                "Convert job to invoice",
                "Record full or partial payment",
                "Track amount paid and balance due",
                "Send invoice by email",
                "Generate invoice PDF",
                "Allow online payment through Stripe",
            ],
            "steps": {
                "create invoice": [
                    "Go to Invoices.",
                    "Click New Invoice.",
                    "Select a customer.",
                    "Enter invoice date, due date, total, and service description or items.",
                    "Save the invoice.",
                ],
                "record partial payment": [
                    "Open the invoice.",
                    "Click Add Payment.",
                    "Enter payment date, amount, and method.",
                    "Save the payment to update the balance.",
                ],
                "email invoice": [
                    "Open the invoice.",
                    "Use Email Preview if you want to review the email first.",
                    "Click Email Invoice.",
                    "The customer receives the invoice PDF and payment link if online payments are enabled.",
                ],
            },
            "notes": [
                "Invoices track total, amount paid, and balance due.",
                "Invoice status should reflect whether the invoice is unpaid, partial, or paid.",
                "Partial payments reduce the balance without closing the invoice.",
                "Paid invoices should remain visible in paid invoice history.",
                "Some invoices can be summary-style instead of detailed line-item style.",
            ],
        },

        "invoice_payments": {
            "summary": "Track payments applied to invoices.",
            "common_tasks": [
                "Add payment",
                "Edit payment",
                "Delete payment",
                "Mark invoice paid or unpaid",
                "Review payment history by invoice",
            ],
            "notes": [
                "Payments are what reduce balance due.",
                "Deleting payments should increase balance due again.",
                "Invoice payment records should stay consistent with invoice status and bookkeeping behavior.",
            ],
        },

        "payments": {
            "summary": "Online invoice payments powered by Stripe.",
            "common_tasks": [
                "Connect Stripe account",
                "Enable invoice payments",
                "Collect customer card payments",
                "Allow companies to receive payouts to their own bank account",
            ],
            "notes": [
                "Stripe handles payment processing.",
                "Connected accounts are used so payments go to the business, not to TerraLedger holding funds.",
                "Invoice emails can include a secure payment link.",
                "Platform fees may be collected through Stripe Connect when configured.",
            ],
        },

        "stripe_connect": {
            "summary": "Connected Stripe account setup for receiving invoice payments.",
            "common_tasks": [
                "Connect business Stripe account",
                "Enable connected payouts",
                "Collect online payments from invoice links",
                "Allow platform fee collection where configured",
            ],
            "notes": [
                "The business needs a valid connected Stripe account before online invoice payment links will work.",
                "If payments are disabled or the Stripe account is missing, payment links should not be created.",
            ],
        },

        "messages": {
            "summary": "Send SMS-style customer updates and reminders.",
            "common_tasks": [
                "Send manual messages",
                "Send job updates",
                "Send invoice reminders",
                "Use saved message templates",
            ],
            "notes": [
                "Messaging may depend on platform configuration and provider setup.",
                "Automated messaging only works if the messaging side is fully configured and approved where required.",
                "Templates help standardize common job or invoice communications.",
            ],
        },

        "time_clock": {
            "summary": "Track employee clock in and clock out activity based on the company pay period.",
            "common_tasks": [
                "Clock employees in",
                "Clock employees out",
                "View current employee status",
                "View hours for today",
                "View hours for the current pay period",
                "Review current or previous pay period entries",
                "Send pay period summary emails",
            ],
            "notes": [
                "Time clock uses the company pay period start day.",
                "Current pay period hours and previous pay period summaries depend on that configured pay period.",
                "Time entries should be grouped by pay period to keep the page clean and useful.",
            ],
        },

        "payroll": {
            "summary": "Track employee pay, deductions, taxes, and payroll records.",
            "common_tasks": [
                "Create payroll entries",
                "Preview payroll taxes",
                "Use time clock hours for hourly payroll",
                "Handle salary and hourly employees",
                "Track deductions and net pay",
                "Print payroll checks",
            ],
            "notes": [
                "Payroll uses pay type, pay frequency, pay rates, and tax settings.",
                "Hourly payroll can pull hours from time clock data.",
                "Salary payroll uses pay frequency instead of hourly time calculations.",
                "Payroll checks are separate from invoice payments.",
            ],
        },

        "payroll_checks": {
            "summary": "Printable payroll checks with check stubs and payment record sections.",
            "common_tasks": [
                "Print payroll checks",
                "Track payroll check number",
                "Store payroll payment method",
                "Keep a stub for employee and company recordkeeping",
            ],
            "notes": [
                "Pre-printed check stock should not duplicate company info or check number if already on the stock.",
                "A payroll check can include employee and company stub copies.",
            ],
        },

        "employees": {
            "summary": "Manage employee details, compensation, and tax-related setup.",
            "common_tasks": [
                "Add employee",
                "Edit employee contact info",
                "Set pay type and pay frequency",
                "Set hourly or salary data",
                "Store filing and tax settings",
            ],
            "notes": [
                "Employee setup affects payroll previews, withholding, and time clock reporting.",
            ],
        },

        "ledger": {
            "summary": "Bookkeeping and financial records tied to business activity.",
            "common_tasks": [
                "Track income from invoice payments",
                "Track job-related costs and expenses",
                "Review financial records",
                "Support profit and loss reporting",
            ],
            "notes": [
                "Invoice and job activity can feed bookkeeping automatically where mapped.",
                "Ledger data should stay consistent with operational records.",
            ],
        },

        "bookkeeping": {
            "summary": "View and manage business financial activity such as income, expenses, and reports.",
            "common_tasks": [
                "Review ledger entries",
                "View profit and loss",
                "Track income and expense categories",
                "Use financial reports for business performance",
            ],
            "notes": [
                "Bookkeeping should reflect invoice revenue, job costs, payroll, and manual entries where supported.",
                "Profit and loss quality depends on category mapping and correct source data.",
            ],
        },

        "reports": {
            "summary": "Business reporting such as sales, expenses, profit, and year-end records.",
            "common_tasks": [
                "Review business performance",
                "Compare totals over time",
                "Use year-end reporting",
                "Prepare for accounting review",
            ],
            "notes": [
                "Some reporting may be summary-based and some may still be evolving.",
            ],
        },

        "w2_and_year_end": {
            "summary": "Year-end payroll reporting and W-2 support.",
            "common_tasks": [
                "Review payroll totals",
                "Print year-end summaries",
                "Prepare W-2 related reporting",
            ],
            "notes": [
                "Year-end reporting depends on payroll data being entered correctly during the year.",
            ],
        },

        "backups": {
            "summary": "Backup and restore company data.",
            "common_tasks": [
                "Create backup",
                "Download backup",
                "Restore company data from backup",
            ],
            "notes": [
                "Backup and restore should be used carefully because restore can overwrite current company data.",
            ],
        },

        "users": {
            "summary": "Manage users, access, and permissions.",
            "common_tasks": [
                "Create user",
                "Control module permissions",
                "Restrict access to sensitive modules",
            ],
            "notes": [
                "Permissions affect whether a user can access jobs, invoices, payroll, and other modules.",
            ],
        },

        "settings": {
            "summary": "Configure company-level defaults and system behavior.",
            "common_tasks": [
                "Update company info",
                "Set branding",
                "Configure taxes",
                "Manage payment settings",
                "Manage messaging or email defaults",
                "Configure time clock pay period settings",
            ],
            "notes": [
                "Settings influence how multiple modules behave throughout the app.",
            ],
        },

        "ai_help": {
            "summary": "In-app assistant for TerraLedger questions and certain calculations.",
            "common_tasks": [
                "Explain how to use modules",
                "Answer workflow questions",
                "Help with material calculations",
                "Clarify what the system does automatically",
            ],
            "notes": [
                "The AI help should explain existing features clearly.",
                "It should not guess that features exist when they do not.",
                "It can support business insights only when connected to real business data or forecasting logic.",
            ],
        },
    },

    "cross_module_logic": [
        "Customers can be used across quotes, jobs, invoices, and messages.",
        "Quotes are estimated work and can convert into jobs.",
        "Jobs represent actual work and often lead into invoices.",
        "Invoices track billed revenue and payment status.",
        "Invoice payments reduce balance due and help determine invoice status.",
        "Online invoice payments rely on Stripe being connected and enabled.",
        "Time clock tracks employee hours by pay period.",
        "Payroll uses employee pay setup and may use time clock hours for hourly workers.",
        "Bookkeeping should reflect operational activity such as invoice revenue and job-related costs.",
    ],

    "important_rules": [
        "Jobs drive operational work and often drive revenue and costs.",
        "Invoices represent customer billing.",
        "Payments reduce invoice balance.",
        "Paid invoices should not still behave like unpaid invoices.",
        "Partial payments should not mark an invoice fully paid.",
        "Stripe handles online payment processing.",
        "Connected Stripe accounts are needed for businesses to receive online payments.",
        "Recurring schedules should generate future jobs without creating duplicates.",
        "Time clock pay periods are based on the company-configured start day.",
        "Payroll and time clock are related but not identical.",
        "Messaging is optional and may depend on provider setup.",
        "The assistant must not claim that unfinished features are fully complete.",
    ],

    "known_patterns": {
        "status_logic": {
            "invoice_statuses": [
                "Draft",
                "Unpaid",
                "Partial",
                "Paid",
            ],
            "job_statuses": [
                "Scheduled",
                "Invoiced",
                "Finished",
                "Completed",
            ],
            "notes": [
                "Status wording may vary by module and workflow.",
                "Invoice status should follow payments and remaining balance.",
            ],
        },
        "payment_logic": [
            "If total is greater than zero and amount paid is zero, invoice is generally unpaid.",
            "If amount paid is greater than zero and balance due is greater than zero, invoice is generally partial.",
            "If balance due is zero and total is greater than zero, invoice is generally paid.",
        ],
        "time_clock_logic": [
            "Current pay period hours should reflect entries between current pay period start and end dates.",
            "Previous pay period summaries should use the previous pay period date range.",
            "Recent entry display should avoid clutter by focusing on the selected pay period window.",
        ],
    },

    "response_rules": [
        "Answer as the TerraLedger assistant.",
        "Use step-by-step instructions when explaining how to do something.",
        "Be direct and practical.",
        "Do not invent features that do not exist.",
        "If something is not built yet, say so clearly.",
        "If the user asks about a workflow, explain how the modules connect.",
        "If the user asks why something happened, prefer a system-logic explanation over a vague answer.",
        "If the user asks for forecasting or sales prediction, explain that historical business data is needed.",
    ],

    "supported_help_topics": [
        "How to create and manage customers",
        "How to create quotes, jobs, and invoices",
        "How to record and track payments",
        "How Stripe payments work in TerraLedger",
        "How recurring jobs and schedules work",
        "How calendar scheduling relates to jobs",
        "How time clock pay periods work",
        "How payroll entries and payroll checks work",
        "How bookkeeping connects to operational activity",
        "How messaging and reminders are intended to work",
        "How app settings affect module behavior",
        "How material calculations are estimated",
    ],

    "unsupported_or_limited_topics": [
        "Do not guarantee legal, tax, or accounting advice.",
        "Do not promise exact financial forecasts unless real business data and forecasting logic are available.",
        "Do not pretend a missing automation already works if it is still under development.",
        "Do not claim the assistant can see live company data unless a connected insight layer provides it.",
    ],
}

DEPTH_MAP = {
    1: 0.08,
    2: 0.17,
    3: 0.25,
    4: 0.33,
    5: 0.42,
    6: 0.50,
}

STONE_KEYWORDS = ["stone", "gravel", "limestone", "rock", "crushed"]
YARD_ONLY_KEYWORDS = ["mulch", "soil", "dirt", "topsoil"]


def _normalize_text(text):
    return (text or "").strip().lower()


def detect_material(text):
    text = _normalize_text(text)

    if any(keyword in text for keyword in STONE_KEYWORDS):
        return "stone"

    if any(keyword in text for keyword in YARD_ONLY_KEYWORDS):
        return "yard"

    return "unknown"


def extract_depth(text):
    text = _normalize_text(text)

    depth_patterns = [
        r"(\d+\.?\d*)\s*(inches|inch|in|feet|foot|ft)\s*deep",
        r"depth\s*(?:of)?\s*(\d+\.?\d*)\s*(inches|inch|in|feet|foot|ft)?",
        r"at\s*(\d+\.?\d*)\s*(inches|inch|in|feet|foot|ft)\s*deep",
        r"(\d+\.?\d*)\s*(inches|inch|in|feet|foot|ft)",
    ]

    for pattern in depth_patterns:
        match = re.search(pattern, text)
        if match:
            depth_value = float(match.group(1))
            depth_unit = match.group(2).lower() if match.group(2) else None
            return depth_value, depth_unit

    nums = list(map(float, re.findall(r"\d+\.?\d*", text)))
    if len(nums) >= 3:
        return nums[2], None

    return None, None


def convert_depth_to_feet(depth_input, depth_unit=None):
    if depth_input is None:
        return None

    if depth_unit in {"feet", "foot", "ft"}:
        return float(depth_input)

    if depth_unit in {"inches", "inch", "in"}:
        try:
            depth_as_int = int(float(depth_input))
            return DEPTH_MAP.get(depth_as_int, float(depth_input) / 12.0)
        except Exception:
            return float(depth_input) / 12.0

    try:
        depth_as_int = int(float(depth_input))
        return DEPTH_MAP.get(depth_as_int, float(depth_input) / 12.0)
    except Exception:
        return float(depth_input)


def extract_square_feet(text):
    text = _normalize_text(text)

    patterns = [
        r"(\d+\.?\d*)\s*(?:square feet|sq feet|sq foot|square foot|sq ft|ft²|sf)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return float(match.group(1))

    return None


def extract_length_width(text):
    text = _normalize_text(text)

    match = re.search(r"(\d+\.?\d*)\s*(?:x|by)\s*(\d+\.?\d*)", text)
    if match:
        return float(match.group(1)), float(match.group(2))

    if extract_square_feet(text) is None:
        nums = list(map(float, re.findall(r"\d+\.?\d*", text)))
        if len(nums) >= 3:
            return nums[0], nums[1]

    return None, None


def calculate_material(text):
    text = _normalize_text(text)

    if not text:
        return None

    material = detect_material(text)
    if material == "unknown":
        return None

    depth_input, depth_unit = extract_depth(text)
    if depth_input is None:
        return None

    depth_feet = convert_depth_to_feet(depth_input, depth_unit)
    if depth_feet is None:
        return None

    square_feet = extract_square_feet(text)
    length, width = extract_length_width(text)

    if square_feet is None:
        if length is None or width is None:
            return None
        square_feet = length * width

    cubic_feet = square_feet * depth_feet
    yards = cubic_feet / 27.0

    depth_label = f"{depth_input:g} {depth_unit}" if depth_unit else f"{depth_input:g} inches"

    if material == "stone":
        tons = yards * 1.3
        return (
            "Material Estimate:\n\n"
            f"Area: {square_feet:.2f} sq ft\n"
            f"Depth: {depth_label}\n"
            f"Depth in Feet: {depth_feet:.3f}\n"
            f"Cubic Feet: {cubic_feet:.2f}\n"
            f"Cubic Yards: {yards:.2f}\n"
            f"Tons: {tons:.2f}\n\n"
            "Breakdown:\n"
            f"• {square_feet:.2f} × {depth_feet:.3f} = {cubic_feet:.2f} cubic feet\n"
            f"• {cubic_feet:.2f} ÷ 27 = {yards:.2f} cubic yards\n"
            f"• {yards:.2f} × 1.3 = {tons:.2f} tons\n\n"
            "Recommended Order:\n"
            f"• {round(yards + 0.25, 2)} yards\n"
            f"• {round(tons + 0.25, 2)} tons"
        )

    if material == "yard":
        return (
            "Material Estimate:\n\n"
            f"Area: {square_feet:.2f} sq ft\n"
            f"Depth: {depth_label}\n"
            f"Depth in Feet: {depth_feet:.3f}\n"
            f"Cubic Feet: {cubic_feet:.2f}\n"
            f"Cubic Yards: {yards:.2f}\n\n"
            "Breakdown:\n"
            f"• {square_feet:.2f} × {depth_feet:.3f} = {cubic_feet:.2f} cubic feet\n"
            f"• {cubic_feet:.2f} ÷ 27 = {yards:.2f} cubic yards\n\n"
            "Recommended Order:\n"
            f"• {round(yards + 0.25, 2)} yards"
        )

    return None


def get_help_knowledge():
    return TERRALEDGER_HELP_KNOWLEDGE