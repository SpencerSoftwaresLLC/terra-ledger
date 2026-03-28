# TerraLedger/ai/knowledge.py

import re

TERRALEDGER_HELP_KNOWLEDGE = {
    "app_name": "TerraLedger",
    "summary": (
        "TerraLedger is a business management platform for landscaping, hauling, "
        "material supply, and contractor businesses. It connects customers, quotes, "
        "jobs, invoices, payments, payroll, and bookkeeping into one system."
    ),

    "core_workflow": [
        "1. Create Customer",
        "2. Create Quote (optional)",
        "3. Convert Quote → Job",
        "4. Track Job (materials, labor, cost)",
        "5. Convert Job → Invoice",
        "6. Send Invoice",
        "7. Record or collect Payment",
        "8. Automatically track income/expenses in bookkeeping",
    ],

    "modules": {

        "dashboard": {
            "summary": "Main business overview with financial and operational data.",
            "common_tasks": [
                "View income, expenses, and profit",
                "See unpaid invoices",
                "View upcoming jobs",
                "Track business performance",
            ],
        },

        "calendar": {
            "summary": "Schedule and visualize jobs by day, week, or month.",
            "common_tasks": [
                "View scheduled jobs",
                "Assign jobs to employees or crews",
                "See job time blocks",
                "Manage overlapping jobs",
            ],
        },

        "customers": {
            "summary": "Manage customer records and contact details.",
            "common_tasks": [
                "Add customer",
                "Edit contact info",
                "Track job and invoice history",
            ],
        },

        "quotes": {
            "summary": "Create estimates for customers before work begins.",
            "common_tasks": [
                "Create quote",
                "Add line items",
                "Convert quote to invoice",
            ],
            "steps": {
                "create quote": [
                    "Go to Quotes.",
                    "Click 'New Quote'.",
                    "Select a customer.",
                    "Add line items and pricing.",
                    "Save the quote.",
                ],
            },
        },

        "jobs": {
            "summary": "Track actual work being performed.",
            "common_tasks": [
                "Create job from customer or quote",
                "Add materials, labor, and costs",
                "Schedule job on calendar",
                "Track profitability",
            ],
        },

        "invoices": {
            "summary": "Bill customers and track payments.",
            "common_tasks": [
                "Create invoice",
                "Convert job to invoice",
                "Record partial payments",
                "Track balance due",
                "Send invoice email",
            ],
            "steps": {
                "record partial payment": [
                    "Open the invoice.",
                    "Click 'Add Payment'.",
                    "Enter the amount paid.",
                    "Save to update balance.",
                ],
            },
        },

        "payments": {
            "summary": "Online payments powered by Stripe.",
            "common_tasks": [
                "Connect Stripe account",
                "Enable invoice payments",
                "Allow partial payments",
                "Receive payouts directly to bank",
            ],
            "notes": [
                "TerraLedger does not hold funds.",
                "Payments go directly to the business via Stripe.",
            ],
        },

        "messages": {
            "summary": "Send SMS updates to customers.",
            "common_tasks": [
                "Send manual messages",
                "Send job updates",
                "Send invoice reminders",
                "Use message templates",
            ],
        },

        "ledger": {
            "summary": "Automatic bookkeeping system.",
            "common_tasks": [
                "Track income from invoices",
                "Track job costs (materials, labor)",
                "View profit",
            ],
            "notes": [
                "Job costs automatically create expense entries.",
                "Invoice payments automatically create income entries.",
            ],
        },

        "payroll": {
            "summary": "Track employee pay and taxes.",
            "common_tasks": [
                "Calculate gross pay",
                "Apply tax rates",
                "Track payroll expenses",
            ],
        },

        "employees": {
            "summary": "Manage employee details and pay settings.",
            "common_tasks": [
                "Add employee",
                "Set hourly or salary",
                "Store tax settings",
            ],
        },

        "users": {
            "summary": "Control access and permissions.",
            "common_tasks": [
                "Create users",
                "Set permissions",
                "Restrict access to modules",
            ],
        },

        "settings": {
            "summary": "Configure company settings.",
            "common_tasks": [
                "Update company info",
                "Set tax rates",
                "Configure branding",
                "Manage system defaults",
            ],
        },
    },

    "important_rules": [
        "Jobs drive revenue and costs.",
        "Invoices generate income.",
        "Payments reduce invoice balance.",
        "Ledger updates automatically from jobs and invoices.",
        "Stripe handles all online payments.",
        "Messaging is optional and can be turned on/off.",
    ],

    "response_rules": [
        "Answer as the TerraLedger assistant.",
        "Use step-by-step instructions when explaining how to do something.",
        "Be direct and practical.",
        "Do not invent features that do not exist.",
        "If something is not built yet, say so clearly.",
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

    # Prefer a depth reference near "deep" or "depth"
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

    # Fallback: third number, assume inches if no explicit unit
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

    # Default business rule: assume inches when omitted
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

    # 30x20 or 30 x 20 or 30 by 20
    match = re.search(r"(\d+\.?\d*)\s*(?:x|by)\s*(\d+\.?\d*)", text)
    if match:
        return float(match.group(1)), float(match.group(2))

    # Fallback: if at least 3 numbers and no square-feet phrase, assume first two are L/W
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