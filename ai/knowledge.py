# TerraLedger/ai/knowledge.py

import re

TERRALEDGER_HELP_KNOWLEDGE = {
    "app_name": "TerraLedger",
    "summary": (
        "TerraLedger is a business management platform for yard, hauling, "
        "landscaping, materials, service, and contractor-style businesses."
    ),
    "modules": {
        "dashboard": {
            "summary": "Main overview of business activity.",
            "common_tasks": [
                "View quick stats",
                "Review totals",
                "Jump to other modules",
            ],
        },
        "customers": {
            "summary": "Create and manage customer records.",
            "common_tasks": [
                "Add a customer",
                "Edit customer details",
                "View customer history",
            ],
        },
        "jobs": {
            "summary": "Create and manage jobs for customers.",
            "common_tasks": [
                "Create a job",
                "Add job details",
                "Track job-related activity",
                "Convert job information into invoice-ready work",
            ],
        },
        "quotes": {
            "summary": "Create and manage quotes for customers.",
            "common_tasks": [
                "Create a quote",
                "Add line items",
                "Save a quote",
                "Email a quote PDF",
                "Convert a quote into an invoice",
            ],
            "steps": {
                "create quote": [
                    "Open Quotes from the navigation.",
                    "Click the button to create a new quote.",
                    "Select the customer.",
                    "Add quote line items, quantities, prices, and notes as needed.",
                    "Save the quote.",
                ],
                "convert quote to invoice": [
                    "Open the saved quote.",
                    "Look for the option to convert the quote into an invoice.",
                    "Review the line items and totals.",
                    "Save the new invoice.",
                ],
            },
        },
        "invoices": {
            "summary": "Create, manage, send, and track invoices.",
            "common_tasks": [
                "Create invoice",
                "Edit invoice",
                "Mark invoice paid",
                "Record partial payment",
                "Email invoice PDF",
            ],
            "steps": {
                "create invoice": [
                    "Open Invoices from the navigation.",
                    "Click the button to create a new invoice.",
                    "Select the customer or related job.",
                    "Add invoice items, quantities, pricing, and tax if needed.",
                    "Save the invoice.",
                ],
                "record payment": [
                    "Open the invoice.",
                    "Use the payment action on the invoice screen.",
                    "Enter the payment amount and payment details.",
                    "Save the payment so the invoice balance updates.",
                ],
            },
        },
        "ledger": {
            "summary": "Track bookkeeping entries like income and expenses.",
            "common_tasks": [
                "Add income entry",
                "Add expense entry",
                "Review bookkeeping history",
            ],
        },
        "payroll": {
            "summary": "Track payroll entries, hours, rates, and deductions.",
            "common_tasks": [
                "Create payroll entry",
                "Review gross pay",
                "Review deductions",
                "Track payroll history",
            ],
        },
        "employees": {
            "summary": "Manage employee records and payroll-related details.",
            "common_tasks": [
                "Add employee",
                "Edit employee",
                "View employee details",
            ],
        },
        "users": {
            "summary": "Manage login users and permission levels.",
            "common_tasks": [
                "Create user",
                "Edit user",
                "Enable or disable user access",
                "Manage permissions",
            ],
        },
        "settings": {
            "summary": "Manage company settings, logos, billing, tax settings, and platform configuration.",
            "common_tasks": [
                "Update company info",
                "Upload logo",
                "Review billing",
                "Configure tax settings",
            ],
        },
    },
    "response_rules": [
        "Answer as the built-in TerraLedger assistant.",
        "Give step-by-step instructions whenever the user asks how to do something.",
        "Use numbered steps whenever possible.",
        "Use real page/module names when known.",
        "Do not invent buttons, features, or pages that do not exist.",
        "If a feature may not exist in this build, say so clearly.",
        "Be practical, direct, and helpful.",
        "For payroll, accounting, tax, and legal topics, explain software workflow but do not pretend to be a CPA or attorney.",
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