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

STONE_KEYWORDS = [
    "stone",
    "gravel",
    "limestone",
    "rock",
    "crushed",
]

YARD_ONLY_KEYWORDS = [
    "mulch",
    "soil",
    "dirt",
    "topsoil",
]


def _normalize_text(text):
    return (text or "").strip().lower()


def detect_material(text):
    text = _normalize_text(text)

    if any(keyword in text for keyword in STONE_KEYWORDS):
        return "stone"

    if any(keyword in text for keyword in YARD_ONLY_KEYWORDS):
        return "yard"

    return "unknown"


def extract_dimensions(text):
    """
    Expects the first two numbers to be length and width,
    and the third number to be depth.

    Examples that work:
    - 30x20 3 inches mulch
    - 30 x 20 x 3 stone
    - 30 by 20 at 3 inches deep for gravel
    """
    text = _normalize_text(text)
    nums = list(map(float, re.findall(r"\d+\.?\d*", text)))

    if len(nums) < 3:
        return None, None, None

    length = nums[0]
    width = nums[1]
    depth_input = nums[2]

    return length, width, depth_input


def convert_depth(depth_input, text):
    """
    Converts the user's depth entry into feet for the volume formula.

    Business rule:
    - If user says inches / inch / in, convert using DEPTH_MAP when possible
    - If user says feet / foot / ft, use the value directly
    - If no unit is given, assume inches and use DEPTH_MAP
    """
    text = _normalize_text(text)

    if re.search(r"\b(ft|feet|foot)\b", text):
        return float(depth_input)

    if re.search(r"\b(in|inch|inches)\b", text):
        try:
            depth_as_int = int(float(depth_input))
            return DEPTH_MAP.get(depth_as_int, float(depth_input))
        except Exception:
            return float(depth_input)

    try:
        depth_as_int = int(float(depth_input))
        return DEPTH_MAP.get(depth_as_int, float(depth_input))
    except Exception:
        return float(depth_input)


def calculate_material(text):
    """
    Material formulas based on your Wrede's process:

    Stone:
        yards = (L * W * D) / 27
        tons = yards * 1.3

    Mulch / soil:
        yards = (L * W * D) / 27

    Depth must be in feet for the formula, so inch inputs are converted first.
    """
    text = _normalize_text(text)

    if not text:
        return None

    material = detect_material(text)
    if material == "unknown":
        return None

    length, width, depth_input = extract_dimensions(text)
    if length is None or width is None or depth_input is None:
        return None

    depth_feet = convert_depth(depth_input, text)
    yards = (length * width * depth_feet) / 27

    if material == "stone":
        tons = yards * 1.3
        return (
            "Material Estimate:\n\n"
            f"Length: {length:g} ft\n"
            f"Width: {width:g} ft\n"
            f"Depth: {depth_input:g}\n\n"
            f"Cubic Yards: {yards:.2f}\n"
            f"Tons: {tons:.2f}\n\n"
            "Recommended Order:\n"
            f"• {round(yards + 0.25, 2)} yards\n"
            f"• {round(tons + 0.25, 2)} tons"
        )

    if material == "yard":
        return (
            "Material Estimate:\n\n"
            f"Length: {length:g} ft\n"
            f"Width: {width:g} ft\n"
            f"Depth: {depth_input:g}\n\n"
            f"Cubic Yards: {yards:.2f}\n\n"
            "Recommended Order:\n"
            f"• {round(yards + 0.25, 2)} yards"
        )

    return None