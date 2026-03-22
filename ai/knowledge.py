# TerraLedger/ai/knowledge.py

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

import re

DEPTH_MAP = {
    1: 0.08,
    2: 0.17,
    3: 0.25,
    4: 0.33,
    5: 0.42,
    6: 0.50
}

STONE_KEYWORDS = ["stone", "gravel", "limestone", "rock", "crushed"]
YARD_ONLY_KEYWORDS = ["mulch", "soil", "dirt", "topsoil"]


def extract_dimensions(text):
    text = text.lower()

    # Find all numbers
    nums = list(map(float, re.findall(r"\d+\.?\d*", text)))

    if len(nums) < 2:
        return None, None, None

    L = nums[0]
    W = nums[1]

    # Try to detect depth specifically
    depth_match = re.search(r"(\d+\.?\d*)\s*(inches|inch|in|ft|feet|foot)?", text)

    if len(nums) >= 3:
        D_input = nums[2]
    else:
        return None, None, None

    return L, W, D_input


def detect_material(text):
    text = text.lower()
    if any(k in text for k in STONE_KEYWORDS):
        return "stone"
    if any(k in text for k in YARD_ONLY_KEYWORDS):
        return "yard"
    return "unknown"


def convert_depth(depth_input, text):
    text = text.lower()

    # If inches mentioned → use your depth map
    if "inch" in text or "in" in text:
        try:
            depth_input = int(depth_input)
            return DEPTH_MAP.get(depth_input, depth_input)
        except:
            return depth_input

    # If feet mentioned → use as-is
    if "ft" in text or "feet" in text or "foot" in text:
        return float(depth_input)

    # Default → assume inches (your business standard)
    try:
        depth_input = int(depth_input)
        return DEPTH_MAP.get(depth_input, depth_input)
    except:
        return float(depth_input)


def calculate_material(text):
    L, W, D_input = extract_dimensions(text)

    if not L:
        return None

    material = detect_material(text)
    D = convert_depth(D_input, text)

    yards = (L * W * D) / 27

    if material == "stone":
        tons = yards * 1.3

        return f"""
Material Estimate:

Length: {L} ft
Width: {W} ft
Depth: {D_input} inches

Cubic Yards: {yards:.2f}
Tons: {tons:.2f}

Recommended Order:
• {round(yards + 0.25, 2)} yards
• {round(tons + 0.25, 2)} tons
"""

    elif material == "yard":
        return f"""
Material Estimate:

Length: {L} ft
Width: {W} ft
Depth: {D_input} inches

Cubic Yards: {yards:.2f}

Recommended Order:
• {round(yards + 0.25, 2)} yards
"""

    return None