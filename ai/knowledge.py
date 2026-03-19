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