from datetime import datetime, date
from flask import Blueprint, request, session, render_template_string, make_response
import io
import csv
import re
import calendar

from db import get_db_connection
from decorators import login_required, subscription_required, require_permission
from page_helpers import render_page

material_usage_bp = Blueprint("material_usage", __name__, url_prefix="/reports")


ITEM_TYPE_LABELS = {
    "mulch": "Mulch",
    "stone": "Stone",
    "dump_fee": "Dump Fee",
    "plants": "Plants",
    "trees": "Trees",
    "soil": "Soil",
    "fertilizer": "Fertilizer",
    "hardscape_material": "Hardscape Material",
    "labor": "Labor",
    "equipment": "Equipment",
    "delivery": "Delivery",
    "fuel": "Fuel",
    "misc": "Misc",
    "material": "Material",
}


def clean_text_input(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"none", "null", "n/a"}:
        return ""
    return text


def safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except Exception:
        return default


def display_item_type(value):
    key = clean_text_input(value).lower()
    if key in ITEM_TYPE_LABELS:
        return ITEM_TYPE_LABELS[key]
    return key.replace("_", " ").title() if key else "Uncategorized"


def format_money(value):
    return f"${safe_float(value):,.2f}"


def format_qty(value):
    return f"{safe_float(value):,.2f}"


def percent_change(current, previous):
    current = safe_float(current)
    previous = safe_float(previous)

    if previous == 0:
        if current == 0:
            return 0.0
        return None

    return ((current - previous) / previous) * 100.0


def format_percent_change(current, previous):
    pct = percent_change(current, previous)
    if pct is None:
        return "—"
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.2f}%"


def parse_date(value):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception:
        return None


def format_date(value):
    if not value:
        return ""
    return value.strftime("%Y-%m-%d")


def shift_date_one_year_back(d):
    try:
        return d.replace(year=d.year - 1)
    except ValueError:
        if d.month == 2 and d.day == 29:
            return d.replace(year=d.year - 1, day=28)
        raise


def get_month_range(today):
    start = today.replace(day=1)
    end = today.replace(day=calendar.monthrange(today.year, today.month)[1])
    return start, end


def get_quarter_range(today):
    quarter = ((today.month - 1) // 3) + 1
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    start = date(today.year, start_month, 1)
    end = date(today.year, end_month, calendar.monthrange(today.year, end_month)[1])
    return start, end


def get_year_range(today):
    start = date(today.year, 1, 1)
    end = date(today.year, 12, 31)
    return start, end


def normalize_spaces(text):
    return re.sub(r"\s+", " ", clean_text_input(text)).strip()


def normalize_unit_key(unit):
    text = normalize_spaces(unit).lower()
    if not text:
        return ""

    replacements = {
        "yd": "yards",
        "yard": "yards",
        "yrds": "yards",
        "yards": "yards",
        "ton": "tons",
        "tons": "tons",
        "hr": "hours",
        "hrs": "hours",
        "hour": "hours",
        "hours": "hours",
        "ea": "ea",
        "each": "ea",
        "gal": "gallons",
        "gallon": "gallons",
        "gallons": "gallons",
        "bag": "bags",
        "bags": "bags",
        "mile": "miles",
        "miles": "miles",
        "rental": "rentals",
        "rentals": "rentals",
    }
    return replacements.get(text, text)


def display_unit(unit_key):
    if not unit_key:
        return ""
    return unit_key.title() if unit_key != "ea" else "EA"


def canonicalize_description(description, item_type):
    """
    Merge obvious aliases while keeping truly different products separate.
    Example:
    - blk mulch -> Black Mulch
    - black mulch -> Black Mulch
    - enhanced black mulch -> Enhanced Black Mulch
    """
    text = normalize_spaces(description).lower()
    item_type = clean_text_input(item_type).lower()

    if not text:
        return "Unnamed Item"

    text = text.replace('"', " in ")
    text = text.replace("'", "")
    text = re.sub(r"[_\-\/]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    replacements = {
        r"\bblk\b": "black",
        r"\bblck\b": "black",
        r"\bchoc\b": "chocolate",
        r"\bbrn\b": "brown",
        r"\benh\b": "enhanced",
        r"\bmulch\b": "mulch",
        r"\btop soil\b": "topsoil",
        r"\brr\b": "river rock",
        r"\blime stone\b": "limestone",
    }

    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)

    text = re.sub(r"\s+", " ", text).strip()

    if item_type == "mulch":
        is_enhanced = "enhanced" in text

        if "black" in text and "mulch" in text:
            return "Enhanced Black Mulch" if is_enhanced else "Black Mulch"

        if "chocolate" in text and "mulch" in text:
            return "Enhanced Chocolate Mulch" if is_enhanced else "Chocolate Mulch"

        if "brown" in text and "mulch" in text:
            return "Enhanced Brown Mulch" if is_enhanced else "Brown Mulch"

    if item_type == "stone":
        if "river rock" in text:
            return "River Rock"
        if "limestone" in text:
            return "Limestone"
        if "gravel" in text:
            return "Gravel"

    if item_type == "soil":
        if "topsoil" in text:
            return "Topsoil"
        if "fill dirt" in text:
            return "Fill Dirt"
        if "soil" in text and "top" not in text and "fill" not in text:
            return "Soil"

    return " ".join(word.capitalize() for word in text.split())


def get_requested_range():
    today = date.today()
    period = clean_text_input(request.args.get("period", "year")).lower()

    if period == "month":
        current_start, current_end = get_month_range(today)
    elif period == "quarter":
        current_start, current_end = get_quarter_range(today)
    elif period == "year":
        current_start, current_end = get_year_range(today)
    else:
        period = "custom"
        current_start = parse_date(request.args.get("start_date"))
        current_end = parse_date(request.args.get("end_date"))

        if not current_start or not current_end:
            current_start, current_end = get_year_range(today)
            period = "year"

    if current_start > current_end:
        current_start, current_end = current_end, current_start

    previous_start = shift_date_one_year_back(current_start)
    previous_end = shift_date_one_year_back(current_end)

    return {
        "period": period,
        "current_start": current_start,
        "current_end": current_end,
        "previous_start": previous_start,
        "previous_end": previous_end,
    }


def fetch_job_items_for_range(company_id, start_date, end_date):
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT
                LOWER(TRIM(COALESCE(ji.item_type, ''))) AS item_type,
                COALESCE(ji.description, '') AS description,
                COALESCE(ji.unit, '') AS unit,
                COALESCE(ji.quantity, 0) AS quantity,
                COALESCE(ji.cost_amount, 0) AS cost_amount,
                COALESCE(ji.line_total, 0) AS line_total,
                ji.job_id,
                j.scheduled_date
            FROM job_items ji
            JOIN jobs j ON j.id = ji.job_id
            WHERE j.company_id = %s
              AND COALESCE(TRIM(j.scheduled_date), '') <> ''
              AND j.scheduled_date::date >= %s
              AND j.scheduled_date::date <= %s
              AND COALESCE(TRIM(ji.description), '') <> ''
            ORDER BY j.scheduled_date::date, ji.job_id, ji.id
            """,
            (company_id, start_date, end_date),
        ).fetchall()
        return rows
    finally:
        conn.close()


def build_period_summary(rows):
    category_map = {}
    grand = {
        "quantity": 0.0,
        "expense": 0.0,
        "gross_income": 0.0,
        "gross_profit": 0.0,
        "net_profit": 0.0,
        "jobs_used_on": set(),
    }

    for row in rows:
        item_type_key = clean_text_input(row["item_type"]).lower() or "uncategorized"
        category_label = display_item_type(item_type_key)

        description_label = canonicalize_description(row["description"], item_type_key)
        unit_key = normalize_unit_key(row["unit"])
        item_key = (description_label.lower(), unit_key)

        quantity = safe_float(row["quantity"])
        expense = safe_float(row["cost_amount"])
        gross_income = safe_float(row["line_total"])
        gross_profit = gross_income - expense
        net_profit = gross_profit
        job_id = row["job_id"]

        if item_type_key not in category_map:
            category_map[item_type_key] = {
                "category_key": item_type_key,
                "category_label": category_label,
                "quantity": 0.0,
                "expense": 0.0,
                "gross_income": 0.0,
                "gross_profit": 0.0,
                "net_profit": 0.0,
                "jobs_used_on": set(),
                "items_map": {},
            }

        category = category_map[item_type_key]

        if item_key not in category["items_map"]:
            category["items_map"][item_key] = {
                "description": description_label,
                "unit": display_unit(unit_key),
                "quantity": 0.0,
                "expense": 0.0,
                "gross_income": 0.0,
                "gross_profit": 0.0,
                "net_profit": 0.0,
                "jobs_used_on": set(),
            }

        item = category["items_map"][item_key]
        item["quantity"] += quantity
        item["expense"] += expense
        item["gross_income"] += gross_income
        item["gross_profit"] += gross_profit
        item["net_profit"] += net_profit
        item["jobs_used_on"].add(job_id)

        category["quantity"] += quantity
        category["expense"] += expense
        category["gross_income"] += gross_income
        category["gross_profit"] += gross_profit
        category["net_profit"] += net_profit
        category["jobs_used_on"].add(job_id)

        grand["quantity"] += quantity
        grand["expense"] += expense
        grand["gross_income"] += gross_income
        grand["gross_profit"] += gross_profit
        grand["net_profit"] += net_profit
        grand["jobs_used_on"].add(job_id)

    categories = []
    for category in sorted(category_map.values(), key=lambda x: x["category_label"].lower()):
        items = []
        for item in sorted(
            category["items_map"].values(),
            key=lambda x: (x["description"].lower(), x["unit"].lower()),
        ):
            item["jobs_used_on"] = len(item["jobs_used_on"])
            items.append(item)

        category["items"] = items
        category["jobs_used_on"] = len(category["jobs_used_on"])
        del category["items_map"]
        categories.append(category)

    grand["jobs_used_on"] = len(grand["jobs_used_on"])

    return {
        "categories": categories,
        "grand": grand,
    }


def build_comparison_report(company_id, current_start, current_end, previous_start, previous_end):
    current_rows = fetch_job_items_for_range(company_id, current_start, current_end)
    previous_rows = fetch_job_items_for_range(company_id, previous_start, previous_end)

    current_summary = build_period_summary(current_rows)
    previous_summary = build_period_summary(previous_rows)

    previous_category_lookup = {
        c["category_key"]: c for c in previous_summary["categories"]
    }

    for current_category in current_summary["categories"]:
        prev_cat = previous_category_lookup.get(current_category["category_key"])
        current_category["previous"] = {
            "quantity": prev_cat["quantity"] if prev_cat else 0.0,
            "expense": prev_cat["expense"] if prev_cat else 0.0,
            "gross_income": prev_cat["gross_income"] if prev_cat else 0.0,
            "gross_profit": prev_cat["gross_profit"] if prev_cat else 0.0,
            "net_profit": prev_cat["net_profit"] if prev_cat else 0.0,
            "jobs_used_on": prev_cat["jobs_used_on"] if prev_cat else 0,
        }
        current_category["change"] = {
            "quantity": percent_change(current_category["quantity"], current_category["previous"]["quantity"]),
            "expense": percent_change(current_category["expense"], current_category["previous"]["expense"]),
            "gross_income": percent_change(current_category["gross_income"], current_category["previous"]["gross_income"]),
            "gross_profit": percent_change(current_category["gross_profit"], current_category["previous"]["gross_profit"]),
            "net_profit": percent_change(current_category["net_profit"], current_category["previous"]["net_profit"]),
        }

        prev_item_lookup = {}
        if prev_cat:
            for item in prev_cat["items"]:
                prev_item_lookup[(item["description"].lower(), item["unit"].lower())] = item

        for item in current_category["items"]:
            prev_item = prev_item_lookup.get((item["description"].lower(), item["unit"].lower()))
            item["previous"] = {
                "quantity": prev_item["quantity"] if prev_item else 0.0,
                "expense": prev_item["expense"] if prev_item else 0.0,
                "gross_income": prev_item["gross_income"] if prev_item else 0.0,
                "gross_profit": prev_item["gross_profit"] if prev_item else 0.0,
                "net_profit": prev_item["net_profit"] if prev_item else 0.0,
                "jobs_used_on": prev_item["jobs_used_on"] if prev_item else 0,
            }
            item["change"] = {
                "quantity": percent_change(item["quantity"], item["previous"]["quantity"]),
                "expense": percent_change(item["expense"], item["previous"]["expense"]),
                "gross_income": percent_change(item["gross_income"], item["previous"]["gross_income"]),
                "gross_profit": percent_change(item["gross_profit"], item["previous"]["gross_profit"]),
                "net_profit": percent_change(item["net_profit"], item["previous"]["net_profit"]),
            }

    current_summary["grand"]["change"] = {
        "quantity": percent_change(current_summary["grand"]["quantity"], previous_summary["grand"]["quantity"]),
        "expense": percent_change(current_summary["grand"]["expense"], previous_summary["grand"]["expense"]),
        "gross_income": percent_change(current_summary["grand"]["gross_income"], previous_summary["grand"]["gross_income"]),
        "gross_profit": percent_change(current_summary["grand"]["gross_profit"], previous_summary["grand"]["gross_profit"]),
        "net_profit": percent_change(current_summary["grand"]["net_profit"], previous_summary["grand"]["net_profit"]),
    }

    return {
        "current": current_summary,
        "previous": previous_summary,
    }


@material_usage_bp.route("/annual-reports", methods=["GET"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def annual_reports():
    company_id = session.get("company_id")

    range_info = get_requested_range()
    comparison = build_comparison_report(
        company_id=company_id,
        current_start=range_info["current_start"],
        current_end=range_info["current_end"],
        previous_start=range_info["previous_start"],
        previous_end=range_info["previous_end"],
    )

    current_summary = comparison["current"]
    previous_summary = comparison["previous"]

    html = """
    <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;">
            <div>
                <h1 style="margin:0;">Annual Reports</h1>
                <p class="muted" style="margin:6px 0 0 0;">
                    Compare current period totals against the same date range from the previous year.
                </p>
            </div>
            <div class="row-actions">
                <a class="btn secondary" href="{{ url_for('material_usage.export_annual_reports_csv', period=period, start_date=current_start_str, end_date=current_end_str) }}">Download CSV</a>
                <a class="btn secondary" href="{{ url_for('settings.settings') }}">Back to Settings</a>
            </div>
        </div>
    </div>

    <div class="card">
        <form method="get">
            <div class="grid">
                <div>
                    <label>Report Type</label>
                    <select name="period" id="period" onchange="toggleDateInputs()">
                        <option value="month" {% if period == 'month' %}selected{% endif %}>Monthly</option>
                        <option value="quarter" {% if period == 'quarter' %}selected{% endif %}>Quarterly</option>
                        <option value="year" {% if period == 'year' %}selected{% endif %}>Yearly</option>
                        <option value="custom" {% if period == 'custom' %}selected{% endif %}>Custom Range</option>
                    </select>
                </div>

                <div id="start_date_wrap" style="{% if period != 'custom' %}display:none;{% endif %}">
                    <label>Start Date</label>
                    <input type="date" name="start_date" value="{{ current_start_str }}">
                </div>

                <div id="end_date_wrap" style="{% if period != 'custom' %}display:none;{% endif %}">
                    <label>End Date</label>
                    <input type="date" name="end_date" value="{{ current_end_str }}">
                </div>

                <div style="display:flex; align-items:flex-end;">
                    <button class="btn" type="submit">Run Report</button>
                </div>
            </div>
        </form>
    </div>

    <div class="grid">
        <div class="card">
            <div class="muted small">Current Period</div>
            <div style="font-size:20px; font-weight:700;">{{ current_start_str }} to {{ current_end_str }}</div>
        </div>
        <div class="card">
            <div class="muted small">Previous Year Comparison</div>
            <div style="font-size:20px; font-weight:700;">{{ previous_start_str }} to {{ previous_end_str }}</div>
        </div>
    </div>

    <div class="grid">
        <div class="card">
            <div class="muted small">Current Gross Income</div>
            <div style="font-size:28px; font-weight:700;">{{ format_money(current_summary.grand.gross_income) }}</div>
            <div class="muted small" style="margin-top:8px;">
                Prev: {{ format_money(previous_summary.grand.gross_income) }}
                |
                {{ format_percent_change(current_summary.grand.gross_income, previous_summary.grand.gross_income) }}
            </div>
        </div>
        <div class="card">
            <div class="muted small">Current Expense</div>
            <div style="font-size:28px; font-weight:700;">{{ format_money(current_summary.grand.expense) }}</div>
            <div class="muted small" style="margin-top:8px;">
                Prev: {{ format_money(previous_summary.grand.expense) }}
                |
                {{ format_percent_change(current_summary.grand.expense, previous_summary.grand.expense) }}
            </div>
        </div>
        <div class="card">
            <div class="muted small">Current Gross Profit</div>
            <div style="font-size:28px; font-weight:700;">{{ format_money(current_summary.grand.gross_profit) }}</div>
            <div class="muted small" style="margin-top:8px;">
                Prev: {{ format_money(previous_summary.grand.gross_profit) }}
                |
                {{ format_percent_change(current_summary.grand.gross_profit, previous_summary.grand.gross_profit) }}
            </div>
        </div>
        <div class="card">
            <div class="muted small">Current Net Profit</div>
            <div style="font-size:28px; font-weight:700;">{{ format_money(current_summary.grand.net_profit) }}</div>
            <div class="muted small" style="margin-top:8px;">
                Prev: {{ format_money(previous_summary.grand.net_profit) }}
                |
                {{ format_percent_change(current_summary.grand.net_profit, previous_summary.grand.net_profit) }}
            </div>
        </div>
    </div>

    {% if current_summary.categories %}
        {% for category in current_summary.categories %}
            <div class="card">
                <h2 style="margin-top:0;">{{ category.category_label }}</h2>

                <div class="table-wrap">
                    <table>
                        <tr>
                            <th>Description</th>
                            <th>Unit</th>
                            <th class="text-right">Current Qty</th>
                            <th class="text-right">Prev Qty</th>
                            <th class="text-right">% Qty</th>
                            <th class="text-right">Current Expense</th>
                            <th class="text-right">Prev Expense</th>
                            <th class="text-right">% Expense</th>
                            <th class="text-right">Current Gross</th>
                            <th class="text-right">Prev Gross</th>
                            <th class="text-right">% Gross</th>
                            <th class="text-right">Current Net</th>
                            <th class="text-right">Prev Net</th>
                            <th class="text-right">% Net</th>
                        </tr>

                        <tr style="background:#f7f7f5; font-weight:700;">
                            <td>{{ category.category_label }} Total</td>
                            <td>-</td>
                            <td class="text-right">{{ format_qty(category.quantity) }}</td>
                            <td class="text-right">{{ format_qty(category.previous.quantity) }}</td>
                            <td class="text-right">{{ format_percent_change(category.quantity, category.previous.quantity) }}</td>
                            <td class="text-right">{{ format_money(category.expense) }}</td>
                            <td class="text-right">{{ format_money(category.previous.expense) }}</td>
                            <td class="text-right">{{ format_percent_change(category.expense, category.previous.expense) }}</td>
                            <td class="text-right">{{ format_money(category.gross_income) }}</td>
                            <td class="text-right">{{ format_money(category.previous.gross_income) }}</td>
                            <td class="text-right">{{ format_percent_change(category.gross_income, category.previous.gross_income) }}</td>
                            <td class="text-right">{{ format_money(category.net_profit) }}</td>
                            <td class="text-right">{{ format_money(category.previous.net_profit) }}</td>
                            <td class="text-right">{{ format_percent_change(category.net_profit, category.previous.net_profit) }}</td>
                        </tr>

                        {% for item in category.items %}
                            <tr>
                                <td style="padding-left:24px;">{{ item.description }}</td>
                                <td>{{ item.unit or "-" }}</td>
                                <td class="text-right">{{ format_qty(item.quantity) }}</td>
                                <td class="text-right">{{ format_qty(item.previous.quantity) }}</td>
                                <td class="text-right">{{ format_percent_change(item.quantity, item.previous.quantity) }}</td>
                                <td class="text-right">{{ format_money(item.expense) }}</td>
                                <td class="text-right">{{ format_money(item.previous.expense) }}</td>
                                <td class="text-right">{{ format_percent_change(item.expense, item.previous.expense) }}</td>
                                <td class="text-right">{{ format_money(item.gross_income) }}</td>
                                <td class="text-right">{{ format_money(item.previous.gross_income) }}</td>
                                <td class="text-right">{{ format_percent_change(item.gross_income, item.previous.gross_income) }}</td>
                                <td class="text-right">{{ format_money(item.net_profit) }}</td>
                                <td class="text-right">{{ format_money(item.previous.net_profit) }}</td>
                                <td class="text-right">{{ format_percent_change(item.net_profit, item.previous.net_profit) }}</td>
                            </tr>
                        {% endfor %}
                    </table>
                </div>
            </div>
        {% endfor %}

        <div class="card">
            <h2>Grand Totals</h2>
            <div class="table-wrap">
                <table>
                    <tr>
                        <th></th>
                        <th class="text-right">Current</th>
                        <th class="text-right">Previous</th>
                        <th class="text-right">% Change</th>
                    </tr>
                    <tr>
                        <td>Total Quantity</td>
                        <td class="text-right">{{ format_qty(current_summary.grand.quantity) }}</td>
                        <td class="text-right">{{ format_qty(previous_summary.grand.quantity) }}</td>
                        <td class="text-right">{{ format_percent_change(current_summary.grand.quantity, previous_summary.grand.quantity) }}</td>
                    </tr>
                    <tr>
                        <td>Total Expense</td>
                        <td class="text-right">{{ format_money(current_summary.grand.expense) }}</td>
                        <td class="text-right">{{ format_money(previous_summary.grand.expense) }}</td>
                        <td class="text-right">{{ format_percent_change(current_summary.grand.expense, previous_summary.grand.expense) }}</td>
                    </tr>
                    <tr>
                        <td>Total Gross Income</td>
                        <td class="text-right">{{ format_money(current_summary.grand.gross_income) }}</td>
                        <td class="text-right">{{ format_money(previous_summary.grand.gross_income) }}</td>
                        <td class="text-right">{{ format_percent_change(current_summary.grand.gross_income, previous_summary.grand.gross_income) }}</td>
                    </tr>
                    <tr>
                        <td>Total Gross Profit</td>
                        <td class="text-right">{{ format_money(current_summary.grand.gross_profit) }}</td>
                        <td class="text-right">{{ format_money(previous_summary.grand.gross_profit) }}</td>
                        <td class="text-right">{{ format_percent_change(current_summary.grand.gross_profit, previous_summary.grand.gross_profit) }}</td>
                    </tr>
                    <tr>
                        <td>Total Net Profit</td>
                        <td class="text-right">{{ format_money(current_summary.grand.net_profit) }}</td>
                        <td class="text-right">{{ format_money(previous_summary.grand.net_profit) }}</td>
                        <td class="text-right">{{ format_percent_change(current_summary.grand.net_profit, previous_summary.grand.net_profit) }}</td>
                    </tr>
                </table>
            </div>
            <p class="muted small" style="margin-top:12px;">
                Net profit currently matches gross profit because item-level overhead is not stored separately yet.
            </p>
        </div>
    {% else %}
        <div class="card">
            <p class="muted" style="margin:0;">No report data found for the selected date range.</p>
        </div>
    {% endif %}

    <style>
        .text-right { text-align:right; }
    </style>

    <script>
        function toggleDateInputs() {
            const period = document.getElementById('period').value;
            const showCustom = period === 'custom';
            document.getElementById('start_date_wrap').style.display = showCustom ? 'block' : 'none';
            document.getElementById('end_date_wrap').style.display = showCustom ? 'block' : 'none';
        }
    </script>
    """

    return render_page(
        render_template_string(
            html,
            period=range_info["period"],
            current_start_str=format_date(range_info["current_start"]),
            current_end_str=format_date(range_info["current_end"]),
            previous_start_str=format_date(range_info["previous_start"]),
            previous_end_str=format_date(range_info["previous_end"]),
            current_summary=current_summary,
            previous_summary=previous_summary,
            format_money=format_money,
            format_qty=format_qty,
            format_percent_change=format_percent_change,
        ),
        "Annual Reports",
    )


@material_usage_bp.route("/annual-reports/export", methods=["GET"])
@login_required
@subscription_required
@require_permission("can_manage_settings")
def export_annual_reports_csv():
    company_id = session.get("company_id")

    range_info = get_requested_range()
    comparison = build_comparison_report(
        company_id=company_id,
        current_start=range_info["current_start"],
        current_end=range_info["current_end"],
        previous_start=range_info["previous_start"],
        previous_end=range_info["previous_end"],
    )

    current_summary = comparison["current"]
    previous_summary = comparison["previous"]

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Current Start",
        "Current End",
        "Previous Start",
        "Previous End",
        "Category",
        "Description",
        "Unit",
        "Current Quantity",
        "Previous Quantity",
        "Quantity % Change",
        "Current Expense",
        "Previous Expense",
        "Expense % Change",
        "Current Gross Income",
        "Previous Gross Income",
        "Gross Income % Change",
        "Current Gross Profit",
        "Previous Gross Profit",
        "Gross Profit % Change",
        "Current Net Profit",
        "Previous Net Profit",
        "Net Profit % Change",
        "Row Type",
    ])

    for category in current_summary["categories"]:
        writer.writerow([
            format_date(range_info["current_start"]),
            format_date(range_info["current_end"]),
            format_date(range_info["previous_start"]),
            format_date(range_info["previous_end"]),
            category["category_label"],
            f"{category['category_label']} Total",
            "",
            f"{category['quantity']:.2f}",
            f"{category['previous']['quantity']:.2f}",
            format_percent_change(category["quantity"], category["previous"]["quantity"]),
            f"{category['expense']:.2f}",
            f"{category['previous']['expense']:.2f}",
            format_percent_change(category["expense"], category["previous"]["expense"]),
            f"{category['gross_income']:.2f}",
            f"{category['previous']['gross_income']:.2f}",
            format_percent_change(category["gross_income"], category["previous"]["gross_income"]),
            f"{category['gross_profit']:.2f}",
            f"{category['previous']['gross_profit']:.2f}",
            format_percent_change(category["gross_profit"], category["previous"]["gross_profit"]),
            f"{category['net_profit']:.2f}",
            f"{category['previous']['net_profit']:.2f}",
            format_percent_change(category["net_profit"], category["previous"]["net_profit"]),
            "Category Total",
        ])

        for item in category["items"]:
            writer.writerow([
                format_date(range_info["current_start"]),
                format_date(range_info["current_end"]),
                format_date(range_info["previous_start"]),
                format_date(range_info["previous_end"]),
                category["category_label"],
                item["description"],
                item["unit"],
                f"{item['quantity']:.2f}",
                f"{item['previous']['quantity']:.2f}",
                format_percent_change(item["quantity"], item["previous"]["quantity"]),
                f"{item['expense']:.2f}",
                f"{item['previous']['expense']:.2f}",
                format_percent_change(item["expense"], item["previous"]["expense"]),
                f"{item['gross_income']:.2f}",
                f"{item['previous']['gross_income']:.2f}",
                format_percent_change(item["gross_income"], item["previous"]["gross_income"]),
                f"{item['gross_profit']:.2f}",
                f"{item['previous']['gross_profit']:.2f}",
                format_percent_change(item["gross_profit"], item["previous"]["gross_profit"]),
                f"{item['net_profit']:.2f}",
                f"{item['previous']['net_profit']:.2f}",
                format_percent_change(item["net_profit"], item["previous"]["net_profit"]),
                "Item Detail",
            ])

    writer.writerow([])
    writer.writerow([
        format_date(range_info["current_start"]),
        format_date(range_info["current_end"]),
        format_date(range_info["previous_start"]),
        format_date(range_info["previous_end"]),
        "Grand Total",
        "",
        "",
        f"{current_summary['grand']['quantity']:.2f}",
        f"{previous_summary['grand']['quantity']:.2f}",
        format_percent_change(current_summary["grand"]["quantity"], previous_summary["grand"]["quantity"]),
        f"{current_summary['grand']['expense']:.2f}",
        f"{previous_summary['grand']['expense']:.2f}",
        format_percent_change(current_summary["grand"]["expense"], previous_summary["grand"]["expense"]),
        f"{current_summary['grand']['gross_income']:.2f}",
        f"{previous_summary['grand']['gross_income']:.2f}",
        format_percent_change(current_summary["grand"]["gross_income"], previous_summary["grand"]["gross_income"]),
        f"{current_summary['grand']['gross_profit']:.2f}",
        f"{previous_summary['grand']['gross_profit']:.2f}",
        format_percent_change(current_summary["grand"]["gross_profit"], previous_summary["grand"]["gross_profit"]),
        f"{current_summary['grand']['net_profit']:.2f}",
        f"{previous_summary['grand']['net_profit']:.2f}",
        format_percent_change(current_summary["grand"]["net_profit"], previous_summary["grand"]["net_profit"]),
        "Grand Total",
    ])

    csv_data = output.getvalue()
    output.close()

    filename = (
        f"annual_reports_{format_date(range_info['current_start'])}_to_"
        f"{format_date(range_info['current_end'])}.csv"
    )

    response = make_response(csv_data)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response