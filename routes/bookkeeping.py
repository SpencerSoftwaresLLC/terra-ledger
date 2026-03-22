from flask import Blueprint, session, url_for, request, redirect, flash
from html import escape
from datetime import date

from db import get_db_connection, ensure_bookkeeping_history_table, table_columns
from decorators import login_required, require_permission
from page_helpers import render_page

bookkeeping_bp = Blueprint("bookkeeping", __name__)


EXPENSE_TYPES = {
    "expense",
    "expenses",
    "cost",
    "job cost",
    "material",
    "materials",
    "mulch",
    "stone",
    "dump fee",
    "dump_fee",
    "plants",
    "trees",
    "soil",
    "fertilizer",
    "hardscape material",
    "hardscape_material",
    "labor",
    "labour",
    "fuel",
    "equipment",
    "delivery",
    "misc",
    "payroll",
    "hand tools",
    "office supplies",
    "maintenance",
    "power equipment",
    "vehicles",
    "insurance",
    "marketing",
    "office and admin",
    "safety gear",
    "licensing & certifications",
    "licensing and certifications",
}

JOB_COST_CATEGORY_MAP = {
    "material": "Material",
    "materials": "Material",
    "mulch": "Mulch",
    "stone": "Stone",
    "dump fee": "Dump Fee",
    "dump_fee": "Dump Fee",
    "plants": "Plants",
    "trees": "Trees",
    "soil": "Soil",
    "fertilizer": "Fertilizer",
    "hardscape material": "Hardscape Material",
    "hardscape_material": "Hardscape Material",
    "labor": "Labor",
    "labour": "Labor",
    "fuel": "Fuel",
    "equipment": "Equipment",
    "delivery": "Delivery",
    "misc": "Misc",
    "payroll": "Payroll",
    "hand tools": "Hand Tools",
    "office supplies": "Office Supplies",
    "maintenance": "Maintenance",
    "power equipment": "Power Equipment",
    "vehicles": "Vehicles",
    "insurance": "Insurance",
    "marketing": "Marketing",
    "office and admin": "Office and Admin",
    "safety gear": "Safety Gear",
    "licensing & certifications": "Licensing & Certifications",
    "licensing and certifications": "Licensing & Certifications",
}


def _safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _fmt_money(value, show_plus=False):
    amount = _safe_float(value)
    if amount < 0:
        return f"-${abs(amount):,.2f}"
    if show_plus and amount > 0:
        return f"+${amount:,.2f}"
    return f"${amount:,.2f}"


def _normalize_text(value):
    return str(value or "").strip().lower()


def _canonicalize_category(value):
    v = _normalize_text(value)
    if v in {"dump_fee", "dump fee"}:
        return "Dump Fee"
    if v in {"hardscape_material", "hardscape material"}:
        return "Hardscape Material"
    if v == "mulch":
        return "Mulch"
    if v == "stone":
        return "Stone"
    if v == "plants":
        return "Plants"
    if v == "trees":
        return "Trees"
    if v == "soil":
        return "Soil"
    if v == "fertilizer":
        return "Fertilizer"
    if v in {"labor", "labour"}:
        return "Labor"
    if v == "fuel":
        return "Fuel"
    if v == "equipment":
        return "Equipment"
    if v == "delivery":
        return "Delivery"
    if v == "misc":
        return "Misc"
    if v == "payroll":
        return "Payroll"
    if v == "hand tools":
        return "Hand Tools"
    if v == "office supplies":
        return "Office Supplies"
    if v == "maintenance":
        return "Maintenance"
    if v == "power equipment":
        return "Power Equipment"
    if v == "vehicles":
        return "Vehicles"
    if v == "insurance":
        return "Insurance"
    if v == "marketing":
        return "Marketing"
    if v == "office and admin":
        return "Office and Admin"
    if v == "safety gear":
        return "Safety Gear"
    if v in {"licensing & certifications", "licensing and certifications"}:
        return "Licensing & Certifications"
    if v == "bank deposits":
        return "Bank Deposits"
    if v in {"invoice payments", "invoice payment"}:
        return "Invoice Payments"
    if v == "income":
        return "Income"
    if v in {"material", "materials"}:
        return "Material"
    if not v:
        return ""
    return v.replace("_", " ").title()


def _is_expense_entry(entry_type, description="", category="", source_type="", reference_type=""):
    et = _normalize_text(entry_type)
    desc = _normalize_text(description)
    cat = _normalize_text(category)
    src = _normalize_text(source_type)
    ref = _normalize_text(reference_type)

    if et in EXPENSE_TYPES or cat in EXPENSE_TYPES:
        return True

    for keyword in EXPENSE_TYPES:
        if keyword and (keyword in et or keyword in cat):
            return True

    if src in {"job_item", "payroll"} or ref in {"job_item", "payroll"}:
        return True

    desc_keywords = {
        "labor",
        "labour",
        "mulch",
        "stone",
        "dump fee",
        "dump_fee",
        "plants",
        "trees",
        "soil",
        "fertilizer",
        "hardscape",
        "fuel",
        "equipment",
        "delivery",
        "payroll",
        "material",
        "materials",
        "hand tools",
        "office supplies",
        "maintenance",
        "power equipment",
        "vehicles",
        "insurance",
        "marketing",
        "office and admin",
        "safety gear",
        "licensing",
        "certifications",
    }
    for keyword in desc_keywords:
        if keyword in desc:
            return True

    return False


def _get_ledger_date_column(conn):
    ledger_cols = table_columns(conn, "ledger_entries")
    for possible in ["entry_date", "date", "posted_at", "created_at"]:
        if possible in ledger_cols:
            return possible
    return None


def _get_ledger_select_parts(conn):
    ledger_cols = table_columns(conn, "ledger_entries")

    date_col = None
    for possible in ["entry_date", "date", "posted_at", "created_at"]:
        if possible in ledger_cols:
            date_col = possible
            break

    description_expr = []
    if "description" in ledger_cols:
        description_expr.append("description")
    if "memo" in ledger_cols:
        description_expr.append("memo")
    if "notes" in ledger_cols:
        description_expr.append("notes")
    if "source_type" in ledger_cols:
        description_expr.append("source_type")
    if "reference_type" in ledger_cols:
        description_expr.append("reference_type")

    desc_sql = "COALESCE(" + ", ".join(description_expr) + ", '')" if description_expr else "''"
    entry_type_expr = "entry_type" if "entry_type" in ledger_cols else "'Entry'"
    amount_expr = "amount" if "amount" in ledger_cols else "0"
    category_expr = "category" if "category" in ledger_cols else "NULL"
    source_type_expr = "source_type" if "source_type" in ledger_cols else "NULL"
    reference_type_expr = "reference_type" if "reference_type" in ledger_cols else "NULL"

    return {
        "ledger_cols": ledger_cols,
        "date_col": date_col,
        "desc_sql": desc_sql,
        "entry_type_expr": entry_type_expr,
        "amount_expr": amount_expr,
        "category_expr": category_expr,
        "source_type_expr": source_type_expr,
        "reference_type_expr": reference_type_expr,
    }


def _guess_job_cost_category(entry_type="", description="", category="", source_type="", reference_type=""):
    et = _normalize_text(entry_type)
    desc = _normalize_text(description)
    cat = _normalize_text(category)
    src = _normalize_text(source_type)
    ref = _normalize_text(reference_type)

    for key, label in JOB_COST_CATEGORY_MAP.items():
        if cat == key or et == key:
            return label

    for key, label in JOB_COST_CATEGORY_MAP.items():
        if key in cat or key in et:
            return label

    if src == "job_item" or ref == "job_item":
        if "dump fee" in desc or "dump_fee" in desc:
            return "Dump Fee"
        if "mulch" in desc:
            return "Mulch"
        if "stone" in desc:
            return "Stone"
        if "plant" in desc:
            return "Plants"
        if "tree" in desc:
            return "Trees"
        if "soil" in desc:
            return "Soil"
        if "fertilizer" in desc:
            return "Fertilizer"
        if "hardscape" in desc:
            return "Hardscape Material"
        if "labor" in desc or "labour" in desc or "hour" in desc or "hours" in desc or "hr" in desc or "hrs" in desc:
            return "Labor"
        if "fuel" in desc:
            return "Fuel"
        if "equipment" in desc:
            return "Equipment"
        if "delivery" in desc:
            return "Delivery"
        if "misc" in desc:
            return "Misc"
        return "Material"

    if src == "payroll" or ref == "payroll" or "payroll" in desc:
        return "Payroll"

    return ""


def _get_pl_bucket(entry_type="", description="", category="", source_type="", reference_type=""):
    is_expense = _is_expense_entry(
        entry_type=entry_type,
        description=description,
        category=category,
        source_type=source_type,
        reference_type=reference_type,
    )

    if not is_expense:
        normalized_category = _normalize_text(category)
        normalized_entry_type = _normalize_text(entry_type)
        normalized_desc = _normalize_text(description)

        if "invoice payment" in normalized_category or "invoice payment" in normalized_entry_type:
            return "Invoice Payments"
        if "bank deposit" in normalized_category or "bank deposit" in normalized_entry_type:
            return "Bank Deposits"
        if "income" in normalized_category:
            return "Income"
        if "invoice" in normalized_desc and "payment" in normalized_desc:
            return "Invoice Payments"
        return "Income"

    job_cost_bucket = _guess_job_cost_category(
        entry_type=entry_type,
        description=description,
        category=category,
        source_type=source_type,
        reference_type=reference_type,
    )
    if job_cost_bucket:
        return job_cost_bucket

    normalized_category = _normalize_text(category)
    normalized_entry_type = _normalize_text(entry_type)

    if normalized_category:
        return _canonicalize_category(normalized_category)
    if normalized_entry_type:
        return _canonicalize_category(normalized_entry_type)

    return "Expense"


def _insert_manual_ledger_entry(conn, company_id, entry_date, entry_type, category, description, amount, notes):
    ledger_cols = table_columns(conn, "ledger_entries")

    values = {}

    if "company_id" in ledger_cols:
        values["company_id"] = company_id
    if "entry_date" in ledger_cols:
        values["entry_date"] = entry_date
    elif "date" in ledger_cols:
        values["date"] = entry_date
    if "entry_type" in ledger_cols:
        values["entry_type"] = entry_type
    if "category" in ledger_cols:
        values["category"] = category
    if "description" in ledger_cols:
        values["description"] = description
    elif "memo" in ledger_cols:
        values["memo"] = description
    if "amount" in ledger_cols:
        values["amount"] = amount
    if "notes" in ledger_cols:
        values["notes"] = notes
    if "source_type" in ledger_cols:
        values["source_type"] = "manual"
    if "reference_type" in ledger_cols:
        values["reference_type"] = "manual"
    if "status" in ledger_cols:
        values["status"] = "posted"

    if not values:
        return

    cols_sql = ", ".join(values.keys())
    placeholders = ", ".join(["%s"] * len(values))
    conn.execute(
        f"INSERT INTO ledger_entries ({cols_sql}) VALUES ({placeholders})",
        tuple(values.values()),
    )


def _insert_manual_history_entry(conn, company_id, entry_date, entry_type, category, description, amount, notes):
    try:
        ensure_bookkeeping_history_table()
        money_in = amount if _normalize_text(entry_type) == "income" else 0
        money_out = amount if _normalize_text(entry_type) != "income" else 0

        conn.execute(
            """
            INSERT INTO bookkeeping_history (
                company_id,
                entry_date,
                category,
                entry_type,
                description,
                money_in,
                money_out,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                company_id,
                entry_date,
                category,
                entry_type,
                description,
                money_in,
                money_out,
                notes,
            ),
        )
    except Exception:
        pass


@bookkeeping_bp.route("/bookkeeping", methods=["GET", "POST"])
@login_required
@require_permission("can_view_bookkeeping")
def bookkeeping():
    conn = get_db_connection()
    cid = session["company_id"]

    if request.method == "POST":
        entry_date = (request.form.get("entry_date") or "").strip() or date.today().isoformat()
        entry_type = (request.form.get("entry_type") or "expense").strip().lower()
        category = _canonicalize_category(request.form.get("category") or "")
        description = (request.form.get("description") or "").strip()
        amount = _safe_float(request.form.get("amount"))
        notes = (request.form.get("notes") or "").strip()

        if amount <= 0:
            conn.close()
            flash("Amount must be greater than 0.")
            return redirect(url_for("bookkeeping.bookkeeping"))

        if not description:
            conn.close()
            flash("Description is required.")
            return redirect(url_for("bookkeeping.bookkeeping"))

        if not category:
            category = "Income" if entry_type == "income" else "Expense"

        _insert_manual_ledger_entry(
            conn=conn,
            company_id=cid,
            entry_date=entry_date,
            entry_type=entry_type,
            category=category,
            description=description,
            amount=amount,
            notes=notes,
        )

        _insert_manual_history_entry(
            conn=conn,
            company_id=cid,
            entry_date=entry_date,
            entry_type=entry_type.title(),
            category=category,
            description=description,
            amount=amount,
            notes=notes,
        )

        conn.commit()
        conn.close()

        flash("Manual bookkeeping entry added.")
        return redirect(url_for("bookkeeping.bookkeeping"))

    select_parts = _get_ledger_select_parts(conn)
    date_col = select_parts["date_col"]

    if date_col:
        rows = conn.execute(
            f"""
            SELECT
                id,
                {select_parts["entry_type_expr"]} AS entry_type,
                {select_parts["amount_expr"]} AS amount,
                {select_parts["desc_sql"]} AS description,
                {select_parts["category_expr"]} AS category,
                {select_parts["source_type_expr"]} AS source_type,
                {select_parts["reference_type_expr"]} AS reference_type,
                {date_col} AS entry_date
            FROM ledger_entries
            WHERE company_id = %s
            ORDER BY
                CASE WHEN {date_col} IS NULL THEN 1 ELSE 0 END,
                {date_col} DESC,
                id DESC
            """,
            (cid,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT
                id,
                {select_parts["entry_type_expr"]} AS entry_type,
                {select_parts["amount_expr"]} AS amount,
                {select_parts["desc_sql"]} AS description,
                {select_parts["category_expr"]} AS category,
                {select_parts["source_type_expr"]} AS source_type,
                {select_parts["reference_type_expr"]} AS reference_type,
                NULL AS entry_date
            FROM ledger_entries
            WHERE company_id = %s
            ORDER BY id DESC
            """,
            (cid,),
        ).fetchall()

    conn.close()

    normalized_rows = []
    total_income = 0.0
    total_expenses = 0.0

    for r in rows:
        raw_amount = abs(_safe_float(r["amount"]))
        entry_type = str(r["entry_type"] or "")
        description = str(r["description"] or "")
        category = str(r["category"] or "")
        source_type = str(r["source_type"] or "")
        reference_type = str(r["reference_type"] or "")

        is_expense = _is_expense_entry(
            entry_type=entry_type,
            description=description,
            category=category,
            source_type=source_type,
            reference_type=reference_type,
        )
        signed_amount = -raw_amount if is_expense else raw_amount

        if is_expense:
            total_expenses += raw_amount
        else:
            total_income += raw_amount

        normalized_rows.append(
            {
                "id": r["id"],
                "entry_date": r["entry_date"],
                "entry_type": entry_type,
                "description": description,
                "category": category,
                "source_type": source_type,
                "reference_type": reference_type,
                "raw_amount": raw_amount,
                "signed_amount": signed_amount,
            }
        )

    net_profit = total_income - total_expenses

    running_balance = 0.0
    balances = {}
    for r in list(normalized_rows)[::-1]:
        running_balance += r["signed_amount"]
        balances[r["id"]] = running_balance

    ledger_row_html = []
    for r in normalized_rows:
        signed_amount = r["signed_amount"]
        balance = balances.get(r["id"], 0.0)
        amount_color = "#16a34a" if signed_amount >= 0 else "#dc2626"
        balance_color = "#16a34a" if balance >= 0 else "#dc2626"

        display_type = _get_pl_bucket(
            entry_type=r["entry_type"],
            description=r["description"],
            category=r["category"],
            source_type=r["source_type"],
            reference_type=r["reference_type"],
        )

        ledger_row_html.append(
            f"""
            <tr>
                <td>#{r['id']}</td>
                <td>{escape(str(r['entry_date'] or '-'))}</td>
                <td>{escape(display_type)}</td>
                <td>{escape(str(r['description'] or '-'))}</td>
                <td class="amount-cell" style="color:{amount_color};">{_fmt_money(signed_amount, show_plus=True)}</td>
                <td class="balance-cell" style="color:{balance_color};">{_fmt_money(balance)}</td>
            </tr>
            """
        )
    ledger_rows = "".join(ledger_row_html)

    net_color = "#16a34a" if net_profit >= 0 else "#dc2626"
    net_profit_text = _fmt_money(net_profit, show_plus=True)

    content = f"""
    <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <h1 style="margin:0;">Bookkeeping</h1>
            <div class="row-actions">
                <a href="{url_for('bookkeeping.bookkeeping_pnl')}" class="btn success">P&amp;L</a>
                <a href="{url_for('bookkeeping.bookkeeping_history')}" class="btn secondary">History</a>
            </div>
        </div>

        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:18px;">
            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Income</div>
                <div class="stat-value" style="color:#16a34a;">{_fmt_money(total_income)}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Expenses</div>
                <div class="stat-value" style="color:#dc2626;">-{abs(total_expenses):,.2f}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Profit / Loss</div>
                <div class="stat-value" style="color:{net_color};">{net_profit_text}</div>
            </div>
        </div>
    </div>

    <div class="card">
        <h2>Manual Bookkeeping Entry</h2>
        <form method="post">
            <div class="grid">
                <div>
                    <label>Date</label>
                    <input type="date" name="entry_date" value="{date.today().isoformat()}">
                </div>
                <div>
                    <label>Entry Type</label>
                    <select name="entry_type" id="manual_entry_type" onchange="toggleManualCategories()">
                        <option value="expense">Expense</option>
                        <option value="income">Income</option>
                    </select>
                </div>
                <div>
                    <label>Category</label>
                    <select name="category" id="manual_category">
                        <option value="Mulch">Mulch</option>
                        <option value="Stone">Stone</option>
                        <option value="Dump Fee">Dump Fee</option>
                        <option value="Plants">Plants</option>
                        <option value="Trees">Trees</option>
                        <option value="Soil">Soil</option>
                        <option value="Fertilizer">Fertilizer</option>
                        <option value="Hardscape Material">Hardscape Material</option>
                        <option value="Labor">Labor</option>
                        <option value="Fuel">Fuel</option>
                        <option value="Equipment">Equipment</option>
                        <option value="Delivery">Delivery</option>
                        <option value="Misc">Misc</option>
                        <option value="Payroll">Payroll</option>
                        <option value="Hand Tools">Hand Tools</option>
                        <option value="Office Supplies">Office Supplies</option>
                        <option value="Maintenance">Maintenance</option>
                        <option value="Power Equipment">Power Equipment</option>
                        <option value="Vehicles">Vehicles</option>
                        <option value="Insurance">Insurance</option>
                        <option value="Marketing">Marketing</option>
                        <option value="Office and Admin">Office and Admin</option>
                        <option value="Safety Gear">Safety Gear</option>
                        <option value="Licensing &amp; Certifications">Licensing &amp; Certifications</option>
                    </select>
                </div>
                <div>
                    <label>Amount</label>
                    <input type="number" step="0.01" min="0.01" name="amount" placeholder="0.00" required>
                </div>
                <div style="grid-column:1 / -1;">
                    <label>Description</label>
                    <input type="text" name="description" placeholder="Enter description..." required>
                </div>
                <div style="grid-column:1 / -1;">
                    <label>Notes</label>
                    <textarea name="notes" placeholder="Optional notes..."></textarea>
                </div>
            </div>

            <div class="row-actions" style="margin-top:14px;">
                <button class="btn success" type="submit">Add Entry</button>
            </div>
        </form>
    </div>

    <div class="card">
        <h2>Ledger</h2>

        <div class="grid" style="margin-top:18px;">
            <div>
                <label>Search</label>
                <input id="ledgerSearch" type="text" placeholder="Search description, type, date...">
            </div>
            <div>
                <label>Type</label>
                <select id="ledgerTypeFilter">
                    <option value="">All</option>
                </select>
            </div>
            <div>
                <label>From Date</label>
                <input id="ledgerDateFrom" type="date">
            </div>
            <div>
                <label>To Date</label>
                <input id="ledgerDateTo" type="date">
            </div>
        </div>

        <div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap;align-items:center;">
            <button type="button" class="btn secondary" onclick="resetLedgerFilters()">Reset Filters</button>
            <div class="muted" id="ledgerCount"></div>
        </div>

        <div class="table-wrap" style="margin-top:16px;">
            <table id="ledgerTable" style="width:100%;border-collapse:collapse;">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Date</th>
                        <th>Type</th>
                        <th>Description</th>
                        <th>Amount</th>
                        <th>Running Balance</th>
                    </tr>
                </thead>
                <tbody>
                    {ledger_rows or '<tr><td colspan="6" class="muted">No bookkeeping entries yet.</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>

    <script>
    function toggleManualCategories() {{
        var typeEl = document.getElementById("manual_entry_type");
        var categoryEl = document.getElementById("manual_category");
        if (!typeEl || !categoryEl) return;

        var expenseOptions = [
            "Mulch",
            "Stone",
            "Dump Fee",
            "Plants",
            "Trees",
            "Soil",
            "Fertilizer",
            "Hardscape Material",
            "Labor",
            "Fuel",
            "Equipment",
            "Delivery",
            "Misc",
            "Payroll",
            "Hand Tools",
            "Office Supplies",
            "Maintenance",
            "Power Equipment",
            "Vehicles",
            "Insurance",
            "Marketing",
            "Office and Admin",
            "Safety Gear",
            "Licensing & Certifications"
        ];

        var incomeOptions = [
            "Income",
            "Invoice Payments",
            "Bank Deposits"
        ];

        var selected = typeEl.value === "income" ? incomeOptions : expenseOptions;
        categoryEl.innerHTML = "";

        selected.forEach(function(opt) {{
            var el = document.createElement("option");
            el.value = opt;
            el.textContent = opt;
            categoryEl.appendChild(el);
        }});
    }}

    function populateLedgerTypes() {{
        var rows = document.querySelectorAll("#ledgerTable tbody tr");
        var typeSelect = document.getElementById("ledgerTypeFilter");
        var found = new Set();

        rows.forEach(function(row) {{
            var cells = row.querySelectorAll("td");
            if (cells.length < 6) return;
            var typeText = cells[2].innerText.trim();
            if (typeText && typeText !== "-") {{
                found.add(typeText);
            }}
        }});

        Array.from(found).sort(function(a, b) {{
            return a.localeCompare(b);
        }}).forEach(function(type) {{
            var opt = document.createElement("option");
            opt.value = type.toLowerCase();
            opt.textContent = type;
            typeSelect.appendChild(opt);
        }});
    }}

    function normalizeDate(dateText) {{
        if (!dateText || dateText === "-") return "";
        var trimmed = dateText.trim();

        if (/^\\d{{4}}-\\d{{2}}-\\d{{2}}/.test(trimmed)) {{
            return trimmed.slice(0, 10);
        }}

        return trimmed;
    }}

    function filterLedgerRows() {{
        var search = document.getElementById("ledgerSearch").value.toLowerCase().trim();
        var typeFilter = document.getElementById("ledgerTypeFilter").value.toLowerCase().trim();
        var fromDate = document.getElementById("ledgerDateFrom").value;
        var toDate = document.getElementById("ledgerDateTo").value;

        var rows = document.querySelectorAll("#ledgerTable tbody tr");
        var visibleCount = 0;

        rows.forEach(function(row) {{
            var cells = row.querySelectorAll("td");
            if (cells.length < 6) return;

            var idText = cells[0].innerText.toLowerCase();
            var rawDateText = cells[1].innerText.trim();
            var dateText = normalizeDate(rawDateText);
            var typeText = cells[2].innerText.toLowerCase().trim();
            var descText = cells[3].innerText.toLowerCase();
            var amountText = cells[4].innerText.toLowerCase();
            var balanceText = cells[5].innerText.toLowerCase();

            var rowText = idText + " " + rawDateText.toLowerCase() + " " + typeText + " " + descText + " " + amountText + " " + balanceText;

            var show = true;

            if (search && rowText.indexOf(search) === -1) {{
                show = false;
            }}

            if (typeFilter && typeText !== typeFilter) {{
                show = false;
            }}

            if (fromDate && dateText && dateText < fromDate) {{
                show = false;
            }}

            if (toDate && dateText && dateText > toDate) {{
                show = false;
            }}

            row.style.display = show ? "" : "none";

            if (show) {{
                visibleCount += 1;
            }}
        }});

        document.getElementById("ledgerCount").innerText =
            visibleCount + " entr" + (visibleCount === 1 ? "y" : "ies") + " shown";
    }}

    function resetLedgerFilters() {{
        document.getElementById("ledgerSearch").value = "";
        document.getElementById("ledgerTypeFilter").value = "";
        document.getElementById("ledgerDateFrom").value = "";
        document.getElementById("ledgerDateTo").value = "";
        filterLedgerRows();
    }}

    document.addEventListener("DOMContentLoaded", function() {{
        toggleManualCategories();
        populateLedgerTypes();
        filterLedgerRows();

        document.getElementById("ledgerSearch").addEventListener("input", filterLedgerRows);
        document.getElementById("ledgerTypeFilter").addEventListener("change", filterLedgerRows);
        document.getElementById("ledgerDateFrom").addEventListener("change", filterLedgerRows);
        document.getElementById("ledgerDateTo").addEventListener("change", filterLedgerRows);
    }});
    </script>
    """

    return render_page(content, "Bookkeeping")


@bookkeeping_bp.route("/bookkeeping-history")
@login_required
@require_permission("can_view_bookkeeping")
def bookkeeping_history():
    ensure_bookkeeping_history_table()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT
            id,
            entry_date,
            category,
            entry_type,
            description,
            money_in,
            money_out,
            notes
        FROM bookkeeping_history
        WHERE company_id = %s
        ORDER BY
            CASE WHEN entry_date IS NULL THEN 1 ELSE 0 END,
            entry_date DESC,
            id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    total_in = sum(_safe_float(r["money_in"]) for r in rows)
    total_out = sum(_safe_float(r["money_out"]) for r in rows)
    net = total_in - total_out

    table_row_html = []
    for r in rows:
        table_row_html.append(
            f"""
            <tr>
                <td>#{r['id']}</td>
                <td>{escape(str(r['entry_date'] or '-'))}</td>
                <td>{escape(str(r['category'] or '-'))}</td>
                <td>{escape(str(r['entry_type'] or '-'))}</td>
                <td>{escape(str(r['description'] or '-'))}</td>
                <td style="color:#16a34a;">{_fmt_money(r['money_in'])}</td>
                <td style="color:#dc2626;">-{abs(_safe_float(r['money_out'])):,.2f}</td>
                <td>{escape(str(r['notes'] or ''))}</td>
            </tr>
            """
        )
    table_rows = "".join(table_row_html)

    net_color = "#16a34a" if net >= 0 else "#dc2626"
    net_text = _fmt_money(net, show_plus=True)

    content = f"""
    <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <h1 style="margin:0;">Bookkeeping History</h1>
            <div class="row-actions">
                <a href="{url_for('bookkeeping.bookkeeping')}" class="btn secondary">Back to Bookkeeping</a>
                <a href="{url_for('bookkeeping.bookkeeping_pnl')}" class="btn success">P&amp;L</a>
            </div>
        </div>

        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:18px;margin-bottom:18px;">
            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Money In</div>
                <div class="stat-value" style="color:#16a34a;">{_fmt_money(total_in)}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Money Out</div>
                <div class="stat-value" style="color:#dc2626;">-{abs(total_out):,.2f}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Net</div>
                <div class="stat-value" style="color:{net_color};">{net_text}</div>
            </div>
        </div>

        <div class="table-wrap">
            <table style="width:100%;border-collapse:collapse;">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Date</th>
                        <th>Category</th>
                        <th>Type</th>
                        <th>Description</th>
                        <th>Money In</th>
                        <th>Money Out</th>
                        <th>Notes</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows or '<tr><td colspan="8" class="muted">No bookkeeping history yet.</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>
    """

    return render_page(content, "Bookkeeping History")


@bookkeeping_bp.route("/bookkeeping/pnl")
@login_required
@require_permission("can_view_bookkeeping")
def bookkeeping_pnl():
    conn = get_db_connection()
    cid = session["company_id"]

    select_parts = _get_ledger_select_parts(conn)
    date_col = select_parts["date_col"]

    date_from = (request.args.get("date_from") or "").strip()
    date_to = (request.args.get("date_to") or "").strip()

    where_clauses = ["company_id = %s"]
    params = [cid]

    if date_col and date_from:
        where_clauses.append(f"{date_col} >= %s")
        params.append(date_from)

    if date_col and date_to:
        where_clauses.append(f"{date_col} <= %s")
        params.append(date_to)

    where_sql = " AND ".join(where_clauses)

    if date_col:
        rows = conn.execute(
            f"""
            SELECT
                id,
                {date_col} AS entry_date,
                {select_parts["entry_type_expr"]} AS entry_type,
                {select_parts["desc_sql"]} AS description,
                {select_parts["amount_expr"]} AS amount,
                {select_parts["category_expr"]} AS category,
                {select_parts["source_type_expr"]} AS source_type,
                {select_parts["reference_type_expr"]} AS reference_type
            FROM ledger_entries
            WHERE {where_sql}
            ORDER BY
                CASE WHEN {date_col} IS NULL THEN 1 ELSE 0 END,
                {date_col} DESC,
                id DESC
            """,
            tuple(params),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT
                id,
                NULL AS entry_date,
                {select_parts["entry_type_expr"]} AS entry_type,
                {select_parts["desc_sql"]} AS description,
                {select_parts["amount_expr"]} AS amount,
                {select_parts["category_expr"]} AS category,
                {select_parts["source_type_expr"]} AS source_type,
                {select_parts["reference_type_expr"]} AS reference_type
            FROM ledger_entries
            WHERE {where_sql}
            ORDER BY id DESC
            """,
            tuple(params),
        ).fetchall()

    conn.close()

    total_income = 0.0
    total_expenses = 0.0
    breakdown = {}

    for r in rows:
        amount = abs(_safe_float(r["amount"]))
        entry_type = str(r["entry_type"] or "")
        description = str(r["description"] or "")
        category = str(r["category"] or "")
        source_type = str(r["source_type"] or "")
        reference_type = str(r["reference_type"] or "")

        bucket = _get_pl_bucket(
            entry_type=entry_type,
            description=description,
            category=category,
            source_type=source_type,
            reference_type=reference_type,
        )

        is_expense = _is_expense_entry(
            entry_type=entry_type,
            description=description,
            category=category,
            source_type=source_type,
            reference_type=reference_type,
        )

        if bucket not in breakdown:
            breakdown[bucket] = 0.0

        if is_expense:
            total_expenses += amount
            breakdown[bucket] -= amount
        else:
            total_income += amount
            breakdown[bucket] += amount

    net_profit = total_income - total_expenses

    rows_html = ""
    for cat, amt in sorted(breakdown.items(), key=lambda x: x[0].lower()):
        color = "#16a34a" if amt >= 0 else "#dc2626"
        rows_html += f"""
        <tr>
            <td>{escape(cat)}</td>
            <td style="color:{color};">{_fmt_money(amt, show_plus=True)}</td>
        </tr>
        """

    net_color = "#16a34a" if net_profit >= 0 else "#dc2626"

    if date_from or date_to:
        range_text = f"{escape(date_from or 'Beginning')} to {escape(date_to or 'Today')}"
    else:
        range_text = "All dates"

    content = f"""
    <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <h1 style="margin:0;">Profit &amp; Loss</h1>
            <div class="row-actions">
                <a href="{url_for('bookkeeping.bookkeeping')}" class="btn secondary">Back</a>
                <a href="{url_for('bookkeeping.bookkeeping_history')}" class="btn secondary">History</a>
            </div>
        </div>

        <form method="get" style="margin-top:18px;">
            <div class="grid">
                <div>
                    <label>From Date</label>
                    <input type="date" name="date_from" value="{escape(date_from)}">
                </div>
                <div>
                    <label>To Date</label>
                    <input type="date" name="date_to" value="{escape(date_to)}">
                </div>
            </div>

            <div class="row-actions" style="margin-top:14px;">
                <button class="btn success" type="submit">Apply Date Filter</button>
                <a href="{url_for('bookkeeping.bookkeeping_pnl')}" class="btn secondary">Clear</a>
            </div>
        </form>

        <div class="muted" style="margin-top:14px;">
            <strong>Date Range:</strong> {range_text}
        </div>

        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:20px;">
            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Income</div>
                <div class="stat-value" style="color:#16a34a;">{_fmt_money(total_income)}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Expenses</div>
                <div class="stat-value" style="color:#dc2626;">-{abs(total_expenses):,.2f}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Net Profit</div>
                <div class="stat-value" style="color:{net_color};">{_fmt_money(net_profit, show_plus=True)}</div>
            </div>
        </div>

        <div class="card" style="margin-top:20px;">
            <h2>Breakdown</h2>
            <div class="table-wrap">
                <table style="width:100%;border-collapse:collapse;">
                    <thead>
                        <tr>
                            <th>Category</th>
                            <th>Amount</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html or '<tr><td colspan="2" class="muted">No P&amp;L data for this date range.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    """

    return render_page(content, "Profit & Loss")