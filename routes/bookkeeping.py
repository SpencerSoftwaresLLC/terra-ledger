from flask import Blueprint, session, url_for, request
from html import escape

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
    "labor",
    "labour",
    "fuel",
    "equipment",
    "delivery",
    "misc",
    "payroll",
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


def _is_expense_entry(entry_type, description=""):
    et = str(entry_type or "").strip().lower()
    desc = str(description or "").strip().lower()

    if et in EXPENSE_TYPES:
        return True

    for keyword in EXPENSE_TYPES:
        if keyword and keyword in et:
            return True

    desc_keywords = {"labor", "labour", "mulch", "stone", "fuel", "equipment", "delivery", "payroll"}
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


@bookkeeping_bp.route("/bookkeeping")
@login_required
@require_permission("can_view_bookkeeping")
def bookkeeping():
    conn = get_db_connection()
    cid = session["company_id"]

    ledger_cols = table_columns(conn, "ledger_entries")

    date_col = None
    for possible in ["created_at", "entry_date", "date", "posted_at"]:
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

    if date_col:
        rows = conn.execute(
            f"""
            SELECT
                id,
                {entry_type_expr} AS entry_type,
                {amount_expr} AS amount,
                {desc_sql} AS description,
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
                {entry_type_expr} AS entry_type,
                {amount_expr} AS amount,
                {desc_sql} AS description,
                NULL AS entry_date
            FROM ledger_entries
            WHERE company_id = %s
            ORDER BY id DESC
            """,
            (cid,),
        ).fetchall()

    ensure_bookkeeping_history_table()

    history_rows = conn.execute(
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

    normalized_rows = []
    total_income = 0.0
    total_expenses = 0.0

    for r in rows:
        raw_amount = abs(_safe_float(r["amount"]))
        entry_type = str(r["entry_type"] or "")
        description = str(r["description"] or "")

        is_expense = _is_expense_entry(entry_type, description)
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

        ledger_row_html.append(
            f"""
            <tr>
                <td>#{r['id']}</td>
                <td>{escape(str(r['entry_date'] or '-'))}</td>
                <td>{escape(str(r['entry_type'] or '-'))}</td>
                <td>{escape(str(r['description'] or '-'))}</td>
                <td class="amount-cell" style="color:{amount_color};">{_fmt_money(signed_amount, show_plus=True)}</td>
                <td class="balance-cell" style="color:{balance_color};">{_fmt_money(balance)}</td>
            </tr>
            """
        )
    ledger_rows = "".join(ledger_row_html)

    history_total_in = sum(_safe_float(r["money_in"]) for r in history_rows)
    history_total_out = sum(_safe_float(r["money_out"]) for r in history_rows)
    history_net = history_total_in - history_total_out

    history_row_html = []
    for r in history_rows:
        history_row_html.append(
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
    history_table_rows = "".join(history_row_html)

    net_color = "#16a34a" if net_profit >= 0 else "#dc2626"
    history_net_color = "#16a34a" if history_net >= 0 else "#dc2626"
    net_profit_text = _fmt_money(net_profit, show_plus=True)
    history_net_text = _fmt_money(history_net, show_plus=True)

    content = f"""
    <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <h1 style="margin:0;">Bookkeeping</h1>
            <div class="row-actions">
                <a href="{url_for('bookkeeping.bookkeeping_history')}" class="btn secondary">History</a>
                <a href="{url_for('bookkeeping.bookkeeping_pnl')}" class="btn success">P&amp;L</a>
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

    <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <h2 style="margin:0;">Bookkeeping History Summary</h2>
            <a href="{url_for('bookkeeping.bookkeeping_history')}" class="btn secondary">Open Full History</a>
        </div>

        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:18px;margin-bottom:18px;">
            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Money In</div>
                <div class="stat-value" style="color:#16a34a;">{_fmt_money(history_total_in)}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Money Out</div>
                <div class="stat-value" style="color:#dc2626;">-{abs(history_total_out):,.2f}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Net</div>
                <div class="stat-value" style="color:{history_net_color};">{history_net_text}</div>
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
                    {history_table_rows or '<tr><td colspan="8" class="muted">No bookkeeping history yet.</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>

    <script>
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

    date_col = _get_ledger_date_column(conn)

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
                entry_type,
                description,
                amount
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
                entry_type,
                description,
                amount
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
        entry_type = str(r["entry_type"] or "Other")
        description = str(r["description"] or "")

        is_expense = _is_expense_entry(entry_type, description)
        category = entry_type if entry_type else "Other"

        if category not in breakdown:
            breakdown[category] = 0.0

        if is_expense:
            total_expenses += amount
            breakdown[category] -= amount
        else:
            total_income += amount
            breakdown[category] += amount

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