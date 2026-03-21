from flask import Blueprint, session, url_for
from html import escape

from db import get_db_connection, ensure_bookkeeping_history_table, table_columns
from decorators import login_required, require_permission
from page_helpers import render_page

bookkeeping_bp = Blueprint("bookkeeping", __name__)


def _safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


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

    if description_expr:
        desc_sql = "COALESCE(" + ", ".join(description_expr) + ", '')"
    else:
        desc_sql = "''"

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
            WHERE company_id = ?
            ORDER BY
                CASE WHEN {date_col} IS NULL OR {date_col} = '' THEN 1 ELSE 0 END,
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
                '' AS entry_date
            FROM ledger_entries
            WHERE company_id = ?
            ORDER BY id DESC
            """,
            (cid,),
        ).fetchall()

    conn.close()

    total_income = sum(_safe_float(r["amount"]) for r in rows if _safe_float(r["amount"]) > 0)
    total_expenses = sum(abs(_safe_float(r["amount"])) for r in rows if _safe_float(r["amount"]) < 0)
    net_profit = total_income - total_expenses

    running_balance = 0.0
    reversed_rows = list(rows)[::-1]
    balances = {}

    for r in reversed_rows:
        amt = _safe_float(r["amount"])
        running_balance += amt
        balances[r["id"]] = running_balance

    ledger_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{escape(str((r['entry_date'] if 'entry_date' in r.keys() else '-') or '-'))}</td>
            <td>{escape(str(r['entry_type'] or '-'))}</td>
            <td>{escape(str(r['description'] or '-'))}</td>
            <td class="amount-cell" style="color:{'#16a34a' if _safe_float(r['amount']) >= 0 else '#dc2626'};">
                {'+' if _safe_float(r['amount']) > 0 else ''}${_safe_float(r['amount']):,.2f}
            </td>
            <td class="balance-cell" style="color:{'#16a34a' if balances.get(r['id'], 0) >= 0 else '#dc2626'};">
                ${balances.get(r['id'], 0):,.2f}
            </td>
        </tr>
        """
        for r in rows
    )

    net_color = "#16a34a" if net_profit >= 0 else "#dc2626"

    content = f"""
    <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <h1 style="margin:0;">Bookkeeping</h1>
            <a href="{url_for('bookkeeping.bookkeeping_history')}" class="btn secondary">History View</a>
        </div>

        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:18px;">
            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Income</div>
                <div class="stat-value" style="color:#16a34a;">${total_income:,.2f}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Expenses</div>
                <div class="stat-value" style="color:#dc2626;">-${total_expenses:,.2f}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Profit / Loss</div>
                <div class="stat-value" style="color:{net_color};">
                    {'+' if net_profit > 0 else ''}${net_profit:,.2f}
                </div>
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

    <script>
    function populateLedgerTypes() {{
        const rows = document.querySelectorAll("#ledgerTable tbody tr");
        const typeSelect = document.getElementById("ledgerTypeFilter");
        const found = new Set();

        rows.forEach(row => {{
            const cells = row.querySelectorAll("td");
            if (cells.length < 6) return;
            const typeText = cells[2].innerText.trim();
            if (typeText) found.add(typeText);
        }});

        const sorted = Array.from(found).sort((a, b) => a.localeCompare(b));

        sorted.forEach(type => {{
            const opt = document.createElement("option");
            opt.value = type.toLowerCase();
            opt.textContent = type;
            typeSelect.appendChild(opt);
        }});
    }}

    function normalizeDate(dateText) {{
        if (!dateText || dateText === "-") return "";

        const trimmed = dateText.trim();

        if (/^\\d{{4}}-\\d{{2}}-\\d{{2}}/.test(trimmed)) {{
            return trimmed.slice(0, 10);
        }}

        return trimmed;
    }}

    function filterLedgerRows() {{
        const search = document.getElementById("ledgerSearch").value.toLowerCase().trim();
        const typeFilter = document.getElementById("ledgerTypeFilter").value.toLowerCase().trim();
        const fromDate = document.getElementById("ledgerDateFrom").value;
        const toDate = document.getElementById("ledgerDateTo").value;

        const rows = document.querySelectorAll("#ledgerTable tbody tr");
        let visibleCount = 0;

        rows.forEach(row => {{
            const cells = row.querySelectorAll("td");
            if (cells.length < 6) return;

            const idText = cells[0].innerText.toLowerCase();
            const rawDateText = cells[1].innerText.trim();
            const dateText = normalizeDate(rawDateText);
            const typeText = cells[2].innerText.toLowerCase().trim();
            const descText = cells[3].innerText.toLowerCase();
            const amountText = cells[4].innerText.toLowerCase();
            const balanceText = cells[5].innerText.toLowerCase();

            const rowText = `${{idText}} ${{rawDateText.toLowerCase()}} ${{typeText}} ${{descText}} ${{amountText}} ${{balanceText}}`;

            let show = true;

            if (search && !rowText.includes(search)) {{
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

            if (show) visibleCount++;
        }});

        document.getElementById("ledgerCount").innerText =
            `${{visibleCount}} entr${{visibleCount === 1 ? "y" : "ies"}} shown`;
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
        WHERE company_id = ?
        ORDER BY
            CASE WHEN entry_date IS NULL OR entry_date = '' THEN 1 ELSE 0 END,
            entry_date DESC,
            id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    total_in = sum(_safe_float(r["money_in"]) for r in rows)
    total_out = sum(_safe_float(r["money_out"]) for r in rows)
    net = total_in - total_out

    table_rows = "".join(
        f"""
        <tr>
            <td>#{r['id']}</td>
            <td>{escape(str(r['entry_date'] or '-'))}</td>
            <td>{escape(str(r['category'] or '-'))}</td>
            <td>{escape(str(r['entry_type'] or '-'))}</td>
            <td>{escape(str(r['description'] or '-'))}</td>
            <td style="color:#16a34a;">${_safe_float(r['money_in']):,.2f}</td>
            <td style="color:#dc2626;">${_safe_float(r['money_out']):,.2f}</td>
            <td>{escape(str(r['notes'] or ''))}</td>
        </tr>
        """
        for r in rows
    )

    net_color = "#16a34a" if net >= 0 else "#dc2626"

    content = f"""
    <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <h1 style="margin:0;">Bookkeeping History</h1>
            <a href="{url_for('bookkeeping.bookkeeping')}" class="btn secondary">Back to Bookkeeping</a>
        </div>

        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:18px;margin-bottom:18px;">
            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Money In</div>
                <div class="stat-value" style="color:#16a34a;">${total_in:,.2f}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Total Money Out</div>
                <div class="stat-value" style="color:#dc2626;">-${total_out:,.2f}</div>
            </div>

            <div class="card stat-card" style="flex:1;min-width:220px;">
                <div class="stat-label">Net</div>
                <div class="stat-value" style="color:{net_color};">
                    {'+' if net > 0 else ''}${net:,.2f}
                </div>
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