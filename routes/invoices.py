from flask import Blueprint, request, redirect, url_for, session, flash, abort
from datetime import date, datetime
from html import escape
import json
import re

from db import get_db_connection
from decorators import login_required, require_permission
from page_helpers import render_page


invoices_bp = Blueprint("invoices", __name__)


# =========================================================
# Helpers
# =========================================================

def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _clean_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"none", "null", "n/a"}:
        return ""
    return text


def _clean_display(value):
    text = _clean_text(value)
    return text if text else "-"


def _table_columns(conn, table_name):
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        (table_name,),
    ).fetchall()

    cols = set()
    for row in rows:
        if hasattr(row, "keys"):
            cols.add(row["column_name"])
        else:
            cols.add(row[0])
    return cols


def ensure_document_number_columns():
    conn = get_db_connection()
    try:
        company_cols = _table_columns(conn, "companies")

        if "next_invoice_number" not in company_cols:
            conn.execute(
                """
                ALTER TABLE companies
                ADD COLUMN next_invoice_number BIGINT NOT NULL DEFAULT 1001
                """
            )

        invoice_cols = _table_columns(conn, "invoices")
        if "invoice_number" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN invoice_number TEXT
                """
            )

        if "amount_paid" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN amount_paid NUMERIC(12,2) NOT NULL DEFAULT 0
                """
            )

        if "balance_due" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN balance_due NUMERIC(12,2) NOT NULL DEFAULT 0
                """
            )

        if "notes" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN notes TEXT
                """
            )

        if "quote_id" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN quote_id BIGINT
                """
            )

        if "job_id" not in invoice_cols:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN job_id BIGINT
                """
            )

        conn.commit()
    finally:
        conn.close()


def ensure_invoice_payment_table():
    conn = get_db_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS invoice_payments (
                id BIGSERIAL PRIMARY KEY,
                company_id BIGINT NOT NULL,
                invoice_id BIGINT NOT NULL,
                payment_date DATE,
                amount NUMERIC(12,2) NOT NULL DEFAULT 0,
                payment_method TEXT,
                reference TEXT,
                notes TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _extract_numeric_invoice_number(invoice_number):
    if not invoice_number:
        return None
    match = re.search(r"(\d+)$", str(invoice_number).strip())
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def get_next_invoice_number(company_id):
    conn = get_db_connection()
    try:
        company = conn.execute(
            """
            SELECT next_invoice_number
            FROM companies
            WHERE id = %s
            """,
            (company_id,),
        ).fetchone()

        if company and company["next_invoice_number"] is not None:
            next_num = int(company["next_invoice_number"])
            conn.execute(
                """
                UPDATE companies
                SET next_invoice_number = %s
                WHERE id = %s
                """,
                (next_num + 1, company_id),
            )
            conn.commit()
            return str(next_num)

        rows = conn.execute(
            """
            SELECT invoice_number
            FROM invoices
            WHERE company_id = %s
            ORDER BY id DESC
            LIMIT 100
            """,
            (company_id,),
        ).fetchall()

        max_num = 1000
        for row in rows:
            parsed = _extract_numeric_invoice_number(row["invoice_number"])
            if parsed and parsed > max_num:
                max_num = parsed

        next_num = max_num + 1
        conn.execute(
            """
            UPDATE companies
            SET next_invoice_number = %s
            WHERE id = %s
            """,
            (next_num + 1, company_id),
        )
        conn.commit()
        return str(next_num)

    finally:
        conn.close()


def recalc_invoice(conn, invoice_id):
    invoice_row = conn.execute(
        """
        SELECT
            id,
            subtotal,
            total,
            amount_paid,
            balance_due
        FROM invoices
        WHERE id = %s
        """,
        (invoice_id,),
    ).fetchone()

    if not invoice_row:
        return

    item_row = conn.execute(
        """
        SELECT
            COALESCE(SUM(line_total), 0) AS items_total,
            COUNT(*) AS item_count
        FROM invoice_items
        WHERE invoice_id = %s
        """,
        (invoice_id,),
    ).fetchone()

    payment_row = conn.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS paid_total
        FROM invoice_payments
        WHERE invoice_id = %s
        """,
        (invoice_id,),
    ).fetchone()

    item_count = int(item_row["item_count"] or 0) if item_row else 0
    items_total = _safe_float(item_row["items_total"] if item_row else 0)
    paid_total = _safe_float(payment_row["paid_total"] if payment_row else 0)

    # IMPORTANT:
    # If invoice has line items, trust the sum of line items.
    # If it has no line items, preserve the stored total instead of dropping to 0.
    if item_count > 0:
        subtotal = items_total
        total = items_total
    else:
        subtotal = _safe_float(invoice_row["subtotal"])
        total = _safe_float(invoice_row["total"])

    balance_due = max(0.0, total - paid_total)

    if total <= 0:
        status = "Draft"
    elif paid_total <= 0:
        status = "Unpaid"
    elif balance_due > 0:
        status = "Partial"
    else:
        status = "Paid"

    conn.execute(
        """
        UPDATE invoices
        SET subtotal = %s,
            total = %s,
            amount_paid = %s,
            balance_due = %s,
            status = %s
        WHERE id = %s
        """,
        (subtotal, total, paid_total, balance_due, status, invoice_id),
    )


def _sync_invoice_status_and_bookkeeping(invoice_id):
    conn = get_db_connection()
    try:
        recalc_invoice(conn, invoice_id)

        invoice = conn.execute(
            """
            SELECT i.*, c.name AS customer_name
            FROM invoices i
            LEFT JOIN customers c ON i.customer_id = c.id
            WHERE i.id = %s
            """,
            (invoice_id,),
        ).fetchone()

        if not invoice:
            conn.commit()
            return

        paid_row = conn.execute(
            """
            SELECT
                COALESCE(SUM(amount), 0) AS paid_total,
                MAX(payment_date) AS latest_payment_date
            FROM invoice_payments
            WHERE invoice_id = %s
            """,
            (invoice_id,),
        ).fetchone()

        paid_total = _safe_float(paid_row["paid_total"] if paid_row else 0)
        latest_payment_date = (
            paid_row["latest_payment_date"]
            if paid_row and paid_row["latest_payment_date"]
            else (invoice["invoice_date"] or date.today())
        )

        total = _safe_float(invoice["total"])
        balance_due = _safe_float(invoice["balance_due"])

        if total > 0 and balance_due <= 0 and paid_total > 0:
            new_status = "Paid"
        elif paid_total > 0:
            new_status = "Partial"
        elif total > 0:
            new_status = "Unpaid"
        else:
            new_status = "Draft"

        conn.execute(
            """
            UPDATE invoices
            SET status = %s
            WHERE id = %s
            """,
            (new_status, invoice_id),
        )

        if "job_id" in invoice.keys() and invoice["job_id"]:
            conn.execute(
                """
                UPDATE jobs
                SET status = %s
                WHERE id = %s AND company_id = %s
                """,
                (
                    "Finished" if new_status == "Paid" else "Invoiced",
                    invoice["job_id"],
                    invoice["company_id"],
                ),
            )

        if "quote_id" in invoice.keys() and invoice["quote_id"]:
            conn.execute(
                """
                UPDATE quotes
                SET status = %s
                WHERE id = %s AND company_id = %s
                """,
                (
                    "Finished" if new_status == "Paid" else "Converted",
                    invoice["quote_id"],
                    invoice["company_id"],
                ),
            )

        ledger_cols = _table_columns(conn, "ledger_entries") if "ledger_entries" in {
            row[0] if not hasattr(row, "keys") else row["table_name"]
            for row in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """
            ).fetchall()
        } else set()

        if not ledger_cols:
            conn.commit()
            return

        existing_ledger = None
        if "invoice_id" in ledger_cols:
            existing_ledger = conn.execute(
                """
                SELECT id
                FROM ledger_entries
                WHERE invoice_id = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (invoice_id,),
            ).fetchone()

        if paid_total <= 0:
            if existing_ledger:
                conn.execute(
                    "DELETE FROM ledger_entries WHERE id = %s",
                    (existing_ledger["id"],),
                )
            conn.commit()
            return

        ledger_data = {}

        if "company_id" in ledger_cols:
            ledger_data["company_id"] = invoice["company_id"]
        if "invoice_id" in ledger_cols:
            ledger_data["invoice_id"] = invoice_id
        if "customer_id" in ledger_cols:
            ledger_data["customer_id"] = invoice["customer_id"]
        if "entry_date" in ledger_cols:
            ledger_data["entry_date"] = latest_payment_date
        elif "date" in ledger_cols:
            ledger_data["date"] = latest_payment_date
        if "amount" in ledger_cols:
            ledger_data["amount"] = paid_total
        if "description" in ledger_cols:
            invoice_number = invoice["invoice_number"] or invoice["id"]
            customer_name = invoice["customer_name"] or "Customer"
            ledger_data["description"] = f"Invoice {invoice_number} payment - {customer_name}"
        if "type" in ledger_cols:
            ledger_data["type"] = "income"
        if "entry_type" in ledger_cols:
            ledger_data["entry_type"] = "income"
        if "category" in ledger_cols:
            ledger_data["category"] = "Invoice Payment"
        if "notes" in ledger_cols:
            ledger_data["notes"] = f"Synced from invoice #{invoice['invoice_number'] or invoice['id']}"
        if "reference" in ledger_cols:
            ledger_data["reference"] = f"INV-{invoice['invoice_number'] or invoice['id']}"
        if "status" in ledger_cols:
            ledger_data["status"] = "posted"

        if existing_ledger and ledger_data:
            set_clause = ", ".join(f"{col} = %s" for col in ledger_data.keys())
            values = list(ledger_data.values()) + [existing_ledger["id"]]
            conn.execute(
                f"UPDATE ledger_entries SET {set_clause} WHERE id = %s",
                values,
            )
        elif ledger_data:
            columns_sql = ", ".join(ledger_data.keys())
            placeholders_sql = ", ".join(["%s"] * len(ledger_data))
            conn.execute(
                f"INSERT INTO ledger_entries ({columns_sql}) VALUES ({placeholders_sql})",
                list(ledger_data.values()),
            )

        conn.commit()
    finally:
        conn.close()


# =========================================================
# Routes
# =========================================================

@invoices_bp.route("/invoices")
@login_required
@require_permission("can_manage_invoices")
def invoices():
    ensure_document_number_columns()
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT i.*, c.name AS customer_name
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.id
        WHERE i.company_id = %s
        ORDER BY i.id DESC
        """,
        (cid,),
    ).fetchall()

    conn.close()

    invoice_rows_html = ""
    for inv in rows:
        status = _clean_display(inv["status"])
        invoice_number = _clean_display(inv["invoice_number"] or inv["id"])
        invoice_rows_html += f"""
        <tr>
            <td>#{escape(str(invoice_number))}</td>
            <td>{escape(_clean_display(inv["customer_name"]))}</td>
            <td>{escape(str(inv["invoice_date"] or "-"))}</td>
            <td>${_safe_float(inv["total"]):,.2f}</td>
            <td>${_safe_float(inv["amount_paid"]):,.2f}</td>
            <td>${_safe_float(inv["balance_due"]):,.2f}</td>
            <td>{escape(status)}</td>
            <td>
                <a class='btn small' href='{url_for("invoices.view_invoice", invoice_id=inv["id"])}'>Open</a>
            </td>
        </tr>
        """

    if not invoice_rows_html:
        invoice_rows_html = """
        <tr>
            <td colspan="8" class="muted">No invoices found.</td>
        </tr>
        """

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; gap:12px; align-items:center; flex-wrap:wrap;'>
            <div>
                <h1>Invoices</h1>
                <p class='muted'>Track invoice totals, payments, balances, and status.</p>
            </div>
            <div>
                <a class='btn success' href='{url_for("invoices.new_invoice")}'>New Invoice</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <table class='table'>
            <thead>
                <tr>
                    <th>Invoice</th>
                    <th>Customer</th>
                    <th>Date</th>
                    <th>Total</th>
                    <th>Paid</th>
                    <th>Balance</th>
                    <th>Status</th>
                    <th></th>
                </tr>
            </thead>
            <tbody>
                {invoice_rows_html}
            </tbody>
        </table>
    </div>
    """
    return render_page(content, "Invoices")


@invoices_bp.route("/invoices/new", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_invoices")
def new_invoice():
    ensure_document_number_columns()
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]

    customers = conn.execute(
        """
        SELECT id, name, company, email
        FROM customers
        WHERE company_id = %s
        ORDER BY name
        """,
        (cid,),
    ).fetchall()

    company_row = conn.execute(
        """
        SELECT next_invoice_number
        FROM companies
        WHERE id = %s
        """,
        (cid,),
    ).fetchone()

    next_invoice_number_preview = "1001"
    if company_row and company_row["next_invoice_number"] is not None:
        next_invoice_number_preview = str(company_row["next_invoice_number"])

    customer_list = [
        {
            "id": c["id"],
            "name": _clean_text(c["name"]),
            "company": _clean_text(c["company"]),
            "email": _clean_text(c["email"]),
        }
        for c in customers
    ]

    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)
        invoice_number = _clean_text(request.form.get("invoice_number", ""))
        invoice_date = _clean_text(request.form.get("invoice_date", ""))
        due_date = _clean_text(request.form.get("due_date", ""))
        description = _clean_text(request.form.get("description", ""))
        status = _clean_text(request.form.get("status", "Unpaid")) or "Unpaid"

        total = _safe_float(request.form.get("total"))

        if not customer_id:
            conn.close()
            flash("Please select a customer.")
            return redirect(url_for("invoices.new_invoice"))

        if not invoice_number:
            conn.close()
            invoice_number = get_next_invoice_number(cid)
            conn = get_db_connection()

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO invoices (
                company_id,
                customer_id,
                invoice_number,
                invoice_date,
                due_date,
                notes,
                subtotal,
                total,
                amount_paid,
                balance_due,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                cid,
                customer_id,
                invoice_number,
                invoice_date or None,
                due_date or None,
                description,
                total,
                total,
                total if status == "Paid" else 0,
                0 if status == "Paid" else total,
                status,
            ),
        )

        row = cur.fetchone()
        if not row or "id" not in row:
            conn.rollback()
            conn.close()
            flash("Could not create invoice.")
            return redirect(url_for("invoices.new_invoice"))

        invoice_id = row["id"]

        if total > 0:
            conn.execute(
                """
                INSERT INTO invoice_items (
                    invoice_id,
                    description,
                    quantity,
                    unit,
                    unit_price,
                    line_total
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    invoice_id,
                    description or "Manual Invoice",
                    1,
                    "ea",
                    total,
                    total,
                ),
            )

        if status == "Paid" and total > 0:
            conn.execute(
                """
                INSERT INTO invoice_payments
                (company_id, invoice_id, payment_date, amount, payment_method, reference, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cid,
                    invoice_id,
                    invoice_date or date.today().isoformat(),
                    total,
                    "Manual Entry",
                    "",
                    "Invoice created as paid",
                ),
            )

        conn.commit()
        conn.close()

        _sync_invoice_status_and_bookkeeping(invoice_id)

        flash(f"Invoice #{invoice_number} created successfully.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    conn.close()

    content = f"""
    <style>
        .customer-search-wrap {{
            position: relative;
            width: 100%;
        }}

        .customer-search-wrap input {{
            width: 100%;
        }}

        .customer-results {{
            display: none;
            position: absolute;
            top: calc(100% + 4px);
            left: 0;
            right: 0;
            background: #fff;
            border: 1px solid #dbe2ea;
            border-radius: 10px;
            box-shadow: 0 8px 20px rgba(0,0,0,.08);
            z-index: 1000;
            max-height: 260px;
            overflow-y: auto;
            margin-top: 0;
            padding: 0;
        }}

        .customer-results.show {{
            display: block;
        }}

        .customer-result-item {{
            padding: 10px 12px;
            cursor: pointer;
            border-bottom: 1px solid #eef2f7;
        }}

        .customer-result-item:last-child {{
            border-bottom: none;
        }}

        .customer-result-item:hover {{
            background: #f8fbff;
        }}
    </style>

    <div class='card'>
        <h1>Create Invoice</h1>
        <p class='muted'>Create an invoice manually without converting a job.</p>
    </div>

    <div class='card'>
        <form method='post'>
            <div class='grid'>
                <div class='customer-search-wrap'>
                    <label>Customer</label>
                    <input type='text'
                           id='customer_search'
                           placeholder='Search customer name, company, or email...'
                           autocomplete='off'
                           required>
                    <input type='hidden' name='customer_id' id='customer_id' required>
                    <div id='customer_results' class='customer-results'></div>
                </div>

                <div>
                    <label>Invoice Number</label>
                    <input type='text' name='invoice_number' placeholder='Auto: {escape(next_invoice_number_preview)}'>
                    <div class='muted' style='margin-top:6px;'>Leave blank to auto-assign the next invoice number.</div>
                </div>

                <div>
                    <label>Invoice Date</label>
                    <input type='date' name='invoice_date' value='{date.today().isoformat()}'>
                </div>

                <div>
                    <label>Due Date</label>
                    <input type='date' name='due_date'>
                </div>

                <div>
                    <label>Status</label>
                    <select name='status'>
                        <option value='Unpaid'>Unpaid</option>
                        <option value='Paid'>Paid</option>
                        <option value='Partial'>Partial</option>
                    </select>
                </div>

                <div>
                    <label>Total</label>
                    <input type='number' step='0.01' min='0' name='total' placeholder='0.00' required>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Description / Notes</label>
                    <textarea name='description' placeholder='Material, labor, fuel, delivery, etc.'></textarea>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>Create Invoice</button>
                <a class='btn secondary' href='{url_for("invoices.invoices")}'>Back to Invoices</a>
            </div>
        </form>
    </div>

    <script>
        const customers = {json.dumps(customer_list)};
        const searchInput = document.getElementById("customer_search");
        const customerIdInput = document.getElementById("customer_id");
        const resultsBox = document.getElementById("customer_results");

        function escapeHtml(text) {{
            return String(text || "")
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }}

        function closeResults() {{
            resultsBox.innerHTML = "";
            resultsBox.classList.remove("show");
        }}

        function renderCustomerResults(matches) {{
            if (!matches.length) {{
                resultsBox.innerHTML = "<div class='customer-result-item muted'>No customers found</div>";
                resultsBox.classList.add("show");
                return;
            }}

            resultsBox.innerHTML = matches.map(c => `
                <div class="customer-result-item" data-id="${{c.id}}">
                    <strong>${{escapeHtml(c.name || "Unnamed Customer")}}</strong>
                    ${{c.company ? `<div class="muted small">${{escapeHtml(c.company)}}</div>` : ""}}
                    ${{c.email ? `<div class="muted small">${{escapeHtml(c.email)}}</div>` : ""}}
                </div>
            `).join("");

            resultsBox.classList.add("show");

            document.querySelectorAll(".customer-result-item[data-id]").forEach(item => {{
                item.addEventListener("click", function () {{
                    const id = this.dataset.id;
                    const customer = customers.find(x => String(x.id) === String(id));
                    if (!customer) return;

                    customerIdInput.value = customer.id;
                    searchInput.value = customer.company
                        ? `${{customer.name}} - ${{customer.company}}`
                        : (customer.name || "Unnamed Customer");

                    closeResults();
                }});
            }});
        }}

        searchInput.addEventListener("input", function () {{
            const q = this.value.trim().toLowerCase();
            customerIdInput.value = "";

            if (!q) {{
                closeResults();
                return;
            }}

            const matches = customers.filter(c =>
                (c.name && c.name.toLowerCase().includes(q)) ||
                (c.company && c.company.toLowerCase().includes(q)) ||
                (c.email && c.email.toLowerCase().includes(q))
            ).slice(0, 8);

            renderCustomerResults(matches);
        }});

        searchInput.addEventListener("focus", function () {{
            const q = this.value.trim().toLowerCase();
            if (!q) return;

            const matches = customers.filter(c =>
                (c.name && c.name.toLowerCase().includes(q)) ||
                (c.company && c.company.toLowerCase().includes(q)) ||
                (c.email && c.email.toLowerCase().includes(q))
            ).slice(0, 8);

            renderCustomerResults(matches);
        }});

        document.addEventListener("click", function (e) {{
            if (!e.target.closest(".customer-search-wrap")) {{
                closeResults();
            }}
        }});
    </script>
    """
    return render_page(content, "Create Invoice")


@invoices_bp.route("/invoices/<int:invoice_id>")
@login_required
@require_permission("can_manage_invoices")
def view_invoice(invoice_id):
    ensure_invoice_payment_table()

    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT i.*, c.name AS customer_name, c.company AS customer_company, c.email AS customer_email
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.id
        WHERE i.id = %s AND i.company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    items = conn.execute(
        """
        SELECT *
        FROM invoice_items
        WHERE invoice_id = %s
        ORDER BY id
        """,
        (invoice_id,),
    ).fetchall()

    payments = conn.execute(
        """
        SELECT *
        FROM invoice_payments
        WHERE invoice_id = %s AND company_id = %s
        ORDER BY payment_date DESC, id DESC
        """,
        (invoice_id, cid),
    ).fetchall()

    conn.close()

    item_rows = ""
    for item in items:
        item_rows += f"""
        <tr>
            <td>{escape(_clean_display(item["description"]))}</td>
            <td>{_safe_float(item["quantity"]):,.2f}</td>
            <td>{escape(_clean_display(item["unit"]))}</td>
            <td>${_safe_float(item["unit_price"]):,.2f}</td>
            <td>${_safe_float(item["line_total"]):,.2f}</td>
        </tr>
        """

    if not item_rows:
        item_rows = "<tr><td colspan='5' class='muted'>No invoice items found.</td></tr>"

    payment_rows = ""
    for p in payments:
        payment_rows += f"""
        <tr>
            <td>{escape(str(p["payment_date"] or "-"))}</td>
            <td>${_safe_float(p["amount"]):,.2f}</td>
            <td>{escape(_clean_display(p["payment_method"]))}</td>
            <td>{escape(_clean_display(p["reference"]))}</td>
            <td>
                <a class='btn small' href='{url_for("invoices.edit_invoice_payment", invoice_id=invoice_id, payment_id=p["id"])}'>Edit</a>
                <form method='post' action='{url_for("invoices.delete_invoice_payment", invoice_id=invoice_id, payment_id=p["id"])}' style='display:inline;'>
                    <button class='btn secondary small' type='submit'>Delete</button>
                </form>
            </td>
        </tr>
        """

    if not payment_rows:
        payment_rows = "<tr><td colspan='5' class='muted'>No payments recorded.</td></tr>"

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; gap:12px; flex-wrap:wrap; align-items:flex-start;'>
            <div>
                <h1>Invoice #{escape(str(invoice["invoice_number"] or invoice["id"]))}</h1>
                <p class='muted'>
                    <strong>Customer:</strong> {escape(_clean_display(invoice["customer_name"]))}<br>
                    <strong>Company:</strong> {escape(_clean_display(invoice["customer_company"]))}<br>
                    <strong>Email:</strong> {escape(_clean_display(invoice["customer_email"]))}<br>
                    <strong>Invoice Date:</strong> {escape(str(invoice["invoice_date"] or "-"))}<br>
                    <strong>Due Date:</strong> {escape(str(invoice["due_date"] or "-"))}<br>
                    <strong>Status:</strong> {escape(_clean_display(invoice["status"]))}
                </p>
            </div>

            <div style='display:flex; gap:8px; flex-wrap:wrap;'>
                <a class='btn secondary' href='{url_for("invoices.invoices")}'>Back</a>

                <form method='post' action='{url_for("invoices.mark_invoice_paid", invoice_id=invoice_id)}' style='display:inline;'>
                    <button class='btn success' type='submit'>Mark Paid</button>
                </form>

                <form method='post' action='{url_for("invoices.mark_invoice_unpaid", invoice_id=invoice_id)}' style='display:inline;'>
                    <button class='btn secondary' type='submit'>Mark Unpaid</button>
                </form>

                <form method='post'
                      action='{url_for("invoices.delete_invoice", invoice_id=invoice_id)}'
                      style='display:inline;'
                      onsubmit="return confirm('Delete this invoice? This will also remove its items and payments.');">
                    <button class='btn danger' type='submit'>Delete Invoice</button>
                </form>
            </div>
        </div>
    </div>

    <div class='stats-grid'>
        <div class='card stat-card'>
            <div class='stat-label'>Total</div>
            <div class='stat-value'>${_safe_float(invoice["total"]):,.2f}</div>
        </div>
        <div class='card stat-card'>
            <div class='stat-label'>Paid</div>
            <div class='stat-value' style='color:#16a34a;'>${_safe_float(invoice["amount_paid"]):,.2f}</div>
        </div>
        <div class='card stat-card'>
            <div class='stat-label'>Balance Due</div>
            <div class='stat-value' style='color:#dc2626;'>${_safe_float(invoice["balance_due"]):,.2f}</div>
        </div>
    </div>

    <div class='card'>
        <h2>Add Payment</h2>
        <p class='muted'>Use this for partial payments. Example: if a $500 invoice gets a $200 payment, TerraLedger will mark it as Partial and leave $300 due.</p>

        <form method='post' action='{url_for("invoices.add_invoice_payment", invoice_id=invoice_id)}'>
            <div class='grid'>
                <div>
                    <label>Amount</label>
                    <input type='number' step='0.01' min='0.01' name='amount' required>
                </div>
                <div>
                    <label>Payment Date</label>
                    <input type='date' name='payment_date' value='{date.today().isoformat()}'>
                </div>
                <div>
                    <label>Payment Method</label>
                    <input name='payment_method' placeholder='Cash, Check, Card, ACH'>
                </div>
                <div>
                    <label>Reference</label>
                    <input name='reference' placeholder='Check # or transaction ID'>
                </div>
                <div style='grid-column:1 / -1;'>
                    <label>Notes</label>
                    <textarea name='notes'></textarea>
                </div>
            </div>

            <br>
            <button class='btn success' type='submit'>Record Payment</button>
        </form>
    </div>

    <div class='card'>
        <h2>Invoice Items</h2>
        <table class='table'>
            <thead>
                <tr>
                    <th>Description</th>
                    <th>Qty</th>
                    <th>Unit</th>
                    <th>Unit Price</th>
                    <th>Line Total</th>
                </tr>
            </thead>
            <tbody>
                {item_rows}
            </tbody>
        </table>
    </div>

    <div class='card'>
        <h2>Payment History</h2>
        <table class='table'>
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Amount</th>
                    <th>Method</th>
                    <th>Reference</th>
                    <th></th>
                </tr>
            </thead>
            <tbody>
                {payment_rows}
            </tbody>
        </table>
    </div>

    <div class='card'>
        <h2>Notes</h2>
        <p>{escape(_clean_display(invoice["notes"]))}</p>
    </div>
    """
    return render_page(content, f"Invoice #{invoice['invoice_number'] or invoice_id}")


@invoices_bp.route("/invoices/<int:invoice_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def delete_invoice(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        abort(404)

    conn.execute("DELETE FROM invoice_payments WHERE invoice_id = %s AND company_id = %s", (invoice_id, cid))
    conn.execute("DELETE FROM invoice_items WHERE invoice_id = %s", (invoice_id,))
    conn.execute("DELETE FROM invoices WHERE id = %s AND company_id = %s", (invoice_id, cid))

    ledger_tables = {
        row[0] if not hasattr(row, "keys") else row["table_name"]
        for row in conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
        ).fetchall()
    }
    if "ledger_entries" in ledger_tables:
        ledger_cols = _table_columns(conn, "ledger_entries")
        if "invoice_id" in ledger_cols:
            conn.execute("DELETE FROM ledger_entries WHERE invoice_id = %s", (invoice_id,))

    conn.commit()
    conn.close()

    flash("Invoice deleted.")
    return redirect(url_for("invoices.invoices"))


@invoices_bp.route("/invoices/<int:invoice_id>/add_payment", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def add_invoice_payment(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT id, total, balance_due, customer_id, invoice_number
        FROM invoices
        WHERE id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        flash("Invoice not found.")
        return redirect(url_for("invoices.invoices"))

    amount = request.form.get("amount", type=float) or 0
    payment_date = request.form.get("payment_date") or date.today().isoformat()
    payment_method = _clean_text(request.form.get("payment_method"))
    reference = _clean_text(request.form.get("reference"))
    notes = _clean_text(request.form.get("notes"))

    if amount <= 0:
        conn.close()
        flash("Payment amount must be greater than 0.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    if amount > _safe_float(invoice["balance_due"]):
        conn.close()
        flash("Payment cannot exceed the remaining balance.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    conn.execute(
        """
        INSERT INTO invoice_payments
        (company_id, invoice_id, payment_date, amount, payment_method, reference, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (cid, invoice_id, payment_date, amount, payment_method, reference, notes),
    )

    conn.commit()
    conn.close()

    _sync_invoice_status_and_bookkeeping(invoice_id)

    flash("Payment recorded.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/payments/<int:payment_id>/edit", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_invoices")
def edit_invoice_payment(invoice_id, payment_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        flash("Invoice not found.")
        return redirect(url_for("invoices.invoices"))

    payment = conn.execute(
        """
        SELECT *
        FROM invoice_payments
        WHERE id = %s AND invoice_id = %s AND company_id = %s
        """,
        (payment_id, invoice_id, cid),
    ).fetchone()

    if not payment:
        conn.close()
        flash("Payment not found.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    if request.method == "POST":
        amount = request.form.get("amount", type=float) or 0
        payment_date = request.form.get("payment_date") or date.today().isoformat()
        payment_method = _clean_text(request.form.get("payment_method"))
        reference = _clean_text(request.form.get("reference"))
        notes = _clean_text(request.form.get("notes"))

        if amount <= 0:
            conn.close()
            flash("Payment amount must be greater than 0.")
            return redirect(url_for("invoices.edit_invoice_payment", invoice_id=invoice_id, payment_id=payment_id))

        other_paid_row = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS other_paid_total
            FROM invoice_payments
            WHERE invoice_id = %s AND id != %s
            """,
            (invoice_id, payment_id),
        ).fetchone()

        other_paid_total = _safe_float(other_paid_row["other_paid_total"] if other_paid_row else 0)
        invoice_total = _safe_float(invoice["total"])

        if other_paid_total + amount > invoice_total:
            conn.close()
            flash("Payment total cannot exceed the invoice total.")
            return redirect(url_for("invoices.edit_invoice_payment", invoice_id=invoice_id, payment_id=payment_id))

        conn.execute(
            """
            UPDATE invoice_payments
            SET payment_date = %s, amount = %s, payment_method = %s, reference = %s, notes = %s
            WHERE id = %s AND invoice_id = %s AND company_id = %s
            """,
            (payment_date, amount, payment_method, reference, notes, payment_id, invoice_id, cid),
        )
        conn.commit()
        conn.close()

        _sync_invoice_status_and_bookkeeping(invoice_id)

        flash("Payment updated.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    payment_method_val = escape(payment["payment_method"] or "", quote=True)
    reference_val = escape(payment["reference"] or "", quote=True)
    notes_val = escape(payment["notes"] or "")
    payment_date_val = payment["payment_date"] or date.today().isoformat()
    amount_val = payment["amount"] or 0

    conn.close()

    content = f"""
    <div class='card'>
        <h1>Edit Payment</h1>
        <form method='post'>
            <div class='grid'>
                <div>
                    <label>Amount</label>
                    <input type='number' step='0.01' min='0.01' name='amount' value='{amount_val}' required>
                </div>
                <div>
                    <label>Payment Date</label>
                    <input type='date' name='payment_date' value='{payment_date_val}'>
                </div>
                <div>
                    <label>Payment Method</label>
                    <input name='payment_method' value='{payment_method_val}' placeholder='Cash, Check, Card, ACH'>
                </div>
                <div>
                    <label>Reference</label>
                    <input name='reference' value='{reference_val}' placeholder='Check # or transaction ID'>
                </div>
            </div>

            <br>
            <label>Notes</label>
            <textarea name='notes'>{notes_val}</textarea>
            <br>

            <div class='row-actions'>
                <a class='btn secondary' href='{url_for("invoices.view_invoice", invoice_id=invoice_id)}'>Cancel</a>
                <button class='btn success' type='submit'>Save Changes</button>
            </div>
        </form>
    </div>
    """
    return render_page(content, "Edit Payment")


@invoices_bp.route("/invoices/<int:invoice_id>/payments/<int:payment_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def delete_invoice_payment(invoice_id, payment_id):
    conn = get_db_connection()
    cid = session["company_id"]

    payment = conn.execute(
        """
        SELECT *
        FROM invoice_payments
        WHERE id = %s AND invoice_id = %s AND company_id = %s
        """,
        (payment_id, invoice_id, cid),
    ).fetchone()

    if not payment:
        conn.close()
        flash("Payment not found.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    conn.execute(
        """
        DELETE FROM invoice_payments
        WHERE id = %s AND invoice_id = %s AND company_id = %s
        """,
        (payment_id, invoice_id, cid),
    )
    conn.commit()
    conn.close()

    _sync_invoice_status_and_bookkeeping(invoice_id)

    flash("Payment deleted.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/mark_paid", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def mark_invoice_paid(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        flash("Invoice not found.")
        return redirect(url_for("invoices.invoices"))

    remaining_balance = _safe_float(invoice["balance_due"])

    if remaining_balance <= 0:
        conn.close()
        flash("Invoice is already fully paid.")
        return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))

    conn.execute(
        """
        INSERT INTO invoice_payments
        (company_id, invoice_id, payment_date, amount, payment_method, reference, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            cid,
            invoice_id,
            date.today().isoformat(),
            remaining_balance,
            "Mark Paid",
            "",
            "Remaining balance marked paid",
        ),
    )

    conn.commit()
    conn.close()

    _sync_invoice_status_and_bookkeeping(invoice_id)

    flash("Invoice marked paid.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))


@invoices_bp.route("/invoices/<int:invoice_id>/mark_unpaid", methods=["POST"])
@login_required
@require_permission("can_manage_invoices")
def mark_invoice_unpaid(invoice_id):
    conn = get_db_connection()
    cid = session["company_id"]

    invoice = conn.execute(
        """
        SELECT *
        FROM invoices
        WHERE id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    ).fetchone()

    if not invoice:
        conn.close()
        flash("Invoice not found.")
        return redirect(url_for("invoices.invoices"))

    conn.execute(
        """
        DELETE FROM invoice_payments
        WHERE invoice_id = %s AND company_id = %s
        """,
        (invoice_id, cid),
    )

    conn.commit()
    conn.close()

    _sync_invoice_status_and_bookkeeping(invoice_id)

    flash("Invoice marked unpaid and payment history cleared.")
    return redirect(url_for("invoices.view_invoice", invoice_id=invoice_id))