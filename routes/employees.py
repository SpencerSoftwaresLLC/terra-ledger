from flask import Blueprint, request, redirect, url_for, session, flash
from html import escape
import sqlite3

from ..db import (
    get_db_connection,
    ensure_employee_status_column,
    ensure_employee_name_columns,
    ensure_employee_payroll_columns,
    ensure_employee_tax_columns,
    get_employee_columns,
)
from ..decorators import login_required, require_permission, subscription_required
from ..page_helpers import render_page


employees_bp = Blueprint("employees", __name__)


def _employee_display_name(employee):
    cols = employee.keys()

    first_name = employee["first_name"] if "first_name" in cols and employee["first_name"] else ""
    last_name = employee["last_name"] if "last_name" in cols and employee["last_name"] else ""
    full_name = employee["full_name"] if "full_name" in cols and employee["full_name"] else ""
    single_name = employee["name"] if "name" in cols and employee["name"] else ""
    employee_name_field = employee["employee_name"] if "employee_name" in cols and employee["employee_name"] else ""
    display_name = employee["display_name"] if "display_name" in cols and employee["display_name"] else ""

    if first_name or last_name:
        return f"{first_name} {last_name}".strip()
    if full_name:
        return full_name
    if single_name:
        return single_name
    if employee_name_field:
        return employee_name_field
    if display_name:
        return display_name
    return f"Employee #{employee['id']}"


def _employee_form_html(employee=None, form_action="", submit_label="Save Employee", page_title="Employee"):
    employee = employee or {}

    def val(key, default=""):
        if hasattr(employee, "keys"):
            value = employee[key] if key in employee.keys() and employee[key] is not None else default
        else:
            value = employee.get(key, default)
        return escape(str(value))

    def selected(key, expected, default=""):
        if hasattr(employee, "keys"):
            current = employee[key] if key in employee.keys() and employee[key] is not None else default
        else:
            current = employee.get(key, default)
        return "selected" if str(current) == str(expected) else ""

    def checked(key):
        if hasattr(employee, "keys"):
            return "checked" if key in employee.keys() and employee[key] else ""
        return "checked" if employee.get(key) else ""

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>{escape(page_title)}</h1>
                <p class='muted' style='margin:0;'>Manage employee information, payroll setup, and federal withholding details.</p>
            </div>
            <div class='row-actions'>
                <a class='btn warning' href='{url_for("payroll.employee_payroll")}'>Payroll</a>
                <a class='btn secondary' href='{url_for("employees.employees")}'>Back to Employees</a>
            </div>
        </div>
    </div>

    <form method='post' action='{form_action}'>
        <div class='card'>
            <h2>Employee Information</h2>
            <div class='grid'>
                <div>
                    <label>First Name</label>
                    <input name='first_name' value='{val("first_name")}' required>
                </div>
                <div>
                    <label>Last Name</label>
                    <input name='last_name' value='{val("last_name")}' required>
                </div>
                <div>
                    <label>Phone</label>
                    <input name='phone' value='{val("phone")}'>
                </div>
                <div>
                    <label>Email</label>
                    <input name='email' type='email' value='{val("email")}'>
                </div>
                <div>
                    <label>Position</label>
                    <input name='position' value='{val("position")}'>
                </div>
                <div>
                    <label>Hire Date</label>
                    <input name='hire_date' type='date' value='{val("hire_date")}'>
                </div>
            </div>
        </div>

        <div class='card'>
            <h2>Payroll Setup</h2>
            <div class='grid'>
                <div>
                    <label>Pay Type</label>
                    <select name='pay_type'>
                        <option value='Hourly' {selected("pay_type", "Hourly", "Hourly")}>Hourly</option>
                        <option value='Salary' {selected("pay_type", "Salary")}>Salary</option>
                    </select>
                </div>
                <div>
                    <label>Hourly Rate</label>
                    <input name='hourly_rate' type='number' step='0.01' value='{val("hourly_rate", "0")}'>
                </div>
                <div>
                    <label>Overtime Rate</label>
                    <input name='overtime_rate' type='number' step='0.01' value='{val("overtime_rate", "0")}'>
                </div>
                <div>
                    <label>Salary Amount</label>
                    <input name='salary_amount' type='number' step='0.01' value='{val("salary_amount", "0")}'>
                </div>
                <div>
                    <label>Default Hours</label>
                    <input name='default_hours' type='number' step='0.01' value='{val("default_hours", "0")}'>
                </div>
                <div>
                    <label>Pay Frequency</label>
                    <select name='pay_frequency'>
                        <option value='Weekly' {selected("pay_frequency", "Weekly")}>Weekly</option>
                        <option value='Biweekly' {selected("pay_frequency", "Biweekly", "Biweekly")}>Biweekly</option>
                        <option value='Semimonthly' {selected("pay_frequency", "Semimonthly")}>Semimonthly</option>
                        <option value='Monthly' {selected("pay_frequency", "Monthly")}>Monthly</option>
                    </select>
                </div>
            </div>

            <div style='margin-top:16px;'>
                <label>Payroll Notes</label>
                <textarea name='payroll_notes'>{val("payroll_notes")}</textarea>
            </div>
        </div>

        <div class='card'>
            <h2>Federal Tax / W-4</h2>
            <div class='grid'>
                <div>
                    <label>Federal Filing Status</label>
                    <select name='federal_filing_status'>
                        <option value='Single' {selected("federal_filing_status", "Single", "Single")}>Single</option>
                        <option value='Married Filing Jointly' {selected("federal_filing_status", "Married Filing Jointly")}>Married Filing Jointly</option>
                        <option value='Married Filing Separately' {selected("federal_filing_status", "Married Filing Separately")}>Married Filing Separately</option>
                        <option value='Head of Household' {selected("federal_filing_status", "Head of Household")}>Head of Household</option>
                    </select>
                </div>

                <div class='checkbox-field'>
                    <label class='checkbox-label'>
                        <input type='checkbox' name='w4_step2_checked' {checked("w4_step2_checked")}>
                        Step 2 Box Checked
                    </label>
                    <div class='muted small'>Check if employee has multiple jobs or spouse works.</div>
                </div>

                <div>
                    <label>Step 3 Credits</label>
                    <input name='w4_step3_amount' type='number' step='0.01' value='{val("w4_step3_amount", "0")}'>
                </div>

                <div>
                    <label>Step 4(a) Other Income</label>
                    <input name='w4_step4a_other_income' type='number' step='0.01' value='{val("w4_step4a_other_income", "0")}'>
                </div>

                <div>
                    <label>Step 4(b) Deductions</label>
                    <input name='w4_step4b_deductions' type='number' step='0.01' value='{val("w4_step4b_deductions", "0")}'>
                </div>

                <div>
                    <label>Step 4(c) Extra Withholding</label>
                    <input name='w4_step4c_extra_withholding' type='number' step='0.01' value='{val("w4_step4c_extra_withholding", "0")}'>
                </div>
            </div>
        </div>

        <div class='card'>
            <div class='row-actions'>
                <button class='btn success' type='submit'>{escape(submit_label)}</button>
                <a class='btn secondary' href='{url_for("employees.employees")}'>Cancel</a>
            </div>
        </div>
    </form>
    """
    return content


@employees_bp.route("/employees")
@login_required
@subscription_required
@require_permission("can_manage_employees")
def employees():
    ensure_employee_status_column()
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()

    conn = get_db_connection()
    cid = session["company_id"]
    show = (request.args.get("show", "active") or "active").strip().lower()

    cols = get_employee_columns()

    if "full_name" in cols:
        name_col = "full_name"
    elif "name" in cols:
        name_col = "name"
    elif "employee_name" in cols:
        name_col = "employee_name"
    else:
        name_col = "id"

    phone_col = "phone" if "phone" in cols else None
    email_col = "email" if "email" in cols else None

    if show == "all":
        rows = conn.execute(
            f"""
            SELECT *
            FROM employees
            WHERE company_id = ?
            ORDER BY is_active DESC, {name_col} ASC, id DESC
            """,
            (cid,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT *
            FROM employees
            WHERE company_id = ? AND is_active = 1
            ORDER BY {name_col} ASC, id DESC
            """,
            (cid,),
        ).fetchall()

    conn.close()

    employee_rows = "".join(
        f"""
        <tr>
            <td>{escape(str(r[name_col])) if name_col != 'id' and r[name_col] else f"Employee #{r['id']}"}</td>
            <td>{escape(str(r[phone_col])) if phone_col and r[phone_col] else '-'}</td>
            <td>{escape(str(r[email_col])) if email_col and r[email_col] else '-'}</td>
            <td>{escape(str(r['position'])) if 'position' in r.keys() and r['position'] else '-'}</td>
            <td>{escape(str(r['pay_type'])) if 'pay_type' in r.keys() and r['pay_type'] else '-'}</td>
            <td>{
                f"${float(r['salary_amount'] or 0):.2f}/yr" if 'pay_type' in r.keys() and r['pay_type'] == 'Salary'
                else f"${float(r['hourly_rate'] or 0):.2f}/hr" if 'hourly_rate' in r.keys() and r['hourly_rate'] is not None
                else '-'
            }</td>
            <td>{"Active" if r['is_active'] else "Inactive"}</td>
            <td>
                <div class='row-actions'>
                    <a class='btn secondary small' href='{url_for("employees.view_employee", employee_id=r["id"])}'>View</a>

                    {
                        f"<form method='post' action='{url_for('employees.deactivate_employee', employee_id=r['id'])}' class='inline-form'>"
                        f"<button class='btn warning small' type='submit'>Set Inactive</button>"
                        f"</form>"
                        if r['is_active']
                        else
                        f"<form method='post' action='{url_for('employees.activate_employee', employee_id=r['id'])}' class='inline-form'>"
                        f"<button class='btn success small' type='submit'>Set Active</button>"
                        f"</form>"
                    }

                    <form method='post'
                        action='{url_for("employees.delete_employee", employee_id=r["id"])}'
                        class='inline-form'
                        onsubmit="return confirm('Delete this employee? This cannot be undone.');">
                        <button class='btn danger small' type='submit'>Delete</button>
                    </form>
                </div>
            </td>
        </tr>
        """
        for r in rows
    )

    active_btn_class = "btn" if show != "all" else "btn secondary"
    all_btn_class = "btn" if show == "all" else "btn secondary"

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Employees</h1>
                <p class='muted' style='margin:0;'>Manage active and inactive employees.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("employees.employees", show="active")}' class='{active_btn_class}'>Active Employees</a>
                <a href='{url_for("employees.employees", show="all")}' class='{all_btn_class}'>All Employees</a>
                <a href='{url_for("payroll.employee_payroll")}' class='btn warning'>Payroll</a>
                <a href='{url_for("employees.new_employee")}' class='btn success'>+ New Employee</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>{'All Employees' if show == 'all' else 'Active Employees'}</h2>
        <table>
            <tr>
                <th>Name</th>
                <th>Phone</th>
                <th>Email</th>
                <th>Position</th>
                <th>Pay Type</th>
                <th>Rate / Salary</th>
                <th>Status</th>
                <th>Actions</th>
            </tr>
            {employee_rows or "<tr><td colspan='8' class='muted'>No employees found.</td></tr>"}
        </table>
    </div>
    """
    return render_page(content, "Employees")


@employees_bp.route("/employees/new", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_employees")
def new_employee():
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_employee_status_column()

    conn = get_db_connection()
    cid = session["company_id"]

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(employees)")
    cols = [row[1] for row in cur.fetchall()]

    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        email = (request.form.get("email") or "").strip()
        position = (request.form.get("position") or "").strip()
        hire_date = (request.form.get("hire_date") or "").strip()
        pay_type = (request.form.get("pay_type") or "Hourly").strip()

        federal_filing_status = (request.form.get("federal_filing_status") or "Single").strip()
        pay_frequency = (request.form.get("pay_frequency") or "Biweekly").strip()
        w4_step2_checked = 1 if request.form.get("w4_step2_checked") else 0
        w4_step3_amount = float(request.form.get("w4_step3_amount") or 0)
        w4_step4a_other_income = float(request.form.get("w4_step4a_other_income") or 0)
        w4_step4b_deductions = float(request.form.get("w4_step4b_deductions") or 0)
        w4_step4c_extra_withholding = float(request.form.get("w4_step4c_extra_withholding") or 0)

        hourly_rate = float(request.form.get("hourly_rate") or 0)
        overtime_rate = float(request.form.get("overtime_rate") or 0)
        salary_amount = float(request.form.get("salary_amount") or 0)
        default_hours = float(request.form.get("default_hours") or 0)
        payroll_notes = (request.form.get("payroll_notes") or "").strip()

        if not first_name or not last_name:
            flash("First and last name are required.")
            conn.close()
            return render_page(
                _employee_form_html(
                    form_action=url_for("employees.new_employee"),
                    submit_label="Save Employee",
                    page_title="Add Employee",
                ),
                "Add Employee",
            )

        full_name = f"{first_name} {last_name}".strip()

        data = {
            "company_id": cid,
            "first_name": first_name,
            "last_name": last_name,
            "full_name": full_name,
            "phone": phone,
            "email": email,
            "position": position,
            "hire_date": hire_date,
            "pay_type": pay_type,
            "hourly_rate": hourly_rate,
            "overtime_rate": overtime_rate,
            "salary_amount": salary_amount,
            "default_hours": default_hours,
            "payroll_notes": payroll_notes,
            "is_active": 1,
            "federal_filing_status": federal_filing_status,
            "pay_frequency": pay_frequency,
            "w4_step2_checked": w4_step2_checked,
            "w4_step3_amount": w4_step3_amount,
            "w4_step4a_other_income": w4_step4a_other_income,
            "w4_step4b_deductions": w4_step4b_deductions,
            "w4_step4c_extra_withholding": w4_step4c_extra_withholding,
        }

        insert_cols = []
        insert_vals = []

        for col in [
            "company_id",
            "first_name",
            "last_name",
            "full_name",
            "phone",
            "email",
            "position",
            "hire_date",
            "pay_type",
            "hourly_rate",
            "overtime_rate",
            "salary_amount",
            "default_hours",
            "payroll_notes",
            "is_active",
            "federal_filing_status",
            "pay_frequency",
            "w4_step2_checked",
            "w4_step3_amount",
            "w4_step4a_other_income",
            "w4_step4b_deductions",
            "w4_step4c_extra_withholding",
        ]:
            if col in cols:
                insert_cols.append(col)
                insert_vals.append(data[col])

        placeholders = ",".join(["?"] * len(insert_cols))
        col_sql = ",".join(insert_cols)

        conn.execute(
            f"INSERT INTO employees ({col_sql}) VALUES ({placeholders})",
            insert_vals,
        )
        conn.commit()
        conn.close()

        flash("Employee added successfully.")
        return redirect(url_for("employees.employees"))

    conn.close()
    return render_page(
        _employee_form_html(
            form_action=url_for("employees.new_employee"),
            submit_label="Save Employee",
            page_title="Add Employee",
        ),
        "Add Employee",
    )


@employees_bp.route("/employees/<int:employee_id>")
@login_required
@require_permission("can_view_employees")
def view_employee(employee_id):
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        """
        SELECT *
        FROM employees
        WHERE id = ? AND company_id = ?
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash("Employee not found.")
        return redirect(url_for("employees.employees"))

    try:
        payroll_rows = conn.execute(
            """
            SELECT
                id,
                pay_date,
                pay_period_start,
                pay_period_end,
                pay_type,
                hours_regular,
                hours_overtime,
                gross_pay,
                net_pay
            FROM payroll_entries
            WHERE employee_id = ? AND company_id = ?
            ORDER BY
                CASE WHEN pay_date IS NULL OR pay_date = '' THEN 1 ELSE 0 END,
                pay_date DESC,
                id DESC
            """,
            (employee_id, cid),
        ).fetchall()
    except sqlite3.OperationalError:
        payroll_rows = []

    conn.close()

    cols = employee.keys()
    employee_name = _employee_display_name(employee)

    phone = employee["phone"] if "phone" in cols and employee["phone"] else "-"
    email = employee["email"] if "email" in cols and employee["email"] else "-"
    position = employee["position"] if "position" in cols and employee["position"] else "-"
    pay_type = employee["pay_type"] if "pay_type" in cols and employee["pay_type"] else "-"
    hire_date = employee["hire_date"] if "hire_date" in cols and employee["hire_date"] else "-"
    payroll_notes = employee["payroll_notes"] if "payroll_notes" in cols and employee["payroll_notes"] else "-"
    default_hours = employee["default_hours"] if "default_hours" in cols and employee["default_hours"] is not None else "-"
    hourly_rate = employee["hourly_rate"] if "hourly_rate" in cols and employee["hourly_rate"] is not None else 0
    overtime_rate = employee["overtime_rate"] if "overtime_rate" in cols and employee["overtime_rate"] is not None else 0
    salary_amount = employee["salary_amount"] if "salary_amount" in cols and employee["salary_amount"] is not None else 0
    status_text = "Active" if ("is_active" in cols and employee["is_active"]) else "Inactive"

    federal_filing_status = (
        employee["federal_filing_status"]
        if "federal_filing_status" in cols and employee["federal_filing_status"]
        else "Single"
    )
    pay_frequency = (
        employee["pay_frequency"]
        if "pay_frequency" in cols and employee["pay_frequency"]
        else "-"
    )
    w4_step2_checked = "Yes" if "w4_step2_checked" in cols and employee["w4_step2_checked"] else "No"
    w4_step3_amount = float(employee["w4_step3_amount"]) if "w4_step3_amount" in cols and employee["w4_step3_amount"] is not None else 0.0
    w4_step4a_other_income = float(employee["w4_step4a_other_income"]) if "w4_step4a_other_income" in cols and employee["w4_step4a_other_income"] is not None else 0.0
    w4_step4b_deductions = float(employee["w4_step4b_deductions"]) if "w4_step4b_deductions" in cols and employee["w4_step4b_deductions"] is not None else 0.0
    w4_step4c_extra_withholding = float(employee["w4_step4c_extra_withholding"]) if "w4_step4c_extra_withholding" in cols and employee["w4_step4c_extra_withholding"] is not None else 0.0

    if pay_type == "Salary":
        pay_display = f"${salary_amount:,.2f} / year"
    elif pay_type == "Hourly":
        pay_display = f"${hourly_rate:,.2f} / hour"
    else:
        pay_display = "-"

    payroll_history_rows = "".join(
        f"""
        <tr>
            <td>{escape(str(r['pay_date'] or '-'))}</td>
            <td>{escape(str(r['pay_period_start'] or '-'))} to {escape(str(r['pay_period_end'] or '-'))}</td>
            <td>{escape(str(r['pay_type'] or '-'))}</td>
            <td>{float(r['hours_regular'] or 0):.2f}</td>
            <td>{float(r['hours_overtime'] or 0):.2f}</td>
            <td>${float(r['gross_pay'] or 0):.2f}</td>
            <td>${float(r['net_pay'] or 0):.2f}</td>
        </tr>
        """
        for r in payroll_rows
    )

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>{escape(employee_name)}</h1>
                <p class='muted' style='margin:0;'>Employee details, payroll profile, and tax setup.</p>
            </div>
            <div class='row-actions'>
                <a class='btn' href='{url_for("employees.edit_employee", employee_id=employee_id)}'>Edit Employee</a>
                <a class='btn warning' href='{url_for("payroll.employee_payroll")}'>Payroll</a>
                <a class='btn secondary' href='{url_for("employees.employees")}'>Back to Employees</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Employee Information</h2>
        <div class='grid'>
            <div><strong>Name</strong><br>{escape(employee_name)}</div>
            <div><strong>Status</strong><br>{escape(status_text)}</div>
            <div><strong>Phone</strong><br>{escape(str(phone))}</div>
            <div><strong>Email</strong><br>{escape(str(email))}</div>
            <div><strong>Position</strong><br>{escape(str(position))}</div>
            <div><strong>Hire Date</strong><br>{escape(str(hire_date))}</div>
        </div>
    </div>

    <div class='card'>
        <h2>Payroll Setup</h2>
        <div class='grid'>
            <div><strong>Pay Type</strong><br>{escape(str(pay_type))}</div>
            <div><strong>Rate / Salary</strong><br>{pay_display}</div>
            <div><strong>Overtime Rate</strong><br>${overtime_rate:,.2f}</div>
            <div><strong>Default Hours</strong><br>{escape(str(default_hours))}</div>
            <div><strong>Pay Frequency</strong><br>{escape(str(pay_frequency))}</div>
        </div>

        <div style='margin-top:18px;'>
            <strong>Payroll Notes</strong><br>
            <div class='muted' style='margin-top:6px;'>{escape(str(payroll_notes))}</div>
        </div>
    </div>

    <div class='card'>
        <h2>Federal Tax / W-4</h2>
        <div class='grid'>
            <div><strong>Federal Filing Status</strong><br>{escape(str(federal_filing_status))}</div>
            <div><strong>Step 2 Box Checked</strong><br>{escape(w4_step2_checked)}</div>
            <div><strong>Step 3 Credits</strong><br>${w4_step3_amount:,.2f}</div>
            <div><strong>Step 4(a) Other Income</strong><br>${w4_step4a_other_income:,.2f}</div>
            <div><strong>Step 4(b) Deductions</strong><br>${w4_step4b_deductions:,.2f}</div>
            <div><strong>Step 4(c) Extra Withholding</strong><br>${w4_step4c_extra_withholding:,.2f}</div>
        </div>
    </div>

    <div class='card'>
        <div class='section-head'>
            <h2>Payroll History</h2>
            <a class='btn small' href='{url_for("payroll.employee_payroll")}'>Open Payroll</a>
        </div>
        <table>
            <tr>
                <th>Pay Date</th>
                <th>Pay Period</th>
                <th>Pay Type</th>
                <th>Reg Hours</th>
                <th>OT Hours</th>
                <th>Gross Pay</th>
                <th>Net Pay</th>
            </tr>
            {payroll_history_rows or "<tr><td colspan='7' class='muted'>No payroll history found.</td></tr>"}
        </table>
    </div>
    """

    return render_page(content, employee_name)


@employees_bp.route("/employees/<int:employee_id>/edit", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_employees")
def edit_employee(employee_id):
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        "SELECT * FROM employees WHERE id = ? AND company_id = ?",
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash("Employee not found.")
        return redirect(url_for("employees.employees"))

    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        email = (request.form.get("email") or "").strip()
        position = (request.form.get("position") or "").strip()
        hire_date = (request.form.get("hire_date") or "").strip()

        if not first_name or not last_name:
            conn.close()
            flash("First and last name are required.")
            return redirect(url_for("employees.edit_employee", employee_id=employee_id))

        pay_type = (request.form.get("pay_type") or "Hourly").strip()
        hourly_rate = float(request.form.get("hourly_rate") or 0)
        overtime_rate = float(request.form.get("overtime_rate") or 0)
        salary_amount = float(request.form.get("salary_amount") or 0)
        default_hours = float(request.form.get("default_hours") or 0)
        payroll_notes = (request.form.get("payroll_notes") or "").strip()

        federal_filing_status = (request.form.get("federal_filing_status") or "Single").strip()
        pay_frequency = (request.form.get("pay_frequency") or "Biweekly").strip()
        w4_step2_checked = 1 if request.form.get("w4_step2_checked") else 0
        w4_step3_amount = float(request.form.get("w4_step3_amount") or 0)
        w4_step4a_other_income = float(request.form.get("w4_step4a_other_income") or 0)
        w4_step4b_deductions = float(request.form.get("w4_step4b_deductions") or 0)
        w4_step4c_extra_withholding = float(request.form.get("w4_step4c_extra_withholding") or 0)

        full_name = f"{first_name} {last_name}".strip()

        conn.execute(
            """
            UPDATE employees
            SET first_name = ?,
                last_name = ?,
                full_name = ?,
                phone = ?,
                email = ?,
                position = ?,
                hire_date = ?,
                pay_type = ?,
                hourly_rate = ?,
                overtime_rate = ?,
                salary_amount = ?,
                default_hours = ?,
                payroll_notes = ?,
                federal_filing_status = ?,
                pay_frequency = ?,
                w4_step2_checked = ?,
                w4_step3_amount = ?,
                w4_step4a_other_income = ?,
                w4_step4b_deductions = ?,
                w4_step4c_extra_withholding = ?
            WHERE id = ? AND company_id = ?
            """,
            (
                first_name,
                last_name,
                full_name,
                phone,
                email,
                position,
                hire_date,
                pay_type,
                hourly_rate,
                overtime_rate,
                salary_amount,
                default_hours,
                payroll_notes,
                federal_filing_status,
                pay_frequency,
                w4_step2_checked,
                w4_step3_amount,
                w4_step4a_other_income,
                w4_step4b_deductions,
                w4_step4c_extra_withholding,
                employee_id,
                cid,
            ),
        )
        conn.commit()
        conn.close()

        flash("Employee updated successfully.")
        return redirect(url_for("employees.view_employee", employee_id=employee_id))

    content = _employee_form_html(
        employee=employee,
        form_action=url_for("employees.edit_employee", employee_id=employee_id),
        submit_label="Save Changes",
        page_title=f"Edit {_employee_display_name(employee)}",
    )
    conn.close()
    return render_page(content, "Edit Employee")


@employees_bp.route("/employees/<int:employee_id>/activate", methods=["POST"])
@login_required
@require_permission("can_manage_employees")
def activate_employee(employee_id):
    ensure_employee_status_column()

    conn = get_db_connection()
    cid = session["company_id"]

    conn.execute(
        """
        UPDATE employees
        SET is_active = 1
        WHERE id = ? AND company_id = ?
        """,
        (employee_id, cid),
    )
    conn.commit()
    conn.close()

    flash("Employee marked active.")
    return redirect(url_for("employees.employees", show="all"))


@employees_bp.route("/employees/<int:employee_id>/deactivate", methods=["POST"])
@login_required
@require_permission("can_manage_employees")
def deactivate_employee(employee_id):
    ensure_employee_status_column()

    conn = get_db_connection()
    cid = session["company_id"]

    conn.execute(
        """
        UPDATE employees
        SET is_active = 0
        WHERE id = ? AND company_id = ?
        """,
        (employee_id, cid),
    )
    conn.commit()
    conn.close()

    flash("Employee marked inactive.")
    return redirect(url_for("employees.employees"))


@employees_bp.route("/employees/<int:employee_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_employees")
def delete_employee(employee_id):
    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        """
        SELECT id
        FROM employees
        WHERE id = ? AND company_id = ?
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash("Employee not found.")
        return redirect(url_for("employees.employees"))

    try:
        payroll_ledger_ids = conn.execute(
            """
            SELECT ledger_entry_id
            FROM payroll_entries
            WHERE employee_id = ? AND company_id = ? AND ledger_entry_id IS NOT NULL
            """,
            (employee_id, cid),
        ).fetchall()

        for row in payroll_ledger_ids:
            conn.execute(
                "DELETE FROM ledger_entries WHERE id = ? AND company_id = ?",
                (row["ledger_entry_id"], cid),
            )

        conn.execute(
            """
            DELETE FROM payroll_entries
            WHERE employee_id = ? AND company_id = ?
            """,
            (employee_id, cid),
        )
    except sqlite3.OperationalError:
        pass

    conn.execute(
        """
        DELETE FROM employees
        WHERE id = ? AND company_id = ?
        """,
        (employee_id, cid),
    )

    conn.commit()
    conn.close()

    flash("Employee deleted.")
    return redirect(url_for("employees.employees"))
