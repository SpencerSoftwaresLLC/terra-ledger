from flask import Blueprint, request, redirect, url_for, session, flash, render_template_string
from html import escape
from datetime import datetime, date, timedelta

from db import (
    get_db_connection,
    ensure_employee_status_column,
    ensure_employee_name_columns,
    ensure_employee_payroll_columns,
    ensure_employee_tax_columns,
    get_employee_columns,
    ensure_employee_time_entries_table,
    ensure_company_profile_table,
    ensure_company_time_clock_columns,
)
from decorators import login_required, require_permission, subscription_required
from page_helpers import render_page
from utils.time_clock_emailing import send_pay_period_summary_emails_for_company


employees_bp = Blueprint("employees", __name__)


INDIANA_COUNTIES = [
    "Adams", "Allen", "Bartholomew", "Benton", "Blackford", "Boone", "Brown",
    "Carroll", "Cass", "Clark", "Clay", "Clinton", "Crawford", "Daviess",
    "Dearborn", "Decatur", "DeKalb", "Delaware", "Dubois", "Elkhart", "Fayette",
    "Floyd", "Fountain", "Franklin", "Fulton", "Gibson", "Grant", "Greene",
    "Hamilton", "Hancock", "Harrison", "Hendricks", "Henry", "Howard",
    "Huntington", "Jackson", "Jasper", "Jay", "Jefferson", "Jennings",
    "Johnson", "Knox", "Kosciusko", "LaGrange", "Lake", "LaPorte", "Lawrence",
    "Madison", "Marion", "Marshall", "Martin", "Miami", "Monroe", "Montgomery",
    "Morgan", "Newton", "Noble", "Ohio", "Orange", "Owen", "Parke", "Perry",
    "Pike", "Porter", "Posey", "Pulaski", "Putnam", "Randolph", "Ripley",
    "Rush", "St. Joseph", "Scott", "Shelby", "Spencer", "Starke", "Steuben",
    "Sullivan", "Switzerland", "Tippecanoe", "Tipton", "Union", "Vanderburgh",
    "Vermillion", "Vigo", "Wabash", "Warren", "Warrick", "Washington", "Wayne",
    "Wells", "White", "Whitley",
]


def _safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        return int(value or 0)
    except Exception:
        return default


def _clean_text(value):
    text = str(value or "").strip()
    if text.lower() in {"none", "null", "n/a"}:
        return ""
    return text


def _normalize_ssn(value):
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(raw) == 9:
        return f"{raw[:3]}-{raw[3:5]}-{raw[5:]}"
    return str(value or "").strip()


def ensure_employee_profile_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS first_name TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS middle_name TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS last_name TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS suffix TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS full_name TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS phone TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS email TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS position TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS hire_date DATE")

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS pay_type TEXT DEFAULT 'Hourly'")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS hourly_rate NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS overtime_rate NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS salary_amount NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS default_hours NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS payroll_notes TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE")

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS federal_filing_status TEXT DEFAULT 'Single'")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS pay_frequency TEXT DEFAULT 'Biweekly'")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step2_checked BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step3_amount NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step4a_other_income NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step4b_deductions NUMERIC(12,2) DEFAULT 0")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w4_step4c_extra_withholding NUMERIC(12,2) DEFAULT 0")

    conn.commit()
    conn.close()


def ensure_employee_local_tax_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS address_line_1 TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS address_line_2 TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS city TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS state TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS zip TEXT")

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS county_of_residence TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS county_of_principal_employment TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS county_tax_effective_year INTEGER")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS is_indiana_resident BOOLEAN DEFAULT TRUE")

    conn.commit()
    conn.close()


def ensure_employee_w2_columns():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS ssn TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_address_line_1 TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_address_line_2 TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_city TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_state TEXT")
    cur.execute("ALTER TABLE employees ADD COLUMN IF NOT EXISTS w2_zip TEXT")

    conn.commit()
    conn.close()


def _employee_display_name(employee):
    cols = employee.keys()

    first_name = employee["first_name"] if "first_name" in cols and employee["first_name"] else ""
    middle_name = employee["middle_name"] if "middle_name" in cols and employee["middle_name"] else ""
    last_name = employee["last_name"] if "last_name" in cols and employee["last_name"] else ""
    suffix = employee["suffix"] if "suffix" in cols and employee["suffix"] else ""
    full_name = employee["full_name"] if "full_name" in cols and employee["full_name"] else ""
    single_name = employee["name"] if "name" in cols and employee["name"] else ""
    employee_name_field = employee["employee_name"] if "employee_name" in cols and employee["employee_name"] else ""
    display_name = employee["display_name"] if "display_name" in cols and employee["display_name"] else ""

    assembled = " ".join(part for part in [first_name, middle_name, last_name, suffix] if part).strip()
    if assembled:
        return assembled
    if full_name:
        return full_name
    if single_name:
        return single_name
    if employee_name_field:
        return employee_name_field
    if display_name:
        return display_name
    return f"Employee #{employee['id']}"


def _county_options_html(selected_value=""):
    selected_value = str(selected_value or "").strip()
    options = ["<option value=''>Select county</option>"]
    for county in INDIANA_COUNTIES:
        sel = "selected" if county == selected_value else ""
        options.append(f"<option value='{escape(county)}' {sel}>{escape(county)}</option>")
    return "".join(options)


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

    def checked(key, default=False):
        if hasattr(employee, "keys"):
            if key in employee.keys() and employee[key] is not None:
                return "checked" if bool(employee[key]) else ""
            return "checked" if default else ""
        return "checked" if bool(employee.get(key, default)) else ""

    county_of_residence = ""
    county_of_principal_employment = ""
    if hasattr(employee, "keys"):
        county_of_residence = employee["county_of_residence"] if "county_of_residence" in employee.keys() and employee["county_of_residence"] else ""
        county_of_principal_employment = employee["county_of_principal_employment"] if "county_of_principal_employment" in employee.keys() and employee["county_of_principal_employment"] else ""
    else:
        county_of_residence = employee.get("county_of_residence", "")
        county_of_principal_employment = employee.get("county_of_principal_employment", "")

    current_year = date.today().year
    county_tax_year_default = current_year
    if hasattr(employee, "keys"):
        if "county_tax_effective_year" in employee.keys() and employee["county_tax_effective_year"] is not None:
            county_tax_year_default = employee["county_tax_effective_year"]
    else:
        county_tax_year_default = employee.get("county_tax_effective_year", current_year)

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>{escape(page_title)}</h1>
                <p class='muted' style='margin:0;'>Manage employee information, payroll setup, federal withholding, Indiana local tax setup, and W-2 identity details.</p>
            </div>
            <div class='row-actions'>
                <a class='btn warning' href='{url_for("payroll.employee_payroll")}'>Payroll</a>
                <a class='btn secondary' href='{url_for("employees.employees")}'>Back to Employees</a>
            </div>
        </div>
    </div>

    <form method='post' action='{form_action}'>
        {{{{ csrf_input() }}}}
        <div class='card'>
            <h2>Employee Information</h2>
            <div class='grid'>
                <div>
                    <label>First Name</label>
                    <input name='first_name' value='{val("first_name")}' required>
                </div>
                <div>
                    <label>Middle Name</label>
                    <input name='middle_name' value='{val("middle_name")}'>
                </div>
                <div>
                    <label>Last Name</label>
                    <input name='last_name' value='{val("last_name")}' required>
                </div>
                <div>
                    <label>Suffix</label>
                    <input name='suffix' value='{val("suffix")}' placeholder='Jr, Sr, II'>
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
            <h2>Employee Address</h2>
            <div class='grid'>
                <div style='grid-column:1 / -1;'>
                    <label>Address Line 1</label>
                    <input name='address_line_1' value='{val("address_line_1")}'>
                </div>
                <div style='grid-column:1 / -1;'>
                    <label>Address Line 2</label>
                    <input name='address_line_2' value='{val("address_line_2")}'>
                </div>
                <div>
                    <label>City</label>
                    <input name='city' value='{val("city")}'>
                </div>
                <div>
                    <label>State</label>
                    <input name='state' value='{val("state", "IN")}' maxlength='2'>
                </div>
                <div>
                    <label>ZIP</label>
                    <input name='zip' value='{val("zip")}'>
                </div>
            </div>
        </div>

        <div class='card'>
            <h2>W-2 Identity & Mailing Info</h2>
            <div class='grid'>
                <div>
                    <label>SSN</label>
                    <input name='ssn' value='{val("ssn")}' placeholder='123-45-6789'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>W-2 Address Line 1</label>
                    <input name='w2_address_line_1' value='{val("w2_address_line_1")}' placeholder='Leave blank to use employee address later if desired'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>W-2 Address Line 2</label>
                    <input name='w2_address_line_2' value='{val("w2_address_line_2")}'>
                </div>

                <div>
                    <label>W-2 City</label>
                    <input name='w2_city' value='{val("w2_city")}'>
                </div>

                <div>
                    <label>W-2 State</label>
                    <input name='w2_state' value='{val("w2_state")}' maxlength='2'>
                </div>

                <div>
                    <label>W-2 ZIP</label>
                    <input name='w2_zip' value='{val("w2_zip")}'>
                </div>
            </div>

            <div class='muted' style='margin-top:12px;'>
                These fields will be used for year-end W-2 preparation and employee statement printing.
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
            <h2>Indiana Local Tax Setup</h2>
            <div class='grid'>
                <div class='checkbox-field'>
                    <label class='checkbox-label'>
                        <input type='checkbox' name='is_indiana_resident' {checked("is_indiana_resident", True)}>
                        Indiana Resident on January 1
                    </label>
                    <div class='muted small'>Residents usually use county of residence. Non-residents usually use county of principal employment.</div>
                </div>

                <div>
                    <label>County Tax Effective Year</label>
                    <input name='county_tax_effective_year' type='number' step='1' value='{escape(str(county_tax_year_default))}'>
                </div>

                <div>
                    <label>County of Residence</label>
                    <select name='county_of_residence'>
                        {_county_options_html(county_of_residence)}
                    </select>
                </div>

                <div>
                    <label>County of Principal Employment</label>
                    <select name='county_of_principal_employment'>
                        {_county_options_html(county_of_principal_employment)}
                    </select>
                </div>
            </div>

            <div class='muted' style='margin-top:12px;'>
                Store the January 1 county values here so payroll can calculate Indiana local withholding correctly.
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


def _format_hours(hours_value):
    try:
        return f"{float(hours_value or 0):.2f}"
    except Exception:
        return "0.00"


def _weekday_label(day_number):
    labels = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    return labels.get(int(day_number), "Wednesday")


def _get_company_time_clock_start_day(company_id):
    ensure_company_profile_table()
    ensure_company_time_clock_columns()

    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT time_clock_pay_period_start_day
        FROM company_profile
        WHERE company_id = %s
        """,
        (company_id,),
    ).fetchone()
    conn.close()

    if row and row["time_clock_pay_period_start_day"] is not None:
        try:
            value = int(row["time_clock_pay_period_start_day"])
            if 0 <= value <= 6:
                return value
        except Exception:
            pass

    return 2


def _get_current_pay_period(start_day):
    today = date.today()
    start_day = int(start_day)

    days_since_start = (today.weekday() - start_day) % 7
    start_date = today - timedelta(days=days_since_start)
    end_date = start_date + timedelta(days=6)

    return start_date, end_date


@employees_bp.route("/employees")
@login_required
@subscription_required
@require_permission("can_manage_employees")
def employees():
    ensure_employee_profile_columns()
    ensure_employee_status_column()
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_employee_local_tax_columns()
    ensure_employee_w2_columns()

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
            WHERE company_id = %s
            ORDER BY is_active DESC, {name_col} ASC, id DESC
            """,
            (cid,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT *
            FROM employees
            WHERE company_id = %s AND is_active = 1
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
            <td>{"Active" if bool(r['is_active']) else "Inactive"}</td>
            <td>
                <div class='row-actions'>
                    <a class='btn secondary small' href='{url_for("employees.view_employee", employee_id=r["id"])}'>View</a>

                    {
                        f"<form method='post' action='{url_for('employees.deactivate_employee', employee_id=r['id'])}' class='inline-form'>"
                        f"{{{{ csrf_input() }}}}"
                        f"<button class='btn warning small' type='submit'>Set Inactive</button>"
                        f"</form>"
                        if bool(r['is_active'])
                        else
                        f"<form method='post' action='{url_for('employees.activate_employee', employee_id=r['id'])}' class='inline-form'>"
                        f"{{{{ csrf_input() }}}}"
                        f"<button class='btn success small' type='submit'>Set Active</button>"
                        f"</form>"
                    }

                    <form method='post'
                        action='{url_for("employees.delete_employee", employee_id=r["id"])}'
                        class='inline-form'
                        onsubmit="return confirm('Delete this employee? This cannot be undone.');">
                        {{{{ csrf_input() }}}}
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
                <a href='{url_for("employees.time_clock")}' class='btn'>Clock In / Out</a>
                <a href='{url_for("payroll.employee_payroll")}' class='btn warning'>Payroll</a>
                <a href='{url_for("employees.new_employee")}' class='btn success'>+ New Employee</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>{'All Employees' if show == 'all' else 'Active Employees'}</h2>
        <div class='table-wrap'>
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
    </div>
    """
    return render_page(content, "Employees")


@employees_bp.route("/employees/new", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_employees")
def new_employee():
    ensure_employee_profile_columns()
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_employee_status_column()
    ensure_employee_local_tax_columns()
    ensure_employee_w2_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    cols = get_employee_columns()

    if request.method == "POST":
        first_name = _clean_text(request.form.get("first_name"))
        middle_name = _clean_text(request.form.get("middle_name"))
        last_name = _clean_text(request.form.get("last_name"))
        suffix = _clean_text(request.form.get("suffix"))
        phone = _clean_text(request.form.get("phone"))
        email = _clean_text(request.form.get("email"))
        position = _clean_text(request.form.get("position"))
        hire_date = _clean_text(request.form.get("hire_date"))
        pay_type = _clean_text(request.form.get("pay_type")) or "Hourly"

        address_line_1 = _clean_text(request.form.get("address_line_1"))
        address_line_2 = _clean_text(request.form.get("address_line_2"))
        city = _clean_text(request.form.get("city"))
        state = (_clean_text(request.form.get("state")) or "IN").upper()
        zip_code = _clean_text(request.form.get("zip"))

        ssn = _normalize_ssn(request.form.get("ssn"))
        w2_address_line_1 = _clean_text(request.form.get("w2_address_line_1"))
        w2_address_line_2 = _clean_text(request.form.get("w2_address_line_2"))
        w2_city = _clean_text(request.form.get("w2_city"))
        w2_state = _clean_text(request.form.get("w2_state")).upper()
        w2_zip = _clean_text(request.form.get("w2_zip"))

        federal_filing_status = _clean_text(request.form.get("federal_filing_status")) or "Single"
        pay_frequency = _clean_text(request.form.get("pay_frequency")) or "Biweekly"

        hourly_rate = _safe_float(request.form.get("hourly_rate"), 0)
        overtime_rate = _safe_float(request.form.get("overtime_rate"), 0)
        salary_amount = _safe_float(request.form.get("salary_amount"), 0)
        default_hours = _safe_float(request.form.get("default_hours"), 0)
        payroll_notes = _clean_text(request.form.get("payroll_notes"))

        county_of_residence = _clean_text(request.form.get("county_of_residence"))
        county_of_principal_employment = _clean_text(request.form.get("county_of_principal_employment"))
        county_tax_effective_year = _safe_int(
            request.form.get("county_tax_effective_year"),
            date.today().year
        )

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

        full_name = " ".join(part for part in [first_name, middle_name, last_name, suffix] if part).strip()

        is_active = 1
        w4_step2_checked = bool(request.form.get("w4_step2_checked"))
        is_indiana_resident = bool(request.form.get("is_indiana_resident"))

        w4_step3_amount = _safe_float(request.form.get("w4_step3_amount"), 0)
        w4_step4a_other_income = _safe_float(request.form.get("w4_step4a_other_income"), 0)
        w4_step4b_deductions = _safe_float(request.form.get("w4_step4b_deductions"), 0)
        w4_step4c_extra_withholding = _safe_float(request.form.get("w4_step4c_extra_withholding"), 0)

        if pay_type.lower() == "salary":
            hourly_rate = 0
            overtime_rate = 0
        else:
            salary_amount = 0

        data = {
            "company_id": cid,
            "first_name": first_name,
            "middle_name": middle_name,
            "last_name": last_name,
            "suffix": suffix,
            "full_name": full_name,
            "phone": phone,
            "email": email,
            "position": position,
            "hire_date": hire_date,
            "address_line_1": address_line_1,
            "address_line_2": address_line_2,
            "city": city,
            "state": state,
            "zip": zip_code,
            "ssn": ssn,
            "w2_address_line_1": w2_address_line_1,
            "w2_address_line_2": w2_address_line_2,
            "w2_city": w2_city,
            "w2_state": w2_state,
            "w2_zip": w2_zip,
            "pay_type": pay_type,
            "hourly_rate": hourly_rate,
            "overtime_rate": overtime_rate,
            "salary_amount": salary_amount,
            "default_hours": default_hours,
            "payroll_notes": payroll_notes,
            "is_active": is_active,
            "federal_filing_status": federal_filing_status,
            "pay_frequency": pay_frequency,
            "w4_step2_checked": w4_step2_checked,
            "w4_step3_amount": w4_step3_amount,
            "w4_step4a_other_income": w4_step4a_other_income,
            "w4_step4b_deductions": w4_step4b_deductions,
            "w4_step4c_extra_withholding": w4_step4c_extra_withholding,
            "is_indiana_resident": is_indiana_resident,
            "county_of_residence": county_of_residence,
            "county_of_principal_employment": county_of_principal_employment,
            "county_tax_effective_year": county_tax_effective_year,
        }

        ordered_columns = [
            "company_id",
            "first_name",
            "middle_name",
            "last_name",
            "suffix",
            "full_name",
            "phone",
            "email",
            "position",
            "hire_date",
            "address_line_1",
            "address_line_2",
            "city",
            "state",
            "zip",
            "ssn",
            "w2_address_line_1",
            "w2_address_line_2",
            "w2_city",
            "w2_state",
            "w2_zip",
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
            "is_indiana_resident",
            "county_of_residence",
            "county_of_principal_employment",
            "county_tax_effective_year",
        ]

        insert_cols = [col for col in ordered_columns if col in cols]
        insert_vals = [data[col] for col in insert_cols]

        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_sql = ", ".join(insert_cols)

        conn.execute(
            f"INSERT INTO employees ({col_sql}) VALUES ({placeholders})",
            tuple(insert_vals),
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
    ensure_employee_profile_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_employee_local_tax_columns()
    ensure_employee_w2_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        """
        SELECT *
        FROM employees
        WHERE id = %s AND company_id = %s
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
            WHERE employee_id = %s AND company_id = %s
            ORDER BY
                CASE WHEN pay_date IS NULL OR pay_date = '' THEN 1 ELSE 0 END,
                pay_date DESC,
                id DESC
            """,
            (employee_id, cid),
        ).fetchall()
    except Exception:
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
    status_text = "Active" if ("is_active" in cols and bool(employee["is_active"])) else "Inactive"

    middle_name = employee["middle_name"] if "middle_name" in cols and employee["middle_name"] else "-"
    suffix = employee["suffix"] if "suffix" in cols and employee["suffix"] else "-"
    ssn = employee["ssn"] if "ssn" in cols and employee["ssn"] else "-"

    address_line_1 = employee["address_line_1"] if "address_line_1" in cols and employee["address_line_1"] else "-"
    address_line_2 = employee["address_line_2"] if "address_line_2" in cols and employee["address_line_2"] else "-"
    city = employee["city"] if "city" in cols and employee["city"] else "-"
    state = employee["state"] if "state" in cols and employee["state"] else "-"
    zip_code = employee["zip"] if "zip" in cols and employee["zip"] else "-"

    w2_address_line_1 = employee["w2_address_line_1"] if "w2_address_line_1" in cols and employee["w2_address_line_1"] else "-"
    w2_address_line_2 = employee["w2_address_line_2"] if "w2_address_line_2" in cols and employee["w2_address_line_2"] else "-"
    w2_city = employee["w2_city"] if "w2_city" in cols and employee["w2_city"] else "-"
    w2_state = employee["w2_state"] if "w2_state" in cols and employee["w2_state"] else "-"
    w2_zip = employee["w2_zip"] if "w2_zip" in cols and employee["w2_zip"] else "-"

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
    w4_step2_checked = "Yes" if "w4_step2_checked" in cols and bool(employee["w4_step2_checked"]) else "No"
    w4_step3_amount = float(employee["w4_step3_amount"]) if "w4_step3_amount" in cols and employee["w4_step3_amount"] is not None else 0.0
    w4_step4a_other_income = float(employee["w4_step4a_other_income"]) if "w4_step4a_other_income" in cols and employee["w4_step4a_other_income"] is not None else 0.0
    w4_step4b_deductions = float(employee["w4_step4b_deductions"]) if "w4_step4b_deductions" in cols and employee["w4_step4b_deductions"] is not None else 0.0
    w4_step4c_extra_withholding = float(employee["w4_step4c_extra_withholding"]) if "w4_step4c_extra_withholding" in cols and employee["w4_step4c_extra_withholding"] is not None else 0.0

    is_indiana_resident = "Yes" if "is_indiana_resident" in cols and bool(employee["is_indiana_resident"]) else "No"
    county_of_residence = employee["county_of_residence"] if "county_of_residence" in cols and employee["county_of_residence"] else "-"
    county_of_principal_employment = employee["county_of_principal_employment"] if "county_of_principal_employment" in cols and employee["county_of_principal_employment"] else "-"
    county_tax_effective_year = employee["county_tax_effective_year"] if "county_tax_effective_year" in cols and employee["county_tax_effective_year"] else "-"

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
                <p class='muted' style='margin:0;'>Employee details, payroll profile, tax setup, and W-2 identity fields.</p>
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
            <div><strong>Middle Name</strong><br>{escape(str(middle_name))}</div>
            <div><strong>Suffix</strong><br>{escape(str(suffix))}</div>
            <div><strong>Phone</strong><br>{escape(str(phone))}</div>
            <div><strong>Email</strong><br>{escape(str(email))}</div>
            <div><strong>Position</strong><br>{escape(str(position))}</div>
            <div><strong>Hire Date</strong><br>{escape(str(hire_date))}</div>
        </div>
    </div>

    <div class='card'>
        <h2>Employee Address</h2>
        <div class='grid'>
            <div style='grid-column:1 / -1;'><strong>Address Line 1</strong><br>{escape(str(address_line_1))}</div>
            <div style='grid-column:1 / -1;'><strong>Address Line 2</strong><br>{escape(str(address_line_2))}</div>
            <div><strong>City</strong><br>{escape(str(city))}</div>
            <div><strong>State</strong><br>{escape(str(state))}</div>
            <div><strong>ZIP</strong><br>{escape(str(zip_code))}</div>
        </div>
    </div>

    <div class='card'>
        <h2>W-2 Identity & Mailing Info</h2>
        <div class='grid'>
            <div><strong>SSN</strong><br>{escape(str(ssn))}</div>
            <div style='grid-column:1 / -1;'><strong>W-2 Address Line 1</strong><br>{escape(str(w2_address_line_1))}</div>
            <div style='grid-column:1 / -1;'><strong>W-2 Address Line 2</strong><br>{escape(str(w2_address_line_2))}</div>
            <div><strong>W-2 City</strong><br>{escape(str(w2_city))}</div>
            <div><strong>W-2 State</strong><br>{escape(str(w2_state))}</div>
            <div><strong>W-2 ZIP</strong><br>{escape(str(w2_zip))}</div>
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
        <h2>Indiana Local Tax Setup</h2>
        <div class='grid'>
            <div><strong>Indiana Resident on Jan 1</strong><br>{escape(is_indiana_resident)}</div>
            <div><strong>County Tax Effective Year</strong><br>{escape(str(county_tax_effective_year))}</div>
            <div><strong>County of Residence</strong><br>{escape(str(county_of_residence))}</div>
            <div><strong>County of Principal Employment</strong><br>{escape(str(county_of_principal_employment))}</div>
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
    ensure_employee_profile_columns()
    ensure_employee_name_columns()
    ensure_employee_payroll_columns()
    ensure_employee_tax_columns()
    ensure_employee_local_tax_columns()
    ensure_employee_w2_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        "SELECT * FROM employees WHERE id = %s AND company_id = %s",
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash("Employee not found.")
        return redirect(url_for("employees.employees"))

    if request.method == "POST":
        first_name = _clean_text(request.form.get("first_name"))
        middle_name = _clean_text(request.form.get("middle_name"))
        last_name = _clean_text(request.form.get("last_name"))
        suffix = _clean_text(request.form.get("suffix"))
        phone = _clean_text(request.form.get("phone"))
        email = _clean_text(request.form.get("email"))
        position = _clean_text(request.form.get("position"))
        hire_date = _clean_text(request.form.get("hire_date"))

        if not first_name or not last_name:
            conn.close()
            flash("First and last name are required.")
            return redirect(url_for("employees.edit_employee", employee_id=employee_id))

        address_line_1 = _clean_text(request.form.get("address_line_1"))
        address_line_2 = _clean_text(request.form.get("address_line_2"))
        city = _clean_text(request.form.get("city"))
        state = (_clean_text(request.form.get("state")) or "IN").upper()
        zip_code = _clean_text(request.form.get("zip"))

        ssn = _normalize_ssn(request.form.get("ssn"))
        w2_address_line_1 = _clean_text(request.form.get("w2_address_line_1"))
        w2_address_line_2 = _clean_text(request.form.get("w2_address_line_2"))
        w2_city = _clean_text(request.form.get("w2_city"))
        w2_state = _clean_text(request.form.get("w2_state")).upper()
        w2_zip = _clean_text(request.form.get("w2_zip"))

        pay_type = _clean_text(request.form.get("pay_type")) or "Hourly"
        hourly_rate = _safe_float(request.form.get("hourly_rate"), 0)
        overtime_rate = _safe_float(request.form.get("overtime_rate"), 0)
        salary_amount = _safe_float(request.form.get("salary_amount"), 0)
        default_hours = _safe_float(request.form.get("default_hours"), 0)
        payroll_notes = _clean_text(request.form.get("payroll_notes"))

        if pay_type.lower() == "salary":
            hourly_rate = 0
            overtime_rate = 0
        else:
            salary_amount = 0

        federal_filing_status = _clean_text(request.form.get("federal_filing_status")) or "Single"
        pay_frequency = _clean_text(request.form.get("pay_frequency")) or "Weekly"

        w4_step2_checked = bool(request.form.get("w4_step2_checked"))
        is_indiana_resident = bool(request.form.get("is_indiana_resident"))

        w4_step3_amount = _safe_float(request.form.get("w4_step3_amount"), 0)
        w4_step4a_other_income = _safe_float(request.form.get("w4_step4a_other_income"), 0)
        w4_step4b_deductions = _safe_float(request.form.get("w4_step4b_deductions"), 0)
        w4_step4c_extra_withholding = _safe_float(request.form.get("w4_step4c_extra_withholding"), 0)

        county_of_residence = _clean_text(request.form.get("county_of_residence"))
        county_of_principal_employment = _clean_text(request.form.get("county_of_principal_employment"))
        county_tax_effective_year = _safe_int(
            request.form.get("county_tax_effective_year"),
            date.today().year
        )

        full_name = " ".join(part for part in [first_name, middle_name, last_name, suffix] if part).strip()

        conn.execute(
            """
            UPDATE employees
            SET first_name = %s,
                middle_name = %s,
                last_name = %s,
                suffix = %s,
                full_name = %s,
                phone = %s,
                email = %s,
                position = %s,
                hire_date = %s,
                address_line_1 = %s,
                address_line_2 = %s,
                city = %s,
                state = %s,
                zip = %s,
                ssn = %s,
                w2_address_line_1 = %s,
                w2_address_line_2 = %s,
                w2_city = %s,
                w2_state = %s,
                w2_zip = %s,
                pay_type = %s,
                hourly_rate = %s,
                overtime_rate = %s,
                salary_amount = %s,
                default_hours = %s,
                payroll_notes = %s,
                federal_filing_status = %s,
                pay_frequency = %s,
                w4_step2_checked = %s,
                w4_step3_amount = %s,
                w4_step4a_other_income = %s,
                w4_step4b_deductions = %s,
                w4_step4c_extra_withholding = %s,
                is_indiana_resident = %s,
                county_of_residence = %s,
                county_of_principal_employment = %s,
                county_tax_effective_year = %s
            WHERE id = %s AND company_id = %s
            """,
            (
                first_name,
                middle_name,
                last_name,
                suffix,
                full_name,
                phone,
                email,
                position,
                hire_date,
                address_line_1,
                address_line_2,
                city,
                state,
                zip_code,
                ssn,
                w2_address_line_1,
                w2_address_line_2,
                w2_city,
                w2_state,
                w2_zip,
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
                is_indiana_resident,
                county_of_residence,
                county_of_principal_employment,
                county_tax_effective_year,
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
    ensure_employee_profile_columns()
    ensure_employee_status_column()

    conn = get_db_connection()
    cid = session["company_id"]

    conn.execute(
        """
        UPDATE employees
        SET is_active = 1
        WHERE id = %s AND company_id = %s
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
    ensure_employee_profile_columns()
    ensure_employee_status_column()

    conn = get_db_connection()
    cid = session["company_id"]

    conn.execute(
        """
        UPDATE employees
        SET is_active = 0
        WHERE id = %s AND company_id = %s
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
    ensure_employee_profile_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    employee = conn.execute(
        """
        SELECT id
        FROM employees
        WHERE id = %s AND company_id = %s
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
            WHERE employee_id = %s AND company_id = %s AND ledger_entry_id IS NOT NULL
            """,
            (employee_id, cid),
        ).fetchall()

        for row in payroll_ledger_ids:
            conn.execute(
                "DELETE FROM ledger_entries WHERE id = %s AND company_id = %s",
                (row["ledger_entry_id"], cid),
            )

        conn.execute(
            """
            DELETE FROM payroll_entries
            WHERE employee_id = %s AND company_id = %s
            """,
            (employee_id, cid),
        )
    except Exception:
        pass

    try:
        conn.execute(
            """
            DELETE FROM employee_time_entries
            WHERE employee_id = %s AND company_id = %s
            """,
            (employee_id, cid),
        )
    except Exception:
        pass

    conn.execute(
        """
        DELETE FROM employees
        WHERE id = %s AND company_id = %s
        """,
        (employee_id, cid),
    )

    conn.commit()
    conn.close()

    flash("Employee deleted.")
    return redirect(url_for("employees.employees"))


@employees_bp.route("/employees/time-clock", methods=["GET"])
@login_required
@require_permission("can_manage_employees")
def time_clock():
    ensure_employee_profile_columns()
    ensure_employee_time_entries_table()
    ensure_company_profile_table()
    ensure_company_time_clock_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    pay_period_start_day = _get_company_time_clock_start_day(cid)
    pay_period_start, pay_period_end = _get_current_pay_period(pay_period_start_day)
    pay_period_end_day = (pay_period_start_day - 1) % 7

    employees = conn.execute(
        """
        SELECT
            id,
            first_name,
            last_name,
            full_name,
            email,
            is_active
        FROM employees
        WHERE company_id = %s AND is_active = 1
        ORDER BY
            COALESCE(NULLIF(last_name, ''), NULLIF(full_name, ''), NULLIF(first_name, ''), 'ZZZ'),
            COALESCE(NULLIF(first_name, ''), ''),
            id
        """,
        (cid,),
    ).fetchall()

    status_rows = conn.execute(
        """
        SELECT
            e.id AS employee_id,
            e.first_name,
            e.last_name,
            e.full_name,
            t.id AS time_entry_id,
            t.clock_in,
            t.clock_out,
            t.total_hours
        FROM employees e
        LEFT JOIN employee_time_entries t
            ON t.employee_id = e.id
           AND t.company_id = e.company_id
           AND t.clock_out IS NULL
        WHERE e.company_id = %s AND e.is_active = 1
        ORDER BY
            COALESCE(NULLIF(e.last_name, ''), NULLIF(e.full_name, ''), NULLIF(e.first_name, ''), 'ZZZ'),
            COALESCE(NULLIF(e.first_name, ''), ''),
            e.id
        """,
        (cid,),
    ).fetchall()

    today_rows = conn.execute(
        """
        SELECT
            employee_id,
            COALESCE(SUM(total_hours), 0) AS today_hours
        FROM employee_time_entries
        WHERE company_id = %s
          AND DATE(clock_in) = %s
        GROUP BY employee_id
        """,
        (cid, date.today().isoformat()),
    ).fetchall()

    pay_period_rows = conn.execute(
        """
        SELECT
            employee_id,
            COALESCE(SUM(total_hours), 0) AS pay_period_hours
        FROM employee_time_entries
        WHERE company_id = %s
          AND DATE(clock_in) >= %s
          AND DATE(clock_in) <= %s
        GROUP BY employee_id
        """,
        (cid, pay_period_start.isoformat(), pay_period_end.isoformat()),
    ).fetchall()

    recent_entries = conn.execute(
        """
        SELECT
            t.id,
            t.employee_id,
            t.clock_in,
            t.clock_out,
            t.total_hours,
            t.notes,
            e.first_name,
            e.last_name,
            e.full_name
        FROM employee_time_entries t
        JOIN employees e ON t.employee_id = e.id
        WHERE t.company_id = %s
        ORDER BY t.clock_in DESC
        LIMIT 25
        """,
        (cid,),
    ).fetchall()

    conn.close()

    today_map = {row["employee_id"]: float(row["today_hours"] or 0) for row in today_rows}
    pay_period_map = {row["employee_id"]: float(row["pay_period_hours"] or 0) for row in pay_period_rows}

    employees_with_status = []
    currently_clocked_in = 0

    for row in status_rows:
        employee_name = (
            f"{(row['first_name'] or '').strip()} {(row['last_name'] or '').strip()}".strip()
            or (row["full_name"] or "").strip()
            or f"Employee #{row['employee_id']}"
        )

        is_clocked_in = bool(row["time_entry_id"])
        if is_clocked_in:
            currently_clocked_in += 1

        employees_with_status.append({
            "employee_id": row["employee_id"],
            "employee_name": employee_name,
            "is_clocked_in": is_clocked_in,
            "clock_in": row["clock_in"],
            "today_hours": today_map.get(row["employee_id"], 0),
            "pay_period_hours": pay_period_map.get(row["employee_id"], 0),
        })

    clocked_in_ids = {
        row["employee_id"]
        for row in employees_with_status
        if row["is_clocked_in"]
    }

    time_clock_html = """
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Clock In / Out</h1>
                <p class='muted' style='margin:0;'>Track employee hours using your company's chosen pay period.</p>
            </div>
            <div class='row-actions'>
                <a class='btn secondary' href='{{ url_for("dashboard.dashboard") }}'>Back to Dashboard</a>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Pay Period Settings</h2>
        <form method='post' action='{{ url_for("employees.update_time_clock_settings") }}'>
            {{ csrf_input() }}
            <div style='display:grid; grid-template-columns:minmax(220px, 1fr) auto; gap:12px; align-items:end;'>
                <div>
                    <label>Pay Period Start Day</label>
                    <select name='time_clock_pay_period_start_day' required>
                        {% for day_number, day_name in weekday_options %}
                            <option value='{{ day_number }}' {% if day_number == pay_period_start_day %}selected{% endif %}>{{ day_name }}</option>
                        {% endfor %}
                    </select>
                    <div class='muted' style='margin-top:6px;'>
                        Current pay period runs {{ pay_period_start_label }} through {{ pay_period_end_label }}.
                    </div>
                </div>
                <div>
                    <button class='btn' type='submit'>Save Pay Period</button>
                </div>
            </div>
        </form>
    </div>

    <div class='card'>
        <div style='display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:16px;'>
            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:16px; background:#f8fafc;'>
                <div class='muted' style='margin-bottom:6px;'>Current Pay Period</div>
                <div style='font-size:1.1rem; font-weight:700;'>{{ pay_period_start }} to {{ pay_period_end }}</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:16px; background:#f8fafc;'>
                <div class='muted' style='margin-bottom:6px;'>Employees</div>
                <div style='font-size:1.4rem; font-weight:700;'>{{ employees|length }}</div>
            </div>

            <div style='border:1px solid #e5e7eb; border-radius:12px; padding:16px; background:#f8fafc;'>
                <div class='muted' style='margin-bottom:6px;'>Currently Clocked In</div>
                <div style='font-size:1.4rem; font-weight:700;'>{{ currently_clocked_in }}</div>
            </div>
        </div>
    </div>

    <div class='card'>
        <h2>Clock Actions</h2>

        <form method='post' action='{{ url_for("employees.time_clock_clock_in") }}' style='margin-bottom:14px;'>
            {{ csrf_input() }}
            <div style='display:grid; grid-template-columns:minmax(220px, 1fr) auto; gap:12px; align-items:end;'>
                <div>
                    <label>Select Employee to Clock In</label>
                    <select name='employee_id' required>
                        <option value=''>Choose employee</option>
                        {% for emp in employees if emp["id"] not in clocked_in_ids %}
                            {% set emp_name = ((emp["first_name"] or "") ~ " " ~ (emp["last_name"] or "")).strip() or (emp["full_name"] or "") or ("Employee #" ~ emp["id"]) %}
                            <option value='{{ emp["id"] }}'>{{ emp_name }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div>
                    <button class='btn success' type='submit'>Clock In</button>
                </div>
            </div>
        </form>

        <form method='post' action='{{ url_for("employees.time_clock_clock_out") }}'>
            {{ csrf_input() }}
            <div style='display:grid; grid-template-columns:minmax(220px, 1fr) auto; gap:12px; align-items:end;'>
                <div>
                    <label>Select Employee to Clock Out</label>
                    <select name='employee_id' required>
                        <option value=''>Choose employee</option>
                        {% for emp in employees if emp["id"] in clocked_in_ids %}
                            {% set emp_name = ((emp["first_name"] or "") ~ " " ~ (emp["last_name"] or "")).strip() or (emp["full_name"] or "") or ("Employee #" ~ emp["id"]) %}
                            <option value='{{ emp["id"] }}'>{{ emp_name }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div>
                    <button class='btn warning' type='submit'>Clock Out</button>
                </div>
            </div>
        </form>
    </div>

    <div class='card'>
        <h2>Current Employee Status</h2>
        {% if employees_with_status %}
            <div style='overflow-x:auto;'>
                <table class='table'>
                    <thead>
                        <tr>
                            <th>Employee</th>
                            <th>Status</th>
                            <th>Clocked In At</th>
                            <th>Today</th>
                            <th>This Pay Period</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in employees_with_status %}
                            <tr>
                                <td>{{ row.employee_name }}</td>
                                <td>
                                    {% if row.is_clocked_in %}
                                        <span style='color:#166534; font-weight:700;'>Clocked In</span>
                                    {% else %}
                                        <span style='color:#666;'>Clocked Out</span>
                                    {% endif %}
                                </td>
                                <td>{{ row.clock_in or "-" }}</td>
                                <td>{{ format_hours(row.today_hours) }} hrs</td>
                                <td>{{ format_hours(row.pay_period_hours) }} hrs</td>
                            </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        {% else %}
            <p class='muted'>No employees found.</p>
        {% endif %}
    </div>

    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:14px;'>
            <div>
                <h2 style='margin-bottom:4px;'>Recent Time Entries</h2>
                <div class='muted'>Review recent punches and send the current hours summary email manually.</div>
            </div>

            <form method='post' action='{{ url_for("employees.send_time_clock_summary_now") }}' style='margin:0;'>
                {{ csrf_input() }}
                <button class='btn warning' type='submit'>Send Last Pay Period Summary</button>
            </form>
        </div>

        {% if recent_entries %}
            <div style='overflow-x:auto;'>
                <table class='table'>
                    <thead>
                        <tr>
                            <th>Employee</th>
                            <th>Clock In</th>
                            <th>Clock Out</th>
                            <th>Total Hours</th>
                            <th>Notes</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in recent_entries %}
                        {% set entry_name = ((row["first_name"] or "") ~ " " ~ (row["last_name"] or "")).strip() or (row["full_name"] or "") or ("Employee #" ~ row["employee_id"]) %}
                            <tr>
                                <td>{{ entry_name }}</td>
                                <td>{{ row["clock_in"] }}</td>
                                <td>{{ row["clock_out"] or "-" }}</td>
                                <td>{{ format_hours(row["total_hours"]) }}</td>
                                <td>{{ row["notes"] or "" }}</td>
                            </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        {% else %}
            <p class='muted'>No time entries yet.</p>
        {% endif %}
    </div>
    """

    return render_page(
        render_template_string(
            time_clock_html,
            employees=employees,
            employees_with_status=employees_with_status,
            recent_entries=recent_entries,
            currently_clocked_in=currently_clocked_in,
            pay_period_start=pay_period_start.isoformat(),
            pay_period_end=pay_period_end.isoformat(),
            pay_period_start_label=_weekday_label(pay_period_start_day),
            pay_period_end_label=_weekday_label(pay_period_end_day),
            pay_period_start_day=pay_period_start_day,
            weekday_options=[
                (0, "Monday"),
                (1, "Tuesday"),
                (2, "Wednesday"),
                (3, "Thursday"),
                (4, "Friday"),
                (5, "Saturday"),
                (6, "Sunday"),
            ],
            clocked_in_ids=clocked_in_ids,
            format_hours=_format_hours,
            csrf_input=lambda: "{{ csrf_input() }}",
        ),
        "Clock In / Out",
    )


@employees_bp.route("/employees/time-clock/settings", methods=["POST"])
@login_required
@require_permission("can_manage_employees")
def update_time_clock_settings():
    ensure_company_profile_table()
    ensure_company_time_clock_columns()

    conn = get_db_connection()
    cid = session["company_id"]

    raw_value = (request.form.get("time_clock_pay_period_start_day") or "").strip()

    try:
        start_day = int(raw_value)
    except Exception:
        conn.close()
        flash("Invalid pay period start day.")
        return redirect(url_for("employees.time_clock"))

    if start_day < 0 or start_day > 6:
        conn.close()
        flash("Invalid pay period start day.")
        return redirect(url_for("employees.time_clock"))

    existing_profile = conn.execute(
        """
        SELECT id
        FROM company_profile
        WHERE company_id = %s
        """,
        (cid,),
    ).fetchone()

    if existing_profile:
        conn.execute(
            """
            UPDATE company_profile
            SET time_clock_pay_period_start_day = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE company_id = %s
            """,
            (start_day, cid),
        )
    else:
        conn.execute(
            """
            INSERT INTO company_profile (company_id, time_clock_pay_period_start_day)
            VALUES (%s, %s)
            """,
            (cid, start_day),
        )

    conn.commit()
    conn.close()

    flash(f"Pay period updated. It now starts on {_weekday_label(start_day)}.")
    return redirect(url_for("employees.time_clock"))


@employees_bp.route("/employees/time-clock/clock-in", methods=["POST"])
@login_required
@require_permission("can_manage_employees")
def time_clock_clock_in():
    ensure_employee_profile_columns()
    ensure_employee_time_entries_table()

    conn = get_db_connection()
    cid = session["company_id"]

    employee_id = (request.form.get("employee_id") or "").strip()

    if not employee_id:
        conn.close()
        flash("Please select an employee.")
        return redirect(url_for("employees.time_clock"))

    employee = conn.execute(
        """
        SELECT id, first_name, last_name, full_name
        FROM employees
        WHERE id = %s AND company_id = %s AND is_active = 1
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        flash("Employee not found.")
        return redirect(url_for("employees.time_clock"))

    existing_open = conn.execute(
        """
        SELECT id
        FROM employee_time_entries
        WHERE company_id = %s
          AND employee_id = %s
          AND clock_out IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (cid, employee_id),
    ).fetchone()

    if existing_open:
        conn.close()
        flash("That employee is already clocked in.")
        return redirect(url_for("employees.time_clock"))

    conn.execute(
        """
        INSERT INTO employee_time_entries (
            company_id,
            employee_id,
            clock_in,
            clock_out,
            total_hours,
            notes
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (cid, employee_id, datetime.now(), None, 0, None),
    )

    conn.commit()
    conn.close()

    flash("Employee clocked in successfully.")
    return redirect(url_for("employees.time_clock"))


@employees_bp.route("/employees/time-clock/clock-out", methods=["POST"])
@login_required
@require_permission("can_manage_employees")
def time_clock_clock_out():
    ensure_employee_profile_columns()
    ensure_employee_time_entries_table()

    conn = get_db_connection()
    cid = session["company_id"]

    employee_id = (request.form.get("employee_id") or "").strip()

    if not employee_id:
        conn.close()
        flash("Please select an employee.")
        return redirect(url_for("employees.time_clock"))

    open_entry = conn.execute(
        """
        SELECT id, clock_in
        FROM employee_time_entries
        WHERE company_id = %s
          AND employee_id = %s
          AND clock_out IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (cid, employee_id),
    ).fetchone()

    if not open_entry:
        conn.close()
        flash("That employee is not currently clocked in.")
        return redirect(url_for("employees.time_clock"))

    clock_in_time = open_entry["clock_in"]
    clock_out_time = datetime.now()

    total_seconds = (clock_out_time - clock_in_time).total_seconds()
    total_hours = round(max(total_seconds / 3600.0, 0), 2)

    conn.execute(
        """
        UPDATE employee_time_entries
        SET clock_out = %s,
            total_hours = %s
        WHERE id = %s
        """,
        (clock_out_time, total_hours, open_entry["id"]),
    )

    conn.commit()
    conn.close()

    flash(f"Employee clocked out successfully. Total hours: {total_hours:.2f}")
    return redirect(url_for("employees.time_clock"))


@employees_bp.route("/employees/time-clock/send-summary", methods=["POST"])
@login_required
@require_permission("can_manage_employees")
def send_time_clock_summary_now():
    cid = session["company_id"]

    try:
        result = send_pay_period_summary_emails_for_company(cid)
        flash(f"Hours summary email sent. Emails delivered: {result['sent']}")
    except Exception as e:
        flash(f"Could not send hours summary email: {e}")

    return redirect(url_for("employees.time_clock"))