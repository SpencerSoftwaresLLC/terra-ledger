from flask import Blueprint, request, redirect, url_for, session, flash, render_template, make_response, jsonify
from datetime import date
import io
import csv

from db import (
    get_db_connection,
    ensure_employee_payroll_columns,
    ensure_bookkeeping_history_table,
    ensure_payroll_table_structure,
)
from decorators import login_required, require_permission, subscription_required
from utils.payroll_tax_service import calculate_payroll_taxes_for_employee
from page_helpers import *

payroll_bp = Blueprint("payroll", __name__)


def safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return default


def get_salary_period_amount(annual_salary, pay_frequency):
    annual_salary = safe_float(annual_salary, 0)
    pay_frequency = (pay_frequency or "Biweekly").strip()

    if pay_frequency == "Weekly":
        return annual_salary / 52
    if pay_frequency == "Biweekly":
        return annual_salary / 26
    if pay_frequency in ("Semi-Monthly", "Semimonthly"):
        return annual_salary / 24
    if pay_frequency == "Monthly":
        return annual_salary / 12
    if pay_frequency == "Quarterly":
        return annual_salary / 4
    if pay_frequency == "Yearly":
        return annual_salary

    return annual_salary / 26


def build_gross_pay(employee, hours_regular, hours_overtime, rate_regular, rate_overtime):
    pay_type = (employee["pay_type"] or "Hourly").strip()
    pay_frequency = (employee["pay_frequency"] or "Biweekly").strip()

    if pay_type == "Salary":
        annual_salary = safe_float(employee["salary_amount"], 0)
        gross_pay = get_salary_period_amount(annual_salary, pay_frequency)

        return {
            "pay_type": pay_type,
            "pay_frequency": pay_frequency,
            "gross_pay": round(max(gross_pay, 0), 2),
            "hours_regular": 1,
            "hours_overtime": 0,
            "rate_regular": 0,
            "rate_overtime": 0,
        }

    if rate_regular <= 0:
        rate_regular = safe_float(employee["hourly_rate"], 0)
    if rate_overtime <= 0:
        rate_overtime = safe_float(employee["overtime_rate"], 0)

    gross_pay = (hours_regular * rate_regular) + (hours_overtime * rate_overtime)

    return {
        "pay_type": pay_type,
        "pay_frequency": pay_frequency,
        "gross_pay": round(max(gross_pay, 0), 2),
        "hours_regular": hours_regular,
        "hours_overtime": hours_overtime,
        "rate_regular": round(rate_regular, 2),
        "rate_overtime": round(rate_overtime, 2),
    }


@payroll_bp.route("/employees/payroll/preview", methods=["POST"])
@login_required
@require_permission("can_manage_payroll")
def payroll_preview():
    ensure_employee_payroll_columns()
    ensure_bookkeeping_history_table()
    ensure_payroll_table_structure()

    conn = get_db_connection()
    cid = session["company_id"]

    employee_id_raw = str(request.form.get("employee_id") or "").strip()
    if not employee_id_raw.isdigit():
        conn.close()
        return jsonify({
            "ok": False,
            "message": "Please select an employee.",
        }), 400

    employee_id = int(employee_id_raw)

    employee = conn.execute(
        """
        SELECT
            id,
            first_name,
            last_name,
            full_name,
            pay_type,
            hourly_rate,
            overtime_rate,
            salary_amount,
            pay_frequency,
            federal_filing_status,
            w4_filing_status,
            w4_step2_checked,
            w4_step3_amount,
            w4_step4a_other_income,
            w4_step4b_deductions,
            w4_step4c_extra_withholding
        FROM employees
        WHERE id = ? AND company_id = ?
        """,
        (employee_id, cid),
    ).fetchone()

    if not employee:
        conn.close()
        return jsonify({
            "ok": False,
            "message": "Employee not found.",
        }), 404

    hours_regular = safe_float(request.form.get("hours_regular"), 0)
    hours_overtime = safe_float(request.form.get("hours_overtime"), 0)
    rate_regular = safe_float(request.form.get("rate_regular"), 0)
    rate_overtime = safe_float(request.form.get("rate_overtime"), 0)
    other_deductions = safe_float(request.form.get("other_deductions"), 0)

    gross_data = build_gross_pay(
        employee=employee,
        hours_regular=hours_regular,
        hours_overtime=hours_overtime,
        rate_regular=rate_regular,
        rate_overtime=rate_overtime,
    )

    gross_pay = gross_data["gross_pay"]

    taxes = calculate_payroll_taxes_for_employee(
        employee=employee,
        gross_pay=gross_pay,
        company_id=cid,
        conn=conn,
    )

    federal_withholding = round(float(taxes.get("federal_withholding", taxes.get("federal_tax", 0)) or 0), 2)
    state_withholding = round(float(taxes.get("state_withholding", taxes.get("state_tax", 0)) or 0), 2)
    social_security = round(float(taxes.get("social_security", 0) or 0), 2)
    medicare = round(float(taxes.get("medicare", 0) or 0), 2)
    local_tax = round(float(taxes.get("local_tax", taxes.get("local_withholding", 0)) or 0), 2)

    net_pay = round(
        gross_pay
        - federal_withholding
        - state_withholding
        - social_security
        - medicare
        - local_tax
        - other_deductions,
        2,
    )

    conn.close()

    return jsonify({
        "ok": True,
        "pay_type": gross_data["pay_type"],
        "pay_frequency": gross_data["pay_frequency"],
        "gross_pay": gross_pay,
        "federal_withholding": federal_withholding,
        "state_withholding": state_withholding,
        "social_security": social_security,
        "medicare": medicare,
        "local_tax": local_tax,
        "other_deductions": round(other_deductions, 2),
        "net_pay": net_pay,
        "provider": taxes.get("provider", "internal"),
        "state_name": taxes.get("state_name", "") or "-",
        "local_name": taxes.get("local_name", "") or "-",
        "hours_regular": gross_data["hours_regular"],
        "hours_overtime": gross_data["hours_overtime"],
        "rate_regular": gross_data["rate_regular"],
        "rate_overtime": gross_data["rate_overtime"],
        "w4_filing_status": employee["w4_filing_status"] or employee["federal_filing_status"] or "Single",
        "w4_step2_checked": 1 if (employee["w4_step2_checked"] or 0) else 0,
        "w4_step3_amount": float(employee["w4_step3_amount"] or 0),
        "w4_step4a_other_income": float(employee["w4_step4a_other_income"] or 0),
        "w4_step4b_deductions": float(employee["w4_step4b_deductions"] or 0),
        "w4_step4c_extra_withholding": float(employee["w4_step4c_extra_withholding"] or 0),
    })


@payroll_bp.route("/employees/payroll", methods=["GET", "POST"])
@login_required
@subscription_required
@require_permission("can_manage_payroll")
def employee_payroll():
    ensure_employee_payroll_columns()
    ensure_bookkeeping_history_table()
    ensure_payroll_table_structure()

    conn = get_db_connection()
    cid = session["company_id"]

    if request.method == "POST":
        employee_id_raw = (request.form.get("employee_id") or "").strip()
        if not employee_id_raw.isdigit():
            flash("Please select an employee.")
            conn.close()
            return redirect(url_for("payroll.employee_payroll"))

        employee_id = int(employee_id_raw)
        pay_date = (request.form.get("pay_date") or "").strip() or date.today().isoformat()
        pay_period_start = (request.form.get("pay_period_start") or "").strip()
        pay_period_end = (request.form.get("pay_period_end") or "").strip()
        hours_regular = safe_float(request.form.get("hours_regular"), 0)
        hours_overtime = safe_float(request.form.get("hours_overtime"), 0)
        rate_regular = safe_float(request.form.get("rate_regular"), 0)
        rate_overtime = safe_float(request.form.get("rate_overtime"), 0)
        other_deductions = safe_float(request.form.get("other_deductions"), 0)
        notes = (request.form.get("notes") or "").strip()

        employee = conn.execute(
            """
            SELECT
                id,
                first_name,
                last_name,
                full_name,
                pay_type,
                hourly_rate,
                overtime_rate,
                salary_amount,
                pay_frequency,
                federal_filing_status,
                w4_filing_status,
                w4_step2_checked,
                w4_step3_amount,
                w4_step4a_other_income,
                w4_step4b_deductions,
                w4_step4c_extra_withholding
            FROM employees
            WHERE id = ? AND company_id = ?
            """,
            (employee_id, cid),
        ).fetchone()

        if not employee:
            flash("Employee not found.")
            conn.close()
            return redirect(url_for("payroll.employee_payroll"))

        gross_data = build_gross_pay(
            employee=employee,
            hours_regular=hours_regular,
            hours_overtime=hours_overtime,
            rate_regular=rate_regular,
            rate_overtime=rate_overtime,
        )

        pay_type = gross_data["pay_type"]
        pay_frequency = gross_data["pay_frequency"]
        gross_pay = gross_data["gross_pay"]
        hours_regular = gross_data["hours_regular"]
        hours_overtime = gross_data["hours_overtime"]
        rate_regular = gross_data["rate_regular"]
        rate_overtime = gross_data["rate_overtime"]

        taxes = calculate_payroll_taxes_for_employee(
            employee=employee,
            gross_pay=gross_pay,
            company_id=cid,
            conn=conn,
        )

        federal_withholding = round(float(taxes.get("federal_withholding", taxes.get("federal_tax", 0)) or 0), 2)
        state_withholding = round(float(taxes.get("state_withholding", taxes.get("state_tax", 0)) or 0), 2)
        social_security = round(float(taxes.get("social_security", 0) or 0), 2)
        medicare = round(float(taxes.get("medicare", 0) or 0), 2)
        local_tax = round(float(taxes.get("local_tax", taxes.get("local_withholding", 0)) or 0), 2)

        net_pay = round(
            gross_pay
            - federal_withholding
            - state_withholding
            - social_security
            - medicare
            - local_tax
            - other_deductions,
            2,
        )

        conn.execute(
            """
            INSERT INTO payroll_entries (
                company_id, employee_id, pay_date, pay_period_start, pay_period_end,
                pay_type, hours_regular, hours_overtime, rate_regular, rate_overtime,
                gross_pay, federal_withholding, state_withholding, social_security,
                medicare, local_tax, other_deductions, net_pay, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cid,
                employee_id,
                pay_date,
                pay_period_start,
                pay_period_end,
                pay_type,
                hours_regular,
                hours_overtime,
                rate_regular,
                rate_overtime,
                gross_pay,
                federal_withholding,
                state_withholding,
                social_security,
                medicare,
                local_tax,
                other_deductions,
                net_pay,
                notes,
            ),
        )

        conn.commit()
        flash(
            f"Payroll entry saved. Gross: ${gross_pay:.2f} | Federal: ${federal_withholding:.2f} | Net: ${net_pay:.2f}"
        )
        conn.close()
        return redirect(url_for("payroll.employee_payroll"))

    employees = conn.execute(
        """
        SELECT
            id,
            first_name,
            last_name,
            pay_type,
            pay_frequency,
            hourly_rate,
            overtime_rate,
            salary_amount,
            is_active,
            federal_filing_status,
            w4_filing_status,
            w4_step2_checked,
            w4_step3_amount,
            w4_step4a_other_income,
            w4_step4b_deductions,
            w4_step4c_extra_withholding
        FROM employees
        WHERE company_id = ? AND is_active = 1
        ORDER BY first_name, last_name
        """,
        (cid,),
    ).fetchall()

    rows = conn.execute(
        """
        SELECT p.*, e.first_name, e.last_name
        FROM payroll_entries p
        JOIN employees e ON p.employee_id = e.id
        WHERE p.company_id = ?
        ORDER BY p.pay_date DESC, p.id DESC
        """,
        (cid,),
    ).fetchall()

    preview_taxes = calculate_payroll_taxes_for_employee(
        employee={
            "pay_frequency": "Biweekly",
            "federal_filing_status": "Single",
            "w4_filing_status": "Single",
            "w4_step2_checked": 0,
            "w4_step3_amount": 0,
            "w4_step4a_other_income": 0,
            "w4_step4b_deductions": 0,
            "w4_step4c_extra_withholding": 0,
        },
        gross_pay=0,
        company_id=cid,
        conn=conn,
    )

    conn.close()

    employee_options = "".join(
        f"""
        <option
            value='{e["id"]}'
            data-pay-type='{e["pay_type"] or "Hourly"}'
            data-pay-frequency='{e["pay_frequency"] or "Biweekly"}'
            data-hourly-rate='{e["hourly_rate"] or 0}'
            data-overtime-rate='{e["overtime_rate"] or 0}'
            data-salary-amount='{e["salary_amount"] or 0}'
            data-filing-status='{e["w4_filing_status"] or e["federal_filing_status"] or "Single"}'
            data-step2-checked='{1 if (e["w4_step2_checked"] or 0) else 0}'
            data-step3-amount='{e["w4_step3_amount"] or 0}'
            data-step4a='{e["w4_step4a_other_income"] or 0}'
            data-step4b='{e["w4_step4b_deductions"] or 0}'
            data-step4c='{e["w4_step4c_extra_withholding"] or 0}'
        >
            {e["first_name"]} {e["last_name"]}
        </option>
        """
        for e in employees
    )

    payroll_rows = "".join(
        f"""
        <tr>
            <td>{r['pay_date'] or '-'}</td>
            <td>{(r['first_name'] or '').strip()} {(r['last_name'] or '').strip()}</td>
            <td>{r['pay_type'] or '-'}</td>
            <td>${float(r['gross_pay'] or 0):.2f}</td>
            <td>${float(r['federal_withholding'] or 0):.2f}</td>
            <td>${float(r['state_withholding'] or 0):.2f}</td>
            <td>${float(r['social_security'] or 0):.2f}</td>
            <td>${float(r['medicare'] or 0):.2f}</td>
            <td>${float(r['local_tax'] or 0):.2f}</td>
            <td>${float(r['other_deductions'] or 0):.2f}</td>
            <td>${float(r['net_pay'] or 0):.2f}</td>
            <td>
                <form method='post'
                      action='{url_for("payroll.delete_payroll_entry", payroll_id=r["id"])}'
                      onsubmit="return confirm('Delete this payroll entry?');"
                      style='margin:0;'>
                    <button class='btn danger small' type='submit'>Delete</button>
                </form>
            </td>
        </tr>
        """
        for r in rows
    )

    tax_defaults_html = f"""
    <div class='card'>
        <h2>Current Tax Defaults</h2>
        <div class='grid'>
            <div><strong>Provider</strong><br>{preview_taxes.get('provider', 'internal')}</div>
            <div><strong>State</strong><br>{preview_taxes.get('state_name', '-') or '-'}</div>
            <div><strong>Social Security</strong><br>6.20%</div>
            <div><strong>Medicare</strong><br>1.45%</div>
            <div><strong>Local</strong><br>{preview_taxes.get('local_name', '-') or '-'}</div>
        </div>
    </div>
    """

    content = f"""
    <div class='card'>
        <div style='display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;'>
            <div>
                <h1 style='margin-bottom:6px;'>Payroll</h1>
                <p class='muted' style='margin:0;'>Track employee pay and payroll deductions.</p>
            </div>
            <div class='row-actions'>
                <a href='{url_for("payroll.export_payroll")}' class='btn secondary'>Export CSV</a>
                <a href='/settings/taxes' class='btn warning'>Tax Defaults</a>
                <a href='{url_for("employees.employees")}' class='btn secondary'>Back to Employees</a>
            </div>
        </div>
    </div>

    {tax_defaults_html}

    <div class='card'>
        <h2>New Payroll Entry</h2>
        <form method='post' id='payroll_form'>
            <div class='grid'>
                <div>
                    <label>Employee</label>
                    <select name='employee_id' id='employee_id' required onchange='fillEmployeePayrollInfo(); triggerPayrollPreview();'>
                        <option value=''>Select employee</option>
                        {employee_options}
                    </select>
                </div>

                <div>
                    <label>Pay Date</label>
                    <input type='date' name='pay_date' value='{date.today().isoformat()}' required>
                </div>

                <div>
                    <label>Pay Period Start</label>
                    <input type='date' name='pay_period_start'>
                </div>

                <div>
                    <label>Pay Period End</label>
                    <input type='date' name='pay_period_end'>
                </div>

                <div>
                    <label>Pay Type</label>
                    <input type='text' id='pay_type_display' readonly placeholder='Auto-filled from employee'>
                </div>

                <div>
                    <label>Pay Frequency</label>
                    <input type='text' id='pay_frequency_display' readonly placeholder='Auto-filled from employee'>
                </div>

                <div id='hourly_rate_wrap'>
                    <label>Regular Rate</label>
                    <input type='number' step='0.01' min='0' name='rate_regular' id='rate_regular' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div id='overtime_rate_wrap'>
                    <label>Overtime Rate</label>
                    <input type='number' step='0.01' min='0' name='rate_overtime' id='rate_overtime' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div id='salary_amount_wrap' style='display:none;'>
                    <label>Annual Salary</label>
                    <input type='number' step='0.01' min='0' id='salary_amount_display' readonly value='0'>
                </div>

                <div id='salary_per_period_wrap' style='display:none;'>
                    <label>Gross Pay</label>
                    <input type='number' step='0.01' min='0' id='salary_per_period_display' readonly value='0'>
                </div>

                <div id='regular_hours_wrap'>
                    <label>Regular Hours</label>
                    <input type='number' step='0.01' min='0' name='hours_regular' id='hours_regular' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div id='overtime_hours_wrap'>
                    <label>Overtime Hours</label>
                    <input type='number' step='0.01' min='0' name='hours_overtime' id='hours_overtime' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div>
                    <label>Other Deductions</label>
                    <input type='number' step='0.01' min='0' name='other_deductions' id='other_deductions' value='0' oninput='triggerPayrollPreview()'>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>W-4 / Tax Snapshot</label>
                    <div class='card' style='padding:12px; margin-bottom:0; background:#f8fbff; border:1px solid #d7e6ff; box-shadow:none;'>
                        <div class='grid'>
                            <div><strong>Filing Status</strong><br><span id='w4_filing_status_display'>-</span></div>
                            <div><strong>Step 2 Checked</strong><br><span id='w4_step2_display'>-</span></div>
                            <div><strong>Step 3</strong><br><span id='w4_step3_display'>$0.00</span></div>
                            <div><strong>Step 4a</strong><br><span id='w4_step4a_display'>$0.00</span></div>
                            <div><strong>Step 4b</strong><br><span id='w4_step4b_display'>$0.00</span></div>
                            <div><strong>Step 4c</strong><br><span id='w4_step4c_display'>$0.00</span></div>
                        </div>
                    </div>
                </div>

                <div style='grid-column:1 / -1;'>
                    <label>Notes</label>
                    <textarea name='notes'></textarea>
                </div>
            </div>

            <div class='row-actions' style='margin-top:20px;'>
                <button class='btn success' type='submit'>Save Payroll Entry</button>
            </div>
        </form>
    </div>

    <div class='card' id='payroll_preview_card'>
        <h2>Payroll Preview</h2>
        <div class='grid'>
            <div><strong>Gross Pay</strong><br><span id='preview_gross'>$0.00</span></div>
            <div><strong>Federal</strong><br><span id='preview_federal'>$0.00</span></div>
            <div><strong>State</strong><br><span id='preview_state'>$0.00</span></div>
            <div><strong>Social Security</strong><br><span id='preview_ss'>$0.00</span></div>
            <div><strong>Medicare</strong><br><span id='preview_medicare'>$0.00</span></div>
            <div><strong>Local Tax</strong><br><span id='preview_local'>$0.00</span></div>
            <div><strong>Other Deductions</strong><br><span id='preview_other'>$0.00</span></div>
            <div><strong>Estimated Net Pay</strong><br><span id='preview_net'>$0.00</span></div>
        </div>
        <div class='muted' id='preview_meta' style='margin-top:14px;'>Select an employee to preview payroll.</div>
    </div>

    <div class='card'>
        <h2>Payroll History</h2>
        <table>
            <tr>
                <th>Date</th>
                <th>Employee</th>
                <th>Pay Type</th>
                <th>Gross</th>
                <th>Federal</th>
                <th>State</th>
                <th>SS</th>
                <th>Medicare</th>
                <th>Local</th>
                <th>Other</th>
                <th>Net</th>
                <th>Actions</th>
            </tr>
            {payroll_rows or "<tr><td colspan='12' class='muted'>No payroll entries yet.</td></tr>"}
        </table>
    </div>

<script>
let payrollPreviewTimeout = null;

function formatMoney(value) {{
    const num = parseFloat(value || 0);
    return '$' + num.toFixed(2);
}}

function fillEmployeePayrollInfo() {{
    const sel = document.getElementById('employee_id');
    const opt = sel.options[sel.selectedIndex];

    const payTypeDisplay = document.getElementById('pay_type_display');
    const payFrequencyDisplay = document.getElementById('pay_frequency_display');
    const rateRegular = document.getElementById('rate_regular');
    const rateOvertime = document.getElementById('rate_overtime');
    const salaryDisplay = document.getElementById('salary_amount_display');
    const salaryPerPeriodDisplay = document.getElementById('salary_per_period_display');

    const hourlyWrap = document.getElementById('hourly_rate_wrap');
    const overtimeWrap = document.getElementById('overtime_rate_wrap');
    const overtimeHoursWrap = document.getElementById('overtime_hours_wrap');
    const regularHoursWrap = document.getElementById('regular_hours_wrap');
    const salaryWrap = document.getElementById('salary_amount_wrap');
    const salaryPerPeriodWrap = document.getElementById('salary_per_period_wrap');

    const hoursRegular = document.getElementById('hours_regular');
    const hoursOvertime = document.getElementById('hours_overtime');

    const w4FilingStatusDisplay = document.getElementById('w4_filing_status_display');
    const w4Step2Display = document.getElementById('w4_step2_display');
    const w4Step3Display = document.getElementById('w4_step3_display');
    const w4Step4aDisplay = document.getElementById('w4_step4a_display');
    const w4Step4bDisplay = document.getElementById('w4_step4b_display');
    const w4Step4cDisplay = document.getElementById('w4_step4c_display');

    if (!opt || !opt.value) {{
        payTypeDisplay.value = '';
        payFrequencyDisplay.value = '';
        rateRegular.value = 0;
        rateOvertime.value = 0;
        salaryDisplay.value = 0;
        salaryPerPeriodDisplay.value = 0;
        hoursRegular.value = 0;
        hoursOvertime.value = 0;

        w4FilingStatusDisplay.innerText = '-';
        w4Step2Display.innerText = '-';
        w4Step3Display.innerText = '$0.00';
        w4Step4aDisplay.innerText = '$0.00';
        w4Step4bDisplay.innerText = '$0.00';
        w4Step4cDisplay.innerText = '$0.00';

        salaryWrap.style.display = 'none';
        salaryPerPeriodWrap.style.display = 'none';
        hourlyWrap.style.display = 'block';
        overtimeWrap.style.display = 'block';
        overtimeHoursWrap.style.display = 'block';
        regularHoursWrap.style.display = 'block';
        return;
    }}

    const payType = opt.getAttribute('data-pay-type') || 'Hourly';
    const payFrequency = opt.getAttribute('data-pay-frequency') || 'Biweekly';
    const hourlyRate = parseFloat(opt.getAttribute('data-hourly-rate') || '0');
    const overtimeRate = parseFloat(opt.getAttribute('data-overtime-rate') || '0');
    const salaryAmount = parseFloat(opt.getAttribute('data-salary-amount') || '0');

    const filingStatus = opt.getAttribute('data-filing-status') || 'Single';
    const step2Checked = parseInt(opt.getAttribute('data-step2-checked') || '0', 10) === 1;
    const step3Amount = parseFloat(opt.getAttribute('data-step3-amount') || '0');
    const step4a = parseFloat(opt.getAttribute('data-step4a') || '0');
    const step4b = parseFloat(opt.getAttribute('data-step4b') || '0');
    const step4c = parseFloat(opt.getAttribute('data-step4c') || '0');

    payTypeDisplay.value = payType;
    payFrequencyDisplay.value = payFrequency;

    w4FilingStatusDisplay.innerText = filingStatus;
    w4Step2Display.innerText = step2Checked ? 'Yes' : 'No';
    w4Step3Display.innerText = '$' + step3Amount.toFixed(2);
    w4Step4aDisplay.innerText = '$' + step4a.toFixed(2);
    w4Step4bDisplay.innerText = '$' + step4b.toFixed(2);
    w4Step4cDisplay.innerText = '$' + step4c.toFixed(2);

    let perPeriod = 0;
    if (payFrequency === 'Weekly') {{
        perPeriod = salaryAmount / 52;
    }} else if (payFrequency === 'Biweekly') {{
        perPeriod = salaryAmount / 26;
    }} else if (payFrequency === 'Semi-Monthly' || payFrequency === 'Semimonthly') {{
        perPeriod = salaryAmount / 24;
    }} else if (payFrequency === 'Monthly') {{
        perPeriod = salaryAmount / 12;
    }} else if (payFrequency === 'Quarterly') {{
        perPeriod = salaryAmount / 4;
    }} else if (payFrequency === 'Yearly') {{
        perPeriod = salaryAmount;
    }} else {{
        perPeriod = salaryAmount / 26;
    }}

    if (payType === 'Salary') {{
        salaryWrap.style.display = 'block';
        salaryPerPeriodWrap.style.display = 'block';
        hourlyWrap.style.display = 'none';
        overtimeWrap.style.display = 'none';
        overtimeHoursWrap.style.display = 'none';
        regularHoursWrap.style.display = 'none';

        salaryDisplay.value = salaryAmount.toFixed(2);
        salaryPerPeriodDisplay.value = perPeriod.toFixed(2);
        rateRegular.value = 0;
        rateOvertime.value = 0;
        hoursOvertime.value = 0;
        hoursRegular.value = 1;
    }} else {{
        salaryWrap.style.display = 'none';
        salaryPerPeriodWrap.style.display = 'none';
        hourlyWrap.style.display = 'block';
        overtimeWrap.style.display = 'block';
        overtimeHoursWrap.style.display = 'block';
        regularHoursWrap.style.display = 'block';

        rateRegular.value = hourlyRate.toFixed(2);
        rateOvertime.value = overtimeRate.toFixed(2);
    }}
}}

function resetPayrollPreview(message) {{
    document.getElementById('preview_gross').innerText = '$0.00';
    document.getElementById('preview_federal').innerText = '$0.00';
    document.getElementById('preview_state').innerText = '$0.00';
    document.getElementById('preview_ss').innerText = '$0.00';
    document.getElementById('preview_medicare').innerText = '$0.00';
    document.getElementById('preview_local').innerText = '$0.00';
    document.getElementById('preview_other').innerText = '$0.00';
    document.getElementById('preview_net').innerText = '$0.00';
    document.getElementById('preview_meta').innerText = message || 'Select an employee to preview payroll.';
}}

async function runPayrollPreview() {{
    const form = document.getElementById('payroll_form');
    const employeeId = document.getElementById('employee_id').value;

    if (!employeeId) {{
        resetPayrollPreview('Select an employee to preview payroll.');
        return;
    }}

    const formData = new FormData(form);

    try {{
        const response = await fetch("{url_for('payroll.payroll_preview')}", {{
            method: 'POST',
            body: formData
        }});

        const data = await response.json();

        if (!response.ok || !data.ok) {{
            resetPayrollPreview(data.message || 'Unable to preview payroll.');
            return;
        }}

        document.getElementById('preview_gross').innerText = formatMoney(data.gross_pay);
        document.getElementById('preview_federal').innerText = formatMoney(data.federal_withholding);
        document.getElementById('preview_state').innerText = formatMoney(data.state_withholding);
        document.getElementById('preview_ss').innerText = formatMoney(data.social_security);
        document.getElementById('preview_medicare').innerText = formatMoney(data.medicare);
        document.getElementById('preview_local').innerText = formatMoney(data.local_tax);
        document.getElementById('preview_other').innerText = formatMoney(data.other_deductions);
        document.getElementById('preview_net').innerText = formatMoney(data.net_pay);

        document.getElementById('preview_meta').innerText =
            'Provider: ' + (data.provider || 'internal') +
            ' | State: ' + (data.state_name || '-') +
            ' | Local: ' + (data.local_name || '-') +
            ' | Pay Type: ' + (data.pay_type || '-') +
            ' | Frequency: ' + (data.pay_frequency || '-') +
            ' | W-4 Step 3: ' + formatMoney(data.w4_step3_amount || 0);
    }} catch (error) {{
        resetPayrollPreview('Unable to preview payroll.');
    }}
}}

function triggerPayrollPreview() {{
    if (payrollPreviewTimeout) {{
        clearTimeout(payrollPreviewTimeout);
    }}

    payrollPreviewTimeout = setTimeout(runPayrollPreview, 250);
}}

document.addEventListener('DOMContentLoaded', function() {{
    fillEmployeePayrollInfo();
    triggerPayrollPreview();
}});
</script>
"""
    return render_page(content, "Employee Payroll")


@payroll_bp.route("/employees/payroll/export")
@login_required
@require_permission("can_manage_payroll")
def export_payroll():
    ensure_employee_payroll_columns()
    ensure_bookkeeping_history_table()
    ensure_payroll_table_structure()

    conn = get_db_connection()
    cid = session["company_id"]

    rows = conn.execute(
        """
        SELECT
            p.*,
            e.first_name,
            e.last_name
        FROM payroll_entries p
        JOIN employees e ON p.employee_id = e.id
        WHERE p.company_id = ?
        ORDER BY p.pay_date DESC, p.id DESC
        """,
        (cid,),
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Payroll ID",
        "Pay Date",
        "Employee",
        "Pay Type",
        "Pay Period Start",
        "Pay Period End",
        "Regular Hours",
        "Overtime Hours",
        "Regular Rate",
        "Overtime Rate",
        "Gross Pay",
        "Federal Withholding",
        "State Withholding",
        "Social Security",
        "Medicare",
        "Local Tax",
        "Other Deductions",
        "Net Pay",
        "Notes",
    ])

    for r in rows:
        employee_name = f"{r['first_name'] or ''} {r['last_name'] or ''}".strip()

        writer.writerow([
            r["id"] or "",
            r["pay_date"] or "",
            employee_name,
            r["pay_type"] or "",
            r["pay_period_start"] or "",
            r["pay_period_end"] or "",
            float(r["hours_regular"] or 0),
            float(r["hours_overtime"] or 0),
            float(r["rate_regular"] or 0),
            float(r["rate_overtime"] or 0),
            float(r["gross_pay"] or 0),
            float(r["federal_withholding"] or 0),
            float(r["state_withholding"] or 0),
            float(r["social_security"] or 0),
            float(r["medicare"] or 0),
            float(r["local_tax"] or 0),
            float(r["other_deductions"] or 0),
            float(r["net_pay"] or 0),
            r["notes"] or "",
        ])

    conn.close()

    csv_data = output.getvalue()
    output.close()

    filename = f"payroll_export_{date.today().isoformat()}.csv"

    response = make_response(csv_data)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response


@payroll_bp.route("/payroll/add", methods=["GET", "POST"])
@login_required
@require_permission("can_manage_payroll")
def add_payroll():
    return redirect(url_for("payroll.employee_payroll"))


@payroll_bp.route("/employees/payroll/<int:payroll_id>/delete", methods=["POST"])
@login_required
@require_permission("can_manage_payroll")
def delete_payroll_entry(payroll_id):
    conn = get_db_connection()
    cid = session["company_id"]

    row = conn.execute(
        """
        SELECT id, ledger_entry_id
        FROM payroll_entries
        WHERE id = ? AND company_id = ?
        """,
        (payroll_id, cid),
    ).fetchone()

    if not row:
        conn.close()
        flash("Payroll entry not found.")
        return redirect(url_for("payroll.employee_payroll"))

    if "ledger_entry_id" in row.keys() and row["ledger_entry_id"]:
        conn.execute(
            "DELETE FROM ledger_entries WHERE id = ? AND company_id = ?",
            (row["ledger_entry_id"], cid),
        )

    conn.execute(
        """
        DELETE FROM payroll_entries
        WHERE id = ? AND company_id = ?
        """,
        (payroll_id, cid),
    )
    conn.commit()
    conn.close()

    flash("Payroll entry deleted.")
    return redirect(url_for("payroll.employee_payroll"))