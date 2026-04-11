# TerraLedger/ai/insights.py

from datetime import date, datetime
from statistics import mean

from db import get_db_connection


MONTH_LABELS = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _pct_change(new_value, old_value):
    new_value = _safe_float(new_value)
    old_value = _safe_float(old_value)

    if old_value == 0:
        if new_value == 0:
            return 0.0
        return None

    return ((new_value - old_value) / old_value) * 100.0


def _round_money(value):
    return round(_safe_float(value), 2)


def _today():
    return date.today()


def _current_year():
    return _today().year


def _current_month():
    return _today().month


def _day_of_year(dt=None):
    dt = dt or _today()
    return dt.timetuple().tm_yday


def _days_in_year(year):
    return 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365


def _month_name(month_number):
    return MONTH_LABELS.get(int(month_number), f"Month {month_number}")


def _dict_from_row(row):
    if not row:
        return {}
    return dict(row)


def _fetch_company_name(conn, company_id):
    row = conn.execute(
        """
        SELECT name
        FROM companies
        WHERE id = %s
        """,
        (company_id,),
    ).fetchone()

    if not row:
        return "Company"

    return (row["name"] or "Company").strip() or "Company"


def _get_paid_invoice_revenue_by_month(conn, company_id, year):
    rows = conn.execute(
        """
        SELECT
            EXTRACT(MONTH FROM payment_date)::INT AS month_num,
            COALESCE(SUM(amount), 0) AS revenue
        FROM invoice_payments
        WHERE company_id = %s
          AND payment_date IS NOT NULL
          AND EXTRACT(YEAR FROM payment_date)::INT = %s
        GROUP BY month_num
        ORDER BY month_num
        """,
        (company_id, year),
    ).fetchall()

    month_map = {m: 0.0 for m in range(1, 13)}
    for row in rows:
        month_map[_safe_int(row["month_num"])] = _round_money(row["revenue"])

    results = []
    for month_num in range(1, 13):
        results.append({
            "month": month_num,
            "month_name": _month_name(month_num),
            "revenue": _round_money(month_map[month_num]),
        })
    return results


def _get_invoice_totals_snapshot(conn, company_id):
    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN COALESCE(status, '') != 'Paid' THEN balance_due ELSE 0 END), 0) AS unpaid_balance_total,
            COALESCE(SUM(CASE WHEN COALESCE(status, '') = 'Partial' THEN balance_due ELSE 0 END), 0) AS partial_balance_total,
            COALESCE(SUM(CASE WHEN COALESCE(status, '') = 'Unpaid' THEN balance_due ELSE 0 END), 0) AS unpaid_only_total,
            COUNT(CASE WHEN COALESCE(status, '') != 'Paid' THEN 1 END) AS open_invoice_count,
            COUNT(CASE WHEN COALESCE(status, '') = 'Partial' THEN 1 END) AS partial_invoice_count,
            COUNT(CASE WHEN COALESCE(status, '') = 'Unpaid' THEN 1 END) AS unpaid_invoice_count
        FROM invoices
        WHERE company_id = %s
        """,
        (company_id,),
    ).fetchone()

    row = _dict_from_row(row)

    return {
        "unpaid_balance_total": _round_money(row.get("unpaid_balance_total")),
        "partial_balance_total": _round_money(row.get("partial_balance_total")),
        "unpaid_only_total": _round_money(row.get("unpaid_only_total")),
        "open_invoice_count": _safe_int(row.get("open_invoice_count")),
        "partial_invoice_count": _safe_int(row.get("partial_invoice_count")),
        "unpaid_invoice_count": _safe_int(row.get("unpaid_invoice_count")),
    }


def _get_job_counts_snapshot(conn, company_id):
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_jobs,
            COUNT(CASE WHEN COALESCE(status, '') = 'Scheduled' THEN 1 END) AS scheduled_jobs,
            COUNT(CASE WHEN COALESCE(status, '') = 'Invoiced' THEN 1 END) AS invoiced_jobs,
            COUNT(CASE WHEN COALESCE(status, '') IN ('Finished', 'Completed') THEN 1 END) AS completed_jobs
        FROM jobs
        WHERE company_id = %s
        """,
        (company_id,),
    ).fetchone()

    row = _dict_from_row(row)

    return {
        "total_jobs": _safe_int(row.get("total_jobs")),
        "scheduled_jobs": _safe_int(row.get("scheduled_jobs")),
        "invoiced_jobs": _safe_int(row.get("invoiced_jobs")),
        "completed_jobs": _safe_int(row.get("completed_jobs")),
    }


def _get_first_payment_year(conn, company_id):
    row = conn.execute(
        """
        SELECT MIN(EXTRACT(YEAR FROM payment_date)::INT) AS first_year
        FROM invoice_payments
        WHERE company_id = %s
          AND payment_date IS NOT NULL
        """,
        (company_id,),
    ).fetchone()

    if not row or row["first_year"] is None:
        return None
    return _safe_int(row["first_year"])


def _get_ytd_revenue(conn, company_id, year, through_date=None):
    through_date = through_date or _today()

    row = conn.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS ytd_revenue
        FROM invoice_payments
        WHERE company_id = %s
          AND payment_date IS NOT NULL
          AND payment_date >= %s
          AND payment_date <= %s
        """,
        (
            company_id,
            date(year, 1, 1).isoformat(),
            through_date.isoformat(),
        ),
    ).fetchone()

    return _round_money(row["ytd_revenue"] if row else 0)


def _get_last_n_days_revenue(conn, company_id, days=30, through_date=None):
    through_date = through_date or _today()
    start_date = through_date.fromordinal(through_date.toordinal() - max(days - 1, 0))

    row = conn.execute(
        """
        SELECT COALESCE(SUM(amount), 0) AS revenue_total
        FROM invoice_payments
        WHERE company_id = %s
          AND payment_date IS NOT NULL
          AND payment_date >= %s
          AND payment_date <= %s
        """,
        (company_id, start_date.isoformat(), through_date.isoformat()),
    ).fetchone()

    total = _round_money(row["revenue_total"] if row else 0)
    average_per_day = _round_money(total / days) if days > 0 else 0.0

    return {
        "days": days,
        "start_date": start_date.isoformat(),
        "end_date": through_date.isoformat(),
        "revenue_total": total,
        "average_per_day": average_per_day,
    }


def _get_same_period_last_year_date(today_value=None):
    today_value = today_value or _today()
    try:
        return date(today_value.year - 1, today_value.month, today_value.day)
    except Exception:
        # Handles Feb 29 fallback
        if today_value.month == 2 and today_value.day == 29:
            return date(today_value.year - 1, 2, 28)
        return date(today_value.year - 1, today_value.month, min(today_value.day, 28))


def _build_seasonality_factors(conn, company_id, current_year):
    first_year = _get_first_payment_year(conn, company_id)
    if first_year is None:
        return {
            "has_history": False,
            "month_factors": {m: 1.0 / 12.0 for m in range(1, 13)},
            "years_used": [],
        }

    usable_years = [y for y in range(first_year, current_year) if y < current_year]
    if not usable_years:
        return {
            "has_history": False,
            "month_factors": {m: 1.0 / 12.0 for m in range(1, 13)},
            "years_used": [],
        }

    year_month_maps = []
    for year in usable_years:
        monthly = _get_paid_invoice_revenue_by_month(conn, company_id, year)
        year_total = sum(_safe_float(row["revenue"]) for row in monthly)

        if year_total <= 0:
            continue

        year_month_maps.append({
            "year": year,
            "shares": {
                row["month"]: (_safe_float(row["revenue"]) / year_total)
                for row in monthly
            },
        })

    if not year_month_maps:
        return {
            "has_history": False,
            "month_factors": {m: 1.0 / 12.0 for m in range(1, 13)},
            "years_used": [],
        }

    month_factors = {}
    for month_num in range(1, 13):
        month_shares = [year_map["shares"].get(month_num, 0.0) for year_map in year_month_maps]
        month_factors[month_num] = sum(month_shares) / len(month_shares)

    total_factor = sum(month_factors.values()) or 1.0
    month_factors = {k: (v / total_factor) for k, v in month_factors.items()}

    return {
        "has_history": True,
        "month_factors": month_factors,
        "years_used": [entry["year"] for entry in year_month_maps],
    }


def _forecast_full_year_from_seasonality(ytd_revenue, month_factors, current_month):
    ytd_factor = sum(month_factors.get(m, 0.0) for m in range(1, current_month + 1))
    if ytd_factor <= 0:
        return None
    return _round_money(ytd_revenue / ytd_factor)


def _forecast_full_year_from_run_rate(ytd_revenue, today_value=None):
    today_value = today_value or _today()
    days_elapsed = _day_of_year(today_value)
    total_days = _days_in_year(today_value.year)

    if days_elapsed <= 0:
        return None

    daily_rate = _safe_float(ytd_revenue) / days_elapsed
    return _round_money(daily_rate * total_days)


def _blended_forecast(ytd_revenue, current_month, seasonality_result, today_value=None):
    today_value = today_value or _today()

    seasonal_forecast = _forecast_full_year_from_seasonality(
        ytd_revenue=ytd_revenue,
        month_factors=seasonality_result["month_factors"],
        current_month=current_month,
    )

    run_rate_forecast = _forecast_full_year_from_run_rate(
        ytd_revenue=ytd_revenue,
        today_value=today_value,
    )

    if seasonal_forecast is None and run_rate_forecast is None:
        return {
            "forecast": 0.0,
            "seasonal_forecast": None,
            "run_rate_forecast": None,
            "method": "no_history",
        }

    if seasonal_forecast is None:
        return {
            "forecast": _round_money(run_rate_forecast),
            "seasonal_forecast": None,
            "run_rate_forecast": _round_money(run_rate_forecast),
            "method": "run_rate_only",
        }

    if run_rate_forecast is None:
        return {
            "forecast": _round_money(seasonal_forecast),
            "seasonal_forecast": _round_money(seasonal_forecast),
            "run_rate_forecast": None,
            "method": "seasonal_only",
        }

    # Blend slightly in favor of seasonality if historical data exists
    blended = (seasonal_forecast * 0.65) + (run_rate_forecast * 0.35)

    return {
        "forecast": _round_money(blended),
        "seasonal_forecast": _round_money(seasonal_forecast),
        "run_rate_forecast": _round_money(run_rate_forecast),
        "method": "blended",
    }


def get_monthly_sales(company_id, year=None):
    year = _safe_int(year, _current_year())
    conn = get_db_connection()

    try:
        company_name = _fetch_company_name(conn, company_id)
        monthly = _get_paid_invoice_revenue_by_month(conn, company_id, year)
        total = _round_money(sum(_safe_float(row["revenue"]) for row in monthly))

        return {
            "company_id": company_id,
            "company_name": company_name,
            "year": year,
            "monthly_sales": monthly,
            "total_sales": total,
        }
    finally:
        conn.close()


def get_year_over_year_sales(company_id, current_year=None):
    current_year = _safe_int(current_year, _current_year())
    previous_year = current_year - 1

    conn = get_db_connection()

    try:
        company_name = _fetch_company_name(conn, company_id)

        current_monthly = _get_paid_invoice_revenue_by_month(conn, company_id, current_year)
        previous_monthly = _get_paid_invoice_revenue_by_month(conn, company_id, previous_year)

        monthly_comparison = []
        for month_num in range(1, 13):
            current_value = _safe_float(current_monthly[month_num - 1]["revenue"])
            previous_value = _safe_float(previous_monthly[month_num - 1]["revenue"])

            monthly_comparison.append({
                "month": month_num,
                "month_name": _month_name(month_num),
                "current_year_revenue": _round_money(current_value),
                "previous_year_revenue": _round_money(previous_value),
                "percent_change": _pct_change(current_value, previous_value),
            })

        current_total = _round_money(sum(_safe_float(row["revenue"]) for row in current_monthly))
        previous_total = _round_money(sum(_safe_float(row["revenue"]) for row in previous_monthly))

        return {
            "company_id": company_id,
            "company_name": company_name,
            "current_year": current_year,
            "previous_year": previous_year,
            "current_year_total": current_total,
            "previous_year_total": previous_total,
            "percent_change": _pct_change(current_total, previous_total),
            "monthly_comparison": monthly_comparison,
        }
    finally:
        conn.close()


def get_sales_snapshot(company_id, today_value=None):
    today_value = today_value or _today()
    current_year = today_value.year
    previous_year = current_year - 1
    current_month = today_value.month

    conn = get_db_connection()

    try:
        company_name = _fetch_company_name(conn, company_id)

        current_ytd = _get_ytd_revenue(conn, company_id, current_year, today_value)
        same_day_last_year = _get_same_period_last_year_date(today_value)
        previous_ytd = _get_ytd_revenue(conn, company_id, previous_year, same_day_last_year)

        current_monthly = _get_paid_invoice_revenue_by_month(conn, company_id, current_year)
        previous_monthly = _get_paid_invoice_revenue_by_month(conn, company_id, previous_year)

        current_month_revenue = _safe_float(current_monthly[current_month - 1]["revenue"])
        previous_month_revenue = _safe_float(previous_monthly[current_month - 1]["revenue"])

        last_30 = _get_last_n_days_revenue(conn, company_id, 30, today_value)
        last_90 = _get_last_n_days_revenue(conn, company_id, 90, today_value)

        invoice_snapshot = _get_invoice_totals_snapshot(conn, company_id)
        job_snapshot = _get_job_counts_snapshot(conn, company_id)

        return {
            "company_id": company_id,
            "company_name": company_name,
            "as_of_date": today_value.isoformat(),
            "current_year": current_year,
            "previous_year": previous_year,
            "current_month": current_month,
            "current_month_name": _month_name(current_month),
            "current_ytd_revenue": current_ytd,
            "previous_ytd_revenue_same_period": previous_ytd,
            "ytd_percent_change": _pct_change(current_ytd, previous_ytd),
            "current_month_revenue": _round_money(current_month_revenue),
            "same_month_last_year_revenue": _round_money(previous_month_revenue),
            "current_month_percent_change": _pct_change(current_month_revenue, previous_month_revenue),
            "last_30_days": last_30,
            "last_90_days": last_90,
            "invoice_snapshot": invoice_snapshot,
            "job_snapshot": job_snapshot,
        }
    finally:
        conn.close()


def forecast_current_year_sales(company_id, today_value=None):
    today_value = today_value or _today()
    current_year = today_value.year
    current_month = today_value.month
    previous_year = current_year - 1

    conn = get_db_connection()

    try:
        company_name = _fetch_company_name(conn, company_id)

        current_ytd = _get_ytd_revenue(conn, company_id, current_year, today_value)
        previous_year_full = _round_money(
            sum(_safe_float(row["revenue"]) for row in _get_paid_invoice_revenue_by_month(conn, company_id, previous_year))
        )

        same_day_last_year = _get_same_period_last_year_date(today_value)
        previous_ytd = _get_ytd_revenue(conn, company_id, previous_year, same_day_last_year)

        yoy_growth = _pct_change(current_ytd, previous_ytd)

        seasonality_result = _build_seasonality_factors(conn, company_id, current_year)
        blended = _blended_forecast(
            ytd_revenue=current_ytd,
            current_month=current_month,
            seasonality_result=seasonality_result,
            today_value=today_value,
        )

        open_invoice_data = _get_invoice_totals_snapshot(conn, company_id)
        open_pipeline = _round_money(open_invoice_data["unpaid_balance_total"])

        conservative_forecast = _round_money(blended["forecast"])
        optimistic_forecast = _round_money(blended["forecast"] + (open_pipeline * 0.35))

        # Bound the forecast lightly if we have a strong prior-year anchor
        if previous_year_full > 0 and yoy_growth is not None:
            yoy_projection = _round_money(previous_year_full * (1 + (yoy_growth / 100.0)))
        else:
            yoy_projection = None

        if yoy_projection and blended["forecast"] > 0:
            final_forecast = _round_money((blended["forecast"] * 0.6) + (yoy_projection * 0.4))
        elif yoy_projection:
            final_forecast = _round_money(yoy_projection)
        else:
            final_forecast = _round_money(blended["forecast"])

        explanation = []

        if current_ytd > 0:
            explanation.append(
                f"Current year-to-date paid revenue is ${current_ytd:,.2f} through {today_value.isoformat()}."
            )

        if previous_ytd > 0:
            if yoy_growth is None:
                explanation.append(
                    "A year-over-year comparison to the same date last year could not be calculated cleanly."
                )
            else:
                direction = "ahead of" if yoy_growth >= 0 else "behind"
                explanation.append(
                    f"That is {abs(yoy_growth):.1f}% {direction} the same point last year."
                )

        if seasonality_result["has_history"]:
            years_used_text = ", ".join(str(y) for y in seasonality_result["years_used"])
            explanation.append(
                f"The forecast uses seasonality from prior years ({years_used_text}) and blends it with current run rate."
            )
        else:
            explanation.append(
                "The forecast relies more heavily on current run rate because there is limited prior-year payment history."
            )

        if open_pipeline > 0:
            explanation.append(
                f"There is also ${open_pipeline:,.2f} in open invoice balance that may still convert to collected revenue this year."
            )

        explanation.append(
            "This is a forecast based on paid invoice history and current pace, not a guaranteed outcome."
        )

        return {
            "company_id": company_id,
            "company_name": company_name,
            "as_of_date": today_value.isoformat(),
            "current_year": current_year,
            "current_month": current_month,
            "current_month_name": _month_name(current_month),
            "current_ytd_revenue": _round_money(current_ytd),
            "same_period_last_year_revenue": _round_money(previous_ytd),
            "yoy_growth_percent": yoy_growth,
            "last_year_full_revenue": _round_money(previous_year_full),
            "seasonality_years_used": seasonality_result["years_used"],
            "forecast_method": blended["method"],
            "run_rate_forecast": blended["run_rate_forecast"],
            "seasonal_forecast": blended["seasonal_forecast"],
            "yoy_projection": yoy_projection,
            "forecast_current_year_sales": _round_money(final_forecast),
            "conservative_forecast": _round_money(conservative_forecast),
            "optimistic_forecast": _round_money(max(optimistic_forecast, final_forecast)),
            "open_invoice_balance": _round_money(open_pipeline),
            "explanation": explanation,
        }
    finally:
        conn.close()


def get_ai_business_context(company_id, include_forecast=True):
    snapshot = get_sales_snapshot(company_id)
    yoy = get_year_over_year_sales(company_id, snapshot["current_year"])
    monthly = get_monthly_sales(company_id, snapshot["current_year"])

    result = {
        "snapshot": snapshot,
        "year_over_year": yoy,
        "monthly_sales": monthly,
    }

    if include_forecast:
        result["forecast"] = forecast_current_year_sales(company_id)

    return result


def format_sales_snapshot_for_ai(company_id):
    context = get_ai_business_context(company_id, include_forecast=True)

    snapshot = context["snapshot"]
    forecast = context["forecast"]
    yoy = context["year_over_year"]

    lines = [
        f"Business Snapshot for {snapshot['company_name']}",
        f"As of: {snapshot['as_of_date']}",
        "",
        f"Current YTD Revenue: ${snapshot['current_ytd_revenue']:,.2f}",
        f"Same Period Last Year: ${snapshot['previous_ytd_revenue_same_period']:,.2f}",
        f"YTD Change: {'N/A' if snapshot['ytd_percent_change'] is None else f'{snapshot['ytd_percent_change']:.1f}%'}",
        "",
        f"{snapshot['current_month_name']} Revenue: ${snapshot['current_month_revenue']:,.2f}",
        f"Same Month Last Year: ${snapshot['same_month_last_year_revenue']:,.2f}",
        f"Month Change: {'N/A' if snapshot['current_month_percent_change'] is None else f'{snapshot['current_month_percent_change']:.1f}%'}",
        "",
        f"Last 30 Days Revenue: ${snapshot['last_30_days']['revenue_total']:,.2f}",
        f"Last 90 Days Revenue: ${snapshot['last_90_days']['revenue_total']:,.2f}",
        "",
        f"Open Invoice Balance: ${snapshot['invoice_snapshot']['unpaid_balance_total']:,.2f}",
        f"Open Invoices: {snapshot['invoice_snapshot']['open_invoice_count']}",
        f"Scheduled Jobs: {snapshot['job_snapshot']['scheduled_jobs']}",
        f"Invoiced Jobs: {snapshot['job_snapshot']['invoiced_jobs']}",
        f"Completed Jobs: {snapshot['job_snapshot']['completed_jobs']}",
        "",
        f"Forecasted {forecast['current_year']} Sales: ${forecast['forecast_current_year_sales']:,.2f}",
        f"Conservative Forecast: ${forecast['conservative_forecast']:,.2f}",
        f"Optimistic Forecast: ${forecast['optimistic_forecast']:,.2f}",
        f"Forecast Method: {forecast['forecast_method']}",
        "",
        "Forecast Notes:",
    ]

    for note in forecast["explanation"]:
        lines.append(f"- {note}")

    lines.append("")
    lines.append("Monthly Revenue This Year:")

    for row in yoy["monthly_comparison"]:
        lines.append(
            f"- {row['month_name']}: ${row['current_year_revenue']:,.2f} "
            f"(last year ${row['previous_year_revenue']:,.2f})"
        )

    return "\n".join(lines)