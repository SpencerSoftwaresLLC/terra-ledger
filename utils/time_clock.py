from datetime import date, timedelta


def weekday_label(day_number):
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


def get_current_pay_period(start_day):
    today = date.today()
    start_day = int(start_day)

    days_since_start = (today.weekday() - start_day) % 7
    start_date = today - timedelta(days=days_since_start)
    end_date = start_date + timedelta(days=6)

    return start_date, end_date


def get_previous_pay_period(start_day):
    current_start, current_end = get_current_pay_period(start_day)
    previous_end = current_start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=6)
    return previous_start, previous_end


def get_company_time_clock_start_day(company_profile_row):
    if company_profile_row and company_profile_row.get("time_clock_pay_period_start_day") is not None:
        try:
            value = int(company_profile_row["time_clock_pay_period_start_day"])
            if 0 <= value <= 6:
                return value
        except Exception:
            pass
    return 2