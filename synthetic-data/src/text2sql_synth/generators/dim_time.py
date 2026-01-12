"""Generator for dim_time dimension table.

Generates a time dimension covering the full date range with calendar attributes
and seasonality flags for analysis.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "dim_time"


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the dim_time dimension table.

    Creates one row per day in the configured time window with:
    - date_key: Integer key in YYYYMMDD format
    - full_date: The actual date
    - day_of_week: 0=Monday, 6=Sunday
    - day_name: Monday, Tuesday, etc.
    - day_of_month: 1-31
    - day_of_year: 1-366
    - week_of_year: ISO week number
    - month: 1-12
    - month_name: January, February, etc.
    - quarter: 1-4
    - year: 4-digit year
    - is_weekend: True if Saturday or Sunday
    - is_holiday: True for major US holidays (simplified)
    - is_month_start: True if first day of month
    - is_month_end: True if last day of month
    - is_quarter_start: True if first day of quarter
    - is_quarter_end: True if last day of quarter
    - seasonality_factor: Float multiplier for seasonal patterns

    Args:
        ctx: Generation context with RNG and time window.
        cfg: Configuration with distribution parameters.

    Returns:
        DataFrame with time dimension data.
    """
    rng = ctx.rng_for(TABLE_NAME)
    start_date = cfg.time_window.start_date
    end_date = cfg.time_window.end_date

    # Generate all dates in range
    num_days = (end_date - start_date).days + 1
    dates = [start_date + timedelta(days=i) for i in range(num_days)]

    # Day names
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    # US Federal Holidays (simplified - fixed dates only)
    # In practice, some holidays are observed on different days
    def is_us_holiday(d: date) -> bool:
        """Check if date is a major US holiday (simplified)."""
        # New Year's Day
        if d.month == 1 and d.day == 1:
            return True
        # Independence Day
        if d.month == 7 and d.day == 4:
            return True
        # Veterans Day
        if d.month == 11 and d.day == 11:
            return True
        # Christmas
        if d.month == 12 and d.day == 25:
            return True
        # Thanksgiving (4th Thursday of November)
        if d.month == 11 and d.weekday() == 3:
            # Count Thursdays in November
            first_day = date(d.year, 11, 1)
            thursdays = 0
            current = first_day
            while current <= d:
                if current.weekday() == 3:
                    thursdays += 1
                    if current == d and thursdays == 4:
                        return True
                current += timedelta(days=1)
        return False

    def get_seasonality_factor(d: date, strength: float) -> float:
        """Compute seasonality factor for a date.

        Models retail seasonality with peaks around holidays and summer lulls.
        Factor ranges from (1-strength) to (1+strength).
        """
        day_of_year = d.timetuple().tm_yday
        # Base sinusoidal pattern: peak in December, trough in summer
        # Shift so December is high, July is low
        import math
        # Day 350 (mid-December) should be peak
        # Day 180 (late June) should be trough
        phase = 2 * math.pi * (day_of_year - 350) / 365
        seasonal = math.cos(phase)
        # Scale to [1-strength, 1+strength]
        return 1.0 + (seasonal * strength)

    def last_day_of_month(d: date) -> date:
        """Get the last day of the month for a date."""
        if d.month == 12:
            return date(d.year, 12, 31)
        return date(d.year, d.month + 1, 1) - timedelta(days=1)

    rows = []
    for d in dates:
        is_weekend = d.weekday() >= 5
        month_end = last_day_of_month(d)

        row = {
            "date_key": int(d.strftime("%Y%m%d")),
            "full_date": d,
            "day_of_week": d.weekday(),
            "day_name": day_names[d.weekday()],
            "day_of_month": d.day,
            "day_of_year": d.timetuple().tm_yday,
            "week_of_year": d.isocalendar()[1],
            "month": d.month,
            "month_name": month_names[d.month - 1],
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
            "is_weekend": is_weekend,
            "is_holiday": is_us_holiday(d),
            "is_month_start": d.day == 1,
            "is_month_end": d == month_end,
            "is_quarter_start": d.day == 1 and d.month in (1, 4, 7, 10),
            "is_quarter_end": d == month_end and d.month in (3, 6, 9, 12),
            "seasonality_factor": round(
                get_seasonality_factor(d, cfg.distribution.seasonality_strength), 4
            ),
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Register in context
    ctx.register_table(TABLE_NAME, df)

    return df
