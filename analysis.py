import pandas as pd

def transform(df):
    if df.empty:
        return {}

    # ======================
    # STANDARDIZE COLUMN
    # ======================
    df.columns = df.columns.str.strip().str.lower()

    # ======================
    # PREPROCESSING
    # ======================
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])

    numeric_cols = [
        'global_active_power',
        'sub_metering_1',
        'sub_metering_2',
        'sub_metering_3'
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['global_active_power'])

    # ======================
    # FEATURE ENGINEERING
    # ======================
    df['date_only'] = df['datetime'].dt.date
    df['hour'] = df['datetime'].dt.hour
    df['month'] = df['datetime'].dt.to_period('M').astype(str)
    df['week'] = df['datetime'].dt.to_period('W').astype(str)
    df['day_name'] = df['datetime'].dt.day_name()

    # ======================
    # 1. DAILY USAGE
    # ======================
    daily_usage = (
        df.groupby('date_only')['global_active_power']
        .sum()
        .reset_index(name='total_power')
    )

    # ======================
    # 2. HOURLY USAGE
    # ======================
    hourly_usage = (
        df.groupby('hour')['global_active_power']
        .mean()
        .reset_index(name='avg_power')
    )

    peak_row = hourly_usage.loc[hourly_usage['avg_power'].idxmax()]
    peak_hour = int(peak_row['hour'])
    peak_value = float(peak_row['avg_power'])

    # ======================
    # 3. WEEKLY USAGE
    # ======================
    weekly_usage = (
        df.groupby('week')['global_active_power']
        .sum()
        .reset_index(name='total_power')
    )

    # ======================
    # 4. MONTHLY USAGE
    # ======================
    monthly_usage = (
        df.groupby('month')['global_active_power']
        .sum()
        .reset_index(name='total_power')
    )

    # ======================
    # 5. WEEKDAY PATTERN
    # ======================
    weekday_pattern = (
        df.groupby('day_name')['global_active_power']
        .mean()
        .reset_index(name='avg_power')
    )

    # ======================
    # 6. MONTHLY HOURLY USAGE
    # ======================
    monthly_hourly_usage = (
        df.groupby(['month', 'hour'])['global_active_power']
        .mean()
        .reset_index(name='avg_power')
    )

    # ======================
    # 7. PEAK PER MONTH
    # ======================
    peak_monthly = monthly_hourly_usage.loc[
        monthly_hourly_usage.groupby('month')['avg_power'].idxmax()
    ]

    peak_monthly = peak_monthly.rename(columns={'avg_power': 'value'})

    # ======================
    # 8. SUMMARY STATS
    # ======================
    avg_usage = float(df['global_active_power'].mean())
    max_usage = float(df['global_active_power'].max())
    min_usage = float(df['global_active_power'].min())

    # ======================
    # 9. SUB METERING
    # ======================
    sub_metering = {
        "sub1": float(df['sub_metering_1'].sum()),
        "sub2": float(df['sub_metering_2'].sum()),
        "sub3": float(df['sub_metering_3'].sum())
    }

    return {
        "daily_usage": daily_usage,
        "hourly_usage": hourly_usage,
        "weekly_usage": weekly_usage,
        "monthly_usage": monthly_usage,
        "weekday_pattern": weekday_pattern,
        "monthly_hourly_usage": monthly_hourly_usage,
        "peak_monthly": peak_monthly,
        "avg_usage": avg_usage,
        "peak_hour": peak_hour,
        "peak_value": peak_value,
        "max_usage": max_usage,
        "min_usage": min_usage,
        "sub_metering": sub_metering
    }