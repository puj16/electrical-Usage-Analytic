import pandas as pd

def transform(df):
    if df.empty:
        return {}

    # ======================
    # STANDARDIZE COLUMN NAME (biar aman)
    # ======================
    df.columns = df.columns.str.strip().str.lower()

    # ======================
    # PREPROCESSING
    # ======================

    # Gabung date + time (karena di DB masih varchar)
    df['datetime'] = pd.to_datetime(
        df['date'] + ' ' + df['time'],
        format='%m/%d/%Y %I:%M:%S %p',
        errors='coerce'
    )

    # Buang data invalid
    df = df.dropna(subset=['datetime'])

    # Convert kolom numerik (jaga-jaga kalau masih string)
    numeric_cols = [
        'global_active_power',
        'sub_metering_1',
        'sub_metering_2',
        'sub_metering_3'
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['global_active_power'])

    # Feature tambahan
    df['date_only'] = df['datetime'].dt.date
    df['hour'] = df['datetime'].dt.hour

    # ======================
    # 1. TOTAL PER HARI
    # ======================
    daily_usage = (
        df.groupby('date_only')['global_active_power']
        .sum()
        .reset_index(name='total_power')
    )

    # ======================
    # 2. RATA-RATA
    # ======================
    avg_usage = float(df['global_active_power'].mean())

    # ======================
    # 3. PEAK HOUR
    # ======================
    hourly_usage = (
        df.groupby('hour')['global_active_power']
        .mean()
        .reset_index()
    )

    peak_row = hourly_usage.loc[
        hourly_usage['global_active_power'].idxmax()
    ]

    peak_hour = int(peak_row['hour'])
    peak_value = float(peak_row['global_active_power'])

    # ======================
    # 4. MAX & MIN
    # ======================
    max_usage = float(df['global_active_power'].max())
    min_usage = float(df['global_active_power'].min())

    # ======================
    # 5. SUB METERING
    # ======================
    sub_metering = {
        "sub1": float(df['sub_metering_1'].sum()),
        "sub2": float(df['sub_metering_2'].sum()),
        "sub3": float(df['sub_metering_3'].sum())
    }

    return {
        "daily_usage": daily_usage,
        "avg_usage": avg_usage,
        "peak_hour": peak_hour,
        "peak_value": peak_value,
        "max_usage": max_usage,
        "min_usage": min_usage,
        "sub_metering": sub_metering
    }