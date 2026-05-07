from db import fetch_dataframe, get_connection
from analysis import transform

def main():
    print("Start ETL...")

    df = fetch_dataframe("SELECT * FROM dataset")

    result = transform(df)

    if not result:
        print("No data")
        return

    conn = get_connection()
    cursor = conn.cursor()

    # ======================
    # DAILY USAGE
    # ======================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_usage (
        date DATE PRIMARY KEY,
        total_power DOUBLE
    )
    """)

    data = list(result["daily_usage"].itertuples(index=False, name=None))

    cursor.executemany("""
    INSERT INTO daily_usage (date, total_power)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        total_power = VALUES(total_power)
    """, data)

    # ======================
    # HOURLY USAGE
    # ======================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS hourly_usage (
        hour INT PRIMARY KEY,
        avg_power DOUBLE
    )
    """)

    data = list(result["hourly_usage"].itertuples(index=False, name=None))

    cursor.executemany("""
    INSERT INTO hourly_usage (hour, avg_power)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        avg_power = VALUES(avg_power)
    """, data)

    # ======================
    # WEEKLY USAGE
    # ======================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weekly_usage (
        week VARCHAR(30) PRIMARY KEY,
        total_power DOUBLE
    )
    """)

    data = list(result["weekly_usage"].itertuples(index=False, name=None))

    cursor.executemany("""
    INSERT INTO weekly_usage (week, total_power)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        total_power = VALUES(total_power)
    """, data)

    # ======================
    # MONTHLY USAGE
    # ======================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS monthly_usage (
        month VARCHAR(10) PRIMARY KEY,
        total_power DOUBLE
    )
    """)

    data = list(result["monthly_usage"].itertuples(index=False, name=None))

    cursor.executemany("""
    INSERT INTO monthly_usage (month, total_power)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        total_power = VALUES(total_power)
    """, data)

    # ======================
    # WEEKDAY PATTERN
    # ======================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weekday_pattern (
        day_name VARCHAR(20) PRIMARY KEY,
        avg_power DOUBLE
    )
    """)

    data = list(result["weekday_pattern"].itertuples(index=False, name=None))

    cursor.executemany("""
    INSERT INTO weekday_pattern (day_name, avg_power)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        avg_power = VALUES(avg_power)
    """, data)

    # ======================
    # MONTHLY HOURLY USAGE
    # ======================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS monthly_hourly_usage (
        month VARCHAR(10),
        hour INT,
        avg_power DOUBLE,
        PRIMARY KEY (month, hour)
    )
    """)

    data = list(result["monthly_hourly_usage"].itertuples(index=False, name=None))

    cursor.executemany("""
    INSERT INTO monthly_hourly_usage (month, hour, avg_power)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        avg_power = VALUES(avg_power)
    """, data)

    # ======================
    # PEAK MONTHLY
    # ======================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS peak_monthly (
        month VARCHAR(10) PRIMARY KEY,
        hour INT,
        value DOUBLE
    )
    """)

    data = list(result["peak_monthly"].itertuples(index=False, name=None))

    cursor.executemany("""
    INSERT INTO peak_monthly (month, hour, value)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        hour = VALUES(hour),
        value = VALUES(value)
    """, data)

    # ======================
    # SUMMARY STATS
    # ======================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS summary_stats (
        metric VARCHAR(50) PRIMARY KEY,
        value DOUBLE
    )
    """)

    cursor.execute("DELETE FROM summary_stats")

    summary_data = [
        ("avg_usage", result["avg_usage"]),
        ("peak_hour", result["peak_hour"]),
        ("peak_value", result["peak_value"]),
        ("max_usage", result["max_usage"]),
        ("min_usage", result["min_usage"]),
    ]

    cursor.executemany("""
    INSERT INTO summary_stats (metric, value)
    VALUES (%s, %s)
    """, summary_data)

    # ======================
    # SUB METERING
    # ======================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sub_metering_stats (
        type VARCHAR(50) PRIMARY KEY,
        total DOUBLE
    )
    """)

    cursor.execute("DELETE FROM sub_metering_stats")

    sub = result["sub_metering"]

    sub_data = [
        ("sub1", sub["sub1"]),
        ("sub2", sub["sub2"]),
        ("sub3", sub["sub3"]),
    ]

    cursor.executemany("""
    INSERT INTO sub_metering_stats (type, total)
    VALUES (%s, %s)
    """, sub_data)

    conn.commit()
    cursor.close()
    conn.close()

    print("ETL Done ✅")

if __name__ == "__main__":
    main()