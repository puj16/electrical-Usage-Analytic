from db import fetch_dataframe, get_connection
from analysis import transform

def main():
    print("Start ETL...")

    try:
        # ======================
        # EXTRACT
        # ======================
        query = "SELECT * FROM electricity_usage"
        df = fetch_dataframe(query)

        print(f"Data diambil: {len(df)} rows")

        if df.empty:
            print("Data kosong ❌")
            return

        # ======================
        # TRANSFORM
        # ======================
        result = transform(df)

        if not result:
            print("Transform gagal ❌")
            return

        conn = get_connection()
        cursor = conn.cursor()

        # ======================
        # 1. DAILY USAGE
        # ======================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_usage (
            date DATE,
            total_power DOUBLE
        )
        """)

        cursor.execute("DELETE FROM daily_usage")

        daily_df = result["daily_usage"]
        data = list(daily_df.itertuples(index=False, name=None))

        cursor.executemany(
            "INSERT INTO daily_usage (date, total_power) VALUES (%s, %s)",
            data
        )

        print("daily_usage inserted")

        # ======================
        # 2. SUMMARY STATS
        # ======================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS summary_stats (
            metric VARCHAR(50),
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

        cursor.executemany(
            "INSERT INTO summary_stats (metric, value) VALUES (%s, %s)",
            summary_data
        )

        print("summary_stats inserted")

        # ======================
        # 3. SUB METERING
        # ======================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sub_metering_stats (
            type VARCHAR(50),
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

        cursor.executemany(
            "INSERT INTO sub_metering_stats (type, total) VALUES (%s, %s)",
            sub_data
        )

        print("sub_metering_stats inserted")

        conn.commit()
        print("Commit berhasil ✅")

    except Exception as e:
        print("Error ❌:", e)

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

    print("Selesai 🚀")

if __name__ == "__main__":
    main()