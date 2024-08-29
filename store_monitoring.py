import pandas as pd
import sqlite3
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, request, send_file
import uuid
import os
import pytz

# Initialize Flask app
app = Flask(__name__)

# Define the database file location
DATABASE = "store_monitoring.db"

def load_csvs_to_db():
    try:
        # Load the CSV files with correct file paths
        status_df = pd.read_csv(r"New folder/store status.csv")
        hours_df = pd.read_csv(r"New folder/bq-results-20230125-202210-1674678181880.csv")
        timezone_df = pd.read_csv(r"New folder/Menu hours.csv")

        # Print the first few rows to verify correct loading
        print("Status DataFrame:", status_df.head())
        print("Hours DataFrame:", hours_df.head())
        print("Timezone DataFrame:", timezone_df.head())

        # Convert the timestamp to datetime, handling different formats and time zones
        status_df["timestamp_utc"] = pd.to_datetime(status_df["timestamp_utc"], errors="coerce", format="%Y-%m-%d %H:%M:%S.%f %Z")
        status_df["timestamp_utc"] = status_df["timestamp_utc"].dt.tz_localize("UTC", errors='coerce')

        # Create a database connection
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()

        # Store the data into the database
        status_df.to_sql("store_status", conn, if_exists="replace", index=False)
        hours_df.to_sql("business_hours", conn, if_exists="replace", index=False)
        timezone_df.to_sql("store_timezones", conn, if_exists="replace", index=False)

        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error loading and storing CSV files: {e}")

@app.route("/")
def index():
    return "Welcome to the Store Monitoring API!"

@app.route("/trigger_report", methods=["POST"])
def trigger_report():
    report_id = str(uuid.uuid4())
    current_time = datetime.now(timezone.utc)

    try:
        # Create a database connection
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()

        # Create a placeholder report entry
        c.execute("INSERT INTO reports (report_id, status) VALUES (?, ?)", (report_id, "Running"))
        conn.commit()
        conn.close()

        # Start report generation
        generate_report(report_id, current_time)

        return jsonify({"report_id": report_id})
    except Exception as e:
        return jsonify({"error": f"Failed to trigger report: {e}"}), 500

@app.route("/get_report", methods=["GET"])
def get_report():
    report_id = request.args.get("report_id")

    if not report_id:
        return jsonify({"error": "Missing report_id parameter"}), 400

    try:
        # Create a database connection
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()

        # Check the status of the report
        c.execute("SELECT status FROM reports WHERE report_id = ?", (report_id,))
        status = c.fetchone()
        conn.close()

        if status is None:
            return jsonify({"error": "Invalid report_id"}), 404

        if status[0] == "Running":
            return jsonify({"status": "Running"})

        # Assuming report is complete, return the report CSV file
        report_path = f"report_{report_id}.csv"
        if os.path.exists(report_path):
            return send_file(
                report_path,
                as_attachment=True,
                mimetype='text/csv',
                download_name=f"{report_id}.csv"  # Correct parameter to set filename
            )
        else:
            return jsonify({"error": "Report not found"}), 404
    except Exception as e:
        return jsonify({"error": f"Failed to get report: {e}"}), 500

def generate_report(report_id, current_time):
    try:
        report_path = f"report_{report_id}.csv"
        with open(report_path, "w") as f:
            # Write the header
            f.write(
                "store_id,uptime_last_hour,uptime_last_day,uptime_last_week,downtime_last_hour,downtime_last_day,downtime_last_week\n"
            )

            # Fetch data from the database
            conn = sqlite3.connect(DATABASE)
            c = conn.cursor()

            # Adjusted SQL query to differentiate between uptime and downtime based on status
            c.execute(
                """
                SELECT 
                    store_id,
                    SUM(CASE WHEN status = 'active' AND timestamp_utc >= datetime('now', '-1 hour') THEN 1 ELSE 0 END) AS uptime_last_hour,
                    SUM(CASE WHEN status = 'active' AND timestamp_utc >= datetime('now', '-1 day') THEN 1 ELSE 0 END) AS uptime_last_day,
                    SUM(CASE WHEN status = 'active' AND timestamp_utc >= datetime('now', '-7 days') THEN 1 ELSE 0 END) AS uptime_last_week,
                    SUM(CASE WHEN status = 'inactive' AND timestamp_utc >= datetime('now', '-1 hour') THEN 1 ELSE 0 END) AS downtime_last_hour,
                    SUM(CASE WHEN status = 'inactive' AND timestamp_utc >= datetime('now', '-1 day') THEN 1 ELSE 0 END) AS downtime_last_day,
                    SUM(CASE WHEN status = 'inactive' AND timestamp_utc >= datetime('now', '-7 days') THEN 1 ELSE 0 END) AS downtime_last_week
                FROM store_status
                GROUP BY store_id
            """
            )

            rows = c.fetchall()

            for row in rows:
                f.write(",".join(map(str, row)) + "\n")

        # Update report status to 'Complete'
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute("UPDATE reports SET status = ? WHERE report_id = ?", ("Complete", report_id))
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error generating report: {e}")

if __name__ == "__main__":
    try:
        # Ensure the necessary table exists
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY,
                status TEXT
            )
            """
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error creating the database table: {e}")

    # Load CSVs into the database
    load_csvs_to_db()

    # Run the Flask app
    app.run(debug=True) 