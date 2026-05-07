from flask import Flask
from main import run_etl

app = Flask(__name__)

@app.route("/")
def home():
    return {
        "status": "running"
    }

@app.route("/run-etl", methods=["POST"])
def trigger_etl():

    try:
        run_etl()

        return {
            "status": "success",
            "message": "ETL completed"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)