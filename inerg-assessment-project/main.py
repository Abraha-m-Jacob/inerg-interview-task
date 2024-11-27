import logging
import os
from logging.handlers import RotatingFileHandler

import click
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask.cli import with_appcontext
from flask_sqlalchemy import SQLAlchemy

from models import AnnualProductionData, db

load_dotenv()


def setup_logger(app):
    """Set up logger to write to a file with rotation."""
    if not os.path.exists("logs"):
        os.mkdir("logs")
    handler = RotatingFileHandler(
        "logs/app.log", maxBytes=10 * 1024 * 1024, backupCount=3
    )
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)


def create_app():
    app = Flask(__name__)

    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URI")
    app.config["DEBUG"] = os.getenv("DEBUG")

    db.init_app(app)
    setup_logger(app)
    with app.app_context():
        db.create_all()
        app.logger.info("Database and tables created successfully")
    return app


app = create_app()


@app.cli.command("load-data")
@click.argument("file")
@with_appcontext
def load_data(file):
    """
    Loads production data from an Excel file, processes it, and inserts it into the database.

    This function reads data from the provided Excel file, groups it by the "API WELL  NUMBER",
    aggregates the oil, gas, and brine columns, and then deletes any existing entries from the
    database before inserting the newly processed data.

    Args:
        file (str): The path to the Excel file containing the production data to be loaded.
                    The file should have columns "API WELL  NUMBER", "OIL", "GAS", and "BRINE".

    Example:
        $ flask load-data data.xlsx
        This will read the data from "data.xlsx", process it, and update the database.

    Raises:
        ValueError: If the Excel file does not contain the required columns.
    """
    app.logger.info(f"Loading data from file: {file}")
    try:
        df = pd.read_excel(file)
        app.logger.info(f"Excel file loaded successfully with columns: {df.columns}")
        grouped_df = (
            df.groupby("API WELL  NUMBER")
            .agg({"OIL": "sum", "GAS": "sum", "BRINE": "sum"})
            .reset_index()
        )
        app.logger.info("Existing data will be deleted from the database.")
        db.session.query(AnnualProductionData).delete()
        db.session.commit()
        new_entries = []
        for _, row in grouped_df.iterrows():
            new_data = AnnualProductionData(
                api_well_number=row["API WELL  NUMBER"].astype(str),
                oil=row["OIL"].astype(str),
                gas=row["GAS"].astype(str),
                brine=row["BRINE"].astype(str),
            )
            new_entries.append(new_data)
        db.session.bulk_save_objects(new_entries)
        db.session.commit()
        app.logger.info("New data inserted successfully.")
    except Exception as e:
        app.logger.error(f"Error occured while loading data: {str(e)}", exc_info=True)
        raise


@app.route("/data", methods=["GET"])
def get_annual_data():
    """
    Fetches annual production data for a specific API well number.

    This route accepts a query parameter `well` representing the API well number,
    retrieves the corresponding production data (oil, gas, and brine) from the database,
    and returns it in JSON format. If the well number is not provided or no data is found
    for the provided well number, an error message is returned.

    Args:
        None (except for the query parameter `well`)

    Query Parameters:
        well (str): The API WELL NUMBER for which the production data is requested.

    Returns:
        JSON response:
            - If data is found: {"oil": <oil_value>, "gas": <gas_value>, "brine": <brine_value>}
            - If no well number is provided or data is not found: {"error": "<error_message>"}

    Error Responses:
        - 400 Bad Request: If the `well` query parameter is missing.
        - 404 Not Found: If no data is found for the provided API WELL NUMBER.

    Example:
        - Request: GET /data?well=1234567890
        - Response:
            {
                "oil": "1000",
                "gas": "500",
                "brine": "300"
            }

    Logs:
        - Logs a warning if no `well` parameter is provided.
        - Logs information about the request and data retrieval process.
        - Logs an error if no data is found for the given well number.
    """
    api_well_number = request.args.get("well")
    if not api_well_number:
        app.logger.warning("No well number provided in the request.")
        return jsonify({"error": "Well number is required"}), 400
    app.logger.info(f"Fetching data for API WELL NUMBER: {api_well_number}")
    data = AnnualProductionData.query.filter_by(api_well_number=api_well_number).first()
    if not data:
        app.logger.error(f"No data found for API WELL NUMBER {api_well_number}")
        return (
            jsonify({"error": f"No data found for API WELL NUMBER {api_well_number}"}),
            404,
        )
    app.logger.info(f"Data found for API WELL NUMBER {api_well_number}")
    return jsonify({"oil": data.oil, "gas": data.gas, "brine": data.brine})


if __name__ == "__main__":
    app.run(debug=os.getenv("DEBUG"))
