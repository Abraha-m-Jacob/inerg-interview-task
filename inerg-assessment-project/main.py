from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import click
from flask.cli import with_appcontext
from models import db, AnnualProductionData
from dotenv import load_dotenv
import os

load_dotenv()


def create_app():
    app = Flask(__name__)

    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URI')
    app.config['DEBUG'] = os.getenv('DEBUG')

    db.init_app(app)
    with app.app_context():
        db.create_all()
        print("Database and tables created successfully")
    return app

app = create_app()

@app.cli.command('load-data')
@click.argument('file')
# @click.option("--overwrite", is_flag=True, help="Set this flag to overwrite the existing data")
@with_appcontext
# def load_data(file, overwrite):
def load_data(file):
    df = pd.read_excel(file)
    print("Columns", df.columns)
    grouped_df = df.groupby('API WELL  NUMBER').agg({
        'OIL': 'sum',
        'GAS': 'sum',
        'BRINE': 'sum'
    }).reset_index()

    # if overwrite:
    db.session.query(AnnualProductionData).delete()
    db.session.commit()
    print("Existing data deleted. Inserting new data")
    new_entries = []
    for _, row in grouped_df.iterrows():
        print("index", _, "API WELL NUMBER", row['API WELL  NUMBER'], "Oil", row['OIL'], "Gas", row['GAS'], "Brine", row['BRINE'])
        new_data = AnnualProductionData(api_well_number=row['API WELL  NUMBER'].astype(str), oil=row['OIL'].astype(str), gas=row['GAS'].astype(str), brine=row['BRINE'].astype(str))
        new_entries.append(new_data)
    db.session.bulk_save_objects(new_entries)
    db.session.commit()
    print("New data inserted successfully")

@app.route('/data', methods=['GET'])
def get_annual_data():
    api_well_number = request.args.get('well')
    if not api_well_number:
        return jsonify({"error": "Well number is required"}), 400
    print("api_well_number", api_well_number)
    data = AnnualProductionData.query.filter_by(api_well_number=api_well_number).first()
    print("data", data)
    if not data:
        return jsonify({"error": f"No data found for API WELL NUMBER {api_well_number}"}), 404
    return jsonify({
        "oil": data.oil,
        "gas": data.gas,
        "brine": data.brine
    })

if __name__ == "__main__":
    app.run(debug=os.getenv('DEBUG'))