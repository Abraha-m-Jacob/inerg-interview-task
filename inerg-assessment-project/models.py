from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class AnnualProductionData(db.Model):

    __tablename__ = "annual_production_data"

    id = db.Column(db.Integer, primary_key=True)
    api_well_number = db.Column(db.String(20), nullable=False)
    oil = db.Column(db.Integer, nullable=False)
    gas = db.Column(db.Integer, nullable=False)
    brine = db.Column(db.Integer, nullable=False)

    def __init__(self, api_well_number, oil, gas, brine):
        self.api_well_number = api_well_number
        self.oil = oil
        self.gas = gas
        self.brine = brine
