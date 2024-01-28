import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Gather creds
dummy = load_dotenv()
DATABASE_USER = os.environ.get("DATABASE_USER")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DATABASE_HOST = os.environ.get("DATABASE_HOST")
engine = create_engine(f"mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:3306")

# loop through paxminer.regions schema_name, add json column to each users table
with engine.connect() as conn:
    schema_names = pd.read_sql("SELECT schema_name FROM paxminer.regions", conn)
    for schema_name in schema_names["schema_name"]:
        try:
            conn.execute(f"ALTER TABLE {schema_name}.users ADD COLUMN json JSON")
        except Exception as e:
            print(f"Error: {e}")
