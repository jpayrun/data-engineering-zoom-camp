
import os

from sqlalchemy import create_engine
import pandas as pd
import pyarrow
import psycopg2

def ingest_callable(table_name, password, host, user, port, db):
    csv = "output.csv"

    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # df = pd.read_parquet(csv)
    # df.head(0).to_sql(name = table_name, con=con, if_exists='replace')
    # df.to_sql(name=table_name, con=con, if_exists='append')
