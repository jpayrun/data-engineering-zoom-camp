import argparse
import os

import pandas as pd
from sqlalchemy import create_engine

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url
    csv = 'output.csv'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    os.system(f"wget -O - {url} | gunzip -c > {csv}")

    df = pd.read_csv(csv, nrows=1)
    df.head(0).to_sql(name=table, con=engine, if_exists='replace')
    for chunk in pd.read_csv(csv, iterator=True, chunksize=100_000):
        chunk.assign(tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime), 
                    tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime))
        chunk.to_sql(name=table, con=engine, if_exists='append')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table', help='table name to write results to')
    parser.add_argument('--url', help='url of the csv file')
    args = parser.parse_args()

    main(args)
