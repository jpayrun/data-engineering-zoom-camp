import argparse
import os

from sqlalchemy import create_engine
import pandas as pd
import psycopg2

def main(parser: argparse.Namespace):
    table_name = parser.table_name
    host = parser.host
    user = parser.user
    password = parser.password
    port = parser.port
    db = parser.db
    url = parser.url
    csv = "output.csv"

    con = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')

    os.system(f'wget -O - {url} | gunzip -c > {csv}')
    df = pd.read_csv(csv, nrows=1)
    df.head(0).to_sql(name = table_name, con=con, if_exists='replace')
    for chunk in pd.read_csv(csv, iterator=True, chunksize=100_000):
        chunk.assign(lpep_pickup_datetime = pd.to_datetime(chunk.lpep_pickup_datetime), 
                    lpep_dropoff_datetime = pd.to_datetime(chunk.lpep_dropoff_datetime))
        chunk.to_sql(name=table_name, con=con, if_exists='append')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--url', help='Download taxi data link')
    parser.add_argument('--table_name', help='Table name')
    parser.add_argument('--host', default='localhost', help="pg host")
    parser.add_argument('--user', default='root', help='User name')
    parser.add_argument('--password', default='root', help='pg password')
    parser.add_argument('--port', default=5432, help='pg port')
    parser.add_argument('--db', default='ny_taxi', help='pg database')
    args = parser.parse_args()

    main(args)
