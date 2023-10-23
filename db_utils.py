import yaml
import pandas as pd
from sqlalchemy import create_engine


class RDSDatabaseConnector:
    def __init__(self, credentials):
        self.credentials = yaml_to_dict(credentials)

    def engine(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        engine = create_engine(
            f"{DATABASE_TYPE}+{DBAPI}://{self.credentials['RDS_USER']}"
            f":{self.credentials['RDS_PASSWORD']}@"
            f"{self.credentials['RDS_HOST']}"
            f":{self.credentials['RDS_PORT']}/"
            f"{self.credentials['RDS_DATABASE']}")
        return engine


def yaml_to_dict(yaml_file):
    with open(yaml_file, 'r') as file:
        return yaml.safe_load(file)


def RDS_to_df(table_name, engine_name):
    table = pd.read_sql_table(table_name, engine_name)
    table.Name = table_name
    return table


def df_to_csv(dataframe_name):
    dataframe_name.to_csv(f'{dataframe_name.Name}.csv')


loan_connection = RDSDatabaseConnector('credentials.yaml')

table = RDS_to_df('loan_payments', loan_connection.engine())
df_to_csv(table)
