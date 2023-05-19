import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
import pandas as pd
import json
from utils.get_aws_secrets import get_secret

try:
    USER = os.environ["SNOWFLAKE_USER"]
    PASSWORD = os.environ["SNOWFLAKE_PASS"]

except KeyError as e:
    print("Did you set your envs? Using data model creds")
    print("\n", e)

    creds = json.loads(get_secret("data_model"))
    USER = creds["USER"]
    PASSWORD = creds["PASS"]

ACCOUNT = "wkb13332"
ROLE = "ANALYTICS"
WAREHOUSE = "COMPUTE_WH"
DATABASE = "ANALYTICS"


class PlanetSnowflake:
    def __init__(
        self, database=DATABASE, account=ACCOUNT, user=USER, password=PASSWORD, role=ROLE, warehouse=WAREHOUSE
    ):
        self.database = database
        self.account = account
        self.user = user
        self.password = password
        self.role = role
        self.warehouse = warehouse

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.conn.close()

    def connect(self):
        self.conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            role=self.role,
            warehouse=self.warehouse,
            database=self.database,
        )
        return self.conn

    def run_sql(self, sql: str, verbose: bool = False):
        self.cursor = self.conn.cursor()

        # executes the code and prints the sql and error if errors
        try:
            self.cursor.execute(sql)
        except ProgrammingError as e:
            raise ProgrammingError(f"SQL statement failed, here's the sql:\n{sql}\nand the error:\n{e}")

        self.conn.commit()
        self.cursor.close()

    def fetch_table_as_df(self, sql: str, verbose: bool = False):
        self.cursor = self.conn.cursor()
        if verbose:
            print(sql)
        self.cursor.execute(sql)
        res = self.cursor.fetch_pandas_all()
        self.cursor.close()
        return res


if __name__ == "__main__":
    with PlanetSnowflake() as ps:
        df = ps.fetch_table_as_df(
            "select model_id, count(*) from model_output_v3.company_indicator_scores_part_z group by model_id"
        )
    print(df.head())
