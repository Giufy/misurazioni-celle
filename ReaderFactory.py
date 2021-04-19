import pandas as pd

from Database import Database

class ReaderFactory:
    @classmethod
    def get_dataframe_from_excel(cls, name :str):
        return pd.read_excel(name)

    @classmethod
    def get_dataframe_from_database(cls, database: Database, schema: str, table: str):
        return pd.read_sql(f'select * from {schema}.{table}', database.engine.connect())
