import pandas as pd
import sqlalchemy as db


class Database:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.engine = db.create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}/{self.database}')

    def read_table(self, table_name):
        with self.engine.connect() as connection:
            return connection.execute(f'select * from {table_name}').fetchall()

    def insert_if_new(self, df: pd.DataFrame, table: str, schema: str, primary_keys: list, do_upsert: bool):
        df_to_insert, df_to_update = self._split_dataframe(df, table, schema, primary_keys)
        self.insert(df_to_insert, table, schema)
        if do_upsert:
            self.update(df_to_update, table, schema, primary_keys)
        print(
            f'Aggiunte {len(df_to_insert.index)} righe e {"NON" if not do_upsert else ""} Aggiornate {len(df_to_update.index)} righe')

    def insert(self, df: pd.DataFrame, table: str, schema: str):
        with self.engine.connect() as connection:
            df.to_sql(table, connection, schema, if_exists='append', index=False)

    def update(self, df: pd.DataFrame, table: str, schema: str, primary_keys: list):
        def delete_rows():
            stmt_delete = ''
            for i in range(len(df)):
                conditions = [f"{pk} = '{df[pk][i]}'" for pk in primary_keys]
                stmt_delete += f"DELETE FROM {schema}.{table} WHERE {' AND '.join(conditions)};\n"

            if stmt_delete:
                self.engine.execute(stmt_delete)

        delete_rows()
        self.insert(df, table, schema)

    def _split_dataframe(self, df: pd.DataFrame, table: str, schema: str, primary_keys: list) -> (
            pd.DataFrame, pd.DataFrame):
        sql = f"SELECT {', '.join(primary_keys)} FROM {schema}.{table}"
        sql_df = pd.read_sql(sql=sql, con=self.engine.connect())
        df.set_index(keys=primary_keys, inplace=True)
        sql_df.set_index(keys=primary_keys, inplace=True)
        df_merge = df.merge(sql_df, how='left', indicator=True, left_index=True, right_index=True)['_merge']
        return df[df_merge == 'left_only'].reset_index(), df[df_merge == 'both'].reset_index()

    @classmethod
    def get_default(cls):
        return Database(host='localhost', database='postgres', user='postgres', password='postgres')
