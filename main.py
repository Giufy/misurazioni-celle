import pandas as pd

from Database import Database
from ReaderFactory import ReaderFactory


class DataframeAlias:
    def __init__(self, df: pd.DataFrame, column_alias: str):
        self.df = df
        self.column_alias = column_alias

    def as_column(self, column_name: str):
        self.df.rename(mapper={self.column_alias: column_name}, axis=1, inplace=True)


class DataframeHandler:

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.mappa_colonne = {}
        self.mapping_table = None
        self.database = Database.get_default()

    def drop_null_rows(self, *columns: str):
        for column in columns:
            self.df = self.df[self.df[column].notnull()]

    def extract_mapping(self):
        return {tipologia: tipologia_id for tipologia_id, tipologia in
                self.database.read_table(self.mapping_table)}

    def get_mapping(self, database: Database):
        pass

    def map_columns_name(self, mappa_colonne):
        self.df.rename(mapper=mappa_colonne, axis=1, inplace=True)

    class MappingUtils:
        def __init__(self, df, column_name, keep_original):
            self.column_name = column_name
            self.keep_original = keep_original
            self.df = df
            self.column_alias = column_name + '_mapped'

        def with_mapping(self, values_map):
            self._map(values_map)
            self._drop_if_needed()
            return DataframeAlias(self.df, self.column_alias)

        def _map(self, values_map):
            self.df[self.column_alias] = self.df.apply(lambda row: values_map[row[self.column_name].strip()], axis=1)

        def _drop_if_needed(self):
            if not self.keep_original:
                self.df.drop(labels=self.column_name, axis=1, inplace=True)

    def replace_column(self, column_name: str):
        return self.MappingUtils(self.df, column_name, keep_original=False)

    def map_columns_if_needed(self):
        if bool(self.mappa_colonne):
            self.map_columns_name(self.mappa_colonne)

    def set_mappa_colonne(self, mappa_colonne):
        self.mappa_colonne = mappa_colonne

    def set_mapping_table(self, table: str, schema: str = 'public'):
        self.mapping_table = f'{schema}.{table}'

    def set_database(self, database: Database):
        self.database = database


def main():
    do_upsert = False
    file_name = 'C:\\Users\\Giulia\\Desktop\\database celle singole.xlsx'
    database = Database.get_default()
    excel = ReaderFactory.get_dataframe_from_excel(file_name)
    table = 'misurazioni_celle'
    schema = 'raw'

    handler = DataframeHandler(excel)
    handler.set_mappa_colonne(
        {"ID": 'id', "Data": 'data', 'h': 'ora', "mV": 'mV', "mA picco": "mA_picco", 'mA 5s': 'mA_5s',
         "microW": "microW", "Data Costruzione": 'data_costruzione',
         "giorni di attività": 'giorni_attivita'})
    handler.set_mapping_table(table='tipologia_celle')

    handler.drop_null_rows('tipologia')
    handler.replace_column('tipologia').with_mapping(handler.extract_mapping()).as_column('id_tipologia')
    handler.map_columns_if_needed()
    print(handler.df)
    database.insert_if_new(handler.df, table, schema, ['id', 'data', 'ora'], do_upsert=do_upsert)


main()
