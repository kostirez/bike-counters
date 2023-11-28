from sqlalchemy import create_engine, MetaData, Table
import pandas as pd
from config.db_config import DATABASE_URL

class Db:
    def __init__(self):
        self.__engine = create_engine(DATABASE_URL)
        self.__dbConnection = self.__engine.connect()
        self.__meta = MetaData()

    def get_df(self, command):
        return pd.read_sql(command, self.__dbConnection);

    def insert_df(self, df, table_name, if_exists='append'):
        df.to_sql(table_name, self.__dbConnection, if_exists=if_exists, index=False)
        self.__dbConnection.commit()
        print('insert', df)

    def create_table(self, name, columns):
        Table(
            name,
            self.__meta, 
            *columns
        )
        self.__meta.create_all(self.__engine)

    def close(self): 
        self.__dbConnection.close()

