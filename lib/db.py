from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey
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



    # get tables

    def get_detection_for_counter(self, counter_id):
        command = f"SELECT * FROM detection WHERE counter_direction_id = '{counter_id}'"
        return self.__get_df(command)
    
    def get_clean_detection_for_counter(self):
        command = f"SELECT * FROM clean_detections"
        return self.__get_df(command)
    
    def get_day_distribution_for_counter(self, counter_id):
        command = f"SELECT * FROM day_distribution WHERE counter_direction_id = '{counter_id}'"
        return self.__get_df(command)
    
    def get_all_counters(self):
        command = f"SELECT * FROM counter"
        return self.__get_df(command)
    


    # create tables

    def create_counter_table(self):
        columns = [
            Column('location_id', String, nullable=False), 
            Column('name', String, nullable=False), 
            Column('route', String, nullable=False),
            Column('direction_id', String, primary_key=True, nullable=False),
            Column('direction_name', String, nullable=False) 
        ]
        self.create_table('counter', columns)  


    def create_detection_table(self):
        columns = [
            Column('counter_direction_id', String, primary_key=True, nullable=False), 
            Column('time', String, primary_key=True, nullable=False), 
            Column('bikes', Integer, nullable=False),
            Column('pedestrians', Integer, nullable=False),
        ]
        self.create_table('detection', columns)  
        

    def create_distribution_table(self):
        columns = [
            Column('counter_direction_id', String, primary_key=True, nullable=False), 
            Column('time', String, primary_key=True, nullable=False), 
            Column('bikes', Integer, nullable=False),
            Column('pedestrians', Integer, nullable=False),
            Column('dayofweek', Integer, primary_key=True, nullable=False),
            Column('bikes_mean_rolling', Integer, nullable=False),
            Column('pedestrians_mean_rolling', Integer, nullable=False) 
        ]
        self.create_table('day_distribution', columns)  
