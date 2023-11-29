"""
The script downloads all data about bike counters from Golemio
and save it into the database table called counter.
"""


import asyncio
from lib.async_api import Api
from lib.db import Db

async def get_counters():
    db = Db()
    api = Api()

    print('Creating counter table')
    db.create_counter_table()

    print('Getting counters')
    counters = await api.get_counters()

    print('Inserting counter to database')

    db.insert_df(counters, 'counter', if_exists='replace')
    
    print('finished')


if __name__=="__main__":
    asyncio.run(get_counters())



